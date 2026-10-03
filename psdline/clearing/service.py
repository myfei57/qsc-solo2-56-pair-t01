"""清客确认服务：对外命令与发车结论的唯一入口。

线程模型：服务内一把可重入锁串行化所有写命令；每条命令在锁内完成"校验 → 折叠
出当前态 → 追加事件到哈希链 → 重新折叠"。不同门区的确认因此可以并发提交而互不
覆盖（每条确认只追加自己的事件），但读结论永远拿到链折叠出的同一份状态。

幂等：每条写命令要求带 ``request_id``（客户端生成的去重键）。同一个键重复提交
（典型场景：超时重试、掉线重连后客户端不知道上一笔落没落）直接返回**当初那笔
的结果**，不产生第二条事件。键里建议由调用方拼上 ``session_id + 动作 + 门区``。

断线重连：客户端什么都不用做——重放同一批 request_id 安全；拿不准当前进度就调
:meth:`snapshot` 对齐。服务端状态全在哈希链上，进程重启 :meth:`restore` 即可。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any

from ..clock import Clock
from ..errors import InterlockError, RailError
from .events import (
    EPOCH_BUMPED,
    INTRUSION_REPORTED,
    INTRUSION_RESOLVED,
    REASON_INTRUSION,
    REASON_TIMEOUT,
    SESSION_FINALIZED,
    SESSION_STARTED,
    CLEARANCE_DISPATCHED,
    ZONE_CONFIRMED,
    ClearingEvent,
)
from .journal import ClearingJournal
from .state import (
    GO,
    STAGE_DISPATCHED,
    ClearanceVerdict,
    SessionState,
    verdict_of,
)

POLICY_PRESERVE = "preserve"  # 超时保留已确认，靠 TTL 让老确认收口时失效
POLICY_RESET = "reset"        # 超时整单代际加一，已确认全部失效重来
TIMEOUT_POLICIES = frozenset({POLICY_PRESERVE, POLICY_RESET})


class ClearingConflict(RailError):
    """命令基于过期状态（旧代际/旧版本），需重新拉取快照后再试。"""

    def __init__(self, message: str, *, snapshot: dict[str, Any] | None = None) -> None:
        super().__init__(message, component="clearing")
        self.snapshot = snapshot or {}


def session_id_of(train_id: str, station_id: str, visit: int) -> str:
    return f"{train_id}@{station_id}#{visit:03d}"


@dataclass
class _Session:
    """内存中一个会话：链折叠出的当前态。"""

    state: SessionState
    timeout_policy: str

    def rebuild(self, events: list[ClearingEvent]) -> None:
        self.state = SessionState.from_events(self.state.session_id, events, self.timeout_policy)


class ClearingService:
    """清客确认的领域服务。"""

    def __init__(
        self,
        journal: ClearingJournal,
        clock: Clock,
        *,
        zones_per_station: dict[str, list[str]] | None = None,
        confirm_ttl_s: float = 120.0,
        timeout_policy: str = POLICY_PRESERVE,
    ) -> None:
        if timeout_policy not in TIMEOUT_POLICIES:
            raise RailError(f"未知清客超时策略: {timeout_policy}", component="clearing")
        if confirm_ttl_s <= 0:
            raise RailError("清客确认有效期必须为正", component="clearing")
        self._journal = journal
        self._clock = clock
        self._zones = {station: list(zones) for station, zones in (zones_per_station or {}).items()}
        self._ttl_s = float(confirm_ttl_s)
        self._policy_default = timeout_policy
        self._lock = threading.RLock()
        self._sessions: dict[str, _Session] = {}
        # 每列车当前停靠的会话，供"只报 train_id"的调用方定位。
        self._active: dict[str, str] = {}
        # 已发车闭环的会话仍保留索引：结论与历史要能被审计、门控事后核对。
        self._closed: dict[str, str] = {}

    @property
    def ttl_s(self) -> float:
        return self._ttl_s

    def set_station_zones(self, station_id: str, zones: list[str]) -> None:
        """登记/更新一个站台的门区顺序（在首次到站前配置）。"""

        if not zones:
            raise RailError("门区清单不能为空", component="clearing")
        with self._lock:
            self._zones[station_id] = list(zones)

    # ---- 重建 ---------------------------------------------------------

    def restore(self) -> int:
        """进程重启后从哈希链重建全部会话（先校验链完整性）。"""

        with self._lock:
            all_events = self._journal.load_all(verify=True)
            for session_id, events in all_events.items():
                policy = str(events[0].payload.get("timeout_policy", self._policy_default))
                state = SessionState.from_events(session_id, events, policy)
                self._sessions[session_id] = _Session(state=state, timeout_policy=policy)
                if state.stage != STAGE_DISPATCHED:
                    self._active[state.train_id] = session_id
                else:
                    self._closed[state.train_id] = session_id
            return len(all_events)

    # ---- 内部工具 -----------------------------------------------------

    def _require_request_id(self, request_id: str) -> str:
        key = str(request_id or "").strip()
        if not key:
            raise RailError("缺少幂等键 request_id", component="clearing")
        return key

    def _replay_or_none(self, request_id: str, session_id: str) -> dict[str, Any] | None:
        """幂等命中：返回当初那笔命令的结果。"""

        found = self._journal.by_request(request_id)
        if found is None:
            return None
        original_session, event = found
        if original_session != session_id:
            raise ClearingConflict(
                f"幂等键 {request_id} 属于另一个会话 {original_session}",
                snapshot=self.snapshot(session_id),
            )
        return self._command_result(session_id, replayed=True, replay_event=event.kind)

    def _get(self, session_id: str) -> _Session:
        session = self._sessions.get(session_id)
        if session is None:
            raise RailError(f"清客会话不存在: {session_id}", component="clearing")
        return session

    def _resolve(self, train_id: str, station_id: str = "", visit: int | None = None) -> str:
        if visit is not None and station_id:
            return session_id_of(train_id, station_id, visit)
        session_id = self._active.get(train_id) or self._closed.get(train_id)
        if session_id is None:
            raise RailError(f"列车 {train_id} 当前没有进行中的清客会话", component="clearing")
        return session_id

    def _append(self, session_id: str, event: ClearingEvent) -> ClearingEvent:
        session = self._sessions.get(session_id)
        stored = self._journal.append(event)
        if session is None:
            policy = str(event.payload.get("timeout_policy", self._policy_default))
            session = _Session(
                state=SessionState.from_events(session_id, [stored], policy),
                timeout_policy=policy,
            )
            self._sessions[session_id] = session
        else:
            session.rebuild(self._journal.events(session_id))
        return stored

    def _command_result(
        self,
        session_id: str,
        *,
        replayed: bool = False,
        replay_event: str = "",
    ) -> dict[str, Any]:
        state = self._get(session_id).state
        return {
            "ok": True,
            "replayed": replayed,
            "replayed_event": replay_event if replayed else "",
            **self.snapshot(session_id),
        }

    def _apply_timeout(self, session: _Session, now: float, request_id: str | None = None) -> bool:
        """按超时策略处理到期会话，返回是否产生了失效事件。

        preserve 策略下老确认由 TTL 在投影时软失效，这里无需写事件；reset 策略下
        停站时间超过 TTL 就把全局代际加一（事件落链），全部确认失效。
        """

        state = session.state
        if state.stage in (STAGE_DISPATCHED,):
            return False
        elapsed = now - state.started_at
        if elapsed <= self._ttl_s:
            return False
        if session.timeout_policy == POLICY_PRESERVE:
            return False  # 软失效：snapshot/verdict 里按 confirm_at + TTL 计算
        # reset：同一条会话只按"每跨过一个 TTL 窗口"翻一次，避免每笔命令都翻代。
        window = int((now - state.started_at) // self._ttl_s)
        if window <= state.timeouts:
            return False
        event = ClearingEvent(
            seq=self._journal.next_seq(state.session_id),
            session_id=state.session_id,
            train_id=state.train_id,
            station_id=state.station_id,
            visit=state.visit,
            at=now,
            kind=EPOCH_BUMPED,
            payload={
                "epoch": state.epoch + 1,
                "reason": REASON_TIMEOUT,
                "policy": POLICY_RESET,
                "window": window,
            },
            request_id=request_id or f"timeout:{state.session_id}:{window}",
        )
        self._append(state.session_id, event)
        return True

    # ---- 命令：会话开始 ----------------------------------------------

    def start_session(
        self,
        train_id: str,
        station_id: str,
        visit: int,
        *,
        zones: list[str] | None = None,
        request_id: str,
        timeout_policy: str | None = None,
        at: float | None = None,
    ) -> dict[str, Any]:
        """列车到站、开门后开始一次清客确认会话。

        同一 (列车, 停靠次数) 重复开会话按幂等处理；若该列车上一次会话还没收口，
        拒绝（必须先把上一次发车或显式作废，避免两份结论并存）。
        """

        request_id = self._require_request_id(request_id)
        policy = timeout_policy or self._policy_default
        if policy not in TIMEOUT_POLICIES:
            raise RailError(f"未知清客超时策略: {policy}", component="clearing")
        zone_list = list(zones or self._zones.get(station_id, []))
        if not zone_list:
            raise RailError(f"站台 {station_id} 没有门区清单，无法开始清客", component="clearing")
        if len(set(zone_list)) != len(zone_list):
            raise RailError(f"站台 {station_id} 门区清单有重复", component="clearing")

        session_id = session_id_of(train_id, station_id, visit)
        with self._lock:
            # 幂等优先：同键重放（重复到站、重连重试）直接回当初的结果，
            # 即使该会话已存在/进行中也不报错。
            hit = self._replay_or_none(request_id, session_id)
            if hit is not None:
                return hit
            active_id = self._active.get(train_id)
            if active_id is not None and active_id != session_id:
                raise InterlockError(
                    f"列车 {train_id} 上一次清客会话 {active_id} 尚未闭环",
                    component="clearing",
                )
            if session_id in self._sessions:
                # 会话已存在但用了另一个 request_id：拒绝而不是覆盖。
                raise RailError(f"清客会话已存在: {session_id}", component="clearing")
            moment = self._clock.now() if at is None else at
            event = ClearingEvent(
                seq=1,
                session_id=session_id,
                train_id=train_id,
                station_id=station_id,
                visit=visit,
                at=moment,
                kind=SESSION_STARTED,
                payload={"zones": zone_list, "timeout_policy": policy},
                request_id=request_id,
            )
            self._append(session_id, event)
            self._active[train_id] = session_id
            return self._command_result(session_id)

    # ---- 命令：门区确认 ----------------------------------------------

    def confirm_zone(
        self,
        train_id: str,
        zone_id: str,
        *,
        station_id: str = "",
        visit: int | None = None,
        confirmed_by: str = "staff",
        request_id: str,
        expected_epoch: int | None = None,
        at: float | None = None,
    ) -> dict[str, Any]:
        """确认一个门区清客完毕（允许任意顺序、允许并发确认不同门区）。

        挂起态也允许确认：站务把闯回的乘客劝离后，顺手把该门区重新确认掉，但
        只要还有未解除的闯入，收口仍然拒绝。``expected_epoch`` 用于乐观并发：
        客户端拿着旧代际的页面提交会被拒并带回最新快照，防止覆盖式重放。
        """

        request_id = self._require_request_id(request_id)
        with self._lock:
            now = self._clock.now() if at is None else at
            session_id = self._resolve(train_id, station_id, visit)
            hit = self._replay_or_none(request_id, session_id)
            if hit is not None:
                return hit
            session = self._get(session_id)
            self._apply_timeout(session, now)
            state = session.state
            if state.stage == STAGE_DISPATCHED:
                raise InterlockError("列车已凭清客结论发车，不能再确认门区", component="clearing")
            if not state.known_zone(zone_id):
                raise RailError(
                    f"门区 {zone_id} 不在会话 {session_id} 的清单内", component="clearing"
                )
            if expected_epoch is not None and expected_epoch != state.epoch:
                raise ClearingConflict(
                    f"门区确认基于旧代际 {expected_epoch}，当前代际 {state.epoch}",
                    snapshot=self.snapshot(session_id, at=now),
                )
            event = ClearingEvent(
                seq=self._journal.next_seq(session_id),
                session_id=session_id,
                train_id=train_id,
                station_id=state.station_id,
                visit=state.visit,
                at=now,
                kind=ZONE_CONFIRMED,
                payload={
                    "zone_id": zone_id,
                    "epoch": state.epoch,
                    "confirmed_by": str(confirmed_by or "staff"),
                },
                request_id=request_id,
            )
            self._append(session_id, event)
            return self._command_result(session_id)

    # ---- 命令：闯回挂起 / 解除 ---------------------------------------

    def report_intrusion(
        self,
        train_id: str,
        *,
        station_id: str = "",
        visit: int | None = None,
        zone_ids: list[str] | None = None,
        reason: str = REASON_INTRUSION,
        request_id: str,
        at: float | None = None,
    ) -> dict[str, Any]:
        """登记有人闯回站台：相关门区已确认事实作废，会话重新挂起。

        ``zone_ids`` 为空表示无法定位门区（整节车厢都有乘客往回挤），按全站所有
        门区处理。只要会话还没发车，收口后也允许闯入登记——此时结论立即作废。
        """

        request_id = self._require_request_id(request_id)
        with self._lock:
            now = self._clock.now() if at is None else at
            session_id = self._resolve(train_id, station_id, visit)
            hit = self._replay_or_none(request_id, session_id)
            if hit is not None:
                return hit
            session = self._get(session_id)
            state = session.state
            if state.stage == STAGE_DISPATCHED:
                raise InterlockError("列车已发车，闯入登记应在站台侧另案处理", component="clearing")
            targets = [str(z) for z in (zone_ids or [])]
            unknown = [z for z in targets if not state.known_zone(z)]
            if unknown:
                raise RailError(f"未知门区: {unknown}", component="clearing")
            event = ClearingEvent(
                seq=self._journal.next_seq(session_id),
                session_id=session_id,
                train_id=train_id,
                station_id=state.station_id,
                visit=state.visit,
                at=now,
                kind=INTRUSION_REPORTED,
                payload={"zone_ids": targets, "reason": str(reason or REASON_INTRUSION)},
                request_id=request_id,
            )
            self._append(session_id, event)
            return self._command_result(session_id)

    def resolve_intrusion(
        self,
        train_id: str,
        *,
        station_id: str = "",
        visit: int | None = None,
        request_id: str,
        at: float | None = None,
    ) -> dict[str, Any]:
        """现场处置完毕、站台恢复可控：解除挂起。

        解除挂起不等于自动恢复已确认状态——被闯过门区必须重新逐块确认，再重新
        收口。这是"中途有人闯回要能重新挂起"的安全语义。
        """

        request_id = self._require_request_id(request_id)
        with self._lock:
            now = self._clock.now() if at is None else at
            session_id = self._resolve(train_id, station_id, visit)
            hit = self._replay_or_none(request_id, session_id)
            if hit is not None:
                return hit
            session = self._get(session_id)
            state = session.state
            if not state.suspended:
                # 幂等式无害重复：没有挂起时重复解除直接返回当前状态，不产事件。
                return self._command_result(session_id)
            event = ClearingEvent(
                seq=self._journal.next_seq(session_id),
                session_id=session_id,
                train_id=train_id,
                station_id=state.station_id,
                visit=state.visit,
                at=now,
                kind=INTRUSION_RESOLVED,
                payload={},
                request_id=request_id,
            )
            self._append(session_id, event)
            return self._command_result(session_id)

    # ---- 命令：收口 ---------------------------------------------------

    def finalize(
        self,
        train_id: str,
        *,
        station_id: str = "",
        visit: int | None = None,
        confirmed_by: str = "dispatcher",
        request_id: str,
        expected_digest: str = "",
        at: float | None = None,
    ) -> dict[str, Any]:
        """统一收口：全部门区当前、未过期、无挂起时，冻结一份发车结论。

        收口是显式动作而不是"确认完自动放行"：最后一块确认和发车之间必须有人
        整体核对一遍。``expected_digest`` 可带客户端看到的事实摘要，不一致说明
        期间事实发生过变化，要求客户端重新核对页面再收口。
        """

        request_id = self._require_request_id(request_id)
        with self._lock:
            now = self._clock.now() if at is None else at
            session_id = self._resolve(train_id, station_id, visit)
            hit = self._replay_or_none(request_id, session_id)
            if hit is not None:
                return hit
            session = self._get(session_id)
            self._apply_timeout(session, now)
            state = session.state
            if state.stage == STAGE_DISPATCHED:
                raise InterlockError("列车已发车，无需再收口", component="clearing")
            if state.suspended:
                raise InterlockError(
                    f"清客会话挂起中（{state.intrusion_reason}），解除并重新确认后才能收口",
                    component="clearing",
                )
            missing = state.pending_for_finalize(now, self._ttl_s)
            if missing:
                raise InterlockError(
                    f"尚有门区不满足收口条件: {missing}", component="clearing"
                )
            digest = state.digest()
            if expected_digest and expected_digest != digest:
                raise ClearingConflict(
                    f"收口基于过期事实（期望摘要 {expected_digest}，当前 {digest}）",
                    snapshot=self.snapshot(session_id, at=now),
                )
            event = ClearingEvent(
                seq=self._journal.next_seq(session_id),
                session_id=session_id,
                train_id=train_id,
                station_id=state.station_id,
                visit=state.visit,
                at=now,
                kind=SESSION_FINALIZED,
                payload={"digest": digest, "confirmed_by": str(confirmed_by or "dispatcher")},
                request_id=request_id,
            )
            self._append(session_id, event)
            return self._command_result(session_id)

    # ---- 命令：凭结论发车（登记结论被消费） --------------------------

    def mark_dispatched(
        self,
        train_id: str,
        *,
        station_id: str = "",
        visit: int | None = None,
        request_id: str,
        at: float | None = None,
    ) -> dict[str, Any]:
        """门控/联锁链在真正发车前调用：校验 GO 并把"已发车"落进哈希链。

        这是发车结论的**唯一消费点**：调用方必须先拿到 :meth:`verdict` 的 GO，
        再走自己的硬件动作，成功后立刻调用本方法闭环。本方法用锁保证同一份结论
        只会被发车消费一次（重复提交幂等；并发的第二笔拿到 already_dispatched）。
        """

        request_id = self._require_request_id(request_id)
        with self._lock:
            now = self._clock.now() if at is None else at
            session_id = self._resolve(train_id, station_id, visit)
            hit = self._replay_or_none(request_id, session_id)
            if hit is not None:
                return hit
            session = self._get(session_id)
            verdict = self._verdict(session, now)
            if not verdict.allowed:
                raise InterlockError(
                    f"清客结论不允许发车: {list(verdict.reasons)}", component="clearing"
                )
            event = ClearingEvent(
                seq=self._journal.next_seq(session_id),
                session_id=session_id,
                train_id=train_id,
                station_id=session.state.station_id,
                visit=session.state.visit,
                at=now,
                kind=CLEARANCE_DISPATCHED,
                payload={"digest": verdict.digest, "version": verdict.version},
                request_id=request_id,
            )
            self._append(session_id, event)
            self._active.pop(train_id, None)
            self._closed[train_id] = session_id
            return self._command_result(session_id)

    # ---- 查询（结论的唯一出口） --------------------------------------

    def _verdict(self, session: _Session, now: float) -> ClearanceVerdict:
        self._apply_timeout(session, now)
        return verdict_of(session.state, now, self._ttl_s)

    def verdict(
        self,
        train_id: str,
        *,
        station_id: str = "",
        visit: int | None = None,
        at: float | None = None,
    ) -> ClearanceVerdict:
        """返回发车结论。门控、联锁、发车编排都只读这一个方法。"""

        with self._lock:
            now = self._clock.now() if at is None else at
            session_id = self._resolve(train_id, station_id, visit)
            return self._verdict(self._get(session_id), now)

    def verdict_for_session(self, session_id: str, at: float | None = None) -> ClearanceVerdict:
        with self._lock:
            now = self._clock.now() if at is None else at
            return self._verdict(self._get(session_id), now)

    def snapshot(self, session_id: str, at: float | None = None) -> dict[str, Any]:
        """返回会话快照（含门区明细与发车结论），供控制台/断线重连对齐。"""

        with self._lock:
            now = self._clock.now() if at is None else at
            session = self._get(session_id)
            self._apply_timeout(session, now)
            state = session.state
            snap = state.snapshot(now, self._ttl_s)
            snap["verdict"] = verdict_of(state, now, self._ttl_s).as_dict()
            snap["confirm_ttl_s"] = self._ttl_s
            return snap

    def snapshot_for_train(self, train_id: str) -> dict[str, Any] | None:
        with self._lock:
            session_id = self._active.get(train_id)
            return None if session_id is None else self.snapshot(session_id)

    def active_session_id(self, train_id: str) -> str:
        with self._lock:
            return self._resolve(train_id)

    def events(self, session_id: str) -> list[dict[str, Any]]:
        """返回会话的原始事实事件（审计/巡检用），历史不可变。"""

        with self._lock:
            return [event.as_dict() for event in self._journal.events(session_id)]

    def verify(self, session_id: str) -> dict[str, Any]:
        with self._lock:
            return self._journal.verify_chain(session_id)

    def list_sessions(self) -> list[str]:
        with self._lock:
            return sorted(self._sessions)
