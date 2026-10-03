"""清客会话状态与投影。

当前态（哪些门区已确认、会话处于什么阶段、能不能发车）完全由事件流折叠得到，
不落第二份"当前态"文档——这是"门控、联锁、审计三处读同一份结论"的结构保证：
三处拿到的都是同一个投影对象，投影的输入是同一条哈希链。

门区失效用"每门区失效代际 ``invalid_epoch``"表达：

* 确认事件记下当时全局代际 ``epoch`` 与门区最新确认；
* 局部闯入只把相关门区的 ``invalid_epoch`` 推到当前代际，别处已确认的事实保留；
* 整体超时（reset 策略）把全局 ``epoch`` 加一，所有确认同时失效。

收口时再叠加一道 TTL 检查（preserve 策略的"软失效"）：确认老于 ``confirm_ttl_s``
视为过期，需重新确认；记录保留在链上不动。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .events import (
    EPOCH_BUMPED,
    INTRUSION_REPORTED,
    INTRUSION_RESOLVED,
    REASON_TIMEOUT,
    SESSION_FINALIZED,
    SESSION_STARTED,
    CLEARANCE_DISPATCHED,
    ZONE_CONFIRMED,
    ZONE_RESET,
    ClearingEvent,
)

# 会话阶段
STAGE_OPEN = "open"            # 清客进行中（可确认）
STAGE_SUSPENDED = "suspended"  # 有人闯回站台，挂起中（可确认，不能收口）
STAGE_FINALIZED = "finalized"  # 全部门区已收口（无有效门区前 GO 长期有效）
STAGE_DISPATCHED = "dispatched"  # 已凭本次结论发车，会话闭环

# 发车结论
GO = "go"
NO_GO = "no_go"


@dataclass
class ZoneState:
    """单个门区的投影状态。"""

    zone_id: str
    order: int
    confirmed: bool = False
    confirm_at: float | None = None
    confirm_epoch: int = 0
    confirm_seq: int = 0       # 最近确认事件的全局序号（会话内 seq）
    confirmed_by: str = ""
    request_id: str = ""
    invalid_epoch: int = 0     # 该门区最近一次被失效时的全局代际
    invalidate_seq: int = 0    # 该门区最近一次被失效事件的全局序号

    def is_current(self, epoch: int) -> bool:
        """确认是否还属于当前代际、且晚于最近一次失效。

        两个条件缺一不可：

        * ``confirm_epoch == epoch`` 挡住整体超时翻代（reset）前的旧确认；
        * ``confirm_seq > invalidate_seq`` 挡住本代际内闯入后没重新确认的门区，
          同时正确放行"闯入处置完之后的重新确认"（事件序号必然更靠后）。
        """

        return (
            self.confirmed
            and self.confirm_epoch == epoch
            and self.confirm_seq > self.invalidate_seq
        )

    def is_fresh(self, now: float, ttl_s: float) -> bool:
        """确认是否未超 TTL；``ttl_s<=0`` 表示不过期。"""

        if not self.confirmed or self.confirm_at is None:
            return False
        return ttl_s <= 0 or (now - self.confirm_at) <= ttl_s

    def as_dict(self) -> dict[str, Any]:
        return {
            "zone_id": self.zone_id,
            "order": self.order,
            "confirmed": self.confirmed,
            "confirm_at": self.confirm_at,
            "confirm_epoch": self.confirm_epoch,
            "confirm_seq": self.confirm_seq,
            "confirmed_by": self.confirmed_by,
            "invalid_epoch": self.invalid_epoch,
            "invalidate_seq": self.invalidate_seq,
        }


@dataclass
class SessionState:
    """一个清客会话（一列车在一个站台的一次停靠）的投影。"""

    session_id: str
    train_id: str = ""
    station_id: str = ""
    visit: int = 0
    zones: list[str] = field(default_factory=list)
    started_at: float = 0.0
    epoch: int = 1
    stage: str = STAGE_OPEN
    suspended: bool = False
    intrusion_reason: str = ""
    intrusions: list[dict[str, Any]] = field(default_factory=list)
    pending_zones: tuple[str, ...] = ()  # 最近一次挂起涉及的门区（空=全站）
    finalized_at: float | None = None
    finalized_seq: int = 0
    finalize_digest: str = ""
    finalized_by: str = ""
    dispatched_at: float | None = None
    depart_request_id: str = ""
    timeout_policy: str = "preserve"
    last_timeout_at: float | None = None
    timeouts: int = 0
    version: int = 0
    _zone_map: dict[str, ZoneState] = field(default_factory=dict)

    # ---- 折叠 ---------------------------------------------------------

    @classmethod
    def from_events(
        cls,
        session_id: str,
        events: list[ClearingEvent],
        timeout_policy: str = "preserve",
    ) -> "SessionState":
        """从事件流确定性地重建会话状态。"""

        state: SessionState | None = None
        for event in events:
            state = apply_event(state, event)
        if state is None:
            raise ValueError(f"会话没有起始事件: {session_id}")
        state.timeout_policy = timeout_policy
        return state

    def zone(self, zone_id: str) -> ZoneState | None:
        return self._zone_map.get(zone_id)

    def zone_states(self) -> list[ZoneState]:
        return [self._zone_map[zone_id] for zone_id in self.zones]

    def known_zone(self, zone_id: str) -> bool:
        return zone_id in self._zone_map

    def zones_current(self) -> list[ZoneState]:
        return [zone for zone in self.zone_states() if zone.is_current(self.epoch)]

    # ---- 失效与收口判定 ----------------------------------------------

    def invalidate_reason(self, zone: ZoneState) -> str:
        """解释一个门区当前为什么不算数。"""

        if not zone.confirmed:
            return "unconfirmed"
        if zone.confirm_epoch != self.epoch:
            return "stale_epoch"  # 整体超时翻代后的旧确认
        if zone.confirm_seq <= zone.invalidate_seq:
            return "intrusion"  # 闯回挂起后没有重新确认（或重新确认早于最近一次闯入）
        return "unconfirmed"

    def pending_for_finalize(self, now: float, ttl_s: float) -> dict[str, str]:
        """返回收口时仍未满足的门区及原因。"""

        missing: dict[str, str] = {}
        for zone in self.zone_states():
            if not zone.is_current(self.epoch):
                missing[zone.zone_id] = self.invalidate_reason(zone)
            elif not zone.is_fresh(now, ttl_s):
                missing[zone.zone_id] = "confirm_expired"
        return missing

    def digest(self) -> str:
        """当前有效事实的确定性摘要。

        摘要覆盖：门区清单与每个门区"当前代际的最新确认（谁、何时、第几条事件）"。
        任何闯回、翻代、重新确认都会改变它；收口与发车携带同一份摘要，门控/联锁/
        审计凭它确认三方看到的是同一组事实。
        """

        import hashlib
        import json

        body = {
            "session_id": self.session_id,
            "train_id": self.train_id,
            "station_id": self.station_id,
            "visit": self.visit,
            "epoch": self.epoch,
            "zones": [
                {
                    "zone_id": zone.zone_id,
                    "order": zone.order,
                    "confirmed": zone.is_current(self.epoch),
                    "confirm_at": zone.confirm_at if zone.is_current(self.epoch) else None,
                    "confirmed_by": zone.confirmed_by if zone.is_current(self.epoch) else "",
                }
                for zone in self.zone_states()
            ],
        }
        material = json.dumps(body, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        return hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]

    def snapshot(self, now: float, ttl_s: float) -> dict[str, Any]:
        """门控/联锁/控制台共用的只读快照。"""

        zones_out = []
        for zone in self.zone_states():
            current = zone.is_current(self.epoch)
            fresh = current and zone.is_fresh(now, ttl_s)
            item = zone.as_dict()
            item["current"] = current
            item["fresh"] = fresh
            if not current:
                item["reason"] = self.invalidate_reason(zone)
            elif not fresh:
                item["reason"] = "confirm_expired"
            else:
                item["reason"] = ""
            zones_out.append(item)
        missing = self.pending_for_finalize(now, ttl_s)
        return {
            "session_id": self.session_id,
            "train_id": self.train_id,
            "station_id": self.station_id,
            "visit": self.visit,
            "stage": self.stage,
            "suspended": self.suspended,
            "intrusion_reason": self.intrusion_reason,
            "epoch": self.epoch,
            "version": self.version,
            "zones": zones_out,
            "zone_order": list(self.zones),
            "confirmed_current": sorted(z.zone_id for z in self.zones_current()),
            "missing": missing,
            "ready_to_finalize": (not self.suspended) and not missing,
            "digest": self.digest(),
            "finalized_at": self.finalized_at,
            "finalize_digest": self.finalize_digest,
            "dispatched_at": self.dispatched_at,
            "started_at": self.started_at,
            "timeout_policy": self.timeout_policy,
            "timeouts": self.timeouts,
        }


def apply_event(state: SessionState | None, event: ClearingEvent) -> SessionState:
    """把一条事件折叠进状态（纯函数语义；返回更新后的状态）。"""

    payload = event.payload
    if event.kind == SESSION_STARTED:
        zones = [str(z) for z in payload.get("zones", [])]
        state = SessionState(
            session_id=event.session_id,
            train_id=event.train_id,
            station_id=event.station_id,
            visit=event.visit,
            zones=zones,
            started_at=event.at,
            timeout_policy=str(payload.get("timeout_policy", "preserve")),
        )
        state._zone_map = {
            zone_id: ZoneState(zone_id=zone_id, order=index)
            for index, zone_id in enumerate(zones, start=1)
        }
        state.version = 1
        return state

    if state is None:
        raise ValueError(f"会话 {event.session_id} 缺少起始事件")
    state.version = event.seq

    if event.kind == ZONE_CONFIRMED:
        zone = state._zone_map.get(str(payload.get("zone_id", "")))
        if zone is not None:
            zone.confirmed = True
            zone.confirm_at = event.at
            zone.confirm_epoch = int(payload.get("epoch", state.epoch))
            zone.confirm_seq = event.seq
            zone.confirmed_by = str(payload.get("confirmed_by", ""))
            zone.request_id = event.request_id

    elif event.kind == ZONE_RESET:
        for zone_id in payload.get("zone_ids", ()):
            zone = state._zone_map.get(str(zone_id))
            if zone is not None:
                zone.invalid_epoch = state.epoch
                zone.invalidate_seq = event.seq
                zone.confirmed = False
                zone.confirm_at = None

    elif event.kind == EPOCH_BUMPED:
        state.epoch = int(payload.get("epoch", state.epoch + 1))
        # 翻代次数必须从事件派生（重启重建后一致），不能只活在运行时内存里。
        state.timeouts = int(payload.get("window", state.timeouts + 1))
        state.last_timeout_at = event.at

    elif event.kind == INTRUSION_REPORTED:
        state.suspended = True
        state.intrusion_reason = str(payload.get("reason", REASON_TIMEOUT))
        state.pending_zones = tuple(str(z) for z in payload.get("zone_ids", ()))
        state.intrusions.append(
            {
                "at": event.at,
                "reason": state.intrusion_reason,
                "zone_ids": list(state.pending_zones),
                "seq": event.seq,
            }
        )
        for zone_id in state.pending_zones or state.zones:
            zone = state._zone_map.get(zone_id)
            if zone is not None:
                # 不抹掉确认事实（历史不可变），只记录"本事件之后该确认失效"；
                # 重新确认的事件 seq 更大，投影自然重新放行。
                zone.invalid_epoch = state.epoch
                zone.invalidate_seq = event.seq
        if state.stage == STAGE_FINALIZED:
            # 收口后闯回：结论立即作废，回到挂起态，必须重新确认重新收口。
            state.stage = STAGE_SUSPENDED
            state.finalized_at = None
            state.finalized_seq = 0
            state.finalize_digest = ""

    elif event.kind == INTRUSION_RESOLVED:
        state.suspended = False
        state.intrusion_reason = ""
        state.pending_zones = ()
        if state.stage == STAGE_SUSPENDED:
            state.stage = STAGE_OPEN

    elif event.kind == SESSION_FINALIZED:
        state.stage = STAGE_FINALIZED
        state.suspended = False
        state.intrusion_reason = ""
        state.pending_zones = ()
        state.finalized_at = event.at
        state.finalized_seq = event.seq
        state.finalize_digest = str(payload.get("digest", ""))
        state.finalized_by = str(payload.get("confirmed_by", ""))

    elif event.kind == CLEARANCE_DISPATCHED:
        state.stage = STAGE_DISPATCHED
        state.dispatched_at = event.at
        state.depart_request_id = event.request_id

    return state


# ---- 发车结论（唯一出口） --------------------------------------------------


@dataclass(frozen=True)
class ClearanceVerdict:
    """门控、联锁、发车共同消费的唯一结论对象。"""

    decision: str
    session_id: str
    train_id: str
    station_id: str
    visit: int
    stage: str
    version: int
    epoch: int
    digest: str
    suspended: bool
    missing: dict[str, str]
    reasons: tuple[str, ...]
    finalized_at: float | None

    @property
    def allowed(self) -> bool:
        return self.decision == GO

    def as_dict(self) -> dict[str, Any]:
        return {
            "decision": self.decision,
            "allowed": self.allowed,
            "session_id": self.session_id,
            "train_id": self.train_id,
            "station_id": self.station_id,
            "visit": self.visit,
            "stage": self.stage,
            "version": self.version,
            "epoch": self.epoch,
            "digest": self.digest,
            "suspended": self.suspended,
            "missing": dict(self.missing),
            "reasons": list(self.reasons),
            "finalized_at": self.finalized_at,
        }


def verdict_of(state: SessionState, now: float, ttl_s: float) -> ClearanceVerdict:
    """从会话投影推导发车结论。

    只有"已收口、未挂起、收口之后事实没有再变过（摘要一致）、全部门区当前且
    未过期、且尚未发车"才是 GO。任一不满足都给出机器可读的原因码，门控和联锁
    无需、也不允许各自再算一遍。
    """

    reasons: list[str] = []
    missing = state.pending_for_finalize(now, ttl_s)

    if state.stage == STAGE_DISPATCHED:
        reasons.append("already_dispatched")
    if state.suspended:
        reasons.append("intrusion_suspended")
    if state.stage not in (STAGE_FINALIZED, STAGE_DISPATCHED):
        reasons.append("not_finalized")
    if missing:
        reasons.append("zones_outstanding")
    if state.stage == STAGE_FINALIZED and state.finalize_digest != state.digest():
        reasons.append("finalization_stale")
    if state.stage == STAGE_DISPATCHED and state.dispatched_at is None:
        reasons.append("dispatched_without_timestamp")

    decision = NO_GO if reasons else GO
    return ClearanceVerdict(
        decision=decision,
        session_id=state.session_id,
        train_id=state.train_id,
        station_id=state.station_id,
        visit=state.visit,
        stage=state.stage,
        version=state.version,
        epoch=state.epoch,
        digest=state.digest(),
        suspended=state.suspended,
        missing=missing,
        reasons=tuple(reasons),
        finalized_at=state.finalized_at,
    )
