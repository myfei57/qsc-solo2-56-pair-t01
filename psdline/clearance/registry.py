"""清客事件流水归约出的当前状态。"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

from ..errors import InterlockError, RailError, StorageError
from ..store import JsonlJournal
from .events import (
    CLEARED,
    CLOSED,
    OPEN,
    RELEASING,
    REQUEST_DEDUPED,
    SESSION_BEGAN,
    SESSION_CLEARED,
    SESSION_RESUMED,
    SESSION_SUSPENDED,
    SUSPENDED,
    ZONES_EXPIRED,
    ZONE_CONFIRMED,
    RELEASE_ABORTED,
    RELEASE_FINISHED,
    RELEASE_PREPARED,
    ClearanceEvent,
    make_event,
    verify_chain,
)

SessionKey = tuple[str, str]


def clearance_doc_id() -> str:
    return "clearance/events"


@dataclass
class ZoneState:
    """一个门区在当前清客轮次中的状态。"""

    id: str
    confirmed: bool = False
    confirmed_at: float = 0.0
    expires_at: float = 0.0
    confirmed_by: str = ""
    confirmed_request_id: str = ""
    confirmed_audit_id: str = ""
    confirmed_event_id: str = ""
    invalidated_at: float = 0.0
    invalid_reason: str = ""
    expired_at: float = 0.0
    revision: int = 0

    def is_fresh(self, at: float) -> bool:
        return self.confirmed and at <= self.expires_at

    def status(self, at: float) -> str:
        if self.confirmed:
            return "confirmed" if at <= self.expires_at else "expired"
        if self.expired_at:
            return "expired"
        return "invalidated" if self.invalidated_at else "pending"

    def as_dict(self, at: float) -> dict:
        return {
            "id": self.id,
            "status": self.status(at),
            "confirmed": self.confirmed and at <= self.expires_at,
            "confirmed_at": self.confirmed_at,
            "expires_at": self.expires_at,
            "confirmed_by": self.confirmed_by,
            "confirmed_event_id": self.confirmed_event_id,
            "invalidated_at": self.invalidated_at,
            "invalid_reason": self.invalid_reason,
            "expired_at": self.expired_at,
            "revision": self.revision,
        }


@dataclass
class SessionState:
    """一列车在一个站台的当前清客轮次。"""

    train_id: str
    station_id: str
    attempt: int
    status: str = OPEN
    zones: dict[str, ZoneState] = field(default_factory=dict)
    revision: int = 0
    started_at: float = 0.0
    cleared_at: float = 0.0
    verdict_id: str = ""
    suspended_at: float = 0.0
    suspend_reason: str = ""
    release_request_id: str = ""
    released_at: float = 0.0
    last_event_id: str = ""

    def pending_zones(self, at: float) -> list[str]:
        return [zone_id for zone_id, zone in self.zones.items() if not zone.is_fresh(at)]

    def confirmed_count(self, at: float) -> int:
        return sum(1 for zone in self.zones.values() if zone.is_fresh(at))

    def as_dict(self, at: float) -> dict:
        return {
            "train_id": self.train_id,
            "station_id": self.station_id,
            "attempt": self.attempt,
            "status": self.status,
            "revision": self.revision,
            "started_at": self.started_at,
            "cleared_at": self.cleared_at,
            "verdict_id": self.verdict_id,
            "suspended_at": self.suspended_at,
            "suspend_reason": self.suspend_reason,
            "release_request_id": self.release_request_id,
            "released_at": self.released_at,
            "last_event_id": self.last_event_id,
            "confirmed_count": self.confirmed_count(at),
            "total_zones": len(self.zones),
            "pending_zones": self.pending_zones(at),
            "zones": [zone.as_dict(at) for zone in self.zones.values()],
        }


class ClearanceRegistry:
    """线程安全的清客事件存储与状态归约。"""

    def __init__(self, journal: JsonlJournal, zone_ttl_s: float) -> None:
        if zone_ttl_s <= 0:
            raise ValueError("门区确认有效期必须为正")
        self._journal = journal
        self._ttl = float(zone_ttl_s)
        self._lock = threading.RLock()
        self._events: list[ClearanceEvent] = []
        self._sessions: dict[SessionKey, SessionState] = {}
        self._attempts: dict[str, int] = {}
        self._requests: dict[str, ClearanceEvent] = {}
        self._replay()

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    @property
    def ttl_s(self) -> float:
        return self._ttl

    def revision(self, train_id: str, station_id: str) -> int:
        with self._lock:
            session = self._sessions.get((train_id, station_id))
            return session.revision if session else 0

    def sequence(self) -> int:
        with self._lock:
            return len(self._events)

    def history(self, train_id: str = "", station_id: str = "", limit: int | None = None) -> list[dict]:
        """返回不可变事件；当前态重放不会改变这里的任何记录。"""

        with self._lock:
            items = [
                event.as_dict()
                for event in self._events
                if (not train_id or event.train_id == train_id)
                and (not station_id or event.station_id == station_id)
            ]
        return items[-limit:] if limit is not None else items

    def request_event(self, request_id: str) -> ClearanceEvent | None:
        with self._lock:
            return self._requests.get(request_id)

    def event(self, event_id: str) -> ClearanceEvent | None:
        with self._lock:
            for event in self._events:
                if event.id == event_id:
                    return event
        return None

    def begin_session(
        self,
        train_id: str,
        station_id: str,
        zones: list[str],
        at: float,
        operator: str,
        audit_id: str = "",
    ) -> ClearanceEvent:
        self._require_identifiers(train_id, station_id)
        if not zones:
            raise RailError("清客门区清单为空", component="clearance")
        if len(set(zones)) != len(zones):
            raise RailError("清客门区清单重复", component="clearance")
        attempt = self._attempts.get(train_id, 0) + 1
        return self._append(
            SESSION_BEGAN,
            train_id=train_id,
            station_id=station_id,
            attempt=attempt,
            at=at,
            request_id="",
            operator=operator,
            audit_id=audit_id,
            zones=tuple(zones),
        )

    def confirm_zone(
        self,
        train_id: str,
        station_id: str,
        zone_id: str,
        at: float,
        operator: str,
        request_id: str,
        audit_id: str = "",
    ) -> ClearanceEvent:
        session = self._require_session(train_id, station_id, at)
        self._require_operator(operator)
        self._reserve_request(request_id, train_id, station_id, session.attempt)
        if session.status != OPEN:
            raise InterlockError(f"清客已{self._status_label(session.status)}，不能确认门区", component="clearance")
        if zone_id not in session.zones:
            raise RailError(f"清客门区不存在: {zone_id}", component="clearance")
        if session.zones[zone_id].is_fresh(at):
            return self._append_dedup(
                session,
                request_id=request_id,
                at=at,
                operator=operator,
                reason="zone_already_confirmed",
                original_event_id=session.zones[zone_id].confirmed_event_id,
            )
        return self._append(
            ZONE_CONFIRMED,
            train_id=train_id,
            station_id=station_id,
            attempt=session.attempt,
            at=at,
            request_id=request_id,
            operator=operator,
            audit_id=audit_id,
            zone_id=zone_id,
            detail={"zone_ttl_s": self._ttl},
        )

    def clear_session(
        self,
        train_id: str,
        station_id: str,
        at: float,
        operator: str,
        request_id: str,
        audit_id: str = "",
        expected_revision: int | None = None,
    ) -> ClearanceEvent:
        session = self._require_session(train_id, station_id, at)
        self._require_operator(operator)
        self._reserve_request(request_id, train_id, station_id, session.attempt)
        if expected_revision is not None and expected_revision != session.revision:
            raise InterlockError(
                f"清客状态已变化: 期望修订 {expected_revision}，当前 {session.revision}",
                component="clearance",
            )
        if session.status == CLEARED:
            return self._append_dedup(
                session,
                request_id=request_id,
                at=at,
                operator=operator,
                reason="already_cleared",
                original_event_id=session.verdict_id,
            )
        if session.status != OPEN:
            raise InterlockError(f"清客已{self._status_label(session.status)}，不能收口", component="clearance")
        pending = session.pending_zones(at)
        if pending:
            raise InterlockError(f"门区尚未全部确认: {', '.join(pending)}", component="clearance")
        return self._append(
            SESSION_CLEARED,
            train_id=train_id,
            station_id=station_id,
            attempt=session.attempt,
            at=at,
            request_id=request_id,
            operator=operator,
            audit_id=audit_id,
        )

    def suspend(
        self,
        train_id: str,
        station_id: str,
        at: float,
        reason: str,
        operator: str,
        request_id: str,
        audit_id: str = "",
    ) -> ClearanceEvent:
        session = self._require_session(train_id, station_id, at)
        self._require_operator(operator)
        self._reserve_request(request_id, train_id, station_id, session.attempt)
        if session.status == SUSPENDED:
            return self._append_dedup(
                session,
                request_id=request_id,
                at=at,
                operator=operator,
                reason="already_suspended",
                original_event_id=session.last_event_id,
            )
        if session.status == CLOSED:
            raise InterlockError("清客轮次已随发车关闭，不能挂起", component="clearance")
        if not reason:
            raise RailError("挂起原因为空", component="clearance")
        return self._append(
            SESSION_SUSPENDED,
            train_id=train_id,
            station_id=station_id,
            attempt=session.attempt,
            at=at,
            request_id=request_id,
            operator=operator,
            audit_id=audit_id,
            reason=reason,
        )

    def resume(
        self,
        train_id: str,
        station_id: str,
        at: float,
        operator: str,
        request_id: str,
        audit_id: str = "",
    ) -> ClearanceEvent:
        session = self._require_session(train_id, station_id, at)
        self._require_operator(operator)
        self._reserve_request(request_id, train_id, station_id, session.attempt)
        if session.status == OPEN:
            return self._append_dedup(
                session,
                request_id=request_id,
                at=at,
                operator=operator,
                reason="already_open",
                original_event_id=session.last_event_id,
            )
        if session.status != SUSPENDED:
            raise InterlockError(f"清客状态为{self._status_label(session.status)}，不能恢复", component="clearance")
        return self._append(
            SESSION_RESUMED,
            train_id=train_id,
            station_id=station_id,
            attempt=session.attempt,
            at=at,
            request_id=request_id,
            operator=operator,
            audit_id=audit_id,
        )

    def expire_due(self, train_id: str, station_id: str, at: float) -> ClearanceEvent | None:
        """把已超时的门区一次性登记为事实；没有超时时不写事件。"""

        self._require_identifiers(train_id, station_id)
        session = self._sessions.get((train_id, station_id))
        if session is None:
            raise InterlockError("清客确认尚未开始", component="clearance")
        if session.status != OPEN:
            return None
        due = [zone_id for zone_id, zone in session.zones.items() if zone.confirmed and at > zone.expires_at]
        if not due:
            return None
        return self._append(
            ZONES_EXPIRED,
            train_id=train_id,
            station_id=station_id,
            attempt=session.attempt,
            at=at,
            request_id="",
            operator="system",
            zones=tuple(due),
            reason="confirmation_timeout",
        )

    def prepare_release(self, train_id: str, station_id: str, at: float, request_id: str) -> ClearanceEvent:
        session = self._require_session(train_id, station_id, at)
        self._reserve_request(request_id, train_id, station_id, session.attempt)
        if session.status == RELEASING:
            raise InterlockError("清客结论正在发车释放中", component="clearance")
        if session.status != CLEARED:
            raise InterlockError("清客结论未收口，不能占用发车", component="clearance")
        return self._append(
            RELEASE_PREPARED,
            train_id=train_id,
            station_id=station_id,
            attempt=session.attempt,
            at=at,
            request_id=request_id,
            operator="system",
        )

    def abort_release(self, train_id: str, station_id: str, at: float, reason: str) -> ClearanceEvent:
        session = self._require_session(train_id, station_id, at)
        if session.status != RELEASING:
            raise InterlockError("清客不在发车释放中，不能回退", component="clearance")
        return self._append(
            RELEASE_ABORTED,
            train_id=train_id,
            station_id=station_id,
            attempt=session.attempt,
            at=at,
            request_id=f"{session.release_request_id}-abort",
            operator="system",
            reason=reason,
        )

    def finish_release(self, train_id: str, station_id: str, at: float) -> ClearanceEvent:
        session = self._require_session(train_id, station_id, at)
        if session.status != RELEASING:
            raise InterlockError("清客不在发车释放中，不能关闭", component="clearance")
        return self._append(
            RELEASE_FINISHED,
            train_id=train_id,
            station_id=station_id,
            attempt=session.attempt,
            at=at,
            request_id=f"{session.release_request_id}-finish",
            operator="system",
        )

    def releasing_sessions(self) -> list[SessionState]:
        with self._lock:
            return [self._copy_session(session) for session in self._sessions.values() if session.status == RELEASING]

    def sessions(self) -> list[SessionState]:
        with self._lock:
            return [self._copy_session(session) for session in sorted(
                self._sessions.values(), key=lambda item: (item.train_id, item.station_id)
            )]

    def latest_for_train(self, train_id: str, at: float) -> SessionState | None:
        with self._lock:
            candidates = [session for session in self._sessions.values() if session.train_id == train_id]
            if not candidates:
                return None
            return self._copy_session(max(candidates, key=lambda item: item.attempt))

    def get_session(self, train_id: str, station_id: str, at: float) -> SessionState | None:
        with self._lock:
            session = self._sessions.get((train_id, station_id))
            return self._copy_session(session) if session else None

    def view(self, train_id: str, station_id: str, at: float) -> dict:
        self.expire_due(train_id, station_id, at)
        session = self.get_session(train_id, station_id, at)
        if session is None:
            return {
                "train_id": train_id,
                "station_id": station_id,
                "attempt": 0,
                "status": "missing",
                "revision": 0,
                "zones": [],
                "pending_zones": [],
                "confirmed_count": 0,
                "total_zones": 0,
                "verdict_id": "",
                "release_allowed": False,
            }
        return session.as_dict(at) | {"release_allowed": session.status == CLEARED}

    def verdict(
        self,
        train_id: str,
        station_id: str,
        at: float,
        release_request_id: str = "",
    ) -> dict:
        """发车链读取的唯一清客结论。"""

        self.expire_due(train_id, station_id, at)
        session = self.get_session(train_id, station_id, at)
        if session is None:
            return {
                "ok": False,
                "reason": "clearance_missing",
                "train_id": train_id,
                "station_id": station_id,
                "violations": ["清客确认未开始"],
            }
        allowed = session.status == CLEARED or (
            session.status == RELEASING
            and bool(release_request_id)
            and session.release_request_id == release_request_id
        )
        if not allowed:
            reason = {
                OPEN: "clearance_incomplete",
                SUSPENDED: "clearance_suspended",
                RELEASING: "clearance_releasing",
                CLOSED: "clearance_closed",
            }.get(session.status, "clearance_not_cleared")
            messages = {
                OPEN: "清客门区未全部确认",
                SUSPENDED: "清客确认已挂起",
                RELEASING: "清客结论正在释放",
                CLOSED: "清客轮次已关闭",
            }
            return {
                "ok": False,
                "reason": reason,
                "train_id": train_id,
                "station_id": station_id,
                "attempt": session.attempt,
                "revision": session.revision,
                "pending_zones": session.pending_zones(at),
                "violations": [messages.get(session.status, "清客结论未收口")],
            }
        return {
            "ok": True,
            "reason": "cleared",
            "train_id": train_id,
            "station_id": station_id,
            "attempt": session.attempt,
            "revision": session.revision,
            "verdict_id": session.verdict_id,
            "violations": [],
        }

    def _replay(self) -> None:
        records = self._journal.read_all()
        events = [ClearanceEvent.from_dict(record) for record in records if isinstance(record, dict)]
        verify_chain(events)
        self._events = events
        for event in events:
            self._apply(event, index_request=True)

    def _append(self, kind: str, **kwargs: object) -> ClearanceEvent:
        with self._lock:
            prev_hash = self._events[-1].hash if self._events else ""
            event = make_event(sequence=len(self._events) + 1, kind=kind, prev_hash=prev_hash, **kwargs)
            self._journal.append(event.as_dict())
            self._events.append(event)
            self._apply(event, index_request=True)
            return event

    def _append_dedup(
        self,
        session: SessionState,
        *,
        request_id: str,
        at: float,
        operator: str,
        reason: str,
        original_event_id: str,
    ) -> ClearanceEvent:
        self._reserve_request(request_id, session.train_id, session.station_id, session.attempt)
        return self._append(
            REQUEST_DEDUPED,
            train_id=session.train_id,
            station_id=session.station_id,
            attempt=session.attempt,
            at=at,
            request_id=request_id,
            operator=operator,
            reason=reason,
            revision=session.revision,
            detail={"original_event_id": original_event_id},
        )

    def _apply(self, event: ClearanceEvent, index_request: bool = False) -> None:
        if event.kind == SESSION_BEGAN:
            self._apply_began(event)
        else:
            session = self._sessions.get((event.train_id, event.station_id))
            if session is None or session.attempt != event.attempt:
                raise StorageError(f"清客事件找不到对应轮次: {event.id}", component="clearance")
            self._apply_session_event(session, event)
        if index_request and event.request_id:
            self._requests[event.request_id] = event

    def _apply_began(self, event: ClearanceEvent) -> None:
        session = SessionState(
            train_id=event.train_id,
            station_id=event.station_id,
            attempt=event.attempt,
            status=OPEN,
            started_at=event.at,
            last_event_id=event.id,
            zones={zone_id: ZoneState(id=zone_id) for zone_id in event.zones},
        )
        session.revision = 1
        self._sessions[(event.train_id, event.station_id)] = session
        self._attempts[event.train_id] = max(self._attempts.get(event.train_id, 0), event.attempt)

    def _apply_session_event(self, session: SessionState, event: ClearanceEvent) -> None:
        session.revision += 1
        session.last_event_id = event.id
        if event.kind == ZONE_CONFIRMED:
            zone = session.zones[event.zone_id]
            zone.confirmed = True
            zone.confirmed_at = event.at
            zone.expires_at = event.at + float(event.detail.get("zone_ttl_s", self._ttl))
            zone.confirmed_by = event.operator
            zone.confirmed_request_id = event.request_id
            zone.confirmed_audit_id = event.audit_id
            zone.confirmed_event_id = event.id
            zone.invalidated_at = 0.0
            zone.invalid_reason = ""
            zone.expired_at = 0.0
            zone.revision += 1
        elif event.kind == SESSION_CLEARED:
            session.status = CLEARED
            session.cleared_at = event.at
            session.verdict_id = event.id
        elif event.kind == SESSION_SUSPENDED:
            session.status = SUSPENDED
            session.suspended_at = event.at
            session.suspend_reason = event.reason
            for zone in session.zones.values():
                zone.confirmed = False
                zone.invalidated_at = event.at
                zone.invalid_reason = event.reason
                zone.revision += 1
        elif event.kind == SESSION_RESUMED:
            session.status = OPEN
            session.suspend_reason = ""
            for zone in session.zones.values():
                zone.confirmed = False
                zone.invalidated_at = event.at
                zone.invalid_reason = "resumed"
                zone.revision += 1
        elif event.kind == ZONES_EXPIRED:
            for zone_id in event.zones:
                zone = session.zones[zone_id]
                zone.confirmed = False
                zone.expired_at = event.at
                zone.revision += 1
        elif event.kind == RELEASE_PREPARED:
            session.status = RELEASING
            session.release_request_id = event.request_id
        elif event.kind == RELEASE_ABORTED:
            session.status = CLEARED
            session.release_request_id = ""
        elif event.kind == RELEASE_FINISHED:
            session.status = CLOSED
            session.released_at = event.at
        elif event.kind == REQUEST_DEDUPED:
            # 幂等重放只追加审计线索，不改变门区结论与客户端版本。
            pass
        else:
            raise StorageError(f"未知清客事件类型: {event.kind}", component="clearance")

    def _require_session(self, train_id: str, station_id: str, at: float) -> SessionState:
        self._require_identifiers(train_id, station_id)
        self.expire_due(train_id, station_id, at)
        session = self._sessions.get((train_id, station_id))
        if session is None:
            raise InterlockError("清客确认尚未开始", component="clearance")
        return session

    def _reserve_request(self, request_id: str, train_id: str, station_id: str, attempt: int) -> None:
        if not request_id:
            raise RailError("请求标识为空", component="clearance")
        existing = self._requests.get(request_id)
        if existing is None:
            return
        if (
            existing.train_id,
            existing.station_id,
            existing.attempt,
        ) != (train_id, station_id, attempt):
            raise RailError(f"请求标识已用于其他清客轮次: {request_id}", component="clearance")

    @staticmethod
    def _require_identifiers(train_id: str, station_id: str) -> None:
        if not train_id:
            raise RailError("列车标识为空", component="clearance")
        if not station_id:
            raise RailError("站台标识为空", component="clearance")

    @staticmethod
    def _require_operator(operator: str) -> None:
        if not operator:
            raise RailError("确认人不能为空", component="clearance")

    @staticmethod
    def _status_label(status: str) -> str:
        return {
            OPEN: "开放确认中",
            SUSPENDED: "挂起",
            CLEARED: "收口",
            RELEASING: "发车释放中",
            CLOSED: "关闭",
        }.get(status, status)

    @staticmethod
    def _copy_session(session: SessionState) -> SessionState:
        copied = SessionState(
            train_id=session.train_id,
            station_id=session.station_id,
            attempt=session.attempt,
            status=session.status,
            revision=session.revision,
            started_at=session.started_at,
            cleared_at=session.cleared_at,
            verdict_id=session.verdict_id,
            suspended_at=session.suspended_at,
            suspend_reason=session.suspend_reason,
            release_request_id=session.release_request_id,
            released_at=session.released_at,
            last_event_id=session.last_event_id,
            zones={},
        )
        for zone_id, zone in session.zones.items():
            copied.zones[zone_id] = ZoneState(**zone.__dict__)
        return copied
