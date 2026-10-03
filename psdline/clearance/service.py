"""清客确认应用服务。

服务层负责把人工动作翻译成只追加事件，并把同一事件镜像到通用审计。所有门区
并发提交都在注册表的同一把锁内串行化；同一 ``request_id`` 的重试只读取已落盘
结果，不产生第二份确认。
"""

from __future__ import annotations

import uuid
from typing import Any, Callable

from ..audit import AuditLog
from ..clock import Clock
from ..door import depart as record_door_departure
from ..errors import RailError
from ..sig import SignalController
from .events import (
    REQUEST_DEDUPED,
    SESSION_CLEARED,
    SESSION_RESUMED,
    SESSION_SUSPENDED,
    ZONE_CONFIRMED,
    ClearanceEvent,
)
from .registry import ClearanceRegistry


def new_request_id() -> str:
    return f"req-{uuid.uuid4().hex}"


class ClearanceService:
    """清客确认的用例边界。"""

    def __init__(
        self,
        registry: ClearanceRegistry,
        audit: AuditLog,
        clock: Clock,
        *,
        request_factory: Callable[[], str] = new_request_id,
    ) -> None:
        self._registry = registry
        self._audit = audit
        self._clock = clock
        self._request_factory = request_factory

    @property
    def registry(self) -> ClearanceRegistry:
        return self._registry

    def begin(self, train_id: str, station_id: str, zones: list[str], operator: str = "system", audit_id: str = "") -> dict:
        """到站开门后开启一个新的清客轮次。"""

        at = self._clock.now()
        with self._registry.lock:
            event = self._registry.begin_session(train_id, station_id, zones, at, operator, audit_id)
            return self._result(event, {"ok": True, "zones": list(zones)})

    def confirm(
        self,
        train_id: str,
        station_id: str,
        zone_id: str,
        operator: str,
        request_id: str = "",
    ) -> dict:
        """确认一个门区；允许不同人员并发确认任意门区。"""

        request_id = self._request(request_id)
        at = self._clock.now()
        with self._registry.lock:
            replay = self._replay_result(request_id, train_id, station_id, ZONE_CONFIRMED, zone_id)
            if replay is not None:
                return replay
            event = self._registry.confirm_zone(
                train_id, station_id, zone_id, at, operator, request_id
            )
            self._audit_event(event, "clearance_zone_confirmed", zone_id=event.zone_id or None)
            return self._result(event, {"ok": True, **self._view(event.train_id, event.station_id)})

    def complete(
        self,
        train_id: str,
        station_id: str,
        operator: str,
        request_id: str = "",
        expected_revision: int | None = None,
    ) -> dict:
        """统一收口；所有门区确认仍有效才生成发车可读的清客结论。"""

        request_id = self._request(request_id)
        at = self._clock.now()
        with self._registry.lock:
            replay = self._replay_result(request_id, train_id, station_id, SESSION_CLEARED)
            if replay is not None:
                return replay
            event = self._registry.clear_session(
                train_id, station_id, at, operator, request_id, expected_revision=expected_revision
            )
            self._audit_event(event, "clearance_cleared")
            return self._result(event, {"ok": True, **self._view(event.train_id, event.station_id)})

    def suspend(
        self,
        train_id: str,
        station_id: str,
        reason: str,
        operator: str = "platform-intrusion",
        request_id: str = "",
    ) -> dict:
        """有人闯回站台或现场要求复检时挂起本轮清客。"""

        request_id = self._request(request_id)
        at = self._clock.now()
        with self._registry.lock:
            replay = self._replay_result(request_id, train_id, station_id, SESSION_SUSPENDED)
            if replay is not None:
                return replay
            event = self._registry.suspend(train_id, station_id, at, reason, operator, request_id)
            self._audit_event(event, "clearance_suspended", reason=reason or None)
            return self._result(event, {"ok": True, **self._view(event.train_id, event.station_id)})

    def resume(
        self,
        train_id: str,
        station_id: str,
        operator: str,
        request_id: str = "",
    ) -> dict:
        """挂起后恢复；保守策略要求门区重新确认。"""

        request_id = self._request(request_id)
        at = self._clock.now()
        with self._registry.lock:
            replay = self._replay_result(request_id, train_id, station_id, SESSION_RESUMED)
            if replay is not None:
                return replay
            event = self._registry.resume(train_id, station_id, at, operator, request_id)
            self._audit_event(event, "clearance_resumed")
            return self._result(event, {"ok": True, **self._view(event.train_id, event.station_id)})

    def prepare_release(self, train_id: str, station_id: str, request_id: str) -> dict:
        """占用已收口结论，阻止关门和升灯期间的重复发车。"""

        at = self._clock.now()
        with self._registry.lock:
            event = self._registry.prepare_release(train_id, station_id, at, request_id)
            self._audit_event(event, "clearance_release_prepared")
            return self._result(event, {"ok": True})

    def abort_release(self, train_id: str, station_id: str, reason: str) -> dict:
        """发车后续动作失败时释放占用，回到已收口状态以便重试。"""

        at = self._clock.now()
        with self._registry.lock:
            event = self._registry.abort_release(train_id, station_id, at, reason)
            self._audit_event(event, "clearance_release_aborted", reason=reason)
            return self._result(event, {"ok": True})

    def finish_release(self, train_id: str, station_id: str) -> dict:
        """发车事实落盘后关闭本轮清客。"""

        at = self._clock.now()
        with self._registry.lock:
            event = self._registry.finish_release(train_id, station_id, at)
            self._audit_event(event, "clearance_release_finished")
            return self._result(event, {"ok": True})

    def recover_pending_releases(
        self,
        signals: SignalController,
        doors: Any,
    ) -> list[dict]:
        """重启后恢复“已占用但未完成”的发车临界区。"""

        recovered: list[dict] = []
        with self._registry.lock:
            for session in self._registry.releasing_sessions():
                if signals.is_raised(session.train_id, "depart"):
                    record_door_departure(doors, session.train_id, self._clock.now())
                    outcome = self.finish_release(session.train_id, session.station_id)
                    outcome["recovery"] = "signal_raised_finished"
                else:
                    outcome = self.abort_release(
                        session.train_id, session.station_id, "service_restart_before_depart_signal"
                    )
                    outcome["recovery"] = "signal_missing_aborted"
                recovered.append(outcome)
        return recovered

    def view(self, train_id: str, station_id: str) -> dict:
        with self._registry.lock:
            before = self._registry.sequence()
            view = self._registry.view(train_id, station_id, self._clock.now())
            self._audit_new_system_events(before)
            return view

    def verdict(self, train_id: str, station_id: str, release_request_id: str = "") -> dict:
        with self._registry.lock:
            before = self._registry.sequence()
            verdict = self._registry.verdict(
                train_id, station_id, self._clock.now(), release_request_id
            )
            self._audit_new_system_events(before)
            return verdict

    def history(self, train_id: str = "", station_id: str = "", limit: int | None = None) -> list[dict]:
        with self._registry.lock:
            return self._registry.history(train_id, station_id, limit)

    def summary(self) -> dict:
        """返回清客模块的当前结论与事件计数。"""

        at = self._clock.now()
        with self._registry.lock:
            events = self._registry.history()
            sessions = self._registry.sessions()
            views = [session.as_dict(at) for session in sessions]
            status_counts: dict[str, int] = {}
            for view in views:
                status_counts[view["status"]] = status_counts.get(view["status"], 0) + 1
            return {
                "zone_ttl_s": self._registry.ttl_s,
                "event_count": len(events),
                "revision": self._registry.sequence(),
                "sessions": views,
                "status_counts": status_counts,
            }

    def _audit_event(self, event: ClearanceEvent, kind: str, **detail: object) -> str:
        if event.kind == REQUEST_DEDUPED:
            return ""
        entry = self._audit.record(
            kind,
            event.train_id,
            event.station_id,
            event.at,
            event_id=event.id,
            request_id=event.request_id or None,
            attempt=event.attempt,
            revision=event.revision,
            operator=event.operator,
            **{key: value for key, value in detail.items() if value is not None},
        )
        return entry.id

    def _audit_new_system_events(self, before: int) -> None:
        for raw_event in self._registry.history()[before:]:
            if raw_event["kind"] != "clearance_zones_expired":
                continue
            self._audit.record(
                "clearance_zones_expired",
                raw_event["train_id"],
                raw_event["station_id"],
                raw_event["at"],
                event_id=raw_event["id"],
                attempt=raw_event["attempt"],
                revision=raw_event["revision"],
                zones=",".join(raw_event["zones"]),
                reason=raw_event["reason"],
            )

    def _replay_result(
        self,
        request_id: str,
        train_id: str,
        station_id: str,
        expected_kind: str,
        zone_id: str = "",
    ) -> dict | None:
        event = self._registry.request_event(request_id)
        if event is None:
            return None
        original = event
        for _ in range(1024):
            if original.kind != REQUEST_DEDUPED:
                break
            referenced = str(original.detail.get("original_event_id", ""))
            next_event = self._registry.event(referenced)
            if next_event is None:
                break
            original = next_event
        if original.kind != expected_kind:
            raise RailError(f"请求标识已用于其他清客动作: {request_id}", component="clearance")
        if original.train_id != train_id or original.station_id != station_id:
            raise RailError(f"请求标识属于其他清客轮次: {request_id}", component="clearance")
        current = self._registry.get_session(train_id, station_id, self._clock.now())
        if current is not None and current.attempt != original.attempt:
            raise RailError(f"请求标识属于上一清客轮次: {request_id}", component="clearance")
        if zone_id and original.zone_id != zone_id:
            raise RailError(f"请求标识已用于其他门区: {request_id}", component="clearance")
        result = self._result(event, {"ok": True, "replay": True, **self._view(original.train_id, original.station_id)})
        result["idempotent"] = True
        result["original_event_id"] = original.id
        return result

    def _view(self, train_id: str, station_id: str) -> dict:
        at = self._clock.now()
        session = self._registry.get_session(train_id, station_id, at)
        if session is None:
            return {}
        view = session.as_dict(at)
        view["release_allowed"] = session.status == "cleared"
        return view

    def _result(self, event: ClearanceEvent, payload: dict) -> dict:
        original_event_id = event.id
        idempotent = False
        session = self._registry.get_session(event.train_id, event.station_id, event.at)
        revision = event.revision if event.kind == REQUEST_DEDUPED else session.revision if session else event.revision
        if event.kind == REQUEST_DEDUPED:
            idempotent = True
            original_event_id = str(event.detail.get("original_event_id", event.id))
        return {
            **payload,
            "event_id": event.id,
            "original_event_id": original_event_id,
            "request_id": event.request_id,
            "idempotent": idempotent,
            "attempt": event.attempt,
            "revision": revision,
            "at": event.at,
        }

    def _request(self, request_id: str) -> str:
        if request_id:
            return request_id
        return self._request_factory()
