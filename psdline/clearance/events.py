"""清客事件与不可变哈希链。"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field

from ..errors import StorageError

SESSION_BEGAN = "clearance_session_began"
ZONE_CONFIRMED = "clearance_zone_confirmed"
SESSION_CLEARED = "clearance_cleared"
SESSION_SUSPENDED = "clearance_suspended"
SESSION_RESUMED = "clearance_resumed"
ZONES_EXPIRED = "clearance_zones_expired"
RELEASE_PREPARED = "clearance_release_prepared"
RELEASE_ABORTED = "clearance_release_aborted"
RELEASE_FINISHED = "clearance_release_finished"
REQUEST_DEDUPED = "clearance_request_deduped"

EVENT_KINDS = frozenset(
    {
        SESSION_BEGAN,
        ZONE_CONFIRMED,
        SESSION_CLEARED,
        SESSION_SUSPENDED,
        SESSION_RESUMED,
        ZONES_EXPIRED,
        RELEASE_PREPARED,
        RELEASE_ABORTED,
        RELEASE_FINISHED,
        REQUEST_DEDUPED,
    }
)

MUTATING_KINDS = frozenset(
    {
        SESSION_BEGAN,
        ZONE_CONFIRMED,
        SESSION_CLEARED,
        SESSION_SUSPENDED,
        SESSION_RESUMED,
        ZONES_EXPIRED,
        RELEASE_PREPARED,
        RELEASE_ABORTED,
        RELEASE_FINISHED,
    }
)

OPEN = "open"
SUSPENDED = "suspended"
CLEARED = "cleared"
RELEASING = "releasing"
CLOSED = "closed"


@dataclass(frozen=True)
class ClearanceEvent:
    """一条不可变清客事实。"""

    sequence: int
    id: str
    kind: str
    train_id: str
    station_id: str
    attempt: int
    at: float
    request_id: str
    operator: str
    prev_hash: str
    hash: str
    audit_id: str = ""
    zones: tuple[str, ...] = ()
    zone_id: str = ""
    reason: str = ""
    revision: int = 0
    detail: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {
            "sequence": self.sequence,
            "id": self.id,
            "kind": self.kind,
            "train_id": self.train_id,
            "station_id": self.station_id,
            "attempt": self.attempt,
            "at": self.at,
            "request_id": self.request_id,
            "operator": self.operator,
            "prev_hash": self.prev_hash,
            "hash": self.hash,
            "audit_id": self.audit_id,
            "zones": list(self.zones),
            "zone_id": self.zone_id,
            "reason": self.reason,
            "revision": self.revision,
            "detail": dict(self.detail),
        }

    @classmethod
    def from_dict(cls, payload: dict) -> "ClearanceEvent":
        zones = payload.get("zones", ())
        detail = payload.get("detail", {})
        return cls(
            sequence=int(payload.get("sequence", 0)),
            id=str(payload.get("id", "")),
            kind=str(payload.get("kind", "")),
            train_id=str(payload.get("train_id", "")),
            station_id=str(payload.get("station_id", "")),
            attempt=int(payload.get("attempt", 0)),
            at=float(payload.get("at", 0.0)),
            request_id=str(payload.get("request_id", "")),
            operator=str(payload.get("operator", "")),
            prev_hash=str(payload.get("prev_hash", "")),
            hash=str(payload.get("hash", "")),
            audit_id=str(payload.get("audit_id", "")),
            zones=tuple(str(item) for item in zones) if isinstance(zones, list) else (),
            zone_id=str(payload.get("zone_id", "")),
            reason=str(payload.get("reason", "")),
            revision=int(payload.get("revision", 0)),
            detail=dict(detail) if isinstance(detail, dict) else {},
        )


def canonical_payload(event: ClearanceEvent) -> bytes:
    """生成除当前哈希外的稳定 JSON 字节串。"""

    payload = event.as_dict()
    payload["hash"] = ""
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def hash_event(event: ClearanceEvent) -> str:
    return hashlib.sha256(canonical_payload(event)).hexdigest()


def make_event(
    *,
    sequence: int,
    kind: str,
    train_id: str,
    station_id: str,
    attempt: int,
    at: float,
    request_id: str,
    operator: str,
    prev_hash: str,
    audit_id: str = "",
    zones: tuple[str, ...] = (),
    zone_id: str = "",
    reason: str = "",
    revision: int = 0,
    detail: dict | None = None,
) -> ClearanceEvent:
    """构造并校验一条事件，调用方写入后即不可修改。"""

    if kind not in EVENT_KINDS:
        raise StorageError(f"未知清客事件类型: {kind}", component="clearance")
    event = ClearanceEvent(
        sequence=sequence,
        id=f"clr-{sequence:010d}",
        kind=kind,
        train_id=train_id,
        station_id=station_id,
        attempt=attempt,
        at=at,
        request_id=request_id,
        operator=operator,
        prev_hash=prev_hash,
        hash="",
        audit_id=audit_id,
        zones=tuple(zones),
        zone_id=zone_id,
        reason=reason,
        revision=revision,
        detail=dict(detail or {}),
    )
    return ClearanceEvent(**{**event.as_dict(), "hash": hash_event(event)})


def verify_chain(events: list[ClearanceEvent]) -> None:
    """回放时校验序号与哈希链；任何篡改或缺口都拒绝继续启动。"""

    previous_hash = ""
    previous_sequence = 0
    for event in events:
        if event.sequence != previous_sequence + 1:
            raise StorageError(
                f"清客流水序号断裂: 期望 {previous_sequence + 1}，实际 {event.sequence}",
                component="clearance",
            )
        if event.prev_hash != previous_hash:
            raise StorageError(f"清客流水前序哈希不匹配: {event.id}", component="clearance")
        if not event.hash:
            raise StorageError(f"清客事件缺少哈希: {event.id}", component="clearance")
        if event.hash != hash_event(event):
            raise StorageError(f"清客流水哈希校验失败: {event.id}", component="clearance")
        previous_hash = event.hash
        previous_sequence = event.sequence
