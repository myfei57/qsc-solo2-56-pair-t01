"""清客确认事件定义。

清客会话的一切状态变化都表达成事件：会话开始、门区确认、闯入挂起、闯入解除、
代际翻代、收口、发车。当前态永远是事件流的折叠结果，事件本身只追加、不修改、
不删除，这是"确认记录历史不可变"的落地方式。

事件类型分两类：``FACT_*`` 是已经发生的事实（确认、闯入、发车），重放多少次
结论都一样；``CMD_*`` 前缀保留给将来的指令流水复用，本模块只产事实事件。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# 会话生命周期
SESSION_STARTED = "session_started"
ZONE_CONFIRMED = "zone_confirmed"
ZONE_RESET = "zone_reset"
EPOCH_BUMPED = "epoch_bumped"
INTRUSION_REPORTED = "intrusion_reported"
INTRUSION_RESOLVED = "intrusion_resolved"
SESSION_FINALIZED = "session_finalized"
CLEARANCE_DISPATCHED = "clearance_dispatched"

# 闯入/翻代原因
REASON_INTRUSION = "intrusion"
REASON_TIMEOUT = "confirm_timeout"

FACT_EVENTS = frozenset(
    {
        SESSION_STARTED,
        ZONE_CONFIRMED,
        ZONE_RESET,
        EPOCH_BUMPED,
        INTRUSION_REPORTED,
        INTRUSION_RESOLVED,
        SESSION_FINALIZED,
        CLEARANCE_DISPATCHED,
    }
)

# 会让已确认门区失效的事件类型（收口时用来解释为什么还差门区）。
INVALIDATING = frozenset({ZONE_RESET, EPOCH_BUMPED, INTRUSION_REPORTED})


@dataclass(frozen=True)
class ClearingEvent:
    """一条清客事实事件（哈希链上的一个环）。

    ``seq`` 是本会话内从 1 开始的连续序号；``request_id`` 是客户端幂等键，
    断线重连重复提交时同一个键只会落成一条事件。
    """

    seq: int
    session_id: str
    train_id: str
    station_id: str
    visit: int
    at: float
    kind: str
    payload: dict[str, Any] = field(default_factory=dict)
    request_id: str = ""
    prev_hash: str = ""
    hash: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "session_id": self.session_id,
            "train_id": self.train_id,
            "station_id": self.station_id,
            "visit": self.visit,
            "at": self.at,
            "kind": self.kind,
            "payload": dict(self.payload),
            "request_id": self.request_id,
            "prev_hash": self.prev_hash,
            "hash": self.hash,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "ClearingEvent":
        payload = raw.get("payload", {})
        return cls(
            seq=int(raw.get("seq", 0)),
            session_id=str(raw.get("session_id", "")),
            train_id=str(raw.get("train_id", "")),
            station_id=str(raw.get("station_id", "")),
            visit=int(raw.get("visit", 0)),
            at=float(raw.get("at", 0.0)),
            kind=str(raw.get("kind", "")),
            payload=dict(payload) if isinstance(payload, dict) else {},
            request_id=str(raw.get("request_id", "")),
            prev_hash=str(raw.get("prev_hash", "")),
            hash=str(raw.get("hash", "")),
        )
