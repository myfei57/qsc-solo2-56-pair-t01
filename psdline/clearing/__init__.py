"""站台清客确认。

列车到站开门后，站务按门区逐块确认"本门区清客完毕"，值班员统一收口；没收口、
挂起中、或收口后事实又发生变化（有人闯回、确认过期）时，发车结论恒为 NO-GO。
门控、联锁、发车编排读取的都是 :func:`~psdline.clearing.state.verdict_of` 折叠
出的**同一个结论对象**，事实来源是哈希链只追加流水。

* 任意门区可先确认、可并发确认不同门区，最后 :meth:`~ClearingService.finalize` 收口；
* 超时策略可选 ``preserve``（保留记录、TTL 软失效，默认）或 ``reset``（整单翻代
  全部失效重来）；
* 所有写命令按 ``request_id`` 幂等，天然覆盖掉线重连与重复提交；
* 会话状态全部可从 :mod:`~psdline.clearing.journal` 的哈希链重建并校验篡改。
"""

from __future__ import annotations

from .events import (
    CLEARANCE_DISPATCHED,
    EPOCH_BUMPED,
    INTRUSION_REPORTED,
    INTRUSION_RESOLVED,
    REASON_INTRUSION,
    REASON_TIMEOUT,
    SESSION_FINALIZED,
    SESSION_STARTED,
    ZONE_CONFIRMED,
    ZONE_RESET,
    ClearingEvent,
)
from .journal import GENESIS_HASH, ChainBroken, ClearingJournal, digest_event, event_hash
from .service import (
    POLICY_PRESERVE,
    POLICY_RESET,
    TIMEOUT_POLICIES,
    ClearingConflict,
    ClearingService,
    session_id_of,
)
from .state import (
    GO,
    NO_GO,
    STAGE_DISPATCHED,
    STAGE_FINALIZED,
    STAGE_OPEN,
    STAGE_SUSPENDED,
    ClearanceVerdict,
    SessionState,
    ZoneState,
    verdict_of,
)

__all__ = [
    "CLEARANCE_DISPATCHED",
    "ChainBroken",
    "ClearingConflict",
    "ClearingEvent",
    "ClearingJournal",
    "ClearingService",
    "ClearanceVerdict",
    "EPOCH_BUMPED",
    "GENESIS_HASH",
    "GO",
    "INTRUSION_REPORTED",
    "INTRUSION_RESOLVED",
    "NO_GO",
    "POLICY_PRESERVE",
    "POLICY_RESET",
    "REASON_INTRUSION",
    "REASON_TIMEOUT",
    "SESSION_FINALIZED",
    "SESSION_STARTED",
    "STAGE_DISPATCHED",
    "STAGE_FINALIZED",
    "STAGE_OPEN",
    "STAGE_SUSPENDED",
    "TIMEOUT_POLICIES",
    "SessionState",
    "ZoneState",
    "digest_event",
    "event_hash",
    "session_id_of",
    "verdict_of",
]
