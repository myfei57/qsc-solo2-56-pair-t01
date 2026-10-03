"""站台清客确认。

清客状态采用只追加事件流水重建：门区确认、收口、挂起、恢复和发车释放都不覆盖
历史。当前结论由 :class:`ClearanceRegistry` 从流水归约得到，门控与联锁只能读取
这份结论，不能各自保存一份“已清客”标记。
"""

from __future__ import annotations

from .events import EVENT_KINDS, ClearanceEvent
from .registry import ClearanceRegistry, SessionState, ZoneState, clearance_doc_id
from .service import ClearanceService

__all__ = [
    "EVENT_KINDS",
    "ClearanceEvent",
    "ClearanceRegistry",
    "ClearanceService",
    "SessionState",
    "ZoneState",
    "clearance_doc_id",
]
