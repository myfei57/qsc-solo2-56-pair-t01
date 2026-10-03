"""发车放行。

发车前必须同时满足三个条件：清客结论已收口、间隙探测已确认、关门记录已
落盘。清客结论从清客事件注册表读取，门控不缓存第二份“已清客”状态；任何一条
不满足都不放行，并返回具体原因，便于值班员判断该去复检哪一处。
"""

from __future__ import annotations

import json

from typing import TYPE_CHECKING

from ..gap import GapRegistry
from ..store import JsonStore
from .sig import SignalController

if TYPE_CHECKING:
    from ..clearance import ClearanceRegistry


def close_doc_id(train_id: str) -> str:
    return f"door/{train_id}/close"


def close_durable(store: JsonStore, train_id: str) -> bool:
    """读取关门记录并判断是否已落盘。"""

    raw = store.read(close_doc_id(train_id))
    if isinstance(raw, dict):
        return bool(raw.get("durable", False))
    if isinstance(raw, str):
        try:
            return bool(json.loads(raw).get("durable", False))
        except json.JSONDecodeError:
            return False
    return False


def depart_checked(
    signals: SignalController,
    store: JsonStore,
    gaps: GapRegistry,
    clearances: "ClearanceRegistry",
    train_id: str,
    station_id: str,
    at: float,
    release_request_id: str = "",
) -> dict:
    """核对清客结论、间隙确认与关门落盘；升灯由发车临界区完成。"""

    raw_clearance = clearances.verdict(train_id, station_id, at, release_request_id)
    if not raw_clearance["ok"]:
        return {
            "ok": False,
            "reason": raw_clearance.get("reason", "clearance_not_cleared"),
            "train_id": train_id,
            "violations": list(raw_clearance["violations"]),
        }
    if not gaps.clear(train_id):
        return {
            "ok": False,
            "reason": "gap_not_clear",
            "train_id": train_id,
            "violations": ["间隙探测未确认"],
        }
    if not close_durable(store, train_id):
        return {
            "ok": False,
            "reason": "close_not_durable",
            "train_id": train_id,
            "violations": ["关门未落盘"],
        }
    return {
        "ok": True,
        "train_id": train_id,
        "clearance_verdict_id": raw_clearance.get("verdict_id", ""),
        "aspect": "green",
        "violations": [],
    }
