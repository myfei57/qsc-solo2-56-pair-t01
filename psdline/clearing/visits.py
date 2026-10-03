"""列车停靠次数计数。

清客会话按 (列车, 站台, 第几次停靠) 唯一标识，停靠次数必须跨重启连续，否则重启
后再来一列车会复用旧会话标识，把两份结论并到同一条哈希链上。计数按列车各存一个
原子 JSON 文档。
"""

from __future__ import annotations

from ..store import JsonStore


class VisitRegistry:
    """按列车累计的到站次数。"""

    def __init__(self, store: JsonStore) -> None:
        self._store = store

    @staticmethod
    def _doc_id(train_id: str) -> str:
        return f"clearing/{train_id}/visit"

    def current(self, train_id: str) -> int:
        """返回列车已记录的停靠次数（未记录为 0）。"""

        raw = self._store.read(self._doc_id(train_id))
        return int(raw.get("visit", 0)) if isinstance(raw, dict) else 0

    def increment(self, train_id: str) -> int:
        """停靠次数加一，返回新值（从 1 开始）。"""

        value = self.current(train_id) + 1
        self._store.write(self._doc_id(train_id), {"train_id": train_id, "visit": value})
        return value
