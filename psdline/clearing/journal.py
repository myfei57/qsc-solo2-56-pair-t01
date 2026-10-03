"""清客事件的只追加哈希链流水。

每个清客会话一条 ``clearing-<session_id>.jsonl`` 流水（沿用项目既有的
:class:`~psdline.store.JsonlJournal`，天然按段滚动、fsync 落盘、只追加不重写）。
每条事件带：

* ``seq``        本会话内连续序号，断号即损坏；
* ``prev_hash``  上一条事件的哈希，链头为全零；
* ``hash``       对除 ``hash`` 外全部字段（含 ``prev_hash``）做 SHA-256。

因此历史记录既不可篡改（改动任意一字节，该环及其后的哈希全部对不上），也不可
在中间插入或删除（seq 断号、前后哈希同时穿帮）。校验由 :meth:`verify_chain`
在重建状态前显式跑一遍。

写入时同时把本会话的事件缓存在内存索引里，正常运行不依赖反复读盘；重建
（进程重启）时 :meth:`load` 从 JSONL 回放。
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import threading
from typing import Iterable

from ..errors import StorageError
from ..store import JsonlJournal
from .events import ClearingEvent

GENESIS_HASH = "0" * 64


def digest_event(seq: int, fields: dict) -> str:
    """对事件除 ``hash`` 外的字段做确定性 SHA-256。

    JSON 以排序键、紧凑分隔序列化，保证任何语言／任何机器重算结果一致。
    """

    material = json.dumps(fields, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def event_hash(event: ClearingEvent) -> str:
    """计算一条事件在给定前驱下应有的哈希。"""

    fields = event.as_dict()
    fields.pop("hash")
    return digest_event(event.seq, fields)


class ChainBroken(StorageError):
    """哈希链或序号校验失败：清客历史不可信，必须拒绝继续发车。"""


class ClearingJournal:
    """按会话分文件的只追加哈希链流水。"""

    def __init__(self, root: str, segment_limit: int = 1024) -> None:
        self._root = os.path.abspath(root)
        self._segment_limit = segment_limit
        os.makedirs(self._root, exist_ok=True)
        self._lock = threading.RLock()
        self._index: dict[str, list[ClearingEvent]] = {}
        self._request_index: dict[str, tuple[str, ClearingEvent]] = {}

    @property
    def root(self) -> str:
        return self._root

    def _journal(self, session_id: str) -> JsonlJournal:
        # JsonlJournal 自带段滚动与 fsync；topic 里带会话标识，一站一文件。
        return JsonlJournal(self._root, f"clearing-{session_id}", self._segment_limit)

    # ---- 写入 ---------------------------------------------------------

    def append(self, event: ClearingEvent) -> ClearingEvent:
        """追加一条事件，落盘后再进内存索引。"""

        with self._lock:
            chain = self._index.setdefault(event.session_id, [])
            expected = len(chain) + 1
            if event.seq != expected:
                raise ChainBroken(
                    f"清客事件断号: 会话 {event.session_id} 期望 seq={expected}，"
                    f"实际 seq={event.seq}",
                    component="clearing",
                )
            # 前驱哈希由流水在追加时盖章，调用方无权指定。
            prev_hash = chain[-1].hash if chain else GENESIS_HASH
            stamped = ClearingEvent(
                seq=event.seq,
                session_id=event.session_id,
                train_id=event.train_id,
                station_id=event.station_id,
                visit=event.visit,
                at=event.at,
                kind=event.kind,
                payload=copy.deepcopy(event.payload),
                request_id=event.request_id,
                prev_hash=prev_hash,
                hash=event_hash(
                    ClearingEvent(
                        seq=event.seq,
                        session_id=event.session_id,
                        train_id=event.train_id,
                        station_id=event.station_id,
                        visit=event.visit,
                        at=event.at,
                        kind=event.kind,
                        payload=copy.deepcopy(event.payload),
                        request_id=event.request_id,
                        prev_hash=prev_hash,
                        hash="",
                    )
                ),
            )
            self._journal(event.session_id).append(stamped.as_dict())
            chain.append(stamped)
            if stamped.request_id:
                self._request_index[stamped.request_id] = (stamped.session_id, stamped)
            return stamped

    def by_request(self, request_id: str) -> tuple[str, ClearingEvent] | None:
        """按幂等键取回当初落成的事件，找不到返回 ``None``。"""

        if not request_id:
            return None
        with self._lock:
            found = self._request_index.get(request_id)
            return None if found is None else (found[0], found[1])

    # ---- 读取 ---------------------------------------------------------

    def events(self, session_id: str) -> list[ClearingEvent]:
        """返回会话的全部事件（副本，调用方改不到链）。"""

        with self._lock:
            return list(self._index.get(session_id, ()))

    def next_seq(self, session_id: str) -> int:
        """返回会话下一条事件应使用的序号。"""

        with self._lock:
            return len(self._index.get(session_id, ())) + 1

    # ---- 重建与校验 ---------------------------------------------------

    def discover(self) -> list[str]:
        """扫描存储目录，找出全部清客流水对应的会话标识。"""

        prefix, suffix = "clearing-", ".jsonl"
        found: set[str] = set()
        for name in os.listdir(self._root):
            if name.startswith(prefix) and name.endswith(suffix):
                # 文件名形如 clearing-<session>-00000.jsonl，去掉段号后缀。
                stem = name[len(prefix) : -len(suffix)]
                found.add(stem.rsplit("-", 1)[0] if stem.rsplit("-", 1)[-1].isdigit() else stem)
        return sorted(found)

    def load(self, session_id: str, verify: bool = True) -> list[ClearingEvent]:
        """从落盘流水重建一个会话的链，并按需做完整性校验。"""

        with self._lock:
            records = self._journal(session_id).read_all()
            chain: list[ClearingEvent] = []
            for raw in records:
                event = ClearingEvent.from_dict(raw)
                if event.session_id != session_id:
                    raise ChainBroken(
                        f"清客流水中混入其他会话事件: {event.session_id} != {session_id}",
                        component="clearing",
                    )
                chain.append(event)
            if verify:
                self._verify(session_id, chain)
            self._index[session_id] = chain
            for event in chain:
                if event.request_id:
                    self._request_index[event.request_id] = (session_id, event)
            return list(chain)

    def load_all(self, verify: bool = True) -> dict[str, list[ClearingEvent]]:
        """发现并重建全部会话。"""

        with self._lock:
            return {session_id: self.load(session_id, verify) for session_id in self.discover()}

    @staticmethod
    def _verify(session_id: str, chain: Iterable[ClearingEvent]) -> None:
        """校验序号连续、前驱相接、每环哈希正确。"""

        prev_hash = GENESIS_HASH
        expected = 1
        for event in chain:
            if event.seq != expected:
                raise ChainBroken(
                    f"清客历史断号: 会话 {session_id} seq={event.seq} 期望 {expected}",
                    component="clearing",
                )
            if event.prev_hash != prev_hash:
                raise ChainBroken(
                    f"清客历史前驱断裂: 会话 {session_id} seq={event.seq}",
                    component="clearing",
                )
            if event.hash != event_hash(event):
                raise ChainBroken(
                    f"清客历史哈希不匹配（记录可能被篡改）: 会话 {session_id} seq={event.seq}",
                    component="clearing",
                )
            prev_hash = event.hash
            expected += 1

    def verify_chain(self, session_id: str) -> dict:
        """对内存中的链做一次完整性校验，返回核对结果。"""

        with self._lock:
            chain = self._index.get(session_id, [])
            self._verify(session_id, chain)
            return {
                "session_id": session_id,
                "events": len(chain),
                "tail_hash": chain[-1].hash if chain else GENESIS_HASH,
                "ok": True,
            }
