# -*- coding: utf-8 -*-
"""只追加（append-only）的预写日志：两阶段提交记录与崩溃注入。

事务序列（与 :mod:`offline_publisher.paged` 配合）：

1. ``PREPARED`` —— 撤销“承诺”。载荷里记录新根物理页号、读者水位与可见集合
   摘要哈希；该记录 flush+fsync 后承诺即 durable。**屏障保证此步先于索引
   指针切换。**
2. 索引指针原子切换（``os.replace``）。
3. ``COMMITTED`` —— 提交“回执”。指针切换成功后才写入并 fsync。

两个被测试精确控制的崩溃点：
- CP1：PREPARED 已落盘、指针尚未切换 -> 恢复时前滚（用已 durable 的新根
  补做指针切换），再补回执。
- CP2：指针已切换、COMMITTED 回执丢失 -> 恢复时识别到指针已指向新代次，
  仅幂等补写回执，不重复发布。
"""
from __future__ import annotations

import os
import struct
import threading
import zlib

JOURNAL_NAME = "revoke.wal"

# 记录：魔数(4) 类型(1) 事务号(4) 载荷长度(4) CRC(4) 载荷
_REC_HEAD = struct.Struct(">4sBII I")
_MAGIC = b"WAL1"
PREPARED = 1
COMMITTED = 2


class SimulatedCrash(RuntimeError):
    """测试注入的进程死亡：抛出后不再执行任何后续写操作。"""


def _frame(rec_type: int, txn_id: int, payload: bytes) -> bytes:
    head0 = _REC_HEAD.pack(_MAGIC, rec_type, txn_id, len(payload), 0)
    crc = zlib.crc32(head0 + payload) & 0xFFFFFFFF
    return _REC_HEAD.pack(_MAGIC, rec_type, txn_id, len(payload), crc) + payload


class Journal:
    def __init__(self, dir_path: str):
        self.path = os.path.join(dir_path, JOURNAL_NAME)
        self._lock = threading.Lock()
        if not os.path.exists(self.path):
            open(self.path, "wb").close()

    def _append(self, rec_type: int, txn_id: int, payload: bytes) -> None:
        # 单句追加写 + flush + fsync：Windows 上同目录句柄的追加不会撕裂
        # 小于一次 write 的记录；CRC 让任何截断都能在 scan 时被识别。
        with self._lock, open(self.path, "ab") as fh:
            fh.write(_frame(rec_type, txn_id, payload))
            fh.flush()
            os.fsync(fh.fileno())

    # ------------------------------------------------------------------ #
    def write_prepare(self, txn_id: int, new_root_phys: int,
                      reader_wm: int, view_hash: bytes) -> None:
        payload = struct.pack(">II", new_root_phys, reader_wm) + view_hash
        self._append(PREPARED, txn_id, payload)

    def write_commit(self, txn_id: int) -> None:
        self._append(COMMITTED, txn_id, b"")

    # ------------------------------------------------------------------ #
    def scan(self) -> list[tuple[int, int, bytes]]:
        """返回 [(type, txn_id, payload), ...]，CRC 失败的尾部记录视为丢失。"""
        if not os.path.exists(self.path) or os.path.getsize(self.path) == 0:
            return []
        out: list[tuple[int, int, bytes]] = []
        with open(self.path, "rb") as fh:
            blob = fh.read()
        pos = 0
        head_size = _REC_HEAD.size
        while pos + head_size <= len(blob):
            magic, rec_type, txn_id, plen, crc = _REC_HEAD.unpack_from(
                blob, pos)
            if magic != _MAGIC:
                raise RuntimeError("WAL 魔数损坏")
            payload = blob[pos + head_size: pos + head_size + plen]
            if len(payload) != plen:
                # 尾部撕裂：崩溃瞬间的半条记录，按“未写入”处理。
                break
            head0 = _REC_HEAD.pack(_MAGIC, rec_type, txn_id, plen, 0)
            if (zlib.crc32(head0 + payload) & 0xFFFFFFFF) != crc:
                break  # 撕裂/损坏记录之后的内容一律不予承认
            out.append((rec_type, txn_id, payload))
            pos += head_size + plen
        return out

    def txn_state(self) -> dict[int, dict[int, bytes]]:
        """txn_id -> {记录类型: 载荷}。"""
        state: dict[int, dict[int, bytes]] = {}
        for rec_type, txn_id, payload in self.scan():
            state.setdefault(txn_id, {})[rec_type] = payload
        return state
