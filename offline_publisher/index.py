# -*- coding: utf-8 -*-
"""倒排索引：词典桶页 + posting 链页 + 元页，增量/全量两条更新路径。

物理布局（全部建立在 :class:`~offline_publisher.paged.PagedFile` 的逻辑页上）：

- 元页（逻辑页 0）：读者水位、桶数、词条数、posting 总数，以及词典桶目录
  （每个桶一个逻辑页号，0 表示空桶）。
- 词典桶页：词条按 ``crc32(term) % N`` 散列到固定数量的桶页，每页存若干
  词典项（词条名、posting 链首页、该链内容的 CRC32 摘要）。摘要让增量更新
  *不必读取未变化词条的 posting 页*即可判定是否需要重写；而桶结构让词典
  侧的增量写入只触及*脏桶*，不必重写整本词典——这是“被访问页数/重写字节”
  能精确区分增量与全量的关键。
- posting 链：每个词条的全部可见倒排项编码后按页切分，页间以 next 指针串联。

可见集合的规范形式（canonical view）由
:meth:`offline_publisher.visibility.VisibilityPolicy.public_view` 唯一定义；
索引与公开清单都从它派生，因此恢复后读者读到的视图可与策略逐字节比对。
"""
from __future__ import annotations

import os
import struct
import zlib

from .journal import COMMITTED, Journal, PREPARED, SimulatedCrash
from .model import Dataset, PostingRef
from .paged import PAYLOAD_CAP, PagedFile, TornPageError
from .visibility import VisibilityPolicy

# 逻辑页 0 固定为元页。
META_PAGE = 0
N_BUCKETS = 256
# 元页：wm, nbuckets, nterms, npost，后接 N_BUCKETS 个桶首页逻辑页号。
_META_HEAD = struct.Struct(">IIII")
# 词典桶页头：next 逻辑页号(4) + 本页项数(2)
_BUCKET_HEAD = struct.Struct(">IH")
# 词典项：名称长度(2) + posting 链首页(4) + 摘要(4) + 名称定长 32 字节
_ENTRY = struct.Struct(">HII32s")
_ENTRY_CAP = (PAYLOAD_CAP - _BUCKET_HEAD.size) // _ENTRY.size
# posting 页头：next 逻辑页号(4) + 本页项数(2)
_POST_HEAD = struct.Struct(">IH")


def canonical_bytes(view: dict[str, list[PostingRef]]) -> bytes:
    """可见集合的规范字节序列：视图哈希与恢复比对都基于它。"""
    chunks = []
    for term in sorted(view):
        tb = term.encode("utf-8")
        chunks.append(struct.pack(">H", len(tb)))
        chunks.append(tb)
        refs = view[term]
        chunks.append(struct.pack(">I", len(refs)))
        for r in refs:
            chunks.append(r.encode())
    return b"".join(chunks)


def view_digest(view: dict[str, list[PostingRef]]) -> bytes:
    """32 字节视图哈希（SHA-256），写入 WAL 的 PREPARED 载荷。"""
    import hashlib
    return hashlib.sha256(canonical_bytes(view)).digest()


def term_digest(refs: list[PostingRef]) -> int:
    """单个词条 posting 列表的 32 位摘要，存于词典项，用于增量变更判定。"""
    return zlib.crc32(b"".join(r.encode() for r in refs)) & 0xFFFFFFFF


def bucket_of(term: str) -> int:
    return zlib.crc32(term.encode("utf-8")) % N_BUCKETS


class DictEntry:
    __slots__ = ("term", "first_post", "digest")

    def __init__(self, term: str, first_post: int, digest: int):
        self.term = term
        self.first_post = first_post
        self.digest = digest


class InvertedIndex:
    """包装页式文件，提供构建、增量撤销、全量重建、快照读取。"""

    def __init__(self, dir_path: str):
        self.dir = dir_path
        self.pager: PagedFile | None = None
        self.journal = Journal(dir_path)
        self.entries: dict[str, DictEntry] = {}
        self.reader_wm = 0
        self.npost = 0
        # 桶目录：下标=桶号，值=逻辑页号（0 表示该桶尚未分配）。
        self.buckets: list[int] = [0] * N_BUCKETS

    # ------------------------------------------------------------------ #
    # 创建 / 打开
    # ------------------------------------------------------------------ #
    @classmethod
    def create(cls, dir_path: str) -> "InvertedIndex":
        os.makedirs(dir_path, exist_ok=True)
        idx = cls(dir_path)
        idx.pager = PagedFile.create(dir_path)
        idx.pager.alloc_logical_page()  # 占位逻辑页 0 = 元页
        idx._write_meta()
        idx.pager.commit(0)
        return idx

    @classmethod
    def open(cls, dir_path: str) -> "InvertedIndex":
        idx = cls(dir_path)
        idx.pager = PagedFile.open(dir_path)
        idx._load_meta_and_dict()
        return idx

    # ------------------------------------------------------------------ #
    # 元页与桶页
    # ------------------------------------------------------------------ #
    def _write_meta(self) -> None:
        payload = _META_HEAD.pack(self.reader_wm, N_BUCKETS,
                                  len(self.entries), self.npost)
        payload += b"".join(struct.pack(">I", p) for p in self.buckets)
        self.pager.write_logical(META_PAGE, payload)

    def _load_meta_and_dict(self) -> None:
        meta = self.pager.read_logical(META_PAGE)
        self.reader_wm, nbuckets, _n, self.npost = _META_HEAD.unpack_from(
            meta, 0)
        if nbuckets != N_BUCKETS:
            raise TornPageError(f"桶数不匹配：{nbuckets} != {N_BUCKETS}")
        pos = _META_HEAD.size
        self.buckets = [struct.unpack_from(">I", meta, pos + 4 * i)[0]
                        for i in range(N_BUCKETS)]
        self.entries.clear()
        # 桶可能跨多页（next 链），逐页读出全部词典项。
        for first_page in self.buckets:
            page = first_page
            while page:
                blob = self.pager.read_logical(page)
                nxt, count = _BUCKET_HEAD.unpack_from(blob, 0)
                bpos = _BUCKET_HEAD.size
                for _ in range(count):
                    namelen, fpost, digest, namebuf = _ENTRY.unpack_from(
                        blob, bpos)
                    bpos += _ENTRY.size
                    term = namebuf[:namelen].decode("utf-8")
                    self.entries[term] = DictEntry(term, fpost, digest)
                page = nxt

    def _bucket_terms(self, bucket: int) -> list[str]:
        return sorted(t for t in self.entries if bucket_of(t) == bucket)

    def _free_bucket_chain(self, first_page: int) -> None:
        page = first_page
        while page:
            blob = self.pager.read_logical(page)
            nxt, _c = _BUCKET_HEAD.unpack_from(blob, 0)
            self.pager.free_logical(page)
            page = nxt

    def _write_bucket(self, bucket: int) -> None:
        """（重）写一个桶，项数超单页容量时自动接 next 链；空桶释放并清零。"""
        terms = self._bucket_terms(bucket)
        old_page = self.buckets[bucket]
        if not terms:
            if old_page:
                self._free_bucket_chain(old_page)
                self.buckets[bucket] = 0
            return
        if old_page:
            self._free_bucket_chain(old_page)
        first = 0
        prev = -1
        for start in range(0, len(terms), _ENTRY_CAP):
            chunk = terms[start:start + _ENTRY_CAP]
            page = self.pager.alloc_logical_page()
            if first == 0:
                first = page
            buf = bytearray(_BUCKET_HEAD.size)
            for term in chunk:
                e = self.entries[term]
                name = term.encode("utf-8")
                if len(name) > 32:
                    raise ValueError("词条名超过 32 字节，需扩展编码")
                buf += _ENTRY.pack(len(name), e.first_post, e.digest,
                                   name.ljust(32, b"\x00"))
            # 先以 next=0 落页，再回填上一页的 next 指针。
            struct.pack_into(">IH", buf, 0, 0, len(chunk))
            self.pager.write_logical(page, bytes(buf))
            if prev >= 0:
                old = bytearray(self.pager.read_logical(prev))
                struct.pack_into(">I", old, 0, page)
                self.pager.write_logical(prev, bytes(old))
            prev = page
        self.buckets[bucket] = first

    # ------------------------------------------------------------------ #
    # posting 链写入 / 读取 / 释放
    # ------------------------------------------------------------------ #
    def _write_posting_chain(self, refs: list[PostingRef]) -> int:
        encoded = [r.encode() for r in refs]
        first = 0
        prev = -1
        i = 0
        while i < len(encoded):
            page = self.pager.alloc_logical_page()
            if first == 0:
                first = page
            buf = bytearray(_POST_HEAD.size)
            count = 0
            while i < len(encoded):
                if len(buf) + len(encoded[i]) > PAYLOAD_CAP:
                    break
                buf += encoded[i]
                count += 1
                i += 1
            struct.pack_into(">H", buf, 4, count)  # next 占位为 0
            self.pager.write_logical(page, bytes(buf))
            if prev >= 0:
                old = bytearray(self.pager.read_logical(prev))
                struct.pack_into(">I", old, 0, page)
                self.pager.write_logical(prev, bytes(old))
            prev = page
        return first

    def _read_posting_chain(self, first: int) -> list[PostingRef]:
        refs: list[PostingRef] = []
        page = first
        while page:
            blob = self.pager.read_logical(page)
            nxt, count = _POST_HEAD.unpack_from(blob, 0)
            pos = _POST_HEAD.size
            for _ in range(count):
                ref, pos = PostingRef.decode(blob, pos)
                refs.append(ref)
            page = nxt
        return refs

    def _free_posting_chain(self, first: int) -> None:
        page = first
        while page:
            blob = self.pager.read_logical(page)
            nxt, _c = _POST_HEAD.unpack_from(blob, 0)
            self.pager.free_logical(page)
            page = nxt

    # ------------------------------------------------------------------ #
    # 全量构建 / 全量重建：每个逻辑页都重写
    # ------------------------------------------------------------------ #
    def build_full(self, view: dict[str, list[PostingRef]],
                   reader_wm: int) -> None:
        self.entries.clear()
        self.buckets = [0] * N_BUCKETS
        self.npost = 0
        # 1) 全部 posting 链与词典项从头写。
        for term in sorted(view):
            refs = view[term]
            first = self._write_posting_chain(refs)
            self.entries[term] = DictEntry(term, first, term_digest(refs))
            self.npost += len(refs)
        # 2) 全部桶页都写一遍（全量语义：没有“未触碰”的词典页）。
        for bucket in range(N_BUCKETS):
            self._write_bucket(bucket)
        self.reader_wm = reader_wm
        self._write_meta()
        self.pager.commit(reader_wm)

    # ------------------------------------------------------------------ #
    # 增量更新：只重写脏桶 + 变化词条的 posting 链 + 元页
    # ------------------------------------------------------------------ #
    def begin_incremental(self) -> None:
        if self.pager._staging:
            raise RuntimeError("已有未提交事务")

    def rebuild_full(self, view: dict[str, list[PostingRef]],
                     reader_wm: int) -> None:
        """全量重建：释放现有 posting 链与全部桶页，再整体重写。

        与 :meth:`build_full` 的区别在于它作用于一个已存在的索引：旧逻辑页
        全部转为孤儿页，新视图的每一个逻辑页都重新落盘——这正是基准里与
        增量路径对照的“全量文件/全量重写”。
        """
        for term in list(self.entries):
            self._free_posting_chain(self.entries[term].first_post)
        for first_page in self.buckets:
            if first_page:
                self._free_bucket_chain(first_page)
        self.entries.clear()
        self.buckets = [0] * N_BUCKETS
        self.npost = 0
        for term in sorted(view):
            refs = view[term]
            first = self._write_posting_chain(refs)
            self.entries[term] = DictEntry(term, first, term_digest(refs))
            self.npost += len(refs)
        for bucket in range(N_BUCKETS):
            self._write_bucket(bucket)
        self.reader_wm = reader_wm
        self._write_meta()
        self.pager.commit(reader_wm)

    def apply_incremental(self, view: dict[str, list[PostingRef]],
                          reader_wm: int) -> dict[str, int]:
        """把索引推进到 ``view``。返回变更分类计数。"""
        stats = {"added": 0, "removed": 0, "modified": 0, "unchanged": 0,
                 "dirty_buckets": 0}
        dirty: set[int] = set()
        new_terms = set(view)
        old_terms = set(self.entries)

        # 1) 消失的词条：整条 posting 链释放（受限来源独占词条走这里）。
        for term in sorted(old_terms - new_terms):
            self._free_posting_chain(self.entries[term].first_post)
            del self.entries[term]
            dirty.add(bucket_of(term))
            stats["removed"] += 1

        # 2) 新增或内容变化的词条：靠词典项 digest 判定，未命中不读 posting 页。
        for term in sorted(new_terms):
            refs = view[term]
            digest = term_digest(refs)
            old = self.entries.get(term)
            if old is None:
                first = self._write_posting_chain(refs)
                self.entries[term] = DictEntry(term, first, digest)
                dirty.add(bucket_of(term))
                stats["added"] += 1
            elif old.digest != digest:
                # 只有真正变化的共享词条才读取并释放旧链、写新链。
                self._free_posting_chain(old.first_post)
                old.first_post = self._write_posting_chain(refs)
                old.digest = digest
                dirty.add(bucket_of(term))
                stats["modified"] += 1
            else:
                stats["unchanged"] += 1

        # 3) 词典侧只重写脏桶。
        for bucket in sorted(dirty):
            self._write_bucket(bucket)
        stats["dirty_buckets"] = len(dirty)

        self.npost = sum(len(v) for v in view.values())
        self.reader_wm = reader_wm
        self._write_meta()
        return stats

    # ------------------------------------------------------------------ #
    # 事务型撤销：与 WAL 配合，暴露 CP1/CP2 两个崩溃钩子
    # ------------------------------------------------------------------ #
    def revoke_transaction(self, txn_id: int, view: dict[str, list[PostingRef]],
                           reader_wm: int, *, crash_at: str | None = None,
                           barrier_cp1=None,
                           barrier_cp2=None) -> dict[str, int]:
        """执行一次带两阶段日志的撤销发布。

        crash_at: None / "CP1"（承诺先于指针切换时崩溃）/
                  "CP2"（指针已切换、回执丢失时崩溃）。
        barrier_cp1 / barrier_cp2: 可选 ``threading.Barrier``，分别在
                  “撤销承诺持久化后、指针切换前”与“指针切换后、回执前”
                  与持续读取线程会合，使两种场景完全确定可复现。
        """
        self.begin_incremental()
        stats = self.apply_incremental(view, reader_wm)
        digest = view_digest(view)

        def cp2_hook():
            if barrier_cp2 is not None:
                barrier_cp2.wait()  # 会合：此刻指针已切换、回执尚未写
            if crash_at == "CP2":
                raise SimulatedCrash("CP2: 指针已切换，提交回执丢失")

        # 阶段 1：数据页/影子页与新页表根落盘（此刻指针仍旧，旧版完整可读）。
        new_root = self.pager.prepare_commit(reader_wm)
        # 阶段 2：PREPARED 落盘——撤销“承诺”durable，严格先于指针切换。
        self.journal.write_prepare(txn_id, new_root, reader_wm, digest)

        # === CP1：撤销承诺已持久化，但索引指针尚未切换 =====================
        # 放在 PREPARED 之后、complete_commit 之前，才能在崩溃后凭日志前滚。
        if barrier_cp1 is not None:
            barrier_cp1.wait()  # 与读取线程会合：确认指针仍旧、旧视图完整
        if crash_at == "CP1":
            raise SimulatedCrash("CP1: 撤销承诺已持久化，指针尚未切换")

        # 阶段 3：原子切换指针；CP2 钩子位于切换之后、回执之前。
        self.pager.complete_commit(new_root, reader_wm, cp2_hook)
        # 阶段 4：COMMITTED 回执：指针切换成功后才写入。
        self.journal.write_commit(txn_id)
        return stats

    # ------------------------------------------------------------------ #
    # 恢复：扫描 WAL，处理 CP1（前滚）与 CP2（幂等补回执）
    # ------------------------------------------------------------------ #
    def recover(self) -> list[str]:
        """重放 WAL 并修正指针，返回执行的恢复动作列表。"""
        actions: list[str] = []
        state = self.journal.txn_state()
        for txn_id in sorted(state):
            recs = state[txn_id]
            if PREPARED not in recs:
                continue  # 连承诺都未 durable：事务从未发生
            new_root, reader_wm = struct.unpack_from(">II", recs[PREPARED], 0)
            if COMMITTED in recs:
                if self.pager.root_phys != new_root:
                    self.pager.adopt_root(new_root, reader_wm)
                    actions.append(
                        f"txn{txn_id}: 指针校正到已提交根 {new_root}")
                continue
            if self.pager.root_phys == new_root:
                # CP2：指针已切换，仅回执丢失 -> 幂等补回执，不重复发布。
                self.journal.write_commit(txn_id)
                actions.append(
                    f"txn{txn_id}: CP2 指针已切换，幂等补写提交回执")
            else:
                # CP1：承诺 durable 但指针仍旧 -> 前滚切换，再补回执。
                self.pager.adopt_root(new_root, reader_wm)
                self.journal.write_commit(txn_id)
                actions.append(
                    f"txn{txn_id}: CP1 前滚指针到新根 {new_root} 并补回执")
        # 用（可能被前滚的）根重新装载内存词典。
        self._load_meta_and_dict()
        return actions

    # ------------------------------------------------------------------ #
    # 读取：把当前已提交索引物化为规范视图
    # ------------------------------------------------------------------ #
    def snapshot_view(self) -> dict[str, list[PostingRef]]:
        view: dict[str, list[PostingRef]] = {}
        for term in sorted(self.entries):
            entry = self.entries[term]
            view[term] = self._read_posting_chain(entry.first_post)
        return view

    def close(self) -> None:
        if self.pager is not None:
            self.pager.close()


# --------------------------------------------------------------------------- #
# 并发快照读取器：每次读取都重新加载指针，保证只能看到完整提交的版本。
# 指针文件要么是旧内容要么是新内容（os.replace 原子），撕裂指针会在
# PagedFile 打开时以 TornPageError 暴露，读取器据此重试，绝不返回半成品。
# --------------------------------------------------------------------------- #
class SnapshotReader:
    def __init__(self, dir_path: str, policy: VisibilityPolicy,
                 ds: Dataset):
        self.dir = dir_path
        self.policy = policy
        self.dataset = ds

    def read_once(self) -> dict[str, list[PostingRef]]:
        """读取一次当前已发布视图。遇到瞬态撕裂则重试。"""
        for attempt in range(8):
            try:
                idx = InvertedIndex.open(self.dir)
                try:
                    return idx.snapshot_view()
                finally:
                    idx.close()
            except (TornPageError, FileNotFoundError, OSError):
                import time
                time.sleep(0.001 * (attempt + 1))
        raise RuntimeError("快照读取连续失败，存储可能持续不可用")

    def expected_view(self, reader_wm: int) -> dict[str, list[PostingRef]]:
        return self.policy.public_view(self.dataset, reader_wm)
