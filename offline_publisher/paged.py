# -*- coding: utf-8 -*-
"""固定页大小的页式文件：影子页（copy-on-write）、CRC 校验与精确 I/O 计数。

设计目标是让“增量更新只重写真正变化的页、全量更新重写所有页”这一区别
可以被*精确度量*：类内部维护被访问页数、实际落盘页数与重写字节数，
基准测试直接读取这些计数器，不做任何估算。

崩溃安全约定：
- 写一个物理页 = 一次 ``seek+write`` 整页（不足补零），页头带 CRC32；
  部分写入（撕裂页）会在下次打开时因 CRC 不匹配被检出。
- 数据页（含新影子页）全部落盘并 flush 后，根指针才通过独立的指针文件
  原子切换（``os.replace``）。因此指针永远只指向一个完整提交的版本。
- 页表（逻辑页号 -> 物理页号）本身作为普通页存储，其物理位置记录在
  指针文件里；提交即“换根 + 换页表指针”。
"""
from __future__ import annotations

import os
import struct
import zlib

from .model import MAGIC_PAGE, PAGE_HEADER, PAGE_SIZE

#: 指针文件与数据文件的固定文件名。
DATA_NAME = "index.pages"
PTR_NAME = "index.ptr"

# 指针文件布局：魔数(4) + 版本(1) + 代次(4) + 物理页号(4) + 读者水位(4)
PTR_STRUCT = struct.Struct(">4sBIII")
PTR_MAGIC = b"IPTR"

PAYLOAD_CAP = PAGE_SIZE - PAGE_HEADER.size


class TornPageError(RuntimeError):
    """页 CRC 校验失败：检测到撕裂/部分写入。"""


class PagedFile:
    def __init__(self, path: str):
        self.dir = path
        self.data_path = os.path.join(path, DATA_NAME)
        self.ptr_path = os.path.join(path, PTR_NAME)
        # 已提交状态
        self.generation: int = 0
        self.root_phys: int = 0
        self.reader_wm: int = 0
        # 工作副本：逻辑页号 -> 物理页号（初始为已提交页表的拷贝）
        self.page_table: dict[int, int] = {}
        # 物理页号 -> 有效载荷（仅缓存本次事务碰过的页；读取走磁盘）
        self._staging: dict[int, bytes] = {}
        self._freed_phys: set[int] = set()
        # 精确 I/O 计数器
        self.pages_read = 0
        self.pages_written = 0
        self.bytes_rewritten = 0
        self._next_phys = 1
        self._fh = None

    # ------------------------------------------------------------------ #
    # 打开 / 创建 / 恢复
    # ------------------------------------------------------------------ #
    @classmethod
    def create(cls, path: str) -> "PagedFile":
        os.makedirs(path, exist_ok=True)
        f = cls(path)
        f._fh = open(f.data_path, "w+b")
        # 物理页 0 作为文件头（魔数 + 页大小），永远不参与逻辑分配。
        header_payload = struct.pack(">4sI", b"IPGD", PAGE_SIZE)
        f._write_physical(0, header_payload)
        f._fh.flush()
        os.fsync(f._fh.fileno())
        return f

    @classmethod
    def open(cls, path: str) -> "PagedFile":
        f = cls(path)
        f._fh = open(f.data_path, "r+b")
        f._load_pointer()
        f._verify_header()
        if f.root_phys:
            f.page_table = f._read_page_table(f.root_phys)
        f._next_phys = f._file_pages()
        return f

    def _load_pointer(self) -> None:
        with open(self.ptr_path, "rb") as fh:
            blob = fh.read()
        if len(blob) != PTR_STRUCT.size:
            raise TornPageError("指针文件长度异常，疑似撕裂写入")
        magic, version, gen, root, wm = PTR_STRUCT.unpack(blob)
        if magic != PTR_MAGIC:
            raise TornPageError("指针文件魔数不匹配")
        self.generation, self.root_phys, self.reader_wm = gen, root, wm

    def _verify_header(self) -> None:
        page = self._read_raw_physical(0)
        magic, page_no, _crc, plen = PAGE_HEADER.unpack(
            page[:PAGE_HEADER.size])
        if magic != MAGIC_PAGE or page_no != 0:
            raise TornPageError("数据文件头损坏")

    def _file_pages(self) -> int:
        self._fh.seek(0, os.SEEK_END)
        return self._fh.tell() // PAGE_SIZE

    # ------------------------------------------------------------------ #
    # 物理页读写（带 CRC）
    # ------------------------------------------------------------------ #
    def _frame_page(self, phys: int, payload: bytes) -> bytes:
        if len(payload) > PAYLOAD_CAP:
            raise ValueError("页有效载荷超限")
        head0 = PAGE_HEADER.pack(MAGIC_PAGE, phys, 0, len(payload))
        crc = zlib.crc32(head0 + payload) & 0xFFFFFFFF
        framed = PAGE_HEADER.pack(MAGIC_PAGE, phys, crc, len(payload)) + payload
        return framed.ljust(PAGE_SIZE, b"\x00")

    def _write_physical(self, phys: int, payload: bytes) -> None:
        blob = self._frame_page(phys, payload)
        self._fh.seek(phys * PAGE_SIZE)
        self._fh.write(blob)
        self.pages_written += 1
        self.bytes_rewritten += PAGE_SIZE

    def _read_raw_physical(self, phys: int) -> bytes:
        self._fh.seek(phys * PAGE_SIZE)
        blob = self._fh.read(PAGE_SIZE)
        self.pages_read += 1
        if len(blob) != PAGE_SIZE:
            raise TornPageError(f"物理页 {phys} 长度不足（撕裂写入）")
        magic, page_no, crc, plen = PAGE_HEADER.unpack(
            blob[:PAGE_HEADER.size])
        if magic != MAGIC_PAGE or page_no != phys:
            raise TornPageError(f"物理页 {phys} 头标识不匹配")
        body = blob[:PAGE_HEADER.size + plen]
        expect = PAGE_HEADER.pack(MAGIC_PAGE, phys, 0, plen) + body[
            PAGE_HEADER.size:]
        if (zlib.crc32(expect) & 0xFFFFFFFF) != crc:
            raise TornPageError(f"物理页 {phys} CRC 校验失败")
        return blob

    def _read_payload(self, phys: int) -> bytes:
        blob = self._read_raw_physical(phys)
        _m, _p, _c, plen = PAGE_HEADER.unpack(blob[:PAGE_HEADER.size])
        return blob[PAGE_HEADER.size:PAGE_HEADER.size + plen]

    # ------------------------------------------------------------------ #
    # 逻辑页：影子页分配。新页追加到文件末尾，旧物理页在提交后变为孤儿页。
    # ------------------------------------------------------------------ #
    def alloc_logical_page(self) -> int:
        logical = (max(self.page_table) + 1) if self.page_table else 0
        phys = self._next_phys
        self._next_phys += 1
        self.page_table[logical] = phys
        self._staging[phys] = b""
        return logical

    def write_logical(self, logical: int, payload: bytes) -> None:
        if logical not in self.page_table:
            raise KeyError(f"逻辑页 {logical} 尚未分配")
        old_phys = self.page_table[logical]
        # 同一事务内重复写同一逻辑页：复用刚分配的影子物理页。
        if old_phys in self._staging and old_phys not in self._freed_phys:
            self._staging[old_phys] = payload
            return
        new_phys = self._next_phys
        self._next_phys += 1
        self._freed_phys.add(old_phys)
        self.page_table[logical] = new_phys
        self._staging[new_phys] = payload

    def read_logical(self, logical: int) -> bytes:
        phys = self.page_table[logical]
        if phys in self._staging:
            self.pages_read += 1
            return self._staging[phys]
        return self._read_payload(phys)

    def logical_page_count(self) -> int:
        return len(self.page_table)

    def free_logical(self, logical: int) -> None:
        """事务内释放一个逻辑页：其物理页转为孤儿，提交后不再被引用。"""
        phys = self.page_table.pop(logical)
        self._freed_phys.add(phys)

    # ------------------------------------------------------------------ #
    # 页表本身也是页，且按 *next 链* 跨多页存储（60000 条记录约上千个
    # 逻辑页，单页装不下完整页表）。指针文件只记录页表首页（根）。
    # 每个页表页：next 物理页号(4) + 本页项数(2) + 若干 (逻辑,物理)(8)。
    # ------------------------------------------------------------------ #
    _PT_HEAD = struct.Struct(">IH")
    _PT_ENTRY = struct.Struct(">II")
    _PT_CAP = (PAYLOAD_CAP - _PT_HEAD.size) // _PT_ENTRY.size

    def _write_page_table_chain(self, table: dict[int, int]) -> int:
        """把页表写成物理页链，返回首页（根）物理页号。

        先在内存组装全部页（含 next 指针），再各写一次，避免回读与重写。
        """
        entries = sorted(table.items())
        # 计算每条链页将占用的物理页号（顺序追加）。
        n_chain = (len(entries) + self._PT_CAP - 1) // self._PT_CAP
        phys_ids = [self._next_phys + i for i in range(max(n_chain, 0))]
        self._next_phys += n_chain
        first = phys_ids[0] if phys_ids else 0
        idx = 0
        for start in range(0, len(entries), self._PT_CAP):
            chunk = entries[start:start + self._PT_CAP]
            nxt = phys_ids[idx + 1] if idx + 1 < len(phys_ids) else 0
            buf = bytearray(self._PT_HEAD.pack(nxt, len(chunk)))
            for lp, pp in chunk:
                buf += self._PT_ENTRY.pack(lp, pp)
            self._write_physical(phys_ids[idx], bytes(buf))
            idx += 1
        return first

    def _read_page_table(self, root_phys: int) -> dict[int, int]:
        out: dict[int, int] = {}
        phys = root_phys
        while phys:
            payload = self._read_payload(phys)
            nxt, count = self._PT_HEAD.unpack_from(payload, 0)
            pos = self._PT_HEAD.size
            for _ in range(count):
                lp, pp = self._PT_ENTRY.unpack_from(payload, pos)
                pos += self._PT_ENTRY.size
                out[lp] = pp
            phys = nxt
        return out

    # ------------------------------------------------------------------ #
    # 两阶段提交：先持久化数据并取得新根（prepare），再原子切换指针
    # （complete）。撤销语义下的崩溃点 CP1/CP2 由上层 index 在 PREPARED
    # 日志写入前后权威控制，本类只负责“数据 durable”与“指针原子切换”。
    # ------------------------------------------------------------------ #
    def prepare_commit(self, new_reader_wm: int) -> int:
        """落盘全部影子页与新页表链，返回页表根（首页）物理页号（不切指针）。"""
        for phys, payload in self._staging.items():
            self._write_physical(phys, payload)
        new_root = self._write_page_table_chain(self.page_table)
        self._fh.flush()
        os.fsync(self._fh.fileno())
        return new_root

    def complete_commit(self, new_root: int, new_reader_wm: int,
                        crash_hook=None) -> None:
        """原子切换指针并提交内存状态。crash_hook 即 CP2（回执丢失）注入点。"""
        self.generation += 1
        self._atomic_swap_pointer(new_root, new_reader_wm)

        # === 崩溃点 CP2：指针已切换，但日志回执将丢失 =====================
        if crash_hook is not None:
            crash_hook()

        self.root_phys = new_root
        self.reader_wm = new_reader_wm
        self._staging.clear()
        self._freed_phys.clear()
        # 旧影子页不就地回收：文件保持只追加，空间回收由显式离线 compact
        # 完成，从而让 pages_written / bytes_rewritten 只反映真实事务写入。

    def commit(self, new_reader_wm: int) -> None:
        """无崩溃注入时的便捷封装。"""
        new_root = self.prepare_commit(new_reader_wm)
        self.complete_commit(new_root, new_reader_wm)

    def adopt_root(self, new_root: int, reader_wm: int) -> None:
        """恢复路径：承认一个已 durable 的新根（CP1 前滚）。

        从新根读出页表、切换指针；对已切换的 CP2 场景则为幂等空操作语义。
        """
        self.page_table = self._read_page_table(new_root)
        self._next_phys = self._file_pages()
        self.generation += 1
        self._atomic_swap_pointer(new_root, reader_wm)
        self.root_phys = new_root
        self.reader_wm = reader_wm
        self._staging.clear()
        self._freed_phys.clear()

    def _atomic_swap_pointer(self, root_phys: int, reader_wm: int) -> None:
        tmp = self.ptr_path + ".tmp"
        blob = PTR_STRUCT.pack(PTR_MAGIC, 1, self.generation, root_phys,
                               reader_wm)
        with open(tmp, "wb") as fh:
            fh.write(blob)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.ptr_path)

    def _page_table_chain_phys(self, root_phys: int) -> set[int]:
        """顺着 next 指针收集整条页表链占用的物理页号。"""
        chain: set[int] = set()
        phys = root_phys
        while phys:
            chain.add(phys)
            payload = self._read_payload(phys)
            nxt, _count = self._PT_HEAD.unpack_from(payload, 0)
            phys = nxt
        return chain

    def orphan_physical_pages(self) -> list[int]:
        """已不被任何逻辑页或页表链引用、但仍占用文件空间的旧影子页。

        增量事务保留这些页（文件只追加），以便指针切换前旧版本依旧完整
        可读；空间回收交给显式、离线的 :meth:`compact`，绝不混入提交
        关键路径，保证“重写字节”计数只反映真实事务写入。
        """
        live = {phys for phys in self.page_table.values()}
        live |= self._page_table_chain_phys(self.root_phys)
        live.add(0)
        return [p for p in range(1, self._file_pages()) if p not in live]

    def compact(self) -> int:
        """离线压缩：把存活页重排到文件前部，返回重写的页数。

        须在无事务进行时调用；压缩写入不经过事务计数器，单独呈现。
        数据页载荷内部只引用*逻辑*页号（压缩保持不变），因此仅需重映射
        页表（逻辑->物理），并据此重建一条干净的页表链。
        """
        # 1) 读出全部存活数据页，分配紧凑物理页号 1..N。
        remap: dict[int, int] = {}
        data_blobs: list[tuple[int, bytes]] = []
        new_phys = 1
        for lp in sorted(self.page_table):
            old_phys = self.page_table[lp]
            data_blobs.append((new_phys, self._read_payload(old_phys)))
            remap[old_phys] = new_phys
            new_phys += 1
        new_page_table = {lp: remap[phys]
                          for lp, phys in self.page_table.items()}

        # 2) 在内存中构建新的页表链（紧跟数据页之后）。
        entries = sorted(new_page_table.items())
        chain_blobs: list[tuple[int, bytes]] = []
        chain_phys: list[int] = []
        for start in range(0, len(entries), self._PT_CAP):
            chunk = entries[start:start + self._PT_CAP]
            chain_phys.append(new_phys)
            buf = bytearray(self._PT_HEAD.pack(0, len(chunk)))
            for lp, pp in chunk:
                buf += self._PT_ENTRY.pack(lp, pp)
            chain_blobs.append((new_phys, bytes(buf)))
            new_phys += 1
        new_root = chain_phys[0] if chain_phys else 0
        # 回填链内 next 指针。
        for i, phys in enumerate(chain_phys[:-1]):
            _p, payload = chain_blobs[i]
            buf = bytearray(payload)
            struct.pack_into(">I", buf, 0, chain_phys[i + 1])
            chain_blobs[i] = (phys, bytes(buf))

        # 3) 一次性重写数据文件。
        self._fh.close()
        with open(self.data_path, "w+b") as fh:
            fh.write(self._frame_page(0, struct.pack(">4sI", b"IPGD",
                                                     PAGE_SIZE)))
            for phys, payload in data_blobs:
                fh.write(self._frame_page(phys, payload))
            for phys, payload in chain_blobs:
                fh.write(self._frame_page(phys, payload))
            fh.flush()
            os.fsync(fh.fileno())
        self._fh = open(self.data_path, "r+b")

        self.page_table = new_page_table
        self.root_phys = new_root
        self._next_phys = new_phys
        self._freed_phys.clear()
        self._staging.clear()
        self._atomic_swap_pointer(self.root_phys, self.reader_wm)
        return len(data_blobs) + len(chain_blobs)

    # ------------------------------------------------------------------ #
    def close(self) -> None:
        if self._fh and not self._fh.closed:
            self._fh.flush()
            self._fh.close()

    def data_file_bytes(self) -> int:
        return os.path.getsize(self.data_path)

    def reset_counters(self) -> None:
        self.pages_read = 0
        self.pages_written = 0
        self.bytes_rewritten = 0
