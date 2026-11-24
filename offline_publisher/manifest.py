# -*- coding: utf-8 -*-
"""公开清单：全量快照与增量 delta 两种变体，以及字节级不泄漏证明。

清单是面向“公开侧”的产物，只包含通过 :class:`VisibilityPolicy` 判定的可见
倒排项。每条清单记录写入词条、来源、页号、偏移与摘要；受限来源的对应锚点
（页号、偏移、摘要、来源 id、合成日志位置）绝不能出现在*任何*变体里。

两种变体：
- ``manifest.full.wm<水位>.bin``：该水位的完整公开清单（自包含快照）。
- ``manifest.delta.<旧水位>-<新水位>.bin``：只含撤销造成的条目变化
  （DROPPED / ADDED），头部带“基线水位 + 目标视图哈希”，应用到旧快照后
  必须与全量快照逐字节一致。

不泄漏校验同时在三个层面进行：
1. 结构化比对：解析清单记录，断言不含任何受限来源倒排项；
2. 字节级扫描：受限来源的页号/偏移/摘要/来源 id 的规范字节串不得在清单
   文件的任何字节偏移处出现（防止编码层泄漏）；
3. 跨变体：对全部全量快照与全部 delta 帧重复 1、2。
"""
from __future__ import annotations

import os
import struct

from .model import Dataset, PostingRef
from .visibility import VisibilityPolicy

MANIFEST_DIR = "manifests"
# 清单记录：来源(4) 页号(4) 偏移(4) 摘要长度(2) 词条长度(2)，后接两段 UTF-8。
_REC = struct.Struct(">IIIHH")
# 全量清单头：魔数(4) 水位(4) 记录数(4) 视图哈希(32)
_FULL_HEAD = struct.Struct(">4sII32s")
# delta 头：魔数(4) 旧水位(4) 新水位(4) 变化数(4) 目标视图哈希(32)
_DELTA_HEAD = struct.Struct(">4sIII32s")
# delta 变化帧：操作(1) + 清单记录
_OP_DROP = 1
_OP_ADD = 2
FULL_MAGIC = b"PMNF"
DELTA_MAGIC = b"PMND"

from .index import view_digest  # noqa: E402


def _encode_record(term: str, ref: PostingRef) -> bytes:
    tb, sb = term.encode("utf-8"), ref.summary.encode("utf-8")
    if len(tb) > 65535 or len(sb) > 65535:
        raise ValueError("字段过长")
    return _REC.pack(ref.source_id, ref.page, ref.offset, len(sb), len(tb)) + sb + tb


def _decode_records(blob: bytes, pos: int, count: int):
    out = []
    for _ in range(count):
        source_id, page, offset, slen, tlen = _REC.unpack_from(blob, pos)
        pos += _REC.size
        summary = blob[pos:pos + slen].decode("utf-8")
        pos += slen
        term = blob[pos:pos + tlen].decode("utf-8")
        pos += tlen
        out.append((term, PostingRef(source_id, page, offset, summary)))
    return out, pos


def write_full_manifest(dir_path: str,
                        view: dict[str, list[PostingRef]],
                        reader_wm: int) -> str:
    """写全量公开清单，返回文件路径。记录按 (词条, 来源, 页, 偏移) 排序。"""
    out_dir = os.path.join(dir_path, MANIFEST_DIR)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"manifest.full.wm{reader_wm}.bin")
    records = [(t, r) for t, refs in view.items() for r in refs]
    records.sort(key=lambda x: (x[0], x[1].source_id, x[1].page, x[1].offset))
    body = b"".join(_encode_record(t, r) for t, r in records)
    head = _FULL_HEAD.pack(FULL_MAGIC, reader_wm, len(records),
                           view_digest(view))
    with open(path, "wb") as fh:
        fh.write(head + body)
    return path


def read_full_manifest(path: str) -> tuple[int, list[tuple[str, PostingRef]], bytes]:
    with open(path, "rb") as fh:
        blob = fh.read()
    magic, wm, count, digest = _FULL_HEAD.unpack_from(blob, 0)
    if magic != FULL_MAGIC:
        raise ValueError("全量清单魔数不匹配")
    records, _ = _decode_records(blob, _FULL_HEAD.size, count)
    return wm, records, digest


def write_delta_manifest(dir_path: str,
                         old_view: dict[str, list[PostingRef]],
                         new_view: dict[str, list[PostingRef]],
                         old_wm: int, new_wm: int) -> str:
    """写增量清单：只包含两个水位间的 DROPPED/ADDED 变化帧。"""
    out_dir = os.path.join(dir_path, MANIFEST_DIR)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir,
                        f"manifest.delta.{old_wm}-{new_wm}.bin")

    def flatten(view):
        return {(t, r.source_id, r.page, r.offset): (t, r)
                for t, refs in view.items() for r in refs}

    old_flat, new_flat = flatten(old_view), flatten(new_view)
    drops = sorted(old_flat.keys() - new_flat.keys())
    adds = sorted(new_flat.keys() - old_flat.keys())
    frames = b""
    for key in drops:
        frames += bytes([_OP_DROP]) + _encode_record(*old_flat[key])
    for key in adds:
        frames += bytes([_OP_ADD]) + _encode_record(*new_flat[key])
    head = _DELTA_HEAD.pack(DELTA_MAGIC, old_wm, new_wm,
                            len(drops) + len(adds), view_digest(new_view))
    with open(path, "wb") as fh:
        fh.write(head + frames)
    return path


def read_delta_manifest(path: str):
    with open(path, "rb") as fh:
        blob = fh.read()
    magic, old_wm, new_wm, count, digest = _DELTA_HEAD.unpack_from(blob, 0)
    if magic != DELTA_MAGIC:
        raise ValueError("增量清单魔数不匹配")
    pos = _DELTA_HEAD.size
    changes = []
    for _ in range(count):
        op = blob[pos]
        pos += 1
        (term, ref), pos = _decode_records(blob, pos, 1)
        changes.append((op, term, ref))
    return old_wm, new_wm, changes, digest


def apply_delta(old_view: dict[str, list[PostingRef]],
                changes) -> dict[str, list[PostingRef]]:
    """把 delta 帧应用到旧视图，返回新视图（用于证明两变体等价）。"""
    view = {t: list(refs) for t, refs in old_view.items()}
    for op, term, ref in changes:
        if op == _OP_DROP:
            view[term] = [r for r in view.get(term, [])
                          if (r.source_id, r.page, r.offset)
                          != (ref.source_id, ref.page, ref.offset)]
            if not view.get(term):
                view.pop(term, None)
        elif op == _OP_ADD:
            view.setdefault(term, []).append(ref)
    for refs in view.values():
        refs.sort(key=lambda r: (r.source_id, r.page, r.offset))
    return dict(sorted(view.items()))


# --------------------------------------------------------------------------- #
# 不泄漏证明
# --------------------------------------------------------------------------- #
class LeakReport:
    def __init__(self):
        self.checked_files: list[str] = []
        self.violations: list[str] = []

    @property
    def clean(self) -> bool:
        return not self.violations

    def text(self) -> str:
        if self.clean:
            return (f"不泄漏校验通过：扫描 {len(self.checked_files)} 个清单"
                    f"变体，受限来源的页号/偏移/摘要/来源均未出现。")
        return "发现泄漏：\n" + "\n".join(self.violations)


def _restricted_secrets(ds: Dataset):
    """构造仅属于受限来源、不会与合法记录巧合碰撞的泄漏证据。

    返回 (字节级 needle 列表, 受限来源 id 集合, 受限 (页号,偏移) 配对集合)。
    - 字节级 needle 只取*高熵且唯一*的对象：受限摘要全文，以及该受限记录
      的完整在线编码字节（同时含来源/页号/偏移/摘要/词条）。不把“偏移 64”
      这类会被其它合法来源复用的通用整数当 needle，杜绝巧合误报。
    - 页号、偏移是否泄漏改由结构化解析后做*精确配对*判定。
    """
    from .model import Classification
    policy = VisibilityPolicy()
    needles: list[tuple[str, bytes]] = []
    restricted_ids: set[int] = set()
    restricted_locs: set[tuple[int, int]] = set()
    for vp in policy.restricted_postings(ds):
        r = vp.ref
        restricted_ids.add(r.source_id)
        restricted_locs.add((r.page, r.offset))
        needles.append((f"受限摘要 {r.summary!r}", r.summary.encode("utf-8")))
        full_rec = _encode_record(vp.term, r)
        needles.append((f"受限完整记录（来源{r.source_id} 页{r.page} "
                        f"偏移{r.offset}）", full_rec))
    # 受限来源日志自身位置同样以“完整 4 字节页号”作为结构锚点（页号唯一）。
    for src in ds.sources.values():
        if src.classification == Classification.RESTRICTED:
            needles.append((f"受限来源日志页 {src.log_page}",
                            struct.pack(">I", src.log_page)))
    # 去重，保持顺序稳定。
    seen, uniq = set(), []
    for desc, blob in needles:
        if blob not in seen:
            seen.add(blob)
            uniq.append((desc, blob))
    return uniq, restricted_ids, restricted_locs


def verify_no_leak(dir_path: str, ds: Dataset) -> LeakReport:
    """对清单目录下*所有*全量/增量变体做结构化 + 字节级泄漏校验。"""
    report = LeakReport()
    needles, restricted_ids, restricted_locs = _restricted_secrets(ds)
    mdir = os.path.join(dir_path, MANIFEST_DIR)
    for name in sorted(os.listdir(mdir)):
        path = os.path.join(mdir, name)
        report.checked_files.append(path)
        with open(path, "rb") as fh:
            blob = fh.read()

        # 1) 字节级：受限摘要 / 完整受限记录字节不得在任意偏移出现。
        for desc, needle in needles:
            idx = blob.find(needle)
            if idx >= 0:
                report.violations.append(
                    f"{name}: 受限证据（{desc}）出现在字节偏移 {idx}")

        # 2) 结构化：解析全部记录，精确核验来源 id 与 (页号,偏移) 配对。
        try:
            if name.startswith("manifest.full"):
                _wm, records, _dig = read_full_manifest(path)
            else:
                _o, _n, changes, _dig = read_delta_manifest(path)
                records = [(t, r) for _op, t, r in changes]
            for term, ref in records:
                if ref.source_id in restricted_ids:
                    report.violations.append(
                        f"{name}: 结构化记录包含受限来源 {ref.source_id}"
                        f"（词条 {term} 页 {ref.page} 偏移 {ref.offset}）")
                if (ref.page, ref.offset) in restricted_locs:
                    report.violations.append(
                        f"{name}: 出现受限来源的页号/偏移配对 "
                        f"({ref.page},{ref.offset})（词条 {term}）")
        except ValueError as exc:
            report.violations.append(f"{name}: 解析失败 {exc}")
    return report
