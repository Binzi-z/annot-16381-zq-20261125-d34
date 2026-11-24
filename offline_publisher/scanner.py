# -*- coding: utf-8 -*-
"""逐来源扫描的参考工具（独立实现，刻意不导入发布器包的判定逻辑）。

用途：用最朴素的“逐条来源、逐条倒排项”扫描，独立算出某个读者水位下的
公开视图与受限锚点，再与发布器产出的清单交叉核对。它只依赖标准库
``json``，并且自带一份与文档一致的判定规则，因此一旦发布器与参考结果
不一致，即可定位到实现偏差，而不是两边共用同一套可能出错的代码。

命令行::

    python -m offline_publisher.scanner sources.json --watermark 100 \\
        --manifest-dir pkg/manifests
"""
from __future__ import annotations

import argparse
import json
import os
import struct

# 与 model.Classification 的数值约定保持一致（此处独立声明，避免导入耦合）。
RESTRICTED = 2


def scan_public_view(sources_json_path: str, reader_wm: int):
    """朴素扫描：返回 (公开视图 dict, 受限倒排项列表)。

    公开视图: {term: [ {source_id,page,offset,summary}, ... ]}
    规则（与 visibility 文档逐条对应）：
      1. 密级 == 受限 -> 任何水位都不可见；
      2. freeze_wm > 读者水位 -> 尚未发布，不可见；
      3. 其余来源的每条倒排项进入公开视图，词条跨来源保留。
    """
    with open(sources_json_path, "r", encoding="utf-8") as fh:
        pkg = json.load(fh)

    view: dict[str, list[dict]] = {}
    restricted: list[dict] = []
    for src in pkg["sources"]:
        for p in src["postings"]:
            record = {"source_id": p["source_id"], "page": p["page"],
                      "offset": p["offset"], "summary": p["summary"],
                      "term": p["term"]}
            if src["classification"] == RESTRICTED:
                restricted.append(record)
                continue
            if src["freeze_wm"] > reader_wm:
                continue  # 水位规则：未来来源对当前读者不存在
            view.setdefault(p["term"], []).append(record)

    for refs in view.values():
        refs.sort(key=lambda r: (r["source_id"], r["page"], r["offset"]))
    return dict(sorted(view.items())), restricted


def cross_check(sources_json_path: str, manifest_dir: str,
                reader_wm: int) -> list[str]:
    """把参考扫描结果与磁盘上的全量/增量清单交叉核对，返回问题列表。"""
    view, restricted = scan_public_view(sources_json_path, reader_wm)
    problems: list[str] = []
    restricted_ids = {r["source_id"] for r in restricted}
    restricted_summaries = {r["summary"].encode("utf-8") for r in restricted}

    # 参考视图拍平成键集合。
    expected = {
        (t, r["source_id"], r["page"], r["offset"])
        for t, refs in view.items() for r in refs
    }

    for name in sorted(os.listdir(manifest_dir)):
        path = os.path.join(manifest_dir, name)
        with open(path, "rb") as fh:
            blob = fh.read()
        # 字节级：受限摘要绝不出现。
        for needle in restricted_summaries:
            if blob.find(needle) >= 0:
                problems.append(f"{name}: 出现受限摘要 {needle!r}")
        # 结构核对仅针对覆盖该水位的全量清单。
        if name.startswith("manifest.full"):
            magic, wm, count, _dig = struct.unpack_from(">4sII32s", blob, 0)
            if wm != reader_wm:
                continue
            pos = struct.calcsize(">4sII32s")  # 全量清单头长度（44 字节）
            got = set()
            rec_head = struct.Struct(">IIIHH")
            for _ in range(count):
                sid, page, off, slen, tlen = rec_head.unpack_from(blob, pos)
                pos += rec_head.size
                summary = blob[pos:pos + slen].decode("utf-8")
                pos += slen
                term = blob[pos:pos + tlen].decode("utf-8")
                pos += tlen
                if sid in restricted_ids:
                    problems.append(
                        f"{name}: 含受限来源 {sid} 的记录")
                got.add((term, sid, page, off))
            if got != expected:
                problems.append(
                    f"{name}: 与参考视图不一致 "
                    f"（缺 {len(expected - got)} 条，多 {len(got - expected)} 条）")
    return problems


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="逐来源扫描的独立参考核对工具")
    ap.add_argument("sources_json", help="合成来源包 sources.json")
    ap.add_argument("--watermark", type=int, required=True,
                    help="读者固定水位")
    ap.add_argument("--manifest-dir", default=None,
                    help="可选：发布器清单目录，提供则交叉核对")
    args = ap.parse_args(argv)

    view, restricted = scan_public_view(args.sources_json, args.watermark)
    n_post = sum(len(v) for v in view.values())
    print(f"[scanner] 读者水位={args.watermark}")
    print(f"[scanner] 可见词条={len(view)} 可见倒排项={n_post}")
    print(f"[scanner] 受限来源倒排项（必须被剔除）={len(restricted)}")
    shared = sorted({r["term"] for r in restricted
                     if r["term"] in view})
    gone = sorted({r["term"] for r in restricted
                   if r["term"] not in view})
    print(f"[scanner] 受限来源引用但因另有授权而保留的词条={shared}")
    print(f"[scanner] 撤销后整体消失的独占词条={gone}")

    if args.manifest_dir:
        problems = cross_check(args.sources_json, args.manifest_dir,
                               args.watermark)
        if problems:
            print("[scanner] 交叉核对发现问题：")
            for p in problems:
                print("  -", p)
            return 1
        print("[scanner] 交叉核对通过：清单与独立逐来源扫描完全一致，"
              "且无受限摘要字节泄漏。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
