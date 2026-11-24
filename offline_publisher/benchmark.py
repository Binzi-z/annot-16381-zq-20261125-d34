# -*- coding: utf-8 -*-
"""60000 条倒排记录基准：增量 vs 全量，精确记录重写字节与被访问页数。

做两次内容完全相同的撤销发布，区别只在更新路径：
- 增量路径 :meth:`InvertedIndex.apply_incremental`：只写脏桶、变化词条的
  posting 链与元页；未变化词条的 posting 页一次都不读取。
- 全量路径 :meth:`InvertedIndex.rebuild_full`：释放并整体重写所有逻辑页。

两组都从*同一撤销前快照*出发，因此最终视图必须逐字节相等；而计数器
（pages_written / bytes_rewritten / pages_read）与文件大小应清晰体现增量的
节省。所有数字来自页式文件内部的真实计数，不做估算。

用法::

    python -m offline_publisher.benchmark            # 默认 60000 条
    python -m offline_publisher.benchmark --sources 6000 --terms 10
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import tempfile
import time

from .index import InvertedIndex, view_digest
from .manifest import (verify_no_leak, write_delta_manifest,
                       write_full_manifest)
from .model import Classification
from .paged import PAGE_SIZE
from .synthdata import REVOKED_ID, build_dataset, export_sources_json
from .visibility import VisibilityPolicy


def _prime_index(index_dir: str, before_view, wm: int) -> None:
    idx = InvertedIndex.create(index_dir)
    idx.build_full(before_view, wm)
    idx.close()


def _measure_rebuild(index_dir, after_view, wm):
    """全量重建路径的真实计数。"""
    idx = InvertedIndex.open(index_dir)
    idx.pager.reset_counters()
    t0 = time.perf_counter()
    idx.rebuild_full(after_view, wm)
    elapsed = time.perf_counter() - t0
    metrics = {
        "pages_written": idx.pager.pages_written,
        "pages_read": idx.pager.pages_read,
        "bytes_rewritten": idx.pager.bytes_rewritten,
        "data_file_bytes": idx.pager.data_file_bytes(),
        "logical_pages": idx.pager.logical_page_count(),
        "orphan_pages": len(idx.pager.orphan_physical_pages()),
        "elapsed_seconds": round(elapsed, 4),
    }
    view = idx.snapshot_view()
    idx.close()
    return metrics, view


def _measure_incremental(index_dir, after_view, wm):
    """增量路径的真实计数（带 WAL 两阶段提交，无崩溃）。"""
    idx = InvertedIndex.open(index_dir)
    idx.pager.reset_counters()
    t0 = time.perf_counter()
    stats = idx.revoke_transaction(1, after_view, wm)
    elapsed = time.perf_counter() - t0
    metrics = {
        "pages_written": idx.pager.pages_written,
        "pages_read": idx.pager.pages_read,
        "bytes_rewritten": idx.pager.bytes_rewritten,
        "data_file_bytes": idx.pager.data_file_bytes(),
        "logical_pages": idx.pager.logical_page_count(),
        "orphan_pages": len(idx.pager.orphan_physical_pages()),
        "elapsed_seconds": round(elapsed, 4),
        "change_stats": stats,
    }
    view = idx.snapshot_view()
    idx.close()
    return metrics, view


def run(n_sources: int, terms_per_source: int, work_root: str | None = None):
    cleanup = work_root is None
    work_root = work_root or tempfile.mkdtemp(prefix="offpub_bench_")
    os.makedirs(work_root, exist_ok=True)

    # 1) 构造“撤销前”数据集（受限来源先按公开处理），再补记为受限。
    ds = build_dataset(n_sources, terms_per_source)
    n_postings = ds.posting_count()
    ds.sources[REVOKED_ID].classification = Classification.PUBLIC
    policy = VisibilityPolicy()
    wm = ds.watermark
    before_view = policy.public_view(ds, wm)

    ds.sources[REVOKED_ID].classification = Classification.RESTRICTED
    after_view = policy.public_view(ds, wm)
    assert view_digest(before_view) != view_digest(after_view)

    inc_dir = os.path.join(work_root, "index_incremental")
    full_dir = os.path.join(work_root, "index_full")
    _prime_index(inc_dir, before_view, wm)
    _prime_index(full_dir, before_view, wm)

    # 2) 两条路径分别执行同一次撤销。
    inc_metrics, inc_view = _measure_incremental(inc_dir, after_view, wm)
    full_metrics, full_view = _measure_rebuild(full_dir, after_view, wm)

    # 3) 两条路径的最终视图必须逐字节一致。
    assert view_digest(inc_view) == view_digest(after_view)
    assert view_digest(full_view) == view_digest(after_view)
    assert inc_view == full_view == after_view

    # 4) 公开清单（全量 + 增量）并做不泄漏校验。
    write_full_manifest(work_root, before_view, wm)
    write_full_manifest(work_root, after_view, wm)
    write_delta_manifest(work_root, before_view, after_view, wm, wm + 1)
    export_sources_json(ds, os.path.join(work_root, "sources.json"))
    leak = verify_no_leak(work_root, ds)
    assert leak.clean, leak.text()

    report = {
        "n_sources": n_sources,
        "terms_per_source": terms_per_source,
        "n_postings": n_postings,
        "n_dictionary_terms": len(ds.dictionary),
        "page_size": PAGE_SIZE,
        "incremental": inc_metrics,
        "full_rebuild": full_metrics,
        "saving": {
            "bytes_rewritten_ratio": round(
                inc_metrics["bytes_rewritten"] /
                full_metrics["bytes_rewritten"], 4),
            "pages_written_ratio": round(
                inc_metrics["pages_written"] /
                full_metrics["pages_written"], 4),
            "pages_read_ratio": round(
                inc_metrics["pages_read"] /
                max(full_metrics["pages_read"], 1), 4),
            "bytes_saved": full_metrics["bytes_rewritten"]
                           - inc_metrics["bytes_rewritten"],
        },
        "leak_check": {"clean": leak.clean,
                       "manifests_scanned": len(leak.checked_files)},
    }
    if cleanup:
        shutil.rmtree(work_root, ignore_errors=True)
    else:
        with open(os.path.join(work_root, "benchmark_report.json"), "w",
                  encoding="utf-8") as fh:
            json.dump(report, fh, ensure_ascii=False, indent=2)
    return report, work_root


def _print_report(report: dict) -> None:
    inc, full = report["incremental"], report["full_rebuild"]
    sav = report["saving"]
    line = "=" * 72
    print(line)
    print(f"基准：{report['n_postings']} 条倒排记录，"
          f"{report['n_dictionary_terms']} 个词条，页大小 {report['page_size']}B")
    print(line)
    header = f"{'指标':<22}{'增量更新':>16}{'全量重建':>16}{'增量/全量':>14}"
    print(header)
    print("-" * 72)
    rows = [
        ("实际重写字节", "bytes_rewritten", True),
        ("落盘页数", "pages_written", True),
        ("被访问(读)页数", "pages_read", True),
        ("数据文件字节", "data_file_bytes", True),
        ("存活逻辑页", "logical_pages", True),
        ("孤儿(旧)页", "orphan_pages", True),
        ("耗时(秒)", "elapsed_seconds", False),
    ]
    for label, key, ratio in rows:
        iv, fv = inc[key], full[key]
        r = f"{iv / fv:8.4f}" if fv else "n/a"
        print(f"{label:<22}{iv:>16}{fv:>16}{r:>14}")
    print("-" * 72)
    print(f"增量节省重写字节：{sav['bytes_saved']} B"
          f"（仅为全量的 {sav['bytes_rewritten_ratio'] * 100:.2f}%）")
    if "change_stats" in inc:
        cs = inc["change_stats"]
        print(f"变更分类：移除 {cs['removed']}，修改 {cs['modified']}，"
              f"新增 {cs['added']}，未触碰 {cs['unchanged']}，"
              f"脏桶 {cs['dirty_buckets']}/{256}")
    print(f"不泄漏校验：{'通过' if report['leak_check']['clean'] else '失败'}"
          f"（扫描 {report['leak_check']['manifests_scanned']} 个清单变体）")
    print(line)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="60000 条倒排记录增量/全量基准")
    ap.add_argument("--sources", type=int, default=6000)
    ap.add_argument("--terms", type=int, default=10)
    ap.add_argument("--keep", action="store_true",
                    help="保留工作目录（默认临时目录跑完即删）")
    ap.add_argument("--out", default=None, help="保留时的输出目录")
    args = ap.parse_args(argv)
    work = args.out if args.keep else None
    report, work_root = run(args.sources, args.terms, work)
    _print_report(report)
    if args.keep:
        print(f"产物与 benchmark_report.json 位于：{work_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
