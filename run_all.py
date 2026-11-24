# -*- coding: utf-8 -*-
"""一键运行：全部单元测试 -> 参考扫描器交叉核对 -> 60000 条基准。

仅使用标准库。产物默认写入 artifacts/（已在 .gitignore 中忽略）。
    python run_all.py
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile
import unittest

from offline_publisher.benchmark import _print_report, run as bench_run
from offline_publisher.publisher import Publisher
from offline_publisher.synthdata import build_small_case
from offline_publisher.visibility import VisibilityPolicy
from offline_publisher import scanner


def run_tests() -> bool:
    loader = unittest.TestLoader()
    suite = loader.discover("tests", pattern="test_*.py")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return result.wasSuccessful()


def run_scanner_demo() -> bool:
    print("\n### 参考扫描器：独立逐来源核对小用例 ###")
    tmp = tempfile.mkdtemp(prefix="offpub_scan_")
    try:
        ds = build_small_case()
        pub = Publisher(os.path.join(tmp, "pkg"), ds)
        pub.initial_publish(100)
        problems = scanner.cross_check(
            pub.sources_json, os.path.join(pub.work_dir, "manifests"), 100)
        if problems:
            print("交叉核对问题：", problems)
            return False
        rc = scanner.main([pub.sources_json, "--watermark", "100",
                           "--manifest-dir",
                           os.path.join(pub.work_dir, "manifests")])
        return rc == 0
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def run_benchmark() -> bool:
    print("\n### 60000 条倒排记录：增量 vs 全量 ###")
    out = os.path.join("artifacts", "benchmark")
    os.makedirs(out, exist_ok=True)
    report, _ = bench_run(6000, 10, work_root=out)
    _print_report(report)
    # 硬性断言：增量确实显著少写，且两条路径视图一致（run 内部已 assert）。
    assert report["saving"]["bytes_rewritten_ratio"] < 0.25, \
        "增量重写占比异常偏高"
    assert report["leak_check"]["clean"]
    return True


def main() -> int:
    ok_tests = run_tests()
    ok_scan = run_scanner_demo()
    ok_bench = False
    if ok_tests and ok_scan:
        ok_bench = run_benchmark()
    print("\n========== 汇总 ==========")
    print(f"单元/并发/崩溃恢复测试 : {'通过' if ok_tests else '失败'}")
    print(f"参考扫描器交叉核对     : {'通过' if ok_scan else '失败'}")
    print(f"60000 条基准与不泄漏   : {'通过' if ok_bench else '失败'}")
    return 0 if (ok_tests and ok_scan and ok_bench) else 1


if __name__ == "__main__":
    raise SystemExit(main())
