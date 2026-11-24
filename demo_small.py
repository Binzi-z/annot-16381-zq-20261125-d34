# -*- coding: utf-8 -*-
"""小用例端到端演示：双崩溃点恢复 + 参考扫描 + 不泄漏证明。

    python demo_small.py
产物写入 artifacts/small/（已在 .gitignore 忽略）。
"""
from __future__ import annotations

import os
import shutil

from offline_publisher.model import Classification
from offline_publisher.synthdata import build_small_case
from offline_publisher.visibility import VisibilityPolicy
from offline_publisher.index import InvertedIndex, view_digest
from offline_publisher.journal import SimulatedCrash
from offline_publisher.manifest import verify_no_leak
from offline_publisher import scanner


OUT = os.path.join("artifacts", "small")


def _crash_then_recover(crash_at: str) -> None:
    work = os.path.join(OUT, crash_at)
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work, exist_ok=True)
    ds = build_small_case()
    pol = VisibilityPolicy()
    # 撤销前来源 2 公开。
    ds.sources[2].classification = Classification.PUBLIC
    before = pol.public_view(ds, 100)
    idx_dir = os.path.join(work, "index")
    idx = InvertedIndex.create(idx_dir)
    idx.build_full(before, 100)
    idx.close()

    # 补记受限并在指定崩溃点注入崩溃。
    ds.sources[2].classification = Classification.RESTRICTED
    after = pol.public_view(ds, 100)
    idx = InvertedIndex.open(idx_dir)
    try:
        idx.revoke_transaction(1, after, 100, crash_at=crash_at)
        crashed = False
    except SimulatedCrash:
        crashed = True
    idx.close()
    print(f"[{crash_at}] 注入崩溃={crashed}")

    # 恢复。
    idx = InvertedIndex.open(idx_dir)
    actions = idx.recover()
    recovered = idx.snapshot_view()
    idx.close()
    for a in actions:
        print("   恢复动作:", a)
    assert view_digest(recovered) == view_digest(after), "恢复后视图不等于完整可见集合"
    assert "word_secret" not in recovered, "受限独占词条泄漏"
    assert "word_shared" in recovered, "共享词条被误删"
    print(f"   恢复后只读到完整可见集合：词条 {len(recovered)}，倒排项 "
          f"{sum(len(v) for v in recovered.values())}")


def main() -> int:
    shutil.rmtree(OUT, ignore_errors=True)
    os.makedirs(OUT, exist_ok=True)

    # 1) 两个崩溃点的恢复演示。
    _crash_then_recover("CP1")
    _crash_then_recover("CP2")

    # 2) 正常发布 + 多水位清单 + 不泄漏证明 + 独立参考扫描器。
    work = os.path.join(OUT, "clean")
    ds = build_small_case()
    pol = VisibilityPolicy()
    from offline_publisher.publisher import Publisher
    pub = Publisher(work, ds)
    pub.initial_publish(50)
    pub.publish_revoke(old_wm=50, new_wm=100, txn_id=1)
    report = pub.verify()
    print("[clean]", report.text())
    assert report.clean

    problems = scanner.cross_check(
        pub.sources_json, os.path.join(pub.work_dir, "manifests"), 100)
    assert not problems, problems
    print("[clean] 独立逐来源扫描交叉核对通过。")
    print(f"[clean] 产物目录：{work}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
