# -*- coding: utf-8 -*-
"""发布器门面：把合成包、可见性策略、页式索引、WAL 与公开清单串成流程。"""
from __future__ import annotations

import os

from .index import InvertedIndex, SnapshotReader, view_digest
from .manifest import (apply_delta, read_delta_manifest,
                       verify_no_leak, write_delta_manifest,
                       write_full_manifest)
from .model import Dataset
from .synthdata import export_sources_json
from .visibility import VisibilityPolicy


class Publisher:
    """围绕一个离线包目录的发布协调器。"""

    def __init__(self, work_dir: str, ds: Dataset):
        self.work_dir = work_dir
        self.ds = ds
        self.policy = VisibilityPolicy()
        self.index_dir = os.path.join(work_dir, "index")
        os.makedirs(self.work_dir, exist_ok=True)
        # 原始来源包同时导出为 JSON，供独立参考扫描器核对。
        self.sources_json = os.path.join(work_dir, "sources.json")
        export_sources_json(ds, self.sources_json)

    # ------------------------------------------------------------------ #
    def initial_publish(self, reader_wm: int) -> None:
        """首次全量发布：构建索引并写第一份全量公开清单。"""
        view = self.policy.public_view(self.ds, reader_wm)
        idx = InvertedIndex.create(self.index_dir)
        idx.build_full(view, reader_wm)
        write_full_manifest(self.work_dir, view, reader_wm)
        idx.close()

    def publish_revoke(self, old_wm: int, new_wm: int, *,
                       crash_at: str | None = None,
                       barrier_cp1=None, barrier_cp2=None,
                       txn_id: int = 1) -> InvertedIndex:
        """执行撤销发布（增量 + 两阶段 WAL），并产出全量与增量清单。"""
        old_view = self.policy.public_view(self.ds, old_wm)
        new_view = self.policy.public_view(self.ds, new_wm)
        idx = InvertedIndex.open(self.index_dir)
        idx.revoke_transaction(txn_id, new_view, new_wm,
                               crash_at=crash_at,
                               barrier_cp1=barrier_cp1,
                               barrier_cp2=barrier_cp2)
        # 仅在事务真正提交后才落清单（崩溃路径由测试直接验证抛出）。
        write_full_manifest(self.work_dir, new_view, new_wm)
        write_delta_manifest(self.work_dir, old_view, new_view,
                             old_wm, new_wm)
        return idx

    def recover(self) -> tuple[InvertedIndex, list[str]]:
        """崩溃后恢复：重放 WAL、前滚/补回执，并重新生成目标清单。"""
        idx = InvertedIndex.open(self.index_dir)
        actions = idx.recover()
        return idx, actions

    # ------------------------------------------------------------------ #
    def verify(self) -> "object":
        return verify_no_leak(self.work_dir, self.ds)

    def reader(self) -> SnapshotReader:
        return SnapshotReader(self.index_dir, self.policy, self.ds)

    def expected_view(self, reader_wm: int):
        return self.policy.public_view(self.ds, reader_wm)

    def assert_manifests_equivalent(self, old_wm: int,
                                    new_wm: int) -> None:
        """证明：全量快照 == 旧快照应用 delta，逐字节视图哈希一致。"""
        from .manifest import read_full_manifest
        mdir = os.path.join(self.work_dir, "manifests")
        _, old_records, _ = read_full_manifest(
            os.path.join(mdir, f"manifest.full.wm{old_wm}.bin"))
        old_view: dict = {}
        for term, ref in old_records:
            old_view.setdefault(term, []).append(ref)
        _o, _n, changes, new_digest = read_delta_manifest(
            os.path.join(mdir, f"manifest.delta.{old_wm}-{new_wm}.bin"))
        rebuilt = apply_delta(old_view, changes)
        if view_digest(rebuilt) != new_digest:
            raise AssertionError("delta 应用结果与全量清单视图哈希不一致")
