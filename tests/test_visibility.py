# -*- coding: utf-8 -*-
"""可见性语义与索引物化测试（标准库 unittest）。"""
import os
import tempfile
import unittest

from offline_publisher.synthdata import (ANCHOR_TERMS, REVOKED_ID,
                                         build_small_case)
from offline_publisher.visibility import VisibilityPolicy
from offline_publisher.index import InvertedIndex, view_digest
from offline_publisher.model import Classification


class TestVisibility(unittest.TestCase):
    def setUp(self):
        self.ds = build_small_case()
        self.pol = VisibilityPolicy()

    def test_watermark_hides_future_source(self):
        # 固定在水位 50：来源 3（freeze_wm=999）必须被水位规则隐藏。
        view = self.pol.public_view(self.ds, 50)
        self.assertNotIn("word_future", view)
        # 提到水位 100 后可见。
        self.assertIn("word_future",
                      self.pol.public_view(self.ds, 100))

    def test_revocation_is_retroactive(self):
        # 撤销回溯生效：即使固定在撤销来源冻结之后的水位，受限来源也不可见。
        for wm in (50, 100, 1000):
            view = self.pol.public_view(self.ds, wm)
            for refs in view.values():
                self.assertTrue(all(r.source_id != 2 for r in refs),
                                f"水位 {wm} 泄漏了受限来源 2")

    def test_shared_term_retained(self):
        view = self.pol.public_view(self.ds, 100)
        # word_shared 被来源 0/1 合法引用，撤销来源 2 后必须保留。
        self.assertIn("word_shared", view)
        ids = {r.source_id for r in view["word_shared"]}
        self.assertEqual(ids, {0, 1})
        # 锚点词条同样保留，且不含受限来源。
        self.assertIn(ANCHOR_TERMS[0], view)
        self.assertTrue(all(r.source_id != 2
                            for r in view[ANCHOR_TERMS[0]]))

    def test_exclusive_term_dropped(self):
        view = self.pol.public_view(self.ds, 100)
        # 受限来源独占词条整体消失。
        self.assertNotIn("word_secret", view)
        self.assertEqual(self.pol.dropped_terms(self.ds, 100),
                         ["word_secret"])

    def test_revoked_source_classification(self):
        self.assertEqual(
            self.ds.sources[2].classification, Classification.RESTRICTED)
        restricted = self.pol.restricted_postings(self.ds)
        self.assertTrue(restricted)
        self.assertTrue(all(vp.ref.source_id == 2 for vp in restricted))


class TestIndexMaterialization(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.ds = build_small_case()
        self.pol = VisibilityPolicy()

    def test_full_then_incremental_roundtrip(self):
        v50 = self.pol.public_view(self.ds, 50)
        v100 = self.pol.public_view(self.ds, 100)
        idx_dir = os.path.join(self.tmp, "idx")
        idx = InvertedIndex.create(idx_dir)
        idx.build_full(v50, 50)
        self.assertEqual(idx.snapshot_view(), v50)
        idx.close()

        idx = InvertedIndex.open(idx_dir)
        idx.pager.reset_counters()
        stats = idx.apply_incremental(v100, 100)
        idx.pager.commit(100)
        self.assertEqual(idx.snapshot_view(), v100)
        # 水位 50->100 带来未来来源 3 的新词条（word_future）。
        self.assertGreaterEqual(stats["added"], 1)
        # 增量必须有未触碰的词条（证明不是全量重写）。
        self.assertGreater(stats["unchanged"], 0)
        idx.close()

        # 重新打开（模拟进程重启）后视图依然正确。
        idx = InvertedIndex.open(idx_dir)
        self.assertEqual(idx.snapshot_view(), v100)
        self.assertEqual(idx.reader_wm, 100)
        idx.close()

    def test_digest_stability(self):
        v = self.pol.public_view(self.ds, 100)
        self.assertEqual(len(view_digest(v)), 32)
        self.assertEqual(view_digest(v), view_digest(dict(v)))


class TestIncrementalRevocation(unittest.TestCase):
    """撤销（公开->受限）在索引物化层的增量移除与共享词条保留。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        # 撤销前：来源 2 公开。
        self.ds = build_small_case()
        self.ds.sources[2].classification = Classification.PUBLIC
        self.pol = VisibilityPolicy()
        self.before = self.pol.public_view(self.ds, 100)
        self.assertIn("word_secret", self.before)

    def test_revoke_removes_exclusive_keeps_shared(self):
        from offline_publisher.index import InvertedIndex
        idx_dir = os.path.join(self.tmp, "idx")
        idx = InvertedIndex.create(idx_dir)
        idx.build_full(self.before, 100)
        idx.close()

        # 补记来源 2 为受限，重算视图并增量应用。
        self.ds.sources[2].classification = Classification.RESTRICTED
        after = self.pol.public_view(self.ds, 100)
        idx = InvertedIndex.open(idx_dir)
        stats = idx.apply_incremental(after, 100)
        idx.pager.commit(100)

        self.assertGreaterEqual(stats["removed"], 1)  # word_secret 被移除
        # 共享词条仍在，且只含授权来源 0/1。
        self.assertEqual(idx.snapshot_view(), after)
        self.assertIn("word_shared", after)
        self.assertEqual({r.source_id for r in after["word_shared"]}, {0, 1})
        self.assertNotIn("word_secret", after)
        idx.close()

        # 重启后视图稳定。
        idx = InvertedIndex.open(idx_dir)
        self.assertEqual(idx.snapshot_view(), after)
        idx.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
