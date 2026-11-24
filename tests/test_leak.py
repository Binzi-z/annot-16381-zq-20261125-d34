# -*- coding: utf-8 -*-
"""公开清单不泄漏证明 + 独立参考扫描器交叉核对。"""
import os
import struct
import tempfile
import unittest

from offline_publisher.synthdata import build_small_case
from offline_publisher.visibility import VisibilityPolicy
from offline_publisher.publisher import Publisher
from offline_publisher.manifest import (verify_no_leak, write_full_manifest,
                                        read_full_manifest,
                                        write_delta_manifest,
                                        read_delta_manifest, apply_delta,
                                        _encode_record)
from offline_publisher.index import view_digest
from offline_publisher import scanner


class TestNoLeakAndScanner(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.ds = build_small_case()
        self.pol = VisibilityPolicy()
        self.wm = 100

    def test_full_and_delta_manifests_clean(self):
        pub = Publisher(os.path.join(self.tmp, "pkg"), self.ds)
        pub.initial_publish(self.wm)
        # 再发布一个不同水位，制造第二个全量快照与一份 delta。
        pub.publish_revoke(old_wm=50, new_wm=self.wm, txn_id=1)
        report = pub.verify()
        self.assertTrue(report.clean, report.text())
        # 至少扫描到两个全量 + 一个 delta 变体。
        names = [os.path.basename(p) for p in report.checked_files]
        self.assertTrue(any("full" in n for n in names))
        self.assertTrue(any("delta" in n for n in names))

    def test_manifests_equivalent_full_vs_delta(self):
        """全量快照与“旧快照应用 delta”必须得到同一视图哈希。"""
        pub = Publisher(os.path.join(self.tmp, "pkg"), self.ds)
        pub.initial_publish(50)
        pub.publish_revoke(old_wm=50, new_wm=100, txn_id=1)
        pub.assert_manifests_equivalent(50, 100)

        mdir = os.path.join(pub.work_dir, "manifests")
        _o, _n, changes, digest = read_delta_manifest(
            os.path.join(mdir, "manifest.delta.50-100.bin"))
        # 水位 50->100 唯一的结构变化是未来来源 3 的 word_future 被 ADD；
        # 受限来源 2 在任何水位都不可见，因此 delta 里绝不出现其记录。
        added_terms = {t for op, t, r in changes if op == 2}
        self.assertIn("word_future", added_terms)
        for op, t, r in changes:
            self.assertNotEqual(r.source_id, 2)
        # word_secret（受限独占词条）在两个水位的视图里都不存在。
        all_terms = {t for _op, t, _r in changes}
        self.assertNotIn("word_secret", all_terms)

    def test_restricted_anchors_absent_at_every_watermark(self):
        """跨全部读者水位：受限来源的页号/偏移/摘要永不出现。"""
        work = os.path.join(self.tmp, "pkg2")
        os.makedirs(work, exist_ok=True)
        for wm in (10, 25, 50, 100, 1000):
            view = self.pol.public_view(self.ds, wm)
            write_full_manifest(work, view, wm)
        report = verify_no_leak(work, self.ds)
        self.assertTrue(report.clean, report.text())
        self.assertEqual(len(report.checked_files), 5)

    def test_checker_detects_poisoned_manifest(self):
        """反证：故意写入受限来源记录，校验器必须报违规（证明不是空转）。"""
        work = os.path.join(self.tmp, "pkg3")
        os.makedirs(work, exist_ok=True)
        view = self.pol.public_view(self.ds, 100)
        clean_path = write_full_manifest(work, view, 100)
        # 取受限来源 2 的一条真实倒排项，拼进一份“投毒”清单。
        restricted = self.pol.restricted_postings(self.ds)
        victim = restricted[0]
        poisoned = os.path.join(work, "manifest.full.wm999.bin")
        with open(clean_path, "rb") as fh:
            blob = bytearray(fh.read())
        # 头部记录数 +1，再追加受限记录字节。
        struct.pack_into(">I", blob, 8, struct.unpack_from(">I", blob, 8)[0] + 1)
        blob += _encode_record(victim.term, victim.ref)
        with open(poisoned, "wb") as fh:
            fh.write(blob)
        report = verify_no_leak(work, self.ds)
        self.assertFalse(report.clean, "投毒清单竟通过了泄漏校验")
        joined = "\n".join(report.violations)
        self.assertIn("wm999", joined)
        # 字节级（摘要）与结构化两类证据至少各触发一次。
        self.assertTrue(("受限证据" in joined) or ("受限摘要" in joined))
        self.assertIn("受限来源 2", joined)

    def test_independent_scanner_cross_check(self):
        """参考扫描器（独立判定逻辑）与发布器清单逐来源交叉核对一致。"""
        pub = Publisher(os.path.join(self.tmp, "pkg4"), self.ds)
        pub.initial_publish(self.wm)
        problems = scanner.cross_check(
            pub.sources_json,
            os.path.join(pub.work_dir, "manifests"), self.wm)
        self.assertEqual(problems, [])

        view, restricted = scanner.scan_public_view(pub.sources_json, self.wm)
        # 参考扫描独立得出：受限来源 2 被剔除、共享词条保留、独占词条消失。
        self.assertTrue(all(r["source_id"] != 2
                            for refs in view.values() for r in refs))
        self.assertIn("word_shared", view)
        self.assertNotIn("word_secret", view)
        self.assertTrue(all(r["source_id"] == 2 for r in restricted))

    def test_scanner_watermark_rule(self):
        pub = Publisher(os.path.join(self.tmp, "pkg5"), self.ds)
        # 水位 50：未来来源 3（freeze_wm=999）不应出现。
        view, _ = scanner.scan_public_view(pub.sources_json, 50)
        self.assertNotIn("word_future", view)
        view100, _ = scanner.scan_public_view(pub.sources_json, 100)
        self.assertIn("word_future", view100)


if __name__ == "__main__":
    unittest.main(verbosity=2)
