# -*- coding: utf-8 -*-
"""两个真实线程 + 屏障 的崩溃恢复一致性测试。

场景：
- *读取线程* 持续做快照读取（每次重新加载原子指针），并在屏障处与更新线程
  确定性会合；它读到的每个视图都必须是“某个完整提交版本”，绝不允许出现
  半切换的混合状态。
- *更新线程* 执行一次撤销发布，分别在 CP1（撤销承诺已持久化、指针切换前）
  与 CP2（指针已切换、提交回执丢失）注入 :class:`SimulatedCrash`。

恢复后断言：读者只能读到*完整的可见集合*，且与
``VisibilityPolicy.public_view`` 逐字节相等；WAL 中每个事务最终恰好一条
PREPARED + 一条 COMMITTED（CP2 的补回执必须幂等，不产生重复发布）。
"""
import os
import tempfile
import threading
import unittest

from offline_publisher.model import Classification
from offline_publisher.synthdata import build_small_case
from offline_publisher.visibility import VisibilityPolicy
from offline_publisher.index import InvertedIndex, SnapshotReader, view_digest
from offline_publisher.journal import COMMITTED, PREPARED, SimulatedCrash


def _pending_revoke_case():
    """撤销*前*的数据集：来源 2 仍是公开，撤销在运行期补记为受限。"""
    ds = build_small_case()
    ds.sources[2].classification = Classification.PUBLIC
    return ds


class TestBarrierCrashRecovery(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.index_dir = os.path.join(self.tmp, "idx")
        self.ds = _pending_revoke_case()
        self.pol = VisibilityPolicy()
        self.wm = 100
        # 撤销前视图（含来源 2）与撤销后目标视图（剔除来源 2）。
        self.before_view = self.pol.public_view(self.ds, self.wm)
        self.ds.sources[2].classification = Classification.RESTRICTED
        self.after_view = self.pol.public_view(self.ds, self.wm)
        # 前置不变量：撤销确实改变了可见集合。
        self.assertIn("word_secret", self.before_view)
        self.assertNotIn("word_secret", self.after_view)

        idx = InvertedIndex.create(self.index_dir)
        idx.build_full(self.before_view, self.wm)
        idx.close()

    def _run_reader(self, ready, gate, observations, errors):
        """持续读取线程：先确认一个稳定旧视图，放行更新，再在崩溃点会合。"""
        reader = SnapshotReader(self.index_dir, self.pol, self.ds)
        try:
            v = reader.read_once()
            # 更新线程被 ready 事件挡在事务开始之前，因此这里必为完整旧视图。
            self.assertEqual(view_digest(v), view_digest(self.before_view))
            observations.append(("pre", v))
            ready.set()      # 放行更新线程
            gate.wait()      # 在受控崩溃点与更新线程确定性会合
            # 会合后连续读取：每个快照都必须是完整的旧版或新版，绝无混合态。
            for _ in range(25):
                v = reader.read_once()
                digest = view_digest(v)
                self.assertIn(
                    digest,
                    {view_digest(self.before_view),
                     view_digest(self.after_view)},
                    "读到了既非旧版也非新版的不完整混合视图")
                observations.append(("post", v))
        except Exception as exc:  # noqa: BLE001 - 测试需把异常带回主线程
            errors.append(exc)
            ready.set()  # 避免更新线程永久等待

    def _crash_scenario(self, crash_at, barrier_attr):
        ready = threading.Event()
        gate = threading.Barrier(2)
        observations: list = []
        errors: list = []
        reader_thread = threading.Thread(
            target=self._run_reader, args=(ready, gate, observations, errors),
            name="snapshot-reader", daemon=True)
        reader_thread.start()

        crash = threading.Event()

        def updater():
            ready.wait(timeout=10)  # 必须等读者确认旧视图后才开始变更
            idx = InvertedIndex.open(self.index_dir)
            try:
                idx.revoke_transaction(
                    1, self.after_view, self.wm, crash_at=crash_at,
                    **{barrier_attr: gate})
            except SimulatedCrash:
                crash.set()
            finally:
                idx.close()

        updater_thread = threading.Thread(target=updater, name="revoker",
                                          daemon=True)
        updater_thread.start()
        reader_thread.join(timeout=20)
        updater_thread.join(timeout=20)
        self.assertFalse(reader_thread.is_alive(), "读取线程超时")
        self.assertFalse(updater_thread.is_alive(), "更新线程超时")
        self.assertTrue(crash.is_set(), "崩溃注入未发生")
        self.assertEqual(errors, [], f"读取线程报错: {errors}")

        # ---------------- 恢复 ----------------
        rec = InvertedIndex.open(self.index_dir)
        actions = rec.recover()
        recovered = rec.snapshot_view()
        rec.close()

        # 恢复后只能读到完整的撤销后可见集合。
        self.assertEqual(view_digest(recovered),
                         view_digest(self.after_view))
        self.assertEqual(recovered, self.after_view)

        # WAL 终态：恰好一条 PREPARED 与一条 COMMITTED，无重复回执。
        rec2 = InvertedIndex.open(self.index_dir)
        state = rec2.journal.txn_state()
        rec2.close()
        self.assertEqual(sorted(state), [1])
        self.assertEqual(sorted(state[1]), [COMMITTED, PREPARED])
        return actions

    def test_cp1_promise_before_pointer_switch(self):
        actions = self._crash_scenario("CP1", "barrier_cp1")
        self.assertTrue(any("CP1 前滚" in a for a in actions),
                        f"未执行 CP1 前滚: {actions}")

    def test_cp2_pointer_switched_ack_lost(self):
        actions = self._crash_scenario("CP2", "barrier_cp2")
        self.assertTrue(any("CP2" in a and "回执" in a for a in actions),
                        f"未执行 CP2 幂等补回执: {actions}")

    def test_clean_concurrent_revoke_no_torn_read(self):
        """无崩溃的纯并发撤销：读取线程全程只见完整版本，最终为新视图。"""
        ready = threading.Event()
        gate = threading.Barrier(2)
        observations: list = []
        errors: list = []
        reader_thread = threading.Thread(
            target=self._run_reader,
            args=(ready, gate, observations, errors),
            name="snapshot-reader", daemon=True)
        reader_thread.start()

        def updater():
            ready.wait(timeout=10)
            idx = InvertedIndex.open(self.index_dir)
            try:
                idx.revoke_transaction(
                    1, self.after_view, self.wm, barrier_cp1=gate)
            finally:
                idx.close()

        updater_thread = threading.Thread(target=updater, name="revoker",
                                          daemon=True)
        updater_thread.start()
        reader_thread.join(timeout=20)
        updater_thread.join(timeout=20)
        self.assertEqual(errors, [])

        idx = InvertedIndex.open(self.index_dir)
        # 无崩溃路径也应能安全 recover（幂等核对，不产生新动作）。
        idx.recover()
        self.assertEqual(idx.snapshot_view(), self.after_view)
        idx.close()

        # 观测中旧完整版本确实出现过，且没有任何混合视图（_run_reader 已断言）。
        digests = {view_digest(v) for _stage, v in observations}
        self.assertIn(view_digest(self.before_view), digests)


class TestRecoveryIdempotent(unittest.TestCase):
    """非线程的直接崩溃/重放测试，明确校验两种 WAL 终态与幂等性。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.index_dir = os.path.join(self.tmp, "idx")
        self.ds = _pending_revoke_case()
        self.pol = VisibilityPolicy()
        self.before = self.pol.public_view(self.ds, 100)
        self.ds.sources[2].classification = Classification.RESTRICTED
        self.after = self.pol.public_view(self.ds, 100)
        idx = InvertedIndex.create(self.index_dir)
        idx.build_full(self.before, 100)
        idx.close()

    def _crash(self, point):
        idx = InvertedIndex.open(self.index_dir)
        with self.assertRaises(SimulatedCrash):
            idx.revoke_transaction(1, self.after, 100, crash_at=point)
        idx.close()

    def test_cp1_wal_then_rollforward(self):
        self._crash("CP1")
        # 崩溃后、恢复前：指针仍指向旧根，旧视图完整可读。
        idx = InvertedIndex.open(self.index_dir)
        self.assertEqual(idx.snapshot_view(), self.before)
        journal = idx.journal
        state = journal.txn_state()
        self.assertIn(PREPARED, state[1])
        self.assertNotIn(COMMITTED, state[1])
        actions = idx.recover()
        self.assertEqual(idx.snapshot_view(), self.after)
        idx.close()
        # 再 recover 一次必须幂等，不重复发布、不新增动作。
        idx = InvertedIndex.open(self.index_dir)
        again = idx.recover()
        self.assertEqual(again, [])
        self.assertEqual(idx.snapshot_view(), self.after)
        idx.close()
        self.assertTrue(any("CP1 前滚" in a for a in actions))

    def test_cp2_wal_ack_lost_then_idempotent_ack(self):
        self._crash("CP2")
        # CP2：指针已切换，恢复前读到的就已经是新视图，只差回执。
        idx = InvertedIndex.open(self.index_dir)
        self.assertEqual(idx.snapshot_view(), self.after)
        actions = idx.recover()
        idx.close()
        idx = InvertedIndex.open(self.index_dir)
        state = idx.journal.txn_state()
        idx.close()
        self.assertIn(COMMITTED, state[1])
        self.assertTrue(any("CP2" in a for a in actions))


if __name__ == "__main__":
    unittest.main(verbosity=2)
