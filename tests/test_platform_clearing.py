"""清客确认与平台发车链的集成测试。

验证关键安全性质：

1. 没逐块确认/没收口，发车编排一定被挡（门控不会带着人发车）；
2. 收口后闯回站台，结论立即失效，发车被挡，重新确认收口后才放行；
3. 门控、联锁（清客结论）、审计三处读的是同一份 session+digest；
4. 到站幂等、清客动作幂等，进程重启后哈希链重建结论一致。
"""

from __future__ import annotations

import unittest

from psdline.app import build_platform
from psdline.clock import ManualClock
from psdline.config import default_config
from psdline.errors import RailError

# default_config 每站 4 个门区 G01..G04


class PlatformClearingTests(unittest.TestCase):
    def setUp(self) -> None:
        import tempfile

        self.clock = ManualClock()
        self.platform = build_platform(
            default_config(), clock=self.clock, storage_root=tempfile.mkdtemp()
        )
        self.platform.start()
        self.platform.arrive("T01", "ST1", 0)
        self.platform.check_gap("T01")

    def _zones(self, train: str = "T01") -> list[str]:
        return self.platform.clearing_session(train)["zone_order"]

    def _confirm_all(self, train: str = "T01", suffix: str = "") -> None:
        for zone in self._zones(train):
            self.platform.confirm_zone(train, zone, f"k-{zone}{suffix}")

    def test_depart_blocked_before_finalize(self) -> None:
        report = self.platform.depart("T01", "ST1")
        self.assertFalse(report["ok"])
        self.assertTrue(any("清客确认未放行" in v for v in report["violations"]))
        self.assertEqual(report["clearing"]["decision"], "no_go")
        # 门控侧不应有发车事实
        from psdline.door import departed

        self.assertFalse(departed(self.platform.doors, "T01"))

    def test_depart_blocked_when_only_partial_zones(self) -> None:
        for zone in self._zones()[:-1]:
            self.platform.confirm_zone("T01", zone, f"k-{zone}")
        with self.assertRaises(RailError):
            self.platform.finalize_clearing("T01", request_id="f")
        self.assertFalse(self.platform.depart("T01", "ST1")["ok"])

    def test_depart_allowed_after_finalize_and_three_facts_align(self) -> None:
        self._confirm_all()
        finalized = self.platform.finalize_clearing("T01", request_id="f")
        digest = finalized["finalize_digest"]
        session_id = finalized["session_id"]

        report = self.platform.depart("T01", "ST1")
        self.assertTrue(report["ok"], report["violations"])
        self.assertTrue(report["reconciled"])
        # 门控事实
        from psdline.door import departed

        self.assertTrue(departed(self.platform.doors, "T01"))
        # 清客/联锁事实：同一 session、同一 digest、已闭环
        verdict = self.platform.clearing_verdict("T01")
        self.assertEqual(verdict["session_id"], session_id)
        self.assertEqual(report["clearing"]["digest"], digest)
        self.assertEqual(verdict["stage"], "dispatched")
        # 审计事实：有闭环流水
        kinds = [e.kind for e in self.platform.audit.list()]
        self.assertIn("clearing_dispatch", kinds)
        dispatch_audit = [e for e in self.platform.audit.list()
                          if e.kind == "clearing_dispatch"][-1]
        self.assertEqual(dispatch_audit.detail["digest"], digest)
        self.assertTrue(dispatch_audit.detail["reconciled"])

    def test_intrusion_after_finalize_reopens_gate(self) -> None:
        self._confirm_all()
        self.platform.finalize_clearing("T01", request_id="f")
        self.platform.report_intrusion("T01", "i", zone_ids=["G02"])
        report = self.platform.depart("T01", "ST1")
        self.assertFalse(report["ok"])
        # 恢复：解除挂起、重确认 G02、重新收口
        self.platform.resolve_intrusion("T01", "r")
        self.platform.confirm_zone("T01", "G02", "k-G02-b")
        self.platform.finalize_clearing("T01", request_id="f-b")
        self.assertTrue(self.platform.depart("T01", "ST1")["ok"])

    def test_idempotent_arrival_does_not_open_second_session(self) -> None:
        # 同一停靠重复到站（重连重放）：visit 不重复加，会话同一份
        first = self.platform.clearing_session("T01")["session_id"]
        again = self.platform.arrive("T01", "ST1", 0)
        self.assertTrue(again["ok"])
        self.assertEqual(again["clearing_session"], first)

    def test_restart_rebuilds_and_verdict_identical(self) -> None:
        self._confirm_all()
        self.platform.finalize_clearing("T01", request_id="f")
        self.platform.depart("T01", "ST1")
        root = self.platform.store.root

        restored = build_platform(default_config(), clock=self.clock, storage_root=root)
        verdict = restored.clearing_verdict("T01")
        self.assertEqual(verdict["stage"], "dispatched")
        self.assertIn("already_dispatched", verdict["reasons"])
        self.assertTrue(restored.verify_clearing("T01")["ok"])

    def test_second_visit_tracks_separate_session(self) -> None:
        self._confirm_all()
        self.platform.finalize_clearing("T01", request_id="f1")
        self.platform.depart("T01", "ST1")
        # 第二站对位里程 24m
        self.platform.arrive("T01", "ST2", 24)
        self.platform.check_gap("T01")
        snap = self.platform.clearing_session("T01")
        self.assertEqual(snap["session_id"], "T01@ST2#002")
        self.assertEqual(snap["stage"], "open")

    def test_stale_arrival_position_does_not_start_session(self) -> None:
        # 对位失败不应开始清客会话
        bad = self.platform.arrive("T02", "ST2", 99)
        self.assertFalse(bad["ok"])
        self.assertIsNone(self.platform.clearing.snapshot_for_train("T02"))

    def test_hardware_failure_does_not_consume_clearance(self) -> None:
        from unittest import mock

        import psdline.app as app_module
        from psdline.errors import RailError as _RailError

        self._confirm_all()
        self.platform.finalize_clearing("T01", request_id="f")
        # 关门硬件失败：清客结论必须保持未消费，发车被挡
        with mock.patch.object(
            app_module, "close_and_audit",
            side_effect=_RailError("门控总线离线", component="door"),
        ):
            failed = self.platform.depart("T01", "ST1")
        self.assertFalse(failed["ok"])
        verdict = self.platform.clearing_verdict("T01")
        self.assertEqual(verdict["stage"], "finalized")
        self.assertEqual(verdict["decision"], "go")
        # 硬件恢复后整条链重试成功
        recovered = self.platform.depart("T01", "ST1")
        self.assertTrue(recovered["ok"])
        # 结论只能消费一次：第三次发车被挡
        self.assertFalse(self.platform.depart("T01", "ST1")["ok"])


if __name__ == "__main__":
    unittest.main()
