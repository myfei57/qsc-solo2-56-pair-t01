"""清客确认领域服务测试。

不依赖 pytest（环境只有标准库），用 unittest 编写，同时可被 pytest 收集。
覆盖：门区顺序、收口闸门、闯入重新挂起、两种超时策略、幂等重放、并发确认、
掉线重连重建、部分失败恢复、哈希链篡改检测、三方结论一致性。
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest

from psdline.clock import ManualClock
from psdline.clearing import (
    GO,
    NO_GO,
    POLICY_PRESERVE,
    POLICY_RESET,
    ChainBroken,
    ClearingConflict,
    ClearingJournal,
    ClearingService,
)
from psdline.errors import InterlockError, RailError

ZONES = ["G01", "G02", "G03", "G04"]


def make_service(tmp: str, clock: ManualClock | None = None, *, ttl: float = 100.0,
                 policy: str = POLICY_PRESERVE) -> tuple[ManualClock, ClearingService]:
    clock = clock or ManualClock()
    journal = ClearingJournal(tmp)
    service = ClearingService(
        journal, clock, zones_per_station={"ST1": ZONES},
        confirm_ttl_s=ttl, timeout_policy=policy,
    )
    return clock, service


def confirm_all(service: ClearingService, train: str = "T1", suffix: str = "") -> None:
    for zone in ZONES:
        service.confirm_zone(train, zone, request_id=f"c-{zone}{suffix}")


class ConfirmationFlowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = ManualClock()
        self.tmp = tempfile.mkdtemp()
        _, self.service = make_service(self.tmp, self.clock)
        self.service.start_session("T1", "ST1", 1, request_id="start")

    def test_zone_confirmation_allowed_in_any_order(self) -> None:
        # 乱序确认全部允许
        for zone in reversed(ZONES):
            result = self.service.confirm_zone("T1", zone, request_id=f"c-{zone}")
            self.assertTrue(result["ok"])
        snap = self.service.snapshot_for_train("T1")
        self.assertEqual(snap["confirmed_current"], sorted(ZONES))
        self.assertTrue(snap["ready_to_finalize"])

    def test_no_departure_without_finalize(self) -> None:
        confirm_all(self.service)
        verdict = self.service.verdict("T1")
        self.assertEqual(verdict.decision, NO_GO)
        self.assertIn("not_finalized", verdict.reasons)

    def test_no_departure_when_any_zone_missing(self) -> None:
        for zone in ZONES[:-1]:
            self.service.confirm_zone("T1", zone, request_id=f"c-{zone}")
        with self.assertRaises(InterlockError):
            self.service.finalize("T1", request_id="f")

    def test_full_flow_to_go(self) -> None:
        confirm_all(self.service)
        self.service.finalize("T1", request_id="f")
        verdict = self.service.verdict("T1")
        self.assertEqual(verdict.decision, GO)
        self.assertEqual(verdict.reasons, ())

    def test_finalize_is_explicit_gate(self) -> None:
        confirm_all(self.service)
        # 即使全确认，收口也是一道显式动作
        self.assertNotEqual(self.service.verdict("T1").decision, GO)
        self.service.finalize("T1", request_id="f")
        self.assertEqual(self.service.verdict("T1").decision, GO)


class IntrusionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = ManualClock()
        self.tmp = tempfile.mkdtemp()
        _, self.service = make_service(self.tmp, self.clock)
        self.service.start_session("T1", "ST1", 1, request_id="start")

    def test_intrusion_suspends_and_invalidates_zone(self) -> None:
        confirm_all(self.service)
        self.service.finalize("T1", request_id="f")
        self.assertEqual(self.service.verdict("T1").decision, GO)

        result = self.service.report_intrusion("T1", zone_ids=["G02"], request_id="i")
        self.assertTrue(result["suspended"])
        verdict = self.service.verdict("T1")
        self.assertEqual(verdict.decision, NO_GO)
        self.assertIn("intrusion_suspended", verdict.reasons)
        self.assertEqual(verdict.missing, {"G02": "intrusion"})

    def test_must_reconfirm_and_refinalize_after_intrusion(self) -> None:
        confirm_all(self.service)
        self.service.finalize("T1", request_id="f")
        self.service.report_intrusion("T1", zone_ids=["G02"], request_id="i")
        self.service.resolve_intrusion("T1", request_id="r")
        # 解除挂起不自动恢复确认
        with self.assertRaises(InterlockError):
            self.service.finalize("T1", request_id="f2")
        # 重新确认被闯门区后才能重新收口
        self.service.confirm_zone("T1", "G02", request_id="c-G02-b")
        self.service.finalize("T1", request_id="f3")
        self.assertEqual(self.service.verdict("T1").decision, GO)

    def test_stationwide_intrusion_invalidates_all_zones(self) -> None:
        confirm_all(self.service)
        self.service.report_intrusion("T1", request_id="i")  # 不带 zone_ids
        snap = self.service.snapshot_for_train("T1")
        self.assertEqual(set(snap["missing"]), set(ZONES))

    def test_other_zone_confirmation_survives_scoped_intrusion(self) -> None:
        confirm_all(self.service)
        self.service.report_intrusion("T1", zone_ids=["G01"], request_id="i")
        self.service.resolve_intrusion("T1", request_id="r")
        snap = self.service.snapshot_for_train("T1")
        self.assertEqual(snap["missing"], {"G01": "intrusion"})
        # 其余三门区仍然有效
        self.assertEqual(
            sorted(snap["confirmed_current"]), ["G02", "G03", "G04"]
        )

    def test_intrusion_after_finalize_voids_conclusion(self) -> None:
        confirm_all(self.service)
        self.service.finalize("T1", request_id="f")
        self.service.report_intrusion("T1", zone_ids=["G01"], request_id="i")
        self.assertEqual(self.service.verdict("T1").stage, "suspended")
        self.assertNotEqual(self.service.verdict("T1").decision, GO)


class TimeoutPolicyTests(unittest.TestCase):
    def test_preserve_soft_expires_old_confirmations(self) -> None:
        clock = ManualClock()
        tmp = tempfile.mkdtemp()
        _, service = make_service(tmp, clock, ttl=50.0, policy=POLICY_PRESERVE)
        service.start_session("T1", "ST1", 1, request_id="s")
        confirm_all(service)
        clock.advance(60)
        verdict = service.verdict("T1")
        self.assertEqual(verdict.decision, NO_GO)
        self.assertTrue(all(reason == "confirm_expired"
                            for reason in verdict.missing.values()))
        # 记录没有被删
        snap = service.snapshot_for_train("T1")
        self.assertTrue(all(zone["confirmed"] for zone in snap["zones"]))
        # 重新确认即可收口
        for zone in ZONES:
            service.confirm_zone("T1", zone, request_id=f"r-{zone}")
        service.finalize("T1", request_id="f")
        self.assertEqual(service.verdict("T1").decision, GO)

    def test_reset_bumps_epoch_and_invalidates_all(self) -> None:
        clock = ManualClock()
        tmp = tempfile.mkdtemp()
        _, service = make_service(tmp, clock, ttl=50.0, policy=POLICY_RESET)
        service.start_session("T1", "ST1", 1, request_id="s")
        confirm_all(service)
        clock.advance(60)
        snap = service.snapshot_for_train("T1")  # 触发翻代
        self.assertEqual(snap["epoch"], 2)
        self.assertEqual(snap["confirmed_current"], [])
        # 旧确认全部 stale_epoch
        self.assertTrue(
            all(v == "stale_epoch" for v in snap["missing"].values())
        )
        # 新代际重新确认
        for zone in ZONES:
            service.confirm_zone("T1", zone, request_id=f"r-{zone}")
        service.finalize("T1", request_id="f")
        self.assertEqual(service.verdict("T1").decision, GO)

    def test_reset_does_not_double_bump_within_window(self) -> None:
        clock = ManualClock()
        tmp = tempfile.mkdtemp()
        _, service = make_service(tmp, clock, ttl=50.0, policy=POLICY_RESET)
        service.start_session("T1", "ST1", 1, request_id="s")
        confirm_all(service)
        clock.advance(60)
        service.snapshot_for_train("T1")
        # 同一窗口内多笔只读命令不能反复翻代
        service.verdict("T1")
        service.snapshot_for_train("T1")
        self.assertEqual(service.snapshot_for_train("T1")["epoch"], 2)

    def test_invalid_policy_rejected(self) -> None:
        with self.assertRaises(RailError):
            ClearingService(ClearingJournal(tempfile.mkdtemp()), ManualClock(),
                            timeout_policy="bogus")


class IdempotencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp()
        self.clock = ManualClock()
        _, self.service = make_service(self.tmp, self.clock)
        self.service.start_session("T1", "ST1", 1, request_id="start")

    def test_duplicate_confirm_is_idempotent(self) -> None:
        first = self.service.confirm_zone("T1", "G01", request_id="k")
        second = self.service.confirm_zone("T1", "G01", request_id="k")
        self.assertFalse(first["replayed"])
        self.assertTrue(second["replayed"])
        session_id = first["session_id"]
        events = self.service.events(session_id)
        self.assertEqual(len(events), 2)  # start + 1 次确认

    def test_every_command_requires_request_id(self) -> None:
        with self.assertRaises(RailError):
            self.service.confirm_zone("T1", "G01", request_id="")

    def test_request_id_cannot_cross_sessions(self) -> None:
        confirm_all(self.service)
        self.service.finalize("T1", request_id="f")
        self.service.mark_dispatched("T1", request_id="d")
        self.service.start_session("T1", "ST1", 2, request_id="start2")
        with self.assertRaises(ClearingConflict):
            # "c-G01" 属于 visit 1
            self.service.confirm_zone("T1", "G01", request_id="c-G01")

    def test_replay_after_reconnect_returns_original_result(self) -> None:
        # 模拟掉线重连：客户端不知道前一笔确认是否落库
        self.service.confirm_zone("T1", "G02", request_id="mobile-1")
        replay = self.service.confirm_zone("T1", "G02", request_id="mobile-1")
        self.assertTrue(replay["replayed"])
        self.assertEqual(replay["replayed_event"], "zone_confirmed")

    def test_finalize_duplicate_is_idempotent(self) -> None:
        confirm_all(self.service)
        f1 = self.service.finalize("T1", request_id="f")
        f2 = self.service.finalize("T1", request_id="f")
        self.assertTrue(f2["replayed"])
        self.assertEqual(f1["finalize_digest"], f2["finalize_digest"])


class ConcurrencyTests(unittest.TestCase):
    def test_concurrent_confirms_different_zones(self) -> None:
        tmp = tempfile.mkdtemp()
        clock = ManualClock()
        _, service = make_service(tmp, clock)
        service.start_session("T1", "ST1", 1, request_id="s")
        errors: list[Exception] = []

        def worker(zone: str) -> None:
            try:
                service.confirm_zone("T1", zone, request_id=f"k-{zone}")
            except Exception as exc:  # pragma: no cover - 失败时收集
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(z,)) for z in ZONES]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        snap = service.snapshot_for_train("T1")
        self.assertEqual(snap["confirmed_current"], sorted(ZONES))

    def test_concurrent_same_request_serialized(self) -> None:
        tmp = tempfile.mkdtemp()
        clock = ManualClock()
        _, service = make_service(tmp, clock)
        service.start_session("T1", "ST1", 1, request_id="s")
        outcomes: list[dict] = []

        def worker() -> None:
            outcomes.append(
                service.confirm_zone("T1", "G01", request_id="same")
            )

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(outcomes), 8)
        # 恰好一笔真正落事件，其余全部重放
        self.assertEqual(sum(1 for o in outcomes if not o["replayed"]), 1)
        events = service.events(outcomes[0]["session_id"])
        self.assertEqual(len(events), 2)

    def test_double_dispatch_only_one_consumes(self) -> None:
        tmp = tempfile.mkdtemp()
        clock = ManualClock()
        _, service = make_service(tmp, clock)
        service.start_session("T1", "ST1", 1, request_id="s")
        confirm_all(service)
        service.finalize("T1", request_id="f")
        service.mark_dispatched("T1", request_id="d1")
        # 不同键的第二次发车必须被挡
        with self.assertRaises(InterlockError):
            service.mark_dispatched("T1", request_id="d2")
        # 同键重放安全
        replay = service.mark_dispatched("T1", request_id="d1")
        self.assertTrue(replay["replayed"])

    def test_optimistic_epoch_conflict(self) -> None:
        tmp = tempfile.mkdtemp()
        clock = ManualClock()
        _, service = make_service(tmp, clock, ttl=50.0, policy=POLICY_RESET)
        service.start_session("T1", "ST1", 1, request_id="s")
        service.confirm_zone("T1", "G01", request_id="a")
        clock.advance(60)
        service.snapshot_for_train("T1")  # epoch -> 2
        with self.assertRaises(ClearingConflict) as ctx:
            service.confirm_zone("T1", "G02", request_id="b", expected_epoch=1)
        self.assertEqual(ctx.exception.snapshot["epoch"], 2)


class ReconnectRestoreTests(unittest.TestCase):
    def test_restore_rebuilds_state_from_chain(self) -> None:
        tmp = tempfile.mkdtemp()
        clock = ManualClock()
        _, service = make_service(tmp, clock)
        service.start_session("T1", "ST1", 1, request_id="s")
        confirm_all(service)
        service.finalize("T1", request_id="f")

        # 新进程：从哈希链重建
        _, rebuilt = make_service(tmp, ManualClock(clock.now()))
        count = rebuilt.restore()
        self.assertEqual(count, 1)
        verdict = rebuilt.verdict("T1")
        self.assertEqual(verdict.decision, GO)
        self.assertTrue(rebuilt.verify(verdict.session_id)["ok"])

    def test_restore_includes_dispatched_session(self) -> None:
        tmp = tempfile.mkdtemp()
        clock = ManualClock()
        _, service = make_service(tmp, clock)
        service.start_session("T1", "ST1", 1, request_id="s")
        confirm_all(service)
        service.finalize("T1", request_id="f")
        service.mark_dispatched("T1", request_id="d")

        _, rebuilt = make_service(tmp, clock)
        rebuilt.restore()
        verdict = rebuilt.verdict("T1")
        self.assertEqual(verdict.stage, "dispatched")
        self.assertIn("already_dispatched", verdict.reasons)

    def test_partial_failure_resume_retries_safely(self) -> None:
        # 客户端在每步后崩溃/重连：重放全部 request_id 不会产生重复事件
        tmp = tempfile.mkdtemp()
        clock = ManualClock()
        _, service = make_service(tmp, clock)
        service.start_session("T1", "ST1", 1, request_id="s")
        for zone in ZONES:
            service.confirm_zone("T1", zone, request_id=f"k-{zone}")
        # “重连后”把前面所有请求原样重发一遍
        for zone in ZONES:
            result = service.confirm_zone("T1", zone, request_id=f"k-{zone}")
            self.assertTrue(result["replayed"])
        service.finalize("T1", request_id="f")
        service.mark_dispatched("T1", request_id="d")
        events = service.events(service.verdict("T1").session_id)
        # start + 4 confirm + finalize + dispatched = 7
        self.assertEqual(len(events), 7)


class HashChainTests(unittest.TestCase):
    def test_tamper_detected_on_verify(self) -> None:
        tmp = tempfile.mkdtemp()
        clock = ManualClock()
        _, service = make_service(tmp, clock)
        service.start_session("T1", "ST1", 1, request_id="s")
        confirm_all(service)
        session_id = service.verdict("T1").session_id

        # 在磁盘上篡改第一条确认事件
        journal = service._journal  # noqa: SLF001
        path = journal._journal(session_id).segments()[0]  # noqa: SLF001
        with open(path, encoding="utf-8") as handle:
            lines = handle.read().splitlines()
        record = json.loads(lines[2])  # 0=start 1=第一条确认(按zone顺序首确认)
        record["payload"]["confirmed_by"] = "attacker"
        lines[2] = json.dumps(record, ensure_ascii=False)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("\n".join(lines) + "\n")

        fresh = ClearingJournal(tmp)
        with self.assertRaises(ChainBroken):
            fresh.load(session_id, verify=True)

    def test_history_is_append_only_canonical(self) -> None:
        tmp = tempfile.mkdtemp()
        clock = ManualClock()
        _, service = make_service(tmp, clock)
        service.start_session("T1", "ST1", 1, request_id="s")
        service.confirm_zone("T1", "G01", request_id="a")
        session_id = service.verdict("T1").session_id
        events = service.events(session_id)
        # 序号连续、哈希相接
        self.assertEqual([e["seq"] for e in events], [1, 2])
        self.assertEqual(events[0]["prev_hash"], "0" * 64)
        self.assertEqual(events[1]["prev_hash"], events[0]["hash"])
        self.assertEqual(len(events[0]["hash"]), 64)


if __name__ == "__main__":
    unittest.main()
