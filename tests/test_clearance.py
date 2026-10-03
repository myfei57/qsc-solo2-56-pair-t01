"""站台清客确认：门区收口、挂起、超时、幂等与重启恢复。"""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from psdline.app import build_platform
from psdline.clearance.events import ClearanceEvent
from psdline.config import default_config
from psdline.door import close_doors, departed
from psdline.errors import RailError


def _arrive(platform, train_id="T01", station_id="ST1"):
    platform.start()
    platform.arrive(train_id, station_id, 0)
    return train_id, station_id


def _confirm_all(platform, train_id="T01", station_id="ST1", operator="staff", request_id=None):
    zones = platform.clearance_view(train_id, station_id)["zones"]
    results = []
    for index, zone in enumerate(zones):
        req = f"confirm-{index}" if request_id == "stable" else ""
        results.append(
            platform.confirm_clearance_zone(train_id, station_id, zone["id"], operator, req)
        )
    return results


def _complete(platform, train_id="T01", station_id="ST1", request_id="complete-1"):
    return platform.complete_clearance(train_id, station_id, "dispatcher", request_id)


def test_arbitrary_zone_confirmation_and_final_clearance(platform):
    _arrive(platform)
    zones = platform.clearance_view("T01", "ST1")["zones"]

    before = platform.depart("T01", "ST1")
    assert before["ok"] is False
    assert before["violations"] == ["清客门区未全部确认"]

    for zone in reversed(zones):
        result = platform.confirm_clearance_zone("T01", "ST1", zone["id"], "staff")
        assert result["ok"] is True

    assert platform.clearance_verdict("T01", "ST1")["ok"] is False
    cleared = _complete(platform)
    assert cleared["status"] == "cleared"
    assert cleared["verdict_id"].startswith("clr-")
    assert platform.clearance_verdict("T01", "ST1")["ok"] is True

    platform.check_gap("T01")
    departure = platform.depart("T01", "ST1")
    assert departure["ok"] is True
    assert departure["clearance_verdict_id"] == cleared["verdict_id"]
    assert platform.clearance_view("T01", "ST1")["status"] == "closed"


def test_suspend_requires_all_zones_reconfirmed(platform):
    _arrive(platform)
    zones = platform.clearance_view("T01", "ST1")["zones"]
    first = zones[0]["id"]
    platform.confirm_clearance_zone("T01", "ST1", first, "staff", "req-1")

    suspended = platform.suspend_clearance("T01", "ST1", "passenger_returned", "sensor", "sus-1")
    assert suspended["status"] == "suspended"
    assert suspended["pending_zones"] == [zone["id"] for zone in zones]

    blocked = platform.depart("T01", "ST1")
    assert blocked["violations"] == ["清客确认已挂起"]

    resumed = platform.resume_clearance("T01", "ST1", "dispatcher", "resume-1")
    assert resumed["status"] == "open"
    assert resumed["confirmed_count"] == 0

    _confirm_all(platform)
    assert _complete(platform)["status"] == "cleared"


def test_zone_timeout_preserves_history_but_invalidates_current_state(clock, tmp_path):
    config = default_config()
    platform = build_platform(config, clock=clock, storage_root=str(tmp_path / "state"))
    _arrive(platform)
    zones = platform.clearance_view("T01", "ST1")["zones"]
    for zone in zones[:2]:
        platform.confirm_clearance_zone("T01", "ST1", zone["id"], "staff")

    clock.advance(config.clearance.zone_ttl_s + 1)
    view = platform.clearance_view("T01", "ST1")
    assert view["confirmed_count"] == 0
    statuses = {zone["id"]: zone["status"] for zone in view["zones"]}
    assert statuses["G01"] == "expired"
    assert statuses["G02"] == "expired"
    assert statuses["G03"] == "pending"
    assert statuses["G04"] == "pending"

    history = platform.clearance_history("T01", "ST1")["events"]
    kinds = [event["kind"] for event in history]
    assert "clearance_zone_confirmed" in kinds
    assert kinds[-1] == "clearance_zones_expired"
    assert platform.depart("T01", "ST1")["violations"] == ["清客门区未全部确认"]


def test_same_request_id_is_idempotent(platform):
    _arrive(platform)
    first = platform.confirm_clearance_zone("T01", "ST1", "G01", "staff", "req-g01")
    duplicate = platform.confirm_clearance_zone("T01", "ST1", "G01", "staff", "req-g01")
    assert duplicate["idempotent"] is True
    assert duplicate["original_event_id"] == first["event_id"]
    assert duplicate["revision"] == first["revision"]

    with pytest.raises(RailError) as exc_info:
        platform.confirm_clearance_zone("T01", "ST1", "G02", "staff", "req-g01")
    assert exc_info.value.message == "请求标识已用于其他门区: req-g01"


def test_concurrent_zone_confirmations_are_serialized_and_complete(platform):
    _arrive(platform)
    zones = [zone["id"] for zone in platform.clearance_view("T01", "ST1")["zones"]]

    def confirm(zone_id: str):
        return platform.confirm_clearance_zone("T01", "ST1", zone_id, "staff", f"req-{zone_id}")

    with ThreadPoolExecutor(max_workers=8) as pool:
        first = list(pool.map(confirm, zones))
        second = list(pool.map(confirm, zones))

    assert len({item["original_event_id"] for item in first}) == len(zones)
    assert all(item["idempotent"] is True for item in second)
    assert {item["original_event_id"] for item in first} == {
        item["original_event_id"] for item in second
    }
    view = platform.clearance_view("T01", "ST1")
    assert view["confirmed_count"] == view["total_zones"]
    assert _complete(platform, request_id="complete-concurrent")["status"] == "cleared"


def test_reconnect_rebuilds_state_from_immutable_journal(platform, clock, tmp_path):
    _arrive(platform)
    _confirm_all(platform)
    cleared = _complete(platform)
    journal = sorted(Path(platform.store.root).glob("clearance-*.jsonl"))
    assert journal and journal[0].read_text(encoding="utf-8").count("\n") >= 6

    restarted = build_platform(
        default_config(), clock=clock, storage_root=platform.store.root
    )
    view = restarted.clearance_view("T01", "ST1")
    assert view["status"] == "cleared"
    assert view["verdict_id"] == cleared["verdict_id"]
    assert len(restarted.clearance_history("T01", "ST1")["events"]) == len(
        platform.clearance_history("T01", "ST1")["events"]
    )

    restarted.check_gap("T01")
    departure = restarted.depart("T01", "ST1")
    assert departure["ok"] is True


def test_restart_aborts_prepared_release_when_signal_is_missing(platform, clock):
    _arrive(platform)
    _confirm_all(platform)
    _complete(platform)
    platform.clearance_service.prepare_release("T01", "ST1", "depart-prepared")

    restarted = build_platform(default_config(), clock=clock, storage_root=platform.store.root)
    view = restarted.clearance_view("T01", "ST1")
    assert view["status"] == "cleared"

    restarted.check_gap("T01")
    departure = restarted.depart("T01", "ST1")
    assert departure["ok"] is True


def test_restart_finishes_prepared_release_when_signal_raised(platform, clock):
    _arrive(platform)
    _confirm_all(platform)
    _complete(platform)
    platform.clearance_service.prepare_release("T01", "ST1", "depart-prepared")
    close_doors(platform.doors, "T01", clock.now())
    platform.signals.raise_signal("T01", "depart", clock.now())

    restarted = build_platform(default_config(), clock=clock, storage_root=platform.store.root)
    assert restarted.clearance_view("T01", "ST1")["status"] == "closed"
    assert departed(restarted.doors, "T01") is True


def test_journal_tampering_is_rejected_on_startup(tmp_path, clock):
    root = tmp_path / "state"
    platform = build_platform(default_config(), clock=clock, storage_root=str(root))
    _arrive(platform)
    platform.confirm_clearance_zone("T01", "ST1", "G01", "staff", "req-g01")

    journal = next(root.glob("clearance-*.jsonl"))
    lines = journal.read_text(encoding="utf-8").splitlines()
    event = ClearanceEvent.from_dict(json.loads(lines[-1]))
    tampered = event.as_dict()
    tampered["operator"] = "forged"
    lines[-1] = json.dumps(tampered, ensure_ascii=False)
    journal.write_text("\n".join(lines) + "\n", encoding="utf-8")

    with pytest.raises(RailError) as exc_info:
        build_platform(default_config(), clock=clock, storage_root=str(root))
    assert exc_info.value.component == "clearance"


def test_stale_completion_revision_conflicts(platform):
    _arrive(platform)
    stale_view = platform.clearance_view("T01", "ST1")
    platform.confirm_clearance_zone("T01", "ST1", "G01", "staff", "req-g01")
    with pytest.raises(RailError) as exc_info:
        platform.complete_clearance(
            "T01", "ST1", "dispatcher", "complete-old", expected_revision=stale_view["revision"]
        )
    assert "清客状态已变化" in exc_info.value.message
