"""测试夹具：所有用例都从干净存储和可推进时钟起步。"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from psdline.app import Platform, build_platform  # noqa: E402
from psdline.clock import ManualClock  # noqa: E402
from psdline.config import default_config  # noqa: E402


@pytest.fixture()
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture()
def platform(tmp_path, clock) -> Platform:
    return build_platform(default_config(), clock=clock, storage_root=str(tmp_path / "state"))


@pytest.fixture()
def online(platform) -> Platform:
    """已到站、已开门、已确认间隙的平台。"""

    platform.start()
    platform.arrive("T01", "ST1", 0)
    platform.check_gap("T01")
    return platform


def clear_passengers(platform, train_id: str, suffix: str = "") -> None:
    """逐门区确认并收口清客（发车站台前置条件）。"""

    snapshot = platform.clearing_session(train_id)
    for zone_id in snapshot["zone_order"]:
        platform.confirm_zone(
            train_id, zone_id, f"clear-{train_id}-{zone_id}{suffix}"
        )
    platform.finalize_clearing(train_id, request_id=f"finalize-{train_id}{suffix}")
