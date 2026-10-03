"""命令行入口。

五个子命令覆盖日常使用：

``serve``   启动控制台 HTTP 服务，可选地在启动后探测接口；
``demo``    在本地跑一遍完整流程（到站开门 → 间隙探测 → 发车 → 批量开门 →
            门区联动 → 故障置位与解除 → 序号回绕 → 控制周期 → 清扫），用于交付
            前的真实运行检查；
``status``  打印当前状态快照；
``config``  打印生效配置；
``reset``   清掉落盘的平台计数。
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
import time

from .app import Platform, build_platform
from .clock import ManualClock, SystemClock
from .config import config_to_mapping, default_config, load_config
from .console.server import ConsoleServer, probe_endpoints
from .errors import RailError

DEFAULT_PROBE_PATHS = ("/api/status", "/api/readiness", "/api/stations", "/", "/clearing", "/alarms")


def _build(args: argparse.Namespace) -> Platform:
    config = load_config(args.config) if getattr(args, "config", None) else default_config()
    if getattr(args, "storage_root", None):
        return build_platform(config, storage_root=args.storage_root)
    return Platform(config=config)


def _dump(payload: object) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))


def _cmd_serve(args: argparse.Namespace) -> int:
    platform = _build(args)
    server = ConsoleServer(platform, host=args.host, port=args.port)
    base_url = server.start()
    payload: dict = {"listening": base_url, "routes": server.api.routes()}
    if args.probe:
        payload["probe"] = probe_endpoints(base_url, list(DEFAULT_PROBE_PATHS))
    _dump(payload)
    if args.duration > 0:
        try:
            time.sleep(args.duration)
        except KeyboardInterrupt:  # pragma: no cover - 交互路径
            pass
        server.stop()
        return 0
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:  # pragma: no cover - 交互路径
        server.stop()
    return 0


def _cmd_demo(args: argparse.Namespace) -> int:
    clock = ManualClock() if args.fast else SystemClock()
    config_path = getattr(args, "config", None)
    config = load_config(config_path) if config_path else default_config()
    storage_root = getattr(args, "storage_root", None) or tempfile.mkdtemp(prefix="psdline-demo-")
    platform = build_platform(config, clock=clock, storage_root=storage_root)

    startup = platform.start()
    arrival = platform.arrive(args.train, args.station, 0)
    position = platform.refresh_position(args.train, 12)
    gap = platform.check_gap(args.train)

    # 清客确认：到站后按门区逐块确认，最后统一收口；没收口发车会被挡下。
    clearing_session = platform.clearing_session(args.train)
    for index, zone_id in enumerate(clearing_session["zone_order"]):
        platform.confirm_zone(
            args.train, zone_id, f"clear-{args.station}-{zone_id}", confirmed_by=f"staff-{index}"
        )
    clearing_final = platform.finalize_clearing(args.train, request_id=f"finalize-{args.station}")

    queued = platform.enqueue(args.train, "cmd-1", "open", args.station)
    processed = platform.process_queue(args.train)
    swept = platform.sweep(args.station)
    fault = platform.set_fault(_first_gate(platform, args.station))
    cleared = platform.clear_fault(_first_gate(platform, args.station))
    batched = platform.batch_open(args.station)
    renumber = platform.renumber(args.station, _mirror_mapping(platform, args.station))
    sequence = platform.sequence_verdict(65534, 4)
    dwell = platform.register_dwell(args.train, 45.0, args.station)
    route = platform.assign_route(args.train, args.station, _next_station(platform, args.station), 1200)
    departure = platform.depart(args.train, args.station)
    cycles = [platform.cycle(dt=args.dt) for _ in range(args.cycles)]
    final_sweep = platform.sweep(args.station)

    # 演示自检用临时端口，避免和本机常驻服务抢 8080。
    server = ConsoleServer(platform, port=0)
    base_url = server.start()
    probe = probe_endpoints(base_url, list(DEFAULT_PROBE_PATHS))
    server.stop()

    report = {
        "storage_root": storage_root,
        "startup": startup,
        "arrival": {key: arrival[key] for key in ("ok", "position_m", "violations")},
        "position": position,
        "gap": gap,
        "clearing": {
            "session": clearing_session["session_id"],
            "zones": len(clearing_session["zones"]),
            "finalized": clearing_final["stage"],
            "decision": clearing_final["verdict"]["decision"],
            "digest": clearing_final["finalize_digest"],
        },
        "queued": {key: queued[key] for key in ("ok", "train_id")},
        "processed": processed,
        "swept": swept,
        "batch": {key: batched[key] for key in ("ok", "order", "opened", "nearest")},
        "renumber": {key: renumber[key] for key in ("ok", "revision", "active_group")},
        "fault": fault,
        "cleared": cleared,
        "sequence": sequence,
        "dwell": dwell,
        "route": route,
        "departure": departure,
        "cycles_run": len(cycles),
        "last_cycle": cycles[-1],
        "final_sweep": {key: final_sweep[key] for key in ("closed", "scanned")},
        "probe": probe,
        "status": platform.status(),
    }
    _dump(report)

    failures = [name for name, item in probe.items() if not item["ok"]]
    for name, operation in (
        ("arrival", arrival),
        ("processed", processed),
        ("renumber", renumber),
        ("cleared", cleared),
        ("sequence", sequence),
        ("departure", departure),
    ):
        if not operation.get("ok"):
            failures.extend(f"{name}:{item}" for item in operation.get("violations", []))
    if clearing_final["verdict"]["decision"] != "go":
        failures.append("clearing:finalize-not-go")
    if failures:
        print(f"检查未通过: {failures}", file=sys.stderr)
        return 1
    return 0


def _mirror_mapping(platform: Platform, station_id: str) -> dict[str, str]:
    """给演示重排构造一份反向映射。"""

    groups = sorted(platform.station_view(station_id)["groups"])
    rotated = groups[1:] + groups[:1]
    return {key: value for key, value in zip(groups, rotated)}


def _first_gate(platform: Platform, station_id: str) -> str:
    """返回演示用的第一樘站台门。"""

    gates = platform.gates.by_station(station_id)
    return gates[0].id if gates else ""


def _next_station(platform: Platform, station_id: str) -> str:
    """返回线路上下一个站台，用于演示交路。"""

    stations = platform.namespace.station_ids()
    if len(stations) < 2:
        return station_id
    index = stations.index(station_id) if station_id in stations else 0
    return stations[(index + 1) % len(stations)]


def _cmd_clear_demo(args: argparse.Namespace) -> int:
    """清客确认专项演示：正常收口、闯回挂起、超时与幂等重放。"""

    import threading

    clock = ManualClock()
    storage_root = getattr(args, "storage_root", None) or tempfile.mkdtemp(prefix="psdline-clear-")
    config = load_config(args.config) if getattr(args, "config", None) else default_config()
    from dataclasses import replace

    from .config import ClearingSection

    config = replace(
        config,
        clearing=ClearingSection(confirm_ttl_s=args.ttl, timeout_policy=args.policy),
    )
    platform = build_platform(config, clock=clock, storage_root=storage_root)
    platform.start()
    log: list[dict] = []

    def step(name: str, payload: object) -> None:
        log.append({"step": name, "payload": payload})
        print(f"[{name}] {json.dumps(payload, ensure_ascii=False, default=str)}")

    step("startup", platform.start())
    step("arrive", _keep(platform.arrive(args.train, args.station, 0),
                         ("ok", "visit", "clearing_session")))
    platform.check_gap(args.train)

    # 1) 没收口：发车被挡
    held = platform.depart(args.train, args.station)
    step("depart_held_before_finalize",
         {"ok": held["ok"], "violations": held["violations"],
          "decision": held["clearing"]["decision"]})

    # 2) 站务并发（多线程）逐块确认
    snapshot = platform.clearing_session(args.train)
    errors: list[str] = []

    def confirm(zone: str, worker: int) -> None:
        try:
            platform.confirm_zone(args.train, zone, f"demo-{zone}", confirmed_by=f"staff-{worker}")
        except RailError as error:
            errors.append(error.message)

    threads = [
        threading.Thread(target=confirm, args=(zone, index))
        for index, zone in enumerate(snapshot["zone_order"])
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    step("concurrent_confirm", {"zones": snapshot["zone_order"], "errors": errors})

    # 3) 断线重连：同一批 request_id 原样重发，全部幂等
    replayed = [
        platform.confirm_zone(args.train, zone, f"demo-{zone}")["replayed"]
        for zone in snapshot["zone_order"]
    ]
    step("reconnect_replay", {"all_replayed": all(replayed), "detail": replayed})

    # 4) 收口
    finalized = platform.finalize_clearing(args.train, request_id="demo-finalize")
    step("finalize", {"stage": finalized["stage"], "decision": finalized["verdict"]["decision"],
                      "digest": finalized["finalize_digest"]})

    # 5) 收口后有人闯回 G02：结论立即作废、发车被挡
    intrusion = platform.report_intrusion(args.train, "demo-intrusion", zone_ids=["G02"])
    step("intrusion", {"suspended": intrusion["suspended"],
                       "decision": intrusion["verdict"]["decision"],
                       "missing": intrusion["missing"]})
    held2 = platform.depart(args.train, args.station)
    step("depart_held_after_intrusion", {"ok": held2["ok"], "violations": held2["violations"]})

    # 6) 处置：解除挂起 → 重确认 G02 → 重新收口
    platform.resolve_intrusion(args.train, "demo-resolve")
    platform.confirm_zone(args.train, "G02", "demo-G02-recheck")
    finalized2 = platform.finalize_clearing(args.train, request_id="demo-finalize-2")
    step("recover", {"decision": finalized2["verdict"]["decision"],
                     "stage": finalized2["stage"]})

    # 7) 发车：三处对账
    departure = platform.depart(args.train, args.station)
    step("depart", {"ok": departure["ok"], "reconciled": departure.get("reconciled"),
                    "clearing": departure.get("clearing"), "violations": departure["violations"]})

    # 8) 哈希链校验
    step("verify_chain", platform.verify_clearing(args.train))

    # 9) 重启重建，结论一致
    rebuilt = build_platform(config, clock=clock, storage_root=storage_root)
    verdict = rebuilt.clearing_verdict(args.train)
    step("restart_verdict", {"stage": verdict["stage"], "decision": verdict["decision"],
                             "reasons": list(verdict["reasons"])})

    ok = (not held["ok"]) and all(replayed) and (not held2["ok"]) and departure["ok"] \
        and departure.get("reconciled") and verdict["stage"] == "dispatched"
    print(json.dumps({"ok": ok, "storage_root": storage_root}, ensure_ascii=False))
    return 0 if ok else 1


def _keep(payload: dict, keys: tuple[str, ...]) -> dict:
    return {key: payload.get(key) for key in keys}


def _cmd_status(args: argparse.Namespace) -> int:
    platform = _build(args)
    payload = platform.status()
    payload["groups"] = platform.snapshot()["groups"]
    _dump(payload)
    return 0


def _cmd_config(args: argparse.Namespace) -> int:
    config_path = getattr(args, "config", None)
    config = load_config(config_path) if config_path else default_config()
    _dump(config_to_mapping(config))
    return 0


def _cmd_reset(args: argparse.Namespace) -> int:
    platform = _build(args)
    removed = platform.reset_state()
    _dump({"storage_root": platform.store.root, "platform_state_removed": removed})
    return 0


def build_parser() -> argparse.ArgumentParser:
    """构造命令行解析器。"""

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--config", help="JSON 配置文件路径", default=argparse.SUPPRESS)
    common.add_argument("--storage-root", help="覆盖状态存储目录", default=argparse.SUPPRESS)
    parser = argparse.ArgumentParser(
        prog="psdline", description="作业顺序与联锁控制服务", parents=[common]
    )
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="启动控制台服务", parents=[common])
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)
    serve.add_argument("--duration", type=float, default=0.0, help="运行多少秒后退出，0 表示常驻")
    serve.add_argument("--probe", action="store_true", help="启动后探测接口")
    serve.set_defaults(func=_cmd_serve)

    demo = sub.add_parser("demo", help="跑一遍完整流程", parents=[common])
    demo.add_argument("--train", default="T01", help="演示用的列车标识")
    demo.add_argument("--station", default="ST1", help="演示用的站台标识")
    demo.add_argument("--cycles", type=int, default=120, help="演示控制周期数")
    demo.add_argument("--dt", type=float, default=1.0, help="控制周期步长（秒）")
    demo.add_argument("--fast", action="store_true", help="使用可推进时钟，跳过真实等待")
    demo.set_defaults(func=_cmd_demo)

    clear_demo = sub.add_parser("clear-demo", help="跑一遍清客确认专项流程", parents=[common])
    clear_demo.add_argument("--train", default="T01", help="演示用的列车标识")
    clear_demo.add_argument("--station", default="ST1", help="演示用的站台标识")
    clear_demo.add_argument("--ttl", type=float, default=120.0, help="确认有效期（秒）")
    clear_demo.add_argument("--policy", choices=("preserve", "reset"), default="preserve",
                            help="确认超时策略")
    clear_demo.set_defaults(func=_cmd_clear_demo)

    status = sub.add_parser("status", help="打印状态快照", parents=[common])
    status.set_defaults(func=_cmd_status)

    config = sub.add_parser("config", help="打印生效配置", parents=[common])
    config.set_defaults(func=_cmd_config)

    reset = sub.add_parser("reset", help="清掉落盘的平台计数", parents=[common])
    reset.set_defaults(func=_cmd_reset)
    return parser


def main(argv: list[str] | None = None) -> int:
    """命令行主入口。"""

    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="backslashreplace")
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except RailError as error:
        print(json.dumps(error.as_record(), ensure_ascii=False), file=sys.stderr)
        return 2
