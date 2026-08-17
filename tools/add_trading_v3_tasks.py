#!/usr/bin/env python3
"""Install the production V3 decision, review and counterfactual tasks."""
from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Sequence
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.scheduler_authority import LAYER4_WRITER_TASK_TYPES
from server.common.scheduler_tasks import (
    read_fresh_scheduler_writers,
    set_scheduler_tasks_enabled_atomically,
    upsert_scheduler_task,
)
from tools.env_config import create_tool_engine, load_project_env


TASKS = (
    {
        "task_name": "V3收盘正期望决策",
        "task_type": "trading_v3_close_decision",
        "group_name": "strategy_v3",
        "script_path": "tools/run_trading_v3_decision.py",
        "script_args": (
            "--mode close --universe-limit 1200 "
            "--per-sleeve-limit 300"
        ),
        "cron_time": "22:05",
        "interval_minutes": 0,
        "date_param": "",
        "date_param_desc": "",
        "description": "收盘后等待日线、题材、资金、公告与QMT成员快照落库，再生成V3/V4/V5/V6模拟组合并推送早报机器人",
        "sort_order": 130,
        "enabled": 1,
    },
    {
        "task_name": "V3盘前组合复核",
        "task_type": "trading_v3_premarket_review",
        "group_name": "strategy_v3",
        "script_path": "tools/run_trading_v3_decision.py",
        "script_args": (
            "--mode premarket --universe-limit 1200 "
            "--per-sleeve-limit 300"
        ),
        "cron_time": "09:15",
        "interval_minutes": 0,
        "date_param": "",
        "date_param_desc": "",
        "description": "开盘前复核最新完整交易日数据；真实交易始终关闭",
        "sort_order": 131,
        "enabled": 1,
    },
    {
        "task_name": "V3 拒绝样本与漏抓审计",
        "task_type": "trading_v3_counterfactual_audit",
        "group_name": "strategy_v3",
        "script_path": "tools/run_trading_v3_counterfactual.py",
        "script_args": "--limit 10000 --max-batches 10",
        "cron_time": "16:30",
        "interval_minutes": 0,
        "date_param": "",
        "date_param_desc": "",
        "description": (
            "先维护旧拒绝样本诊断，再按冻结 T+1/T+5/T+20 合同生成"
            "独立 outcome ledger、反事实学习与持续校准门禁；全程 Shadow、"
            "失败即任务失败且绝不授予订单权限"
        ),
        "sort_order": 212,
        "enabled": 1,
    },
    {
        "task_name": "V3 多周期持续校准与 Shadow 发布",
        "task_type": "trading_v3_continuous_calibration",
        "group_name": "strategy_v3",
        "script_path": "tools/run_trading_v3_continuous_calibration.py",
        "script_args": (
            "--lock-timeout-seconds 0 "
            "--training-timeout-seconds 19800"
        ),
        "cron_time": "17:10",
        "interval_minutes": 0,
        "date_param": "",
        "date_param_desc": "",
        "description": (
            "发现/深验 T+1/T+5/T+20 JSON 模型；缺失或过期时用全 A 股"
            "点时数据触发真实训练，按新增 forward outcome 做受控刷新，写入"
            "不可变证据并执行 Shadow 注册、持续门禁与自动降级；无外部签名"
            "和 EXECUTABLE_VERIFIED 证据时保持 COLLECTING 且绝不授予订单权限"
        ),
        "sort_order": 213,
        "enabled": 1,
    },
)

WRITER_QUIESCENCE_BLOCK_EXIT_CODE = 3


def deployment_tasks(*, activate_layer4: bool) -> tuple[dict, ...]:
    """Return task definitions with the durable Layer-4 writer fence applied."""

    result = []
    for definition in TASKS:
        task = dict(definition)
        if task["task_type"] in LAYER4_WRITER_TASK_TYPES:
            task["enabled"] = 1 if activate_layer4 else 0
        result.append(task)
    return tuple(result)


def layer4_activation_preconditions(engine) -> dict:
    """Deep-check all three migrations before any Layer-4 writer is enabled."""

    # Lazy import avoids a module cycle: readiness reads TASKS to derive the
    # canonical post-activation task definitions.
    from tools.trading_v3_fourth_layer_readiness import (
        collect_migration_readiness,
    )

    return collect_migration_readiness(engine)


def activate_layer4_writers_atomically(engine) -> int:
    """Lift the two-row writer fence in one transaction or not at all."""

    task_types = tuple(LAYER4_WRITER_TASK_TYPES)
    if len(task_types) != 2:
        raise RuntimeError("Layer-4 writer contract must contain exactly two tasks")
    return set_scheduler_tasks_enabled_atomically(
        engine,
        task_types,
        enabled=True,
        expected_row_count=len(task_types),
    )


def enforce_layer4_writer_fence_atomically(engine) -> int:
    """Disable every matching writer row, including accidental duplicates."""

    return set_scheduler_tasks_enabled_atomically(
        engine,
        tuple(LAYER4_WRITER_TASK_TYPES),
        enabled=False,
    )


def wait_for_scheduler_writer_quiescence(
    engine,
    *,
    timeout_seconds: float,
    poll_seconds: float,
    monotonic=time.monotonic,
    sleep=time.sleep,
) -> tuple[dict, ...]:
    """Wait for stopped remote heartbeats to age out, then fail on survivors."""

    if timeout_seconds < 0:
        raise ValueError("timeout_seconds must be non-negative")
    if poll_seconds <= 0:
        raise ValueError("poll_seconds must be positive")
    deadline = monotonic() + timeout_seconds
    while True:
        live_writers = tuple(read_fresh_scheduler_writers(engine))
        if not live_writers:
            return ()
        remaining = deadline - monotonic()
        if remaining <= 0:
            return live_writers
        sleep(min(poll_seconds, remaining))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Deploy Trading V3 scheduler tasks with Layer-4 writers fenced "
            "unless an explicitly verified activation is requested."
        )
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--writer-fence",
        action="store_true",
        help=(
            "persist counterfactual/continuous tasks as disabled (default)"
        ),
    )
    mode.add_argument(
        "--activate-layer4",
        action="store_true",
        help=(
            "enable Layer-4 writers only after all migration ledgers, "
            "progress rows and schemas verify"
        ),
    )
    parser.add_argument(
        "--require-no-live-scheduler-writers",
        action="store_true",
        help=(
            "after persisting the writer fence, require every shared scheduler "
            "heartbeat to be stale before task definitions are staged"
        ),
    )
    parser.add_argument(
        "--writer-drain-timeout-seconds",
        type=float,
        default=0.0,
        help="maximum time to wait for stopped scheduler heartbeats to expire",
    )
    parser.add_argument(
        "--writer-drain-poll-seconds",
        type=float,
        default=5.0,
        help="shared heartbeat polling interval while draining writers",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    activate_layer4 = bool(args.activate_layer4)
    load_project_env()
    engine = create_tool_engine()
    results = []
    fenced_row_count = 0
    preconditions: dict = {
        "checked": False,
        "ready": None,
        "reason_codes": [],
    }
    writer_quiescence: dict = {
        "checked": False,
        "ready": None,
        "live_writers": [],
    }
    try:
        # Fence every matching row before validation or definition upserts.
        # This also neutralizes duplicate legacy rows and makes an accidental
        # early activation request fail safe instead of leaving old writers on.
        fenced_row_count = enforce_layer4_writer_fence_atomically(engine)
        if args.require_no_live_scheduler_writers:
            try:
                live_writers = wait_for_scheduler_writer_quiescence(
                    engine,
                    timeout_seconds=args.writer_drain_timeout_seconds,
                    poll_seconds=args.writer_drain_poll_seconds,
                )
            except Exception as exc:
                writer_quiescence = {
                    "checked": True,
                    "ready": False,
                    "reason_codes": [
                        "SCHEDULER_WRITER_QUIESCENCE_UNVERIFIED"
                    ],
                    "error": f"{type(exc).__name__}: {exc}"[:500],
                    "live_writers": [],
                }
                print(json.dumps(
                    {
                        "status": "blocked",
                        "mode": (
                            "activate-layer4"
                            if activate_layer4
                            else "writer-fence"
                        ),
                        "writer_fence_active": True,
                        "fenced_row_count": fenced_row_count,
                        "layer4_writers_enabled": False,
                        "writer_quiescence": writer_quiescence,
                        "migration_readiness": preconditions,
                        "tasks": [],
                    },
                    ensure_ascii=False,
                    indent=2,
                    default=str,
                ))
                return WRITER_QUIESCENCE_BLOCK_EXIT_CODE
            writer_quiescence = {
                "checked": True,
                "ready": not live_writers,
                "reason_codes": (
                    [] if not live_writers else ["SCHEDULER_LIVE_WRITERS_REMAIN"]
                ),
                "live_writers": list(live_writers),
            }
            if live_writers:
                print(json.dumps(
                    {
                        "status": "blocked",
                        "mode": (
                            "activate-layer4"
                            if activate_layer4
                            else "writer-fence"
                        ),
                        "writer_fence_active": True,
                        "fenced_row_count": fenced_row_count,
                        "layer4_writers_enabled": False,
                        "writer_quiescence": writer_quiescence,
                        "migration_readiness": preconditions,
                        "tasks": [],
                    },
                    ensure_ascii=False,
                    indent=2,
                    default=str,
                ))
                return WRITER_QUIESCENCE_BLOCK_EXIT_CODE
        if activate_layer4:
            preconditions = {
                **layer4_activation_preconditions(engine),
                "checked": True,
            }
            if preconditions.get("ready") is not True:
                print(json.dumps(
                    {
                        "status": "blocked",
                        "mode": "activate-layer4",
                        "writer_fence_active": True,
                        "fenced_row_count": fenced_row_count,
                        "layer4_writers_enabled": False,
                        "writer_quiescence": writer_quiescence,
                        "migration_readiness": preconditions,
                        "tasks": [],
                    },
                    ensure_ascii=False,
                    indent=2,
                    default=str,
                ))
                return 2
        # Always stage exact definitions behind the durable fence first.  The
        # explicit activation path lifts both rows atomically only after every
        # upsert succeeds, so a crash cannot leave one writer live by itself.
        for task in deployment_tasks(activate_layer4=False):
            result = upsert_scheduler_task(
                engine,
                task,
                lookup_where="task_type = :task_type",
                lookup_params={"task_type": task["task_type"]},
                update_exclude={"task_type"},
            )
            results.append({**result, "task_type": task["task_type"]})
        if activate_layer4:
            activate_layer4_writers_atomically(engine)
    finally:
        engine.dispose()
    print(json.dumps(
        {
            "status": "ok",
            "mode": (
                "activate-layer4" if activate_layer4 else "writer-fence"
            ),
            "writer_fence_active": not activate_layer4,
            "fenced_row_count": fenced_row_count,
            "layer4_writers_enabled": activate_layer4,
            "writer_quiescence": writer_quiescence,
            "migration_readiness": preconditions,
            "tasks": results,
        },
        ensure_ascii=False,
        indent=2,
        default=str,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
