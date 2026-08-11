#!/usr/bin/env python3
"""Install scheduler tasks required by the V2 candidate context overlay."""
from __future__ import annotations

import sys
from pathlib import Path

from sqlalchemy import text


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.scheduler_tasks import (
    update_scheduler_tasks,
    upsert_scheduler_task,
)
from tools.env_config import create_tool_engine, load_project_env


TASK = {
    "task_name": "V2收盘决策外围市场快照",
    "task_type": "external_market_close_context",
    "group_name": "strategy_v2",
    "script_path": "tools/refresh_external_market_context.py",
    "script_args": "--attempts 3 --retry-delay-seconds 5 --json",
    "cron_time": "17:55",
    "interval_minutes": 0,
    "date_param": "",
    "date_param_desc": "",
    "description": (
        "在15:45收盘候选决策前抓取股指现货/期指、A50、外汇、商品、"
        "VIX和美债；缺失项保留UNKNOWN，不使用旧快照加分。"
    ),
    "sort_order": 109,
    "enabled": 1,
}


def main() -> int:
    load_project_env()
    engine = create_tool_engine()
    try:
        result = upsert_scheduler_task(
            engine,
            TASK,
            lookup_where="task_type = :task_type",
            lookup_params={"task_type": TASK["task_type"]},
            update_exclude={"task_type"},
        )
        close_updates = update_scheduler_tasks(
            engine,
            {"cron_time": "18:10"},
            lookup_where=(
                "task_type = :task_type AND cron_time <> :cron_time"
            ),
            lookup_params={
                "task_type": "trading_v2_close_decision",
                "cron_time": "18:10",
            },
        )
        with engine.connect() as connection:
            row = connection.execute(
                text(
                    """
                    SELECT id, task_name, task_type, script_path, script_args,
                           cron_time, enabled, sort_order
                    FROM st_scheduled_tasks
                    WHERE task_type = :task_type
                    """
                ),
                {"task_type": TASK["task_type"]},
            ).mappings().one()
        print(
            {
                "action": result["action"],
                "task": dict(row),
                "close_decision_schedule_updates": int(
                    close_updates
                ),
            }
        )
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
