#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Install the validated ETF sync and forward-ledger scheduler task."""
from __future__ import annotations

import sys
from pathlib import Path

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.scheduler_tasks import upsert_scheduler_task
from tools.env_config import create_tool_engine, load_project_env

TASK_TYPE = "etf_forward_daily"
SCRIPT_PATH = "tools/run_etf_forward_daily.py"


def main() -> int:
    load_project_env()
    engine = create_tool_engine()
    try:
        payload = {
            "task_name": "ETF验证行情与冻结策略前向记录",
            "task_type": TASK_TYPE,
            "group_name": "strategy",
            "script_path": SCRIPT_PATH,
            "script_args": "--execute",
            "cron_time": "15:20",
            "interval_minutes": 0,
            "date_param": "",
            "date_param_desc": "",
            "description": (
                "QMT ETF cross-validation sync followed by append-only "
                "forward research ledger; never submits orders"
            ),
            "sort_order": 95,
            "enabled": 1,
        }
        installed = upsert_scheduler_task(
            engine,
            payload,
            lookup_where="task_type = :task_type",
            lookup_params={"task_type": TASK_TYPE},
            update_exclude={"task_type"},
        )
        with engine.connect() as connection:
            row = connection.execute(
                text(
                    """
                    SELECT id, task_name, task_type, script_path,
                           script_args, cron_time, enabled, sort_order
                      FROM st_scheduled_tasks
                     WHERE task_type = :task_type
                    """
                ),
                {"task_type": TASK_TYPE},
            ).mappings().one()
        print(
            {
                "action": installed["action"],
                "task": dict(row),
            }
        )
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
