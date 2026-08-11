#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Add or update the intraday simulated-trading scheduler task."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text

from env_config import create_tool_engine, resolve_tool_mysql_url
from server.common.scheduler_tasks import upsert_scheduler_task

SCRIPT_PATH = "biz/analysis/sync_sim_trade.py"


def main() -> None:
    engine = create_tool_engine(resolve_tool_mysql_url())

    payload = {
        "task_name": "模拟交易扫描(1min)",
        "task_type": "sim_trade",
        "script_path": SCRIPT_PATH,
        "script_args": "",
        "cron_time": "09:31",
        "interval_minutes": 1,
        "date_param": "",
        "date_param_desc": "",
        "sort_order": 99,
        "enabled": 1,
    }
    result = upsert_scheduler_task(
        engine,
        payload,
        lookup_where="script_path = :script_path",
        lookup_params={"script_path": SCRIPT_PATH},
        update_exclude={"script_path"},
    )
    print(f"[OK] 模拟交易任务已{result['action']}: id={result['id']}")

    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT id, task_name, interval_minutes, enabled
                FROM st_scheduled_tasks
                WHERE script_path = :script_path
                """
            ),
            {"script_path": SCRIPT_PATH},
        ).fetchone()
    if row:
        print(f"  ID: {row[0]}, 名称: {row[1]}, 间隔: {row[2]}分钟, 启用: {row[3]}")


if __name__ == "__main__":
    main()
