#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Add or update the Sina hot-stock scheduler task."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text

from env_config import create_tool_engine, resolve_tool_mysql_url
from server.common.scheduler_tasks import upsert_scheduler_task

TASK_TYPE = "hot_rank_sina"


def main() -> None:
    engine = create_tool_engine(resolve_tool_mysql_url())

    payload = {
        "task_name": "新浪热股TOP100",
        "task_type": TASK_TYPE,
        "script_path": "tools/fetch_hot_rank_sina.py",
        "script_args": "",
        "cron_time": "17:16",
        "date_param": "",
        "date_param_desc": "空=当天，或 YYYY-MM-DD",
        "sort_order": 14,
        "enabled": 1,
    }
    result = upsert_scheduler_task(
        engine,
        payload,
        lookup_where="task_type = :task_type",
        lookup_params={"task_type": TASK_TYPE},
        update_exclude={"task_type"},
    )
    print(f"[OK] 新浪热股任务已{result['action']}: id={result['id']}")

    print("\n当前热门数据定时任务:")
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT task_name, task_type, cron_time, sort_order
                FROM st_scheduled_tasks
                WHERE sort_order BETWEEN 10 AND 19
                ORDER BY sort_order
                """
            )
        ).fetchall()
    for row in rows:
        print(f"  {row[3]:>2}. [{row[2]}] {row[0]} ({row[1]})")


if __name__ == "__main__":
    main()
