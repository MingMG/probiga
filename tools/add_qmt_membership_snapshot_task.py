#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Install the local post-close BigQMT membership snapshot task."""
from __future__ import annotations

import sys
from pathlib import Path

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.scheduler_tasks import upsert_scheduler_task
from tools.env_config import create_tool_engine, load_project_env

TASK_TYPE = "qmt_membership_snapshot"
SCRIPT_PATH = "tools/sync_bigqmt_reference.py"


def main() -> int:
    load_project_env()
    engine = create_tool_engine()
    try:
        payload = {
            "task_name": "QMT概念行业成员收盘快照",
            "task_type": TASK_TYPE,
            "group_name": "qmt_reference",
            "script_path": SCRIPT_PATH,
            "script_args": (
                "--apply --force-reference-refresh "
                "--promote-production --json"
            ),
            "cron_time": "15:12",
            "interval_minutes": 0,
            "date_param": "",
            "date_param_desc": "",
            "description": (
                "国金BigQMT全量校验后追加不可变概念/行业日快照，"
                "并将同一哈希快照自动发布到生产决策库"
            ),
            "sort_order": 96,
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
        print({"action": installed["action"], "task": dict(row)})
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
