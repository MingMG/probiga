#!/usr/bin/env python3
"""Install the production V3 decision, review and counterfactual tasks."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.scheduler_tasks import upsert_scheduler_task
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
        "task_name": "V3漏抓反事实复盘",
        "task_type": "trading_v3_counterfactual_audit",
        "group_name": "strategy_v3",
        "script_path": "tools/run_trading_v3_counterfactual.py",
        "script_args": "",
        "cron_time": "16:30",
        "interval_minutes": 0,
        "date_param": "",
        "date_param_desc": "",
        "description": "记录漏抓与误触发结果，作为后续校准证据，不产生订单",
        "sort_order": 132,
        "enabled": 1,
    },
)


def main() -> int:
    load_project_env()
    engine = create_tool_engine()
    results = []
    try:
        for task in TASKS:
            results.append({
                **upsert_scheduler_task(
                    engine,
                    task,
                    lookup_where="task_type = :task_type",
                    lookup_params={"task_type": task["task_type"]},
                    update_exclude={"task_type"},
                ),
                "task_type": task["task_type"],
            })
    finally:
        engine.dispose()
    print(json.dumps({"status": "ok", "tasks": results}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
