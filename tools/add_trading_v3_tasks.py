#!/usr/bin/env python3
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
        "task_name": "V3 收盘正期望组合决策",
        "task_type": "trading_v3_close_decision",
        "group_name": "strategy_v3",
        "script_path": "tools/run_trading_v3_decision.py",
        "script_args": (
            "--mode close --universe-limit 5000 "
            "--per-sleeve-limit 5000"
        ),
        "cron_time": "16:05",
        "interval_minutes": 0,
        "date_param": "",
        "date_param_desc": "",
        "description": (
            "基于已收盘 QMT 日K、概念快照、财务和公告数据，生成七策略"
            "独立预测、概率市场状态和扣费后组合；未通过样本外校准不得下单"
        ),
        "sort_order": 210,
        "enabled": 1,
    },
    {
        "task_name": "V3 盘前组合复核",
        "task_type": "trading_v3_premarket_review",
        "group_name": "strategy_v3",
        "script_path": "tools/run_trading_v3_decision.py",
        "script_args": (
            "--mode premarket --universe-limit 5000 "
            "--per-sleeve-limit 5000"
        ),
        "cron_time": "09:15",
        "interval_minutes": 0,
        "date_param": "",
        "date_param_desc": "",
        "description": (
            "开盘前用最新可得事实复核组合，仍然只允许 ProBigA 模拟盘，"
            "真实交易开关固定关闭"
        ),
        "sort_order": 211,
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
            "在预测期限成熟后记录所有接受和拒绝样本的真实净收益、"
            "Recall@20/50、误杀原因和校准偏差"
        ),
        "sort_order": 212,
        "enabled": 1,
    },
)


def main() -> int:
    load_project_env()
    engine = create_tool_engine()
    results = []
    try:
        for task in TASKS:
            result = upsert_scheduler_task(
                engine,
                task,
                lookup_where="task_type = :task_type",
                lookup_params={"task_type": task["task_type"]},
                update_exclude={"task_type"},
            )
            results.append({**result, "task_type": task["task_type"]})
    finally:
        engine.dispose()
    print(json.dumps(
        {"status": "ok", "tasks": results},
        ensure_ascii=False,
        indent=2,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
