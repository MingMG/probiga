#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Install the isolated V2 decision, paper-tick and reconciliation tasks."""
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
        "task_name": "V2盘前组合计划",
        "task_type": "trading_v2_premarket_decision",
        "group_name": "strategy_v2",
        "script_path": "tools/run_trading_v2_decision.py",
        "script_args": "--mode premarket",
        "cron_time": "09:20",
        "interval_minutes": 0,
        "date_param": "",
        "date_param_desc": "",
        "description": "Worker生成不可变V2决策快照；GET接口不会现场计算",
        "sort_order": 110,
        "enabled": 0,
    },
    {
        "task_name": "V2公共行情自动替补",
        "task_type": "public_quote_failover",
        "group_name": "strategy_v2",
        "script_path": "tools/run_public_quote_failover.py",
        "script_args": "",
        "cron_time": "",
        "interval_minutes": 1,
        "date_param": "",
        "date_param_desc": "",
        "description": (
            "QMT主源不健康时抓取新浪与腾讯；"
            "至少双源一致且全市场质量达标才供ProBigA模拟盘使用"
        ),
        "sort_order": 111,
        "enabled": 1,
    },
    {
        "task_name": "V2盘中观察池动态激活",
        "task_type": "trading_v2_intraday_activation",
        "group_name": "strategy_v2",
        "script_path": "tools/run_trading_v2_intraday_activation.py",
        "script_args": "",
        "cron_time": "",
        "interval_minutes": 1,
        "date_param": "",
        "date_param_desc": "",
        "description": (
            "每分钟检查盘前池、全市场水下修复、爆量上攻和极速冲板；"
            "报警不依赖全天分钟完整，模拟买入必须通过量价、板块和盈亏比"
        ),
        "sort_order": 112,
        "enabled": 0,
    },
    {
        "task_name": "V2模拟撮合Tick",
        "task_type": "trading_v2_paper_tick",
        "group_name": "strategy_v2",
        "script_path": "tools/run_trading_v2_paper_tick.py",
        "script_args": "",
        "cron_time": "",
        "interval_minutes": 1,
        "date_param": "",
        "date_param_desc": "",
        "description": "ProBigA内部模拟盘；优先QMT Level1，缺失时使用新鲜QMT快照和冻结滑点",
        "sort_order": 113,
        "enabled": 1,
    },
    {
        "task_name": "V2日终账本对账",
        "task_type": "trading_v2_reconciliation",
        "group_name": "strategy_v2",
        "script_path": "tools/run_trading_v2_reconciliation.py",
        "script_args": "",
        "cron_time": "15:10",
        "interval_minutes": 0,
        "date_param": "",
        "date_param_desc": "",
        "description": "现金、股数、订单与成交逐项对账；失败即冻结新仓",
        "sort_order": 114,
        "enabled": 1,
    },
    {
        "task_name": "V2收盘候选与次日基础状态",
        "task_type": "trading_v2_close_decision",
        "group_name": "strategy_v2",
        "script_path": "tools/run_trading_v2_decision.py",
        "script_args": "--mode close",
        "cron_time": "15:45",
        "interval_minutes": 0,
        "date_param": "",
        "date_param_desc": "",
        "description": (
            "等待国金QMT当日日K逐行补证和概念行业成员快照完成后，"
            "生成下一交易日研究候选和基础市场状态"
        ),
        "sort_order": 115,
        "enabled": 0,
    },
    {
        "task_name": "V2异步任务Worker",
        "task_type": "trading_v2_job_worker",
        "group_name": "strategy_v2",
        "script_path": "tools/run_trading_v2_job_worker.py",
        "script_args": "",
        "cron_time": "",
        "interval_minutes": 1,
        "date_param": "",
        "date_param_desc": "",
        "description": "串行消费回测和决策任务；使用幂等键且不会加载真实交易",
        "sort_order": 116,
        "enabled": 1,
    },
    {
        "task_name": "V2 Level1连续性验收",
        "task_type": "trading_v2_level1_validation",
        "group_name": "strategy_v2",
        "script_path": "tools/validate_trading_v2_level1.py",
        "script_args": "",
        "cron_time": "15:12",
        "interval_minutes": 0,
        "date_param": "",
        "date_param_desc": "",
        "description": "按五个完整交易日、分钟覆盖、价量完整率和延迟清除或保持B-003",
        "sort_order": 117,
        "enabled": 1,
    },
    {
        "task_name": "V2策略健康与自动暂停",
        "task_type": "trading_v2_strategy_health",
        "group_name": "strategy_v2",
        "script_path": "tools/run_trading_v2_health.py",
        "script_args": "",
        "cron_time": "15:50",
        "interval_minutes": 0,
        "date_param": "",
        "date_param_desc": "",
        "description": "计算20/60/120日健康；60日RED策略自动进入SUSPENDED且不得自动恢复",
        "sort_order": 118,
        "enabled": 1,
    },
)


def main() -> int:
    load_project_env()
    engine = create_tool_engine()
    results = []
    try:
        for task in TASKS:
            results.append(
                {
                    **upsert_scheduler_task(
                        engine,
                        task,
                        lookup_where="task_type = :task_type",
                        lookup_params={"task_type": task["task_type"]},
                        update_exclude={"task_type"},
                    ),
                    "task_type": task["task_type"],
                }
            )
    finally:
        engine.dispose()
    print(json.dumps({"status": "ok", "tasks": results}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
