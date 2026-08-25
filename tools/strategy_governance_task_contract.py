"""Immutable scheduler contract for the daily strategy-governance close."""
from __future__ import annotations


TASK = {
    "task_name": "动态策略治理每日更新",
    "task_type": "strategy_governance_daily",
    "group_name": "strategy_governance",
    "script_path": "tools/run_strategy_governance_daily.py",
    "script_args": "--limit 500",
    "cron_time": "22:35",
    "interval_minutes": 0,
    "date_param": "",
    "date_param_desc": "",
    "description": "每日更新策略健康、中文生命周期、单策略与组合竞技榜、分层票池和模拟资金权重；无合格策略保持现金",
    "sort_order": 218,
    "enabled": 1,
}


# Import-time validation freezes the cross-host order: the Windows-owned QMT
# announcement capture must precede Linux analysis, which must precede this
# governance close.  Runtime scheduling independently checks today's terminal
# prerequisite rows before launching either downstream task.
from tools.qmt_announcement_task_contract import validate_pipeline_order


validate_pipeline_order(governance_cron=TASK["cron_time"])
