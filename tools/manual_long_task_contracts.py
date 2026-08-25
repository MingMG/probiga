"""Frozen registrations for manual/API-triggerable long-running tasks.

The release/registration owner imports these definitions.  API routes only
read an existing row and submit a strict row copy to the scheduler launcher.
"""
from __future__ import annotations


COMMENTARY_WATCH_TASK = {
    "task_name": "股评配置到点评估",
    "task_type": "commentary_watch",
    "group_name": "资讯公告",
    "script_path": "tools/run_commentary_watch.py",
    "script_args": "--run-due --push --json",
    "cron_time": "00:00",
    "interval_minutes": 1,
    "enabled": 1,
    "sort_order": 96,
    "date_param": "",
    "description": "每分钟只读筛选到点且启用的股评配置，在调度任务内评估并按配置推送。",
}

JQ_MINUTE_TASK = {
    "task_name": "盘中聚宽分钟GML同步",
    "task_type": "jq_minute_gml",
    "group_name": "盘中交易",
    "script_path": "tools/sync_jq_minute_gml.py",
    "script_args": (
        "--universe latest-kline --count 3 --batch-size 200 "
        "--min-coverage 0.0 --skip-closed --json"
    ),
    "cron_time": "09:30",
    "interval_minutes": 1,
    "enabled": 1,
    "sort_order": 71,
    "date_param": "",
    "description": "交易时段每分钟从聚宽同步最新1分钟K线，写入sm_stock_minute_gml。",
}

SCREENER_TASKS = (
    {
        "task_name": "盘前生产候选榜自动交付",
        "task_type": "screener_premarket_delivery",
        "group_name": "交易决策",
        "script_path": "tools/run_screener_delivery.py",
        "script_args": "--preset capital_support --top 100 --json",
        "cron_time": "09:08",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 78,
        "date_param": "",
        "description": "盘前候选榜生成、不可变落库及调度任务内交付。",
    },
    {
        "task_name": "开盘生产融合候选榜自动交付",
        "task_type": "screener_intraday_delivery",
        "group_name": "交易决策",
        "script_path": "tools/run_screener_delivery.py",
        "script_args": "--preset intraday_sector --top 100 --json",
        "cron_time": "09:32",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 79,
        "date_param": "",
        "description": "开盘候选榜生成、不可变落库及调度任务内交付。",
    },
)

SCREENER_TASKS_BY_TYPE = {str(task["task_type"]): task for task in SCREENER_TASKS}


__all__ = [
    "COMMENTARY_WATCH_TASK",
    "JQ_MINUTE_TASK",
    "SCREENER_TASKS",
    "SCREENER_TASKS_BY_TYPE",
]
