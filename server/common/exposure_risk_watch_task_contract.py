"""Immutable Linux scheduler identity for targeted official-announcement risk."""
from __future__ import annotations


TASK_TYPE = "exposure_event_risk_watch"
SCRIPT_PATH = "tools/run_exposure_event_risk_watch.py"
CHECKPOINT_ROOT = "/var/lib/probiga/jobs/exposure-event-risk-watch-v1"
SCRIPT_ARGS = (
    f"--checkpoint-root {CHECKPOINT_ROOT} "
    "--lookback-days 3 --deadline-minutes 20 --batch-size 4 --json"
)

TASK = {
    "task_name": "当前暴露股票重大公告监测",
    "task_type": TASK_TYPE,
    "group_name": "资讯公告",
    "script_path": SCRIPT_PATH,
    "script_args": SCRIPT_ARGS,
    "cron_time": "00:05",
    "interval_minutes": 60,
    "enabled": 1,
    "sort_order": 32,
    "date_param": "",
    "description": (
        "全天每小时仅扫描自选、持仓及最新正式确认、可交易池；逐股查询"
        "巨潮官方公告，断点续跑后整批原子写入PIT事件账本。失败不写"
        "部分覆盖，不触发任何模拟或实盘订单。"
    ),
}


__all__ = [
    "CHECKPOINT_ROOT",
    "SCRIPT_ARGS",
    "SCRIPT_PATH",
    "TASK",
    "TASK_TYPE",
]
