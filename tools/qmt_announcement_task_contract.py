"""Frozen production scheduler contract for full-market QMT announcements."""
from __future__ import annotations


QMT_ANNOUNCEMENT_CHECKPOINT_DIR = (
    "/var/lib/probiga/qmt-announcement-checkpoints"
)


TASK = {
    "task_name": "国金QMT全市场公告PIT同步",
    "task_type": "qmt_announcement_pit",
    "group_name": "strategy_governance",
    "script_path": "tools/sync_qmt_announcement_pit.py",
    "script_args": (
        "--window-days 30 --batch-size 100 "
        f"--checkpoint-dir {QMT_ANNOUNCEMENT_CHECKPOINT_DIR}"
    ),
    "cron_time": "18:20",
    "interval_minutes": 0,
    "date_param": "",
    "date_param_desc": "",
    "description": (
        "盘后按不可变QMT股票目录全市场下载announcement，"
        "同一事实截止、缺一股即DATA_BLOCKED，18:50策略分析不得回退东财"
    ),
    "sort_order": 89,
    "enabled": 1,
}


def _minutes(value: str) -> int:
    hour, minute = (int(item) for item in str(value).split(":"))
    if not 0 <= hour <= 23 or not 0 <= minute <= 59:
        raise ValueError("scheduler clock is invalid")
    return hour * 60 + minute


def validate_pipeline_order(
    *, analysis_cron: str = "18:50", governance_cron: str = "22:35"
) -> dict[str, int]:
    event = _minutes(TASK["cron_time"])
    analysis = _minutes(analysis_cron)
    governance = _minutes(governance_cron)
    if not event < analysis < governance:
        raise ValueError(
            "QMT announcement -> analysis -> governance order is invalid"
        )
    if analysis - event > 30:
        raise ValueError(
            "analysis starts outside the 30-minute QMT fact-cutoff bound"
        )
    return {
        "qmt_announcement_minutes": event,
        "analysis_minutes": analysis,
        "governance_minutes": governance,
    }


validate_pipeline_order()


__all__ = [
    "QMT_ANNOUNCEMENT_CHECKPOINT_DIR",
    "TASK",
    "validate_pipeline_order",
]
