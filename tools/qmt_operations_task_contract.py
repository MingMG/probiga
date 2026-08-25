"""Frozen production contracts for the five QMT foundation scheduler tasks."""
from __future__ import annotations


QMT_FULL_HISTORY_STATE_ROOT = (
    "/var/lib/probiga/qmt-full-market-history"
)
QMT_FULL_HISTORY_LOCK_PATH = (
    f"{QMT_FULL_HISTORY_STATE_ROOT}/qmt-full-market-history.lock"
)
QMT_FULL_HISTORY_LOG_PATH = (
    f"{QMT_FULL_HISTORY_STATE_ROOT}/qmt-full-market-history-2024.jsonl"
)
QMT_GAP_REPAIR_STATE_ROOT = "/var/lib/probiga/qmt-local-gap-repair"
QMT_GAP_REPAIR_LOCK_PATH = (
    f"{QMT_GAP_REPAIR_STATE_ROOT}/qmt-local-gap-repair.lock"
)
QMT_DAILY_BACKFILL_LOCK_PATH = (
    f"{QMT_GAP_REPAIR_STATE_ROOT}/qmt-local-daily-backfill.lock"
)


TASKS = (
    {
        "task_name": "Guojin QMT local history gap repair execute",
        "task_type": "qmt_local_gap_repair_execute",
        "group_name": "Guojin QMT",
        "script_path": "tools/backfill_guojin_qmt_local_history.py",
        "script_args": (
            "from-gaps --gap-limit 2 --apply "
            f"--state-root {QMT_GAP_REPAIR_STATE_ROOT} "
            f"--lock-path {QMT_GAP_REPAIR_LOCK_PATH} --json"
        ),
        "cron_time": "07:05",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 90,
        "date_param": "",
        "description": (
            "After the 00:00-07:00 bulk local-history window, repair a small "
            "number of registered QMT history gaps into the local history DB; "
            "the apply lock lives in a protected persistent state root."
        ),
    },
    {
        "task_name": "国金QMT凌晨缺口扫描",
        "task_type": "qmt_nightly_reconciliation",
        "group_name": "国金QMT",
        "script_path": "tools/nightly_guojin_qmt_reconciliation.py",
        "script_args": "--scan-days 20 --json",
        "cron_time": "01:30",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 87,
        "date_param": "",
        "description": (
            "每天凌晨扫描国金QMT待写队列、最近20个交易日覆盖率和质量规则；"
            "历史缺口登记到 sys_data_gap 后续补。"
        ),
    },
    {
        "task_name": "国金QMT本地历史补数(2024起)",
        "task_type": "qmt_local_history_2024",
        "group_name": "国金QMT",
        "script_path": "tools/run_guojin_qmt_full_market_history.py",
        "script_args": (
            "--start-date 2024-01-01 --mode all --daily-batch-size 120 "
            "--minute-batch-size 80 --sleep-seconds 0.2 --stop-at 07:00 "
            f"--state-root {QMT_FULL_HISTORY_STATE_ROOT} "
            f"--lock-path {QMT_FULL_HISTORY_LOCK_PATH} "
            f"--log-path {QMT_FULL_HISTORY_LOG_PATH} --json"
        ),
        "cron_time": "00:00",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 88,
        "date_param": "",
        "description": (
            "每天00:00启动国金QMT本地历史补数，补2024年至最新交易日；"
            "07:00自然停止，次日按本地覆盖率续跑。运行锁和日志固定写入"
            "受保护的持久状态根，不写只读发布目录。"
        ),
    },
    {
        "task_name": "国金QMT基础目录增量同步",
        "task_type": "qmt_reference_incremental",
        "group_name": "国金QMT",
        "script_path": "tools/sync_guojin_qmt_reference_data.py",
        "script_args": "--skip-refresh --include-calendar --json",
        "cron_time": "03:20",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 89,
        "date_param": "",
        "description": (
            "每天凌晨同步国金QMT板块、证券基础信息、指数权重，"
            "并追加不可变QMT交易日历来源凭据。"
        ),
    },
    {
        "task_name": "国金QMT历史缺口修复队列",
        "task_type": "qmt_gap_repair_plan",
        "group_name": "国金QMT",
        "script_path": "tools/repair_guojin_qmt_gaps.py",
        "script_args": "--limit 50 --json",
        "cron_time": "02:00",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 89,
        "date_param": "",
        "description": (
            "每天凌晨列出待修复历史缺口；当前仅计划不自动拉取，"
            "避免在QMT历史下载未完全验收前误写。"
        ),
    },
)

TASKS_BY_TYPE = {
    str(task["task_type"]): task
    for task in TASKS
}
TASK_TYPES = frozenset(TASKS_BY_TYPE)


__all__ = [
    "QMT_DAILY_BACKFILL_LOCK_PATH",
    "QMT_GAP_REPAIR_LOCK_PATH",
    "QMT_GAP_REPAIR_STATE_ROOT",
    "QMT_FULL_HISTORY_LOCK_PATH",
    "QMT_FULL_HISTORY_LOG_PATH",
    "QMT_FULL_HISTORY_STATE_ROOT",
    "TASKS",
    "TASKS_BY_TYPE",
    "TASK_TYPES",
]
