"""Frozen cross-host ownership for production QMT scheduler tasks.

Task names such as ``stock_kline`` historically selected their provider from
ambient configuration.  They cannot prove whether Linux or the signed-in QMT
desktop is capable of executing them, so they are explicitly quarantined until
they are replaced by provider-specific task identities.
"""
from __future__ import annotations

from tools.qmt_announcement_task_contract import TASK as QMT_ANNOUNCEMENT_TASK
from tools.qmt_operations_task_contract import TASKS_BY_TYPE


QMT_CATALOG_CAPABILITY_TASK = {
    "task_name": "Guojin QMT API catalog capability refresh",
    "task_type": "qmt_catalog_capability_refresh",
    "group_name": "Guojin QMT",
    "script_path": "tools/setup_guojin_qmt_catalog.py",
    "script_args": "",
    "cron_time": "01:10",
    "interval_minutes": 0,
    "enabled": 1,
    "sort_order": 86,
    "date_param": "",
    "description": (
        "Validate the privileged-installed QMT API registry, refresh the "
        "capability ledger, and record unverified probes explicitly."
    ),
}

QMT_INTRADAY_REALTIME_TASK = {
    "task_name": "国金QMT盘中实时行情同步",
    "task_type": "qmt_intraday_realtime",
    "group_name": "国金QMT",
    "script_path": "tools/sync_qmt_realtime.py",
    "script_args": "--min-coverage 0.60 --no-archive-snapshot --json",
    "cron_time": "09:25",
    "interval_minutes": 1,
    "enabled": 1,
    "sort_order": 71,
    "date_param": "",
    "description": (
        "国金QMT独立实时行情通道；写入 sm_stock_current 使用安全 Upsert，"
        "不清空正式表。"
    ),
}

QMT_MEMBERSHIP_SNAPSHOT_TASK = {
    "task_name": "QMT industry membership snapshot",
    "task_type": "qmt_membership_snapshot",
    "group_name": "Guojin QMT",
    "script_path": "tools/sync_bigqmt_reference.py",
    "script_args": "--apply --force-reference-refresh --json",
    "cron_time": "15:12",
    "interval_minutes": 0,
    "enabled": 1,
    "sort_order": 94,
    "date_param": "",
    "description": (
        "Windows QMT edge-owned daily reference and immutable industry "
        "membership snapshot required by the quantitative review gate."
    ),
}


WINDOWS_QMT_EDGE_TASKS = (
    QMT_CATALOG_CAPABILITY_TASK,
    QMT_INTRADAY_REALTIME_TASK,
    QMT_MEMBERSHIP_SNAPSHOT_TASK,
    QMT_ANNOUNCEMENT_TASK,
    TASKS_BY_TYPE["qmt_local_gap_repair_execute"],
    TASKS_BY_TYPE["qmt_local_history_2024"],
    TASKS_BY_TYPE["qmt_reference_incremental"],
)
LINUX_QMT_TASKS = (
    TASKS_BY_TYPE["qmt_nightly_reconciliation"],
    TASKS_BY_TYPE["qmt_gap_repair_plan"],
)

WINDOWS_QMT_EDGE_TASKS_BY_TYPE = {
    str(task["task_type"]): task for task in WINDOWS_QMT_EDGE_TASKS
}
LINUX_QMT_TASKS_BY_TYPE = {
    str(task["task_type"]): task for task in LINUX_QMT_TASKS
}
WINDOWS_QMT_EDGE_TASK_TYPES = frozenset(WINDOWS_QMT_EDGE_TASKS_BY_TYPE)
LINUX_QMT_TASK_TYPES = frozenset(LINUX_QMT_TASKS_BY_TYPE)

# Only these three long-running foundation tasks need recent execution proof.
# The other edge jobs are still exact owned task contracts, but requiring a
# cron success after every arbitrary release time would deadlock deployment.
WINDOWS_QMT_EXECUTION_PROOF_TASK_TYPES = (
    "qmt_local_gap_repair_execute",
    "qmt_local_history_2024",
    "qmt_reference_incremental",
)

# This independent non-QMT source is deliberately not part of the QMT set.  It
# remains on the Windows egress lane because production Linux receives a WAF
# page from Xueqiu.
WINDOWS_NON_QMT_EGRESS_TASKS_BY_TYPE = {
    "fetch_hot_rank_xq": {
        "script_path": "tools/fetch_hot_rank_xq.py",
    },
}
WINDOWS_NON_QMT_EGRESS_TASK_TYPES = frozenset(
    WINDOWS_NON_QMT_EGRESS_TASKS_BY_TYPE
)

# Legacy generic task identities select a provider from ambient configuration.
# Neither executor may claim them until a provider-specific identity and
# immutable task contract are introduced.
UNFROZEN_PROVIDER_TASK_TYPES = frozenset({
    "all_code",
    "all_index_code",
    "concept_code_east",
    "concept_constituent_east",
    "etf_forward_daily",
    "index_constituent",
    "index_current",
    "index_kline",
    "index_minute",
    "stock_current",
    "stock_kline",
    "stock_relations_qmt",
})
UNFROZEN_PROVIDER_SCRIPT_PATHS = frozenset({
    "tools/run_single_table.py",
    "tools/run_etf_forward_daily.py",
})

if WINDOWS_QMT_EDGE_TASK_TYPES & LINUX_QMT_TASK_TYPES:
    raise RuntimeError("QMT host ownership contract overlaps")
if WINDOWS_QMT_EDGE_TASK_TYPES & UNFROZEN_PROVIDER_TASK_TYPES:
    raise RuntimeError("frozen QMT task is also provider-unfrozen")
if set(WINDOWS_QMT_EXECUTION_PROOF_TASK_TYPES) - WINDOWS_QMT_EDGE_TASK_TYPES:
    raise RuntimeError("QMT execution proof task is not Windows edge-owned")
if len(WINDOWS_QMT_EDGE_TASKS_BY_TYPE) != len(WINDOWS_QMT_EDGE_TASKS):
    raise RuntimeError("duplicate Windows QMT edge task identity")
if len(LINUX_QMT_TASKS_BY_TYPE) != len(LINUX_QMT_TASKS):
    raise RuntimeError("duplicate Linux QMT task identity")


__all__ = [
    "LINUX_QMT_TASKS",
    "LINUX_QMT_TASKS_BY_TYPE",
    "LINUX_QMT_TASK_TYPES",
    "QMT_CATALOG_CAPABILITY_TASK",
    "QMT_INTRADAY_REALTIME_TASK",
    "QMT_MEMBERSHIP_SNAPSHOT_TASK",
    "UNFROZEN_PROVIDER_TASK_TYPES",
    "UNFROZEN_PROVIDER_SCRIPT_PATHS",
    "WINDOWS_NON_QMT_EGRESS_TASKS_BY_TYPE",
    "WINDOWS_NON_QMT_EGRESS_TASK_TYPES",
    "WINDOWS_QMT_EDGE_TASKS",
    "WINDOWS_QMT_EDGE_TASKS_BY_TYPE",
    "WINDOWS_QMT_EDGE_TASK_TYPES",
    "WINDOWS_QMT_EXECUTION_PROOF_TASK_TYPES",
]
