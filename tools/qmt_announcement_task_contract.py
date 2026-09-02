"""Frozen production scheduler contract for full-market QMT announcements."""
from __future__ import annotations


QMT_ANNOUNCEMENT_CHECKPOINT_DIR = (
    "/var/lib/probiga/qmt-announcement-checkpoints"
)
QMT_ANNOUNCEMENT_PRIMARY_SOURCE = "qmt.announcement"
QMT_ANNOUNCEMENT_FALLBACK_PROVIDER = "cninfo"
QMT_ANNOUNCEMENT_FALLBACK_SOURCE = "cninfo.announcement"
QMT_ANNOUNCEMENT_FALLBACK_REASON_CODES = frozenset({
    "QMT_ANNOUNCEMENT_NO_PERMISSION_OR_QUERY_FAILED",
    "QMT_ANNOUNCEMENT_FULL_MARKET_ALL_EMPTY_UNPROVEN",
    "QMT_ANNOUNCEMENT_SDK_UNAVAILABLE",
    "QMT_ANNOUNCEMENT_TERMINAL_DEPENDENCY_UNAVAILABLE",
})
QMT_ANNOUNCEMENT_FALLBACK_EGRESS_CONTRACT = {
    "schema": "probiga.qmt-announcement-fallback-egress.v1",
    "owner": "qmt_windows_edge",
    "primary_source": QMT_ANNOUNCEMENT_PRIMARY_SOURCE,
    "fallback_provider": QMT_ANNOUNCEMENT_FALLBACK_PROVIDER,
    "fallback_source": QMT_ANNOUNCEMENT_FALLBACK_SOURCE,
    "activation": "frozen-primary-unavailability-only",
    "eligible_reason_codes": tuple(
        sorted(QMT_ANNOUNCEMENT_FALLBACK_REASON_CODES)
    ),
}

# One immutable same-trading-day evidence DAG.  The announcement collector has
# its own bounded capture duration, but downstream analysis must wait for every
# other required data product; it is therefore intentionally not constrained
# to start within 30 minutes of the announcement task.
QMT_ANNOUNCEMENT_CRON = "18:20"
ANALYSIS_UPPER_EVIDENCE_CRON = "22:10"
ANALYSIS_FAST_CRON = "22:20"
STRATEGY_GOVERNANCE_CRON = "22:35"
ANALYSIS_DAILY_PIPELINE_DECISION_TIME = f"{ANALYSIS_FAST_CRON}:00"
MIN_ANALYSIS_GOVERNANCE_GAP_MINUTES = 10


TASK = {
    "task_name": "国金QMT全市场公告PIT同步",
    "task_type": "qmt_announcement_pit",
    "group_name": "strategy_governance",
    "script_path": "tools/sync_qmt_announcement_pit.py",
    "script_args": (
        "--window-days 30 --overlap-days 3 --batch-size 100 "
        "--fallback-provider cninfo "
        f"--checkpoint-dir {QMT_ANNOUNCEMENT_CHECKPOINT_DIR}"
    ),
    "cron_time": QMT_ANNOUNCEMENT_CRON,
    "interval_minutes": 0,
    "date_param": "",
    "date_param_desc": "",
    "description": (
        "Windows QMT边缘节点复用昨日已验证不可变基线，正常日仅按3日"
        "重叠窗口逐分片下载announcement；目录变化或基线不可验证时才"
        "重建30日基线。"
        "仅在QMT返回冻结的不可用理由后，才允许巨潮官方逐股完整批次兜底。"
        "两种来源均要求同一事实截止、目录全覆盖和不可变回执，缺一股即"
        "DATA_BLOCKED；不允许自动切换其他未冻结来源。"
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
    *,
    upper_evidence_cron: str = ANALYSIS_UPPER_EVIDENCE_CRON,
    analysis_cron: str = ANALYSIS_FAST_CRON,
    governance_cron: str = STRATEGY_GOVERNANCE_CRON,
) -> dict[str, int]:
    event = _minutes(TASK["cron_time"])
    upper_evidence = _minutes(upper_evidence_cron)
    analysis = _minutes(analysis_cron)
    governance = _minutes(governance_cron)
    if not event < upper_evidence < analysis < governance:
        raise ValueError(
            "QMT announcement -> upper evidence -> analysis -> governance "
            "order is invalid"
        )
    if governance - analysis < MIN_ANALYSIS_GOVERNANCE_GAP_MINUTES:
        raise ValueError(
            "analysis and governance must be separated by at least "
            f"{MIN_ANALYSIS_GOVERNANCE_GAP_MINUTES} minutes"
        )
    return {
        "qmt_announcement_minutes": event,
        "upper_evidence_minutes": upper_evidence,
        "analysis_minutes": analysis,
        "governance_minutes": governance,
    }


validate_pipeline_order()


__all__ = [
    "ANALYSIS_DAILY_PIPELINE_DECISION_TIME",
    "ANALYSIS_FAST_CRON",
    "ANALYSIS_UPPER_EVIDENCE_CRON",
    "MIN_ANALYSIS_GOVERNANCE_GAP_MINUTES",
    "QMT_ANNOUNCEMENT_CHECKPOINT_DIR",
    "QMT_ANNOUNCEMENT_FALLBACK_EGRESS_CONTRACT",
    "QMT_ANNOUNCEMENT_FALLBACK_PROVIDER",
    "QMT_ANNOUNCEMENT_FALLBACK_REASON_CODES",
    "QMT_ANNOUNCEMENT_FALLBACK_SOURCE",
    "QMT_ANNOUNCEMENT_PRIMARY_SOURCE",
    "QMT_ANNOUNCEMENT_CRON",
    "STRATEGY_GOVERNANCE_CRON",
    "TASK",
    "validate_pipeline_order",
]
