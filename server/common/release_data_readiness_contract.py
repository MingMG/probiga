"""Shared release data-readiness and catch-up ownership contract."""
from __future__ import annotations

from datetime import datetime, time
import hashlib
import json
import re
from typing import Any, Mapping

from tools.qmt_announcement_task_contract import (
    ANALYSIS_FAST_CRON,
    ANALYSIS_UPPER_EVIDENCE_CRON,
)


RELEASE_DATA_ACTIVATION_SCHEMA = "probiga.release-data-activation.v1"
RELEASE_DATA_ACTIVATION_TASK_TYPE = "release_data_activation"
RELEASE_DATA_ACTIVATION_TRIGGER_SOURCE = "release_activation"
_BUILD_SHA_RE = re.compile(r"[0-9a-f]{40}")
_ACTIVATION_FIELDS = frozenset(
    {
        "schema",
        "build_sha",
        "scheduler_instance_id",
        "scheduler_host_name",
        "scheduler_pid",
        "scheduler_started_at",
        "activated_at",
        "automatic_real_order_submission",
        "real_order_authority",
    }
)


def _activation_build_sha(value: object) -> str:
    build_sha = str(value or "").strip().lower()
    if _BUILD_SHA_RE.fullmatch(build_sha) is None or build_sha == "0" * 40:
        raise ValueError("release activation build SHA is invalid")
    return build_sha


def _activation_datetime(value: object, *, field: str) -> str:
    raw = (
        value.replace(microsecond=0).isoformat(timespec="seconds")
        if isinstance(value, datetime)
        else str(value or "").strip()
    )
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"release activation {field} is invalid") from exc
    normalized = parsed.replace(microsecond=0).isoformat(timespec="seconds")
    if raw != normalized:
        raise ValueError(f"release activation {field} is not canonical")
    return normalized


def _activation_digest(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def release_data_activation_run_uid(
    build_sha: str,
    scheduler_instance_id: str,
    scheduler_started_at: object,
) -> str:
    build = _activation_build_sha(build_sha)
    instance = str(scheduler_instance_id or "").strip()
    if not instance or len(instance) > 128:
        raise ValueError("release activation scheduler instance is invalid")
    started = _activation_datetime(
        scheduler_started_at,
        field="scheduler_started_at",
    )
    return (
        f"release-active-{build[:12]}-"
        f"{hashlib.sha256(f'{instance}|{started}'.encode('utf-8')).hexdigest()[:24]}"
    )


def build_release_data_activation_receipt(
    *,
    build_sha: str,
    scheduler_instance_id: str,
    scheduler_host_name: str,
    scheduler_pid: int,
    scheduler_started_at: object,
    activated_at: object,
) -> dict[str, Any]:
    """Build the canonical Linux-active evidence consumed by both hosts."""

    build = _activation_build_sha(build_sha)
    instance = str(scheduler_instance_id or "").strip()
    host = str(scheduler_host_name or "").strip()
    if not instance or len(instance) > 128 or not host or len(host) > 128:
        raise ValueError("release activation scheduler identity is invalid")
    if isinstance(scheduler_pid, bool) or int(scheduler_pid) <= 0:
        raise ValueError("release activation scheduler PID is invalid")
    if instance != f"{host}-{int(scheduler_pid)}":
        raise ValueError("release activation scheduler instance differs")
    started = _activation_datetime(
        scheduler_started_at,
        field="scheduler_started_at",
    )
    activated = _activation_datetime(activated_at, field="activated_at")
    if activated < started:
        raise ValueError("release activation predates scheduler start")
    unsigned = {
        "schema": RELEASE_DATA_ACTIVATION_SCHEMA,
        "build_sha": build,
        "scheduler_instance_id": instance,
        "scheduler_host_name": host,
        "scheduler_pid": int(scheduler_pid),
        "scheduler_started_at": started,
        "activated_at": activated,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    return {**unsigned, "receipt_hash": _activation_digest(unsigned)}


def validate_release_data_activation_receipt(
    value: Mapping[str, Any] | str,
    *,
    expected_build_sha: str,
    expected_scheduler_instance_id: str,
) -> dict[str, Any]:
    try:
        payload = json.loads(value) if isinstance(value, str) else dict(value)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("release activation receipt JSON is invalid") from exc
    if set(payload) != _ACTIVATION_FIELDS | {"receipt_hash"}:
        raise ValueError("release activation receipt fields differ")
    rebuilt = build_release_data_activation_receipt(
        build_sha=str(payload.get("build_sha") or ""),
        scheduler_instance_id=str(payload.get("scheduler_instance_id") or ""),
        scheduler_host_name=str(payload.get("scheduler_host_name") or ""),
        scheduler_pid=payload.get("scheduler_pid"),
        scheduler_started_at=payload.get("scheduler_started_at"),
        activated_at=payload.get("activated_at"),
    )
    if payload != rebuilt:
        raise ValueError("release activation receipt hash/content differs")
    if rebuilt["build_sha"] != _activation_build_sha(expected_build_sha):
        raise ValueError("release activation receipt build differs")
    if rebuilt["scheduler_instance_id"] != str(
        expected_scheduler_instance_id or ""
    ):
        raise ValueError("release activation receipt scheduler differs")
    return rebuilt


RELEASE_DATA_READINESS_TASK_TYPES = frozenset(
    {
        "qmt_stock_daily_canonical",
        "qmt_index_current",
        "qmt_index_kline",
        "qmt_index_minute",
        "alist_daily",
        "alist_info",
        "eastmoney_concept_current",
        "eastmoney_concept_kline",
        "eastmoney_concept_minute",
        "eastmoney_concept_flow_snapshot",
        "sector_heat_east",
        "hot_concept",
        "hot_rank_ths",
        "hot_pop_east",
        "hot_fused",
        "capital_flow_batch_fast",
        "etf_forward_daily",
        "market_overview_daily",
        "stock_snapshot_daily",
        "stock_finance",
        "notice_eastmoney",
        "notice_eastmoney_historical_repair",
        "stock_dividend_baidu",
        "news_sync",
        "analysis_fast",
        "trading_v3_close_decision",
        "sim_trade_signal_prepare",
    }
)

# Full-market stock minute bars and native minute flow are useful independent
# data products, but neither canonical recommendation publisher consumes them:
# ``analysis_fast`` reads daily bars/capital-flow/PIT facts and Trading V3's
# close decision/backtest reads attested daily bars.  Keep the minute tasks in
# the ordinary scheduler with their strict validators; do not turn an absent
# optional minute history into a release-time strategy outage.  The two broad
# gap-repair jobs are likewise ordinary maintenance because their default plans
# include minute-derived partitions that are not recommendation inputs.
RELEASE_OPTIONAL_MARKET_MAINTENANCE_TASK_TYPES = frozenset(
    {
        "qmt_stock_minute_canonical",
        "qmt_stock_minute_flow_canonical",
        "qmt_canonical_history_gap_repair",
        "linux_recent_data_gap_repair",
    }
)

# QMT point-in-time announcements and the strict morning pool are release
# prerequisites/outputs but are not themselves dashboard data products.  A
# new release still has to replay them so both recommendation pools are built
# only after the exact-build market inputs have converged.
RELEASE_DATA_CATCHUP_SUPPORT_TASK_TYPES = frozenset(
    {
        "qmt_announcement_pit",
        "qmt_membership_snapshot",
        "target_turnover_snapshot",
        "analysis_upper_evidence_prepare",
        "analysis_morning_strict",
    }
)
RELEASE_DATA_CATCHUP_TASK_TYPES = (
    RELEASE_DATA_READINESS_TASK_TYPES | RELEASE_DATA_CATCHUP_SUPPORT_TASK_TYPES
)

# Release evidence for these jobs is meaningful only for one exact data date.
# Keep the classification shared by the scheduler and the SELECT-only release
# gate so a successful row cannot survive an authoritative session rollover.
RELEASE_CATCHUP_CLOSED_TARGET_TASK_TYPES = frozenset(
    {
        "analysis_fast",
        "target_turnover_snapshot",
        "analysis_upper_evidence_prepare",
        "alist_daily",
        "alist_info",
        "capital_flow_batch_fast",
        "eastmoney_concept_current",
        "eastmoney_concept_flow_snapshot",
        "eastmoney_concept_kline",
        "eastmoney_concept_minute",
        "etf_forward_daily",
        "market_overview_daily",
        "sector_heat_east",
        "stock_finance",
        "stock_snapshot_daily",
        "trading_v3_close_decision",
        "qmt_stock_daily_canonical",
        "qmt_index_kline",
        "qmt_index_minute",
        "qmt_membership_snapshot",
        "qmt_announcement_pit",
        "stock_finance",
    }
)

# A close-derived provider may prove its final partition before the platform's
# conservative 18:00 aggregate-data boundary.  Keep those cutoffs beside the
# exact-target classification so scheduler dispatch and the SELECT-only
# readiness gate cannot disagree about when D-1 evidence rolls over to D.
RELEASE_CATCHUP_CLOSED_TARGET_READY_TIMES = {
    # Preserve the immutable analysis evidence windows during release replay.
    # A new build may collect today's evidence only once each ordinary source
    # window is final; it must never relabel a previous session after cutoff.
    "target_turnover_snapshot": "15:50",
    "analysis_upper_evidence_prepare": ANALYSIS_UPPER_EVIDENCE_CRON,
    "analysis_fast": ANALYSIS_FAST_CRON,
    "stock_finance": "21:00",
    "etf_forward_daily": "15:10",
    "sector_heat_east": "15:10",
    "alist_daily": "16:30",
    "alist_info": "16:30",
    # The QMT-owning host publishes the immutable close snapshot at 15:12.
    # Roll release verification one minute later so the read-only replay can
    # never replace the ordinary publisher at its wall-clock deadline.
    "qmt_membership_snapshot": "15:13",
}


def release_catchup_closed_ready_time(task_type: str) -> time:
    """Return the authoritative close cutoff for one release data product."""

    raw = RELEASE_CATCHUP_CLOSED_TARGET_READY_TIMES.get(
        str(task_type or "").strip(),
        "18:00",
    )
    return time.fromisoformat(raw)
RELEASE_CATCHUP_PREVIOUS_SESSION_TARGET_TASK_TYPES = frozenset(
    {"analysis_morning_strict"}
)
RELEASE_CATCHUP_CURRENT_TARGET_TASK_TYPES = frozenset(
    {
        "hot_concept",
        "hot_rank_ths",
        "hot_pop_east",
        "hot_fused",
        "sim_trade_signal_prepare",
        "qmt_index_current",
    }
)
RELEASE_CATCHUP_EXACT_TARGET_TASK_TYPES = frozenset(
    RELEASE_CATCHUP_CLOSED_TARGET_TASK_TYPES
    | RELEASE_CATCHUP_PREVIOUS_SESSION_TARGET_TASK_TYPES
    | RELEASE_CATCHUP_CURRENT_TARGET_TASK_TYPES
)
if not RELEASE_CATCHUP_EXACT_TARGET_TASK_TYPES <= RELEASE_DATA_CATCHUP_TASK_TYPES:
    raise RuntimeError("release target-date contract contains an unmanaged task")

# Dependencies are build-bound: a downstream catch-up waits for a validated
# successful history row produced by the same active release.  The graph is
# acyclic and contains no live-order execution task.
RELEASE_DATA_CATCHUP_DEPENDENCIES = {
    "alist_info": ("alist_daily",),
    "capital_flow_batch_fast": ("qmt_stock_daily_canonical",),
    "target_turnover_snapshot": ("qmt_stock_daily_canonical",),
    "analysis_upper_evidence_prepare": (
        "target_turnover_snapshot",
        "capital_flow_batch_fast",
        "qmt_membership_snapshot",
        "qmt_announcement_pit",
        "qmt_stock_daily_canonical",
        "stock_finance",
        "notice_eastmoney",
    ),
    "eastmoney_concept_kline": ("eastmoney_concept_current",),
    "eastmoney_concept_minute": ("eastmoney_concept_current",),
    "eastmoney_concept_flow_snapshot": ("eastmoney_concept_current",),
    "hot_fused": ("hot_rank_ths", "hot_pop_east"),
    "market_overview_daily": (
        "qmt_stock_daily_canonical",
        "capital_flow_batch_fast",
    ),
    "stock_snapshot_daily": (
        "qmt_stock_daily_canonical",
        "capital_flow_batch_fast",
        "market_overview_daily",
    ),
    "analysis_fast": (
        "analysis_upper_evidence_prepare",
        "target_turnover_snapshot",
        "qmt_membership_snapshot",
        "qmt_announcement_pit",
        "qmt_stock_daily_canonical",
        "capital_flow_batch_fast",
        "stock_finance",
        "notice_eastmoney",
    ),
    "analysis_morning_strict": (
        "qmt_announcement_pit",
        "qmt_stock_daily_canonical",
        "capital_flow_batch_fast",
        "stock_finance",
        "notice_eastmoney",
    ),
    "trading_v3_close_decision": (
        "analysis_fast",
        "qmt_announcement_pit",
        "qmt_stock_daily_canonical",
        "stock_finance",
        "notice_eastmoney_historical_repair",
    ),
    "sim_trade_signal_prepare": ("trading_v3_close_decision",),
}

# The ordinary nightly delivery is a target-date DAG, not a collection of
# unrelated wall-clock crons.  Keep this graph separate from build catch-up:
# a release may reuse validated immutable data, while a missed daily result
# must continue recovering the same authoritative closed session across
# midnight until governance has published its terminal receipt.
DAILY_RESULT_RECOVERY_DEPENDENCIES = {
    "capital_flow_batch_fast": ("qmt_stock_daily_canonical",),
    "target_turnover_snapshot": ("qmt_stock_daily_canonical",),
    "analysis_upper_evidence_prepare": (
        "target_turnover_snapshot",
        "capital_flow_batch_fast",
        "qmt_membership_snapshot",
        "qmt_announcement_pit",
        "qmt_stock_daily_canonical",
        "stock_finance",
        "notice_eastmoney",
    ),
    "analysis_fast": (
        "analysis_upper_evidence_prepare",
        "target_turnover_snapshot",
        "qmt_membership_snapshot",
        "qmt_announcement_pit",
        "qmt_stock_daily_canonical",
        "capital_flow_batch_fast",
        "stock_finance",
        "notice_eastmoney",
    ),
    # Governance is the canonical endpoint of the 22:10 -> 22:20 -> 22:35
    # delivery chain.  It is deliberately included here even though it is not
    # a release data-ingestion task and can never submit a real order.
    "strategy_governance_daily": ("analysis_fast",),
}
DAILY_RESULT_RECOVERY_TASK_TYPES = frozenset(
    set(DAILY_RESULT_RECOVERY_DEPENDENCIES)
    | {
        dependency
        for dependencies in DAILY_RESULT_RECOVERY_DEPENDENCIES.values()
        for dependency in dependencies
    }
)

# Collection must keep advancing even when an older strategy cannot be
# delivered. These source-data publishers use the latest closed calendar
# session; bounded recent-data repair owns missed historical partitions.
# They retain stage leases and immutable evidence, but cannot change a
# strategy delivery's signed terminal status. Derived analysis/pool publishers
# deliberately do not belong here.
DAILY_DATA_INGESTION_TASK_TYPES = frozenset(
    {
        "qmt_stock_daily_canonical",
        "capital_flow_batch_fast",
        "qmt_membership_snapshot",
        "qmt_announcement_pit",
        "stock_finance",
        "notice_eastmoney",
        "target_turnover_snapshot",
    }
)
if not DAILY_DATA_INGESTION_TASK_TYPES <= DAILY_RESULT_RECOVERY_TASK_TYPES:
    raise RuntimeError("daily ingestion contract contains an unmanaged task")

# Outbound delivery is a separate, retryable tail of the recovery DAG.  It is
# target-bound and automatic, but it deliberately does not own the canonical
# daily-control watermark: the sender validates the two completed terminal
# receipts before delivering either pool.
FINAL_POOL_WECOM_DELIVERY_TASK_TYPE = "final_pool_wecom_delivery"
DAILY_RESULT_POST_DELIVERY_DEPENDENCIES = {
    FINAL_POOL_WECOM_DELIVERY_TASK_TYPE: ("strategy_governance_daily",),
}
DAILY_RESULT_TARGET_BOUND_TASK_TYPES = frozenset(
    set(DAILY_RESULT_RECOVERY_TASK_TYPES)
    | set(DAILY_RESULT_POST_DELIVERY_DEPENDENCIES)
)
MANUAL_SCHEDULER_RUN_FORBIDDEN_TASK_TYPES = DAILY_RESULT_TARGET_BOUND_TASK_TYPES

# Per-attempt deadlines leave room for a bounded retry inside each stage's
# recovery window.  These values intentionally replace the blanket six-hour
# timeout for the user-facing critical path; maintenance jobs retain their
# separate long-running policy.
DAILY_RESULT_STAGE_TIMEOUT_MINUTES = {
    "qmt_stock_daily_canonical": 180,
    "qmt_announcement_pit": 90,
    "stock_finance": 180,
    "notice_eastmoney": 90,
    "capital_flow_batch_fast": 90,
    "qmt_membership_snapshot": 60,
    "target_turnover_snapshot": 60,
    "analysis_upper_evidence_prepare": 30,
    "analysis_fast": 90,
    "strategy_governance_daily": 30,
}

# These tasks may submit or execute orders/ticks and are deliberately outside
# the release catch-up contract.  Keep the explicit disjointness assertion so
# a future readiness edit cannot accidentally grant release-time execution.
RELEASE_CATCHUP_FORBIDDEN_EXECUTION_TASK_TYPES = frozenset(
    {
        "sim_trade",
        "trading_v2_paper_tick",
        "trading_v2_intraday_activation",
        "live_order_submit",
        "real_order_submit",
    }
)
if RELEASE_DATA_CATCHUP_TASK_TYPES & RELEASE_CATCHUP_FORBIDDEN_EXECUTION_TASK_TYPES:
    raise RuntimeError("release data catch-up includes an execution task")


__all__ = [
    "DAILY_RESULT_POST_DELIVERY_DEPENDENCIES",
    "DAILY_RESULT_RECOVERY_DEPENDENCIES",
    "DAILY_RESULT_RECOVERY_TASK_TYPES",
    "DAILY_RESULT_STAGE_TIMEOUT_MINUTES",
    "DAILY_RESULT_TARGET_BOUND_TASK_TYPES",
    "FINAL_POOL_WECOM_DELIVERY_TASK_TYPE",
    "MANUAL_SCHEDULER_RUN_FORBIDDEN_TASK_TYPES",
    "RELEASE_DATA_ACTIVATION_SCHEMA",
    "RELEASE_DATA_ACTIVATION_TASK_TYPE",
    "RELEASE_DATA_ACTIVATION_TRIGGER_SOURCE",
    "RELEASE_CATCHUP_FORBIDDEN_EXECUTION_TASK_TYPES",
    "RELEASE_CATCHUP_CLOSED_TARGET_TASK_TYPES",
    "RELEASE_CATCHUP_CLOSED_TARGET_READY_TIMES",
    "RELEASE_CATCHUP_CURRENT_TARGET_TASK_TYPES",
    "RELEASE_CATCHUP_EXACT_TARGET_TASK_TYPES",
    "RELEASE_CATCHUP_PREVIOUS_SESSION_TARGET_TASK_TYPES",
    "RELEASE_DATA_CATCHUP_DEPENDENCIES",
    "RELEASE_DATA_CATCHUP_SUPPORT_TASK_TYPES",
    "RELEASE_DATA_CATCHUP_TASK_TYPES",
    "RELEASE_DATA_READINESS_TASK_TYPES",
    "RELEASE_OPTIONAL_MARKET_MAINTENANCE_TASK_TYPES",
    "build_release_data_activation_receipt",
    "release_data_activation_run_uid",
    "release_catchup_closed_ready_time",
    "validate_release_data_activation_receipt",
]
