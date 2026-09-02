# -*- coding: utf-8 -*-
import logging
import hashlib
import json
import os
import re
import signal
import stat
import subprocess
import sys
import threading
import time
import uuid
from datetime import date, datetime, time as datetime_time, timedelta
from socket import gethostname
from pathlib import Path
from urllib.request import Request, urlopen

from sqlalchemy import text

from server.api.routers._engine import get_engine
from server.common.authoritative_market_clock import (
    PRODUCTION_TIMEZONE,
    authoritative_closed_trade_date,
)
from server.common.config import get_api_mysql_pool_config, get_scheduler_runtime_config
from server.common.analysis_pool_receipt import (
    ANALYSIS_POOL_PUBLISHER_TASK_TYPES,
    canonical_sha256,
    read_persisted_pool_manifest,
    research_only_publication_is_safe,
)
from server.common.process_env import build_child_env
from server.common.release_data_readiness_contract import (
    DAILY_RESULT_RECOVERY_DEPENDENCIES,
    DAILY_RESULT_RECOVERY_TASK_TYPES,
    DAILY_RESULT_STAGE_TIMEOUT_MINUTES,
    RELEASE_CATCHUP_CLOSED_TARGET_TASK_TYPES,
    RELEASE_CATCHUP_CURRENT_TARGET_TASK_TYPES,
    RELEASE_CATCHUP_EXACT_TARGET_TASK_TYPES,
    RELEASE_CATCHUP_PREVIOUS_SESSION_TARGET_TASK_TYPES,
    RELEASE_DATA_ACTIVATION_TASK_TYPE,
    RELEASE_DATA_ACTIVATION_TRIGGER_SOURCE,
    RELEASE_DATA_CATCHUP_DEPENDENCIES,
    RELEASE_DATA_CATCHUP_TASK_TYPES,
    build_release_data_activation_receipt,
    release_catchup_closed_ready_time,
    release_data_activation_run_uid,
    validate_release_data_activation_receipt,
)
from server.common.scheduler_runtime_health import (
    check_linux_standalone_active_release,
    check_qmt_windows_edge_release_receipt,
)
from server.common.scheduler_script_policy import (
    SchedulerScriptPolicyError,
    resolve_scheduler_script,
)
from server.common.scheduler_args import (
    ANALYSIS_DAILY_EVIDENCE_TASK_TYPES,
    ANALYSIS_DAILY_PIPELINE_DECISION_TIME,
    build_scheduler_task_args,
)
from server.common.scheduler_tasks import (
    claim_scheduler_task_run,
    update_scheduler_task,
)
from server.common.strategy_governance_mode import (
    StrategyGovernanceModeError,
    strategy_governance_database_deferred,
)
from server.common.scheduler_validation import (
    is_market_closed_skip_output,
    scheduler_output_status,
    validate_scheduler_task_result,
)
from tools.qmt_host_ownership_contract import (
    LINUX_PROVIDER_TASKS_BY_TYPE,
    LINUX_PROVIDER_TASK_TYPES,
    LINUX_QMT_TASKS_BY_TYPE,
    LINUX_QMT_TASK_TYPES,
    UNFROZEN_PROVIDER_SCRIPT_PATHS,
    UNFROZEN_PROVIDER_TASK_TYPES,
    WINDOWS_NON_QMT_EGRESS_TASKS_BY_TYPE,
    WINDOWS_NON_QMT_EGRESS_TASK_TYPES,
    WINDOWS_QMT_EDGE_TASKS_BY_TYPE,
    WINDOWS_QMT_EDGE_TASK_TYPES,
)

logger = logging.getLogger("scheduler_daemon")
if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)

DEFAULT_TASK_TIMEOUT_MINUTES = int(os.environ.get("SCHEDULER_TASK_TIMEOUT_MINUTES", "180"))
FAST_TASK_TIMEOUT_MINUTES = int(os.environ.get("SCHEDULER_FAST_TASK_TIMEOUT_MINUTES", "20"))
LONG_TASK_TIMEOUT_MINUTES = int(os.environ.get("SCHEDULER_LONG_TASK_TIMEOUT_MINUTES", "360"))
QMT_FULL_HISTORY_TASK_TIMEOUT_MINUTES = 8 * 60
CRON_CATCHUP_WINDOW_SECONDS = int(os.environ.get("SCHEDULER_CRON_CATCHUP_WINDOW_SECONDS", "180"))
CRITICAL_CRON_CATCHUP_WINDOW_SECONDS = int(os.environ.get("SCHEDULER_CRITICAL_CRON_CATCHUP_WINDOW_SECONDS", "10800"))
CRON_RETRY_INTERVAL_MINUTES = max(1, int(os.environ.get("SCHEDULER_CRON_RETRY_INTERVAL_MINUTES", "15")))
RELEASE_CATCHUP_RETRY_INTERVAL_MINUTES = max(
    5,
    int(os.environ.get("SCHEDULER_RELEASE_CATCHUP_RETRY_MINUTES", "15")),
)
RELEASE_CATCHUP_BLOCKED_RETRY_INTERVAL_MINUTES = max(
    RELEASE_CATCHUP_RETRY_INTERVAL_MINUTES,
    int(os.environ.get("SCHEDULER_RELEASE_CATCHUP_BLOCKED_RETRY_MINUTES", "30")),
)
RELEASE_TURNOVER_DECISION_LEAD_SECONDS = 5 * 60
RELEASE_UPPER_DECISION_LEAD_SECONDS = 60
_QMT_MEMBERSHIP_TASK_TYPE = "qmt_membership_snapshot"
_QMT_MEMBERSHIP_PROVIDER = "gj_big_qmt_inner"
RELEASE_CATCHUP_AUTHORITATIVE_DATE_TASK_TYPES = (
    RELEASE_CATCHUP_CLOSED_TARGET_TASK_TYPES
)
RELEASE_CATCHUP_PREVIOUS_SESSION_TASK_TYPES = (
    RELEASE_CATCHUP_PREVIOUS_SESSION_TARGET_TASK_TYPES
)
# These ingestion jobs stamp a live snapshot and explicitly reject backdating.
# They remain on the Shanghai run date; they are not closed-session outputs.
RELEASE_CATCHUP_RUN_DATE_SNAPSHOT_TASK_TYPES = frozenset(
    {
        "hot_concept",
        "hot_rank_ths",
        "hot_pop_east",
        "qmt_index_current",
    }
)
RELEASE_CATCHUP_CURRENT_SNAPSHOT_READY_TIMES = {
    "hot_concept": datetime_time(17, 10),
    "hot_rank_ths": datetime_time(17, 12),
    "hot_pop_east": datetime_time(17, 14),
    "qmt_index_current": datetime_time(15, 10),
}
RETRYABLE_CRON_STATUSES = frozenset({"failed", "timeout", "stopped"})
RETRYABLE_BLOCKED_ORCHESTRATION_STATUSES = frozenset(
    {"DATA_BLOCKED", "NOT_READY", "TRANSIENT_DATA_BLOCKED"}
)
DAILY_RESULT_RECOVERY_MAX_AGE_DAYS = max(
    1,
    int(os.environ.get("SCHEDULER_DAILY_RESULT_RECOVERY_MAX_AGE_DAYS", "7")),
)
STALE_RUNNING_GRACE_MINUTES = int(os.environ.get("SCHEDULER_STALE_RUNNING_GRACE_MINUTES", "5"))
HISTORY_RETENTION_DAYS = max(1, int(os.environ.get("SCHEDULER_HISTORY_RETENTION_DAYS", "90")))
HISTORY_CLEANUP_BATCH_SIZE = max(1, int(os.environ.get("SCHEDULER_HISTORY_CLEANUP_BATCH_SIZE", "1000")))
HISTORY_CLEANUP_MAX_BATCHES = max(1, int(os.environ.get("SCHEDULER_HISTORY_CLEANUP_MAX_BATCHES", "4")))
HISTORY_CLEANUP_INTERVAL_SECONDS = max(
    300,
    int(os.environ.get("SCHEDULER_HISTORY_CLEANUP_INTERVAL_SECONDS", "3600")),
)
# Briefings and reviews are user-facing daily deliverables.  Once their cron
# time has passed, keep post-market reports eligible for the rest of that
# calendar day; keep an early briefing eligible only through the morning so a
# scheduler recovery never emits a stale “morning” message at night.
# The date check in ``_critical_cron_catchup_allowed`` prevents cross-day
# replay; failed runs continue to use the normal retry backoff above.
USER_DELIVERY_CRON_CATCHUP_WINDOW_SECONDS = int(
    os.environ.get("SCHEDULER_USER_DELIVERY_CATCHUP_WINDOW_SECONDS", "86400")
)
EARLY_BRIEFING_CRON_CATCHUP_WINDOW_SECONDS = int(
    os.environ.get("SCHEDULER_EARLY_BRIEFING_CATCHUP_WINDOW_SECONDS", "12600")
)
# A daily recommendation is a user-facing deliverable.  It must not be lost
# merely because a long post-market sync occupies the scheduler at its exact
# cron minute; allow it to be claimed later on the same day.
CRITICAL_CRON_CATCHUP_TASK_TYPES = {"analysis_morning_strict", "analysis_fast"}
CRITICAL_CRON_CATCHUP_TASK_TYPES.add("analysis_premarket_external")
CRITICAL_CRON_CATCHUP_TASK_TYPES.add("strategy_external_overlay")
CRITICAL_CRON_CATCHUP_TASK_TYPES.add("sim_trade_signal_prepare")
CRITICAL_CRON_CATCHUP_TASK_TYPES.update(
    {
        "target_turnover_snapshot",
        "analysis_upper_evidence_prepare",
        "alist_daily",
        "alist_info",
        "concept_flow",
        "eastmoney_concept_flow_snapshot",
        "eastmoney_concept_current",
        "eastmoney_concept_kline",
        "eastmoney_concept_minute",
        "etf_forward_daily",
        "hot_concept",
        "hot_rank_ths",
        "hot_pop_east",
        "hot_fused",
        "hot_fused_3",
        "hot_fused_5",
        "notice_eastmoney",
        "quality_check_post",
        "quality_check_pre",
        "sector_heat_east",
        "stock_snapshot_daily",
        "stock_finance",
        "stock_dividend_baidu",
        "news_daily",
        "daily_review",
        "evening_review",
        "trading_v2_premarket_decision",
        "trading_v2_close_decision",
        "trading_v2_reconciliation",
        "trading_v2_level1_validation",
        "trading_v2_strategy_health",
        "strategy_governance_daily",
        "stock_kline",
        "trading_v3_close_decision",
        "trading_v3_premarket_review",
        "trading_v3_counterfactual_audit",
        "trading_v3_continuous_calibration",
        "qmt_membership_snapshot",
        "qmt_announcement_pit",
        "qmt_index_kline",
        "qmt_index_minute",
        "qmt_stock_daily_canonical",
        "qmt_stock_minute_canonical",
        "qmt_stock_minute_flow_canonical",
        "qmt_canonical_history_gap_repair",
        "linux_recent_data_gap_repair",
        "concept_constituent_east",
        "capital_flow",
        "capital_flow_batch_fast",
        "market_overview_daily",
        "news_sync",
        "screener_premarket_delivery",
        "screener_intraday_delivery",
    }
)
CRITICAL_CRON_CATCHUP_WINDOWS_SECONDS = {
    "target_turnover_snapshot": 8 * 60 * 60,
    "analysis_upper_evidence_prepare": 2 * 60 * 60,
    # These source snapshots and quality gates must not disappear merely
    # because the single general worker was occupied during their exact cron
    # minute.  They remain bounded to the same trading day and `_cron_due`
    # still prevents a second successful run.
    "alist_daily": 8 * 60 * 60,
    "alist_info": 8 * 60 * 60,
    "concept_flow": 8 * 60 * 60,
    "eastmoney_concept_flow_snapshot": 8 * 60 * 60,
    "eastmoney_concept_current": 8 * 60 * 60,
    "eastmoney_concept_kline": 8 * 60 * 60,
    "eastmoney_concept_minute": 8 * 60 * 60,
    "etf_forward_daily": 8 * 60 * 60,
    "hot_concept": 8 * 60 * 60,
    "hot_rank_ths": 8 * 60 * 60,
    "hot_pop_east": 8 * 60 * 60,
    "hot_fused": 8 * 60 * 60,
    "hot_fused_3": 8 * 60 * 60,
    "hot_fused_5": 8 * 60 * 60,
    "notice_eastmoney": 3 * 60 * 60,
    "quality_check_post": 4 * 60 * 60,
    "quality_check_pre": 3 * 60 * 60,
    "sector_heat_east": 8 * 60 * 60,
    "stock_snapshot_daily": 4 * 60 * 60,
    "stock_finance": 4 * 60 * 60,
    "stock_dividend_baidu": 6 * 60 * 60,
    "news_daily": EARLY_BRIEFING_CRON_CATCHUP_WINDOW_SECONDS,
    "daily_review": USER_DELIVERY_CRON_CATCHUP_WINDOW_SECONDS,
    "evening_review": USER_DELIVERY_CRON_CATCHUP_WINDOW_SECONDS,
    "trading_v2_premarket_decision": 6 * 60 * 60,
    "trading_v2_close_decision": 4 * 60 * 60,
    "trading_v2_reconciliation": 4 * 60 * 60,
    "trading_v2_level1_validation": 4 * 60 * 60,
    "trading_v2_strategy_health": 4 * 60 * 60,
    "strategy_governance_daily": 6 * 60 * 60,
    "strategy_external_overlay": 2 * 60 * 60,
    "stock_kline": 8 * 60 * 60,
    "trading_v3_close_decision": 8 * 60 * 60,
    "trading_v3_premarket_review": 3 * 60 * 60,
    "trading_v3_counterfactual_audit": 8 * 60 * 60,
    "trading_v3_continuous_calibration": 8 * 60 * 60,
    "qmt_membership_snapshot": 8 * 60 * 60,
    "qmt_announcement_pit": 4 * 60 * 60,
    "qmt_index_kline": 8 * 60 * 60,
    "qmt_index_minute": 8 * 60 * 60,
    "qmt_stock_daily_canonical": 8 * 60 * 60,
    "qmt_stock_minute_canonical": 8 * 60 * 60,
    "qmt_stock_minute_flow_canonical": 8 * 60 * 60,
    "qmt_canonical_history_gap_repair": 24 * 60 * 60,
    "linux_recent_data_gap_repair": 24 * 60 * 60,
    "concept_constituent_east": 8 * 60 * 60,
    # These tables feed the watchlist's current-session market and funds
    # labels.  Missing their exact cron minute must not leave the UI pinned to
    # a prior trading day for the rest of the session.
    "capital_flow": 8 * 60 * 60,
    "capital_flow_batch_fast": 8 * 60 * 60,
    "market_overview_daily": 8 * 60 * 60,
    "news_sync": 8 * 60 * 60,
}
# Expensive repair/backfill jobs are useful only after the user-facing close
# pipeline has produced the strategy pool and watchlist for the latest closed
# session.  Reserving the post-close window prevents an overdue maintenance
# catch-up (especially after a deployment) from occupying the sole worker in
# front of the 22:10/22:20/22:35 delivery chain.
DAILY_RESULT_MAINTENANCE_TASK_TYPES = frozenset(
    {
        "qmt_canonical_history_gap_repair",
        "qmt_local_gap_repair_execute",
        "qmt_local_history_2024",
        "qmt_nightly_reconciliation",
        "linux_recent_data_gap_repair",
        "notice_eastmoney_historical_repair",
    }
)
DAILY_RESULT_PIPELINE_TASK_TYPE = "strategy_governance_daily"
DAILY_RESULT_PIPELINE_RESERVATION_TIME = datetime_time(15, 30)
# One common close boundary owns the recovery target for every stage.  It is
# deliberately no later than the first daily-result publisher (the 15:12 QMT
# membership snapshot), so ordinary current-session work is not delayed while
# still preventing a pre-close wall-clock date from entering the DAG.
DAILY_RESULT_RECOVERY_TARGET_READY_TIME = datetime_time(15, 10)
PREMARKET_RECOMMENDATION_CATCHUP_WINDOW_SECONDS = int(
    os.environ.get("SCHEDULER_PREMARKET_RECOMMENDATION_CATCHUP_WINDOW_SECONDS", "7200")
)
RECOMMENDATION_CRON_CATCHUP_WINDOW_SECONDS = int(
    os.environ.get("SCHEDULER_RECOMMENDATION_CATCHUP_WINDOW_SECONDS", "21600")
)
NON_TRADING_DAY_SKIP_TYPES = {
    "target_turnover_snapshot",
    "analysis_upper_evidence_prepare",
    "analysis_fast",
    "analysis_morning_strict",
    "analysis_premarket_external",
    "alist_daily",
    "alist_info",
    "capital_flow",
    "concept_east_current",
    "concept_east_kline",
    "concept_east_minute",
    "concept_flow",
    "eastmoney_concept_flow_snapshot",
    "eastmoney_concept_current",
    "eastmoney_concept_kline",
    "eastmoney_concept_minute",
    "concept_ths_current",
    "concept_ths_kline",
    "concept_ths_minute",
    "daily_review",
    "evening_review",
    "hot_concept",
    "hot_rank_ths",
    "hot_pop_east",
    "hot_fused",
    "hot_fused_3",
    "hot_fused_5",
    "etf_forward_daily",
    "index_current",
    "index_kline",
    "index_minute",
    "notice_eastmoney",
    "intraday_quality_check",
    "intraday_realtime",
    "intraday_market_alert",
    "intraday_minute_flow",
    "intraday_capital_flow_fast",
    "intraday_minute_kline",
    "capital_flow_batch_fast",
    "market_overview_daily",
    "news_daily",
    "qmt_intraday_realtime",
    "qmt_index_current",
    "qmt_index_kline",
    "qmt_index_minute",
    "qmt_stock_daily_canonical",
    "qmt_stock_minute_canonical",
    "qmt_stock_minute_flow_canonical",
    "quality_check_post",
    "quality_check_pre",
    "sector_heat_east",
    "qmt_membership_snapshot",
    "qmt_announcement_pit",
    "qmt_local_gap_repair_execute",
    "qmt_local_history_2024",
    "qmt_reference_incremental",
    "public_quote_failover",
    "sim_trade",
    "sim_trade_signal_prepare",
    "stock_bar",
    "stock_current",
    "stock_five",
    "stock_kline",
    "stock_minute",
    "stock_minute_flow",
    "stock_snapshot_daily",
    "strategy_governance_daily",
    "strategy_external_overlay",
    "trading_v2_intraday_activation",
    "trading_v2_level1_validation",
    "trading_v2_paper_tick",
    "trading_v3_close_decision",
    "trading_v3_premarket_review",
    "screener_premarket_delivery",
    "screener_intraday_delivery",
}
NON_TRADING_DAY_SKIP_PATHS = {
    "biz/analysis/sync_analysis_fast.py",
    "tools/crawl_minute_kline.py",
    "tools/crawl_realtime_batch.py",
    "tools/data_quality_check.py",
    "tools/run_single_table.py",
}
# Exact QMT host identity is task-type based.  Script-path matching used to
# claim unrelated Eastmoney jobs on Windows and allowed provider-generic rows
# to inherit whichever host happened to see them first.
WINDOWS_QMT_BRIDGE_TASK_TYPES = WINDOWS_QMT_EDGE_TASK_TYPES
WINDOWS_QMT_BRIDGE_SCRIPT_PATHS = frozenset(
    str(task["script_path"]) for task in WINDOWS_QMT_EDGE_TASKS_BY_TYPE.values()
)
SCHEDULER_OWNER_LINUX = "linux_standalone"
SCHEDULER_OWNER_WINDOWS_QMT = "qmt_windows_edge"
SCHEDULER_OWNER_WINDOWS_EGRESS = "windows_non_qmt_egress"
SCHEDULER_OWNER_UNAVAILABLE = "unavailable"
_running_procs: dict[int, subprocess.Popen] = {}
_running_task_ids: set[int] = set()
_running_history_uids: dict[int, str] = {}
# ``*_pending`` means termination is being attempted; workers only treat the
# corresponding ``*_requested`` set as authoritative after exit confirmation.
_stop_pending_task_ids: set[int] = set()
_stop_requested_task_ids: set[int] = set()
_timeout_pending_task_ids: set[int] = set()
_timeout_requested_task_ids: set[int] = set()
_fast_lane_running_task_ids: set[int] = set()
_quote_lane_running_task_ids: set[int] = set()
_alert_lane_running_task_ids: set[int] = set()
_delivery_lane_running_task_ids: set[int] = set()
_running_lock = threading.Lock()
_running_skip_logged_at: dict[int, datetime] = {}
_intraday_skip_logged_for: set[tuple[int, str]] = set()
_overdue_skip_logged_for: set[tuple[int, str]] = set()
_delegated_skip_logged_for: set[tuple[int, str]] = set()
_task_semaphore: threading.Semaphore | None = None
_fast_lane_semaphore: threading.Semaphore | None = None
_quote_lane_semaphore: threading.Semaphore | None = None
_alert_lane_semaphore: threading.Semaphore | None = None
_delivery_lane_semaphore: threading.Semaphore | None = None
_scheduler_thread: threading.Thread | None = None
_scheduler_stop_event: threading.Event | None = None
_scheduler_wake_event = threading.Event()
def _now_shanghai_naive() -> datetime:
    """Return the sole scheduler/DB wall clock (Asia/Shanghai, naive)."""

    return datetime.now(PRODUCTION_TIMEZONE).replace(tzinfo=None)


def _wait_for_scheduler_poll(
    stop_event: threading.Event | None,
    poll_seconds: int,
) -> bool:
    """Wait for the timer, shutdown, or a completed dependency stage."""

    deadline = time.monotonic() + max(0, int(poll_seconds))
    while True:
        if _scheduler_wake_event.is_set():
            _scheduler_wake_event.clear()
            return bool(stop_event and stop_event.is_set())
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        interval = min(1.0, remaining)
        if stop_event is not None:
            if stop_event.wait(interval):
                return True
        elif _scheduler_wake_event.wait(interval):
            _scheduler_wake_event.clear()
            return False


_scheduler_started_at = _now_shanghai_naive()
_scheduler_instance_id = f"{gethostname()}-{os.getpid()}"
_task_history_schema_lock = threading.Lock()
_task_history_ready_engines: set[int] = set()
_history_cleanup_lock = threading.Lock()
_history_cleanup_next_at = 0.0
# ``output`` is a MySQL TEXT column (65,535 bytes).  Keep a deliberately
# bounded replay envelope plus a small operator-facing tail below that hard
# limit; never depend on an unbounded child-process log being durable.
_HISTORY_OUTPUT_LIMIT = 60000
_HISTORY_REPLAY_OUTPUT_LIMIT = 24000
_HISTORY_EVIDENCE_LIMIT = 50000
_HISTORY_EVIDENCE_SCHEMA = "probiga.scheduler-validation-evidence.v1"
_HISTORY_SECRET_PATTERNS = (
    (re.compile(r"(?i)(\bBearer\s+)([A-Za-z0-9._~+\-/=]+)"), r"\1[REDACTED]"),
    (re.compile(r"(?i)([\"']?\b(?:authorization|password|passwd|pwd|token|api[_-]?key|api[_-]?secret|access[_-]?token|secret)\b[\"']?\s*[:=]\s*[\"']?)([^\"'\s,;&}]+)"), r"\1[REDACTED]"),
    (re.compile(r"(?i)([?&](?:key|token|access_token|api_key|secret|password)=)([^&#\s]+)"), r"\1[REDACTED]"),
    (re.compile(r"(?i)(\b[a-z][a-z0-9+.-]*://[^\s:/@]+:)([^\s/@]+)(@)"), r"\1[REDACTED]\3"),
    (re.compile(r"(?i)(\b(?:sk-|ghp_|github_pat_|xox[baprs]-))([A-Za-z0-9_-]{12,})"), r"\1[REDACTED]"),
)


LONG_RUNNING_TASK_TYPES = {
    "target_turnover_snapshot",
    "analysis_upper_evidence_prepare",
    "analysis_fast",
    "analysis_premarket_external",
    "strategy_external_overlay",
    "capital_flow",
    "capital_flow_batch_fast",
    "concept_east_kline",
    "concept_east_minute",
    "concept_flow",
    "eastmoney_concept_flow_snapshot",
    "etf_forward_daily",
    "concept_ths_kline",
    "concept_ths_minute",
    "index_kline",
    "index_minute",
    "qmt_local_gap_repair_execute",
    "qmt_local_history_2024",
    "qmt_nightly_reconciliation",
    "qmt_announcement_pit",
    "qmt_index_kline",
    "qmt_index_minute",
    "qmt_stock_daily_canonical",
    "qmt_stock_minute_canonical",
    "qmt_stock_minute_flow_canonical",
    "qmt_canonical_history_gap_repair",
    "linux_recent_data_gap_repair",
    "notice_eastmoney",
    "stock_kline",
    "stock_minute",
    "stock_minute_flow",
    "stock_finance",
    "stock_dividend_baidu",
    "trading_v3_continuous_calibration",
}
LONG_RUNNING_PATH_PARTS = {
    "biz/analysis/sync_analysis_fast.py",
    "biz/stock_market/sync_stock_market.py",
    "tools/run_single_table.py",
}
FAST_RUNNING_TASK_TYPES = {
    "screener_premarket_delivery",
    "screener_intraday_delivery",
    "hot_rank_ths",
    "hot_pop_east",
    "intraday_minute_flow",
    "intraday_capital_flow_fast",
    "intraday_minute_kline",
    "intraday_quality_check",
    "intraday_realtime",
    "quality_check_post",
    "quality_check_pre",
    "qmt_intraday_realtime",
    "qmt_index_current",
    "public_quote_failover",
    "sim_trade",
    "trading_v2_intraday_activation",
    "trading_v2_paper_tick",
}
# The simulated-trading tick is lightweight but latency-sensitive.  A
# dedicated one-worker lane keeps long data syncs from blocking market checks.
FAST_LANE_TASK_TYPES = {
    "intraday_capital_flow_fast",
    "sim_trade",
    "trading_v2_intraday_activation",
    "trading_v2_paper_tick",
}
# The full-market current quote snapshot is the source of truth for the
# watchlist's price, daily change and P&L. Keep it on an independent
# single-worker lane: minute K-line/flow providers can block for many minutes,
# and neither the general lane nor the trading-tick lane may be allowed to pin
# the user-facing watchlist to the previous close.
QUOTE_LANE_TASK_TYPES = {
    "intraday_realtime",
    "public_quote_failover",
}
# Event-driven intraday alerts get their own single-worker lane so a bulk sync
# or trading tick cannot delay a user-visible notification.  Single-flight
# execution also prevents two observations from sending duplicate alerts.
ALERT_LANE_TASK_TYPES = {"intraday_market_alert"}
# User-visible briefings and reviews get one independent worker slot.  This is
# deliberately separate from both the general sync pool and the latency lane:
# a long market-data job must not consume the only opportunity to deliver a
# report, while report generation must not delay intraday safety checks.
USER_DELIVERY_LANE_TASK_TYPES = {
    "news_daily",
    "daily_review",
    "evening_review",
}
INTRADAY_WINDOW_TASK_TYPES = {
    "intraday_capital_flow_fast",
    "intraday_minute_flow",
    "intraday_minute_kline",
    "intraday_quality_check",
    "intraday_realtime",
    "intraday_market_alert",
    "qmt_intraday_realtime",
    "qmt_index_current",
    "public_quote_failover",
    "sim_trade",
    "stock_current",
    "trading_v2_intraday_activation",
    "trading_v2_paper_tick",
}
INTRADAY_WINDOW_PATH_PARTS = {
    "tools/crawl_minute_kline.py",
    "tools/crawl_realtime_batch.py",
    "tools/sync_market_realtime.py",
    "tools/sync_qmt_realtime.py",
    "tools/run_intraday_market_alert.py",
}


def _is_trade_day(engine, day: date | None = None) -> bool | None:
    """Return the explicit calendar state, or ``None`` when it is unknown.

    Missing calendar rows are not equivalent to an exchange holiday: the
    calendar sync may be incomplete or between truncate and refill.  Callers
    must only skip work for an explicit ``trade_status = 0`` row.
    """
    day = day or _now_shanghai_naive().date()
    try:
        with engine.connect() as conn:
            trade_status = conn.execute(
                text(
                    "SELECT trade_status FROM si_trade_calendar "
                    "WHERE trade_date = :trade_date LIMIT 1"
                ),
                {"trade_date": day.isoformat()},
            ).scalar()
        if trade_status is None:
            logger.warning("trade calendar has no row for %s; keep scheduled tasks due", day)
            return None
        status_value = int(trade_status)
        if status_value == 1:
            return True
        if status_value == 0:
            return False
        logger.warning(
            "trade calendar has unexpected status %r for %s; keep scheduled tasks due",
            trade_status,
            day,
        )
        return None
    except Exception as exc:
        logger.warning("trade calendar lookup failed; keep scheduled tasks due: %s", exc)
        return None


def _coerce_datetime(value) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.strptime(str(value)[:19], "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _iter_scheduler_output_payloads(value: object):
    """Yield bounded JSON receipts, including scheduler evidence envelopes."""

    pending = [str(value or "")]
    seen: set[str] = set()
    while pending and len(seen) < 32:
        source = pending.pop()
        if source in seen:
            continue
        seen.add(source)
        for raw_line in source.splitlines():
            candidate = raw_line.strip()
            if not candidate.startswith("{"):
                continue
            try:
                payload = json.loads(candidate)
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict):
                continue
            yield payload
            replay_output = payload.get("replay_output")
            if isinstance(replay_output, str) and replay_output not in seen:
                pending.append(replay_output)


def _scheduler_output_target_dates(value: object) -> frozenset[str]:
    """Extract only explicit, canonical session identities from receipts."""

    dates: set[str] = set()
    fields = (
        "target_trade_date",
        "trade_date",
        "data_trade_date",
        "release_target_date",
        "snapshot_date",
        "pick_date",
        "target_date",
        "as_of",
        "as_of_date",
        "expected_trade_date",
        "session_date",
        "decision_session_date",
    )
    for payload in _iter_scheduler_output_payloads(value):
        for field in fields:
            raw = str(payload.get(field) or "")[:10]
            try:
                parsed = date.fromisoformat(raw)
            except ValueError:
                continue
            if parsed.isoformat() == raw:
                dates.add(raw)
    return frozenset(dates)


def _retryable_blocked_output(value: object) -> bool:
    """Recognize only an explicit transient blocked receipt.

    Legal no-data and policy BLOCK results remain terminal.  A free-form log
    containing the word ``retryable`` cannot turn itself into scheduler work.
    """

    for payload in _iter_scheduler_output_payloads(value):
        if payload.get("retryable") is not True:
            continue
        orchestration_status = str(
            payload.get("orchestration_status")
            or payload.get("status")
            or payload.get("error_class")
            or ""
        ).strip().upper()
        if orchestration_status in RETRYABLE_BLOCKED_ORCHESTRATION_STATUSES:
            return True
    return False


def _retryable_blocked_marker(value: object) -> str:
    """Return a small durable retry marker copied from a valid receipt."""

    for payload in _iter_scheduler_output_payloads(value):
        if payload.get("retryable") is not True:
            continue
        status = str(
            payload.get("orchestration_status")
            or payload.get("status")
            or payload.get("error_class")
            or ""
        ).strip().upper()
        if status not in RETRYABLE_BLOCKED_ORCHESTRATION_STATUSES:
            continue
        marker = {
            "schema": "probiga.scheduler-transient-block.v1",
            "orchestration_status": status,
            "retryable": True,
        }
        for field in ("reason_code", "target_trade_date", "trade_date"):
            if payload.get(field) not in (None, ""):
                marker[field] = payload[field]
        return json.dumps(
            marker,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    return ""


def _task_status_is_retryable(row: dict) -> bool:
    status = str(row.get("last_run_status") or "").strip().lower()
    return status in RETRYABLE_CRON_STATUSES or (
        status == "blocked"
        and _retryable_blocked_output(row.get("last_run_output"))
    )


def _row_matches_target_trade_date(row: dict, target_trade_date: str) -> bool:
    """Bind a persisted terminal row to one target without calendar guessing."""

    try:
        parsed_target = date.fromisoformat(str(target_trade_date or ""))
    except ValueError:
        return False
    target = parsed_target.isoformat()
    output_dates = _scheduler_output_target_dates(row.get("last_run_output"))
    if output_dates:
        return target in output_dates
    triggered = _coerce_datetime(row.get("last_triggered_at"))
    # Legacy producers without a machine date are accepted only when their
    # durable dispatch day is the target itself.  Cross-midnight recovery must
    # emit an explicit target before it can satisfy a downstream dependency.
    return triggered is not None and triggered.date() == parsed_target


def _row_recovery_target(row: dict, *, now: datetime) -> date:
    raw = str(row.get("_scheduler_target_trade_date") or "").strip()
    try:
        parsed = date.fromisoformat(raw)
    except ValueError:
        return now.date()
    return parsed


def _prior_target_recovery_allowed(row: dict, *, now: datetime) -> bool:
    target = _row_recovery_target(row, now=now)
    age_days = (now.date() - target).days
    return 1 <= age_days <= DAILY_RESULT_RECOVERY_MAX_AGE_DAYS


def _select_daily_result_recovery_target(
    trade_dates: list[object],
    governance_rows: list[dict],
    *,
    latest_target: str,
) -> str:
    """Select the oldest ungoverned session from one bounded calendar window.

    The calendar is the target-date authority and a hash-bearing canonical
    governance row is the terminal receipt.  Every stage receives the same
    selected date; no stage-specific source cutoff may advance part of the DAG
    while an older session is still unfinished.
    """

    try:
        parsed_latest = date.fromisoformat(str(latest_target or ""))
    except ValueError as exc:
        raise RuntimeError(
            "daily-result latest authoritative target is invalid"
        ) from exc
    if parsed_latest.isoformat() != latest_target:
        raise RuntimeError(
            "daily-result latest authoritative target is invalid"
        )

    normalized_dates: list[str] = []
    for value in trade_dates:
        raw = str(value or "")[:10]
        try:
            parsed = date.fromisoformat(raw)
        except ValueError as exc:
            raise RuntimeError(
                "daily-result recovery calendar contains an invalid date"
            ) from exc
        if parsed.isoformat() != raw:
            raise RuntimeError(
                "daily-result recovery calendar contains an invalid date"
            )
        normalized_dates.append(raw)
    if (
        not normalized_dates
        or len(normalized_dates) != len(set(normalized_dates))
        or normalized_dates != sorted(normalized_dates)
        or normalized_dates[-1] != latest_target
    ):
        raise RuntimeError(
            "daily-result recovery calendar is incomplete or ambiguous"
        )

    completed_by_date: dict[str, dict] = {}
    for raw_row in governance_rows:
        row = dict(raw_row)
        trade_date_value = str(row.get("trade_date") or "")[:10]
        if trade_date_value not in normalized_dates:
            raise RuntimeError(
                "canonical governance receipt is outside the recovery window"
            )
        if (
            str(row.get("status") or "").strip().upper() != "COMPLETED"
            or row.get("is_canonical") not in (1, True)
        ):
            continue
        if trade_date_value in completed_by_date:
            raise RuntimeError(
                "canonical governance receipt is not unique for one date"
            )
        run_uid = str(row.get("run_uid") or "").strip().lower()
        result_hash = str(row.get("result_hash") or "").strip().lower()
        if (
            re.fullmatch(r"[0-9a-f]{32}", run_uid) is None
            or re.fullmatch(r"[0-9a-f]{64}", result_hash) is None
            or row.get("finished_at") is None
        ):
            raise RuntimeError(
                "canonical governance completion receipt is invalid"
            )
        completed_by_date[trade_date_value] = row

    # A canonical completion is a monotonic delivery watermark.  Dates before
    # the newest valid watermark may predate this pipeline or be intentionally
    # outside its governed history; their absence is not authority to invent a
    # backfill.  Recover only the contiguous sessions after that watermark.
    # With no watermark, run the latest authoritative session unless a future
    # extension supplies a separate durable requested-target receipt.
    if not completed_by_date:
        return latest_target
    watermark = max(completed_by_date)
    for trade_date_value in normalized_dates:
        if (
            trade_date_value > watermark
            and trade_date_value not in completed_by_date
        ):
            return trade_date_value
    # Keeping the latest completed target attached is intentional: ordinary
    # cron idempotency can still prove that no work is due, while the scheduler
    # never falls back to an unbound host-calendar date.
    return latest_target


def _daily_result_recovery_target(
    engine,
    *,
    now: datetime,
) -> str:
    """Resolve one durable backlog target for the complete daily-result DAG."""

    current = now
    if current.tzinfo is not None:
        current = current.astimezone(PRODUCTION_TIMEZONE).replace(tzinfo=None)
    latest_target = authoritative_closed_trade_date(
        engine,
        now=current,
        close_ready_time=DAILY_RESULT_RECOVERY_TARGET_READY_TIME,
    )
    try:
        latest_date = date.fromisoformat(str(latest_target or ""))
    except ValueError as exc:
        raise RuntimeError(
            "daily-result authoritative target is unavailable"
        ) from exc
    if latest_date.isoformat() != latest_target:
        raise RuntimeError(
            "daily-result authoritative target is unavailable"
        )
    window_start = latest_date - timedelta(
        days=DAILY_RESULT_RECOVERY_MAX_AGE_DAYS
    )
    with engine.connect() as connection:
        trade_dates = [
            str(row.get("trade_date") or "")[:10]
            for row in connection.execute(
                text(
                    "SELECT trade_date FROM si_trade_calendar "
                    "WHERE trade_status=1 "
                    "AND trade_date BETWEEN :window_start AND :latest_target "
                    "ORDER BY trade_date"
                ),
                {
                    "window_start": window_start.isoformat(),
                    "latest_target": latest_target,
                },
            ).mappings()
        ]
        governance_rows = [
            dict(row)
            for row in connection.execute(
                text(
                    "SELECT run_uid, trade_date, status, is_canonical, "
                    "result_hash, finished_at FROM st_strategy_governance_run "
                    "WHERE trade_date BETWEEN :window_start AND :latest_target "
                    "AND is_canonical=1 "
                    "ORDER BY trade_date, run_revision, created_at"
                ),
                {
                    "window_start": window_start.isoformat(),
                    "latest_target": latest_target,
                },
            ).mappings()
        ]
    return _select_daily_result_recovery_target(
        trade_dates,
        governance_rows,
        latest_target=latest_target,
    )


def _release_history_evidence_valid(row: dict, build_sha: str) -> bool:
    """Validate enough persisted identity to decide whether catch-up is due.

    The full hashes and data proof are revalidated by the final readiness gate;
    this smaller check only prevents a status-only row from suppressing the
    new-build replay that could create that proof.
    """

    source = str(row.get("_release_terminal_output") or "")
    candidates = []
    for line in source.splitlines():
        candidate = line.strip()
        if not candidate.startswith("{"):
            continue
        try:
            payload = json.loads(candidate)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if (
            isinstance(payload, dict)
            and payload.get("schema") == _HISTORY_EVIDENCE_SCHEMA
        ):
            candidates.append(payload)
    if len(candidates) != 1:
        return False
    evidence = candidates[0]
    if row.get("_release_expected_target_required") is True:
        if row.get("_release_expected_target_available") is not True:
            return False
        expected_target = str(
            row.get("_release_expected_target_date") or ""
        ).strip()
        evidence_target = str(evidence.get("release_target_date") or "").strip()
        try:
            parsed_expected = date.fromisoformat(expected_target)
            parsed_evidence = date.fromisoformat(evidence_target)
        except ValueError:
            return False
        if (
            parsed_expected.isoformat() != expected_target
            or parsed_evidence.isoformat() != evidence_target
            or evidence_target != expected_target
        ):
            return False
    evidence_task_id = evidence.get("task_id")
    evidence_exit_code = evidence.get("exit_code")
    terminal_exit_code = row.get("_release_terminal_exit_code")
    if (
        isinstance(evidence_task_id, bool)
        or not isinstance(evidence_task_id, int)
        or isinstance(evidence_exit_code, bool)
        or not isinstance(evidence_exit_code, int)
        or isinstance(terminal_exit_code, bool)
        or not isinstance(terminal_exit_code, int)
    ):
        return False
    supplied_hash = str(evidence.get("evidence_sha256") or "").lower()
    core = dict(evidence)
    core.pop("evidence_sha256", None)
    canonical_core = json.dumps(
        core,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    replay_output = str(evidence.get("replay_output") or "")
    return (
        re.fullmatch(r"[0-9a-f]{64}", supplied_hash) is not None
        and _history_digest(canonical_core) == supplied_hash
        and re.fullmatch(
            r"[0-9a-f]{64}",
            str(evidence.get("machine_output_sha256") or "").lower(),
        )
        is not None
        and str(evidence.get("replay_output_sha256") or "").lower()
        == _history_digest(replay_output)
        and str(row.get("_release_terminal_status") or "") == "success"
        and str(row.get("_release_terminal_build_sha") or "").lower()
        == build_sha
        and terminal_exit_code == 0
        and str(evidence.get("run_uid") or "")
        == str(row.get("_release_terminal_run_uid") or "")
        and evidence_task_id == int(row.get("id") or 0)
        and str(evidence.get("task_type") or "")
        == str(row.get("task_type") or "")
        and str(evidence.get("build_sha") or "").lower() == build_sha
        and evidence.get("status") == "success"
        and evidence_exit_code == 0
        and evidence.get("validation_checked") is True
        and evidence.get("validation_ok") is True
    )


def _attach_release_catchup_history(engine, rows: list[dict]) -> bool:
    """Attach each readiness task's latest terminal audit row using SELECT only."""

    selected = [
        row
        for row in rows
        if str(row.get("task_type") or "").strip()
        in RELEASE_DATA_CATCHUP_TASK_TYPES
    ]
    for row in selected:
        row["_release_history_available"] = False
    if not selected:
        return True
    task_pairs = sorted(
        {
            (int(row["id"]), str(row.get("task_type") or "").strip())
            for row in selected
        }
    )
    pair_predicates = " OR ".join(
        "(task_id=:release_task_id_"
        f"{index} AND task_type=:release_task_type_{index})"
        for index, _ in enumerate(task_pairs)
    )
    params = {}
    for index, (task_id, task_type) in enumerate(task_pairs):
        params.update(
            {
                f"release_task_id_{index}": task_id,
                f"release_task_type_{index}": task_type,
            }
        )
    statement = text(f"""
        SELECT history.id, history.run_uid, history.task_id, history.task_type,
               history.status,
               history.build_sha, history.run_at, history.finished_at,
               history.exit_code, history.output, history.trigger_source
          FROM st_scheduled_task_history AS history
          JOIN (
                SELECT task_id, MAX(id) AS latest_id
                 FROM st_scheduled_task_history
                 WHERE ({pair_predicates})
                   AND status IN ('success','blocked','failed','timeout','stopped')
                 GROUP BY task_id, task_type
               ) AS latest
            ON latest.latest_id=history.id
         ORDER BY history.task_id
    """)
    try:
        with engine.connect() as connection:
            history_rows = {
                (int(item["task_id"]), str(item.get("task_type") or "")): dict(item)
                for item in connection.execute(statement, params).mappings()
            }
    except Exception as exc:
        logger.error(
            "Release data catch-up history is unavailable; fail closed: %s",
            type(exc).__name__,
        )
        return False
    for row in selected:
        history = history_rows.get(
            (int(row["id"]), str(row.get("task_type") or "").strip()),
            {},
        )
        row.update(
            {
                "_release_history_available": True,
                "_release_terminal_id": history.get("id"),
                "_release_terminal_run_uid": history.get("run_uid"),
                "_release_terminal_status": str(history.get("status") or ""),
                "_release_terminal_build_sha": str(
                    history.get("build_sha") or ""
                ).lower(),
                "_release_terminal_run_at": history.get("run_at"),
                "_release_terminal_finished_at": history.get("finished_at"),
                "_release_terminal_exit_code": history.get("exit_code"),
                "_release_terminal_output": history.get("output"),
                "_release_terminal_trigger_source": str(
                    history.get("trigger_source") or ""
                ).strip(),
            }
        )
    return True


def _load_local_release_health(*, timeout_seconds: int = 5) -> dict:
    request = Request(
        "http://127.0.0.1/api/health",
        headers={"Accept": "application/json", "User-Agent": "probiga-scheduler/1"},
        method="GET",
    )
    with urlopen(request, timeout=max(1, int(timeout_seconds))) as response:
        content_length = response.headers.get("Content-Length")
        if content_length and int(content_length) > 1024 * 1024:
            raise RuntimeError("active release health response is too large")
        raw = response.read(1024 * 1024 + 1)
    if len(raw) > 1024 * 1024:
        raise RuntimeError("active release health response is too large")
    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("active release health response is invalid")
    return payload


def _linux_active_release_ready(
    build_sha: str,
    *,
    health_loader=_load_local_release_health,
    active_code_root_loader=lambda: Path("/opt/ProBigA-current").resolve(
        strict=True
    ),
) -> tuple[bool, str]:
    """Require the exact healthy API and static link before Linux catch-up."""

    expected_root = f"/opt/ProBigA-releases/{build_sha}"
    if os.name != "posix":
        return False, "linux_runtime_required"
    if str(os.environ.get("PROBIGA_CODE_ROOT") or "").replace("\\", "/") != expected_root:
        return False, "code_root_mismatch"
    try:
        active_root = str(active_code_root_loader()).replace("\\", "/")
        health = health_loader()
    except Exception as exc:
        return False, f"active_health_unavailable:{type(exc).__name__}"
    revision = health.get("release_revision")
    scheduler_heartbeat = health.get("standalone_scheduler_heartbeat")
    heartbeat_detail = (
        scheduler_heartbeat.get("detail")
        if isinstance(scheduler_heartbeat, dict)
        else None
    )
    if active_root != expected_root:
        return False, "active_link_mismatch"
    if not isinstance(revision, dict):
        return False, "release_revision_missing"
    if (
        health.get("status") != "ok"
        or revision.get("deployment_mode") != "production"
        or revision.get("matches_expected") is not True
        or revision.get("code_worktree_clean") is not True
        or str(revision.get("expected_git_sha") or "").lower() != build_sha
        or str(revision.get("actual_git_sha") or "").lower() != build_sha
        or not isinstance(scheduler_heartbeat, dict)
        or scheduler_heartbeat.get("ready") is not True
        or not isinstance(heartbeat_detail, dict)
        or str(heartbeat_detail.get("expected_build_sha") or "").lower()
        != build_sha
        or health.get("automatic_real_order_submission") is not False
        or health.get("real_order_authority") is not False
    ):
        return False, "active_health_identity_mismatch"
    return True, "ready"


def _publish_linux_release_activation(
    engine,
    *,
    build_sha: str,
    anchor_task_id: int,
    activated_at: datetime,
) -> tuple[bool, str]:
    """Append one canonical active-release receipt for this Linux lease."""

    started_at = _scheduler_started_at.replace(microsecond=0)
    receipt = build_release_data_activation_receipt(
        build_sha=build_sha,
        scheduler_instance_id=_scheduler_instance_id,
        scheduler_host_name=gethostname(),
        scheduler_pid=os.getpid(),
        scheduler_started_at=started_at,
        activated_at=activated_at.replace(microsecond=0),
    )
    run_uid = release_data_activation_run_uid(
        build_sha,
        _scheduler_instance_id,
        started_at,
    )
    serialized = json.dumps(
        receipt,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    try:
        with engine.begin() as connection:
            existing = [
                dict(row)
                for row in connection.execute(
                    text(
                        "SELECT run_uid, task_id, task_type, status, exit_code, "
                        "output, host_name, scheduler_instance_id, build_sha, "
                        "trigger_source FROM st_scheduled_task_history "
                        "WHERE run_uid=:run_uid"
                    ),
                    {"run_uid": run_uid},
                ).mappings()
            ]
            if existing:
                if len(existing) != 1:
                    return False, "activation_receipt_not_unique"
                row = existing[0]
                validate_release_data_activation_receipt(
                    str(row.get("output") or ""),
                    expected_build_sha=build_sha,
                    expected_scheduler_instance_id=_scheduler_instance_id,
                )
                if (
                    str(row.get("run_uid") or "") != run_uid
                    or int(row.get("task_id") or 0) != int(anchor_task_id)
                    or str(row.get("task_type") or "")
                    != RELEASE_DATA_ACTIVATION_TASK_TYPE
                    or str(row.get("status") or "") != "success"
                    or int(
                        row.get("exit_code")
                        if row.get("exit_code") is not None
                        else -1
                    )
                    != 0
                    or str(row.get("host_name") or "") != gethostname()
                    or str(row.get("scheduler_instance_id") or "")
                    != _scheduler_instance_id
                    or str(row.get("build_sha") or "").lower() != build_sha
                    or str(row.get("trigger_source") or "")
                    != RELEASE_DATA_ACTIVATION_TRIGGER_SOURCE
                ):
                    return False, "activation_receipt_row_mismatch"
                return True, "idempotent"
            connection.execute(
                text(
                    "INSERT INTO st_scheduled_task_history ("
                    "run_uid, task_id, task_name, task_type, run_at, "
                    "finished_at, status, duration, exit_code, output, "
                    "host_name, scheduler_instance_id, build_sha, "
                    "trigger_source) VALUES ("
                    ":run_uid, :task_id, 'Release data activation', "
                    ":task_type, :activated_at, :activated_at, 'success', 0, "
                    "0, :output, :host_name, :scheduler_instance_id, "
                    ":build_sha, :trigger_source)"
                ),
                {
                    "run_uid": run_uid,
                    "task_id": int(anchor_task_id),
                    "task_type": RELEASE_DATA_ACTIVATION_TASK_TYPE,
                    "activated_at": receipt["activated_at"].replace("T", " "),
                    "output": serialized,
                    "host_name": gethostname(),
                    "scheduler_instance_id": _scheduler_instance_id,
                    "build_sha": build_sha,
                    "trigger_source": RELEASE_DATA_ACTIVATION_TRIGGER_SOURCE,
                },
            )
    except Exception as exc:
        return False, f"activation_publish_failed:{type(exc).__name__}"
    return True, "inserted"


def _windows_release_activation_ready(
    engine,
    *,
    build_sha: str,
) -> tuple[bool, str]:
    """Require current Linux activation and QMT bootstrap on Windows."""

    try:
        expected_poll_seconds = int(
            get_scheduler_runtime_config()["poll_seconds"]
        )
        with engine.connect() as connection:
            linux_ready, linux_detail = check_linux_standalone_active_release(
                connection,
                expected_build_sha=build_sha,
                expected_poll_seconds=expected_poll_seconds,
            )
            current = linux_detail.get("current") if linux_ready else None
            if not isinstance(current, dict):
                return False, "linux_active_lease_unavailable"
            activation_rows = [
                dict(row)
                for row in connection.execute(
                    text(
                        "SELECT run_uid, task_type, status, exit_code, output, "
                        "host_name, scheduler_instance_id, build_sha, "
                        "trigger_source FROM st_scheduled_task_history "
                        "WHERE task_type=:task_type AND build_sha=:build_sha "
                        "AND scheduler_instance_id=:scheduler_instance_id "
                        "AND status='success' AND exit_code=0 "
                        "ORDER BY finished_at DESC, id DESC"
                    ),
                    {
                        "task_type": RELEASE_DATA_ACTIVATION_TASK_TYPE,
                        "build_sha": build_sha,
                        "scheduler_instance_id": current["instance_id"],
                    },
                ).mappings()
            ]
            if len(activation_rows) != 1:
                return False, "linux_activation_receipt_not_unique"
            activation_row = activation_rows[0]
            receipt = validate_release_data_activation_receipt(
                str(activation_row.get("output") or ""),
                expected_build_sha=build_sha,
                expected_scheduler_instance_id=str(current["instance_id"]),
            )
            if (
                str(activation_row.get("task_type") or "")
                != RELEASE_DATA_ACTIVATION_TASK_TYPE
                or str(activation_row.get("status") or "") != "success"
                or int(
                    activation_row.get("exit_code")
                    if activation_row.get("exit_code") is not None
                    else -1
                )
                != 0
                or str(activation_row.get("host_name") or "")
                != str(current.get("host_name") or "")
                or str(activation_row.get("scheduler_instance_id") or "")
                != str(current.get("instance_id") or "")
                or str(activation_row.get("build_sha") or "").lower()
                != build_sha
                or str(activation_row.get("trigger_source") or "")
                != RELEASE_DATA_ACTIVATION_TRIGGER_SOURCE
                or receipt["scheduler_started_at"]
                != str(current.get("started_at") or "")
            ):
                return False, "linux_activation_receipt_mismatch"
            qmt_ready, _qmt_detail = check_qmt_windows_edge_release_receipt(
                connection,
                expected_build_sha=build_sha,
                expected_poll_seconds=expected_poll_seconds,
            )
            if not qmt_ready:
                return False, "qmt_release_bootstrap_unavailable"
    except Exception as exc:
        return False, f"release_activation_check_failed:{type(exc).__name__}"
    return True, "ready"


def _attach_release_catchup_authorization(
    engine,
    rows: list[dict],
    *,
    mode: str,
    now: datetime,
) -> tuple[bool, str]:
    """Attach one host-wide active-release decision before task sorting."""

    selected = [
        row
        for row in rows
        if str(row.get("task_type") or "").strip()
        in RELEASE_DATA_CATCHUP_TASK_TYPES
    ]
    for row in selected:
        row["_release_catchup_authorized"] = False
    if not selected:
        return True, "not_applicable"
    if _release_catchup_disabled_for_deferred_database():
        return False, "governance_database_deferred"
    build_sha = _scheduler_build_commit_sha()
    if build_sha == "0" * 40:
        return False, "build_identity_unavailable"
    role = _scheduler_executor_role(mode)
    if role == "linux_standalone":
        ready, reason = _linux_active_release_ready(build_sha)
        if ready:
            ready, reason = _publish_linux_release_activation(
                engine,
                build_sha=build_sha,
                anchor_task_id=min(int(row["id"]) for row in selected),
                activated_at=now,
            )
    elif role == "qmt_windows_edge":
        ready, reason = _windows_release_activation_ready(
            engine,
            build_sha=build_sha,
        )
    else:
        ready, reason = False, "executor_role_unclassified"
    if ready:
        for row in selected:
            row["_release_catchup_authorized"] = True
    return ready, reason


def _qmt_windows_dispatch_preflight(
    engine,
    *,
    mode: str,
) -> tuple[bool, str]:
    """Require the exact live QMT release receipt before business dispatch.

    The updater starts the scheduler so its build-bound heartbeat can be used
    to create the release receipt.  During that bootstrap interval the
    scheduler must publish only its heartbeat; no ordinary or catch-up QMT job
    may escape before the Linux activation and exact Windows receipt agree.
    """

    if _scheduler_executor_role(mode) != SCHEDULER_OWNER_WINDOWS_QMT:
        return True, "not_applicable"
    build_sha = _scheduler_build_commit_sha()
    if build_sha == "0" * 40:
        return False, "build_identity_unavailable"
    return _windows_release_activation_ready(engine, build_sha=build_sha)


def _release_build_catchup_allowed(row: dict, *, now: datetime) -> bool:
    """Return whether this host should replay an exact task for the new build."""

    if _release_catchup_disabled_for_deferred_database():
        return False
    task_type = str(row.get("task_type") or "").strip()
    if (
        task_type not in RELEASE_DATA_CATCHUP_TASK_TYPES
        or row.get("_release_history_available") is not True
        or row.get("_release_catchup_authorized") is not True
        or (
            row.get("_release_expected_target_required") is True
            and row.get("_release_expected_target_available") is not True
        )
    ):
        return False
    if task_type in (ANALYSIS_DAILY_EVIDENCE_TASK_TYPES | {"analysis_fast"}):
        current = now
        if current.tzinfo is not None:
            current = current.astimezone(PRODUCTION_TIMEZONE).replace(tzinfo=None)
        expected_target = str(
            row.get("_release_expected_target_date") or ""
        )[:10]
        target_is_prior_session = False
        try:
            target_is_prior_session = (
                date.fromisoformat(expected_target) < current.date()
            )
        except ValueError:
            target_is_prior_session = False
        if (
            not target_is_prior_session
            and current.time() < release_catchup_closed_ready_time(task_type)
        ):
            return False
    build_sha = _scheduler_build_commit_sha()
    if not re.fullmatch(r"[0-9a-f]{40}", build_sha) or build_sha == "0" * 40:
        return False
    if _release_history_evidence_valid(row, build_sha):
        return False
    terminal_build = str(row.get("_release_terminal_build_sha") or "").lower()
    terminal_status = str(row.get("_release_terminal_status") or "").lower()
    if terminal_build != build_sha or not terminal_status:
        return True
    # A terminal success with invalid evidence is not a failed attempt that
    # needs backoff.  This includes the deliberate 18:00 target rollover: run
    # the new closed session immediately instead of waiting behind the old
    # target's otherwise successful receipt.
    if terminal_status == "success":
        return True
    if terminal_status == "blocked":
        terminal_output = str(row.get("_release_terminal_output") or "")
        retryable_block = (
            terminal_output.lstrip().startswith("DATA_BLOCKED:")
            or _retryable_blocked_output(terminal_output)
        )
        explicitly_terminal = any(
            payload.get("retryable") is False
            for payload in _iter_scheduler_output_payloads(terminal_output)
        )
        if explicitly_terminal and not retryable_block:
            # Explicit policy blocks and lawful no-data receipts are terminal.
            # Older blocked history rows predate the retryable field, so keep
            # their bounded retry behavior instead of stranding a release.
            return False
    retry_reference = _coerce_datetime(
        row.get("_release_terminal_finished_at")
        or row.get("_release_terminal_run_at")
    )
    if retry_reference is None:
        return True
    retry_minutes = (
        RELEASE_CATCHUP_BLOCKED_RETRY_INTERVAL_MINUTES
        if terminal_status == "blocked"
        else RELEASE_CATCHUP_RETRY_INTERVAL_MINUTES
    )
    return (now - retry_reference).total_seconds() >= retry_minutes * 60


def _release_build_catchup_pending(row: dict) -> bool:
    """Fail closed while a release task lacks exact-build success evidence.

    Authorization/history outages must not turn ``release_catchup_due`` false
    and accidentally let the same row run through its ordinary cron path.
    """

    if _release_catchup_disabled_for_deferred_database():
        return False
    if (
        str(row.get("task_type") or "").strip()
        not in RELEASE_DATA_CATCHUP_TASK_TYPES
    ):
        return False
    build_sha = _scheduler_build_commit_sha()
    if not re.fullmatch(r"[0-9a-f]{40}", build_sha) or build_sha == "0" * 40:
        return True
    return not _release_history_evidence_valid(row, build_sha)


def _release_catchup_disabled_for_deferred_database() -> bool:
    """Disable build replay while a release intentionally defers DB cutover.

    DEFERRED_DB has no release request or activation receipt by design.  Its
    writer fences remain enforced elsewhere; ordinary non-governance data jobs
    must keep their normal interval/cron behavior instead of waiting forever
    for release evidence that this deployment mode cannot create.
    """

    try:
        return strategy_governance_database_deferred()
    except StrategyGovernanceModeError:
        # Preserve the existing fail-closed release behavior for an invalid
        # mode.  Only the exact, explicitly configured DEFERRED_DB mode opts
        # out of release replay.
        return False


def _release_catchup_dependencies_ready(
    row: dict,
    rows: list[dict],
) -> tuple[bool, str]:
    """Require exact-build validated upstream histories before downstream replay."""

    task_type = str(row.get("task_type") or "").strip()
    dependencies = RELEASE_DATA_CATCHUP_DEPENDENCIES.get(task_type, ())
    if not dependencies:
        return True, "not_applicable"
    grouped: dict[str, list[dict]] = {}
    for candidate in rows:
        grouped.setdefault(
            str(candidate.get("task_type") or "").strip(), []
        ).append(candidate)
    build_sha = _scheduler_build_commit_sha()
    downstream_target = str(
        row.get("_release_expected_target_date") or ""
    ).strip()
    for dependency in dependencies:
        matches = grouped.get(dependency, [])
        if len(matches) != 1:
            return False, f"{dependency}:missing_or_duplicate"
        upstream = matches[0]
        if not _release_history_evidence_valid(upstream, build_sha):
            return False, f"{dependency}:exact_build_not_ready"
        if (
            task_type
            in (ANALYSIS_DAILY_EVIDENCE_TASK_TYPES | {"analysis_fast"})
            and upstream.get("_release_expected_target_required") is True
        ):
            upstream_target = str(
                upstream.get("_release_expected_target_date") or ""
            ).strip()
            if (
                not downstream_target
                or upstream_target != downstream_target
            ):
                return False, f"{dependency}:target_date_mismatch"
    return True, "ready"


def _parse_hhmm(value: str) -> int | None:
    try:
        hour, minute = str(value or "").strip().split(":")
        hour_i = int(hour)
        minute_i = int(minute)
    except (TypeError, ValueError):
        return None
    if not (0 <= hour_i <= 23 and 0 <= minute_i <= 59):
        return None
    return hour_i * 60 + minute_i


def _ordinary_cron_required_after_early_release(
    row: dict,
    *,
    now: datetime,
    cron_time: str,
) -> bool:
    """Do not let a pre-cron release replay replace today's scheduled run.

    Release catch-up proves that the new build can publish the data available
    at deployment time.  For close-derived and continuously disclosed feeds,
    a successful replay before the task's ordinary wall-clock deadline is not
    proof that the later daily source window was captured.
    """

    if (
        str(row.get("task_type") or "").strip()
        not in RELEASE_DATA_CATCHUP_TASK_TYPES
        or str(row.get("_release_terminal_trigger_source") or "").strip()
        != "release_catchup"
        or str(row.get("_release_terminal_status") or "").strip().lower()
        != "success"
    ):
        return False
    cron_min = _parse_hhmm(cron_time)
    release_run_at = _coerce_datetime(row.get("_release_terminal_run_at"))
    if (
        cron_min is None
        or release_run_at is None
        or release_run_at.date() != now.date()
    ):
        return False
    release_min = release_run_at.hour * 60 + release_run_at.minute
    return release_min < cron_min


def _cron_catchup_allowed(*, now: datetime, cron_time: str, startup_time: datetime) -> bool:
    if CRON_CATCHUP_WINDOW_SECONDS <= 0:
        return False
    cron_min = _parse_hhmm(cron_time)
    if cron_min is None:
        return False
    current_min = now.hour * 60 + now.minute
    missed_seconds = (current_min - cron_min) * 60
    startup_age_seconds = (now - startup_time).total_seconds()
    return 0 < missed_seconds <= CRON_CATCHUP_WINDOW_SECONDS and startup_age_seconds <= CRON_CATCHUP_WINDOW_SECONDS


def _critical_cron_catchup_allowed(row: dict, *, now: datetime, cron_time: str) -> bool:
    task_type = str(row.get("task_type") or "").strip()
    if task_type not in CRITICAL_CRON_CATCHUP_TASK_TYPES:
        return False
    catchup_window = CRITICAL_CRON_CATCHUP_WINDOWS_SECONDS.get(
        task_type,
        (
        RECOMMENDATION_CRON_CATCHUP_WINDOW_SECONDS
        if task_type in {"analysis_fast", "sim_trade_signal_prepare"}
        else PREMARKET_RECOMMENDATION_CATCHUP_WINDOW_SECONDS
        if task_type == "analysis_premarket_external"
        else CRITICAL_CRON_CATCHUP_WINDOW_SECONDS
        ),
    )
    if catchup_window <= 0:
        return False
    last_triggered = _coerce_datetime(row.get("last_triggered_at"))
    early_release_needs_ordinary = _ordinary_cron_required_after_early_release(
        row,
        now=now,
        cron_time=cron_time,
    )
    if (
        last_triggered
        and last_triggered.date() == now.date()
        and not early_release_needs_ordinary
    ):
        if not _task_status_is_retryable(row):
            return False
        retry_at = _cron_retry_reference(row, fallback=last_triggered)
        if (
            now - retry_at
        ).total_seconds() < CRON_RETRY_INTERVAL_MINUTES * 60:
            return False
    cron_min = _parse_hhmm(cron_time)
    if cron_min is None:
        return False
    if row.get("_dependency_recovery_due") is True:
        last_triggered = _coerce_datetime(row.get("last_triggered_at"))
        if last_triggered is None:
            return True
        dependency_latest = _coerce_datetime(
            row.get("_dependency_latest_at")
        )
        attempted_current_inputs = (
            dependency_latest is not None
            and last_triggered >= dependency_latest
            and _row_matches_target_trade_date(
                row,
                str(row.get("_scheduler_target_trade_date") or ""),
            )
        )
        if attempted_current_inputs and not _task_status_is_retryable(row):
            return False
        if not attempted_current_inputs:
            return True
        retry_at = _cron_retry_reference(
            row,
            fallback=last_triggered or now,
        )
        return (
            now - retry_at
        ).total_seconds() >= CRON_RETRY_INTERVAL_MINUTES * 60
    if _prior_target_recovery_allowed(row, now=now):
        last_triggered = _coerce_datetime(row.get("last_triggered_at"))
        if last_triggered and _row_matches_target_trade_date(
            row,
            str(row.get("_scheduler_target_trade_date") or ""),
        ):
            if not _task_status_is_retryable(row):
                return False
            retry_at = _cron_retry_reference(row, fallback=last_triggered)
            if (
                now - retry_at
            ).total_seconds() < CRON_RETRY_INTERVAL_MINUTES * 60:
                return False
        return True
    current_min = now.hour * 60 + now.minute
    missed_seconds = (current_min - cron_min) * 60
    return 0 < missed_seconds <= catchup_window


def _cron_retry_reference(row: dict, *, fallback: datetime) -> datetime:
    """Approximate completion time from persisted start plus duration."""
    started_at = _coerce_datetime(row.get("last_run_at")) or fallback
    try:
        duration_seconds = max(0, int(row.get("last_run_duration") or 0))
    except (TypeError, ValueError):
        duration_seconds = 0
    return started_at + timedelta(seconds=duration_seconds)


def _overdue_cron_allowed(
    row: dict,
    *,
    now: datetime,
    cron_time: str,
    startup_time: datetime,
) -> bool:
    """Allow bounded restart catch-up instead of replaying the whole day."""
    if now.strftime("%H:%M") == cron_time:
        return True
    return _cron_catchup_allowed(
        now=now,
        cron_time=cron_time,
        startup_time=startup_time,
    ) or _critical_cron_catchup_allowed(row, now=now, cron_time=cron_time)


def _cron_due(row: dict, *, now: datetime) -> bool:
    """Return whether a daily cron task is due, including missed runs.

    Cron jobs are persisted by date rather than by the scheduler process uptime.
    This lets a restart at 10:00 still run a missed 08:30 job once, while failed
    or timed-out jobs can be retried later the same day.
    """
    cron_min = _parse_hhmm(str(row.get("cron_time") or "17:10"))
    if cron_min is None:
        return False
    target = _row_recovery_target(row, now=now)
    prior_target_recovery = _prior_target_recovery_allowed(row, now=now)
    current_min = now.hour * 60 + now.minute
    if current_min < cron_min and not prior_target_recovery:
        return False

    last_triggered = _coerce_datetime(row.get("last_triggered_at"))
    if last_triggered and last_triggered.date() > now.date():
        return False
    if row.get("_dependency_recovery_due") is True:
        dependency_latest = _coerce_datetime(
            row.get("_dependency_latest_at")
        )
        attempted_current_inputs = (
            last_triggered is not None
            and dependency_latest is not None
            and last_triggered >= dependency_latest
            and _row_matches_target_trade_date(row, target.isoformat())
        )
        if not attempted_current_inputs:
            return True
        if not _task_status_is_retryable(row):
            return False
        retry_at = _cron_retry_reference(row, fallback=last_triggered)
        return (
            now - retry_at
        ).total_seconds() >= CRON_RETRY_INTERVAL_MINUTES * 60
    if prior_target_recovery:
        if not last_triggered or not _row_matches_target_trade_date(
            row,
            target.isoformat(),
        ):
            return True
        if not _task_status_is_retryable(row):
            return False
        retry_at = _cron_retry_reference(row, fallback=last_triggered)
        return (
            now - retry_at
        ).total_seconds() >= CRON_RETRY_INTERVAL_MINUTES * 60
    if not last_triggered or last_triggered.date() < now.date():
        return True

    if _ordinary_cron_required_after_early_release(
        row,
        now=now,
        cron_time=str(row.get("cron_time") or "17:10"),
    ):
        return True

    if not _task_status_is_retryable(row):
        return False
    retry_at = _cron_retry_reference(row, fallback=last_triggered)
    return (now - retry_at).total_seconds() >= CRON_RETRY_INTERVAL_MINUTES * 60


def _intraday_window_minutes() -> tuple[int, int]:
    start = _parse_hhmm(os.environ.get("SCHEDULER_INTRADAY_START", "09:15")) or (9 * 60 + 15)
    end = _parse_hhmm(os.environ.get("SCHEDULER_INTRADAY_END", "15:10")) or (15 * 60 + 10)
    return start, end


def _should_skip_outside_intraday_window(row: dict, now: datetime) -> bool:
    task_type = str(row.get("task_type") or "").strip()
    script_path = str(row.get("script_path") or "").replace("\\", "/").strip()
    # One script can serve both intraday incremental and post-close full-market
    # tasks (notably crawl_minute_kline.py).  An explicit task type is therefore
    # authoritative; use the path only for legacy rows without a task type.
    is_intraday = task_type in INTRADAY_WINDOW_TASK_TYPES or (
        not task_type and script_path in INTRADAY_WINDOW_PATH_PARTS
    )
    if not is_intraday:
        return False
    start, end = _intraday_window_minutes()
    if task_type in ALERT_LANE_TASK_TYPES:
        configured_start = _parse_hhmm(str(row.get("cron_time") or ""))
        if configured_start is not None:
            start = max(start, configured_start)
    current = now.hour * 60 + now.minute
    return current < start or current > end


def _contract_path_matches(row: dict, contract: dict) -> bool:
    script_path = str(row.get("script_path") or "").strip().replace("\\", "/")
    # Some ownership-only callers intentionally select task_type without the
    # payload.  A present path, however, must match the frozen identity.
    if script_path and script_path != str(contract["script_path"]):
        return False
    # Ownership-only callers omit payload fields.  A persisted scheduler row
    # includes them, so any present argument string must match exactly; a
    # drifted row may not inherit the provider identity from task_type alone.
    if "script_args" in contract and "script_args" in row:
        script_args = str(row.get("script_args") or "").strip()
        if script_args != str(contract.get("script_args") or "").strip():
            return False
    return True


def scheduler_task_host_owner(row: dict) -> str:
    """Resolve one exact executor or return ``unavailable`` fail-closed."""

    task_type = str(row.get("task_type") or "").strip()
    script_path = str(row.get("script_path") or "").strip().replace("\\", "/")
    group_name = str(row.get("group_name") or "").strip().casefold()
    if not task_type:
        return SCHEDULER_OWNER_UNAVAILABLE
    if task_type in WINDOWS_QMT_EDGE_TASK_TYPES:
        contract = WINDOWS_QMT_EDGE_TASKS_BY_TYPE[task_type]
        return (
            SCHEDULER_OWNER_WINDOWS_QMT
            if _contract_path_matches(row, contract)
            else SCHEDULER_OWNER_UNAVAILABLE
        )
    if task_type in LINUX_QMT_TASK_TYPES:
        contract = LINUX_QMT_TASKS_BY_TYPE[task_type]
        return (
            SCHEDULER_OWNER_LINUX
            if _contract_path_matches(row, contract)
            else SCHEDULER_OWNER_UNAVAILABLE
        )
    if task_type in LINUX_PROVIDER_TASK_TYPES:
        contract = LINUX_PROVIDER_TASKS_BY_TYPE[task_type]
        return (
            SCHEDULER_OWNER_LINUX
            if _contract_path_matches(row, contract)
            else SCHEDULER_OWNER_UNAVAILABLE
        )
    if task_type in WINDOWS_NON_QMT_EGRESS_TASK_TYPES:
        contract = WINDOWS_NON_QMT_EGRESS_TASKS_BY_TYPE[task_type]
        return (
            SCHEDULER_OWNER_WINDOWS_EGRESS
            if _contract_path_matches(row, contract)
            else SCHEDULER_OWNER_UNAVAILABLE
        )
    # The legacy crawler path is quarantined for generic/aliased rows.  The
    # two explicitly named intraday public-source jobs retain their Linux
    # ownership; their task types, arguments and post-run validators are
    # independent of the canonical QMT close publishers.
    frozen_script_alias = (
        script_path in UNFROZEN_PROVIDER_SCRIPT_PATHS
        and task_type not in {"intraday_minute_kline", "intraday_minute_flow"}
    )
    if task_type in UNFROZEN_PROVIDER_TASK_TYPES or frozen_script_alias:
        return SCHEDULER_OWNER_UNAVAILABLE
    if (
        task_type.startswith("qmt_")
        or "qmt" in group_name
        or "qmt" in script_path.casefold()
        or script_path in WINDOWS_QMT_BRIDGE_SCRIPT_PATHS
    ):
        return SCHEDULER_OWNER_UNAVAILABLE
    return SCHEDULER_OWNER_LINUX


def _is_windows_qmt_bridge_task(row: dict) -> bool:
    return scheduler_task_host_owner(row) in {
        SCHEDULER_OWNER_WINDOWS_QMT,
        SCHEDULER_OWNER_WINDOWS_EGRESS,
    }


def _should_delegate_to_windows_qmt_bridge(
    row: dict,
    *,
    platform_name: str | None = None,
) -> bool:
    current_platform = platform_name or os.name
    return current_platform == "posix" and _is_windows_qmt_bridge_task(row)


def _should_skip_task_for_host(
    row: dict,
    *,
    platform_name: str | None = None,
) -> bool:
    """Run only tasks bound to this host; ambiguous identities never run."""

    current_platform = platform_name or os.name
    owner = scheduler_task_host_owner(row)
    if owner == SCHEDULER_OWNER_UNAVAILABLE:
        return True
    if current_platform == "nt":
        return owner not in {
            SCHEDULER_OWNER_WINDOWS_QMT,
            SCHEDULER_OWNER_WINDOWS_EGRESS,
        }
    if current_platform == "posix":
        return owner != SCHEDULER_OWNER_LINUX
    return True


def scheduler_task_owned_by_current_host(row: dict) -> bool:
    """Return whether this host may manually control the task process."""

    return not _should_skip_task_for_host(row)


_PIPELINE_TERMINAL_STATUSES = frozenset(
    {"success", "blocked", "failed", "timeout", "stopped"}
)
_HOT_RANK_PIPELINE_DEPENDENCIES: dict[str, tuple[str, ...]] = {
    "hot_fused": ("hot_rank_ths", "hot_pop_east"),
    "hot_fused_3": ("hot_fused",),
    "hot_fused_5": ("hot_fused_3",),
}
_DAILY_ANALYSIS_EVIDENCE_DEPENDENCIES: dict[str, tuple[str, ...]] = {
    task_type: tuple(RELEASE_DATA_CATCHUP_DEPENDENCIES[task_type])
    for task_type in (
        "target_turnover_snapshot",
        "analysis_upper_evidence_prepare",
        "analysis_fast",
    )
}


def strategy_governance_task_block_reason(row: dict) -> str:
    """Return a fail-closed reason when governance dispatch is unavailable."""

    if str(row.get("task_type") or "").strip() != "strategy_governance_daily":
        return ""
    try:
        if strategy_governance_database_deferred():
            return "governance_database_deferred"
    except StrategyGovernanceModeError:
        return "governance_mode_invalid"
    return ""


def evaluate_strategy_pipeline_dependencies(
    task_type: str,
    dependency_rows: list[dict],
    *,
    now: datetime,
    target_trade_date: str | None = None,
) -> tuple[bool, str]:
    """Require exact-target inputs before analysis/governance work."""

    normalized_type = str(task_type or "").strip()
    if normalized_type not in {"analysis_fast", "strategy_governance_daily"}:
        return True, "not_applicable"
    expected_target = str(target_trade_date or now.date().isoformat())
    try:
        if date.fromisoformat(expected_target).isoformat() != expected_target:
            return False, "target_trade_date_invalid"
    except ValueError:
        return False, "target_trade_date_invalid"
    grouped: dict[str, list[dict]] = {}
    for row in dependency_rows:
        grouped.setdefault(str(row.get("task_type") or ""), []).append(row)
    required = ["qmt_announcement_pit", "capital_flow_batch_fast"]
    if normalized_type == "strategy_governance_daily":
        required.append("analysis_fast")
    for dependency in required:
        rows = grouped.get(dependency, [])
        if len(rows) != 1:
            return False, f"{dependency}:missing_or_duplicate"
        row = rows[0]
        triggered = _coerce_datetime(row.get("last_triggered_at"))
        status = str(row.get("last_run_status") or "").strip().lower()
        if int(row.get("enabled") or 0) != 1:
            return False, f"{dependency}:disabled"
        if (
            triggered is None
            or not _row_matches_target_trade_date(row, expected_target)
        ):
            return False, f"{dependency}:not_terminal_today"
        if dependency in {
            "capital_flow_batch_fast",
            "analysis_fast",
        } and status != "success":
            return False, f"{dependency}:not_success_today"
        if (
            dependency not in {"capital_flow_batch_fast", "analysis_fast"}
            and status not in _PIPELINE_TERMINAL_STATUSES
        ):
            return False, f"{dependency}:not_terminal_today"
    if normalized_type == "strategy_governance_daily":
        upstream_times = [
            _coerce_datetime(
                grouped[dependency][0].get("last_triggered_at")
            )
            for dependency in ("qmt_announcement_pit", "capital_flow_batch_fast")
        ]
        analysis_time = _coerce_datetime(
            grouped["analysis_fast"][0].get("last_triggered_at")
        )
        if (
            any(item is None for item in upstream_times)
            or analysis_time is None
            or analysis_time < max(item for item in upstream_times if item is not None)
        ):
            return False, "analysis_fast:ran_before_strategy_input"
    return True, "ready"


def evaluate_hot_rank_pipeline_dependencies(
    task_type: str,
    dependency_rows: list[dict],
    *,
    now: datetime,
) -> tuple[bool, str]:
    """Require each fusion stage's exact upstream tasks to succeed today."""

    normalized_type = str(task_type or "").strip()
    required = _HOT_RANK_PIPELINE_DEPENDENCIES.get(normalized_type)
    if required is None:
        return True, "not_applicable"
    grouped: dict[str, list[dict]] = {}
    for row in dependency_rows:
        grouped.setdefault(str(row.get("task_type") or "").strip(), []).append(row)
    dependency_times: list[datetime] = []
    for dependency in required:
        rows = grouped.get(dependency, [])
        if len(rows) != 1:
            return False, f"{dependency}:missing_or_duplicate"
        dependency_row = rows[0]
        triggered = _coerce_datetime(dependency_row.get("last_triggered_at"))
        status = str(
            dependency_row.get("last_run_status") or ""
        ).strip().lower()
        if int(dependency_row.get("enabled") or 0) != 1:
            return False, f"{dependency}:disabled"
        if triggered is None or triggered.date() != now.date():
            return False, f"{dependency}:not_run_today"
        if status != "success":
            return False, f"{dependency}:not_success_today"
        dependency_times.append(triggered)
    downstream_triggered = _coerce_datetime(
        next(
            (
                row.get("last_triggered_at")
                for row in grouped.get(normalized_type, [])
                if row.get("last_triggered_at")
            ),
            None,
        )
    )
    if (
        downstream_triggered is not None
        and downstream_triggered.date() == now.date()
        and dependency_times
        and downstream_triggered < max(dependency_times)
    ):
        return False, f"{normalized_type}:ran_before_dependency"
    return True, "ready"


def evaluate_daily_analysis_evidence_dependencies(
    task_type: str,
    dependency_rows: list[dict],
    *,
    now: datetime,
    target_trade_date: str | None = None,
) -> tuple[bool, str]:
    """Require each cross-host evidence producer to finish in DAG order."""

    normalized_type = str(task_type or "").strip()
    required = _DAILY_ANALYSIS_EVIDENCE_DEPENDENCIES.get(normalized_type)
    if required is None:
        return True, "not_applicable"
    expected_target = str(target_trade_date or now.date().isoformat())
    try:
        if date.fromisoformat(expected_target).isoformat() != expected_target:
            return False, "target_trade_date_invalid"
    except ValueError:
        return False, "target_trade_date_invalid"
    grouped: dict[str, list[dict]] = {}
    for item in dependency_rows:
        grouped.setdefault(
            str(item.get("task_type") or "").strip(), []
        ).append(item)
    dependency_times: list[datetime] = []
    for dependency in required:
        rows = grouped.get(dependency, [])
        if len(rows) != 1:
            return False, f"{dependency}:missing_or_duplicate"
        upstream = rows[0]
        triggered = _coerce_datetime(upstream.get("last_triggered_at"))
        if int(upstream.get("enabled") or 0) != 1:
            return False, f"{dependency}:disabled"
        if (
            triggered is None
            or not _row_matches_target_trade_date(upstream, expected_target)
        ):
            return False, f"{dependency}:not_run_today"
        if (
            str(upstream.get("last_run_status") or "").strip().lower()
            != "success"
        ):
            return False, f"{dependency}:not_success_today"
        dependency_times.append(triggered)
    downstream_rows = grouped.get(normalized_type, [])
    if len(downstream_rows) > 1:
        return False, f"{normalized_type}:duplicate"
    downstream_triggered = (
        _coerce_datetime(downstream_rows[0].get("last_triggered_at"))
        if downstream_rows
        else None
    )
    if (
        downstream_triggered is not None
        and _row_matches_target_trade_date(
            downstream_rows[0],
            expected_target,
        )
        and dependency_times
        and downstream_triggered < max(dependency_times)
    ):
        return False, f"{normalized_type}:ran_before_dependency"
    return True, "ready"


def evaluate_daily_result_pipeline_gate(
    dependency_rows: list[dict],
    *,
    expected_trade_date: str,
) -> tuple[bool, str]:
    """Prove that the latest closed session has a real governance result."""

    try:
        parsed_expected = date.fromisoformat(str(expected_trade_date or ""))
    except ValueError:
        return False, "target_trade_date_invalid"
    expected = parsed_expected.isoformat()
    rows = [
        row
        for row in dependency_rows
        if str(row.get("task_type") or "").strip()
        == DAILY_RESULT_PIPELINE_TASK_TYPE
    ]
    if len(rows) != 1:
        return False, "strategy_governance_daily:missing_or_duplicate"
    row = rows[0]
    if int(row.get("enabled") or 0) != 1:
        return False, "strategy_governance_daily:disabled"
    triggered = _coerce_datetime(row.get("last_triggered_at"))
    if triggered is None or triggered.date() < parsed_expected:
        return False, "strategy_governance_daily:not_run_for_target"
    if str(row.get("last_run_status") or "").strip().lower() != "success":
        return False, "strategy_governance_daily:not_success_for_target"

    pending_outputs = [str(row.get("last_run_output") or "")]
    seen_outputs: set[str] = set()
    payloads: list[dict] = []
    while pending_outputs:
        source = pending_outputs.pop()
        if source in seen_outputs:
            continue
        seen_outputs.add(source)
        for raw_line in source.splitlines():
            candidate = raw_line.strip()
            if not candidate.startswith("{"):
                continue
            try:
                payload = json.loads(candidate)
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict):
                continue
            payloads.append(payload)
            replay_output = payload.get("replay_output")
            if isinstance(replay_output, str):
                pending_outputs.append(replay_output)
    for payload in payloads:
        output_date = str(
            payload.get("trade_date")
            or payload.get("target_trade_date")
            or ""
        )[:10]
        result_ready = (
            payload.get("status") == "ok"
            or payload.get("orchestration_status") == "NOT_DUE"
        )
        if (
            result_ready
            and output_date == expected
            and payload.get("automatic_real_order_submission") is False
            and payload.get("real_order_authority") is False
        ):
            return True, "ready"
    return False, "strategy_governance_daily:target_receipt_unavailable"


def _attach_daily_recovery_targets(
    engine,
    rows: list[dict],
    *,
    now: datetime,
) -> bool:
    """Bind the ordinary daily-result DAG to one authoritative session."""

    current = now
    if current.tzinfo is not None:
        current = current.astimezone(PRODUCTION_TIMEZONE).replace(tzinfo=None)
    selected = [
        row
        for row in rows
        if str(row.get("task_type") or "").strip()
        in DAILY_RESULT_RECOVERY_TASK_TYPES
    ]
    for row in selected:
        row["_scheduler_target_trade_date"] = ""
        row["_scheduler_target_available"] = False
        row["_dependency_recovery_due"] = False
    if not selected:
        return True

    try:
        target = _daily_result_recovery_target(engine, now=current)
    except Exception as exc:
        logger.warning(
            "Daily-result backlog authority is unavailable; the complete DAG "
            "remains unclaimed: %s",
            type(exc).__name__,
        )
        return False
    for row in selected:
        row["_scheduler_target_trade_date"] = target
        row["_scheduler_target_available"] = True
    _attach_daily_dependency_recovery(rows, now=current)
    return True


def _attach_daily_dependency_recovery(
    rows: list[dict],
    *,
    now: datetime,
) -> None:
    """Wake a downstream stage when newer exact-target inputs have landed."""

    grouped: dict[str, list[dict]] = {}
    for row in rows:
        grouped.setdefault(str(row.get("task_type") or "").strip(), []).append(row)
    for downstream_type, dependencies in DAILY_RESULT_RECOVERY_DEPENDENCIES.items():
        downstream_rows = grouped.get(downstream_type, [])
        if len(downstream_rows) != 1:
            continue
        downstream = downstream_rows[0]
        if downstream.get("_scheduler_target_available") is not True:
            continue
        target = str(downstream.get("_scheduler_target_trade_date") or "")
        upstream_times: list[datetime] = []
        upstream_ready = True
        for dependency in dependencies:
            matches = grouped.get(dependency, [])
            if len(matches) != 1:
                upstream_ready = False
                break
            upstream = matches[0]
            triggered = _coerce_datetime(upstream.get("last_triggered_at"))
            if (
                int(upstream.get("enabled") or 0) != 1
                or str(upstream.get("last_run_status") or "").strip().lower()
                != "success"
                or triggered is None
                or not _row_matches_target_trade_date(upstream, target)
            ):
                upstream_ready = False
                break
            upstream_times.append(
                _cron_retry_reference(upstream, fallback=triggered)
            )
        if not upstream_ready or not upstream_times:
            continue
        downstream_triggered = _coerce_datetime(
            downstream.get("last_triggered_at")
        )
        downstream_satisfied = (
            downstream_triggered is not None
            and str(downstream.get("last_run_status") or "").strip().lower()
            == "success"
            and _row_matches_target_trade_date(downstream, target)
            and downstream_triggered >= max(upstream_times)
        )
        downstream["_dependency_recovery_due"] = not downstream_satisfied
        downstream["_dependency_latest_at"] = max(upstream_times)


def _daily_result_pipeline_gate(
    engine,
    *,
    now: datetime,
) -> tuple[bool, str]:
    """Resolve the reserved close-session target and validate its receipt."""

    try:
        expected_trade_date = authoritative_closed_trade_date(
            engine,
            now=now,
            close_ready_time=DAILY_RESULT_PIPELINE_RESERVATION_TIME,
        )
        with engine.connect() as connection:
            rows = [
                dict(item)
                for item in connection.execute(
                    text(
                        "SELECT task.task_type, task.enabled, "
                        "history.run_at AS last_triggered_at, "
                        "history.status AS last_run_status, "
                        "history.output AS last_run_output "
                        "FROM st_scheduled_tasks AS task "
                        "JOIN st_scheduled_task_history AS history "
                        "ON history.task_id=task.id "
                        "AND history.task_type=task.task_type "
                        "WHERE task.task_type=:task_type "
                        "AND history.status='success' "
                        "AND history.exit_code=0 "
                        "ORDER BY history.id DESC LIMIT 1"
                    ),
                    {"task_type": DAILY_RESULT_PIPELINE_TASK_TYPE},
                ).mappings()
            ]
    except Exception as exc:
        return False, f"daily_result_preflight_failed:{type(exc).__name__}"
    return evaluate_daily_result_pipeline_gate(
        rows,
        expected_trade_date=str(expected_trade_date or ""),
    )


def _strategy_pipeline_dependencies_ready(
    row: dict, engine, now: datetime
) -> tuple[bool, str]:
    task_type = str(row.get("task_type") or "").strip()
    hot_dependencies = _HOT_RANK_PIPELINE_DEPENDENCIES.get(task_type)
    evidence_dependencies = _DAILY_ANALYSIS_EVIDENCE_DEPENDENCIES.get(
        task_type
    )
    if (
        task_type not in {"analysis_fast", "strategy_governance_daily"}
        and hot_dependencies is None
        and evidence_dependencies is None
    ):
        return True, "not_applicable"
    dependency_types = (
        set(hot_dependencies or ()) | {task_type}
        if hot_dependencies is not None
        else (
            set(evidence_dependencies or ()) | {task_type}
            if evidence_dependencies is not None
            else {
                "qmt_announcement_pit",
                "capital_flow_batch_fast",
                "analysis_fast",
            }
        )
    )
    placeholders = ",".join(
        f":dependency_{index}" for index, _ in enumerate(sorted(dependency_types))
    )
    params = {
        f"dependency_{index}": dependency
        for index, dependency in enumerate(sorted(dependency_types))
    }
    try:
        with engine.connect() as connection:
            dependencies = [
                dict(item)
                for item in connection.execute(
                    text(
                        "SELECT task_type, enabled, last_triggered_at, "
                        "last_run_status, last_run_output FROM st_scheduled_tasks "
                        f"WHERE task_type IN ({placeholders}) "
                        "ORDER BY task_type, id"
                    ),
                    params,
                ).mappings()
            ]
    except Exception as exc:
        return False, f"dependency_query_failed:{type(exc).__name__}"
    if hot_dependencies is not None:
        return evaluate_hot_rank_pipeline_dependencies(
            task_type, dependencies, now=now
        )
    if evidence_dependencies is not None:
        return evaluate_daily_analysis_evidence_dependencies(
            task_type,
            dependencies,
            now=now,
            target_trade_date=str(
                row.get("_scheduler_target_trade_date") or ""
            ) or None,
        )
    return evaluate_strategy_pipeline_dependencies(
        task_type,
        dependencies,
        now=now,
        target_trade_date=str(
            row.get("_scheduler_target_trade_date") or ""
        ) or None,
    )


def _task_timeout_minutes(
    row: dict,
    *,
    now: datetime | None = None,
) -> int:
    task_type = str(row.get("task_type") or "").strip()
    script_path = str(row.get("script_path") or "").replace("\\", "/").strip()
    interval_minutes = int(row.get("interval_minutes") or 0)

    if (
        task_type == "qmt_local_history_2024"
        or script_path in {
            "tools/run_guojin_qmt_full_market_history.py",
            "tools/run_guojin_qmt_full_market_history_2024.py",
        }
    ):
        base_timeout = max(
            LONG_TASK_TIMEOUT_MINUTES,
            QMT_FULL_HISTORY_TASK_TIMEOUT_MINUTES,
        )
    elif interval_minutes > 0:
        base_timeout = max(
            FAST_TASK_TIMEOUT_MINUTES,
            min(DEFAULT_TASK_TIMEOUT_MINUTES, interval_minutes * 3),
        )
    elif task_type in LONG_RUNNING_TASK_TYPES or script_path in LONG_RUNNING_PATH_PARTS:
        base_timeout = LONG_TASK_TIMEOUT_MINUTES
    elif task_type in FAST_RUNNING_TASK_TYPES:
        base_timeout = FAST_TASK_TIMEOUT_MINUTES
    else:
        base_timeout = DEFAULT_TASK_TIMEOUT_MINUTES

    stage_timeout = DAILY_RESULT_STAGE_TIMEOUT_MINUTES.get(task_type)
    if stage_timeout is None:
        return base_timeout
    bounded = min(base_timeout, int(stage_timeout))
    current = now or _now_shanghai_naive()
    target = _row_recovery_target(row, now=current)
    # A prior-session recovery gets a fresh bounded attempt.  For the ordinary
    # same-day window, reserve one retry interval before the stage SLA closes.
    if target != current.date():
        return max(1, bounded)
    cron_min = _parse_hhmm(str(row.get("cron_time") or ""))
    window_seconds = CRITICAL_CRON_CATCHUP_WINDOWS_SECONDS.get(task_type)
    if cron_min is None or not window_seconds:
        return max(1, bounded)
    deadline = datetime.combine(target, datetime.min.time()) + timedelta(
        minutes=cron_min,
        seconds=int(window_seconds),
    )
    remaining_minutes = int((deadline - current).total_seconds() // 60)
    retry_reserve = CRON_RETRY_INTERVAL_MINUTES
    return max(1, min(bounded, max(1, remaining_minutes - retry_reserve)))


def _scheduler_task_sort_key(row: dict, *, now: datetime) -> tuple[int, float, int]:
    """Order due tasks by how long they have been waiting.

    The scheduler used to scan rows in ``sort_order`` and launch the first
    due task.  With a single worker, a one-minute realtime task at the top of
    the list could therefore starve a 15/30-minute minute-data task forever.
    Due tasks are now ordered by overdue seconds; the oldest due task gets the
    worker slot first, while not-yet-due tasks remain at the end.
    """
    if _release_build_catchup_allowed(row, now=now):
        # New-build proofs are finite release work.  Give them priority over
        # recurring realtime tasks while preserving stable task-id ordering.
        return (0, -7 * 24 * 60 * 60.0, int(row.get("id") or 0))
    interval_minutes = int(row.get("interval_minutes") or 0)
    if interval_minutes > 0:
        reference = _coerce_datetime(row.get("last_triggered_at")) or _coerce_datetime(row.get("last_run_at"))
        if reference is None:
            overdue_seconds = float(interval_minutes * 60)
        else:
            overdue_seconds = (now - reference).total_seconds() - interval_minutes * 60
        if overdue_seconds < 0:
            return (1, 0.0, int(row.get("id") or 0))
        return (0, -overdue_seconds, int(row.get("id") or 0))

    if not _cron_due(row, now=now):
        return (1, 0.0, int(row.get("id") or 0))
    cron_minute = _parse_hhmm(str(row.get("cron_time") or "17:10"))
    current_minute = now.hour * 60 + now.minute
    overdue_seconds = max(0, current_minute - (cron_minute or current_minute)) * 60
    return (0, -float(overdue_seconds), int(row.get("id") or 0))


def _terminate_process(proc: subprocess.Popen | None) -> None:
    if not proc or proc.poll() is not None:
        return
    try:
        if os.name == "nt":
            proc.kill()
        else:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except Exception as exc:
        logger.debug("primary process termination failed, retrying with kill: %s", exc)
        try:
            proc.kill()
        except Exception as kill_exc:
            logger.warning("failed to terminate process pid=%s: %s", getattr(proc, "pid", None), kill_exc)


def _terminate_process_and_confirm(
    proc: subprocess.Popen | None,
    *,
    timeout_seconds: float = 5.0,
) -> bool:
    """Terminate one exact child and confirm that it is no longer alive."""
    if proc is None or proc.poll() is not None:
        return False
    _terminate_process(proc)
    try:
        proc.wait(timeout=max(0.1, float(timeout_seconds)))
    except subprocess.TimeoutExpired:
        return False
    except Exception as exc:
        logger.warning(
            "failed to confirm scheduler child termination pid=%s: %s",
            getattr(proc, "pid", None),
            exc,
        )
        return False
    return proc.poll() is not None


def _task_stop_requested(task_id: int) -> bool:
    with _running_lock:
        return int(task_id) in _stop_requested_task_ids


def _task_timeout_requested(task_id: int) -> bool:
    with _running_lock:
        return int(task_id) in _timeout_requested_task_ids


def request_stop_owned_scheduler_task(
    task_id: int,
    *,
    timeout_seconds: float = 5.0,
) -> dict[str, object]:
    """Stop only an exact child owned by this API process, or fail closed.

    The standalone scheduler is another process even when it runs on the same
    host.  Its child cannot be proven or controlled through this process-local
    registry.  The owning worker persists ``stopped`` and closes the matching
    audit row only after termination is confirmed.
    """
    task_id = int(task_id)
    with _running_lock:
        if task_id in _timeout_pending_task_ids or task_id in _timeout_requested_task_ids:
            return {
                "accepted": False,
                "status": "timeout_in_progress",
                "task_id": task_id,
                "process_killed": False,
            }
        if task_id in _stop_pending_task_ids or task_id in _stop_requested_task_ids:
            return {
                "accepted": False,
                "status": "stop_in_progress",
                "task_id": task_id,
                "process_killed": False,
            }
        proc = _running_procs.get(task_id)
        history_run_uid = str(_running_history_uids.get(task_id) or "").strip()
        locally_owned = (
            task_id in _running_task_ids
            and proc is not None
            and proc.poll() is None
            and bool(history_run_uid)
        )
        if not locally_owned:
            return {
                "accepted": False,
                "status": "not_owned_by_api_process",
                "task_id": task_id,
                "process_killed": False,
            }
        _stop_pending_task_ids.add(task_id)

    if not _terminate_process_and_confirm(proc, timeout_seconds=timeout_seconds):
        with _running_lock:
            _stop_pending_task_ids.discard(task_id)
        return {
            "accepted": False,
            "status": "stop_not_confirmed",
            "task_id": task_id,
            "process_killed": False,
        }

    with _running_lock:
        still_exact_owner = (
            _running_procs.get(task_id) is proc
            and task_id in _running_task_ids
            and str(_running_history_uids.get(task_id) or "").strip()
            == history_run_uid
        )
        _stop_pending_task_ids.discard(task_id)
        if still_exact_owner:
            _stop_requested_task_ids.add(task_id)
    if not still_exact_owner:
        return {
            "accepted": False,
            "status": "stop_raced_with_terminal_state",
            "task_id": task_id,
            "process_killed": False,
        }

    return {
        "accepted": True,
        "status": "stop_requested",
        "task_id": task_id,
        "job_id": history_run_uid,
        "process_killed": True,
    }


def _cleanup_stale_running_tasks(engine) -> int:
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT id, task_name, task_type, script_path, interval_minutes, last_run_at, last_triggered_at "
                    "FROM st_scheduled_tasks "
                    "WHERE last_run_status = 'running'"
                )
            ).mappings().all()
    except Exception as exc:
        logger.warning("僵尸检测异常: %s", exc)
        return 0

    now = _now_shanghai_naive()
    cleaned = 0
    for row in rows:
        data = dict(row)
        # The scheduler table is shared by the Linux production host and the
        # Windows QMT bridge.  A process registry is necessarily host-local,
        # so this scheduler must never time out or fail a task owned by the
        # other host merely because it cannot see that host's child process.
        if _should_skip_task_for_host(data):
            continue
        started_at = _coerce_datetime(data.get("last_run_at")) or _coerce_datetime(data.get("last_triggered_at"))
        if not started_at:
            continue
        timeout_minutes = _task_timeout_minutes(data, now=now)
        age_minutes = int((now - started_at).total_seconds() / 60)
        task_id = int(data["id"])
        # A service restart drops the process registry but does not prove the
        # old child died.  Releasing the database claim here could start a
        # second copy of the same writer while the first one is still alive.
        # Keep the task claimed and require operator/service-manager evidence
        # instead of inventing a terminal state.
        with _running_lock:
            has_local_process = task_id in _running_procs
        interrupted_by_restart = (
            started_at < _scheduler_started_at and not has_local_process
        )
        if interrupted_by_restart:
            if _recover_interrupted_manual_claim(engine, data, started_at):
                with _running_lock:
                    _running_procs.pop(task_id, None)
                    _running_history_uids.pop(task_id, None)
                    _running_task_ids.discard(task_id)
                    _fast_lane_running_task_ids.discard(task_id)
                    _quote_lane_running_task_ids.discard(task_id)
                    _alert_lane_running_task_ids.discard(task_id)
                    _delivery_lane_running_task_ids.discard(task_id)
                cleaned += 1
                continue
            task_name = data.get("task_name") or task_id
            logger.error(
                "服务重启后发现归属不明的运行任务，保持 running 并禁止重跑: "
                "%s (id=%s, started=%s)",
                task_name,
                task_id,
                started_at,
            )
            continue
        if age_minutes < timeout_minutes + STALE_RUNNING_GRACE_MINUTES:
            continue

        task_id = int(data["id"])
        task_name = data.get("task_name") or task_id
        logger.warning(
            "检测到超时任务: %s (id=%s, started=%s, age=%dm, timeout=%dm), 自动清理",
            task_name,
            task_id,
            started_at,
            age_minutes,
            timeout_minutes,
        )
        with _running_lock:
            proc = _running_procs.get(task_id)
            history_run_uid = str(_running_history_uids.get(task_id) or "").strip()
            locally_owned = (
                task_id in _running_task_ids
                and proc is not None
                and proc.poll() is None
                and bool(history_run_uid)
                and task_id not in _stop_pending_task_ids
                and task_id not in _stop_requested_task_ids
                and task_id not in _timeout_pending_task_ids
                and task_id not in _timeout_requested_task_ids
            )
            if locally_owned:
                _timeout_pending_task_ids.add(task_id)
        if not locally_owned:
            logger.error(
                "超时任务缺少当前实例的精确进程与审计归属，保持 running: "
                "%s (id=%s)",
                task_name,
                task_id,
            )
            continue
        if not _terminate_process_and_confirm(proc):
            with _running_lock:
                _timeout_pending_task_ids.discard(task_id)
            logger.error(
                "超时任务终止未确认，保持 running 且不释放抢占: %s (id=%s)",
                task_name,
                task_id,
            )
            continue
        with _running_lock:
            still_exact_owner = (
                _running_procs.get(task_id) is proc
                and task_id in _running_task_ids
                and str(_running_history_uids.get(task_id) or "").strip()
                == history_run_uid
            )
            _timeout_pending_task_ids.discard(task_id)
            if still_exact_owner:
                _timeout_requested_task_ids.add(task_id)
        if not still_exact_owner:
            logger.info(
                "超时终止确认时任务已由原执行线程自然收口: %s (id=%s)",
                task_name,
                task_id,
            )
            continue
        # The exact owning worker is the sole terminal-state writer.  It will
        # observe the timeout request after communicate() returns, persist the
        # timeout against the same audit run, then release all local slots.
        cleaned += 1
    return cleaned


def _owner_pid_is_absent(instance_id: object, *, host_name: str) -> bool:
    """Prove that one exact local scheduler/API owner PID no longer exists."""

    if os.name != "posix":
        return False
    match = re.fullmatch(rf"{re.escape(host_name)}-([1-9][0-9]*)", str(instance_id or ""))
    if match is None:
        return False
    owner_pid = int(match.group(1))
    if owner_pid == os.getpid():
        return False
    try:
        os.kill(owner_pid, 0)
    except ProcessLookupError:
        return True
    except (PermissionError, OSError):
        return False
    return False


def _recover_interrupted_manual_claim(
    engine,
    row: dict,
    started_at: datetime,
) -> bool:
    """Release a proven-dead previous-build claim after scheduler restart.

    A generic missing process registry is not sufficient evidence: another
    host may still own the writer.  Recovery is allowed only when the durable
    running history identifies exactly one manual/scheduled owner on this
    host, its build differs from the active release, its PID is proven absent,
    and its start timestamp is the task-table claim being released.  A
    same-build process is never reclaimed from PID evidence alone because PID
    reuse and service-manager races cannot be fenced by this process.
    """

    task_id = int(row["id"])
    host_name = gethostname()
    try:
        current_build = _scheduler_build_commit_sha()
        with engine.begin() as connection:
            history_rows = [
                dict(item)
                for item in connection.execute(
                    text(
                        "SELECT run_uid, run_at, status, host_name, "
                        "scheduler_instance_id, build_sha, trigger_source "
                        "FROM st_scheduled_task_history "
                        "WHERE task_id=:task_id AND status='running' "
                        "ORDER BY run_at DESC, id DESC LIMIT 2 FOR UPDATE"
                    ),
                    {"task_id": task_id},
                ).mappings().all()
            ]
            if len(history_rows) != 1:
                return False
            history = history_rows[0]
            history_started_at = _coerce_datetime(history.get("run_at"))
            previous_build = str(history.get("build_sha") or "").strip().lower()
            instance_id = str(history.get("scheduler_instance_id") or "").strip()
            if (
                str(history.get("trigger_source") or "").strip()
                not in {"manual", "scheduled", "release_catchup"}
                or str(history.get("host_name") or "").strip() != host_name
                or history_started_at is None
                or abs((history_started_at - started_at).total_seconds()) > 1
                or re.fullmatch(r"[0-9a-f]{40}", previous_build) is None
                or previous_build == current_build
                or not _owner_pid_is_absent(instance_id, host_name=host_name)
            ):
                return False

            run_uid = str(history.get("run_uid") or "").strip()
            if not run_uid:
                return False
            output = (
                "INTERRUPTED_OWNER_GONE: exact scheduler task owner exited "
                f"before completion; previous_instance={instance_id}; "
                f"previous_build={previous_build}; current_build={current_build}; "
                "released_for_scheduler_catchup=true"
            )
            history_update = connection.execute(
                text(
                    "UPDATE st_scheduled_task_history SET finished_at=NOW(), "
                    "status='failed', "
                    "duration=GREATEST(0, TIMESTAMPDIFF(SECOND, run_at, NOW())), "
                    "exit_code=NULL, output=:output "
                    "WHERE run_uid=:run_uid AND status='running'"
                ),
                {"run_uid": run_uid, "output": output},
            )
            if int(getattr(history_update, "rowcount", 0) or 0) != 1:
                raise RuntimeError("manual history recovery cardinality mismatch")
            task_update = connection.execute(
                text(
                    "UPDATE st_scheduled_tasks SET last_run_status='failed', "
                    "last_run_output=:output, "
                    "last_run_duration=GREATEST(0, TIMESTAMPDIFF(SECOND, "
                    "last_run_at, NOW())), updated_at=NOW() "
                    "WHERE id=:task_id AND last_run_status='running' "
                    "AND last_run_at=:started_at"
                ),
                {
                    "task_id": task_id,
                    "started_at": started_at,
                    "output": output,
                },
            )
            if int(getattr(task_update, "rowcount", 0) or 0) != 1:
                raise RuntimeError("manual task recovery cardinality mismatch")
    except Exception as exc:
        logger.warning("中断的手工任务恢复失败，保持 running: id=%s error=%s", task_id, exc)
        return False

    logger.warning(
        "已恢复精确死亡进程遗留的任务占用: id=%s previous_instance=%s",
        task_id,
        instance_id,
    )
    return True


def _should_skip_non_trading_day(
    row: dict,
    engine,
    now: datetime | None = None,
) -> bool | None:
    """Return True to skip, False to run, or None to defer for calendar data.

    Deferring leaves the cron task unclaimed, so the next scheduler poll can
    retry after a calendar truncate/refill or transient lookup failure.  This
    avoids both a false-success skip and an early briefing built from stale
    market data.
    """
    now = now or _now_shanghai_naive()
    task_type = str(row.get("task_type") or "").strip()
    script_path = str(row.get("script_path") or "").replace("\\", "/").strip()
    script_args = str(row.get("script_args") or "").strip()

    market_day_sensitive = task_type in NON_TRADING_DAY_SKIP_TYPES or (
        script_path in NON_TRADING_DAY_SKIP_PATHS
        and (
            script_args.startswith("sm_")
            or script_args.startswith("st_a_list")
            or "stock_" in script_args
            or "index_" in script_args
            or "concept_" in script_args
            or "quality" in task_type
        )
    )
    if not market_day_sensitive:
        return False

    target_day = _row_recovery_target(row, now=now)
    trade_day = _is_trade_day(engine, target_day)
    if trade_day is None:
        return None
    return not trade_day


def _mark_non_trading_day_skip(row: dict, engine, now: datetime) -> None:
    target_day = _row_recovery_target(row, now=now)
    output = f"Skipped automatically: {target_day.isoformat()} is not a trading day."
    update_scheduler_task(
        engine,
        int(row["id"]),
        {
            "last_run_status": "success",
            "last_run_output": output,
            "last_run_duration": 0,
        },
        now_columns={"last_run_at", "last_triggered_at"},
    )


def _claim_task_run(row: dict, engine) -> bool:
    """Atomically claim a scheduled task before launching a worker.

    The in-process lock prevents duplicate launches in one Python process. This
    database claim prevents two scheduler processes from launching the same task
    at the same time.
    """
    return claim_scheduler_task_run(engine, int(row["id"]))


def _build_task_args(row: dict, script_path: str, today: str) -> list[str]:
    return build_scheduler_task_args(row, script_path, today)


def _release_catchup_closed_target_date(
    engine,
    *,
    now: datetime | None = None,
    task_type: str = "",
    close_ready_time: datetime_time | None = None,
) -> str:
    """Resolve one DB-authoritative closed session or fail without guessing."""

    ready_time = close_ready_time or release_catchup_closed_ready_time(task_type)
    try:
        target = authoritative_closed_trade_date(
            engine,
            now=now,
            close_ready_time=ready_time,
        )
        parsed = date.fromisoformat(str(target or ""))
    except Exception as exc:
        raise RuntimeError(
            "release catch-up authoritative closed target date is unavailable"
        ) from exc
    if parsed.isoformat() != target:
        raise RuntimeError(
            "release catch-up authoritative closed target date is unavailable"
        )
    return target


class ReleaseCatchupDataBlocked(RuntimeError):
    """A release replay cannot yet bind a current-only source to today."""


def _release_catchup_current_snapshot_date(
    engine,
    *,
    task_type: str,
    now: datetime,
) -> str:
    """Prove a current-only source is in today's post-close publish window."""

    current = now
    if current.tzinfo is not None:
        current = current.astimezone(PRODUCTION_TIMEZONE)
    ready_time = RELEASE_CATCHUP_CURRENT_SNAPSHOT_READY_TIMES.get(task_type)
    if ready_time is None:
        raise ReleaseCatchupDataBlocked(
            "release catch-up current-snapshot task is unclassified"
        )
    try:
        published_session = authoritative_closed_trade_date(
            engine,
            now=current,
            close_ready_time=ready_time,
        )
        parsed = date.fromisoformat(str(published_session or ""))
    except Exception as exc:
        raise ReleaseCatchupDataBlocked(
            "release catch-up current-snapshot publication authority is unavailable"
        ) from exc
    run_date = current.date().isoformat()
    if parsed.isoformat() != published_session or published_session != run_date:
        raise ReleaseCatchupDataBlocked(
            "release catch-up current-snapshot publication window is not open"
        )
    return run_date


def _release_catchup_previous_session_target_date(
    engine,
    *,
    now: datetime,
) -> str:
    """Resolve the session before the bound execution date without fallback."""

    current = now
    if current.tzinfo is not None:
        current = current.astimezone(PRODUCTION_TIMEZONE)
    try:
        target = authoritative_closed_trade_date(
            engine,
            # Midnight preserves the bound execution date while forcing the
            # shared market clock's strict ``trade_date < execution_date``
            # branch, including at 23:59:59.999999.
            now=current.replace(hour=0, minute=0, second=0, microsecond=0),
        )
        parsed = date.fromisoformat(str(target or ""))
    except Exception as exc:
        raise ReleaseCatchupDataBlocked(
            "release catch-up previous-session authority is unavailable"
        ) from exc
    if parsed.isoformat() != target or target >= current.date().isoformat():
        raise ReleaseCatchupDataBlocked(
            "release catch-up previous-session authority is unavailable"
        )
    return target


def _attach_release_catchup_expected_targets(
    engine,
    rows: list[dict],
    *,
    now: datetime,
) -> bool:
    """Bind date-sensitive release rows to this poll's authoritative target.

    The binding is recomputed before every scheduler sort.  In particular, an
    18:00 Shanghai closed-session rollover invalidates a 17:59 success receipt
    even when its build SHA is unchanged.  Calendar lookup failure blocks only
    the affected release rows and never falls back to the host calendar.
    """

    current = now
    if current.tzinfo is not None:
        current = current.astimezone(PRODUCTION_TIMEZONE)
    selected = [
        row
        for row in rows
        if str(row.get("task_type") or "").strip()
        in RELEASE_CATCHUP_EXACT_TARGET_TASK_TYPES
    ]
    selected_ids = {id(row) for row in selected}
    for row in rows:
        required = id(row) in selected_ids
        row["_release_expected_target_required"] = required
        if required:
            row["_release_expected_target_available"] = False
            row["_release_expected_target_date"] = ""
        row["_membership_ordinary_snapshot_available"] = False
    if not selected:
        return True

    resolved: dict[str, str] = {
        "current": current.date().isoformat(),
    }
    failures: dict[str, str] = {}
    closed_task_types = sorted(
        {
            str(row.get("task_type") or "").strip()
            for row in selected
            if str(row.get("task_type") or "").strip()
            in RELEASE_CATCHUP_CLOSED_TARGET_TASK_TYPES
            and not (
                str(row.get("task_type") or "").strip()
                in DAILY_RESULT_RECOVERY_TASK_TYPES
                and row.get("_scheduler_target_available") is True
            )
        }
    )
    for task_type in closed_task_types:
        category = f"closed:{task_type}"
        try:
            resolved[category] = _release_catchup_closed_target_date(
                engine,
                now=current,
                task_type=task_type,
            )
        except Exception as exc:
            failures[category] = type(exc).__name__
    if any(
        str(row.get("task_type") or "").strip()
        in RELEASE_CATCHUP_PREVIOUS_SESSION_TARGET_TASK_TYPES
        for row in selected
    ):
        try:
            resolved["previous"] = (
                _release_catchup_previous_session_target_date(
                    engine,
                    now=current,
                )
            )
        except Exception as exc:
            failures["previous"] = type(exc).__name__

    for row in selected:
        task_type = str(row.get("task_type") or "").strip()
        category = (
            f"closed:{task_type}"
            if task_type in RELEASE_CATCHUP_CLOSED_TARGET_TASK_TYPES
            else "previous"
            if task_type in RELEASE_CATCHUP_PREVIOUS_SESSION_TARGET_TASK_TYPES
            else "current"
        )
        recovery_target = (
            str(row.get("_scheduler_target_trade_date") or "").strip()
            if task_type in DAILY_RESULT_RECOVERY_TASK_TYPES
            and row.get("_scheduler_target_available") is True
            else ""
        )
        target = recovery_target or str(resolved.get(category) or "")
        if target:
            row["_release_expected_target_available"] = True
            row["_release_expected_target_date"] = target
    membership_rows = [
        row for row in rows
        if str(row.get("task_type") or "").strip()
        == _QMT_MEMBERSHIP_TASK_TYPE
    ]
    if membership_rows:
        try:
            with engine.connect() as connection:
                snapshot_rows = connection.execute(text("""
                    SELECT snapshot_date, source, quality_status
                    FROM qmt_membership_snapshot_run
                    WHERE snapshot_date=:snapshot_date
                      AND source=:source
                      AND quality_status='QMT_VALIDATED'
                    LIMIT 2
                """), {
                    "snapshot_date": current.date().isoformat(),
                    "source": _QMT_MEMBERSHIP_PROVIDER,
                }).mappings().all()
            snapshot_available = len(snapshot_rows) == 1
        except Exception as exc:
            snapshot_available = False
            logger.warning(
                "QMT membership ordinary-publisher state is unavailable; "
                "publisher remains fail-closed due: %s",
                type(exc).__name__,
            )
        for row in membership_rows:
            row["_membership_ordinary_snapshot_available"] = (
                snapshot_available
            )
    if failures:
        logger.warning(
            "Release catch-up target authority is unavailable; fail closed: %s",
            ",".join(
                f"{category}={error}"
                for category, error in sorted(failures.items())
            ),
        )
    return not failures


def _membership_ordinary_publish_due(row: dict, *, now: datetime) -> bool:
    """Prioritize the 15:12 publisher while today's snapshot is absent."""

    if (
        str(row.get("task_type") or "").strip()
        != _QMT_MEMBERSHIP_TASK_TYPE
        or row.get("_membership_ordinary_snapshot_available") is True
    ):
        return False
    cron_min = _parse_hhmm(str(row.get("cron_time") or "15:12"))
    if cron_min is None or now.hour * 60 + now.minute < cron_min:
        return False
    last_triggered = _coerce_datetime(row.get("last_triggered_at"))
    if last_triggered is None or last_triggered.date() < now.date():
        return True
    if last_triggered.date() > now.date():
        return False
    if last_triggered.hour * 60 + last_triggered.minute < cron_min:
        # In particular, a 15:10 read-only release BLOCK must not consume the
        # ordinary 15:12 publisher slot or impose release backoff on it.
        return True
    retry_at = _cron_retry_reference(row, fallback=last_triggered)
    return (
        now - retry_at
    ).total_seconds() >= CRON_RETRY_INTERVAL_MINUTES * 60


def _task_dispatch_date(
    row: dict,
    engine,
    *,
    now: datetime | None = None,
) -> str:
    task_type = str(row.get("task_type") or "").strip()
    trigger_source = str(row.get("_trigger_source") or "").strip()
    current = now or datetime.now(PRODUCTION_TIMEZONE)
    if current.tzinfo is not None:
        current = current.astimezone(PRODUCTION_TIMEZONE)
    scheduler_target = str(
        row.get("_scheduler_target_trade_date") or ""
    ).strip()
    if scheduler_target:
        try:
            parsed_scheduler_target = date.fromisoformat(scheduler_target)
        except ValueError as exc:
            raise RuntimeError(
                "scheduler target trade date is invalid"
            ) from exc
        if parsed_scheduler_target.isoformat() != scheduler_target:
            raise RuntimeError("scheduler target trade date is invalid")
        return scheduler_target
    if (
        trigger_source == "release_catchup"
        and task_type in RELEASE_CATCHUP_AUTHORITATIVE_DATE_TASK_TYPES
    ):
        return _release_catchup_closed_target_date(
            engine,
            now=current,
            task_type=task_type,
        )
    if (
        trigger_source == "release_catchup"
        and task_type in RELEASE_CATCHUP_RUN_DATE_SNAPSHOT_TASK_TYPES
    ):
        return _release_catchup_current_snapshot_date(
            engine,
            task_type=task_type,
            now=current,
        )
    return current.date().isoformat()


def _task_argument_row(
    row: dict,
    *,
    now: datetime,
    target_date: str,
    engine=None,
) -> dict:
    """Bind formal analysis to one Shanghai execution/decision wall clock."""

    task_type = str(row.get("task_type") or "").strip()
    trigger_source = str(row.get("_trigger_source") or "").strip()
    daily_pipeline = (
        task_type in ANALYSIS_DAILY_EVIDENCE_TASK_TYPES
        or task_type == "analysis_fast"
    )
    if (
        task_type not in ANALYSIS_POOL_PUBLISHER_TASK_TYPES
        and not daily_pipeline
    ):
        return row
    current = now
    if current.tzinfo is not None:
        current = current.astimezone(PRODUCTION_TIMEZONE).replace(tzinfo=None)
    bound = current.replace(microsecond=0).isoformat(timespec="seconds")
    if daily_pipeline:
        try:
            parsed_target = date.fromisoformat(str(target_date or ""))
        except ValueError as exc:
            raise RuntimeError(
                "scheduler analysis pipeline target date is invalid"
            ) from exc
        exact_target = parsed_target.isoformat()
        if exact_target != str(target_date or ""):
            raise RuntimeError(
                "scheduler analysis pipeline target date is invalid"
            )
        if trigger_source != "release_catchup":
            bound = (
                f"{exact_target}T"
                f"{ANALYSIS_DAILY_PIPELINE_DECISION_TIME}"
            )
        elif task_type == "target_turnover_snapshot":
            bound = (
                current + timedelta(
                    seconds=RELEASE_TURNOVER_DECISION_LEAD_SECONDS
                )
            ).replace(microsecond=0).isoformat(timespec="seconds")
        elif task_type == "analysis_upper_evidence_prepare":
            bound = (
                current + timedelta(
                    seconds=RELEASE_UPPER_DECISION_LEAD_SECONDS
                )
            ).replace(microsecond=0).isoformat(timespec="seconds")
        else:
            if engine is None:
                raise RuntimeError(
                    "release analysis cutoff requires persisted upper evidence"
                )
            rows = []
            try:
                with engine.connect() as connection:
                    rows = connection.execute(text("""
                        SELECT decision_at
                        FROM st_market_field_capture_run
                        WHERE target_date=:target_date
                          AND status='COMPLETED'
                          AND capture_kind='DAILY_UPPER_LIMIT_HISTORY'
                          AND provider='myquant.gm.get_history_instruments'
                          AND collector_build_sha=:build_sha
                        ORDER BY published_at DESC, run_id DESC
                        LIMIT 1
                    """), {
                        "target_date": exact_target,
                        "build_sha": _scheduler_build_commit_sha(),
                    }).mappings().all()
            except Exception as exc:
                raise ReleaseCatchupDataBlocked(
                    "release analysis upper cutoff is unavailable"
                ) from exc
            if len(rows) != 1:
                raise ReleaseCatchupDataBlocked(
                    "release analysis upper cutoff is unavailable"
                )
            try:
                upper_cutoff = rows[0]["decision_at"]
                if not isinstance(upper_cutoff, datetime):
                    upper_cutoff = datetime.fromisoformat(str(upper_cutoff))
            except (TypeError, ValueError) as exc:
                raise ReleaseCatchupDataBlocked(
                    "release analysis upper cutoff is invalid"
                ) from exc
            if upper_cutoff.tzinfo is not None:
                upper_cutoff = upper_cutoff.astimezone(
                    PRODUCTION_TIMEZONE
                ).replace(tzinfo=None)
            bound = upper_cutoff.replace(microsecond=0).isoformat(
                timespec="seconds"
            )
            if current < upper_cutoff.replace(microsecond=0):
                raise ReleaseCatchupDataBlocked(
                    "release analysis is waiting for the actual recovery cutoff"
                )
    result = {
        **row,
        "_scheduler_execution_time": bound,
    }
    if daily_pipeline:
        result["_scheduler_pipeline_decision_at"] = bound
        result["_scheduler_pipeline_target_date"] = exact_target
    if (
        trigger_source == "release_catchup"
        and task_type in RELEASE_CATCHUP_PREVIOUS_SESSION_TASK_TYPES
    ):
        result["_release_execution_time"] = bound
    return result


def _bind_release_validation_target(
    row: dict,
    engine,
    *,
    dispatch_date: str,
    now: datetime,
) -> dict:
    """Carry the exact scheduler-selected data date into durable validation."""

    if str(row.get("_trigger_source") or "").strip() != "release_catchup":
        return row
    task_type = str(row.get("task_type") or "").strip()
    if task_type in (
        RELEASE_CATCHUP_AUTHORITATIVE_DATE_TASK_TYPES
        | RELEASE_CATCHUP_CURRENT_TARGET_TASK_TYPES
    ):
        target = dispatch_date
    elif task_type in RELEASE_CATCHUP_PREVIOUS_SESSION_TASK_TYPES:
        target = _release_catchup_previous_session_target_date(
            engine,
            now=now,
        )
    else:
        return row
    return {**row, "_release_target_date": target}


def _get_task_semaphore() -> threading.Semaphore:
    global _task_semaphore
    if _task_semaphore is None:
        limit = int(get_scheduler_runtime_config()["max_concurrent_tasks"])
        _task_semaphore = threading.Semaphore(limit)
    return _task_semaphore


def _uses_fast_lane(row: dict) -> bool:
    return str(row.get("task_type") or "").strip() in FAST_LANE_TASK_TYPES


def _uses_quote_lane(row: dict) -> bool:
    return str(row.get("task_type") or "").strip() in QUOTE_LANE_TASK_TYPES


def _uses_alert_lane(row: dict) -> bool:
    return str(row.get("task_type") or "").strip() in ALERT_LANE_TASK_TYPES


def _uses_delivery_lane(row: dict) -> bool:
    return str(row.get("task_type") or "").strip() in USER_DELIVERY_LANE_TASK_TYPES


def _get_fast_lane_semaphore() -> threading.Semaphore:
    global _fast_lane_semaphore
    if _fast_lane_semaphore is None:
        _fast_lane_semaphore = threading.Semaphore(1)
    return _fast_lane_semaphore


def _get_quote_lane_semaphore() -> threading.Semaphore:
    global _quote_lane_semaphore
    if _quote_lane_semaphore is None:
        _quote_lane_semaphore = threading.Semaphore(1)
    return _quote_lane_semaphore


def _get_alert_lane_semaphore() -> threading.Semaphore:
    global _alert_lane_semaphore
    if _alert_lane_semaphore is None:
        _alert_lane_semaphore = threading.Semaphore(1)
    return _alert_lane_semaphore


def _get_delivery_lane_semaphore() -> threading.Semaphore:
    global _delivery_lane_semaphore
    if _delivery_lane_semaphore is None:
        _delivery_lane_semaphore = threading.Semaphore(1)
    return _delivery_lane_semaphore


def _task_lane_semaphore(row: dict) -> threading.Semaphore:
    if _uses_quote_lane(row):
        return _get_quote_lane_semaphore()
    if _uses_alert_lane(row):
        return _get_alert_lane_semaphore()
    if _uses_delivery_lane(row):
        return _get_delivery_lane_semaphore()
    if _uses_fast_lane(row):
        return _get_fast_lane_semaphore()
    return _get_task_semaphore()


def _scheduler_lane_has_capacity(row: dict, *, max_general_tasks: int) -> bool:
    """Return lane capacity while ``_running_lock`` is held by the caller."""
    if _uses_quote_lane(row):
        return len(_quote_lane_running_task_ids) < 1
    if _uses_alert_lane(row):
        return len(_alert_lane_running_task_ids) < 1
    if _uses_delivery_lane(row):
        return len(_delivery_lane_running_task_ids) < 1
    if _uses_fast_lane(row):
        return len(_fast_lane_running_task_ids) < 1
    general_running = len(
        _running_task_ids
        - _fast_lane_running_task_ids
        - _quote_lane_running_task_ids
        - _delivery_lane_running_task_ids
        - _alert_lane_running_task_ids
    )
    return general_running < max(1, int(max_general_tasks))

def scheduler_runtime_info() -> dict[str, int | bool]:
    scheduler = get_scheduler_runtime_config()
    api_pool = get_api_mysql_pool_config()
    return {
        "embedded_scheduler_enabled": bool(scheduler["embedded_enabled"]),
        "embedded_scheduler_running": bool(_scheduler_thread and _scheduler_thread.is_alive()),
        "scheduler_max_concurrent_tasks": int(scheduler["max_concurrent_tasks"]),
        "scheduler_quote_lane_tasks": 1,
        "scheduler_alert_lane_tasks": 1,
        "scheduler_delivery_lane_tasks": 1,
        "scheduler_poll_seconds": int(scheduler["poll_seconds"]),
        "api_mysql_pool_size": int(api_pool["pool_size"]),
        "api_mysql_max_overflow": int(api_pool["max_overflow"]),
        "api_mysql_pool_recycle": int(api_pool["pool_recycle"]),
    }


def _path_is_within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _reject_existing_symlink_components(path: Path) -> None:
    candidate = path
    while True:
        if os.path.lexists(candidate) and candidate.is_symlink():
            raise RuntimeError(f"detached job log path contains symlink: {candidate}")
        if candidate.parent == candidate:
            break
        candidate = candidate.parent


def _detached_job_log_root(
    *, root: Path, env: dict[str, str],
) -> Path:
    configured = str(
        env.get("PROBIGA_JOB_LOG_ROOT")
        or os.environ.get("PROBIGA_JOB_LOG_ROOT")
        or ""
    ).strip()
    if configured:
        log_root = Path(configured)
    elif os.name == "nt":
        program_data = str(
            env.get("PROGRAMDATA") or os.environ.get("PROGRAMDATA") or ""
        ).strip()
        if not program_data:
            raise RuntimeError(
                "PROBIGA_JOB_LOG_ROOT or PROGRAMDATA is required for detached jobs"
            )
        log_root = Path(program_data) / "ProBigA" / "jobs"
    else:
        log_root = Path("/var/lib/probiga/jobs")
    if not log_root.is_absolute():
        raise RuntimeError("detached job log root must be absolute")

    code_root = root.resolve(strict=True)
    _reject_existing_symlink_components(log_root)
    prospective = log_root.resolve(strict=False)
    if _path_is_within(prospective, code_root):
        raise RuntimeError("detached job logs must not be written inside the code tree")
    log_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    _reject_existing_symlink_components(log_root)
    resolved = log_root.resolve(strict=True)
    if _path_is_within(resolved, code_root):
        raise RuntimeError("detached job logs must not be written inside the code tree")
    if resolved == Path(resolved.anchor):
        raise RuntimeError("detached job log root is too broad")

    if os.name != "nt":
        root_stat = resolved.stat()
        if root_stat.st_uid != os.geteuid():
            raise RuntimeError("detached job log root is not owned by the service user")
        if stat.S_IMODE(root_stat.st_mode) != 0o700:
            raise RuntimeError("detached job log root mode must be 0700")
    return resolved


def _open_detached_job_log(path: Path):
    if os.path.lexists(path) and path.is_symlink():
        raise RuntimeError(f"detached job log is a symlink: {path}")
    flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(path, flags, 0o600)
    try:
        file_stat = os.fstat(fd)
        path_stat = os.lstat(path)
        if (
            not stat.S_ISREG(file_stat.st_mode)
            or file_stat.st_dev != path_stat.st_dev
            or file_stat.st_ino != path_stat.st_ino
        ):
            raise RuntimeError(f"detached job log identity changed: {path}")
        if os.name != "nt":
            os.fchmod(fd, 0o600)
        return os.fdopen(fd, "a", encoding="utf-8")
    except Exception:
        os.close(fd)
        raise


def start_detached_python_job(
    *,
    cmd: list[str],
    root: Path,
    env: dict[str, str],
    log_name: str,
    nice: int = 10,
) -> dict[str, object]:
    """Start a long-running Python job outside the API request lifecycle."""
    data_dir = _detached_job_log_root(root=root, env=env)
    safe_log_name = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in log_name).strip("_")
    safe_log_name = safe_log_name or "detached_job"
    out_path = data_dir / f"{safe_log_name}.out.log"
    err_path = data_dir / f"{safe_log_name}.err.log"
    stdout_handle = _open_detached_job_log(out_path)
    try:
        stderr_handle = _open_detached_job_log(err_path)
    except Exception:
        stdout_handle.close()
        raise
    try:
        popen_kwargs = {
            "cwd": str(root),
            "env": env,
            "stdout": stdout_handle,
            "stderr": stderr_handle,
            "text": True,
        }
        if os.name != "nt":
            popen_kwargs["preexec_fn"] = lambda: os.nice(max(0, int(nice)))
        proc = subprocess.Popen(cmd, **popen_kwargs)
    finally:
        stdout_handle.close()
        stderr_handle.close()
    return {"pid": proc.pid, "stdout_log": str(out_path), "stderr_log": str(err_path)}


def _scheduler_build_commit_sha() -> str:
    value = str(os.environ.get("PROBIGA_BUILD_COMMIT_SHA") or "").strip().lower()
    return value if re.fullmatch(r"[0-9a-f]{40}", value) else "0" * 40


def _scheduler_executor_role(mode: str) -> str:
    configured = str(
        os.environ.get("PROBIGA_SCHEDULER_EXECUTOR_ROLE") or ""
    ).strip().lower()
    if str(mode or "").strip().lower() == "standalone" and os.name != "nt":
        return "linux_standalone"
    if os.name == "nt" and configured == "qmt_windows_edge":
        return configured
    return "unclassified_scheduler"


def _write_scheduler_heartbeat(engine, mode: str) -> None:
    runtime = get_scheduler_runtime_config()
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO st_scheduler_runtime "
                "(instance_id, mode, host_name, pid, build_sha, executor_role, "
                "started_at, heartbeat_at, poll_seconds, max_concurrent_tasks) "
                "VALUES (:instance_id, :mode, :host_name, :pid, :build_sha, "
                ":executor_role, :started_at, NOW(), :poll_seconds, "
                ":max_concurrent_tasks) "
                "ON DUPLICATE KEY UPDATE mode=VALUES(mode), host_name=VALUES(host_name), pid=VALUES(pid), "
                "build_sha=VALUES(build_sha), executor_role=VALUES(executor_role), "
                "started_at=VALUES(started_at), heartbeat_at=NOW(), poll_seconds=VALUES(poll_seconds), "
                "max_concurrent_tasks=VALUES(max_concurrent_tasks)"
            ),
            {
                "instance_id": _scheduler_instance_id,
                "mode": mode,
                "host_name": gethostname(),
                "pid": os.getpid(),
                "build_sha": _scheduler_build_commit_sha(),
                "executor_role": _scheduler_executor_role(mode),
                "started_at": _scheduler_started_at,
                "poll_seconds": int(runtime["poll_seconds"]),
                "max_concurrent_tasks": int(runtime["max_concurrent_tasks"]),
            },
        )


def _standalone_heartbeat_allows_dispatch(
    engine,
    mode: str,
) -> tuple[bool, dict[str, object]]:
    """Require this exact standalone executor to be the only fresh owner.

    The heartbeat row is not merely an observability signal: it is the
    scheduler's authority lease.  Production dispatch therefore fails closed
    whenever the release SHA is missing, the role is unclassified, a clock is
    in the future, or more than one executor for the role is fresh.
    """

    normalized_mode = str(mode or "").strip().lower()
    if normalized_mode != "standalone":
        return True, {"mode": normalized_mode, "errors": []}

    role = _scheduler_executor_role(normalized_mode)
    build_sha = _scheduler_build_commit_sha()
    host_name = gethostname()
    pid = os.getpid()
    expected_instance_id = f"{host_name}-{pid}"
    errors: list[str] = []
    if role not in {"linux_standalone", "qmt_windows_edge"}:
        errors.append("executor_role_unclassified")
    if not re.fullmatch(r"[0-9a-f]{40}", build_sha) or build_sha == "0" * 40:
        errors.append("build_sha_invalid")
    if not host_name or len(host_name) > 128:
        errors.append("host_name_invalid")
    if errors:
        return False, {
            "mode": normalized_mode,
            "executor_role": role,
            "expected_instance_id": expected_instance_id,
            "expected_build_sha": build_sha,
            "fresh_row_count": 0,
            "future_row_count": 0,
            "errors": errors,
        }

    try:
        with engine.connect() as conn:
            rows = [
                dict(row)
                for row in conn.execute(
                    text(
                        "SELECT instance_id, mode, host_name, pid, build_sha, "
                        "executor_role, started_at, heartbeat_at, "
                        "TIMESTAMPDIFF(SECOND, heartbeat_at, NOW()) "
                        "AS heartbeat_age_seconds, poll_seconds, "
                        "max_concurrent_tasks FROM st_scheduler_runtime "
                        "WHERE executor_role=:executor_role "
                        "ORDER BY heartbeat_at DESC, instance_id ASC"
                    ),
                    {"executor_role": role},
                ).mappings()
            ]
    except Exception:
        return False, {
            "mode": normalized_mode,
            "executor_role": role,
            "expected_instance_id": expected_instance_id,
            "expected_build_sha": build_sha,
            "fresh_row_count": 0,
            "future_row_count": 0,
            "errors": ["heartbeat_read_failed"],
        }

    try:
        expected_poll_seconds = int(
            get_scheduler_runtime_config()["poll_seconds"]
        )
    except (KeyError, TypeError, ValueError):
        expected_poll_seconds = 0
    if expected_poll_seconds < 15:
        errors.append("expected_poll_seconds_invalid")

    fresh_rows: list[dict[str, object]] = []
    future_rows: list[dict[str, object]] = []
    for row in rows:
        try:
            age = int(row.get("heartbeat_age_seconds"))
        except (TypeError, ValueError):
            errors.append("heartbeat_age_invalid")
            continue
        if age < 0:
            future_rows.append(row)
        try:
            poll = int(row.get("poll_seconds"))
        except (TypeError, ValueError):
            if 0 <= age <= 2 * max(15, expected_poll_seconds):
                errors.append("fresh_heartbeat_poll_invalid")
            continue
        if (
            poll < 15
            and 0 <= age <= 2 * max(15, expected_poll_seconds)
        ):
            errors.append("fresh_heartbeat_poll_invalid")
        if poll >= 15 and 0 <= age <= 2 * poll:
            fresh_rows.append(row)

    if future_rows:
        errors.append("future_heartbeat_present")
    if len(fresh_rows) != 1:
        errors.append("fresh_heartbeat_not_unique")

    current = fresh_rows[0] if len(fresh_rows) == 1 else None
    if current is not None:
        try:
            current_pid = int(current.get("pid"))
            current_poll = int(current.get("poll_seconds"))
            current_concurrency = int(current.get("max_concurrent_tasks"))
        except (TypeError, ValueError):
            current_pid = 0
            current_poll = 0
            current_concurrency = 0
        if str(current.get("instance_id") or "") != expected_instance_id:
            errors.append("instance_id_mismatch")
        if str(current.get("mode") or "").strip().lower() != normalized_mode:
            errors.append("mode_mismatch")
        if str(current.get("host_name") or "") != host_name:
            errors.append("host_name_mismatch")
        if current_pid != pid:
            errors.append("pid_mismatch")
        if str(current.get("build_sha") or "").strip().lower() != build_sha:
            errors.append("build_sha_mismatch")
        if str(current.get("executor_role") or "").strip().lower() != role:
            errors.append("executor_role_mismatch")
        if current_poll != expected_poll_seconds:
            errors.append("poll_seconds_mismatch")
        if current_concurrency < 1:
            errors.append("max_concurrent_tasks_invalid")
        if current.get("started_at") is None:
            errors.append("started_at_missing")
        if current.get("heartbeat_at") is None:
            errors.append("heartbeat_at_missing")

    return not errors, {
        "mode": normalized_mode,
        "executor_role": role,
        "expected_instance_id": expected_instance_id,
        "expected_build_sha": build_sha,
        "fresh_row_count": len(fresh_rows),
        "future_row_count": len(future_rows),
        "errors": errors,
    }


def read_scheduler_heartbeat() -> dict[str, object] | None:
    try:
        engine = get_engine()
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT instance_id, mode, host_name, pid, build_sha, "
                    "executor_role, started_at, heartbeat_at, "
                    "TIMESTAMPDIFF(SECOND, heartbeat_at, NOW()) AS heartbeat_age_seconds, "
                    "poll_seconds, max_concurrent_tasks "
                    "FROM st_scheduler_runtime ORDER BY heartbeat_at DESC LIMIT 1"
                )
            ).mappings().first()
        if not row:
            return None
        data = dict(row)
        for key in ("started_at", "heartbeat_at"):
            if data.get(key):
                data[key] = str(data[key])[:19]
        return data
    except Exception as exc:
        logger.warning("读取调度心跳异常: %s", exc)
        return None


def _ensure_task_history_table(engine) -> None:
    """Read-only runtime proof that the privileged audit schema is usable."""
    engine_key = id(engine)
    with _task_history_schema_lock:
        if engine_key in _task_history_ready_engines:
            return
        with engine.connect() as conn:
            conn.execute(
                text(
                    "SELECT id, run_uid, task_id, task_name, task_type, "
                    "run_at, finished_at, status, duration, exit_code, "
                    "output, host_name, scheduler_instance_id, build_sha, "
                    "trigger_source FROM st_scheduled_task_history LIMIT 0"
                )
            )
            index_rows = conn.execute(
                text("SHOW INDEX FROM st_scheduled_task_history")
            ).mappings().all()
        indexes: dict[str, dict[str, object]] = {}
        for row in index_rows:
            name = str(row.get("Key_name") or row.get("key_name") or "")
            if not name:
                continue
            entry = indexes.setdefault(
                name,
                {
                    "unique": int(
                        row.get("Non_unique")
                        if row.get("Non_unique") is not None
                        else row.get("non_unique") or 0
                    ) == 0,
                    "columns": [],
                },
            )
            entry["columns"].append(
                (
                    int(
                        row.get("Seq_in_index")
                        or row.get("seq_in_index")
                        or 0
                    ),
                    str(
                        row.get("Column_name")
                        or row.get("column_name")
                        or ""
                    ),
                )
            )
        index_shapes = {
            (
                bool(entry["unique"]),
                tuple(
                    column
                    for _sequence, column in sorted(entry["columns"])
                ),
            )
            for entry in indexes.values()
        }
        if (True, ("run_uid",)) not in index_shapes:
            raise RuntimeError(
                "scheduler history unique run_uid index is unavailable"
            )
        if not any(
            columns == ("task_id", "run_at")
            for _unique, columns in index_shapes
        ):
            raise RuntimeError(
                "scheduler history task/run index is unavailable"
            )
        _task_history_ready_engines.add(engine_key)


def _maybe_cleanup_history(engine, *, monotonic_now: float | None = None) -> dict[str, int]:
    """Delete a bounded batch of old audit rows at a low fixed frequency."""
    global _history_cleanup_next_at
    current = time.monotonic() if monotonic_now is None else float(monotonic_now)
    with _history_cleanup_lock:
        if current < _history_cleanup_next_at:
            return {}
        # Advance before I/O so a missing table or transient database failure
        # cannot turn the scheduler's minute poll into a cleanup hot loop.
        _history_cleanup_next_at = current + HISTORY_CLEANUP_INTERVAL_SECONDS

    deleted: dict[str, int] = {}
    table_dates = {
        "st_scheduled_task_history": "run_at",
        "sys_wecom_delivery_receipt": "started_at",
    }
    try:
        with engine.connect() as conn:
            existing = {
                str(row[0])
                for row in conn.execute(
                    text(
                        "SELECT TABLE_NAME FROM information_schema.TABLES "
                        "WHERE TABLE_SCHEMA = DATABASE() "
                        "AND TABLE_NAME IN ('st_scheduled_task_history', 'sys_wecom_delivery_receipt')"
                    )
                ).fetchall()
            }
        for table_name, date_column in table_dates.items():
            if table_name not in existing:
                continue
            table_deleted = 0
            for _batch in range(HISTORY_CLEANUP_MAX_BATCHES):
                with engine.begin() as conn:
                    protected_filter = (
                        " AND task_type NOT IN "
                        "('qmt_edge_release_request',"
                        "'qmt_edge_release_bootstrap',"
                        "'release_data_activation')"
                        if table_name == "st_scheduled_task_history"
                        else ""
                    )
                    result = conn.execute(
                        text(
                            f"DELETE FROM `{table_name}` "
                            f"WHERE `{date_column}` < NOW() - INTERVAL {HISTORY_RETENTION_DAYS} DAY "
                            f"{protected_filter} "
                            f"LIMIT {HISTORY_CLEANUP_BATCH_SIZE}"
                        )
                    )
                batch_deleted = int(getattr(result, "rowcount", 0) or 0)
                table_deleted += batch_deleted
                if batch_deleted < HISTORY_CLEANUP_BATCH_SIZE:
                    break
            deleted[table_name] = table_deleted
    except Exception as exc:
        logger.warning("history retention cleanup failed: %s", exc)
    return deleted


def _redact_history_output(value: object) -> str:
    output = str(value or "")
    for pattern, replacement in _HISTORY_SECRET_PATTERNS:
        output = pattern.sub(replacement, output)
    encoded = output.encode("utf-8")
    if len(encoded) <= _HISTORY_OUTPUT_LIMIT:
        return output
    return encoded[-_HISTORY_OUTPUT_LIMIT:].decode("utf-8", errors="ignore")


def _history_digest(value: object) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def _history_validation_replay_output(machine_output: object) -> str:
    """Keep only bounded machine receipts and explicit daily date markers.

    Scheduler children may emit megabytes of diagnostics.  Release
    revalidation needs the schema-labelled result receipts, not those logs.
    Redaction happens before selection so the evidence itself cannot become a
    credential side channel.  An oversized individual receipt fails closed:
    it is never silently truncated into something that merely looks valid.
    """

    redacted = str(machine_output or "")
    for pattern, replacement in _HISTORY_SECRET_PATTERNS:
        redacted = pattern.sub(replacement, redacted)

    def contains_machine_schema(value: object, depth: int = 0) -> bool:
        if depth > 4:
            return False
        if isinstance(value, dict):
            schema = str(value.get("schema") or "")
            return schema.startswith("probiga.") or any(
                contains_machine_schema(item, depth + 1)
                for item in value.values()
                if isinstance(item, (dict, list, tuple, str))
            )
        if isinstance(value, (list, tuple)):
            return any(contains_machine_schema(item, depth + 1) for item in value)
        if isinstance(value, str) and "{" in value:
            for nested_line in value.splitlines():
                nested = nested_line.strip()
                if not nested.startswith("{"):
                    continue
                try:
                    parsed = json.loads(nested)
                except (TypeError, ValueError, json.JSONDecodeError):
                    continue
                if contains_machine_schema(parsed, depth + 1):
                    return True
        return False

    selected: list[str] = []
    seen: set[str] = set()
    for raw_line in redacted.splitlines():
        line = raw_line.strip()
        candidate = ""
        if re.fullmatch(r"DATE=\d{4}-\d{2}-\d{2}", line):
            candidate = line
        elif line.startswith("{"):
            try:
                payload = json.loads(line)
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            if isinstance(payload, dict) and contains_machine_schema(payload):
                candidate = json.dumps(
                    payload,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                )
        if not candidate or candidate in seen:
            continue
        candidate_size = len(candidate.encode("utf-8"))
        if candidate_size > _HISTORY_REPLAY_OUTPUT_LIMIT:
            raise RuntimeError(
                "scheduler machine receipt exceeds bounded history evidence"
            )
        projected = "\n".join([*selected, candidate])
        if len(projected.encode("utf-8")) > _HISTORY_REPLAY_OUTPUT_LIMIT:
            raise RuntimeError(
                "scheduler machine receipts exceed bounded history evidence"
            )
        seen.add(candidate)
        selected.append(candidate)
    return "\n".join(selected)


def _build_history_validation_evidence(
    row: dict,
    *,
    run_uid: str,
    machine_output: object,
    status: str,
    exit_code: int,
    started_at: datetime,
    validation_message: object,
) -> str:
    """Return one hash-bound, replayable, non-secret scheduler evidence line."""

    replay_output = _history_validation_replay_output(machine_output)
    replay_disposition = scheduler_output_status(
        row,
        replay_output,
        return_code=exit_code,
    )
    if replay_disposition is not None and replay_disposition != status:
        raise RuntimeError(
            "bounded scheduler receipt cannot replay the successful disposition"
        )
    safe_validation_message = str(validation_message or "")[:2000]
    for pattern, replacement in _HISTORY_SECRET_PATTERNS:
        safe_validation_message = pattern.sub(
            replacement,
            safe_validation_message,
        )
    core = {
        "schema": _HISTORY_EVIDENCE_SCHEMA,
        "run_uid": str(run_uid),
        "task_id": int(row["id"]),
        "task_name": str(row.get("task_name") or ""),
        "task_type": str(row.get("task_type") or ""),
        "build_sha": _scheduler_build_commit_sha(),
        "status": str(status),
        "exit_code": int(exit_code),
        "started_at": started_at.replace(microsecond=0).isoformat(sep=" "),
        "validation_checked": True,
        "validation_ok": True,
        "validation_message": safe_validation_message,
        "machine_output_sha256": _history_digest(machine_output),
        "replay_output": replay_output,
        "replay_output_sha256": _history_digest(replay_output),
    }
    scheduler_target_date = str(
        row.get("_scheduler_target_trade_date") or ""
    ).strip()
    if scheduler_target_date:
        try:
            parsed_scheduler_target = date.fromisoformat(
                scheduler_target_date
            )
        except ValueError as exc:
            raise RuntimeError(
                "scheduler validation target trade date is invalid"
            ) from exc
        if parsed_scheduler_target.isoformat() != scheduler_target_date:
            raise RuntimeError(
                "scheduler validation target trade date is invalid"
            )
        core["target_trade_date"] = scheduler_target_date
    release_target_date = str(row.get("_release_target_date") or "").strip()
    if release_target_date:
        try:
            parsed_target = date.fromisoformat(release_target_date)
        except ValueError as exc:
            raise RuntimeError(
                "scheduler release validation target date is invalid"
            ) from exc
        if (
            str(row.get("_trigger_source") or "").strip() != "release_catchup"
            or parsed_target.isoformat() != release_target_date
        ):
            raise RuntimeError(
                "scheduler release validation target date is invalid"
            )
        core["release_target_date"] = release_target_date
    evidence = {**core, "evidence_sha256": _history_digest(json.dumps(
        core,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ))}
    encoded = json.dumps(
        evidence,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    if len(encoded.encode("utf-8")) > _HISTORY_EVIDENCE_LIMIT:
        raise RuntimeError("scheduler validation evidence exceeds TEXT budget")
    return encoded


def _history_output_with_validation_evidence(
    display_output: object,
    evidence: str,
) -> str:
    tail = _redact_history_output(display_output)[-6000:]
    combined = (tail + "\n" + evidence).strip()
    if len(combined.encode("utf-8")) > _HISTORY_OUTPUT_LIMIT:
        combined = evidence
    if len(combined.encode("utf-8")) > _HISTORY_OUTPUT_LIMIT:
        raise RuntimeError("scheduler validation evidence exceeds history budget")
    return combined


def _task_history_start(engine, row: dict, *, run_uid: str | None = None) -> str | None:
    """Append one claimed run. History failure must not prevent delivery."""
    task_id = int(row["id"])
    run_uid = str(run_uid or uuid.uuid4().hex)[:64]
    try:
        _ensure_task_history_table(engine)
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO st_scheduled_task_history "
                    "(run_uid, task_id, task_name, task_type, run_at, status, "
                    "host_name, scheduler_instance_id, build_sha, "
                    "trigger_source) "
                    "VALUES (:run_uid, :task_id, :task_name, :task_type, NOW(), "
                    "'running', :host_name, :instance_id, :build_sha, "
                    ":trigger_source)"
                ),
                {
                    "run_uid": run_uid,
                    "task_id": task_id,
                    "task_name": str(row.get("task_name") or "")[:255],
                    "task_type": str(row.get("task_type") or "")[:64],
                    "host_name": gethostname()[:128],
                    "instance_id": _scheduler_instance_id[:128],
                    "build_sha": _scheduler_build_commit_sha(),
                    "trigger_source": str(row.get("_trigger_source") or "scheduled")[:32],
                },
            )
        return run_uid
    except Exception as exc:
        logger.warning("Failed to append scheduler history start for task %s: %s", task_id, exc)
        return None


def _activate_analysis_strategy_pool(
    connection,
    *,
    run_uid: str,
    task_type: str,
) -> None:
    """Activate one validated pool in the scheduler-success transaction."""

    if task_type not in ANALYSIS_POOL_PUBLISHER_TASK_TYPES:
        return
    audit_rows = connection.execute(text("""
        SELECT task_type, status
        FROM st_scheduled_task_history
        WHERE run_uid=:run_uid
        LIMIT 2
    """), {"run_uid": run_uid}).mappings().all()
    if (
        len(audit_rows) != 1
        or str(audit_rows[0].get("task_type") or "").strip() != task_type
        or str(audit_rows[0].get("status") or "").strip().lower()
        != "running"
    ):
        raise RuntimeError("analysis pool activation scheduler audit differs")
    history_rows = connection.execute(text("""
        SELECT run_uid, scheduler_job_id, publisher_task_type, trade_date,
               status, total, passed, executable_count,
               canonical_pool_sha256, membership_snapshot_date,
               membership_snapshot_source, membership_proof_sha256
        FROM st_recommended_run_history
        WHERE run_uid=:run_uid
        LIMIT 2
    """), {"run_uid": run_uid}).mappings().all()
    if len(history_rows) != 1:
        raise RuntimeError(
            "analysis pool activation history is unavailable or ambiguous"
        )
    history = history_rows[0]
    try:
        analysis_count = int(history.get("total") or 0)
        expected_rows = int(history.get("passed") or 0)
        executable_count = int(history.get("executable_count") or 0)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            "analysis pool activation counters are invalid"
        ) from exc
    if (
        str(history.get("run_uid") or "").strip().lower() != run_uid
        or str(history.get("scheduler_job_id") or "").strip().lower()
        != run_uid
        or str(history.get("publisher_task_type") or "").strip()
        != task_type
        or str(history.get("status") or "").strip().lower() != "done"
        or analysis_count <= 0
        or expected_rows <= 0
        or executable_count < 0
        or executable_count > expected_rows
        or re.fullmatch(
            r"[0-9a-f]{64}",
            str(history.get("canonical_pool_sha256") or "").strip().lower(),
        ) is None
    ):
        raise RuntimeError("analysis pool activation history differs")
    trade_date = str(history.get("trade_date") or "")[:10]
    history_hash = str(
        history.get("canonical_pool_sha256") or ""
    ).strip().lower()
    membership = {
        "snapshot_date": str(
            history.get("membership_snapshot_date") or ""
        )[:10],
        "source": str(
            history.get("membership_snapshot_source") or ""
        ).strip(),
        "proof_sha256": str(
            history.get("membership_proof_sha256") or ""
        ).strip().lower(),
    }
    if (
        membership["snapshot_date"] != trade_date
        or membership["source"] != _QMT_MEMBERSHIP_PROVIDER
        or re.fullmatch(r"[0-9a-f]{64}", membership["proof_sha256"])
        is None
    ):
        raise RuntimeError("analysis pool activation membership identity differs")
    from server.engine.strategy_industry_history import (
        resolve_analysis_industry_membership_binding,
    )

    membership_binding = resolve_analysis_industry_membership_binding(
        connection.engine,
        trade_date=trade_date,
        decision_known_at=datetime.now(PRODUCTION_TIMEZONE).replace(tzinfo=None),
    )
    if (
        membership_binding.get("snapshot_date") != trade_date
        or membership_binding.get("source") != membership["source"]
        or membership_binding.get("proof_sha256")
        != membership["proof_sha256"]
    ):
        raise RuntimeError("analysis pool activation membership proof differs")
    staged_manifest = read_persisted_pool_manifest(connection, trade_date)
    if (
        int(staged_manifest["analysis_count"]) != analysis_count
        or int(staged_manifest["recommendation_count"]) != expected_rows
        or int(staged_manifest["executable_count"]) != executable_count
        or str(staged_manifest["canonical_pool_sha256"]).lower()
        != history_hash
        or staged_manifest.get("publisher_run_uids") != [run_uid]
        or staged_manifest.get("publication_statuses") not in (
            ["PENDING"],
            ["ACTIVE"],
        )
        or staged_manifest.get("live_gate_alignment") is not True
        or staged_manifest.get("membership_proofs") != [membership]
        or (
            executable_count == 0
            and not research_only_publication_is_safe(staged_manifest)
        )
    ):
        raise RuntimeError("analysis pool activation manifest differs")
    state = connection.execute(text("""
        SELECT COUNT(*) AS total_rows,
               SUM(CASE WHEN publication_status='PENDING' THEN 1 ELSE 0 END)
                   AS pending_rows,
               SUM(CASE WHEN publication_status='ACTIVE' THEN 1 ELSE 0 END)
                   AS active_rows,
               SUM(CASE WHEN publication_status='ACTIVE'
                         AND recommend_status=candidate_recommend_status
                         AND ordinary_buy_eligible=
                             candidate_ordinary_buy_eligible
                        THEN 1 ELSE 0 END) AS aligned_active_rows
        FROM st_recommended_stocks
        WHERE pick_date=:trade_date
          AND publisher_run_uid=:run_uid
    """), {
        "trade_date": history.get("trade_date"),
        "run_uid": run_uid,
    }).mappings().one()
    total_rows = int(state.get("total_rows") or 0)
    pending_rows = int(state.get("pending_rows") or 0)
    active_rows = int(state.get("active_rows") or 0)
    aligned_active_rows = int(state.get("aligned_active_rows") or 0)
    if total_rows != expected_rows:
        raise RuntimeError("analysis pool activation row count differs")
    if active_rows == expected_rows and aligned_active_rows == expected_rows:
        return
    if pending_rows != expected_rows or active_rows != 0:
        raise RuntimeError("analysis pool activation state is mixed")
    result = connection.execute(text("""
        UPDATE st_recommended_stocks
        SET recommend_status=candidate_recommend_status,
            ordinary_buy_eligible=candidate_ordinary_buy_eligible,
            publication_status='ACTIVE'
        WHERE pick_date=:trade_date
          AND publisher_run_uid=:run_uid
          AND publication_status='PENDING'
    """), {
        "trade_date": history.get("trade_date"),
        "run_uid": run_uid,
    })
    if int(result.rowcount or 0) != expected_rows:
        raise RuntimeError("analysis pool activation did not update exact rows")
    activated = connection.execute(text("""
        SELECT COUNT(*)
        FROM st_recommended_stocks
        WHERE pick_date=:trade_date
          AND publisher_run_uid=:run_uid
          AND publication_status='ACTIVE'
          AND recommend_status=candidate_recommend_status
          AND ordinary_buy_eligible=candidate_ordinary_buy_eligible
    """), {
        "trade_date": history.get("trade_date"),
        "run_uid": run_uid,
    }).scalar()
    if int(activated or 0) != expected_rows:
        raise RuntimeError("analysis pool activation readback differs")
    activated_manifest = read_persisted_pool_manifest(connection, trade_date)
    if (
        activated_manifest.get("publication_statuses") != ["ACTIVE"]
        or activated_manifest.get("live_gate_alignment") is not True
        or activated_manifest.get("publisher_run_uids") != [run_uid]
        or activated_manifest.get("membership_proofs") != [membership]
        or int(activated_manifest["analysis_count"]) != analysis_count
        or int(activated_manifest["recommendation_count"]) != expected_rows
        or int(activated_manifest["executable_count"]) != executable_count
        or str(activated_manifest["canonical_pool_sha256"]).lower()
        != history_hash
        or (
            executable_count == 0
            and not research_only_publication_is_safe(activated_manifest)
        )
    ):
        raise RuntimeError("analysis pool activated manifest differs")


def _task_history_finish(
    engine,
    run_uid: str | None,
    *,
    status: str,
    duration: int,
    exit_code: int | None,
    output: object,
    task_type: str = "",
) -> None:
    if not run_uid:
        return
    try:
        with engine.begin() as conn:
            if (
                status == "success"
                and str(task_type or "").strip()
                in ANALYSIS_POOL_PUBLISHER_TASK_TYPES
            ):
                _activate_analysis_strategy_pool(
                    conn,
                    run_uid=str(run_uid or "").strip().lower(),
                    task_type=str(task_type or "").strip(),
                )
            normalized_exit_code = None
            if exit_code is not None:
                normalized_exit_code = int(exit_code)
                # Windows exposes a terminated process status as an unsigned
                # 32-bit value (for example 0xFFFFFFFF for -1).  MySQL INT is
                # signed, so persist the equivalent signed status instead of
                # leaving the exact scheduler audit stuck in ``running``.
                if 2**31 <= normalized_exit_code <= 2**32 - 1:
                    normalized_exit_code -= 2**32
                if not -(2**31) <= normalized_exit_code <= 2**31 - 1:
                    normalized_exit_code = None
            conn.execute(
                text(
                    "UPDATE st_scheduled_task_history SET finished_at=NOW(), "
                    "status=:status, duration=:duration, exit_code=:exit_code, "
                    "output=:output WHERE run_uid=:run_uid"
                ),
                {
                    "run_uid": run_uid,
                    "status": str(status or "failed")[:32],
                    "duration": max(0, int(duration or 0)),
                    "exit_code": normalized_exit_code,
                    "output": _redact_history_output(output),
                },
            )
    except Exception as exc:
        if (
            status == "success"
            and str(task_type or "").strip()
            in ANALYSIS_POOL_PUBLISHER_TASK_TYPES
        ):
            raise RuntimeError(
                "validated analysis pool activation/terminal audit failed"
            ) from exc
        logger.warning("Failed to finish scheduler history %s: %s", run_uid, exc)


def _run_task(row: dict, root: Path, engine) -> None:
    """Execute one task and leave a terminal audit row on every code path."""
    requested_run_uid = str(row.get("_history_run_uid") or "") or None
    history_run_uid = requested_run_uid if row.get("_history_started") else _task_history_start(
        engine,
        row,
        run_uid=requested_run_uid,
    )
    if not history_run_uid:
        try:
            update_scheduler_task(
                engine,
                int(row["id"]),
                {
                    "last_run_status": "failed",
                    "last_run_output": (
                        "scheduler execution rejected: audit row unavailable"
                    ),
                    "last_run_duration": 0,
                },
            )
        except Exception as exc:
            logger.warning(
                "Failed to persist audit-unavailable state for task %s: %s",
                row.get("id"),
                exc,
            )
        return
    started_at = datetime.now()
    try:
        _run_task_impl(row, root, engine, history_run_uid=history_run_uid)
    except Exception as exc:
        duration = max(0, int((datetime.now() - started_at).total_seconds()))
        task_id = int(row["id"])
        stopped_by_user = _task_stop_requested(task_id)
        timed_out = _task_timeout_requested(task_id)
        release_data_blocked = isinstance(exc, ReleaseCatchupDataBlocked)
        status = "stopped" if stopped_by_user else (
            "timeout" if timed_out else (
                "blocked" if release_data_blocked else "failed"
            )
        )
        output = (
            f"scheduler task stopped after confirmed termination: {exc}"
            if stopped_by_user
            else (
                f"scheduler task timed out after confirmed termination: {exc}"
                if timed_out
                else (
                    f"DATA_BLOCKED: {exc}"
                    if release_data_blocked
                    else f"scheduler task execution failed: {exc}"
                )
            )
        )
        try:
            update_scheduler_task(
                engine,
                task_id,
                {
                    "last_run_status": status,
                    "last_run_output": output,
                    "last_run_duration": duration,
                },
            )
        except Exception as update_exc:
            logger.warning("Failed to persist task %s failure: %s", row.get("id"), update_exc)
        _task_history_finish(
            engine,
            history_run_uid,
            status=status,
            duration=duration,
            exit_code=None,
            output=output,
        )
        logger.exception("Scheduler task %s failed before completion", row.get("task_name"))


def _run_task_impl(
    row: dict,
    root: Path,
    engine,
    *,
    history_run_uid: str | None,
) -> None:
    """执行单个定时任务"""
    task_id = row["id"]
    task_name = row["task_name"]
    script_path = row["script_path"] or ""

    try:
        script = resolve_scheduler_script(root, script_path)
    except SchedulerScriptPolicyError as exc:
        logger.warning("拒绝不安全的调度脚本路径: %s", exc)
        update_scheduler_task(
            engine,
            int(task_id),
            {
                "last_run_status": "failed",
                "last_run_output": f"SCHEDULER_SCRIPT_BLOCKED: {exc}",
                "last_run_duration": 0,
            },
        )
        _task_history_finish(
            engine,
            history_run_uid,
            status="failed",
            duration=0,
            exit_code=126,
            output=f"SCHEDULER_SCRIPT_BLOCKED: {exc}",
        )
        return
    if not script.exists():
        logger.warning("脚本不存在: %s", script)
        update_scheduler_task(
            engine,
            int(task_id),
            {
                "last_run_status": "failed",
                "last_run_output": f"脚本不存在: {script}",
                "last_run_duration": 0,
            },
        )
        _task_history_finish(
            engine,
            history_run_uid,
            status="failed",
            duration=0,
            exit_code=127,
            output=f"script not found: {script}",
        )
        return

    dispatch_now = datetime.now(PRODUCTION_TIMEZONE)
    dispatch_date = _task_dispatch_date(row, engine, now=dispatch_now)
    argument_row = _task_argument_row(
        row,
        now=dispatch_now,
        target_date=dispatch_date,
        engine=engine,
    )
    argument_row = _bind_release_validation_target(
        argument_row,
        engine,
        dispatch_date=dispatch_date,
        now=dispatch_now,
    )
    args = _build_task_args(argument_row, script_path, dispatch_date)

    cmd = [sys.executable, str(script)] + args

    child_env = build_child_env(root, engine=engine)
    exact_history_uid = str(history_run_uid or "").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{32}", exact_history_uid):
        raise RuntimeError(
            "scheduler child launch requires an exact 32-hex audit identity"
        )
    task_type = str(row.get("task_type") or "").strip()
    child_env.update(
        {
            "PROBIGA_SCHEDULER_HISTORY_RUN_UID": exact_history_uid,
            "PROBIGA_SCHEDULER_TASK_ID": str(int(task_id)),
            "PROBIGA_SCHEDULER_TASK_TYPE": task_type[:64],
            "PROBIGA_SCHEDULER_BUILD_SHA": _scheduler_build_commit_sha(),
        }
    )
    # Post-run strategy validation must be bound to this exact scheduler
    # audit row and build.  These private fields never cross the process
    # boundary or become user-controlled command-line arguments.
    argument_row = {
        **argument_row,
        "_scheduler_history_run_uid": exact_history_uid,
        "_scheduler_expected_build_sha": child_env[
            "PROBIGA_SCHEDULER_BUILD_SHA"
        ],
    }
    if task_type == "linux_recent_data_gap_repair":
        expected_script_path = str(
            LINUX_PROVIDER_TASKS_BY_TYPE[task_type]["script_path"]
        ).replace("\\", "/")
        if str(script_path).replace("\\", "/").strip() != expected_script_path:
            raise RuntimeError(
                "Linux gap-repair child identity requires its exact script path"
            )
        parent_executor_role = str(
            child_env.get("PROBIGA_SCHEDULER_EXECUTOR_ROLE") or ""
        ).strip()
        if os.name != "posix":
            raise RuntimeError(
                "Linux gap-repair child role override requires a POSIX host"
            )
        if parent_executor_role != "linux_standalone":
            raise RuntimeError(
                "Linux gap-repair child role override requires the exact "
                "linux_standalone parent role"
            )
        # The standalone daemon remains the exclusive Linux scheduler owner;
        # only this narrowly identified child runs as the provider repair
        # executor required by the script's fail-closed ownership contract.
        child_env["PROBIGA_SCHEDULER_EXECUTOR_ROLE"] = "linux_provider"

    update_scheduler_task(
        engine,
        int(task_id),
        {"last_run_status": "running"},
        now_columns={"last_run_at", "last_triggered_at"},
    )

    start_t = datetime.now()
    # Database audit timestamps and all market-data knowledge cutoffs use
    # naive Asia/Shanghai wall time.  Never compare them with the host's
    # local ``datetime.now()`` (the Linux scheduler commonly runs in UTC).
    validation_started_at = dispatch_now.astimezone(
        PRODUCTION_TIMEZONE
    ).replace(tzinfo=None)
    validation = None
    machine_output = ""
    task_timeout_minutes = _task_timeout_minutes(
        row,
        now=validation_started_at,
    )
    try:
        timeout_seconds = max(60, task_timeout_minutes * 60)
        popen_kwargs = dict(
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=str(root),
            env=child_env,
        )
        if os.name == "nt":
            popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            popen_kwargs["preexec_fn"] = os.setsid
        proc = subprocess.Popen(cmd, **popen_kwargs)
        with _running_lock:
            _running_procs[task_id] = proc
        try:
            stdout, stderr = proc.communicate(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            _terminate_process(proc)
            stdout, stderr = proc.communicate()
            with _running_lock:
                if _running_procs.get(task_id) is proc:
                    _running_procs.pop(task_id, None)
            duration = int((datetime.now() - start_t).total_seconds())
            stopped_by_user = _task_stop_requested(int(task_id))
            status = "stopped" if stopped_by_user else "timeout"
            if stopped_by_user:
                output = (
                    "用户手动停止；子进程已确认退出。\n"
                    + (stdout or "")[-2500:]
                    + "\n---STDERR---\n"
                    + (stderr or "")[-1500:]
                )
            else:
                output = (
                    f"任务执行超过 {task_timeout_minutes} 分钟，已自动终止。\n"
                    + (stdout or "")[-2500:]
                    + "\n---STDERR---\n"
                    + (stderr or "")[-1500:]
                )
            update_scheduler_task(
                engine,
                int(task_id),
                {
                    "last_run_status": status,
                    "last_run_output": output,
                    "last_run_duration": duration,
                },
            )
            logger.warning("任务 %s 超时终止 (%ds)", task_name, duration)
            _task_history_finish(
                engine,
                history_run_uid,
                status=status,
                duration=duration,
                exit_code=getattr(proc, "returncode", None),
                output=output,
            )
            return
        with _running_lock:
            if _running_procs.get(task_id) is proc:
                _running_procs.pop(task_id, None)

        duration = int((datetime.now() - start_t).total_seconds())
        stopped_by_user = _task_stop_requested(int(task_id))
        timed_out = _task_timeout_requested(int(task_id))
        status = "stopped" if stopped_by_user else (
            "timeout" if timed_out else (
                "success" if proc.returncode == 0 else "failed"
            )
        )
        machine_output = (stdout or "") + "\n" + (stderr or "")
        output = (stdout or "")[-3000:] + "\n---STDERR---\n" + (stderr or "")[-2000:]
        # Preserve the Level-1 validator's explicit BLOCK state even though
        # its CLI exits non-zero.  BLOCK is not an execution failure and must
        # not be retried every fifteen minutes.
        if stopped_by_user:
            output = "用户手动停止；子进程已确认退出。\n" + output
        elif timed_out:
            output = (
                f"任务执行超过 {task_timeout_minutes} 分钟；"
                "子进程已确认退出。\n" + output
            )
        else:
            status = scheduler_output_status(
                row, machine_output, return_code=proc.returncode
            ) or status
            if status == "blocked":
                retry_marker = _retryable_blocked_marker(machine_output)
                if retry_marker:
                    output = output + "\n" + retry_marker
        validate_blocked_v3_receipt = (
            status == "blocked"
            and str(row.get("task_type") or "").strip()
            in {
                "trading_v3_close_decision",
                "trading_v3_premarket_review",
            }
        )
        if (
            status == "success" or validate_blocked_v3_receipt
        ) and not is_market_closed_skip_output(output):
            validation = validate_scheduler_task_result(
                argument_row,
                engine=engine,
                started_at=validation_started_at,
                now=datetime.now(PRODUCTION_TIMEZONE).replace(tzinfo=None),
                output=machine_output,
            )
            if validation.checked:
                marker = "DATA_VALIDATION_OK" if validation.ok else "DATA_VALIDATION_FAILED"
                output = output + f"\n{marker}: {validation.message}"
                if not validation.ok:
                    status = "failed"
    except Exception as exc:
        with _running_lock:
            if _running_procs.get(task_id) is locals().get("proc"):
                _running_procs.pop(task_id, None)
        stopped_by_user = _task_stop_requested(int(task_id))
        timed_out = _task_timeout_requested(int(task_id))
        status = "stopped" if stopped_by_user else (
            "timeout" if timed_out else "failed"
        )
        duration = 0
        output = (
            f"用户手动停止；子进程已确认退出。\n{exc}"
            if stopped_by_user
            else (
                f"任务超时；子进程已确认退出。\n{exc}"
                if timed_out
                else str(exc)
            )
        )

    history_output = output
    if (
        status == "success"
        and getattr(validation, "checked", None) is True
        and getattr(validation, "ok", None) is True
    ):
        try:
            evidence = _build_history_validation_evidence(
                argument_row,
                run_uid=str(history_run_uid or ""),
                machine_output=machine_output,
                status=status,
                exit_code=int(getattr(locals().get("proc"), "returncode", 0)),
                started_at=validation_started_at,
                validation_message=validation.message,
            )
            history_output = _history_output_with_validation_evidence(
                output,
                evidence,
            )
        except Exception as exc:
            status = "failed"
            output = output + f"\nDATA_EVIDENCE_FAILED: {exc}"
            history_output = output

    try:
        _task_history_finish(
            engine,
            history_run_uid,
            status=status,
            duration=duration,
            exit_code=getattr(locals().get("proc"), "returncode", None),
            output=history_output,
            task_type=str(row.get("task_type") or "").strip(),
        )
    except Exception as exc:
        status = "failed"
        output = output + f"\nPOOL_ACTIVATION_FAILED: {exc}"
        history_output = output
        # The activation transaction rolled back, so every staged row remains
        # fail-closed.  Terminalize the scheduler audit as failed in a second
        # transaction; never leave the task summary claiming success.
        _task_history_finish(
            engine,
            history_run_uid,
            status="failed",
            duration=duration,
            exit_code=getattr(locals().get("proc"), "returncode", None),
            output=history_output,
        )
    update_scheduler_task(
        engine,
        int(task_id),
        {
            "last_run_status": status,
            "last_run_output": output,
            "last_run_duration": duration,
        },
    )
    logger.info("任务 %s 完成: %s (%ds)", task_name, status, duration)


def _run_task_async(row: dict, root: Path, engine) -> None:
    semaphore = _task_lane_semaphore(row)
    with semaphore:
        task_id = int(row["id"])
        history_run_uid = str(row.get("_history_run_uid") or "").strip()
        with _running_lock:
            if history_run_uid:
                _running_history_uids[task_id] = history_run_uid
        try:
            _run_task(row, root, engine)
        finally:
            with _running_lock:
                _running_procs.pop(task_id, None)
                _running_history_uids.pop(task_id, None)
                _stop_pending_task_ids.discard(task_id)
                _stop_requested_task_ids.discard(task_id)
                _timeout_pending_task_ids.discard(task_id)
                _timeout_requested_task_ids.discard(task_id)
                _running_task_ids.discard(task_id)
                _fast_lane_running_task_ids.discard(task_id)
                _quote_lane_running_task_ids.discard(task_id)
                _alert_lane_running_task_ids.discard(task_id)
                _delivery_lane_running_task_ids.discard(task_id)
                _running_skip_logged_at.pop(task_id, None)
            # Re-evaluate the target-date DAG immediately.  A completed
            # upstream should not wait a full poll interval before its
            # downstream becomes claimable.
            _scheduler_wake_event.set()


def launch_scheduler_task(
    row: dict,
    *,
    root: Path | None = None,
    engine=None,
) -> dict[str, object]:
    """Claim and launch one scheduler task without blocking an API worker.

    The database claim is shared with the standalone scheduler, so a button
    click cannot start a second copy of a task that is already running there.
    """
    engine = engine or get_engine()
    root = root or Path(__file__).resolve().parents[2]
    task_id = int(row["id"])
    task_name = str(row.get("task_name") or task_id)
    requested_history_uid = str(
        row.get("_manual_history_run_uid") or ""
    ).strip().lower()
    governance_block_reason = strategy_governance_task_block_reason(row)
    if governance_block_reason:
        return {
            "accepted": False,
            "status": governance_block_reason,
            "task_id": task_id,
            "task_name": task_name,
            "job_id": "",
        }
    if requested_history_uid and not re.fullmatch(
        r"[0-9a-f]{32}", requested_history_uid
    ):
        return {
            "accepted": False,
            "status": "invalid_history_identity",
            "task_id": task_id,
            "task_name": task_name,
            "job_id": "",
        }
    if not scheduler_task_owned_by_current_host(row):
        return {
            "accepted": False,
            "status": "delegated_to_other_host",
            "task_id": task_id,
            "task_name": task_name,
        }
    if (
        str(os.environ.get("PROBIGA_DEPLOYMENT_MODE") or "").strip().lower()
        == "production"
        and _scheduler_build_commit_sha() == "0" * 40
    ):
        return {
            "accepted": False,
            "status": "build_identity_unavailable",
            "task_id": task_id,
            "task_name": task_name,
        }
    if int(row.get("enabled") or 0) != 1:
        return {
            "accepted": False,
            "status": "disabled",
            "task_id": task_id,
            "task_name": task_name,
        }

    with _running_lock:
        proc = _running_procs.get(task_id)
        if task_id in _running_task_ids or (
            proc is not None and proc.poll() is None
        ):
            return {
                "accepted": False,
                "status": "already_running",
                "task_id": task_id,
                "task_name": task_name,
            }
        _running_task_ids.add(task_id)
        if _uses_fast_lane(row):
            _fast_lane_running_task_ids.add(task_id)
        if _uses_quote_lane(row):
            _quote_lane_running_task_ids.add(task_id)
        if _uses_alert_lane(row):
            _alert_lane_running_task_ids.add(task_id)
        if _uses_delivery_lane(row):
            _delivery_lane_running_task_ids.add(task_id)

    try:
        claimed = _claim_task_run(row, engine)
    except Exception:
        with _running_lock:
            _running_task_ids.discard(task_id)
            _fast_lane_running_task_ids.discard(task_id)
            _quote_lane_running_task_ids.discard(task_id)
            _alert_lane_running_task_ids.discard(task_id)
            _delivery_lane_running_task_ids.discard(task_id)
        raise
    if not claimed:
        with _running_lock:
            _running_task_ids.discard(task_id)
            _fast_lane_running_task_ids.discard(task_id)
            _quote_lane_running_task_ids.discard(task_id)
            _alert_lane_running_task_ids.discard(task_id)
            _delivery_lane_running_task_ids.discard(task_id)
        return {
            "accepted": False,
            "status": "already_running",
            "task_id": task_id,
            "task_name": task_name,
        }

    manual_row = dict(row)
    manual_row["_trigger_source"] = "manual"
    manual_history_uid = _task_history_start(
        engine,
        manual_row,
        run_uid=requested_history_uid or uuid.uuid4().hex,
    )
    if not manual_history_uid:
        with _running_lock:
            _running_task_ids.discard(task_id)
            _fast_lane_running_task_ids.discard(task_id)
            _quote_lane_running_task_ids.discard(task_id)
            _alert_lane_running_task_ids.discard(task_id)
            _delivery_lane_running_task_ids.discard(task_id)
        update_scheduler_task(
            engine,
            task_id,
            {
                "last_run_status": "failed",
                "last_run_output": (
                    "manual launch rejected: scheduler audit row unavailable"
                ),
                "last_run_duration": 0,
            },
        )
        return {
            "accepted": False,
            "status": "audit_unavailable",
            "task_id": task_id,
            "task_name": task_name,
            "job_id": "",
        }
    if requested_history_uid and manual_history_uid != requested_history_uid:
        with _running_lock:
            _running_task_ids.discard(task_id)
            _fast_lane_running_task_ids.discard(task_id)
            _quote_lane_running_task_ids.discard(task_id)
            _alert_lane_running_task_ids.discard(task_id)
            _delivery_lane_running_task_ids.discard(task_id)
        update_scheduler_task(
            engine,
            task_id,
            {
                "last_run_status": "failed",
                "last_run_output": (
                    "manual launch rejected: scheduler audit identity mismatch"
                ),
                "last_run_duration": 0,
            },
        )
        _task_history_finish(
            engine,
            manual_history_uid,
            status="failed",
            duration=0,
            exit_code=None,
            output="manual launch rejected: scheduler audit identity mismatch",
        )
        return {
            "accepted": False,
            "status": "audit_identity_mismatch",
            "task_id": task_id,
            "task_name": task_name,
            "job_id": "",
        }
    manual_row["_history_run_uid"] = manual_history_uid
    manual_row["_history_started"] = bool(manual_history_uid)
    worker = threading.Thread(
        target=_run_task_async,
        args=(manual_row, root, engine),
        daemon=True,
        name=f"scheduler-manual-task-{task_id}",
    )
    try:
        worker.start()
    except Exception:
        with _running_lock:
            _running_task_ids.discard(task_id)
            _fast_lane_running_task_ids.discard(task_id)
            _quote_lane_running_task_ids.discard(task_id)
            _alert_lane_running_task_ids.discard(task_id)
            _delivery_lane_running_task_ids.discard(task_id)
        update_scheduler_task(
            engine,
            task_id,
            {
                "last_run_status": "failed",
                "last_run_output": "manual task thread failed to start",
                "last_run_duration": 0,
            },
        )
        _task_history_finish(
            engine,
            manual_row.get("_history_run_uid"),
            status="failed",
            duration=0,
            exit_code=None,
            output="manual task thread failed to start",
        )
        raise
    return {
        "accepted": True,
        "status": "running",
        "task_id": task_id,
        "task_name": task_name,
        "job_id": str(manual_history_uid or ""),
    }


def _catchup_on_startup() -> None:
    """启动时将卡在 running 的任务重置"""
    try:
        engine = get_engine()
        # Cleanup is ownership-aware.  The old bulk reset touched every
        # ``running`` row in the shared database and could therefore clear a
        # healthy task still executing on the other scheduler host.
        _cleanup_stale_running_tasks(engine)
        logger.info("启动重置已完成")
    except Exception as exc:
        logger.error("启动补跑异常: %s", exc)


def _check_and_run_tasks(mode: str = "embedded", stop_event: threading.Event | None = None) -> None:
    """后台调度线程：每分钟检查一次，到点执行任务"""
    root = Path(__file__).resolve().parents[2]
    startup_time = _now_shanghai_naive()
    poll_seconds = int(get_scheduler_runtime_config()["poll_seconds"])
    while not (stop_event and stop_event.is_set()):
        try:
            engine = get_engine()
            dispatch_authorized = mode != "standalone"
            heartbeat_detail: dict[str, object] = {}
            try:
                _write_scheduler_heartbeat(engine, mode)
                dispatch_authorized, heartbeat_detail = (
                    _standalone_heartbeat_allows_dispatch(engine, mode)
                )
            except Exception as exc:
                dispatch_authorized = False
                heartbeat_detail = {"errors": ["heartbeat_write_failed"]}
                logger.error("写入调度心跳异常，停止本轮任务派发: %s", exc)

            if mode == "standalone" and not dispatch_authorized:
                logger.error(
                    "独立调度器身份无法唯一证明，停止本轮任务派发: %s",
                    heartbeat_detail.get("errors") or ["authority_unknown"],
                )
                if _wait_for_scheduler_poll(stop_event, poll_seconds):
                    break
                continue

            try:
                _cleanup_stale_running_tasks(engine)
            except Exception as exc:
                logger.warning("僵尸检测异常: %s", exc)

            _maybe_cleanup_history(engine)

            with engine.connect() as conn:
                result = conn.execute(
                    text("SELECT id, task_name, task_type, script_path, script_args, cron_time, interval_minutes, "
                         "enabled, date_param, last_run_at, last_triggered_at, last_run_status, last_run_duration, "
                         "last_run_output "
                         "FROM st_scheduled_tasks WHERE enabled = 1 ORDER BY sort_order")
                )
                rows = [dict(zip(result.keys(), row)) for row in result.fetchall()]

            now = _now_shanghai_naive()
            _attach_daily_recovery_targets(engine, rows, now=now)
            if _release_catchup_disabled_for_deferred_database():
                release_authorized = False
                release_authorization_reason = "governance_database_deferred"
            else:
                _attach_release_catchup_history(engine, rows)
                _attach_release_catchup_expected_targets(
                    engine,
                    rows,
                    now=now,
                )
                release_authorized, release_authorization_reason = (
                    _attach_release_catchup_authorization(
                        engine,
                        rows,
                        mode=mode,
                        now=now,
                    )
                )
            if not release_authorized:
                logger.debug(
                    "Release data catch-up is not active: %s",
                    release_authorization_reason,
                )
            qmt_dispatch_authorized, qmt_dispatch_reason = (
                _qmt_windows_dispatch_preflight(engine, mode=mode)
            )
            if not qmt_dispatch_authorized:
                logger.warning(
                    "QMT dispatch preflight is not ready; business tasks remain "
                    "paused: %s",
                    qmt_dispatch_reason,
                )
            if any(
                str(row.get("task_type") or "").strip()
                in DAILY_RESULT_MAINTENANCE_TASK_TYPES
                for row in rows
            ):
                daily_result_ready, daily_result_reason = (
                    _daily_result_pipeline_gate(engine, now=now)
                )
            else:
                daily_result_ready, daily_result_reason = (
                    True,
                    "not_applicable",
                )
            time_str = now.strftime("%H:%M")
            max_pending_tasks = max(1, int(get_scheduler_runtime_config()["max_concurrent_tasks"]))

            # Do not let the database row order decide who gets the only
            # worker slot.  A continuously due realtime task must yield to a
            # minute task that has been waiting longer.
            rows.sort(key=lambda row: _scheduler_task_sort_key(row, now=now))

            for row in rows:
                task_id = row["id"]
                task_name = row["task_name"]
                cron_time = str(row["cron_time"] or "17:10")
                interval_minutes = int(row.get("interval_minutes") or 0)
                last_triggered = row.get("last_triggered_at")
                release_catchup_due = _release_build_catchup_allowed(
                    row,
                    now=now,
                )
                release_catchup_pending = _release_build_catchup_pending(row)
                membership_ordinary_due = _membership_ordinary_publish_due(
                    row,
                    now=now,
                )
                if membership_ordinary_due:
                    # The ordinary QMT publisher owns creation of today's
                    # immutable snapshot.  Read-only release verification is
                    # allowed only on a later poll after that row exists.
                    release_catchup_due = False

                governance_block_reason = strategy_governance_task_block_reason(
                    row
                )
                if governance_block_reason:
                    logger.warning(
                        "Skip strategy governance task while dispatch is blocked: "
                        "%s (reason=%s)",
                        task_name,
                        governance_block_reason,
                    )
                    continue

                owner = scheduler_task_host_owner(row)
                if _should_skip_task_for_host(row):
                    skip_key = (int(task_id), now.strftime("%Y-%m-%d"))
                    if skip_key not in _delegated_skip_logged_for:
                        if owner == SCHEDULER_OWNER_UNAVAILABLE:
                            logger.error(
                                "Skip task with unavailable/unfrozen provider identity: "
                                "%s (type=%s)",
                                task_name,
                                row.get("task_type"),
                            )
                        else:
                            logger.info(
                                "Skip task owned by the other scheduler host: "
                                "%s (type=%s owner=%s)",
                                task_name,
                                row.get("task_type"),
                                owner,
                            )
                        _delegated_skip_logged_for.add(skip_key)
                    continue

                if (
                    owner == SCHEDULER_OWNER_WINDOWS_QMT
                    and not qmt_dispatch_authorized
                ):
                    logger.warning(
                        "Defer QMT business task until exact release preflight: "
                        "%s (reason=%s)",
                        task_name,
                        qmt_dispatch_reason,
                    )
                    continue

                if (
                    str(row.get("task_type") or "").strip()
                    in DAILY_RESULT_MAINTENANCE_TASK_TYPES
                    and not daily_result_ready
                ):
                    logger.warning(
                        "Defer historical maintenance until the latest daily "
                        "strategy/watchlist result is ready: %s (reason=%s)",
                        task_name,
                        daily_result_reason,
                    )
                    continue

                if (
                    release_catchup_pending
                    and not release_catchup_due
                    and not membership_ordinary_due
                ):
                    logger.debug(
                        "Defer ordinary dispatch for exact-build release task: %s",
                        task_name,
                    )
                    continue

                if interval_minutes > 0:
                    ref_time = last_triggered or row.get("last_run_at")
                    if ref_time and isinstance(ref_time, str):
                        try:
                            ref_time = datetime.strptime(ref_time[:19], "%Y-%m-%d %H:%M:%S")
                        except Exception as exc:
                            logger.debug("invalid last_triggered_at for task %s: %s", task_name, exc)
                            ref_time = None
                    if ref_time:
                        elapsed = (now - ref_time).total_seconds() / 60
                        if elapsed < interval_minutes and not release_catchup_due:
                            continue
                else:
                    if (
                        not release_catchup_due
                        and not membership_ordinary_due
                        and not _cron_due(row, now=now)
                    ):
                        continue
                    if (
                        not release_catchup_due
                        and not membership_ordinary_due
                        and time_str != cron_time
                    ):
                        if not _overdue_cron_allowed(
                            row,
                            now=now,
                            cron_time=cron_time,
                            startup_time=startup_time,
                        ):
                            skip_key = (int(task_id), now.strftime("%Y-%m-%d"))
                            if skip_key not in _overdue_skip_logged_for:
                                logger.info(
                                    "Skip stale overdue cron task after restart: %s (cron=%s, now=%s)",
                                    task_name,
                                    cron_time,
                                    time_str,
                                )
                                _overdue_skip_logged_for.add(skip_key)
                            continue
                        status = str(row.get("last_run_status") or "").strip().lower()
                        if _task_status_is_retryable(row):
                            logger.warning(
                                "retry overdue cron task: %s (cron=%s, now=%s, status=%s)",
                                task_name,
                                cron_time,
                                time_str,
                                status,
                            )
                        else:
                            logger.warning(
                                "run overdue cron task after scheduler restart: %s (cron=%s, now=%s)",
                                task_name,
                                cron_time,
                                time_str,
                            )

                if (
                    str(row.get("task_type") or "").strip()
                    in DAILY_RESULT_RECOVERY_TASK_TYPES
                    and row.get("_scheduler_target_available") is not True
                ):
                    target_block_output = (
                        "DATA_BLOCKED: authoritative daily-result target trade "
                        "date is unavailable; retryable=true"
                    )
                    if (
                        str(row.get("last_run_status") or "").strip().lower()
                        != "failed"
                        or str(row.get("last_run_output") or "").strip()
                        != target_block_output
                    ):
                        update_scheduler_task(
                            engine,
                            int(task_id),
                            {
                                "last_run_status": "failed",
                                "last_run_output": target_block_output,
                                "last_run_duration": 0,
                            },
                        )
                    logger.warning(
                        "Defer due daily-result stage until authoritative target "
                        "trade date is available: %s",
                        task_name,
                    )
                    continue

                dependency_ready, dependency_reason = (
                    _strategy_pipeline_dependencies_ready(row, engine, now)
                )
                if not dependency_ready:
                    logger.warning(
                        "Defer strategy pipeline task %s until prerequisite: %s",
                        task_name,
                        dependency_reason,
                    )
                    continue

                if release_catchup_due:
                    release_dependencies_ready, release_dependency_reason = (
                        _release_catchup_dependencies_ready(row, rows)
                    )
                    if not release_dependencies_ready:
                        logger.info(
                            "Defer release catch-up task %s until exact-build "
                            "prerequisite: %s",
                            task_name,
                            release_dependency_reason,
                        )
                        continue

                non_trading_day_action = (
                    False
                    if release_catchup_due
                    else _should_skip_non_trading_day(row, engine, now)
                )
                if non_trading_day_action is None:
                    logger.warning(
                        "Defer scheduled market task until trade calendar is available: %s (type=%s)",
                        task_name,
                        row.get("task_type"),
                    )
                    continue
                if non_trading_day_action:
                    logger.info(
                        "Skip scheduled market task on non-trading day: %s (type=%s)",
                        task_name,
                        row.get("task_type"),
                    )
                    _mark_non_trading_day_skip(row, engine, now)
                    continue

                if _should_skip_outside_intraday_window(row, now):
                    skip_key = (int(task_id), now.strftime("%Y-%m-%d"))
                    if skip_key not in _intraday_skip_logged_for:
                        logger.info(
                            "Skip intraday task outside trading window: %s (type=%s, now=%s)",
                            task_name,
                            row.get("task_type"),
                            time_str,
                        )
                        _intraday_skip_logged_for.add(skip_key)
                    continue

                if release_catchup_due:
                    row["_trigger_source"] = "release_catchup"

                with _running_lock:
                    task_running = task_id in _running_task_ids
                    if not task_running:
                        proc = _running_procs.get(task_id)
                        task_running = bool(proc and proc.poll() is None)
                    if task_running:
                        last_log = _running_skip_logged_at.get(int(task_id))
                        if not last_log or (now - last_log).total_seconds() >= 300:
                            logger.warning("任务 %s 仍在运行，跳过本次触发", task_name)
                            _running_skip_logged_at[int(task_id)] = now
                        else:
                            logger.debug("任务 %s 仍在运行，跳过本次触发", task_name)
                        continue
                    uses_delivery_lane = _uses_delivery_lane(row)
                    uses_fast_lane = _uses_fast_lane(row)
                    uses_quote_lane = _uses_quote_lane(row)
                    uses_alert_lane = _uses_alert_lane(row)
                    if uses_quote_lane and not _scheduler_lane_has_capacity(
                        row,
                        max_general_tasks=max_pending_tasks,
                    ):
                        logger.debug("Scheduler quote lane full; defer task %s", task_name)
                        continue
                    if uses_alert_lane and not _scheduler_lane_has_capacity(
                        row,
                        max_general_tasks=max_pending_tasks,
                    ):
                        logger.debug("Scheduler alert lane full; defer task %s", task_name)
                        continue
                    if uses_delivery_lane and not _scheduler_lane_has_capacity(
                        row,
                        max_general_tasks=max_pending_tasks,
                    ):
                        logger.debug("Scheduler delivery lane full; defer task %s", task_name)
                        continue
                    if uses_fast_lane and not _scheduler_lane_has_capacity(
                        row,
                        max_general_tasks=max_pending_tasks,
                    ):
                        logger.debug("Scheduler fast lane full; defer task %s", task_name)
                        continue
                    if (
                        not uses_alert_lane
                        and not uses_delivery_lane
                        and not uses_fast_lane
                        and not uses_quote_lane
                        and not _scheduler_lane_has_capacity(
                            row,
                            max_general_tasks=max_pending_tasks,
                        )
                    ):
                        logger.debug(
                            "Scheduler capacity full (%s); defer task %s",
                            max_pending_tasks,
                            task_name,
                        )
                        continue
                    _running_task_ids.add(int(task_id))
                    if uses_fast_lane:
                        _fast_lane_running_task_ids.add(int(task_id))
                    if uses_quote_lane:
                        _quote_lane_running_task_ids.add(int(task_id))
                    if uses_alert_lane:
                        _alert_lane_running_task_ids.add(int(task_id))
                    if uses_delivery_lane:
                        _delivery_lane_running_task_ids.add(int(task_id))

                try:
                    claimed = _claim_task_run(row, engine)
                except Exception as exc:
                    logger.warning("任务 %s 抢占失败，跳过本次触发: %s", task_name, exc)
                    with _running_lock:
                        _running_task_ids.discard(int(task_id))
                        _fast_lane_running_task_ids.discard(int(task_id))
                        _quote_lane_running_task_ids.discard(int(task_id))
                        _alert_lane_running_task_ids.discard(int(task_id))
                        _delivery_lane_running_task_ids.discard(int(task_id))
                    continue
                if not claimed:
                    logger.warning("任务 %s 已被其他调度实例抢占，跳过本次触发", task_name)
                    with _running_lock:
                        _running_task_ids.discard(int(task_id))
                        _fast_lane_running_task_ids.discard(int(task_id))
                        _quote_lane_running_task_ids.discard(int(task_id))
                        _alert_lane_running_task_ids.discard(int(task_id))
                        _delivery_lane_running_task_ids.discard(int(task_id))
                    continue

                logger.info("执行定时任务: %s (cron=%s, now=%s)", task_name, cron_time, time_str)
                history_uid = _task_history_start(
                    engine,
                    row,
                    run_uid=uuid.uuid4().hex,
                )
                if not history_uid:
                    with _running_lock:
                        _running_task_ids.discard(int(task_id))
                        _fast_lane_running_task_ids.discard(int(task_id))
                        _quote_lane_running_task_ids.discard(int(task_id))
                        _alert_lane_running_task_ids.discard(int(task_id))
                        _delivery_lane_running_task_ids.discard(int(task_id))
                    update_scheduler_task(
                        engine,
                        int(task_id),
                        {
                            "last_run_status": "failed",
                            "last_run_output": (
                                "scheduled execution rejected: "
                                "audit row unavailable"
                            ),
                            "last_run_duration": 0,
                        },
                    )
                    logger.error(
                        "任务 %s 的审计记录不可用，拒绝启动执行线程",
                        task_name,
                    )
                    continue
                row["_history_run_uid"] = history_uid
                row["_history_started"] = True
                worker = threading.Thread(
                    target=_run_task_async,
                    args=(row, root, engine),
                    daemon=True,
                    name=f"scheduler-task-{task_id}",
                )
                try:
                    worker.start()
                except Exception as exc:
                    with _running_lock:
                        _running_task_ids.discard(int(task_id))
                        _fast_lane_running_task_ids.discard(int(task_id))
                        _quote_lane_running_task_ids.discard(int(task_id))
                        _alert_lane_running_task_ids.discard(int(task_id))
                        _delivery_lane_running_task_ids.discard(int(task_id))
                    output = f"scheduled task thread failed to start: {exc}"
                    update_scheduler_task(
                        engine,
                        int(task_id),
                        {
                            "last_run_status": "failed",
                            "last_run_output": output,
                            "last_run_duration": 0,
                        },
                    )
                    _task_history_finish(
                        engine,
                        history_uid,
                        status="failed",
                        duration=0,
                        exit_code=None,
                        output=output,
                    )
                    logger.exception("Failed to start scheduler task thread for %s", task_name)

        except Exception as exc:
            logger.error("调度线程异常: %s", exc)

        if _wait_for_scheduler_poll(stop_event, poll_seconds):
            break


def start_embedded_scheduler() -> threading.Thread | None:
    global _scheduler_thread, _scheduler_stop_event
    runtime = get_scheduler_runtime_config()
    if not runtime["embedded_enabled"]:
        logger.info("内嵌调度已禁用；如需独立调度进程，请运行 tools/run_scheduler_daemon.py")
        return None
    if _scheduler_thread and _scheduler_thread.is_alive():
        return _scheduler_thread
    _scheduler_stop_event = threading.Event()
    _scheduler_thread = threading.Thread(
        target=_check_and_run_tasks,
        args=("embedded", _scheduler_stop_event),
        daemon=True,
        name="scheduler-daemon",
    )
    _scheduler_thread.start()
    logger.info(
        "定时调度线程已启动 (max_concurrent_tasks=%s, poll=%ss)",
        runtime["max_concurrent_tasks"],
        runtime["poll_seconds"],
    )
    return _scheduler_thread


def stop_embedded_scheduler(timeout_seconds: float = 5.0) -> None:
    """Signal the embedded scheduler loop to stop and wait briefly for it."""
    global _scheduler_thread, _scheduler_stop_event
    thread = _scheduler_thread
    stop_event = _scheduler_stop_event
    if stop_event is not None:
        stop_event.set()
        _scheduler_wake_event.set()
    if thread is not None and thread.is_alive() and thread is not threading.current_thread():
        thread.join(timeout=max(0.0, float(timeout_seconds)))
        if thread.is_alive():
            logger.warning("Embedded scheduler did not stop within %.1fs", float(timeout_seconds))
            return
    _scheduler_thread = None
    _scheduler_stop_event = None


def run_scheduler_forever() -> None:
    """Run the scheduler loop as a standalone process."""
    runtime = get_scheduler_runtime_config()
    logger.info(
        "独立调度进程启动 (max_concurrent_tasks=%s, poll=%ss)",
        runtime["max_concurrent_tasks"],
        runtime["poll_seconds"],
    )
    _check_and_run_tasks(mode="standalone")
