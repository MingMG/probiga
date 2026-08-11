# -*- coding: utf-8 -*-
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import date, datetime
from socket import gethostname
from pathlib import Path

from sqlalchemy import text

from server.api.routers._engine import get_engine
from server.common.config import get_api_mysql_pool_config, get_scheduler_runtime_config
from server.common.process_env import build_child_env
from server.common.scheduler_script_policy import (
    SchedulerScriptPolicyError,
    resolve_scheduler_script,
)
from server.common.scheduler_args import build_scheduler_task_args
from server.common.scheduler_tasks import (
    claim_scheduler_task_run,
    update_scheduler_task,
)
from server.common.scheduler_validation import (
    is_market_closed_skip_output,
    scheduler_output_status,
    validate_scheduler_task_result,
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
CRON_CATCHUP_WINDOW_SECONDS = int(os.environ.get("SCHEDULER_CRON_CATCHUP_WINDOW_SECONDS", "180"))
CRITICAL_CRON_CATCHUP_WINDOW_SECONDS = int(os.environ.get("SCHEDULER_CRITICAL_CRON_CATCHUP_WINDOW_SECONDS", "10800"))
CRON_RETRY_INTERVAL_MINUTES = max(1, int(os.environ.get("SCHEDULER_CRON_RETRY_INTERVAL_MINUTES", "15")))
STALE_RUNNING_GRACE_MINUTES = int(os.environ.get("SCHEDULER_STALE_RUNNING_GRACE_MINUTES", "5"))
# A daily recommendation is a user-facing deliverable.  It must not be lost
# merely because a long post-market sync occupies the scheduler at its exact
# cron minute; allow it to be claimed later on the same day.
CRITICAL_CRON_CATCHUP_TASK_TYPES = {"analysis_morning_strict", "analysis_fast"}
CRITICAL_CRON_CATCHUP_TASK_TYPES.add("analysis_premarket_external")
CRITICAL_CRON_CATCHUP_TASK_TYPES.add("sim_trade_signal_prepare")
CRITICAL_CRON_CATCHUP_TASK_TYPES.update(
    {
        "trading_v2_premarket_decision",
        "trading_v2_close_decision",
        "trading_v2_reconciliation",
        "trading_v2_level1_validation",
        "trading_v2_strategy_health",
        "stock_kline",
        "trading_v3_close_decision",
        "trading_v3_premarket_review",
        "trading_v3_counterfactual_audit",
        "qmt_membership_snapshot",
        "concept_constituent_east",
        "capital_flow",
        "capital_flow_batch_fast",
        "market_overview_daily",
    }
)
CRITICAL_CRON_CATCHUP_WINDOWS_SECONDS = {
    "trading_v2_premarket_decision": 6 * 60 * 60,
    "trading_v2_close_decision": 4 * 60 * 60,
    "trading_v2_reconciliation": 4 * 60 * 60,
    "trading_v2_level1_validation": 4 * 60 * 60,
    "trading_v2_strategy_health": 4 * 60 * 60,
    "stock_kline": 8 * 60 * 60,
    "trading_v3_close_decision": 8 * 60 * 60,
    "trading_v3_premarket_review": 3 * 60 * 60,
    "trading_v3_counterfactual_audit": 8 * 60 * 60,
    "qmt_membership_snapshot": 8 * 60 * 60,
    "concept_constituent_east": 8 * 60 * 60,
    # These tables feed the watchlist's current-session market and funds
    # labels.  Missing their exact cron minute must not leave the UI pinned to
    # a prior trading day for the rest of the session.
    "capital_flow": 8 * 60 * 60,
    "capital_flow_batch_fast": 8 * 60 * 60,
    "market_overview_daily": 8 * 60 * 60,
}
PREMARKET_RECOMMENDATION_CATCHUP_WINDOW_SECONDS = int(
    os.environ.get("SCHEDULER_PREMARKET_RECOMMENDATION_CATCHUP_WINDOW_SECONDS", "7200")
)
RECOMMENDATION_CRON_CATCHUP_WINDOW_SECONDS = int(
    os.environ.get("SCHEDULER_RECOMMENDATION_CATCHUP_WINDOW_SECONDS", "21600")
)
NON_TRADING_DAY_SKIP_TYPES = {
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
    "concept_ths_current",
    "concept_ths_kline",
    "concept_ths_minute",
    "etf_forward_daily",
    "index_current",
    "index_kline",
    "index_minute",
    "intraday_quality_check",
    "intraday_realtime",
    "intraday_minute_flow",
    "intraday_capital_flow_fast",
    "intraday_minute_kline",
    "capital_flow_batch_fast",
    "market_overview_daily",
    "qmt_intraday_realtime",
    "quality_check_post",
    "quality_check_pre",
    "qmt_membership_snapshot",
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
    "trading_v2_intraday_activation",
    "trading_v2_level1_validation",
    "trading_v2_paper_tick",
    "trading_v3_close_decision",
    "trading_v3_premarket_review",
}
NON_TRADING_DAY_SKIP_PATHS = {
    "biz/analysis/sync_analysis_fast.py",
    "tools/crawl_minute_kline.py",
    "tools/crawl_realtime_batch.py",
    "tools/data_quality_check.py",
    "tools/run_single_table.py",
}
# These tasks require the signed-in Guojin desktop QMT process and are owned by
# the Windows scheduler/bridge.  The production Linux scheduler must leave
# their database rows visible/enabled, but must never attempt to execute them.
WINDOWS_QMT_BRIDGE_TASK_TYPES = {
    "all_code",
    "all_index_code",
    "concept_code_east",
    "concept_constituent_east",
    "etf_forward_daily",
    # Xueqiu serves JSON from the operator's Windows egress but returns an
    # HTML WAF page from the production Linux egress.  Keep this source on the
    # same Windows-owned scheduler lane so the shared task row cannot be
    # claimed by the incapable host.
    "fetch_hot_rank_xq",
    "index_constituent",
    "index_current",
    "index_kline",
    "index_minute",
    "intraday_minute_kline",
    "intraday_capital_flow_fast",
    "intraday_realtime",
    "qmt_membership_snapshot",
    "stock_current",
    "stock_kline",
    "stock_relations_qmt",
}
WINDOWS_QMT_BRIDGE_SCRIPT_PATHS = {
    "tools/run_etf_forward_daily.py",
    "tools/sync_bigqmt_reference.py",
    "tools/sync_qmt_primary.py",
}
_running_procs: dict[int, subprocess.Popen] = {}
_running_task_ids: set[int] = set()
_fast_lane_running_task_ids: set[int] = set()
_running_lock = threading.Lock()
_running_skip_logged_at: dict[int, datetime] = {}
_intraday_skip_logged_for: set[tuple[int, str]] = set()
_overdue_skip_logged_for: set[tuple[int, str]] = set()
_delegated_skip_logged_for: set[tuple[int, str]] = set()
_task_semaphore: threading.Semaphore | None = None
_fast_lane_semaphore: threading.Semaphore | None = None
_scheduler_thread: threading.Thread | None = None
_scheduler_stop_event: threading.Event | None = None
_scheduler_started_at = datetime.now()
_scheduler_instance_id = f"{gethostname()}-{os.getpid()}"


LONG_RUNNING_TASK_TYPES = {
    "analysis_fast",
    "analysis_premarket_external",
    "capital_flow",
    "capital_flow_batch_fast",
    "concept_east_kline",
    "concept_east_minute",
    "concept_flow",
    "concept_ths_kline",
    "concept_ths_minute",
    "index_kline",
    "index_minute",
    "qmt_local_gap_repair_execute",
    "qmt_nightly_reconciliation",
    "stock_kline",
    "stock_minute",
    "stock_minute_flow",
}
LONG_RUNNING_PATH_PARTS = {
    "biz/analysis/sync_analysis_fast.py",
    "biz/stock_market/sync_stock_market.py",
    "tools/run_single_table.py",
}
FAST_RUNNING_TASK_TYPES = {
    "hot_rank_sina",
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
INTRADAY_WINDOW_TASK_TYPES = {
    "intraday_capital_flow_fast",
    "intraday_minute_flow",
    "intraday_minute_kline",
    "intraday_quality_check",
    "intraday_realtime",
    "qmt_intraday_realtime",
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
}


def _is_trade_day(engine, day: date | None = None) -> bool:
    day = day or datetime.now().date()
    try:
        with engine.connect() as conn:
            count = conn.execute(
                text(
                    "SELECT COUNT(*) FROM si_trade_calendar "
                    "WHERE trade_date = :trade_date AND trade_status = 1"
                ),
                {"trade_date": day.isoformat()},
            ).scalar()
        return bool(count)
    except Exception as exc:
        logger.warning("trade calendar lookup failed, fallback to weekday: %s", exc)
        return day.weekday() < 5


def _coerce_datetime(value) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.strptime(str(value)[:19], "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


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
    if last_triggered and last_triggered.date() == now.date():
        status = str(row.get("last_run_status") or "").strip().lower()
        if status not in {"failed", "timeout", "stopped"}:
            return False
        retry_at = _coerce_datetime(row.get("last_run_at")) or last_triggered
        if (
            now - retry_at
        ).total_seconds() < CRON_RETRY_INTERVAL_MINUTES * 60:
            return False
    cron_min = _parse_hhmm(cron_time)
    if cron_min is None:
        return False
    current_min = now.hour * 60 + now.minute
    missed_seconds = (current_min - cron_min) * 60
    return 0 < missed_seconds <= catchup_window


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
    current_min = now.hour * 60 + now.minute
    if current_min < cron_min:
        return False

    today = now.date()
    last_triggered = _coerce_datetime(row.get("last_triggered_at"))
    if last_triggered and last_triggered.date() > today:
        return False
    if not last_triggered or last_triggered.date() < today:
        return True

    status = str(row.get("last_run_status") or "").strip().lower()
    if status not in {"failed", "timeout", "stopped"}:
        return False
    retry_at = _coerce_datetime(row.get("last_run_at")) or last_triggered
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
    current = now.hour * 60 + now.minute
    return current < start or current > end


def _is_windows_qmt_bridge_task(row: dict) -> bool:
    task_type = str(row.get("task_type") or "").strip()
    script_path = str(row.get("script_path") or "").strip().replace("\\", "/")
    return (
        task_type in WINDOWS_QMT_BRIDGE_TASK_TYPES
        or script_path in WINDOWS_QMT_BRIDGE_SCRIPT_PATHS
    )


def _should_delegate_to_windows_qmt_bridge(
    row: dict,
    *,
    platform_name: str | None = None,
) -> bool:
    current_platform = platform_name or os.name
    return current_platform != "nt" and _is_windows_qmt_bridge_task(row)


def _should_skip_task_for_host(
    row: dict,
    *,
    platform_name: str | None = None,
) -> bool:
    """Run QMT jobs only on Windows and all other jobs only on Linux."""
    current_platform = platform_name or os.name
    is_windows_owned = _is_windows_qmt_bridge_task(row)
    return (not is_windows_owned) if current_platform == "nt" else is_windows_owned


def _task_timeout_minutes(row: dict) -> int:
    task_type = str(row.get("task_type") or "").strip()
    script_path = str(row.get("script_path") or "").replace("\\", "/").strip()
    interval_minutes = int(row.get("interval_minutes") or 0)

    if interval_minutes > 0:
        return max(FAST_TASK_TIMEOUT_MINUTES, min(DEFAULT_TASK_TIMEOUT_MINUTES, interval_minutes * 3))
    if task_type in LONG_RUNNING_TASK_TYPES or script_path in LONG_RUNNING_PATH_PARTS:
        return LONG_TASK_TIMEOUT_MINUTES
    if task_type in FAST_RUNNING_TASK_TYPES:
        return FAST_TASK_TIMEOUT_MINUTES
    return DEFAULT_TASK_TIMEOUT_MINUTES


def _scheduler_task_sort_key(row: dict, *, now: datetime) -> tuple[int, float, int]:
    """Order due tasks by how long they have been waiting.

    The scheduler used to scan rows in ``sort_order`` and launch the first
    due task.  With a single worker, a one-minute realtime task at the top of
    the list could therefore starve a 15/30-minute minute-data task forever.
    Due tasks are now ordered by overdue seconds; the oldest due task gets the
    worker slot first, while not-yet-due tasks remain at the end.
    """
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

    now = datetime.now()
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
        timeout_minutes = _task_timeout_minutes(data)
        age_minutes = int((now - started_at).total_seconds() / 60)
        task_id = int(data["id"])
        # A service restart drops the in-memory process registry.  Do not keep
        # a database row in ``running`` until the normal long-task timeout if
        # its run started before this scheduler instance; the old child is no
        # longer owned by this process and would otherwise occupy a slot.
        interrupted_by_restart = started_at < _scheduler_started_at and task_id not in _running_procs
        if interrupted_by_restart:
            task_name = data.get("task_name") or task_id
            logger.warning(
                "服务重启后恢复未完成任务: %s (id=%s, started=%s)",
                task_name,
                task_id,
                started_at,
            )
            update_scheduler_task(
                engine,
                task_id,
                {
                    "last_run_status": "failed",
                    "last_run_output": "调度服务重启，上一轮任务已中断并释放调度槽",
                    "last_run_duration": max(0, age_minutes * 60),
                },
            )
            with _running_lock:
                _running_task_ids.discard(task_id)
                _fast_lane_running_task_ids.discard(task_id)
            cleaned += 1
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
        proc = _running_procs.pop(task_id, None)
        _terminate_process(proc)
        update_scheduler_task(
            engine,
            task_id,
            {
                "last_run_status": "timeout",
                "last_run_output": f"任务超时自动清理(>{timeout_minutes}分钟)",
                "last_run_duration": age_minutes * 60,
            },
        )
        with _running_lock:
            _running_task_ids.discard(task_id)
            _fast_lane_running_task_ids.discard(task_id)
        cleaned += 1
    return cleaned


def _should_skip_non_trading_day(row: dict, engine, now: datetime | None = None) -> bool:
    now = now or datetime.now()
    if _is_trade_day(engine, now.date()):
        return False

    task_type = str(row.get("task_type") or "").strip()
    script_path = str(row.get("script_path") or "").replace("\\", "/").strip()
    script_args = str(row.get("script_args") or "").strip()

    if task_type in NON_TRADING_DAY_SKIP_TYPES:
        return True
    if script_path in NON_TRADING_DAY_SKIP_PATHS and (
        script_args.startswith("sm_")
        or script_args.startswith("st_a_list")
        or "stock_" in script_args
        or "index_" in script_args
        or "concept_" in script_args
        or "quality" in task_type
    ):
        return True
    return False


def _mark_non_trading_day_skip(row: dict, engine, now: datetime) -> None:
    output = f"Skipped automatically: {now.date().isoformat()} is not a trading day."
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


def _get_task_semaphore() -> threading.Semaphore:
    global _task_semaphore
    if _task_semaphore is None:
        limit = int(get_scheduler_runtime_config()["max_concurrent_tasks"])
        _task_semaphore = threading.Semaphore(limit)
    return _task_semaphore


def _uses_fast_lane(row: dict) -> bool:
    return str(row.get("task_type") or "").strip() in FAST_LANE_TASK_TYPES


def _get_fast_lane_semaphore() -> threading.Semaphore:
    global _fast_lane_semaphore
    if _fast_lane_semaphore is None:
        _fast_lane_semaphore = threading.Semaphore(1)
    return _fast_lane_semaphore


def scheduler_runtime_info() -> dict[str, int | bool]:
    scheduler = get_scheduler_runtime_config()
    api_pool = get_api_mysql_pool_config()
    return {
        "embedded_scheduler_enabled": bool(scheduler["embedded_enabled"]),
        "embedded_scheduler_running": bool(_scheduler_thread and _scheduler_thread.is_alive()),
        "scheduler_max_concurrent_tasks": int(scheduler["max_concurrent_tasks"]),
        "scheduler_poll_seconds": int(scheduler["poll_seconds"]),
        "api_mysql_pool_size": int(api_pool["pool_size"]),
        "api_mysql_max_overflow": int(api_pool["max_overflow"]),
        "api_mysql_pool_recycle": int(api_pool["pool_recycle"]),
    }


def start_detached_python_job(
    *,
    cmd: list[str],
    root: Path,
    env: dict[str, str],
    log_name: str,
    nice: int = 10,
) -> dict[str, object]:
    """Start a long-running Python job outside the API request lifecycle."""
    data_dir = root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    safe_log_name = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in log_name).strip("_")
    safe_log_name = safe_log_name or "detached_job"
    out_path = data_dir / f"{safe_log_name}.out.log"
    err_path = data_dir / f"{safe_log_name}.err.log"
    stdout_handle = open(out_path, "a", encoding="utf-8")
    stderr_handle = open(err_path, "a", encoding="utf-8")
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


def _ensure_runtime_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE IF NOT EXISTS st_scheduler_runtime ("
                "instance_id VARCHAR(128) PRIMARY KEY, "
                "mode VARCHAR(32) NOT NULL, "
                "host_name VARCHAR(128) NULL, "
                "pid INT NULL, "
                "started_at DATETIME NULL, "
                "heartbeat_at DATETIME NOT NULL, "
                "poll_seconds INT NULL, "
                "max_concurrent_tasks INT NULL, "
                "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
                ")"
            )
        )


def _write_scheduler_heartbeat(engine, mode: str) -> None:
    runtime = get_scheduler_runtime_config()
    _ensure_runtime_table(engine)
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO st_scheduler_runtime "
                "(instance_id, mode, host_name, pid, started_at, heartbeat_at, poll_seconds, max_concurrent_tasks) "
                "VALUES (:instance_id, :mode, :host_name, :pid, :started_at, NOW(), :poll_seconds, :max_concurrent_tasks) "
                "ON DUPLICATE KEY UPDATE mode=VALUES(mode), host_name=VALUES(host_name), pid=VALUES(pid), "
                "started_at=VALUES(started_at), heartbeat_at=NOW(), poll_seconds=VALUES(poll_seconds), "
                "max_concurrent_tasks=VALUES(max_concurrent_tasks)"
            ),
            {
                "instance_id": _scheduler_instance_id,
                "mode": mode,
                "host_name": gethostname(),
                "pid": os.getpid(),
                "started_at": _scheduler_started_at,
                "poll_seconds": int(runtime["poll_seconds"]),
                "max_concurrent_tasks": int(runtime["max_concurrent_tasks"]),
            },
        )


def read_scheduler_heartbeat() -> dict[str, object] | None:
    try:
        engine = get_engine()
        _ensure_runtime_table(engine)
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT instance_id, mode, host_name, pid, started_at, heartbeat_at, "
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


def _run_task(row: dict, root: Path, engine) -> None:
    """执行单个定时任务"""
    task_id = row["id"]
    task_name = row["task_name"]
    script_path = row["script_path"] or ""
    today = datetime.now().strftime("%Y-%m-%d")

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
        return

    args = _build_task_args(row, script_path, today)

    cmd = [sys.executable, str(script)] + args

    child_env = build_child_env(root, engine=engine)

    update_scheduler_task(
        engine,
        int(task_id),
        {"last_run_status": "running"},
        now_columns={"last_run_at", "last_triggered_at"},
    )

    start_t = datetime.now()
    try:
        timeout_seconds = max(60, _task_timeout_minutes(row) * 60)
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
        _running_procs[task_id] = proc
        try:
            stdout, stderr = proc.communicate(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            _terminate_process(proc)
            stdout, stderr = proc.communicate()
            _running_procs.pop(task_id, None)
            duration = int((datetime.now() - start_t).total_seconds())
            status = "timeout"
            output = (
                f"任务执行超过 {_task_timeout_minutes(row)} 分钟，已自动终止。\n"
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
            return
        _running_procs.pop(task_id, None)

        duration = int((datetime.now() - start_t).total_seconds())
        status = "success" if proc.returncode == 0 else "failed"
        machine_output = (stdout or "") + "\n" + (stderr or "")
        output = (stdout or "")[-3000:] + "\n---STDERR---\n" + (stderr or "")[-2000:]
        # Preserve the Level-1 validator's explicit BLOCK state even though
        # its CLI exits non-zero.  BLOCK is not an execution failure and must
        # not be retried every fifteen minutes.
        status = scheduler_output_status(row, machine_output) or status
        if status == "success" and not is_market_closed_skip_output(output):
            validation = validate_scheduler_task_result(row, engine=engine, started_at=start_t)
            if validation.checked:
                marker = "DATA_VALIDATION_OK" if validation.ok else "DATA_VALIDATION_FAILED"
                output = output + f"\n{marker}: {validation.message}"
                if not validation.ok:
                    status = "failed"
    except Exception as exc:
        _running_procs.pop(task_id, None)
        status = "failed"
        duration = 0
        output = str(exc)

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
    semaphore = _get_fast_lane_semaphore() if _uses_fast_lane(row) else _get_task_semaphore()
    with semaphore:
        try:
            _run_task(row, root, engine)
        finally:
            with _running_lock:
                task_id = int(row["id"])
                _running_task_ids.discard(task_id)
                _fast_lane_running_task_ids.discard(task_id)
                _running_skip_logged_at.pop(task_id, None)


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

    try:
        claimed = _claim_task_run(row, engine)
    except Exception:
        with _running_lock:
            _running_task_ids.discard(task_id)
            _fast_lane_running_task_ids.discard(task_id)
        raise
    if not claimed:
        with _running_lock:
            _running_task_ids.discard(task_id)
            _fast_lane_running_task_ids.discard(task_id)
        return {
            "accepted": False,
            "status": "already_running",
            "task_id": task_id,
            "task_name": task_name,
        }

    worker = threading.Thread(
        target=_run_task_async,
        args=(dict(row), root, engine),
        daemon=True,
        name=f"scheduler-manual-task-{task_id}",
    )
    try:
        worker.start()
    except Exception:
        with _running_lock:
            _running_task_ids.discard(task_id)
            _fast_lane_running_task_ids.discard(task_id)
        update_scheduler_task(
            engine,
            task_id,
            {
                "last_run_status": "failed",
                "last_run_output": "manual task thread failed to start",
                "last_run_duration": 0,
            },
        )
        raise
    return {
        "accepted": True,
        "status": "running",
        "task_id": task_id,
        "task_name": task_name,
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
    _catchup_on_startup()
    startup_time = datetime.now()
    poll_seconds = int(get_scheduler_runtime_config()["poll_seconds"])
    while not (stop_event and stop_event.is_set()):
        try:
            engine = get_engine()
            try:
                _write_scheduler_heartbeat(engine, mode)
            except Exception as exc:
                logger.warning("写入调度心跳异常: %s", exc)

            try:
                _cleanup_stale_running_tasks(engine)
            except Exception as exc:
                logger.warning("僵尸检测异常: %s", exc)

            with engine.connect() as conn:
                result = conn.execute(
                    text("SELECT id, task_name, task_type, script_path, script_args, cron_time, interval_minutes, "
                         "enabled, date_param, last_run_at, last_triggered_at, last_run_status "
                         "FROM st_scheduled_tasks WHERE enabled = 1 ORDER BY sort_order")
                )
                rows = [dict(zip(result.keys(), row)) for row in result.fetchall()]

            now = datetime.now()
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

                if _should_skip_task_for_host(row):
                    skip_key = (int(task_id), now.strftime("%Y-%m-%d"))
                    if skip_key not in _delegated_skip_logged_for:
                        logger.info(
                            "Skip task owned by the other scheduler host: %s (type=%s)",
                            task_name,
                            row.get("task_type"),
                        )
                        _delegated_skip_logged_for.add(skip_key)
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
                        if elapsed < interval_minutes:
                            continue
                else:
                    if not _cron_due(row, now=now):
                        continue
                    if time_str != cron_time:
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
                        if status in {"failed", "timeout", "stopped"}:
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

                if _should_skip_non_trading_day(row, engine, now):
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
                    uses_fast_lane = _uses_fast_lane(row)
                    general_running_count = len(_running_task_ids - _fast_lane_running_task_ids)
                    if uses_fast_lane and len(_fast_lane_running_task_ids) >= 1:
                        logger.debug("Scheduler fast lane full; defer task %s", task_name)
                        continue
                    if not uses_fast_lane and general_running_count >= max_pending_tasks:
                        logger.debug(
                            "Scheduler capacity full (%s); defer task %s",
                            max_pending_tasks,
                            task_name,
                        )
                        continue
                    _running_task_ids.add(int(task_id))
                    if uses_fast_lane:
                        _fast_lane_running_task_ids.add(int(task_id))

                try:
                    claimed = _claim_task_run(row, engine)
                except Exception as exc:
                    logger.warning("任务 %s 抢占失败，跳过本次触发: %s", task_name, exc)
                    with _running_lock:
                        _running_task_ids.discard(int(task_id))
                        _fast_lane_running_task_ids.discard(int(task_id))
                    continue
                if not claimed:
                    logger.warning("任务 %s 已被其他调度实例抢占，跳过本次触发", task_name)
                    with _running_lock:
                        _running_task_ids.discard(int(task_id))
                        _fast_lane_running_task_ids.discard(int(task_id))
                    continue

                logger.info("执行定时任务: %s (cron=%s, now=%s)", task_name, cron_time, time_str)
                threading.Thread(
                    target=_run_task_async,
                    args=(row, root, engine),
                    daemon=True,
                    name=f"scheduler-task-{task_id}",
                ).start()

        except Exception as exc:
            logger.error("调度线程异常: %s", exc)

        if stop_event:
            if stop_event.wait(poll_seconds):
                break
        else:
            time.sleep(poll_seconds)


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
