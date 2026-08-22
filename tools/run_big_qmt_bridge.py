from __future__ import annotations

"""Consume standard QMT built-in-Python snapshots into ProBigA.

The producer runs inside the desktop QMT process.  This consumer writes the
watchlist contract, validates snapshot coverage and publishes rows to
``sm_stock_current`` without importing or connecting to miniQMT/xtquant.
"""

import argparse
import hashlib
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import bindparam, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.env_config import load_project_env

load_project_env()

from integrations.bigqmt.spool import (
    PROVIDER_ID,
    bridge_paths,
    install_qmt_strategy,
    merge_snapshot_frames,
    read_json,
    read_snapshot,
    resolve_big_qmt_home,
    snapshot_frame,
    write_watchlist,
)
from integrations.bigqmt.bridge import (
    level1_snapshot,
    request_level1_reconnect,
)
from server.trading_v2.quotes import persist_quote_events
from integrations.qmt.backend import to_qmt_symbol
from server.common.batch_db import create_batch_engine, write_frame
from server.common.mysql_lock import mysql_named_lock
from server.common.scheduler_tasks import (
    claim_scheduler_task_run,
    update_scheduler_task,
)
from tools.remote_support import remote_host


_last_remote_portfolio_codes: list[str] = []
_trade_day_cache: dict[str, bool] = {}
_maintenance_process_lock = threading.Lock()
_maintenance_processes: set[subprocess.Popen] = set()
MEMBERSHIP_SNAPSHOT_TASK_TYPE = "qmt_membership_snapshot"
ETF_FORWARD_TASK_TYPE = "etf_forward_daily"
ETF_FORWARD_RETRY_MINUTES = 10

ACTIVE_UNIVERSE_SQL = """
SELECT DISTINCT code.stock_code, code.short_name
  FROM si_all_code AS code
  JOIN sm_stock_kline AS kline
    ON kline.stock_code = code.stock_code
   AND kline.k_type = 1
   AND kline.adjust_type = 0
 WHERE kline.trade_date = (
       SELECT MAX(latest.trade_date)
         FROM sm_stock_kline AS latest
        WHERE latest.k_type = 1
          AND latest.adjust_type = 0
 )
 ORDER BY code.stock_code
"""


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _is_live_market_window(
    now: datetime,
    *,
    is_trade_day: bool,
) -> bool:
    """Require fresh snapshots only while the exchange session can update."""
    if not is_trade_day:
        return False
    hhmm = now.hour * 100 + now.minute
    return 915 <= hhmm <= 1510


def _snapshot_freshness_required(
    engine,
    *,
    now: datetime | None = None,
) -> bool:
    current = now or datetime.now()
    day_text = current.date().isoformat()
    is_trade_day = _trade_day_cache.get(day_text)
    if is_trade_day is None:
        with engine.connect() as conn:
            count = conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                      FROM si_trade_calendar
                     WHERE trade_date = :trade_date
                       AND trade_status = 1
                    """
                ),
                {"trade_date": current.date()},
            ).scalar()
        is_trade_day = bool(int(count or 0))
        _trade_day_cache.clear()
        _trade_day_cache[day_text] = is_trade_day
    return _is_live_market_window(
        current,
        is_trade_day=is_trade_day,
    )


def _membership_snapshot_task(engine) -> dict[str, Any] | None:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT id, task_name, task_type, cron_time, enabled,
                       last_run_at, last_triggered_at, last_run_status,
                       last_run_duration, last_run_output
                  FROM st_scheduled_tasks
                 WHERE task_type = :task_type
                 LIMIT 1
                """
            ),
            {"task_type": MEMBERSHIP_SNAPSHOT_TASK_TYPE},
        ).mappings().first()
    return dict(row) if row else None


def _etf_forward_task(engine) -> dict[str, Any] | None:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT id, task_name, task_type, cron_time, enabled,
                       last_run_at, last_triggered_at, last_run_status,
                       last_run_duration, last_run_output
                  FROM st_scheduled_tasks
                 WHERE task_type = :task_type
                 LIMIT 1
                """
            ),
            {"task_type": ETF_FORWARD_TASK_TYPE},
        ).mappings().first()
    return dict(row) if row else None


def _coerce_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value)[:19])
    except ValueError:
        return None


def _process_identity(pid: int) -> tuple[bool, str]:
    """Return whether a process exists and a PID-reuse-resistant start token."""
    if int(pid) <= 0:
        return False, ""
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        open_process = kernel32.OpenProcess
        open_process.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        open_process.restype = wintypes.HANDLE
        get_process_times = kernel32.GetProcessTimes
        get_process_times.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(wintypes.FILETIME),
            ctypes.POINTER(wintypes.FILETIME),
            ctypes.POINTER(wintypes.FILETIME),
            ctypes.POINTER(wintypes.FILETIME),
        ]
        get_process_times.restype = wintypes.BOOL
        close_handle = kernel32.CloseHandle
        close_handle.argtypes = [wintypes.HANDLE]
        close_handle.restype = wintypes.BOOL

        handle = open_process(0x1000, False, int(pid))
        if not handle:
            # ERROR_INVALID_PARAMETER means the PID does not exist.  Access
            # denied is treated as alive/unknown so recovery stays fail-closed.
            return ctypes.get_last_error() not in {87, 1168}, ""
        try:
            created = wintypes.FILETIME()
            exited = wintypes.FILETIME()
            kernel = wintypes.FILETIME()
            user = wintypes.FILETIME()
            if not get_process_times(
                handle,
                ctypes.byref(created),
                ctypes.byref(exited),
                ctypes.byref(kernel),
                ctypes.byref(user),
            ):
                return True, ""
            token = (int(created.dwHighDateTime) << 32) | int(
                created.dwLowDateTime
            )
            return True, str(token)
        finally:
            close_handle(handle)

    proc_path = Path(f"/proc/{int(pid)}")
    try:
        return True, str(proc_path.stat().st_ctime_ns)
    except FileNotFoundError:
        return False, ""
    except OSError:
        return True, ""


def _bridge_task_owner() -> dict[str, Any]:
    alive, start_token = _process_identity(os.getpid())
    if not alive:
        start_token = ""
    return {
        "executor": "windows_big_qmt_bridge",
        "lease_version": 1,
        "host": socket.gethostname().casefold(),
        "pid": os.getpid(),
        "process_start_token": start_token,
        "claimed_at": datetime.now().isoformat(timespec="seconds"),
    }


def _parse_bridge_task_owner(value: Any) -> dict[str, Any] | None:
    try:
        payload = json.loads(str(value or ""))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    try:
        lease_version = int(payload.get("lease_version") or 0)
        owner_pid = int(payload.get("pid") or 0)
    except (TypeError, ValueError, OverflowError):
        return None
    if (
        payload.get("executor") != "windows_big_qmt_bridge"
        or lease_version != 1
        or owner_pid <= 0
    ):
        return None
    return payload


def _bridge_task_owner_is_alive(owner: dict[str, Any]) -> bool:
    owner_host = str(owner.get("host") or "").casefold()
    if owner_host and owner_host != socket.gethostname().casefold():
        # A different QMT host cannot be inspected locally.  Never steal its
        # lease; this remains fail-closed even if the topology changes later.
        return True
    alive, actual_start = _process_identity(int(owner.get("pid") or 0))
    if not alive:
        return False
    expected_start = str(owner.get("process_start_token") or "")
    return not expected_start or not actual_start or expected_start == actual_start


def _bridge_task_lease_seconds(
    task: dict[str, Any],
    *,
    task_type: str,
) -> float:
    if task_type == ETF_FORWARD_TASK_TYPE:
        env_name = "BIG_QMT_ETF_FORWARD_LEASE_SECONDS"
        default_seconds = 1800.0
        minimum_seconds = 1500.0
    else:
        env_name = "BIG_QMT_MEMBERSHIP_LEASE_SECONDS"
        default_seconds = 14400.0
        minimum_seconds = 1800.0
    try:
        configured = float(os.environ.get(env_name, str(default_seconds)))
    except (TypeError, ValueError):
        configured = default_seconds
    try:
        previous_duration = max(0.0, float(task.get("last_run_duration") or 0))
    except (TypeError, ValueError):
        previous_duration = 0.0
    return max(minimum_seconds, configured, previous_duration * 2.0)


def _recover_abandoned_bridge_task(
    engine,
    task: dict[str, Any],
    *,
    task_type: str,
    now: datetime | None = None,
) -> bool:
    """CAS-reset a dead owner or an expired legacy maintenance lease."""
    if str(task.get("last_run_status") or "").lower() != "running":
        return False

    owner = _parse_bridge_task_owner(task.get("last_run_output"))
    reason = "owner process exited"
    if owner is not None:
        if _bridge_task_owner_is_alive(owner):
            return False
    else:
        reference = _coerce_datetime(task.get("last_run_at")) or _coerce_datetime(
            task.get("last_triggered_at")
        )
        lease_seconds = _bridge_task_lease_seconds(task, task_type=task_type)
        if reference is not None:
            age_seconds = max(
                0.0,
                ((now or datetime.now()) - reference).total_seconds(),
            )
            if age_seconds < lease_seconds:
                return False
        reason = "legacy maintenance lease expired"

    with engine.begin() as conn:
        result = conn.execute(
            text(
                """
                UPDATE st_scheduled_tasks
                   SET last_run_status = 'failed',
                       last_run_output = :output,
                       last_run_duration = GREATEST(
                           0,
                           TIMESTAMPDIFF(
                               SECOND,
                               COALESCE(last_run_at, last_triggered_at),
                               NOW()
                           )
                       ),
                       updated_at = NOW()
                 WHERE id = :id
                   AND last_run_status = 'running'
                   AND last_run_at <=> :last_run_at
                   AND last_triggered_at <=> :last_triggered_at
                """
            ),
            {
                "id": int(task["id"]),
                "last_run_at": task.get("last_run_at"),
                "last_triggered_at": task.get("last_triggered_at"),
                "output": (
                    "executor=windows_big_qmt_bridge\n"
                    f"recovered_at={datetime.now().isoformat(timespec='seconds')}\n"
                    f"reason={reason}"
                ),
            },
        )
    return int(getattr(result, "rowcount", 0) or 0) > 0


def _claim_bridge_task_run(
    engine,
    task: dict[str, Any],
    *,
    task_type: str,
) -> bool:
    if str(task.get("last_run_status") or "").lower() == "running":
        if not _recover_abandoned_bridge_task(
            engine,
            task,
            task_type=task_type,
        ):
            return False
    task_id = int(task["id"])
    if not claim_scheduler_task_run(engine, task_id):
        return False
    update_scheduler_task(
        engine,
        task_id,
        {
            "last_run_output": json.dumps(
                _bridge_task_owner(),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        },
    )
    return True


def _calendar_trade_day(engine, current: datetime) -> bool:
    with engine.connect() as conn:
        count = conn.execute(
            text(
                """
                SELECT COUNT(*)
                  FROM si_trade_calendar
                 WHERE trade_date = :trade_date
                   AND trade_status = 1
                """
            ),
            {"trade_date": current.date()},
        ).scalar()
    return bool(int(count or 0))


def _terminate_maintenance_process_tree(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    if os.name == "nt":
        try:
            subprocess.run(
                ["taskkill.exe", "/PID", str(process.pid), "/T", "/F"],
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            pass
    else:
        try:
            os.killpg(os.getpgid(process.pid), signal.SIGKILL)
        except (OSError, ProcessLookupError):
            pass
    if process.poll() is None:
        try:
            process.kill()
        except OSError:
            pass


def _terminate_active_maintenance_processes() -> None:
    with _maintenance_process_lock:
        processes = list(_maintenance_processes)
    for process in processes:
        _terminate_maintenance_process_tree(process)


def _run_etf_forward_command() -> dict[str, Any]:
    popen_kwargs: dict[str, Any] = {
        "cwd": ROOT,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
        "text": True,
        "encoding": "utf-8",
        "errors": "replace",
    }
    if os.name != "nt":
        popen_kwargs["start_new_session"] = True
    daily = subprocess.Popen(
        [
            sys.executable,
            str(ROOT / "tools" / "run_etf_forward_daily.py"),
            "--execute",
        ],
        **popen_kwargs,
    )
    with _maintenance_process_lock:
        _maintenance_processes.add(daily)
    try:
        stdout, stderr = daily.communicate(timeout=1200)
    except subprocess.TimeoutExpired:
        _terminate_maintenance_process_tree(daily)
        raise
    finally:
        with _maintenance_process_lock:
            _maintenance_processes.discard(daily)
    return {
        "returncode": int(daily.returncode),
        "daily_stdout_tail": stdout.strip()[-3000:],
        "daily_stderr_tail": stderr.strip()[-1500:],
        "delivery_mode": "CONFIGURED_DATABASE_DIRECT",
        "promotion_stdout_tail": "",
        "promotion_stderr_tail": "",
    }


def maybe_run_etf_forward_daily(
    engine,
    *,
    now: datetime | None = None,
    runner=None,
) -> dict[str, Any]:
    """Run the delegated post-close ETF task on the QMT-owning Windows host."""
    current = now or datetime.now()
    task = _etf_forward_task(engine)
    if not task or not bool(task.get("enabled")):
        return {"status": "disabled_or_missing"}

    cron_text = str(task.get("cron_time") or "15:20")
    try:
        cron_hour, cron_minute = (
            int(part) for part in cron_text.split(":", 1)
        )
    except (TypeError, ValueError):
        cron_hour, cron_minute = 15, 20
    if (current.hour, current.minute) < (cron_hour, cron_minute):
        return {"status": "not_due"}
    if not _calendar_trade_day(engine, current):
        return {"status": "not_trade_day"}

    last_triggered = _coerce_datetime(task.get("last_triggered_at"))
    last_run = _coerce_datetime(task.get("last_run_at")) or last_triggered
    last_status = str(task.get("last_run_status") or "").lower()
    if last_triggered and last_triggered.date() == current.date():
        if last_status == "success":
            return {"status": "current"}
        if (
            last_status in {"failed", "timeout", "stopped"}
            and last_run
            and (current - last_run).total_seconds()
            < ETF_FORWARD_RETRY_MINUTES * 60
        ):
            return {"status": "retry_wait"}

    task_id = int(task["id"])
    if not _claim_bridge_task_run(
        engine,
        task,
        task_type=ETF_FORWARD_TASK_TYPE,
    ):
        return {"status": "already_running"}

    started = time.monotonic()
    execute = runner or _run_etf_forward_command
    try:
        command_result = execute()
        duration = int(time.monotonic() - started)
        succeeded = int(command_result.get("returncode") or 0) == 0
        status = "success" if succeeded else "failed"
        output = json.dumps(
            {
                "executor": "windows_big_qmt_bridge",
                "trade_date": current.date().isoformat(),
                **command_result,
            },
            ensure_ascii=False,
            default=str,
        )
        update_scheduler_task(
            engine,
            task_id,
            {
                "last_run_status": status,
                "last_run_output": output[-5000:],
                "last_run_duration": duration,
            },
        )
        return {
            "status": status,
            "trade_date": current.date().isoformat(),
            "duration_seconds": duration,
            **command_result,
        }
    except Exception as exc:
        duration = int(time.monotonic() - started)
        update_scheduler_task(
            engine,
            task_id,
            {
                "last_run_status": "failed",
                "last_run_output": (
                    "executor=windows_big_qmt_bridge\n"
                    f"trade_date={current.date().isoformat()}\n"
                    f"error={exc}"
                )[-5000:],
                "last_run_duration": duration,
            },
        )
        return {
            "status": "error",
            "trade_date": current.date().isoformat(),
            "duration_seconds": duration,
            "error": str(exc),
        }


def _membership_snapshot_exists(engine, snapshot_date) -> bool:
    if not _table_exists(engine, "qmt_membership_snapshot_run"):
        return False
    with engine.connect() as conn:
        count = conn.execute(
            text(
                """
                SELECT COUNT(*)
                  FROM qmt_membership_snapshot_run
                 WHERE snapshot_date = :snapshot_date
                   AND source = :source
                   AND quality_status = 'QMT_VALIDATED'
                """
            ),
            {"snapshot_date": snapshot_date, "source": PROVIDER_ID},
        ).scalar()
    return bool(int(count or 0))


def _run_membership_snapshot(engine, snapshot_date) -> dict[str, Any]:
    # Lazy import keeps the quote bridge's market-session startup light and
    # makes the QMT reference collector active only after the close.
    from tools.sync_bigqmt_reference import fetch_and_validate, publish

    frames, counts = fetch_and_validate(
        engine,
        force_reference_refresh=True,
    )
    snapshot = publish(
        engine,
        frames,
        snapshot_date=snapshot_date,
    )
    return {"counts": counts, "snapshot": snapshot}


def maybe_sync_membership_snapshot(
    engine,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Capture today's immutable membership snapshot on the QMT-owning host."""
    current = now or datetime.now()
    task = _membership_snapshot_task(engine)
    if not task or not bool(task.get("enabled")):
        return {"status": "disabled_or_missing"}

    cron_text = str(task.get("cron_time") or "15:12")
    try:
        cron_hour, cron_minute = (int(part) for part in cron_text.split(":", 1))
    except (TypeError, ValueError):
        cron_hour, cron_minute = 15, 12
    if (current.hour, current.minute) < (cron_hour, cron_minute):
        return {"status": "not_due"}

    with engine.connect() as conn:
        is_trade_day = conn.execute(
            text(
                """
                SELECT COUNT(*)
                  FROM si_trade_calendar
                 WHERE trade_date = :trade_date
                   AND trade_status = 1
                """
            ),
            {"trade_date": current.date()},
        ).scalar()
    if not bool(int(is_trade_day or 0)):
        return {"status": "not_trade_day"}

    from tools.sync_bigqmt_reference import resolve_snapshot_date

    snapshot_date = resolve_snapshot_date(engine)
    if snapshot_date != current.date():
        return {
            "status": "not_completed",
            "snapshot_date": snapshot_date.isoformat(),
        }
    if _membership_snapshot_exists(engine, snapshot_date):
        return {
            "status": "current",
            "snapshot_date": snapshot_date.isoformat(),
        }

    task_id = int(task["id"])
    if not _claim_bridge_task_run(
        engine,
        task,
        task_type=MEMBERSHIP_SNAPSHOT_TASK_TYPE,
    ):
        return {
            "status": "already_running",
            "snapshot_date": snapshot_date.isoformat(),
        }

    started = time.monotonic()
    try:
        result = _run_membership_snapshot(engine, snapshot_date)
        duration = int(time.monotonic() - started)
        output = json.dumps(
            {
                "executor": "windows_big_qmt_bridge",
                "snapshot_date": snapshot_date.isoformat(),
                **result,
            },
            ensure_ascii=False,
            default=str,
        )
        update_scheduler_task(
            engine,
            task_id,
            {
                "last_run_status": "success",
                "last_run_output": output[-5000:],
                "last_run_duration": duration,
            },
        )
        return {
            "status": "success",
            "snapshot_date": snapshot_date.isoformat(),
            "duration_seconds": duration,
            **result,
        }
    except Exception as exc:
        duration = int(time.monotonic() - started)
        update_scheduler_task(
            engine,
            task_id,
            {
                "last_run_status": "failed",
                "last_run_output": (
                    "executor=windows_big_qmt_bridge\n"
                    f"snapshot_date={snapshot_date.isoformat()}\n"
                    f"error={exc}"
                )[-5000:],
                "last_run_duration": duration,
            },
        )
        return {
            "status": "error",
            "snapshot_date": snapshot_date.isoformat(),
            "duration_seconds": duration,
            "error": str(exc),
        }


def _read_universe(engine) -> tuple[list[str], dict[str, str]]:
    with engine.connect() as conn:
        rows = conn.execute(text(ACTIVE_UNIVERSE_SQL)).fetchall()
    codes: list[str] = []
    names: dict[str, str] = {}
    for raw_code, raw_name in rows:
        code = str(raw_code or "").strip().zfill(6)
        if not to_qmt_symbol(code):
            continue
        codes.append(code)
        names[code] = str(raw_name or "").strip()
    return list(dict.fromkeys(codes)), names


def _read_tracked_codes(engine, limit: int) -> list[str]:
    queries: list[tuple[str, dict[str, Any]]] = [
        (
            "SELECT DISTINCT stock_code FROM st_user_portfolio "
            "WHERE stock_code IS NOT NULL AND stock_code <> '' ORDER BY stock_code",
            {},
        ),
        (
            "SELECT DISTINCT stock_code FROM st_sim_position "
            "WHERE status = 'holding' AND stock_code IS NOT NULL AND stock_code <> '' "
            "ORDER BY stock_code",
            {},
        ),
        (
            "SELECT stock_code FROM st_recommended_stocks "
            "WHERE pick_date = (SELECT MAX(pick_date) FROM st_recommended_stocks) "
            "AND stock_code IS NOT NULL AND stock_code <> '' LIMIT :limit",
            {"limit": max(20, int(limit))},
        ),
    ]
    result: list[str] = []
    for sql, params in queries:
        try:
            with engine.connect() as conn:
                rows = conn.execute(text(sql), params).fetchall()
        except Exception:
            continue
        for row in rows:
            code = str(row[0] or "").strip().zfill(6)
            if to_qmt_symbol(code):
                result.append(code)
    return list(dict.fromkeys(result))[: max(1, min(280, int(limit)))]


def _production_portfolio_url() -> str:
    explicit = os.environ.get("BIG_QMT_PORTFOLIO_CODES_URL", "").strip()
    if explicit:
        return explicit
    base = (
        os.environ.get("PROBIGA_PRODUCTION_BASE_URL", "").strip()
        or os.environ.get("PROBIGA_BASE_URL", "").strip()
    )
    if not base:
        host = remote_host()
        base = host if "://" in host else f"http://{host}"
    return f"{base.rstrip('/')}/api/portfolio/codes"


def _read_remote_portfolio_codes(limit: int) -> list[str]:
    """Keep production watchlist codes in the local QMT push subscription."""
    global _last_remote_portfolio_codes
    if not _env_bool("BIG_QMT_REMOTE_PORTFOLIO_ENABLED", True):
        return []
    request = urllib.request.Request(
        _production_portfolio_url(),
        headers={"Accept": "application/json", "User-Agent": "ProBigA-BigQMT-Bridge/1"},
    )
    timeout = max(1.0, float(os.environ.get("BIG_QMT_REMOTE_PORTFOLIO_TIMEOUT_SECONDS", "5")))
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
        raw_rows = payload.get("data", []) if isinstance(payload, dict) else []
        codes: list[str] = []
        for item in raw_rows:
            raw_code = item.get("stock_code") if isinstance(item, dict) else item
            code = str(raw_code or "").strip().zfill(6)
            if to_qmt_symbol(code):
                codes.append(code)
        _last_remote_portfolio_codes = list(dict.fromkeys(codes))[: max(1, min(280, int(limit)))]
    except Exception:
        # A temporary production/network failure must not drop previously
        # subscribed production watchlist codes from the long connection.
        return list(_last_remote_portfolio_codes)
    return list(_last_remote_portfolio_codes)


def refresh_watchlist(engine, *, qmt_home: Path, tracked_limit: int) -> dict[str, Any]:
    universe, names = _read_universe(engine)
    remote_portfolio = _read_remote_portfolio_codes(tracked_limit)
    local_tracked = _read_tracked_codes(engine, tracked_limit)
    tracked = list(dict.fromkeys([*remote_portfolio, *local_tracked]))[
        : max(1, min(280, int(tracked_limit)))
    ]
    path = write_watchlist(
        all_codes=universe,
        tracked_codes=tracked,
        qmt_home=qmt_home,
        full_refresh_seconds=int(os.environ.get("BIG_QMT_FULL_REFRESH_SECONDS", "30")),
        tracked_flush_seconds=float(os.environ.get("BIG_QMT_TRACKED_FLUSH_SECONDS", "1")),
        full_batch_size=int(os.environ.get("BIG_QMT_FULL_BATCH_SIZE", "800")),
    )
    return {
        "path": str(path),
        "universe": universe,
        "tracked": tracked,
        "remote_portfolio": remote_portfolio,
        "short_name_map": names,
    }


def _table_columns(engine, table_name: str) -> list[str]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name "
                "ORDER BY ORDINAL_POSITION"
            ),
            {"table_name": table_name},
        ).fetchall()
    return [str(row[0]) for row in rows]


def _table_exists(engine, table_name: str) -> bool:
    return bool(_table_columns(engine, table_name))


def _database_frame(engine, frame: pd.DataFrame) -> pd.DataFrame:
    columns = [column for column in _table_columns(engine, "sm_stock_current") if column != "id"]
    if not columns:
        raise RuntimeError("sm_stock_current does not exist")
    selected = [column for column in columns if column in frame.columns]
    required = {"stock_code", "price", "snapshot_at", "etl_sync_at"}
    if not required.issubset(selected):
        raise RuntimeError(f"sm_stock_current is missing required bridge columns: {sorted(required - set(selected))}")
    out = frame[selected].copy()
    return out.astype(object).where(pd.notna(out), None)


def _replace_full_snapshot(engine, frame: pd.DataFrame) -> int:
    if frame.empty:
        raise ValueError("Big QMT full snapshot is empty")
    frame = _database_frame(engine, frame)
    # Keep the existing table object and schema stable.  RENAME TABLE needs a
    # global metadata lock and can queue behind ordinary dashboard SELECTs,
    # which made the self-selected-stock page time out.  A short InnoDB
    # transaction gives readers MVCC consistency without any DDL lock.
    with mysql_named_lock(engine, "probiga:stock_current", timeout_seconds=1):
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM `sm_stock_current`"))
            write_frame(
                frame,
                "sm_stock_current",
                conn,
                if_exists="append",
                index=False,
                chunksize=1000,
                method="multi",
            )
    return int(len(frame))


def _replace_tracked_subset(engine, frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    frame = _database_frame(engine, frame)
    codes = frame["stock_code"].astype(str).drop_duplicates().tolist()
    delete_sql = text(
        "DELETE FROM sm_stock_current WHERE stock_code IN :stock_codes"
    ).bindparams(bindparam("stock_codes", expanding=True))
    with mysql_named_lock(engine, "probiga:stock_current", timeout_seconds=1):
        with engine.begin() as conn:
            conn.execute(delete_sql, {"stock_codes": codes})
            write_frame(
                frame,
                "sm_stock_current",
                conn,
                if_exists="append",
                index=False,
                chunksize=500,
                method="multi",
            )
    return int(len(frame))


def _snapshot_token(payload: dict[str, Any]) -> str:
    return str(payload.get("generated_ts") or payload.get("generated_at") or payload.get("batch_id") or "")


def _read_snapshot_if_changed(
    kind: str,
    *,
    qmt_home: Path,
    max_age_seconds: float | None,
    tokens: dict[str, str],
) -> tuple[dict[str, Any], str]:
    path = bridge_paths(qmt_home)[kind]
    if not path.is_file():
        return {}, ""
    stat = path.stat()
    age = max(0.0, time.time() - stat.st_mtime)
    if max_age_seconds is not None and age > max_age_seconds:
        raise RuntimeError(f"Big QMT bridge file is stale: {path} age={age:.1f}s")
    file_token = f"{stat.st_mtime_ns}:{stat.st_size}"
    if tokens.get(f"{kind}_file") == file_token:
        return {}, file_token
    return read_snapshot(kind, qmt_home=qmt_home, max_age_seconds=max_age_seconds), file_token


def _replace_with_retry(
    temporary: Path,
    path: Path,
    *,
    retry_seconds: float = 2.0,
    retry_interval: float = 0.02,
) -> None:
    deadline = time.monotonic() + max(0.0, float(retry_seconds))
    while True:
        try:
            os.replace(temporary, path)
            return
        except OSError as exc:
            transient = isinstance(exc, PermissionError) or getattr(exc, "winerror", None) in {5, 32, 33}
            if not transient or time.monotonic() >= deadline:
                raise
            time.sleep(max(0.01, float(retry_interval)))


def _write_status(qmt_home: Path, payload: dict[str, Any]) -> None:
    payload = {
        **payload,
        "generated_ts": time.time(),
    }
    path = bridge_paths(qmt_home)["consumer_status"]
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, ensure_ascii=False, default=str, separators=(",", ":"))
        handle.flush()
        os.fsync(handle.fileno())
    _replace_with_retry(temporary, path)


def _timestamp_datetime(value: Any) -> datetime | None:
    try:
        return datetime.fromtimestamp(float(value)).replace(microsecond=0)
    except (TypeError, ValueError, OSError):
        return None


def _record_realtime_sync_receipt(
    engine,
    *,
    full_payload: dict[str, Any],
    full_file_token: str,
    heartbeat: dict[str, Any],
    expected_count: int,
    observed_count: int,
    coverage: float,
    published_at: datetime,
    capture_mode: str,
) -> dict[str, Any]:
    """Persist proof that the exact QMT file reached the current-quote table."""

    source_generated_at = _timestamp_datetime(
        full_payload.get("generated_ts")
    )
    heartbeat_at = _timestamp_datetime(heartbeat.get("updated_ts"))
    heartbeat_age = (
        max(0.0, (published_at - heartbeat_at).total_seconds())
        if heartbeat_at is not None
        else None
    )
    quality_status = (
        "PASS"
        if (
            source_generated_at is not None
            and heartbeat_at is not None
            and heartbeat_age is not None
            and heartbeat_age <= 30
            and str(heartbeat.get("status") or "").lower()
            in {"running", "busy"}
            and coverage >= float(
                os.environ.get("BIG_QMT_MIN_FULL_COVERAGE", "0.95")
            )
            and observed_count > 0
        )
        else "BLOCK"
    )
    source_snapshot_token = _snapshot_token(full_payload)
    receipt_payload = {
        "source_provider": PROVIDER_ID,
        "source_snapshot_token": source_snapshot_token,
        "source_full_file_token": full_file_token,
        "source_generated_at": source_generated_at,
        "heartbeat_at": heartbeat_at,
        "expected_count": expected_count,
        "observed_count": observed_count,
        "coverage": round(float(coverage), 8),
        "published_at": published_at,
        "capture_mode": capture_mode,
        "quality_status": quality_status,
    }
    receipt_id = hashlib.sha256(
        json.dumps(
            receipt_payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()[:32]
    evidence = {
        "strategy_status": heartbeat.get("status"),
        "heartbeat_age_seconds": heartbeat_age,
        "full_batch_id": full_payload.get("batch_id"),
        "full_quote_count": full_payload.get("quote_count"),
        "source_full_file_token": full_file_token,
    }
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS st_qmt_realtime_sync_receipt_v2 (
                    receipt_id VARCHAR(64) PRIMARY KEY,
                    source_provider VARCHAR(80) NOT NULL,
                    source_snapshot_token VARCHAR(128) NOT NULL,
                    source_full_file_token VARCHAR(160) NOT NULL,
                    source_generated_at DATETIME NOT NULL,
                    heartbeat_at DATETIME NOT NULL,
                    expected_count INT NOT NULL,
                    observed_count INT NOT NULL,
                    coverage DECIMAL(18,8) NOT NULL,
                    published_at DATETIME NOT NULL,
                    capture_mode VARCHAR(32) NOT NULL,
                    quality_status VARCHAR(16) NOT NULL,
                    evidence_json LONGTEXT NOT NULL,
                    created_at DATETIME NOT NULL,
                    UNIQUE KEY uk_qmt_realtime_source_snapshot
                        (source_provider, source_snapshot_token),
                    KEY idx_qmt_realtime_receipt_latest
                        (capture_mode, quality_status, published_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO st_qmt_realtime_sync_receipt_v2 (
                    receipt_id, source_provider, source_snapshot_token,
                    source_full_file_token, source_generated_at, heartbeat_at,
                    expected_count, observed_count, coverage, published_at,
                    capture_mode, quality_status, evidence_json, created_at
                )
                VALUES (
                    :receipt_id, :source_provider, :source_snapshot_token,
                    :source_full_file_token, :source_generated_at, :heartbeat_at,
                    :expected_count, :observed_count, :coverage, :published_at,
                    :capture_mode, :quality_status, :evidence_json, :created_at
                )
                ON DUPLICATE KEY UPDATE
                    source_full_file_token=VALUES(source_full_file_token),
                    heartbeat_at=VALUES(heartbeat_at),
                    expected_count=VALUES(expected_count),
                    observed_count=VALUES(observed_count),
                    coverage=VALUES(coverage),
                    published_at=VALUES(published_at),
                    capture_mode=VALUES(capture_mode),
                    quality_status=VALUES(quality_status),
                    evidence_json=VALUES(evidence_json)
                """
            ),
            {
                "receipt_id": receipt_id,
                **receipt_payload,
                "evidence_json": json.dumps(
                    evidence,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                ),
                "created_at": published_at,
            },
        )
    return {
        "receipt_id": receipt_id,
        "source_batch_id": str(full_payload.get("batch_id") or ""),
        "source_snapshot_token": source_snapshot_token,
        "source_full_file_token": full_file_token,
        "source_generated_at": (
            source_generated_at.isoformat(sep=" ")
            if source_generated_at is not None
            else None
        ),
        "heartbeat_at": (
            heartbeat_at.isoformat(sep=" ")
            if heartbeat_at is not None
            else None
        ),
        "expected_count": expected_count,
        "observed_count": observed_count,
        "coverage": round(float(coverage), 8),
        "published_at": published_at.isoformat(sep=" "),
        "capture_mode": capture_mode,
        "quality_status": quality_status,
    }


def ingest_once(
    engine,
    *,
    qmt_home: Path,
    universe: list[str],
    tracked: list[str],
    short_name_map: dict[str, str],
    last_tokens: dict[str, Any] | None = None,
) -> dict[str, Any]:
    tokens = last_tokens if last_tokens is not None else {}
    max_age = float(os.environ.get("BIG_QMT_SNAPSHOT_MAX_AGE_SECONDS", "75"))
    freshness_required = _snapshot_freshness_required(engine)
    effective_max_age = max_age if freshness_required else None
    result: dict[str, Any] = {
        "status": "waiting_for_qmt_strategy",
        "source": PROVIDER_ID,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "full_rows": 0,
        "tracked_rows": 0,
        "market_session": (
            "active" if freshness_required else "off_session"
        ),
        "freshness_required": freshness_required,
    }
    heartbeat = read_json(bridge_paths(qmt_home)["heartbeat"])

    full_payload, full_file_token = _read_snapshot_if_changed(
        "full",
        qmt_home=qmt_home,
        max_age_seconds=effective_max_age,
        tokens=tokens,
    )
    tracked_payload, tracked_file_token = _read_snapshot_if_changed(
        "tracked",
        qmt_home=qmt_home,
        max_age_seconds=effective_max_age,
        tokens=tokens,
    )
    universe_set = set(universe)
    tracked_set = set(tracked)

    full_token = _snapshot_token(full_payload)
    if full_payload and full_token and full_token != tokens.get("full"):
        full_frame = snapshot_frame(full_payload, short_name_map=short_name_map)
        if universe_set and not full_frame.empty:
            full_frame = full_frame.loc[full_frame["stock_code"].isin(universe_set)].copy()
        expected = len(universe_set)
        actual = int(full_frame["stock_code"].nunique()) if not full_frame.empty else 0
        coverage = actual / max(expected, 1)
        required = min(1.0, max(0.50, float(os.environ.get("BIG_QMT_MIN_FULL_COVERAGE", "0.95"))))
        result.update({"full_received": actual, "full_expected": expected, "full_coverage": round(coverage, 4)})
        if expected <= 0:
            raise RuntimeError("cannot publish Big QMT snapshot because si_all_code is empty")
        if coverage < required:
            raise RuntimeError(
                f"Big QMT full snapshot coverage too low: {actual}/{expected} ({coverage:.1%}) < {required:.1%}"
            )
        published_at = datetime.now().replace(microsecond=0)
        result["full_rows"] = _replace_full_snapshot(engine, full_frame)
        sync_receipt = _record_realtime_sync_receipt(
            engine,
            full_payload=full_payload,
            full_file_token=full_file_token,
            heartbeat=heartbeat,
            expected_count=expected,
            observed_count=actual,
            coverage=coverage,
            published_at=published_at,
            capture_mode=(
                "LIVE_FORWARD"
                if freshness_required
                else "OFF_SESSION_SNAPSHOT"
            ),
        )
        tokens["full"] = full_token
        tokens["full_file"] = full_file_token
        tokens["full_sync_receipt"] = sync_receipt
        result["full_sync_receipt"] = sync_receipt
        result["status"] = "success"

    tracked_token = _snapshot_token(tracked_payload)
    if tracked_payload and tracked_token and tracked_token != tokens.get("tracked"):
        tracked_frame = snapshot_frame(tracked_payload, short_name_map=short_name_map)
        if tracked_set and not tracked_frame.empty:
            tracked_frame = tracked_frame.loc[tracked_frame["stock_code"].isin(tracked_set)].copy()
        # ``sm_stock_current`` may be refreshed from QMT's cached post-close
        # snapshot, but Level-1 evidence is accepted only while the exchange
        # session is live.  Otherwise a bridge restart would persist yesterday's
        # final quote with a new receive timestamp.
        if freshness_required and _table_exists(engine, "st_quote_event_v2"):
            live_frame, level1_receipt = level1_snapshot(
                tracked,
                qmt_home=qmt_home,
                now=datetime.now(),
                heartbeat_max_age_seconds=float(
                    os.environ.get("BIG_QMT_HEARTBEAT_MAX_AGE_SECONDS", "30")
                ),
                snapshot_max_age_seconds=float(
                    os.environ.get("BIG_QMT_LEVEL1_SNAPSHOT_MAX_AGE_SECONDS", "15")
                ),
                event_max_age_seconds=float(
                    os.environ.get("BIG_QMT_LEVEL1_EVENT_MAX_AGE_SECONDS", "15")
                ),
                max_ingress_seconds=float(
                    os.environ.get("BIG_QMT_LEVEL1_MAX_INGRESS_SECONDS", "15")
                ),
            )
            result["level1_receipt"] = level1_receipt
            quote_result = (
                persist_quote_events(
                    engine,
                    live_frame.to_dict(orient="records"),
                )
                if level1_receipt.get("status") == "PASS"
                and not live_frame.empty
                else {"received": 0, "inserted": 0}
            )
            result["quote_events_received"] = quote_result["received"]
            result["quote_events_inserted"] = quote_result["inserted"]
        elif not freshness_required:
            result["quote_events_skipped_off_session"] = int(len(tracked_frame))
        result["tracked_rows"] = _replace_tracked_subset(engine, tracked_frame)
        tokens["tracked"] = tracked_token
        tokens["tracked_file"] = tracked_file_token
        result["status"] = "success"

    if (
        "full_sync_receipt" not in result
        and isinstance(tokens.get("full_sync_receipt"), dict)
    ):
        result["full_sync_receipt"] = dict(
            tokens["full_sync_receipt"]
        )
    if heartbeat:
        result["qmt_strategy_status"] = heartbeat.get("status")
        result["qmt_strategy_updated_at"] = heartbeat.get("updated_at")
        result["qmt_strategy_error"] = heartbeat.get("last_error") or ""
        heartbeat_ts = heartbeat.get("updated_ts")
        try:
            result["qmt_strategy_heartbeat_age_seconds"] = round(
                max(0.0, time.time() - float(heartbeat_ts)),
                1,
            )
        except (TypeError, ValueError):
            pass
    if result["status"] == "waiting_for_qmt_strategy" and (full_file_token or tracked_file_token):
        result["status"] = "idle"
    if not freshness_required and (full_file_token or tracked_file_token):
        result["status"] = "idle_market_closed"
    _write_status(qmt_home, result)
    return result


def sync_big_qmt_realtime(
    *,
    engine=None,
    codes: list[str] | None = None,
    tracked_limit: int = 280,
) -> dict[str, Any]:
    """Synchronously publish the latest standard-QMT spool snapshot.

    Explicit codes use a transactional subset replacement for low-latency UI
    refreshes.  An empty code list validates coverage and replaces the full
    market snapshot atomically.
    """
    engine = engine or create_batch_engine(future=True)
    qmt_home = resolve_big_qmt_home(required=True)
    assert qmt_home is not None
    watchlist = refresh_watchlist(engine, qmt_home=qmt_home, tracked_limit=tracked_limit)
    clean_codes = list(
        dict.fromkeys(
            str(code).strip().split(".", 1)[0].zfill(6)
            for code in (codes or [])
            if to_qmt_symbol(str(code).strip())
        )
    )
    if not clean_codes:
        return ingest_once(
            engine,
            qmt_home=qmt_home,
            universe=watchlist["universe"],
            tracked=watchlist["tracked"],
            short_name_map=watchlist["short_name_map"],
        )

    max_age = float(os.environ.get("BIG_QMT_SNAPSHOT_MAX_AGE_SECONDS", "75"))
    freshness_required = _snapshot_freshness_required(engine)
    effective_max_age = max_age if freshness_required else None
    full_payload = read_snapshot(
        "full",
        qmt_home=qmt_home,
        max_age_seconds=effective_max_age,
    )
    tracked_payload = read_snapshot(
        "tracked",
        qmt_home=qmt_home,
        max_age_seconds=effective_max_age,
    )
    frame = merge_snapshot_frames(
        snapshot_frame(full_payload, short_name_map=watchlist["short_name_map"]),
        snapshot_frame(tracked_payload, short_name_map=watchlist["short_name_map"]),
    )
    frame = frame.loc[frame["stock_code"].isin(set(clean_codes))].copy() if not frame.empty else frame
    if frame.empty:
        raise RuntimeError("standard QMT bridge has no fresh quotes for the requested stocks")
    level1_receipt: dict[str, Any] | None = None
    if freshness_required and _table_exists(engine, "st_quote_event_v2"):
        live_frame, level1_receipt = level1_snapshot(
            clean_codes,
            qmt_home=qmt_home,
            now=datetime.now(),
        )
        if level1_receipt.get("status") == "PASS" and not live_frame.empty:
            persist_quote_events(
                engine,
                live_frame.to_dict(orient="records"),
            )
    written = _replace_tracked_subset(engine, frame)
    result = {
        "status": "success",
        "source": PROVIDER_ID,
        "tracked_rows": written,
        "requested": len(clean_codes),
        "received": int(frame["stock_code"].nunique()),
        "market_session": "active" if freshness_required else "off_session",
        "quote_events_skipped_off_session": (
            0 if freshness_required else int(len(frame))
        ),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    if level1_receipt is not None:
        result["level1_receipt"] = level1_receipt
    _write_status(qmt_home, result)
    return result


def _launch_maintenance_job(
    state: dict[str, Any],
    *,
    name: str,
    runner,
) -> bool:
    """Run one slow maintenance job without blocking quote ingestion.

    ETF and membership maintenance can take minutes.  They used to execute in
    the quote-consumer loop, leaving the sync receipt stale and prompting the
    supervisor to recycle an otherwise healthy bridge.  Keep a single daemon
    slot so those heavy jobs remain serialized while quote receipts continue.
    """
    existing = state.get("thread")
    if isinstance(existing, threading.Thread) and existing.is_alive():
        return False

    results = state.setdefault("results", {})
    results[name] = {
        "status": "running",
        "started_at": datetime.now().isoformat(timespec="seconds"),
    }

    def target() -> None:
        try:
            result = runner()
            if not isinstance(result, dict):
                result = {
                    "status": "error",
                    "error": "maintenance runner returned a non-object result",
                }
        except Exception as exc:
            result = {"status": "error", "error": str(exc)}
        results[name] = result
        state["active_name"] = None
        if result.get("status") in {"success", "failed", "error"}:
            print(
                json.dumps({name: result}, ensure_ascii=False, default=str),
                flush=True,
            )

    thread = threading.Thread(
        target=target,
        name=f"big-qmt-{name}",
        daemon=True,
    )
    state["thread"] = thread
    state["active_name"] = name
    try:
        thread.start()
    except Exception as exc:
        state["thread"] = None
        state["active_name"] = None
        results[name] = {"status": "error", "error": str(exc)}
        return False
    return True


def _run_maintenance_with_fresh_engine(runner) -> dict[str, Any]:
    """Give a background maintenance job an isolated SQLAlchemy pool."""
    maintenance_engine = create_batch_engine(future=True)
    try:
        return runner(maintenance_engine)
    finally:
        dispose = getattr(maintenance_engine, "dispose", None)
        if callable(dispose):
            dispose()


def _shutdown_maintenance_job(
    state: dict[str, Any],
    *,
    wait_seconds: float = 15.0,
) -> None:
    _terminate_active_maintenance_processes()
    thread = state.get("thread")
    if isinstance(thread, threading.Thread) and thread.is_alive():
        thread.join(max(0.0, float(wait_seconds)))


def run_daemon(*, qmt_home: Path, poll_seconds: float, tracked_limit: int) -> int:
    engine = create_batch_engine(future=True)
    stopped = False

    def request_stop(_signum, _frame):
        nonlocal stopped
        stopped = True

    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, request_stop)

    watchlist = refresh_watchlist(engine, qmt_home=qmt_home, tracked_limit=tracked_limit)
    last_watchlist_refresh = time.monotonic()
    last_membership_check = 0.0
    last_membership_result: dict[str, Any] = {}
    last_etf_forward_check = 0.0
    last_etf_forward_result: dict[str, Any] = {}
    maintenance_state: dict[str, Any] = {"results": {}}
    last_tokens: dict[str, str] = {}
    last_level1_reconnect = 0.0
    while not stopped:
        try:
            interval = max(10.0, float(os.environ.get("BIG_QMT_WATCHLIST_REFRESH_SECONDS", "30")))
            if time.monotonic() - last_watchlist_refresh >= interval:
                watchlist = refresh_watchlist(engine, qmt_home=qmt_home, tracked_limit=tracked_limit)
                last_watchlist_refresh = time.monotonic()
            result = ingest_once(
                engine,
                qmt_home=qmt_home,
                universe=watchlist["universe"],
                tracked=watchlist["tracked"],
                short_name_map=watchlist["short_name_map"],
                last_tokens=last_tokens,
            )
            if result.get("freshness_required"):
                _live_frame, level1_receipt = level1_snapshot(
                    watchlist["tracked"],
                    qmt_home=qmt_home,
                    now=datetime.now(),
                )
                result["level1_receipt"] = level1_receipt
                reconnectable = level1_receipt.get("reason") in {
                    "subscription_missing",
                    "no_fresh_live_callback",
                    "tracked_snapshot_stale",
                }
                reconnect_cooldown = max(
                    5.0,
                    float(
                        os.environ.get(
                            "BIG_QMT_LEVEL1_RECONNECT_COOLDOWN_SECONDS",
                            "20",
                        )
                    ),
                )
                current_mono = time.monotonic()
                if (
                    level1_receipt.get("status") != "PASS"
                    and reconnectable
                    and current_mono - last_level1_reconnect
                    >= reconnect_cooldown
                ):
                    result["level1_reconnect"] = request_level1_reconnect(
                        qmt_home=qmt_home,
                        now=datetime.now(),
                    )
                    last_level1_reconnect = current_mono
            # Publish the first quote receipt before any optional slow job.
            # A post-close ETF run can take several minutes; running it before
            # ingest made a healthy cold consumer look dead to the supervisor.
            etf_check_seconds = max(
                30.0,
                float(
                    os.environ.get(
                        "BIG_QMT_ETF_FORWARD_CHECK_SECONDS",
                        "60",
                    )
                ),
            )
            if (
                time.monotonic() - last_etf_forward_check
                >= etf_check_seconds
            ):
                if _launch_maintenance_job(
                    maintenance_state,
                    name="etf_forward_daily",
                    runner=lambda: _run_maintenance_with_fresh_engine(
                        maybe_run_etf_forward_daily
                    ),
                ):
                    last_etf_forward_check = time.monotonic()
            membership_check_seconds = max(
                60.0,
                float(os.environ.get("BIG_QMT_MEMBERSHIP_CHECK_SECONDS", "300")),
            )
            if time.monotonic() - last_membership_check >= membership_check_seconds:
                if _launch_maintenance_job(
                    maintenance_state,
                    name="membership_snapshot",
                    runner=lambda: _run_maintenance_with_fresh_engine(
                        maybe_sync_membership_snapshot
                    ),
                ):
                    last_membership_check = time.monotonic()
            maintenance_results = maintenance_state.get("results", {})
            if isinstance(maintenance_results, dict):
                last_etf_forward_result = dict(
                    maintenance_results.get("etf_forward_daily") or {}
                )
                last_membership_result = dict(
                    maintenance_results.get("membership_snapshot") or {}
                )
            if last_membership_result:
                result["membership_snapshot"] = last_membership_result
            if last_etf_forward_result:
                result["etf_forward_daily"] = last_etf_forward_result
            if last_membership_result or last_etf_forward_result:
                _write_status(qmt_home, result)
            if result.get("status") == "success":
                print(json.dumps(result, ensure_ascii=False, default=str), flush=True)
        except Exception as exc:
            error = {
                "status": "error",
                "source": PROVIDER_ID,
                "error": str(exc),
                "generated_at": datetime.now().isoformat(timespec="seconds"),
            }
            _write_status(qmt_home, error)
            print(json.dumps(error, ensure_ascii=False), file=sys.stderr, flush=True)
        time.sleep(max(0.2, poll_seconds))
    _shutdown_maintenance_job(maintenance_state)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the standard QMT quote bridge consumer")
    parser.add_argument("--install-strategy", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--tracked-limit", type=int, default=280)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    qmt_home = resolve_big_qmt_home(required=True)
    assert qmt_home is not None
    installed_path = None
    if args.install_strategy:
        installed_path = install_qmt_strategy(qmt_home=qmt_home)

    if args.once:
        engine = create_batch_engine(future=True)
        watchlist = refresh_watchlist(
            engine,
            qmt_home=qmt_home,
            tracked_limit=args.tracked_limit,
        )
        result = ingest_once(
            engine,
            qmt_home=qmt_home,
            universe=watchlist["universe"],
            tracked=watchlist["tracked"],
            short_name_map=watchlist["short_name_map"],
        )
        if installed_path:
            result["installed_strategy"] = str(installed_path)
        print(json.dumps(result, ensure_ascii=False, default=str) if args.json else result)
        return 0

    if installed_path:
        print(f"Installed QMT strategy: {installed_path}", flush=True)
    try:
        return run_daemon(
            qmt_home=qmt_home,
            poll_seconds=max(0.2, args.poll_seconds),
            tracked_limit=max(1, min(280, args.tracked_limit)),
        )
    finally:
        _terminate_active_maintenance_processes()


if __name__ == "__main__":
    raise SystemExit(main())
