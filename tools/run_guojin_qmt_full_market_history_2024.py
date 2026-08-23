from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime, time as datetime_time, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.qmt.local_history import (
    LOCAL_KLINE_TABLE,
    LOCAL_MINUTE_TABLE,
    backfill_daily_kline_local,
    backfill_minute_local,
    ensure_local_history_tables,
    get_local_history_engine,
    load_stock_codes,
    load_trade_dates,
)
from server.common.config import get_mysql_url
from integrations.bigqmt.spool import PROVIDER_ID as BIGQMT_PROVIDER_ID

PRODUCTION_KLINE_TABLE = "sm_stock_kline"
LOCK_INITIALIZATION_GRACE_SECONDS = 5.0


def _source_engine():
    return create_engine(get_mysql_url(required=True), pool_pre_ping=True, future=True)


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        process_query_limited_information = 0x1000
        still_active = 259
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.OpenProcess.argtypes = (
            wintypes.DWORD,
            wintypes.BOOL,
            wintypes.DWORD,
        )
        kernel32.OpenProcess.restype = wintypes.HANDLE
        kernel32.GetExitCodeProcess.argtypes = (
            wintypes.HANDLE,
            ctypes.POINTER(wintypes.DWORD),
        )
        kernel32.GetExitCodeProcess.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
        kernel32.CloseHandle.restype = wintypes.BOOL

        handle = kernel32.OpenProcess(
            process_query_limited_information,
            False,
            pid,
        )
        if not handle:
            return False
        try:
            exit_code = wintypes.DWORD()
            if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                return False
            return exit_code.value == still_active
        finally:
            kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _acquire_lock(lock_path: Path) -> tuple[bool, str]:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    payload = f"{os.getpid()} {datetime.now().isoformat(timespec='seconds')}"
    owner = ""
    for _attempt in range(3):
        try:
            descriptor = os.open(
                lock_path,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            )
        except FileExistsError:
            before = None
            try:
                before = lock_path.stat()
                owner = lock_path.read_text(
                    encoding="utf-8", errors="ignore"
                ).strip()
                existing_pid = int(owner.split()[0])
            except (OSError, ValueError, IndexError):
                existing_pid = 0
            if _pid_alive(existing_pid):
                return False, owner
            if (
                existing_pid <= 0
                and before is not None
                and max(0.0, time.time() - before.st_mtime)
                < LOCK_INITIALIZATION_GRACE_SECONDS
            ):
                return False, owner or "lock_initializing"
            try:
                if before is None:
                    continue
                after = lock_path.stat()
                before_identity = (
                    before.st_ino,
                    before.st_size,
                    before.st_mtime_ns,
                )
                after_identity = (
                    after.st_ino,
                    after.st_size,
                    after.st_mtime_ns,
                )
                if before_identity == after_identity:
                    lock_path.unlink()
            except (OSError, UnboundLocalError):
                pass
            continue
        except OSError as exc:
            return False, f"lock_error:{type(exc).__name__}"
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                stream.write(payload)
                stream.flush()
        except Exception:
            try:
                os.close(descriptor)
            except OSError:
                pass
            lock_path.unlink(missing_ok=True)
            raise
        return True, ""
    return False, owner or "stale_lock_could_not_be_replaced"


def _release_lock(lock_path: Path) -> None:
    try:
        raw = lock_path.read_text(encoding="utf-8", errors="ignore").strip()
        pid = int(raw.split()[0])
        if pid == os.getpid():
            lock_path.unlink(missing_ok=True)
    except Exception:
        pass


def _latest_trade_date(source_engine) -> str:
    value = None
    try:
        with source_engine.begin() as conn:
            value = conn.execute(
                text("SELECT MAX(trade_date) FROM si_trade_calendar WHERE trade_status = 1 AND trade_date <= CURDATE()")
            ).scalar()
    except Exception:
        value = None
    if value:
        return str(value)[:10]
    with source_engine.begin() as conn:
        value = conn.execute(
            text("SELECT MAX(trade_date) FROM sm_stock_kline WHERE k_type = 1 AND trade_date <= CURDATE()")
        ).scalar()
    if not value:
        return date.today().isoformat()
    return str(value)[:10]


def _local_count(local_engine, *, table: str, trade_date: str) -> int:
    native_raw_filter = (
        " AND adjust_type=0 AND pre_close_origin='NATIVE_QMT'"
        f" AND provider='{BIGQMT_PROVIDER_ID}'"
        if table == LOCAL_KLINE_TABLE
        else ""
    )
    with local_engine.begin() as conn:
        return int(
            conn.execute(
                text(
                    f"SELECT COUNT(*) FROM `{table}` WHERE trade_date = :d"
                    f"{native_raw_filter}"
                ),
                {"d": trade_date},
            ).scalar()
            or 0
        )


def _daily_stock_set(
    engine,
    *,
    table: str,
    trade_date: str,
    require_native_qmt: bool,
) -> set[str]:
    native_raw_filter = (
        " AND adjust_type=0 AND pre_close_origin='NATIVE_QMT'"
        f" AND provider='{BIGQMT_PROVIDER_ID}'"
        if require_native_qmt
        else " AND adjust_type=0"
    )
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                f"SELECT DISTINCT stock_code FROM `{table}` "
                "WHERE trade_date = :d AND k_type=1 "
                "AND stock_code REGEXP '^(0|3|6)'"
                f"{native_raw_filter}"
            ),
            {"d": trade_date},
        )
        return {
            str(row[0]).strip()
            for row in rows
            if row[0] is not None and str(row[0]).strip()
        }


def _daily_coverage_error(
    *,
    trade_date: str,
    expected: set[str],
    actual: set[str],
) -> RuntimeError:
    missing = sorted(expected - actual)
    unexpected = sorted(actual - expected)
    return RuntimeError(
        "QMT daily coverage mismatch after backfill: "
        f"trade_date={trade_date}, expected={len(expected)}, actual={len(actual)}, "
        f"missing={len(missing)} sample={missing[:20]}, "
        f"unexpected={len(unexpected)} sample={unexpected[:20]}"
    )


def _log(path: Path, payload: dict[str, Any]) -> None:
    payload = {"logged_at": datetime.now().isoformat(timespec="seconds"), **payload}
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(payload, ensure_ascii=False, default=str, sort_keys=True) + "\n")


def _expected_minute_rows(stock_count: int) -> int:
    # A-share 1m data normally has 241 bars from 09:30 to 15:00 inclusive.
    return stock_count * 241


def _parse_stop_at(value: str) -> datetime_time | None:
    if not value:
        return None
    try:
        hour, minute = value.split(":", 1)
        return datetime_time(hour=int(hour), minute=int(minute))
    except Exception as exc:
        raise ValueError("--stop-at must be HH:MM, for example 07:00") from exc


def _stop_time_reached(stop_at: datetime_time | None) -> bool:
    if stop_at is None:
        return False
    return datetime.now().time() >= stop_at


def run_full_history(
    *,
    start_date: str,
    end_date: str,
    modes: set[str],
    daily_batch_size: int,
    minute_batch_size: int,
    sleep_seconds: float,
    resume: bool,
    log_path: Path,
    stop_at: datetime_time | None = None,
) -> dict[str, Any]:
    source_engine = _source_engine()
    local_engine = get_local_history_engine()
    ensure_local_history_tables(local_engine)
    codes = load_stock_codes(source_engine)
    trade_dates = load_trade_dates(source_engine, start_date=start_date, end_date=end_date)
    stock_count = len(codes)
    summary = {
        "status": "started",
        "start_date": start_date,
        "end_date": end_date,
        "trade_days": len(trade_dates),
        "stock_count": stock_count,
        "modes": sorted(modes),
        "local_database": str(local_engine.url.database or ""),
        "stop_at": stop_at.isoformat(timespec="minutes") if stop_at else "",
    }
    _log(log_path, {"event": "start", **summary})

    daily_done = 0
    minute_done = 0
    errors = 0
    stopped_by_window = False
    for trade_date in trade_dates:
        if _stop_time_reached(stop_at):
            stopped_by_window = True
            _log(log_path, {"event": "stop_window_reached", "trade_date": trade_date})
            break
        if "daily" in modes:
            expected_daily_codes = _daily_stock_set(
                source_engine,
                table=PRODUCTION_KLINE_TABLE,
                trade_date=trade_date,
                require_native_qmt=False,
            )
            if not expected_daily_codes:
                error = RuntimeError(
                    "production daily target set is empty: "
                    f"trade_date={trade_date}, table={PRODUCTION_KLINE_TABLE}"
                )
                _log(log_path, {"event": "daily_error", "trade_date": trade_date, "error": str(error)})
                raise error
            current_daily_codes = _daily_stock_set(
                local_engine,
                table=LOCAL_KLINE_TABLE,
                trade_date=trade_date,
                require_native_qmt=True,
            )
            if resume and current_daily_codes == expected_daily_codes:
                _log(
                    log_path,
                    {
                        "event": "skip_daily",
                        "trade_date": trade_date,
                        "rows": len(current_daily_codes),
                        "expected_rows": len(expected_daily_codes),
                        "coverage": "exact_stock_set",
                    },
                )
            else:
                try:
                    result = backfill_daily_kline_local(
                        source_engine=source_engine,
                        local_engine=local_engine,
                        stock_codes=codes,
                        start_date=trade_date,
                        end_date=trade_date,
                        batch_size=daily_batch_size,
                        dry_run=False,
                    )
                except Exception as exc:
                    errors += 1
                    _log(log_path, {"event": "daily_error", "trade_date": trade_date, "error": str(exc)})
                else:
                    verified_daily_codes = _daily_stock_set(
                        local_engine,
                        table=LOCAL_KLINE_TABLE,
                        trade_date=trade_date,
                        require_native_qmt=True,
                    )
                    if verified_daily_codes != expected_daily_codes:
                        errors += 1
                        error = _daily_coverage_error(
                            trade_date=trade_date,
                            expected=expected_daily_codes,
                            actual=verified_daily_codes,
                        )
                        _log(
                            log_path,
                            {"event": "daily_error", "trade_date": trade_date, "error": str(error)},
                        )
                        raise error
                    daily_done += 1
                    _log(
                        log_path,
                        {
                            "event": "daily_done",
                            "trade_date": trade_date,
                            "fetched_rows": result.fetched_rows,
                            "written_rows": result.written_rows,
                            "batch_count": result.batch_count,
                            "expected_rows": len(expected_daily_codes),
                            "verified_rows": len(verified_daily_codes),
                            "coverage": "exact_stock_set",
                        },
                    )
        if _stop_time_reached(stop_at):
            stopped_by_window = True
            _log(log_path, {"event": "stop_window_reached", "trade_date": trade_date, "after": "daily"})
            break
        if "minute" in modes:
            expected_minute = max(1, int(_expected_minute_rows(stock_count) * 0.80))
            current_minute = _local_count(local_engine, table=LOCAL_MINUTE_TABLE, trade_date=trade_date)
            if resume and current_minute >= expected_minute:
                _log(log_path, {"event": "skip_minute", "trade_date": trade_date, "rows": current_minute})
            else:
                try:
                    result = backfill_minute_local(
                        source_engine=source_engine,
                        local_engine=local_engine,
                        stock_codes=codes,
                        trade_dates=[trade_date],
                        batch_size=minute_batch_size,
                        dry_run=False,
                    )
                    minute_done += 1
                    _log(
                        log_path,
                        {
                            "event": "minute_done",
                            "trade_date": trade_date,
                            "fetched_rows": result.fetched_rows,
                            "written_rows": result.written_rows,
                            "batch_count": result.batch_count,
                        },
                    )
                except Exception as exc:
                    errors += 1
                    _log(log_path, {"event": "minute_error", "trade_date": trade_date, "error": str(exc)})
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    final = {
        "status": "stopped_window" if stopped_by_window else "finished",
        "daily_trade_days_done": daily_done,
        "minute_trade_days_done": minute_done,
        "errors": errors,
        "trade_days": len(trade_dates),
        "stock_count": stock_count,
        "stop_at": stop_at.isoformat(timespec="minutes") if stop_at else "",
        "finished_at": datetime.now().isoformat(timespec="seconds"),
    }
    _log(log_path, {"event": "finish", **final})
    return final


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Guojin QMT full-market local history backfill.")
    parser.add_argument("--start-date", default="2024-01-01")
    parser.add_argument("--end-date", default="")
    parser.add_argument("--mode", choices=["all", "daily", "minute"], default="all")
    parser.add_argument("--daily-batch-size", type=int, default=120)
    parser.add_argument("--minute-batch-size", type=int, default=80)
    parser.add_argument("--sleep-seconds", type=float, default=0.2)
    parser.add_argument("--no-resume", action="store_true")
    parser.add_argument("--stop-at", default="", help="Stop naturally once local time reaches HH:MM, e.g. 07:00.")
    parser.add_argument("--log-path", default="")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    source_engine = _source_engine()
    end_date = args.end_date or _latest_trade_date(source_engine)
    modes = {"daily", "minute"} if args.mode == "all" else {args.mode}
    log_path = Path(args.log_path) if args.log_path else ROOT / "data" / "logs" / "qmt_full_market_history_2024.jsonl"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    stop_at = _parse_stop_at(args.stop_at)
    lock_path = ROOT / "data" / "runtime" / "qmt_full_market_history.lock"
    acquired, owner = _acquire_lock(lock_path)
    if not acquired:
        result = {
            "status": "already_running",
            "lock_path": str(lock_path),
            "owner": owner,
            "finished_at": datetime.now().isoformat(timespec="seconds"),
        }
    else:
        try:
            result = run_full_history(
                start_date=args.start_date,
                end_date=end_date,
                modes=modes,
                daily_batch_size=max(1, args.daily_batch_size),
                minute_batch_size=max(1, args.minute_batch_size),
                sleep_seconds=max(0.0, args.sleep_seconds),
                resume=not args.no_resume,
                log_path=log_path,
                stop_at=stop_at,
            )
        finally:
            _release_lock(lock_path)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    else:
        print(result)
    return 0 if result.get("status") == "already_running" or result.get("errors", 0) == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
