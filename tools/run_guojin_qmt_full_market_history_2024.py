from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time as datetime_time, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import text

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
from integrations.bigqmt.spool import PROVIDER_ID as BIGQMT_PROVIDER_ID
from server.common.batch_db import create_batch_engine, quote_identifier


def _source_engine():
    return create_batch_engine(future=True)


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _acquire_lock(lock_path: Path) -> tuple[bool, str]:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    if lock_path.exists():
        raw = lock_path.read_text(encoding="utf-8", errors="ignore").strip()
        try:
            existing_pid = int(raw.split()[0])
        except Exception:
            existing_pid = 0
        if _pid_alive(existing_pid):
            return False, raw
    lock_path.write_text(f"{os.getpid()} {datetime.now().isoformat(timespec='seconds')}", encoding="utf-8")
    return True, ""


def _release_lock(lock_path: Path) -> None:
    try:
        raw = lock_path.read_text(encoding="utf-8", errors="ignore").strip()
        pid = int(raw.split()[0])
        if pid == os.getpid():
            lock_path.unlink(missing_ok=True)
    except Exception as exc:
        print(f"[WARN] failed to release lock {lock_path}: {exc}", file=sys.stderr)


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
    if table == LOCAL_KLINE_TABLE:
        predicate = (
            "trade_date = :d AND provider = :provider "
            "AND period = '1d' AND adjust_type = 0"
        )
    elif table == LOCAL_MINUTE_TABLE:
        predicate = (
            "trade_date = :d AND provider = :provider "
            "AND period = '1m'"
        )
    else:
        raise ValueError(f"unsupported local history table: {table}")
    with local_engine.begin() as conn:
        return int(
            conn.execute(
                text(
                    f"SELECT COUNT(*) FROM {quote_identifier(table)} "
                    f"WHERE {predicate}"
                ),
                {"d": trade_date, "provider": BIGQMT_PROVIDER_ID},
            ).scalar()
            or 0
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
    reverse: bool = False,
    date_workers: int = 1,
) -> dict[str, Any]:
    source_engine = _source_engine()
    local_engine = get_local_history_engine()
    ensure_local_history_tables(local_engine)
    codes = load_stock_codes(source_engine)
    trade_dates = load_trade_dates(source_engine, start_date=start_date, end_date=end_date)
    if reverse:
        trade_dates = list(reversed(trade_dates))
    stock_count = len(codes)
    worker_count = max(1, int(date_workers or 1))
    log_lock = threading.Lock()

    def log_event(payload: dict[str, Any]) -> None:
        with log_lock:
            _log(log_path, payload)

    summary = {
        "status": "started",
        "start_date": start_date,
        "end_date": end_date,
        "trade_days": len(trade_dates),
        "stock_count": stock_count,
        "modes": sorted(modes),
        "local_database": str(local_engine.url.database or ""),
        "stop_at": stop_at.isoformat(timespec="minutes") if stop_at else "",
        "reverse": reverse,
        "date_workers": worker_count,
    }
    log_event({"event": "start", **summary})

    def process_trade_date(trade_date: str) -> dict[str, Any]:
        daily_done = 0
        minute_done = 0
        errors = 0
        if _stop_time_reached(stop_at):
            log_event({"event": "stop_window_reached", "trade_date": trade_date})
            return {"trade_date": trade_date, "daily_done": 0, "minute_done": 0, "errors": 0, "stopped": True}
        if "daily" in modes:
            expected_daily = max(1, int(stock_count * 0.80))
            current_daily = _local_count(local_engine, table=LOCAL_KLINE_TABLE, trade_date=trade_date)
            if resume and current_daily >= expected_daily:
                log_event({"event": "skip_daily", "trade_date": trade_date, "rows": current_daily})
            else:
                try:
                    result = backfill_daily_kline_local(
                        source_engine=source_engine,
                        local_engine=local_engine,
                        stock_codes=codes,
                        start_date=trade_date,
                        end_date=trade_date,
                        batch_size=daily_batch_size,
                        dividend_type="none",
                        backend="bigqmt",
                        dry_run=False,
                    )
                    daily_done += 1
                    log_event(
                        {
                            "event": "daily_done",
                            "trade_date": trade_date,
                            "fetched_rows": result.fetched_rows,
                            "written_rows": result.written_rows,
                            "batch_count": result.batch_count,
                        }
                    )
                except Exception as exc:
                    errors += 1
                    log_event({"event": "daily_error", "trade_date": trade_date, "error": str(exc)})
        if _stop_time_reached(stop_at):
            log_event({"event": "stop_window_reached", "trade_date": trade_date, "after": "daily"})
            return {
                "trade_date": trade_date,
                "daily_done": daily_done,
                "minute_done": minute_done,
                "errors": errors,
                "stopped": True,
            }
        if "minute" in modes:
            expected_minute = max(1, int(_expected_minute_rows(stock_count) * 0.80))
            current_minute = _local_count(local_engine, table=LOCAL_MINUTE_TABLE, trade_date=trade_date)
            if resume and current_minute >= expected_minute:
                log_event({"event": "skip_minute", "trade_date": trade_date, "rows": current_minute})
            else:
                try:
                    result = backfill_minute_local(
                        source_engine=source_engine,
                        local_engine=local_engine,
                        stock_codes=codes,
                        trade_dates=[trade_date],
                        batch_size=minute_batch_size,
                        backend="bigqmt",
                        dry_run=False,
                    )
                    minute_done += 1
                    log_event(
                        {
                            "event": "minute_done",
                            "trade_date": trade_date,
                            "fetched_rows": result.fetched_rows,
                            "written_rows": result.written_rows,
                            "batch_count": result.batch_count,
                        }
                    )
                except Exception as exc:
                    errors += 1
                    log_event({"event": "minute_error", "trade_date": trade_date, "error": str(exc)})
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
        return {
            "trade_date": trade_date,
            "daily_done": daily_done,
            "minute_done": minute_done,
            "errors": errors,
            "stopped": False,
        }

    daily_done = 0
    minute_done = 0
    errors = 0
    stopped_by_window = False

    if worker_count <= 1:
        for trade_date in trade_dates:
            result = process_trade_date(trade_date)
            daily_done += int(result.get("daily_done") or 0)
            minute_done += int(result.get("minute_done") or 0)
            errors += int(result.get("errors") or 0)
            if result.get("stopped"):
                stopped_by_window = True
                break
    else:
        log_event({"event": "parallel_start", "date_workers": worker_count, "trade_days": len(trade_dates)})
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {}
            for trade_date in trade_dates:
                if _stop_time_reached(stop_at):
                    stopped_by_window = True
                    log_event({"event": "stop_window_reached", "trade_date": trade_date, "before_submit": True})
                    break
                futures[executor.submit(process_trade_date, trade_date)] = trade_date
            for future in as_completed(futures):
                trade_date = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    errors += 1
                    log_event({"event": "trade_date_error", "trade_date": trade_date, "error": str(exc)})
                    continue
                daily_done += int(result.get("daily_done") or 0)
                minute_done += int(result.get("minute_done") or 0)
                errors += int(result.get("errors") or 0)
                stopped_by_window = stopped_by_window or bool(result.get("stopped"))

    final = {
        "status": "stopped_window" if stopped_by_window else "finished",
        "daily_trade_days_done": daily_done,
        "minute_trade_days_done": minute_done,
        "errors": errors,
        "trade_days": len(trade_dates),
        "stock_count": stock_count,
        "stop_at": stop_at.isoformat(timespec="minutes") if stop_at else "",
        "date_workers": worker_count,
        "finished_at": datetime.now().isoformat(timespec="seconds"),
    }
    log_event({"event": "finish", **final})
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
    parser.add_argument("--reverse", action="store_true", help="Process trade dates from end-date backwards.")
    parser.add_argument("--date-workers", type=int, default=1, help="Trade-date level parallel workers. Start with 2 for QMT.")
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
                reverse=args.reverse,
                date_workers=max(1, args.date_workers),
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
