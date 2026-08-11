from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from server.common.process_env import build_child_env, child_process_timeout
from tools.env_config import load_project_env


def log(message: str) -> None:
    print(f"{datetime.now().isoformat(timespec='seconds')} {message}", flush=True)


def run_cmd(args: list[str], *, env: dict[str, str] | None = None, timeout_seconds: int | None = None) -> int:
    log("RUN " + " ".join(args))
    timeout = child_process_timeout(
        timeout_seconds or 6 * 60 * 60,
        env_name="PROBIGA_MISSING_DATA_CMD_TIMEOUT",
    )
    try:
        proc = subprocess.run(args, cwd=str(ROOT), env=env or build_child_env(ROOT), text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        log(f"TIMEOUT after {timeout}s :: {' '.join(args[:3])}")
        return 124
    log(f"DONE exit={proc.returncode} :: {' '.join(args[:3])}")
    return int(proc.returncode)


def running_scheduler_tasks(engine) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id, task_name, task_type, last_run_at,
                       TIMESTAMPDIFF(SECOND, last_run_at, NOW()) AS age_seconds
                FROM st_scheduled_tasks
                WHERE last_run_status = 'running'
                ORDER BY last_run_at
                """
            )
        ).mappings().all()
    return [dict(row) for row in rows]


def wait_for_scheduler_idle(engine, *, stable_seconds: int, poll_seconds: int, max_wait_seconds: int) -> None:
    started = time.monotonic()
    idle_since: float | None = None
    while True:
        running = running_scheduler_tasks(engine)
        if running:
            idle_since = None
            summary = ", ".join(f"#{row['id']} {row['task_name']} age={row.get('age_seconds')}s" for row in running)
            log(f"WAIT scheduler busy: {summary}")
        else:
            if idle_since is None:
                idle_since = time.monotonic()
                log("WAIT scheduler has no running DB task; watching for queued task handoff")
            idle_for = time.monotonic() - idle_since
            if idle_for >= stable_seconds:
                log(f"IDLE scheduler stable for {int(idle_for)}s")
                return

        if max_wait_seconds > 0 and time.monotonic() - started >= max_wait_seconds:
            log("WAIT max wait reached; continuing with backfill anyway")
            return
        time.sleep(max(5, poll_seconds))


def trade_days(engine, *, start_date: str, end_date: str) -> list[str]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT trade_date
                FROM si_trade_calendar
                WHERE trade_status = 1
                  AND trade_date >= :start
                  AND trade_date <= :end
                ORDER BY trade_date DESC
                """
            ),
            {"start": start_date, "end": end_date},
        ).fetchall()
    return [str(row[0])[:10] for row in rows]


def coverage_for_dates(engine, dates: list[str]) -> dict[str, dict[str, int]]:
    if not dates:
        return {}
    params = {f"d{i}": value for i, value in enumerate(dates)}
    in_sql = ", ".join(f":d{i}" for i in range(len(dates)))
    result = {value: {"daily_stocks": 0, "minute_rows": 0} for value in dates}
    with engine.connect() as conn:
        daily = conn.execute(
            text(
                f"""
                SELECT trade_date, COUNT(DISTINCT stock_code) AS cnt
                FROM sm_stock_kline
                WHERE k_type = 1 AND adjust_type = 0
                  AND trade_date IN ({in_sql})
                GROUP BY trade_date
                """
            ),
            params,
        ).mappings().all()
        minute = conn.execute(
            text(
                f"""
                SELECT trade_date, COUNT(*) AS cnt
                FROM sm_stock_minute
                WHERE trade_date IN ({in_sql})
                GROUP BY trade_date
                """
            ),
            params,
        ).mappings().all()
    for row in daily:
        result[str(row["trade_date"])[:10]]["daily_stocks"] = int(row["cnt"] or 0)
    for row in minute:
        result[str(row["trade_date"])[:10]]["minute_rows"] = int(row["cnt"] or 0)
    return result


def find_gaps(
    engine,
    *,
    start_date: str,
    end_date: str,
    min_daily_stocks: int,
    min_minute_rows: int,
    max_dates: int,
) -> list[str]:
    days = trade_days(engine, start_date=start_date, end_date=end_date)
    coverage = coverage_for_dates(engine, days)
    gaps = []
    for day in days:
        item = coverage.get(day, {})
        if int(item.get("daily_stocks") or 0) < min_daily_stocks or int(item.get("minute_rows") or 0) < min_minute_rows:
            gaps.append(day)
    if max_dates > 0:
        gaps = gaps[:max_dates]
    log("GAPS " + json.dumps({day: coverage.get(day, {}) for day in gaps}, ensure_ascii=False, sort_keys=True))
    return gaps


def backfill_mode_for_coverage(
    coverage: dict[str, int],
    *,
    min_daily_stocks: int,
    min_minute_rows: int,
) -> str:
    daily_missing = int(coverage.get("daily_stocks") or 0) < min_daily_stocks
    minute_missing = int(coverage.get("minute_rows") or 0) < min_minute_rows
    if daily_missing and minute_missing:
        return "all"
    if minute_missing:
        return "minute"
    if daily_missing:
        return "daily"
    return "all"


def main() -> int:
    parser = argparse.ArgumentParser(description="Wait for scheduler idle, then repair missing QMT business history.")
    parser.add_argument("--start-date", default="2026-06-01")
    parser.add_argument("--end-date", default="")
    parser.add_argument("--max-dates", type=int, default=12)
    parser.add_argument("--stable-idle-seconds", type=int, default=180)
    parser.add_argument("--poll-seconds", type=int, default=60)
    parser.add_argument("--max-wait-seconds", type=int, default=14400)
    parser.add_argument("--min-daily-stocks", type=int, default=4441)
    parser.add_argument("--min-minute-rows", type=int, default=1_070_425)
    parser.add_argument("--minute-batch-size", type=int, default=80)
    parser.add_argument("--daily-batch-size", type=int, default=120)
    parser.add_argument("--command-timeout-seconds", type=int, default=6 * 60 * 60)
    args = parser.parse_args()

    load_project_env()
    engine = create_batch_engine(future=True)
    wait_for_scheduler_idle(
        engine,
        stable_seconds=max(30, args.stable_idle_seconds),
        poll_seconds=max(10, args.poll_seconds),
        max_wait_seconds=max(0, args.max_wait_seconds),
    )

    end_date = args.end_date or (date.today() - timedelta(days=1)).isoformat()
    gaps = find_gaps(
        engine,
        start_date=args.start_date,
        end_date=end_date,
        min_daily_stocks=args.min_daily_stocks,
        min_minute_rows=args.min_minute_rows,
        max_dates=args.max_dates,
    )
    if not gaps:
        log("No gaps found.")
        return 0

    env = build_child_env(ROOT)
    log_path = ROOT / "data" / "logs" / f"qmt_missing_backfill_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
    failures = 0
    for day in gaps:
        coverage_before = coverage_for_dates(engine, [day]).get(day, {})
        mode = backfill_mode_for_coverage(
            coverage_before,
            min_daily_stocks=args.min_daily_stocks,
            min_minute_rows=args.min_minute_rows,
        )
        log(f"BACKFILL start {day} mode={mode} coverage={json.dumps(coverage_before, ensure_ascii=False, sort_keys=True)}")
        rc = run_cmd(
            [
                sys.executable,
                "tools/run_guojin_qmt_full_market_history.py",
                "--start-date",
                day,
                "--end-date",
                day,
                "--mode",
                mode,
                "--daily-batch-size",
                str(max(1, args.daily_batch_size)),
                "--minute-batch-size",
                str(max(1, args.minute_batch_size)),
                "--date-workers",
                "1",
                "--sleep-seconds",
                "0.2",
                "--log-path",
                str(log_path),
                "--json",
            ],
            env=env,
            timeout_seconds=args.command_timeout_seconds,
        )
        if rc != 0:
            failures += 1
            log(f"BACKFILL qmt failed for {day}; promote will still try existing local rows")

        promote_args = [sys.executable, "tools/promote_qmt_local_history_to_business.py"]
        if mode in {"all", "daily"}:
            promote_args.extend(["--daily-dates", day])
        if mode in {"all", "minute"}:
            promote_args.extend(["--minute-dates", day, "--derive-daily-from-minute-dates", day])
        rc = run_cmd(
            promote_args,
            env=env,
            timeout_seconds=args.command_timeout_seconds,
        )
        if rc != 0:
            failures += 1
        coverage = coverage_for_dates(engine, [day]).get(day, {})
        log("COVERAGE " + json.dumps({day: coverage}, ensure_ascii=False, sort_keys=True))

    log(f"FINISH gaps={len(gaps)} failures={failures}")
    return 0 if failures == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
