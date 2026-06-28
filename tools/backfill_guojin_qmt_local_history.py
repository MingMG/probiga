from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.qmt.local_history import (
    backfill_daily_kline_local,
    backfill_minute_local,
    ensure_local_history_tables,
    get_local_history_engine,
    load_stock_codes,
    load_trade_dates,
    result_dict,
)
from server.common.config import get_mysql_url


def _source_engine():
    return create_engine(get_mysql_url(required=True), pool_pre_ping=True, future=True)


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
    except Exception:
        pass


def _codes_from_arg(source_engine, raw_codes: str, *, limit: int) -> list[str]:
    codes = [item.strip().zfill(6) for item in raw_codes.split(",") if item.strip()]
    return load_stock_codes(source_engine, codes=codes or None, limit=max(0, int(limit or 0)))


@dataclass(frozen=True)
class ResolvedLimits:
    stock_limit: int
    gap_limit: int


def _resolve_limits(mode: str, *, limit: int, stock_limit: int | None, gap_limit: int | None) -> ResolvedLimits:
    """Resolve legacy --limit without accidentally shrinking full-market gap repair."""
    raw_limit = max(0, int(limit or 0))
    if mode == "from-gaps":
        return ResolvedLimits(
            stock_limit=max(0, int(stock_limit or 0)),
            gap_limit=max(1, int(gap_limit or raw_limit or 20)),
        )
    return ResolvedLimits(
        stock_limit=max(0, int(stock_limit if stock_limit is not None else raw_limit)),
        gap_limit=max(1, int(gap_limit or 20)),
    )


def _gap_rows(source_engine, *, limit: int, dataset: str = "") -> list[dict[str, Any]]:
    where_dataset = "AND dataset = :dataset" if dataset else ""
    params: dict[str, Any] = {"limit": max(1, int(limit or 20))}
    if dataset:
        params["dataset"] = dataset
    with source_engine.begin() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT id, dataset, period, gap_start, gap_end
                FROM sys_data_gap
                WHERE provider = 'gj_qmt'
                  AND status IN ('PENDING', 'RETRYING')
                  AND (next_retry_at IS NULL OR next_retry_at <= NOW() OR status = 'PENDING')
                  {where_dataset}
                ORDER BY
                  CASE status WHEN 'PENDING' THEN 0 ELSE 1 END,
                  COALESCE(next_retry_at, created_at),
                  id
                LIMIT :limit
                """
            ),
            params,
        ).mappings().fetchall()
    return [dict(row) for row in rows]


def _update_gap_status(
    source_engine,
    *,
    gap_id: int,
    run_id: str | None,
    resolved: bool,
    message: str,
    dry_run: bool,
) -> str:
    if dry_run:
        return "DRY_RUN"
    if resolved:
        with source_engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE sys_data_gap
                    SET status = 'RESOLVED',
                        resolved_at = NOW(),
                        last_run_id = :run_id,
                        last_error = :message,
                        next_retry_at = NULL,
                        updated_at = NOW()
                    WHERE id = :gap_id
                    """
                ),
                {"gap_id": int(gap_id), "run_id": run_id, "message": message[:1000]},
            )
        return "RESOLVED"

    with source_engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE sys_data_gap
                SET status = 'PENDING',
                    retry_count = retry_count + 1,
                    last_run_id = :run_id,
                    last_error = :message,
                    next_retry_at = DATE_ADD(NOW(), INTERVAL 6 HOUR),
                    updated_at = NOW()
                WHERE id = :gap_id
                """
            ),
            {"gap_id": int(gap_id), "run_id": run_id, "message": message[:1000]},
        )
    return "PENDING"


def _print(payload: dict[str, Any], *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str))
        return
    print(json.dumps(payload, ensure_ascii=False, default=str))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill bulky Guojin QMT historical data into a local/off-production MySQL database."
    )
    parser.add_argument("mode", choices=["init", "daily", "minute", "from-gaps"])
    parser.add_argument("--local-url", default="", help="Override QMT_HISTORY_MYSQL_URL/MINUTE_MYSQL_URL.")
    parser.add_argument("--codes", default="", help="Comma-separated stock codes. Empty means si_all_code universe.")
    parser.add_argument("--limit", type=int, default=0, help="Legacy limiter: stocks in daily/minute, gaps in from-gaps.")
    parser.add_argument("--stock-limit", type=int, default=None, help="Limit stock universe. In from-gaps, default is full market.")
    parser.add_argument("--gap-limit", type=int, default=None, help="Limit sys_data_gap rows. Defaults to --limit in from-gaps.")
    parser.add_argument("--start-date", default="", help="YYYY-MM-DD or YYYYMMDD.")
    parser.add_argument("--end-date", default="", help="YYYY-MM-DD or YYYYMMDD.")
    parser.add_argument("--trade-date", default="", help="One trading date for minute mode.")
    parser.add_argument("--batch-size", type=int, default=80)
    parser.add_argument("--dividend-type", default="front", choices=["none", "front", "back", "qfq", "hfq"])
    parser.add_argument("--gap-dataset", default="", choices=["", "sm_stock_kline.1d", "sm_stock_minute.1m"])
    parser.add_argument("--apply", action="store_true", help="Actually write rows and update sys_data_gap. Default is dry-run.")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    source_engine = _source_engine()
    local_engine = get_local_history_engine(args.local_url or None)
    ensure_local_history_tables(local_engine)
    limits = _resolve_limits(args.mode, limit=args.limit, stock_limit=args.stock_limit, gap_limit=args.gap_limit)

    if args.mode == "init":
        _print(
            {
                "status": "ok",
                "mode": "init",
                "local_database": str(local_engine.url.database or ""),
                "tables": ["qmt_local_stock_kline", "qmt_local_stock_minute", "qmt_local_backfill_run"],
            },
            as_json=args.json,
        )
        return 0

    codes = _codes_from_arg(source_engine, args.codes, limit=limits.stock_limit)
    if not codes:
        raise RuntimeError("No stock codes available for QMT local history backfill")

    dry_run = not args.apply
    if args.mode == "daily":
        if not args.start_date or not args.end_date:
            raise RuntimeError("daily mode requires --start-date and --end-date")
        result = backfill_daily_kline_local(
            source_engine=source_engine,
            local_engine=local_engine,
            stock_codes=codes,
            start_date=args.start_date,
            end_date=args.end_date,
            batch_size=max(1, args.batch_size),
            dividend_type=args.dividend_type,
            dry_run=dry_run,
        )
        payload = result_dict(result)
        payload["dry_run"] = dry_run
        _print(payload, as_json=args.json)
        return 0 if result.status == "SUCCESS" else 2

    if args.mode == "minute":
        trade_dates = [args.trade_date] if args.trade_date else load_trade_dates(
            source_engine,
            start_date=args.start_date,
            end_date=args.end_date,
            limit=0,
        )
        if not trade_dates:
            raise RuntimeError("minute mode requires --trade-date, or parseable --start-date/--end-date")
        result = backfill_minute_local(
            source_engine=source_engine,
            local_engine=local_engine,
            stock_codes=codes,
            trade_dates=trade_dates,
            batch_size=max(1, args.batch_size),
            dry_run=dry_run,
        )
        payload = result_dict(result)
        payload["dry_run"] = dry_run
        _print(payload, as_json=args.json)
        return 0 if result.status == "SUCCESS" else 2

    lock_path = ROOT / "data" / "runtime" / "qmt_local_gap_repair.lock"
    acquired, owner = (True, "")
    if args.apply:
        acquired, owner = _acquire_lock(lock_path)
    if not acquired:
        _print(
            {
                "status": "already_running",
                "mode": "from-gaps",
                "dry_run": dry_run,
                "lock_path": str(lock_path),
                "owner": owner,
            },
            as_json=args.json,
        )
        return 0

    gaps = _gap_rows(source_engine, limit=limits.gap_limit, dataset=args.gap_dataset)
    results: list[dict[str, Any]] = []
    try:
        for gap in gaps:
            dataset = str(gap["dataset"])
            start_date = str(gap["gap_start"])[:10]
            end_date = str(gap["gap_end"])[:10]
            try:
                if dataset == "sm_stock_kline.1d":
                    result = backfill_daily_kline_local(
                        source_engine=source_engine,
                        local_engine=local_engine,
                        stock_codes=codes,
                        start_date=start_date,
                        end_date=end_date,
                        batch_size=max(1, args.batch_size),
                        dividend_type=args.dividend_type,
                        dry_run=dry_run,
                    )
                elif dataset == "sm_stock_minute.1m":
                    result = backfill_minute_local(
                        source_engine=source_engine,
                        local_engine=local_engine,
                        stock_codes=codes,
                        trade_dates=[start_date],
                        batch_size=max(1, min(args.batch_size, 80)),
                        dry_run=dry_run,
                    )
                else:
                    results.append(
                        {
                            "gap_id": gap["id"],
                            "dataset": dataset,
                            "status": "skipped",
                            "reason": "unsupported_dataset",
                        }
                    )
                    continue

                item = result_dict(result)
                item["gap_id"] = gap["id"]
                resolved = result.status == "SUCCESS" and (dry_run or result.written_rows > 0)
                message = (
                    f"backfill {result.status}: fetched={result.fetched_rows}, "
                    f"written={result.written_rows}, dry_run={dry_run}"
                )
                item["gap_status_update"] = _update_gap_status(
                    source_engine,
                    gap_id=int(gap["id"]),
                    run_id=result.run_id,
                    resolved=resolved,
                    message=message,
                    dry_run=dry_run,
                )
                results.append(item)
            except Exception as exc:
                update_status = _update_gap_status(
                    source_engine,
                    gap_id=int(gap["id"]),
                    run_id=None,
                    resolved=False,
                    message=f"backfill failed: {exc}",
                    dry_run=dry_run,
                )
                results.append(
                    {
                        "gap_id": gap["id"],
                        "dataset": dataset,
                        "status": "failed",
                        "error": str(exc),
                        "gap_status_update": update_status,
                    }
                )
    finally:
        if args.apply:
            _release_lock(lock_path)

    failed_count = sum(1 for item in results if item.get("status") == "failed")
    _print(
        {
            "status": "ok" if failed_count == 0 else "partial_failed",
            "mode": "from-gaps",
            "dry_run": dry_run,
            "stock_count": len(codes),
            "stock_limit": limits.stock_limit,
            "gap_limit": limits.gap_limit,
            "gap_count": len(gaps),
            "executed": len(results),
            "resolved": sum(1 for item in results if item.get("gap_status_update") == "RESOLVED"),
            "failed": failed_count,
            "results": results,
        },
        as_json=args.json,
    )
    return 0 if failed_count == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
