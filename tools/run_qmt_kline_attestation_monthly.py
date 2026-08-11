#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Run row-level BigQMT daily-bar attestation in bounded monthly transactions."""
from __future__ import annotations

import argparse
import json
import sys
from calendar import monthrange
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterator

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.bigqmt.spool import PROVIDER_ID
from server.common.batch_db import create_batch_engine
from tools.attest_qmt_daily_kline import attest_range, ensure_attestation_tables
from tools.env_config import load_project_env


def _parse_date(value: str) -> date:
    return datetime.strptime(str(value).strip()[:10], "%Y-%m-%d").date()


def month_ranges(start_date: str, end_date: str) -> Iterator[tuple[str, str]]:
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    if start > end:
        raise ValueError("start_date must not be after end_date")
    cursor = start
    while cursor <= end:
        month_end = date(
            cursor.year,
            cursor.month,
            monthrange(cursor.year, cursor.month)[1],
        )
        bounded_end = min(month_end, end)
        yield cursor.isoformat(), bounded_end.isoformat()
        cursor = bounded_end.fromordinal(bounded_end.toordinal() + 1)


def _completed_month(
    engine,
    *,
    provider: str,
    start_date: str,
    end_date: str,
) -> dict[str, Any] | None:
    with engine.begin() as connection:
        row = connection.execute(
            text(
                """
                SELECT run_id, status, target_rows, qmt_rows, matched_rows,
                       missing_qmt_rows, mismatched_rows,
                       already_attested_rows, updated_rows, finished_at
                FROM qmt_kline_attestation_run
                WHERE provider=:provider
                  AND start_date=:start_date
                  AND end_date=:end_date
                  AND status IN ('COMPLETED', 'PARTIAL')
                  AND updated_rows + already_attested_rows >= matched_rows
                ORDER BY finished_at DESC
                LIMIT 1
                """
            ),
            {
                "provider": provider,
                "start_date": start_date,
                "end_date": end_date,
            },
        ).mappings().first()
    return dict(row) if row else None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--provider", default=PROVIDER_ID)
    parser.add_argument("--mismatch-sample-limit", type=int, default=5000)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    load_project_env()
    engine = create_batch_engine(future=True)
    ensure_attestation_tables(engine)
    monthly_results: list[dict[str, Any]] = []
    failed = False
    try:
        for month_start, month_end in month_ranges(args.start_date, args.end_date):
            if args.apply and args.resume:
                previous = _completed_month(
                    engine,
                    provider=args.provider,
                    start_date=month_start,
                    end_date=month_end,
                )
                if previous:
                    result = {
                        **previous,
                        "start_date": month_start,
                        "end_date": month_end,
                        "skipped": True,
                    }
                    monthly_results.append(result)
                    print(
                        json.dumps(
                            {"event": "month_skipped", **result},
                            ensure_ascii=False,
                            default=str,
                        ),
                        flush=True,
                    )
                    continue
            try:
                result = attest_range(
                    engine,
                    start_date=month_start,
                    end_date=month_end,
                    apply=args.apply,
                    provider=args.provider,
                    mismatch_sample_limit=args.mismatch_sample_limit,
                )
                result["skipped"] = False
                monthly_results.append(result)
                print(
                    json.dumps(
                        {"event": "month_completed", **result},
                        ensure_ascii=False,
                        default=str,
                    ),
                    flush=True,
                )
            except Exception as exc:
                failed = True
                result = {
                    "start_date": month_start,
                    "end_date": month_end,
                    "status": "FAILED",
                    "error": str(exc),
                    "skipped": False,
                }
                monthly_results.append(result)
                print(
                    json.dumps(
                        {"event": "month_failed", **result},
                        ensure_ascii=False,
                        default=str,
                    ),
                    flush=True,
                )
                break
    finally:
        engine.dispose()

    numeric_fields = (
        "target_rows",
        "qmt_rows",
        "matched_rows",
        "missing_qmt_rows",
        "mismatched_rows",
        "already_attested_rows",
        "updated_rows",
    )
    summary: dict[str, Any] = {
        "status": "FAILED" if failed else "SUCCESS",
        "apply": bool(args.apply),
        "provider": args.provider,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "month_count": len(monthly_results),
        "skipped_months": sum(bool(item.get("skipped")) for item in monthly_results),
    }
    for field in numeric_fields:
        summary[field] = sum(int(item.get(field) or 0) for item in monthly_results)
    if args.json:
        summary["months"] = monthly_results
    print(
        json.dumps(
            {"event": "summary", **summary},
            ensure_ascii=False,
            default=str,
        ),
        flush=True,
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
