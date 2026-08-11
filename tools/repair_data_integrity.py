#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Repair canonical daily business-key duplicates before adding constraints.

Examples:
  python tools/repair_data_integrity.py --date 2026-07-16 --date 2026-07-17 --apply
  python tools/repair_data_integrity.py --all-daily-flow --apply
  python tools/repair_data_integrity.py --date 2026-07-17 --apply --add-unique-index
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from server.db.data_integrity import deduplicate_daily_flow, deduplicate_daily_kline
from server.db.migrations import run_migrations


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Repair daily data business-key duplicates safely.")
    parser.add_argument("--date", action="append", default=[], help="K-line trade date; repeatable")
    parser.add_argument("--start-date", default="", help="K-line repair range start")
    parser.add_argument("--end-date", default="", help="K-line repair range end")
    parser.add_argument("--all-daily-flow", action="store_true", help="deduplicate all daily capital-flow keys")
    parser.add_argument("--apply", action="store_true", help="delete older duplicate rows")
    parser.add_argument("--add-unique-index", action="store_true", help="run unique-key migrations after repair")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    if not args.date and not args.start_date and not args.end_date and not args.all_daily_flow:
        parser.error("specify --date, --start-date/--end-date, or --all-daily-flow")
    engine = create_batch_engine()
    results: list[dict] = []

    dates = sorted({str(value).strip()[:10] for value in args.date if str(value).strip()})
    if dates:
        for day in dates:
            if args.apply:
                result = deduplicate_daily_kline(engine, start_date=day, end_date=day, dry_run=False)
            else:
                result = deduplicate_daily_kline(engine, start_date=day, end_date=day, dry_run=True)
            result["scope"] = {"start_date": day, "end_date": day}
            results.append(result)
    elif args.start_date or args.end_date:
        results.append(
            deduplicate_daily_kline(
                engine,
                start_date=args.start_date.strip()[:10] or None,
                end_date=args.end_date.strip()[:10] or None,
                dry_run=not args.apply,
            )
        )

    if args.all_daily_flow:
        results.append(deduplicate_daily_flow(engine, dry_run=not args.apply))

    if args.add_unique_index:
        if not args.apply:
            parser.error("--add-unique-index requires --apply")
        migration_results = run_migrations(engine)
        results.extend(item.as_dict() for item in migration_results)

    payload = {"status": "ok", "apply": bool(args.apply), "results": results}
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    else:
        print(json.dumps(payload, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
