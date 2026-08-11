# -*- coding: utf-8 -*-
"""Run ProBigA schema migrations.

Usage:
  python tools/migrate.py --dry-run
  python tools/migrate.py --json
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
from server.db.migrations import (
    run_migrations,
    run_portfolio_collation_migration,
    summarize_results,
)


def get_engine():
    return create_batch_engine()


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply idempotent ProBigA schema migrations")
    parser.add_argument("--dry-run", action="store_true", help="show planned changes without applying them")
    parser.add_argument("--json", action="store_true", help="emit JSON")
    parser.add_argument(
        "--scope",
        choices=("all", "portfolio-collation"),
        default="all",
        help="limit execution to one guarded migration scope",
    )
    args = parser.parse_args()

    runner = run_portfolio_collation_migration if args.scope == "portfolio-collation" else run_migrations
    results = runner(get_engine(), dry_run=args.dry_run)
    payload = {
        "status": "ok",
        "dry_run": bool(args.dry_run),
        "scope": args.scope,
        "summary": summarize_results(results),
        "results": [item.as_dict() for item in results],
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"status={payload['status']} dry_run={payload['dry_run']} summary={payload['summary']}")
        for item in results:
            print(f"{item.table}.{item.column}: {item.status} - {item.message}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
