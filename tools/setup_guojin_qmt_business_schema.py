from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from sqlalchemy import create_engine

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.qmt.business_schema import migrate_qmt_business_tables, qmt_business_tables, result_dicts
from server.common.config import get_mysql_url


LARGE_MARKET_TABLES = {"sm_stock_kline", "sm_stock_minute"}


def main() -> int:
    parser = argparse.ArgumentParser(description="Add Guojin QMT provenance columns to existing business tables.")
    parser.add_argument("--apply", action="store_true", help="Apply ALTER TABLE changes. Default is dry-run.")
    parser.add_argument(
        "--include-large-market-tables",
        action="store_true",
        help="When applying all tables, also include sm_stock_kline and sm_stock_minute. These can lock on MySQL 5.5.",
    )
    parser.add_argument("--tables", nargs="*", default=None, help="Optional table names. Default comes from QMT catalog targets.")
    args = parser.parse_args()

    engine = create_engine(get_mysql_url(required=True), pool_pre_ping=True, future=True)
    skipped_large_tables: list[str] = []
    if args.tables:
        tables = args.tables
    else:
        tables = qmt_business_tables()
        if args.apply and not args.include_large_market_tables:
            skipped_large_tables = sorted(table for table in tables if table in LARGE_MARKET_TABLES)
            tables = [table for table in tables if table not in LARGE_MARKET_TABLES]
    results = migrate_qmt_business_tables(engine, tables=tables, dry_run=not args.apply)
    payload = {
        "mode": "apply" if args.apply else "dry_run",
        "table_count": len(results),
        "migrated_or_pending": sum(1 for item in results if item.status in {"MIGRATED", "DRY_RUN"}),
        "unchanged": sum(1 for item in results if item.status == "UNCHANGED"),
        "missing": sum(1 for item in results if item.status == "SKIPPED_TABLE_MISSING"),
        "errors": sum(1 for item in results if item.status == "ERROR"),
        "skipped_large_tables": skipped_large_tables,
        "results": result_dicts(results),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if payload["errors"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
