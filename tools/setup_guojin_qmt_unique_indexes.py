from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from sqlalchemy import create_engine

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.qmt.unique_index import ensure_qmt_unique_indexes, result_dicts
from server.common.config import get_mysql_url


def main() -> int:
    parser = argparse.ArgumentParser(description="Create safe unique indexes required by Guojin QMT upserts.")
    parser.add_argument("--apply", action="store_true", help="Create indexes. Default is dry-run.")
    args = parser.parse_args()

    engine = create_engine(get_mysql_url(required=True), pool_pre_ping=True, future=True)
    results = ensure_qmt_unique_indexes(engine, dry_run=not args.apply)
    payload = {
        "mode": "apply" if args.apply else "dry_run",
        "created_or_pending": sum(1 for item in results if item.status in {"CREATED", "DRY_RUN"}),
        "unchanged": sum(1 for item in results if item.status == "UNCHANGED"),
        "blocked": sum(1 for item in results if item.status == "BLOCKED_DUPLICATES"),
        "errors": sum(1 for item in results if item.status == "ERROR"),
        "results": result_dicts(results),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if payload["errors"] == 0 and payload["blocked"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
