#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Apply the isolated V2 paper-trading schema migrations."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from server.db.migrations_v2 import run_v2_migrations
from tools.env_config import load_project_env


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument(
        "--allow-execution-evidence",
        action="store_true",
        help=(
            "explicitly allow pending V2 execution-evidence DDL; use only "
            "after dedicated MySQL review/acceptance"
        ),
    )
    args = parser.parse_args()
    load_project_env()
    results = run_v2_migrations(
        create_batch_engine(),
        dry_run=args.dry_run,
        allow_execution_evidence=args.allow_execution_evidence,
    )
    payload = {
        "status": "ok",
        "dry_run": args.dry_run,
        "allow_execution_evidence": args.allow_execution_evidence,
        "migrations": [item.as_dict() for item in results],
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        for item in results:
            print(f"{item.version}: {item.status} ({item.statement_count} statements)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
