#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Repair stale V2 backtest rows whose owning jobs already failed."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.trading_v2.job_worker import repair_orphaned_backtests
from tools.env_config import create_tool_engine, load_project_env


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stale-minutes",
        type=int,
        default=15,
        help="Only repair RUNNING rows at least this old.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Apply repairs. Without this flag the command is read-only.",
    )
    args = parser.parse_args()
    if not args.execute:
        print(
            json.dumps(
                {
                    "status": "dry_run",
                    "message": "rerun with --execute to repair rows",
                    "stale_minutes": max(1, args.stale_minutes),
                },
                ensure_ascii=False,
            )
        )
        return 0

    load_project_env()
    engine = create_tool_engine()
    try:
        repaired = repair_orphaned_backtests(
            engine,
            stale_after_minutes=args.stale_minutes,
        )
    finally:
        engine.dispose()
    print(
        json.dumps(
            {
                "status": "success",
                "repaired_count": len(repaired),
                "repaired": repaired,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
