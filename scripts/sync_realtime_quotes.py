#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Intraday realtime quote sync.

This is a thin, scheduler-friendly wrapper around tools.crawl_realtime_batch.
It refreshes the latest full-market quote table and appends the same batch to
sm_rt_quote_snapshot, giving intraday monitors a durable minute-level stream.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from tools.crawl_realtime_batch import is_trading_time, refresh_snapshot


def sync_realtime(*, min_coverage: float = 0.70, skip_closed: bool = True) -> dict:
    engine = create_batch_engine(future=True)
    if skip_closed and not is_trading_time(engine):
        return {
            "status": "skipped",
            "reason": "market_closed",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
        }

    count = refresh_snapshot(
        engine,
        min_coverage=min_coverage,
        archive_snapshot=True,
    )
    return {
        "status": "success",
        "snapshot_count": count,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync full-market realtime quotes")
    parser.add_argument("--min-coverage", type=float, default=0.70)
    parser.add_argument("--no-skip-closed", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    result = sync_realtime(
        min_coverage=args.min_coverage,
        skip_closed=not args.no_skip_closed,
    )
    if args.json:
        print(json.dumps(result, ensure_ascii=False, default=str))
    else:
        print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
