#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, time
from pathlib import Path

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from server.trading_v2.calendar import is_trade_day
from server.trading_v2.decision_worker import run_daily_decision
from server.trading_v3.config import load_v3_config
from tools.env_config import load_project_env


def _previous_trade_day(engine, current_date: date) -> date:
    with engine.connect() as connection:
        value = connection.execute(
            text(
                """
                SELECT MAX(trade_date) FROM si_trade_calendar
                WHERE trade_status = 1 AND trade_date < :current_date
                """
            ),
            {"current_date": current_date},
        ).scalar()
    if value is None:
        raise RuntimeError("trade calendar has no previous trade day")
    return (
        value
        if isinstance(value, date)
        else date.fromisoformat(str(value)[:10])
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trade-date", default="")
    parser.add_argument(
        "--mode",
        choices=("close", "premarket"),
        default="close",
    )
    parser.add_argument("--limit", type=int, default=500)
    args = parser.parse_args()
    load_project_env()
    routes = dict(load_v3_config().get("production_routes") or {})
    if (
        routes.get("decision_engine") == "V3_ONLY"
        and not routes.get("legacy_v2_entry_enabled", False)
    ):
        print(json.dumps({
            "status": "skipped",
            "reason": "V3_ONLY_ROUTE",
            "mode": args.mode,
            "real_order_count": 0,
        }, ensure_ascii=False, indent=2))
        return 0
    engine = create_batch_engine()
    today = date.today()
    if not args.trade_date and not is_trade_day(engine, today):
        print(json.dumps({
            "status": "skipped_non_trade_day",
            "trade_date": today.isoformat(),
            "mode": args.mode,
            "real_order_count": 0,
        }, ensure_ascii=False, indent=2))
        return 0
    if args.trade_date:
        target = date.fromisoformat(args.trade_date)
    elif args.mode == "premarket":
        target = _previous_trade_day(engine, today)
    else:
        target = today
    if args.mode == "premarket":
        decision_at = datetime.combine(today, time(9, 20))
    elif target == today:
        # A post-close decision may only consume facts that had actually
        # arrived when the worker ran.  Persist the real run time instead of
        # pretending late QMT evidence was already available at 15:20.
        decision_at = datetime.now().replace(microsecond=0)
    else:
        decision_at = datetime.combine(target, time(15, 20))
    result = run_daily_decision(
        engine,
        trade_date=target.isoformat(),
        decision_at=decision_at,
        limit=max(1, min(500, args.limit)),
    )
    result["mode"] = args.mode
    result["source_trade_date"] = target.isoformat()
    result["decision_at"] = decision_at.isoformat(sep=" ")
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result.get("status") in {"ok", "idempotent_hit"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
