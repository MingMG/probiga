#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from server.trading_v2.calendar import is_trade_day
from server.trading_v2.reconciliation import reconcile_account
from tools.env_config import load_project_env


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trade-date", default="")
    parser.add_argument("--account-id", default="paper-main-v2")
    args = parser.parse_args()
    load_project_env()
    engine = create_batch_engine()
    target = date.fromisoformat(args.trade_date) if args.trade_date else date.today()
    if not args.trade_date and not is_trade_day(engine, target):
        print(json.dumps({
            "status": "skipped_non_trade_day",
            "trade_date": target.isoformat(),
        }, ensure_ascii=False, indent=2))
        return 0
    result = reconcile_account(
        engine,
        account_id=args.account_id,
        trade_date=target,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return (
        0
        if result["status"] in {"PASS", "SKIPPED_BEFORE_ACCOUNT_OPEN"}
        else 2
    )


if __name__ == "__main__":
    raise SystemExit(main())
