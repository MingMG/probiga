#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from server.trading_v2.calendar import latest_trade_day
from server.trading_v2.health import run_strategy_health
from tools.env_config import load_project_env


def main() -> int:
    load_project_env()
    engine = create_batch_engine()
    trade_date = latest_trade_day(engine, date.today())
    result = run_strategy_health(engine, trade_date=trade_date)
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
