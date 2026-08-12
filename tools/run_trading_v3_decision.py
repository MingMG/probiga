#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.kline_data import get_kline_engine
from server.trading_v3.decision_worker import run_daily_decision_v3
from tools.env_config import create_tool_engine, load_project_env


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--as-of", default="")
    parser.add_argument(
        "--mode",
        choices=("close", "premarket", "manual"),
        default="close",
    )
    parser.add_argument("--universe-limit", type=int, default=5000)
    parser.add_argument("--per-sleeve-limit", type=int, default=5000)
    args = parser.parse_args()
    load_project_env()
    primary = create_tool_engine()
    kline = get_kline_engine()
    try:
        result = run_daily_decision_v3(
            primary,
            as_of=(
                date.fromisoformat(args.as_of)
                if args.as_of
                else date.today()
            ),
            decision_at=datetime.now().replace(microsecond=0),
            mode=args.mode,
            universe_limit=max(100, min(args.universe_limit, 5000)),
            per_sleeve_limit=max(
                50,
                min(args.per_sleeve_limit, 5000),
            ),
            kline_engine=kline,
        )
        from biz.analysis.trading_wecom import notify_v3_decision_result
        result["notification"] = notify_v3_decision_result(result)
    finally:
        primary.dispose()
        kline.dispose()
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
