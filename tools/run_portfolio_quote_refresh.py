#!/usr/bin/env python3
"""Refresh the user watchlist from an audited Sina/Tencent quorum."""
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
from server.trading_v2.config import load_frozen_json
from server.trading_v2.public_quote_failover import (
    collect_portfolio_quote_refresh,
)
from tools.env_config import load_project_env


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--force",
        action="store_true",
        help="Collect even outside the configured trading window.",
    )
    parser.add_argument("--lock-timeout-seconds", type=int, default=0)
    args = parser.parse_args()
    load_project_env()
    config, _ = load_frozen_json("strategies/intraday_activation_v2.json")
    engine = create_batch_engine(future=True)
    try:
        result = collect_portfolio_quote_refresh(
            engine,
            now=datetime.now(),
            config=config.get("public_quote_failover") or {},
            force=bool(args.force),
            lock_timeout_seconds=max(0, args.lock_timeout_seconds),
        )
    finally:
        engine.dispose()
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result.get("status") not in {"blocked"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
