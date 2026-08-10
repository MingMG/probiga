#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.kline_data import get_kline_engine
from server.trading_v3.counterfactual_worker import (
    drain_counterfactual_backlog,
)
from tools.env_config import create_tool_engine, load_project_env


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=10000)
    parser.add_argument("--max-batches", type=int, default=10)
    parser.add_argument(
        "--skip-rebuild-recall",
        action="store_true",
    )
    args = parser.parse_args()
    load_project_env()
    primary = create_tool_engine()
    kline = get_kline_engine()
    try:
        result = drain_counterfactual_backlog(
            primary,
            kline,
            batch_size=args.limit,
            max_batches=args.max_batches,
            rebuild_recall=not args.skip_rebuild_recall,
        )
    finally:
        primary.dispose()
        kline.dispose()
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
