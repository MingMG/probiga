# -*- coding: utf-8 -*-
"""Run one QMT full-market anomaly radar scan.

Examples:
    .venv\\Scripts\\python.exe tools\\run_market_radar.py --once
    .venv\\Scripts\\python.exe tools\\run_market_radar.py --loop
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from biz.market_radar.core import get_shared_radar_engine
from server.common.batch_db import create_batch_engine
from server.common.config import get_market_radar_runtime_config


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one or continuously run the QMT market radar")
    parser.add_argument("--once", action="store_true", help="scan once and exit")
    parser.add_argument("--loop", action="store_true", help="keep scanning with configured interval")
    args = parser.parse_args()
    loop = bool(args.loop)
    if not args.once and not args.loop:
        loop = False

    engine = get_shared_radar_engine(create_batch_engine())
    config = get_market_radar_runtime_config()
    while True:
        result = engine.scan_once()
        print(json.dumps(result, ensure_ascii=False, default=str))
        if not loop:
            return 0
        time.sleep(float(config["poll_seconds"]))


if __name__ == "__main__":
    raise SystemExit(main())
