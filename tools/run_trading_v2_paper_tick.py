#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from server.trading_v2.paper_tick import run_paper_tick
from tools.env_config import load_project_env


def main() -> int:
    load_project_env()
    result = run_paper_tick(create_batch_engine())
    print(json.dumps(result, ensure_ascii=False, default=str))
    return (
        0
        if result["status"] in {"idle", "market_closed", "completed"}
        else 2
    )


if __name__ == "__main__":
    raise SystemExit(main())
