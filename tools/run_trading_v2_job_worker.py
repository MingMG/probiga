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
from server.trading_v2.job_worker import run_one_job
from tools.env_config import load_project_env


def main() -> int:
    load_project_env()
    engine = create_batch_engine()
    try:
        result = run_one_job(engine)
        if result["status"] == "idle":
            from server.engine.strategy_execution_adapters import bootstrap_strategy_execution_adapter_registry
            from server.engine.autonomous_strategy_research import research_tick
            bootstrap_strategy_execution_adapter_registry(registry_rows=())
            result["autonomous_research"] = research_tick(engine)
            if result["autonomous_research"]["status"] in {"COMPLETE", "BLOCKED", "FAILED"}:
                from server.engine.strategy_native_backtest import native_tick
                result["native_backtest"] = native_tick(engine)
    finally:
        engine.dispose()
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0 if result["status"] in {"idle", "COMPLETED"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
