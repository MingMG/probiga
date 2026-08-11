#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Run one paper-only V2 intraday activation tick."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.trading_v2.intraday_activation import run_intraday_activation
from server.trading_v3.config import load_v3_config
from server.trading_v3.intraday_hypotheses import (
    update_intraday_hypotheses,
)
from tools.env_config import create_tool_engine, load_project_env


def main() -> int:
    load_project_env()
    routes = dict(load_v3_config().get("production_routes") or {})
    if (
        routes.get("decision_engine") == "V3_ONLY"
        and not routes.get("legacy_v2_entry_enabled", False)
    ):
        print(json.dumps({
            "status": "skipped",
            "reason": "V3_ONLY_ROUTE",
            "v3_hypothesis_update": {
                "status": "blocked",
                "reason": "VALIDATED_V3_INTRADAY_MODEL_MISSING",
                "updated_count": 0,
            },
            "real_order_count": 0,
        }, ensure_ascii=False, sort_keys=True))
        return 0
    engine = create_tool_engine()
    try:
        result = run_intraday_activation(engine)
        result["v3_hypothesis_update"] = (
            update_intraday_hypotheses(
                engine,
                intraday_result=result,
            )
        )
    finally:
        engine.dispose()
    print(
        json.dumps(
            result,
            ensure_ascii=False,
            sort_keys=True,
            default=str,
        )
    )
    return 0 if result.get("status") not in {"failed"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
