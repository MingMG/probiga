#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Capture the 08:30 external market and revise strategy governance scores."""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from biz.analysis.sync_analysis_fast import previous_trade_date
from biz.market_context.external_market import (
    fetch_external_market_snapshot,
    store_external_market_snapshot,
)
from server.common.authoritative_market_clock import PRODUCTION_TIMEZONE
from server.common.batch_db import create_batch_engine
from server.engine.strategy_center import external_market_score_adjustment
from tools.run_strategy_governance_daily import (
    _load_project_env,
    run_daily_governance,
)


def _now_shanghai_naive() -> datetime:
    return datetime.now(PRODUCTION_TIMEZONE).replace(
        tzinfo=None,
        microsecond=0,
    )


def _neutral_external_context(reason: str) -> dict:
    return {
        "snapshot_id": "",
        "captured_at": _now_shanghai_naive().isoformat(sep=" "),
        "external_market_status": "UNKNOWN",
        "external_market_score": 50.0,
        "external_market_data_quality": "UNKNOWN",
        "available_count": 0,
        "expected_count": 0,
        "source_warnings": [str(reason or "external market unavailable")[:300]],
    }


def capture_external_context(engine) -> dict:
    """Make one bounded fetch attempt; any failure becomes a neutral factor."""

    try:
        snapshot = fetch_external_market_snapshot(as_of=_now_shanghai_naive())
    except Exception as exc:
        logging.warning(
            "External market capture failed; continuing neutral: %s",
            type(exc).__name__,
        )
        return _neutral_external_context(type(exc).__name__)
    try:
        return store_external_market_snapshot(engine, snapshot)
    except Exception as exc:
        logging.warning(
            "External market storage failed; continuing neutral: %s",
            type(exc).__name__,
        )
        return _neutral_external_context(f"storage:{type(exc).__name__}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="08:30外围市场评分修正并更新正式策略治理票池"
    )
    parser.add_argument("--date", default="", help="默认使用上一完整交易日")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    _load_project_env()
    engine = create_batch_engine()
    try:
        now = _now_shanghai_naive()
        target = str(args.date or "").strip()[:10] or previous_trade_date(
            engine,
            now.isoformat(sep=" "),
        )
        external_context = capture_external_context(engine)
    finally:
        engine.dispose()

    build_sha = str(
        os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA") or ""
    ).strip().lower()
    if build_sha and re.fullmatch(r"[0-9a-f]{40}", build_sha) is None:
        raise RuntimeError("scheduler build SHA is invalid")
    governance, process_exit = run_daily_governance(
        requested_trade_date=target,
        strategy_limit=500,
        expected_build_sha=build_sha,
        external_market_context=external_context,
    )
    completed = (
        process_exit == 0
        and governance.get("status") == "ok"
        and governance.get("orchestration_status") == "COMPLETED"
        and governance.get("automatic_real_order_submission") is False
        and governance.get("real_order_authority") is False
    )
    payload = {
        "status": "ok" if completed else "blocked",
        "trade_date": target,
        "external_market": external_context,
        "score_adjustment": external_market_score_adjustment(
            external_context
        ),
        "governance": governance,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    print(json.dumps(payload, ensure_ascii=False, default=str))
    return 0 if completed else (process_exit or 2)


if __name__ == "__main__":
    raise SystemExit(main())
