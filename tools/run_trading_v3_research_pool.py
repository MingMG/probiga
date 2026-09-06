#!/usr/bin/env python3
"""Publish the latest completed session's research observations for the UI."""
from __future__ import annotations

import argparse
from datetime import date, datetime, time
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.authoritative_market_clock import (
    DAILY_CLOSE_READY_TIME,
    PRODUCTION_TIMEZONE,
    authoritative_closed_trade_date,
)
from server.common.kline_data import get_kline_engine
from server.trading_v3.decision_worker import run_retrospective_research_v3
from server.trading_v3.research_pool import publish_research_pool, read_research_pool
from server.trading_v3.versioning import code_version
from tools.env_config import create_tool_engine, load_project_env


def generate_research_pool(engine, *, kline_engine, now: datetime | None = None) -> dict:
    current = now or datetime.now(PRODUCTION_TIMEZONE)
    if current.tzinfo is not None:
        current = current.astimezone(PRODUCTION_TIMEZONE)
    known_at = current.replace(tzinfo=None, microsecond=0)
    target = date.fromisoformat(authoritative_closed_trade_date(engine, now=current))
    cutoff = datetime.combine(
        target, DAILY_CLOSE_READY_TIME if target == known_at.date() else time.max
    )
    if cutoff >= known_at:
        raise ValueError("Research observations require a completed session")
    result = run_retrospective_research_v3(
        engine,
        as_of=target,
        decision_at=cutoff,
        research_known_at=known_at,
        mode="close",
        kline_engine=kline_engine,
        universe_limit=1200,
        per_sleeve_limit=300,
        resolve_fact_cutoff_from_evidence=True,
    )
    result["notification"] = {"status": "suppressed", "reason": "RETROSPECTIVE_RESEARCH"}
    publication = publish_research_pool(
        result,
        publisher_build_sha=code_version()[0],
    )
    published_pool = read_research_pool(target)
    if (
        published_pool.get("pool_readable") is not True
        or published_pool.get("status") not in {"READY", "EMPTY"}
        or published_pool.get("trade_date") != target.isoformat()
        or published_pool.get("artifact_sha256")
        != publication.get("artifact_sha256")
        or published_pool.get("payload_file_sha256")
        != publication.get("payload_file_sha256")
    ):
        raise RuntimeError("Published research pool failed exact readback")
    summary = dict(published_pool.get("summary") or {})
    observation_count = summary.get("observation_stock_count")
    if (
        not isinstance(observation_count, int)
        or isinstance(observation_count, bool)
        or observation_count < 0
    ):
        raise RuntimeError("Published research pool readback summary is invalid")
    if observation_count == 0:
        raise RuntimeError(
            "NO_RESEARCH_OBSERVATION_CANDIDATES: "
            f"total_forecast_count={summary.get('total_forecast_count')!r}, "
            f"excluded_forecast_count={summary.get('excluded_forecast_count')!r}"
        )
    return {
        "schema": "probiga.trading-v3-research-pool-task.v1",
        "status": "completed",
        "trade_date": target.isoformat(),
        "research_known_at": known_at.isoformat(sep=" "),
        "database_writes": False,
        "order_authority": False,
        "notification_eligible": False,
        "publication": publication,
        "readback": {
            "status": published_pool["status"],
            "artifact_sha256": published_pool["artifact_sha256"],
            "payload_file_sha256": published_pool["payload_file_sha256"],
            "summary": summary,
        },
    }


def main() -> int:
    argparse.ArgumentParser(description=__doc__).parse_args()
    load_project_env()
    primary = create_tool_engine()
    kline = get_kline_engine()
    try:
        result = generate_research_pool(primary, kline_engine=kline)
    finally:
        primary.dispose()
        if kline is not primary:
            kline.dispose()
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
