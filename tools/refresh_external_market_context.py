#!/usr/bin/env python3
"""Fetch and persist one point-in-time external-market snapshot."""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from biz.market_context.external_market import (
    PREMARKET_EXTERNAL_CAPTURE_TIME,
    PREMARKET_EXTERNAL_SNAPSHOT_SCHEMA,
    _external_premarket_persisted_proof,
    fetch_external_market_snapshot,
    store_external_market_snapshot,
)
from sqlalchemy import text
from server.common.authoritative_market_clock import (
    PRODUCTION_TIMEZONE, STRATEGY_POOL_PREMARKET_REVISION_TIME,
)
from tools.env_config import create_tool_engine, load_project_env


def _now_shanghai() -> datetime:
    return datetime.now(PRODUCTION_TIMEZONE).replace(tzinfo=None)


def _capture_premarket_external_snapshot(engine) -> dict:
    started = _now_shanghai()
    session = started.date().isoformat()
    cutoff = datetime.combine(started.date(), STRATEGY_POOL_PREMARKET_REVISION_TIME)
    result = {
        "schema": PREMARKET_EXTERNAL_SNAPSHOT_SCHEMA,
        "status": "FAILED",
        "execution_session_date": session,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
        "canonical_recommendation_writes": False,
    }
    if not PREMARKET_EXTERNAL_CAPTURE_TIME <= started.time() < cutoff.time():
        return {**result, "reason_code": "EXTERNAL_MARKET_CAPTURE_WINDOW_CLOSED"}

    stage = "CALENDAR"
    try:
        with engine.connect() as connection:
            is_open = connection.execute(text(
                "SELECT trade_status FROM si_trade_calendar WHERE trade_date=:session"
            ), {"session": session}).scalar_one_or_none()
        if is_open != 1:
            reason = "EXTERNAL_MARKET_EXCHANGE_CLOSED" if is_open == 0 else "EXTERNAL_MARKET_CALENDAR_UNAVAILABLE"
            return {**result, "reason_code": reason}

        stage = "SOURCE"
        snapshot = fetch_external_market_snapshot(as_of=started)
        observed_at = _now_shanghai()
        if not started <= observed_at < cutoff:
            return {**result, "reason_code": "EXTERNAL_MARKET_SOURCE_DEADLINE"}
        if int(snapshot.get("available_count") or 0) <= 0:
            return {**result, "reason_code": "EXTERNAL_MARKET_SOURCE_EMPTY"}
        captured_at = observed_at.replace(microsecond=0)
        snapshot = {**snapshot, "context_date": session, "captured_at": captured_at}

        stage = "STORAGE"
        report = store_external_market_snapshot(engine, snapshot)
        report["source_warnings"] = [str(item)[:300] for item in report["source_warnings"][:20]]
        result["snapshot_id"] = report["snapshot_id"]
        stage = "READBACK"
        digest, missing = _external_premarket_persisted_proof(
            engine, snapshot_id=report["snapshot_id"], captured_at=captured_at,
        )
        stored_at = _now_shanghai()
        if not observed_at <= stored_at <= cutoff:
            return {**result, "reason_code": "EXTERNAL_MARKET_STORAGE_DEADLINE"}
        if missing != report["missing_symbols"]:
            return {**result, "reason_code": "EXTERNAL_MARKET_READBACK_COVERAGE_DIFFERS"}
        return {
            **result, "status": "COMPLETE",
            "captured_at": observed_at.isoformat(),
            "stored_at": stored_at.isoformat(),
            "persisted_sha256": digest,
            "external_market": report,
        }
    except Exception as exc:
        return {**result, "reason_code": f"EXTERNAL_MARKET_{stage}_FAILED", "detail": type(exc).__name__}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument("--retry-delay-seconds", type=float, default=5.0)
    parser.add_argument("--premarket", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    if args.premarket and (args.attempts != 1 or not args.json):
        parser.error("premarket requires one attempt and JSON output")
    load_project_env()
    if args.premarket:
        engine = create_tool_engine(
            connect_args={"connect_timeout": 8, "read_timeout": 10, "write_timeout": 10},
        )
        try:
            receipt = _capture_premarket_external_snapshot(engine)
        finally:
            engine.dispose()
        print(json.dumps(receipt, ensure_ascii=False, default=str))
        return 0 if receipt["status"] == "COMPLETE" else 2
    attempts = max(1, int(args.attempts))
    snapshot = None
    errors: list[str] = []
    for attempt in range(1, attempts + 1):
        try:
            snapshot = fetch_external_market_snapshot()
            if int(snapshot.get("available_count") or 0) > 0:
                break
            errors.append(f"attempt {attempt}: no available items")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"attempt {attempt}: {type(exc).__name__}: {exc}")
        if attempt < attempts:
            time.sleep(max(0.0, float(args.retry_delay_seconds)))
    if snapshot is None:
        raise RuntimeError("; ".join(errors) or "external snapshot unavailable")
    engine = create_tool_engine()
    try:
        report = store_external_market_snapshot(engine, snapshot)
    finally:
        engine.dispose()
    report["fetch_errors"] = errors
    if args.json:
        print(json.dumps(report, ensure_ascii=False, default=str))
    else:
        print(report)
    return 0 if int(report.get("available_count") or 0) > 0 else 3


if __name__ == "__main__":
    raise SystemExit(main())
