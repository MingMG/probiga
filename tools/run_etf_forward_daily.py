#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Run the formal BigQMT ETF close publisher and current-only research ledger."""
from __future__ import annotations

import argparse
from datetime import date, datetime, time
import hashlib
import json
from pathlib import Path
import sys
from typing import Any, Mapping
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.env_config import create_tool_engine, load_project_env
from tools.run_etf_forward_simulation import run_forward
from tools.sync_etf_bigqmt_daily import (
    PROVIDER_ID,
    resolve_expected_build_sha,
    run_sync,
)
from server.common.authoritative_market_clock import (
    authoritative_closed_trade_date,
)


SHANGHAI = ZoneInfo("Asia/Shanghai")
RECEIPT_SCHEMA = "probiga.etf-forward-daily-receipt.v1"
ETF_CLOSE_READY_TIME = time(15, 10)


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _receipt(payload: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    result["receipt_id"] = _digest(result)
    return result


def _market_summary(receipt: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: receipt.get(key)
        for key in (
            "receipt_id",
            "status",
            "trade_date",
            "batch_id",
            "groups",
            "database",
            "universe",
            "source_identity",
        )
        if key in receipt
    }


def _forward_summary(receipt: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: receipt.get(key)
        for key in (
            "receipt_id",
            "status",
            "write_status",
            "data_date",
            "strategy_version",
            "config_hash",
            "input_hash",
            "signal_type",
            "partition",
        )
        if key in receipt
    }


def run_daily(
    engine: Any,
    *,
    trade_date: str,
    expected_build_sha: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    current = now or datetime.now(SHANGHAI)
    if current.tzinfo is None:
        current = current.replace(tzinfo=SHANGHAI)
    current = current.replace(microsecond=0)
    target = date.fromisoformat(trade_date)
    market = run_sync(
        engine,
        trade_date=trade_date,
        expected_build_sha=expected_build_sha,
        now=current,
    )
    market_status = str(market.get("status") or "")
    if market_status == "SKIPPED_NON_TRADE_DAY":
        forward: dict[str, Any] = {
            "status": "NOT_RUN_NON_TRADE_DAY",
            "data_date": trade_date,
        }
        status = "PASS"
    elif market_status != "PASS":
        raise RuntimeError("ETF market-data phase did not pass")
    elif target != current.date():
        # Historical data repair is allowed; historical research observations
        # are not.  The next live close may append exactly once.
        forward = {
            "status": "NOT_RUN_HISTORICAL_BACKFILL_PROHIBITED",
            "data_date": trade_date,
        }
        status = "PASS"
    else:
        forward_receipt = run_forward(engine, now=current)
        if forward_receipt.get("status") != "PASS":
            raise RuntimeError("ETF forward-ledger phase did not pass")
        forward = _forward_summary(forward_receipt)
        status = "PASS"
    return _receipt(
        {
            "schema": RECEIPT_SCHEMA,
            "status": status,
            "trade_date": trade_date,
            "provider": PROVIDER_ID,
            "executor_owner": "qmt_windows_edge",
            "market_data": _market_summary(market),
            "forward_ledger": forward,
            "automatic_order_submission": False,
        }
    )


def _failure_receipt(*, trade_date: str, error: BaseException) -> dict[str, Any]:
    return _receipt(
        {
            "schema": RECEIPT_SCHEMA,
            "status": "DATA_BLOCKED",
            "trade_date": trade_date,
            "provider": PROVIDER_ID,
            "executor_owner": "qmt_windows_edge",
            "error_type": type(error).__name__,
            "error": str(error)[:1000],
            "automatic_order_submission": False,
        }
    )


def resolve_target_trade_date(
    engine: Any,
    *,
    requested_trade_date: str = "",
    now: datetime | None = None,
) -> str:
    """Resolve an implicit run to the latest exchange session closed by 15:10."""

    requested = str(requested_trade_date or "").strip()
    if requested:
        parsed = date.fromisoformat(requested)
        if parsed.isoformat() != requested:
            raise ValueError("ETF trade date is not canonical ISO format")
        return requested
    target = authoritative_closed_trade_date(
        engine,
        now=now,
        close_ready_time=ETF_CLOSE_READY_TIME,
    )
    if not target:
        raise RuntimeError(
            "ETF latest closed session is unavailable from si_trade_calendar"
        )
    return target


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--trade-date", default="")
    parser.add_argument("--expected-build-sha", default="")
    args = parser.parse_args(argv)
    now = datetime.now(SHANGHAI).replace(microsecond=0)
    trade_date = str(args.trade_date or "").strip()
    try:
        if trade_date:
            date.fromisoformat(trade_date)
        if not args.execute:
            dry_run_date = trade_date or now.date().isoformat()
            result = _receipt(
                {
                    "schema": RECEIPT_SCHEMA,
                    "status": "DRY_RUN",
                    "trade_date": dry_run_date,
                    "provider": PROVIDER_ID,
                    "executor_owner": "qmt_windows_edge",
                    "automatic_order_submission": False,
                }
            )
            print(_canonical_json(result), flush=True)
            return 0
        load_project_env()
        expected_build_sha = resolve_expected_build_sha(args.expected_build_sha)
        engine = create_tool_engine()
        try:
            trade_date = resolve_target_trade_date(
                engine,
                requested_trade_date=trade_date,
                now=now,
            )
            result = run_daily(
                engine,
                trade_date=trade_date,
                expected_build_sha=expected_build_sha,
                now=now,
            )
        finally:
            engine.dispose()
    except Exception as exc:
        result = _failure_receipt(trade_date=trade_date, error=exc)
        print(_canonical_json(result), flush=True)
        return 1
    print(_canonical_json(result), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
