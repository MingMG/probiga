#!/usr/bin/env python3
"""Scheduler-only entry point for exact final-pool WeCom delivery."""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from biz.analysis.final_pool_wecom import (  # noqa: E402
    FINAL_POOL_DELIVERY_SCHEMA,
    FinalPoolDeliveryBlocked,
    send_final_pool_batch,
)


def _create_tool_engine():
    from tools.env_config import create_tool_engine, load_project_env

    load_project_env()
    return create_tool_engine()


def validate_cli_result(payload: object, return_code: int) -> str:
    if not isinstance(payload, dict) or payload.get("schema") != FINAL_POOL_DELIVERY_SCHEMA:
        return "failed"
    target = str(payload.get("target_trade_date") or "")
    covered = payload.get("covered_trade_dates")
    deliveries = payload.get("deliveries")
    try:
        exact_deliveries = bool(
            isinstance(covered, list)
            and len(covered) == 2
            and all(isinstance(value, str) for value in covered)
            and covered == sorted(set(covered))
            and all(re.fullmatch(r"\d{4}-\d{2}-\d{2}", value) for value in covered)
            and target == covered[-1]
            and isinstance(deliveries, list)
            and len(deliveries) == 2
            and all(isinstance(item, dict) for item in deliveries)
            and [str(item.get("trade_date") or "") for item in deliveries]
            == covered
            and all(
                item.get("status") == "SUCCEEDED"
                and re.fullmatch(r"[0-9a-f]{32}", str(item.get("governance_run_uid") or ""))
                and re.fullmatch(r"[0-9a-f]{32}", str(item.get("analysis_run_uid") or ""))
                and re.fullmatch(r"[0-9a-f]{40}", str(item.get("build_sha") or ""))
                and re.fullmatch(r"[0-9a-f]{64}", str(item.get("governance_result_sha256") or ""))
                and re.fullmatch(r"[0-9a-f]{64}", str(item.get("canonical_pool_sha256") or ""))
                and re.fullmatch(r"[0-9a-f]{64}", str(item.get("gate_hash") or ""))
                and re.fullmatch(r"[0-9a-f]{64}", str(item.get("content_sha256") or ""))
                and bool(str(item.get("delivery_id") or ""))
                and int(item.get("segment_count") or 0) > 0
                and int(item.get("delivered_count") or 0)
                == int(item.get("segment_count") or 0)
                and item.get("automatic_substitution") is False
                and item.get("automatic_real_order_submission") is False
                and item.get("real_order_authority") is False
                for item in deliveries
            )
        )
    except (TypeError, ValueError, OverflowError):
        exact_deliveries = False
    if (
        payload.get("status") == "SUCCEEDED"
        and return_code == 0
        and payload.get("automatic_substitution") is False
        and payload.get("automatic_real_order_submission") is False
        and payload.get("real_order_authority") is False
        and int(payload.get("delivery_count") or 0) == 2
        and exact_deliveries
    ):
        return "completed"
    if (
        payload.get("status") == "DATA_BLOCKED"
        and payload.get("retryable") is True
        and return_code == 2
    ):
        return "not_ready"
    return "failed"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    try:
        engine = _create_tool_engine()
        try:
            payload = send_final_pool_batch(
                engine,
                target_trade_date=str(args.trade_date),
            )
        finally:
            engine.dispose()
        code = 0
    except FinalPoolDeliveryBlocked as exc:
        payload = {
            "schema": FINAL_POOL_DELIVERY_SCHEMA,
            "status": "DATA_BLOCKED",
            "target_trade_date": str(args.trade_date),
            "reason_code": "FINAL_POOL_IDENTITY_NOT_READY",
            "error": str(exc)[:500],
            "retryable": True,
            "automatic_substitution": False,
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
        code = 2
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
