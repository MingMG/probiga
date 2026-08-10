#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from server.common.kline_data import get_kline_engine
from server.trading_v2.calendar import latest_trade_day
from server.trading_v2.quotes import validate_level1_continuity
from tools.env_config import load_project_env


MINIMUM_LIVE_RECEIPT_MINUTES = 228
MINIMUM_COMPLETE_LEVEL1_RATIO = Decimal("0.99")


def _live_receipt_gate(
    evidence_engine,
    trade_days: date | list[date],
) -> dict[str, object]:
    """Require a continuous producer/consumer receipt beside quote rows."""

    days = [trade_days] if isinstance(trade_days, date) else list(trade_days)
    daily: list[dict[str, object]] = []
    try:
        with evidence_engine.connect() as connection:
            for trade_day in days:
                row = connection.execute(
                    text(
                        """
                        SELECT
                            COUNT(*) AS receipt_count,
                            COUNT(DISTINCT DATE_FORMAT(source_generated_at, '%H:%i'))
                                AS receipt_minutes,
                            MAX(source_generated_at) AS latest_source_at,
                            MAX(published_at) AS latest_published_at
                        FROM st_qmt_realtime_sync_receipt_v2
                        WHERE DATE(source_generated_at) = :trade_date
                          AND capture_mode = 'LIVE_FORWARD'
                          AND quality_status = 'PASS'
                          AND (
                            TIME(source_generated_at) BETWEEN '09:31:00' AND '11:30:59'
                            OR TIME(source_generated_at) BETWEEN '13:01:00' AND '15:00:59'
                          )
                        """
                    ),
                    {"trade_date": trade_day},
                ).mappings().first()
                receipt_count = int((row or {}).get("receipt_count") or 0)
                receipt_minutes = int((row or {}).get("receipt_minutes") or 0)
                daily.append(
                    {
                        "trade_date": trade_day.isoformat(),
                        "receipt_count": receipt_count,
                        "receipt_minutes": receipt_minutes,
                        "minimum_receipt_minutes": MINIMUM_LIVE_RECEIPT_MINUTES,
                        "latest_source_at": (row or {}).get("latest_source_at"),
                        "latest_published_at": (row or {}).get("latest_published_at"),
                        "passed": bool(
                            receipt_count > 0
                            and receipt_minutes >= MINIMUM_LIVE_RECEIPT_MINUTES
                        ),
                    }
                )
    except Exception as exc:
        return {
            "status": "BLOCK",
            "reason": "live_receipt_query_failed",
            "error": str(exc),
            "minimum_receipt_minutes": MINIMUM_LIVE_RECEIPT_MINUTES,
            "days": daily,
        }
    passed = bool(days) and len(daily) == len(days) and all(
        bool(item["passed"]) for item in daily
    )
    receipt_count = sum(int(item["receipt_count"]) for item in daily)
    receipt_minutes = min(
        (int(item["receipt_minutes"]) for item in daily),
        default=0,
    )
    return {
        "status": "PASS" if passed else "BLOCK",
        "reason": "continuous_live_receipts" if passed else "insufficient_live_receipts",
        "trade_date": days[0].isoformat() if len(days) == 1 else None,
        "trade_day_count": len(days),
        "receipt_count": receipt_count,
        "receipt_minutes": receipt_minutes,
        "minimum_receipt_minutes": MINIMUM_LIVE_RECEIPT_MINUTES,
        "days": daily,
    }


def _persist_receipt_block(engine, result: dict[str, object]) -> None:
    evidence = dict(result.get("evidence") or {})
    now = datetime.now()
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE st_execution_capability_v2
                   SET status = 'BLOCK',
                       evidence_json = :evidence,
                       checked_at = :checked_at,
                       passed_at = NULL,
                       updated_at = :checked_at
                 WHERE capability_code = :capability_code
                """
            ),
            {
                "capability_code": result["capability_code"],
                "evidence": json.dumps(
                    evidence,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                ),
                "checked_at": now,
            },
        )


def _genuine_level1_event_gate(
    evidence_engine,
    trade_days: date | list[date],
) -> dict[str, object]:
    """Reject public snapshots, backfills and non-callback event latency."""

    days = [trade_days] if isinstance(trade_days, date) else list(trade_days)
    daily: list[dict[str, object]] = []
    try:
        with evidence_engine.connect() as connection:
            for trade_day in days:
                row = connection.execute(
                    text(
                        """
                        SELECT
                            COUNT(*) AS event_count,
                            COUNT(DISTINCT DATE_FORMAT(quote_at, '%H:%i'))
                                AS session_minutes,
                            SUM(CASE WHEN bid1 > 0 AND ask1 > 0
                                          AND bid1_volume > 0 AND ask1_volume > 0
                                     THEN 1 ELSE 0 END) AS complete_events,
                            MIN(TIMESTAMPDIFF(SECOND, quote_at, received_at))
                                AS minimum_ingress_seconds,
                            MAX(TIMESTAMPDIFF(SECOND, quote_at, received_at))
                                AS maximum_ingress_seconds
                        FROM st_quote_event_v2
                        WHERE DATE(quote_at) = :trade_date
                          AND source_provider = 'gj_big_qmt_inner'
                          AND received_at >= quote_at
                          AND TIMESTAMPDIFF(SECOND, quote_at, received_at) <= 15
                          AND (
                            TIME(quote_at) BETWEEN '09:31:00' AND '11:30:59'
                            OR TIME(quote_at) BETWEEN '13:01:00' AND '15:00:59'
                          )
                        """
                    ),
                    {"trade_date": trade_day},
                ).mappings().first()
                event_count = int((row or {}).get("event_count") or 0)
                session_minutes = int((row or {}).get("session_minutes") or 0)
                complete_events = int((row or {}).get("complete_events") or 0)
                complete_ratio = (
                    Decimal(complete_events) / Decimal(event_count)
                    if event_count
                    else Decimal("0")
                )
                passed = bool(
                    session_minutes >= MINIMUM_LIVE_RECEIPT_MINUTES
                    and complete_ratio >= MINIMUM_COMPLETE_LEVEL1_RATIO
                )
                daily.append(
                    {
                        "trade_date": trade_day.isoformat(),
                        "event_count": event_count,
                        "session_minutes": session_minutes,
                        "complete_event_ratio": str(complete_ratio),
                        "minimum_ingress_seconds": (row or {}).get(
                            "minimum_ingress_seconds"
                        ),
                        "maximum_ingress_seconds": (row or {}).get(
                            "maximum_ingress_seconds"
                        ),
                        "passed": passed,
                    }
                )
    except Exception as exc:
        return {
            "status": "BLOCK",
            "reason": "genuine_level1_query_failed",
            "error": str(exc),
            "days": daily,
        }
    passed = bool(days) and len(daily) == len(days) and all(
        bool(item["passed"]) for item in daily
    )
    return {
        "status": "PASS" if passed else "BLOCK",
        "reason": "genuine_live_level1_events" if passed else "insufficient_genuine_level1_events",
        "trade_day_count": len(days),
        "minimum_session_minutes": MINIMUM_LIVE_RECEIPT_MINUTES,
        "minimum_complete_event_ratio": str(MINIMUM_COMPLETE_LEVEL1_RATIO),
        "days": daily,
    }


def run_validation() -> dict[str, object]:
    engine = create_batch_engine()
    evidence_engine = None
    try:
        evidence_engine = get_kline_engine()
        end_date = latest_trade_day(engine, date.today())
        result = validate_level1_continuity(
            engine,
            end_date=end_date,
            evidence_engine=evidence_engine,
        )
        evidence_days = [
            date.fromisoformat(str(item["trade_date"])[:10])
            for item in (result.get("evidence") or {}).get("days", [])
            if item.get("trade_date")
        ]
        receipt_gate = _live_receipt_gate(
            evidence_engine,
            evidence_days or end_date,
        )
        genuine_event_gate = _genuine_level1_event_gate(
            evidence_engine,
            evidence_days or end_date,
        )
        result["receipt_gate"] = receipt_gate
        result["genuine_event_gate"] = genuine_event_gate
        result.setdefault("evidence", {})["live_receipt_gate"] = receipt_gate
        result["evidence"]["genuine_level1_event_gate"] = genuine_event_gate
        failed_gate = next(
            (
                gate
                for gate in (genuine_event_gate, receipt_gate)
                if gate.get("status") != "PASS"
            ),
            None,
        )
        if result.get("status") == "PASS" and failed_gate is not None:
            result["status"] = "BLOCK"
            result["block_reason"] = str(
                failed_gate.get("reason") or "level1_acceptance_gate_blocked"
            )
            _persist_receipt_block(engine, result)
        return result
    finally:
        engine.dispose()
        if evidence_engine is not None and evidence_engine is not engine:
            evidence_engine.dispose()


def main() -> int:
    load_project_env()
    try:
        result = run_validation()
    except Exception as exc:
        result = {
            "status": "ERROR",
            "error": str(exc),
            "checked_at": datetime.now().isoformat(timespec="seconds"),
        }
        print(json.dumps(result, ensure_ascii=False, default=str))
        return 2
    print(json.dumps(result, ensure_ascii=False, default=str))
    # A blocked capability is an acceptance failure and must be visible to the
    # scheduler/operations layer instead of masquerading as a successful task.
    return 0 if result.get("status") == "PASS" else 3


if __name__ == "__main__":
    raise SystemExit(main())
