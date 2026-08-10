"""Durable QMT Level-1 quote events and the B-003 continuity gate."""
from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from .config import canonical_json_hash
from .domain import Quote, decimal_value


CAPABILITY_CODE = "B-003_RELIABLE_LEVEL1_BID_ASK"
PROTOCOL_VERSION = "level1_continuity_v2.0.0"
MINIMUM_COMPLETE_TRADE_DAYS = 5
MINIMUM_SESSION_MINUTES = 228
MINIMUM_COMPLETE_ROW_RATIO = Decimal("0.99")


def _nullable_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        number = decimal_value(value)
        return number if number.is_finite() and number > 0 else None
    except (InvalidOperation, TypeError, ValueError):
        return None


def _nullable_quantity(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        number = int(float(value))
    except (OverflowError, TypeError, ValueError):
        return None
    return number if number > 0 else None


def _canonical_decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value.normalize(), "f")


def build_quote_event(row: Mapping[str, Any]) -> dict[str, Any] | None:
    stock_code = str(row.get("stock_code") or "").strip().zfill(6)
    quote_at = row.get("source_time") or row.get("snapshot_at")
    received_at = row.get("received_at") or row.get("etl_sync_at")
    if not stock_code or not quote_at or not received_at:
        return None
    bid1 = _nullable_decimal(row.get("bid1"))
    ask1 = _nullable_decimal(row.get("ask1"))
    bid1_volume = _nullable_quantity(row.get("bid1_volume"))
    ask1_volume = _nullable_quantity(row.get("ask1_volume"))
    last_price = _nullable_decimal(row.get("price"))
    pre_close = _nullable_decimal(row.get("pre_close"))
    upper_limit = _nullable_decimal(row.get("upper_limit"))
    lower_limit = _nullable_decimal(row.get("lower_limit"))
    source_batch_id = str(row.get("batch_id") or "")
    suspended = bool(row.get("is_suspended", False))
    # Event identity is based only on exchange/source content.  ``received_at``
    # and ``batch_id`` are transport metadata: including either would turn the
    # same cached QMT quote into a new market event whenever the bridge restarts.
    payload = {
        "stock_code": stock_code,
        "quote_at": str(quote_at),
        "bid1": _canonical_decimal_text(bid1),
        "bid1_volume": bid1_volume,
        "ask1": _canonical_decimal_text(ask1),
        "ask1_volume": ask1_volume,
        "last_price": _canonical_decimal_text(last_price),
        "pre_close": _canonical_decimal_text(pre_close),
        "upper_limit": _canonical_decimal_text(upper_limit),
        "lower_limit": _canonical_decimal_text(lower_limit),
        "suspended": suspended,
        "source_provider": str(row.get("data_source") or "gj_big_qmt_inner"),
        "stock_status": row.get("stock_status"),
    }
    payload_hash = canonical_json_hash(payload)
    # QMT's numeric stockStatus is retained in the source payload but is not
    # interpreted here because its meaning is terminal/version dependent.
    # Tradability is decided by the effective instrument-rule record.
    return {
        "quote_event_id": payload_hash,
        "payload_hash": payload_hash,
        "stock_code": stock_code,
        "quote_at": quote_at,
        "received_at": received_at,
        "bid1": bid1,
        "bid1_volume": bid1_volume,
        "ask1": ask1,
        "ask1_volume": ask1_volume,
        "last_price": last_price,
        "pre_close": pre_close,
        "upper_limit": upper_limit,
        "lower_limit": lower_limit,
        "suspended": suspended,
        "source_provider": payload["source_provider"],
        "source_batch_id": source_batch_id[:120],
        "created_at": datetime.now(),
    }


def persist_quote_events(
    engine: Engine,
    rows: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    events = [
        event
        for event in (build_quote_event(row) for row in rows)
        if event is not None
    ]
    if not events:
        return {"received": 0, "inserted": 0}
    statement = text(
        """
        INSERT IGNORE INTO st_quote_event_v2
        (quote_event_id, stock_code, quote_at, received_at,
         bid1, bid1_volume, ask1, ask1_volume, last_price, pre_close,
         upper_limit, lower_limit, suspended, source_provider,
         source_batch_id, payload_hash, created_at)
        VALUES
        (:quote_event_id, :stock_code, :quote_at, :received_at,
         :bid1, :bid1_volume, :ask1, :ask1_volume, :last_price, :pre_close,
         :upper_limit, :lower_limit, :suspended, :source_provider,
         :source_batch_id, :payload_hash, :created_at)
        """
    )
    inserted = 0
    with engine.begin() as connection:
        for event in events:
            result = connection.execute(statement, event)
            inserted += max(0, int(result.rowcount or 0))
    return {"received": len(events), "inserted": inserted}


def latest_quote(
    connection: Connection,
    *,
    stock_code: str,
) -> Quote | None:
    row = connection.execute(
        text(
            """
            SELECT * FROM st_quote_event_v2
            WHERE stock_code = :stock_code
            ORDER BY quote_at DESC, received_at DESC, quote_event_id DESC
            LIMIT 1
            """
        ),
        {"stock_code": stock_code},
    ).mappings().first()
    if not row:
        return None
    return Quote(
        stock_code=str(row["stock_code"]),
        event_id=str(row["quote_event_id"]),
        quote_at=row["quote_at"],
        received_at=row["received_at"],
        bid1=_nullable_decimal(row["bid1"]),
        bid1_volume=_nullable_quantity(row["bid1_volume"]),
        ask1=_nullable_decimal(row["ask1"]),
        ask1_volume=_nullable_quantity(row["ask1_volume"]),
        last_price=_nullable_decimal(row["last_price"]),
        upper_limit=_nullable_decimal(row["upper_limit"]),
        lower_limit=_nullable_decimal(row["lower_limit"]),
        suspended=bool(row["suspended"]),
    )


def _trade_days(engine: Engine, end_date: date, limit: int) -> list[date]:
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT trade_date FROM si_trade_calendar
                WHERE trade_status = 1 AND trade_date <= :end_date
                ORDER BY trade_date DESC LIMIT :limit
                """
            ),
            {"end_date": end_date, "limit": int(limit)},
        ).fetchall()
    return [
        value if isinstance(value, date) else date.fromisoformat(str(value)[:10])
        for (value,) in rows
    ]


def validate_level1_continuity(
    engine: Engine,
    *,
    end_date: date,
    evidence_engine: Engine | None = None,
) -> dict[str, Any]:
    """Validate five complete sessions before clearing B-003.

    Coverage is calculated from exchange-session minute buckets. A day passes
    only when at least 95% of the 240 session minutes contain a quote event and
    at least 99% of events have positive bid1/ask1 prices and quantities.
    """
    source_engine = evidence_engine or engine
    days = _trade_days(engine, end_date, MINIMUM_COMPLETE_TRADE_DAYS)
    evidence_days: list[dict[str, Any]] = []
    consecutive = 0
    with source_engine.connect() as connection:
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
                        MAX(TIMESTAMPDIFF(SECOND, quote_at, received_at))
                            AS maximum_ingress_seconds
                    FROM st_quote_event_v2
                    WHERE DATE(quote_at) = :trade_date
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
            maximum_ingress = int(
                (row or {}).get("maximum_ingress_seconds") or 0
            )
            passed = (
                session_minutes >= MINIMUM_SESSION_MINUTES
                and complete_ratio >= MINIMUM_COMPLETE_ROW_RATIO
                and maximum_ingress <= 15
            )
            evidence_days.append(
                {
                    "trade_date": trade_day.isoformat(),
                    "event_count": event_count,
                    "session_minutes": session_minutes,
                    "minimum_session_minutes": MINIMUM_SESSION_MINUTES,
                    "complete_event_ratio": str(complete_ratio),
                    "maximum_ingress_seconds": maximum_ingress,
                    "passed": passed,
                }
            )
            if passed:
                consecutive += 1
            else:
                break
    passed = (
        len(days) == MINIMUM_COMPLETE_TRADE_DAYS
        and consecutive == MINIMUM_COMPLETE_TRADE_DAYS
    )
    now = datetime.now()
    evidence = {
        "protocol_version": PROTOCOL_VERSION,
        "minimum_complete_trade_days": MINIMUM_COMPLETE_TRADE_DAYS,
        "minimum_session_minutes": MINIMUM_SESSION_MINUTES,
        "minimum_complete_row_ratio": str(MINIMUM_COMPLETE_ROW_RATIO),
        "maximum_quote_age_seconds": 15,
        "last_price_fallback": "PROHIBITED",
        "days": evidence_days,
    }
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO st_execution_capability_v2
                (capability_code, status, protocol_version,
                 consecutive_trade_days, evidence_json, checked_at,
                 passed_at, updated_at)
                VALUES
                (:code, :status, :protocol, :days, :evidence, :now,
                 :passed_at, :now)
                ON DUPLICATE KEY UPDATE
                    status = VALUES(status),
                    protocol_version = VALUES(protocol_version),
                    consecutive_trade_days = VALUES(consecutive_trade_days),
                    evidence_json = VALUES(evidence_json),
                    checked_at = VALUES(checked_at),
                    passed_at = CASE
                        WHEN VALUES(status) = 'PASS'
                        THEN COALESCE(passed_at, VALUES(passed_at))
                        ELSE NULL END,
                    updated_at = VALUES(updated_at)
                """
            ),
            {
                "code": CAPABILITY_CODE,
                "status": "PASS" if passed else "BLOCK",
                "protocol": PROTOCOL_VERSION,
                "days": consecutive,
                "evidence": json.dumps(
                    evidence,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                "now": now,
                "passed_at": now if passed else None,
            },
        )
    return {
        "capability_code": CAPABILITY_CODE,
        "status": "PASS" if passed else "BLOCK",
        "consecutive_trade_days": consecutive,
        "evidence": evidence,
    }
