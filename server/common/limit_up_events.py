# -*- coding: utf-8 -*-
"""Derive auditable limit-up state transitions from QMT full-tick quotes."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import json
import math
from typing import Any, Mapping

from server.common.limit_up_event_schema import LIMIT_UP_EVENT_TABLE


LIMIT_STATUS_BASIS = "PRE_CLOSE_BOARD_RULE_ROUND_HALF_UP"
_CENT = Decimal("0.01")
_ZERO = Decimal("0")
_PRICE_TOLERANCE = Decimal("0.000001")
_EVENT_COOLDOWN_SECONDS = 60


def _decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        result = Decimal(str(value))
    except Exception:
        return None
    return result if result.is_finite() else None


def board_limit_ratio(stock_code: str, short_name: str = "") -> Decimal:
    """Return the current A-share price-limit ratio for a stock.

    The exchange applies ST status and board rules independently from provider
    price fields.  The caller stores ``LIMIT_STATUS_BASIS`` alongside each event
    so consumers can distinguish this rule-derived boundary from a provider
    supplied ``UpStopPrice``.
    """

    code = str(stock_code or "").strip().zfill(6)
    name = str(short_name or "").upper()
    if "ST" in name:
        return Decimal("0.05")
    if code.startswith(("300", "301", "688", "689")):
        return Decimal("0.20")
    if code.startswith(("4", "8", "920")):
        return Decimal("0.30")
    return Decimal("0.10")


def derived_upper_limit(
    stock_code: str,
    short_name: str,
    pre_close: Any,
) -> Decimal | None:
    """Return the board-rule upper limit rounded to one fen."""

    previous = _decimal(pre_close)
    if previous is None or previous <= _ZERO:
        return None
    ratio = board_limit_ratio(stock_code, short_name)
    return (previous * (Decimal("1") + ratio)).quantize(
        _CENT,
        rounding=ROUND_HALF_UP,
    )


def _json(value: Mapping[str, Any]) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _first_level(values: Any) -> Decimal | None:
    if isinstance(values, str):
        try:
            values = json.loads(values)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
    if not isinstance(values, (list, tuple)) or not values:
        return None
    return _decimal(values[0])


def _event_key(
    *,
    event_type: str,
    stock_code: str,
    event_at: datetime,
    source_timetag: str,
) -> str:
    payload = "|".join(
        (
            event_type,
            stock_code,
            event_at.isoformat(sep=" ", timespec="microseconds"),
            source_timetag,
        )
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def derive_limit_up_events(
    quote: Mapping[str, Any],
    previous: Mapping[str, Any] | None,
    *,
    now: datetime,
    queue_snapshot_seconds: int = _EVENT_COOLDOWN_SECONDS,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """Return lifecycle events and the next per-stock state.

    The function never estimates seal amounts from QMT volume because the raw
    quantity unit varies by transport.  It records the provider's raw first bid
    volume and its change instead.
    """

    code = str(quote.get("stock_code") or "").strip().zfill(6)
    if len(code) != 6 or not code.isdigit() or bool(quote.get("stale")):
        return [], None
    snapshot_at = quote.get("snapshot_dt")
    if not isinstance(snapshot_at, datetime):
        return [], None
    if snapshot_at.time().isoformat(timespec="minutes") < "09:30":
        return [], None
    short_name = str(quote.get("short_name") or code).strip() or code
    pre_close = _decimal(quote.get("pre_close"))
    price = _decimal(quote.get("price"))
    upper_limit = derived_upper_limit(code, short_name, pre_close)
    if price is None or price <= _ZERO or upper_limit is None:
        return [], None

    bid_price = _first_level(quote.get("bid_price"))
    bid_volume = _first_level(quote.get("bid_vol"))
    ask_price = _first_level(quote.get("ask_price"))
    ask_volume = _first_level(quote.get("ask_vol"))
    at_limit = price >= upper_limit - _PRICE_TOLERANCE
    has_bid_at_limit = (
        bid_price is not None
        and bid_volume is not None
        and bid_price >= upper_limit - _PRICE_TOLERANCE
        and bid_volume > _ZERO
    )
    no_visible_ask = (
        ask_price is None
        or ask_volume is None
        or ask_price <= _ZERO
        or ask_volume <= _ZERO
    )
    sealed = bool(at_limit and has_bid_at_limit and no_visible_ask)

    previous = previous or {}
    previous_at_limit = bool(previous.get("at_limit"))
    previous_sealed = bool(previous.get("sealed"))
    previously_sealed = bool(previous.get("ever_sealed"))
    transition_types: list[str] = []
    if at_limit and not previous_at_limit:
        if previously_sealed and sealed:
            transition_types.append("RESEAL")
        else:
            transition_types.append("TOUCH")
            if sealed:
                transition_types.append("SEAL")
    elif at_limit and sealed and not previous_sealed:
        transition_types.append("RESEAL" if previously_sealed else "SEAL")
    elif previous_at_limit and not at_limit:
        transition_types.append("BREAK")

    previous_volume = _decimal(previous.get("bid1_volume_raw"))
    previous_queue_at = previous.get("last_queue_event_at")
    if (
        sealed
        and previous_sealed
        and isinstance(previous_queue_at, datetime)
        and (snapshot_at - previous_queue_at).total_seconds()
        >= max(10, int(queue_snapshot_seconds))
        and bid_volume is not None
        and bid_volume != previous_volume
    ):
        transition_types.append("QUEUE_SNAPSHOT")

    source_timetag = str(
        quote.get("qmt_timetag") or quote.get("source_timetag") or ""
    ).strip()
    evidence = {
        "at_limit": at_limit,
        "sealed": sealed,
        "limit_status_basis": LIMIT_STATUS_BASIS,
        "bid1_at_limit": has_bid_at_limit,
        "no_visible_ask": no_visible_ask,
        "raw_level_unit": "QMT_TRANSPORT_RAW",
    }
    events: list[dict[str, Any]] = []
    for event_type in transition_types:
        events.append(
            {
                "event_key": _event_key(
                    event_type=event_type,
                    stock_code=code,
                    event_at=snapshot_at,
                    source_timetag=source_timetag,
                ),
                "trade_date": snapshot_at.date(),
                "stock_code": code,
                "short_name": short_name,
                "event_type": event_type,
                "event_at": snapshot_at,
                "price": price,
                "pre_close": pre_close,
                "upper_limit": upper_limit,
                "limit_status_basis": LIMIT_STATUS_BASIS,
                "bid1_price": bid_price,
                "bid1_volume_raw": bid_volume,
                "previous_bid1_volume_raw": previous_volume,
                "ask1_price": ask_price,
                "ask1_volume_raw": ask_volume,
                "source_provider": str(
                    quote.get("data_source") or "qmt_full_tick_5level"
                ),
                "source_snapshot_at": now,
                "source_timetag": source_timetag or None,
                "evidence_json": _json(evidence),
                "created_at": now,
            }
        )

    last_queue_event_at = previous_queue_at
    if "QUEUE_SNAPSHOT" in transition_types:
        last_queue_event_at = snapshot_at
    elif sealed and not previous_sealed:
        last_queue_event_at = snapshot_at
    if not sealed:
        last_queue_event_at = None
    next_state = {
        "at_limit": at_limit,
        "sealed": sealed,
        "ever_sealed": previously_sealed or sealed,
        "bid1_volume_raw": bid_volume,
        "last_queue_event_at": last_queue_event_at,
    }
    return events, next_state


def event_insert_sql() -> str:
    """Return the canonical idempotent insert for the event table."""

    return f"""
        INSERT IGNORE INTO {LIMIT_UP_EVENT_TABLE} (
            event_key, trade_date, stock_code, short_name, event_type,
            event_at, price, pre_close, upper_limit, limit_status_basis,
            bid1_price, bid1_volume_raw, previous_bid1_volume_raw,
            ask1_price, ask1_volume_raw, source_provider, source_snapshot_at,
            source_timetag, evidence_json, created_at
        ) VALUES (
            :event_key, :trade_date, :stock_code, :short_name, :event_type,
            :event_at, :price, :pre_close, :upper_limit, :limit_status_basis,
            :bid1_price, :bid1_volume_raw, :previous_bid1_volume_raw,
            :ask1_price, :ask1_volume_raw, :source_provider,
            :source_snapshot_at, :source_timetag, :evidence_json, :created_at
        )
    """


__all__ = [
    "LIMIT_STATUS_BASIS",
    "board_limit_ratio",
    "derived_upper_limit",
    "derive_limit_up_events",
    "event_insert_sql",
]
