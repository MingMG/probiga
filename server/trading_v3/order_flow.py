from __future__ import annotations

import math
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Iterable

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine


def _finite(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def calculate_order_flow(events: Iterable[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(
        (dict(item) for item in events),
        key=lambda item: (item.get("quote_at"), item.get("quote_event_id", "")),
    )
    valid = [
        item
        for item in ordered
        if all(
            (_finite(item.get(key)) or 0) > 0
            for key in ("bid1", "bid1_volume", "ask1", "ask1_volume")
        )
    ]
    if not valid:
        return {
            "quality_status": "BLOCK",
            "event_count": 0,
            "ofi": 0.0,
            "ofi_normalized": 0.0,
            "queue_imbalance": 0.0,
            "spread_bps": None,
            "freshness_seconds": None,
        }
    ofi = 0.0
    for left, right in zip(valid, valid[1:]):
        left_bid = float(left["bid1"])
        right_bid = float(right["bid1"])
        left_bid_qty = float(left["bid1_volume"])
        right_bid_qty = float(right["bid1_volume"])
        left_ask = float(left["ask1"])
        right_ask = float(right["ask1"])
        left_ask_qty = float(left["ask1_volume"])
        right_ask_qty = float(right["ask1_volume"])
        if right_bid > left_bid:
            bid_event = right_bid_qty
        elif right_bid == left_bid:
            bid_event = right_bid_qty - left_bid_qty
        else:
            bid_event = -left_bid_qty
        if right_ask < left_ask:
            ask_event = right_ask_qty
        elif right_ask == left_ask:
            ask_event = right_ask_qty - left_ask_qty
        else:
            ask_event = -left_ask_qty
        ofi += bid_event - ask_event
    latest = valid[-1]
    bid_qty = float(latest["bid1_volume"])
    ask_qty = float(latest["ask1_volume"])
    depth = max(1.0, (bid_qty + ask_qty) / 2.0)
    queue_imbalance = (bid_qty - ask_qty) / max(1.0, bid_qty + ask_qty)
    midpoint = (float(latest["bid1"]) + float(latest["ask1"])) / 2.0
    spread_bps = (
        (float(latest["ask1"]) - float(latest["bid1"]))
        / midpoint
        * 10_000.0
        if midpoint > 0
        else None
    )
    quote_at = latest.get("quote_at")
    freshness = None
    if isinstance(quote_at, datetime):
        freshness = max(0.0, (datetime.now() - quote_at).total_seconds())
    return {
        "quality_status": "PASS" if len(valid) >= 2 else "PARTIAL",
        "event_count": len(valid),
        "ofi": round(ofi, 6),
        "ofi_normalized": round(ofi / depth, 6),
        "queue_imbalance": round(queue_imbalance, 6),
        "spread_bps": round(spread_bps, 6) if spread_bps is not None else None,
        "freshness_seconds": round(freshness, 3) if freshness is not None else None,
        "latest_price": _finite(latest.get("last_price")),
        "latest_bid1": _finite(latest.get("bid1")),
        "latest_ask1": _finite(latest.get("ask1")),
        "source_provider": str(latest.get("source_provider") or ""),
        "quote_at": quote_at,
    }


def load_recent_order_flow(
    engine: Engine,
    *,
    stock_codes: Iterable[str],
    observed_at: datetime,
    lookback_minutes: int = 10,
    maximum_events_per_stock: int = 240,
) -> dict[str, dict[str, Any]]:
    codes = sorted({str(code).strip().zfill(6) for code in stock_codes if code})
    if not codes:
        return {}
    cutoff = observed_at - timedelta(minutes=max(1, int(lookback_minutes)))
    statement = text(
        """
        SELECT quote_event_id, stock_code, quote_at, bid1, bid1_volume,
               ask1, ask1_volume, last_price, source_provider
        FROM st_quote_event_v2
        WHERE stock_code IN :codes
          AND quote_at >= :cutoff
          AND quote_at <= :observed_at
        ORDER BY stock_code, quote_at, quote_event_id
        """
    ).bindparams(bindparam("codes", expanding=True))
    with engine.connect() as connection:
        rows = connection.execute(
            statement,
            {
                "codes": codes,
                "cutoff": cutoff,
                "observed_at": observed_at,
            },
        ).mappings().all()
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        code = str(row["stock_code"])
        grouped[code].append(dict(row))
        if len(grouped[code]) > maximum_events_per_stock:
            grouped[code] = grouped[code][-maximum_events_per_stock:]
    return {
        code: calculate_order_flow(events)
        for code, events in grouped.items()
    }
