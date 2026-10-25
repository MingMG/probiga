from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest

from tools.import_sina_minute_history import (
    MINUTE_GRID,
    DailyAnchor,
    ImportErrorBase,
    parse_sina_session,
)


def _payload() -> list[dict]:
    target = dt.date(2026, 10, 23)
    rows = []
    for clock in MINUTE_GRID:
        stamp = dt.datetime.combine(
            target, dt.time.fromisoformat(clock)
        )
        close = "11.50" if clock == "15:00:00" else "11.20"
        rows.append({
            "day": stamp.strftime("%Y-%m-%d %H:%M:%S"),
            "open": "11.20",
            "high": "11.50",
            "low": "10.90",
            "close": close,
            "volume": "10000",
            "amount": "112000",
        })
    return rows


def _anchor() -> DailyAnchor:
    return DailyAnchor(
        stock_code="603278",
        qmt_code="603278.SH",
        pre_close=Decimal("10.45"),
        close=Decimal("11.50"),
        high=Decimal("11.50"),
        low=Decimal("10.90"),
    )


def test_sina_parser_accepts_exact_238_bar_contract():
    rows = parse_sina_session(
        _payload(),
        anchor=_anchor(),
        trade_date="2026-10-23",
        captured_at=dt.datetime(2026, 10, 24, 9, 0),
        batch_id="sina-test",
    )

    assert len(rows) == 238
    assert rows[0]["trade_time"].strftime("%H:%M:%S") == "09:31:00"
    assert rows[-1]["trade_time"].strftime("%H:%M:%S") == "15:00:00"
    assert rows[-1]["data_source"] == "sina_public_1m"
    assert rows[-1]["price"] == Decimal("11.50")


def test_sina_parser_rejects_an_incomplete_session():
    payload = _payload()
    payload.pop(100)

    with pytest.raises(ImportErrorBase, match="incomplete source grid"):
        parse_sina_session(
            payload,
            anchor=_anchor(),
            trade_date="2026-10-23",
            captured_at=dt.datetime(2026, 10, 24, 9, 0),
            batch_id="sina-test",
        )
