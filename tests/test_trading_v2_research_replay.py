from __future__ import annotations

from decimal import Decimal

import pandas as pd

from server.trading_v2.research_replay import (
    fifo_completed_trade_rows,
    metrics_for_trade_rows,
    remove_best_n_net_pnl,
    remove_largest_profit_security_net_pnl,
)
from tools.backtest_etf_ensemble import MarketData


def _market_data() -> MarketData:
    dates = pd.bdate_range("2026-01-01", periods=15)
    close = pd.DataFrame({"510300": [10.0] * len(dates)}, index=dates)
    return MarketData(
        open=close.copy(),
        close=close,
        amount=pd.DataFrame(
            {"510300": [100_000_000.0] * len(dates)},
            index=dates,
        ),
        names={"510300": "沪深300ETF"},
        asset_classes={"510300": "A股宽基"},
    )


def test_fifo_replay_allocates_fees_once_and_calculates_net_pnl() -> None:
    data = _market_data()
    dates = data.close.index
    fills = pd.DataFrame(
        [
            {
                "trade_date": dates[10],
                "etf_code": "510300",
                "side": "BUY",
                "filled_units": 100,
                "execution_price": 10,
                "commission": 5,
            },
            {
                "trade_date": dates[11],
                "etf_code": "510300",
                "side": "SELL",
                "filled_units": 40,
                "execution_price": 12,
                "commission": 2,
            },
            {
                "trade_date": dates[12],
                "etf_code": "510300",
                "side": "SELL",
                "filled_units": 60,
                "execution_price": 8,
                "commission": 3,
            },
        ]
    )

    rows = fifo_completed_trade_rows(fills, data)

    assert len(rows) == 2
    assert rows[0]["buy_fees"] == Decimal("2.00")
    assert rows[1]["buy_fees"] == Decimal("3.00")
    assert rows[0]["trade_net_pnl"] == Decimal("76.00")
    assert rows[1]["trade_net_pnl"] == Decimal("-126.00")
    assert sum(
        (row["trade_net_pnl"] for row in rows),
        Decimal("0"),
    ) == Decimal("-50.00")
    assert sum(
        (row["initial_risk_amount"] for row in rows),
        Decimal("0"),
    ) == Decimal("150.00")

    metrics = metrics_for_trade_rows(
        rows,
        equity=pd.Series(
            [1000.0, 1100.0, 950.0],
            index=dates[:3],
        ),
    )
    assert metrics["cumulative_net_pnl"] == "-50.00"
    assert metrics["completed_trade_count"] == 2
    assert remove_best_n_net_pnl(rows, 1) == Decimal("-126.00")
    assert (
        remove_largest_profit_security_net_pnl(rows)
        == Decimal("0")
    )


def test_fifo_replay_is_deterministic_for_same_fill_order() -> None:
    data = _market_data()
    dates = data.close.index
    fills = pd.DataFrame(
        [
            {
                "trade_date": dates[10],
                "etf_code": "510300",
                "side": "BUY",
                "filled_units": 100,
                "execution_price": 10,
                "commission": 5,
            },
            {
                "trade_date": dates[11],
                "etf_code": "510300",
                "side": "SELL",
                "filled_units": 100,
                "execution_price": 11,
                "commission": 5,
            },
        ]
    )

    first = fifo_completed_trade_rows(fills, data)
    second = fifo_completed_trade_rows(fills, data)

    assert first == second
    assert first[0]["trade_id"] == second[0]["trade_id"]
