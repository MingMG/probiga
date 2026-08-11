from __future__ import annotations

import pandas as pd
import pytest

from tools.backtest_retail_dynamic_themes import (
    _prepare_features,
    _trade_fee,
)


def test_adjusted_prices_chain_official_pre_close_across_corporate_action():
    frame = pd.DataFrame(
        [
            {
                "stock_code": "600000",
                "short_name": "样本",
                "trade_date": pd.Timestamp("2026-01-05"),
                "open": 10.0,
                "high": 10.5,
                "low": 9.8,
                "close": 10.0,
                "pre_close": 9.8,
                "volume": 1_000_000,
                "amount": 10_000_000,
                "change_pct": 2.04,
                "turnover_ratio": 1.0,
            },
            {
                "stock_code": "600000",
                "short_name": "样本",
                "trade_date": pd.Timestamp("2026-01-06"),
                "open": 5.0,
                "high": 5.2,
                "low": 4.9,
                "close": 5.1,
                "pre_close": 5.0,
                "volume": 2_000_000,
                "amount": 10_000_000,
                "change_pct": 2.0,
                "turnover_ratio": 1.0,
            },
        ]
    )
    out = _prepare_features(frame)
    assert out.loc[1, "adjust_factor"] == pytest.approx(2.0)
    assert out.loc[1, "adj_close"] / out.loc[0, "adj_close"] == pytest.approx(1.02)


def test_stock_fee_includes_minimum_and_sell_taxes():
    assert _trade_fee(10_000, "buy") == pytest.approx(5.1)
    assert _trade_fee(10_000, "sell") == pytest.approx(10.1)
