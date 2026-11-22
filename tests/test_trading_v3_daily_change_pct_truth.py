from __future__ import annotations

import inspect

import pandas as pd
import pytest

from server.trading_v3 import daily_features


def test_daily_change_pct_ignores_mutable_stored_value():
    frame = pd.DataFrame({
        "close": [10.2],
        "pre_close": [10.0],
        "change_pct": [-88.0],
    })

    result = daily_features._derive_change_pct_from_close_pre_close(frame)

    assert result.tolist() == pytest.approx([2.0])


def test_daily_bar_loader_uses_unadjusted_prices_and_no_stored_change_pct():
    source = inspect.getsource(daily_features._load_bars)

    assert "adjust_type = 0" in source
    assert "pre_close, amount, change_pct" not in source
    assert "normalize_daily_bars" in source

    frame = pd.DataFrame({"stock_code": ["000001", "000001"],
        "trade_date": ["2026-11-19", "2026-11-20"],
        "open": [10., 10.2], "high": [10.3, 10.5], "low": [9.9, 10.1],
        "close": [10.2, 10.4], "pre_close": [10., 10.2], "amount": [1e8, 2e8],
        "change_pct": [-88., -99.]})
    actual = daily_features.normalize_daily_bars(frame)
    assert actual.change_pct.tolist() == pytest.approx([2., (10.4 / 10.2 - 1) * 100])
    assert actual.raw_close.tolist() == [10.2, 10.4]
    changed = frame.copy()
    changed.loc[1, "close"] = 100
    assert daily_features.normalize_daily_bars(changed).iloc[0].to_dict() == actual.iloc[0].to_dict()


def test_daily_change_pct_fails_closed_without_valid_pre_close():
    with pytest.raises(RuntimeError, match="cannot derive finite"):
        daily_features._derive_change_pct_from_close_pre_close(
            pd.DataFrame({"close": [10.0], "pre_close": [None]})
        )
