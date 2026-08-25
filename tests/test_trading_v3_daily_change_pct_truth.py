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
    assert "_derive_change_pct_from_close_pre_close" in source


def test_daily_change_pct_fails_closed_without_valid_pre_close():
    with pytest.raises(RuntimeError, match="cannot derive finite"):
        daily_features._derive_change_pct_from_close_pre_close(
            pd.DataFrame({"close": [10.0], "pre_close": [None]})
        )
