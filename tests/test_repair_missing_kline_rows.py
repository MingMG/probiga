from unittest.mock import patch

import pandas as pd
import pytest

from tools.repair_missing_kline_rows import (
    _fetch_replacement,
    _normalize_codes,
)


def _primary_frame() -> pd.DataFrame:
    return pd.DataFrame([{
        "stock_code": "301234",
        "short_name": "sample",
        "trade_time": "2026-07-21 15:00:00",
        "trade_date": "2026-07-21",
        "k_type": 1,
        "adjust_type": 0,
        "open": 10.0,
        "close": 11.0,
        "high": 11.2,
        "low": 9.9,
        "volume": 1000.0,
        "amount": 10500.0,
        "change": 1.0,
        "change_pct": 10.0,
        "turnover_ratio": 1.2,
        "pre_close": 10.0,
        "etl_sync_at": None,
    }])


def test_normalize_codes_deduplicates_and_rejects_non_a_share():
    assert _normalize_codes(["301234, 301234", "688237"]) == [
        "301234",
        "688237",
    ]
    with pytest.raises(ValueError):
        _normalize_codes(["500123"])


def test_fetch_replacement_requires_independent_ohlc_match():
    reference = {
        "open": 10.0,
        "high": 11.2,
        "low": 9.9,
        "close": 11.0,
    }
    with patch(
        "tools.repair_missing_kline_rows._fetch_builtin_one",
        return_value=_primary_frame(),
    ), patch(
        "tools.repair_missing_kline_rows._fetch_independent_reference",
        return_value=("tencent", reference),
    ):
        row, evidence = _fetch_replacement(
            "301234",
            "sample",
            "2026-07-21",
        )

    assert evidence["status"] == "agreed"
    assert row is not None
    assert row["pre_close"] == 10.0
    assert row["change_pct"] == 10.0
