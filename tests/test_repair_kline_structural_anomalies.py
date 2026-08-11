from unittest.mock import patch

import pandas as pd

from tools.repair_kline_structural_anomalies import _replacement_from_sina


def _bad_row():
    return {
        "stock_code": "603031",
        "short_name": "安孚科技",
        "trade_time": "2026-07-10 15:00:00",
        "trade_date": "2026-07-10",
        "k_type": 1,
        "adjust_type": 0,
        "open": 49.15,
        "high": 50.65,
        "low": 47.94,
        "close": 47.90,
        "volume": 1,
        "amount": 1,
        "change": 0.5,
        "change_pct": 1,
        "turnover_ratio": 1,
        "pre_close": 47.4,
        "etl_sync_at": "2026-07-10 18:00:00",
    }


def _sina_frame():
    return pd.DataFrame([{
        "stock_code": "603031",
        "short_name": "安孚科技",
        "trade_time": "2026-07-10 15:00:00",
        "trade_date": "2026-07-10",
        "k_type": 1,
        "adjust_type": 0,
        "open": 49.15,
        "high": 50.65,
        "low": 47.53,
        "close": 47.67,
        "volume": 18281130,
        "amount": 900809249,
        "change": 0.27,
        "change_pct": 0.57,
        "turnover_ratio": 5.1,
        "pre_close": 47.4,
    }])


def test_repair_requires_cross_source_agreement():
    with patch(
        "tools.repair_kline_structural_anomalies._fetch_builtin_one",
        return_value=_sina_frame(),
    ), patch(
        "tools.repair_kline_structural_anomalies._fetch_tencent_reference",
        return_value={"open": 49.15, "high": 50.65, "low": 48.0, "close": 47.67},
    ):
        replacement, evidence = _replacement_from_sina(_bad_row())

    assert replacement is None
    assert evidence["status"] == "conflict"


def test_repair_builds_valid_replacement_after_cross_source_agreement():
    with patch(
        "tools.repair_kline_structural_anomalies._fetch_builtin_one",
        return_value=_sina_frame(),
    ), patch(
        "tools.repair_kline_structural_anomalies._fetch_tencent_reference",
        return_value={"open": 49.15, "high": 50.65, "low": 47.53, "close": 47.67},
    ):
        replacement, evidence = _replacement_from_sina(_bad_row())

    assert evidence["status"] == "agreed"
    assert replacement is not None
    assert replacement["low"] == 47.53
    assert replacement["close"] == 47.67
