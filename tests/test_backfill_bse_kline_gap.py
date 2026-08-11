from unittest.mock import patch

import pandas as pd
import pytest

from tools.backfill_bse_kline_gap import (
    _fetch_code,
    _find_content_page,
    _parse_bse_jsonp,
    _reference_pre_close,
)
from tools.fetch_sm_stock_kline_daily import _parse_ths_history


def _business_frame(close: float = 11.0) -> pd.DataFrame:
    return pd.DataFrame([{
        "stock_code": "920002",
        "short_name": "万达轴承",
        "trade_time": "2026-07-09 15:00:00",
        "trade_date": "2026-07-09",
        "k_type": 1,
        "adjust_type": 0,
        "open": 10.5,
        "close": close,
        "high": 11.2,
        "low": 10.2,
        "volume": 1000.0,
        "amount": 10800.0,
        "change": None,
        "change_pct": None,
        "turnover_ratio": 1.2,
        "pre_close": None,
    }])


def _ths_frame(close: float = 11.0) -> pd.DataFrame:
    return pd.DataFrame([
        {
            "trade_date": "2026-07-08",
            "open": 9.8,
            "high": 10.1,
            "low": 9.7,
            "close": 10.0,
            "volume": 900.0,
            "amount": 9000.0,
        },
        {
            "trade_date": "2026-07-09",
            "open": 10.5,
            "high": 11.2,
            "low": 10.2,
            "close": close,
            "volume": 1000.0,
            "amount": 10800.0,
        },
    ])


def test_parse_ths_history_maps_provider_field_ids():
    payload = {
        "status_code": 0,
        "data": {
            "quote_data": [{
                "market": "151",
                "code": "920002",
                "data_fields": ["1", "7", "8", "9", "11", "13", "19"],
                "value": [[1783526400000, 58.44, 59.88, 56.21, 57.58, 2133065, 123394033]],
            }],
        },
    }

    frame = _parse_ths_history(payload, "920002")

    assert frame.to_dict(orient="records") == [{
        "trade_date": "2026-07-09",
        "open": 58.44,
        "high": 59.88,
        "low": 56.21,
        "close": 57.58,
        "volume": 2133065.0,
        "amount": 123394033.0,
    }]


def test_reference_pre_close_uses_adjusted_return_not_raw_previous_close():
    adjusted = [
        {"trade_date": "2026-07-08", "close": 20.0},
        {"trade_date": "2026-07-09", "close": 21.0},
    ]

    assert _reference_pre_close(11.0, "2026-07-09", adjusted) == 11.0 / 1.05


def test_parse_official_bse_bulk_trade_jsonp_page():
    page = _find_content_page(_parse_bse_jsonp(
        'probigaBseBulk([[{"content":[{"hqzqdm":"920015"}],'
        '"totalPages":1}], \'\'])'
    ))

    assert page is not None
    assert page["content"][0]["hqzqdm"] == "920015"


def test_fetch_code_accepts_only_dual_source_match_and_derives_change_fields():
    def ths_side_effect(_code, *, count, adjust_type):
        assert count == 120
        assert adjust_type in {"actual", "forward"}
        return _ths_frame()

    with patch(
        "tools.backfill_bse_kline_gap._sina_history",
        return_value=_business_frame(),
    ), patch(
        "tools.backfill_bse_kline_gap._fetch_ths_history",
        side_effect=ths_side_effect,
    ):
        outcome = _fetch_code(
            "920002",
            "万达轴承",
            ["2026-07-09"],
            "2026-06-01",
            "2026-07-09",
            request_delay=0,
        )

    assert outcome.error == ""
    assert outcome.matched_rows == 1
    assert outcome.mismatch_rows == []
    row = outcome.frame.iloc[0]
    assert row["_data_source"] == "sina"
    assert row["pre_close"] == 10.0
    assert row["change"] == 1.0
    assert row["change_pct"] == pytest.approx(10.0)


def test_fetch_code_reconciles_sina_omitted_bulk_trade_with_official_bse_row():
    reference = _ths_frame()
    reference.loc[
        reference["trade_date"] == "2026-07-09",
        ["volume", "amount"],
    ] = [101000.0, 110800.0]

    with patch(
        "tools.backfill_bse_kline_gap._sina_history",
        return_value=_business_frame(),
    ), patch(
        "tools.backfill_bse_kline_gap._fetch_ths_history",
        side_effect=[reference, _ths_frame()],
    ):
        outcome = _fetch_code(
            "920002",
            "涓囪揪杞存壙",
            ["2026-07-09"],
            "2026-06-01",
            "2026-07-09",
            request_delay=0,
            official_bulk_trades={
                ("920002", "2026-07-09"): {
                    "volume": 100000.0,
                    "amount": 100000.0,
                    "trade_count": 1.0,
                },
            },
        )

    assert outcome.matched_rows == 1
    row = outcome.frame.iloc[0]
    assert row["volume"] == 101000.0
    assert row["amount"] == 110800.0
    assert row["_data_source"] == "sina+bse_official_bulk"


def test_fetch_code_does_not_double_count_bulk_when_vendors_already_match():
    with patch(
        "tools.backfill_bse_kline_gap._sina_history",
        return_value=_business_frame(),
    ), patch(
        "tools.backfill_bse_kline_gap._fetch_ths_history",
        side_effect=[_ths_frame(), _ths_frame()],
    ):
        outcome = _fetch_code(
            "920002",
            "万达轴承",
            ["2026-07-09"],
            "2026-06-01",
            "2026-07-09",
            request_delay=0,
            official_bulk_trades={
                ("920002", "2026-07-09"): {
                    "volume": 100000.0,
                    "amount": 100000.0,
                    "trade_count": 1.0,
                },
            },
        )

    assert outcome.matched_rows == 1
    row = outcome.frame.iloc[0]
    assert row["volume"] == 1000.0
    assert row["amount"] == 10800.0
    assert row["_data_source"] == "sina"


def test_fetch_code_reconciles_ths_omitted_bulk_trade_with_official_bse_row():
    primary = _business_frame()
    primary.loc[:, ["volume", "amount"]] = [101000.0, 110800.0]

    with patch(
        "tools.backfill_bse_kline_gap._sina_history",
        return_value=primary,
    ), patch(
        "tools.backfill_bse_kline_gap._fetch_ths_history",
        side_effect=[_ths_frame(), _ths_frame()],
    ):
        outcome = _fetch_code(
            "920002",
            "万达轴承",
            ["2026-07-09"],
            "2026-06-01",
            "2026-07-09",
            request_delay=0,
            official_bulk_trades={
                ("920002", "2026-07-09"): {
                    "volume": 100000.0,
                    "amount": 100000.0,
                    "trade_count": 1.0,
                },
            },
        )

    assert outcome.matched_rows == 1
    row = outcome.frame.iloc[0]
    assert row["volume"] == 101000.0
    assert row["amount"] == 110800.0
    assert row["_data_source"] == "sina+bse_official_bulk"


def test_fetch_code_blocks_mismatched_reference_row():
    with patch(
        "tools.backfill_bse_kline_gap._sina_history",
        return_value=_business_frame(),
    ), patch(
        "tools.backfill_bse_kline_gap._fetch_ths_history",
        side_effect=[_ths_frame(close=12.0), _ths_frame(close=12.0)],
    ):
        outcome = _fetch_code(
            "920002",
            "万达轴承",
            ["2026-07-09"],
            "2026-06-01",
            "2026-07-09",
            request_delay=0,
        )

    assert outcome.matched_rows == 0
    assert len(outcome.mismatch_rows) == 1
    assert outcome.frame.empty
