import pandas as pd

from tools.rebuild_capital_flow_range_baidu import (
    _dataset_hash,
    _fetch_code,
    _identity_failure_counts,
    _identity_failure_samples,
    _parse_history_rows,
    _provider_amount,
    _read_staging,
    _write_staging,
)
from unittest.mock import patch


def test_parse_baidu_history_rows_normalizes_dates_and_units():
    rows = _parse_history_rows(
        [
            {
                "date": "2026/07/23",
                "extMainIn": "1.5万",
                "littleNetIn": "-2万",
                "mediumNetIn": "5000元",
                "largeNetIn": "1万",
                "superNetIn": "5000",
            },
        ],
        "000001",
    )

    assert rows == [{
        "stock_code": "000001",
        "trade_date": "2026-07-23",
        "main_net_inflow": 15000.0,
        "sm_net_inflow": -20000.0,
        "mid_net_inflow": 5000.0,
        "lg_net_inflow": 10000.0,
        "max_net_inflow": 5000.0,
        "data_source": "baidu_history",
        "_main_net_inflow_rounding_error": 500.0,
        "_sm_net_inflow_rounding_error": 5000.0,
        "_mid_net_inflow_rounding_error": 0.5,
        "_lg_net_inflow_rounding_error": 5000.0,
        "_max_net_inflow_rounding_error": 0.5,
    }]


def test_provider_amount_rejects_missing_and_tracks_display_rounding():
    assert _provider_amount("--") is None
    assert _provider_amount("1.27亿") == (127_000_000.0, 500_000.0)
    assert _provider_amount("841.11万") == (8_411_100.0, 50.0)


def test_identity_failure_counts_checks_components_and_market_balance():
    frame = pd.DataFrame([
        {
            "stock_code": "000001",
            "trade_date": "2026-07-22",
            "main_net_inflow": 30_000_000,
            "max_net_inflow": 10_000_000,
            "lg_net_inflow": 20_000_000,
            "mid_net_inflow": -12_000_000,
            "sm_net_inflow": -18_000_000,
        },
        {
            "stock_code": "000002",
            "trade_date": "2026-07-23",
            "main_net_inflow": 30_000_000,
            "max_net_inflow": 1_000_000,
            "lg_net_inflow": 1_000_000,
            "mid_net_inflow": -1_000_000,
            "sm_net_inflow": -1_000_000,
        },
    ])

    assert _identity_failure_counts(frame) == (1, 1)
    samples = _identity_failure_samples(frame)
    assert len(samples) == 1
    assert samples[0]["main_component_delta"] == 28_000_000
    assert samples[0]["market_balance_delta"] == 28_000_000


def test_staging_round_trip_preserves_codes_and_values(tmp_path):
    frame = pd.DataFrame([{
        "stock_code": "000001",
        "trade_date": "2026-07-23",
        "main_net_inflow": 30_000_000.25,
        "max_net_inflow": 10_000_000.0,
        "lg_net_inflow": 20_000_000.25,
        "mid_net_inflow": -12_000_000.0,
        "sm_net_inflow": -18_000_000.25,
        "data_source": "baidu_history",
    }])

    path = _write_staging(frame, str(tmp_path / "flow.csv"))
    loaded = _read_staging(str(path))

    assert loaded.iloc[0]["stock_code"] == "000001"
    assert loaded.iloc[0]["trade_date"] == "2026-07-23"
    assert loaded.iloc[0]["main_net_inflow"] == 30_000_000.25
    assert _dataset_hash(loaded) == _dataset_hash(frame)


def test_fetch_code_skips_empty_cursor_page_and_continues_backward():
    newest = [{
        "stock_code": "000002",
        "trade_date": "2026-06-26",
        "main_net_inflow": 0.0,
        "max_net_inflow": 0.0,
        "lg_net_inflow": 0.0,
        "mid_net_inflow": 0.0,
        "sm_net_inflow": 0.0,
        "data_source": "baidu_history",
    }]
    older = [
        {
            **newest[0],
            "trade_date": trade_date,
        }
        for trade_date in ("2026-06-24", "2026-06-23")
    ]
    with patch(
        "tools.rebuild_capital_flow_range_baidu._fetch_page",
        side_effect=[newest, [], older],
    ) as fetch:
        outcome = _fetch_code(
            "000002",
            {"2026-06-23", "2026-06-24", "2026-06-26"},
            "2026-06-26",
        )

    assert outcome.error == ""
    assert [row["trade_date"] for row in outcome.rows] == [
        "2026-06-23",
        "2026-06-24",
        "2026-06-26",
    ]
    assert fetch.call_args_list[1].args[1] == "2026-06-26"
    assert fetch.call_args_list[2].args[1] == "2026-06-27"
