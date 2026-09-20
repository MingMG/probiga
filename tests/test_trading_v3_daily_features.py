from __future__ import annotations

from datetime import date, datetime

import pandas as pd
import pytest

from server.trading_v3.daily_features import (
    _block_entry_candidate_features,
    _eligible_daily_history_codes,
    _missing_finance_numeric_fields,
    _qmt_attestation_evidence,
    _restricted_entry_name,
)
from server.trading_v3.engine import TradingV3Engine


def test_finance_completeness_ignores_pit_metadata_fields():
    values = {
        "net_asset_ps": 10.0,
        "oper_cf_ps": 1.0,
        "total_rev_yoy_gr": 2.0,
        "net_profit_yoy_gr": 3.0,
        "roe_wtd": 4.0,
        "gross_margin": 5.0,
        "net_margin": 6.0,
        "cash_flow_ratio": 7.0,
        "asset_liab_ratio": 8.0,
        "finance_pit_status": "AVAILABLE",
        "finance_manifest_hash": "a" * 64,
        "finance_known_at": "2026-09-06 22:41:36",
    }

    assert _missing_finance_numeric_fields(values) == []

    values["oper_cf_ps"] = None
    assert _missing_finance_numeric_fields(values) == ["oper_cf_ps"]


def test_qmt_attestation_uses_the_exact_validated_window_target():
    window = {"sessions": ["2026-08-19"], "latest_daily_truth": {
        "schema": "probiga.qmt-daily-market-consumer-truth.v1",
        "requested_sessions": ["2026-08-19"], "run_id": "valid",
        "run_start_date": "2026-08-19", "run_end_date": "2026-08-19",
        "attested_row_count": 5547,
    }}
    evidence = _qmt_attestation_evidence(
        window,
        trade_date=date(2026, 8, 19),
    )

    assert evidence["qmt_attestation_current"] is True
    assert evidence["qmt_attestation_run_id"] == "valid"
    assert evidence["qmt_attestation_target_rows"] == 5547


def test_qmt_attestation_rejects_run_counters_without_validated_input_window():
    with pytest.raises(RuntimeError, match="window target evidence"):
        _qmt_attestation_evidence(
            {"run_id": "counter-only", "target_rows": 5547, "matched_rows": 5547},
            trade_date=date(2026, 8, 19),
        )


def test_entry_history_requires_each_stock_to_have_the_exact_target_bar():
    sessions = list(pd.bdate_range("2026-05-26", periods=66).date)
    target = sessions[-1]
    rows = []
    for code, observed in (
        ("000001", sessions[1:]),
        ("000002", sessions[:-1]),
        ("000003", sessions[:-1]),
    ):
        rows.extend(
            {"stock_code": code, "trade_date": trade_date}
            for trade_date in observed
        )
    frame = pd.DataFrame(rows)

    codes = _eligible_daily_history_codes(
        frame,
        expected_trade_date=target,
        required_codes={"000003"},
    )

    assert codes == ["000001", "000003"]
    assert "000002" not in codes


def test_empty_st_and_delisting_names_are_all_fail_closed():
    assert _restricted_entry_name("") is True
    assert _restricted_entry_name(None) is True
    assert _restricted_entry_name("*ST 风险") is True
    assert _restricted_entry_name("退市整理") is True
    assert _restricted_entry_name("平安银行") is False


def test_stale_held_stock_is_monitoring_only_and_every_sleeve_is_insufficient():
    strategy_features = {
        key: 1.0
        for key in {
            "amount_ratio_1_20",
            "amount_ratio_5_20",
            "atr_14d_pct",
            "average_amount_20d",
            "breakout_20d_proximity",
            "cashflow_quality_percentile",
            "close_above_ma20",
            "distance_ma20_pct",
            "distance_ma5_pct",
            "drawdown_20d_pct",
            "event_decay",
            "event_novelty",
            "event_price_confirmation",
            "event_priced_in",
            "event_source_reliability",
            "event_surprise",
            "fill_probability",
            "growth_percentile",
            "interval_return_pct",
            "intraday_amount_surprise_z",
            "latest_amount",
            "latest_change_pct",
            "latest_relative_to_market_pct",
            "leadership_quality",
            "ma20_above_ma60",
            "ma20_slope_5d_pct",
            "market_return_20d_pct",
            "momentum_60d_percentile",
            "previous_change_pct",
            "price_vs_vwap_pct",
            "quality_percentile",
            "rebound_from_low_pct",
            "relative_strength_20d_pct",
            "return_20d_pct",
            "return_2d_pct",
            "return_5d_pct",
            "return_60d_pct",
            "sector_amount_acceleration_pct",
            "sector_breadth_acceleration_pct",
            "sector_breadth_pct",
            "sector_crowding",
            "sector_relative_return_pct",
            "spread_bps",
            "stock_leadership_score",
            "stock_relative_to_theme_5d_pct",
            "theme_opportunity_score",
            "valuation_percentile",
            "volatility_20d_percentile",
        }
    }
    strategy_features.update(
        {
            "stock_code": "000003",
            "stock_name": "000003",
            "price": 10.0,
            "latest_low": 9.5,
            "entry_eligible": 1.0,
            "required_position_monitor": 1.0,
            "latest_tradable": 1.0,
        }
    )

    _block_entry_candidate_features(
        strategy_features,
        reason=(
            "MISSING_EXACT_TARGET_BAR:"
            "expected=2026-08-26,actual=2026-08-25"
        ),
    )
    forecasts = TradingV3Engine({}).evaluate_stock(
        "000003",
        "000003",
        strategy_features,
        datetime(2026, 8, 26, 15, 0),
        datetime(2026, 8, 31, 15, 0),
    )

    assert strategy_features["price"] == 10.0
    assert strategy_features["latest_low"] == 9.5
    assert strategy_features["required_position_monitor"] == 1.0
    assert strategy_features["entry_eligible"] == 0.0
    assert strategy_features["latest_tradable"] == 0.0
    assert strategy_features["return_20d_pct"] is None
    assert strategy_features["data_quality_status"] == "DATA_BLOCKED"
    assert forecasts
    assert {forecast.status for forecast in forecasts} == {
        "INSUFFICIENT_DATA"
    }
