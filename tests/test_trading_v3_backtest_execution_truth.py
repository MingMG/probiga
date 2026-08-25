from datetime import date

import pandas as pd

from server.trading_v3.backtest import _signal_samples, _simulate_portfolio


def _row(day: str, code: str = "000001", **changes):
    result = {
        "stock_code": code,
        "trade_date": pd.Timestamp(day),
        "amount": 2_000_000.0,
        "volume": 200_000.0,
        "raw_open": 10.0,
        "raw_close": 10.1,
        "raw_high": 10.2,
        "raw_low": 9.9,
        "raw_pre_close": 10.0,
        "close_above_ma20": 1,
        "ma20_above_ma60": 1,
    }
    result.update(changes)
    return result


def _signal(day: str = "2026-07-01"):
    return {
        "stock_code": "000001",
        "short_name": "测试股份",
        "trade_date": pd.Timestamp(day),
        "score": 0.9,
        "initial_stop_pct": -5.0,
        "raw_close": 10.0,
    }


def test_portfolio_replay_has_complete_capacity_and_impact_evidence():
    features = pd.DataFrame([
        _row("2026-07-01", raw_close=10.0),
        _row("2026-07-02", raw_open=10.0, raw_pre_close=10.0),
        _row(
            "2026-07-03",
            raw_open=10.1,
            raw_close=10.2,
            raw_pre_close=10.1,
            close_above_ma20=0,
        ),
        _row(
            "2026-07-06",
            raw_open=10.2,
            raw_close=10.3,
            raw_pre_close=10.2,
        ),
    ])

    metrics, trades, _curve = _simulate_portfolio(
        features,
        pd.DataFrame([_signal()]),
        None,
        start_date=date(2026, 7, 1),
        end_date=date(2026, 7, 6),
    )

    assert metrics["execution_evidence_valid"] is True
    assert metrics["signal_disposition_coverage"] == 1.0
    assert metrics["execution_status_counts"]["FILLED"] == 2
    assert metrics["total_nonlinear_impact_cny"] > 0
    assert metrics["maximum_participation_rate"] == 0.05
    assert metrics["order_authority"] is False
    assert len(trades) == 1
    assert trades[0]["entry_participation_rate"] <= 0.05
    assert trades[0]["exit_participation_rate"] <= 0.05


def test_locked_limit_entry_is_visible_known_unfilled_not_a_data_gap():
    features = pd.DataFrame([
        _row("2026-07-01", raw_close=10.0),
        _row(
            "2026-07-02",
            raw_open=10.5,
            raw_high=10.5,
            raw_low=10.5,
            raw_close=10.5,
            raw_pre_close=10.0,
        ),
    ])

    metrics, trades, _curve = _simulate_portfolio(
        features,
        pd.DataFrame([_signal()]),
        None,
        start_date=date(2026, 7, 1),
        end_date=date(2026, 7, 2),
    )

    assert trades == []
    assert metrics["execution_evidence_valid"] is True
    assert metrics["execution_status_counts"]["KNOWN_UNFILLED"] == 1
    assert metrics["execution_reason_counts"]["LOCKED_LIMIT_UP"] == 1


def test_portfolio_does_not_hindsight_resize_order_from_entry_day_turnover():
    features = pd.DataFrame([
        _row("2026-07-01", raw_close=10.0),
        _row(
            "2026-07-02",
            amount=100_000.0,
            raw_open=10.0,
            raw_pre_close=10.0,
        ),
    ])

    metrics, trades, _curve = _simulate_portfolio(
        features,
        pd.DataFrame([_signal()]),
        None,
        start_date=date(2026, 7, 1),
        end_date=date(2026, 7, 2),
    )

    assert trades == []
    assert metrics["execution_evidence_valid"] is True
    assert metrics["execution_reason_counts"]["ENTRY_CAPACITY_EXCEEDED"] == 1


def test_locked_limit_exit_stays_unresolved_and_nulls_funding_metrics():
    features = pd.DataFrame([
        _row("2026-07-01", raw_close=10.0),
        _row("2026-07-02", raw_open=10.0, raw_pre_close=10.0),
        _row(
            "2026-07-03",
            raw_open=10.1,
            raw_close=10.2,
            raw_pre_close=10.1,
            close_above_ma20=0,
        ),
        _row(
            "2026-07-06",
            raw_open=9.69,
            raw_high=9.69,
            raw_low=9.69,
            raw_close=9.69,
            raw_pre_close=10.2,
        ),
    ])

    metrics, trades, _curve = _simulate_portfolio(
        features,
        pd.DataFrame([_signal()]),
        None,
        start_date=date(2026, 7, 1),
        end_date=date(2026, 7, 6),
    )

    assert trades == []
    assert metrics["execution_evidence_valid"] is False
    assert metrics["unresolved_position_count"] == 1
    assert metrics["execution_status_counts"]["KNOWN_UNFILLED"] == 1
    assert metrics["execution_status_counts"]["UNRESOLVED_EXIT"] == 1
    assert metrics["profit_factor"] is None
    assert metrics["net_expectancy_pct"] is None


def test_missing_held_stock_bar_is_data_blocked_and_mark_does_not_vanish():
    features = pd.DataFrame([
        _row("2026-07-01", raw_close=10.0),
        _row("2026-07-02", raw_open=10.0, raw_pre_close=10.0),
        # A second security proves 07-03 was a market session while the held
        # security's row is absent.
        _row("2026-07-03", code="000002", raw_close=20.0),
        _row("2026-07-06", raw_open=10.1, raw_pre_close=10.1),
    ])

    metrics, _trades, curve = _simulate_portfolio(
        features,
        pd.DataFrame([_signal()]),
        None,
        start_date=date(2026, 7, 1),
        end_date=date(2026, 7, 6),
    )

    assert metrics["execution_evidence_valid"] is False
    assert metrics["data_gap_count"] >= 1
    assert metrics["execution_reason_counts"]["MISSING_HELD_POSITION_BAR"] == 1
    assert curve[2]["position_count"] == 1
    assert curve[2]["equity"] > curve[2]["cash"]


def test_locked_entry_candidate_remains_in_signal_denominator():
    common = {
        "stock_code": "000001",
        "short_name": "测试股份",
        "name_excluded": 0,
        "amount": 100_000_000.0,
        "amount20": 100_000_000.0,
        "volume": 10_000_000.0,
        "close": 10.0,
        "close_above_ma20": 1,
        "ma20_above_ma60": 1,
        "market_return_20d_pct": 3.0,
        "return_60d_pct": 20.0,
        "return_20d_pct": 10.0,
        "ma20_slope_5d_pct": 1.0,
        "distance_ma20_pct": 3.0,
        "amount_ratio_5_20": 1.2,
        "score": 0.9,
        "initial_stop_pct": -5.0,
        "raw_open": 10.0,
        "raw_high": 10.2,
        "raw_low": 9.9,
        "raw_close": 10.0,
        "raw_pre_close": 9.9,
    }
    signal = {
        **common,
        "trade_date": pd.Timestamp("2026-07-01"),
        "change_pct": 1.0,
    }
    locked_entry = {
        **common,
        "trade_date": pd.Timestamp("2026-07-02"),
        "change_pct": 10.0,
        "raw_open": 10.5,
        "raw_high": 10.5,
        "raw_low": 10.5,
        "raw_close": 10.5,
        "raw_pre_close": 10.0,
    }

    samples = _signal_samples(
        pd.DataFrame([signal, locked_entry]),
        start_date=date(2026, 7, 1),
        end_date=date(2026, 7, 2),
        horizon_days=10,
        top_per_day=10,
    )

    assert len(samples) == 1
    assert samples.iloc[0]["execution_status"] == "KNOWN_UNFILLED"
    assert samples.iloc[0]["exit_reason"] == "LOCKED_LIMIT_UP"
    assert bool(samples.iloc[0]["label_mature"]) is False


def test_missing_holding_session_cannot_be_skipped_inside_calibration_label():
    common = {
        "short_name": "测试股份",
        "name_excluded": 0,
        "amount": 100_000_000.0,
        "amount20": 100_000_000.0,
        "volume": 10_000_000.0,
        "close": 10.0,
        "close_above_ma20": 1,
        "ma20_above_ma60": 1,
        "market_return_20d_pct": 3.0,
        "return_60d_pct": 20.0,
        "return_20d_pct": 10.0,
        "ma20_slope_5d_pct": 1.0,
        "distance_ma20_pct": 3.0,
        "amount_ratio_5_20": 1.2,
        "score": 0.9,
        "initial_stop_pct": -5.0,
        "change_pct": 1.0,
        "raw_open": 10.0,
        "raw_high": 10.2,
        "raw_low": 9.9,
        "raw_close": 10.0,
        "raw_pre_close": 9.9,
    }
    rows = [
        {**common, "stock_code": "000001", "trade_date": pd.Timestamp("2026-07-01")},
        {
            **common,
            "stock_code": "000001",
            "trade_date": pd.Timestamp("2026-07-02"),
            "change_pct": 10.0,
        },
        # This other security establishes a market session on which 000001 is
        # absent. It is deliberately ineligible as a candidate.
        {
            **common,
            "stock_code": "000002",
            "trade_date": pd.Timestamp("2026-07-03"),
            "change_pct": 10.0,
        },
        {
            **common,
            "stock_code": "000001",
            "trade_date": pd.Timestamp("2026-07-06"),
            "change_pct": 10.0,
        },
    ]

    samples = _signal_samples(
        pd.DataFrame(rows),
        start_date=date(2026, 7, 1),
        end_date=date(2026, 7, 6),
        horizon_days=10,
        top_per_day=10,
    )

    assert len(samples) == 1
    assert samples.iloc[0]["execution_status"] == "DATA_BLOCKED"
    assert samples.iloc[0]["exit_reason"] == "MISSING_HOLDING_BAR"
    assert samples.iloc[0]["first_missing_holding_session"] == "2026-07-03"
    assert pd.isna(samples.iloc[0]["net_return_pct"])


def test_overlapping_same_stock_signals_do_not_inflate_calibration_samples():
    base = {
        "stock_code": "000001",
        "short_name": "测试股份",
        "name_excluded": 0,
        "amount": 100_000_000.0,
        "amount20": 100_000_000.0,
        "volume": 10_000_000.0,
        "close": 10.0,
        "close_above_ma20": 1,
        "ma20_above_ma60": 1,
        "market_return_20d_pct": 3.0,
        "return_60d_pct": 20.0,
        "return_20d_pct": 10.0,
        "ma20_slope_5d_pct": 1.0,
        "distance_ma20_pct": 3.0,
        "amount_ratio_5_20": 1.2,
        "score": 0.9,
        "initial_stop_pct": -5.0,
        "change_pct": 10.0,
        "raw_open": 10.0,
        "raw_high": 10.2,
        "raw_low": 9.9,
        "raw_close": 10.0,
        "raw_pre_close": 10.0,
    }
    rows = []
    for day in (
        "2026-07-01", "2026-07-02", "2026-07-03",
        "2026-07-06", "2026-07-07", "2026-07-08",
    ):
        rows.append({**base, "trade_date": pd.Timestamp(day)})
    # Two distinct signal dates pass the setup.
    rows[0]["change_pct"] = 1.0
    rows[3]["change_pct"] = 1.0
    # Each entry session invalidates trend and exits on the following open.
    rows[1]["close_above_ma20"] = 0
    rows[4]["close_above_ma20"] = 0

    samples = _signal_samples(
        pd.DataFrame(rows),
        start_date=date(2026, 7, 1),
        end_date=date(2026, 7, 8),
        horizon_days=10,
        top_per_day=10,
    ).sort_values("trade_date")

    assert samples["label_mature"].tolist() == [True, True]
    assert samples["calibration_eligible"].tolist() == [True, False]
