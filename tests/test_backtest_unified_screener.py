import pandas as pd

from tools.backtest_unified_screener import (
    HORIZONS,
    _benchmark_comparison,
    _data_audit,
    _forward_return,
    _release_decision,
    _summary,
)


def test_forward_return_chains_official_reference_prices():
    dates = ["2026-07-10", "2026-07-13", "2026-07-14", "2026-07-15"]
    prices = {
        ("000001", "2026-07-13"): {
            "open": 10.0,
            "close": 10.5,
            "volume": 100,
            "pre_close": 9.8,
        },
        ("000001", "2026-07-14"): {
            "open": 5.1,
            "close": 5.25,
            "volume": 100,
            # Official ex-right reference is 5.0, not prior raw close 10.5.
            "pre_close": 5.0,
        },
        ("000001", "2026-07-15"): {
            "open": 5.2,
            "close": 5.0,
            "volume": 100,
            "pre_close": 5.25,
        },
    }

    value, reason = _forward_return(
        prices,
        dates,
        {value: index for index, value in enumerate(dates)},
        "2026-07-10",
        "000001",
        3,
    )

    assert reason == "ok"
    assert value is not None
    assert round(value, 6) == 0.05


def test_forward_return_rejects_missing_official_reference():
    dates = ["2026-07-10", "2026-07-13", "2026-07-14"]
    prices = {
        ("000001", "2026-07-13"): {
            "open": 10,
            "close": 10.2,
            "volume": 100,
        },
        ("000001", "2026-07-14"): {
            "close": 10.3,
            "pre_close": None,
        },
    }

    value, reason = _forward_return(
        prices,
        dates,
        {value: index for index, value in enumerate(dates)},
        "2026-07-10",
        "000001",
        2,
    )

    assert value is None
    assert reason == "missing_official_reference_price"


def test_summary_reports_gross_and_cost_adjusted_metrics():
    result = _summary([0.01, -0.005], round_trip_cost=0.002)

    assert result["sample"] == 2
    assert result["gross_average_pct"] == 0.25
    assert result["net_average_pct"] == 0.05
    assert result["net_average_win_loss"] == 1.1429
    assert result["net_max_drawdown_pct"] == 0.7


def test_backtest_uses_confirmed_multi_horizon_contract():
    assert HORIZONS == (1, 5, 20)


def test_release_decision_requires_all_evidence_and_never_grants_orders():
    metrics = {
        horizon: {
            "sample": 100,
            "net_profit_factor": 1.5,
            "net_average_win_loss": 1.2,
        }
        for horizon in ("T+1", "T+5", "T+20")
    }
    audit = {
        "expected_trade_dates": [str(index) for index in range(20)],
        "actual_trade_dates": [str(index) for index in range(20)],
        "row_count": 1000,
        "duplicate_business_keys": 0,
        "bad_ohlc": 0,
        "invalid_prices": 0,
        "missing_pre_close_rows": 0,
        "inconsistent_reference_return_rows": 0,
    }

    result = _release_decision(metrics, audit, 20)

    assert result["status"] == "PASS_ADVISORY_RELEASE"
    assert result["passed"] is True
    assert result["order_authority"] is False
    assert result["automatic_real_order_submission"] is False

    metrics["T+20"]["sample"] = 79
    blocked = _release_decision(metrics, audit, 20)
    assert blocked["status"] == "SHADOW_ONLY"
    assert blocked["checks"]["T+20_evidence"] is False


def test_benchmark_comparison_reports_cost_adjusted_excess_return():
    result = _benchmark_comparison(
        [(0.01, 0.004), (-0.005, -0.01)],
        round_trip_cost=0.002,
    )

    assert result["benchmark_sample"] == 2
    assert result["market_average_pct"] == -0.3
    assert result["gross_excess_average_pct"] == 0.55
    assert result["net_excess_average_pct"] == 0.35


def test_data_audit_detects_inconsistent_pre_close_return():
    frame = pd.DataFrame([{
        "stock_code": "000001",
        "trade_date": "2026-07-13",
        "open": 10,
        "high": 11,
        "low": 9,
        "close": 10.5,
        "volume": 100,
        "amount": 1000,
        "pre_close": 10,
        "change_pct": 1,
    }])

    audit = _data_audit(frame, ["2026-07-13"])

    assert audit["inconsistent_reference_return_rows"] == 1
