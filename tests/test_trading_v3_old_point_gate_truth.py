from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from server.trading_v3 import backtest, engine as engine_module
from server.trading_v3.calibration import CalibrationBucket, CalibrationTable
from server.trading_v3.domain import RawSignal


def _bucket(lower, upper, *, expected, profit_factor, payoff_ratio):
    return CalibrationBucket(
        lower_score=lower,
        upper_score=upper,
        sample_count=100,
        expected_return_net_pct=expected,
        q10_pct=0.1,
        q50_pct=0.5,
        q90_pct=1.0,
        probability_positive=0.6,
        expected_mae_pct=-1.0,
        expected_mfe_pct=2.0,
        profit_factor=profit_factor,
        payoff_ratio=payoff_ratio,
    )


def _table(*, profit_factor, payoff_ratio):
    return CalibrationTable(
        strategy_key="truth-test",
        model_version="truth-test-v1",
        dataset_hash="a" * 64,
        buckets=(
            _bucket(
                0.0,
                0.49,
                expected=0.1,
                profit_factor=1.4,
                payoff_ratio=1.1,
            ),
            _bucket(
                0.5,
                1.0,
                expected=0.8,
                profit_factor=profit_factor,
                payoff_ratio=payoff_ratio,
            ),
        ),
    )


@pytest.mark.parametrize(
    ("profit_factor", "payoff_ratio"),
    [(float("inf"), 1.2), (1.5, float("inf"))],
)
def test_backtest_bucket_gate_rejects_non_finite_point_estimate(
    monkeypatch,
    profit_factor,
    payoff_ratio,
):
    monkeypatch.setattr(
        backtest,
        "load_v3_config",
        lambda: {"profit_gate": {
            "minimum_oos_samples": 80,
            "minimum_profit_factor": 1.3,
            "minimum_payoff_ratio": 1.0,
        }},
    )

    assert backtest._validated_bucket(
        _table(
            profit_factor=profit_factor,
            payoff_ratio=payoff_ratio,
        ),
        0.8,
    ) is False


@pytest.mark.parametrize(
    ("profit_factor", "payoff_ratio"),
    [(float("inf"), 1.2), (1.5, float("inf"))],
)
def test_live_forecast_point_gate_rejects_and_scrubs_non_finite_metric(
    monkeypatch,
    profit_factor,
    payoff_ratio,
):
    monkeypatch.setattr(
        engine_module,
        "load_v3_config",
        lambda: {
            "calibration_version_tokens": {},
            "calibration": {"minimum_bucket_count": 2},
            "paper_discovery": {"enabled": False},
            "profit_gate": {
                "minimum_oos_samples": 80,
                "minimum_expected_return_net_pct": 0.0,
                "minimum_profit_factor": 1.3,
                "minimum_payoff_ratio": 1.0,
            },
        },
    )
    table = _table(
        profit_factor=profit_factor,
        payoff_ratio=payoff_ratio,
    )
    now = datetime(2026, 8, 24, 15, 0)
    forecast = engine_module.TradingV3Engine(
        {"truth-test": table}
    ).forecast(RawSignal(
        stock_code="000001",
        stock_name="test",
        strategy_key="truth-test",
        horizon_days=5,
        score=0.8,
        feature_time=now,
        valid_until=now + timedelta(days=5),
        initial_stop_pct=-5.0,
    ))

    assert forecast.status == "RESEARCH_ONLY_PROFIT_GATE_FAILED"
    if profit_factor == float("inf"):
        assert forecast.profit_factor is None
    if payoff_ratio == float("inf"):
        assert forecast.payoff_ratio is None
