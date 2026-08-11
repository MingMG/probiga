from __future__ import annotations

import numpy as np
import pandas as pd

from tools.backtest_etf_ensemble import (
    MarketData,
    build_target_schedule,
    simulate,
)


def _market_data() -> MarketData:
    dates = pd.bdate_range("2024-01-02", periods=620)
    trend = pd.Series(
        np.linspace(1.0, 1.8, len(dates)),
        index=dates,
    )
    cash = pd.Series(
        np.linspace(100.0, 101.0, len(dates)),
        index=dates,
    )
    close = pd.DataFrame({"510300": trend, "511880": cash})
    return MarketData(
        open=close.copy(),
        close=close,
        amount=pd.DataFrame(
            {
                "510300": 100_000_000.0,
                "511880": 100_000_000.0,
            },
            index=dates,
        ),
        names={"510300": "沪深300ETF", "511880": "银华日利ETF"},
        asset_classes={"510300": "A股宽基", "511880": "现金管理"},
    )


def test_signals_execute_only_after_signal_close() -> None:
    data = _market_data()
    targets, records = build_target_schedule(
        data,
        backtest_start="2025-02-01",
        end_date=str(data.calendar.max().date()),
        mode="trend_risk",
        execution_lag=2,
    )
    assert targets
    for record in records:
        signal = pd.Timestamp(record["signal_date"])
        execution = pd.Timestamp(record["execution_date"])
        assert execution > signal
        later = data.calendar[data.calendar > signal]
        assert execution == later[1]


def test_double_cost_stress_cannot_improve_same_trade_path() -> None:
    data = _market_data()
    dates = data.calendar
    targets = {
        dates[300]: pd.Series({"510300": 0.60, "511880": 0.40}),
        dates[360]: pd.Series({"510300": 0.20, "511880": 0.80}),
        dates[420]: pd.Series({"510300": 0.70, "511880": 0.30}),
    }
    base, _ = simulate(
        data,
        targets,
        end_date=str(dates[500].date()),
        cost_multiplier=1.0,
    )
    stressed, _ = simulate(
        data,
        targets,
        end_date=str(dates[500].date()),
        cost_multiplier=2.0,
    )
    assert stressed.iloc[-1] < base.iloc[-1]
    assert base.index.equals(stressed.index)
