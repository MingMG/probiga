from __future__ import annotations

import numpy as np
import pandas as pd

from tools.backtest_etf_ensemble import MarketData
from tools.backtest_etf_robust import (
    ExecutionAssumptions,
    build_fast_risk_schedule,
    freeze_universe,
    moving_block_bootstrap,
    simulate_realistic,
    subset_market_data,
)


def _market_data() -> MarketData:
    dates = pd.bdate_range("2020-01-02", periods=520)
    rising = np.linspace(1.0, 1.8, len(dates))
    falling_tail = rising.copy()
    falling_tail[-20:] = np.linspace(1.8, 1.45, 20)
    close = pd.DataFrame(
        {
            "510300": falling_tail,
            "511880": np.linspace(100.0, 101.0, len(dates)),
        },
        index=dates,
    )
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


def test_frozen_universe_uses_only_cutoff_information() -> None:
    data = _market_data()
    data.close.loc[: "2020-04-30", "510300"] = np.nan
    data.open.loc[: "2020-04-30", "510300"] = np.nan
    frozen, audit = freeze_universe(
        data,
        cutoff_date="2020-12-31",
        minimum_history_days=120,
    )
    assert "510300" in frozen.close.columns
    row = audit.set_index("etf_code").loc["510300"]
    assert row["first_observed_date"] >= "2020-05-01"
    assert bool(row["eligible"])


def test_fast_risk_exit_is_next_day_not_same_close() -> None:
    data = _market_data()
    dates = data.calendar
    monthly = {
        dates[300]: pd.Series({"510300": 1.0}),
    }
    schedule, contexts, exits = build_fast_risk_schedule(
        data,
        monthly,
        end_date=str(dates[-1].date()),
        risk_mode="daily_vol_stop",
    )
    assert not exits.empty
    first = exits.iloc[0]
    assert pd.Timestamp(first["execution_date"]) > pd.Timestamp(
        first["signal_date"]
    )
    execution = pd.Timestamp(first["execution_date"])
    assert contexts[execution]["event_type"] == "fast_risk_exit"
    assert schedule[execution].get("511880", 0.0) > 0


def test_fast_risk_exit_can_reenter_after_confirmed_trend_recovery() -> None:
    data = _market_data()
    dates = data.calendar
    data.close.loc[dates[300] : dates[310], "510300"] = 1.8
    data.open.loc[dates[300] : dates[310], "510300"] = 1.8
    data.close.loc[dates[311] : dates[312], "510300"] = 1.45
    data.open.loc[dates[311] : dates[312], "510300"] = 1.45
    data.close.loc[dates[313] :, "510300"] = 1.9
    data.open.loc[dates[313] :, "510300"] = 1.9
    monthly = {dates[300]: pd.Series({"510300": 1.0})}

    schedule, contexts, exits = build_fast_risk_schedule(
        data,
        monthly,
        end_date=str(dates[-1].date()),
        risk_mode="daily_vol_stop",
        reentry_mode="trend_resume",
        reentry_cooldown_days=1,
    )

    assert not exits.empty
    reentry_days = [
        day
        for day, context in contexts.items()
        if context.get("event_type") == "fast_risk_reentry"
    ]
    assert reentry_days
    first_reentry = reentry_days[0]
    assert schedule[first_reentry].get("510300", 0.0) > 0


def test_realistic_execution_uses_board_lots_and_minimum_fee() -> None:
    data = _market_data()
    day = data.calendar[300]
    targets = {day: pd.Series({"510300": 1.0})}
    equity, rebalances, trades = simulate_realistic(
        data,
        targets,
        contexts={day: {"event_type": "test"}},
        end_date=str(data.calendar[305].date()),
        assumptions=ExecutionAssumptions(initial_capital=10_000.0),
    )
    filled = trades.loc[trades["status"] == "filled"]
    assert not filled.empty
    assert all(filled["filled_units"].astype(int) % 100 == 0)
    assert filled["commission"].min() >= 5.0
    assert equity.iloc[-1] > 0
    assert rebalances.iloc[0]["event_type"] == "test"


def test_subset_market_data_removes_only_requested_assets() -> None:
    data = _market_data()
    reduced = subset_market_data(data, {"unused"})
    assert list(reduced.close.columns) == list(data.close.columns)
    with np.testing.assert_raises(ValueError):
        subset_market_data(data, {"510300"})


def test_moving_block_bootstrap_is_deterministic() -> None:
    dates = pd.bdate_range("2021-01-01", periods=120)
    returns = np.tile([0.002, -0.001, 0.0015], 40)
    equity = pd.Series(
        np.cumprod(1.0 + returns),
        index=dates,
    )
    first = moving_block_bootstrap(
        equity,
        simulations=50,
        block_days=10,
        seed=7,
    )
    second = moving_block_bootstrap(
        equity,
        simulations=50,
        block_days=10,
        seed=7,
    )
    assert first == second
    assert first["probability_total_return_positive"] == 1.0
