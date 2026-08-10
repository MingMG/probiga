# -*- coding: utf-8 -*-
"""Robust ETF backtest with point-in-time universe and realistic execution.

The strategy signal remains the fixed trend/risk-budget model from
``backtest_etf_ensemble.py``. This second-stage harness addresses research
limitations that a normalized, friction-only simulation cannot:

* freeze the investable universe at a historical cutoff;
* execute only on the next trading-day open;
* apply integer board lots, minimum commission, spread/impact and ADV caps;
* reject missing/limit-locked orders and retry capacity-limited orders;
* optionally add a predeclared short-horizon risk overlay;
* run cost, capital-size and subperiod stress tests.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.backtest_etf_ensemble import (
    CASH_CODE,
    MarketData,
    build_target_schedule,
    load_market_data,
    performance_metrics,
    split_metrics,
)
from tools.env_config import create_tool_engine, load_project_env


RISK_MODES = ("none", "weekly_trend", "daily_vol_stop", "combined")
LOT_SIZE = 100
LIMIT_LOCK_RATIO_BY_CODE = {
    # 159915 tracks ChiNext constituents, whose price limits are 20%.
    "159915": 0.198,
}


@dataclass(frozen=True)
class ExecutionAssumptions:
    initial_capital: float = 1_000_000.0
    minimum_commission: float = 5.0
    max_adv_participation: float = 0.02
    impact_at_one_pct: float = 0.0008
    max_impact: float = 0.0050
    limit_lock_ratio: float = 0.098
    cost_multiplier: float = 1.0
    adverse_open_gap_rate: float = 0.0


def _limit_lock_ratio(
    code: str,
    assumptions: ExecutionAssumptions,
) -> float:
    return LIMIT_LOCK_RATIO_BY_CODE.get(
        code,
        assumptions.limit_lock_ratio,
    )


def freeze_universe(
    data: MarketData,
    *,
    cutoff_date: str,
    minimum_history_days: int = 120,
    minimum_average_amount: float = 5_000_000.0,
) -> tuple[MarketData, pd.DataFrame]:
    """Freeze eligible products using only observations available at cutoff."""
    cutoff = pd.Timestamp(cutoff_date)
    records: list[dict[str, Any]] = []
    keep: list[str] = []
    for code in data.close.columns:
        close = data.close.loc[:cutoff, code].dropna()
        amount = data.amount.loc[:cutoff, code].dropna().tail(20)
        history_days = int(len(close))
        avg_amount = float(amount.mean()) if not amount.empty else 0.0
        eligible = (
            history_days >= minimum_history_days
            and len(amount) >= 15
            and avg_amount >= minimum_average_amount
        )
        records.append(
            {
                "etf_code": code,
                "short_name": data.names.get(code, ""),
                "asset_class": data.asset_classes.get(code, ""),
                "first_observed_date": (
                    close.index.min().date().isoformat()
                    if not close.empty
                    else None
                ),
                "cutoff_date": cutoff.date().isoformat(),
                "history_days_at_cutoff": history_days,
                "average_amount_20_at_cutoff": avg_amount,
                "eligible": eligible,
                "exclusion_reason": (
                    ""
                    if eligible
                    else "insufficient_history"
                    if history_days < minimum_history_days
                    else "insufficient_liquidity"
                ),
            }
        )
        if eligible:
            keep.append(code)
    if CASH_CODE not in keep:
        raise RuntimeError(f"cash proxy {CASH_CODE} is not cutoff-eligible")
    if "510300" not in keep:
        raise RuntimeError("510300 is required as the strategy calendar/regime proxy")
    frozen = MarketData(
        open=data.open.loc[:, keep],
        close=data.close.loc[:, keep],
        amount=data.amount.loc[:, keep],
        names={code: data.names.get(code, "") for code in keep},
        asset_classes={
            code: data.asset_classes.get(code, "")
            for code in keep
        },
    )
    return frozen, pd.DataFrame(records).sort_values("etf_code")


def _last_trading_day_of_week(
    calendar: pd.DatetimeIndex,
    offset: int,
) -> bool:
    if offset + 1 >= len(calendar):
        return True
    current = calendar[offset]
    following = calendar[offset + 1]
    return (
        current.isocalendar().year,
        current.isocalendar().week,
    ) != (
        following.isocalendar().year,
        following.isocalendar().week,
    )


def build_fast_risk_schedule(
    data: MarketData,
    monthly_targets: dict[pd.Timestamp, pd.Series],
    *,
    end_date: str,
    risk_mode: str,
    volatility_multiplier: float = 3.0,
    minimum_stop: float = 0.06,
    maximum_stop: float = 0.15,
    reentry_mode: str = "none",
    reentry_cooldown_days: int = 3,
) -> tuple[
    dict[pd.Timestamp, pd.Series],
    dict[pd.Timestamp, dict[str, Any]],
    pd.DataFrame,
]:
    """Add risk exits signalled at close and executed at next-day open."""
    if risk_mode not in RISK_MODES:
        raise ValueError(f"unsupported risk mode: {risk_mode}")
    if reentry_mode not in {"none", "trend_resume"}:
        raise ValueError(f"unsupported reentry mode: {reentry_mode}")
    if reentry_cooldown_days < 0:
        raise ValueError("reentry_cooldown_days must be non-negative")
    calendar = data.calendar
    calendar = calendar[
        (calendar >= min(monthly_targets))
        & (calendar <= pd.Timestamp(end_date))
    ]
    base_targets = {
        pd.Timestamp(day): weights.copy()
        for day, weights in monthly_targets.items()
    }
    schedule = {
        day: weights.copy()
        for day, weights in base_targets.items()
    }
    contexts: dict[pd.Timestamp, dict[str, Any]] = {
        day: {"event_type": "monthly_rebalance", "risk_mode": risk_mode}
        for day in base_targets
    }
    pending: dict[pd.Timestamp, pd.Series] = {}
    pending_context: dict[pd.Timestamp, dict[str, Any]] = {}
    current_target: pd.Series | None = None
    strategic_target: pd.Series | None = None
    peaks: dict[str, float] = {}
    exited: dict[str, dict[str, Any]] = {}
    exit_rows: list[dict[str, Any]] = []

    for offset, day in enumerate(calendar):
        if day in base_targets:
            current_target = base_targets[day].copy()
            strategic_target = base_targets[day].copy()
            peaks = {}
            exited = {}
        elif day in pending:
            current_target = pending[day].copy()
            contexts[day] = pending_context[day]
        if current_target is None:
            continue

        for code, weight in current_target.items():
            if code == CASH_CODE or float(weight) <= 1e-8:
                continue
            close_value = data.close.at[day, code]
            if pd.notna(close_value) and float(close_value) > 0:
                peaks[code] = max(
                    float(close_value),
                    float(peaks.get(code, close_value)),
                )

        if risk_mode == "none" or offset + 1 >= len(calendar):
            continue
        execution_date = pd.Timestamp(calendar[offset + 1])
        # A new monthly decision supersedes an exit based on the old target.
        if execution_date in base_targets:
            continue

        weekly_check = _last_trading_day_of_week(calendar, offset)
        exits: list[dict[str, Any]] = []
        for code, weight in current_target.items():
            if code == CASH_CODE or float(weight) <= 1e-8:
                continue
            series = data.close.loc[:day, code].dropna()
            if len(series) < 55 or series.index[-1] != day:
                continue
            close_value = float(series.iloc[-1])
            ma50 = float(series.tail(50).mean())
            return20 = close_value / float(series.iloc[-21]) - 1.0
            daily_returns = series.pct_change().dropna().tail(20)
            daily_vol20 = float(daily_returns.std(ddof=1))
            peak = float(peaks.get(code, close_value))
            drawdown_from_peak = close_value / peak - 1.0
            stop_threshold = float(
                np.clip(
                    volatility_multiplier * daily_vol20,
                    minimum_stop,
                    maximum_stop,
                )
            )
            trend_break = (
                risk_mode in {"weekly_trend", "combined"}
                and weekly_check
                and close_value < ma50
                and return20 < 0
            )
            volatility_stop = (
                risk_mode in {"daily_vol_stop", "combined"}
                and drawdown_from_peak <= -stop_threshold
            )
            if not (trend_break or volatility_stop):
                continue
            reason = (
                "weekly_ma50_and_return20"
                if trend_break
                else "volatility_scaled_trailing_stop"
            )
            exits.append(
                {
                    "signal_date": day,
                    "execution_date": execution_date,
                    "etf_code": code,
                    "short_name": data.names.get(code, ""),
                    "asset_class": data.asset_classes.get(code, ""),
                    "reason": reason,
                    "close": close_value,
                    "ma50": ma50,
                    "return20": return20,
                    "peak_close": peak,
                    "drawdown_from_peak": drawdown_from_peak,
                    "stop_threshold": stop_threshold,
                    "exited_weight": float(weight),
                    "risk_mode": risk_mode,
                }
            )

        reentries: list[dict[str, Any]] = []
        if (
            reentry_mode == "trend_resume"
            and strategic_target is not None
        ):
            for code, state in sorted(exited.items()):
                if offset - int(state["exit_offset"]) < reentry_cooldown_days:
                    continue
                series = data.close.loc[:day, code].dropna()
                if len(series) < 21 or series.index[-1] != day:
                    continue
                close_value = float(series.iloc[-1])
                ma20 = float(series.tail(20).mean())
                return20 = close_value / float(series.iloc[-21]) - 1.0
                if close_value <= ma20 or return20 <= 0:
                    continue
                desired_weight = float(strategic_target.get(code, 0.0))
                if desired_weight <= 1e-8:
                    continue
                reentries.append(
                    {
                        "signal_date": day,
                        "execution_date": execution_date,
                        "etf_code": code,
                        "reason": "ma20_and_return20_recovery",
                        "close": close_value,
                        "ma20": ma20,
                        "return20": return20,
                        "restored_weight": desired_weight,
                    }
                )

        if not exits and not reentries:
            continue
        revised = current_target.copy()
        exited_weight = 0.0
        for item in exits:
            code = str(item["etf_code"])
            removed_weight = float(revised.get(code, 0.0))
            exited_weight += removed_weight
            revised.loc[code] = 0.0
            peaks.pop(code, None)
            exited[code] = {
                "exit_offset": offset,
                "exited_weight": removed_weight,
            }
        cash_weight = float(revised.get(CASH_CODE, 0.0)) + exited_weight
        restored_codes: list[str] = []
        for item in reentries:
            code = str(item["etf_code"])
            restored_weight = min(
                float(item["restored_weight"]),
                cash_weight,
            )
            if restored_weight <= 1e-8:
                continue
            revised.loc[code] = (
                float(revised.get(code, 0.0)) + restored_weight
            )
            cash_weight -= restored_weight
            peaks[code] = float(item["close"])
            exited.pop(code, None)
            restored_codes.append(code)
        revised.loc[CASH_CODE] = cash_weight
        revised = revised[revised > 1e-8]
        revised /= revised.sum()
        pending[execution_date] = revised
        pending_context[execution_date] = {
            "event_type": (
                "fast_risk_exit_and_reentry"
                if exits and restored_codes
                else "fast_risk_exit"
                if exits
                else "fast_risk_reentry"
            ),
            "risk_mode": risk_mode,
            "reasons": sorted({str(item["reason"]) for item in exits}),
            "exited_codes": [str(item["etf_code"]) for item in exits],
            "reentry_mode": reentry_mode,
            "restored_codes": restored_codes,
        }
        schedule[execution_date] = revised
        exit_rows.extend(exits)

    return (
        dict(sorted(schedule.items())),
        contexts,
        pd.DataFrame(exit_rows),
    )


def _commission_rate(code: str, data: MarketData) -> float:
    if code == CASH_CODE:
        return 0.0001
    return 0.0003


def _half_spread(code: str, data: MarketData) -> float:
    asset_class = data.asset_classes.get(code, "")
    if code == CASH_CODE:
        return 0.00005
    if asset_class in {"债券"}:
        return 0.00010
    if asset_class in {"海外权益", "港股权益", "商品"}:
        return 0.00050
    return 0.00020


def _adv_before(
    data: MarketData,
    day: pd.Timestamp,
    code: str,
) -> float:
    history = data.amount.loc[:day, code].dropna()
    if len(history) > 1:
        history = history.iloc[:-1]
    return float(history.tail(20).mean()) if not history.empty else 0.0


def _slippage_rate(
    code: str,
    order_notional: float,
    adv20: float,
    data: MarketData,
    assumptions: ExecutionAssumptions,
) -> tuple[float, float]:
    participation = (
        max(0.0, float(order_notional)) / adv20
        if adv20 > 0
        else assumptions.max_adv_participation
    )
    impact = assumptions.impact_at_one_pct * math.sqrt(
        max(0.0, participation) / 0.01
    )
    impact = min(assumptions.max_impact, impact)
    rate = (
        _half_spread(code, data) + impact
    ) * assumptions.cost_multiplier
    return rate, participation


def _commission(
    code: str,
    notional: float,
    data: MarketData,
    assumptions: ExecutionAssumptions,
) -> float:
    if notional <= 0:
        return 0.0
    return max(
        assumptions.minimum_commission,
        notional
        * _commission_rate(code, data)
        * assumptions.cost_multiplier,
    )


def _target_units(
    target_value: float,
    reference_price: float,
) -> int:
    if target_value <= 0 or reference_price <= 0:
        return 0
    return int(target_value / reference_price / LOT_SIZE) * LOT_SIZE


def simulate_realistic(
    data: MarketData,
    targets: dict[pd.Timestamp, pd.Series],
    *,
    contexts: dict[pd.Timestamp, dict[str, Any]] | None,
    end_date: str,
    assumptions: ExecutionAssumptions,
) -> tuple[pd.Series, pd.DataFrame, pd.DataFrame]:
    """Execute target weights with lots, minimum fees, spread and capacity."""
    first_date = min(targets)
    calendar = data.calendar
    calendar = calendar[
        (calendar >= first_date)
        & (calendar <= pd.Timestamp(end_date))
    ]
    codes = list(data.close.columns)
    close_panel = data.close.reindex(calendar).ffill()
    open_panel = data.open.reindex(calendar)
    units = pd.Series(0, index=codes, dtype="int64")
    average_cost = pd.Series(0.0, index=codes)
    cash = float(assumptions.initial_capital)
    active_target: pd.Series | None = None
    retry_target = False
    equity_rows: list[tuple[pd.Timestamp, float]] = []
    trade_rows: list[dict[str, Any]] = []
    rebalance_rows: list[dict[str, Any]] = []

    for offset, day in enumerate(calendar):
        previous_close = (
            close_panel.iloc[offset - 1]
            if offset > 0
            else close_panel.loc[day]
        )
        raw_open = open_panel.loc[day]
        mark_open = raw_open.where(
            raw_open.notna() & (raw_open > 0),
            previous_close,
        )
        event = day in targets
        if event:
            active_target = targets[day].reindex(codes).fillna(0.0)
            active_target /= active_target.sum()
            retry_target = True

        if active_target is not None and (event or retry_target):
            portfolio_open = float((units * mark_open).sum() + cash)
            desired_value = active_target * portfolio_open
            desired_units = pd.Series(
                {
                    code: _target_units(
                        float(desired_value.get(code, 0.0)),
                        float(mark_open.get(code, 0.0)),
                    )
                    for code in codes
                },
                dtype="int64",
            )
            day_notional = 0.0
            day_commission = 0.0
            day_impact = 0.0
            blocked_orders = 0
            partial_orders = 0

            # Sell first so buys use actually available cash.
            for code in codes:
                requested_units = int(units[code] - desired_units[code])
                if requested_units < LOT_SIZE:
                    continue
                open_price = raw_open.get(code)
                prev_price = previous_close.get(code)
                if pd.isna(open_price) or float(open_price) <= 0:
                    blocked_orders += 1
                    trade_rows.append(
                        {
                            "trade_date": day,
                            "etf_code": code,
                            "side": "SELL",
                            "status": "blocked_no_open",
                            "requested_units": requested_units,
                        }
                    )
                    continue
                execution_open = float(open_price) * (
                    1.0 - assumptions.adverse_open_gap_rate
                )
                if (
                    pd.notna(prev_price)
                    and float(prev_price) > 0
                    and execution_open / float(prev_price) - 1.0
                    <= -_limit_lock_ratio(code, assumptions)
                ):
                    blocked_orders += 1
                    trade_rows.append(
                        {
                            "trade_date": day,
                            "etf_code": code,
                            "side": "SELL",
                            "status": "blocked_limit_down",
                            "requested_units": requested_units,
                        }
                    )
                    continue
                adv20 = _adv_before(data, day, code)
                max_notional = (
                    adv20 * assumptions.max_adv_participation
                )
                max_units = (
                    int(max_notional / execution_open / LOT_SIZE)
                    * LOT_SIZE
                    if max_notional > 0
                    else 0
                )
                fill_units = min(requested_units, max_units)
                if fill_units < LOT_SIZE:
                    blocked_orders += 1
                    trade_rows.append(
                        {
                            "trade_date": day,
                            "etf_code": code,
                            "side": "SELL",
                            "status": "blocked_capacity",
                            "requested_units": requested_units,
                            "adv20": adv20,
                        }
                    )
                    continue
                if fill_units < requested_units:
                    partial_orders += 1
                open_notional = fill_units * execution_open
                slip_rate, participation = _slippage_rate(
                    code,
                    open_notional,
                    adv20,
                    data,
                    assumptions,
                )
                execution_price = execution_open * (1.0 - slip_rate)
                notional = fill_units * execution_price
                commission = _commission(
                    code,
                    notional,
                    data,
                    assumptions,
                )
                realized_pnl = (
                    notional
                    - commission
                    - fill_units * float(average_cost[code])
                )
                cash += notional - commission
                units[code] -= fill_units
                if units[code] <= 0:
                    units[code] = 0
                    average_cost[code] = 0.0
                day_notional += notional
                day_commission += commission
                impact_cost = fill_units * (
                    execution_open - execution_price
                )
                day_impact += impact_cost
                trade_rows.append(
                    {
                        "trade_date": day,
                        "etf_code": code,
                        "short_name": data.names.get(code, ""),
                        "side": "SELL",
                        "status": (
                            "partial_capacity"
                            if fill_units < requested_units
                            else "filled"
                        ),
                        "requested_units": requested_units,
                        "filled_units": fill_units,
                        "open_price": execution_open,
                        "execution_price": execution_price,
                        "notional": notional,
                        "commission": commission,
                        "impact_cost": impact_cost,
                        "participation": participation,
                        "realized_pnl": realized_pnl,
                    }
                )

            for code in codes:
                requested_units = int(desired_units[code] - units[code])
                if requested_units < LOT_SIZE:
                    continue
                open_price = raw_open.get(code)
                prev_price = previous_close.get(code)
                if pd.isna(open_price) or float(open_price) <= 0:
                    blocked_orders += 1
                    trade_rows.append(
                        {
                            "trade_date": day,
                            "etf_code": code,
                            "side": "BUY",
                            "status": "blocked_no_open",
                            "requested_units": requested_units,
                        }
                    )
                    continue
                execution_open = float(open_price) * (
                    1.0 + assumptions.adverse_open_gap_rate
                )
                if (
                    pd.notna(prev_price)
                    and float(prev_price) > 0
                    and execution_open / float(prev_price) - 1.0
                    >= _limit_lock_ratio(code, assumptions)
                ):
                    blocked_orders += 1
                    trade_rows.append(
                        {
                            "trade_date": day,
                            "etf_code": code,
                            "side": "BUY",
                            "status": "blocked_limit_up",
                            "requested_units": requested_units,
                        }
                    )
                    continue
                adv20 = _adv_before(data, day, code)
                max_notional = (
                    adv20 * assumptions.max_adv_participation
                )
                max_units = (
                    int(max_notional / execution_open / LOT_SIZE)
                    * LOT_SIZE
                    if max_notional > 0
                    else 0
                )
                fill_units = min(requested_units, max_units)
                if fill_units < LOT_SIZE:
                    blocked_orders += 1
                    trade_rows.append(
                        {
                            "trade_date": day,
                            "etf_code": code,
                            "side": "BUY",
                            "status": "blocked_capacity",
                            "requested_units": requested_units,
                            "adv20": adv20,
                        }
                    )
                    continue
                if fill_units < requested_units:
                    partial_orders += 1
                open_notional = fill_units * execution_open
                slip_rate, participation = _slippage_rate(
                    code,
                    open_notional,
                    adv20,
                    data,
                    assumptions,
                )
                execution_price = execution_open * (1.0 + slip_rate)
                # Reduce one board lot at a time until cash covers price + fee.
                while fill_units >= LOT_SIZE:
                    notional = fill_units * execution_price
                    commission = _commission(
                        code,
                        notional,
                        data,
                        assumptions,
                    )
                    if notional + commission <= cash + 1e-8:
                        break
                    fill_units -= LOT_SIZE
                if fill_units < LOT_SIZE:
                    blocked_orders += 1
                    trade_rows.append(
                        {
                            "trade_date": day,
                            "etf_code": code,
                            "side": "BUY",
                            "status": "blocked_cash",
                            "requested_units": requested_units,
                        }
                    )
                    continue
                notional = fill_units * execution_price
                commission = _commission(
                    code,
                    notional,
                    data,
                    assumptions,
                )
                old_units = int(units[code])
                old_basis = old_units * float(average_cost[code])
                cash -= notional + commission
                units[code] += fill_units
                average_cost[code] = (
                    old_basis + notional + commission
                ) / int(units[code])
                day_notional += notional
                day_commission += commission
                impact_cost = fill_units * (
                    execution_price - execution_open
                )
                day_impact += impact_cost
                trade_rows.append(
                    {
                        "trade_date": day,
                        "etf_code": code,
                        "short_name": data.names.get(code, ""),
                        "side": "BUY",
                        "status": (
                            "partial_capacity_or_cash"
                            if fill_units < requested_units
                            else "filled"
                        ),
                        "requested_units": requested_units,
                        "filled_units": fill_units,
                        "open_price": execution_open,
                        "execution_price": execution_price,
                        "notional": notional,
                        "commission": commission,
                        "impact_cost": impact_cost,
                        "participation": participation,
                        "realized_pnl": 0.0,
                    }
                )

            remaining = (desired_units - units).abs()
            retry_target = bool((remaining >= LOT_SIZE).any())
            event_context = (contexts or {}).get(day, {})
            rebalance_rows.append(
                {
                    "execution_date": day,
                    "event_type": event_context.get(
                        "event_type",
                        "capacity_retry" if not event else "rebalance",
                    ),
                    "risk_mode": event_context.get("risk_mode", ""),
                    "portfolio_open": portfolio_open,
                    "turnover": (
                        day_notional / max(portfolio_open, 1e-12)
                    ),
                    "commission": day_commission,
                    "spread_impact_cost": day_impact,
                    "blocked_orders": blocked_orders,
                    "partial_orders": partial_orders,
                    "retry_required": retry_target,
                    "cash_after": cash,
                }
            )

        closing_prices = close_panel.loc[day]
        equity = float((units * closing_prices).sum() + cash)
        equity_rows.append((day, equity))

    equity = pd.Series(
        [value for _, value in equity_rows],
        index=pd.DatetimeIndex([day for day, _ in equity_rows]),
        name="equity",
    )
    return equity, pd.DataFrame(rebalance_rows), pd.DataFrame(trade_rows)


def realized_trade_metrics(trades: pd.DataFrame) -> dict[str, Any]:
    if trades.empty or "realized_pnl" not in trades:
        return {
            "sell_events": 0,
            "win_rate": None,
            "profit_factor": None,
            "net_realized_pnl": 0.0,
        }
    sells = trades.loc[
        (trades.get("side") == "SELL")
        & trades["realized_pnl"].notna()
    ].copy()
    if sells.empty:
        return {
            "sell_events": 0,
            "win_rate": None,
            "profit_factor": None,
            "net_realized_pnl": 0.0,
        }
    wins = sells.loc[sells["realized_pnl"] > 0, "realized_pnl"]
    losses = sells.loc[sells["realized_pnl"] < 0, "realized_pnl"]
    return {
        "sell_events": int(len(sells)),
        "win_rate": float((sells["realized_pnl"] > 0).mean()),
        "profit_factor": (
            float(wins.sum() / abs(losses.sum()))
            if not losses.empty and abs(float(losses.sum())) > 0
            else None
        ),
        "net_realized_pnl": float(sells["realized_pnl"].sum()),
    }


def _safe_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _safe_json(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_safe_json(item) for item in value]
    if isinstance(value, (pd.Timestamp, pd.Period)):
        return str(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return (
            None
            if not math.isfinite(float(value))
            else float(value)
        )
    if isinstance(value, float):
        return None if not math.isfinite(value) else value
    return value


def subset_market_data(
    data: MarketData,
    excluded_codes: set[str],
) -> MarketData:
    """Return a strategy-ready view after removing specified risky assets."""
    protected = {CASH_CODE, "510300"}
    overlap = protected & excluded_codes
    if overlap:
        raise ValueError(
            f"cannot exclude required codes: {sorted(overlap)}"
        )
    keep = [
        code
        for code in data.close.columns
        if code not in excluded_codes
    ]
    return MarketData(
        open=data.open.loc[:, keep],
        close=data.close.loc[:, keep],
        amount=data.amount.loc[:, keep],
        names={code: data.names.get(code, "") for code in keep},
        asset_classes={
            code: data.asset_classes.get(code, "")
            for code in keep
        },
    )


def moving_block_bootstrap(
    equity: pd.Series,
    *,
    simulations: int = 2_000,
    block_days: int = 20,
    seed: int = 20260725,
) -> dict[str, Any]:
    """Estimate path uncertainty by resampling contiguous daily-return blocks."""
    returns = (
        equity.astype(float)
        .pct_change()
        .replace([np.inf, -np.inf], np.nan)
        .dropna()
        .to_numpy()
    )
    if len(returns) < block_days * 2:
        raise ValueError("not enough observations for block bootstrap")
    rng = np.random.default_rng(seed)
    starts = np.arange(0, len(returns) - block_days + 1)
    rows: list[dict[str, float]] = []
    blocks_needed = math.ceil(len(returns) / block_days)
    for _ in range(simulations):
        sampled_starts = rng.choice(
            starts,
            size=blocks_needed,
            replace=True,
        )
        sampled = np.concatenate(
            [
                returns[start : start + block_days]
                for start in sampled_starts
            ]
        )[: len(returns)]
        path = np.cumprod(1.0 + sampled)
        total_return = float(path[-1] - 1.0)
        cagr = float(
            path[-1] ** (252.0 / len(sampled)) - 1.0
        )
        daily_std = float(np.std(sampled, ddof=1))
        sharpe = (
            float(np.mean(sampled) / daily_std * math.sqrt(252))
            if daily_std > 0
            else 0.0
        )
        running_peak = np.maximum.accumulate(path)
        max_drawdown = float(np.min(path / running_peak - 1.0))
        rows.append(
            {
                "total_return": total_return,
                "cagr": cagr,
                "sharpe": sharpe,
                "max_drawdown": max_drawdown,
            }
        )
    frame = pd.DataFrame(rows)

    def interval(column: str) -> dict[str, float]:
        quantiles = frame[column].quantile([0.025, 0.5, 0.975])
        return {
            "p2_5": float(quantiles.loc[0.025]),
            "median": float(quantiles.loc[0.5]),
            "p97_5": float(quantiles.loc[0.975]),
        }

    return {
        "method": "moving_block_bootstrap",
        "simulations": simulations,
        "block_days": block_days,
        "seed": seed,
        "probability_total_return_positive": float(
            (frame["total_return"] > 0).mean()
        ),
        "probability_cagr_positive": float(
            (frame["cagr"] > 0).mean()
        ),
        "probability_max_drawdown_below_minus_15pct": float(
            (frame["max_drawdown"] < -0.15).mean()
        ),
        "intervals": {
            column: interval(column)
            for column in (
                "total_return",
                "cagr",
                "sharpe",
                "max_drawdown",
            )
        },
        "caveat": (
            "Resamples the observed return process; it does not prove "
            "future profitability or model structural breaks."
        ),
    }


def _run_case(
    data: MarketData,
    monthly_targets: dict[pd.Timestamp, pd.Series],
    *,
    end_date: str,
    risk_mode: str,
    assumptions: ExecutionAssumptions,
    output_dir: Path,
    label: str,
) -> dict[str, Any]:
    schedule, contexts, exits = build_fast_risk_schedule(
        data,
        monthly_targets,
        end_date=end_date,
        risk_mode=risk_mode,
    )
    equity, rebalances, trades = simulate_realistic(
        data,
        schedule,
        contexts=contexts,
        end_date=end_date,
        assumptions=assumptions,
    )
    normalized = equity / assumptions.initial_capital
    metrics = performance_metrics(normalized)
    result = {
        "label": label,
        "risk_mode": risk_mode,
        "assumptions": asdict(assumptions),
        "performance": metrics,
        "subperiods": split_metrics(normalized),
        "realized_trades": realized_trade_metrics(trades),
        "risk_exit_events": int(len(exits)),
        "rebalance_attempts": int(len(rebalances)),
        "commission_total": (
            float(rebalances["commission"].sum())
            if not rebalances.empty
            else 0.0
        ),
        "spread_impact_total": (
            float(rebalances["spread_impact_cost"].sum())
            if not rebalances.empty
            else 0.0
        ),
        "blocked_orders": (
            int(rebalances["blocked_orders"].sum())
            if not rebalances.empty
            else 0
        ),
        "partial_orders": (
            int(rebalances["partial_orders"].sum())
            if not rebalances.empty
            else 0
        ),
    }
    normalized.rename("equity").to_csv(
        output_dir / f"equity_{label}.csv",
        header=True,
    )
    rebalances.to_csv(
        output_dir / f"rebalances_{label}.csv",
        index=False,
        encoding="utf-8-sig",
    )
    trades.to_csv(
        output_dir / f"trades_{label}.csv",
        index=False,
        encoding="utf-8-sig",
    )
    exits.to_csv(
        output_dir / f"risk_exits_{label}.csv",
        index=False,
        encoding="utf-8-sig",
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-start", default="2019-01-01")
    parser.add_argument("--universe-cutoff", default="2020-12-31")
    parser.add_argument("--backtest-start", default="2021-01-04")
    parser.add_argument("--end", default="2026-07-24")
    parser.add_argument("--initial-capital", type=float, default=1_000_000)
    parser.add_argument(
        "--proposed-risk-mode",
        choices=RISK_MODES,
        default="daily_vol_stop",
        help="Predeclared candidate evaluated by adoption gates.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(
            ROOT
            / "artifacts"
            / "etf_robust_backtest_v2_20260725"
        ),
    )
    args = parser.parse_args()

    load_project_env()
    engine = create_tool_engine()
    source_data = load_market_data(
        engine,
        args.data_start,
        args.end,
    )
    engine.dispose()
    data, universe_audit = freeze_universe(
        source_data,
        cutoff_date=args.universe_cutoff,
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    universe_audit.to_csv(
        output_dir / "frozen_universe_audit.csv",
        index=False,
        encoding="utf-8-sig",
    )

    monthly_targets, monthly_records = build_target_schedule(
        data,
        backtest_start=args.backtest_start,
        end_date=args.end,
        mode="trend_risk",
        execution_lag=1,
    )
    pd.DataFrame(monthly_records).to_csv(
        output_dir / "monthly_targets.csv",
        index=False,
        encoding="utf-8-sig",
    )

    cases: dict[str, Any] = {}
    for risk_mode in RISK_MODES:
        label = (
            "baseline_realistic"
            if risk_mode == "none"
            else risk_mode
        )
        cases[label] = _run_case(
            data,
            monthly_targets,
            end_date=args.end,
            risk_mode=risk_mode,
            assumptions=ExecutionAssumptions(
                initial_capital=args.initial_capital
            ),
            output_dir=output_dir,
            label=label,
        )

    proposed_mode = args.proposed_risk_mode
    proposed_cost_label = f"{proposed_mode}_cost2"
    cases[proposed_cost_label] = _run_case(
        data,
        monthly_targets,
        end_date=args.end,
        risk_mode=proposed_mode,
        assumptions=ExecutionAssumptions(
            initial_capital=args.initial_capital,
            cost_multiplier=2.0,
        ),
        output_dir=output_dir,
        label=proposed_cost_label,
    )

    capital_cases: dict[str, Any] = {}
    for capital in (100_000.0, 1_000_000.0, 10_000_000.0, 100_000_000.0):
        label = f"{proposed_mode}_capital_{int(capital)}"
        capital_cases[label] = _run_case(
            data,
            monthly_targets,
            end_date=args.end,
            risk_mode=proposed_mode,
            assumptions=ExecutionAssumptions(
                initial_capital=capital
            ),
            output_dir=output_dir,
            label=label,
        )

    proposed_label = (
        "baseline_realistic"
        if proposed_mode == "none"
        else proposed_mode
    )
    proposed = cases[proposed_label]
    baseline = cases["baseline_realistic"]
    proposed_equity = pd.read_csv(
        output_dir / f"equity_{proposed_label}.csv",
        index_col=0,
        parse_dates=True,
    )["equity"]
    bootstrap = moving_block_bootstrap(proposed_equity)

    exclusion_definitions = {
        "without_gold": {"518880"},
        "without_overseas": {"513100", "513500"},
        "without_commodities": {"159985", "518880"},
    }
    exclusion_cases: dict[str, Any] = {}
    for scenario, excluded_codes in exclusion_definitions.items():
        scenario_data = subset_market_data(data, excluded_codes)
        scenario_targets, _ = build_target_schedule(
            scenario_data,
            backtest_start=args.backtest_start,
            end_date=args.end,
            mode="trend_risk",
            execution_lag=1,
        )
        label = f"{proposed_mode}_{scenario}"
        result = _run_case(
            scenario_data,
            scenario_targets,
            end_date=args.end,
            risk_mode=proposed_mode,
            assumptions=ExecutionAssumptions(
                initial_capital=args.initial_capital
            ),
            output_dir=output_dir,
            label=label,
        )
        result["excluded_codes"] = sorted(excluded_codes)
        exclusion_cases[scenario] = result

    proposed_perf = proposed["performance"]
    baseline_perf = baseline["performance"]
    adoption_checks = {
        "positive_total_return": proposed_perf["total_return"] > 0,
        "monthly_profit_factor_above_1_2": (
            proposed_perf["monthly_profit_factor"] is not None
            and proposed_perf["monthly_profit_factor"] > 1.2
        ),
        "max_drawdown_not_worse_than_baseline": (
            proposed_perf["max_drawdown"]
            >= baseline_perf["max_drawdown"]
        ),
        "july_drawdown_improved": (
            proposed["subperiods"]["july_2026"]["total_return"]
            >= baseline["subperiods"]["july_2026"]["total_return"]
        ),
        "positive_2021_2023": (
            proposed["subperiods"]["2021_2023"]["total_return"] > 0
        ),
        "double_cost_still_positive": (
            cases[proposed_cost_label]["performance"]["total_return"] > 0
        ),
        "asset_removal_cases_still_positive": all(
            result["performance"]["total_return"] > 0
            for result in exclusion_cases.values()
        ),
        "bootstrap_positive_probability_above_80pct": (
            bootstrap["probability_total_return_positive"] >= 0.80
        ),
    }
    report = {
        "schema_version": "etf_robust_backtest.v2",
        "generated_for_data_end": args.end,
        "data_source": "gj_big_qmt_inner",
        "universe_cutoff": args.universe_cutoff,
        "eligible_universe": universe_audit.loc[
            universe_audit["eligible"],
            "etf_code",
        ].tolist(),
        "universe_evidence_hash": hashlib.sha256(
            universe_audit.to_csv(index=False).encode("utf-8")
        ).hexdigest(),
        "proposed_risk_mode": proposed_mode,
        "proposed_case": proposed_label,
        "cases": cases,
        "capital_cases": capital_cases,
        "asset_removal_cases": exclusion_cases,
        "bootstrap": bootstrap,
        "adoption_checks": adoption_checks,
        "adoption_passed": all(adoption_checks.values()),
    }
    (output_dir / "report.json").write_text(
        json.dumps(_safe_json(report), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    summary_rows = []
    all_summary_cases = {
        **cases,
        **capital_cases,
        **{
            f"{proposed_mode}_{label}": result
            for label, result in exclusion_cases.items()
        },
    }
    for label, result in all_summary_cases.items():
        summary_rows.append(
            {
                "case": label,
                "risk_mode": result["risk_mode"],
                "initial_capital": result["assumptions"][
                    "initial_capital"
                ],
                "cost_multiplier": result["assumptions"][
                    "cost_multiplier"
                ],
                **result["performance"],
                "risk_exit_events": result["risk_exit_events"],
                "commission_total": result["commission_total"],
                "spread_impact_total": result[
                    "spread_impact_total"
                ],
                "blocked_orders": result["blocked_orders"],
                "partial_orders": result["partial_orders"],
            }
        )
    pd.DataFrame(summary_rows).to_csv(
        output_dir / "summary.csv",
        index=False,
        encoding="utf-8-sig",
    )
    print(json.dumps(_safe_json(report), ensure_ascii=False, indent=2))
    return 0 if report["adoption_passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
