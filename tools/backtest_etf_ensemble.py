# -*- coding: utf-8 -*-
"""Point-in-time backtest for a mature cross-asset ETF strategy ensemble.

Signals are formed at month-end close and executed at the next trading day's
open. The implementation uses only validated forward-adjusted Guojin QMT bars,
models one-way ETF costs, and keeps unused risk budget in a cash-management ETF.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.env_config import create_tool_engine, load_project_env
from server.common.sql_reader import read_sql_rows

CASH_CODE = "511880"
TARGET_ANNUAL_VOL = 0.10
MIN_AVG_AMOUNT_20 = 5_000_000.0
EQUITY_CLASSES = {"A股宽基", "A股红利", "港股权益", "海外权益"}


@dataclass
class MarketData:
    open: pd.DataFrame
    close: pd.DataFrame
    amount: pd.DataFrame
    names: dict[str, str]
    asset_classes: dict[str, str]

    @property
    def calendar(self) -> pd.DatetimeIndex:
        if "510300" in self.close:
            return pd.DatetimeIndex(self.close["510300"].dropna().index)
        return pd.DatetimeIndex(self.close.dropna(how="all").index)


def load_market_data(engine: Any, start_date: str, end_date: str) -> MarketData:
    sql = """
        SELECT k.etf_code, k.short_name, k.trade_date,
               k.open, k.close, k.amount, c.asset_class
          FROM sm_etf_kline k
          JOIN si_etf_code c ON c.etf_code = k.etf_code
         WHERE k.adjust_type = 1
           AND k.k_type = 1
           AND k.validation_status = 'passed'
           AND k.quality_status = 'validated'
           AND k.trade_date BETWEEN :start_date AND :end_date
         ORDER BY k.trade_date, k.etf_code
        """
    frame = pd.DataFrame(
        read_sql_rows(
            engine,
            sql,
            params={"start_date": start_date, "end_date": end_date},
            context="etf_ensemble_market_data",
        )
    )
    if frame.empty:
        raise RuntimeError("validated ETF K-line table is empty")
    frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    for column in ("open", "close", "amount"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    names = (
        frame[["etf_code", "short_name"]]
        .drop_duplicates("etf_code", keep="last")
        .set_index("etf_code")["short_name"]
        .astype(str)
        .to_dict()
    )
    classes = (
        frame[["etf_code", "asset_class"]]
        .drop_duplicates("etf_code", keep="last")
        .set_index("etf_code")["asset_class"]
        .astype(str)
        .to_dict()
    )
    panels = {}
    for column in ("open", "close", "amount"):
        panels[column] = frame.pivot(
            index="trade_date", columns="etf_code", values=column
        ).sort_index()
    return MarketData(
        open=panels["open"],
        close=panels["close"],
        amount=panels["amount"],
        names=names,
        asset_classes=classes,
    )


def _capped_normalize(weights: pd.Series, cap: float) -> pd.Series:
    weights = weights.clip(lower=0).astype(float)
    if weights.sum() <= 0:
        return weights
    weights /= weights.sum()
    for _ in range(20):
        over = weights > cap + 1e-12
        if not bool(over.any()):
            break
        excess = float((weights.loc[over] - cap).sum())
        weights.loc[over] = cap
        under = ~over
        capacity = (cap - weights.loc[under]).clip(lower=0)
        if capacity.sum() <= 0:
            break
        weights.loc[under] += excess * capacity / capacity.sum()
    return weights / weights.sum() if weights.sum() > 0 else weights


def _select_diversified(
    ranked_codes: list[str],
    returns: pd.DataFrame,
    asset_classes: dict[str, str],
    *,
    limit: int,
    max_correlation: float = 0.85,
) -> list[str]:
    selected: list[str] = []
    used_classes: set[str] = set()
    correlation = returns.corr(min_periods=40)
    for code in ranked_codes:
        asset_class = asset_classes.get(code, "")
        if asset_class in used_classes:
            continue
        if selected:
            pairwise = correlation.loc[code, selected].abs().dropna()
            if not pairwise.empty and float(pairwise.max()) > max_correlation:
                continue
        selected.append(code)
        used_classes.add(asset_class)
        if len(selected) >= limit:
            break
    return selected


def _feature_snapshot(data: MarketData, signal_date: pd.Timestamp) -> dict[str, Any]:
    history = data.close.loc[:signal_date].copy()
    if history.empty:
        return {}
    returns = history.pct_change(fill_method=None)
    rows: list[dict[str, Any]] = []
    for code in history.columns:
        series = history[code].dropna()
        if len(series) < 252 or series.index[-1] != signal_date:
            continue
        recent_amount = data.amount.loc[:signal_date, code].dropna().tail(20)
        if len(recent_amount) < 15 or float(recent_amount.mean()) < MIN_AVG_AMOUNT_20:
            continue
        tail = series.tail(253)
        close = float(tail.iloc[-1])
        r63 = close / float(tail.iloc[-64]) - 1.0
        r126 = close / float(tail.iloc[-127]) - 1.0
        r252 = close / float(tail.iloc[-253]) - 1.0
        ma200 = float(series.tail(200).mean())
        vol63 = float(returns[code].dropna().tail(63).std(ddof=1) * math.sqrt(252))
        if not math.isfinite(vol63) or vol63 <= 0:
            continue
        rows.append(
            {
                "code": code,
                "close": close,
                "r63": r63,
                "r126": r126,
                "r252": r252,
                "ma200": ma200,
                "vol63": vol63,
                "trend_ok": close > ma200 and r126 > 0 and r252 > 0,
                "score": 0.20 * r63 + 0.30 * r126 + 0.50 * r252,
            }
        )
    features = pd.DataFrame(rows).set_index("code") if rows else pd.DataFrame()
    return {
        "features": features,
        "returns": returns.tail(252),
        "signal_date": signal_date,
    }


def _dual_momentum_weights(
    snapshot: dict[str, Any],
    data: MarketData,
) -> pd.Series:
    features = snapshot["features"]
    if features.empty:
        return pd.Series(dtype=float)
    candidates = features.loc[
        features["trend_ok"] & (features.index != CASH_CODE)
    ].sort_values("score", ascending=False)
    selected = _select_diversified(
        candidates.index.tolist(),
        snapshot["returns"],
        data.asset_classes,
        limit=3,
    )
    if not selected:
        return pd.Series(dtype=float)
    return pd.Series(1.0 / len(selected), index=selected, dtype=float)


def _trend_risk_weights(
    snapshot: dict[str, Any],
    data: MarketData,
) -> pd.Series:
    features = snapshot["features"]
    if features.empty:
        return pd.Series(dtype=float)
    candidates = features.loc[
        features["trend_ok"] & (features.index != CASH_CODE)
    ].copy()
    candidates["risk_adjusted_score"] = (
        candidates["score"] / candidates["vol63"].clip(lower=0.03)
    )
    candidates.sort_values("risk_adjusted_score", ascending=False, inplace=True)
    selected = _select_diversified(
        candidates.index.tolist(),
        snapshot["returns"].tail(126),
        data.asset_classes,
        limit=4,
    )
    if not selected:
        return pd.Series(dtype=float)
    inverse_vol = 1.0 / features.loc[selected, "vol63"]
    weights = _capped_normalize(inverse_vol, 0.40)
    covariance = (
        snapshot["returns"][selected].tail(63).cov(min_periods=40) * 252.0
    )
    vector = weights.reindex(selected).fillna(0).to_numpy()
    portfolio_var = float(vector @ covariance.to_numpy() @ vector)
    portfolio_vol = math.sqrt(max(0.0, portfolio_var))
    scale = min(1.0, TARGET_ANNUAL_VOL / portfolio_vol) if portfolio_vol > 0 else 0.0
    return weights * scale


def _regime_adjust(
    weights: pd.Series,
    snapshot: dict[str, Any],
    data: MarketData,
) -> pd.Series:
    if weights.empty:
        return weights
    features = snapshot["features"]
    if "510300" not in features.index:
        return weights
    market_risk_on = bool(features.loc["510300", "trend_ok"])
    if market_risk_on:
        return weights
    equity_codes = [
        code
        for code in weights.index
        if data.asset_classes.get(code) in EQUITY_CLASSES
    ]
    equity_weight = float(weights.reindex(equity_codes).fillna(0).sum())
    max_equity_weight = 0.20
    if equity_weight <= max_equity_weight:
        return weights
    scale = max_equity_weight / equity_weight
    adjusted = weights.copy()
    adjusted.loc[equity_codes] *= scale
    return adjusted


def target_weights(
    data: MarketData,
    signal_date: pd.Timestamp,
    mode: str,
) -> tuple[pd.Series, dict[str, Any]]:
    snapshot = _feature_snapshot(data, signal_date)
    if not snapshot:
        return pd.Series({CASH_CODE: 1.0}), {"risk_on": False}
    dual = _dual_momentum_weights(snapshot, data)
    trend = _trend_risk_weights(snapshot, data)
    if mode == "dual_momentum":
        risky = dual
    elif mode == "trend_risk":
        risky = trend
    elif mode == "ensemble":
        codes = sorted(set(dual.index) | set(trend.index))
        risky = (
            dual.reindex(codes).fillna(0) * 0.40
            + trend.reindex(codes).fillna(0) * 0.60
        )
        risky = _regime_adjust(risky, snapshot, data)
    else:
        raise ValueError(f"unsupported mode: {mode}")
    risky = risky[risky > 1e-8].copy()
    if risky.sum() > 1:
        risky /= risky.sum()
    cash_weight = max(0.0, 1.0 - float(risky.sum()))
    weights = risky.copy()
    weights.loc[CASH_CODE] = weights.get(CASH_CODE, 0.0) + cash_weight
    weights = weights[weights > 1e-8]
    weights /= weights.sum()
    features = snapshot["features"]
    risk_on = bool(
        "510300" in features.index and features.loc["510300", "trend_ok"]
    )
    return weights.sort_index(), {
        "risk_on": risk_on,
        "eligible": int(len(features)),
    }


def build_target_schedule(
    data: MarketData,
    *,
    backtest_start: str,
    end_date: str,
    mode: str,
    execution_lag: int = 1,
) -> tuple[dict[pd.Timestamp, pd.Series], list[dict[str, Any]]]:
    if execution_lag < 1:
        raise ValueError("execution_lag must be at least 1")
    calendar = data.calendar
    calendar = calendar[calendar <= pd.Timestamp(end_date)]
    month_ends = (
        pd.Series(calendar, index=calendar)
        .groupby(calendar.to_period("M"))
        .last()
        .tolist()
    )
    targets: dict[pd.Timestamp, pd.Series] = {}
    records: list[dict[str, Any]] = []
    start_ts = pd.Timestamp(backtest_start)
    for signal_date in month_ends:
        later = calendar[calendar > signal_date]
        if len(later) < execution_lag:
            continue
        execution_date = pd.Timestamp(later[execution_lag - 1])
        if execution_date < start_ts:
            continue
        weights, context = target_weights(data, pd.Timestamp(signal_date), mode)
        targets[execution_date] = weights
        for code, weight in weights.items():
            records.append(
                {
                    "mode": mode,
                    "signal_date": pd.Timestamp(signal_date),
                    "execution_date": execution_date,
                    "etf_code": code,
                    "short_name": data.names.get(code, ""),
                    "asset_class": data.asset_classes.get(code, ""),
                    "weight": float(weight),
                    **context,
                }
            )
    if not targets:
        raise RuntimeError(f"no target schedule generated for {mode}")
    return targets, records


def one_way_cost(
    code: str,
    data: MarketData,
    *,
    cost_multiplier: float = 1.0,
) -> float:
    asset_class = data.asset_classes.get(code, "")
    if code == CASH_CODE:
        return 0.0002 * cost_multiplier
    if asset_class in {"海外权益", "港股权益", "商品"}:
        return 0.0010 * cost_multiplier
    return 0.0005 * cost_multiplier


def simulate(
    data: MarketData,
    targets: dict[pd.Timestamp, pd.Series],
    *,
    end_date: str,
    cost_multiplier: float = 1.0,
) -> tuple[pd.Series, pd.DataFrame]:
    first_date = min(targets)
    calendar = data.calendar
    calendar = calendar[(calendar >= first_date) & (calendar <= pd.Timestamp(end_date))]
    codes = list(data.close.columns)
    close_panel = data.close.reindex(calendar).ffill()
    open_panel = data.open.reindex(calendar)
    units = pd.Series(0.0, index=codes)
    free_cash = 1.0
    equity_rows: list[tuple[pd.Timestamp, float]] = []
    rebalance_rows: list[dict[str, Any]] = []

    for position, day in enumerate(calendar):
        previous_close = (
            close_panel.iloc[position - 1]
            if position > 0
            else close_panel.loc[day]
        )
        trade_prices = open_panel.loc[day].where(
            open_panel.loc[day].notna() & (open_panel.loc[day] > 0),
            previous_close,
        )
        if day in targets:
            requested = targets[day].reindex(codes).fillna(0.0)
            unavailable = (
                requested.gt(0)
                & (
                    open_panel.loc[day].isna()
                    | (open_panel.loc[day] <= 0)
                )
            )
            if bool(unavailable.any()):
                missing_weight = float(requested.loc[unavailable].sum())
                requested.loc[unavailable] = 0.0
                requested.loc[CASH_CODE] += missing_weight
            requested /= requested.sum()

            current_values = units * trade_prices
            portfolio_open = float(current_values.sum() + free_cash)
            estimated_cost = 0.0
            desired = requested * portfolio_open
            trades = desired - current_values
            for _ in range(8):
                estimated_cost = float(
                    sum(
                        abs(float(trades[code]))
                        * one_way_cost(
                            code,
                            data,
                            cost_multiplier=cost_multiplier,
                        )
                        for code in codes
                    )
                )
                investable = max(0.0, portfolio_open - estimated_cost)
                desired = requested * investable
                trades = desired - current_values
            units = desired / trade_prices
            units = units.replace([np.inf, -np.inf], 0).fillna(0)
            free_cash = portfolio_open - estimated_cost - float(desired.sum())
            turnover = float(trades.abs().sum() / max(portfolio_open, 1e-12))
            rebalance_rows.append(
                {
                    "execution_date": day,
                    "equity_at_open": portfolio_open,
                    "turnover": turnover,
                    "cost": estimated_cost,
                    "target_count": int((requested > 1e-8).sum()),
                }
            )
        closing_prices = close_panel.loc[day]
        equity = float((units * closing_prices).sum() + free_cash)
        equity_rows.append((day, equity))
    return (
        pd.Series(
            [value for _, value in equity_rows],
            index=pd.DatetimeIndex([day for day, _ in equity_rows]),
            name="equity",
        ),
        pd.DataFrame(rebalance_rows),
    )


def monthly_returns(equity: pd.Series) -> pd.Series:
    month_end_equity = equity.groupby(equity.index.to_period("M")).last()
    result = month_end_equity.pct_change()
    if not result.empty:
        result.iloc[0] = float(month_end_equity.iloc[0]) - 1.0
    result.index = result.index.astype(str)
    return result


def performance_metrics(equity: pd.Series) -> dict[str, Any]:
    daily = equity.pct_change().dropna()
    monthly = monthly_returns(equity)
    total_return = float(equity.iloc[-1] / 1.0 - 1.0)
    years = max(1 / 252, len(equity) / 252.0)
    cagr = float((equity.iloc[-1] / 1.0) ** (1 / years) - 1)
    annual_vol = float(daily.std(ddof=1) * math.sqrt(252)) if len(daily) > 1 else 0
    sharpe = float(daily.mean() / daily.std(ddof=1) * math.sqrt(252)) if daily.std(ddof=1) > 0 else 0
    drawdown = equity / equity.cummax() - 1.0
    wins = monthly[monthly > 0]
    losses = monthly[monthly < 0]
    payoff = (
        float(wins.mean() / abs(losses.mean()))
        if not wins.empty and not losses.empty
        else None
    )
    profit_factor = (
        float(wins.sum() / abs(losses.sum()))
        if not losses.empty and abs(float(losses.sum())) > 0
        else None
    )
    return {
        "start_date": equity.index.min().date().isoformat(),
        "end_date": equity.index.max().date().isoformat(),
        "trading_days": int(len(equity)),
        "total_return": total_return,
        "cagr": cagr,
        "annual_vol": annual_vol,
        "sharpe": sharpe,
        "max_drawdown": float(drawdown.min()),
        "calmar": float(cagr / abs(drawdown.min())) if drawdown.min() < 0 else None,
        "monthly_observations": int(len(monthly)),
        "monthly_win_rate": float((monthly > 0).mean()) if len(monthly) else None,
        "monthly_payoff_ratio": payoff,
        "monthly_profit_factor": profit_factor,
        "monthly_expectancy": float(monthly.mean()) if len(monthly) else None,
    }


def trade_episodes(
    data: MarketData,
    targets: dict[pd.Timestamp, pd.Series],
    *,
    end_date: str,
    cost_multiplier: float = 1.0,
) -> pd.DataFrame:
    codes = [code for code in data.close.columns if code != CASH_CODE]
    active: dict[str, dict[str, Any]] = {}
    rows: list[dict[str, Any]] = []
    previous = pd.Series(dtype=float)
    for execution_date, target in sorted(targets.items()):
        for code in codes:
            old_weight = float(previous.get(code, 0.0))
            new_weight = float(target.get(code, 0.0))
            price = data.open.at[execution_date, code] if (
                execution_date in data.open.index and code in data.open
            ) else np.nan
            if old_weight <= 1e-8 and new_weight > 1e-8 and pd.notna(price) and price > 0:
                active[code] = {
                    "etf_code": code,
                    "short_name": data.names.get(code, ""),
                    "asset_class": data.asset_classes.get(code, ""),
                    "entry_date": execution_date,
                    "entry_price": float(price),
                }
            elif old_weight > 1e-8 and new_weight <= 1e-8 and code in active and pd.notna(price) and price > 0:
                episode = active.pop(code)
                gross = float(price) / episode["entry_price"] - 1.0
                net = gross - 2.0 * one_way_cost(
                    code,
                    data,
                    cost_multiplier=cost_multiplier,
                )
                rows.append(
                    {
                        **episode,
                        "exit_date": execution_date,
                        "exit_price": float(price),
                        "gross_return": gross,
                        "net_return": net,
                        "status": "closed",
                    }
                )
        previous = target
    final_date = pd.Timestamp(end_date)
    for code, episode in active.items():
        series = data.close.loc[:final_date, code].dropna()
        if series.empty:
            continue
        price = float(series.iloc[-1])
        gross = price / episode["entry_price"] - 1.0
        rows.append(
            {
                **episode,
                "exit_date": series.index[-1],
                "exit_price": price,
                "gross_return": gross,
                "net_return": gross
                - 2.0
                * one_way_cost(
                    code,
                    data,
                    cost_multiplier=cost_multiplier,
                ),
                "status": "open_marked",
            }
        )
    return pd.DataFrame(rows)


def episode_metrics(episodes: pd.DataFrame) -> dict[str, Any]:
    closed = episodes.loc[episodes["status"] == "closed"].copy()
    if closed.empty:
        return {
            "closed_episodes": 0,
            "win_rate": None,
            "payoff_ratio": None,
            "profit_factor": None,
            "expectancy": None,
        }
    wins = closed.loc[closed["net_return"] > 0, "net_return"]
    losses = closed.loc[closed["net_return"] < 0, "net_return"]
    return {
        "closed_episodes": int(len(closed)),
        "win_rate": float((closed["net_return"] > 0).mean()),
        "payoff_ratio": (
            float(wins.mean() / abs(losses.mean()))
            if not wins.empty and not losses.empty
            else None
        ),
        "profit_factor": (
            float(wins.sum() / abs(losses.sum()))
            if not losses.empty and abs(float(losses.sum())) > 0
            else None
        ),
        "expectancy": float(closed["net_return"].mean()),
        "average_win": float(wins.mean()) if not wins.empty else None,
        "average_loss": float(losses.mean()) if not losses.empty else None,
    }


def benchmark_equity(
    data: MarketData,
    *,
    start_date: pd.Timestamp,
    end_date: str,
    code: str = "510300",
    cost_multiplier: float = 1.0,
) -> pd.Series:
    calendar = data.calendar
    calendar = calendar[(calendar >= start_date) & (calendar <= pd.Timestamp(end_date))]
    entry = float(data.open.loc[start_date, code])
    initial_cost = one_way_cost(
        code,
        data,
        cost_multiplier=cost_multiplier,
    )
    units = (1.0 - initial_cost) / entry
    return data.close.reindex(calendar)[code].ffill() * units


def split_metrics(equity: pd.Series) -> dict[str, Any]:
    result: dict[str, Any] = {}
    final_date = equity.index.max().date().isoformat()
    for label, start, end in (
        ("2021_2023", "2021-01-01", "2023-12-31"),
        ("2024_2026", "2024-01-01", final_date),
        ("july_2026", "2026-07-01", final_date),
    ):
        part = equity.loc[start:end]
        if part.empty:
            continue
        base = float(equity.loc[:part.index[0]].iloc[-2]) if len(equity.loc[:part.index[0]]) > 1 else 1.0
        normalized = part / base
        result[label] = performance_metrics(normalized)
    return result


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, (pd.Timestamp, pd.Period)):
        return str(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not math.isfinite(float(value)) else float(value)
    if isinstance(value, float):
        return None if not math.isfinite(value) else value
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-start", default="2019-01-01")
    parser.add_argument("--backtest-start", default="2021-01-04")
    parser.add_argument("--end", default="2026-07-24")
    parser.add_argument(
        "--execution-lag",
        type=int,
        default=1,
        help="Trading-day lag after the signal close; 1 means next-day open.",
    )
    parser.add_argument(
        "--cost-multiplier",
        type=float,
        default=1.0,
        help="Stress multiplier applied to all one-way costs.",
    )
    parser.add_argument(
        "--exclude-codes",
        default="",
        help="Comma-separated ETF codes removed from the investable universe.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(ROOT / "artifacts" / "etf_ensemble_backtest_20260725"),
    )
    args = parser.parse_args()

    load_project_env()
    engine = create_tool_engine()
    data = load_market_data(engine, args.data_start, args.end)
    engine.dispose()
    excluded_codes = {
        code.strip()
        for code in str(args.exclude_codes).split(",")
        if code.strip()
    }
    if CASH_CODE in excluded_codes:
        raise ValueError(f"cash proxy {CASH_CODE} cannot be excluded")
    if excluded_codes:
        keep = [
            code
            for code in data.close.columns
            if code not in excluded_codes
        ]
        data = MarketData(
            open=data.open.loc[:, keep],
            close=data.close.loc[:, keep],
            amount=data.amount.loc[:, keep],
            names={
                code: name
                for code, name in data.names.items()
                if code in keep
            },
            asset_classes={
                code: asset_class
                for code, asset_class in data.asset_classes.items()
                if code in keep
            },
        )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    report: dict[str, Any] = {
        "methodology": {
            "data_source": "gj_big_qmt_inner",
            "validation": "10jqka/Sina cross-source passed",
            "signal_time": "month-end close",
            "execution_time": "next trading-day open",
            "execution_lag": args.execution_lag,
            "cost_multiplier": args.cost_multiplier,
            "excluded_codes": sorted(excluded_codes),
            "target_annual_vol": TARGET_ANNUAL_VOL,
            "cash_proxy": CASH_CODE,
            "cost_model": {
                "domestic_etf_one_way": 0.0005,
                "qdii_commodity_hk_one_way": 0.0010,
                "cash_etf_one_way": 0.0002,
            },
        },
        "strategies": {},
    }
    summary_rows: list[dict[str, Any]] = []
    for mode in ("dual_momentum", "trend_risk", "ensemble"):
        targets, target_records = build_target_schedule(
            data,
            backtest_start=args.backtest_start,
            end_date=args.end,
            mode=mode,
            execution_lag=args.execution_lag,
        )
        equity, rebalances = simulate(
            data,
            targets,
            end_date=args.end,
            cost_multiplier=args.cost_multiplier,
        )
        episodes = trade_episodes(
            data,
            targets,
            end_date=args.end,
            cost_multiplier=args.cost_multiplier,
        )
        metrics = performance_metrics(equity)
        episode_result = episode_metrics(episodes)
        strategy_report = {
            "performance": metrics,
            "episodes": episode_result,
            "subperiods": split_metrics(equity),
            "rebalances": int(len(rebalances)),
            "average_turnover": float(rebalances["turnover"].mean()),
            "total_cost": float(rebalances["cost"].sum()),
        }
        report["strategies"][mode] = strategy_report
        summary_rows.append(
            {
                "strategy": mode,
                **metrics,
                **{f"episode_{key}": value for key, value in episode_result.items()},
                "average_turnover": strategy_report["average_turnover"],
                "total_cost": strategy_report["total_cost"],
            }
        )
        equity.rename(mode).to_csv(output_dir / f"equity_{mode}.csv", header=True)
        monthly_returns(equity).rename("return").to_csv(
            output_dir / f"monthly_returns_{mode}.csv", header=True
        )
        pd.DataFrame(target_records).to_csv(
            output_dir / f"targets_{mode}.csv", index=False, encoding="utf-8-sig"
        )
        rebalances.to_csv(
            output_dir / f"rebalances_{mode}.csv", index=False, encoding="utf-8-sig"
        )
        episodes.to_csv(
            output_dir / f"episodes_{mode}.csv", index=False, encoding="utf-8-sig"
        )

    first_execution = min(
        build_target_schedule(
            data,
            backtest_start=args.backtest_start,
            end_date=args.end,
            mode="ensemble",
            execution_lag=args.execution_lag,
        )[0]
    )
    benchmark = benchmark_equity(
        data,
        start_date=first_execution,
        end_date=args.end,
        cost_multiplier=args.cost_multiplier,
    )
    report["benchmark_510300"] = {
        "performance": performance_metrics(benchmark),
        "subperiods": split_metrics(benchmark),
    }
    benchmark.rename("510300_buy_hold").to_csv(
        output_dir / "equity_benchmark_510300.csv", header=True
    )
    summary = pd.DataFrame(summary_rows)
    summary.to_csv(output_dir / "summary.csv", index=False, encoding="utf-8-sig")
    (output_dir / "report.json").write_text(
        json.dumps(_json_safe(report), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(_json_safe(report), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
