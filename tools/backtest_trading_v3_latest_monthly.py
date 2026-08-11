#!/usr/bin/env python3
"""Research replay of the latest Trading V3 multi-sleeve portfolio.

The replay uses the current immutable sleeve formulas and portfolio engine.
Daily sleeves are calibrated only on pre-validation observations.  July is
then replayed strictly as-of, with next-open paper fills, A-share T+1,
whole-board quantities, limit/suspension checks and confirmed Guojin fees.

The historical Level-1 record required by ``intraday_surprise`` does not exist
before 2026-07-27.  That sleeve is therefore reported as unavailable and never
silently replaced by a hindsight proxy.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import pickle
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.kline_data import get_kline_engine
from server.trading_v3.backtest import (
    _build_features,
    _dynamic_signal_outcome,
    _load_history,
)
from server.trading_v3.calibration import CalibrationTable, fit_calibration
from server.trading_v3.config import config_hash, load_v3_config
from server.trading_v3.context import load_asof_context
from server.trading_v3.daily_features import (
    _event_features,
    _load_finance,
    _load_industries,
    _load_recent_notices,
    _load_theme_memberships,
)
from server.trading_v3.engine import TradingV3Engine
from server.trading_v3.exit_policy import daily_exit_reason
from server.trading_v3.metrics import maximum_drawdown, trade_metrics
from server.trading_v3.sleeves import SLEEVE_BUILDERS
from server.trading_v3.theme_features import (
    attach_best_theme,
    calculate_theme_statistics,
    diversified_universe_codes,
)
from tools.env_config import create_tool_engine, load_project_env


DAILY_SLEEVES = (
    "theme_diffusion",
    "low_base_ignition",
    "right_side_trend",
    "event_drift",
    "quality_momentum",
    "oversold_reversal",
)
SLEEVE_NAMES = {
    "theme_diffusion": "主题扩散",
    "low_base_ignition": "低位点火预判",
    "right_side_trend": "右侧趋势",
    "event_drift": "事件漂移",
    "quality_momentum": "质量动量",
    "oversold_reversal": "超跌抄底实验",
    "intraday_surprise": "盘中超预期",
}


def _float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def _iso(value: Any) -> str:
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _latest_weekly_dates(
    trading_days: list[pd.Timestamp],
    *,
    start_date: date,
    end_date: date,
) -> list[pd.Timestamp]:
    selected = [
        item
        for item in trading_days
        if start_date <= item.date() <= end_date
    ]
    weekly: dict[tuple[int, int], pd.Timestamp] = {}
    for item in selected:
        iso = item.isocalendar()
        weekly[(int(iso.year), int(iso.week))] = item
    return sorted(weekly.values())


def _prepare_feature_frame(frame: pd.DataFrame) -> pd.DataFrame:
    features = _build_features(frame)
    groups = features.groupby("stock_code", sort=False)
    features["return_5d_pct"] = groups["close"].pct_change(5) * 100.0
    features["average_amount_20d"] = features["amount20"]
    features["latest_amount"] = features["amount"]
    features["price"] = features["raw_close"]
    return features


def _market_features(
    features: pd.DataFrame,
    *,
    trade_day: pd.Timestamp,
    trading_days: list[pd.Timestamp],
    industries: dict[str, tuple[str, str]],
) -> dict[str, float]:
    index = trading_days.index(trade_day)
    current = features[features["trade_date"] == trade_day]
    prior = features[
        features["trade_date"] == trading_days[max(0, index - 5)]
    ]
    market_return = float(
        current["return_20d_pct"].dropna().median()
    )
    market_return = market_return if math.isfinite(market_return) else 0.0
    breadth = float((current["change_pct"] > 0).mean() * 100.0)
    prior_breadth = float((prior["change_pct"] > 0).mean() * 100.0)
    recent_dates = trading_days[max(0, index - 20) : index + 1]
    equal_daily = (
        features[features["trade_date"].isin(recent_dates)]
        .groupby("trade_date")["change_pct"]
        .median()
    )
    volatility = float(equal_daily.std(ddof=0) or 0.0)
    current_amount = current[
        ["stock_code", "amount"]
    ].copy()
    current_amount["industry_code"] = current_amount[
        "stock_code"
    ].map(lambda code: industries.get(str(code), ("", ""))[0])
    sector_amount = (
        current_amount[current_amount["industry_code"] != ""]
        .groupby("industry_code")["amount"]
        .sum()
    )
    total_amount = float(sector_amount.sum())
    top_share = (
        float(sector_amount.nlargest(5).sum()) / total_amount * 100.0
        if total_amount > 0
        else 0.0
    )
    return {
        "market_return_20d_pct": market_return,
        "market_latest_change_pct": float(
            current["change_pct"].median()
        ),
        "market_breadth_pct": breadth,
        "breadth_change_5d_pct": breadth - prior_breadth,
        "realized_volatility_20d_pct": volatility,
        "limit_down_ratio_pct": float(
            (current["change_pct"] <= -9.5).mean() * 100.0
        ),
        "sector_concentration_pct": top_share,
    }


def _snapshot(
    primary,
    features: pd.DataFrame,
    *,
    trade_day: pd.Timestamp,
    trading_days: list[pd.Timestamp],
    industries: dict[str, tuple[str, str]],
    limit: int,
) -> dict[str, Any]:
    market = _market_features(
        features,
        trade_day=trade_day,
        trading_days=trading_days,
        industries=industries,
    )
    market.update(load_asof_context(primary, as_of=trade_day.date()))
    day = features[features["trade_date"] == trade_day].copy()
    names = day["short_name"].fillna("").astype(str)
    day = day[
        (day["raw_close"] >= 2)
        & (day["amount"] > 0)
        & (day["amount20"] >= 50_000_000)
        & (~names.str.upper().str.contains("ST", regex=False))
        & (~names.str.contains("退", regex=False))
        & day["return_5d_pct"].notna()
        & day["return_20d_pct"].notna()
        & day["return_60d_pct"].notna()
        & day["atr_14d_pct"].notna()
    ].copy()
    day["theme_code"] = day["stock_code"].map(
        lambda code: industries.get(str(code), ("", ""))[0]
    )
    day["theme_name"] = day["stock_code"].map(
        lambda code: industries.get(str(code), ("", ""))[1]
    )
    day["market_return_20d_pct"] = market["market_return_20d_pct"]

    day_codes = day["stock_code"].astype(str).tolist()
    memberships, concept_snapshot_date = _load_theme_memberships(
        primary,
        as_of=trade_day.date(),
        codes=day_codes,
        industries=industries,
    )
    theme_base: dict[str, dict[str, Any]] = {}
    for row in day.itertuples(index=False):
        theme_base[str(row.stock_code)] = {
            "stock_code": str(row.stock_code),
            "stock_name": str(row.short_name or ""),
            "theme_code": str(row.theme_code or ""),
            "theme_name": str(row.theme_name or ""),
            "return_5d_pct": _float(row.return_5d_pct),
            "return_2d_pct": _float(row.return_2d_pct),
            "return_20d_pct": _float(row.return_20d_pct),
            "relative_strength_20d_pct": _float(
                row.relative_strength_20d_pct
            ),
            "breakout_20d_proximity": _float(
                row.breakout_20d_proximity
            ),
            "amount_ratio_5_20": _float(row.amount_ratio_5_20),
            "distance_ma20_pct": _float(row.distance_ma20_pct),
            "distance_ma5_pct": _float(row.distance_ma5_pct),
            "drawdown_20d_pct": _float(row.drawdown_20d_pct),
            "rebound_from_low_pct": _float(
                row.rebound_from_low_pct
            ),
            "previous_change_pct": _float(
                row.previous_change_pct
            ),
            "amount_ratio_1_20": _float(row.amount_ratio_1_20),
            "latest_relative_to_market_pct": _float(
                row.latest_relative_to_market_pct
            ),
            "latest_change_pct": _float(row.change_pct),
            "latest_amount": _float(row.amount),
        }
    theme_statistics = calculate_theme_statistics(
        features[
            features["trade_date"].isin(
                trading_days[max(0, trading_days.index(trade_day) - 6) :
                             trading_days.index(trade_day) + 1]
            )
        ],
        as_of=trade_day,
        memberships=memberships,
    )
    attach_best_theme(
        theme_base,
        memberships=memberships,
        statistics=theme_statistics,
    )
    theme_columns = (
        "theme_code",
        "theme_name",
        "theme_codes",
        "theme_names",
        "theme_source",
        "theme_member_count",
        "sector_breadth_pct",
        "sector_breadth_prior_pct",
        "sector_breadth_3d_prior_pct",
        "sector_breadth_acceleration_pct",
        "sector_return_5d_pct",
        "sector_relative_return_pct",
        "sector_amount_acceleration_pct",
        "sector_leadership_depth",
        "sector_crowding",
        "theme_opportunity_score",
        "stock_relative_to_theme_5d_pct",
        "stock_leadership_score",
        "leadership_quality",
    )
    day_index = day["stock_code"].astype(str)
    for column in theme_columns:
        day[column] = day_index.map(
            lambda code, key=column: theme_base.get(code, {}).get(key)
        )

    codes = day["stock_code"].astype(str).tolist()
    finance = _load_finance(
        primary,
        as_of=trade_day.date(),
        codes=codes,
    )
    finance_rows: dict[str, dict[str, float]] = {}
    finance_missing_fields: dict[str, tuple[str, ...]] = {}
    for code, values in finance.items():
        missing = tuple(
            key for key, value in values.items() if value is None
        )
        finance_missing_fields[code] = missing
        net_asset = values["net_asset_ps"]
        row_price = day.loc[
            day["stock_code"].astype(str) == code,
            "raw_close",
        ]
        price = float(row_price.iloc[0]) if not row_price.empty else 0.0
        finance_rows[code] = {}
        if all(
            values[key] is not None
            for key in (
                "roe_wtd",
                "gross_margin",
                "net_margin",
                "asset_liab_ratio",
            )
        ):
            finance_rows[code]["quality_raw"] = (
                values["roe_wtd"]
                + values["gross_margin"] * 0.25
                + values["net_margin"] * 0.25
                - values["asset_liab_ratio"] * 0.15
            )
        if all(
            values[key] is not None
            for key in ("total_rev_yoy_gr", "net_profit_yoy_gr")
        ):
            finance_rows[code]["growth_raw"] = (
                values["total_rev_yoy_gr"]
                + values["net_profit_yoy_gr"]
            )
        if all(
            values[key] is not None
            for key in ("oper_cf_ps", "cash_flow_ratio")
        ):
            finance_rows[code]["cashflow_raw"] = (
                values["oper_cf_ps"]
                + values["cash_flow_ratio"] * 0.1
            )
        if net_asset is not None and net_asset > 0:
            finance_rows[code]["valuation_raw"] = (
                price / net_asset if net_asset > 0 else math.nan
            )
    finance_frame = pd.DataFrame.from_dict(
        finance_rows,
        orient="index",
    )
    if not finance_frame.empty:
        percentile_sources = {
            "quality_percentile": ("quality_raw", True),
            "growth_percentile": ("growth_raw", True),
            "cashflow_quality_percentile": (
                "cashflow_raw",
                True,
            ),
            "valuation_percentile": ("valuation_raw", False),
        }
        for percentile, (source, ascending) in (
            percentile_sources.items()
        ):
            if source in finance_frame:
                finance_frame[percentile] = finance_frame[
                    source
                ].rank(
                    method="average",
                    pct=True,
                    ascending=ascending,
                )
        day_index = day["stock_code"].astype(str)
        for column in percentile_sources:
            if column in finance_frame:
                day[column] = day_index.map(finance_frame[column])
    day["momentum_60d_percentile"] = day["return_60d_pct"].rank(
        method="average",
        pct=True,
    )
    day["volatility_20d_percentile"] = day["atr_14d_pct"].rank(
        method="average",
        pct=True,
    )

    selection_base = {
        str(row["stock_code"]): row.to_dict()
        for _, row in day.iterrows()
    }
    ranked_codes = diversified_universe_codes(
        selection_base,
        limit=limit,
    )
    day = (
        day.set_index(day["stock_code"].astype(str), drop=False)
        .loc[ranked_codes]
        .reset_index(drop=True)
    )
    notices = _load_recent_notices(
        primary,
        as_of=trade_day.date(),
        codes=ranked_codes,
    )
    stocks = []
    for row in day.itertuples(index=False):
        item = {
            "stock_code": str(row.stock_code),
            "stock_name": str(row.short_name or ""),
            "price": _float(row.raw_close),
            "theme_code": str(row.theme_code or ""),
            "theme_name": str(row.theme_name or ""),
            "theme_codes": tuple(
                str(value)
                for value in (getattr(row, "theme_codes", ()) or ())
                if value
            ),
            "theme_names": tuple(
                str(value)
                for value in (getattr(row, "theme_names", ()) or ())
                if value
            ),
            "return_5d_pct": _float(row.return_5d_pct),
            "return_2d_pct": _float(row.return_2d_pct),
            "return_20d_pct": _float(row.return_20d_pct),
            "return_60d_pct": _float(row.return_60d_pct),
            "ma20_slope_5d_pct": _float(row.ma20_slope_5d_pct),
            "breakout_20d_proximity": _float(
                row.breakout_20d_proximity
            ),
            "amount_ratio_5_20": _float(row.amount_ratio_5_20),
            "relative_strength_20d_pct": _float(
                row.relative_strength_20d_pct
            ),
            "market_return_20d_pct": _float(
                market["market_return_20d_pct"]
            ),
            "market_latest_change_pct": _float(
                market["market_latest_change_pct"]
            ),
            "latest_relative_to_market_pct": _float(
                row.latest_relative_to_market_pct
            ),
            "distance_ma20_pct": _float(row.distance_ma20_pct),
            "distance_ma5_pct": _float(row.distance_ma5_pct),
            "drawdown_20d_pct": _float(row.drawdown_20d_pct),
            "rebound_from_low_pct": _float(
                row.rebound_from_low_pct
            ),
            "previous_change_pct": _float(
                row.previous_change_pct
            ),
            "close_above_ma20": _float(row.close_above_ma20),
            "ma20_above_ma60": _float(row.ma20_above_ma60),
            "atr_14d_pct": _float(row.atr_14d_pct),
            "latest_change_pct": _float(row.change_pct),
            "amount_ratio_1_20": _float(row.amount_ratio_1_20),
            "latest_amount": _float(row.amount),
            "average_amount_20d": _float(row.amount20),
            "finance_data_complete": float(
                not finance_missing_fields.get(str(row.stock_code), ())
            ),
            "finance_missing_count": float(
                len(
                    finance_missing_fields.get(
                        str(row.stock_code),
                        (),
                    )
                )
            ),
            "finance_missing_fields": finance_missing_fields.get(
                str(row.stock_code),
                (),
            ),
        }
        for column in (
            "sector_breadth_pct",
            "sector_breadth_acceleration_pct",
            "sector_relative_return_pct",
            "sector_amount_acceleration_pct",
            "leadership_quality",
            "sector_crowding",
            "theme_opportunity_score",
            "stock_relative_to_theme_5d_pct",
            "stock_leadership_score",
            "sector_leadership_depth",
            "quality_percentile",
            "growth_percentile",
            "cashflow_quality_percentile",
            "valuation_percentile",
            "momentum_60d_percentile",
            "volatility_20d_percentile",
        ):
            value = getattr(row, column, None)
            if value is not None and math.isfinite(_float(value, math.nan)):
                item[column] = float(value)
        item.update(
            _event_features(
                notices.get(item["stock_code"], []),
                as_of=trade_day.date(),
                return_5d_pct=item["return_5d_pct"],
                amount_ratio=item["amount_ratio_5_20"],
                distance_ma20_pct=item["distance_ma20_pct"],
            )
        )
        stocks.append(item)
    return {
        "trade_date": trade_day,
        "market_features": market,
        "stocks": stocks,
        "concept_snapshot_date": (
            concept_snapshot_date.isoformat()
            if concept_snapshot_date
            else None
        ),
        "theme_count": len(theme_statistics),
    }


def _dynamic_outcomes(
    features: pd.DataFrame,
    snapshots: dict[pd.Timestamp, dict[str, Any]],
    *,
    config: dict[str, Any],
) -> dict[tuple[str, pd.Timestamp, str], dict[str, Any]]:
    """Build completed dynamic outcomes without crossing calibration cutoff."""

    if not snapshots:
        return {}
    groups = {
        str(code): group.sort_values("trade_date").reset_index(drop=True)
        for code, group in features.groupby(
            "stock_code",
            sort=False,
            observed=True,
        )
    }
    locations = {
        code: {
            pd.Timestamp(trade_date): index
            for index, trade_date in enumerate(group["trade_date"])
        }
        for code, group in groups.items()
    }
    calibration_cutoff = max(pd.Timestamp(day) for day in snapshots)
    outcomes: dict[
        tuple[str, pd.Timestamp, str],
        dict[str, Any],
    ] = {}
    for trade_day, snapshot in sorted(snapshots.items()):
        feature_time = datetime.combine(
            trade_day.date(),
            datetime.min.time(),
        ).replace(hour=15)
        valid_until = feature_time + timedelta(days=30)
        signals: dict[str, list[Any]] = defaultdict(list)
        for stock in snapshot["stocks"]:
            for key in DAILY_SLEEVES:
                signal = SLEEVE_BUILDERS[key](
                    stock["stock_code"],
                    stock["stock_name"],
                    stock,
                    feature_time,
                    valid_until,
                )
                if signal.status == "SCORED":
                    signals[key].append(signal)
        for key, values in signals.items():
            for signal in sorted(
                values,
                key=lambda item: (-item.score, item.stock_code),
            )[:10]:
                code = str(signal.stock_code)
                signal_index = locations.get(code, {}).get(
                    pd.Timestamp(trade_day)
                )
                if signal_index is None:
                    continue
                outcome = _dynamic_signal_outcome(
                    groups[code],
                    signal_index=signal_index,
                    config=config,
                    initial_stop_pct=float(signal.initial_stop_pct),
                )
                if (
                    outcome is None
                    or pd.Timestamp(outcome["exit_date"])
                    > calibration_cutoff
                ):
                    continue
                outcomes[(key, pd.Timestamp(trade_day), code)] = outcome
    return outcomes


def _calibrate(
    snapshots: dict[pd.Timestamp, dict[str, Any]],
    outcomes: dict[
        tuple[str, pd.Timestamp, str],
        dict[str, Any],
    ],
    *,
    model_prefix: str,
) -> tuple[dict[str, CalibrationTable], dict[str, dict[str, Any]]]:
    samples: dict[str, list[dict[str, float]]] = defaultdict(list)
    for trade_day, snapshot in sorted(snapshots.items()):
        signals: dict[str, list[Any]] = defaultdict(list)
        feature_time = datetime.combine(
            trade_day.date(),
            datetime.min.time(),
        ).replace(hour=15)
        valid_until = feature_time + timedelta(days=30)
        for stock in snapshot["stocks"]:
            for key in DAILY_SLEEVES:
                signal = SLEEVE_BUILDERS[key](
                    stock["stock_code"],
                    stock["stock_name"],
                    stock,
                    feature_time,
                    valid_until,
                )
                if signal.status == "SCORED":
                    signals[key].append(signal)
        for key, values in signals.items():
            top = sorted(
                values,
                key=lambda item: (-item.score, item.stock_code),
            )[:10]
            for signal in top:
                row = outcomes.get(
                    (
                        key,
                        pd.Timestamp(trade_day),
                        str(signal.stock_code),
                    )
                )
                if row is None:
                    continue
                required = (
                    row.get("net_return_pct"),
                    row.get("mae_pct"),
                    row.get("mfe_pct"),
                )
                if not all(math.isfinite(_float(value, math.nan)) for value in required):
                    continue
                samples[key].append({
                    "score": signal.score,
                    "net_return_pct": float(row["net_return_pct"]),
                    "mae_pct": float(row["mae_pct"]),
                    "mfe_pct": float(row["mfe_pct"]),
                })
    calibrations = {}
    evidence = {}
    config = load_v3_config()
    version_tokens = dict(
        config.get("calibration_version_tokens") or {}
    )
    for key in DAILY_SLEEVES:
        strategy_samples = samples.get(key, [])
        minimum_bucket_samples = int(
            load_v3_config()["profit_gate"]["minimum_oos_samples"]
        )
        bucket_count = max(
            1,
            min(5, len(strategy_samples) // minimum_bucket_samples),
        )
        table = fit_calibration(
            key,
            strategy_samples,
            model_version=(
                f"{key}."
                f"{version_tokens.get(key) or config.get('calibration_version_token')}"
                f"-{model_prefix}"
            ),
            bucket_count=bucket_count,
        )
        calibrations[key] = table
        values = [
            float(item["net_return_pct"])
            for item in samples.get(key, [])
        ]
        metrics = trade_metrics(values)
        metrics["sample_count"] = len(values)
        gate = config["profit_gate"]
        passing_buckets = [
            bucket
            for bucket in table.buckets
            if (
                bucket.sample_count
                >= int(gate["minimum_oos_samples"])
                and bucket.expected_return_net_pct
                > float(gate["minimum_expected_return_net_pct"])
                and bucket.profit_factor is not None
                and bucket.profit_factor
                >= float(gate["minimum_profit_factor"])
                and bucket.payoff_ratio is not None
                and bucket.payoff_ratio
                >= float(gate["minimum_payoff_ratio"])
            )
        ]
        evidence[key] = {
            "strategy_name": SLEEVE_NAMES[key],
            "label_protocol": "DYNAMIC_PRODUCTION_EXIT_NEXT_OPEN",
            "model_version": table.model_version,
            "dataset_hash": table.dataset_hash,
            "sample_count": len(values),
            "metrics": metrics,
            "bucket_count": len(table.buckets),
            "passing_bucket_count": len(passing_buckets),
            "calibration_direction_valid": (
                table.has_valid_score_direction()
            ),
            "profit_gate_passed": bool(
                passing_buckets
                and table.has_valid_score_direction()
            ),
            "buckets": [
                {
                    "lower_score": bucket.lower_score,
                    "upper_score": bucket.upper_score,
                    "sample_count": bucket.sample_count,
                    "expected_return_net_pct": (
                        bucket.expected_return_net_pct
                    ),
                    "profit_factor": bucket.profit_factor,
                    "payoff_ratio": bucket.payoff_ratio,
                }
                for bucket in table.buckets
            ],
        }
    return calibrations, evidence


def _fee(
    value: float,
    *,
    sell: bool,
    config: dict[str, Any],
) -> float:
    account = config["account"]
    result = max(
        float(account["minimum_commission_cny"]),
        value * float(account["commission_rate"]),
    )
    result += value * float(account["transfer_fee_rate"])
    if sell:
        result += value * float(account["sell_stamp_duty_rate"])
    return result


def _limit_rate(code: str) -> float:
    if code.startswith("92"):
        return 0.30
    return 0.20 if code.startswith(("30", "68")) else 0.10


def _bar_lookup(
    features: pd.DataFrame,
    trading_days: list[pd.Timestamp],
) -> dict[pd.Timestamp, pd.DataFrame]:
    return {
        day: group.set_index("stock_code")
        for day, group in features[
            features["trade_date"].isin(trading_days)
        ].groupby("trade_date")
    }


def _simulate(
    snapshots: dict[pd.Timestamp, dict[str, Any]],
    calibrations: dict[str, CalibrationTable],
    features: pd.DataFrame,
    *,
    month_days: list[pd.Timestamp],
) -> dict[str, Any]:
    config = load_v3_config()
    account = config["account"]
    policy = config["portfolio"]
    engine = TradingV3Engine(calibrations)
    bars = _bar_lookup(features, month_days)
    next_day = {
        month_days[index]: month_days[index + 1]
        for index in range(len(month_days) - 1)
    }
    initial_cash = float(account["initial_cash_cny"])
    cash = initial_cash
    positions: dict[str, dict[str, Any]] = {}
    pending_buys: dict[pd.Timestamp, list[dict[str, Any]]] = defaultdict(list)
    pending_sells: dict[pd.Timestamp, list[dict[str, Any]]] = defaultdict(list)
    trades: list[dict[str, Any]] = []
    daily: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    orders: list[dict[str, Any]] = []
    equity_curve: list[dict[str, Any]] = []
    total_cost = 0.0
    trade_day_index = {
        item: index for index, item in enumerate(month_days)
    }
    cooldown_until_index: dict[str, int] = {}
    cooldown_days = int(
        config.get("paper_discovery", {}).get(
            "cooldown_trade_days_after_exit",
            5,
        )
    )

    def execute_sell(
        code: str,
        position: dict[str, Any],
        trade_day: pd.Timestamp,
        reference_price: float,
        reason: str,
    ) -> None:
        nonlocal cash, total_cost
        execution_price = reference_price * (
            1.0 - float(account["default_slippage_rate"])
        )
        value = execution_price * position["quantity"]
        sell_fee = _fee(value, sell=True, config=config)
        cash += value - sell_fee
        total_cost += sell_fee
        gross_cost = position["entry_price"] * position["quantity"]
        net = (
            (execution_price - position["entry_price"])
            * position["quantity"]
            - position["buy_fee"]
            - sell_fee
        )
        record = trades[position["trade_index"]]
        record.update({
            "exit_signal_date": position.get("exit_signal_date", ""),
            "exit_date": _iso(trade_day),
            "exit_price": round(execution_price, 4),
            "sell_quantity": position["quantity"],
            "sell_fee_cny": round(sell_fee, 2),
            "net_pnl_cny": round(net, 2),
            "net_return_pct": round(net / gross_cost * 100.0, 6),
            "exit_reason": reason,
            "holding_trade_days": position["holding_trade_days"],
            "status": "CLOSED",
        })
        orders.append({
            "trade_date": _iso(trade_day),
            "stock_code": code,
            "stock_name": position["stock_name"],
            "side": "SELL",
            "status": "FILLED",
            "reference_price": round(reference_price, 4),
            "execution_price": round(execution_price, 4),
            "quantity": position["quantity"],
            "reason": reason,
        })
        cooldown_until_index[code] = (
            trade_day_index[trade_day] + cooldown_days
        )
        del positions[code]

    for trade_day in month_days:
        day_bars = bars.get(trade_day)
        if day_bars is None:
            continue

        for request in pending_sells.pop(trade_day, []):
            code = request["stock_code"]
            position = positions.get(code)
            if not position or code not in day_bars.index:
                continue
            row = day_bars.loc[code]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[0]
            if _float(row["amount"]) <= 0:
                following = next_day.get(trade_day)
                if following:
                    pending_sells[following].append(request)
                continue
            pre_close = _float(row.get("pre_close"))
            raw_open = _float(row["raw_open"])
            limit_rate = _limit_rate(code)
            limit_locked = bool(
                pre_close > 0
                and raw_open <= pre_close * (1.0 - limit_rate + 0.002)
                and abs(_float(row["raw_high"]) - _float(row["raw_low"]))
                < 1e-6
            )
            if limit_locked:
                following = next_day.get(trade_day)
                if following:
                    pending_sells[following].append(request)
                orders.append({
                    "trade_date": _iso(trade_day),
                    "stock_code": code,
                    "stock_name": position["stock_name"],
                    "side": "SELL",
                    "status": "LIMIT_LOCKED",
                    "reference_price": raw_open,
                    "execution_price": None,
                    "quantity": position["quantity"],
                    "reason": request["reason"],
                })
                continue
            position["exit_signal_date"] = request["signal_date"]
            execute_sell(
                code,
                position,
                trade_day,
                raw_open,
                request["reason"],
            )

        for request in pending_buys.pop(trade_day, []):
            code = request["stock_code"]
            if code in positions or code not in day_bars.index:
                continue
            if (
                trade_day_index[trade_day]
                <= cooldown_until_index.get(code, -1)
            ):
                orders.append({
                    "trade_date": _iso(trade_day),
                    "stock_code": code,
                    "stock_name": request["stock_name"],
                    "side": "BUY",
                    "status": "COOLDOWN",
                    "reference_price": request["reference_price"],
                    "execution_price": None,
                    "quantity": request["target_quantity"],
                    "reason": "最近退出后冷却5个交易日，避免立即反复试错",
                })
                continue
            if len(positions) >= int(policy["maximum_positions"]):
                continue
            row = day_bars.loc[code]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[0]
            if _float(row["amount"]) <= 0:
                orders.append({
                    "trade_date": _iso(trade_day),
                    "stock_code": code,
                    "stock_name": request["stock_name"],
                    "side": "BUY",
                    "status": "SUSPENDED",
                    "reference_price": request["reference_price"],
                    "execution_price": None,
                    "quantity": request["target_quantity"],
                    "reason": "停牌或零成交，订单失效",
                })
                continue
            raw_open = _float(row["raw_open"])
            limit_price = request["reference_price"] * 1.005
            if raw_open > limit_price:
                orders.append({
                    "trade_date": _iso(trade_day),
                    "stock_code": code,
                    "stock_name": request["stock_name"],
                    "side": "BUY",
                    "status": "NOT_FILLED_GAP",
                    "reference_price": request["reference_price"],
                    "execution_price": None,
                    "quantity": request["target_quantity"],
                    "reason": "次日开盘超过不追高限价0.5%",
                })
                continue
            pre_close = _float(row.get("pre_close"))
            limit_rate = _limit_rate(code)
            if (
                pre_close > 0
                and raw_open >= pre_close * (1.0 + limit_rate - 0.002)
                and abs(_float(row["raw_high"]) - _float(row["raw_low"]))
                < 1e-6
            ):
                orders.append({
                    "trade_date": _iso(trade_day),
                    "stock_code": code,
                    "stock_name": request["stock_name"],
                    "side": "BUY",
                    "status": "LIMIT_LOCKED",
                    "reference_price": request["reference_price"],
                    "execution_price": None,
                    "quantity": request["target_quantity"],
                    "reason": "一字涨停或涨停锁死，无法成交",
                })
                continue
            execution_price = raw_open * (
                1.0 + float(account["default_slippage_rate"])
            )
            target_quantity = int(request["target_quantity"])
            probe_quantity = max(1, target_quantity // 200) * 100
            if (
                probe_quantity * execution_price
                >= float(policy["minimum_economic_order_cny"])
            ):
                quantity = min(target_quantity, probe_quantity)
            else:
                quantity = target_quantity
            value = quantity * execution_price
            buy_fee = _fee(value, sell=False, config=config)
            if quantity <= 0 or value + buy_fee > cash:
                continue
            cash -= value + buy_fee
            total_cost += buy_fee
            trade_index = len(trades)
            trades.append({
                "trade_id": f"T{trade_index + 1:03d}",
                "stock_code": code,
                "stock_name": request["stock_name"],
                "strategy_keys": request["strategy_keys"],
                "entry_signal_date": request["signal_date"],
                "entry_date": _iso(trade_day),
                "entry_price": round(execution_price, 4),
                "buy_quantity": quantity,
                "buy_fee_cny": round(buy_fee, 2),
                "exit_signal_date": "",
                "exit_date": "",
                "exit_price": None,
                "sell_quantity": None,
                "sell_fee_cny": None,
                "net_pnl_cny": None,
                "net_return_pct": None,
                "exit_reason": "",
                "holding_trade_days": 0,
                "status": "OPEN",
            })
            positions[code] = {
                "stock_name": request["stock_name"],
                "strategy_keys": request["strategy_keys"],
                "quantity": quantity,
                "entry_price": execution_price,
                "entry_date": trade_day,
                "buy_fee": buy_fee,
                "protective_stop": execution_price
                * (1.0 + request["initial_stop_pct"] / 100.0),
                "holding_trade_days": 0,
                "trade_index": trade_index,
            }
            orders.append({
                "trade_date": _iso(trade_day),
                "stock_code": code,
                "stock_name": request["stock_name"],
                "side": "BUY",
                "status": "FILLED",
                "reference_price": round(
                    request["reference_price"],
                    4,
                ),
                "execution_price": round(execution_price, 4),
                "quantity": quantity,
                "reason": "V3多策略组合次日限价试仓",
            })

        for code, position in list(positions.items()):
            if code not in day_bars.index:
                continue
            row = day_bars.loc[code]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[0]
            if _float(row["amount"]) <= 0:
                continue
            position["holding_trade_days"] += 1

        position_values: dict[str, float] = {}
        current_theme_weights: dict[str, float] = defaultdict(float)
        snapshot = snapshots[trade_day]
        stock_map = {
            item["stock_code"]: item for item in snapshot["stocks"]
        }
        for code, position in positions.items():
            if code not in day_bars.index:
                continue
            row = day_bars.loc[code]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[0]
            value = position["quantity"] * _float(row["raw_close"])
            position_values[code] = value
        marked_before_decision = cash + sum(position_values.values())
        current_position_weights = {
            code: value / max(marked_before_decision, 1.0)
            for code, value in position_values.items()
        }
        current_position_quantities = {
            code: int(position["quantity"])
            for code, position in positions.items()
        }
        current_position_themes: dict[str, tuple[str, ...]] = {}
        for code, value in position_values.items():
            stock = stock_map.get(code, {})
            themes = tuple(
                str(theme)
                for theme in (
                    stock.get("theme_codes")
                    or (stock.get("theme_code"),)
                )
                if theme
            )
            current_position_themes[code] = themes
            weight = value / max(marked_before_decision, 1.0)
            for theme in themes:
                current_theme_weights[theme] += weight
        current_open_risk_weight = sum(
            max(
                float(position["entry_price"])
                - float(position["protective_stop"]),
                0.0,
            )
            * int(position["quantity"])
            for position in positions.values()
        ) / max(marked_before_decision, 1.0)

        forecasts = []
        by_sleeve: dict[str, list[Any]] = defaultdict(list)
        feature_time = datetime.combine(
            trade_day.date(),
            datetime.min.time(),
        ).replace(hour=15)
        valid_until = feature_time + timedelta(days=30)
        for stock in snapshot["stocks"]:
            stock_forecasts = engine.evaluate_stock(
                stock["stock_code"],
                stock["stock_name"],
                stock,
                feature_time,
                valid_until,
            )
            forecasts.extend(stock_forecasts)
            for item in stock_forecasts:
                by_sleeve[item.strategy_key].append(item)
        decision = engine.decide(
            forecasts,
            market_features=snapshot["market_features"],
            prices={
                item["stock_code"]: item["price"]
                for item in snapshot["stocks"]
            },
            equity=marked_before_decision,
            current_theme_weights=dict(current_theme_weights),
            current_position_weights=current_position_weights,
            current_position_quantities=current_position_quantities,
            current_position_themes=current_position_themes,
            current_open_risk_weight=current_open_risk_weight,
            allow_paper_discovery=True,
        )
        target_map = {
            item["stock_code"]: item
            for item in decision["portfolio"]["targets"]
        }
        target_codes = {
            item["stock_code"] for item in target_map.values()
        }
        rejection_map = {
            item["stock_code"]: item
            for item in decision["portfolio"]["rejected"]
        }
        for key in DAILY_SLEEVES:
            top = sorted(
                by_sleeve.get(key, []),
                key=lambda item: (
                    -_float(item.raw_score),
                    item.stock_code,
                ),
            )[:10]
            for rank_no, forecast in enumerate(top, start=1):
                rejected = rejection_map.get(forecast.stock_code, {})
                candidates.append({
                    "trade_date": _iso(trade_day),
                    "strategy_key": key,
                    "strategy_name": SLEEVE_NAMES[key],
                    "rank_no": rank_no,
                    "stock_code": forecast.stock_code,
                    "stock_name": forecast.stock_name,
                    "raw_score": forecast.raw_score,
                    "forecast_status": forecast.status,
                    "expected_return_net_pct": (
                        forecast.expected_return_net_pct
                    ),
                    "probability_positive": forecast.probability_positive,
                    "profit_factor": forecast.profit_factor,
                    "payoff_ratio": forecast.payoff_ratio,
                    "theme_code": forecast.theme_code,
                    "action": (
                        (
                            "次日模拟试错买入"
                            if "paper_discovery"
                            in target_map[forecast.stock_code][
                                "strategy_keys"
                            ]
                            else "次日计划买入"
                        )
                        if forecast.stock_code in target_map
                        else "观察/未入组合"
                    ),
                    "rejection_code": rejected.get("reason_code", ""),
                    "reason": (
                        rejected.get("reason")
                        or "；".join(forecast.reasons)
                    ),
                })

        research_by_code: dict[str, list[Any]] = defaultdict(list)
        for forecast in forecasts:
            if forecast.status in {
                "VALIDATED_POSITIVE",
                "RESEARCH_ONLY_UNCALIBRATED",
                "RESEARCH_ONLY_PROFIT_GATE_FAILED",
                "RESEARCH_ONLY_MODEL_VERSION_MISMATCH",
            }:
                research_by_code[forecast.stock_code].append(forecast)
        following = next_day.get(trade_day)
        if following:
            for code, position in list(positions.items()):
                if code not in day_bars.index:
                    continue
                row = day_bars.loc[code]
                if isinstance(row, pd.DataFrame):
                    row = row.iloc[0]
                reason = daily_exit_reason(
                    protective_stop=position["protective_stop"],
                    session_low=row["raw_low"],
                    close_above_ma20=row["close_above_ma20"],
                    ma20_above_ma60=row["ma20_above_ma60"],
                )
                discovery_position = (
                    "paper_discovery"
                    in set(position.get("strategy_keys") or [])
                )
                discovery_ended = bool(
                    discovery_position and code not in target_codes
                )
                if reason is not None or discovery_ended:
                    resolved_reason = (
                        reason
                        if reason is not None
                        else "PAPER_DISCOVERY_SIGNAL_ENDED"
                    )
                    pending_sells[following].append({
                        "stock_code": code,
                        "signal_date": _iso(trade_day),
                        "reason": resolved_reason,
                    })
            available = max(
                0,
                int(policy["maximum_positions"])
                - len(positions)
                - len(pending_buys.get(following, [])),
            )
            for target in decision["portfolio"]["targets"]:
                code = str(target["stock_code"])
                if available <= 0:
                    break
                if code in positions:
                    continue
                if (
                    trade_day_index[following]
                    <= cooldown_until_index.get(code, -1)
                ):
                    continue
                pending_buys[following].append({
                    "stock_code": code,
                    "stock_name": target["stock_name"],
                    "signal_date": _iso(trade_day),
                    "reference_price": (
                        float(target["target_value"])
                        / max(int(target["target_quantity"]), 1)
                    ),
                    "target_quantity": int(target["target_quantity"]),
                    "initial_stop_pct": min(
                        (
                            item.initial_stop_pct
                            for item in research_by_code.get(code, [])
                        ),
                        default=-5.0,
                    ),
                    "strategy_keys": list(target["strategy_keys"]),
                })
                available -= 1

        marked = cash
        open_rows = []
        for code, position in positions.items():
            if code not in day_bars.index:
                continue
            row = day_bars.loc[code]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[0]
            close_price = _float(row["raw_close"])
            market_value = position["quantity"] * close_price
            marked += market_value
            open_rows.append({
                "stock_code": code,
                "stock_name": position["stock_name"],
                "quantity": position["quantity"],
                "entry_price": position["entry_price"],
                "close_price": close_price,
                "market_value": market_value,
            })
        previous_equity = (
            equity_curve[-1]["equity"] if equity_curve else initial_cash
        )
        regime = decision["regime"]
        daily.append({
            "trade_date": _iso(trade_day),
            "dominant_regime": regime["dominant_state"],
            "risk_asset_cap": regime["risk_asset_cap"],
            "market_return_20d_pct": snapshot["market_features"][
                "market_return_20d_pct"
            ],
            "market_breadth_pct": snapshot["market_features"][
                "market_breadth_pct"
            ],
            "raw_stock_count": len(snapshot["stocks"]),
            "validated_forecast_count": sum(
                item.status == "VALIDATED_POSITIVE"
                for item in forecasts
            ),
            "target_count": len(decision["portfolio"]["targets"]),
            "portfolio_status": decision["portfolio"]["status"],
            "buy_filled_count": sum(
                item["trade_date"] == _iso(trade_day)
                and item["side"] == "BUY"
                and item["status"] == "FILLED"
                for item in orders
            ),
            "sell_filled_count": sum(
                item["trade_date"] == _iso(trade_day)
                and item["side"] == "SELL"
                and item["status"] == "FILLED"
                for item in orders
            ),
            "position_count": len(positions),
            "cash": round(cash, 2),
            "market_value": round(marked - cash, 2),
            "equity": round(marked, 2),
            "daily_return_pct": round(
                (marked / previous_equity - 1.0) * 100.0,
                6,
            ),
            "positions": open_rows,
        })
        equity_curve.append({
            "trade_date": _iso(trade_day),
            "cash": round(cash, 2),
            "market_value": round(marked - cash, 2),
            "equity": round(marked, 2),
            "position_count": len(positions),
            "daily_return_pct": round(
                (marked / previous_equity - 1.0) * 100.0,
                6,
            ),
        })

    final_day = month_days[-1]
    final_bars = bars[final_day]
    open_positions = []
    unrealized = 0.0
    for code, position in positions.items():
        row = final_bars.loc[code]
        if isinstance(row, pd.DataFrame):
            row = row.iloc[0]
        close_price = _float(row["raw_close"])
        estimated_sell_fee = _fee(
            close_price * position["quantity"],
            sell=True,
            config=config,
        )
        pnl = (
            (close_price - position["entry_price"]) * position["quantity"]
            - position["buy_fee"]
            - estimated_sell_fee
        )
        unrealized += pnl
        record = trades[position["trade_index"]]
        record.update({
            "month_end_price": round(close_price, 4),
            "month_end_unrealized_pnl_cny": round(pnl, 2),
            "month_end_unrealized_return_pct": round(
                pnl
                / (position["entry_price"] * position["quantity"])
                * 100.0,
                6,
            ),
        })
        open_positions.append({
            "stock_code": code,
            "stock_name": position["stock_name"],
            "strategy_keys": position["strategy_keys"],
            "entry_date": _iso(position["entry_date"]),
            "entry_price": round(position["entry_price"], 4),
            "quantity": position["quantity"],
            "month_end_price": round(close_price, 4),
            "unrealized_pnl_cny": round(pnl, 2),
        })

    closed = [item for item in trades if item["status"] == "CLOSED"]
    closed_returns = [
        float(item["net_return_pct"]) for item in closed
    ]
    closed_metrics = trade_metrics(closed_returns)
    equity_values = [initial_cash] + [
        float(item["equity"]) for item in equity_curve
    ]
    final_equity = equity_values[-1]
    summary = {
        "initial_cash_cny": initial_cash,
        "final_equity_cny": round(final_equity, 2),
        "net_profit_cny": round(final_equity - initial_cash, 2),
        "total_return_pct": round(
            (final_equity / initial_cash - 1.0) * 100.0,
            6,
        ),
        "maximum_drawdown_pct": round(
            abs(float(maximum_drawdown(equity_values) or 0.0)),
            6,
        ),
        "buy_trade_count": len(trades),
        "closed_trade_count": len(closed),
        "open_position_count": len(open_positions),
        "win_rate": closed_metrics["win_rate"],
        "net_expectancy_pct": closed_metrics["net_expectancy_pct"],
        "average_win_pct": closed_metrics["average_win_pct"],
        "average_loss_pct": closed_metrics["average_loss_pct"],
        "payoff_ratio": closed_metrics["payoff_ratio"],
        "profit_factor": closed_metrics["profit_factor"],
        "realized_net_pnl_cny": round(
            sum(float(item["net_pnl_cny"]) for item in closed),
            2,
        ),
        "month_end_unrealized_pnl_cny": round(unrealized, 2),
        "total_fee_cny": round(total_cost, 2),
    }
    return {
        "summary": summary,
        "daily": daily,
        "candidates": candidates,
        "orders": orders,
        "trades": trades,
        "open_positions": open_positions,
        "equity_curve": equity_curve,
    }


def run_backtest(
    *,
    calibration_start: date,
    calibration_end: date,
    start_date: date,
    end_date: date,
    snapshot_cache: Path | None = None,
    refresh_snapshot_cache: bool = False,
) -> dict[str, Any]:
    load_project_env()
    config = load_v3_config()
    cache_key = {
        "schema": "trading-v3-replay-snapshot-v2-dynamic-exit",
        "calibration_start": calibration_start.isoformat(),
        "calibration_end": calibration_end.isoformat(),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "universe_limit": 1200,
    }
    cached: dict[str, Any] | None = None
    if (
        snapshot_cache is not None
        and snapshot_cache.is_file()
        and not refresh_snapshot_cache
    ):
        with snapshot_cache.open("rb") as handle:
            candidate = pickle.load(handle)
        if candidate.get("cache_key") != cache_key:
            raise RuntimeError(
                "回测快照缓存日期或结构不匹配；请使用 --refresh-snapshot-cache"
            )
        cached = candidate

    if cached is not None:
        features = cached["month_features"]
        month_days = cached["month_days"]
        calibration_days = cached["calibration_days"]
        snapshots = cached["snapshots"]
        outcomes = cached["outcomes"]
        print(
            f"loaded snapshot cache {snapshot_cache} "
            f"days={len(snapshots)}",
            flush=True,
        )
    else:
        primary = create_tool_engine()
        kline = get_kline_engine()
        try:
            history = _load_history(
                kline,
                start_date=calibration_start,
                end_date=end_date,
            )
            all_features = _prepare_feature_frame(history)
            trading_days = sorted(
                all_features["trade_date"].drop_duplicates()
            )
            month_days = [
                item
                for item in trading_days
                if start_date <= item.date() <= end_date
            ]
            calibration_days = _latest_weekly_dates(
                trading_days,
                start_date=calibration_start,
                end_date=calibration_end,
            )
            codes = sorted(
                all_features["stock_code"].astype(str).unique()
            )
            snapshots: dict[pd.Timestamp, dict[str, Any]] = {}
            snapshot_days = sorted(
                set(calibration_days + month_days)
            )
            for index, trade_day in enumerate(snapshot_days, start=1):
                industries = _load_industries(
                    primary,
                    codes,
                    as_of=trade_day.date(),
                )
                snapshots[trade_day] = _snapshot(
                    primary,
                    all_features,
                    trade_day=trade_day,
                    trading_days=trading_days,
                    industries=industries,
                    limit=1200,
                )
                print(
                    f"snapshot {index}/{len(snapshot_days)} "
                    f"{trade_day.date()} rows="
                    f"{len(snapshots[trade_day]['stocks'])}",
                    flush=True,
                )
            outcomes = _dynamic_outcomes(
                all_features,
                {
                    day: snapshots[day]
                    for day in calibration_days
                },
                config=config,
            )
            features = all_features[
                all_features["trade_date"].isin(month_days)
            ].copy()
            if snapshot_cache is not None:
                snapshot_cache.parent.mkdir(
                    parents=True,
                    exist_ok=True,
                )
                with snapshot_cache.open("wb") as handle:
                    pickle.dump(
                        {
                            "cache_key": cache_key,
                            "month_features": features,
                            "month_days": month_days,
                            "calibration_days": calibration_days,
                            "snapshots": snapshots,
                            "outcomes": outcomes,
                        },
                        handle,
                        protocol=pickle.HIGHEST_PROTOCOL,
                    )
                print(
                    f"saved snapshot cache {snapshot_cache}",
                    flush=True,
                )
        finally:
            primary.dispose()
            kline.dispose()

    training_snapshots = {
        day: snapshots[day] for day in calibration_days
    }
    calibrations, calibration_evidence = _calibrate(
        training_snapshots,
        outcomes,
        model_prefix=(
            f"{config['strategy_version']}-latest-research-"
            f"{calibration_end.isoformat()}"
        ),
    )
    simulation = _simulate(
        {day: snapshots[day] for day in month_days},
        calibrations,
        features,
        month_days=month_days,
    )

    snapshot_hash = hashlib.sha256(
        json.dumps(
            {
                _iso(day): {
                    "stock_count": len(snapshot["stocks"]),
                    "market_features": snapshot["market_features"],
                }
                for day, snapshot in snapshots.items()
            },
            ensure_ascii=False,
            sort_keys=True,
            default=str,
        ).encode("utf-8")
    ).hexdigest()
    return {
        "methodology": {
            "strategy_version": config["strategy_version"],
            "replay_scope": "LATEST_MULTI_SLEEVE_RESEARCH",
            "daily_sleeves": list(DAILY_SLEEVES),
            "intraday_sleeve": {
                "strategy_key": "intraday_surprise",
                "included_in_pnl": False,
                "reason": (
                    "精确公式需要Level-1点差和成交概率；"
                    "真实连续记录始于2026-07-27，不能回填7月历史"
                ),
            },
            "calibration_start": calibration_start.isoformat(),
            "calibration_end": calibration_end.isoformat(),
            "validation_start": start_date.isoformat(),
            "validation_end": end_date.isoformat(),
            "calibration_frequency": "每周最后交易日",
            "signal_time": "交易日15:00收盘后",
            "execution": (
                "下一交易日开盘，0.5%不追高限价，"
                "整手、停牌/涨跌停、T+1、动态退出"
            ),
            "account": config["account"],
            "portfolio": config["portfolio"],
            "profit_gate": config["profit_gate"],
            "config_sha256": config_hash(),
            "snapshot_hash": snapshot_hash,
            "future_data_policy": (
                "校准期截至5月；7月每天仅使用当日及以前可得数据"
            ),
            "historical_membership_limit": (
                "7月23日起严格读取QMT当日概念成员快照；"
                "此前无真实概念快照，仅使用申万一级行业回退，"
                "不以当前概念成员回填历史"
            ),
            "paper_discovery": config.get("paper_discovery", {}),
        },
        "calibrations": calibration_evidence,
        **simulation,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--calibration-start", default="2026-01-01")
    parser.add_argument("--calibration-end", default="2026-05-29")
    parser.add_argument("--start-date", default="2026-07-01")
    parser.add_argument("--end-date", default="2026-07-28")
    parser.add_argument("--snapshot-cache", default="")
    parser.add_argument(
        "--refresh-snapshot-cache",
        action="store_true",
    )
    parser.add_argument(
        "--output",
        default=(
            "outputs/20260728_v3_latest_july_backtest/"
            "latest_multi_sleeve_replay.json"
        ),
    )
    args = parser.parse_args()
    result = run_backtest(
        calibration_start=date.fromisoformat(args.calibration_start),
        calibration_end=date.fromisoformat(args.calibration_end),
        start_date=date.fromisoformat(args.start_date),
        end_date=date.fromisoformat(args.end_date),
        snapshot_cache=(
            (ROOT / args.snapshot_cache).resolve()
            if args.snapshot_cache
            else None
        ),
        refresh_snapshot_cache=bool(args.refresh_snapshot_cache),
    )
    output = ROOT / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(
            result,
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    print(json.dumps({
        "status": "ok",
        "summary": result["summary"],
        "calibrations": result["calibrations"],
        "output": str(output),
    }, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
