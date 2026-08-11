#!/usr/bin/env python3
"""Research-only scan of simple, predeclared trend entry families.

The script never registers a model. It exists to diagnose whether the entry
problem is continuation, extension, breakout chasing, or pullback timing.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.kline_data import get_kline_engine
from server.trading_v3.backtest import (
    _band_series,
    _build_features,
    _dynamic_signal_outcome,
    _load_history,
    _scaled,
)
from server.trading_v3.config import load_v3_config
from server.trading_v3.metrics import trade_metrics
from tools.env_config import load_project_env


def _summary(frame: pd.DataFrame) -> dict:
    values = (
        frame["net_return_pct"].astype(float).tolist()
        if "net_return_pct" in frame
        else []
    )
    result = trade_metrics(values)
    result["sample_count"] = len(values)
    result["win_rate"] = (
        sum(value > 0 for value in values) / len(values)
        if values
        else None
    )
    return result


def _diagnostics(frame: pd.DataFrame) -> dict:
    if frame.empty:
        return {
            "half_year": {},
            "exit_reason": {},
            "score_quintile": {},
        }
    working = frame.copy()
    trade_dates = pd.to_datetime(working["trade_date"])
    working["half_year"] = (
        trade_dates.dt.year.astype(str)
        + "H"
        + ((trade_dates.dt.month.sub(1) // 6) + 1).astype(str)
    )
    half_year = {
        str(key): _summary(group)
        for key, group in working.groupby("half_year")
    }
    exit_reason = {
        str(key): _summary(group)
        for key, group in working.groupby("exit_reason")
    }
    ranked = working["score"].rank(method="first")
    working["score_quintile"] = pd.qcut(
        ranked,
        q=min(5, len(working)),
        labels=False,
        duplicates="drop",
    )
    score_quintile = {
        str(int(key) + 1): {
            **_summary(group),
            "minimum_score": float(group["score"].min()),
            "maximum_score": float(group["score"].max()),
        }
        for key, group in working.groupby("score_quintile")
    }
    regime = {}
    regime_specs = {
        "market_return_60d_pct": (
            [-math.inf, 0, 3, 6, 10, math.inf],
            ["<0", "0-3", "3-6", "6-10", ">=10"],
        ),
        "market_breadth_ma20_pct": (
            [-math.inf, 45, 55, 65, math.inf],
            ["<45", "45-55", "55-65", ">=65"],
        ),
        "market_aligned_breadth_pct": (
            [-math.inf, 35, 45, 55, math.inf],
            ["<35", "35-45", "45-55", ">=55"],
        ),
        "market_return_20d_change_10d_pct": (
            [-math.inf, -3, 0, 3, math.inf],
            ["<-3", "-3-0", "0-3", ">=3"],
        ),
    }
    for column, (bins, labels) in regime_specs.items():
        if column not in working:
            continue
        bucket_column = f"_{column}_bucket"
        working[bucket_column] = pd.cut(
            working[column],
            bins=bins,
            labels=labels,
            right=False,
        )
        regime[column] = {
            str(key): _summary(group)
            for key, group in working.groupby(
                bucket_column,
                observed=True,
            )
        }
    return {
        "half_year": half_year,
        "exit_reason": exit_reason,
        "score_quintile": score_quintile,
        "market_regime": regime,
    }


def _dynamic_labels(
    frame: pd.DataFrame,
    selected: pd.DataFrame,
    *,
    config: dict,
) -> pd.DataFrame:
    selected_by_code = {
        str(code): group.to_dict("records")
        for code, group in selected.groupby(
            "stock_code",
            sort=False,
            observed=True,
        )
    }
    records = []
    for code_value, code_group in frame.groupby(
        "stock_code",
        sort=False,
        observed=True,
    ):
        code = str(code_value)
        candidates = selected_by_code.get(code)
        if not candidates:
            continue
        group = code_group.reset_index(drop=True)
        locations = {
            pd.Timestamp(trade_date): index
            for index, trade_date in enumerate(group["trade_date"])
        }
        for item in candidates:
            signal_index = locations.get(
                pd.Timestamp(item["trade_date"])
            )
            if signal_index is None:
                continue
            outcome = _dynamic_signal_outcome(
                group,
                signal_index=signal_index,
                config=config,
                initial_stop_pct=float(item["initial_stop_pct"]),
            )
            if outcome is not None:
                records.append({**item, **outcome})
    return pd.DataFrame(records)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--families",
        help="comma-separated family names; default scans every family",
    )
    parser.add_argument(
        "--output",
        default="artifacts/trading_v3/signal_family_scan.json",
    )
    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        default=date(2024, 1, 1),
    )
    parser.add_argument(
        "--end-date",
        type=date.fromisoformat,
        default=date(2026, 7, 27),
    )
    parser.add_argument(
        "--training-end",
        type=date.fromisoformat,
        default=date(2025, 6, 30),
    )
    parser.add_argument(
        "--validation-start",
        type=date.fromisoformat,
        default=date(2025, 7, 1),
    )
    args = parser.parse_args()
    load_project_env()
    engine = get_kline_engine()
    try:
        history = _load_history(
            engine,
            start_date=args.start_date,
            end_date=args.end_date,
        )
    finally:
        engine.dispose()
    frame = _build_features(history)
    market_return_60d = frame.groupby("trade_date")[
        "return_60d_pct"
    ].median()
    frame["market_return_60d_pct"] = frame["trade_date"].map(
        market_return_60d
    )
    market_daily = frame.groupby("trade_date").agg(
        market_breadth_ma20_pct=(
            "close_above_ma20",
            lambda values: float(values.mean()) * 100.0,
        ),
        market_aligned_breadth_pct=(
            "ma20_above_ma60",
            lambda values: float(values.mean()) * 100.0,
        ),
    )
    market_daily["market_return_20d_change_10d_pct"] = (
        frame.groupby("trade_date")["market_return_20d_pct"].first()
        - frame.groupby("trade_date")[
            "market_return_20d_pct"
        ].first().shift(10)
    )
    for column in market_daily.columns:
        frame[column] = frame["trade_date"].map(
            market_daily[column]
        )
    trend_alignment = (
        (frame["close_above_ma20"] == 1)
        & (frame["ma20_above_ma60"] == 1)
    ).astype(float)
    frame["balanced_pullback_score"] = (
        0.18 * trend_alignment
        + 0.16 * _band_series(
            frame["return_60d_pct"], 10, 35, 15
        )
        + 0.14 * _band_series(
            frame["return_20d_pct"], 2, 15, 10
        )
        + 0.14 * _band_series(
            frame["relative_strength_20d_pct"], 3, 18, 12
        )
        + 0.12 * _band_series(
            frame["distance_ma20_pct"], 0.5, 4, 4
        )
        + 0.10 * _band_series(
            frame["amount_ratio_5_20"], 0.8, 1.4, 0.7
        )
        + 0.10 * _band_series(
            frame["atr_14d_pct"], 1.2, 4, 2
        )
        + 0.06 * _band_series(
            frame["change_pct"], -1, 3, 4
        )
    ).clip(lower=0, upper=1)
    frame["balanced_relative_strength_score"] = (
        0.18 * trend_alignment
        + 0.20 * _band_series(
            frame["relative_strength_20d_pct"], 5, 18, 10
        )
        + 0.14 * _band_series(
            frame["return_60d_pct"], 8, 35, 15
        )
        + 0.12 * _band_series(
            frame["ma20_slope_5d_pct"], 0.2, 2.5, 2
        )
        + 0.12 * _band_series(
            frame["distance_ma20_pct"], 0.5, 4.5, 4
        )
        + 0.10 * _band_series(
            frame["amount_ratio_5_20"], 0.8, 1.5, 0.8
        )
        + 0.08 * _band_series(
            frame["atr_14d_pct"], 1.2, 4, 2
        )
        + 0.06 * _band_series(
            frame["change_pct"], -1, 3.5, 4
        )
    ).clip(lower=0, upper=1)
    # The production eligibility rules already require an established trend.
    # The current score keeps rewarding more extension inside that eligible
    # set, even though the production walk-forward shows that the highest
    # scores have the worst hard-stop rate.  These two predeclared alternatives
    # test whether ranking the *least extended qualifying trend* restores score
    # direction without weakening any eligibility or model gates.
    frame["inverse_production_score"] = 1.0 - frame["score"]
    frame["risk_aware_trend_score"] = (
        0.22 * _scaled(
            frame["relative_strength_20d_pct"], 2, 22
        )
        + 0.18 * _scaled(
            frame["ma20_slope_5d_pct"], 0.2, 4
        )
        + 0.12 * _scaled(
            frame["return_60d_pct"], 12, 55
        )
        + 0.08 * _band_series(
            frame["amount_ratio_5_20"], 0.9, 1.35, 0.45
        )
        + 0.16 * (
            1.0 - _scaled(
                frame["distance_ma20_pct"], 0, 8
            )
        )
        + 0.12 * (
            1.0 - _scaled(
                frame["atr_14d_pct"], 1, 5
            )
        )
        + 0.07 * (
            1.0 - _scaled(
                frame["return_5d_pct"], 0, 12
            )
        )
        + 0.05 * (
            1.0 - _scaled(
                frame["change_pct"], 0, 9.5
            )
        )
    ).clip(lower=0, upper=1)
    defensive_entry_score = (
        0.50 * (
            1.0 - _scaled(frame["atr_14d_pct"], 1, 5)
        )
        + 0.30 * (
            1.0 - _scaled(frame["distance_ma20_pct"], 0, 8)
        )
        + 0.20 * (
            1.0 - _scaled(frame["return_5d_pct"], 0, 12)
        )
    )
    frame["defensive_entry_score"] = defensive_entry_score.clip(
        lower=0,
        upper=1,
    )
    frame["regime_defensive_score"] = (
        0.20 * _scaled(
            frame["market_return_60d_pct"], 3, 15
        )
        + 0.15 * _scaled(
            frame["market_breadth_ma20_pct"], 65, 80
        )
        + 0.15 * _scaled(
            frame["market_aligned_breadth_pct"], 55, 75
        )
        + 0.10 * _scaled(
            frame["market_return_20d_change_10d_pct"], 0, 5
        )
        + 0.40 * defensive_entry_score
    ).clip(lower=0, upper=1)
    config = load_v3_config()
    family_scores: dict[str, str] = {}
    families = {
        "moderate_trend_pullback": (
            (frame["close_above_ma20"] == 1)
            & (frame["ma20_above_ma60"] == 1)
            & frame["return_60d_pct"].between(8, 45)
            & frame["return_20d_pct"].between(0, 18)
            & frame["distance_ma20_pct"].between(-1, 6)
            & frame["amount_ratio_5_20"].between(0.7, 1.5)
            & frame["change_pct"].between(-2, 4)
        ),
        "breakout_with_volume": (
            (frame["close_above_ma20"] == 1)
            & (frame["ma20_above_ma60"] == 1)
            & frame["return_20d_pct"].between(5, 28)
            & frame["distance_ma20_pct"].between(2, 10)
            & (frame["breakout_20d_proximity"] >= 0.98)
            & frame["amount_ratio_5_20"].between(1.1, 2.2)
            & frame["change_pct"].between(0.5, 8)
        ),
        "trend_reacceleration": (
            (frame["close_above_ma20"] == 1)
            & (frame["ma20_above_ma60"] == 1)
            & frame["return_60d_pct"].between(12, 55)
            & frame["return_20d_pct"].between(2, 22)
            & frame["ma20_slope_5d_pct"].between(0.2, 4)
            & frame["distance_ma20_pct"].between(0, 8)
            & frame["amount_ratio_5_20"].between(0.9, 1.8)
        ),
        "relative_strength_not_extended": (
            (frame["close_above_ma20"] == 1)
            & (frame["ma20_above_ma60"] == 1)
            & frame["relative_strength_20d_pct"].between(5, 22)
            & frame["distance_ma20_pct"].between(0, 7)
            & frame["atr_14d_pct"].between(1, 5)
            & frame["change_pct"].between(-1, 5)
        ),
        "ma20_reclaim": (
            (frame["ma20_above_ma60"] == 1)
            & frame["distance_ma20_pct"].between(0, 2.5)
            & frame["return_60d_pct"].between(5, 45)
            & frame["change_pct"].between(0.5, 6)
            & frame["amount_ratio_5_20"].between(0.8, 2)
        ),
    }
    base_families = dict(families)
    for family_name, family_mask in base_families.items():
        families[family_name + "_market_nonnegative"] = (
            family_mask & (frame["market_return_20d_pct"] >= 0)
        )
        families[family_name + "_market_positive"] = (
            family_mask & (frame["market_return_20d_pct"] >= 2)
        )
    production_eligibility = (
        base_families["trend_reacceleration"]
        & (frame["market_return_20d_pct"] >= 2)
    )
    rank_candidates = {
        "trend_reacceleration_inverse_rank": (
            production_eligibility,
            "inverse_production_score",
        ),
        "trend_reacceleration_risk_aware_rank": (
            production_eligibility,
            "risk_aware_trend_score",
        ),
        "trend_reacceleration_risk_aware_atr4": (
            production_eligibility
            & frame["atr_14d_pct"].between(1, 4),
            "risk_aware_trend_score",
        ),
        "trend_reacceleration_inverse_atr4": (
            production_eligibility
            & frame["atr_14d_pct"].between(1, 4),
            "inverse_production_score",
        ),
    }
    for family_name, (family_mask, score_column) in (
        rank_candidates.items()
    ):
        families[family_name] = family_mask
        family_scores[family_name] = score_column
    balanced_pullback = (
        base_families["moderate_trend_pullback"]
        & (frame["market_return_20d_pct"] >= 2)
        & frame["atr_14d_pct"].between(1.0, 4.5)
        & frame["return_5d_pct"].between(-3, 10)
    )
    balanced_relative_strength = (
        base_families["relative_strength_not_extended"]
        & (frame["market_return_20d_pct"] >= 2)
        & frame["return_5d_pct"].between(-2, 10)
        & frame["change_pct"].between(-1, 4)
    )
    for threshold in (0, 3, 6):
        pullback_name = (
            "balanced_pullback_market60_"
            + str(threshold)
        )
        relative_name = (
            "balanced_relative_strength_market60_"
            + str(threshold)
        )
        families[pullback_name] = (
            balanced_pullback
            & (frame["market_return_60d_pct"] >= threshold)
        )
        families[relative_name] = (
            balanced_relative_strength
            & (frame["market_return_60d_pct"] >= threshold)
        )
        family_scores[pullback_name] = "balanced_pullback_score"
        family_scores[relative_name] = (
            "balanced_relative_strength_score"
        )
    sustained_regime = (
        (frame["market_return_60d_pct"] >= 3)
        & (frame["market_breadth_ma20_pct"] >= 55)
        & (
            frame["market_return_20d_change_10d_pct"]
            >= -1
        )
    )
    strict_sustained_regime = (
        (frame["market_return_60d_pct"] >= 6)
        & (frame["market_breadth_ma20_pct"] >= 60)
        & (
            frame["market_return_20d_change_10d_pct"]
            >= 0
        )
    )
    broad_sustained_regime = (
        (frame["market_return_60d_pct"] >= 3)
        & (frame["market_breadth_ma20_pct"] >= 65)
        & (frame["market_aligned_breadth_pct"] >= 55)
        & (
            frame["market_return_20d_change_10d_pct"]
            >= 0
        )
    )
    regime_candidates = {
        "trend_reacceleration_sustained_regime": (
            production_eligibility & sustained_regime,
            "score",
        ),
        "trend_reacceleration_inverse_atr4_sustained_regime": (
            production_eligibility
            & frame["atr_14d_pct"].between(1, 4)
            & sustained_regime,
            "inverse_production_score",
        ),
        "trend_reacceleration_strict_sustained_regime": (
            production_eligibility & strict_sustained_regime,
            "score",
        ),
        "moderate_pullback_sustained_regime": (
            base_families["moderate_trend_pullback"]
            & (frame["market_return_20d_pct"] >= 2)
            & sustained_regime,
            "score",
        ),
        "relative_strength_sustained_regime": (
            base_families["relative_strength_not_extended"]
            & (frame["market_return_20d_pct"] >= 2)
            & sustained_regime,
            "score",
        ),
        "moderate_pullback_broad_regime_defensive": (
            base_families["moderate_trend_pullback"]
            & (frame["market_return_20d_pct"] >= 2)
            & broad_sustained_regime,
            "defensive_entry_score",
        ),
        "moderate_pullback_broad_regime_scored": (
            base_families["moderate_trend_pullback"]
            & (frame["market_return_20d_pct"] >= 2)
            & broad_sustained_regime,
            "regime_defensive_score",
        ),
        "relative_strength_broad_regime_defensive": (
            base_families["relative_strength_not_extended"]
            & (frame["market_return_20d_pct"] >= 2)
            & broad_sustained_regime,
            "defensive_entry_score",
        ),
        "relative_strength_broad_regime_scored": (
            base_families["relative_strength_not_extended"]
            & (frame["market_return_20d_pct"] >= 2)
            & broad_sustained_regime,
            "regime_defensive_score",
        ),
    }
    for family_name, (family_mask, score_column) in (
        regime_candidates.items()
    ):
        families[family_name] = family_mask
        family_scores[family_name] = score_column
    if args.families:
        requested = {
            item.strip()
            for item in args.families.split(",")
            if item.strip()
        }
        unknown = requested.difference(families)
        if unknown:
            raise ValueError(
                "unknown families: " + ", ".join(sorted(unknown))
            )
        families = {
            name: mask
            for name, mask in families.items()
            if name in requested
        }
    results = {}
    for name, mask in families.items():
        selected = frame[
            mask
            & (frame["amount20"] >= 50_000_000)
            & (frame["raw_close"] >= 2)
            & (frame["change_pct"] < 9.5)
            & (frame["trade_date"].dt.date >= args.start_date)
            & (frame["trade_date"].dt.date <= args.end_date)
        ].copy()
        score_column = family_scores.get(name, "score")
        selected["score"] = frame.loc[
            selected.index,
            score_column,
        ]
        selected["rank"] = selected.groupby("trade_date")[
            "score"
        ].rank(method="first", ascending=False)
        selected = selected[selected["rank"] <= 10]
        labelled = _dynamic_labels(
            frame,
            selected,
            config=config,
        )
        if labelled.empty:
            results[name] = {
                "label_protocol": "DYNAMIC_PRODUCTION_EXIT_NEXT_OPEN",
                "training": _summary(labelled),
                "validation": _summary(labelled),
            }
            continue
        train = labelled[
            (
                labelled["trade_date"].dt.date
                <= args.training_end
            )
            & (
                labelled["exit_date"].dt.date
                <= args.training_end
            )
        ]
        oos = labelled[
            labelled["trade_date"].dt.date >= args.validation_start
        ]
        results[name] = {
            "label_protocol": "DYNAMIC_PRODUCTION_EXIT_NEXT_OPEN",
            "training": _summary(train),
            "validation": _summary(oos),
            "diagnostics": _diagnostics(labelled),
        }
    output = (ROOT / args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(results, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    ranked = sorted(
        results.items(),
        key=lambda item: (
            -float(
                item[1]["validation"].get("profit_factor") or 0
            ),
            -float(
                item[1]["validation"].get("net_expectancy_pct") or -999
            ),
        ),
    )
    print(json.dumps(
        {"status": "ok", "top": ranked[:20], "artifact": str(output)},
        ensure_ascii=False,
        indent=2,
        default=str,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
