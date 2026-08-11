#!/usr/bin/env python3
"""Run the frozen monthly quality/value/low-volatility OOS campaign.

This command is research-only.  Historical results can create a paper
candidate artifact, but cannot register a production model or submit orders.
"""
from __future__ import annotations

import argparse
import gc
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.trading_v3.backtest import _band_series
from server.trading_v3.config import load_v3_config
from server.trading_v3.research_v4 import (
    _common_liquid_mask,
    aggregate_candidate,
    attach_point_in_time_finance,
    evaluate_fold,
    label_bounded_candidate,
    select_top_per_day,
)
from tools.env_config import load_project_env
from tools.research_trading_v4_bounded_campaign import (
    _load_or_build_features,
    _raw_aggregate,
)
from tools.research_trading_v4_campaign import (
    _close_by_day,
    _contract_hash,
    _load_finance_rows,
    _max_t_adjustment,
)


def _monthly_market(frame: pd.DataFrame) -> pd.DataFrame:
    """Return the complete cross-section on each calendar month end."""

    month = frame["trade_date"].dt.to_period("M")
    month_ends = frame.groupby(month, observed=True)["trade_date"].max()
    return frame[frame["trade_date"].isin(month_ends)].copy()


def _finance_enriched_monthly(frame: pd.DataFrame) -> pd.DataFrame:
    monthly = _monthly_market(frame)
    monthly = monthly.loc[
        _common_liquid_mask(monthly)
        & (monthly["amount20"] >= 100_000_000)
        & (monthly["raw_close"] >= 2.0)
    ].copy()
    finance_rows = _load_finance_rows(frame["trade_date"].max().date())
    try:
        enriched = attach_point_in_time_finance(
            monthly,
            market_frame=frame,
            finance_rows=finance_rows,
        )
    finally:
        del finance_rows
        gc.collect()
    if enriched.empty:
        return enriched
    enriched["momentum60_rank"] = enriched.groupby(
        "trade_date", observed=True
    )["return_60d_pct"].rank(pct=True)
    enriched["inverse_atr_rank"] = enriched.groupby(
        "trade_date", observed=True
    )["atr_14d_pct"].rank(pct=True, ascending=False)
    return enriched


def _base_finance_mask(frame: pd.DataFrame) -> pd.Series:
    return (
        frame["quality_percentile"].notna()
        & frame["cashflow_percentile"].notna()
        & frame["valuation_percentile"].notna()
        & frame["asset_liab_ratio_pit"].notna()
        & frame["net_profit_yoy_gr_pit"].notna()
    )


def _candidate_universes(frame: pd.DataFrame) -> dict[str, pd.DataFrame]:
    common = _base_finance_mask(frame)
    value_mask = (
        common
        & (frame["quality_percentile"] >= 0.55)
        & (frame["cashflow_percentile"] >= 0.50)
        & (frame["asset_liab_ratio_pit"] <= 70)
        & (frame["net_profit_yoy_gr_pit"] > -30)
        & frame["return_60d_pct"].between(-10, 40)
        & frame["return_20d_pct"].between(-10, 20)
        & frame["atr_14d_pct"].between(1, 5)
    )
    value_score = (
        0.35 * frame["quality_percentile"]
        + 0.20 * frame["cashflow_percentile"]
        + 0.20 * frame["valuation_percentile"]
        + 0.15 * frame["inverse_atr_rank"]
        + 0.10 * _band_series(frame["return_20d_pct"], -5, 12, 10)
    ).clip(lower=0.0, upper=1.0)

    momentum_mask = (
        common
        & (frame["quality_percentile"] >= 0.50)
        & (frame["cashflow_percentile"] >= 0.50)
        & (frame["asset_liab_ratio_pit"] <= 70)
        & (frame["net_profit_yoy_gr_pit"] > -30)
        & frame["return_60d_pct"].between(0, 50)
        & frame["return_20d_pct"].between(-5, 25)
        & frame["atr_14d_pct"].between(1, 5)
    )
    momentum_score = (
        0.30 * frame["quality_percentile"]
        + 0.15 * frame["cashflow_percentile"]
        + 0.10 * frame["valuation_percentile"]
        + 0.25 * frame["momentum60_rank"]
        + 0.20 * frame["inverse_atr_rank"]
    ).clip(lower=0.0, upper=1.0)

    defensive_mask = (
        value_mask
        & (frame["market_return_60d_pct"] >= 0)
        & (frame["market_aligned_breadth_pct"] >= 45)
        & (frame["market_daily_return_pct"] > -2.5)
    )
    defensive_score = (
        0.40 * frame["quality_percentile"]
        + 0.20 * frame["cashflow_percentile"]
        + 0.20 * frame["valuation_percentile"]
        + 0.20 * frame["inverse_atr_rank"]
    ).clip(lower=0.0, upper=1.0)

    definitions = {
        "monthly_quality_value_lowvol_v1": (value_mask, value_score),
        "monthly_quality_momentum_lowvol_v1": (
            momentum_mask,
            momentum_score,
        ),
        "monthly_defensive_quality_v1": (defensive_mask, defensive_score),
    }
    universes: dict[str, pd.DataFrame] = {}
    for candidate_id, (mask, score) in definitions.items():
        selected = frame.loc[mask].copy()
        selected["score"] = score.loc[selected.index].astype("float32")
        selected["candidate_id"] = candidate_id
        selected["exit_sleeve"] = "monthly_bounded"
        if "short_name" not in selected:
            selected["short_name"] = selected["stock_code"].astype(str)
        universes[candidate_id] = selected
    return universes


def _raw_gate_failures(
    raw: dict[str, Any],
    *,
    gate: dict[str, Any],
) -> list[str]:
    checks = (
        (raw.get("trade_count", 0) < gate["minimum_portfolio_trades"], "RAW_PORTFOLIO_TRADES_TOO_LOW"),
        ((raw.get("net_expectancy_pct") or 0.0) <= gate["minimum_portfolio_net_expectancy_pct"], "RAW_EXPECTANCY_NOT_POSITIVE"),
        ((raw.get("profit_factor") or 0.0) < gate["minimum_portfolio_profit_factor"], "RAW_PROFIT_FACTOR_TOO_LOW"),
        ((raw.get("payoff_ratio") or 0.0) < gate["minimum_portfolio_payoff_ratio"], "RAW_PAYOFF_RATIO_TOO_LOW"),
        (raw.get("maximum_drawdown_pct", 0.0) > gate["maximum_drawdown_pct"], "RAW_DRAWDOWN_TOO_HIGH"),
        (raw.get("positive_outer_folds", 0) < gate["minimum_positive_outer_folds"], "RAW_POSITIVE_OUTER_FOLD_COUNT_TOO_LOW"),
    )
    return [reason for failed, reason in checks if failed]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--protocol",
        default="strategies/trading_v4_monthly_factor_campaign.json",
    )
    parser.add_argument(
        "--output",
        default="artifacts/trading_v4/monthly_factor_oos_20260801.json",
    )
    args = parser.parse_args()
    load_project_env()
    protocol = json.loads((ROOT / args.protocol).read_text(encoding="utf-8"))
    specs = protocol["candidate_control"]["candidates"]
    if len(specs) > int(protocol["candidate_control"]["maximum_new_candidate_count"]):
        raise RuntimeError("candidate count exceeds frozen maximum")
    contract_hash = _contract_hash({
        **protocol,
        "data_protocol": protocol["execution_protocol"],
        "purge_and_embargo": protocol["execution_protocol"],
    })
    print(json.dumps({
        "phase": "load_features",
        "contract_hash": contract_hash,
        "candidate_count": len(specs),
    }), flush=True)
    frame, cache_path = _load_or_build_features()
    print(json.dumps({
        "phase": "features_ready",
        "rows": len(frame),
        "dates": int(frame["trade_date"].nunique()),
        "local_cache": cache_path,
    }), flush=True)
    monthly = _finance_enriched_monthly(frame)
    universes = _candidate_universes(monthly)
    del monthly
    gc.collect()

    execution = protocol["execution_protocol"]
    top_per_day = int(execution["calibration_top_per_signal_date"])
    selected = {
        spec["id"]: select_top_per_day(
            universes[spec["id"]],
            top_per_day=top_per_day,
        )
        for spec in specs
    }
    close_by_day = _close_by_day(frame)
    runtime = load_v3_config()
    results: dict[str, Any] = {}
    raw_trade_map: dict[str, list[dict[str, Any]]] = {}
    for spec in specs:
        candidate_id = spec["id"]
        print(json.dumps({
            "phase": "label_and_evaluate",
            "candidate_id": candidate_id,
            "unranked_rows": len(universes[candidate_id]),
            "selected_rows": len(selected[candidate_id]),
        }), flush=True)
        labels = label_bounded_candidate(
            frame,
            selected[candidate_id],
            stop_pct=float(spec["stop_pct"]),
            take_profit_pct=float(execution["take_profit_pct"]),
            maximum_holding_sessions=int(execution["maximum_holding_sessions"]),
            maximum_entry_gap_pct=float(execution["maximum_entry_gap_pct"]),
            config=runtime,
        )
        folds = [
            evaluate_fold(
                labels,
                fold=fold,
                candidate_id=candidate_id,
                close_by_day=close_by_day,
                config=runtime,
                bucket_count=int(execution["calibration_bucket_count"]),
            )
            for fold in protocol["outer_folds"]
        ]
        aggregate = aggregate_candidate(
            folds,
            config=runtime,
            required_positive_folds=int(
                protocol["profit_gate"]["minimum_positive_outer_folds"]
            ),
            minimum_fold_profit_factor=float(
                protocol["profit_gate"]["minimum_outer_fold_profit_factor"]
            ),
        )
        raw = _raw_aggregate(
            folds,
            float(protocol["profit_gate"]["minimum_outer_fold_profit_factor"]),
        )
        raw["gate_failures"] = _raw_gate_failures(
            raw,
            gate=protocol["profit_gate"],
        )
        raw["gate_status"] = "PASS" if not raw["gate_failures"] else "BLOCK"
        raw_trades = [trade for fold in folds for trade in fold["raw_trades"]]
        raw_trade_map[candidate_id] = raw_trades
        results[candidate_id] = {
            "unranked_signal_count": len(universes[candidate_id]),
            "selected_signal_count": len(selected[candidate_id]),
            "episode_count": len(labels),
            "mature_episode_count": int(
                labels.get("label_mature", pd.Series(dtype=bool)).sum()
            ),
            "outer_folds": [
                {key: value for key, value in fold.items() if key != "equity_curve"}
                for fold in folds
            ],
            "raw_aggregate": raw,
            "aggregate": aggregate,
        }
        print(json.dumps({
            "phase": "candidate_complete",
            "candidate_id": candidate_id,
            "raw": raw,
            "gate_status": aggregate["gate_status"],
            "block_reasons": aggregate["block_reasons"],
        }, default=str), flush=True)

    max_t = _max_t_adjustment(
        raw_trade_map,
        iterations=int(protocol["stress_tests"]["minimum_bootstrap_iterations"]),
    )
    for candidate_id, result in results.items():
        result["raw_aggregate"]["multiple_testing_adjusted_p"] = (
            max_t["adjusted_p_values"].get(candidate_id)
        )
    ranking = sorted(
        results,
        key=lambda name: (
            results[name]["aggregate"]["gate_status"] != "PASS",
            results[name]["raw_aggregate"]["gate_status"] != "PASS",
            -int(results[name]["raw_aggregate"]["positive_outer_folds"]),
            -float(results[name]["raw_aggregate"].get("profit_factor") or 0.0),
        ),
    )
    artifact = {
        "schema_version": "probiga.trading-v4-monthly-factor-oos.v1",
        "campaign_id": protocol["campaign_id"],
        "research_contract_sha256": contract_hash,
        "execution_boundary": protocol["execution_boundary"],
        "feature_cache": cache_path,
        "candidate_order": [spec["id"] for spec in specs],
        "ranking": ranking,
        "multiple_testing": max_t,
        "results": results,
        "real_order_submission": False,
    }
    output = (ROOT / args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(artifact, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    print(json.dumps({
        "phase": "complete",
        "artifact": str(output),
        "ranking": ranking,
        "passes": [
            name for name in ranking
            if results[name]["aggregate"]["gate_status"] == "PASS"
        ],
    }), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
