#!/usr/bin/env python3
"""Run the frozen scored broad-reversal research campaign."""
from __future__ import annotations

import argparse
import glob
import hashlib
import json
import sys
from datetime import date
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
from tools.research_trading_v4_bounded_campaign import _raw_aggregate
from tools.research_trading_v4_campaign import (
    _close_by_day,
    _load_finance_rows,
    _max_t_adjustment,
)


def _hash(protocol: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(
        protocol,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")).hexdigest()


def _between(series: pd.Series, bounds: list[float]) -> pd.Series:
    return series.between(float(bounds[0]), float(bounds[1]))


def _base_price_pool(frame: pd.DataFrame, spec: dict[str, Any]) -> pd.DataFrame:
    mask = (
        _common_liquid_mask(frame)
        & _between(frame["return_20d_pct"], spec["return_20d_pct"])
        & _between(frame["drawdown_20d_pct"], spec["drawdown_20d_pct"])
        & _between(frame["distance_ma20_pct"], spec["distance_ma20_pct"])
        & _between(frame["atr_14d_pct"], spec["atr_14d_pct"])
        & (frame["market_return_20d_pct"] >= float(spec["market_return_20d_min_pct"]))
        & (
            frame["breadth_ma20_change_5d_pct"]
            >= float(spec["breadth_ma20_change_5d_min_pct"])
        )
        & (
            frame["limit_down_breadth_pct"]
            <= float(spec["limit_down_breadth_max_pct"])
        )
        & (
            frame["raw_low"]
            <= frame["prior_10d_low"] * float(spec["prior_10d_low_touch_multiplier"])
        )
        & (frame["close_location_value"] >= float(spec["close_location_value_min"]))
        & _between(frame["change_pct"], spec["change_pct"])
        & (
            frame["latest_relative_to_market_pct"]
            >= float(spec["latest_relative_to_market_min_pct"])
        )
        & (frame["rebound_from_low_pct"] >= float(spec["rebound_from_low_min_pct"]))
        & _between(frame["amount_ratio_1_20"], spec["amount_ratio_1_20"])
    )
    result = frame.loc[mask].copy()
    result["short_name"] = result["stock_code"].astype(str)
    result["exit_sleeve"] = "bounded"
    return result


def _finance_filter(frame: pd.DataFrame, spec: dict[str, Any]) -> pd.DataFrame:
    return frame[
        (frame["quality_percentile"] >= float(spec["quality_percentile_min"]))
        & (frame["cashflow_percentile"] >= float(spec["cashflow_percentile_min"]))
        & (frame["asset_liab_ratio_pit"] <= float(spec["asset_liab_ratio_max"]))
        & (frame["net_profit_yoy_gr_pit"] > float(spec["net_profit_yoy_gr_min"]))
    ].copy()


def _moderate_score(frame: pd.DataFrame) -> pd.Series:
    return (
        0.25 * _band_series(frame["market_health"], 0.40, 0.65, 0.20)
        + 0.15 * _band_series(frame["market_return_20d_pct"], -8, 0, 6)
        + 0.15 * _band_series(frame["market_aligned_breadth_pct"], 35, 55, 20)
        + 0.15 * _band_series(frame["quality_percentile"], 0.55, 0.90, 0.20)
        + 0.10 * frame["cashflow_percentile"]
        + 0.10 * frame["reversal_confirmation_score"]
        + 0.10 * _band_series(frame["breadth_ma20_change_5d_pct"], 3, 10, 8)
    ).clip(lower=0.0, upper=1.0)


def _panic_score(frame: pd.DataFrame) -> pd.Series:
    return (
        0.20 * _band_series(frame["market_health"], 0.15, 0.50, 0.15)
        + 0.20 * _band_series(frame["market_return_20d_pct"], -12, -2, 8)
        + 0.15 * _band_series(frame["market_aligned_breadth_pct"], 25, 45, 20)
        + 0.15 * _band_series(frame["breadth_ma20_change_5d_pct"], 3, 10, 8)
        + 0.10 * _band_series(frame["quality_percentile"], 0.55, 0.90, 0.20)
        + 0.10 * frame["cashflow_percentile"]
        + 0.10 * frame["reversal_confirmation_score"]
    ).clip(lower=0.0, upper=1.0)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--protocol",
        default="strategies/trading_v4_scored_reversal_campaign.json",
    )
    parser.add_argument(
        "--output",
        default="artifacts/trading_v4/scored_reversal_oos_20260801.json",
    )
    args = parser.parse_args()
    load_project_env()
    protocol = json.loads((ROOT / args.protocol).read_text(encoding="utf-8"))
    candidates = protocol["candidate_control"]["candidates"]
    if len(candidates) > int(protocol["candidate_control"]["maximum_new_candidate_count"]):
        raise RuntimeError("candidate count exceeds frozen maximum")
    caches = sorted(glob.glob(str(
        ROOT / "artifacts" / "trading_v4" / "research_features*.pkl"
    )))
    if not caches:
        raise RuntimeError("local feature cache missing")
    cache = caches[-1]
    print(json.dumps({
        "phase": "load_local_features",
        "cache": cache,
        "contract_hash": _hash(protocol),
    }), flush=True)
    frame = pd.read_pickle(cache)
    parent = _base_price_pool(frame, protocol["broad_parent_universe"])
    print(json.dumps({
        "phase": "point_in_time_finance",
        "price_parent_rows": len(parent),
    }), flush=True)
    finance_rows = _load_finance_rows(date(2026, 7, 31))
    parent = attach_point_in_time_finance(
        parent,
        market_frame=frame,
        finance_rows=finance_rows,
    )
    parent = _finance_filter(parent, protocol["broad_parent_universe"])
    scores = {
        "MODERATE": _moderate_score(parent),
        "PANIC_RECOVERY": _panic_score(parent),
    }
    top_per_day = int(protocol["execution_protocol"]["top_per_day"])
    selected: dict[str, pd.DataFrame] = {}
    for candidate in candidates:
        rows = parent.copy()
        rows["candidate_id"] = candidate["id"]
        rows["score"] = scores[candidate["score_variant"]]
        selected[candidate["id"]] = select_top_per_day(
            rows,
            top_per_day=top_per_day,
        )
    close_by_day = _close_by_day(frame)
    runtime = load_v3_config()
    bucket_count = int(protocol["execution_protocol"]["calibration_bucket_count"])
    results: dict[str, Any] = {}
    raw_trades_by_candidate: dict[str, list[dict[str, Any]]] = {}
    for candidate in candidates:
        candidate_id = candidate["id"]
        rows = selected[candidate_id]
        print(json.dumps({
            "phase": "label_and_evaluate",
            "candidate_id": candidate_id,
            "eligible_rows": len(parent),
            "selected_rows": len(rows),
        }), flush=True)
        labels = label_bounded_candidate(
            frame,
            rows,
            stop_pct=float(candidate["stop_pct"]),
            take_profit_pct=float(candidate["take_profit_pct"]),
            maximum_holding_sessions=int(candidate["maximum_holding_sessions"]),
            maximum_entry_gap_pct=float(candidate["maximum_entry_gap_pct"]),
            config=runtime,
        )
        folds = [
            evaluate_fold(
                labels,
                fold=fold,
                candidate_id=candidate_id,
                close_by_day=close_by_day,
                config=runtime,
                bucket_count=bucket_count,
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
        raw_trades = [trade for fold in folds for trade in fold["raw_trades"]]
        raw_trades_by_candidate[candidate_id] = raw_trades
        results[candidate_id] = {
            "eligible_signal_count": len(parent),
            "selected_signal_count": len(rows),
            "episode_count": len(labels),
            "mature_episode_count": int(labels.get("label_mature", pd.Series(dtype=bool)).sum()),
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
            "gate": aggregate,
        }, default=str), flush=True)
    max_t = _max_t_adjustment(
        raw_trades_by_candidate,
        iterations=int(protocol["stress_tests"]["minimum_bootstrap_iterations"]),
    )
    for candidate_id, result in results.items():
        adjusted_p = max_t["adjusted_p_values"].get(candidate_id)
        result["raw_aggregate"]["multiple_testing_adjusted_p"] = adjusted_p
        if result["aggregate"]["gate_status"] == "PASS" and (
            adjusted_p is None or adjusted_p >= 0.05
        ):
            result["aggregate"]["gate_status"] = "BLOCK"
            result["aggregate"]["block_reasons"].append(
                "MULTIPLE_TESTING_MAX_T_NOT_SIGNIFICANT"
            )
    ranking = sorted(
        results,
        key=lambda name: (
            results[name]["aggregate"]["gate_status"] != "PASS",
            -int(results[name]["raw_aggregate"]["positive_outer_folds"]),
            -float(results[name]["raw_aggregate"].get("profit_factor") or 0.0),
        ),
    )
    artifact = {
        "schema_version": "probiga.trading-v4-scored-reversal-oos.v1",
        "campaign_id": protocol["campaign_id"],
        "research_contract_sha256": _hash(protocol),
        "execution_boundary": protocol["execution_boundary"],
        "feature_cache": cache,
        "candidate_order": [candidate["id"] for candidate in candidates],
        "ranking": ranking,
        "multiple_testing": max_t,
        "results": results,
        "real_order_submission": False,
    }
    output = (ROOT / args.output).resolve()
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
