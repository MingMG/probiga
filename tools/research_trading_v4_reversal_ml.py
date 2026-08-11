#!/usr/bin/env python3
"""Run nested-OOF ridge ranking on the broad reversal parent."""
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
    aggregate_candidate,
    attach_point_in_time_finance,
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
from tools.research_trading_v4_ml_campaign import _evaluate_ml_fold
from tools.research_trading_v4_scored_reversal import (
    _base_price_pool,
    _finance_filter,
    _moderate_score,
)


def _hash(protocol: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(
        protocol,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")).hexdigest()


def _engineer(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    result["health_moderate_band"] = _band_series(
        result["market_health"], 0.40, 0.65, 0.20
    )
    result["health_panic_band"] = _band_series(
        result["market_health"], 0.15, 0.50, 0.15
    )
    result["market20_reversal_band"] = _band_series(
        result["market_return_20d_pct"], -8, 0, 6
    )
    result["aligned_breadth_band"] = _band_series(
        result["market_aligned_breadth_pct"], 35, 55, 20
    )
    result["breadth_recovery_band"] = _band_series(
        result["breadth_ma20_change_5d_pct"], 3, 10, 8
    )
    result["quality_mid_band"] = _band_series(
        result["quality_percentile"], 0.55, 0.90, 0.20
    )
    result["valuation_mid_band"] = _band_series(
        result["valuation_percentile"], 0.30, 0.70, 0.30
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--protocol",
        default="strategies/trading_v4_reversal_ml_campaign.json",
    )
    parser.add_argument(
        "--output",
        default="artifacts/trading_v4/reversal_ml_oos_20260801.json",
    )
    args = parser.parse_args()
    load_project_env()
    protocol = json.loads((ROOT / args.protocol).read_text(encoding="utf-8"))
    candidates = protocol["candidate_control"]["candidates"]
    if len(candidates) > int(protocol["candidate_control"]["maximum_new_candidate_count"]):
        raise RuntimeError("candidate count exceeds frozen maximum")
    parent_protocol = json.loads(
        (ROOT / "strategies/trading_v4_scored_reversal_campaign.json")
        .read_text(encoding="utf-8")
    )
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
    parent = _base_price_pool(
        frame,
        parent_protocol["broad_parent_universe"],
    )
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
    parent = _finance_filter(
        parent,
        parent_protocol["broad_parent_universe"],
    )
    parent = _engineer(parent)
    parent["score"] = _moderate_score(parent)
    parent = select_top_per_day(
        parent,
        top_per_day=int(protocol["nested_validation"]["base_preselection_top_per_day"]),
    )
    runtime = load_v3_config()
    exit_variants = protocol["exit_variants"]
    labels_by_exit: dict[str, pd.DataFrame] = {}
    for exit_name, exit_spec in exit_variants.items():
        rows = parent.assign(
            candidate_id=f"base_{exit_name.lower()}",
            exit_sleeve="bounded",
        )
        labels_by_exit[exit_name] = label_bounded_candidate(
            frame,
            rows,
            stop_pct=float(exit_spec["stop_pct"]),
            take_profit_pct=float(exit_spec["take_profit_pct"]),
            maximum_holding_sessions=int(exit_spec["maximum_holding_sessions"]),
            maximum_entry_gap_pct=float(exit_spec["maximum_entry_gap_pct"]),
            config=runtime,
        )
    print(json.dumps({
        "phase": "base_labels_ready",
        "eligible_rows": len(parent),
        "episodes": {
            name: len(labels) for name, labels in labels_by_exit.items()
        },
    }), flush=True)
    close_by_day = _close_by_day(frame)
    predictor = protocol["predictor_protocol"]
    nested = protocol["nested_validation"]
    features = tuple(predictor["features"])
    target_clip = tuple(float(value) for value in predictor["target_clip_pct"])
    results: dict[str, Any] = {}
    raw_trades_by_candidate: dict[str, list[dict[str, Any]]] = {}
    for candidate in candidates:
        candidate_id = candidate["id"]
        labels = labels_by_exit[candidate["exit_variant"]]
        print(json.dumps({
            "phase": "nested_oos",
            "candidate_id": candidate_id,
            "episode_count": len(labels),
        }), flush=True)
        folds = [
            _evaluate_ml_fold(
                labels,
                fold=fold,
                candidate_id=candidate_id,
                features=features,
                ridge_lambda=float(candidate["ridge_lambda"]),
                target_clip=target_clip,
                inner_months=int(nested["inner_oof_months"]),
                embargo_days=int(nested["inner_fit_signal_embargo_calendar_days"]),
                minimum_fit_samples=int(predictor["minimum_fit_samples"]),
                top_per_day=int(nested["inner_and_outer_top_per_day"]),
                close_by_day=close_by_day,
                config=runtime,
                bucket_count=int(nested["calibration_bucket_count"]),
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
            "base_episode_count": len(labels),
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
        "schema_version": "probiga.trading-v4-reversal-ml-oos.v1",
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
