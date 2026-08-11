#!/usr/bin/env python3
"""Run the frozen broad quality failed-breakdown reversal campaign."""
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

from server.trading_v3.backtest import _band_series, _scaled
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


def _contract_hash(protocol: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(
        protocol,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")).hexdigest()


def _range_mask(series: pd.Series, bounds: list[float]) -> pd.Series:
    return series.between(float(bounds[0]), float(bounds[1]))


def _price_pool(frame: pd.DataFrame, spec: dict[str, Any]) -> pd.DataFrame:
    mask = (
        _common_liquid_mask(frame)
        & _range_mask(frame["return_20d_pct"], spec["return_20d_pct"])
        & _range_mask(frame["drawdown_20d_pct"], spec["drawdown_20d_pct"])
        & _range_mask(frame["distance_ma20_pct"], spec["distance_ma20_pct"])
        & _range_mask(frame["atr_14d_pct"], spec["atr_14d_pct"])
        & _range_mask(frame["market_return_20d_pct"], spec["market_return_20d_pct"])
        & _range_mask(frame["market_health"], spec["market_health"])
        & _range_mask(
            frame["market_aligned_breadth_pct"],
            spec["market_aligned_breadth_pct"],
        )
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
            <= frame["prior_10d_low"]
            * float(spec["prior_10d_low_touch_multiplier"])
        )
        & (
            frame["close_location_value"]
            >= float(spec["close_location_value_min"])
        )
        & _range_mask(frame["change_pct"], spec["change_pct"])
        & (
            frame["latest_relative_to_market_pct"]
            >= float(spec["latest_relative_to_market_min_pct"])
        )
        & (
            frame["rebound_from_low_pct"]
            >= float(spec["rebound_from_low_min_pct"])
        )
        & _range_mask(frame["amount_ratio_1_20"], spec["amount_ratio_1_20"])
    )
    selected = frame.loc[mask].copy()
    selected["short_name"] = selected["stock_code"].astype(str)
    selected["exit_sleeve"] = "bounded"
    return selected


def _finance_mask(frame: pd.DataFrame, spec: dict[str, Any]) -> pd.Series:
    return (
        _range_mask(frame["quality_percentile"], spec["quality_percentile"])
        & (
            frame["cashflow_percentile"]
            >= float(spec["cashflow_percentile_min"])
        )
        & _range_mask(frame["valuation_percentile"], spec["valuation_percentile"])
        & (
            frame["asset_liab_ratio_pit"]
            <= float(spec["asset_liab_ratio_max"])
        )
        & (
            frame["net_profit_yoy_gr_pit"]
            > float(spec["net_profit_yoy_gr_min"])
        )
    )


def _score(frame: pd.DataFrame) -> pd.Series:
    return (
        0.20 * _band_series(frame["quality_percentile"], 0.60, 0.85, 0.15)
        + 0.15 * frame["cashflow_percentile"]
        + 0.15 * _band_series(frame["valuation_percentile"], 0.30, 0.70, 0.30)
        + 0.25 * frame["reversal_confirmation_score"]
        + 0.15 * frame["market_repair_score"]
        + 0.10 * _scaled(-frame["drawdown_20d_pct"], 8, 40)
    ).clip(lower=0.0, upper=1.0)


def _strict_spec(protocol: dict[str, Any]) -> dict[str, Any]:
    result = dict(protocol["broad_entry"])
    result.update(protocol["strict_confirmation_overrides"])
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--protocol",
        default="strategies/trading_v4_qfbr_campaign.json",
    )
    parser.add_argument(
        "--output",
        default="artifacts/trading_v4/qfbr_oos_campaign_20260801.json",
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
        raise RuntimeError("local research feature cache is missing")
    cache = caches[-1]
    print(json.dumps({
        "phase": "load_local_features",
        "cache": cache,
        "contract_hash": _contract_hash(protocol),
    }), flush=True)
    frame = pd.read_pickle(cache)
    broad_price = _price_pool(frame, protocol["broad_entry"])
    strict_price = _price_pool(frame, _strict_spec(protocol))
    finance_pool = pd.concat([broad_price, strict_price], ignore_index=True).drop_duplicates(
        ["trade_date", "stock_code"],
        keep="first",
    )
    print(json.dumps({
        "phase": "point_in_time_finance",
        "broad_price_rows": len(broad_price),
        "strict_price_rows": len(strict_price),
    }), flush=True)
    finance = _load_finance_rows(date(2026, 7, 31))
    enriched = attach_point_in_time_finance(
        finance_pool,
        market_frame=frame,
        finance_rows=finance,
    )
    finance_columns = [
        "trade_date",
        "stock_code",
        "quality_percentile",
        "cashflow_percentile",
        "valuation_percentile",
        "asset_liab_ratio_pit",
        "net_profit_yoy_gr_pit",
    ]
    finance_map = enriched[finance_columns].drop_duplicates(
        ["trade_date", "stock_code"],
        keep="last",
    )
    broad = broad_price.merge(
        finance_map,
        on=["trade_date", "stock_code"],
        how="left",
    )
    strict = strict_price.merge(
        finance_map,
        on=["trade_date", "stock_code"],
        how="left",
    )
    broad = broad.loc[_finance_mask(broad, protocol["broad_entry"])].copy()
    strict_spec = _strict_spec(protocol)
    strict = strict.loc[_finance_mask(strict, strict_spec)].copy()
    broad["score"] = _score(broad)
    strict["score"] = _score(strict)
    universes = {
        "BROAD": broad,
        "STRICT_CONFIRMATION": strict,
    }
    top_per_day = int(protocol["execution_protocol"]["top_per_day"])
    selected = {
        candidate["id"]: select_top_per_day(
            universes[candidate["entry_variant"]].assign(
                candidate_id=candidate["id"],
            ),
            top_per_day=top_per_day,
        )
        for candidate in candidates
    }
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
            "eligible_rows": len(universes[candidate["entry_variant"]]),
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
            "eligible_signal_count": len(universes[candidate["entry_variant"]]),
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
        "schema_version": "probiga.trading-v4-broad-qfbr-oos.v1",
        "campaign_id": protocol["campaign_id"],
        "research_contract_sha256": _contract_hash(protocol),
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
