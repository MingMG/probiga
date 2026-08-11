#!/usr/bin/env python3
"""Run the preregistered bounded-horizon stock-selection campaign."""
from __future__ import annotations

import argparse
import gc
import hashlib
import json
import math
import sys
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.kline_data import get_kline_engine
from server.trading_v3.backtest import (
    _band_series,
    _build_features,
    _load_history,
    _scaled,
)
from server.trading_v3.config import load_v3_config
from server.trading_v3.research_v4 import (
    _common_liquid_mask,
    _metric,
    aggregate_candidate,
    attach_point_in_time_finance,
    candidate_universes,
    enrich_research_features,
    evaluate_fold,
    label_bounded_candidate,
    quality_reversal_universe,
    select_top_per_day,
)
from tools.env_config import load_project_env
from tools.research_trading_v4_campaign import (
    _close_by_day,
    _load_finance_rows,
    _max_t_adjustment,
)


def _source_hash() -> str:
    digest = hashlib.sha256()
    for relative in (
        "server/trading_v3/backtest.py",
        "server/trading_v3/research_v4.py",
    ):
        digest.update((ROOT / relative).read_bytes())
    return digest.hexdigest()[:16]


def _feature_cache_path() -> Path:
    return ROOT / "artifacts" / "trading_v4" / (
        "research_features_20200102_20260731_" + _source_hash() + ".pkl"
    )


def _load_or_build_features() -> tuple[pd.DataFrame, str]:
    cache = _feature_cache_path()
    if cache.exists():
        return pd.read_pickle(cache), str(cache)
    start = date(2020, 1, 2)
    end = date(2026, 7, 31)
    engine = get_kline_engine()
    try:
        history = _load_history(engine, start_date=start, end_date=end)
    finally:
        engine.dispose()
    frame = _build_features(history)
    del history
    gc.collect()
    frame = frame[
        (frame["trade_date"].dt.date >= start)
        & (frame["trade_date"].dt.date <= end)
    ].copy()
    frame = enrich_research_features(frame)
    cache.parent.mkdir(parents=True, exist_ok=True)
    frame.to_pickle(cache)
    return frame, str(cache)


def _contract_hash(protocol: dict[str, Any]) -> str:
    encoded = json.dumps(
        protocol,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _add_candidate(
    frame: pd.DataFrame,
    *,
    candidate_id: str,
    mask: pd.Series,
    score: pd.Series,
) -> pd.DataFrame:
    selected = frame.loc[_common_liquid_mask(frame) & mask].copy()
    selected["score"] = score.loc[selected.index].astype("float32")
    selected["candidate_id"] = candidate_id
    selected["exit_sleeve"] = "bounded"
    if "short_name" not in selected:
        selected["short_name"] = selected["stock_code"].astype(str)
    return selected


def _build_universes(frame: pd.DataFrame) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    existing = candidate_universes(frame)
    pullback = existing["rs_hpb_no_health_v1"]
    universes: dict[str, pd.DataFrame] = {}
    low_vol = pullback[
        (pullback["market_return_60d_pct"] >= 0)
        & (pullback["market_aligned_breadth_pct"] >= 45)
    ].copy()
    low_vol["candidate_id"] = "bounded_low_vol_pullback_10_v1"
    low_vol["exit_sleeve"] = "bounded"
    universes["bounded_low_vol_pullback_10_v1"] = low_vol

    breakout_mask = (
        (frame["close_above_ma20"] == 1)
        & (frame["ma20_above_ma60"] == 1)
        & frame["return_20d_pct"].between(4, 20)
        & frame["return_60d_pct"].between(8, 40)
        & (frame["breakout_20d_proximity"] >= 0.985)
        & frame["amount_ratio_5_20"].between(1.05, 1.8)
        & frame["change_pct"].between(0.5, 5)
        & frame["atr_14d_pct"].between(1, 4)
        & (frame["market_return_20d_pct"] >= 0)
    )
    breakout_score = (
        0.30 * _scaled(frame["breakout_20d_proximity"], 0.985, 1.0)
        + 0.25 * _band_series(frame["relative_strength_20d_pct"], 4, 18, 10)
        + 0.15 * _band_series(frame["amount_ratio_5_20"], 1.05, 1.5, 0.5)
        + 0.15 * _band_series(frame["change_pct"], 0.5, 3.5, 2)
        + 0.15 * (1.0 - _scaled(frame["atr_14d_pct"], 1, 4))
    ).clip(lower=0.0, upper=1.0)
    universes["bounded_breakout_7_v1"] = _add_candidate(
        frame,
        candidate_id="bounded_breakout_7_v1",
        mask=breakout_mask,
        score=breakout_score,
    )

    reacceleration_mask = (
        (frame["close_above_ma20"] == 1)
        & (frame["ma20_above_ma60"] == 1)
        & frame["return_5d_pct"].between(-2, 6)
        & frame["return_20d_pct"].between(2, 15)
        & frame["return_60d_pct"].between(8, 40)
        & frame["ma20_slope_5d_pct"].between(0.2, 2.5)
        & frame["distance_ma20_pct"].between(0, 4)
        & frame["amount_ratio_5_20"].between(0.8, 1.5)
        & frame["atr_14d_pct"].between(1, 4.5)
        & (frame["market_return_60d_pct"] >= 0)
    )
    reacceleration_score = (
        0.25 * _band_series(frame["relative_strength_20d_pct"], 3, 18, 12)
        + 0.20 * _band_series(frame["return_5d_pct"], -1, 5, 5)
        + 0.20 * _band_series(frame["distance_ma20_pct"], 0, 3, 3)
        + 0.15 * _band_series(frame["ma20_slope_5d_pct"], 0.2, 2, 1.5)
        + 0.10 * _band_series(frame["amount_ratio_5_20"], 0.8, 1.4, 0.6)
        + 0.10 * (1.0 - _scaled(frame["atr_14d_pct"], 1, 4.5))
    ).clip(lower=0.0, upper=1.0)
    universes["bounded_reacceleration_10_v1"] = _add_candidate(
        frame,
        candidate_id="bounded_reacceleration_10_v1",
        mask=reacceleration_mask,
        score=reacceleration_score,
    )

    nvcr = existing["nvcr_price_reversal_v1"].copy()
    nvcr["candidate_id"] = "bounded_nvcr_3_v1"
    nvcr["exit_sleeve"] = "bounded"
    universes["bounded_nvcr_3_v1"] = nvcr

    quality_price_mask = (
        (frame["close_above_ma20"] == 1)
        & (frame["ma20_above_ma60"] == 1)
        & frame["return_60d_pct"].between(5, 35)
        & frame["return_20d_pct"].between(0, 15)
        & frame["distance_ma20_pct"].between(-1, 5)
        & frame["atr_14d_pct"].between(1, 4)
        & (frame["market_return_60d_pct"] >= 0)
    )
    quality_price = _add_candidate(
        frame,
        candidate_id="bounded_quality_momentum_20_v1",
        mask=quality_price_mask,
        score=pd.Series(0.0, index=frame.index),
    )
    universes["_quality_price_base"] = quality_price
    finance_pool = pd.concat([nvcr, quality_price], ignore_index=True)
    finance_pool = finance_pool.drop_duplicates(
        ["trade_date", "stock_code"],
        keep="first",
    )
    return universes, finance_pool


def _attach_finance_universes(
    frame: pd.DataFrame,
    universes: dict[str, pd.DataFrame],
    finance_pool: pd.DataFrame,
) -> None:
    finance_rows = _load_finance_rows(date(2026, 7, 31))
    enriched = attach_point_in_time_finance(
        finance_pool,
        market_frame=frame,
        finance_rows=finance_rows,
    )
    del finance_rows
    gc.collect()
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
    nvcr = universes["bounded_nvcr_3_v1"].merge(
        finance_map,
        on=["trade_date", "stock_code"],
        how="left",
    )
    qfbr = quality_reversal_universe(nvcr)
    qfbr["candidate_id"] = "bounded_qfbr_5_v1"
    qfbr["exit_sleeve"] = "bounded"
    universes["bounded_qfbr_5_v1"] = qfbr

    quality = universes["_quality_price_base"].merge(
        finance_map,
        on=["trade_date", "stock_code"],
        how="left",
    )
    quality = quality[
        (quality["quality_percentile"] >= 0.70)
        & (quality["cashflow_percentile"] >= 0.60)
        & (quality["asset_liab_ratio_pit"] <= 65)
        & (quality["net_profit_yoy_gr_pit"] > 0)
    ].copy()
    quality["score"] = (
        0.30 * quality["quality_percentile"]
        + 0.15 * quality["cashflow_percentile"]
        + 0.15 * quality["valuation_percentile"]
        + 0.20 * _band_series(quality["relative_strength_20d_pct"], 3, 18, 12)
        + 0.20 * (1.0 - _scaled(quality["atr_14d_pct"], 1, 4))
    ).clip(lower=0.0, upper=1.0)
    quality["exit_sleeve"] = "bounded"
    universes["bounded_quality_momentum_20_v1"] = quality


def _raw_aggregate(folds: list[dict[str, Any]], minimum_fold_pf: float) -> dict[str, Any]:
    trades = [trade for fold in folds for trade in fold["raw_trades"]]
    metrics = _metric(float(trade["net_return_pct"]) for trade in trades)
    metrics.update({
        "trade_count": len(trades),
        "net_profit_cny": sum(
            float(fold["raw_portfolio"]["net_profit_cny"])
            for fold in folds
        ),
        "maximum_drawdown_pct": max(
            float(fold["raw_portfolio"]["maximum_drawdown_pct"])
            for fold in folds
        ),
        "positive_outer_folds": sum(
            float(fold["raw_portfolio"].get("net_profit_cny") or 0.0) > 0
            and float(fold["raw_portfolio"].get("profit_factor") or 0.0)
            >= minimum_fold_pf
            for fold in folds
        ),
    })
    return metrics


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--protocol",
        default="strategies/trading_v4_bounded_campaign.json",
    )
    parser.add_argument(
        "--output",
        default="artifacts/trading_v4/bounded_oos_campaign_20260801.json",
    )
    args = parser.parse_args()
    load_project_env()
    protocol = json.loads((ROOT / args.protocol).read_text(encoding="utf-8"))
    specs = protocol["candidate_control"]["candidates"]
    if len(specs) > int(protocol["candidate_control"]["maximum_new_candidate_count"]):
        raise RuntimeError("candidate count exceeds frozen maximum")
    print(json.dumps({
        "phase": "load_features",
        "contract_hash": _contract_hash(protocol),
        "candidate_count": len(specs),
    }), flush=True)
    frame, cache_path = _load_or_build_features()
    print(json.dumps({
        "phase": "features_ready",
        "rows": len(frame),
        "dates": int(frame["trade_date"].nunique()),
        "local_cache": cache_path,
    }), flush=True)
    universes, finance_pool = _build_universes(frame)
    _attach_finance_universes(frame, universes, finance_pool)
    del finance_pool
    gc.collect()
    top_per_day = int(protocol["execution_protocol"]["top_per_day"])
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
            take_profit_pct=float(spec["take_profit_pct"]),
            maximum_holding_sessions=int(spec["maximum_holding_sessions"]),
            maximum_entry_gap_pct=float(spec["maximum_entry_gap_pct"]),
            config=runtime,
        )
        folds = [
            evaluate_fold(
                labels,
                fold=fold,
                candidate_id=candidate_id,
                close_by_day=close_by_day,
                config=runtime,
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
        raw_trade_map[candidate_id] = raw_trades
        results[candidate_id] = {
            "unranked_signal_count": len(universes[candidate_id]),
            "selected_signal_count": len(selected[candidate_id]),
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
            -int(results[name]["raw_aggregate"]["positive_outer_folds"]),
            -float(results[name]["raw_aggregate"].get("profit_factor") or 0.0),
        ),
    )
    artifact = {
        "schema_version": "probiga.trading-v4-bounded-oos.v1",
        "campaign_id": protocol["campaign_id"],
        "research_contract_sha256": _contract_hash(protocol),
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
