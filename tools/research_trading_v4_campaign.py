#!/usr/bin/env python3
"""Run the preregistered Trading V4 historical OOS research campaign.

This command is research-only.  It neither registers a model nor submits an
order.  Historical success can create a paper candidate artifact only.
"""
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

import numpy as np
import pandas as pd
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.kline_data import get_kline_engine
from server.common.batch_db import read_frame
from server.trading_v3.backtest import _build_features, _load_history
from server.trading_v3.config import load_v3_config
from server.trading_v3.research_v4 import (
    aggregate_candidate,
    attach_point_in_time_finance,
    candidate_universes,
    combine_dual_regime,
    enrich_research_features,
    evaluate_fold,
    label_candidate_signals,
    quality_reversal_universe,
    select_top_per_day,
)
from tools.env_config import (
    create_tool_engine,
    load_project_env,
    resolve_tool_mysql_url,
)


def _contract_hash(protocol: dict[str, Any]) -> str:
    payload = {
        "candidate_control": protocol["candidate_control"],
        "data_protocol": protocol["data_protocol"],
        "outer_folds": protocol["outer_folds"],
        "purge_and_embargo": protocol["purge_and_embargo"],
        "profit_gate": protocol["profit_gate"],
        "stress_tests": protocol["stress_tests"],
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _load_finance_rows(end_date: date) -> pd.DataFrame:
    engine = create_tool_engine(resolve_tool_mysql_url(), pool_pre_ping=True)
    try:
        return read_frame(
            text(
                """
                SELECT id, LEFT(stock_code, 6) AS stock_code,
                       report_date, notice_date, net_asset_ps, oper_cf_ps,
                       net_profit_yoy_gr, roe_wtd, gross_margin, net_margin,
                       cash_flow_ratio, asset_liab_ratio
                FROM si_stock_finance
                WHERE notice_date <= :end_date
                  AND report_date <= :end_date
                  AND notice_date >= report_date
                ORDER BY notice_date, id
                """
            ),
            engine,
            params={"end_date": end_date},
        )
    finally:
        engine.dispose()


def _close_by_day(frame: pd.DataFrame) -> dict[pd.Timestamp, pd.Series]:
    return {
        pd.Timestamp(day): group.set_index("stock_code")["raw_close"]
        for day, group in frame.groupby(
            "trade_date",
            sort=True,
            observed=True,
        )
    }


def _t_stat(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    array = np.asarray(values, dtype=float)
    deviation = float(array.std(ddof=1))
    if not math.isfinite(deviation) or deviation <= 0:
        return math.inf if float(array.mean()) > 0 else None
    return float(array.mean()) / (deviation / math.sqrt(len(array)))


def _max_t_adjustment(
    candidate_trades: dict[str, list[dict[str, Any]]],
    *,
    iterations: int,
    seed: int = 20260801,
) -> dict[str, Any]:
    by_candidate_month: dict[str, dict[str, list[float]]] = {}
    months: set[str] = set()
    observed: dict[str, float | None] = {}
    for candidate, trades in candidate_trades.items():
        grouped: dict[str, list[float]] = {}
        values: list[float] = []
        for trade in trades:
            value = float(trade["net_return_pct"])
            month = str(trade["exit_date"])[:7]
            grouped.setdefault(month, []).append(value)
            months.add(month)
            values.append(value)
        by_candidate_month[candidate] = grouped
        observed[candidate] = _t_stat(values)
    ordered_months = sorted(months)
    if not ordered_months:
        return {
            "method": "calendar_month_block_bootstrap_max_t",
            "iterations": iterations,
            "adjusted_p_values": {name: None for name in candidate_trades},
        }
    centered: dict[str, dict[str, list[float]]] = {}
    for candidate, grouped in by_candidate_month.items():
        all_values = [value for values in grouped.values() for value in values]
        mean_value = float(np.mean(all_values)) if all_values else 0.0
        centered[candidate] = {
            month: [value - mean_value for value in values]
            for month, values in grouped.items()
        }
    rng = np.random.default_rng(seed)
    maxima: list[float] = []
    for _ in range(iterations):
        sampled = rng.choice(ordered_months, size=len(ordered_months), replace=True)
        maximum = -math.inf
        for candidate, grouped in centered.items():
            values = [
                value
                for month in sampled
                for value in grouped.get(str(month), [])
            ]
            statistic = _t_stat(values)
            if statistic is not None and math.isfinite(statistic):
                maximum = max(maximum, statistic)
        maxima.append(maximum if math.isfinite(maximum) else 0.0)
    adjusted = {}
    for candidate, statistic in observed.items():
        if statistic is None:
            adjusted[candidate] = None
        elif math.isinf(statistic):
            adjusted[candidate] = 0.0
        else:
            adjusted[candidate] = (
                1 + sum(value >= statistic for value in maxima)
            ) / (iterations + 1)
    return {
        "method": "calendar_month_joint_block_bootstrap_max_t",
        "iterations": iterations,
        "seed": seed,
        "observed_t": observed,
        "adjusted_p_values": adjusted,
    }


def _compact_fold(fold: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in fold.items()
        if key not in {"equity_curve"}
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--protocol",
        default="strategies/trading_v4_research_campaign.json",
    )
    parser.add_argument(
        "--output",
        default="artifacts/trading_v4/oos_campaign_20260801.json",
    )
    parser.add_argument("--bootstrap-iterations", type=int)
    args = parser.parse_args()
    load_project_env()
    protocol_path = (ROOT / args.protocol).resolve()
    protocol = json.loads(protocol_path.read_text(encoding="utf-8"))
    contract_hash = _contract_hash(protocol)
    available_start = date.fromisoformat(protocol["data_protocol"]["available_start"])
    available_end = date.fromisoformat(protocol["data_protocol"]["available_end"])
    top_per_day = int(protocol["data_protocol"]["top_per_day"])
    candidate_ids = [item["id"] for item in protocol["candidate_control"]["candidates"]]
    if len(candidate_ids) > int(protocol["candidate_control"]["maximum_candidate_count"]):
        raise RuntimeError("candidate count exceeds frozen maximum")
    if len(candidate_ids) != len(set(candidate_ids)):
        raise RuntimeError("duplicate candidate id")
    print(json.dumps({
        "phase": "load_history",
        "contract_hash": contract_hash,
        "candidate_count": len(candidate_ids),
    }), flush=True)
    kline_engine = get_kline_engine()
    try:
        history = _load_history(
            kline_engine,
            start_date=available_start,
            end_date=available_end,
        )
    finally:
        kline_engine.dispose()
    features = _build_features(history)
    del history
    gc.collect()
    features = features[
        (features["trade_date"].dt.date >= available_start)
        & (features["trade_date"].dt.date <= available_end)
    ].copy()
    features = enrich_research_features(features)
    print(json.dumps({
        "phase": "features_ready",
        "rows": len(features),
        "dates": int(features["trade_date"].nunique()),
    }), flush=True)
    universes = candidate_universes(features)
    print(json.dumps({
        "phase": "load_point_in_time_finance",
        "reversal_price_rows": len(universes["nvcr_price_reversal_v1"]),
    }), flush=True)
    finance_rows = _load_finance_rows(available_end)
    finance_reversal = attach_point_in_time_finance(
        universes["nvcr_price_reversal_v1"],
        market_frame=features,
        finance_rows=finance_rows,
    )
    del finance_rows
    gc.collect()
    universes["qfbr_quality_reversal_v1"] = quality_reversal_universe(
        finance_reversal
    )
    universes["dual_regime_health_reversal_v1"] = combine_dual_regime(
        universes["rs_hpb_v1"],
        universes["qfbr_quality_reversal_v1"],
        top_per_day=top_per_day,
    )
    selected = {
        candidate_id: (
            universes[candidate_id]
            if candidate_id == "dual_regime_health_reversal_v1"
            else select_top_per_day(
                universes[candidate_id],
                top_per_day=top_per_day,
            )
        )
        for candidate_id in candidate_ids
    }
    close_by_day = _close_by_day(features)
    runtime = load_v3_config()
    results: dict[str, Any] = {}
    candidate_trades: dict[str, list[dict[str, Any]]] = {}
    for candidate_id in candidate_ids:
        print(json.dumps({
            "phase": "label_and_evaluate",
            "candidate_id": candidate_id,
            "selected_rows": len(selected[candidate_id]),
        }), flush=True)
        labels = label_candidate_signals(
            features,
            selected[candidate_id],
            config=runtime,
        )
        fold_results = [
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
            fold_results,
            config=runtime,
            required_positive_folds=int(
                protocol["profit_gate"]["minimum_positive_outer_folds"]
            ),
            minimum_fold_profit_factor=float(
                protocol["profit_gate"]["minimum_outer_fold_profit_factor"]
            ),
        )
        trades = [trade for fold in fold_results for trade in fold["trades"]]
        candidate_trades[candidate_id] = trades
        results[candidate_id] = {
            "unranked_signal_count": len(universes[candidate_id]),
            "selected_signal_count": len(selected[candidate_id]),
            "episode_count": len(labels),
            "mature_episode_count": int(labels.get("label_mature", pd.Series(dtype=bool)).sum()),
            "outer_folds": [_compact_fold(fold) for fold in fold_results],
            "aggregate": aggregate,
        }
        print(json.dumps({
            "phase": "candidate_complete",
            "candidate_id": candidate_id,
            "gate_status": aggregate["gate_status"],
            "positive_outer_folds": aggregate["positive_outer_folds"],
            "portfolio": aggregate["portfolio"],
        }, default=str), flush=True)
    iterations = int(
        args.bootstrap_iterations
        or protocol["stress_tests"]["minimum_bootstrap_iterations"]
    )
    multiple_testing = _max_t_adjustment(
        candidate_trades,
        iterations=iterations,
    )
    for candidate_id, result in results.items():
        adjusted_p = multiple_testing["adjusted_p_values"].get(candidate_id)
        result["aggregate"]["multiple_testing_adjusted_p"] = adjusted_p
        if result["aggregate"]["gate_status"] == "PASS" and (
            adjusted_p is None or adjusted_p >= 0.05
        ):
            result["aggregate"]["gate_status"] = "BLOCK"
            result["aggregate"]["block_reasons"].append(
                "MULTIPLE_TESTING_MAX_T_NOT_SIGNIFICANT"
            )
    ranked = sorted(
        results,
        key=lambda candidate: (
            results[candidate]["aggregate"]["gate_status"] != "PASS",
            -float(results[candidate]["aggregate"]["portfolio"].get("profit_factor") or 0.0),
            -float(results[candidate]["aggregate"]["portfolio"].get("net_expectancy_pct") or -999.0),
        ),
    )
    artifact = {
        "schema_version": "probiga.trading-v4-oos-campaign.v1",
        "campaign_id": protocol["campaign_id"],
        "research_contract_sha256": contract_hash,
        "execution_boundary": protocol["execution_boundary"],
        "data_range": {
            "start": available_start.isoformat(),
            "end": available_end.isoformat(),
        },
        "candidate_order": candidate_ids,
        "ranking": ranked,
        "multiple_testing": multiple_testing,
        "results": results,
        "historical_evidence_ceiling": protocol["evidence_status"]["historical_oos_can_grant"],
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
        "ranking": ranked,
        "passes": [
            candidate for candidate in ranked
            if results[candidate]["aggregate"]["gate_status"] == "PASS"
        ],
    }), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
