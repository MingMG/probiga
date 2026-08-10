#!/usr/bin/env python3
"""Run the frozen nested-OOF ridge research campaign (paper only)."""
from __future__ import annotations

import argparse
import gc
import hashlib
import json
import math
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.kline_data import get_kline_engine
from server.trading_v3.backtest import (
    _build_features,
    _load_history,
    _validated_bucket,
)
from server.trading_v3.calibration import fit_calibration
from server.trading_v3.config import load_v3_config
from server.trading_v3.research_v4 import (
    HurdleReturnModel,
    RegimeExpertModel,
    _metric,
    _portfolio_replay,
    aggregate_candidate,
    attach_point_in_time_finance,
    candidate_universes,
    enrich_research_features,
    fit_hurdle_return_model,
    fit_ridge_return_model,
    fit_regime_expert_model,
    label_candidate_signals,
    predict_hurdle_return,
    predict_ridge_return,
    predict_regime_expert_return,
    prediction_to_score,
    portfolio_capacity_training_rows,
    quality_reversal_universe,
    select_top_per_day,
)
from server.trading_v3.research_governance import (
    CandidatePreregistration,
    familywise_trial_counts,
    label_research_result,
)
from tools.env_config import load_project_env
from tools.research_trading_v4_campaign import (
    _close_by_day,
    _load_finance_rows,
    _max_t_adjustment,
)


PIT_FINANCE_FEATURES = (
    "quality_percentile",
    "cashflow_percentile",
    "valuation_percentile",
    "asset_liab_ratio_pit",
    "net_profit_yoy_gr_pit",
)

_CANDIDATE_BASES: dict[str, dict[str, Any]] = {
    "rs_hpb_no_health_v1": {
        "declared_sleeve": "trend",
        "source_universes": ("rs_hpb_no_health_v1",),
        "exit_sleeves": ("trend",),
        "market_route": "NONE_INDEPENDENT_SLEEVE",
    },
    "qfbr_quality_reversal_v1": {
        "declared_sleeve": "reversal",
        "source_universes": ("nvcr_price_reversal_v1",),
        "exit_sleeves": ("reversal",),
        "market_route": "NONE_INDEPENDENT_SLEEVE",
    },
    "routed_trend_qfbr_v1": {
        "declared_sleeve": "multi_sleeve",
        "source_universes": (
            "rs_hpb_no_health_v1",
            "nvcr_price_reversal_v1",
        ),
        "exit_sleeves": ("trend", "reversal"),
        "market_route": (
            "trend when market_return_60d_pct>=3 and "
            "market_aligned_breadth_pct>=50; reversal when "
            "market_return_60d_pct<3 or market_health<0.55"
        ),
    },
}


def _resolve_candidate_base(spec: Mapping[str, Any]) -> dict[str, Any]:
    """Resolve a candidate through an auditable declaration, never its id text."""

    base_id = str(spec.get("base_universe_id") or "").strip()
    declaration_source = "base_universe_id"
    if not base_id:
        # Compatibility for the already frozen v4/v5 contracts.  This is
        # intentionally based on their declared universe, not on candidate-id
        # naming conventions such as the previous implicit ``"dual" in id``.
        description = str(spec.get("base_universe") or "").strip().lower()
        declaration_source = "legacy_base_universe_declaration"
        if description.startswith("rs_hpb_no_health_v1"):
            base_id = "rs_hpb_no_health_v1"
        elif "trend" in description and "qfbr" in description:
            base_id = "routed_trend_qfbr_v1"
        elif description.startswith("qfbr"):
            base_id = "qfbr_quality_reversal_v1"
        else:
            raise ValueError(
                f"candidate {spec.get('id')} requires a supported base_universe_id"
            )
    definition = _CANDIDATE_BASES.get(base_id)
    if definition is None:
        raise ValueError(f"unsupported candidate base_universe_id: {base_id}")
    declared_sleeve = str(
        spec.get("sleeve") or definition["declared_sleeve"]
    ).strip()
    if declared_sleeve != definition["declared_sleeve"]:
        raise ValueError(
            f"candidate {spec.get('id')} declares sleeve {declared_sleeve}, "
            f"but {base_id} is {definition['declared_sleeve']}"
        )
    return {
        "base_universe_id": base_id,
        "declaration_source": declaration_source,
        **definition,
    }


def _finite_coverage(
    frame: pd.DataFrame,
    columns: tuple[str, ...],
) -> dict[str, float]:
    coverage: dict[str, float] = {}
    for column in columns:
        if column not in frame.columns or frame.empty:
            coverage[column] = 0.0
            continue
        values = pd.to_numeric(frame[column], errors="coerce")
        coverage[column] = float(values.notna().mean())
    return coverage


def _build_candidate_bases(
    *,
    candidates: list[Mapping[str, Any]],
    universes: Mapping[str, pd.DataFrame],
    market_frame: pd.DataFrame,
    finance_rows: pd.DataFrame,
    top_per_day: int,
) -> tuple[dict[str, pd.DataFrame], dict[str, dict[str, Any]]]:
    """Build explicit sleeve bases and attach PIT finance to every candidate."""

    resolved = {
        str(spec["id"]): _resolve_candidate_base(spec)
        for spec in candidates
    }
    if len(resolved) != len(candidates):
        raise ValueError("candidate ids must be unique")
    pool_cache: dict[str, tuple[pd.DataFrame, int]] = {}

    def build_pool(base_id: str) -> tuple[pd.DataFrame, int]:
        cached = pool_cache.get(base_id)
        if cached is not None:
            return cached
        if base_id == "rs_hpb_no_health_v1":
            source = universes["rs_hpb_no_health_v1"]
            selected = select_top_per_day(source, top_per_day=top_per_day)
            result = attach_point_in_time_finance(
                selected,
                market_frame=market_frame,
                finance_rows=finance_rows,
            )
            row_count_before_top_k = len(source)
        elif base_id == "qfbr_quality_reversal_v1":
            source = universes["nvcr_price_reversal_v1"]
            finance_reversal = attach_point_in_time_finance(
                source,
                market_frame=market_frame,
                finance_rows=finance_rows,
            )
            quality_reversal = quality_reversal_universe(finance_reversal)
            result = select_top_per_day(
                quality_reversal,
                top_per_day=top_per_day,
            )
            row_count_before_top_k = len(quality_reversal)
        elif base_id == "routed_trend_qfbr_v1":
            trend = universes["rs_hpb_no_health_v1"]
            trend_routed = trend[
                (trend["market_return_60d_pct"] >= 3)
                & (trend["market_aligned_breadth_pct"] >= 50)
            ]
            reversal = universes["nvcr_price_reversal_v1"]
            finance_reversal = attach_point_in_time_finance(
                reversal,
                market_frame=market_frame,
                finance_rows=finance_rows,
            )
            quality_reversal = quality_reversal_universe(finance_reversal)
            reversal_routed = quality_reversal[
                (quality_reversal["market_return_60d_pct"] < 3)
                | (quality_reversal["market_health"] < 0.55)
            ]
            routed = pd.concat(
                [trend_routed, reversal_routed],
                ignore_index=True,
            )
            row_count_before_top_k = len(routed)
            selected = select_top_per_day(routed, top_per_day=top_per_day)
            # The second attachment is deliberate: it gives the trend rows the
            # same PIT financial columns and recomputes all rows from the same
            # statement-availability clock.
            result = attach_point_in_time_finance(
                selected,
                market_frame=market_frame,
                finance_rows=finance_rows,
            )
        else:  # pragma: no cover - guarded by _resolve_candidate_base
            raise AssertionError(base_id)
        pool_cache[base_id] = (result, row_count_before_top_k)
        return pool_cache[base_id]

    bases: dict[str, pd.DataFrame] = {}
    reports: dict[str, dict[str, Any]] = {}
    for spec in candidates:
        candidate_id = str(spec["id"])
        route = resolved[candidate_id]
        base, row_count_before_top_k = build_pool(route["base_universe_id"])
        candidate_base = base.copy()
        actual_sleeves = sorted(
            candidate_base["exit_sleeve"].dropna().astype(str).unique().tolist()
        )
        expected_sleeves = set(route["exit_sleeves"])
        if not set(actual_sleeves).issubset(expected_sleeves):
            raise ValueError(
                f"candidate {candidate_id} has unexpected exit_sleeve values: "
                f"{actual_sleeves}"
            )
        candidate_base["research_candidate_id"] = candidate_id
        bases[candidate_id] = candidate_base
        reports[candidate_id] = {
            **route,
            "rows_before_top_k": row_count_before_top_k,
            "rows_after_top_k": len(candidate_base),
            "observed_exit_sleeves": actual_sleeves,
            "exit_sleeve_preserved": bool(
                "exit_sleeve" in candidate_base.columns
                and set(actual_sleeves).issubset(expected_sleeves)
            ),
            "point_in_time_finance": {
                "protocol": "NOTICE_DATE_LTE_SIGNAL_DATE_CROSS_SECTIONAL_RANK_V1",
                "features": list(PIT_FINANCE_FEATURES),
                "coverage_before_train_fold_gate": _finite_coverage(
                    candidate_base,
                    PIT_FINANCE_FEATURES,
                ),
                "train_fold_gate": "TRAINING_FOLD_ONLY_FAIL_CLOSED",
            },
            "historical_context": {
                "concept_membership_added": False,
                "news_features_added": False,
                "reason": "NO_VERIFIED_POINT_IN_TIME_HISTORY",
            },
        }
    return bases, reports


def _contract_hash(protocol: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        protocol,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _governance_registrations(
    protocol: Mapping[str, Any],
    *,
    runtime: Mapping[str, Any],
) -> tuple[CandidatePreregistration, ...]:
    governance = protocol.get("research_governance")
    if not isinstance(governance, Mapping):
        return ()
    feature_base = {
        "predictor_protocol": protocol["predictor_protocol"],
        "feature_builder": "server.trading_v3.research_v4.enrich_research_features",
    }
    calibration_base = {
        "nested_validation": protocol["nested_validation"],
        "calibration_algorithm": "PAVA_MONOTONIC_OOF_ONLY",
    }
    portfolio_base = {
        "profit_gate": protocol["profit_gate"],
        "runtime_costs": runtime.get("costs"),
        "runtime_portfolio": runtime.get("portfolio"),
        "runtime_execution": runtime.get("execution"),
    }
    registrations: list[CandidatePreregistration] = []
    for spec in protocol["candidate_control"]["candidates"]:
        candidate_id = str(spec["id"])
        family = str(spec.get("family") or "").strip()
        if not family:
            raise ValueError(
                f"governed candidate {candidate_id} requires an explicit family"
            )
        registrations.append(CandidatePreregistration(
            candidate_id=candidate_id,
            family=family,
            feature_protocol_hash=_contract_hash({
                **feature_base,
                "candidate_universe": spec.get("base_universe"),
                "candidate_features": spec.get("features"),
                "candidate_model_kind": spec.get("model_kind", "ridge"),
                "candidate_regime_experts": spec.get("regime_experts"),
            }),
            calibration_protocol_hash=_contract_hash({
                **calibration_base,
                "candidate_score": spec.get("score"),
            }),
            portfolio_protocol_hash=_contract_hash({
                **portfolio_base,
                "candidate_exit_policy": spec.get("exit_policy"),
                "candidate_target_clip_pct": spec.get("target_clip_pct"),
                "candidate_ridge_lambda": spec.get("ridge_lambda"),
            }),
            outer_folds=tuple(protocol["outer_folds"]),
            data_cutoff=governance["data_cutoff"],
            created_at=governance["preregistered_at"],
            research_classification=governance["research_classification"],
        ))
    return tuple(registrations)


def _month_starts(start: pd.Timestamp, end: pd.Timestamp) -> list[pd.Timestamp]:
    current = start.replace(day=1)
    result = []
    while current <= end:
        result.append(current)
        current = current + pd.offsets.MonthBegin(1)
    return result


def _score_rows(model, rows: pd.DataFrame, *, top_per_day: int) -> pd.DataFrame:
    if rows.empty:
        return rows.copy()
    scored = rows.copy()
    prediction = (
        predict_regime_expert_return(model, scored)
        if isinstance(model, RegimeExpertModel)
        else predict_hurdle_return(model, scored)
        if isinstance(model, HurdleReturnModel)
        else predict_ridge_return(model, scored)
    )
    scored["predicted_net_return_pct"] = prediction
    scored["score"] = prediction_to_score(prediction)
    return select_top_per_day(scored, top_per_day=top_per_day)


def _fit_research_model(
    frame: pd.DataFrame,
    *,
    model_kind: str,
    regime_experts: bool,
    features: tuple[str, ...],
    ridge_lambda: float,
    target_clip: tuple[float, float],
    minimum_regime_fit_samples: int,
    minimum_feature_coverage: float,
):
    if model_kind == "hurdle_ridge":
        if regime_experts:
            raise ValueError("hurdle_ridge uses shared regime probabilities, not hard experts")
        return fit_hurdle_return_model(
            frame,
            features=features,
            ridge_lambda=ridge_lambda,
            target_clip=target_clip,
            minimum_feature_coverage=minimum_feature_coverage,
        )
    if model_kind != "ridge":
        raise ValueError(f"unsupported model_kind: {model_kind}")
    if regime_experts:
        return fit_regime_expert_model(
            frame,
            features=features,
            ridge_lambda=ridge_lambda,
            target_clip=target_clip,
            minimum_regime_samples=minimum_regime_fit_samples,
            minimum_feature_coverage=minimum_feature_coverage,
        )
    return fit_ridge_return_model(
        frame,
        features=features,
        ridge_lambda=ridge_lambda,
        target_clip=target_clip,
        minimum_feature_coverage=minimum_feature_coverage,
    )


def _evaluate_ml_fold(
    labels: pd.DataFrame,
    *,
    fold: Mapping[str, str],
    candidate_id: str,
    features: tuple[str, ...],
    ridge_lambda: float,
    target_clip: tuple[float, float],
    inner_months: int,
    embargo_days: int,
    minimum_fit_samples: int,
    top_per_day: int,
    close_by_day: Mapping[pd.Timestamp, pd.Series],
    config: Mapping[str, Any],
    bucket_count: int | None = None,
    regime_experts: bool = False,
    minimum_regime_fit_samples: int = 160,
    minimum_feature_coverage: float = 0.0,
    portfolio_aligned_training: bool = False,
    model_kind: str = "ridge",
) -> dict[str, Any]:
    training_start = pd.Timestamp(fold["training_start"])
    training_end = pd.Timestamp(fold["training_end"])
    validation_start = pd.Timestamp(fold["validation_start"])
    validation_end = pd.Timestamp(fold["validation_end"])
    mature = labels[
        labels["label_mature"].fillna(False)
        & labels["net_return_pct"].notna()
    ]
    outer_train = mature[
        (mature["trade_date"] >= training_start)
        & (mature["trade_date"] <= training_end)
        & (mature["exit_date"] <= training_end)
    ].copy()
    oof_start = max(
        training_start,
        (training_end - pd.DateOffset(months=inner_months)).replace(day=1),
    )
    oof_parts: list[pd.DataFrame] = []
    inner_models: list[dict[str, Any]] = []
    for month_start in _month_starts(oof_start, training_end):
        next_month = month_start + pd.offsets.MonthBegin(1)
        cutoff = month_start - pd.Timedelta(days=embargo_days)
        fit = mature[
            (mature["trade_date"] >= training_start)
            & (mature["trade_date"] <= cutoff)
            & (mature["exit_date"] <= cutoff)
        ]
        model_fit = (
            portfolio_capacity_training_rows(fit, config=config)
            if portfolio_aligned_training
            else fit
        )
        if len(model_fit) < minimum_fit_samples:
            continue
        predict = outer_train[
            (outer_train["trade_date"] >= month_start)
            & (outer_train["trade_date"] < next_month)
        ]
        if predict.empty:
            continue
        model = _fit_research_model(
            model_fit,
            model_kind=model_kind,
            regime_experts=regime_experts,
            features=features,
            ridge_lambda=ridge_lambda,
            target_clip=target_clip,
            minimum_regime_fit_samples=minimum_regime_fit_samples,
            minimum_feature_coverage=minimum_feature_coverage,
        )
        scored = _score_rows(model, predict, top_per_day=top_per_day)
        if not scored.empty:
            oof_parts.append(scored)
            inner_models.append({
                "prediction_month": month_start.date().isoformat(),
                "fit_cutoff": cutoff.date().isoformat(),
                "fit_samples": len(fit),
                "portfolio_aligned_fit_samples": len(model_fit),
                "model": model.as_dict(),
            })
    oof = (
        pd.concat(oof_parts, ignore_index=True)
        if oof_parts
        else outer_train.iloc[0:0].copy()
    )
    calibration = fit_calibration(
        candidate_id,
        oof[["score", "net_return_pct", "mae_pct", "mfe_pct"]].to_dict("records"),
        model_version=f"{candidate_id}-{fold['name']}-nested-oof",
        bucket_count=int(
            bucket_count
            if bucket_count is not None
            else config.get("calibration", {}).get("bucket_count", 5)
        ),
    )
    direction_valid = calibration.has_valid_score_direction()
    final_model = None
    validation_scored = labels.iloc[0:0].copy()
    final_fit = (
        portfolio_capacity_training_rows(outer_train, config=config)
        if portfolio_aligned_training
        else outer_train
    )
    if len(final_fit) >= minimum_fit_samples:
        final_model = _fit_research_model(
            final_fit,
            model_kind=model_kind,
            regime_experts=regime_experts,
            features=features,
            ridge_lambda=ridge_lambda,
            target_clip=target_clip,
            minimum_regime_fit_samples=minimum_regime_fit_samples,
            minimum_feature_coverage=minimum_feature_coverage,
        )
        validation_pool = labels[
            (labels["trade_date"] >= validation_start)
            & (labels["trade_date"] <= validation_end)
        ]
        validation_scored = _score_rows(
            final_model,
            validation_pool,
            top_per_day=top_per_day,
        )
    raw_outcomes = validation_scored[
        validation_scored["label_mature"].fillna(False)
        & validation_scored["net_return_pct"].notna()
    ]
    raw_portfolio, raw_trades, _ = _portfolio_replay(
        validation_scored,
        start_date=validation_start.date(),
        end_date=validation_end.date(),
        close_by_day=close_by_day,
        config=config,
        calibration=calibration,
        enforce_production_edge=False,
    )
    if direction_valid and not validation_scored.empty:
        accepted = validation_scored[
            validation_scored["score"].map(
                lambda score: _validated_bucket(calibration, float(score))
            ).astype(bool)
        ].copy()
    else:
        accepted = validation_scored.iloc[0:0].copy()
    accepted_outcomes = accepted[
        accepted["label_mature"].fillna(False)
        & accepted["net_return_pct"].notna()
    ]
    portfolio, trades, curve = _portfolio_replay(
        accepted,
        start_date=validation_start.date(),
        end_date=validation_end.date(),
        close_by_day=close_by_day,
        config=config,
        calibration=calibration,
        enforce_production_edge=True,
    )
    return {
        "name": fold["name"],
        "outer_training_samples": len(outer_train),
        "portfolio_aligned_training_samples": len(final_fit),
        "training_objective_protocol": (
            "AFTER_COST_PRODUCTION_CONSTRAINT_PARITY_V2"
            if portfolio_aligned_training
            else "AFTER_COST_SIGNAL_RETURN_V1"
        ),
        "model_architecture": (
            "POINT_IN_TIME_HURDLE_RIDGE_V1"
            if model_kind == "hurdle_ridge"
            else "POINT_IN_TIME_REGIME_EXPERT_RIDGE_V1"
            if regime_experts
            else "GLOBAL_RIDGE_V1"
        ),
        "inner_oof_samples": len(oof),
        "inner_models": inner_models,
        "calibration": calibration.as_dict(),
        "calibration_direction_valid": direction_valid,
        "final_model": final_model.as_dict() if final_model else None,
        "raw_validation": _metric(raw_outcomes["net_return_pct"].tolist()),
        "raw_portfolio": raw_portfolio,
        "raw_trades": raw_trades,
        "accepted_validation": _metric(accepted_outcomes["net_return_pct"].tolist()),
        "portfolio": portfolio,
        "trades": trades,
        "equity_curve": curve,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--protocol",
        default="strategies/trading_v4_ml_campaign.json",
    )
    parser.add_argument(
        "--output",
        default="artifacts/trading_v4/ml_oos_campaign_20260801.json",
    )
    args = parser.parse_args()
    load_project_env()
    protocol_path = (ROOT / args.protocol).resolve()
    protocol = json.loads(protocol_path.read_text(encoding="utf-8"))
    contract_hash = _contract_hash(protocol)
    candidates = protocol["candidate_control"]["candidates"]
    maximum_new_candidates = min(
        3,
        int(protocol["candidate_control"]["maximum_new_candidate_count"]),
    )
    if len(candidates) > maximum_new_candidates:
        raise RuntimeError("candidate count exceeds frozen maximum")
    if protocol["execution_boundary"].get("real_order_submission") is not False:
        raise RuntimeError("research campaign must keep real order submission disabled")
    prior_trial_count = int(
        protocol["candidate_control"].get(
            "prior_recorded_candidate_searches",
            0,
        )
    )
    cumulative_trial_count = prior_trial_count + len(candidates)
    maximum_familywise_count = int(
        protocol["candidate_control"].get(
            "maximum_familywise_candidate_count",
            cumulative_trial_count,
        )
    )
    if cumulative_trial_count > maximum_familywise_count:
        raise RuntimeError("cumulative familywise candidate count exceeds frozen maximum")
    run_started_at = datetime.now(timezone.utc)
    runtime = load_v3_config()
    registrations = _governance_registrations(protocol, runtime=runtime)
    registration_by_candidate = {
        item.candidate_id: item for item in registrations
    }
    future_registrations = [
        item.candidate_id
        for item in registrations
        if datetime.fromisoformat(
            item.created_at.replace("Z", "+00:00")
        ) > run_started_at
    ]
    if future_registrations:
        raise RuntimeError(
            "governance preregistration time is after campaign start: "
            + ", ".join(future_registrations)
        )
    start = date(2020, 1, 2)
    end = date(2026, 7, 31)
    print(json.dumps({
        "phase": "load_history",
        "contract_hash": contract_hash,
        "candidate_count": len(candidates),
    }), flush=True)
    engine = get_kline_engine()
    try:
        history = _load_history(engine, start_date=start, end_date=end)
    finally:
        engine.dispose()
    features_frame = _build_features(history)
    del history
    gc.collect()
    features_frame = features_frame[
        (features_frame["trade_date"].dt.date >= start)
        & (features_frame["trade_date"].dt.date <= end)
    ].copy()
    features_frame = enrich_research_features(features_frame)
    print(json.dumps({
        "phase": "features_ready",
        "rows": len(features_frame),
        "dates": int(features_frame["trade_date"].nunique()),
    }), flush=True)
    universes = candidate_universes(features_frame)
    finance_rows = _load_finance_rows(end)
    base_top = int(protocol["nested_validation"]["base_preselection_top_per_day"])
    candidate_bases, candidate_build_report = _build_candidate_bases(
        candidates=candidates,
        universes=universes,
        market_frame=features_frame,
        finance_rows=finance_rows,
        top_per_day=base_top,
    )
    del finance_rows
    gc.collect()
    required_sleeves = set(
        protocol.get("research_scope", {}).get(
            "required_independent_sleeves",
            [],
        )
    )
    observed_declared_sleeves = {
        report["declared_sleeve"]
        for report in candidate_build_report.values()
    }
    if not required_sleeves.issubset(observed_declared_sleeves):
        missing = sorted(required_sleeves.difference(observed_declared_sleeves))
        raise RuntimeError("campaign missing required independent sleeves: " + ", ".join(missing))
    print(json.dumps({
        "phase": "label_base_episodes",
        "candidate_mapping": candidate_build_report,
    }), flush=True)
    labels_by_candidate: dict[str, pd.DataFrame] = {}
    labels_by_base: dict[str, pd.DataFrame] = {}
    for spec in candidates:
        candidate_id = str(spec["id"])
        base_id = candidate_build_report[candidate_id]["base_universe_id"]
        base_labels = labels_by_base.get(base_id)
        if base_labels is None:
            base_labels = label_candidate_signals(
                features_frame,
                candidate_bases[candidate_id],
                config=runtime,
            )
            base_labels["exit_sleeve_reversal"] = (
                base_labels["exit_sleeve"].astype(str) == "reversal"
            ).astype(float)
            labels_by_base[base_id] = base_labels
        labels = base_labels.copy()
        labels["research_candidate_id"] = candidate_id
        labels_by_candidate[candidate_id] = labels
        candidate_build_report[candidate_id]["labeled_episode_count"] = len(labels)
        candidate_build_report[candidate_id]["labeled_exit_sleeves"] = sorted(
            labels["exit_sleeve"].dropna().astype(str).unique().tolist()
        )
    for labels in labels_by_candidate.values():
        labels["exit_sleeve_reversal"] = (
            labels["exit_sleeve"].astype(str) == "reversal"
        ).astype(float)
    close_by_day = _close_by_day(features_frame)
    feature_columns = tuple(protocol["predictor_protocol"]["features"])
    minimum_fit = int(protocol["predictor_protocol"]["minimum_fit_samples"])
    predictor = protocol["predictor_protocol"]
    nested = protocol["nested_validation"]
    results: dict[str, Any] = {}
    all_trades: dict[str, list[dict[str, Any]]] = {}
    for spec in candidates:
        candidate_id = spec["id"]
        labels = labels_by_candidate[candidate_id]
        candidate_features = tuple(spec.get("features") or feature_columns)
        print(json.dumps({
            "phase": "nested_oos",
            "candidate_id": candidate_id,
            "candidate_mapping": candidate_build_report[candidate_id],
            "episode_count": len(labels),
        }), flush=True)
        folds = [
            _evaluate_ml_fold(
                labels,
                fold=fold,
                candidate_id=candidate_id,
                features=candidate_features,
                ridge_lambda=float(spec["ridge_lambda"]),
                target_clip=tuple(
                    float(value) for value in spec["target_clip_pct"]
                ),
                inner_months=int(nested["inner_oof_months"]),
                embargo_days=int(nested["inner_fit_signal_embargo_calendar_days"]),
                minimum_fit_samples=minimum_fit,
                top_per_day=int(nested["inner_and_outer_top_per_day"]),
                close_by_day=close_by_day,
                config=runtime,
                bucket_count=int(
                    nested.get(
                        "calibration_bucket_count",
                        runtime.get("calibration", {}).get(
                            "bucket_count",
                            5,
                        ),
                    )
                ),
                regime_experts=bool(
                    spec["regime_experts"]
                    if "regime_experts" in spec
                    else predictor.get("regime_experts")
                ),
                minimum_regime_fit_samples=int(
                    predictor.get("minimum_regime_fit_samples", 160)
                ),
                minimum_feature_coverage=float(
                    predictor.get("minimum_feature_coverage", 0.0)
                ),
                portfolio_aligned_training=bool(
                    predictor.get("portfolio_aligned_training", False)
                ),
                model_kind=str(spec.get("model_kind") or "ridge"),
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
        trades = [trade for fold in folds for trade in fold["trades"]]
        all_trades[candidate_id] = trades
        results[candidate_id] = {
            "base_episode_count": len(labels),
            "candidate_mapping": candidate_build_report[candidate_id],
            "requested_predictor_features": list(candidate_features),
            "minimum_train_fold_feature_coverage": float(
                predictor.get("minimum_feature_coverage", 0.0)
            ),
            "outer_folds": [
                {key: value for key, value in fold.items() if key != "equity_curve"}
                for fold in folds
            ],
            "aggregate": aggregate,
        }
        print(json.dumps({
            "phase": "candidate_complete",
            "candidate_id": candidate_id,
            "gate_status": aggregate["gate_status"],
            "positive_outer_folds": aggregate["positive_outer_folds"],
            "portfolio": aggregate["portfolio"],
        }, default=str), flush=True)
    max_t = _max_t_adjustment(
        all_trades,
        iterations=int(protocol["stress_tests"]["minimum_bootstrap_iterations"]),
    )
    familywise_alpha = 0.05 / max(1, cumulative_trial_count)
    governance_envelopes: dict[str, Any] = {}
    evaluated_at = datetime.now(timezone.utc)
    for candidate_id, result in results.items():
        adjusted_p = max_t["adjusted_p_values"].get(candidate_id)
        result["aggregate"]["multiple_testing_adjusted_p"] = adjusted_p
        result["aggregate"]["multiple_testing_significance_alpha"] = (
            familywise_alpha
        )
        if result["aggregate"]["gate_status"] == "PASS" and (
            adjusted_p is None or adjusted_p >= familywise_alpha
        ):
            result["aggregate"]["gate_status"] = "BLOCK"
            result["aggregate"]["block_reasons"].append(
                "MULTIPLE_TESTING_MAX_T_NOT_SIGNIFICANT"
            )
        registration = registration_by_candidate.get(candidate_id)
        if registration is None:
            result["aggregate"]["research_evidence_classification"] = (
                "legacy_ungoverned"
            )
            result["aggregate"]["activation_eligible"] = False
            continue
        strict_result = json.loads(json.dumps(
            result,
            allow_nan=False,
            default=str,
        ))
        summary = {
            "result_sha256": _contract_hash(strict_result),
            "gate_status": result["aggregate"]["gate_status"],
            "block_reasons": result["aggregate"]["block_reasons"],
            "portfolio": result["aggregate"]["portfolio"],
            "multiple_testing_adjusted_p": adjusted_p,
            "multiple_testing_significance_alpha": familywise_alpha,
        }
        envelope = label_research_result(
            registration,
            summary,
            evaluated_at=evaluated_at,
        )
        governance_envelopes[candidate_id] = envelope
        result["aggregate"]["research_evidence_classification"] = envelope[
            "evidence_classification"
        ]
        result["aggregate"]["activation_eligible"] = bool(
            result["aggregate"]["gate_status"] == "PASS"
            and envelope["confirmatory_claim_allowed"]
        )
    ranking = sorted(
        results,
        key=lambda name: (
            results[name]["aggregate"]["gate_status"] != "PASS",
            -float(results[name]["aggregate"]["portfolio"].get("profit_factor") or 0.0),
            -float(results[name]["aggregate"]["portfolio"].get("net_expectancy_pct") or -999.0),
        ),
    )
    artifact = {
        "schema_version": protocol.get(
            "artifact_schema_version",
            "probiga.trading-v4-nested-ml-oos.v1",
        ),
        "campaign_id": protocol["campaign_id"],
        "research_contract_sha256": contract_hash,
        "execution_boundary": protocol["execution_boundary"],
        "research_scope": protocol.get("research_scope"),
        "evidence_status": protocol.get("evidence_status"),
        "candidate_order": [item["id"] for item in candidates],
        "candidate_build_report": candidate_build_report,
        "ranking": ranking,
        "multiple_testing": max_t,
        "research_governance": {
            "status": "GOVERNED" if registrations else "LEGACY_UNGOVERNED",
            "execution_started_at": run_started_at.isoformat(),
            "evaluated_at": evaluated_at.isoformat(),
            "registrations": [item.as_dict() for item in registrations],
            "current_campaign_trial_counts": (
                familywise_trial_counts(registrations)
                if registrations
                else None
            ),
            "prior_recorded_candidate_searches": prior_trial_count,
            "cumulative_familywise_candidate_count": cumulative_trial_count,
            "familywise_significance_alpha": familywise_alpha,
            "result_envelopes": governance_envelopes,
        },
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
