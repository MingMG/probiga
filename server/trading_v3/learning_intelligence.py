from __future__ import annotations

import math
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from datetime import date, datetime
from statistics import mean
from typing import Any

from .horizon_contracts import (
    HorizonForecastContract,
    PredictionKind,
)


COUNTERFACTUAL_SCHEMA = "probiga.trading-v3.counterfactual-learning.v1"


class LearningIntelligenceError(ValueError):
    """Raised when learning data cannot be interpreted without leakage."""


def _date(value: Any, field: str) -> date:
    if isinstance(value, datetime):
        raise LearningIntelligenceError(f"{field} must be a date, not datetime")
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value or "").strip())
    except ValueError as exc:
        raise LearningIntelligenceError(f"{field} must be an ISO-8601 date") from exc


def _number(value: Any, field: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise LearningIntelligenceError(f"{field} must be numeric") from exc
    if not math.isfinite(result):
        raise LearningIntelligenceError(f"{field} must be finite")
    return result


def _quadrant(selected: bool, won: bool) -> str:
    if selected and won:
        return "SELECTED_WIN"
    if selected:
        return "SELECTED_LOSS"
    if won:
        return "REJECTED_WIN"
    return "REJECTED_CORRECT"


def build_counterfactual_samples(
    forecasts: Iterable[HorizonForecastContract],
    selections: Mapping[str, Mapping[str, Any]],
    outcomes: Mapping[str, Mapping[str, Any]],
    *,
    evaluation_date: date | str,
    winner_threshold_net_pct: float = 0.0,
) -> dict[str, Any]:
    """Create the four selected/rejected × win/loss learning samples.

    Samples are keyed by forecast id rather than stock code, so two horizons
    or model sleeves cannot borrow each other's outcome.  Immature rows remain
    pending; timing/cost violations are quarantined.  Neither class can tune a
    live model automatically.
    """

    evaluated_on = _date(evaluation_date, "evaluation_date")
    winner_threshold = _number(
        winner_threshold_net_pct, "winner_threshold_net_pct"
    )
    rows = tuple(forecasts)
    if len({item.forecast_id for item in rows}) != len(rows):
        raise LearningIntelligenceError("forecast_id must be unique")
    samples: list[dict[str, Any]] = []
    pending: list[dict[str, Any]] = []
    quarantined: list[dict[str, Any]] = []
    for forecast in rows:
        selection = selections.get(forecast.forecast_id)
        if selection is None:
            quarantined.append({
                "forecast_id": forecast.forecast_id,
                "reason_code": "SELECTION_RECORD_MISSING",
            })
            continue
        if not isinstance(selection, Mapping) or not isinstance(
            selection.get("selected"), bool
        ):
            quarantined.append({
                "forecast_id": forecast.forecast_id,
                "reason_code": "SELECTION_CONTRACT_VIOLATION",
                "reason": "selection.selected must be a JSON boolean",
            })
            continue
        if not forecast.sample_is_mature(evaluated_on):
            pending.append({
                "forecast_id": forecast.forecast_id,
                "matures_on": forecast.outcome_matures_on.isoformat(),
                "reason_code": "OUTCOME_NOT_MATURE",
            })
            continue
        outcome = outcomes.get(forecast.forecast_id)
        if outcome is None:
            pending.append({
                "forecast_id": forecast.forecast_id,
                "matures_on": forecast.outcome_matures_on.isoformat(),
                "reason_code": "MATURE_OUTCOME_MISSING",
            })
            continue
        try:
            entry_date = _date(
                outcome.get("entry_trade_date"),
                f"outcomes[{forecast.forecast_id}].entry_trade_date",
            )
            exit_date = _date(
                outcome.get("exit_trade_date"),
                f"outcomes[{forecast.forecast_id}].exit_trade_date",
            )
            if entry_date < forecast.entry_trade_date:
                raise LearningIntelligenceError(
                    "outcome entered before the frozen entry clock"
                )
            if exit_date <= entry_date:
                raise LearningIntelligenceError(
                    "outcome exit must follow the actual entry date"
                )
            if exit_date > evaluated_on:
                raise LearningIntelligenceError(
                    "outcome exit must not be later than evaluation_date"
                )
            if not forecast.outcome_can_exit(exit_date):
                raise LearningIntelligenceError(
                    "outcome exited before the T+1 execution boundary"
                )
            actual_maturity = forecast.outcome_matures_on
            if entry_date > forecast.entry_trade_date:
                raw_actual_maturity = outcome.get("outcome_matures_on")
                if raw_actual_maturity in (None, ""):
                    raise LearningIntelligenceError(
                        "delayed actual entry requires outcome_matures_on "
                        "from the trading calendar"
                    )
                actual_maturity = _date(
                    raw_actual_maturity,
                    f"outcomes[{forecast.forecast_id}].outcome_matures_on",
                )
                if actual_maturity <= entry_date:
                    raise LearningIntelligenceError(
                        "actual outcome maturity must follow actual entry"
                    )
            if exit_date < actual_maturity:
                raise LearningIntelligenceError(
                    "outcome exit precedes the applicable maturity date"
                )
            gross_return = _number(
                outcome.get("gross_return_pct"),
                f"outcomes[{forecast.forecast_id}].gross_return_pct",
            )
            realized_cost = _number(
                outcome.get("realized_cost_pct"),
                f"outcomes[{forecast.forecast_id}].realized_cost_pct",
            )
            if realized_cost < 0:
                raise LearningIntelligenceError(
                    "realized_cost_pct must not be negative"
                )
        except LearningIntelligenceError as exc:
            quarantined.append({
                "forecast_id": forecast.forecast_id,
                "reason_code": "OUTCOME_CONTRACT_VIOLATION",
                "reason": str(exc),
            })
            continue
        selected = selection["selected"]
        net_return = gross_return - realized_cost
        won = net_return > winner_threshold
        quadrant = _quadrant(selected, won)
        expected = forecast.expected_return_net_pct
        calibration_error = (
            abs(net_return - float(expected))
            if expected is not None
            else None
        )
        observed_positive = 1.0 if won else 0.0
        probability = forecast.probability_positive
        brier = (
            (float(probability) - observed_positive) ** 2
            if probability is not None
            else None
        )
        opportunity_cost = (
            max(0.0, net_return)
            if not selected
            else max(0.0, -net_return)
        )
        samples.append({
            "forecast_id": forecast.forecast_id,
            "run_uid": forecast.run_uid,
            "stock_code": forecast.stock_code,
            "model_key": forecast.model_key,
            "model_version": forecast.model_version,
            "horizon_days": forecast.horizon_days,
            "prediction_kind": forecast.prediction_kind.value,
            "selected": selected,
            "reason_code": str(selection.get("reason_code") or ""),
            "entry_trade_date": entry_date.isoformat(),
            "exit_trade_date": exit_date.isoformat(),
            "outcome_matures_on": actual_maturity.isoformat(),
            "gross_return_pct": gross_return,
            "realized_cost_pct": realized_cost,
            "realized_net_return_pct": net_return,
            "winner_threshold_net_pct": winner_threshold,
            "quadrant": quadrant,
            "missed_opportunity": quadrant == "REJECTED_WIN",
            "false_positive": quadrant == "SELECTED_LOSS",
            "opportunity_cost_pct": opportunity_cost,
            "expected_return_net_pct": expected,
            "probability_positive": probability,
            "absolute_forecast_error_pct": calibration_error,
            "brier_score": brier,
            "calibration_eligible": (
                forecast.prediction_kind is PredictionKind.CALIBRATED_OOS
            ),
            "evidence_kind": "COUNTERFACTUAL_SHADOW",
            "can_activate_model": False,
            "order_authority": False,
        })
    return {
        "schema_version": COUNTERFACTUAL_SCHEMA,
        "status": "READY" if samples else "COLLECTING",
        "evaluation_date": evaluated_on.isoformat(),
        "samples": samples,
        "pending": pending,
        "quarantined": quarantined,
        "sample_count": len(samples),
        "pending_count": len(pending),
        "quarantined_count": len(quarantined),
        "shadow_evidence_can_activate_model": False,
        "order_authority": False,
    }


def _metric_group(samples: list[Mapping[str, Any]]) -> dict[str, Any]:
    quadrants = Counter(str(item.get("quadrant") or "") for item in samples)
    selected_count = quadrants["SELECTED_WIN"] + quadrants["SELECTED_LOSS"]
    winner_count = quadrants["SELECTED_WIN"] + quadrants["REJECTED_WIN"]
    calibrated = [
        item
        for item in samples
        if bool(item.get("calibration_eligible"))
        and item.get("absolute_forecast_error_pct") is not None
        and item.get("brier_score") is not None
    ]
    returns = [float(item["realized_net_return_pct"]) for item in samples]
    selected_returns = [
        float(item["realized_net_return_pct"])
        for item in samples
        if bool(item.get("selected"))
    ]
    opportunity_costs = [float(item.get("opportunity_cost_pct") or 0.0) for item in samples]
    reason_regret: defaultdict[str, float] = defaultdict(float)
    for item in samples:
        if str(item.get("quadrant")) == "REJECTED_WIN":
            reason_regret[str(item.get("reason_code") or "UNKNOWN")] += float(
                item.get("opportunity_cost_pct") or 0.0
            )
    return {
        "sample_count": len(samples),
        "quadrant_counts": dict(sorted(quadrants.items())),
        "selection_precision": (
            quadrants["SELECTED_WIN"] / selected_count
            if selected_count
            else None
        ),
        "winner_recall": (
            quadrants["SELECTED_WIN"] / winner_count
            if winner_count
            else None
        ),
        "false_positive_rate": (
            quadrants["SELECTED_LOSS"] / selected_count
            if selected_count
            else None
        ),
        "missed_opportunity_rate": (
            quadrants["REJECTED_WIN"] / winner_count
            if winner_count
            else None
        ),
        "average_net_return_pct": mean(returns) if returns else None,
        "selected_average_net_return_pct": (
            mean(selected_returns) if selected_returns else None
        ),
        "total_opportunity_cost_pct": sum(opportunity_costs),
        "average_opportunity_cost_pct": (
            mean(opportunity_costs) if opportunity_costs else None
        ),
        "calibrated_sample_count": len(calibrated),
        "mean_absolute_forecast_error_pct": (
            mean(float(item["absolute_forecast_error_pct"]) for item in calibrated)
            if calibrated
            else None
        ),
        "mean_brier_score": (
            mean(float(item["brier_score"]) for item in calibrated)
            if calibrated
            else None
        ),
        "rejection_reason_regret_pct": {
            key: round(value, 8) for key, value in sorted(reason_regret.items())
        },
    }


def counterfactual_learning_metrics(
    samples: Iterable[Mapping[str, Any]],
    *,
    minimum_mature_samples: int,
    minimum_mature_samples_by_horizon: Mapping[str | int, Any] | None = None,
) -> dict[str, Any]:
    """Aggregate learning diagnostics without granting release authority."""

    rows = [dict(item) for item in samples]
    if int(minimum_mature_samples) <= 0:
        raise LearningIntelligenceError("minimum_mature_samples must be positive")
    by_horizon: defaultdict[int, list[Mapping[str, Any]]] = defaultdict(list)
    for index, item in enumerate(rows):
        try:
            horizon = int(item.get("horizon_days"))
        except (TypeError, ValueError) as exc:
            raise LearningIntelligenceError(
                f"samples[{index}].horizon_days must be an integer"
            ) from exc
        if horizon not in {1, 5, 20}:
            raise LearningIntelligenceError(
                f"samples[{index}].horizon_days must be 1, 5 or 20"
            )
        if str(item.get("quadrant") or "") not in {
            "SELECTED_WIN",
            "SELECTED_LOSS",
            "REJECTED_WIN",
            "REJECTED_CORRECT",
        }:
            raise LearningIntelligenceError(
                f"samples[{index}].quadrant is invalid"
            )
        by_horizon[horizon].append(item)
    overall = _metric_group(rows)
    horizon_readiness: dict[str, dict[str, Any]] = {}
    if minimum_mature_samples_by_horizon is not None:
        for horizon in (1, 5, 20):
            raw_required = minimum_mature_samples_by_horizon.get(
                str(horizon),
                minimum_mature_samples_by_horizon.get(horizon),
            )
            try:
                required = int(raw_required)
            except (TypeError, ValueError) as exc:
                raise LearningIntelligenceError(
                    f"minimum mature samples for T+{horizon} must be an integer"
                ) from exc
            if required <= 0:
                raise LearningIntelligenceError(
                    f"minimum mature samples for T+{horizon} must be positive"
                )
            observed = len(by_horizon[horizon])
            horizon_readiness[f"T+{horizon}"] = {
                "required": required,
                "observed": observed,
                "ready": observed >= required,
            }
        evidence_ready = all(
            item["ready"] for item in horizon_readiness.values()
        )
    else:
        evidence_ready = len(rows) >= int(minimum_mature_samples)
    status = "EVIDENCE_READY" if evidence_ready else "COLLECTING"
    return {
        "schema_version": COUNTERFACTUAL_SCHEMA,
        "status": status,
        "minimum_mature_samples": int(minimum_mature_samples),
        "overall": overall,
        "by_horizon": {
            f"T+{horizon}": _metric_group(by_horizon[horizon])
            for horizon in sorted(by_horizon)
        },
        "horizon_readiness": horizon_readiness,
        "diagnostic_evidence_only": True,
        "can_activate_model": False,
        "order_authority": False,
    }
