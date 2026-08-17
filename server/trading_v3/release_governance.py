from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from .horizon_contracts import PredictionKind, SUPPORTED_HORIZONS


RELEASE_GATE_SCHEMA = "probiga.trading-v3.continuous-calibration-gate.v1"
SHADOW_RELEASE_SCHEMA = "probiga.trading-v3.shadow-release-state.v1"


class ReleaseGovernanceError(ValueError):
    """Raised when release evidence or policy is structurally invalid."""


class ReleaseStage(str, Enum):
    DRAFT = "DRAFT"
    SHADOW = "SHADOW"
    CALIBRATION_REVIEW = "CALIBRATION_REVIEW"
    PAPER_ELIGIBLE = "PAPER_ELIGIBLE"
    BLOCKED = "BLOCKED"
    RETIRED = "RETIRED"


class ReleaseEvent(str, Enum):
    START_SHADOW = "START_SHADOW"
    REQUEST_CALIBRATION_REVIEW = "REQUEST_CALIBRATION_REVIEW"
    APPROVE_PAPER_ELIGIBILITY = "APPROVE_PAPER_ELIGIBILITY"
    CALIBRATION_FAILED = "CALIBRATION_FAILED"
    RESET_TO_SHADOW = "RESET_TO_SHADOW"
    RETIRE = "RETIRE"
    REQUEST_ORDER_AUTHORITY = "REQUEST_ORDER_AUTHORITY"


def _text(value: Any, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise ReleaseGovernanceError(f"{field} must not be empty")
    return result


def _number(value: Any, field: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ReleaseGovernanceError(f"{field} must be numeric") from exc
    if not math.isfinite(result):
        raise ReleaseGovernanceError(f"{field} must be finite")
    return result


def _aware(value: Any, field: str) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        raw = _text(value, field)
        try:
            result = datetime.fromisoformat(
                raw[:-1] + "+00:00" if raw.endswith("Z") else raw
            )
        except ValueError as exc:
            raise ReleaseGovernanceError(
                f"{field} must be an ISO-8601 datetime"
            ) from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise ReleaseGovernanceError(f"{field} must include a timezone")
    return result.astimezone(timezone.utc)


def _threshold_for_horizon(
    policy: Mapping[str, Any], field: str, horizon: int
) -> float:
    value = policy.get(field)
    if isinstance(value, Mapping):
        value = value.get(str(horizon), value.get(horizon))
    return _number(value, f"policy.{field}[T+{horizon}]")


@dataclass(frozen=True, slots=True)
class ContinuousCalibrationEvidence:
    release_id: str
    model_key: str
    model_version: str
    horizon_days: int
    prediction_kind: PredictionKind | str
    matured_sample_count: int
    oos_sample_count: int
    walk_forward_fold_count: int
    direction_rank_correlation: float
    calibration_mae: float
    brier_score: float
    population_stability_index: float
    net_expectancy_after_cost_pct: float
    profit_factor: float
    cost_coverage_ratio: float
    observed_at: datetime
    valid_until: datetime

    def __post_init__(self) -> None:
        for field in ("release_id", "model_key", "model_version"):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        horizon = int(self.horizon_days)
        if horizon not in SUPPORTED_HORIZONS:
            raise ReleaseGovernanceError("horizon_days must be 1, 5 or 20")
        object.__setattr__(self, "horizon_days", horizon)
        try:
            kind = PredictionKind(self.prediction_kind)
        except ValueError as exc:
            raise ReleaseGovernanceError(
                "prediction_kind must be PROXY_SCORE or CALIBRATED_OOS"
            ) from exc
        object.__setattr__(self, "prediction_kind", kind)
        for field in (
            "matured_sample_count",
            "oos_sample_count",
            "walk_forward_fold_count",
        ):
            value = int(getattr(self, field))
            if value < 0:
                raise ReleaseGovernanceError(f"{field} must not be negative")
            object.__setattr__(self, field, value)
        if self.oos_sample_count > self.matured_sample_count:
            raise ReleaseGovernanceError(
                "oos_sample_count must not exceed matured_sample_count"
            )
        for field in (
            "direction_rank_correlation",
            "calibration_mae",
            "brier_score",
            "population_stability_index",
            "net_expectancy_after_cost_pct",
            "profit_factor",
            "cost_coverage_ratio",
        ):
            object.__setattr__(self, field, _number(getattr(self, field), field))
        if self.calibration_mae < 0 or self.brier_score < 0:
            raise ReleaseGovernanceError(
                "calibration_mae and brier_score must not be negative"
            )
        if self.population_stability_index < 0:
            raise ReleaseGovernanceError(
                "population_stability_index must not be negative"
            )
        if self.profit_factor < 0 or self.cost_coverage_ratio < 0:
            raise ReleaseGovernanceError(
                "profit_factor and cost_coverage_ratio must not be negative"
            )
        observed = _aware(self.observed_at, "observed_at")
        valid_until = _aware(self.valid_until, "valid_until")
        if valid_until <= observed:
            raise ReleaseGovernanceError("valid_until must follow observed_at")
        object.__setattr__(self, "observed_at", observed)
        object.__setattr__(self, "valid_until", valid_until)


@dataclass(frozen=True, slots=True)
class CalibrationGateDecision:
    status: str
    failure_codes: tuple[str, ...]
    release_id: str
    horizon_days: int
    evidence_observed_at: str
    evidence_valid_until: str
    evaluated_at: str
    recommended_stage: str
    order_authority: bool = False
    evidence_provenance_status: str = "UNVERIFIED_PREVIEW"

    @property
    def passed(self) -> bool:
        return self.status == "PASS"

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": RELEASE_GATE_SCHEMA,
            **asdict(self),
            "passed": self.passed,
            "external_execution_grant_required": self.passed,
        }


def evaluate_continuous_calibration(
    evidence: ContinuousCalibrationEvidence,
    *,
    policy: Mapping[str, Any],
    evaluated_at: datetime | str,
) -> CalibrationGateDecision:
    """Apply sample/OOS/direction/error/drift/cost gates fail-closed."""

    now = _aware(evaluated_at, "evaluated_at")
    horizon = evidence.horizon_days
    minimum_mature = int(
        _threshold_for_horizon(policy, "minimum_mature_samples", horizon)
    )
    minimum_oos = int(
        _threshold_for_horizon(policy, "minimum_oos_samples", horizon)
    )
    minimum_folds = int(
        _threshold_for_horizon(policy, "minimum_walk_forward_folds", horizon)
    )
    minimum_direction = _threshold_for_horizon(
        policy, "minimum_direction_rank_correlation", horizon
    )
    maximum_mae = _threshold_for_horizon(
        policy, "maximum_calibration_mae", horizon
    )
    maximum_brier = _threshold_for_horizon(
        policy, "maximum_brier_score", horizon
    )
    maximum_psi = _threshold_for_horizon(
        policy, "maximum_population_stability_index", horizon
    )
    minimum_expectancy = _threshold_for_horizon(
        policy, "minimum_net_expectancy_after_cost_pct", horizon
    )
    minimum_profit_factor = _threshold_for_horizon(
        policy, "minimum_profit_factor", horizon
    )
    minimum_cost_coverage = _threshold_for_horizon(
        policy, "minimum_cost_coverage_ratio", horizon
    )
    maximum_age_days = _threshold_for_horizon(
        policy, "maximum_evidence_age_days", horizon
    )
    if min(minimum_mature, minimum_oos, minimum_folds) <= 0:
        raise ReleaseGovernanceError(
            "sample and fold thresholds must be positive"
        )
    if min(maximum_mae, maximum_brier, maximum_psi, maximum_age_days) < 0:
        raise ReleaseGovernanceError("maximum thresholds must not be negative")

    failures: list[str] = []
    if evidence.prediction_kind is not PredictionKind.CALIBRATED_OOS:
        failures.append("PREDICTION_IS_PROXY_NOT_CALIBRATED")
    if evidence.matured_sample_count < minimum_mature:
        failures.append("MATURE_SAMPLE_COUNT_TOO_LOW")
    if evidence.oos_sample_count < minimum_oos:
        failures.append("OOS_SAMPLE_COUNT_TOO_LOW")
    if evidence.walk_forward_fold_count < minimum_folds:
        failures.append("WALK_FORWARD_FOLD_COUNT_TOO_LOW")
    if evidence.direction_rank_correlation < minimum_direction:
        failures.append("SCORE_DIRECTION_TOO_WEAK")
    if evidence.calibration_mae > maximum_mae:
        failures.append("CALIBRATION_MAE_TOO_HIGH")
    if evidence.brier_score > maximum_brier:
        failures.append("BRIER_SCORE_TOO_HIGH")
    if evidence.population_stability_index > maximum_psi:
        failures.append("FEATURE_DRIFT_TOO_HIGH")
    if evidence.net_expectancy_after_cost_pct <= minimum_expectancy:
        failures.append("NET_EXPECTANCY_AFTER_COST_NOT_POSITIVE")
    if evidence.profit_factor < minimum_profit_factor:
        failures.append("PROFIT_FACTOR_TOO_LOW")
    if evidence.cost_coverage_ratio < minimum_cost_coverage:
        failures.append("COST_COVERAGE_TOO_LOW")
    if now < evidence.observed_at:
        failures.append("EVIDENCE_NOT_YET_AVAILABLE")
    age_days = (now - evidence.observed_at).total_seconds() / 86400.0
    if age_days > maximum_age_days:
        failures.append("EVIDENCE_TOO_OLD")
    if now > evidence.valid_until:
        failures.append("EVIDENCE_EXPIRED")
    unique_failures = tuple(dict.fromkeys(failures))
    return CalibrationGateDecision(
        status="BLOCK" if unique_failures else "PASS",
        failure_codes=unique_failures,
        release_id=evidence.release_id,
        horizon_days=horizon,
        evidence_observed_at=evidence.observed_at.isoformat(),
        evidence_valid_until=evidence.valid_until.isoformat(),
        evaluated_at=now.isoformat(),
        recommended_stage=(
            ReleaseStage.BLOCKED.value
            if unique_failures
            else ReleaseStage.PAPER_ELIGIBLE.value
        ),
        order_authority=False,
    )


@dataclass(frozen=True, slots=True)
class ReleaseTransition:
    previous_stage: str
    event: str
    next_stage: str
    accepted: bool
    reason_code: str
    order_authority: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SHADOW_RELEASE_SCHEMA,
            **asdict(self),
            "real_order_allowed": False,
            "external_execution_grant_required": (
                self.next_stage == ReleaseStage.PAPER_ELIGIBLE.value
            ),
        }


def transition_shadow_release(
    current_stage: ReleaseStage | str,
    event: ReleaseEvent | str,
    *,
    calibration_gate: CalibrationGateDecision | None = None,
) -> ReleaseTransition:
    """Advance advisory release state without ever creating order authority."""

    try:
        stage = ReleaseStage(current_stage)
    except ValueError as exc:
        raise ReleaseGovernanceError(f"unknown release stage: {current_stage}") from exc
    try:
        requested = ReleaseEvent(event)
    except ValueError as exc:
        raise ReleaseGovernanceError(f"unknown release event: {event}") from exc

    if requested is ReleaseEvent.REQUEST_ORDER_AUTHORITY:
        return ReleaseTransition(
            previous_stage=stage.value,
            event=requested.value,
            next_stage=stage.value,
            accepted=False,
            reason_code="ORDER_AUTHORITY_OUTSIDE_RELEASE_STATE_MACHINE",
        )
    if requested is ReleaseEvent.RETIRE and stage is not ReleaseStage.RETIRED:
        return ReleaseTransition(
            previous_stage=stage.value,
            event=requested.value,
            next_stage=ReleaseStage.RETIRED.value,
            accepted=True,
            reason_code="RELEASE_RETIRED",
        )
    if requested is ReleaseEvent.CALIBRATION_FAILED and stage not in {
        ReleaseStage.DRAFT,
        ReleaseStage.RETIRED,
    }:
        return ReleaseTransition(
            previous_stage=stage.value,
            event=requested.value,
            next_stage=ReleaseStage.BLOCKED.value,
            accepted=True,
            reason_code="CONTINUOUS_CALIBRATION_BLOCK",
        )
    allowed = {
        (ReleaseStage.DRAFT, ReleaseEvent.START_SHADOW): ReleaseStage.SHADOW,
        (
            ReleaseStage.SHADOW,
            ReleaseEvent.REQUEST_CALIBRATION_REVIEW,
        ): ReleaseStage.CALIBRATION_REVIEW,
        (
            ReleaseStage.BLOCKED,
            ReleaseEvent.RESET_TO_SHADOW,
        ): ReleaseStage.SHADOW,
    }
    target = allowed.get((stage, requested))
    if target is not None:
        return ReleaseTransition(
            previous_stage=stage.value,
            event=requested.value,
            next_stage=target.value,
            accepted=True,
            reason_code="TRANSITION_ACCEPTED",
        )
    if (
        stage is ReleaseStage.CALIBRATION_REVIEW
        and requested is ReleaseEvent.APPROVE_PAPER_ELIGIBILITY
    ):
        if calibration_gate is None or not calibration_gate.passed:
            return ReleaseTransition(
                previous_stage=stage.value,
                event=requested.value,
                next_stage=ReleaseStage.BLOCKED.value,
                accepted=False,
                reason_code="CALIBRATION_GATE_NOT_PASSED",
            )
        return ReleaseTransition(
            previous_stage=stage.value,
            event=requested.value,
            next_stage=ReleaseStage.PAPER_ELIGIBLE.value,
            accepted=True,
            reason_code="ADVISORY_PAPER_ELIGIBLE",
        )
    return ReleaseTransition(
        previous_stage=stage.value,
        event=requested.value,
        next_stage=stage.value,
        accepted=False,
        reason_code="ILLEGAL_RELEASE_TRANSITION",
    )


def enforce_continuous_gate(
    current_stage: ReleaseStage | str,
    calibration_gate: CalibrationGateDecision,
) -> ReleaseTransition:
    """Demote a released advisory immediately when fresh evidence fails."""

    stage = ReleaseStage(current_stage)
    if calibration_gate.passed:
        return ReleaseTransition(
            previous_stage=stage.value,
            event="CONTINUOUS_CALIBRATION_PASS",
            next_stage=stage.value,
            accepted=True,
            reason_code="NO_AUTOMATIC_PROMOTION",
        )
    if stage in {ReleaseStage.DRAFT, ReleaseStage.RETIRED}:
        return ReleaseTransition(
            previous_stage=stage.value,
            event="CONTINUOUS_CALIBRATION_BLOCK",
            next_stage=stage.value,
            accepted=False,
            reason_code="NON_ACTIVE_RELEASE_REMAINS_NON_ACTIVE",
        )
    return ReleaseTransition(
        previous_stage=stage.value,
        event="CONTINUOUS_CALIBRATION_BLOCK",
        next_stage=ReleaseStage.BLOCKED.value,
        accepted=True,
        reason_code="FAIL_CLOSED_DEMOTION",
    )
