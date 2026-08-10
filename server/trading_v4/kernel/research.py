"""Deterministic, non-actionable V4 research observation kernel.

This kernel deliberately does *not* estimate return, loss, probability or a
tradable action.  It turns the already traceable V4 chase-risk feature vector
into a transparent screening observation that can be used for forward
research.  The production default remains :class:`BlockedDecisionKernel`.

The score is an uncalibrated ordering heuristic, not a forecast.  Keeping that
distinction in the type boundary is important: observations live only in the
bundle diagnostics, while ``forecasts``, ``actions`` and
``execution_intents`` stay empty.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any

from ..domain import (
    AvailabilityStatus,
    CandidateStatus,
    DecisionBundle,
    DecisionBundleStatus,
    DecisionInput,
    FeatureVector,
    QualityStatus,
    ResearchStatus,
    ScopeType,
    derive_decision_id,
    deterministic_hash,
    deterministic_id,
)


RESEARCH_FEATURE_SET_VERSION = "v4:daily-bar-chase-risk-v2"
RESEARCH_FEATURE_BUILDER_VERSION = "v4:daily-bar-chase-risk-builder-v2"
RESEARCH_CAPABILITY_NAME = "daily_bar_chase_risk"
RESEARCH_SCORING_POLICY_VERSION = "v4:transparent-screening-policy:v1"

_ZERO = Decimal("0")
_ONE_HUNDRED = Decimal("100")
_WATCH_THRESHOLD = Decimal("65")
_SCORE_QUANTUM = Decimal("0.0001")


@dataclass(frozen=True, slots=True)
class ResearchObservation:
    """One auditable research-only screen result.

    ``heuristic_screening_score`` is intentionally nullable.  A missing or
    non-PASS input produces a DATA_BLOCKED observation instead of a made-up
    replacement value.
    """

    instrument: str
    feature_hash: str
    evidence_classification: str
    candidate_status: CandidateStatus
    heuristic_screening_score: Decimal | None
    source_record_count: int
    reason_codes: tuple[str, ...]
    observation_id: str = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.instrument, str) or not self.instrument.strip():
            raise ValueError("instrument must be non-empty text")
        if (
            not isinstance(self.feature_hash, str)
            or len(self.feature_hash) != 64
            or any(character not in "0123456789abcdef" for character in self.feature_hash)
        ):
            raise ValueError("feature_hash must be a lowercase SHA-256 digest")
        if self.evidence_classification not in {
            item.value for item in ResearchStatus
        }:
            raise ValueError("evidence_classification is not a ResearchStatus")
        status = CandidateStatus(self.candidate_status)
        if status not in {
            CandidateStatus.DATA_BLOCKED,
            CandidateStatus.RESEARCH_ONLY,
            CandidateStatus.WATCH,
        }:
            raise ValueError("research observation status is not permitted")
        if type(self.source_record_count) is not int or self.source_record_count < 1:
            raise ValueError("source_record_count must be a positive integer")
        score = self.heuristic_screening_score
        if score is not None:
            if not isinstance(score, Decimal) or not score.is_finite():
                raise TypeError("heuristic_screening_score must be a finite Decimal")
            if score < _ZERO or score > _ONE_HUNDRED:
                raise ValueError("heuristic_screening_score must be within 0..100")
        if status == CandidateStatus.DATA_BLOCKED and score is not None:
            raise ValueError("DATA_BLOCKED observation cannot carry a score")
        reasons = tuple(sorted(set(self.reason_codes)))
        if not reasons:
            raise ValueError("research observation requires reason_codes")
        object.__setattr__(self, "instrument", self.instrument.strip())
        object.__setattr__(self, "candidate_status", status)
        object.__setattr__(self, "reason_codes", reasons)
        identity = {
            "instrument": self.instrument,
            "feature_hash": self.feature_hash,
            "policy_version": RESEARCH_SCORING_POLICY_VERSION,
            "evidence_classification": self.evidence_classification,
            "candidate_status": self.candidate_status,
            "heuristic_screening_score": self.heuristic_screening_score,
            "source_record_count": self.source_record_count,
            "reason_codes": self.reason_codes,
        }
        object.__setattr__(
            self,
            "observation_id",
            deterministic_id("v4observation", identity),
        )

    def as_payload(self) -> Mapping[str, Any]:
        return {
            "observation_id": self.observation_id,
            "instrument": self.instrument,
            "feature_hash": self.feature_hash,
            "evidence_classification": self.evidence_classification,
            "candidate_status": self.candidate_status.value,
            "heuristic_screening_score": self.heuristic_screening_score,
            "source_record_count": self.source_record_count,
            "reason_codes": self.reason_codes,
        }


@dataclass(frozen=True, slots=True)
class ResearchDecisionKernel:
    """Produce only transparent V4 research observations.

    The class has no configurable thresholds.  Any semantic change must get a
    new class/version rather than silently changing historical reproduction.
    """

    kernel_version: str = field(
        default="v4:kernel:research-observation:v1",
        init=False,
    )
    policy_version: str = field(
        default=RESEARCH_SCORING_POLICY_VERSION,
        init=False,
    )

    def evaluate(self, decision_input: DecisionInput) -> DecisionBundle:
        if type(decision_input) is not DecisionInput:
            raise TypeError("decision_input must be exactly DecisionInput")

        observations: list[ResearchObservation] = []
        unsupported_feature_count = 0
        for feature in decision_input.feature_vectors:
            if not _is_supported_feature(feature):
                unsupported_feature_count += 1
                continue
            observations.append(_evaluate_feature(decision_input, feature))

        observations.sort(key=_observation_sort_key)
        watch_count = sum(
            item.candidate_status == CandidateStatus.WATCH
            for item in observations
        )
        research_count = sum(
            item.candidate_status == CandidateStatus.RESEARCH_ONLY
            for item in observations
        )
        blocked_count = sum(
            item.candidate_status == CandidateStatus.DATA_BLOCKED
            for item in observations
        )

        if watch_count:
            bundle_status = DecisionBundleStatus.WATCH_ONLY
        elif research_count:
            bundle_status = DecisionBundleStatus.RESEARCH_ONLY
        else:
            bundle_status = DecisionBundleStatus.DATA_BLOCKED

        global_reasons = {
            "ACTIONABLE_OUTPUT_DISABLED",
            "NO_RETURN_FORECAST",
            "NO_PROBABILITY_ESTIMATE",
            "OOS_GATE_NOT_PASSED",
            "RESEARCH_OBSERVATION_ONLY",
        }
        if not observations:
            global_reasons.add("NO_SUPPORTED_V4_FEATURES")
        if unsupported_feature_count:
            global_reasons.add("UNSUPPORTED_FEATURES_IGNORED")

        decision_id = derive_decision_id(decision_input, self.kernel_version)
        payloads = tuple(item.as_payload() for item in observations)
        return DecisionBundle(
            decision_id=decision_id,
            decision_input=decision_input,
            kernel_version=self.kernel_version,
            status=bundle_status,
            forecasts=(),
            actions=(),
            execution_intents=(),
            diagnostics={
                "schema_version": "probiga.trading-v4.research-observations.v1",
                "kernel_mode": "RESEARCH_OBSERVATION_ONLY",
                "policy_version": self.policy_version,
                "feature_set_version": RESEARCH_FEATURE_SET_VERSION,
                "feature_builder_version": RESEARCH_FEATURE_BUILDER_VERSION,
                "observations": payloads,
                "observation_set_hash": deterministic_hash(payloads),
                "observation_count": len(observations),
                "watch_count": watch_count,
                "research_count": research_count,
                "blocked_count": blocked_count,
                "unsupported_feature_count": unsupported_feature_count,
                "expected_return_estimated": False,
                "probability_estimated": False,
                "actionable_output_allowed": False,
                "paper_buy_outbox_open": False,
                "production_activation_allowed": False,
                "reason_codes": tuple(sorted(global_reasons)),
            },
        )


def _is_supported_feature(feature: FeatureVector) -> bool:
    return bool(
        feature.scope.scope_type == ScopeType.INSTRUMENT
        and feature.feature_set_version == RESEARCH_FEATURE_SET_VERSION
        and feature.feature_builder_version == RESEARCH_FEATURE_BUILDER_VERSION
        and feature.capability_name == RESEARCH_CAPABILITY_NAME
    )


def _evaluate_feature(
    decision_input: DecisionInput,
    feature: FeatureVector,
) -> ResearchObservation:
    capability = decision_input.context.capability_statuses[feature.capability_name]
    reasons = {
        "HEURISTIC_SCREEN_NOT_FORECAST",
        "NO_EXECUTION_AUTHORITY",
        "OOS_GATE_NOT_PASSED",
        *feature.reason_codes,
        *capability.reason_codes,
    }
    evidence = capability.research_status.value
    if capability.research_status == ResearchStatus.FORWARD_ONLY:
        reasons.add("FORWARD_ONLY_EVIDENCE")
    elif capability.research_status == ResearchStatus.DISPLAY_ONLY:
        reasons.add("DISPLAY_ONLY_EVIDENCE")

    blocked = bool(
        feature.quality_status != QualityStatus.PASS
        or capability.quality_status != QualityStatus.PASS
        or capability.availability_status == AvailabilityStatus.BLOCKED
        or capability.research_status == ResearchStatus.DISPLAY_ONLY
    )
    if feature.quality_status != QualityStatus.PASS:
        reasons.add("FEATURE_QUALITY_NOT_PASS")
    if capability.quality_status != QualityStatus.PASS:
        reasons.add("CAPABILITY_QUALITY_NOT_PASS")
    if capability.availability_status == AvailabilityStatus.BLOCKED:
        reasons.add("CAPABILITY_BLOCKED")

    parsed, parse_reasons = _parse_feature_values(feature.values)
    reasons.update(parse_reasons)
    if parsed is None:
        blocked = True

    if blocked or parsed is None:
        return ResearchObservation(
            instrument=feature.scope.scope_id,
            feature_hash=feature.feature_hash,
            evidence_classification=evidence,
            candidate_status=CandidateStatus.DATA_BLOCKED,
            heuristic_screening_score=None,
            source_record_count=len(feature.source_record_ids),
            reason_codes=tuple(reasons),
        )

    source_candidate_status = parsed["candidate_status"]
    has_capacity = parsed["has_verified_capacity"]
    ordinary_buy_eligible = parsed["ordinary_buy_eligible"]
    extreme_extension = parsed["extreme_extension"]
    cooldown_active = parsed["cooldown_active"]
    if (
        source_candidate_status
        in {CandidateStatus.DATA_BLOCKED, CandidateStatus.EXECUTION_BLOCKED}
        or not has_capacity
    ):
        reasons.add("SOURCE_RISK_GATE_BLOCKED")
        return ResearchObservation(
            instrument=feature.scope.scope_id,
            feature_hash=feature.feature_hash,
            evidence_classification=evidence,
            candidate_status=CandidateStatus.DATA_BLOCKED,
            heuristic_screening_score=None,
            source_record_count=len(feature.source_record_ids),
            reason_codes=tuple(reasons),
        )

    score = _screening_score(
        parsed["return_5d_pct"],
        parsed["return_20d_pct"],
        parsed["ma20_extension_pct"],
    )
    risk_gate_clear = bool(
        ordinary_buy_eligible
        and not extreme_extension
        and not cooldown_active
        and source_candidate_status == CandidateStatus.RESEARCH_ONLY
        and capability.availability_status == AvailabilityStatus.ACTIVE
    )
    if not risk_gate_clear:
        reasons.add("RISK_GATE_NOT_CLEAR")
    if capability.availability_status == AvailabilityStatus.DEGRADED:
        reasons.add("CAPABILITY_DEGRADED")

    status = (
        CandidateStatus.WATCH
        if risk_gate_clear and score >= _WATCH_THRESHOLD
        else CandidateStatus.RESEARCH_ONLY
    )
    reasons.add(
        "HEURISTIC_WATCH_THRESHOLD_MET"
        if status == CandidateStatus.WATCH
        else "HEURISTIC_WATCH_THRESHOLD_NOT_MET"
    )
    return ResearchObservation(
        instrument=feature.scope.scope_id,
        feature_hash=feature.feature_hash,
        evidence_classification=evidence,
        candidate_status=status,
        heuristic_screening_score=score,
        source_record_count=len(feature.source_record_ids),
        reason_codes=tuple(reasons),
    )


def _parse_feature_values(
    values: Mapping[str, Any],
) -> tuple[dict[str, Any] | None, tuple[str, ...]]:
    reasons: set[str] = set()
    parsed: dict[str, Any] = {}
    for name in ("return_5d_pct", "return_20d_pct", "ma20_extension_pct"):
        value = values.get(name)
        try:
            parsed[name] = _finite_decimal(value)
        except (TypeError, ValueError):
            reasons.add(f"INVALID_OR_MISSING_{name.upper()}")
    for name in (
        "has_verified_capacity",
        "ordinary_buy_eligible",
        "extreme_extension",
        "cooldown_active",
    ):
        value = values.get(name)
        if type(value) is not bool:
            reasons.add(f"INVALID_OR_MISSING_{name.upper()}")
        else:
            parsed[name] = value
    try:
        parsed["candidate_status"] = CandidateStatus(values.get("candidate_status"))
    except (TypeError, ValueError):
        reasons.add("INVALID_OR_MISSING_CANDIDATE_STATUS")
    if reasons:
        return None, tuple(sorted(reasons))
    return parsed, ()


def _finite_decimal(value: Any) -> Decimal:
    if isinstance(value, bool) or value is None or isinstance(value, float):
        raise TypeError("value must be an exact decimal-compatible value")
    try:
        converted = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError("value must be decimal-compatible") from exc
    if not converted.is_finite():
        raise ValueError("value must be finite")
    return converted


def _screening_score(
    return_5d_pct: Decimal,
    return_20d_pct: Decimal,
    ma20_extension_pct: Decimal,
) -> Decimal:
    """Transparent V1 ordering formula; it is not fitted to outcomes."""

    score = (
        Decimal("50")
        + _clamp(return_5d_pct, Decimal("-15"), Decimal("15"))
        * Decimal("1.5")
        + _clamp(return_20d_pct, Decimal("-30"), Decimal("30"))
        * Decimal("0.5")
        - max(ma20_extension_pct - Decimal("12"), _ZERO) * Decimal("2")
        - max(Decimal("-15") - ma20_extension_pct, _ZERO)
    )
    return _clamp(score, _ZERO, _ONE_HUNDRED).quantize(_SCORE_QUANTUM)


def _clamp(value: Decimal, lower: Decimal, upper: Decimal) -> Decimal:
    return min(max(value, lower), upper)


def _observation_sort_key(
    observation: ResearchObservation,
) -> tuple[int, Decimal, str]:
    status_rank = {
        CandidateStatus.WATCH: 0,
        CandidateStatus.RESEARCH_ONLY: 1,
        CandidateStatus.DATA_BLOCKED: 2,
    }[observation.candidate_status]
    score = observation.heuristic_screening_score
    return status_rank, -(score if score is not None else _ZERO), observation.instrument


__all__ = [
    "RESEARCH_CAPABILITY_NAME",
    "RESEARCH_FEATURE_BUILDER_VERSION",
    "RESEARCH_FEATURE_SET_VERSION",
    "RESEARCH_SCORING_POLICY_VERSION",
    "ResearchDecisionKernel",
    "ResearchObservation",
]
