"""Fail-closed validation for V4's non-actionable research kernel."""

from __future__ import annotations

from collections.abc import Mapping

from ..domain import (
    CandidateStatus,
    DecisionBundle,
    DecisionBundleStatus,
    ResearchStatus,
    deterministic_hash,
)
from ..kernel.research import (
    RESEARCH_FEATURE_BUILDER_VERSION,
    RESEARCH_FEATURE_SET_VERSION,
    RESEARCH_SCORING_POLICY_VERSION,
    ResearchDecisionKernel,
    ResearchObservation,
)


class ResearchBundleValidationError(ValueError):
    """Raised when a research bundle crosses its non-actionable boundary."""


def validate_research_observation_bundle(
    bundle: DecisionBundle,
) -> DecisionBundle:
    """Validate the stricter state matrix used by the V4 research release."""

    if type(bundle) is not DecisionBundle:
        raise TypeError("bundle must be exactly DecisionBundle")
    if bundle.kernel_version != "v4:kernel:research-observation:v1":
        raise ResearchBundleValidationError(
            "unexpected kernel_version for V4 research observation bundle"
        )
    expected_bundle = ResearchDecisionKernel().evaluate(bundle.decision_input)
    if bundle != expected_bundle:
        raise ResearchBundleValidationError(
            "bundle does not match canonical evaluation of its DecisionInput"
        )
    if bundle.forecasts or bundle.actions or bundle.execution_intents:
        raise ResearchBundleValidationError(
            "research observation bundle cannot contain forecasts, actions or intents"
        )
    if bundle.status not in {
        DecisionBundleStatus.DATA_BLOCKED,
        DecisionBundleStatus.RESEARCH_ONLY,
        DecisionBundleStatus.WATCH_ONLY,
    }:
        raise ResearchBundleValidationError(
            "research observation bundle has an actionable status"
        )

    diagnostics = bundle.diagnostics
    if diagnostics.get("kernel_mode") != "RESEARCH_OBSERVATION_ONLY":
        raise ResearchBundleValidationError("kernel_mode is not research-only")
    expected_versions = {
        "policy_version": RESEARCH_SCORING_POLICY_VERSION,
        "feature_set_version": RESEARCH_FEATURE_SET_VERSION,
        "feature_builder_version": RESEARCH_FEATURE_BUILDER_VERSION,
    }
    for field_name, expected in expected_versions.items():
        if diagnostics.get(field_name) != expected:
            raise ResearchBundleValidationError(
                f"{field_name} does not match the frozen V4 release"
            )
    for field_name in (
        "expected_return_estimated",
        "probability_estimated",
        "actionable_output_allowed",
        "paper_buy_outbox_open",
        "production_activation_allowed",
    ):
        if diagnostics.get(field_name) is not False:
            raise ResearchBundleValidationError(
                f"{field_name} must be exactly false"
            )

    observations = diagnostics.get("observations")
    if not isinstance(observations, tuple):
        raise ResearchBundleValidationError("observations must be an immutable tuple")
    observation_ids: set[str] = set()
    input_features = {
        (item.scope.scope_id, item.feature_hash): item
        for item in bundle.decision_input.feature_vectors
    }
    status_counts = {
        CandidateStatus.WATCH: 0,
        CandidateStatus.RESEARCH_ONLY: 0,
        CandidateStatus.DATA_BLOCKED: 0,
    }
    for observation in observations:
        if not isinstance(observation, Mapping):
            raise ResearchBundleValidationError(
                "every observation must be a mapping"
            )
        try:
            candidate_status = CandidateStatus(observation["candidate_status"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ResearchBundleValidationError(
                "observation has an invalid candidate_status"
            ) from exc
        if candidate_status not in status_counts:
            raise ResearchBundleValidationError(
                "observation status is outside the research-only matrix"
            )
        status_counts[candidate_status] += 1
        if observation.get("evidence_classification") != ResearchStatus.FORWARD_ONLY.value:
            raise ResearchBundleValidationError(
                "V4.1 release accepts FORWARD_ONLY evidence only"
            )
        observation_id = observation.get("observation_id")
        if not isinstance(observation_id, str) or not observation_id:
            raise ResearchBundleValidationError("observation_id is required")
        if observation_id in observation_ids:
            raise ResearchBundleValidationError("duplicate observation_id")
        observation_ids.add(observation_id)
        reasons = observation.get("reason_codes")
        required_disclosures = {
            "CALLER_SUPPLIED_FORWARD_DATA",
            "FORWARD_ONLY_EVIDENCE",
            "OOS_GATE_NOT_PASSED",
            "SOURCE_AUTHENTICITY_NOT_CERTIFIED",
        }
        if not isinstance(reasons, tuple) or not required_disclosures.issubset(reasons):
            raise ResearchBundleValidationError(
                "observation must retain all forward-research disclosures"
            )
        score = observation.get("heuristic_screening_score")
        if candidate_status == CandidateStatus.DATA_BLOCKED and score is not None:
            raise ResearchBundleValidationError(
                "DATA_BLOCKED observation cannot carry a score"
            )
        instrument = observation.get("instrument")
        feature_hash = observation.get("feature_hash")
        if (instrument, feature_hash) not in input_features:
            raise ResearchBundleValidationError(
                "observation feature_hash is not bound to its input instrument"
            )
        try:
            reconstructed = ResearchObservation(
                instrument=instrument,
                feature_hash=feature_hash,
                evidence_classification=observation.get("evidence_classification"),
                candidate_status=candidate_status,
                heuristic_screening_score=score,
                source_record_count=observation.get("source_record_count"),
                reason_codes=reasons,
            )
        except (TypeError, ValueError) as exc:
            raise ResearchBundleValidationError(
                "observation payload violates the frozen research contract"
            ) from exc
        if dict(observation) != dict(reconstructed.as_payload()):
            raise ResearchBundleValidationError(
                "observation_id or payload does not match observation content"
            )

    expected_counts = {
        "observation_count": len(observations),
        "watch_count": status_counts[CandidateStatus.WATCH],
        "research_count": status_counts[CandidateStatus.RESEARCH_ONLY],
        "blocked_count": status_counts[CandidateStatus.DATA_BLOCKED],
    }
    for field_name, expected in expected_counts.items():
        if diagnostics.get(field_name) != expected:
            raise ResearchBundleValidationError(
                f"{field_name} does not match observations"
            )
    if diagnostics.get("observation_set_hash") != deterministic_hash(observations):
        raise ResearchBundleValidationError(
            "observation_set_hash does not match observations"
        )

    if status_counts[CandidateStatus.WATCH]:
        expected_status = DecisionBundleStatus.WATCH_ONLY
    elif status_counts[CandidateStatus.RESEARCH_ONLY]:
        expected_status = DecisionBundleStatus.RESEARCH_ONLY
    else:
        expected_status = DecisionBundleStatus.DATA_BLOCKED
    if bundle.status != expected_status:
        raise ResearchBundleValidationError(
            "bundle status does not match observation statuses"
        )
    return bundle


__all__ = [
    "ResearchBundleValidationError",
    "validate_research_observation_bundle",
]
