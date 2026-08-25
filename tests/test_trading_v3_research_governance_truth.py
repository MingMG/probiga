from __future__ import annotations

from copy import deepcopy

import pytest

from server.trading_v3.research_governance import (
    CALLER_DECLARED_UNVERIFIED,
    CONFIRMATORY,
    EXPLORATORY,
    RETROSPECTIVE,
    CandidatePreregistration,
    ResearchGovernanceError,
    assert_result_governance,
    familywise_registration_manifest,
    label_research_result,
)


def _registration(
    candidate_id: str,
    *,
    created_at: str = "2026-08-24T08:00:00Z",
    classification: str = EXPLORATORY,
) -> CandidatePreregistration:
    return CandidatePreregistration(
        candidate_id=candidate_id,
        family="trend-family",
        feature_protocol_hash="1" * 64,
        calibration_protocol_hash="2" * 64,
        portfolio_protocol_hash=("3" if candidate_id == "a" else "4") * 64,
        outer_folds=({
            "name": "oos-2025",
            "training_start": "2020-01-01",
            "training_end": "2024-11-30",
            "validation_start": "2025-01-01",
            "validation_end": "2025-12-31",
        },),
        data_cutoff="2025-12-31",
        created_at=created_at,
        research_classification=classification,
    )


def test_post_hoc_registration_is_explicitly_retrospective_and_non_confirmatory():
    registration = _registration("a")

    envelope = label_research_result(
        registration,
        {"profit_factor": 2.0},
        evaluated_at="2026-08-24T09:00:00Z",
        family_registrations=(registration,),
    )

    assert registration.timing_status == RETROSPECTIVE
    assert envelope["preregistration_timing_status"] == RETROSPECTIVE
    assert envelope["registration_authority"] == CALLER_DECLARED_UNVERIFIED
    assert envelope["authoritative_preregistration_receipt_verified"] is False
    assert envelope["confirmatory_claim_allowed"] is False
    assert envelope["activation_eligible"] is False


def test_caller_declared_confirmatory_registration_cannot_self_authorize():
    registration = _registration("a", classification=CONFIRMATORY)

    with pytest.raises(
        ResearchGovernanceError,
        match="persisted authoritative preregistration receipt",
    ):
        label_research_result(
            registration,
            {"profit_factor": 2.0},
            evaluated_at="2026-08-24T09:00:00Z",
            family_registrations=(registration,),
        )


def test_result_assertion_rejects_family_shrink_after_results_are_known():
    first = _registration("a")
    second = _registration("b")
    family = (first, second)
    envelope = label_research_result(
        first,
        {"profit_factor": 2.0},
        evaluated_at="2026-08-24T09:00:00Z",
        family_registrations=family,
    )

    assert_result_governance(envelope, family)
    with pytest.raises(ResearchGovernanceError, match="manifest"):
        assert_result_governance(envelope, (first,))

    forged = deepcopy(envelope)
    forged["familywise_registration_manifest"] = (
        familywise_registration_manifest((first,))
    )
    with pytest.raises(ResearchGovernanceError, match="manifest"):
        assert_result_governance(forged, family)


def test_familywise_manifest_requires_prior_contract_hashes_not_a_count():
    registration = _registration("a")
    prior_hash = "f" * 64
    manifest = familywise_registration_manifest(
        (registration,),
        prior_registration_contract_hashes=(prior_hash,),
    )

    assert manifest["prior_trial_count"] == 1
    assert manifest["total_familywise_trial_count"] == 2
    assert manifest["caller_reported_count_accepted"] is False
    assert len(manifest["manifest_hash"]) == 64

    envelope = label_research_result(
        registration,
        {"profit_factor": 2.0},
        evaluated_at="2026-08-24T09:00:00Z",
        family_registrations=(registration,),
        prior_registration_contract_hashes=(prior_hash,),
    )
    assert_result_governance(
        envelope,
        (registration,),
        prior_registration_contract_hashes=(prior_hash,),
    )
    with pytest.raises(ResearchGovernanceError, match="manifest"):
        assert_result_governance(envelope, (registration,))
