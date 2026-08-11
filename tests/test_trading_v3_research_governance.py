from __future__ import annotations

import json
from dataclasses import FrozenInstanceError

import pytest

from server.trading_v3.research_governance import (
    CONFIRMATORY,
    EXPLORATORY,
    CandidatePreregistration,
    OuterFold,
    ResearchGovernanceError,
    assert_result_governance,
    detect_repeated_holdout_consumption,
    familywise_trial_counts,
    holdout_consumption_report,
    is_strictly_json_serializable,
    label_research_result,
)
from tools.research_trading_v4_ml_campaign import _governance_registrations


HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64


def _fold(
    name: str = "oos_2025",
    *,
    training_end: str = "2024-11-26",
    validation_start: str = "2025-01-01",
    validation_end: str = "2025-12-31",
) -> OuterFold:
    return OuterFold(
        name=name,
        training_start="2020-01-02",
        training_end=training_end,
        validation_start=validation_start,
        validation_end=validation_end,
    )


def _registration(
    candidate_id: str = "trend_v1",
    *,
    family: str = "trend",
    classification: str = CONFIRMATORY,
    fold: OuterFold | None = None,
    portfolio_hash: str = HASH_C,
) -> CandidatePreregistration:
    return CandidatePreregistration(
        candidate_id=candidate_id,
        family=family,
        feature_protocol_hash=HASH_A,
        calibration_protocol_hash=HASH_B,
        portfolio_protocol_hash=portfolio_hash,
        outer_folds=(fold or _fold(),),
        data_cutoff="2026-07-31",
        created_at="2026-08-02T09:30:00+08:00",
        research_classification=classification,
    )


def test_preregistration_contract_is_frozen_deterministic_and_json_serializable():
    registration = _registration()

    payload = registration.as_dict()
    assert payload["candidate_id"] == "trend_v1"
    assert payload["family"] == "trend"
    assert payload["feature_protocol_hash"] == HASH_A
    assert payload["calibration_protocol_hash"] == HASH_B
    assert payload["portfolio_protocol_hash"] == HASH_C
    assert payload["outer_folds"][0]["validation_end"] == "2025-12-31"
    assert payload["data_cutoff"] == "2026-07-31"
    assert payload["created_at"] == "2026-08-02T01:30:00Z"
    assert payload["research_classification"] == CONFIRMATORY
    assert json.loads(json.dumps(payload)) == payload
    assert len(registration.contract_hash) == 64

    restored = CandidatePreregistration.from_dict(payload)
    assert restored == registration
    assert restored.contract_hash == registration.contract_hash
    with pytest.raises(FrozenInstanceError):
        registration.family = "edited"


def test_contract_rejects_invalid_hash_duplicate_holdout_and_future_fold():
    with pytest.raises(ResearchGovernanceError, match="SHA-256"):
        _registration(portfolio_hash="not-a-hash")

    with pytest.raises(ResearchGovernanceError, match="same holdout"):
        CandidatePreregistration(
            candidate_id="duplicate_fold_v1",
            family="trend",
            feature_protocol_hash=HASH_A,
            calibration_protocol_hash=HASH_B,
            portfolio_protocol_hash=HASH_C,
            outer_folds=(_fold("first"), _fold("renamed_same_holdout")),
            data_cutoff="2026-07-31",
            created_at="2026-08-02T01:00:00Z",
            research_classification=EXPLORATORY,
        )

    with pytest.raises(ResearchGovernanceError, match="data_cutoff"):
        _registration(
            fold=_fold(
                "future",
                validation_start="2026-01-01",
                validation_end="2026-12-31",
            )
        )


def test_outer_fold_mapping_accepts_existing_validation_and_holdout_aliases():
    validation_fold = OuterFold.from_mapping(_fold().as_dict())
    holdout_fold = OuterFold.from_mapping(
        {
            "fold_id": "oos_2025",
            "training_start": "2020-01-02",
            "training_end": "2024-11-26",
            "holdout_start": "2025-01-01",
            "holdout_end": "2025-12-31",
        }
    )

    assert validation_fold == holdout_fold
    assert holdout_fold.holdout_key == "2025-01-01/2025-12-31"


def test_repeated_holdout_detection_and_familywise_counts_use_unique_contracts():
    first = _registration("trend_v1")
    second = _registration(
        "trend_v2",
        classification=EXPLORATORY,
        portfolio_hash="d" * 64,
    )
    third = _registration(
        "reversal_v1",
        family="reversal",
        fold=_fold(
            "oos_2024",
            training_end="2023-11-26",
            validation_start="2024-01-01",
            validation_end="2024-12-31",
        ),
        portfolio_hash="e" * 64,
    )
    registrations = [first, first, second, third]

    all_holdouts = holdout_consumption_report(registrations)
    repeated = detect_repeated_holdout_consumption(registrations)
    counts = familywise_trial_counts(registrations)

    assert len(all_holdouts) == 2
    assert len(repeated) == 1
    assert repeated[0].familywise_trial_count == 2
    assert set(repeated[0].candidate_ids) == {"trend_v1", "trend_v2"}
    assert counts == {
        "familywise_trial_count": 3,
        "by_family": {"reversal": 1, "trend": 2},
        "by_research_classification": {
            EXPLORATORY: 1,
            CONFIRMATORY: 2,
        },
        "by_holdout": {
            "2024-01-01/2024-12-31": 1,
            "2025-01-01/2025-12-31": 2,
        },
        "reused_holdout_count": 1,
    }
    assert is_strictly_json_serializable(counts)


def test_changed_contract_counts_as_another_trial_on_the_same_holdout():
    original = _registration("trend_v1")
    edited_after_inspection = _registration(
        "trend_v1",
        portfolio_hash="f" * 64,
    )

    repeated = detect_repeated_holdout_consumption(
        [original, edited_after_inspection]
    )

    assert len(repeated) == 1
    assert repeated[0].familywise_trial_count == 2
    assert repeated[0].candidate_ids == ("trend_v1", "trend_v1")


def test_exploratory_result_cannot_masquerade_as_confirmatory():
    exploratory = _registration(
        "idea_v1",
        classification=EXPLORATORY,
    )

    with pytest.raises(ResearchGovernanceError, match="cannot produce"):
        label_research_result(
            exploratory,
            {"profit_factor": 1.8},
            evaluated_at="2026-08-02T10:00:00+08:00",
            claimed_classification=CONFIRMATORY,
        )

    envelope = label_research_result(
        exploratory,
        {"profit_factor": 1.8},
        evaluated_at="2026-08-02T10:00:00+08:00",
    )
    assert envelope["evidence_classification"] == EXPLORATORY
    assert envelope["confirmatory_claim_allowed"] is False
    assert_result_governance(envelope, [exploratory])

    forged = dict(envelope)
    forged["evidence_classification"] = CONFIRMATORY
    forged["confirmatory_claim_allowed"] = True
    with pytest.raises(ResearchGovernanceError, match="cannot be relabelled"):
        assert_result_governance(forged, [exploratory])


def test_confirmatory_result_is_bound_to_contract_and_strict_json():
    confirmatory = _registration()
    envelope = label_research_result(
        confirmatory,
        {"passed": True, "fold_profit_factors": [1.4, 1.5]},
        evaluated_at="2026-08-02T02:00:00Z",
    )

    assert envelope["evidence_classification"] == CONFIRMATORY
    assert envelope["preregistration_contract_hash"] == confirmatory.contract_hash
    assert_result_governance(envelope, [confirmatory])
    assert is_strictly_json_serializable(envelope)

    tampered = dict(envelope, candidate_id="different_candidate")
    with pytest.raises(ResearchGovernanceError, match="candidate_id"):
        assert_result_governance(tampered, [confirmatory])

    missing_payload = dict(envelope)
    del missing_payload["result"]
    with pytest.raises(ResearchGovernanceError, match="payload is missing"):
        assert_result_governance(missing_payload, [confirmatory])

    with pytest.raises(ResearchGovernanceError, match="JSON serializable"):
        label_research_result(
            confirmatory,
            {"profit_factor": float("nan")},
            evaluated_at="2026-08-02T02:00:00Z",
        )
    assert not is_strictly_json_serializable({"value": float("inf")})


def test_ml_campaign_builds_candidate_specific_frozen_registrations():
    protocol = {
        "predictor_protocol": {"features": ["return_20d_pct"]},
        "nested_validation": {"calibration_bucket_count": 5},
        "profit_gate": {"minimum_oos_samples": 80},
        "outer_folds": [_fold().as_dict()],
        "candidate_control": {
            "candidates": [
                {
                    "id": "trend_v1",
                    "family": "regime_expert_trend",
                    "base_universe": "trend",
                    "score": "frozen_score",
                    "exit_policy": "dynamic_trend",
                    "target_clip_pct": [-12, 20],
                    "ridge_lambda": 30,
                },
                {
                    "id": "dual_v1",
                    "family": "regime_expert_dual",
                    "base_universe": "dual",
                    "score": "frozen_score",
                    "exit_policy": "component_specific",
                    "target_clip_pct": [-12, 20],
                    "ridge_lambda": 30,
                },
            ]
        },
        "research_governance": {
            "data_cutoff": "2026-07-31",
            "preregistered_at": "2026-08-02T16:00:00+08:00",
            "research_classification": EXPLORATORY,
        },
    }
    runtime = {
        "costs": {"commission_rate": 0.0003},
        "portfolio": {"maximum_positions": 10},
        "execution": {"entry": "next_open"},
    }

    registrations = _governance_registrations(protocol, runtime=runtime)

    assert [item.candidate_id for item in registrations] == [
        "trend_v1",
        "dual_v1",
    ]
    assert all(item.research_classification == EXPLORATORY for item in registrations)
    assert registrations[0].feature_protocol_hash != registrations[1].feature_protocol_hash
    assert familywise_trial_counts(registrations)["familywise_trial_count"] == 2
