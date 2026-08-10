from __future__ import annotations

import json
import hashlib
import math
from pathlib import Path

import pytest

from server.trading_v5.evidence import (
    EvidenceContractError,
    audit_campaign_bytes,
)


ROOT = Path(__file__).resolve().parents[1]
REGIME_CAMPAIGN = ROOT / "strategies/trading_v5_regime_expert_campaign.json"
REGIME_ARTIFACT = (
    ROOT / "artifacts/trading_v5/regime_expert_capacity_oos_20260802.json"
)
UNIFIED_CAMPAIGN = ROOT / "strategies/trading_v5_unified_router_campaign.json"
UNIFIED_ARTIFACT = (
    ROOT / "artifacts/trading_v5/unified_router_capacity_oos_20260802.json"
)


@pytest.fixture(scope="module")
def regime_audit():
    return audit_campaign_bytes(
        REGIME_CAMPAIGN.read_bytes(),
        REGIME_ARTIFACT.read_bytes(),
        expected_campaign_sha256=(
            "9bafc682995480b41b85ab0ef45e4b45e7415e4cf45743e1a703eef276eb8198"
        ),
        expected_artifact_sha256=(
            "1a4f8c5fa229352ad20324f8cf4cc9ad85891d5d6aed78d1eb76795a64ba6259"
        ),
    )


@pytest.fixture(scope="module")
def unified_audit():
    return audit_campaign_bytes(
        UNIFIED_CAMPAIGN.read_bytes(),
        UNIFIED_ARTIFACT.read_bytes(),
        expected_campaign_sha256=(
            "4cbaea2ea7fc4639605c8c6facd3eb3cf2a6a03ba5085406e5197622b0d9cafd"
        ),
        expected_artifact_sha256=(
            "2702acf26801020b0f6ffae1081d9641bd50b1b7f5461afc7731f25d6ea58477"
        ),
    )


def _audit_with_self_hash(campaign: bytes, artifact: bytes):
    return audit_campaign_bytes(
        campaign,
        artifact,
        expected_campaign_sha256=hashlib.sha256(campaign).hexdigest(),
        expected_artifact_sha256=hashlib.sha256(artifact).hexdigest(),
    )


def test_regime_legacy_evidence_is_recomputed_and_blocked(regime_audit) -> None:
    assert regime_audit.status == "BLOCK"
    assert regime_audit.governance_status == "LEGACY_UNGOVERNED"
    assert regime_audit.non_strict_json_constants == ("Infinity", "Infinity")
    assert regime_audit.stress_matrix_expected_scenarios == 108
    assert regime_audit.stress_matrix_recorded_scenarios == 0
    assert regime_audit.stress_matrix_complete is False
    by_id = {item.candidate_id: item for item in regime_audit.candidates}
    best = by_id["regime_expert_trend_l30_capacity_v1"]
    assert best.recorded_status == "BLOCK"
    assert best.trade_count == 50
    assert best.positive_outer_folds == 1
    assert math.isclose(best.net_expectancy_pct, 0.752407383983474)
    assert math.isclose(best.profit_factor, 1.2983656511614539)
    assert "STRESS_MATRIX_INCOMPLETE" in best.block_reasons
    assert "LEGACY_NON_STRICT_JSON" in best.block_reasons
    assert best.activation_eligible is False


def test_unified_legacy_evidence_is_recomputed_and_blocked(unified_audit) -> None:
    assert unified_audit.status == "BLOCK"
    assert unified_audit.governance_status == (
        "RECORDED_GOVERNED_EXPLORATORY_BYTE_FROZEN_NOT_RECOMPUTED"
    )
    assert unified_audit.non_strict_json_constants == ()
    assert unified_audit.stress_matrix_expected_scenarios == 36
    candidate = unified_audit.candidates[0]
    assert candidate.trade_count == 144
    assert candidate.positive_outer_folds == 0
    assert math.isclose(candidate.net_expectancy_pct, -0.9782011176806968)
    assert math.isclose(candidate.profit_factor, 0.642315013325732)
    assert math.isclose(candidate.trade_pnl_sum_cny, -12802.89697849489)
    assert math.isclose(
        candidate.recorded_portfolio_net_profit_cny,
        -3910.766425222799,
    )
    assert candidate.status == "BLOCK"


def test_audit_json_is_strict_finite_and_cannot_claim_orders(regime_audit) -> None:
    payload = regime_audit.to_json()
    parsed = json.loads(payload, parse_constant=lambda value: pytest.fail(value))
    assert parsed["historical_gate_status"] == "BLOCK"
    assert parsed["activation_status"] == "BLOCK"
    assert parsed["actionable_output_allowed"] is False
    assert parsed["paper_orders_allowed"] is False
    assert parsed["real_orders_allowed"] is False


def test_canonical_contract_ignores_json_layout() -> None:
    campaign = json.loads(UNIFIED_CAMPAIGN.read_text(encoding="utf-8"))
    rearranged = json.dumps(
        campaign,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    artifact = UNIFIED_ARTIFACT.read_bytes()
    audit = _audit_with_self_hash(rearranged, artifact)
    assert audit.status == "BLOCK"


def test_semantic_contract_tamper_is_rejected() -> None:
    campaign = json.loads(UNIFIED_CAMPAIGN.read_text(encoding="utf-8"))
    campaign["profit_gate"]["minimum_profit_factor"] = 0.1
    with pytest.raises(EvidenceContractError, match="research_contract_sha256"):
        modified = json.dumps(campaign, separators=(",", ":")).encode()
        audit_campaign_bytes(
            modified,
            UNIFIED_ARTIFACT.read_bytes(),
            expected_campaign_sha256=hashlib.sha256(modified).hexdigest(),
            expected_artifact_sha256=hashlib.sha256(
                UNIFIED_ARTIFACT.read_bytes()
            ).hexdigest(),
        )


def test_duplicate_json_key_is_rejected() -> None:
    original = UNIFIED_CAMPAIGN.read_text(encoding="utf-8")
    duplicated = original.replace(
        '"schema_version":',
        '"schema_version":"duplicate","schema_version":',
        1,
    ).encode()
    with pytest.raises(EvidenceContractError, match="duplicate JSON key"):
        _audit_with_self_hash(duplicated, UNIFIED_ARTIFACT.read_bytes())


def test_forged_pass_is_recomputed_as_block() -> None:
    artifact = json.loads(UNIFIED_ARTIFACT.read_text(encoding="utf-8"))
    candidate_id = artifact["candidate_order"][0]
    aggregate = artifact["results"][candidate_id]["aggregate"]
    aggregate["gate_status"] = "PASS"
    aggregate["block_reasons"] = []
    artifact_bytes = json.dumps(artifact, separators=(",", ":")).encode()
    audit = _audit_with_self_hash(UNIFIED_CAMPAIGN.read_bytes(), artifact_bytes)
    candidate = audit.candidates[0]
    assert candidate.status == "BLOCK"
    assert "RECORDED_GATE_STATUS_INCONSISTENT" in candidate.block_reasons
    assert "RECORDED_BLOCK_REASONS_INCOMPLETE" in candidate.block_reasons


def test_forged_summary_metric_is_rejected() -> None:
    artifact = json.loads(UNIFIED_ARTIFACT.read_text(encoding="utf-8"))
    candidate_id = artifact["candidate_order"][0]
    artifact["results"][candidate_id]["aggregate"]["validation"][
        "profit_factor"
    ] = 99.0
    with pytest.raises(EvidenceContractError, match="profit_factor is inconsistent"):
        _audit_with_self_hash(
            UNIFIED_CAMPAIGN.read_bytes(),
            json.dumps(artifact, separators=(",", ":")).encode(),
        )


def test_activation_field_smuggling_is_rejected() -> None:
    artifact = json.loads(UNIFIED_ARTIFACT.read_text(encoding="utf-8"))
    artifact["activation_eligible"] = True
    with pytest.raises(EvidenceContractError, match="attempts to claim activation"):
        _audit_with_self_hash(
            UNIFIED_CAMPAIGN.read_bytes(),
            json.dumps(artifact, separators=(",", ":")).encode(),
        )


@pytest.mark.parametrize("token", ["NaN", "Infinity", "-Infinity", "1e309"])
def test_new_non_finite_artifact_is_rejected(token: str) -> None:
    artifact = UNIFIED_ARTIFACT.read_text(encoding="utf-8")
    artifact = artifact.replace('"real_order_submission": false', f'"x": {token}, "real_order_submission": false', 1)
    with pytest.raises(EvidenceContractError):
        _audit_with_self_hash(UNIFIED_CAMPAIGN.read_bytes(), artifact.encode())


def test_expected_byte_hash_rejects_coordinated_rewrite() -> None:
    with pytest.raises(EvidenceContractError, match="artifact byte SHA-256 mismatch"):
        audit_campaign_bytes(
            UNIFIED_CAMPAIGN.read_bytes(),
            UNIFIED_ARTIFACT.read_bytes() + b"\n",
            expected_campaign_sha256=(
                "4cbaea2ea7fc4639605c8c6facd3eb3cf2a6a03ba5085406e5197622b0d9cafd"
            ),
            expected_artifact_sha256=(
                "2702acf26801020b0f6ffae1081d9641bd50b1b7f5461afc7731f25d6ea58477"
            ),
        )
