from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from server.evaluation.baseline_epoch import (
    BASELINE_SCHEMA_VERSION,
    BaselineEpoch,
    BaselineEpochError,
    hash_artifact_paths,
)


HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64
HASH_D = "d" * 64
HASH_E = "e" * 64
HASH_F = "f" * 64


def _epoch() -> BaselineEpoch:
    return BaselineEpoch(
        baseline_epoch_id="v3-common-infra-20260803",
        source_system="V3_COMMON_INFRA_CONTROL",
        created_at="2026-08-03T09:30:00+08:00",
        code_hash=HASH_A,
        config_hash=HASH_B,
        model_hash=HASH_C,
        calibration_hash=HASH_D,
        feature_schema_hash=HASH_E,
        universe_version="pit-a-share-20260803",
        forecast_contract_id="actionable-entry-v1",
        exit_policy_id="v3-frozen-exit-v1",
        portfolio_policy_id="common-risk-budget-v1",
        execution_policy_id="neutral-paper-execution-v1",
        fee_schedule_version="cn-equity-fees-2026-v1",
        raw_data_manifest_hash=HASH_F,
        decision_clocks=("POST_CLOSE", "PRE_OPEN"),
        cumulative_search_count=77,
        evidence_status="BLOCK",
        metadata={"oos_report": "2026-08-01"},
    )


def test_baseline_epoch_is_frozen_deterministic_and_round_trips():
    epoch = _epoch()

    payload = epoch.as_dict()
    assert payload["schema_version"] == BASELINE_SCHEMA_VERSION
    assert payload["created_at"] == "2026-08-03T01:30:00Z"
    assert payload["cumulative_search_count"] == 77
    assert len(payload["contract_hash"]) == 64
    assert BaselineEpoch.from_mapping(payload) == epoch
    assert BaselineEpoch.from_mapping(payload).contract_hash == epoch.contract_hash
    with pytest.raises(FrozenInstanceError):
        epoch.evidence_status = "PASS"
    with pytest.raises(TypeError):
        epoch.metadata["oos_report"] = "changed"


def test_baseline_epoch_rejects_invalid_or_incomplete_evidence_contract():
    payload = _epoch().as_dict()
    payload["code_hash"] = "not-a-hash"
    with pytest.raises(BaselineEpochError, match="SHA-256"):
        BaselineEpoch.from_mapping(payload)

    payload = _epoch().as_dict()
    payload["decision_clocks"] = []
    with pytest.raises(BaselineEpochError, match="decision_clocks"):
        BaselineEpoch.from_mapping(payload)

    payload = _epoch().as_dict()
    payload["cumulative_search_count"] = -1
    with pytest.raises(BaselineEpochError, match="non-negative"):
        BaselineEpoch.from_mapping(payload)

    payload = _epoch().as_dict()
    payload["cumulative_search_count"] = 77.5
    with pytest.raises(BaselineEpochError, match="integer"):
        BaselineEpoch.from_mapping(payload)


def test_artifact_hash_is_path_and_content_stable(tmp_path):
    first = tmp_path / "a.py"
    nested = tmp_path / "pkg"
    nested.mkdir()
    second = nested / "b.json"
    ignored = nested / "__pycache__"
    ignored.mkdir()
    (ignored / "a.pyc").write_bytes(b"ignored")
    first.write_text("value = 1\n", encoding="utf-8")
    second.write_text('{"b":2}\n', encoding="utf-8")

    forward = hash_artifact_paths(tmp_path, ("a.py", "pkg"))
    reversed_order = hash_artifact_paths(tmp_path, ("pkg", "a.py"))
    assert forward == reversed_order

    second.write_text('{"b":3}\n', encoding="utf-8")
    assert hash_artifact_paths(tmp_path, ("a.py", "pkg")) != forward


def test_artifact_hash_rejects_missing_and_escaping_paths(tmp_path):
    with pytest.raises(BaselineEpochError, match="does not exist"):
        hash_artifact_paths(tmp_path, ("missing.py",))

    outside = tmp_path.parent / "outside-baseline.txt"
    outside.write_text("outside", encoding="utf-8")
    try:
        with pytest.raises(BaselineEpochError, match="escapes"):
            hash_artifact_paths(tmp_path, ("../outside-baseline.txt",))
    finally:
        outside.unlink()
