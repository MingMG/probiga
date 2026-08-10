from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from server.trading_v6.evidence import V6EvidenceError, audit_v6_evidence_bytes


ROOT = Path(__file__).resolve().parents[1]
CAMPAIGN = ROOT / "strategies/trading_v6_multi_sleeve_pit_finance_campaign.json"
ARTIFACT = ROOT / "artifacts/trading_v6/multi_sleeve_pit_finance_oos_20260802.json"
RUNNER = ROOT / "tools/research_trading_v4_ml_campaign.py"
LOG_PATHS = {
    "failed_stdout": ROOT / "artifacts/trading_v6/multi_sleeve_pit_finance_oos_20260802.stdout.log",
    "failed_stderr": ROOT / "artifacts/trading_v6/multi_sleeve_pit_finance_oos_20260802.stderr.log",
    "completed_stdout": ROOT / "artifacts/trading_v6/multi_sleeve_pit_finance_oos_20260802.py313.stdout.log",
    "completed_stderr": ROOT / "artifacts/trading_v6/multi_sleeve_pit_finance_oos_20260802.py313.stderr.log",
}
EXPECTED_LOG_HASHES = {
    "failed_stdout": "45c16b525603605ea1cc4f1c85c18263f94ed4040e3fdc18e54bfb6732af5a8d",
    "failed_stderr": "40a709e6745a006e7271aba17f4750edbffc568b59184f8b2efd2cc2c61a3740",
    "completed_stdout": "3cea8ce9d1ffd833e65771956dcf07a5f1b3d96d56f9430f0657564a2bb2b1a1",
    "completed_stderr": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
}


def _inputs():
    return {
        "campaign_bytes": CAMPAIGN.read_bytes(),
        "artifact_bytes": ARTIFACT.read_bytes(),
        "runner_bytes": RUNNER.read_bytes(),
        "log_bytes": {name: path.read_bytes() for name, path in LOG_PATHS.items()},
        "expected_campaign_sha256": "b89c5bc54f2e12eb61f5d3b12f06bc62b6c49820fe01417be59aa647ba891caa",
        "expected_artifact_sha256": "4097c64da94417520771e129b6a0028c3387e52707f3c1dc48c04d2b65fa0137",
        "expected_runner_sha256": "cfae4b9dd18f5e612a03bfa197385818cd24cb71645c9a4341afb9b3827c424f",
        "expected_log_sha256": dict(EXPECTED_LOG_HASHES),
    }


def test_frozen_v6_history_is_blocked_and_honestly_classified() -> None:
    audit = audit_v6_evidence_bytes(**_inputs())
    report = audit.as_dict()
    assert report["status"] == "BLOCK"
    assert sum(item["raw_closed_trade_count"] for item in report["candidates"]) == 842
    assert sum(item["final_trade_count"] for item in report["candidates"]) == 0
    assert report["multiple_testing"]["declared_iterations"] == 2000
    assert report["multiple_testing"]["performed_iterations_inferred_from_bound_early_exit"] == 0
    assert report["multiple_testing"]["complete"] is False
    assert report["stress_matrix"] == {
        "expected_scenarios_per_candidate": 36,
        "expected_scenarios_total": 108,
        "recorded_scenarios": 0,
        "complete": False,
    }
    assert report["registered_model_count"] == 0
    assert report["forecasts"] == report["actions"] == report["execution_intents"] == []
    assert report["activation_eligible"] is False
    json.dumps(report, allow_nan=False)


def test_each_frozen_byte_anchor_is_required() -> None:
    values = _inputs()
    values["runner_bytes"] += b"\n"
    with pytest.raises(V6EvidenceError, match="runner byte SHA-256 mismatch"):
        audit_v6_evidence_bytes(**values)

    values = _inputs()
    values["log_bytes"]["completed_stderr"] = b"warning"
    with pytest.raises(V6EvidenceError, match="completed_stderr byte SHA-256 mismatch"):
        audit_v6_evidence_bytes(**values)


def test_coordinated_artifact_pass_claim_is_rejected() -> None:
    values = _inputs()
    document = json.loads(values["artifact_bytes"])
    candidate = "regime_expert_trend_pit_finance_l30_v1"
    document["results"][candidate]["aggregate"]["gate_status"] = "PASS"
    document["results"][candidate]["aggregate"]["block_reasons"] = []
    payload = json.dumps(
        document, ensure_ascii=False, allow_nan=False, sort_keys=True
    ).encode("utf-8")
    values["artifact_bytes"] = payload
    values["expected_artifact_sha256"] = hashlib.sha256(payload).hexdigest()
    with pytest.raises(V6EvidenceError, match="candidate mappings|gate_status differs"):
        audit_v6_evidence_bytes(**values)


def test_duplicate_key_and_nonfinite_json_fail_closed() -> None:
    values = _inputs()
    payload = values["campaign_bytes"].replace(
        b'"campaign_id":', b'"campaign_id":"duplicate","campaign_id":', 1
    )
    values["campaign_bytes"] = payload
    values["expected_campaign_sha256"] = hashlib.sha256(payload).hexdigest()
    with pytest.raises(V6EvidenceError, match="duplicate key"):
        audit_v6_evidence_bytes(**values)

    values = _inputs()
    payload = values["campaign_bytes"].replace(b'"ridge_lambda": 30.0', b'"ridge_lambda": NaN', 1)
    values["campaign_bytes"] = payload
    values["expected_campaign_sha256"] = hashlib.sha256(payload).hexdigest()
    with pytest.raises(V6EvidenceError, match="non-finite"):
        audit_v6_evidence_bytes(**values)


def test_completed_log_cannot_smuggle_a_pass_even_with_rehashed_bytes() -> None:
    values = _inputs()
    lines = values["log_bytes"]["completed_stdout"].decode("utf-8").splitlines()
    final = json.loads(lines[-1])
    final["passes"] = ["hurdle_trend_pit_finance_l30_v1"]
    lines[-1] = json.dumps(final, ensure_ascii=False)
    payload = ("\n".join(lines) + "\n").encode("utf-8")
    values["log_bytes"]["completed_stdout"] = payload
    values["expected_log_sha256"]["completed_stdout"] = hashlib.sha256(payload).hexdigest()
    with pytest.raises(V6EvidenceError, match="PASS"):
        audit_v6_evidence_bytes(**values)
