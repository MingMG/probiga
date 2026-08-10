from __future__ import annotations

from copy import deepcopy
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from tools.trading_v6 import audit_release, release_integrity
from tools.trading_v6.release_integrity import validate_v6_release
from tools import validate_trading_release as generic_release


ROOT = Path(__file__).resolve().parents[1]
RELEASE_ID = "trading_v6.0.0-research"


def test_release_manifest_and_all_owned_hashes_validate() -> None:
    result = validate_v6_release()
    assert result.document["release_id"] == RELEASE_ID
    assert result.document["release_decision"] == "BLOCK"
    assert result.document["database_tests_run"] is False
    assert result.status == "INTERNAL_HASH_CONSISTENT_NOT_EXTERNALLY_ANCHORED"


def test_integrity_validator_rejects_open_runtime_semantics() -> None:
    runtime = json.loads((
        ROOT
        / "strategies/trading_v6/releases"
        / RELEASE_ID
        / "runtime.json"
    ).read_text(encoding="utf-8"))
    runtime["execution_boundary"]["real_orders_allowed"] = True
    with pytest.raises(release_integrity.V6ReleaseIntegrityError, match="boundary"):
        release_integrity._validate_runtime(runtime)


def test_generic_validator_checks_historical_dependency_hash(
    monkeypatch,
) -> None:
    manifest_path = (
        ROOT / "versions/trading_v6/releases" / RELEASE_ID / "manifest.json"
    )
    original = generic_release._read_json_no_duplicates

    def altered(path):
        document = original(path)
        if path.resolve() == manifest_path.resolve():
            document = deepcopy(document)
            document["historical_dependency_files"]["legacy_runner"][
                "sha256"
            ] = "0" * 64
        return document

    monkeypatch.setattr(generic_release, "_read_json_no_duplicates", altered)
    with pytest.raises(generic_release.ReleaseManifestError, match="hash drifted"):
        generic_release.validate_release_manifest(manifest_path)


def test_release_audit_has_no_model_action_or_order_payload() -> None:
    report = audit_release.build_release_audit()
    assert report["release_decision"] == "BLOCK"
    assert report["historical_evidence_audit"]["status"] == "BLOCK"
    assert report["historical_evidence_audit"]["multiple_testing"]["complete"] is False
    assert report["historical_evidence_audit"]["stress_matrix"]["recorded_scenarios"] == 0
    assert report["registered_model_count"] == 0
    assert report["forecasts"] == report["actions"] == report["execution_intents"] == []
    assert report["database"] == {
        "status": "DEFERRED_MIGRATION_IN_PROGRESS",
        "tests_run": False,
        "counts_as_pass": False,
    }
    assert report["activation_eligible"] is False
    assert report["paper_orders_allowed"] is False
    assert report["real_orders_allowed"] is False
    json.dumps(report, allow_nan=False)


@pytest.mark.parametrize(
    "section,name,value",
    [
        ("database", "counts_as_pass", True),
        ("prospective_gate", "counts_as_pass", True),
        ("research_primitives", "historical_result_reproduced_by_new_primitives", True),
        ("historical_limitations", "stress_matrix_complete", True),
    ],
)
def test_runtime_contradictory_claims_are_rejected(
    monkeypatch, section, name, value
) -> None:
    original = audit_release._read_json_once

    def altered(path_text, expected_hash, label):
        document = original(path_text, expected_hash, label)
        if label == "runtime":
            document = deepcopy(document)
            document[section][name] = value
        return document

    monkeypatch.setattr(audit_release, "_read_json_once", altered)
    with pytest.raises(audit_release.V6ReleaseAuditError):
        audit_release.build_release_audit()


def test_cli_runs_with_site_packages_disabled_from_unrelated_directory(tmp_path) -> None:
    environment = dict(os.environ)
    environment["PYTHONPATH"] = ""
    process = subprocess.run(
        [sys.executable, "-S", str(ROOT / "tools/trading_v6/audit_release.py")],
        cwd=tmp_path, env=environment, capture_output=True, text=True, check=False,
    )
    assert process.returncode == 0, process.stderr
    report = json.loads(process.stdout)
    assert report["release_id"] == RELEASE_ID
    assert report["release_decision"] == "BLOCK"


def test_cli_output_is_version_owned_atomic_and_never_overwrites(
    tmp_path, monkeypatch
) -> None:
    namespace = tmp_path / "artifacts/trading_v6/releases" / RELEASE_ID
    monkeypatch.setattr(audit_release, "ARTIFACT_NAMESPACE", namespace.resolve())
    output = namespace / "audit.json"
    assert audit_release.main(["--output", str(output)]) == 0
    original = output.read_bytes()
    assert audit_release.main(["--output", str(output)]) == 2
    assert output.read_bytes() == original
    outside = tmp_path / "outside.json"
    assert audit_release.main(["--output", str(outside)]) == 2
    assert not outside.exists()
