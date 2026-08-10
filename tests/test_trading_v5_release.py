from __future__ import annotations

from copy import deepcopy
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from tools.trading_v5 import audit_release, release_integrity
from tools.trading_v5.release_integrity import validate_v5_release


ROOT = Path(__file__).resolve().parents[1]
RELEASE_ID = "trading_v5.0.0-research"
MANIFEST = (
    ROOT
    / "versions/trading_v5/releases"
    / RELEASE_ID
    / "manifest.json"
)


def test_release_manifest_and_all_owned_hashes_validate() -> None:
    result = validate_v5_release()
    assert result.status == "INTERNAL_HASH_CONSISTENT_NOT_EXTERNALLY_ANCHORED"
    assert result.document["system"] == "trading_v5"
    assert result.document["release_id"] == RELEASE_ID
    assert result.document["lifecycle_status"] == "RESEARCH_ONLY"
    assert len(result.document["source_files"]) == 7
    assert len(result.document["test_files"]) == 4


def test_integrity_validator_rejects_open_runtime_semantics() -> None:
    runtime = json.loads((
        ROOT
        / "strategies/trading_v5/releases"
        / RELEASE_ID
        / "runtime.json"
    ).read_text(encoding="utf-8"))
    runtime["execution_boundary"]["actionable_output_allowed"] = True
    with pytest.raises(release_integrity.V5ReleaseIntegrityError, match="boundary"):
        release_integrity._validate_runtime(runtime)


def test_release_audit_stays_blocked_and_has_no_action_payload() -> None:
    report = audit_release.build_release_audit()
    assert report["release_decision"] == "BLOCK"
    assert report["manifest_integrity_status"] == (
        "INTERNAL_HASH_CONSISTENT_NOT_EXTERNALLY_ANCHORED"
    )
    assert report["lifecycle_status"] == "RESEARCH_ONLY"
    assert len(report["historical_campaigns"]) == 2
    assert all(
        item["historical_gate_status"] == "BLOCK"
        for item in report["historical_campaigns"]
    )
    assert report["registered_model_count"] == 0
    assert report["forecasts"] == []
    assert report["actions"] == []
    assert report["execution_intents"] == []
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
    "label,mutate",
    [
        (
            "runtime",
            lambda document: document["prospective_gate"].__setitem__(
                "counts_as_pass", True
            ),
        ),
        (
            "runtime",
            lambda document: document["model_research"].__setitem__(
                "model_activation_claim_forbidden", False
            ),
        ),
        (
            "runtime",
            lambda document: document["owned_campaigns"][0].__setitem__(
                "historical_gate_status", "PASS"
            ),
        ),
        (
            "model environment",
            lambda document: document.__setitem__(
                "counts_as_activation_evidence", True
            ),
        ),
    ],
)
def test_cli_rejects_contradictory_nested_claims(
    monkeypatch,
    label,
    mutate,
) -> None:
    original = audit_release._read_frozen_json

    def altered(path_text, expected_hash, actual_label):
        document = original(path_text, expected_hash, actual_label)
        if actual_label == label:
            document = deepcopy(document)
            mutate(document)
        return document

    monkeypatch.setattr(audit_release, "_read_frozen_json", altered)
    with pytest.raises(audit_release.ReleaseAuditError):
        audit_release.build_release_audit()


def test_cli_runs_from_an_unrelated_directory_with_empty_pythonpath(tmp_path) -> None:
    environment = dict(os.environ)
    environment["PYTHONPATH"] = ""
    process = subprocess.run(
        [
            sys.executable,
            str(ROOT / "tools/trading_v5/audit_release.py"),
        ],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert process.returncode == 0, process.stderr
    report = json.loads(process.stdout)
    assert report["release_id"] == RELEASE_ID
    assert report["release_decision"] == "BLOCK"


def test_cli_output_is_version_owned_and_never_overwrites(
    tmp_path,
    monkeypatch,
) -> None:
    namespace = tmp_path / "artifacts/trading_v5/releases" / RELEASE_ID
    monkeypatch.setattr(
        audit_release,
        "EXPECTED_ARTIFACT_NAMESPACE",
        namespace.resolve(),
    )
    output = namespace / "audit.json"
    assert audit_release.main(["--output", str(output)]) == 0
    assert output.is_file()
    original = output.read_bytes()
    assert audit_release.main(["--output", str(output)]) == 2
    assert output.read_bytes() == original
    outside = tmp_path / "outside.json"
    assert audit_release.main(["--output", str(outside)]) == 2
    assert not outside.exists()
