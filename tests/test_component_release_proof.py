from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from server.common.component_release import (
    build_component_release,
    write_component_release,
)
from server.common.component_release_proof import (
    PROOF_FILENAME,
    ComponentReleaseProofError,
    build_component_release_lite,
    validate_component_release_lite,
    write_component_release_lite,
)


LINUX = "b" * 40
WINDOWS = "a" * 40


def _manifest():
    return build_component_release(
        linux_build_sha=LINUX,
        windows_build_sha=WINDOWS,
        contract_build_sha=WINDOWS,
        contract_sha256="d" * 64,
        parent_linux_build_sha=WINDOWS,
        scope="LINUX",
        created_at="2026-11-01T00:00:00Z",
    )


def test_lite_manifest_is_an_independently_signed_envelope():
    key = Ed25519PrivateKey.generate()
    manifest = _manifest()
    proof = build_component_release_lite(manifest, key)

    assert proof["schema"] == "probiga.component-release-lite.v1"
    assert proof["schema_version"] == "lite-1"
    assert proof["mode"] == "fast"
    assert proof["linux_build_sha"] == LINUX
    assert json.loads(proof["manifest_json"]) == manifest
    assert validate_component_release_lite(
        proof,
        manifest,
        expected_linux_sha=LINUX,
    ) == proof


def test_lite_manifest_rejects_signature_or_manifest_tampering():
    key = Ed25519PrivateKey.generate()
    manifest = _manifest()
    proof = build_component_release_lite(manifest, key)
    tampered = dict(proof)
    tampered["signature"] = "A" * 88
    with pytest.raises(ComponentReleaseProofError):
        validate_component_release_lite(
            tampered,
            manifest,
            expected_linux_sha=LINUX,
        )

    other = build_component_release(
        linux_build_sha=LINUX,
        windows_build_sha=WINDOWS,
        contract_build_sha=WINDOWS,
        contract_sha256="e" * 64,
        parent_linux_build_sha=WINDOWS,
        scope="LINUX",
        created_at="2026-11-01T00:00:01Z",
    )
    with pytest.raises(ComponentReleaseProofError):
        validate_component_release_lite(
            proof,
            other,
            expected_linux_sha=LINUX,
        )


def test_lite_manifest_writer_is_atomic_and_immutable(tmp_path: Path):
    key = Ed25519PrivateKey.generate()
    manifest = _manifest()
    manifest_path = tmp_path / "component-release.json"
    write_component_release(manifest_path, manifest)
    try:
        proof_path = write_component_release_lite(manifest_path, key)
        assert proof_path.name == PROOF_FILENAME
        first = proof_path.read_bytes()
        assert write_component_release_lite(manifest_path, key) == proof_path
        assert proof_path.read_bytes() == first
    finally:
        proof_path = tmp_path / PROOF_FILENAME
        if proof_path.exists():
            os.chmod(proof_path, 0o600)


def test_fast_dispatch_signs_locally_and_schedules_ledger_after_schema_guard():
    script = (
        Path(__file__).resolve().parents[1]
        / "deploy"
        / "production_deploy.sh"
    ).read_text(encoding="utf-8")
    manifest_branch = script.index(
        'if [ "$RELEASE_MODE" = fast ]; then\n'
        "    /usr/bin/env -i"
    )
    assert "--mode sign" in script[manifest_branch:manifest_branch + 1200]
    dispatch = script.index(
        'if [ "$RELEASE_EXECUTION_SCOPE" = LINUX ]; then\n'
        "  CUTOVER_STEP=fast_release_schema_guard"
    )
    guard = script.index("check_fast_release_schema", dispatch)
    ledger = script.index("start_fast_component_ledger_publication", guard)
    activation = script.index("activate_linux_release", ledger)
    assert guard < ledger < activation
    scheduler = script.index("start_fast_component_ledger_publication() {")
    scheduler_body = script[scheduler:script.index("recover_linux_activation_before_inventory() {", scheduler)]
    assert "/usr/bin/systemd-run" in scheduler_body
    assert "--on-active=\"${LEDGER_PENDING_DELAY_SECONDS}s\"" in scheduler_body
    assert "--pending-marker \"$marker\"" in scheduler_body
    assert "ledger-pending" in script
