"""Native content proof parity on Windows and Git-free sealed Linux releases."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import subprocess

import pytest

from integrations.bigqmt.release_identity import render_strategy_artifact
from server.common import qmt_strategy_bridge_proof as proof


ROOT = Path(__file__).resolve().parents[1]
APP_BUILD = "a" * 40
OLD_NATIVE_BUILD = "b" * 40


def identity(source, direct, *, native_build=APP_BUILD):
    source_hash = hashlib.sha256(source).hexdigest()
    blob = hashlib.sha1(b"blob " + str(len(source)).encode("ascii") + b"\0" + source).hexdigest()
    rendered = render_strategy_artifact(source, build_sha=native_build, git_blob=blob, source_sha256=source_hash)
    return {
        "source": "gj_big_qmt_inner", "model_instance_id": "c" * 32,
        "strategy_build_sha": native_build, "strategy_git_blob": blob,
        "strategy_source_sha256": source_hash, "strategy_artifact_sha256": rendered["artifact_sha256"],
        "strategy_loaded_identity_sha256": rendered["identity_sha256"],
        "direct_acquisition_model_sha256": hashlib.sha256(direct).hexdigest(),
        "strategy_identity_frozen": True, "strategy_identity_status": "BOUND",
        "updated_at": "2026-10-08T22:50:00+08:00",
    }


@pytest.fixture
def sealed(tmp_path):
    source = (ROOT / proof.STRATEGY_SOURCE).read_bytes()
    direct = (ROOT / proof.DIRECT_MODEL_SOURCE).read_bytes()
    for relative, content in [(proof.STRATEGY_SOURCE, source), (proof.DIRECT_MODEL_SOURCE, direct)]:
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    return tmp_path, source, direct


def validate(bridge, root, *, expected=APP_BUILD):
    return proof.validate_qmt_strategy_bridge_identity(bridge, expected_app_build_sha=expected, root=root)


@pytest.mark.parametrize("native_build,expected_status", [(APP_BUILD, "EXACT_BUILD"), (OLD_NATIVE_BUILD, "CONTENT_COMPATIBLE")])
def test_exact_and_content_identical_native_build_are_proved_without_git(sealed, monkeypatch, native_build, expected_status):
    root, source, direct = sealed
    assert not (root / ".git").exists()
    monkeypatch.setattr(subprocess, "run", lambda *_args, **_kwargs: pytest.fail("Git is unavailable in a sealed runtime"))
    monkeypatch.setattr(subprocess, "check_output", lambda *_args, **_kwargs: pytest.fail("no child process is permitted"))
    raw = identity(source, direct, native_build=native_build)
    before = deepcopy(raw)
    verified = validate(raw, root)
    assert verified["strategy_compatibility_status"] == expected_status
    assert verified["compatible_app_build_sha"] == APP_BUILD
    assert verified["strategy_build_sha"] == native_build
    assert verified["strategy_git_blob"] == raw["strategy_git_blob"]
    assert raw == before
    assert json.loads(json.dumps(verified, allow_nan=False)) == verified
    assert "read_only" not in verified and "simulation_only" not in verified
    assert "automatic_real_order_submission" not in verified and "real_order_authority" not in verified


@pytest.mark.parametrize("field,value", [
    ("source", "other"), ("model_instance_id", ""), ("model_instance_id", "0" * 32),
    ("model_instance_id", "X" * 32), ("strategy_build_sha", "0" * 40),
    ("strategy_build_sha", "A" * 40), ("strategy_build_sha", "invalid"),
    ("strategy_git_blob", "d" * 40), ("strategy_git_blob", "d" * 64),
    ("strategy_source_sha256", "d" * 64), ("strategy_source_sha256", "D" * 64),
    ("strategy_artifact_sha256", "d" * 64), ("strategy_artifact_sha256", None),
    ("strategy_loaded_identity_sha256", "d" * 64),
    ("direct_acquisition_model_sha256", "d" * 64),
    ("strategy_identity_frozen", False), ("strategy_identity_frozen", 1),
    ("strategy_identity_status", "UNAVAILABLE"), ("updated_at", ""), ("updated_at", None),
])
def test_format_valid_or_invalid_identity_tampering_fails_closed(sealed, field, value):
    root, source, direct = sealed
    raw = identity(source, direct, native_build=OLD_NATIVE_BUILD)
    raw[field] = value
    with pytest.raises(ValueError, match="QMT_SIMULATION_BRIDGE_"):
        validate(raw, root)


@pytest.mark.parametrize("field", proof.IDENTITY_FIELDS)
def test_each_captured_field_is_required(sealed, field):
    root, source, direct = sealed
    raw = identity(source, direct)
    raw.pop(field)
    with pytest.raises(ValueError, match="IDENTITY_INCOMPLETE"):
        validate(raw, root)


@pytest.mark.parametrize("expected", ["", "0" * 40, "A" * 40, "not-a-build"])
def test_current_app_build_cannot_be_missing_or_unbound(sealed, expected):
    root, source, direct = sealed
    with pytest.raises(ValueError, match="APP_BUILD_INVALID"):
        validate(identity(source, direct), root, expected=expected)


@pytest.mark.parametrize("field,value", [
    ("bridge_version", "different"), ("strategy_release_protocol", "different"),
    ("strategy_identity_protocol", "different"), ("read_only", False), ("read_only", 1),
    ("simulation_only", False), ("automatic_real_order_submission", True),
    ("real_order_authority", True),
])
def test_available_native_protocol_and_safety_claims_are_not_ignored(sealed, field, value):
    root, source, direct = sealed
    raw = identity(source, direct)
    raw[field] = value
    with pytest.raises(ValueError, match="NATIVE_CLAIM_DIFFERS"):
        validate(raw, root)


def test_actual_optional_heartbeat_protocols_and_safe_capability_fields_match(sealed):
    root, source, direct = sealed
    raw = identity(source, direct)
    raw.update({"bridge_version": "bigqmt_inner_v2", "strategy_release_protocol": proof.STRATEGY_RELEASE_PROTOCOL,
                "strategy_identity_protocol": proof.STRATEGY_IDENTITY_PROTOCOL,
                "read_only": True, "simulation_only": True,
                "automatic_real_order_submission": False, "real_order_authority": False})
    assert validate(raw, root)["strategy_compatibility_status"] == "EXACT_BUILD"


def test_self_consistent_old_different_source_is_not_content_compatible(sealed):
    root, source, direct = sealed
    old = source + b"\n# previous native implementation differs\n"
    raw = identity(old, direct, native_build=OLD_NATIVE_BUILD)
    with pytest.raises(ValueError, match="NATIVE_SOURCE_DIFFERS"):
        validate(raw, root)


def test_changing_only_native_build_without_reproducing_artifact_is_rejected(sealed):
    root, source, direct = sealed
    raw = identity(source, direct)
    raw["strategy_build_sha"] = OLD_NATIVE_BUILD
    with pytest.raises(ValueError, match="FROZEN_ARTIFACT_DIFFERS"):
        validate(raw, root)


def test_artifact_for_other_build_cannot_pair_with_current_loaded_identity(sealed):
    root, source, direct = sealed
    raw = identity(source, direct)
    old = identity(source, direct, native_build=OLD_NATIVE_BUILD)
    raw["strategy_artifact_sha256"] = old["strategy_artifact_sha256"]
    with pytest.raises(ValueError, match="FROZEN_ARTIFACT_DIFFERS"):
        validate(raw, root)


def test_trusted_direct_model_must_match_native_embedded_binding_even_when_claim_matches_file(sealed):
    root, source, direct = sealed
    changed = direct + b"\n# changed native direct acquisition\n"
    (root / proof.DIRECT_MODEL_SOURCE).write_bytes(changed)
    raw = identity(source, changed)
    with pytest.raises(ValueError, match="DIRECT_MODEL_DIFFERS"):
        validate(raw, root)


def test_changed_current_strategy_bytes_cannot_claim_prior_native_content(sealed):
    root, source, direct = sealed
    raw = identity(source, direct, native_build=OLD_NATIVE_BUILD)
    (root / proof.STRATEGY_SOURCE).write_bytes(source + b"\n# current native implementation differs\n")
    with pytest.raises(ValueError, match="NATIVE_SOURCE_DIFFERS"):
        validate(raw, root)


@pytest.mark.parametrize("relative", [proof.STRATEGY_SOURCE, proof.DIRECT_MODEL_SOURCE])
def test_crlf_checkout_is_rejected_not_silently_normalized(sealed, relative):
    root, source, direct = sealed
    path = root / relative
    path.write_bytes(path.read_bytes().replace(b"\n", b"\r\n"))
    with pytest.raises(ValueError, match="TRUSTED_SOURCE_BYTES_INVALID"):
        validate(identity(source, direct), root)


@pytest.mark.parametrize("relative", [proof.STRATEGY_SOURCE, proof.DIRECT_MODEL_SOURCE])
def test_missing_component_source_fails_without_filesystem_path_leak(sealed, relative):
    root, source, direct = sealed
    (root / relative).unlink()
    with pytest.raises(ValueError, match="TRUSTED_SOURCE_UNAVAILABLE") as exc:
        validate(identity(source, direct), root)
    assert str(root) not in str(exc.value)


def test_duplicate_protocol_binding_in_current_source_is_not_a_verified_contract(sealed):
    root, source, direct = sealed
    changed = source + b'\nSTRATEGY_RELEASE_PROTOCOL = "probiga.bigqmt-strategy-release.v2"\n'
    (root / proof.STRATEGY_SOURCE).write_bytes(changed)
    with pytest.raises(ValueError, match="TRUSTED_SOURCE_CONTRACT_INVALID"):
        validate(identity(changed, direct), root)


def test_native_protocol_version_must_match_established_release_contract(sealed):
    root, source, direct = sealed
    changed = source.replace(b'BRIDGE_VERSION = "bigqmt_inner_v2"', b'BRIDGE_VERSION = "unknown_future_protocol"')
    (root / proof.STRATEGY_SOURCE).write_bytes(changed)
    with pytest.raises(ValueError, match="TRUSTED_SOURCE_PROTOCOL_DIFFERS"):
        validate(identity(changed, direct), root)


def test_invalid_render_markers_cannot_forge_reproducible_native_artifact(sealed):
    root, source, direct = sealed
    raw = identity(source, direct)
    changed = source.replace(b'"__PROBIGA_EMBEDDED_BUILD_SHA__"', b'"unrendered-native-build"')
    (root / proof.STRATEGY_SOURCE).write_bytes(changed)
    raw["strategy_source_sha256"] = hashlib.sha256(changed).hexdigest()
    raw["strategy_git_blob"] = hashlib.sha1(b"blob " + str(len(changed)).encode("ascii") + b"\0" + changed).hexdigest()
    with pytest.raises(ValueError, match="REPRODUCIBLE_ARTIFACT_UNAVAILABLE"):
        validate(raw, root)
