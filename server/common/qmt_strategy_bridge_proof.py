"""Reproduce native QMT identity from trusted, sealed component source bytes.

The application and the loaded model have different build identities.  An older
native build is valid only when every frozen content identity reproduces from
the current sealed native source.  The supplied root is a caller-owned release
root, never a value from a request.  No Git history, subprocess, network, or QMT
capabilities are fabricated here.  Callers still verify heartbeat freshness,
the scheduler/app build and the captured execution timestamps independently.
"""
from __future__ import annotations

import ast
import hashlib
from pathlib import Path
import re
import stat
from typing import Any, Mapping

from integrations.bigqmt.release_identity import (
    STRATEGY_IDENTITY_PROTOCOL,
    STRATEGY_RELEASE_MANIFEST_SCHEMA,
    STRATEGY_RELEASE_PROTOCOL,
    render_strategy_artifact,
    strategy_loaded_identity_sha256,
)


ROOT = Path(__file__).resolve().parents[2]
STRATEGY_SOURCE = Path("integrations/bigqmt/qmt_strategy/probiga_big_qmt_bridge.py")
DIRECT_MODEL_SOURCE = Path("acquisition/qmt_model.py")
IDENTITY_FIELDS = (
    "source", "model_instance_id", "strategy_build_sha", "strategy_git_blob",
    "strategy_source_sha256", "strategy_artifact_sha256",
    "strategy_loaded_identity_sha256", "direct_acquisition_model_sha256",
    "strategy_identity_frozen", "strategy_identity_status", "updated_at",
)


def _fail(reason: str) -> None:
    raise ValueError("QMT_SIMULATION_BRIDGE_" + reason)


def _hex(value: Any, length: int) -> bool:
    return (isinstance(value, str) and re.fullmatch("[0-9a-f]{" + str(length) + "}", value) is not None
            and value != "0" * length)


def _trusted_bytes(root: Path, relative: Path) -> bytes:
    """Reject links/reparse points and non-LF bytes instead of normalizing them."""
    try:
        if not root.is_absolute():
            _fail("TRUSTED_SOURCE_ROOT_INVALID")
        cursor = root
        for index, part in enumerate((None,) + relative.parts):
            if part is not None:
                cursor = cursor / part
            info = cursor.lstat()
            if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
                _fail("TRUSTED_SOURCE_NOT_ORDINARY")
            final = index == len(relative.parts)
            if not (stat.S_ISREG(info.st_mode) if final else stat.S_ISDIR(info.st_mode)):
                _fail("TRUSTED_SOURCE_NOT_ORDINARY")
        if not 0 < info.st_size <= 2 * 1024 * 1024:
            _fail("TRUSTED_SOURCE_SIZE_INVALID")
        encoded = cursor.read_bytes()
        if not encoded or b"\r" in encoded or b"\x00" in encoded:
            _fail("TRUSTED_SOURCE_BYTES_INVALID")
        return encoded
    except (OSError, RuntimeError):
        _fail("TRUSTED_SOURCE_UNAVAILABLE")


def _native_constants(source: bytes) -> dict[str, str]:
    expected = {
        "BRIDGE_VERSION": "bigqmt_inner_v2",
        "STRATEGY_RELEASE_PROTOCOL": STRATEGY_RELEASE_PROTOCOL,
        "STRATEGY_IDENTITY_PROTOCOL": STRATEGY_IDENTITY_PROTOCOL,
        "STRATEGY_RELEASE_MANIFEST_SCHEMA": STRATEGY_RELEASE_MANIFEST_SCHEMA,
    }
    names = set(expected) | {"DIRECT_ACQUISITION_MODEL_SHA256"}
    found: dict[str, list[Any]] = {name: [] for name in names}
    try:
        for node in ast.parse(source.decode("utf-8")).body:
            if isinstance(node, ast.Assign) and len(node.targets) == 1:
                target = node.targets[0]
                if isinstance(target, ast.Name) and target.id in names:
                    found[target.id].append(ast.literal_eval(node.value))
    except (ValueError, SyntaxError, UnicodeError):
        _fail("TRUSTED_SOURCE_CONTRACT_INVALID")
    if any(len(values) != 1 or not isinstance(values[0], str) for values in found.values()):
        _fail("TRUSTED_SOURCE_CONTRACT_INVALID")
    constants = {name: values[0] for name, values in found.items()}
    if any(constants[name] != value for name, value in expected.items()):
        _fail("TRUSTED_SOURCE_PROTOCOL_DIFFERS")
    if not _hex(constants["DIRECT_ACQUISITION_MODEL_SHA256"], 64):
        _fail("TRUSTED_DIRECT_MODEL_BINDING_INVALID")
    return constants


def validate_qmt_strategy_bridge_identity(
    bridge: Mapping[str, Any], *, expected_app_build_sha: str, root: Path | None = None,
) -> dict[str, Any]:
    """Validate the 11 captured identity fields without weakening build ownership.

    Safety/protocol claims, when present, are also checked.  Their absence in
    the native heartbeat is not represented as an RPC safety assertion: this
    result is a source-content proof, not a fabricated capabilities response.
    """
    if not _hex(expected_app_build_sha, 40):
        _fail("APP_BUILD_INVALID")
    if not isinstance(bridge, Mapping) or any(key not in bridge for key in IDENTITY_FIELDS):
        _fail("IDENTITY_INCOMPLETE")
    if (bridge["source"] != "gj_big_qmt_inner"
            or bridge["strategy_identity_frozen"] is not True
            or bridge["strategy_identity_status"] != "BOUND"
            or not isinstance(bridge["updated_at"], str) or not bridge["updated_at"]):
        _fail("IDENTITY_UNBOUND")
    lengths = {"model_instance_id": 32, "strategy_build_sha": 40, "strategy_git_blob": 40,
               "strategy_source_sha256": 64, "strategy_artifact_sha256": 64,
               "strategy_loaded_identity_sha256": 64, "direct_acquisition_model_sha256": 64}
    if any(not _hex(bridge[key], length) for key, length in lengths.items()):
        _fail("IDENTITY_HASH_INVALID")
    optional = {"bridge_version": "bigqmt_inner_v2", "strategy_release_protocol": STRATEGY_RELEASE_PROTOCOL,
                "strategy_identity_protocol": STRATEGY_IDENTITY_PROTOCOL,
                "read_only": True, "simulation_only": True,
                "automatic_real_order_submission": False, "real_order_authority": False}
    for key, expected in optional.items():
        if key in bridge and (bridge[key] is not expected if isinstance(expected, bool) else bridge[key] != expected):
            _fail("NATIVE_CLAIM_DIFFERS")
    trusted_root = ROOT if root is None else Path(root)
    source = _trusted_bytes(trusted_root, STRATEGY_SOURCE)
    direct = _trusted_bytes(trusted_root, DIRECT_MODEL_SOURCE)
    constants = _native_constants(source)
    source_hash = hashlib.sha256(source).hexdigest()
    git_blob = hashlib.sha1(b"blob " + str(len(source)).encode("ascii") + b"\0" + source).hexdigest()
    direct_hash = hashlib.sha256(direct).hexdigest()
    if (bridge["strategy_source_sha256"] != source_hash or bridge["strategy_git_blob"] != git_blob):
        _fail("NATIVE_SOURCE_DIFFERS")
    if (constants["DIRECT_ACQUISITION_MODEL_SHA256"] != direct_hash
            or bridge["direct_acquisition_model_sha256"] != direct_hash):
        _fail("DIRECT_MODEL_DIFFERS")
    loaded_build = bridge["strategy_build_sha"]
    try:
        rendered = render_strategy_artifact(source, build_sha=loaded_build, git_blob=git_blob,
                                            source_sha256=source_hash)
        identity_hash = strategy_loaded_identity_sha256(build_sha=loaded_build, git_blob=git_blob,
                                                        source_sha256=source_hash)
    except RuntimeError:
        _fail("REPRODUCIBLE_ARTIFACT_UNAVAILABLE")
    if (bridge["strategy_artifact_sha256"] != rendered["artifact_sha256"]
            or bridge["strategy_loaded_identity_sha256"] != identity_hash):
        _fail("FROZEN_ARTIFACT_DIFFERS")
    return {
        "schema": "probiga.qmt-strategy-bridge-content-proof.v1",
        **{key: bridge[key] for key in IDENTITY_FIELDS},
        "compatible_app_build_sha": expected_app_build_sha,
        "strategy_compatibility_status": "EXACT_BUILD" if loaded_build == expected_app_build_sha else "CONTENT_COMPATIBLE",
        "strategy_release_protocol": STRATEGY_RELEASE_PROTOCOL,
        "strategy_identity_protocol": STRATEGY_IDENTITY_PROTOCOL,
        "verification_basis": "SEALED_CURRENT_SOURCE_AND_REPRODUCIBLE_NATIVE_ARTIFACT",
    }
