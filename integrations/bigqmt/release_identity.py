from __future__ import annotations

"""Build-bound identity contract for the QMT built-in strategy.

The strategy itself is copied from a Git blob and freezes the accompanying
manifest when QMT loads the model.  Consumers compare that frozen identity
with the same Git object, so replacing the file on disk cannot make an old
in-memory model claim that it was reloaded.
"""

import hashlib
import subprocess
from pathlib import Path
from typing import Any


STRATEGY_RELEASE_PROTOCOL = "probiga.bigqmt-strategy-release.v2"
STRATEGY_IDENTITY_PROTOCOL = "probiga.bigqmt-loaded-strategy-identity.v1"
STRATEGY_RELEASE_MANIFEST_SCHEMA = "probiga.bigqmt-strategy-manifest.v1"
STRATEGY_RELEASE_MANIFEST_NAME = "probiga_big_qmt_bridge.release.json"
EMBEDDED_BUILD_SHA_MARKER = "__PROBIGA_EMBEDDED_BUILD_SHA__"
EMBEDDED_GIT_BLOB_MARKER = "__PROBIGA_EMBEDDED_GIT_BLOB__"
EMBEDDED_SOURCE_SHA256_MARKER = "__PROBIGA_EMBEDDED_SOURCE_SHA256__"
EMBEDDED_IDENTITY_SHA256_MARKER = "__PROBIGA_EMBEDDED_IDENTITY_SHA256__"


def normalize_build_sha(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if (
        len(normalized) != 40
        or normalized == "0" * 40
        or any(character not in "0123456789abcdef" for character in normalized)
    ):
        raise RuntimeError("BigQMT strategy release build SHA is invalid")
    return normalized


def _normalize_git_object(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if (
        len(normalized) not in {40, 64}
        or any(character not in "0123456789abcdef" for character in normalized)
    ):
        raise RuntimeError("BigQMT strategy Git blob identity is invalid")
    return normalized


def git_strategy_artifact(
    *,
    root: Path,
    source_path: Path,
    build_sha: str,
) -> dict[str, Any]:
    """Load the exact strategy bytes and blob id from one Git commit."""

    normalized_build = normalize_build_sha(build_sha)
    resolved_root = root.resolve()
    resolved_source = source_path.resolve()
    try:
        relative_source = resolved_source.relative_to(resolved_root).as_posix()
    except ValueError as exc:
        raise RuntimeError("BigQMT strategy source is outside the repository") from exc
    object_spec = f"{normalized_build}:{relative_source}"
    blob_result = subprocess.run(
        ["git", "-C", str(resolved_root), "rev-parse", object_spec],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="strict",
        check=True,
        timeout=30,
    )
    content_result = subprocess.run(
        ["git", "-C", str(resolved_root), "show", object_spec],
        capture_output=True,
        check=True,
        timeout=30,
    )
    source_bytes = bytes(content_result.stdout)
    if not source_bytes:
        raise RuntimeError("BigQMT strategy Git blob is empty")
    return {
        "build_sha": normalized_build,
        "git_blob": _normalize_git_object(blob_result.stdout),
        "source_bytes": source_bytes,
        "source_sha256": hashlib.sha256(source_bytes).hexdigest(),
        "repository_path": relative_source,
    }


def build_strategy_release_manifest(
    *,
    build_sha: str,
    git_blob: str,
    source_sha256: str,
    artifact_sha256: str,
    identity_sha256: str,
) -> dict[str, str]:
    normalized_hash = str(source_sha256 or "").strip().lower()
    if (
        len(normalized_hash) != 64
        or any(character not in "0123456789abcdef" for character in normalized_hash)
    ):
        raise RuntimeError("BigQMT strategy source hash is invalid")
    normalized_artifact_hash = str(artifact_sha256 or "").strip().lower()
    normalized_identity_hash = str(identity_sha256 or "").strip().lower()
    if any(
        len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
        for value in (normalized_artifact_hash, normalized_identity_hash)
    ):
        raise RuntimeError("BigQMT generated strategy identity hash is invalid")
    return {
        "schema": STRATEGY_RELEASE_MANIFEST_SCHEMA,
        "strategy_release_protocol": STRATEGY_RELEASE_PROTOCOL,
        "strategy_identity_protocol": STRATEGY_IDENTITY_PROTOCOL,
        "strategy_build_sha": normalize_build_sha(build_sha),
        "strategy_git_blob": _normalize_git_object(git_blob),
        "strategy_source_sha256": normalized_hash,
        "strategy_artifact_sha256": normalized_artifact_hash,
        "strategy_loaded_identity_sha256": normalized_identity_hash,
    }


def strategy_loaded_identity_sha256(
    *,
    build_sha: str,
    git_blob: str,
    source_sha256: str,
) -> str:
    payload = (
        STRATEGY_IDENTITY_PROTOCOL
        + "\n"
        + normalize_build_sha(build_sha)
        + "\n"
        + _normalize_git_object(git_blob)
        + "\n"
        + str(source_sha256 or "").strip().lower()
    )
    return hashlib.sha256(payload.encode("ascii")).hexdigest()


def render_strategy_artifact(
    source_bytes: bytes,
    *,
    build_sha: str,
    git_blob: str,
    source_sha256: str,
) -> dict[str, Any]:
    """Inject immutable release literals into the exact strategy Git blob."""

    normalized_build = normalize_build_sha(build_sha)
    normalized_blob = _normalize_git_object(git_blob)
    normalized_source = str(source_sha256 or "").strip().lower()
    if hashlib.sha256(source_bytes).hexdigest() != normalized_source:
        raise RuntimeError("BigQMT strategy Git blob hash differs")
    identity_hash = strategy_loaded_identity_sha256(
        build_sha=normalized_build,
        git_blob=normalized_blob,
        source_sha256=normalized_source,
    )
    rendered = bytes(source_bytes)
    replacements = {
        EMBEDDED_BUILD_SHA_MARKER: normalized_build,
        EMBEDDED_GIT_BLOB_MARKER: normalized_blob,
        EMBEDDED_SOURCE_SHA256_MARKER: normalized_source,
        EMBEDDED_IDENTITY_SHA256_MARKER: identity_hash,
    }
    for marker, value in replacements.items():
        encoded_marker = marker.encode("ascii")
        if rendered.count(encoded_marker) != 1:
            raise RuntimeError(
                f"BigQMT strategy embedded marker count differs: {marker}"
            )
        rendered = rendered.replace(encoded_marker, value.encode("ascii"))
    return {
        "source_bytes": rendered,
        "artifact_sha256": hashlib.sha256(rendered).hexdigest(),
        "identity_sha256": identity_hash,
    }


def validate_strategy_release_payload(
    payload: dict[str, Any],
    *,
    expected_build_sha: str,
    root: Path,
    source_path: Path,
    expected_source_sha256: str | None = None,
    expected_git_blob: str | None = None,
    expected_artifact_sha256: str | None = None,
) -> dict[str, Any]:
    """Validate QMT's frozen, in-memory identity against an exact Git blob."""

    expected_build = normalize_build_sha(expected_build_sha)
    if (
        expected_source_sha256 is None
        or expected_git_blob is None
        or expected_artifact_sha256 is None
    ):
        artifact = git_strategy_artifact(
            root=root,
            source_path=source_path,
            build_sha=expected_build,
        )
        expected_hash = str(artifact["source_sha256"])
        expected_blob = str(artifact["git_blob"])
        rendered = render_strategy_artifact(
            artifact["source_bytes"],
            build_sha=expected_build,
            git_blob=expected_blob,
            source_sha256=expected_hash,
        )
        expected_artifact_hash = str(rendered["artifact_sha256"])
    else:
        expected_hash = str(expected_source_sha256 or "").strip().lower()
        expected_blob = _normalize_git_object(expected_git_blob)
        expected_artifact_hash = str(
            expected_artifact_sha256 or ""
        ).strip().lower()
    if (
        len(expected_hash) != 64
        or any(character not in "0123456789abcdef" for character in expected_hash)
    ):
        raise RuntimeError("BigQMT expected strategy source hash is invalid")
    if (
        len(expected_artifact_hash) != 64
        or any(
            character not in "0123456789abcdef"
            for character in expected_artifact_hash
        )
    ):
        raise RuntimeError("BigQMT expected strategy artifact hash is invalid")
    expected_identity_hash = strategy_loaded_identity_sha256(
        build_sha=expected_build,
        git_blob=expected_blob,
        source_sha256=expected_hash,
    )

    native = payload.get("native_capabilities")
    native_by_name = {
        str(item.get("capability") or ""): item
        for item in native
        if isinstance(item, dict)
    } if isinstance(native, list) else {}
    calendar = native_by_name.get("trading_calendar") or {}
    index_weight = native_by_name.get("index_weight") or {}
    actions = payload.get("actions")
    if (
        payload.get("status") != "ok"
        or payload.get("source") != "gj_big_qmt_inner"
        or payload.get("bridge_version") != "bigqmt_inner_v2"
        or payload.get("strategy_release_protocol") != STRATEGY_RELEASE_PROTOCOL
        or payload.get("strategy_identity_protocol") != STRATEGY_IDENTITY_PROTOCOL
        or payload.get("strategy_identity_frozen") is not True
        or payload.get("strategy_identity_status") != "BOUND"
        or str(payload.get("strategy_build_sha") or "").lower()
        != expected_build
        or str(payload.get("strategy_git_blob") or "").lower()
        != expected_blob
        or str(payload.get("strategy_source_sha256") or "").lower()
        != expected_hash
        or str(payload.get("strategy_artifact_sha256") or "").lower()
        != expected_artifact_hash
        or str(payload.get("strategy_loaded_identity_sha256") or "").lower()
        != expected_identity_hash
        or not isinstance(actions, list)
        or "trading_calendar" not in actions
        or calendar.get("action") != "trading_calendar"
        or calendar.get("available") is not True
        or calendar.get("source_method") != "ContextInfo.get_trading_dates"
        or index_weight.get("available") is not False
        or index_weight.get("source_method")
        != "membership_only_no_native_weight"
    ):
        raise RuntimeError(
            "exact-main BigQMT strategy release proof is unavailable; "
            "install and reload the QMT model before retrying"
        )
    return {
        "schema": "probiga.bigqmt-strategy-release-proof.v2",
        "strategy_release_protocol": STRATEGY_RELEASE_PROTOCOL,
        "strategy_identity_protocol": STRATEGY_IDENTITY_PROTOCOL,
        "strategy_identity_frozen": True,
        "strategy_identity_status": "BOUND",
        "strategy_build_sha": expected_build,
        "strategy_git_blob": expected_blob,
        "strategy_source_sha256": expected_hash,
        "strategy_artifact_sha256": expected_artifact_hash,
        "strategy_loaded_identity_sha256": expected_identity_hash,
        "trading_calendar": dict(calendar),
        "index_weight": dict(index_weight),
    }
