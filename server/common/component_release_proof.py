"""Signed local proof for a fast component release awaiting ledger publication."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import sys
import tempfile
from typing import Any, Mapping

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from server.common.component_release import (
    COMPONENT_RELEASE_FILENAME,
    COMPONENT_RELEASE_ROOT,
    _atomic_install_noreplace,
    load_runtime_component_release,
    validate_component_release,
)
from server.common.component_release_ledger import (
    sign_component_manifest,
    signing_public_key,
    verify_signed_component_manifest,
)


PROOF_SCHEMA = "probiga.component-release-lite.v1"
PROOF_SCHEMA_VERSION = "lite-1"
PROOF_FILENAME = "component-release-lite.json"
_MAX_BYTES = 32768
_SHA40 = re.compile(r"[0-9a-f]{40}\Z")
_FIELDS = frozenset({
    "schema",
    "schema_version",
    "algorithm",
    "mode",
    "scope",
    "linux_build_sha",
    "manifest_json",
    "signature",
    "public_key",
    "proof_sha256",
})


class ComponentReleaseProofError(RuntimeError):
    """The local fast-release proof cannot be trusted."""


def _canonical(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ComponentReleaseProofError("component lite proof has duplicate fields")
        result[key] = value
    return result


def _decode(data: bytes) -> dict[str, str]:
    if len(data) > _MAX_BYTES:
        raise ComponentReleaseProofError("component lite proof exceeds size limit")
    try:
        payload = json.loads(
            data.decode("utf-8"),
            object_pairs_hook=_unique,
        )
    except (UnicodeError, ValueError) as exc:
        raise ComponentReleaseProofError("component lite proof is invalid JSON") from exc
    if not isinstance(payload, dict):
        raise ComponentReleaseProofError("component lite proof must be an object")
    return payload


def validate_component_release_lite(
    payload: Mapping[str, object],
    manifest: Mapping[str, object],
    *,
    expected_linux_sha: str,
) -> dict[str, str]:
    """Validate the lite envelope, embedded manifest and Ed25519 signature."""
    if not isinstance(payload, Mapping) or set(payload) != _FIELDS:
        raise ComponentReleaseProofError("component lite proof fields differ")
    if any(not isinstance(value, str) for value in payload.values()):
        raise ComponentReleaseProofError("component lite proof values must be strings")
    result = dict(payload)
    if (
        result["schema"] != PROOF_SCHEMA
        or result["schema_version"] != PROOF_SCHEMA_VERSION
        or result["algorithm"] != "Ed25519"
        or result["mode"] != "fast"
        or result["scope"] != "LINUX"
    ):
        raise ComponentReleaseProofError("component lite proof identity differs")
    if (
        not _SHA40.fullmatch(expected_linux_sha)
        or expected_linux_sha == "0" * 40
        or result["linux_build_sha"] != expected_linux_sha
    ):
        raise ComponentReleaseProofError("component lite proof Linux build differs")
    try:
        embedded = validate_component_release(manifest)
    except Exception as exc:
        raise ComponentReleaseProofError("component lite manifest is invalid") from exc
    if embedded["linux_build_sha"] != expected_linux_sha:
        raise ComponentReleaseProofError("component lite manifest Linux build differs")
    if result["manifest_json"] != _canonical(embedded):
        raise ComponentReleaseProofError("component lite manifest encoding differs")
    try:
        signed = verify_signed_component_manifest(
            result["manifest_json"],
            result["signature"],
            result["public_key"],
            expected_linux_sha,
        )
    except Exception as exc:
        raise ComponentReleaseProofError("component lite signature differs") from exc
    if signed != embedded:
        raise ComponentReleaseProofError("component lite signed manifest differs")
    core = {
        key: value
        for key, value in result.items()
        if key != "proof_sha256"
    }
    if (
        not re.fullmatch(r"[0-9a-f]{64}", result["proof_sha256"])
        or hashlib.sha256(_canonical(core).encode("utf-8")).hexdigest()
        != result["proof_sha256"]
    ):
        raise ComponentReleaseProofError("component lite proof seal differs")
    return result


def build_component_release_lite(
    manifest: Mapping[str, object],
    key: Ed25519PrivateKey,
) -> dict[str, str]:
    checked = validate_component_release(manifest)
    if checked["scope"] != "LINUX":
        raise ComponentReleaseProofError(
            "component lite proof requires a Linux-scope manifest"
        )
    manifest_json, signature = sign_component_manifest(checked, key)
    core = {
        "schema": PROOF_SCHEMA,
        "schema_version": PROOF_SCHEMA_VERSION,
        "algorithm": "Ed25519",
        "mode": "fast",
        "scope": "LINUX",
        "linux_build_sha": checked["linux_build_sha"],
        "manifest_json": manifest_json,
        "signature": signature,
        "public_key": signing_public_key(key),
    }
    return {
        **core,
        "proof_sha256": hashlib.sha256(_canonical(core).encode("utf-8")).hexdigest(),
    }


def write_component_release_lite(
    manifest_path: str | Path,
    key: Ed25519PrivateKey,
) -> Path:
    """Atomically install the fixed lite proof beside its component manifest."""
    source = Path(manifest_path)
    if not source.is_absolute() or source.name != COMPONENT_RELEASE_FILENAME:
        raise ComponentReleaseProofError("component manifest path is not canonical")
    manifest = validate_component_release(
        json.loads(source.read_bytes())
    )
    payload = build_component_release_lite(manifest, key)
    target = source.with_name(PROOF_FILENAME)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.",
        dir=target.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write((_canonical(payload) + "\n").encode("utf-8"))
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, 0o444)
        try:
            _atomic_install_noreplace(temporary, target)
        except FileExistsError:
            validate_component_release_lite(
                _decode(target.read_bytes()),
                manifest,
                expected_linux_sha=manifest["linux_build_sha"],
            )
        if temporary.exists():
            if os.name == "nt":
                os.chmod(temporary, 0o600)
            temporary.unlink()
        if os.name == "posix":
            directory_fd = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        return target
    finally:
        if temporary.exists():
            if os.name == "nt":
                os.chmod(temporary, 0o600)
            temporary.unlink()


def _trusted_read(path: Path) -> bytes:
    try:
        for parent in reversed(path.parents):
            info = parent.lstat()
            if (
                not stat.S_ISDIR(info.st_mode)
                or info.st_uid != 0
                or info.st_mode & 0o022
            ):
                raise ComponentReleaseProofError(
                    "component lite proof parent is unsafe"
                )
        before = path.lstat()
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != 0
            or before.st_mode & 0o022
            or before.st_nlink != 1
        ):
            raise ComponentReleaseProofError("component lite proof file is unsafe")
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            opened = os.fstat(descriptor)
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_uid != 0
                or opened.st_nlink != 1
                or (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino)
            ):
                raise ComponentReleaseProofError(
                    "component lite proof identity changed"
                )
            with os.fdopen(descriptor, "rb", closefd=False) as stream:
                return stream.read(_MAX_BYTES + 1)
        finally:
            os.close(descriptor)
    except OSError as exc:
        raise ComponentReleaseProofError("component lite proof is unavailable") from exc


def load_runtime_component_release_lite(
    manifest: Mapping[str, object] | None = None,
) -> dict[str, str]:
    """Load the root-owned proof for the exact configured runtime manifest."""
    if not sys.platform.startswith("linux"):
        raise ComponentReleaseProofError("component lite proof is Linux-only")
    manifest = (
        validate_component_release(manifest)
        if manifest is not None
        else load_runtime_component_release()
    )
    configured = os.environ.get("PROBIGA_COMPONENT_RELEASE_PATH", "")
    if not configured:
        raise ComponentReleaseProofError("component release path is missing")
    manifest_path = Path(configured)
    expected = PurePosixPath(
        f"{COMPONENT_RELEASE_ROOT}/{manifest['linux_build_sha']}/"
        f"{COMPONENT_RELEASE_FILENAME}"
    )
    if not manifest_path.is_absolute() or PurePosixPath(configured) != expected:
        raise ComponentReleaseProofError("component release path is not canonical")
    payload = _decode(_trusted_read(manifest_path.with_name(PROOF_FILENAME)))
    validate_component_release_lite(
        payload,
        manifest,
        expected_linux_sha=manifest["linux_build_sha"],
    )
    return payload


__all__ = [
    "PROOF_FILENAME",
    "PROOF_SCHEMA",
    "PROOF_SCHEMA_VERSION",
    "ComponentReleaseProofError",
    "build_component_release_lite",
    "load_runtime_component_release_lite",
    "validate_component_release_lite",
    "write_component_release_lite",
]
