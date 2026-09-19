"""Sealed component identities for independently published Linux releases.

The publisher proves the contract-tree digest before sealing this file. Runtime
code verifies that root-owned seal, not the Git trees or database schema.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import sys
import tempfile
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

COMPONENT_RELEASE_SCHEMA = "probiga.component-release.v1"
COMPONENT_RELEASE_FILENAME = "component-release.json"
COMPONENT_RELEASE_ROOT = "/var/lib/probiga/release-artifacts"
_SHA40 = re.compile(r"[0-9a-f]{40}\Z")
_SHA64 = re.compile(r"[0-9a-f]{64}\Z")
_FIELDS = frozenset({
    "schema", "linux_build_sha", "windows_build_sha", "contract_build_sha",
    "contract_sha256", "parent_linux_build_sha", "scope", "created_at",
    "manifest_sha256",
})
_MAX_BYTES = 16384
_BUILD_ENV = (
    "PROBIGA_BUILD_COMMIT_SHA", "PROBIGA_EXPECTED_GIT_SHA", "EXPECTED_GIT_SHA",
)


class ComponentReleaseError(RuntimeError):
    """The component identity cannot be established from trusted evidence."""


def _canonical(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _sha40(value: object, field: str) -> str:
    if not isinstance(value, str) or not _SHA40.fullmatch(value) or value == "0" * 40:
        raise ComponentReleaseError(f"component release {field} must be a nonzero lowercase SHA40")
    return value


def validate_component_release(payload: Mapping[str, object]) -> dict[str, str]:
    """Validate exact fields and a canonical self-seal; never coerce identities."""
    if not isinstance(payload, Mapping) or set(payload) != _FIELDS:
        raise ComponentReleaseError("component release fields differ")
    if any(not isinstance(value, str) for value in payload.values()):
        raise ComponentReleaseError("component release values must be strings")
    result = dict(payload)
    if result["schema"] != COMPONENT_RELEASE_SCHEMA:
        raise ComponentReleaseError("unsupported component release schema")
    for field in ("linux_build_sha", "windows_build_sha", "contract_build_sha", "parent_linux_build_sha"):
        _sha40(result[field], field)
    if not _SHA64.fullmatch(result["contract_sha256"]):
        raise ComponentReleaseError("component release contract_sha256 is invalid")
    if result["scope"] not in {"COORDINATED", "LINUX"}:
        raise ComponentReleaseError("component release scope is invalid")
    if result["windows_build_sha"] != result["contract_build_sha"]:
        raise ComponentReleaseError("Windows build must equal the coordinated contract build")
    if result["scope"] == "COORDINATED" and result["linux_build_sha"] != result["contract_build_sha"]:
        raise ComponentReleaseError("coordinated component builds must be identical")
    try:
        created = datetime.fromisoformat(result["created_at"].replace("Z", "+00:00"))
    except ValueError as exc:
        raise ComponentReleaseError("component release created_at is invalid") from exc
    if created.tzinfo is None or created.utcoffset() is None:
        raise ComponentReleaseError("component release created_at must include timezone")
    core = {key: value for key, value in result.items() if key != "manifest_sha256"}
    if not _SHA64.fullmatch(result["manifest_sha256"]) or hashlib.sha256(_canonical(core)).hexdigest() != result["manifest_sha256"]:
        raise ComponentReleaseError("component release seal is invalid")
    return result


def build_component_release(
    *, linux_build_sha: str, windows_build_sha: str, contract_build_sha: str,
    contract_sha256: str, parent_linux_build_sha: str, scope: str,
    created_at: str | None = None,
) -> dict[str, str]:
    core = {
        "schema": COMPONENT_RELEASE_SCHEMA,
        "linux_build_sha": linux_build_sha,
        "windows_build_sha": windows_build_sha,
        "contract_build_sha": contract_build_sha,
        "contract_sha256": contract_sha256,
        "parent_linux_build_sha": parent_linux_build_sha,
        "scope": scope,
        "created_at": created_at if created_at is not None else datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    return validate_component_release({**core, "manifest_sha256": hashlib.sha256(_canonical(core)).hexdigest()})


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ComponentReleaseError(f"duplicate component release key: {key}")
        result[key] = value
    return result


def _decode(encoded: bytes) -> dict[str, str]:
    if len(encoded) > _MAX_BYTES:
        raise ComponentReleaseError("component release exceeds size limit")
    try:
        payload = json.loads(encoded.decode("utf-8"), object_pairs_hook=_unique_object)
    except (ValueError, UnicodeError) as exc:
        raise ComponentReleaseError("component release is not valid UTF-8 JSON") from exc
    return validate_component_release(payload)


def _read_regular(path: Path) -> dict[str, str]:
    descriptor = None
    try:
        before = path.lstat()
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            raise ComponentReleaseError("component release must be a regular single-link file")
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_BINARY", 0))
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1 or (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino):
            raise ComponentReleaseError("component release file identity changed")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = None
            return _decode(handle.read(_MAX_BYTES + 1))
    except OSError as exc:
        raise ComponentReleaseError("component release is unreadable") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _fsync_directory(directory: Path) -> None:
    if os.name != "posix":
        return
    descriptor = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _linux_rename_noreplace(source: Path, target: Path) -> None:
    """One atomic publication, without a hardlink or overwrite crash window."""
    import ctypes
    try:
        operation = ctypes.CDLL(None, use_errno=True).renameat2
    except (AttributeError, OSError):
        raise ComponentReleaseError("atomic no-replace rename is unavailable") from None
    operation.argtypes = (ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint)
    operation.restype = ctypes.c_int
    if operation(-100, os.fsencode(source), -100, os.fsencode(target), 1) != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), os.fspath(target))


def _atomic_install_noreplace(source: Path, target: Path) -> None:
    if os.name == "nt":
        os.rename(source, target)
    elif sys.platform.startswith("linux"):
        _linux_rename_noreplace(source, target)
    else:
        raise ComponentReleaseError("atomic no-replace installation is unsupported")


def write_component_release(path: str | Path, payload: Mapping[str, object]) -> Path:
    """Atomically install one immutable identity without replacing existing data.

    Output may be a staging file. Only load_runtime_component_release establishes
    the production trust seal, including ownership of every parent directory.
    """
    normalized = validate_component_release(payload)
    target = Path(path)
    if not target.is_absolute():
        raise ComponentReleaseError("component release output path must be absolute")
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() or target.is_symlink():
        if _read_regular(target) != normalized:
            raise ComponentReleaseError("existing component release is immutable and differs")
        return target
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{target.name}.", dir=target.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(_canonical(normalized) + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o444)
        try:
            _atomic_install_noreplace(temporary, target)
        except FileExistsError:
            if _read_regular(target) != normalized:
                raise ComponentReleaseError("existing component release is immutable and differs")
        if temporary.exists():
            if os.name == "nt":
                os.chmod(temporary, 0o600)
            temporary.unlink()
        _fsync_directory(target.parent)
        return target
    finally:
        if temporary.exists():
            if os.name == "nt":
                os.chmod(temporary, 0o600)
            temporary.unlink()


def _configured_build_sha(*, required: bool) -> str:
    configured = []
    for name in _BUILD_ENV:
        if name in os.environ:
            configured.append(_sha40(os.environ[name], name))
    if len(set(configured)) > 1:
        raise ComponentReleaseError("configured runtime build identities disagree")
    if not configured and required:
        raise ComponentReleaseError("configured runtime build identity is required")
    return configured[0] if configured else ""


def _trusted_read(path: Path) -> dict[str, str]:
    # Check lexical ancestors without resolve(): resolve would hide symlinks.
    try:
        for parent in reversed(path.parents):
            info = parent.lstat()
            if not stat.S_ISDIR(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
                raise ComponentReleaseError("component release parent must be a root-owned nonwritable directory")
        info = path.lstat()
        if info.st_uid != 0 or info.st_mode & 0o022:
            raise ComponentReleaseError("component release must be root-owned and not group/world-writable")
    except OSError as exc:
        raise ComponentReleaseError("trusted component release is unavailable") from exc
    return _read_regular(path)


def load_runtime_component_release() -> dict[str, str]:
    """Read a root-sealed Linux manifest from its fixed production location."""
    if not sys.platform.startswith("linux"):
        raise ComponentReleaseError("Linux component release manifests are Linux-only")
    actual = _configured_build_sha(required=True)
    configured = os.environ.get("PROBIGA_COMPONENT_RELEASE_PATH", "")
    expected = f"{COMPONENT_RELEASE_ROOT}/{actual}/{COMPONENT_RELEASE_FILENAME}"
    # Exact lexical equality excludes traversal, aliases and alternate roots.
    if not configured or configured != expected or not PurePosixPath(configured).is_absolute():
        raise ComponentReleaseError("component release path must be the fixed path for this Linux build")
    manifest = _trusted_read(Path(configured))
    if manifest["linux_build_sha"] != actual:
        raise ComponentReleaseError("component release Linux build differs from runtime build")
    return manifest


def runtime_component_build_sha(role: str, expected_build_sha: str | None = None) -> str:
    """Resolve a component identity without changing the actual code identity."""
    if role not in {"linux", "windows", "contract"}:
        raise ComponentReleaseError("component role must be linux, windows or contract")
    production = os.environ.get("PROBIGA_DEPLOYMENT_MODE", "").strip().lower() == "production"
    actual = _configured_build_sha(required=production)
    expected = _sha40(expected_build_sha, "expected_build_sha") if expected_build_sha is not None else ""
    if expected and actual and expected != actual:
        raise ComponentReleaseError("expected build differs from actual runtime build")
    if not production:
        return expected or actual
    edge = os.environ.get("PROBIGA_SCHEDULER_EXECUTOR_ROLE", "").strip().lower() == "qmt_windows_edge"
    if sys.platform == "win32":
        if not edge:
            raise ComponentReleaseError("production Windows must use the QMT edge role")
        if role == "linux":
            raise ComponentReleaseError("Windows cannot establish the Linux component build")
        return actual
    if not sys.platform.startswith("linux") or edge:
        raise ComponentReleaseError("unsupported production component runtime")
    manifest = load_runtime_component_release()
    return manifest[f"{role}_build_sha"]


def runtime_contract_build_sha(expected_build_sha: str | None = None) -> str:
    return runtime_component_build_sha("contract", expected_build_sha)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    builder = commands.add_parser("build")
    for field in ("linux-build-sha", "windows-build-sha", "contract-build-sha", "contract-sha256", "parent-linux-build-sha"):
        builder.add_argument(f"--{field}", required=True)
    builder.add_argument("--scope", required=True, choices=("COORDINATED", "LINUX"))
    builder.add_argument("--created-at")
    builder.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    fields = vars(args).copy()
    fields.pop("command")
    output = fields.pop("output")
    try:
        print(write_component_release(output, build_component_release(**fields)))
    except (ComponentReleaseError, OSError) as exc:
        parser.exit(1, f"component release: {exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
