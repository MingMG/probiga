"""Deterministic, explicit evidence manifests for a frozen V3 baseline.

This module deliberately never imports the V3 runtime and never discovers
files by walking the repository.  Callers must name every evidence file.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import unicodedata
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath, PureWindowsPath
from types import MappingProxyType
from typing import Any


V3_BASELINE_MANIFEST_SCHEMA = "probiga.v3-baseline-evidence.v1"
REQUIRED_EVIDENCE_TYPES = frozenset(
    {
        "code",
        "config",
        "model",
        "calibration",
        "task",
        "schema",
    }
)

_EVIDENCE_TYPE_PATTERN = re.compile(r"^[a-z][a-z0-9_-]*$")
_HEX_DIGITS = frozenset("0123456789abcdef")
_FILE_HASH_CHUNK_SIZE = 1024 * 1024


class V3BaselineManifestError(ValueError):
    """Raised when V3 baseline evidence is incomplete or inconsistent."""


def _required_text(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise V3BaselineManifestError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise V3BaselineManifestError(f"{field_name} must not be empty")
    return normalized


def _sha256(value: Any, field_name: str) -> str:
    normalized = _required_text(value, field_name).lower()
    if len(normalized) != 64 or any(
        character not in _HEX_DIGITS for character in normalized
    ):
        raise V3BaselineManifestError(
            f"{field_name} must be a 64-character SHA-256 digest"
        )
    return normalized


def _git_commit(value: Any, field_name: str = "git_commit") -> str:
    normalized = _required_text(value, field_name).lower()
    if len(normalized) not in {40, 64} or any(
        character not in _HEX_DIGITS for character in normalized
    ):
        raise V3BaselineManifestError(
            f"{field_name} must be a full 40- or 64-character Git commit"
        )
    return normalized


def _evidence_type(value: Any, field_name: str) -> str:
    normalized = _required_text(value, field_name).casefold()
    if _EVIDENCE_TYPE_PATTERN.fullmatch(normalized) is None:
        raise V3BaselineManifestError(
            f"{field_name} must match {_EVIDENCE_TYPE_PATTERN.pattern}"
        )
    return normalized


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise V3BaselineManifestError(
            "manifest must be strictly JSON serializable"
        ) from exc


def _hash_canonical(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _canonical_manifest_path(value: Any, field_name: str = "path") -> str:
    raw = _required_text(value, field_name)
    if "\x00" in raw:
        raise V3BaselineManifestError(f"{field_name} contains a NUL byte")
    if "\\" in raw:
        raise V3BaselineManifestError(
            f"{field_name} must use canonical forward slashes"
        )
    normalized_unicode = unicodedata.normalize("NFC", raw)
    path = PurePosixPath(normalized_unicode)
    if (
        path.is_absolute()
        or PureWindowsPath(normalized_unicode).drive
        or not path.parts
    ):
        raise V3BaselineManifestError(f"{field_name} must be relative")
    if any(part in {"", ".", ".."} for part in path.parts):
        raise V3BaselineManifestError(
            f"{field_name} is not a canonical repository-relative path"
        )
    canonical = path.as_posix()
    if canonical != normalized_unicode:
        raise V3BaselineManifestError(
            f"{field_name} is not a canonical repository-relative path"
        )
    return canonical


def _path_identity(value: str) -> str:
    return unicodedata.normalize("NFC", value).casefold()


def _repository_root(repo_root: str | Path) -> Path:
    try:
        root = Path(repo_root).resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise V3BaselineManifestError(
            f"repository root does not exist: {repo_root}"
        ) from exc
    if not root.is_dir():
        raise V3BaselineManifestError(
            f"repository root is not a directory: {repo_root}"
        )
    return root


def _run_git(repo_root: Path) -> tuple[Path, str]:
    try:
        completed = subprocess.run(
            [
                "git",
                "-C",
                str(repo_root),
                "rev-parse",
                "--show-toplevel",
                "HEAD^{commit}",
            ],
            check=False,
            capture_output=True,
            encoding="utf-8",
            errors="strict",
            shell=False,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError, UnicodeError) as exc:
        raise V3BaselineManifestError(
            "unable to resolve repository Git commit"
        ) from exc
    if completed.returncode != 0:
        detail = completed.stderr.strip() or "git rev-parse failed"
        raise V3BaselineManifestError(
            f"unable to resolve repository Git commit: {detail}"
        )
    lines = tuple(
        line.strip() for line in completed.stdout.splitlines() if line.strip()
    )
    if len(lines) != 2:
        raise V3BaselineManifestError(
            "git rev-parse returned an unexpected result"
        )
    try:
        top_level = Path(lines[0]).resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise V3BaselineManifestError(
            "git top-level path does not exist"
        ) from exc
    return top_level, _git_commit(lines[1])


def resolve_repository_commit(repo_root: str | Path) -> str:
    """Return the full HEAD commit and require ``repo_root`` to be top-level."""

    root = _repository_root(repo_root)
    top_level, commit = _run_git(root)
    try:
        same_root = os.path.samefile(root, top_level)
    except OSError:
        same_root = os.path.normcase(str(root)) == os.path.normcase(str(top_level))
    if not same_root:
        raise V3BaselineManifestError(
            "repo_root must be the Git repository top-level directory"
        )
    return commit


def _resolve_explicit_file(
    repo_root: Path,
    relative_path: str | Path,
) -> tuple[Path, str]:
    raw_value = (
        os.fspath(relative_path)
        if isinstance(relative_path, os.PathLike)
        else relative_path
    )
    raw = _required_text(raw_value, "include path")
    if any(character in raw for character in "*?[]"):
        raise V3BaselineManifestError(
            f"include path must not contain glob wildcards: {raw}"
        )
    windows_path = PureWindowsPath(raw)
    if (
        PurePosixPath(raw.replace("\\", "/")).is_absolute()
        or windows_path.is_absolute()
        or windows_path.drive
    ):
        raise V3BaselineManifestError(f"include path must be relative: {raw}")
    try:
        candidate = (repo_root / Path(raw)).resolve(strict=True)
    except FileNotFoundError as exc:
        raise V3BaselineManifestError(
            f"included evidence file does not exist: {raw}"
        ) from exc
    except (OSError, RuntimeError) as exc:
        raise V3BaselineManifestError(
            f"unable to resolve included evidence file: {raw}"
        ) from exc
    try:
        relative = candidate.relative_to(repo_root)
    except ValueError as exc:
        raise V3BaselineManifestError(
            f"included evidence path escapes repository root: {raw}"
        ) from exc
    if not candidate.is_file():
        raise V3BaselineManifestError(
            f"included evidence path must name one explicit file: {raw}"
        )
    canonical = _canonical_manifest_path(relative.as_posix(), "include path")
    return candidate, canonical


def _tracked_blob_oid(
    repo_root: Path,
    *,
    git_commit: str,
    canonical_path: str,
) -> str:
    """Return the exact blob at ``git_commit`` or reject untracked evidence."""

    try:
        completed = subprocess.run(
            [
                "git",
                "-C",
                str(repo_root),
                "ls-tree",
                "-z",
                "--full-tree",
                git_commit,
                "--",
                canonical_path,
            ],
            check=False,
            capture_output=True,
            encoding="utf-8",
            errors="strict",
            shell=False,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError, UnicodeError) as exc:
        raise V3BaselineManifestError(
            f"unable to resolve Git blob for evidence file: {canonical_path}"
        ) from exc
    if completed.returncode != 0:
        detail = completed.stderr.strip() or "git ls-tree failed"
        raise V3BaselineManifestError(
            "unable to resolve Git blob for evidence file "
            f"{canonical_path}: {detail}"
        )

    entries = tuple(item for item in completed.stdout.split("\x00") if item)
    if len(entries) != 1 or "\t" not in entries[0]:
        raise V3BaselineManifestError(
            "evidence file is not tracked at Git commit "
            f"{git_commit}: {canonical_path}"
        )
    metadata, tracked_path = entries[0].split("\t", 1)
    parts = metadata.split()
    if (
        len(parts) != 3
        or parts[1] != "blob"
        or tracked_path != canonical_path
    ):
        raise V3BaselineManifestError(
            "evidence file is not an exact tracked blob at Git commit "
            f"{git_commit}: {canonical_path}"
        )
    object_id = parts[2].lower()
    expected_length = len(git_commit)
    if len(object_id) != expected_length or any(
        character not in _HEX_DIGITS for character in object_id
    ):
        raise V3BaselineManifestError(
            f"Git returned an invalid blob identity for: {canonical_path}"
        )
    return object_id


def _hash_file(
    path: Path,
    *,
    git_object_hash_name: str,
) -> tuple[str, int, str]:
    """Hash one stable read as both manifest SHA-256 and a Git blob."""

    try:
        before = path.stat()
        digest = hashlib.sha256()
        git_digest = hashlib.new(git_object_hash_name)
        git_digest.update(f"blob {before.st_size}\0".encode("ascii"))
        size = 0
        with path.open("rb") as stream:
            while True:
                chunk = stream.read(_FILE_HASH_CHUNK_SIZE)
                if not chunk:
                    break
                digest.update(chunk)
                git_digest.update(chunk)
                size += len(chunk)
        after = path.stat()
    except (OSError, ValueError) as exc:
        raise V3BaselineManifestError(
            f"unable to read included evidence file: {path}"
        ) from exc
    if (
        before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
        or size != after.st_size
    ):
        raise V3BaselineManifestError(
            f"included evidence file changed while hashing: {path}"
        )
    return digest.hexdigest(), size, git_digest.hexdigest()


def _hash_git_bound_evidence(
    repo_root: Path,
    *,
    absolute_path: Path,
    canonical_path: str,
    git_commit: str,
) -> tuple[str, int]:
    expected_blob = _tracked_blob_oid(
        repo_root,
        git_commit=git_commit,
        canonical_path=canonical_path,
    )
    hash_name = "sha1" if len(git_commit) == 40 else "sha256"
    digest, size, working_blob = _hash_file(
        absolute_path,
        git_object_hash_name=hash_name,
    )
    if working_blob != expected_blob:
        raise V3BaselineManifestError(
            "evidence file bytes do not match Git commit blob: "
            f"{canonical_path}"
        )
    return digest, size


@dataclass(frozen=True, slots=True)
class V3BaselineEvidenceFile:
    evidence_type: str
    path: str
    sha256: str
    size_bytes: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "evidence_type",
            _evidence_type(self.evidence_type, "evidence_type"),
        )
        object.__setattr__(
            self,
            "path",
            _canonical_manifest_path(self.path),
        )
        object.__setattr__(self, "sha256", _sha256(self.sha256, "sha256"))
        if not isinstance(self.size_bytes, int) or isinstance(self.size_bytes, bool):
            raise V3BaselineManifestError("size_bytes must be an integer")
        if self.size_bytes < 0:
            raise V3BaselineManifestError("size_bytes must be non-negative")

    def as_dict(self) -> dict[str, Any]:
        return {
            "evidence_type": self.evidence_type,
            "path": self.path,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> V3BaselineEvidenceFile:
        if not isinstance(value, Mapping):
            raise V3BaselineManifestError("evidence file must be a mapping")
        expected = {"evidence_type", "path", "sha256", "size_bytes"}
        if set(value) != expected:
            raise V3BaselineManifestError(
                "evidence file fields do not match the schema"
            )
        return cls(**dict(value))


@dataclass(frozen=True, slots=True)
class V3BaselineEvidenceManifest:
    git_commit: str
    files: tuple[V3BaselineEvidenceFile, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "git_commit", _git_commit(self.git_commit))
        try:
            files = tuple(self.files)
        except TypeError as exc:
            raise V3BaselineManifestError("files must be iterable") from exc
        for index, item in enumerate(files):
            if type(item) is not V3BaselineEvidenceFile:
                raise V3BaselineManifestError(
                    f"files[{index}] must be exactly V3BaselineEvidenceFile"
                )
        ordered = tuple(sorted(files, key=lambda item: (item.evidence_type, item.path)))
        path_identities = [_path_identity(item.path) for item in ordered]
        if len(path_identities) != len(set(path_identities)):
            raise V3BaselineManifestError(
                "manifest contains duplicate normalized evidence paths"
            )
        present_types = {item.evidence_type for item in ordered}
        missing = REQUIRED_EVIDENCE_TYPES - present_types
        if missing:
            raise V3BaselineManifestError(
                "manifest is missing required evidence types: "
                f"{tuple(sorted(missing))}"
            )
        object.__setattr__(self, "files", ordered)

    @property
    def evidence_hashes(self) -> Mapping[str, str]:
        evidence_types = sorted({item.evidence_type for item in self.files})
        hashes = {
            evidence_type: _hash_canonical(
                {
                    "evidence_type": evidence_type,
                    "files": [
                        item.as_dict()
                        for item in self.files
                        if item.evidence_type == evidence_type
                    ],
                }
            )
            for evidence_type in evidence_types
        }
        return MappingProxyType(hashes)

    @property
    def schema_checksum(self) -> str:
        return self.evidence_hashes["schema"]

    def _hash_payload(self) -> dict[str, Any]:
        return {
            "schema_version": V3_BASELINE_MANIFEST_SCHEMA,
            "git_commit": self.git_commit,
            "files": [item.as_dict() for item in self.files],
            "evidence_hashes": dict(self.evidence_hashes),
            "schema_checksum": self.schema_checksum,
        }

    @property
    def manifest_hash(self) -> str:
        return _hash_canonical(self._hash_payload())

    def as_dict(self) -> dict[str, Any]:
        return {**self._hash_payload(), "manifest_hash": self.manifest_hash}

    def canonical_json(self) -> str:
        return _canonical_json(self.as_dict())

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> V3BaselineEvidenceManifest:
        if not isinstance(value, Mapping):
            raise V3BaselineManifestError("manifest must be a mapping")
        expected = {
            "schema_version",
            "git_commit",
            "files",
            "evidence_hashes",
            "schema_checksum",
            "manifest_hash",
        }
        if set(value) != expected:
            raise V3BaselineManifestError(
                "manifest fields do not match the schema"
            )
        if value["schema_version"] != V3_BASELINE_MANIFEST_SCHEMA:
            raise V3BaselineManifestError(
                f"unsupported manifest schema: {value['schema_version']}"
            )
        raw_files = value["files"]
        if not isinstance(raw_files, Sequence) or isinstance(
            raw_files,
            (str, bytes, bytearray),
        ):
            raise V3BaselineManifestError("files must be an array")
        manifest = cls(
            git_commit=value["git_commit"],
            files=tuple(
                V3BaselineEvidenceFile.from_mapping(item) for item in raw_files
            ),
        )
        declared_hashes = value["evidence_hashes"]
        if not isinstance(declared_hashes, Mapping):
            raise V3BaselineManifestError("evidence_hashes must be a mapping")
        normalized_hashes: dict[str, str] = {}
        for key, item in declared_hashes.items():
            evidence_type = _evidence_type(key, "evidence_hashes key")
            if evidence_type in normalized_hashes:
                raise V3BaselineManifestError(
                    "evidence_hashes contains duplicate evidence types "
                    "after normalization"
                )
            normalized_hashes[evidence_type] = _sha256(
                item,
                "evidence_hashes value",
            )
        if normalized_hashes != dict(manifest.evidence_hashes):
            raise V3BaselineManifestError("evidence hashes do not match files")
        if _sha256(value["schema_checksum"], "schema_checksum") != (
            manifest.schema_checksum
        ):
            raise V3BaselineManifestError("schema checksum does not match files")
        if _sha256(value["manifest_hash"], "manifest_hash") != (
            manifest.manifest_hash
        ):
            raise V3BaselineManifestError("manifest hash does not match content")
        return manifest


def _normalize_include_spec(
    include_files: Mapping[str, Iterable[str | Path]],
) -> tuple[tuple[str, tuple[str | Path, ...]], ...]:
    if not isinstance(include_files, Mapping):
        raise V3BaselineManifestError("include_files must be a mapping")
    normalized: list[tuple[str, tuple[str | Path, ...]]] = []
    seen_types: set[str] = set()
    for raw_type, raw_paths in include_files.items():
        evidence_type = _evidence_type(raw_type, "include_files key")
        if evidence_type in seen_types:
            raise V3BaselineManifestError(
                "include_files contains duplicate evidence types after normalization"
            )
        seen_types.add(evidence_type)
        if isinstance(raw_paths, (str, bytes, bytearray, os.PathLike)):
            raise V3BaselineManifestError(
                f"include_files[{evidence_type!r}] must be an explicit file list"
            )
        try:
            paths = tuple(raw_paths)
        except TypeError as exc:
            raise V3BaselineManifestError(
                f"include_files[{evidence_type!r}] must be iterable"
            ) from exc
        if not paths:
            raise V3BaselineManifestError(
                f"include_files[{evidence_type!r}] must not be empty"
            )
        normalized.append((evidence_type, paths))
    missing = REQUIRED_EVIDENCE_TYPES - seen_types
    if missing:
        raise V3BaselineManifestError(
            "include_files is missing required evidence types: "
            f"{tuple(sorted(missing))}"
        )
    return tuple(sorted(normalized, key=lambda item: item[0]))


def build_v3_baseline_manifest(
    repo_root: str | Path,
    include_files: Mapping[str, Iterable[str | Path]],
    *,
    expected_git_commit: str | None = None,
) -> V3BaselineEvidenceManifest:
    """Hash exactly the requested files and bind them to the repository HEAD."""

    root = _repository_root(repo_root)
    actual_commit = resolve_repository_commit(root)
    if expected_git_commit is not None and _git_commit(expected_git_commit) != (
        actual_commit
    ):
        raise V3BaselineManifestError(
            "repository HEAD does not match expected_git_commit"
        )
    entries: list[V3BaselineEvidenceFile] = []
    seen_paths: set[str] = set()
    for evidence_type, paths in _normalize_include_spec(include_files):
        for relative_path in paths:
            absolute, canonical = _resolve_explicit_file(root, relative_path)
            identity = _path_identity(canonical)
            if identity in seen_paths:
                raise V3BaselineManifestError(
                    "include_files contains duplicate normalized evidence paths"
                )
            seen_paths.add(identity)
            digest, size = _hash_git_bound_evidence(
                root,
                absolute_path=absolute,
                canonical_path=canonical,
                git_commit=actual_commit,
            )
            entries.append(
                V3BaselineEvidenceFile(
                    evidence_type=evidence_type,
                    path=canonical,
                    sha256=digest,
                    size_bytes=size,
                )
            )
    return V3BaselineEvidenceManifest(
        git_commit=actual_commit,
        files=tuple(entries),
    )


def verify_v3_baseline_manifest(
    repo_root: str | Path,
    manifest: V3BaselineEvidenceManifest,
    *,
    expected_manifest_hash: str,
    expected_git_commit: str | None = None,
) -> V3BaselineEvidenceManifest:
    """Verify trusted manifest identity, repository HEAD and every file byte."""

    if type(manifest) is not V3BaselineEvidenceManifest:
        raise V3BaselineManifestError(
            "manifest must be exactly V3BaselineEvidenceManifest"
        )
    trusted_hash = _sha256(expected_manifest_hash, "expected_manifest_hash")
    if manifest.manifest_hash != trusted_hash:
        raise V3BaselineManifestError(
            "manifest does not match expected_manifest_hash"
        )
    root = _repository_root(repo_root)
    actual_commit = resolve_repository_commit(root)
    if actual_commit != manifest.git_commit:
        raise V3BaselineManifestError(
            "repository HEAD does not match manifest git_commit"
        )
    if expected_git_commit is not None and actual_commit != _git_commit(
        expected_git_commit
    ):
        raise V3BaselineManifestError(
            "repository HEAD does not match expected_git_commit"
        )
    for entry in manifest.files:
        absolute, canonical = _resolve_explicit_file(root, entry.path)
        if canonical != entry.path:
            raise V3BaselineManifestError(
                f"manifest path is not canonical for repository: {entry.path}"
            )
        digest, size = _hash_git_bound_evidence(
            root,
            absolute_path=absolute,
            canonical_path=canonical,
            git_commit=actual_commit,
        )
        if digest != entry.sha256 or size != entry.size_bytes:
            raise V3BaselineManifestError(
                f"evidence file hash or size changed: {entry.path}"
            )
    return manifest


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise V3BaselineManifestError(
                f"manifest JSON contains duplicate key: {key}"
            )
        result[key] = value
    return result


def _reject_non_finite_json_number(value: str) -> None:
    raise V3BaselineManifestError(
        f"manifest JSON contains non-finite value: {value}"
    )


def load_v3_baseline_manifest(
    manifest_path: str | Path,
    *,
    expected_manifest_hash: str,
) -> V3BaselineEvidenceManifest:
    """Read strict JSON and require an externally trusted manifest hash."""

    try:
        raw = Path(manifest_path).read_text(encoding="utf-8")
    except OSError as exc:
        raise V3BaselineManifestError(
            f"unable to read manifest file: {manifest_path}"
        ) from exc
    try:
        payload = json.loads(
            raw,
            object_pairs_hook=_reject_duplicate_json_keys,
            parse_constant=_reject_non_finite_json_number,
        )
    except V3BaselineManifestError:
        raise
    except (TypeError, ValueError) as exc:
        raise V3BaselineManifestError("manifest file is not strict JSON") from exc
    manifest = V3BaselineEvidenceManifest.from_mapping(payload)
    trusted_hash = _sha256(expected_manifest_hash, "expected_manifest_hash")
    if manifest.manifest_hash != trusted_hash:
        raise V3BaselineManifestError(
            "manifest does not match expected_manifest_hash"
        )
    return manifest
