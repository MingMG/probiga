"""Strict, self-contained integrity validator for the V4 research release.

This proves only repository-local byte consistency.  It deliberately does not
claim a clean Git commit, a signed tag, an external immutable anchor, model
validity, or production readiness.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path, PurePosixPath
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parents[2]
RELEASE_ID = "trading_v4.1.0-research"
MANIFEST_PATH = (
    f"versions/trading_v4/releases/{RELEASE_ID}/manifest.json"
)
RUNTIME_PATH = (
    f"strategies/trading_v4/releases/{RELEASE_ID}/runtime.json"
)


class V4ReleaseIntegrityError(ValueError):
    """Raised when the local V4 release contract or owned bytes drift."""


@dataclass(frozen=True, slots=True)
class V4ReleaseIntegrity:
    document: Mapping[str, Any]
    manifest_sha256: str
    source_tree_sha256: str
    status: str = "INTERNAL_HASH_CONSISTENT_NOT_EXTERNALLY_ANCHORED"


def validate_v4_release() -> V4ReleaseIntegrity:
    manifest_bytes = _owned_file(
        MANIFEST_PATH, "versions/trading_v4/releases"
    ).read_bytes()
    document = _strict_json(manifest_bytes, "manifest")
    for field, expected in {
        "schema_version": "probiga.immutable-trading-release.v1",
        "system": "trading_v4",
        "system_version": "4.1.0-research",
        "release_id": RELEASE_ID,
        "lifecycle_status": "RESEARCH_ONLY",
        "release_decision": "BLOCK",
        "immutable": True,
        "code_commit_clean": False,
        "git_anchor_status": "UNTRACKED_DIRTY_WORKTREE_NOT_COMMIT_REPRODUCIBLE",
        "trusted_external_anchor_present": False,
        "source_coverage_required": True,
        "database_status": "DEFERRED_MIGRATION_IN_PROGRESS",
        "database_tests_run": False,
        "database_counts_as_pass": False,
        "actionable_output_allowed": False,
        "paper_orders_allowed": False,
        "real_orders_allowed": False,
        "activation_eligible": False,
        "paper_eligible": False,
        "production_eligible": False,
        "api_prefix": None,
        "task_namespace": None,
    }.items():
        _exact(document, field, expected)

    source_files = _digest_mapping(document.get("source_files"), "source_files")
    discovered = {
        path.relative_to(ROOT).as_posix()
        for base in (ROOT / "server/trading_v4", ROOT / "tools/trading_v4")
        for path in base.rglob("*.py")
        if path.is_file()
    }
    if set(source_files) != discovered:
        raise V4ReleaseIntegrityError(
            "source coverage differs "
            f"missing={sorted(discovered - set(source_files))} "
            f"extra={sorted(set(source_files) - discovered)}"
        )
    for relative_path, digest in source_files.items():
        if not relative_path.startswith(("server/trading_v4/", "tools/trading_v4/")):
            raise V4ReleaseIntegrityError("source crosses V4 ownership")
        owner = relative_path.split("/", 2)[:2]
        _verify_hash(relative_path, digest, "/".join(owner))
    tree_hash = _source_tree_hash(source_files)
    _exact(document, "source_tree_sha256", tree_hash)

    _exact(document, "config_path", RUNTIME_PATH)
    config_hash = _digest(document.get("config_sha256"), "config_sha256")
    config_files = _digest_mapping(document.get("config_files"), "config_files")
    if config_files != {RUNTIME_PATH: config_hash}:
        raise V4ReleaseIntegrityError("V4 config file set differs")
    _verify_hash(
        RUNTIME_PATH,
        config_hash,
        f"strategies/trading_v4/releases/{RELEASE_ID}",
    )
    _validate_runtime(_strict_json(_owned_file(
        RUNTIME_PATH,
        f"strategies/trading_v4/releases/{RELEASE_ID}",
    ).read_bytes(), "runtime"))

    tests_path = f"versions/trading_v4/releases/{RELEASE_ID}/tests.txt"
    _exact(document, "test_manifest_path", tests_path)
    _verify_hash(
        tests_path,
        _digest(document.get("test_manifest_sha256"), "test_manifest_sha256"),
        f"versions/trading_v4/releases/{RELEASE_ID}",
    )
    test_files = _digest_mapping(document.get("test_files"), "test_files")
    lines = _owned_file(
        tests_path, f"versions/trading_v4/releases/{RELEASE_ID}"
    ).read_text(encoding="utf-8").splitlines()
    if not lines or len(lines) != len(set(lines)) or set(lines) != set(test_files):
        raise V4ReleaseIntegrityError("tests.txt and test_files differ")
    for relative_path, digest in test_files.items():
        if not relative_path.startswith("tests/"):
            raise V4ReleaseIntegrityError("test file escapes tests ownership")
        _verify_hash(relative_path, digest, "tests")

    evidence = _mapping(document.get("historical_evidence"), "historical_evidence")
    _exact(evidence, "decision", "BLOCK")
    evidence_path = _text(evidence.get("path"), "historical evidence path")
    if not evidence_path.startswith("artifacts/trading_v4/"):
        raise V4ReleaseIntegrityError("historical evidence crosses V4 ownership")
    _verify_hash(
        evidence_path,
        _digest(evidence.get("sha256"), "historical evidence sha256"),
        "artifacts/trading_v4",
    )
    _exact(document, "parent_release_id", None)
    _exact(document, "parent_manifest_sha256", None)
    _exact(document, "entrypoint", "tools/trading_v4/run_research.py")
    if document["entrypoint"] not in source_files:
        raise V4ReleaseIntegrityError("entrypoint is not source-owned")
    return V4ReleaseIntegrity(
        document=document,
        manifest_sha256=_sha256(manifest_bytes),
        source_tree_sha256=tree_hash,
    )


def _validate_runtime(runtime: Mapping[str, Any]) -> None:
    for field, expected in {
        "schema_version": "probiga.trading-v4.release-runtime.v1",
        "system": "trading_v4",
        "system_version": "4.1.0-research",
        "release_id": RELEASE_ID,
        "lifecycle_status": "RESEARCH_ONLY",
        "release_decision": "BLOCK",
        "immutable": True,
        "activation_eligible": False,
        "paper_eligible": False,
        "production_eligible": False,
        "api_prefix": None,
        "task_namespace": None,
    }.items():
        _exact(runtime, field, expected)
    boundary = _mapping(runtime.get("execution_boundary"), "execution_boundary")
    required_false = {
        "expected_return_estimation", "probability_estimation",
        "forecast_emission", "action_emission", "execution_intent_emission",
        "paper_order_submission", "real_order_submission",
        "v2_or_v3_runtime_write",
    }
    if set(boundary) != required_false or any(
        boundary[field] is not False for field in required_false
    ):
        raise V4ReleaseIntegrityError("runtime execution boundary is not closed")
    research = _mapping(runtime.get("research_contract"), "research_contract")
    _exact(research, "decision_clock", "AFTER_CLOSE")
    _exact(research, "after_close_local_time_minimum", "15:00:00")
    _exact(research, "timestamp_offset_policy", "ASIA_SHANGHAI_PLUS_08_ONLY")
    database = _mapping(
        _mapping(runtime.get("evidence_gates"), "evidence_gates").get("database"),
        "database gate",
    )
    _exact(database, "status", "DEFERRED_MIGRATION_IN_PROGRESS")
    _exact(database, "tests_run", False)
    _exact(database, "counts_as_pass", False)


def _strict_json(payload: bytes, label: str) -> Mapping[str, Any]:
    def no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise V4ReleaseIntegrityError(f"{label} duplicate key: {key}")
            result[key] = value
        return result

    def reject_constant(value: str) -> None:
        raise V4ReleaseIntegrityError(f"{label} contains non-finite {value}")

    try:
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=no_duplicates,
            parse_constant=reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V4ReleaseIntegrityError(f"{label} is not strict UTF-8 JSON") from exc
    _reject_nonfinite(value, label)
    return _mapping(value, label)


def _reject_nonfinite(value: Any, path: str) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise V4ReleaseIntegrityError(f"{path} contains non-finite number")
    if isinstance(value, Mapping):
        for key, child in value.items():
            _reject_nonfinite(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_nonfinite(child, f"{path}[{index}]")


def _digest_mapping(value: Any, label: str) -> dict[str, str]:
    mapping = _mapping(value, label)
    result: dict[str, str] = {}
    for raw_path, raw_digest in mapping.items():
        path = _canonical_path(raw_path, f"{label} path")
        if path in result:
            raise V4ReleaseIntegrityError(f"{label} canonical path collision")
        result[path] = _digest(raw_digest, f"{label} digest")
    return result


def _verify_hash(relative_path: str, expected: str, owner: str) -> None:
    actual = _sha256(_owned_file(relative_path, owner).read_bytes())
    if actual != expected:
        raise V4ReleaseIntegrityError(f"release file hash drifted: {relative_path}")


def _owned_file(relative_path: str, owner: str) -> Path:
    path_text = _canonical_path(relative_path, "release path")
    owner_text = _canonical_path(owner, "owner path")
    lexical = ROOT / Path(*PurePosixPath(path_text).parts)
    lexical_owner = ROOT / Path(*PurePosixPath(owner_text).parts)
    _reject_reparse_points(lexical, lexical_owner)
    owner_path = lexical_owner.resolve()
    resolved = lexical.resolve()
    try:
        resolved.relative_to(owner_path)
    except ValueError as exc:
        raise V4ReleaseIntegrityError(f"path escapes owner: {relative_path}") from exc
    if not resolved.is_file():
        raise V4ReleaseIntegrityError(f"release file is missing: {relative_path}")
    return resolved


def _reject_reparse_points(path: Path, owner_path: Path) -> None:
    current = path
    while True:
        try:
            metadata = current.stat(follow_symlinks=False)
        except OSError as exc:
            raise V4ReleaseIntegrityError(
                f"cannot inspect release path metadata: {current}"
            ) from exc
        if current.is_symlink() or (
            getattr(metadata, "st_file_attributes", 0) & 0x400
        ):
            raise V4ReleaseIntegrityError(
                f"release path cannot be a symlink/reparse point: {current}"
            )
        if current == owner_path:
            return
        if owner_path not in current.parents:
            raise V4ReleaseIntegrityError("release owner parent chain differs")
        current = current.parent


def _canonical_path(value: Any, label: str) -> str:
    text = _text(value, label)
    path = PurePosixPath(text)
    canonical = path.as_posix()
    if (
        canonical != text
        or path.is_absolute()
        or "\\" in text
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise V4ReleaseIntegrityError(f"{label} must be canonical and relative")
    return canonical


def _source_tree_hash(files: Mapping[str, str]) -> str:
    payload = "".join(
        f"{path}\0{digest}\n" for path, digest in sorted(files.items())
    ).encode("utf-8")
    return _sha256(payload)


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise V4ReleaseIntegrityError(f"{label} must be an object")
    return value


def _text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise V4ReleaseIntegrityError(f"{label} must be canonical text")
    return value


def _digest(value: Any, label: str) -> str:
    text = _text(value, label)
    if len(text) != 64 or any(char not in "0123456789abcdef" for char in text):
        raise V4ReleaseIntegrityError(f"{label} must be lowercase SHA-256")
    return text


def _exact(mapping: Mapping[str, Any], field: str, expected: Any) -> None:
    if field not in mapping or type(mapping[field]) is not type(expected) or mapping[field] != expected:
        raise V4ReleaseIntegrityError(f"{field} must equal {expected!r}")


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


__all__ = [
    "V4ReleaseIntegrity",
    "V4ReleaseIntegrityError",
    "validate_v4_release",
]
