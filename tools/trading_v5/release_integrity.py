"""Self-contained internal hash validator for the frozen V5 release.

This proves repository-local byte consistency only.  It deliberately does not
claim a signed Git tag, clean commit, or trusted external immutable anchor.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path, PurePosixPath
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parents[2]
RELEASE_ID = "trading_v5.0.0-research"
MANIFEST_RELATIVE_PATH = (
    "versions/trading_v5/releases/"
    f"{RELEASE_ID}/manifest.json"
)
RUNTIME_RELATIVE_PATH = (
    "strategies/trading_v5/releases/"
    f"{RELEASE_ID}/runtime.json"
)
ARTIFACT_NAMESPACE = f"artifacts/trading_v5/releases/{RELEASE_ID}"
SOURCE_PREFIXES = ("server/trading_v5/", "tools/trading_v5/")


class V5ReleaseIntegrityError(ValueError):
    """Raised when any byte-owned V5 release contract drifts."""


@dataclass(frozen=True, slots=True)
class V5ReleaseIntegrity:
    document: Mapping[str, Any]
    manifest_sha256: str
    source_tree_sha256: str
    status: str = "INTERNAL_HASH_CONSISTENT_NOT_EXTERNALLY_ANCHORED"


def validate_v5_release() -> V5ReleaseIntegrity:
    manifest_path = _owned_file(
        MANIFEST_RELATIVE_PATH,
        "versions/trading_v5/releases",
    )
    manifest_bytes = manifest_path.read_bytes()
    document = _strict_json(manifest_bytes, "manifest")
    _exact(document, "schema_version", "probiga.immutable-trading-release.v1")
    _exact(document, "system", "trading_v5")
    _exact(document, "system_version", "5.0.0-research")
    _exact(document, "release_id", RELEASE_ID)
    _exact(document, "lifecycle_status", "RESEARCH_ONLY")
    _exact(document, "release_decision", "BLOCK")
    _exact(document, "immutable", True)
    _exact(document, "source_coverage_required", True)
    _exact(document, "trusted_external_anchor_present", False)
    _exact(document, "code_commit_clean", False)
    _exact(
        document,
        "git_anchor_status",
        "UNTRACKED_DIRTY_WORKTREE_NOT_COMMIT_REPRODUCIBLE",
    )
    _exact(
        document,
        "immutability_enforcement",
        "HASH_VERIFICATION_AND_APPEND_ONLY_POLICY",
    )
    _exact(document, "source_hashes_are_authoritative", True)
    for field in (
        "activation_eligible",
        "paper_eligible",
        "production_eligible",
    ):
        _exact(document, field, False)

    source_files = _digest_mapping(document.get("source_files"), "source_files")
    if not source_files:
        raise V5ReleaseIntegrityError("source_files must not be empty")
    discovered = {
        path.relative_to(ROOT).as_posix()
        for relative_root in (ROOT / "server/trading_v5", ROOT / "tools/trading_v5")
        for path in relative_root.rglob("*.py")
        if path.is_file()
    }
    if set(source_files) != discovered:
        raise V5ReleaseIntegrityError(
            "source file coverage differs "
            f"missing={sorted(discovered - set(source_files))} "
            f"extra={sorted(set(source_files) - discovered)}"
        )
    for relative_path, digest in source_files.items():
        prefix = next(
            (item for item in SOURCE_PREFIXES if relative_path.startswith(item)),
            None,
        )
        if prefix is None:
            raise V5ReleaseIntegrityError("source path crosses V5 ownership")
        _verify_owned_hash(relative_path, digest, prefix.rstrip("/"))
    tree_hash = _source_tree_hash(source_files)
    _exact(document, "source_tree_sha256", tree_hash)

    _exact(document, "config_path", RUNTIME_RELATIVE_PATH)
    config_hash = _digest(document.get("config_sha256"), "config_sha256")
    config_files = _digest_mapping(document.get("config_files"), "config_files")
    if config_files.get(RUNTIME_RELATIVE_PATH) != config_hash:
        raise V5ReleaseIntegrityError("runtime hash is absent from config_files")
    config_root = f"strategies/trading_v5/releases/{RELEASE_ID}"
    discovered_configs = {
        path.relative_to(ROOT).as_posix()
        for path in (ROOT / config_root).rglob("*")
        if path.is_file()
    }
    if set(config_files) != discovered_configs:
        raise V5ReleaseIntegrityError(
            "config file coverage differs "
            f"missing={sorted(discovered_configs - set(config_files))} "
            f"extra={sorted(set(config_files) - discovered_configs)}"
        )
    for relative_path, digest in config_files.items():
        if not relative_path.startswith(config_root + "/"):
            raise V5ReleaseIntegrityError("config crosses the release namespace")
        _verify_owned_hash(relative_path, digest, config_root)
    runtime_bytes = _owned_file(RUNTIME_RELATIVE_PATH, config_root).read_bytes()
    if _sha256(runtime_bytes) != config_hash:
        raise V5ReleaseIntegrityError("runtime bytes changed during validation")
    _validate_runtime(_strict_json(runtime_bytes, "runtime"))

    test_manifest_path = (
        f"versions/trading_v5/releases/{RELEASE_ID}/tests.txt"
    )
    _exact(document, "test_manifest_path", test_manifest_path)
    _verify_owned_hash(
        test_manifest_path,
        _digest(document.get("test_manifest_sha256"), "test manifest digest"),
        f"versions/trading_v5/releases/{RELEASE_ID}",
    )
    test_files = _digest_mapping(document.get("test_files"), "test_files")
    if not test_files:
        raise V5ReleaseIntegrityError("test_files must not be empty")
    test_lines = _owned_file(
        test_manifest_path,
        f"versions/trading_v5/releases/{RELEASE_ID}",
    ).read_text(encoding="utf-8").splitlines()
    if not test_lines or len(test_lines) != len(set(test_lines)):
        raise V5ReleaseIntegrityError("tests.txt must be non-empty and unique")
    if set(test_lines) != set(test_files):
        raise V5ReleaseIntegrityError("tests.txt differs from test_files")
    for relative_path, digest in test_files.items():
        if not relative_path.startswith("tests/"):
            raise V5ReleaseIntegrityError("test file escapes tests/")
        _verify_owned_hash(relative_path, digest, "tests")

    evidence_files = document.get("historical_evidence_files")
    if not isinstance(evidence_files, Mapping) or set(evidence_files) != {
        "regime_expert_capacity",
        "unified_router_capacity",
    }:
        raise V5ReleaseIntegrityError("historical evidence set differs")
    for name, raw_item in evidence_files.items():
        item = _mapping(raw_item, f"evidence {name}")
        _exact(item, "decision", "BLOCK")
        _exact(item, "current_source_reproducible", False)
        relative_path = _text(item.get("path"), f"evidence {name} path")
        if not relative_path.startswith("artifacts/trading_v5/"):
            raise V5ReleaseIntegrityError("evidence crosses V5 ownership")
        _verify_owned_hash(
            relative_path,
            _digest(item.get("sha256"), f"evidence {name} digest"),
            "artifacts/trading_v5",
        )

    parent_id = "trading_v4.1.0-research"
    parent_path = (
        "versions/trading_v4/releases/"
        f"{parent_id}/manifest.json"
    )
    _exact(document, "parent_release_id", parent_id)
    _exact(document, "parent_manifest_path", parent_path)
    parent_digest = _digest(
        document.get("parent_manifest_sha256"),
        "parent_manifest_sha256",
    )
    parent_file = _owned_file(parent_path, "versions/trading_v4/releases")
    if _sha256(parent_file.read_bytes()) != parent_digest:
        raise V5ReleaseIntegrityError("parent manifest byte hash differs")
    parent = _strict_json(parent_file.read_bytes(), "parent manifest")
    _exact(parent, "system", "trading_v4")
    _exact(parent, "release_id", parent_id)

    _exact(document, "entrypoint", "tools/trading_v5/audit_release.py")
    if document["entrypoint"] not in source_files:
        raise V5ReleaseIntegrityError("entrypoint is not source-owned")
    _exact(document, "artifact_namespace", ARTIFACT_NAMESPACE)
    _exact(document, "database_status", "DEFERRED_MIGRATION_IN_PROGRESS")
    _exact(document, "database_tests_run", False)
    _exact(document, "database_counts_as_pass", False)
    _exact(document, "historical_candidate_pass_count", 0)
    _exact(document, "full_stress_matrix_executed", False)
    _exact(document, "registered_model_count", 0)
    return V5ReleaseIntegrity(
        document=document,
        manifest_sha256=_sha256(manifest_bytes),
        source_tree_sha256=tree_hash,
    )


def _validate_runtime(runtime: Mapping[str, Any]) -> None:
    """Validate safety semantics, not merely the runtime file digest."""

    expected_scalars = {
        "schema_version": "probiga.trading-v5-release-runtime.v1",
        "system": "trading_v5",
        "system_version": "5.0.0-research",
        "release_id": RELEASE_ID,
        "lifecycle_status": "RESEARCH_ONLY",
        "release_decision": "BLOCK",
        "entrypoint": "tools/trading_v5/audit_release.py",
        "artifact_namespace": ARTIFACT_NAMESPACE,
        "api_prefix": None,
        "task_namespace": None,
        "activation_eligible": False,
        "paper_eligible": False,
        "production_eligible": False,
    }
    for name, expected in expected_scalars.items():
        _exact(runtime, name, expected)

    database = _mapping(runtime.get("database"), "runtime database")
    if database != {
        "status": "DEFERRED_MIGRATION_IN_PROGRESS",
        "tests_run": False,
        "counts_as_pass": False,
    }:
        raise V5ReleaseIntegrityError("runtime database evidence is not deferred")

    execution = _mapping(
        runtime.get("execution_boundary"),
        "runtime execution_boundary",
    )
    if execution != {
        "actionable_output_allowed": False,
        "paper_orders_allowed": False,
        "real_orders_allowed": False,
        "order_intents_allowed": False,
        "v2_v3_v4_runtime_imports_allowed": False,
    }:
        raise V5ReleaseIntegrityError("runtime execution boundary is not closed")

    historical = _mapping(
        runtime.get("historical_evidence"),
        "runtime historical_evidence",
    )
    _exact(historical, "counts_as_activation_evidence", False)
    prospective = _mapping(
        runtime.get("prospective_gate"),
        "runtime prospective_gate",
    )
    _exact(prospective, "recorded_evidence_trading_days", 0)
    _exact(prospective, "recorded_evidence_mature_portfolio_trades", 0)
    _exact(prospective, "status", "NOT_EVALUATED")
    _exact(prospective, "counts_as_pass", False)
    model_research = _mapping(
        runtime.get("model_research"),
        "runtime model_research",
    )
    _exact(model_research, "model_activation_claim_forbidden", True)
    _exact(model_research, "serialized_model_reload_supported", False)
    _exact(model_research, "registered_model_count", 0)


def _strict_json(payload: bytes, label: str) -> Mapping[str, Any]:
    def no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise V5ReleaseIntegrityError(f"{label} duplicate key: {key}")
            result[key] = value
        return result

    def reject_constant(value: str) -> None:
        raise V5ReleaseIntegrityError(f"{label} contains non-finite {value}")

    try:
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=no_duplicates,
            parse_constant=reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V5ReleaseIntegrityError(f"{label} is not strict UTF-8 JSON") from exc
    _reject_non_finite(value, label)
    return _mapping(value, label)


def _reject_non_finite(value: Any, path: str) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise V5ReleaseIntegrityError(f"{path} contains a non-finite number")
    if isinstance(value, Mapping):
        for key, child in value.items():
            _reject_non_finite(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_non_finite(child, f"{path}[{index}]")


def _digest_mapping(value: Any, label: str) -> dict[str, str]:
    mapping = _mapping(value, label)
    result: dict[str, str] = {}
    for raw_path, raw_digest in mapping.items():
        canonical = _canonical_path(raw_path, f"{label} path")
        if canonical in result:
            raise V5ReleaseIntegrityError(f"{label} has a canonical path collision")
        result[canonical] = _digest(raw_digest, f"{label} digest")
    return result


def _verify_owned_hash(relative_path: str, digest: str, owner: str) -> None:
    path = _owned_file(relative_path, owner)
    if _sha256(path.read_bytes()) != digest:
        raise V5ReleaseIntegrityError(f"release file hash drifted: {relative_path}")


def _owned_file(relative_path: str, owner: str) -> Path:
    canonical = _canonical_path(relative_path, "release file path")
    lexical_path = ROOT / Path(*PurePosixPath(canonical).parts)
    lexical_owner = ROOT / Path(*PurePosixPath(owner).parts)
    _reject_reparse_points(lexical_path, lexical_owner)
    path = lexical_path.resolve()
    owner_path = lexical_owner.resolve()
    try:
        path.relative_to(owner_path)
    except ValueError as exc:
        raise V5ReleaseIntegrityError(
            f"release file escapes resolved owner root: {relative_path}"
        ) from exc
    if not path.is_file():
        raise V5ReleaseIntegrityError(f"release file is missing: {relative_path}")
    return path


def _reject_reparse_points(path: Path, owner_path: Path) -> None:
    current = path
    while True:
        try:
            metadata = current.stat(follow_symlinks=False)
        except OSError as exc:
            raise V5ReleaseIntegrityError(
                f"cannot inspect release path metadata: {current}"
            ) from exc
        if current.is_symlink() or (
            getattr(metadata, "st_file_attributes", 0) & 0x400
        ):
            raise V5ReleaseIntegrityError(
                f"release ownership path cannot be a symlink/reparse point: {current}"
            )
        if current == owner_path:
            return
        if owner_path not in current.parents:
            raise V5ReleaseIntegrityError("release ownership parent chain differs")
        current = current.parent


def _canonical_path(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise V5ReleaseIntegrityError(f"{label} must be canonical text")
    if "\\" in value or "//" in value or "/./" in value:
        raise V5ReleaseIntegrityError(f"{label} is not canonical")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or path.as_posix() != value:
        raise V5ReleaseIntegrityError(f"{label} escapes or is not canonical")
    return value


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise V5ReleaseIntegrityError(f"{label} must be an object")
    return value


def _text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise V5ReleaseIntegrityError(f"{label} must be non-empty text")
    return value


def _digest(value: Any, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise V5ReleaseIntegrityError(f"{label} must be a lowercase SHA-256")
    return value


def _exact(document: Mapping[str, Any], name: str, expected: Any) -> None:
    if document.get(name) != expected or type(document.get(name)) is not type(expected):
        raise V5ReleaseIntegrityError(
            f"{name} differs: expected={expected!r} actual={document.get(name)!r}"
        )


def _source_tree_hash(source_files: Mapping[str, str]) -> str:
    payload = "".join(
        f"{path}\0{digest}\n"
        for path, digest in sorted(source_files.items())
    ).encode("utf-8")
    return _sha256(payload)


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


__all__ = [
    "V5ReleaseIntegrity",
    "V5ReleaseIntegrityError",
    "validate_v5_release",
]
