#!/usr/bin/env python3
"""Validate immutable, version-owned Trading V4/V5/V6 release manifests."""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path, PurePosixPath
import sys
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SUPPORTED_SYSTEMS = frozenset({"trading_v4", "trading_v5", "trading_v6"})
SUPPORTED_LIFECYCLES = frozenset(
    {
        "RESEARCH_ONLY",
        "EXPLORATORY_ONLY",
        "PAPER_TRIAL",
        "PAPER_ACTIVE",
        "PRODUCTION",
    }
)


class ReleaseManifestError(ValueError):
    """Raised when a release identity or immutable hash drifts."""


@dataclass(frozen=True, slots=True)
class ReleaseValidation:
    manifest_path: str
    system: str
    release_id: str
    lifecycle_status: str
    manifest_sha256: str
    source_tree_sha256: str
    source_file_count: int
    test_file_count: int


def validate_release_manifest(
    manifest_path: Path,
    *,
    repository_root: Path | None = None,
) -> ReleaseValidation:
    root = (repository_root or REPOSITORY_ROOT).resolve()
    path = manifest_path.resolve()
    relative = _relative_file(root, path, "manifest")
    document = _read_json_no_duplicates(path)

    system = _required_text(document, "system")
    if system not in SUPPORTED_SYSTEMS:
        raise ReleaseManifestError(f"unsupported trading system: {system}")
    release_id = _required_text(document, "release_id")
    expected_manifest = PurePosixPath(
        "versions", system, "releases", release_id, "manifest.json"
    )
    if PurePosixPath(relative) != expected_manifest:
        raise ReleaseManifestError(
            f"manifest path does not match release identity: {relative}"
        )
    if document.get("schema_version") != "probiga.immutable-trading-release.v1":
        raise ReleaseManifestError("unsupported release manifest schema_version")
    if document.get("immutable") is not True:
        raise ReleaseManifestError("release manifest must be immutable")
    lifecycle = _required_text(document, "lifecycle_status")
    if lifecycle not in SUPPORTED_LIFECYCLES:
        raise ReleaseManifestError(f"unsupported lifecycle_status: {lifecycle}")

    source_files = _digest_mapping(document, "source_files")
    if not source_files:
        raise ReleaseManifestError("source_files must not be empty")
    allowed_source_prefixes = (
        f"server/{system}/",
        f"tools/{system}/",
    )
    for source_path, expected_hash in source_files.items():
        if not source_path.startswith(allowed_source_prefixes):
            raise ReleaseManifestError(
                f"source file crosses version ownership: {source_path}"
            )
        _verify_file_hash(root, source_path, expected_hash)
    if system in {"trading_v5", "trading_v6"} and document.get(
        "source_coverage_required"
    ) is not True:
        raise ReleaseManifestError(
            f"source_coverage_required must be true for {system}"
        )
    if document.get("source_coverage_required") is True:
        discovered_sources = {
            path.relative_to(root).as_posix()
            for prefix in allowed_source_prefixes
            for path in (root / prefix).rglob("*.py")
            if path.is_file()
        }
        if set(source_files) != discovered_sources:
            missing = sorted(discovered_sources - set(source_files))
            extra = sorted(set(source_files) - discovered_sources)
            raise ReleaseManifestError(
                "source_files do not cover the version-owned Python tree "
                f"missing={missing} extra={extra}"
            )
    source_tree_hash = _source_tree_hash(source_files)
    if document.get("source_tree_sha256") != source_tree_hash:
        raise ReleaseManifestError(
            "source_tree_sha256 does not match the source_files mapping"
        )

    config_path = _required_text(document, "config_path")
    expected_config_prefix = f"strategies/{system}/releases/{release_id}/"
    if not config_path.startswith(expected_config_prefix):
        raise ReleaseManifestError("config_path crosses the release namespace")
    _verify_file_hash(
        root,
        config_path,
        _required_digest(document, "config_sha256"),
    )
    config_files_raw = document.get("config_files")
    if config_files_raw is not None:
        config_files = _digest_mapping(document, "config_files")
        if config_files.get(config_path) != document.get("config_sha256"):
            raise ReleaseManifestError(
                "config_files must include config_path with the same digest"
            )
        for owned_config_path, owned_config_hash in config_files.items():
            if not owned_config_path.startswith(expected_config_prefix):
                raise ReleaseManifestError(
                    f"config file crosses the release namespace: {owned_config_path}"
                )
            _verify_file_hash(root, owned_config_path, owned_config_hash)

    entrypoint = _required_text(document, "entrypoint")
    if entrypoint not in source_files:
        raise ReleaseManifestError("entrypoint is absent from source_files")
    expected_artifact_namespace = f"artifacts/{system}/releases/{release_id}"
    if document.get("artifact_namespace") != expected_artifact_namespace:
        raise ReleaseManifestError("artifact_namespace does not match release_id")

    test_manifest_path = _required_text(document, "test_manifest_path")
    expected_test_manifest = (
        f"versions/{system}/releases/{release_id}/tests.txt"
    )
    if test_manifest_path != expected_test_manifest:
        raise ReleaseManifestError("test_manifest_path crosses release namespace")
    _verify_file_hash(
        root,
        test_manifest_path,
        _required_digest(document, "test_manifest_sha256"),
    )
    test_files = _digest_mapping(document, "test_files")
    test_lines = (root / test_manifest_path).read_text(
        encoding="utf-8"
    ).splitlines()
    if not test_lines or len(test_lines) != len(set(test_lines)):
        raise ReleaseManifestError("tests.txt must be non-empty and unique")
    if set(test_lines) != set(test_files):
        raise ReleaseManifestError("tests.txt and test_files do not match")
    for test_path, expected_hash in test_files.items():
        if not test_path.startswith("tests/"):
            raise ReleaseManifestError(f"test path is outside tests/: {test_path}")
        _verify_file_hash(root, test_path, expected_hash)

    evidence = document.get("historical_evidence")
    if evidence is not None:
        if not isinstance(evidence, Mapping):
            raise ReleaseManifestError("historical_evidence must be an object")
        evidence_path = _required_text(evidence, "path")
        if not evidence_path.startswith(f"artifacts/{system}/"):
            raise ReleaseManifestError("historical_evidence crosses version ownership")
        if lifecycle in {"RESEARCH_ONLY", "EXPLORATORY_ONLY"} and evidence.get(
            "decision"
        ) != "BLOCK":
            raise ReleaseManifestError("research historical_evidence must be BLOCK")
        _verify_file_hash(
            root,
            evidence_path,
            _required_digest(evidence, "sha256"),
        )
    evidence_files = document.get("historical_evidence_files")
    if evidence_files is not None:
        if not isinstance(evidence_files, Mapping) or not evidence_files:
            raise ReleaseManifestError(
                "historical_evidence_files must be a non-empty object"
            )
        for name, item in evidence_files.items():
            if not isinstance(name, str) or not name.strip():
                raise ReleaseManifestError("evidence names must be non-empty text")
            if not isinstance(item, Mapping):
                raise ReleaseManifestError(f"evidence {name} must be an object")
            evidence_path = _required_text(item, "path")
            if not evidence_path.startswith(f"artifacts/{system}/"):
                raise ReleaseManifestError(
                    f"evidence crosses the version namespace: {evidence_path}"
                )
            _verify_file_hash(
                root,
                evidence_path,
                _required_digest(item, "sha256"),
            )
            if lifecycle in {"RESEARCH_ONLY", "EXPLORATORY_ONLY"} and item.get(
                "decision"
            ) != "BLOCK":
                raise ReleaseManifestError(
                    f"research evidence {name} must remain BLOCK"
                )

    dependency_files = document.get("historical_dependency_files")
    if dependency_files is not None:
        if not isinstance(dependency_files, Mapping) or not dependency_files:
            raise ReleaseManifestError(
                "historical_dependency_files must be a non-empty object"
            )
        for name, item in dependency_files.items():
            if not isinstance(name, str) or not name.strip():
                raise ReleaseManifestError(
                    "historical dependency names must be non-empty text"
                )
            if not isinstance(item, Mapping):
                raise ReleaseManifestError(
                    f"historical dependency {name} must be an object"
                )
            dependency_path = _required_text(item, "path")
            if not dependency_path.startswith(("server/", "tools/", "strategies/")):
                raise ReleaseManifestError(
                    f"historical dependency path is outside code/config roots: "
                    f"{dependency_path}"
                )
            _verify_file_hash(
                root,
                dependency_path,
                _required_digest(item, "sha256"),
            )
            if lifecycle in {"RESEARCH_ONLY", "EXPLORATORY_ONLY"}:
                if item.get("decision") != "BLOCK":
                    raise ReleaseManifestError(
                        f"research dependency {name} must remain BLOCK"
                    )
                if item.get("current_source_reproducible") is not False:
                    raise ReleaseManifestError(
                        f"research dependency {name} must not claim current-source "
                        "reproducibility"
                    )

    parent_release_id = document.get("parent_release_id")
    parent_manifest_sha = document.get("parent_manifest_sha256")
    parent_manifest_path = document.get("parent_manifest_path")
    if parent_release_id is None:
        if parent_manifest_sha is not None or parent_manifest_path is not None:
            raise ReleaseManifestError(
                "parent manifest identity must be entirely null or entirely present"
            )
    else:
        if not isinstance(parent_release_id, str) or not parent_release_id.strip():
            raise ReleaseManifestError("parent_release_id must be non-empty text")
        if not isinstance(parent_manifest_path, str):
            raise ReleaseManifestError("parent_manifest_path is required")
        parent_system = parent_release_id.split(".", 1)[0]
        if parent_system not in SUPPORTED_SYSTEMS:
            raise ReleaseManifestError("parent_release_id has an unsupported system")
        expected_parent_path = (
            f"versions/{parent_system}/releases/"
            f"{parent_release_id}/manifest.json"
        )
        if parent_manifest_path != expected_parent_path:
            raise ReleaseManifestError("parent manifest path differs from parent id")
        _verify_file_hash(
            root,
            parent_manifest_path,
            _digest(parent_manifest_sha, "parent_manifest_sha256"),
        )
        parent_document = _read_json_no_duplicates(root / parent_manifest_path)
        if parent_document.get("system") != parent_system:
            raise ReleaseManifestError("parent manifest system differs")
        if parent_document.get("release_id") != parent_release_id:
            raise ReleaseManifestError("parent manifest release_id differs")

    if lifecycle in {"RESEARCH_ONLY", "EXPLORATORY_ONLY"}:
        for field_name in (
            "activation_eligible",
            "paper_eligible",
            "production_eligible",
        ):
            if document.get(field_name) is not False:
                raise ReleaseManifestError(
                    f"{field_name} must be false for {lifecycle}"
                )
    if any(
        document.get(field_name) is True
        for field_name in (
            "activation_eligible",
            "paper_eligible",
            "production_eligible",
        )
    ):
        raise ReleaseManifestError(
            "this internal hash validator cannot authorize activation eligibility"
        )

    return ReleaseValidation(
        manifest_path=relative,
        system=system,
        release_id=release_id,
        lifecycle_status=lifecycle,
        manifest_sha256=_sha256(path.read_bytes()),
        source_tree_sha256=source_tree_hash,
        source_file_count=len(source_files),
        test_file_count=len(test_files),
    )


def discover_release_manifests(
    repository_root: Path | None = None,
) -> tuple[Path, ...]:
    root = (repository_root or REPOSITORY_ROOT).resolve()
    return tuple(
        sorted(root.glob("versions/trading_v[456]/releases/*/manifest.json"))
    )


def _read_json_no_duplicates(path: Path) -> Mapping[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ReleaseManifestError(f"cannot read manifest: {path}") from exc

    def no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ReleaseManifestError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    try:
        document = json.loads(
            raw,
            object_pairs_hook=no_duplicates,
            parse_constant=lambda value: (_ for _ in ()).throw(
                ReleaseManifestError(
                    f"manifest contains non-finite JSON constant: {value}"
                )
            ),
        )
    except json.JSONDecodeError as exc:
        raise ReleaseManifestError("manifest is not valid JSON") from exc
    if not isinstance(document, Mapping):
        raise ReleaseManifestError("manifest root must be a JSON object")
    _reject_non_finite(document, "manifest")
    return document


def _digest_mapping(document: Mapping[str, Any], name: str) -> dict[str, str]:
    value = document.get(name)
    if not isinstance(value, Mapping):
        raise ReleaseManifestError(f"{name} must be an object")
    result: dict[str, str] = {}
    for raw_path, raw_digest in value.items():
        if not isinstance(raw_path, str):
            raise ReleaseManifestError(f"{name} paths must be text")
        canonical_path = _canonical_relative_path(raw_path, f"{name} path")
        result[canonical_path] = _digest(raw_digest, f"{name} digest")
    return result


def _required_text(document: Mapping[str, Any], name: str) -> str:
    value = document.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ReleaseManifestError(f"{name} must be non-empty text")
    return value.strip()


def _required_digest(document: Mapping[str, Any], name: str) -> str:
    return _digest(document.get(name), name)


def _digest(value: Any, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ReleaseManifestError(f"{name} must be a lowercase SHA-256 digest")
    return value


def _canonical_relative_path(value: str, name: str) -> str:
    if not value or value != value.strip() or "\\" in value:
        raise ReleaseManifestError(f"{name} is not canonical")
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or ".." in path.parts
        or not path.parts
        or path.as_posix() != value
        or "//" in value
        or "/./" in value
    ):
        raise ReleaseManifestError(f"{name} must stay within the repository")
    return path.as_posix()


def _verify_file_hash(root: Path, relative_path: str, expected_hash: str) -> None:
    canonical = _canonical_relative_path(relative_path, "file path")
    path = (root / Path(*PurePosixPath(canonical).parts)).resolve()
    _relative_file(root, path, "file")
    if not path.is_file():
        raise ReleaseManifestError(f"release file is missing: {canonical}")
    actual = _sha256(path.read_bytes())
    if actual != expected_hash:
        raise ReleaseManifestError(
            f"release file hash drifted: {canonical} expected={expected_hash} actual={actual}"
        )


def _relative_file(root: Path, path: Path, label: str) -> str:
    try:
        relative = path.relative_to(root)
    except ValueError as exc:
        raise ReleaseManifestError(f"{label} escapes repository root") from exc
    return relative.as_posix()


def _source_tree_hash(source_files: Mapping[str, str]) -> str:
    payload = "".join(
        f"{path}\0{digest}\n"
        for path, digest in sorted(source_files.items())
    ).encode("utf-8")
    return _sha256(payload)


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _reject_non_finite(value: Any, path: str) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise ReleaseManifestError(f"{path} contains a non-finite number")
    if isinstance(value, Mapping):
        for key, child in value.items():
            _reject_non_finite(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_non_finite(child, f"{path}[{index}]")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate immutable V4/V5/V6 release files and hashes."
    )
    parser.add_argument(
        "manifests",
        nargs="*",
        type=Path,
        help="Manifest paths. Omit to validate every discovered release.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    manifests = tuple(args.manifests) or discover_release_manifests()
    if not manifests:
        print("release_validation=FAILED error=no release manifests found", file=sys.stderr)
        return 2
    try:
        results = tuple(validate_release_manifest(path) for path in manifests)
    except (OSError, ReleaseManifestError) as exc:
        print(f"release_validation=FAILED error={exc}", file=sys.stderr)
        return 2
    for result in results:
        print(
            "release_validation=PASS "
            f"system={result.system} release_id={result.release_id} "
            f"lifecycle={result.lifecycle_status} "
            f"manifest_sha256={result.manifest_sha256} "
            f"source_tree_sha256={result.source_tree_sha256}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
