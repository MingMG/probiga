#!/usr/bin/env python3
"""Build or verify a Git-bound V3 baseline evidence manifest.

The command never discovers evidence by walking the repository.  A build
requires a six-category include specification, an explicitly expected HEAD,
and a new output path.  It produces a candidate for human review; it cannot
record the manifest hash in an external trust store or authorize V4 runtime.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.evaluation.v3_baseline_manifest import (
    REQUIRED_EVIDENCE_TYPES,
    V3BaselineManifestError,
    build_v3_baseline_manifest,
    load_v3_baseline_manifest,
    verify_v3_baseline_manifest,
)


PRODUCTION_ACTIVATION_ALLOWED = False
ACTIONABLE_OUTPUT_ALLOWED = False


def _duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise V3BaselineManifestError(
                f"include spec contains duplicate JSON key: {key}"
            )
        result[key] = value
    return result


def _non_finite(value: str) -> None:
    raise V3BaselineManifestError(
        f"include spec contains non-finite JSON value: {value}"
    )


def load_include_spec(path: str | Path) -> dict[str, tuple[str, ...]]:
    try:
        raw = Path(path).read_text(encoding="utf-8")
    except OSError as exc:
        raise V3BaselineManifestError(
            f"unable to read V3 baseline include spec: {path}"
        ) from exc
    try:
        value = json.loads(
            raw,
            object_pairs_hook=_duplicate_keys,
            parse_constant=_non_finite,
        )
    except V3BaselineManifestError:
        raise
    except (TypeError, ValueError) as exc:
        raise V3BaselineManifestError(
            "V3 baseline include spec is not strict JSON"
        ) from exc
    if not isinstance(value, Mapping):
        raise V3BaselineManifestError("include spec must be one JSON object")
    if set(value) != REQUIRED_EVIDENCE_TYPES:
        raise V3BaselineManifestError(
            "include spec keys must be exactly: "
            + ", ".join(sorted(REQUIRED_EVIDENCE_TYPES))
        )
    normalized: dict[str, tuple[str, ...]] = {}
    for evidence_type in sorted(REQUIRED_EVIDENCE_TYPES):
        paths = value[evidence_type]
        if not isinstance(paths, Sequence) or isinstance(
            paths,
            (str, bytes, bytearray),
        ):
            raise V3BaselineManifestError(
                f"include spec {evidence_type!r} must be an explicit array"
            )
        if not paths or any(type(item) is not str for item in paths):
            raise V3BaselineManifestError(
                f"include spec {evidence_type!r} must contain explicit paths"
            )
        normalized[evidence_type] = tuple(paths)
    return normalized


def _write_new_manifest(path: Path, canonical_json: str) -> None:
    if not path.parent.is_dir():
        raise V3BaselineManifestError(
            "manifest output parent directory does not exist"
        )
    try:
        with path.open("x", encoding="utf-8", newline="\n") as stream:
            stream.write(canonical_json)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError as exc:
        raise V3BaselineManifestError(
            "manifest output already exists; frozen candidates are never overwritten"
        ) from exc
    except OSError as exc:
        raise V3BaselineManifestError(
            f"unable to write manifest output: {path}"
        ) from exc


def _report(
    *,
    operation: str,
    manifest_hash: str,
    git_commit: str,
    file_count: int,
    output_path: Path | None,
) -> dict[str, Any]:
    return {
        "status": "CANDIDATE_REQUIRES_EXTERNAL_CONFIRMATION",
        "operation": operation,
        "git_commit": git_commit,
        "manifest_hash": manifest_hash,
        "file_count": file_count,
        "output_path": str(output_path.resolve()) if output_path else None,
        "external_trusted_hash_recorded": False,
        "human_confirmation_required": True,
        "production_activation_allowed": PRODUCTION_ACTIVATION_ALLOWED,
        "actionable_output_allowed": ACTIONABLE_OUTPUT_ALLOWED,
    }


def build_candidate(
    *,
    repo_root: str | Path,
    include_spec_path: str | Path,
    output_path: str | Path,
    expected_git_commit: str,
) -> dict[str, Any]:
    include_spec = load_include_spec(include_spec_path)
    manifest = build_v3_baseline_manifest(
        repo_root,
        include_spec,
        expected_git_commit=expected_git_commit,
    )
    output = Path(output_path)
    _write_new_manifest(output, manifest.canonical_json())
    return _report(
        operation="BUILD",
        manifest_hash=manifest.manifest_hash,
        git_commit=manifest.git_commit,
        file_count=len(manifest.files),
        output_path=output,
    )


def verify_candidate(
    *,
    repo_root: str | Path,
    manifest_path: str | Path,
    expected_manifest_hash: str,
    expected_git_commit: str,
) -> dict[str, Any]:
    manifest = load_v3_baseline_manifest(
        manifest_path,
        expected_manifest_hash=expected_manifest_hash,
    )
    verified = verify_v3_baseline_manifest(
        repo_root,
        manifest,
        expected_manifest_hash=expected_manifest_hash,
        expected_git_commit=expected_git_commit,
    )
    return _report(
        operation="VERIFY",
        manifest_hash=verified.manifest_hash,
        git_commit=verified.git_commit,
        file_count=len(verified.files),
        output_path=Path(manifest_path),
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build or verify a Git-bound V3 baseline candidate without "
            "authorizing production or actionable V4 output"
        )
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build")
    build.add_argument("--repo-root", required=True)
    build.add_argument("--include-spec", required=True)
    build.add_argument("--output", required=True)
    build.add_argument("--expected-git-commit", required=True)
    verify = subparsers.add_parser("verify")
    verify.add_argument("--repo-root", required=True)
    verify.add_argument("--manifest", required=True)
    verify.add_argument("--expected-manifest-hash", required=True)
    verify.add_argument("--expected-git-commit", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "build":
        report = build_candidate(
            repo_root=args.repo_root,
            include_spec_path=args.include_spec,
            output_path=args.output,
            expected_git_commit=args.expected_git_commit,
        )
    else:
        report = verify_candidate(
            repo_root=args.repo_root,
            manifest_path=args.manifest,
            expected_manifest_hash=args.expected_manifest_hash,
            expected_git_commit=args.expected_git_commit,
        )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

