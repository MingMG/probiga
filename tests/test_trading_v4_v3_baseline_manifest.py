from __future__ import annotations

import json
import subprocess
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from server.evaluation.v3_baseline_manifest import (
    REQUIRED_EVIDENCE_TYPES,
    V3_BASELINE_MANIFEST_SCHEMA,
    V3BaselineEvidenceManifest,
    V3BaselineManifestError,
    build_v3_baseline_manifest,
    load_v3_baseline_manifest,
    resolve_repository_commit,
    verify_v3_baseline_manifest,
)


def _git(repo_root: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repo_root), *arguments],
        check=True,
        capture_output=True,
        encoding="utf-8",
        errors="strict",
        shell=False,
        timeout=10,
    )
    return completed.stdout.strip()


def _write(repo_root: Path, relative_path: str, content: bytes) -> None:
    target = repo_root / relative_path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(content)


def _baseline_repository(
    tmp_path: Path,
) -> tuple[Path, dict[str, tuple[str, ...]], str]:
    repo_root = tmp_path / "v3-repository"
    repo_root.mkdir()
    _git(repo_root, "init", "--quiet")
    _git(repo_root, "config", "user.email", "baseline-tests@example.invalid")
    _git(repo_root, "config", "user.name", "Baseline Tests")

    contents = {
        "server/trading_v3/kernel.py": b"VERSION = 'v3'\n",
        "server/trading_v3/signals.py": b"def signal(): return 1\n",
        "strategies/trading_v3.json": b'{"strategy":"v3"}\n',
        "artifacts/v3/model.bin": b"frozen-model-v3\x00\x01",
        "artifacts/v3/calibration.json": b'{"temperature":0.9}\n',
        "tools/run_trading_v3.py": b"print('v3 task')\n",
        "server/db/v3_schema.sql": b"CREATE TABLE signals(id INTEGER);\n",
    }
    for relative_path, content in contents.items():
        _write(repo_root, relative_path, content)
    _git(repo_root, "add", "--", ".")
    _git(repo_root, "commit", "--quiet", "-m", "freeze v3 baseline")
    commit = _git(repo_root, "rev-parse", "HEAD")
    include_files = {
        "code": (
            "server/trading_v3/kernel.py",
            "server/trading_v3/signals.py",
        ),
        "config": ("strategies/trading_v3.json",),
        "model": ("artifacts/v3/model.bin",),
        "calibration": ("artifacts/v3/calibration.json",),
        "task": ("tools/run_trading_v3.py",),
        "schema": ("server/db/v3_schema.sql",),
    }
    return repo_root, include_files, commit


def _with_replacement(
    include_files: dict[str, tuple[str, ...]],
    evidence_type: str,
    paths: tuple[str, ...],
) -> dict[str, tuple[str, ...]]:
    replaced = dict(include_files)
    replaced[evidence_type] = paths
    return replaced


def test_manifest_is_deterministic_frozen_and_complete(tmp_path):
    repo_root, include_files, commit = _baseline_repository(tmp_path)

    first = build_v3_baseline_manifest(repo_root, include_files)
    reordered = {
        evidence_type: tuple(reversed(paths))
        for evidence_type, paths in reversed(tuple(include_files.items()))
    }
    second = build_v3_baseline_manifest(repo_root, reordered)

    assert first == second
    assert first.canonical_json() == second.canonical_json()
    assert first.manifest_hash == second.manifest_hash
    assert first.git_commit == commit == resolve_repository_commit(repo_root)
    assert set(first.evidence_hashes) == REQUIRED_EVIDENCE_TYPES
    assert first.schema_checksum == first.evidence_hashes["schema"]
    assert first.as_dict()["schema_version"] == V3_BASELINE_MANIFEST_SCHEMA
    assert first.files == tuple(
        sorted(first.files, key=lambda item: (item.evidence_type, item.path))
    )
    assert V3BaselineEvidenceManifest.from_mapping(first.as_dict()) == first

    with pytest.raises(FrozenInstanceError):
        first.git_commit = "0" * 40
    with pytest.raises(TypeError):
        first.evidence_hashes["model"] = "0" * 64


def test_load_and_verify_require_an_external_trusted_hash(tmp_path):
    repo_root, include_files, _ = _baseline_repository(tmp_path)
    manifest = build_v3_baseline_manifest(repo_root, include_files)
    manifest_path = tmp_path / "v3-baseline-manifest.json"
    manifest_path.write_text(manifest.canonical_json(), encoding="utf-8")

    loaded = load_v3_baseline_manifest(
        manifest_path,
        expected_manifest_hash=manifest.manifest_hash,
    )

    assert loaded == manifest
    assert verify_v3_baseline_manifest(
        repo_root,
        loaded,
        expected_manifest_hash=manifest.manifest_hash,
        expected_git_commit=manifest.git_commit,
    ) is loaded
    with pytest.raises(V3BaselineManifestError, match="expected_manifest_hash"):
        load_v3_baseline_manifest(
            manifest_path,
            expected_manifest_hash="0" * 64,
        )
    with pytest.raises(V3BaselineManifestError, match="expected_manifest_hash"):
        verify_v3_baseline_manifest(
            repo_root,
            loaded,
            expected_manifest_hash="0" * 64,
        )


def test_verification_rejects_content_drift_and_missing_files(tmp_path):
    repo_root, include_files, _ = _baseline_repository(tmp_path)
    manifest = build_v3_baseline_manifest(repo_root, include_files)
    config_path = repo_root / "strategies/trading_v3.json"
    original_config = config_path.read_bytes()

    config_path.write_bytes(b'{"strategy":"changed"}\n')
    with pytest.raises(V3BaselineManifestError, match="Git commit blob"):
        verify_v3_baseline_manifest(
            repo_root,
            manifest,
            expected_manifest_hash=manifest.manifest_hash,
        )

    config_path.write_bytes(original_config)
    (repo_root / "artifacts/v3/model.bin").unlink()
    with pytest.raises(V3BaselineManifestError, match="does not exist"):
        verify_v3_baseline_manifest(
            repo_root,
            manifest,
            expected_manifest_hash=manifest.manifest_hash,
        )


def test_builder_rejects_dirty_selected_evidence_only(tmp_path):
    repo_root, include_files, _ = _baseline_repository(tmp_path)

    # An unrelated dirty/untracked file must not block a per-evidence freeze.
    _write(repo_root, "scratch/unrelated.txt", b"not baseline evidence\n")
    assert build_v3_baseline_manifest(repo_root, include_files)

    selected = repo_root / "server/trading_v3/kernel.py"
    selected.write_bytes(b"VERSION = 'dirty-v3'\n")
    with pytest.raises(V3BaselineManifestError, match="Git commit blob"):
        build_v3_baseline_manifest(repo_root, include_files)


def test_builder_rejects_untracked_selected_evidence(tmp_path):
    repo_root, include_files, _ = _baseline_repository(tmp_path)
    _write(repo_root, "tools/untracked_v3_task.py", b"print('untracked')\n")
    untracked = _with_replacement(
        include_files,
        "task",
        ("tools/untracked_v3_task.py",),
    )

    with pytest.raises(V3BaselineManifestError, match="not tracked"):
        build_v3_baseline_manifest(repo_root, untracked)


def test_builder_rejects_missing_absolute_directory_and_glob_paths(tmp_path):
    repo_root, include_files, _ = _baseline_repository(tmp_path)
    outside = tmp_path / "outside.py"
    outside.write_text("outside = True\n", encoding="utf-8")

    invalid_cases = (
        (
            _with_replacement(include_files, "code", ("missing.py",)),
            "does not exist",
        ),
        (
            _with_replacement(
                include_files,
                "code",
                (str(repo_root / "server/trading_v3/kernel.py"),),
            ),
            "must be relative",
        ),
        (
            _with_replacement(include_files, "code", ("../outside.py",)),
            "escapes repository root",
        ),
        (
            _with_replacement(include_files, "code", ("server/trading_v3",)),
            "one explicit file",
        ),
        (
            _with_replacement(include_files, "code", ("server/**/*.py",)),
            "glob wildcards",
        ),
    )
    for invalid_include_files, message in invalid_cases:
        with pytest.raises(V3BaselineManifestError, match=message):
            build_v3_baseline_manifest(repo_root, invalid_include_files)


def test_builder_rejects_normalized_path_and_type_collisions(tmp_path):
    repo_root, include_files, _ = _baseline_repository(tmp_path)

    duplicate_paths = _with_replacement(
        include_files,
        "code",
        (
            "server/trading_v3/kernel.py",
            "server/trading_v3/../trading_v3/kernel.py",
        ),
    )
    with pytest.raises(V3BaselineManifestError, match="duplicate normalized"):
        build_v3_baseline_manifest(repo_root, duplicate_paths)

    cross_type_duplicate = _with_replacement(
        include_files,
        "task",
        ("server/trading_v3/kernel.py",),
    )
    with pytest.raises(V3BaselineManifestError, match="duplicate normalized"):
        build_v3_baseline_manifest(repo_root, cross_type_duplicate)

    duplicate_types = dict(include_files)
    duplicate_types[" MODEL "] = ("artifacts/v3/model.bin",)
    with pytest.raises(V3BaselineManifestError, match="duplicate evidence types"):
        build_v3_baseline_manifest(repo_root, duplicate_types)


def test_parser_rejects_normalized_collisions_and_internal_tampering(tmp_path):
    repo_root, include_files, _ = _baseline_repository(tmp_path)
    manifest = build_v3_baseline_manifest(repo_root, include_files)

    hash_collision = manifest.as_dict()
    hash_collision["evidence_hashes"][" MODEL "] = hash_collision[
        "evidence_hashes"
    ]["model"]
    with pytest.raises(V3BaselineManifestError, match="duplicate evidence types"):
        V3BaselineEvidenceManifest.from_mapping(hash_collision)

    path_collision = manifest.as_dict()
    code_entries = [
        item for item in path_collision["files"] if item["evidence_type"] == "code"
    ]
    code_entries[1]["path"] = code_entries[0]["path"].upper()
    with pytest.raises(V3BaselineManifestError, match="duplicate normalized"):
        V3BaselineEvidenceManifest.from_mapping(path_collision)

    tampered_file_hash = manifest.as_dict()
    tampered_file_hash["files"][0]["sha256"] = "0" * 64
    with pytest.raises(V3BaselineManifestError, match="evidence hashes"):
        V3BaselineEvidenceManifest.from_mapping(tampered_file_hash)


def test_builder_requires_every_explicit_evidence_group(tmp_path):
    repo_root, include_files, _ = _baseline_repository(tmp_path)

    missing_schema = dict(include_files)
    del missing_schema["schema"]
    with pytest.raises(V3BaselineManifestError, match="missing required"):
        build_v3_baseline_manifest(repo_root, missing_schema)

    empty_model = _with_replacement(include_files, "model", ())
    with pytest.raises(V3BaselineManifestError, match="must not be empty"):
        build_v3_baseline_manifest(repo_root, empty_model)

    not_a_list = dict(include_files)
    not_a_list["task"] = "tools/run_trading_v3.py"
    with pytest.raises(V3BaselineManifestError, match="explicit file list"):
        build_v3_baseline_manifest(repo_root, not_a_list)


def test_commit_binding_rejects_wrong_expected_or_changed_head(tmp_path):
    repo_root, include_files, commit = _baseline_repository(tmp_path)

    with pytest.raises(V3BaselineManifestError, match="expected_git_commit"):
        build_v3_baseline_manifest(
            repo_root,
            include_files,
            expected_git_commit="0" * len(commit),
        )

    manifest = build_v3_baseline_manifest(
        repo_root,
        include_files,
        expected_git_commit=commit,
    )
    _write(repo_root, "unrelated.txt", b"new commit\n")
    _git(repo_root, "add", "--", "unrelated.txt")
    _git(repo_root, "commit", "--quiet", "-m", "advance head")
    with pytest.raises(V3BaselineManifestError, match="manifest git_commit"):
        verify_v3_baseline_manifest(
            repo_root,
            manifest,
            expected_manifest_hash=manifest.manifest_hash,
        )

    with pytest.raises(V3BaselineManifestError, match="top-level"):
        resolve_repository_commit(repo_root / "server")


def test_strict_json_rejects_duplicate_keys_and_non_finite_numbers(tmp_path):
    duplicate_path = tmp_path / "duplicate.json"
    duplicate_path.write_text(
        '{"schema_version":"one","schema_version":"two"}',
        encoding="utf-8",
    )
    with pytest.raises(V3BaselineManifestError, match="duplicate key"):
        load_v3_baseline_manifest(
            duplicate_path,
            expected_manifest_hash="0" * 64,
        )

    non_finite_path = tmp_path / "non-finite.json"
    non_finite_path.write_text('{"value":NaN}', encoding="utf-8")
    with pytest.raises(V3BaselineManifestError, match="non-finite"):
        load_v3_baseline_manifest(
            non_finite_path,
            expected_manifest_hash="0" * 64,
        )


def test_external_hash_detects_a_self_consistent_rewritten_manifest(tmp_path):
    repo_root, include_files, _ = _baseline_repository(tmp_path)
    original = build_v3_baseline_manifest(repo_root, include_files)
    _write(
        repo_root,
        "strategies/trading_v3.json",
        (json.dumps({"strategy": "attacker-rewritten"}) + "\n").encode(
            "utf-8"
        ),
    )
    with pytest.raises(V3BaselineManifestError, match="Git commit blob"):
        build_v3_baseline_manifest(repo_root, include_files)

    _git(repo_root, "add", "--", "strategies/trading_v3.json")
    _git(repo_root, "commit", "--quiet", "-m", "rewrite baseline evidence")
    rewritten = build_v3_baseline_manifest(repo_root, include_files)
    assert rewritten.manifest_hash != original.manifest_hash

    manifest_path = tmp_path / "rewritten.json"
    manifest_path.write_text(rewritten.canonical_json(), encoding="utf-8")
    with pytest.raises(V3BaselineManifestError, match="expected_manifest_hash"):
        load_v3_baseline_manifest(
            manifest_path,
            expected_manifest_hash=original.manifest_hash,
        )
