from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from server.evaluation.v3_baseline_manifest import V3BaselineManifestError
from tools.manage_v3_baseline_manifest import (
    build_candidate,
    load_include_spec,
    verify_candidate,
)


def _git(root: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(root), *arguments],
        check=True,
        capture_output=True,
        encoding="utf-8",
        shell=False,
        timeout=10,
    )
    return completed.stdout.strip()


def _repository(tmp_path: Path) -> tuple[Path, Path, str]:
    root = tmp_path / "repo"
    root.mkdir()
    _git(root, "init", "--quiet")
    _git(root, "config", "user.email", "v3-freeze@example.invalid")
    _git(root, "config", "user.name", "V3 Freeze Test")
    files = {
        "v3/code.py": b"VERSION = 'v3'\n",
        "v3/config.json": b"{}\n",
        "v3/model.bin": b"model\n",
        "v3/calibration.json": b"{}\n",
        "v3/task.py": b"pass\n",
        "v3/schema.sql": b"SELECT 1;\n",
    }
    for relative, content in files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    _git(root, "add", "--", ".")
    _git(root, "commit", "--quiet", "-m", "freeze v3")
    commit = _git(root, "rev-parse", "HEAD")
    spec = root / "include.json"
    spec.write_text(
        json.dumps(
            {
                "code": ["v3/code.py"],
                "config": ["v3/config.json"],
                "model": ["v3/model.bin"],
                "calibration": ["v3/calibration.json"],
                "task": ["v3/task.py"],
                "schema": ["v3/schema.sql"],
            }
        ),
        encoding="utf-8",
    )
    return root, spec, commit


def test_build_and_verify_candidate_are_git_bound_and_never_authorize(tmp_path):
    root, spec, commit = _repository(tmp_path)
    output = tmp_path / "v3-baseline.json"

    built = build_candidate(
        repo_root=root,
        include_spec_path=spec,
        output_path=output,
        expected_git_commit=commit,
    )
    verified = verify_candidate(
        repo_root=root,
        manifest_path=output,
        expected_manifest_hash=built["manifest_hash"],
        expected_git_commit=commit,
    )

    assert built["status"] == "CANDIDATE_REQUIRES_EXTERNAL_CONFIRMATION"
    assert built["file_count"] == 6
    assert verified["manifest_hash"] == built["manifest_hash"]
    assert built["external_trusted_hash_recorded"] is False
    assert built["human_confirmation_required"] is True
    assert built["production_activation_allowed"] is False
    assert built["actionable_output_allowed"] is False
    with pytest.raises(V3BaselineManifestError, match="never overwritten"):
        build_candidate(
            repo_root=root,
            include_spec_path=spec,
            output_path=output,
            expected_git_commit=commit,
        )


def test_build_rejects_dirty_or_untracked_selected_evidence(tmp_path):
    root, spec, commit = _repository(tmp_path)
    (root / "v3/code.py").write_text("VERSION = 'dirty'\n", encoding="utf-8")
    with pytest.raises(V3BaselineManifestError, match="Git commit blob"):
        build_candidate(
            repo_root=root,
            include_spec_path=spec,
            output_path=tmp_path / "dirty.json",
            expected_git_commit=commit,
        )


@pytest.mark.parametrize(
    "raw",
    (
        '{"code":[],"code":[]}',
        '{"code":[],"config":[],"model":[],"calibration":[],"task":[]}',
        '{"code":"v3/code.py","config":[],"model":[],"calibration":[],"task":[],"schema":[]}',
    ),
)
def test_include_spec_is_strict_complete_and_explicit(tmp_path, raw):
    path = tmp_path / "bad.json"
    path.write_text(raw, encoding="utf-8")
    with pytest.raises(V3BaselineManifestError):
        load_include_spec(path)
