from __future__ import annotations

import hashlib
from pathlib import Path
import subprocess
from types import SimpleNamespace

import pytest

from server.common import scheduler_script_policy


def _blob(content: bytes) -> str:
    return hashlib.sha1(
        f"blob {len(content)}\0".encode("ascii") + content
    ).hexdigest()


def _git(root: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", "-C", str(root), *arguments],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    ).stdout.strip()


def _repository(tmp_path: Path) -> tuple[Path, Path, str]:
    root = tmp_path.resolve()
    script = root / "tools" / "run_job.py"
    script.parent.mkdir()
    script.write_text("print('ok')\n", encoding="utf-8", newline="\n")
    _git(root, "init")
    _git(root, "config", "user.email", "scheduler-policy@example.invalid")
    _git(root, "config", "user.name", "Scheduler Policy Test")
    _git(root, "add", "tools/run_job.py")
    _git(root, "commit", "-m", "add scheduler script")
    return root, script, _git(root, "rev-parse", "HEAD")


def test_git_policy_uses_exact_head_index_and_worktree_blobs_without_diff(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path.resolve()
    script = root / "tools" / "run_job.py"
    script.parent.mkdir()
    script.write_text("print('ok')\n", encoding="utf-8", newline="\n")
    head = "a" * 40
    blob = _blob(script.read_bytes())
    calls: list[tuple[list[str], dict[str, object]]] = []

    def fake_run(command, **kwargs):
        calls.append((list(command), dict(kwargs)))
        output = (
            f"{head}\n{blob}\n"
            if "rev-parse" in command
            else f"100644 {blob} 0\ttools/run_job.py\n"
        )
        return SimpleNamespace(stdout=output)

    monkeypatch.setattr(scheduler_script_policy.subprocess, "run", fake_run)

    resolved = scheduler_script_policy.resolve_scheduler_script(
        root,
        "tools/run_job.py",
    )

    assert resolved == script
    assert len(calls) == 2
    for command, kwargs in calls:
        assert command[:3] == ["git", "-c", f"safe.directory={root}"]
        assert kwargs["cwd"] == root
        assert kwargs["check"] is True
        assert kwargs["capture_output"] is True
        assert kwargs["text"] is True
        assert kwargs["env"]["GIT_OPTIONAL_LOCKS"] == "0"
        assert kwargs["timeout"] == 30
        assert "diff" not in command


def test_production_script_policy_binds_manifest_runtime_head_and_file(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root, script, head = _repository(tmp_path)
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_CODE_ROOT", str(root))
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", head)
    monkeypatch.setattr(
        scheduler_script_policy,
        "verify_runtime_release_manifest",
        lambda _root: {
            "verified": True,
            "manifest": {"release_id": head},
        },
    )

    assert scheduler_script_policy.resolve_scheduler_script(
        root, "tools/run_job.py"
    ) == script

    script.write_text("print('drift')\n", encoding="utf-8", newline="\n")
    with pytest.raises(
        scheduler_script_policy.SchedulerScriptPolicyError,
        match="Git blob differs",
    ):
        scheduler_script_policy.resolve_scheduler_script(
            root, "tools/run_job.py"
        )


def test_windows_qmt_edge_uses_exact_git_identity_without_linux_manifest(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root, script, head = _repository(tmp_path)
    monkeypatch.setattr(scheduler_script_policy.os, "name", "nt")
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_CODE_ROOT", str(root))
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", head)
    monkeypatch.setenv("PROBIGA_SCHEDULER_EXECUTOR_ROLE", "qmt_windows_edge")
    monkeypatch.setattr(
        scheduler_script_policy,
        "verify_runtime_release_manifest",
        lambda _root: (_ for _ in ()).throw(
            AssertionError("Linux release manifest must not be read")
        ),
    )

    assert scheduler_script_policy.resolve_scheduler_script(
        root, "tools/run_job.py"
    ) == script


def test_git_policy_accepts_clean_windows_crlf_materialization(
    tmp_path: Path,
) -> None:
    root, script, _head = _repository(tmp_path)
    script.write_bytes(b"print('ok')\r\n")

    assert scheduler_script_policy.resolve_scheduler_script(
        root, "tools/run_job.py"
    ) == script


@pytest.mark.parametrize(
    "script_path",
    [
        "tools/sync_bigqmt_reference.py",
        "tools/sync_qmt_index_edge.py",
        "tools/sync_qmt_realtime.py",
    ],
)
def test_git_policy_accepts_real_windows_qmt_scheduler_scripts(
    script_path: str,
) -> None:
    root = Path(__file__).resolve().parents[1]

    assert scheduler_script_policy.resolve_scheduler_script(
        root,
        script_path,
    ) == (root / script_path)


def test_git_policy_rejects_non_line_ending_drift_with_crlf(
    tmp_path: Path,
) -> None:
    root, script, _head = _repository(tmp_path)
    script.write_bytes(b"print('drift')\r\n")

    with pytest.raises(
        scheduler_script_policy.SchedulerScriptPolicyError,
        match="Git blob differs",
    ):
        scheduler_script_policy.resolve_scheduler_script(
            root, "tools/run_job.py"
        )


def test_git_policy_rejects_isolated_carriage_return(
    tmp_path: Path,
) -> None:
    root, script, _head = _repository(tmp_path)
    script.write_bytes(b"print('ok')\r")

    with pytest.raises(
        scheduler_script_policy.SchedulerScriptPolicyError,
        match="isolated CR byte",
    ):
        scheduler_script_policy.resolve_scheduler_script(
            root, "tools/run_job.py"
        )


def test_production_script_policy_rejects_manifest_or_index_identity_drift(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root, script, head = _repository(tmp_path)
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_CODE_ROOT", str(root))
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", head)
    manifest = {
        "verified": True,
        "manifest": {"release_id": head},
    }
    monkeypatch.setattr(
        scheduler_script_policy,
        "verify_runtime_release_manifest",
        lambda _root: manifest,
    )

    script.write_text("print('staged')\n", encoding="utf-8", newline="\n")
    _git(root, "add", "tools/run_job.py")
    script.write_text("print('ok')\n", encoding="utf-8", newline="\n")
    with pytest.raises(
        scheduler_script_policy.SchedulerScriptPolicyError,
        match="Git blob differs",
    ):
        scheduler_script_policy.resolve_scheduler_script(
            root, "tools/run_job.py"
        )

    _git(root, "reset")
    manifest["manifest"] = {"release_id": "b" * 40}
    with pytest.raises(
        scheduler_script_policy.SchedulerScriptPolicyError,
        match="manifest and Git HEAD differ",
    ):
        scheduler_script_policy.resolve_scheduler_script(
            root, "tools/run_job.py"
        )


def test_git_policy_retries_timeout_once_without_bypassing_identity(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path.resolve()
    script = root / "tools" / "run_job.py"
    script.parent.mkdir()
    script.write_text("print('ok')\n", encoding="utf-8", newline="\n")
    calls = 0

    def timeout(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        raise subprocess.TimeoutExpired("git", 30)

    monkeypatch.setattr(scheduler_script_policy.subprocess, "run", timeout)

    with pytest.raises(
        scheduler_script_policy.SchedulerScriptPolicyError,
        match=(
            "unchanged file from Git HEAD: command=rev-parse HEAD "
            "error=TimeoutExpired"
        ),
    ):
        scheduler_script_policy.resolve_scheduler_script(
            root, "tools/run_job.py"
        )
    assert calls == 2
