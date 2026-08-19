from __future__ import annotations

from pathlib import Path

from server.common import scheduler_script_policy


def test_git_policy_trusts_only_release_root_without_optional_locks(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path.resolve()
    script = root / "tools" / "run_job.py"
    script.parent.mkdir()
    script.write_text("print('ok')\n", encoding="utf-8")
    calls: list[tuple[list[str], dict[str, object]]] = []

    def fake_run(command, **kwargs):
        calls.append((list(command), dict(kwargs)))
        return None

    monkeypatch.setattr(scheduler_script_policy.subprocess, "run", fake_run)

    resolved = scheduler_script_policy.resolve_scheduler_script(
        root,
        "tools/run_job.py",
    )

    assert resolved == script
    assert len(calls) == 3
    for command, kwargs in calls:
        assert command[:3] == ["git", "-c", f"safe.directory={root}"]
        assert kwargs["cwd"] == root
        assert kwargs["check"] is True
        assert kwargs["capture_output"] is True
        assert kwargs["env"]["GIT_OPTIONAL_LOCKS"] == "0"

    assert calls[0][0][3:] == [
        "ls-files",
        "--error-unmatch",
        "--",
        "tools/run_job.py",
    ]
    assert calls[1][0][3:] == [
        "diff",
        "--quiet",
        "HEAD",
        "--",
        "tools/run_job.py",
    ]
    assert calls[2][0][3:] == [
        "diff",
        "--cached",
        "--quiet",
        "HEAD",
        "--",
        "tools/run_job.py",
    ]
