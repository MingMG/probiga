from __future__ import annotations

from pathlib import Path

import pytest

from server.common.scheduler_script_policy import (
    SchedulerScriptPolicyError,
    resolve_scheduler_script,
)


def test_scheduler_script_policy_accepts_owned_python_file(tmp_path: Path) -> None:
    script = tmp_path / "tools" / "job.py"
    script.parent.mkdir()
    script.write_text("print('ok')\n", encoding="utf-8")

    assert resolve_scheduler_script(
        tmp_path,
        "tools/job.py",
        require_git_tracked=False,
    ) == script.resolve()


@pytest.mark.parametrize(
    "script_path",
    [
        "../outside.py",
        "/tmp/outside.py",
        "tools\\job.py",
        "tools/trading_v4/run_research.py",
        "server/trading_v5/models.py",
        "tools/research_trading_v6_campaign.py",
        "tools/job.txt",
    ],
)
def test_scheduler_script_policy_blocks_escape_and_research_paths(
    tmp_path: Path,
    script_path: str,
) -> None:
    with pytest.raises(SchedulerScriptPolicyError):
        resolve_scheduler_script(
            tmp_path,
            script_path,
            require_git_tracked=False,
        )


def test_scheduler_script_policy_rejects_symlink(tmp_path: Path) -> None:
    target = tmp_path / "target.py"
    target.write_text("print('target')\n", encoding="utf-8")
    link = tmp_path / "tools" / "job.py"
    link.parent.mkdir()
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("symlink creation is unavailable on this host")

    with pytest.raises(SchedulerScriptPolicyError, match="symlink|reparse"):
        resolve_scheduler_script(
            tmp_path,
            "tools/job.py",
            require_git_tracked=False,
        )


def test_scheduler_script_policy_rejects_untracked_repository_file(
    tmp_path: Path,
) -> None:
    script = tmp_path / "tools" / "job.py"
    script.parent.mkdir()
    script.write_text("print('untracked')\n", encoding="utf-8")

    with pytest.raises(SchedulerScriptPolicyError, match="Git HEAD"):
        resolve_scheduler_script(tmp_path, "tools/job.py")
