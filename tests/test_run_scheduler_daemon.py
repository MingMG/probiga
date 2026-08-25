from __future__ import annotations

import os

import pytest

from tools import run_scheduler_daemon


def test_windows_env_loader_overrides_runtime_and_removes_launcher_controls(
    monkeypatch,
):
    monkeypatch.setattr(run_scheduler_daemon.os, "name", "nt")
    monkeypatch.setattr(
        "dotenv.dotenv_values",
        lambda _path: {
            "MYSQL_URL": "mysql://runtime",
            "SCHEDULER_INTRADAY_START": "09:20",
        },
    )
    monkeypatch.setenv("MYSQL_URL", "mysql://stale")
    monkeypatch.setenv("PROBIGA_SCHEDULER_STDOUT", "must-not-leak")
    monkeypatch.setattr(
        run_scheduler_daemon,
        "_bind_windows_build_sha",
        lambda: "a" * 40,
    )
    monkeypatch.setattr(
        run_scheduler_daemon,
        "_bind_windows_state_roots",
        lambda: {
            "PROBIGA_JOB_LOG_ROOT": r"C:\ProgramData\ProBigA\jobs",
            "PROBIGA_SCHEDULER_STATE_ROOT": (
                r"C:\ProgramData\ProBigA\scheduler"
            ),
        },
    )

    loaded = run_scheduler_daemon._load_windows_runtime_env()

    assert loaded == 2
    assert os.environ["MYSQL_URL"] == "mysql://runtime"
    assert os.environ["SCHEDULER_INTRADAY_START"] == "09:20"
    assert "PROBIGA_SCHEDULER_STDOUT" not in os.environ
    assert os.environ["PROBIGA_SCHEDULER_EXECUTOR_ROLE"] == "qmt_windows_edge"


def test_windows_build_identity_is_bound_to_exact_checkout(monkeypatch):
    sha = "a" * 40
    monkeypatch.delenv("PROBIGA_BUILD_COMMIT_SHA", raising=False)
    monkeypatch.delenv("PROBIGA_EXPECTED_GIT_SHA", raising=False)
    monkeypatch.setattr(
        run_scheduler_daemon.subprocess,
        "run",
        lambda *_args, **_kwargs: type(
            "Completed", (), {"returncode": 0, "stdout": sha + "\n"}
        )(),
    )

    assert run_scheduler_daemon._bind_windows_build_sha() == sha
    assert os.environ["PROBIGA_BUILD_COMMIT_SHA"] == sha

    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", "b" * 40)
    with pytest.raises(RuntimeError, match="differs"):
        run_scheduler_daemon._bind_windows_build_sha()


def test_non_windows_env_loader_is_a_noop(monkeypatch):
    monkeypatch.setattr(run_scheduler_daemon.os, "name", "posix")
    monkeypatch.setenv("PROBIGA_SCHEDULER_STDOUT", "untouched")

    assert run_scheduler_daemon._load_windows_runtime_env() == 0
    assert os.environ["PROBIGA_SCHEDULER_STDOUT"] == "untouched"


def test_windows_edge_has_explicit_autostart_installer():
    installer = (
        run_scheduler_daemon.ROOT
        / "tools"
        / "register_qmt_windows_edge_scheduler_task.ps1"
    ).read_text(encoding="utf-8")

    assert '"ProBigA QMT Windows Edge Scheduler"' in installer
    assert "Register-ScheduledTask" in installer
    assert "Start-ScheduledTask" in installer
    assert "run_local_scheduler_task.ps1" in installer
    assert "update_qmt_windows_edge.ps1" in installer
    assert '"ProBigA QMT Windows Edge Updater"' in installer
    assert "New-TimeSpan -Minutes 5" in installer
    assert "ProBigA\\scheduler" in installer
    assert "ProBigA\\jobs" in installer
    assert "-MultipleInstances IgnoreNew" in installer


def test_windows_scheduler_wrapper_writes_only_to_protected_programdata():
    wrapper = (
        run_scheduler_daemon.ROOT
        / "tools"
        / "run_local_scheduler_task.ps1"
    ).read_text(encoding="utf-8")

    assert 'Join-Path $ProgramDataRoot "ProBigA\\scheduler"' in wrapper
    assert 'Join-Path $ProgramDataRoot "ProBigA\\jobs"' in wrapper
    assert 'Join-Path $ExpectedRoot "data"' not in wrapper
    assert "ReparsePoint" in wrapper
    assert "$env:PROBIGA_JOB_LOG_ROOT = $JobLogRoot" in wrapper


def test_windows_edge_updater_is_clean_fast_forward_only_and_restarts():
    updater = (
        run_scheduler_daemon.ROOT
        / "tools"
        / "update_qmt_windows_edge.ps1"
    ).read_text(encoding="utf-8")

    assert '"status", "--porcelain", "--untracked-files=normal"' in updater
    assert '"merge-base", "--is-ancestor"' not in updater
    assert "merge-base --is-ancestor HEAD origin/main" in updater
    assert '"merge", "--ff-only", "origin/main"' in updater
    assert "Stop-ScheduledTask" in updater
    assert "backfill_guojin_qmt_local_history.py" in updater
    assert "init --windows-local-option-file --json" in updater
    assert "Start-ScheduledTask" in updater
    assert "git reset" not in updater.lower()
    assert "git clean" not in updater.lower()
    assert "ReparsePoint" in updater


def test_windows_state_roots_are_bound_outside_source_tree(monkeypatch, tmp_path):
    program_data = tmp_path / "ProgramData"
    jobs = program_data / "ProBigA" / "jobs"
    scheduler = program_data / "ProBigA" / "scheduler"
    jobs.mkdir(parents=True)
    scheduler.mkdir(parents=True)
    monkeypatch.setenv("ProgramData", str(program_data))

    result = run_scheduler_daemon._bind_windows_state_roots()

    assert result["PROBIGA_JOB_LOG_ROOT"] == str(jobs.resolve())
    assert result["PROBIGA_SCHEDULER_STATE_ROOT"] == str(
        scheduler.resolve()
    )
    assert os.environ["PROBIGA_API_SCHEDULER_POLL_SECONDS"] == "60"
