from __future__ import annotations

import os
from datetime import datetime, timezone

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
            "QMT_PYTHON": r"E:\My Code\ProBigA\runtime\stale.exe",
        },
    )
    monkeypatch.setenv("MYSQL_URL", "mysql://stale")
    monkeypatch.setenv("PROBIGA_SCHEDULER_STDOUT", "must-not-leak")
    # Track every variable mutated directly by the loader so this test cannot
    # leak the Windows executor identity into later in-process test modules.
    monkeypatch.setenv("PROBIGA_SCHEDULER_EXECUTOR_ROLE", "stale")
    monkeypatch.setenv("PROBIGA_JOB_LOG_ROOT", "stale")
    monkeypatch.setenv("PROBIGA_SCHEDULER_STATE_ROOT", "stale")
    monkeypatch.setattr(
        run_scheduler_daemon,
        "_bind_windows_build_sha",
        lambda: "a" * 40,
    )
    monkeypatch.setattr(
        run_scheduler_daemon,
        "_bind_windows_state_roots",
        lambda: (
            os.environ.update({
                "PROBIGA_JOB_LOG_ROOT": r"C:\ProgramData\ProBigA\jobs",
                "PROBIGA_SCHEDULER_STATE_ROOT": (
                    r"C:\ProgramData\ProBigA\scheduler"
                ),
            })
            or {
                "PROBIGA_JOB_LOG_ROOT": r"C:\ProgramData\ProBigA\jobs",
                "PROBIGA_SCHEDULER_STATE_ROOT": (
                    r"C:\ProgramData\ProBigA\scheduler"
                ),
            }
        ),
    )
    monkeypatch.setattr(
        run_scheduler_daemon,
        "_bind_windows_qmt_python",
        lambda: os.environ.__setitem__(
            "QMT_PYTHON",
            r"E:\My Code\ProBigA-qmt-production\runtime\qmt-py313\Scripts\python.exe",
        ),
    )

    loaded = run_scheduler_daemon._load_windows_runtime_env()

    assert loaded == 3
    assert os.environ["MYSQL_URL"] == "mysql://runtime"
    assert os.environ["SCHEDULER_INTRADAY_START"] == "09:20"
    assert "PROBIGA_SCHEDULER_STDOUT" not in os.environ
    assert os.environ["PROBIGA_SCHEDULER_EXECUTOR_ROLE"] == "qmt_windows_edge"
    assert os.environ["PROBIGA_SCHEDULER_STATE_ROOT"] == (
        r"C:\ProgramData\ProBigA\scheduler"
    )
    assert os.environ["QMT_PYTHON"].startswith(
        r"E:\My Code\ProBigA-qmt-production"
    )


def test_windows_qmt_python_is_frozen_inside_checkout(monkeypatch, tmp_path):
    root = tmp_path / "production-checkout"
    qmt_python = root / "runtime/qmt-py313/Scripts/python.exe"
    qmt_python.parent.mkdir(parents=True)
    qmt_python.touch()
    monkeypatch.setattr(run_scheduler_daemon, "ROOT", root)
    monkeypatch.setenv("QMT_PYTHON", r"E:\dirty-user-tree\python.exe")

    assert run_scheduler_daemon._bind_windows_qmt_python() == str(
        qmt_python.resolve()
    )
    assert os.environ["QMT_PYTHON"] == str(qmt_python.resolve())


def test_windows_qmt_python_missing_from_checkout_fails_closed(
    monkeypatch,
    tmp_path,
):
    root = tmp_path / "production-checkout"
    root.mkdir()
    monkeypatch.setattr(run_scheduler_daemon, "ROOT", root)

    with pytest.raises(RuntimeError, match="runtime is unavailable"):
        run_scheduler_daemon._bind_windows_qmt_python()


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
    assert '@("fetch", "--prune", "origin", "main")' in installer
    assert '@("symbolic-ref", "--short", "HEAD")' in installer
    assert '"status", "--porcelain", "--untracked-files=normal"' in installer
    assert '@("rev-parse", "origin/main")' in installer
    assert '$CurrentSha -cne $TargetSha' in installer
    assert "registration refused" in installer
    assert '[string]$ProductionRoot' in installer
    assert '"^[A-Za-z]:[\\\\/]"' in installer
    assert "https://github.com/MingMG/probiga.git" in installer
    assert '"System32\\WindowsPowerShell\\v1.0\\powershell.exe"' in installer
    assert "-Execute $PowerShellExe" in installer
    assert installer.count("-WorkingDirectory $ExpectedRoot") == 2
    assert '@("rev-parse", "--show-toplevel")' in installer
    assert '@("remote", "get-url", "origin")' in installer
    assert installer.index('@("remote", "get-url", "origin")') < (
        installer.index('@("fetch", "--prune", "origin", "main")')
    )
    assert 'Join-Path $Root ".env"' in installer
    assert 'Join-Path $Root "runtime\\qmt-py313\\Scripts\\python.exe"' in installer
    assert '-RegisteredRoot `"$ExpectedRoot`"' in installer
    assert "$Registered.Actions[0].Arguments -cne $SchedulerArgument" in installer
    assert "$RegisteredUpdater.Actions[0].Arguments -cne $UpdaterArgument" in installer
    assert "Stop-ExistingTask $UpdateTaskName" in installer
    assert "Stop-ExistingTask $TaskName" in installer
    assert installer.index("Stop-ExistingTask $TaskName") < installer.index(
        "Register-ScheduledTask"
    )
    assert installer.index("scheduled task registration/root binding differs") < (
        installer.index("Start-ScheduledTask -TaskName $TaskName")
    )
    assert 'GetFullPath("E:\\My Code\\ProBigA")' not in installer
    assert "git reset" not in installer.lower()
    assert "git clean" not in installer.lower()


def test_windows_powershell_git_wrappers_ignore_successful_fetch_stderr():
    for name in (
        "register_qmt_windows_edge_scheduler_task.ps1",
        "run_local_scheduler_task.ps1",
        "update_qmt_windows_edge.ps1",
    ):
        source = (
            run_scheduler_daemon.ROOT / "tools" / name
        ).read_text(encoding="utf-8")
        assert "@Arguments 2>$null" in source
        assert "@Arguments 2>&1" not in source
        assert '$ErrorActionPreference = "Continue"' in source
        assert "$ErrorActionPreference = $PreviousPreference" in source
        assert "$ExitCode = $LASTEXITCODE" in source
        assert "$ExitCode -ne 0" in source


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
    assert '[string]$RegisteredRoot' in wrapper
    assert 'Get-ScheduledTask -TaskName $SchedulerTaskName' in wrapper
    assert 'Get-ScheduledTask -TaskName $UpdateTaskName' in wrapper
    assert '@("fetch", "--prune", "origin", "main")' in wrapper
    assert '@("rev-parse", "origin/main")' in wrapper
    assert "$BuildSha -cne $TargetSha" in wrapper
    assert 'Join-Path $ExpectedRoot ".env"' in wrapper
    assert 'Join-Path $ExpectedRoot "runtime\\qmt-py313\\Scripts\\python.exe"' in wrapper
    assert "$env:QMT_PYTHON = $QmtPythonExe" in wrapper
    assert "https://github.com/MingMG/probiga.git" in wrapper
    assert '"System32\\WindowsPowerShell\\v1.0\\powershell.exe"' in wrapper
    assert "$Registered.Actions[0].Execute -ine $PowerShellExe" in wrapper
    assert "$Registered.Actions[0].WorkingDirectory -ine $ExpectedRoot" in wrapper
    assert 'GetFullPath("E:\\My Code\\ProBigA")' not in wrapper
    assert "ProBigASchedulerJob" in wrapper
    assert "AssignProcessToJobObject" in wrapper
    assert "0x00002000" in wrapper
    assert "$Job.Assign($Process)" in wrapper
    assert "$Process.WaitForExit()" in wrapper


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
    assert "validate-schema --windows-local-option-file --json" in updater
    assert "init --windows-local-option-file --json" not in updater
    assert "$SchemaValidationExit -ne 0" in updater
    assert "dedicated privileged migration or boundary" in updater
    assert "Start-ScheduledTask" in updater
    assert "git reset" not in updater.lower()
    assert "git clean" not in updater.lower()
    assert "ReparsePoint" in updater
    assert '[string]$RegisteredRoot' in updater
    assert 'Get-ScheduledTask -TaskName $SchedulerTaskName' in updater
    assert 'Get-ScheduledTask -TaskName $UpdateTaskName' in updater
    assert "$Registered.Actions[0].Arguments -cne $SchedulerArgument" in updater
    assert "$RegisteredUpdater.Actions[0].Arguments -cne $UpdaterArgument" in updater
    assert '@("rev-parse", "--show-toplevel")' in updater
    assert '@("remote", "get-url", "origin")' in updater
    assert 'Join-Path $ExpectedRoot ".env"' in updater
    assert 'Join-Path $ExpectedRoot "runtime\\qmt-py313\\Scripts\\python.exe"' in updater
    assert "$env:QMT_PYTHON = $QmtPythonExe" in updater
    assert "https://github.com/MingMG/probiga.git" in updater
    assert '"System32\\WindowsPowerShell\\v1.0\\powershell.exe"' in updater
    assert "$Registered.Actions[0].Execute -ine $PowerShellExe" in updater
    assert "$Registered.Actions[0].WorkingDirectory -ine $ExpectedRoot" in updater
    assert 'GetFullPath("E:\\My Code\\ProBigA")' not in updater
    assert '"scheduler-runtime.json"' in updater
    assert '"scheduler-shutdown-request.json"' in updater
    assert '"scheduler-shutdown-receipt.json"' in updater
    assert "$Runtime.instance_id" in updater
    assert "$Runtime.pid" in updater
    assert "$Runtime.build_sha" in updater
    assert "$Runtime.heartbeat_at_utc" in updater
    assert "$Receipt.request_uid" in updater
    assert '$TaskState -ne "Running"' in updater
    assert "process tree did not stop within 120 seconds" in updater


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


def test_shutdown_request_requires_exact_fresh_runtime_identity():
    identity = {
        "instance_id": "11111111-1111-1111-1111-111111111111",
        "pid": 4321,
        "build_sha": "a" * 40,
    }
    payload = {
        "schema_version": 1,
        "request_uid": "22222222-2222-2222-2222-222222222222",
        **identity,
        "requested_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    assert run_scheduler_daemon._shutdown_request_matches(
        payload,
        identity=identity,
    )
    for key, value in (
        ("instance_id", "33333333-3333-3333-3333-333333333333"),
        ("pid", 4322),
        ("build_sha", "b" * 40),
    ):
        mismatched = dict(payload)
        mismatched[key] = value
        assert not run_scheduler_daemon._shutdown_request_matches(
            mismatched,
            identity=identity,
        )
