from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
from datetime import datetime, timezone

import pytest

from tools import run_scheduler_daemon


def _powershell_function(source: str, name: str) -> str:
    start = source.index(f"function {name}")
    brace = source.index("{", start)
    depth = 0
    for index in range(brace, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return source[start : index + 1]
    raise AssertionError(f"unterminated PowerShell function: {name}")


def _run_powershell_json(program: str) -> dict[str, object]:
    executable = shutil.which("powershell.exe") or shutil.which("pwsh")
    assert executable is not None, "PowerShell is required for release tests"
    encoded = base64.b64encode(program.encode("utf-16-le")).decode("ascii")
    completed = subprocess.run(
        [
            executable,
            "-NoProfile",
            "-NonInteractive",
            "-EncodedCommand",
            encoded,
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=10,
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout.strip())


def _run_powershell_file_json(program: str, path) -> dict[str, object]:
    executable = shutil.which("powershell.exe") or shutil.which("pwsh")
    assert executable is not None, "PowerShell is required for release tests"
    path.write_text(program, encoding="utf-8")
    completed = subprocess.run(
        [
            executable,
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(path),
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=10,
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout.strip())


def _powershell_single_quoted(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _canonical_digest(value: dict[str, object]) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _activation_payload(
    *,
    status: str = "READY",
    include_hold: bool = True,
) -> dict[str, object]:
    build_sha = "a" * 40
    attempt_id = "b" * 32 if include_hold else ""
    hold = None
    grant = None
    if include_hold:
        unsigned_hold: dict[str, object] = {
            "schema": "probiga.qmt-windows-edge-release-quiescence.v1",
            "build_sha": build_sha,
            "deployment_attempt_id": attempt_id,
            "hold_run_uid": f"qmt-edge-hold-{attempt_id}",
            "request_run_uid": f"qmt-edge-request-{build_sha}",
            "requested_at": "2026-09-04T09:00:00",
            "real_order": False,
        }
        hold = {**unsigned_hold, "hold_hash": _canonical_digest(unsigned_hold)}
        if status == "READY":
            unsigned_grant: dict[str, object] = {
                "schema": "probiga.qmt-windows-edge-release-activation.v1",
                "build_sha": build_sha,
                "deployment_attempt_id": attempt_id,
                "grant_run_uid": f"qmt-edge-grant-{attempt_id}",
                "hold_run_uid": hold["hold_run_uid"],
                "hold_hash": hold["hold_hash"],
                "granted_at": "2026-09-04T09:01:00",
                "schema_cutover_verified": True,
                "real_order": False,
            }
            grant = {
                **unsigned_grant,
                "grant_hash": _canonical_digest(unsigned_grant),
            }
    ready = status == "READY"
    return {
        "mode": "check-activation",
        "status": status,
        "build_sha": build_sha,
        "deployment_attempt_id": attempt_id,
        "activation_granted": ready,
        "reason_code": "" if ready else "QMT_EDGE_RELEASE_ACTIVATION_PENDING",
        "hold": hold,
        "grant": grant,
        "database_writes": False,
    }


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
            "PROBIGA_DEPLOYMENT_MODE": "development",
            "PROBIGA_CODE_ROOT": r"E:\My Code\stale-release",
        },
    )
    monkeypatch.setenv("MYSQL_URL", "mysql://stale")
    monkeypatch.setenv("PROBIGA_SCHEDULER_STDOUT", "must-not-leak")
    # Track every variable mutated directly by the loader so this test cannot
    # leak the Windows executor identity into later in-process test modules.
    monkeypatch.setenv("PROBIGA_SCHEDULER_EXECUTOR_ROLE", "stale")
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "staging")
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

    assert loaded == 5
    assert os.environ["MYSQL_URL"] == "mysql://runtime"
    assert os.environ["SCHEDULER_INTRADAY_START"] == "09:20"
    assert "PROBIGA_SCHEDULER_STDOUT" not in os.environ
    assert os.environ["PROBIGA_DEPLOYMENT_MODE"] == "production"
    assert os.environ["PROBIGA_CODE_ROOT"] == str(
        run_scheduler_daemon.ROOT.resolve()
    )
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


def test_windows_daemon_activation_gate_accepts_only_exact_ready_proof(
    monkeypatch,
):
    sha = "a" * 40
    monkeypatch.setattr(run_scheduler_daemon.os, "name", "nt")

    def completed(returncode, payload, stderr=""):
        return type(
            "Completed",
            (),
            {
                "returncode": returncode,
                "stdout": __import__("json").dumps(payload),
                "stderr": stderr,
            },
        )()

    observed = []
    monkeypatch.setattr(
        run_scheduler_daemon.subprocess,
        "run",
        lambda args, **kwargs: (
            observed.append((args, kwargs))
            or completed(0, _activation_payload())
        ),
    )

    assert run_scheduler_daemon._windows_release_activation_granted(sha)
    assert "--check-activation" in observed[0][0]
    assert observed[0][0][-3:] == [
        "--expected-build-sha",
        sha,
        "--compact",
    ]
    assert observed[0][1]["check"] is False

    monkeypatch.setattr(
        run_scheduler_daemon.subprocess,
        "run",
        lambda *_args, **_kwargs: completed(
            4,
            _activation_payload(status="PENDING"),
        ),
    )
    assert not run_scheduler_daemon._windows_release_activation_granted(sha)

    monkeypatch.setattr(
        run_scheduler_daemon.subprocess,
        "run",
        lambda *_args, **_kwargs: completed(
            4,
            _activation_payload(status="PENDING", include_hold=False),
        ),
    )
    assert not run_scheduler_daemon._windows_release_activation_granted(sha)

    monkeypatch.setattr(
        run_scheduler_daemon.subprocess,
        "run",
        lambda *_args, **_kwargs: completed(
            0,
            _activation_payload(status="PENDING"),
        ),
    )
    with pytest.raises(RuntimeError, match="activation proof differs"):
        run_scheduler_daemon._windows_release_activation_granted(sha)


@pytest.mark.parametrize(
    "tamper",
    [
        pytest.param(
            lambda value: value.update({"unexpected": True}),
            id="extra-top-level-field",
        ),
        pytest.param(
            lambda value: value.update({"database_writes": 0}),
            id="non-boolean-database-writes",
        ),
        pytest.param(
            lambda value: value.update({"deployment_attempt_id": "0" * 32}),
            id="zero-attempt",
        ),
        pytest.param(
            lambda value: value["hold"].update({"hold_hash": "c" * 64}),
            id="hold-hash",
        ),
        pytest.param(
            lambda value: value["grant"].update(
                {"hold_run_uid": "qmt-edge-hold-" + "c" * 32}
            ),
            id="grant-hold-binding",
        ),
        pytest.param(
            lambda value: value["grant"].update({"real_order": 0}),
            id="non-boolean-real-order",
        ),
        pytest.param(
            lambda value: value["grant"].update(
                {"granted_at": "2026-09-04T08:59:59"}
            ),
            id="grant-predates-hold",
        ),
    ],
)
def test_windows_daemon_activation_gate_rejects_tampered_nested_proof(
    monkeypatch,
    tamper,
):
    sha = "a" * 40
    payload = _activation_payload()
    tamper(payload)
    monkeypatch.setattr(run_scheduler_daemon.os, "name", "nt")
    monkeypatch.setattr(
        run_scheduler_daemon.subprocess,
        "run",
        lambda *_args, **_kwargs: type(
            "Completed",
            (),
            {
                "returncode": 0,
                "stdout": json.dumps(payload),
                "stderr": "",
            },
        )(),
    )

    with pytest.raises(RuntimeError, match="activation proof differs"):
        run_scheduler_daemon._windows_release_activation_granted(sha)


def test_windows_daemon_checks_activation_before_runtime_or_heartbeat():
    source = (
        run_scheduler_daemon.ROOT / "tools" / "run_scheduler_daemon.py"
    ).read_text(encoding="utf-8")
    main = source.index("def main() -> int:")
    gate = source.index(
        "_windows_release_activation_granted(build_sha)",
        main,
    )
    runtime_import = source.index(
        "from server.api.scheduler_runtime import (",
        main,
    )
    local_heartbeat = source.index(
        "_start_windows_shutdown_monitor(build_sha=build_sha)",
        main,
    )
    dispatch = source.index("run_scheduler_forever(", main)

    assert gate < runtime_import < local_heartbeat < dispatch


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
    assert '"System32\\wscript.exe"' in installer
    assert "-Execute $PowerShellExe" in installer
    assert "-Execute $WScriptExe" in installer
    assert '"//B //NoLogo `"$UpdaterLauncher`" `"$ExpectedRoot`""' in installer
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
    assert "$RegisteredUpdater.Actions[0].Execute -ine $WScriptExe" in installer
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


def test_windows_edge_updater_uses_a_windowless_launcher():
    launcher = (
        run_scheduler_daemon.ROOT
        / "tools"
        / "run_hidden_qmt_updater.vbs"
    ).read_text(encoding="utf-8")

    assert "Option Explicit" in launcher
    assert "WScript.Arguments" in launcher
    assert "WScript.ScriptFullName" in launcher
    assert '"tools\\update_qmt_windows_edge.ps1"' in launcher
    assert '" -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass"' in launcher
    assert '" -WindowStyle Hidden -File "' in launcher
    assert "shell.Run(command, 0, True)" in launcher

    for name in (
        "register_qmt_windows_edge_scheduler_task.ps1",
        "run_local_scheduler_task.ps1",
        "update_qmt_windows_edge.ps1",
    ):
        source = (
            run_scheduler_daemon.ROOT / "tools" / name
        ).read_text(encoding="utf-8")
        assert '"System32\\wscript.exe"' in source
        assert '"tools\\run_hidden_qmt_updater.vbs"' in source
        assert '"//B //NoLogo `"$UpdaterLauncher`" `"$ExpectedRoot`""' in source
        assert "$RegisteredUpdater.Actions[0].Execute -ine $WScriptExe" in source


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
    assert "merge-base --is-ancestor $TargetSha origin/main" in updater
    assert "merge-base --is-ancestor HEAD $TargetSha" in updater
    assert '@("merge", "--ff-only", $TargetSha)' in updater
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
    activation_contract = updater.index(
        "function Test-QmtReleaseActivationPayload"
    )
    activation_helper = updater.index("function Confirm-QmtReleaseActivation")
    production_binding = updater.index(
        '$env:PROBIGA_DEPLOYMENT_MODE = "production"'
    )
    first_release_check = updater.index("--check-request")
    equal_sha = updater.index("if ($CurrentSha -ceq $TargetSha)")
    equal_sha_gate = updater.index(
        "Confirm-QmtReleaseActivation $TargetSha",
        equal_sha,
    )
    equal_sha_ready = updater.index(
        "Invoke-ReadOnlyStrategyPreflight $TargetSha",
        equal_sha,
    )
    fast_forward = updater.index('@("merge", "--ff-only", $TargetSha)')
    updated_sha = updater.index("$CurrentSha = $UpdatedSha", fast_forward)
    post_fast_forward_gate = updater.index(
        "Confirm-QmtReleaseActivation $CurrentSha",
        updated_sha,
    )
    final_gate = updater.rindex("Confirm-QmtReleaseActivation $CurrentSha")
    scheduler_start = updater.index("Start-EdgeScheduler", final_gate)
    helper = updater[activation_helper:equal_sha]

    assert activation_contract < activation_helper < equal_sha
    assert production_binding < first_release_check < equal_sha_gate
    assert equal_sha_gate < equal_sha_ready
    assert fast_forward < updated_sha < post_fast_forward_gate
    assert post_fast_forward_gate < final_gate < scheduler_start
    assert "--check-activation" in helper
    assert "ConvertFrom-Json -ErrorAction Stop" in helper
    assert "$global:LASTEXITCODE = -1" in helper
    assert "$ActivationExit = $global:LASTEXITCODE" in helper
    for field in (
        "mode",
        "status",
        "build_sha",
        "activation_granted",
        "database_writes",
    ):
        assert f"$ActivationPayload.{field}" in helper
    assert "Test-QmtReleaseActivationPayload" not in helper
    assert '$ActivationPayload.status -ceq "READY"' in helper
    assert '$ActivationPayload.status -ceq "PENDING"' in helper
    assert "$ActivationPayload.build_sha -ceq $ExpectedBuild" in helper
    assert "Stop-EdgeScheduler" in helper
    assert "exit 0" in helper
    assert "release activation proof failed closed" in helper


@pytest.mark.parametrize(
    ("activation_output", "expect_stopped", "expect_failure"),
    [
        pytest.param("not-json", True, True, id="malformed-json"),
        pytest.param(
            json.dumps({**_activation_payload(), "activation_granted": False}),
            True,
            True,
            id="false-ready",
        ),
        pytest.param(
            json.dumps(_activation_payload()),
            False,
            False,
            id="exact-ready-control",
        ),
    ],
)
def test_windows_edge_updater_activation_ready_proof_fails_closed(
    activation_output,
    expect_stopped,
    expect_failure,
    tmp_path,
):
    updater = (
        run_scheduler_daemon.ROOT
        / "tools"
        / "update_qmt_windows_edge.ps1"
    ).read_text(encoding="utf-8")
    contract = _powershell_function(
        updater,
        "Test-QmtReleaseActivationPayload",
    )
    helper = _powershell_function(updater, "Confirm-QmtReleaseActivation")
    output_literal = _powershell_single_quoted(activation_output)
    program = f"""
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$PythonExe = "Invoke-ActivationStub"
$BootstrapTool = "unused"
$script:ActivationOutput = {output_literal}
$script:Stopped = $false
function Invoke-ActivationStub {{
    Write-Output $script:ActivationOutput
    $global:LASTEXITCODE = 0
}}
function Stop-EdgeScheduler {{
    $script:Stopped = $true
}}
function Write-UpdateLog([string]$Message) {{
}}
{contract}
{helper}
$Failed = $false
$FailureMessage = ""
try {{
    Confirm-QmtReleaseActivation "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
}}
catch {{
    $Failed = $true
    $FailureMessage = $_.Exception.Message
}}
[ordered]@{{
    stopped = $script:Stopped
    failed = $Failed
    failure_message = $FailureMessage
}} | ConvertTo-Json -Compress
"""

    result = _run_powershell_file_json(
        program,
        tmp_path / "activation-ready-proof.ps1",
    )

    assert result["stopped"] is expect_stopped, result
    assert result["failed"] is expect_failure, result
    if expect_failure:
        assert "activation proof failed closed" in result["failure_message"]


@pytest.mark.parametrize("helper_name", ["activation", "preflight"])
@pytest.mark.parametrize(
    ("stub_exit", "startup_failure", "expect_failure"),
    [
        pytest.param(0, False, False, id="exit-zero"),
        pytest.param(4, False, True, id="exit-four"),
        pytest.param(0, True, True, id="startup-failure-after-success"),
    ],
)
def test_windows_edge_helpers_capture_global_native_exit_in_ps5_file(
    helper_name,
    stub_exit,
    startup_failure,
    expect_failure,
    tmp_path,
):
    system_root = os.environ.get("SystemRoot")
    if not system_root:
        pytest.skip("Windows PowerShell 5.1 is only available on Windows")
    powershell = (
        Path(system_root)
        / "System32"
        / "WindowsPowerShell"
        / "v1.0"
        / "powershell.exe"
    )
    if not powershell.is_file():
        pytest.skip("Windows PowerShell 5.1 is required for this regression test")

    updater = (
        run_scheduler_daemon.ROOT
        / "tools"
        / "update_qmt_windows_edge.ps1"
    ).read_text(encoding="utf-8")
    activation = _powershell_function(updater, "Confirm-QmtReleaseActivation")
    preflight = _powershell_function(updater, "Invoke-ReadOnlyStrategyPreflight")
    payload = json.dumps(
        _activation_payload()
        if helper_name == "activation"
        else {"status": "READY"},
        separators=(",", ":"),
    )
    native_stub = tmp_path / "native-status.cmd"
    native_stub.write_text(
        f"@echo off\necho {payload}\nexit /b {stub_exit}\n",
        encoding="ascii",
    )
    executable = (
        "Missing-ProBigA-Native-Command"
        if startup_failure
        else str(native_stub)
    )
    invocation = (
        'Confirm-QmtReleaseActivation "' + "a" * 40 + '"'
        if helper_name == "activation"
        else 'Invoke-ReadOnlyStrategyPreflight "' + "a" * 40 + '"'
    )
    program = f"""
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$script:Stopped = $false
$script:Logs = @()
function Stop-EdgeScheduler {{ $script:Stopped = $true }}
function Write-UpdateLog([string]$Message) {{ $script:Logs += $Message }}
$PythonExe = {_powershell_single_quoted(executable)}
$PowerShellExe = {_powershell_single_quoted(executable)}
$BootstrapTool = "unused"
$StrategyReloader = "unused"
$ExpectedRoot = {_powershell_single_quoted(str(tmp_path))}
{activation}
{preflight}
$script:LASTEXITCODE = 0
$global:LASTEXITCODE = 0
$Failed = $false
$FailureMessage = ""
$Result = $null
try {{
    $Result = {invocation}
}}
catch {{
    $Failed = $true
    $FailureMessage = $_.Exception.Message
}}
[ordered]@{{
    failed = $Failed
    failure_message = $FailureMessage
    result = $Result
    stopped = $script:Stopped
    log_count = $script:Logs.Count
    global_exit = $global:LASTEXITCODE
}} | ConvertTo-Json -Compress
"""
    script = tmp_path / f"{helper_name}-{stub_exit}-{startup_failure}.ps1"
    script.write_text(program, encoding="utf-8")
    completed = subprocess.run(
        [
            str(powershell),
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(script),
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=10,
    )
    assert completed.returncode == 0, completed.stderr
    result = json.loads(completed.stdout.strip())
    assert result["failed"] is expect_failure, result
    if helper_name == "preflight" and not expect_failure:
        assert result["result"] == "READY", result
    if startup_failure:
        assert result["global_exit"] == -1, result
    else:
        assert result["global_exit"] == stub_exit, result


@pytest.mark.parametrize(
    "script_name",
    ["update_qmt_windows_edge.ps1", "reload_big_qmt_strategy.ps1"],
)
def test_powershell_activation_contract_validates_exact_nested_proof(
    script_name,
    tmp_path,
):
    ready = _activation_payload()
    pending_hold = _activation_payload(status="PENDING")
    pending_empty = _activation_payload(
        status="PENDING",
        include_hold=False,
    )
    extra = {**ready, "unexpected": True}
    false_as_integer = {**ready, "database_writes": 0}
    hold_hash = json.loads(json.dumps(ready))
    hold_hash["hold"]["hold_hash"] = "c" * 64
    grant_binding = json.loads(json.dumps(ready))
    grant_binding["grant"]["hold_run_uid"] = "qmt-edge-hold-" + "c" * 32
    grant_timestamp = json.loads(json.dumps(ready))
    grant_timestamp["grant"]["granted_at"] = "2026-09-04T08:59:59"
    cases = {
        "ready": (ready, "READY"),
        "pending_hold": (pending_hold, "PENDING"),
        "pending_empty": (pending_empty, "PENDING"),
        "extra": (extra, ""),
        "false_as_integer": (false_as_integer, ""),
        "hold_hash": (hold_hash, ""),
        "grant_binding": (grant_binding, ""),
        "grant_timestamp": (grant_timestamp, ""),
    }
    source = (
        run_scheduler_daemon.ROOT / "tools" / script_name
    ).read_text(encoding="utf-8")
    contract = _powershell_function(
        source,
        "Test-QmtReleaseActivationPayload",
    )
    checks = []
    for name, (payload, _expected) in cases.items():
        literal = _powershell_single_quoted(json.dumps(payload))
        checks.extend((
            f"$Payload = {literal} | ConvertFrom-Json",
            "$Results[" + _powershell_single_quoted(name) + "] = "
            "Test-QmtReleaseActivationPayload $Payload "
            "\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"",
        ))
    program = f"""
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
{contract}
$Results = [ordered]@{{}}
{chr(10).join(checks)}
$Results | ConvertTo-Json -Compress
"""

    result = _run_powershell_file_json(
        program,
        tmp_path / f"{script_name}.contract-test.ps1",
    )

    assert result == {
        name: expected
        for name, (_payload, expected) in cases.items()
    }


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
