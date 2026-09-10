"""Exercise the Windows updater's local daemon binding under real PowerShell."""

import json
import os
from pathlib import Path
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "tools/update_qmt_windows_edge.ps1").read_text(encoding="utf-8")
BUILD = "a" * 40


def _function(name):
    start = SOURCE.index(f"function {name}(")
    return SOURCE[start : SOURCE.index("\nfunction ", start + 1)]


def _quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def _run_ps(tmp_path, body):
    powershell = (
        Path(os.environ.get("SystemRoot", ""))
        / "System32/WindowsPowerShell/v1.0/powershell.exe"
    )
    if not powershell.is_file():
        pytest.skip("Windows PowerShell 5.1 is required")
    script = tmp_path / "start-identity.ps1"
    script.write_text(
        "$ErrorActionPreference='Stop'\nSet-StrictMode -Version Latest\n" + body,
        encoding="utf-8-sig",
    )
    result = subprocess.run(
        [
            str(powershell), "-NoProfile", "-NonInteractive",
            "-ExecutionPolicy", "Bypass", "-File", str(script),
        ],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
        timeout=20,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout.strip())


def _fixture(tmp_path):
    production = tmp_path / "production root"
    venv = production / ".venv"
    venv.mkdir(parents=True)
    base_python = tmp_path / "base python.exe"
    base_python.write_bytes(b"test fixture; never executed")
    (venv / "pyvenv.cfg").write_text(
        f"executable = {base_python}\n", encoding="utf-8"
    )
    return (
        f"$ExpectedRoot={_quote(production)}\n"
        "$PythonExe=Join-Path $ExpectedRoot '.venv\\Scripts\\python.exe'\n"
        f"$script:EdgeBasePython={_quote(base_python)}\n"
        f"$SchedulerRuntimePath={_quote(tmp_path / 'scheduler-runtime.json')}\n"
        f"$BuildSha='{BUILD}'\n"
        "$SchedulerTaskName='test scheduler'; $script:ForwardOnlySchedulerGate=$false\n"
        "$script:trace=[Collections.Generic.List[string]]::new()\n"
        "$script:task=[pscustomobject]@{State='Running';Settings=[pscustomobject]@{Enabled=$true}}\n"
        "$script:now=[DateTimeOffset]::UtcNow\n"
        "$script:runtime=[ordered]@{schema_version=1;"
        "instance_id='f4f24c18-5e2a-42a3-99aa-3c5f8dd4a74a';pid=7001;"
        "build_sha=$BuildSha;started_at_utc=$script:now.AddSeconds(-39).ToString('o');"
        "heartbeat_at_utc=$script:now.ToString('o')}\n"
        "$script:daemon=[pscustomobject]@{ProcessId=7001;"
        "CreationDate=$script:now.AddSeconds(-40);ExecutablePath=$script:EdgeBasePython;"
        "CommandLine=('\"'+$PythonExe+'\" -P \"'+"
        "(Join-Path $ExpectedRoot 'tools\\run_scheduler_daemon.py')+'\"')}\n"
        "function Write-TestRuntime { $script:runtime | ConvertTo-Json -Compress | "
        "Set-Content -LiteralPath $SchedulerRuntimePath -Encoding UTF8 }\n"
        "function Publish-NewDaemon {\n"
        "  $created=[DateTimeOffset]::UtcNow\n"
        "  $script:runtime.pid=7002\n"
        "  $script:runtime.instance_id='f4f24c18-5e2a-42a3-99aa-3c5f8dd4a74b'\n"
        "  $script:runtime.started_at_utc=$created.ToString('o')\n"
        "  $script:runtime.heartbeat_at_utc=$created.ToString('o')\n"
        "  $script:daemon.ProcessId=7002; $script:daemon.CreationDate=$created\n"
        "  Write-TestRuntime\n"
        "}\n"
        "function Get-CimInstance($ClassName,$Filter,$ErrorAction) {\n"
        "  $script:trace.Add('lookup:'+$Filter)\n"
        "  return $script:daemon\n"
        "}\n"
        "function Get-ScheduledTask($TaskName,$ErrorAction) { return $script:task }\n"
        "function Enable-ScheduledTask($TaskName,$ErrorAction) {\n"
        "  $script:trace.Add('enable'); $script:task.Settings.Enabled=$true\n"
        "}\n"
        "Write-TestRuntime\n"
    )


@pytest.mark.parametrize(
    "case",
    [
        "valid", "missing", "malformed", "wrong_schema", "empty_instance",
        "wrong_build", "stale_heartbeat", "future_heartbeat", "dead_pid",
        "pid_mismatch", "foreign_root", "foreign_executable",
        "process_created_after_runtime", "process_predates_launch",
        "runtime_start_after_heartbeat", "startup_delay_excessive",
    ],
)
def test_local_runtime_must_bind_the_live_exact_daemon(case, tmp_path):
    changes = {
        "valid": "",
        "missing": "Remove-Item -LiteralPath $SchedulerRuntimePath\n",
        "malformed": "Set-Content -LiteralPath $SchedulerRuntimePath -Value '{'\n",
        "wrong_schema": "$script:runtime.schema_version=2; Write-TestRuntime\n",
        "empty_instance": (
            "$script:runtime.instance_id=[Guid]::Empty.ToString(); Write-TestRuntime\n"
        ),
        "wrong_build": "$script:runtime.build_sha=('b'*40); Write-TestRuntime\n",
        "stale_heartbeat": (
            "$script:runtime.heartbeat_at_utc=$script:now.AddSeconds(-20).ToString('o');"
            " Write-TestRuntime\n"
        ),
        "future_heartbeat": (
            "$script:runtime.heartbeat_at_utc=$script:now.AddSeconds(30).ToString('o');"
            " Write-TestRuntime\n"
        ),
        "dead_pid": "$script:daemon=$null\n",
        "pid_mismatch": "$script:daemon.ProcessId=7009\n",
        "foreign_root": (
            "$script:daemon.CommandLine=$script:daemon.CommandLine.Replace("
            "$ExpectedRoot,($ExpectedRoot+'-other'))\n"
        ),
        "foreign_executable": "$script:daemon.ExecutablePath='C:\\other\\python.exe'\n",
        "process_created_after_runtime": (
            "$script:daemon.CreationDate=$script:now.AddSeconds(-30)\n"
        ),
        "process_predates_launch": "$NotBeforeUtc=$script:now.AddSeconds(-20)\n",
        "runtime_start_after_heartbeat": (
            "$script:runtime.started_at_utc=$script:now.AddSeconds(1).ToString('o');"
            " Write-TestRuntime\n"
        ),
        "startup_delay_excessive": (
            "$script:daemon.CreationDate=$script:now.AddSeconds(-300)\n"
        ),
    }
    observed = _run_ps(
        tmp_path,
        _fixture(tmp_path)
        + "$NotBeforeUtc=[DateTimeOffset]::MinValue\n"
        + changes[case]
        + _function("Get-EdgeSchedulerIdentity")
        + "$identity=$null; $failure=''\n"
        + "try {$identity=Get-EdgeSchedulerIdentity $BuildSha $NotBeforeUtc} "
        + "catch {$failure=$_.Exception.Message}\n"
        + "[ordered]@{identity=$identity;failure=$failure;trace=@($script:trace)} "
        + "| ConvertTo-Json -Depth 8 -Compress\n",
    )
    if case == "valid":
        assert observed["identity"]["pid"] == 7001
        assert observed["identity"]["scheduler_instance_id"].endswith("-7001")
        assert observed["identity"]["local_instance_id"] != observed["identity"]["scheduler_instance_id"]
        assert observed["failure"] == ""
    else:
        assert observed["identity"] is None, observed
    assert all(item.startswith("lookup:") for item in observed["trace"])


@pytest.mark.parametrize(
    "case",
    [
        "already_running", "delayed_start", "running_delayed_identity",
        "never_publishes", "missing_runtime", "running_never_publishes",
        "not_running", "disabled", "forward_gate",
        "launch_failure",
    ],
)
def test_start_waits_for_bound_daemon_before_returning(case, tmp_path):
    setup = {
        "already_running": "",
        "delayed_start": "$script:task.State='Ready'\n",
        "running_delayed_identity": (
            "$script:runtime.heartbeat_at_utc=$script:now.AddSeconds(-20).ToString('o');"
            " Write-TestRuntime\n"
        ),
        "never_publishes": "$script:task.State='Ready'\n",
        "missing_runtime": (
            "$script:task.State='Ready'; Remove-Item -LiteralPath $SchedulerRuntimePath\n"
        ),
        "running_never_publishes": (
            "$script:runtime.heartbeat_at_utc=$script:now.AddSeconds(-20).ToString('o');"
            " Write-TestRuntime\n"
        ),
        "not_running": "$script:task.State='Ready'\n",
        "disabled": "$script:task.Settings.Enabled=$false\n",
        "forward_gate": (
            "$script:task.State='Ready'; $script:task.Settings.Enabled=$false;"
            " $script:ForwardOnlySchedulerGate=$true\n"
        ),
        "launch_failure": "$script:task.State='Ready'\n",
    }
    observed = _run_ps(
        tmp_path,
        _fixture(tmp_path)
        + f"$case='{case}'; $script:waits=0\n"
        + setup[case]
        + "function Start-ScheduledTask($TaskName,$ErrorAction) {\n"
        + "  $script:trace.Add('start')\n"
        + "  if ($case -eq 'launch_failure') {throw 'task launch rejected'}\n"
        + "  if ($case -ne 'not_running') {$script:task.State='Running'}\n"
        + "  if ($case -in @('forward_gate','not_running')) {Publish-NewDaemon}\n"
        + "}\n"
        + "function Stop-ScheduledTask($TaskName,$ErrorAction) {\n"
        + "  $script:trace.Add('stop'); $script:task.State='Ready'\n"
        + "}\n"
        + "function Start-Sleep($Milliseconds) {\n"
        + "  $script:waits++; $script:trace.Add('wait')\n"
        + "  if ($script:waits -eq 2 -and $case -in @('delayed_start','running_delayed_identity')) "
        + "{Publish-NewDaemon}\n"
        + "}\n"
        + _function("Get-EdgeSchedulerIdentity")
        + _function("Start-EdgeScheduler")
        + "$timeout=0; if ($case -in @('delayed_start','running_delayed_identity')) {$timeout=5}\n"
        + "$identity=$null; $failure=''\n"
        + "try {$identity=Start-EdgeScheduler $BuildSha $timeout} "
        + "catch {$failure=$_.Exception.Message}\n"
        + "[ordered]@{identity=$identity;failure=$failure;waits=$script:waits;"
        + "task_state=$script:task.State;trace=@($script:trace)} "
        + "| ConvertTo-Json -Depth 8 -Compress\n",
    )
    success = case in {
        "already_running", "delayed_start", "running_delayed_identity", "forward_gate"
    }
    if success:
        expected_pid = 7001 if case == "already_running" else 7002
        assert observed["identity"]["pid"] == expected_pid, observed
        assert observed["identity"]["scheduler_instance_id"].endswith(f"-{expected_pid}")
        assert observed["failure"] == ""
    else:
        assert observed["identity"] is None, observed
        assert observed["failure"]
    assert observed["trace"].count("start") == int(
        case not in {
            "already_running", "running_delayed_identity", "running_never_publishes", "disabled"
        }
    )
    assert observed["trace"].count("enable") == int(case == "forward_gate")
    # A launch error can leave the newly requested task's result uncertain.
    # The same owned-task cleanup must cover that path too.
    owned_start_failure = case in {
        "never_publishes", "missing_runtime", "not_running", "launch_failure"
    }
    assert observed["trace"].count("stop") == int(owned_start_failure)
    if owned_start_failure:
        assert observed["task_state"] != "Running"
    if case == "running_never_publishes":
        assert observed["task_state"] == "Running"
    if case in {"delayed_start", "running_delayed_identity"}:
        assert observed["waits"] == 2
        assert "lookup:ProcessId = 7001" in observed["trace"] or case == "running_delayed_identity"
        assert observed["trace"][-1] == "lookup:ProcessId = 7002"
