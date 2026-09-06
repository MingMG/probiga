"""Exercise the one-time prior-release recovery gates under real PS 5.1."""
import json
import os
from pathlib import Path
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "tools/resume_qmt_prior_edge.ps1").read_text(encoding="utf-8")
PRIOR = "a" * 40
TARGET = "b" * 40


def function(name):
    start = SOURCE.index(f"function {name}(")
    return SOURCE[start:SOURCE.index("\nfunction ", start + 1)]


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def run_ps(tmp_path, body):
    ps = Path(os.environ.get("SystemRoot", "")) / "System32/WindowsPowerShell/v1.0/powershell.exe"
    if not ps.is_file():
        pytest.skip("Windows PowerShell 5.1 is required")
    script = tmp_path / "resume-gate.ps1"
    script.write_text(
        "$ErrorActionPreference='Stop'\nSet-StrictMode -Version Latest\n" + body,
        encoding="utf-8",
    )
    result = subprocess.run(
        [str(ps), "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", str(script)],
        capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=15,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout.strip())


@pytest.mark.parametrize("case", [
    "ready", "missing", "denied", "selected_other", "wrong_selector_build",
    "wrong_selector_mode", "native_failure", "wrong_strategy",
])
def test_prior_resume_requires_native_current_activation_selection_and_strategy(case, tmp_path):
    activation = {"mode": "check-activation", "status": "READY", "build_sha": PRIOR,
                  "activation_granted": case != "denied", "database_writes": False}
    selection = {"mode": "select-update-target", "status": "SELECTED", "build_sha": PRIOR,
                 "target_build_sha": TARGET if case == "selected_other" else PRIOR,
                 "database_writes": False, "writer_authorized": False}
    if case == "wrong_selector_build":
        selection["build_sha"] = TARGET
    if case == "wrong_selector_mode":
        selection["mode"] = "check-ready"
    strategy = {"mode": "check-strategy", "status": "READY", "database_writes": False,
                "expected_build_sha": TARGET if case == "wrong_strategy" else PRIOR}
    native = tmp_path / "bootstrap.cmd"
    native.write_text(
        '@echo off\nif "%~3"=="--select-update-target" goto selected\n'
        'if "%~3"=="--check-strategy" goto strategy\necho '
        + json.dumps(activation, separators=(",", ":"))
        + f"\nexit /b {4 if case == 'native_failure' else 0}\n:selected\necho "
        + json.dumps(selection, separators=(",", ":")) + "\nexit /b 0\n:strategy\necho "
        + json.dumps(strategy, separators=(",", ":")) + "\nexit /b 0\n",
        encoding="ascii",
    )
    if case == "missing":
        native = tmp_path / "not-installed.exe"
    observed = run_ps(tmp_path,
        f"$PythonExe={quote(native)}\n$BootstrapTool='probe.py'\n$PriorBuildSha='{PRIOR}'\n"
        "$global:LASTEXITCODE=0\n"
        + function("Invoke-JsonTool") + function("Assert-PriorReady")
        + "$allowed=$false; $failure=''\ntry { Assert-PriorReady; $allowed=$true } "
        "catch { $failure=$_.Exception.Message }\n"
        "[ordered]@{allowed=$allowed;failure=$failure} | ConvertTo-Json -Compress\n")
    assert observed["allowed"] is (case == "ready"), observed
    if case != "ready":
        assert observed["failure"]


@pytest.mark.parametrize("case", ["ready", "binding_changed", "updater_denied", "scheduler_denied", "daemon_raced"])
def test_exclusive_task_gate_restores_original_enabled_state_before_exit(case, tmp_path):
    observed = run_ps(tmp_path,
        f"$case='{case}'\n$SchedulerTaskName='scheduler'; $UpdaterTaskName='updater'\n"
        "$script:trace=[Collections.Generic.List[string]]::new()\n"
        "$script:tasks=@{scheduler=[pscustomobject]@{State='Ready';Settings=[pscustomobject]@{Enabled=$true}};"
        "updater=[pscustomobject]@{State='Ready';Settings=[pscustomobject]@{Enabled=$true}}}\n"
        "function Get-ScheduledTask($TaskName) { return $script:tasks[$TaskName] }\n"
        "function Assert-TaskBindings($Scheduler,$Updater) { if ($case -eq 'binding_changed') {throw 'binding changed'} }\n"
        "function Disable-ScheduledTask($TaskName) { $script:trace.Add('disable:'+$TaskName); "
        "if (($case -eq 'updater_denied' -and $TaskName -eq 'updater') -or "
        "($case -eq 'scheduler_denied' -and $TaskName -eq 'scheduler')) {throw 'access denied'}; "
        "$script:tasks[$TaskName].Settings.Enabled=$false }\n"
        "function Enable-ScheduledTask($TaskName) { $script:trace.Add('enable:'+$TaskName); $script:tasks[$TaskName].Settings.Enabled=$true }\n"
        "function Assert-NoDaemon { if ($case -eq 'daemon_raced') {throw 'another daemon'} }\n"
        + function("Enter-TaskGate") + function("Exit-TaskGate")
        + "$entered=$false; $failure=''\ntry { $gate=Enter-TaskGate; $entered=$true; Exit-TaskGate $gate } "
        "catch { $failure=$_.Exception.Message }\n"
        "[ordered]@{entered=$entered;failure=$failure;scheduler=$script:tasks.scheduler.Settings.Enabled;"
        "updater=$script:tasks.updater.Settings.Enabled;trace=@($script:trace.ToArray())}|ConvertTo-Json -Compress\n")
    assert observed["entered"] is (case == "ready"), observed
    assert observed["scheduler"] is True and observed["updater"] is True, observed
    if case == "binding_changed":
        assert observed["trace"] == []
    elif case == "scheduler_denied":
        assert observed["trace"] == ["disable:updater", "disable:scheduler", "enable:updater"]
    elif case == "updater_denied":
        assert observed["trace"] == ["disable:updater"]


@pytest.mark.parametrize("case", ["exact", "other_pid", "other_host", "other_build"])
def test_handoff_must_bind_the_live_prior_process(case, tmp_path):
    observed = run_ps(tmp_path,
        f"$PriorBuildSha='{PRIOR}'; $TargetBuildSha='{TARGET}'\n"
        "$RecoveryProtocol='probiga.qmt-edge-precutover-recovery.v1'\n"
        "$daemon=[pscustomobject]@{Id=1234}\n"
        "$ctx=[pscustomobject]@{schema='probiga.qmt-edge-precutover-context.v1';protocol=$RecoveryProtocol;"
        "build_sha=$TargetBuildSha;prior_build_sha=$PriorBuildSha;prior_pid=1234;"
        "prior_host_name=[Net.Dns]::GetHostName();prior_instance_id=([Net.Dns]::GetHostName()+'-1234');"
        "prior_running=$true;deployment_attempt_id=('c'*32)}\n"
        "$payload=[pscustomobject]@{mode='check-transition';status='PENDING';build_sha=$PriorBuildSha;"
        "target_build_sha=$TargetBuildSha;database_writes=$false;writer_authorized=$false;context=$ctx}\n"
        + ({"other_pid": "$ctx.prior_pid=1235\n", "other_host": "$ctx.prior_host_name='another-host';$ctx.prior_instance_id='another-host-1234'\n",
            "other_build": "$ctx.prior_build_sha=$TargetBuildSha\n"}.get(case, ""))
        + function("Test-ExactContext")
        + "[ordered]@{accepted=(Test-ExactContext $payload $daemon)}|ConvertTo-Json -Compress\n")
    assert observed["accepted"] is (case == "exact"), observed


@pytest.mark.parametrize("case", ["exact", "exited", "unmatched"])
def test_bootstrap_cannot_run_before_the_owned_child_has_a_runtime(case, tmp_path):
    child_start = SOURCE.index("$Daemon = Start-Process")
    bootstrap_start = SOURCE.index("$Bootstrap = Start-Process", child_start)
    assert child_start < SOURCE.index("$Runtime = Wait-OwnedRuntime $Daemon", child_start) < bootstrap_start
    observed = run_ps(tmp_path,
        f"$case='{case}'\n$daemon=[pscustomobject]@{{Id=1234;HasExited=($case -eq 'exited')}}\n"
        "function Read-OwnedRuntime($Daemon) { if ($case -eq 'exact') {return [pscustomobject]@{pid=$Daemon.Id}}; return $null }\n"
        + function("Wait-OwnedRuntime")
        + "$bootstrapCalls=0;$failure=''\ntry { $runtime=Wait-OwnedRuntime $daemon 1; $bootstrapCalls++ } "
        "catch { $failure=$_.Exception.Message }\n"
        "[ordered]@{bootstrap_calls=$bootstrapCalls;failure=$failure}|ConvertTo-Json -Compress\n")
    assert observed["bootstrap_calls"] == (1 if case == "exact" else 0), observed
    if case != "exact":
        assert observed["failure"]


@pytest.mark.parametrize("case", ["inserted", "idempotent", "no_writes", "other_pid"])
def test_bootstrap_requires_a_new_receipt_for_the_owned_child(case, tmp_path):
    observed = run_ps(tmp_path,
        f"$PriorBuildSha='{PRIOR}';$case='{case}'\n$daemon=[pscustomobject]@{{Id=1234}}\n"
        "$proof=[pscustomobject]@{mode='bootstrap';status='inserted';database_writes=$true;qmt_calls=$true;"
        "expected_build_sha=$PriorBuildSha;release_receipt=[pscustomobject]@{status='AVAILABLE'};"
        "identity=[pscustomobject]@{current=[pscustomobject]@{pid=1234;build_sha=$PriorBuildSha;"
        "host_name=[Net.Dns]::GetHostName();instance_id=([Net.Dns]::GetHostName()+'-1234')}}}\n"
        "if ($case -eq 'idempotent') {$proof.status='idempotent';$proof.database_writes=$false;$proof.qmt_calls=$false}\n"
        "if ($case -eq 'no_writes') {$proof.database_writes=$false}\n"
        "if ($case -eq 'other_pid') {$proof.identity.current.pid=1235}\n"
        + function("Assert-OwnedBootstrap")
        + "$accepted=$false;try {Assert-OwnedBootstrap $proof $daemon;$accepted=$true}catch {}\n"
        "[ordered]@{accepted=$accepted}|ConvertTo-Json -Compress\n")
    assert observed["accepted"] is (case == "inserted"), observed
