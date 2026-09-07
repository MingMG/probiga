"""Exercise launcher control flow in PS 5.1 without requesting real elevation."""
import json
import os
from pathlib import Path
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "tools/start_qmt_edge_recovery.ps1").read_text(encoding="utf-8")


def function(name):
    start = SOURCE.index(f"function {name}(")
    end = SOURCE.find("\nfunction ", start + 1)
    if end == -1:
        end = SOURCE.index("\nif (!$ElevatedChild)", start)
    return SOURCE[start:end]


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def run_ps(tmp_path, body):
    ps = Path(os.environ.get("SystemRoot", "")) / "System32/WindowsPowerShell/v1.0/powershell.exe"
    if not ps.is_file():
        pytest.skip("Windows PowerShell 5.1 is required")
    script = tmp_path / "launcher-test.ps1"
    script.write_text("$ErrorActionPreference='Stop'\nSet-StrictMode -Version Latest\n" + body, encoding="utf-8")
    result = subprocess.run(
        [str(ps), "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", str(script)],
        capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=20,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout.strip())


MOCKS = """
$script:events=[Collections.Generic.List[object]]::new()
$script:starts=0;$script:locks=0;$script:released=0
$MutexName='test';$PowerShellExe='powershell.exe';$ControllerRoot='C:\\controller';$LaunchId='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
$StatePath='unused';$StdoutPath='unused.out';$StderrPath='unused.err'
function Write-LaunchState($Status,$Stage,$Reason='',$ExitCode=-1,$ControllerPid=0) {
    $script:events.Add([pscustomobject]@{status=$Status;stage=$Stage;reason=$Reason;exit_code=$ExitCode;pid=$ControllerPid})
}
function Protect-DeployDiagnostic($Text) { return $Text }
function Assert-LauncherRepository { if($case-eq'network'){throw 'launcher.remote-main: timed out'} }
function Assert-DeployAdministrator { if($case-eq'permission'){throw 'administrator required'} }
function Enter-LaunchMutex($Name) {
    $script:locks++; if($case-eq'duplicate'){throw 'already-active'}
    $Gate=[pscustomobject]@{name=$Name}
    $Gate|Add-Member ScriptMethod ReleaseMutex {$script:released++}
    $Gate|Add-Member ScriptMethod Dispose {}
    return $Gate
}
function Exit-LaunchMutex($Mutex) { if($null-ne$Mutex){$script:released++} }
function Get-RecoveryArguments { param([switch]$ForElevatedChild) return 'fixed args' }
function Test-Path { return $false }
function New-ControllerJob {
    $Job=[pscustomobject]@{}
    $Job|Add-Member ScriptMethod Assign {param($Process) if($case-eq'job_assignment'){throw 'job assignment failed'}}
    $Job|Add-Member ScriptMethod Dispose {}
    return $Job
}
function Start-Process {
    param($FilePath,$Verb,$ArgumentList,$WorkingDirectory,$WindowStyle,[switch]$PassThru,$RedirectStandardOutput,$RedirectStandardError)
    $script:starts++
    if($case-eq'cancelled'){throw [ComponentModel.Win32Exception]::new(1223)}
    if($case-eq'launch_denied'){throw [ComponentModel.Win32Exception]::new(5)}
    $Code=0;if($case-eq'controller_failed'){$Code=7}
    $Process=[pscustomobject]@{Id=123;Handle=[IntPtr]::Zero;ExitCode=$Code;HasExited=$true}
    if($case-eq'job_assignment'){$Process.HasExited=$false}
    $Process|Add-Member ScriptMethod WaitForExit {}
    $Process|Add-Member ScriptMethod Refresh {}
    $Process|Add-Member ScriptMethod Kill {$this.HasExited=$true;$script:events.Add([pscustomobject]@{status='OWNED_CHILD_KILLED';stage='cleanup';pid=$this.Id})}
    return $Process
}
function Get-Content {
    $Status='COMPLETED';$Started=$true;$Id=$LaunchId;$Code=0
    if($case-eq'never_started'){$Status='WAITING_CONFIRMATION';$Started=$false}
    if($case-eq'stale'){$Id='bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'}
    if($case-eq'controller_failed'){$Code=7}
    if($case-eq'exit_mismatch'){$Code=4}
    @{status=$Status;launch_id=$Id;launcher_pid=123;controller_started=$Started;exit_code=$Code}|ConvertTo-Json -Compress
}
"""


@pytest.mark.parametrize("case,expected,starts", [
    ("ready", 0, 1), ("cancelled", 1223, 1), ("launch_denied", 1, 1),
    ("network", 1, 0), ("duplicate", 1, 0), ("never_started", 1, 1),
    ("stale", 1, 1), ("exit_mismatch", 1, 1), ("controller_failed", 7, 1),
])
def test_parent_tracks_real_uac_outcome_and_requires_controller_completion(tmp_path, case, expected, starts):
    observed = run_ps(tmp_path, f"$case='{case}'\n" + MOCKS
        + function("Test-UacCancelled") + function("Invoke-RecoveryLaunch")
        + "$code=Invoke-RecoveryLaunch\n"
          "@{code=$code;starts=$script:starts;events=@($script:events.ToArray());released=$script:released}|ConvertTo-Json -Depth 8 -Compress")
    assert observed["code"] == expected, observed
    assert observed["starts"] == starts, observed
    if case in {"network", "duplicate"}:
        assert all(event["status"] != "WAITING_CONFIRMATION" for event in observed["events"])
    else:
        assert observed["events"][0]["status"] == "WAITING_CONFIRMATION"
    if case == "cancelled":
        assert observed["events"][-1]["status"] == "CANCELLED"
    if case in {"never_started", "stale", "exit_mismatch"}:
        assert observed["events"][-1]["status"] == "FAILED"
    if case != "duplicate":
        assert observed["released"] == 2


@pytest.mark.parametrize("case,expected,starts", [
    ("ready", 0, 1), ("permission", 1, 0), ("network", 1, 0),
    ("duplicate", 1, 0), ("launch_denied", 1, 1), ("controller_failed", 7, 1), ("job_assignment", 1, 1),
])
def test_child_rechecks_permissions_and_network_before_controller(tmp_path, case, expected, starts):
    observed = run_ps(tmp_path, f"$case='{case}'\n" + MOCKS + function("Invoke-ElevatedRecovery")
        + "$code=Invoke-ElevatedRecovery\n"
          "@{code=$code;starts=$script:starts;events=@($script:events.ToArray());released=$script:released}|ConvertTo-Json -Depth 8 -Compress")
    assert observed["code"] == expected, observed
    assert observed["starts"] == starts, observed
    if case in {"ready", "controller_failed"}:
        assert [(item["status"], item["stage"]) for item in observed["events"]] == [
            ("STARTED", "elevated-preflight"), ("STARTED", "controller"), ("COMPLETED", "controller-exited")]
        assert observed["events"][-1]["pid"] == 123
    elif case == "job_assignment":
        assert observed["events"][-2]["status"] == "FAILED"
        assert observed["events"][-1]["status"] == "OWNED_CHILD_KILLED"
    else:
        assert observed["events"][-1]["status"] == "FAILED"
        assert observed["events"][-1]["pid"] == 0


def test_native_argument_round_trip_does_not_execute_path_characters(tmp_path):
    reader = tmp_path / "read arguments.ps1"
    reader.write_text("param([string]$Value)\n@{value=$Value}|ConvertTo-Json -Compress", encoding="utf-8")
    value = 'E:\\code with space\\literal & $() apostrophe\' quote"\\'
    observed = run_ps(tmp_path, function("ConvertTo-LauncherArgument")
        + f"$Reader={quote(reader)};$Value={quote(value)}\n"
        "$Arguments=(@('-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',$Reader,'-Value',$Value)|"
        "ForEach-Object{ConvertTo-LauncherArgument $_})-join' '\n"
        f"$Out={quote(tmp_path / 'roundtrip.out')}\n"
        "$Proc=Start-Process -FilePath (Join-Path $PSHOME 'powershell.exe') -ArgumentList $Arguments -WindowStyle Hidden "
        "-RedirectStandardOutput $Out -PassThru;$null=$Proc.Handle;$Proc.WaitForExit();Get-Content -LiteralPath $Out -Raw")
    assert observed["value"] == value


def test_uac_cancel_detection_accepts_wrapped_win32_error_only(tmp_path):
    observed = run_ps(tmp_path, function("Test-UacCancelled")
        + "$Inner=[ComponentModel.Win32Exception]::new(1223)\n"
          "$Wrapped=[Exception]::new('outer',$Inner)\n"
          "@{cancelled=(Test-UacCancelled $Wrapped);denied=(Test-UacCancelled ([ComponentModel.Win32Exception]::new(5)))}|ConvertTo-Json -Compress")
    assert observed == {"cancelled": True, "denied": False}


def test_mutex_excludes_an_independent_runspace_and_releases_afterwards(tmp_path):
    functions = function("Enter-LaunchMutex") + function("Exit-LaunchMutex")
    contender = functions + "\ntry{$gate=Enter-LaunchMutex $args[0];Exit-LaunchMutex $gate;'accepted'}catch{'rejected'}"
    observed = run_ps(tmp_path, functions
        + "$Name='Local\\ProBigA.Launcher.Test.'+[Guid]::NewGuid().ToString('N')\n"
          "$Gate=Enter-LaunchMutex $Name;$Shell=[PowerShell]::Create()\n"
        + f"$null=$Shell.AddScript({quote(contender)}).AddArgument($Name)\n"
          "$First=@($Shell.Invoke())[0];Exit-LaunchMutex $Gate\n"
          "$Second=@($Shell.Invoke())[0];$Shell.Dispose()\n"
          "@{first=$First;second=$Second}|ConvertTo-Json -Compress")
    assert observed == {"first": "rejected", "second": "accepted"}


def test_launch_history_distinguishes_host_started_and_controller_started(tmp_path):
    state = tmp_path / "state.json"
    history = tmp_path / "events.jsonl"
    observed = run_ps(tmp_path,
        f"$StatePath={quote(state)};$HistoryPath={quote(history)}\n"
        "$LaunchId='a'*32;$PriorBuildSha='b'*40;$TargetBuildSha='c'*40\n"
        "$StdoutPath='stdout';$StderrPath='stderr'\n" + function("Write-LaunchState")
        + "Write-LaunchState 'WAITING_CONFIRMATION' 'uac'\n"
          "Write-LaunchState 'STARTED' 'elevated-preflight'\n"
          "Write-LaunchState 'STARTED' 'controller' '' -1 123\n"
          "Write-LaunchState 'COMPLETED' 'controller-exited' '' 0 123\n"
          "Get-Content -LiteralPath $StatePath -Raw")
    assert observed["status"] == "COMPLETED" and observed["controller_pid"] == 123
    events = [json.loads(line) for line in history.read_text(encoding="utf-8").splitlines()]
    assert [event["controller_started"] for event in events] == [False, False, True, True]
    assert [event["status"] for event in events] == ["WAITING_CONFIRMATION", "STARTED", "STARTED", "COMPLETED"]
    assert not list(tmp_path.glob("*.tmp"))


def test_launcher_has_no_arbitrary_elevated_script_or_result_path_input():
    parameters = SOURCE[:SOURCE.index("$ErrorActionPreference")]
    assert "$ScriptPath" not in parameters and "$StatePath" not in parameters
    assert "-Verb RunAs" in SOURCE and "-WindowStyle Hidden" in SOURCE
    assert "-EncodedCommand" not in SOURCE
    assert "-ExecutionPolicy" not in SOURCE
    assert "Assert-LauncherRepository" in function("Invoke-ElevatedRecovery")


def test_closing_launcher_job_terminates_only_its_owned_process(tmp_path):
    observed = run_ps(tmp_path, function("New-ControllerJob")
        + "$Job=New-ControllerJob;$Child=$null\n"
          "try{\n"
          "$Child=Start-Process -FilePath (Join-Path $PSHOME 'powershell.exe') "
          "-ArgumentList '-NoProfile -NonInteractive -Command Start-Sleep -Seconds 30' -WindowStyle Hidden -PassThru\n"
          "$null=$Child.Handle;$Job.Assign($Child);$Job.Dispose()\n"
          "$Stopped=$Child.WaitForExit(5000)\n"
          "@{stopped=$Stopped}|ConvertTo-Json -Compress\n"
          "}finally{$Job.Dispose();if($null-ne$Child-and!$Child.HasExited){$Child.Kill();$Child.WaitForExit()}}")
    assert observed == {"stopped": True}
