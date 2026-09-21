"""Exercise launcher decisions without starting services or touching QMT."""
import json
from pathlib import Path
import shutil
import subprocess
import uuid

import pytest


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("existing", [False, True])
def test_launcher_uses_lifetime_mutex_when_process_command_line_is_unavailable(tmp_path, existing):
    powershell = shutil.which("powershell.exe")
    if not powershell:
        pytest.skip("Windows PowerShell required")
    source = (ROOT / "tools/launch_local_live_supervisor.ps1").read_text(encoding="utf8")
    mutex = "Local\\ProBigA.TestSupervisor." + uuid.uuid4().hex
    # Use a private mutex and temporary log paths; production is untouched.
    source = source.replace("Local\\ProBigA.LocalLiveSupervisor", mutex)
    launcher = tmp_path / "tools/launch.ps1"
    launcher.parent.mkdir()
    launcher.write_text(source, encoding="utf8")
    probe = tmp_path / "probe.ps1"
    probe.write_text(r'''
param([string]$Launcher, [string]$MutexName, [int]$Existing)
$ErrorActionPreference = 'Stop'
$global:SupervisorProbe = @{starts=0;style=''}
function Get-CimInstance { throw 'Process CommandLine must not be used as liveness proof' }
function Start-Process {
    param($FilePath, $ArgumentList, $WorkingDirectory, $WindowStyle,
          $RedirectStandardOutput, $RedirectStandardError, [switch]$PassThru)
    $global:SupervisorProbe.starts += 1
    $global:SupervisorProbe.style = $WindowStyle
    $global:StartedOwner = [Threading.Mutex]::new($true, $MutexName)
    return [pscustomobject]@{HasExited=$false}
}
$owner = $null
if ($Existing) { $owner = [Threading.Mutex]::new($true, $MutexName) }
try { & $Launcher }
finally {
    if ($owner) { $owner.ReleaseMutex(); $owner.Dispose() }
    if ($global:StartedOwner) { $global:StartedOwner.ReleaseMutex(); $global:StartedOwner.Dispose() }
}
$global:SupervisorProbe | ConvertTo-Json -Compress
''', encoding="utf8")
    result = subprocess.run(
        [powershell, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
         "-File", str(probe), "-Launcher", str(launcher), "-MutexName", mutex,
         "-Existing", str(int(existing))],
        capture_output=True, text=True, timeout=15, check=True,
    )
    assert json.loads(result.stdout) == {
        "starts": 0 if existing else 1, "style": "" if existing else "Hidden",
    }


def test_both_activated_updater_paths_ensure_supervisor_before_returning_ready():
    source = (ROOT / "tools/update_qmt_windows_edge.ps1").read_text(encoding="utf8")
    fast = source[source.index("if ($CurrentSha -ceq $TargetSha) {"):]
    fast = fast[:fast.index("# Phase two")]
    assert fast.index("Confirm-QmtReleaseActivation") < fast.index("$ReadyExit -eq 0")
    assert fast.index("$ReadyExit -eq 0") < fast.index("launch_local_live_supervisor.ps1")
    assert fast.index("launch_local_live_supervisor.ps1") < fast.index("release already exact-ready")
    slow = source[source.index("$RuntimeScheduler = Start-EdgeScheduler $CurrentSha"):]
    assert slow.index("launch_local_live_supervisor.ps1") < slow.index("$StrategyPreflightStatus =")


def test_consumer_runs_in_frozen_qmt_runtime():
    source = (ROOT / "tools/start_local_live_services.ps1").read_text(encoding="utf8")
    call = source[source.index('    Ensure-Process `\n        -PythonExe $qmtPython `\n        -ScriptName "run_big_qmt_bridge.py"'):]
    assert '-ExpectedBuildSha $consumerBuildSha' in call[:500]
