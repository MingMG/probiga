from pathlib import Path
import json
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    "scenario, expected_starts, expected_sleeps, expected_failed",
    [
        ("absent", 1, 1, False),
        ("client_appeared", 0, 0, False),
        ("recovery_owns_lock", 0, 0, False),
        ("start_failed", 1, 0, True),
        ("abandoned", 1, 1, False),
    ],
)
def test_qmt_supervisor_serializes_only_process_start(
    tmp_path, scenario, expected_starts, expected_sleeps, expected_failed
):
    powershell = shutil.which("powershell.exe")
    if powershell is None:
        pytest.skip("Windows PowerShell is required for the native mutex contract")
    # Extract only the function AST: never execute the real supervisor, read
    # its .env or use the production recovery mutex during a test.
    script = tmp_path / "probe.ps1"
    script.write_text(
        r'''
param([string]$Source, [string]$DataDir, [string]$Scenario)
$ErrorActionPreference = "Stop"
$tokens = $null
$errors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    $Source, [ref]$tokens, [ref]$errors
)
if ($errors.Count) { throw "Supervisor syntax invalid" }
$function = $ast.Find({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -eq "Ensure-QmtClient"
}, $true)
if (!$function) { throw "Ensure-QmtClient missing" }
$script:MutexName = "Local\ProBigA.QmtLaunchTest." + [Guid]::NewGuid().ToString("N")
$body = $function.Extent.Text.Replace(
    "Local\ProBigA.BigQmtStrategyRecovery", $script:MutexName
)
. ([scriptblock]::Create($body))
Add-Type -TypeDefinition @'
using System;
using System.Threading;
public static class MutexProbe {
    static Thread holder;
    static Mutex held;
    static ManualResetEventSlim ready = new ManualResetEventSlim(false);
    static ManualResetEventSlim release = new ManualResetEventSlim(false);
    public static bool CanAcquire(string name) {
        bool acquired = false;
        var thread = new Thread(() => {
            using (var mutex = new Mutex(false, name)) {
                try { acquired = mutex.WaitOne(0); }
                catch (AbandonedMutexException) { acquired = true; }
                if (acquired) mutex.ReleaseMutex();
            }
        });
        thread.Start(); thread.Join();
        return acquired;
    }
    public static void Hold(string name, bool abandon) {
        holder = new Thread(() => {
            held = new Mutex(false, name);
            held.WaitOne(); ready.Set();
            if (!abandon) { release.Wait(); held.ReleaseMutex(); }
        });
        holder.Start(); ready.Wait();
        if (abandon) holder.Join();
    }
    public static void Finish() {
        release.Set();
        if (holder != null) holder.Join();
        if (held != null) held.Dispose();
    }
}
'@
$script:Checks = 0
$script:Starts = 0
$script:Sleeps = 0
function Get-QmtProcesses {
    $script:Checks += 1
    if ($Scenario -eq "client_appeared" -and $script:Checks -gt 1) {
        [pscustomobject]@{ Id = 123 }
    }
}
function Test-QmtClientLoggedIn { return $false }
function Test-QmtAutoStartWindow { return $true }
function Resolve-QmtClientPath { return (Join-Path $DataDir "XtItClient.exe") }
function Get-QmtRetryDelaySeconds { return 30 }
function Write-QmtAlert {
    if (![MutexProbe]::CanAcquire($script:MutexName)) {
        throw "Alert executed inside launch lock"
    }
}
function Start-Process {
    param($FilePath, $WorkingDirectory, $WindowStyle)
    if ([MutexProbe]::CanAcquire($script:MutexName)) {
        throw "Start executed outside launch lock"
    }
    $script:Starts += 1
    if ($Scenario -eq "start_failed") { throw "Expected start failure" }
}
function Start-Sleep {
    param($Seconds)
    if ($Seconds -ne 15 -or ![MutexProbe]::CanAcquire($script:MutexName)) {
        throw "Sleep executed inside launch lock"
    }
    $script:Sleeps += 1
}
$failed = $false
try {
    if ($Scenario -eq "recovery_owns_lock") {
        # Contention begins after the existing alert, at the actual launch
        # boundary, so this also checks that it cannot reach Start-Process.
        function Write-QmtAlert {
            [MutexProbe]::Hold($script:MutexName, $false)
        }
    }
    if ($Scenario -eq "abandoned") {
        function Write-QmtAlert {
            [MutexProbe]::Hold($script:MutexName, $true)
        }
    }
    try { Ensure-QmtClient }
    catch {
        if ($_.Exception.Message -ne "Expected start failure") { throw }
        $failed = $true
    }
}
finally { [MutexProbe]::Finish() }
if (![MutexProbe]::CanAcquire($script:MutexName)) { throw "Launch lock leaked" }
@{ starts = $script:Starts; sleeps = $script:Sleeps; failed = $failed } |
    ConvertTo-Json -Compress
''',
        encoding="utf-8",
    )
    result = subprocess.run(
        [
            powershell, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
            "-File", str(script), "-Source", str(ROOT / "tools/start_local_live_services.ps1"),
            "-DataDir", str(tmp_path), "-Scenario", scenario,
        ],
        capture_output=True, text=True, timeout=30, check=True,
    )
    assert json.loads(result.stdout) == {
        "starts": expected_starts,
        "sleeps": expected_sleeps,
        "failed": expected_failed,
    }


def test_qmt_autostart_never_recycles_a_running_client_from_title_only():
    source = (ROOT / "tools" / "start_local_live_services.ps1").read_text(
        encoding="utf-8"
    )
    assert "Test-QmtAutoStartWindow" in source
    assert "DayOfWeek]::Saturday" in source
    assert "Get-QmtRetryDelaySeconds" in source
    assert "QMT_CLIENT_MIN_BACKOFF_SECONDS" in source
    assert "QMT_CLIENT_MAX_BACKOFF_SECONDS" in source
    assert "no daily attempt limit" in source
    assert "QMT_CLIENT_MAX_RESTART_ATTEMPTS" not in source
    assert "XtItClient.exe" in source
    assert "Test-QmtClientLoggedIn" in source
    ensure_source = source.split("function Ensure-QmtClient", 1)[1].split(
        "$python = Resolve-PythonPath", 1
    )[0]
    assert 'status = "login_unverified"' in ensure_source
    assert "Leaving the client untouched" in ensure_source
    assert "Stop-Process" not in ensure_source
    assert '$running.Count -eq 0 -and [string]$state.status -eq "login_unverified"' in ensure_source
    assert "$failures = 0" in ensure_source


def test_big_qmt_strategy_recovery_uses_end_to_end_persistent_backoff():
    source = (
        ROOT / "tools" / "ensure_big_qmt_strategy_running.ps1"
    ).read_text(encoding="utf-8")
    assert "Test-RecoveryWindow" in source
    assert "DayOfWeek]::Saturday" in source
    assert "Get-EndToEndHealth" in source
    assert "FullSnapshotMaxAgeSeconds" in source
    assert "SyncReceiptMaxAgeSeconds" in source
    assert "MinimumBackoffSeconds" in source
    assert "MaximumBackoffSeconds" in source
    assert "MaxAttemptsPerDay" not in source
    assert "PROBIGA_BIGQMT_BRIDGE" in source
    assert "Test-HeartbeatHealthy" in source
    assert '$status -notin @("running", "busy")' in source
    assert "WaitOne(0)" in source
    assert "AbandonedMutexException" in source
    assert "StaleTakeover" in source
    assert "TotalSeconds -lt 120" in source
    assert "client is not logged in" in source
    assert '"CLIENT_OFFLINE"' in source
    assert '"LOGIN_REQUIRED"' in source
    assert "$ExpectedClientPid" in source
    assert "heartbeat.model_instance_id" in source
    assert "heartbeat.heartbeat_seq" in source
    assert "oldest_pending_request_age_seconds" in source
    assert "oldest_inflight_request_age_seconds" in source
    assert "client_started_at" in source
    assert "no daily attempt limit" in source
    assert "[switch]$AllowLegacyEditorRecovery" in source
    assert "if (!$AllowLegacyEditorRecovery)" in source
    assert '"needs_user_action"' in source
    assert "NEEDS_USER_ACTION:MODEL_TRADING_REQUIRED" in source
    assert source.index("if (!$AllowLegacyEditorRecovery)") < source.index(
        "Add-Type -AssemblyName System.Windows.Forms"
    )
    assert "QMT 2.1.19" in source
    assert "0.107 0.077" in source
    assert "0.056/0.039 is the account badge" in source
    assert "0.470 0.015" in source
    assert "FindStrategyPaneLeft" in source
    assert "CreateDIBSection" in source
    assert "BitBlt" in source
    assert "$fullWidthList" in source
    assert "$embeddedList" in source
    assert "$paneLeft + 70" in source
    assert "$paneLeft + 322" in source
    assert "SearchX = 0.325" not in source
    assert "EditX = 0.458" not in source
    assert "0.339 0.151" in source
    assert "last_price" not in source


def test_local_supervisor_invokes_big_qmt_strategy_recovery():
    source = (ROOT / "tools" / "start_local_live_services.ps1").read_text(
        encoding="utf-8"
    )
    assert "BIG_QMT_STRATEGY_AUTO_RECOVER" in source
    assert "ensure_big_qmt_strategy_running.ps1" in source
    strategy_flag = source.split("function Test-BigQmtStrategyAutoRecover", 1)[1].split(
        "function Resolve-QmtClientPath", 1
    )[0]
    assert "return $false" in strategy_flag
    assert "return Test-QmtClientAutoRestart" not in strategy_flag


def test_big_qmt_consumer_gets_cold_start_grace_before_receipt_restart():
    source = (ROOT / "tools" / "start_local_live_services.ps1").read_text(
        encoding="utf-8"
    )
    assert "BIG_QMT_CONSUMER_STARTUP_GRACE_SECONDS" in source
    assert "$consumerStartupGraceSeconds = 300" in source
    assert "$consumerAgeSeconds" in source
    assert "$consumerAgeSeconds -ge $consumerStartupGraceSeconds" in source
    assert "BIG_QMT_CONSUMER_FAILURE_GRACE_SECONDS" in source
    assert "BIG_QMT_CONSUMER_FAILURE_CHECKS" in source
    assert "BIG_QMT_CONSUMER_MAX_SAMPLE_GAP_SECONDS" in source
    assert "$failureCount -ge $consumerFailureChecks" in source
    assert "$failureAgeSeconds -ge $consumerFailureGraceSeconds" in source
    assert "$consumerMaxSampleGapSeconds" in source
    assert "consumer_started_at" in source
    assert "Unknown health must break the consecutive-failure series" in source
    assert "Sync receipt recovered; consumer restart guard reset." in source
    assert "WaitForExit(150000)" in source
    assert source.index("$consumerAgeSeconds -ge $consumerStartupGraceSeconds") < source.index(
        "check_big_qmt_end_to_end_health.py"
    )


def test_big_qmt_consumer_restart_terminates_its_delegated_process_tree():
    source = (ROOT / "tools" / "start_local_live_services.ps1").read_text(
        encoding="utf-8"
    )
    stop_source = source.split("function Stop-ManagedProcess", 1)[1].split(
        "$script:ServiceProcessInventoryLoaded", 1
    )[0]
    assert '$ServiceKey -eq "big_qmt_bridge"' in stop_source
    assert "taskkill.exe /PID $proc.Id /T /F" in stop_source
    assert "Get-Process -Id $proc.Id" in stop_source


def test_launcher_does_not_start_legacy_unbounded_watchdog():
    source = (ROOT / "tools" / "launch_local_live_supervisor.ps1").read_text(
        encoding="utf-8"
    )
    assert "run_qmt_client_watchdog.ps1" not in source


def test_legacy_watchdog_has_no_three_attempt_daily_stop():
    source = (ROOT / "tools" / "run_qmt_client_watchdog.ps1").read_text(
        encoding="utf-8"
    )
    assert "$minimumBackoffSeconds = 30" in source
    assert "$maximumBackoffSeconds = 900" in source
    assert "$attemptCount -lt 3" not in source
    assert "no_daily_limit" in source
    assert "[System.DayOfWeek]::Saturday" in source
    assert "[TimeSpan]::FromHours(6.5)" in source
    assert "XtItClient.exe" in source


def test_supervisor_checks_the_thirty_second_sla_frequently():
    source = (
        ROOT / "tools" / "run_local_live_supervisor.ps1"
    ).read_text(encoding="utf-8")
    assert "Start-Sleep -Seconds 5" in source


def test_local_status_includes_sanitized_login_diagnostic():
    source = (ROOT / "tools" / "status_local_live_services.ps1").read_text(
        encoding="utf-8"
    )
    assert "diagnose_bigqmt_login.py" in source
    assert "sanitized" in source
