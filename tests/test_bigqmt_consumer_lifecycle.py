"""Execute only isolated supervisor functions; never touch a live QMT process."""
from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
BUILD = "a" * 40
OLD_BUILD = "b" * 40


def _probe(tmp_path: Path, scenario: str) -> dict:
    powershell = shutil.which("powershell.exe")
    if powershell is None:
        pytest.skip("Windows PowerShell is required for the supervisor contract")
    script = tmp_path / "consumer-lifecycle.ps1"
    script.write_text(
        r'''
param([string]$Source, [string]$DataDir, [string]$Scenario)
$ErrorActionPreference = "Stop"
$Root = $DataDir
$tokens = $null
$errors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile($Source, [ref]$tokens, [ref]$errors)
if ($errors.Count) { throw "Supervisor syntax invalid" }
$names = @("Get-ServiceKeyFromScriptName", "Get-ManagedPidPath", "Get-ManagedProcess",
    "Set-ManagedProcess", "Get-ConsumerCheckoutBuild", "Get-BuildBoundConsumer", "Ensure-Process")
foreach ($name in $names) {
    $node = $ast.Find({ param($n)
        $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq $name
    }, $true)
    if (!$node) { throw "Function missing: $name" }
    . ([scriptblock]::Create($node.Extent.Text))
}
$build = "a" * 40
$oldBuild = "b" * 40
$script:Current = [pscustomobject]@{ Id = 123; ProcessName = "python"; StartTime = [DateTime]::UtcNow.AddMinutes(-30) }
$script:Starts = 0
$script:Stops = @()
$script:ChildEnvironment = @{}
$script:Arguments = ""
$environmentNames = @("PROBIGA_BUILD_COMMIT_SHA", "PROBIGA_SCHEDULER_BUILD_SHA", "PROBIGA_EXPECTED_GIT_SHA", "EXPECTED_GIT_SHA",
    "PROBIGA_DEPLOYMENT_MODE", "PROBIGA_SCHEDULER_EXECUTOR_ROLE", "PROBIGA_CODE_ROOT", "QMT_PYTHON")
$original = @{}
foreach ($name in $environmentNames) {
    $value = if ($name -eq "EXPECTED_GIT_SHA") { $null } else { $oldBuild }
    [Environment]::SetEnvironmentVariable($name, $value, "Process")
    $original[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
}
if ($Scenario -eq "linux_only") {
    # A remote Linux release must not change the local Windows component.
    $env:PROBIGA_COMPONENT_RELEASE_PATH = "/var/lib/probiga/release-artifacts/other/component-release.json"
}
function git {
    $global:LASTEXITCODE = if ($Scenario -eq "git_failed") { 128 } else { 0 }
    if ($Scenario -eq "zero_build") { return ("0" * 40) }
    if ($Scenario -eq "invalid_build") { return "not-a-build" }
    return $build
}
function Get-Process {
    [CmdletBinding()] param([int]$Id)
    if (!$script:Current -or $Id -ne $script:Current.Id) { throw "No process" }
    return $script:Current
}
function Stop-ManagedProcess {
    param([string]$ServiceKey)
    if ($ServiceKey -ne "big_qmt_bridge") { throw "Touched a different process" }
    $script:Stops += $ServiceKey
    $script:Current = $null
    Remove-Item -LiteralPath (Get-ManagedPidPath $ServiceKey) -ErrorAction SilentlyContinue
}
function Write-Host { param($Object) }
function Start-Process {
    param($FilePath, $ArgumentList, $WorkingDirectory, $WindowStyle,
        $RedirectStandardOutput, $RedirectStandardError, [switch]$PassThru)
    if ($FilePath -ne "isolated-python" -or $WindowStyle -ne "Hidden") { throw "Unexpected process" }
    $script:Starts += 1
    $script:Arguments = $ArgumentList
    foreach ($name in $environmentNames) {
        $script:ChildEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
    }
    if ($Scenario -eq "start_failed") { throw "Expected startup failure" }
    $script:Current = [pscustomobject]@{ Id = 456; ProcessName = "python"; StartTime = [DateTime]::UtcNow }
    return $script:Current
}
$recordBuild = if ($Scenario -in @("same_build", "linux_only")) { $build } else { $oldBuild }
Set-ManagedProcess "big_qmt_bridge" "run_big_qmt_bridge.py" $script:Current $recordBuild
if ($Scenario -eq "unbound") {
    Set-Content -LiteralPath (Get-ManagedPidPath "big_qmt_bridge") -Encoding Ascii -Value (
        "123|" + $script:Current.StartTime.ToUniversalTime().ToFileTimeUtc() + "|run_big_qmt_bridge.py"
    )
}
if ($Scenario -eq "pid_reused") { $script:Current.StartTime = [DateTime]::UtcNow }
if ($Scenario -eq "absent") {
    $script:Current = $null
    Remove-Item -LiteralPath (Get-ManagedPidPath "big_qmt_bridge")
}
$failure = ""
try {
    $checkout = Get-ConsumerCheckoutBuild
    $existing = Get-BuildBoundConsumer -BuildSha $checkout
    Ensure-Process -PythonExe "isolated-python" -ScriptName "run_big_qmt_bridge.py" `
        -ArgLine "tools/run_big_qmt_bridge.py" -ExpectedBuildSha $checkout `
        -StdOutPath "unused.out" -StdErrPath "unused.err" | Out-Null
    # A second supervisor pass must retain the freshly bound process.
    $existing = Get-BuildBoundConsumer -BuildSha $checkout
    Ensure-Process -PythonExe "isolated-python" -ScriptName "run_big_qmt_bridge.py" `
        -ArgLine "tools/run_big_qmt_bridge.py" -ExpectedBuildSha $checkout `
        -StdOutPath "unused.out" -StdErrPath "unused.err" | Out-Null
}
catch { $failure = $_.Exception.Message }
$restored = @{}
foreach ($name in $environmentNames) {
    $restored[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
}
$record = if (Test-Path -LiteralPath (Get-ManagedPidPath "big_qmt_bridge")) {
    ([string](Get-Content -LiteralPath (Get-ManagedPidPath "big_qmt_bridge") -Raw)).Trim()
} else { "" }
@{ starts = $script:Starts; stops = $script:Stops; child = $script:ChildEnvironment;
    arguments = $script:Arguments; original = $original; restored = $restored;
    record = $record; failure = $failure } | ConvertTo-Json -Compress -Depth 4
''',
        encoding="utf-8",
    )
    result = subprocess.run(
        [powershell, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
         "-File", str(script), "-Source", str(ROOT / "tools/start_local_live_services.ps1"),
         "-DataDir", str(tmp_path), "-Scenario", scenario],
        capture_output=True, text=True, timeout=30, check=True,
    )
    return json.loads(result.stdout)


@pytest.mark.parametrize("scenario, stops", [
    ("rollover", ["big_qmt_bridge"]), ("unbound", ["big_qmt_bridge"]),
    ("absent", []), ("pid_reused", []),
])
def test_consumer_restarts_for_its_windows_build_without_source_mtime(tmp_path, scenario, stops):
    result = _probe(tmp_path, scenario)
    assert result["failure"] == ""
    assert result["starts"] == 1
    assert result["stops"] == stops
    assert result["arguments"] == f"tools/run_big_qmt_bridge.py --expected-build-sha {BUILD}"
    _assert_windows_identity(result["child"], tmp_path)
    assert result["record"].startswith("456|")
    assert result["record"].endswith(f"|run_big_qmt_bridge.py|{BUILD}")
    assert result["restored"] == result["original"]


@pytest.mark.parametrize("scenario", ["same_build", "linux_only"])
def test_unchanged_windows_component_retains_consumer(tmp_path, scenario):
    result = _probe(tmp_path, scenario)
    assert result["failure"] == ""
    assert result["starts"] == 0
    assert result["stops"] == []
    assert result["record"].startswith("123|")
    assert result["restored"] == result["original"]


@pytest.mark.parametrize("scenario", ["git_failed", "zero_build", "invalid_build"])
def test_unknown_checkout_cannot_stop_or_start_consumer(tmp_path, scenario):
    result = _probe(tmp_path, scenario)
    assert "checkout build is unavailable" in result["failure"]
    assert result["starts"] == 0
    assert result["stops"] == []
    assert result["record"].endswith(OLD_BUILD)


def test_failed_consumer_start_restores_inherited_environment_without_pid_record(tmp_path):
    result = _probe(tmp_path, "start_failed")
    assert result["failure"] == "Expected startup failure"
    assert result["starts"] == 1
    assert result["stops"] == ["big_qmt_bridge"]
    assert result["restored"] == result["original"]
    _assert_windows_identity(result["child"], tmp_path)
    assert result["record"] == ""


def _assert_windows_identity(child, root):
    assert child == {
        "PROBIGA_BUILD_COMMIT_SHA": BUILD,
        "PROBIGA_SCHEDULER_BUILD_SHA": BUILD,
        "PROBIGA_EXPECTED_GIT_SHA": BUILD,
        "EXPECTED_GIT_SHA": BUILD,
        "PROBIGA_DEPLOYMENT_MODE": "production",
        "PROBIGA_SCHEDULER_EXECUTOR_ROLE": "qmt_windows_edge",
        "PROBIGA_CODE_ROOT": str(root),
        "QMT_PYTHON": str(root / "runtime/qmt-py313/Scripts/python.exe"),
    }
