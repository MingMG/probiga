"""Cold target lifecycle contracts; no installations, auth, QMT or databases run."""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "tools/secondary_edge"
HELPER = (TOOLS / "migrate_target.ps1").read_text(encoding="utf-8")
ENTRY = (TOOLS / "target_entry.ps1").read_text(encoding="utf-8")
CMD = (TOOLS / "start_target_migration.cmd").read_text(encoding="utf-8")
PS = shutil.which("powershell.exe")


def run_ps(code: str):
    return subprocess.run(
        [PS, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command",
         "$ErrorActionPreference='Stop';" + code],
        capture_output=True, text=True, timeout=30,
    )


def function_definition(name: str, script: str = "migrate_target") -> str:
    """Retrieve one trusted function by AST without executing either script."""
    path = str(TOOLS / (script + ".ps1")).replace("'", "''")
    result = run_ps(
        "$t=$null;$e=$null;$a=[System.Management.Automation.Language.Parser]::ParseFile('"
        + path + "',[ref]$t,[ref]$e);$a.FindAll({param($n)$n -is "
        "[System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq '"
        + name + "'},$true)|ForEach-Object {$_.Extent.Text}"
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().startswith("function " + name)
    return result.stdout


@pytest.mark.skipif(os.name != "nt" or not PS, reason="Windows PowerShell parser required")
@pytest.mark.parametrize("name", ["migrate_target", "target_entry"])
def test_windows_powershell_51_parses_real_scripts(name):
    path = str(TOOLS / (name + ".ps1")).replace("'", "''")
    result = run_ps(
        "$t=$null;$e=$null;[System.Management.Automation.Language.Parser]::ParseFile('"
        + path + "',[ref]$t,[ref]$e)|Out-Null;if($e.Count){throw ($e|Out-String)}"
    )
    assert result.returncode == 0, result.stderr


def test_single_entry_dispatches_native_powershell_without_second_script():
    assert "System32\\WindowsPowerShell\\v1.0\\powershell.exe" in CMD
    assert "target_entry.ps1" in CMD
    assert "verify_target" not in CMD + ENTRY + HELPER
    assert "install_target" not in CMD + ENTRY + HELPER
    assert "RunAs" in ENTRY
    assert "-OriginalUserSid" in ENTRY
    assert "param([string]$InstallRoot=''" in ENTRY
    assert "[string]$InstallRoot = 'C:\\ProBigA'" in HELPER
    assert ENTRY.index("$manifest=Read-PublicManifest") < ENTRY.index("$InstallRoot=Select-TargetInstallRoot")
    assert "install_root=$InstallRoot" in HELPER


@pytest.mark.parametrize("script", [ENTRY, HELPER])
def test_native_module_path_is_selected_before_imports(script):
    assert script.index("$env:PSModulePath") < script.index("Import-Module")
    assert "System32\\WindowsPowerShell\\v1.0\\Modules" in script


def test_full_v2_validation_precedes_all_target_writes():
    assert "Assert-ColdPackage" in HELPER
    assert "Assert-ColdPackage" not in ENTRY
    assert "$manifest=Read-PublicManifest $PackageRoot" in ENTRY
    assert HELPER.index("$manifest = Assert-ColdPackage") < HELPER.index("New-Item -ItemType Directory -Path $InstallRoot")
    assert HELPER.index("SOURCE_COMPUTER_BLOCKED") < HELPER.index("New-Item -ItemType Directory -Path $InstallRoot")
    assert "TARGET_UNOWNED_DIRECTORY" in HELPER
    assert "TARGET_OWNER_MISMATCH" in HELPER
    assert "manifest_sha256" in HELPER + ENTRY
    assert "TARGET_REPARSE_PATH_REFUSED" in HELPER
    assert "WINDOWS11_REQUIRED" in HELPER
    assert "MEMORY_INSUFFICIENT" in HELPER
    assert "TARGET_CAPACITY_INSUFFICIENT" in HELPER


def test_database_is_a_cold_physical_final_instance_not_an_empty_candidate():
    assert "'ProBigA-MySQL84'" in HELPER
    assert "33085" not in HELPER + ENTRY
    assert "--initialize-insecure" not in HELPER
    assert "--skip-grant-tables" not in HELPER
    assert "tools.secondary_edge.cold_database','restore'" in HELPER
    assert "--skip-networking --shared-memory" in HELPER
    assert "--persisted-globals-load=OFF" in HELPER
    assert "Invoke-DatabaseVerification 'memory'" in HELPER
    assert "Invoke-DatabaseVerification 'tcp'" in HELPER
    assert "--defaults-file=$InstallRoot\\root-client.ini" in HELPER
    assert "password=" not in HELPER + ENTRY


def test_private_bootstrap_shutdown_precedes_normal_tls_service_start():
    start = HELPER.index("$script:BootstrapProcess = Start-Process")
    memory = HELPER.index("Invoke-DatabaseVerification 'memory'", start)
    shutdown = HELPER.index("Stop-Bootstrap", memory)
    normal = HELPER.index("Start-Service -Name $serviceName", shutdown)
    tls = HELPER.index("Invoke-DatabaseVerification 'tcp'", normal)
    stop = HELPER.index("Stop-OwnedService", tls)
    assert start < memory < shutdown < normal < tls < stop
    assert "MYSQL_LISTENER_OWNER_MISMATCH" in HELPER
    assert "MYSQL_SERVICE_OWNER_MISMATCH" in HELPER
    assert "Assert-OwnedMysqlListener" in HELPER
    assert "ParentProcessId" in HELPER
    assert "CreationDate" in HELPER
    assert "Stop-Process" not in HELPER
    assert "taskkill" not in HELPER


def test_paused_receipt_requires_real_stopped_disabled_service():
    assert "$stopped.State -ne 'Stopped'" in HELPER
    assert "$stopped.StartMode -ne 'Disabled'" in HELPER
    assert "Set-Service -Name $serviceName -StartupType Disabled" in HELPER
    assert "@('--install-manual',$serviceName" in HELPER
    assert "@('--install',$serviceName" not in HELPER
    assert "status='paused-installed'" in HELPER
    assert "database_stopped=$true" in HELPER
    assert "production_active=$false" in HELPER + ENTRY
    assert "restore_requested=$false" in HELPER + ENTRY
    assert "authentication_pending=$true" in HELPER + ENTRY
    assert "function Assert-TargetPaused" in ENTRY
    assert "$service.ProcessId -ne 0" in ENTRY
    assert "$service.PathName -notmatch $servicePattern" in ENTRY
    assert ENTRY.index("$script:EntryStage='final-pause-verification'") < ENTRY.index("Write-UserReceipt ([ordered]@{status='paused-installed'")
    assert "Linux services were not moved or restarted" in ENTRY
    assert "windows_time_zone_id=(Get-TimeZone" in HELPER
    assert "clock_calibration='NOT_VERIFIED_REQUIRES_PRODUCTION_RESUME_GATE'" in HELPER
    assert "Set-TimeZone" not in HELPER + ENTRY


def test_qmt_and_production_workers_are_never_started():
    for forbidden in ["start_terminal", "WindowsQmtLoginDriver", "run_local_scheduler_task",
                      "run_local_live_supervisor", "start_local_live_services", "--install-strategy",
                      "wait_for_recovered_bridge", "provider.ask", "worker/claim", "13306"]:
        assert forbidden not in HELPER + ENTRY
    assert "Get-AuthenticodeSignature -LiteralPath $qmtExe" in ENTRY
    assert "QMT_MUST_REMAIN_STOPPED" in ENTRY
    assert "qmt_authentication='deferred-until-restoration'" in ENTRY
    assert "ai_generation_verified=$false" in ENTRY
    assert "codex_history_continuity='not-verified'" in ENTRY


def test_auth_is_in_original_user_not_in_admin_helper():
    assert "CODEX_HOME" not in HELPER
    assert "CdpConnection" not in HELPER
    assert "$env:CODEX_HOME=$codexHome" in ENTRY
    assert "& $codex login" in ENTRY
    assert "DeepSeekChromeSession" in ENTRY
    assert "state.get(\"ready\") and not state.get(\"captcha\")" in ENTRY
    assert "auth.json" not in ENTRY + HELPER
    assert "source-codex-history-archive" in HELPER


def test_reboot_continues_in_interactive_limited_original_session():
    assert "exit 3010" in HELPER + ENTRY
    assert "boot_time=$bootTime" in HELPER
    assert "New-ScheduledTaskTrigger -AtLogOn -User $OriginalUserSid" in HELPER
    assert "-LogonType Interactive -RunLevel Limited" in HELPER
    assert "-MultipleInstances IgnoreNew" in HELPER
    assert "New-Object Threading.Mutex" in ENTRY
    assert "Remove-OwnedContinuation" in ENTRY
    assert "Unregister-ScheduledTask" in ENTRY
    assert "DefaultPassword" not in HELPER + ENTRY
    assert "AutoAdminLogon" not in HELPER + ENTRY
    assert "\npause\n" not in CMD


def test_sensitive_mysql_files_do_not_inherit_user_read():
    assert "Protect-AdministratorPath $InstallRoot -UserTraverse" in HELPER
    assert "Protect-AdministratorPath $InstallRoot -UserRead" not in HELPER
    assert "$rights,'None','None','Allow'" in HELPER
    assert "ReadPermissions" in HELPER
    assert "Grant-UserWrite $auth" in HELPER
    assert "Protect-AdministratorPath $control -UserRead" in HELPER
    assert "Grant-UserWrite (Join-Path $InstallRoot 'mysql" not in HELPER


def test_complete_source_state_is_private_hash_verified_archive_not_active_runtime():
    assert "function Copy-SealedSourceState" in HELPER
    assert "$prefix='audit/source-project-state/'" in HELPER
    assert "$archive=Join-Path $InstallRoot 'source-state-archive'" in HELPER
    assert "Protect-AdministratorPath $archive -UserRead" not in HELPER
    assert "Protect-AdministratorPath $archive\n" in HELPER
    assert "(Get-Sha256 $file) -ne $entry.sha256" in HELPER
    assert "SOURCE_STATE_ARCHIVE_EXTRA_OR_MISSING_FILE" in HELPER
    assert "SOURCE_STATE_ARCHIVE_INVENTORY_MISSING" in HELPER
    assert "runtime_activation=$false;authentication_reuse=$false" in HELPER
    assert HELPER.index("    Copy-SealedSourceState") < HELPER.index("Write-Receipt $softwareReceipt ([ordered]@{status='paused-installed'")


@pytest.mark.skipif(os.name != "nt" or not PS, reason="Windows PowerShell function mocks required")
def test_existing_newer_vc_runtime_is_detected_without_installation():
    definition = function_definition("Install-Artifact")
    code = r"""
$script:NeedsRestart=$false;$control='C:\fixture';$bootTime='boot';$script:written=$null
function Get-AuthenticodeSignature { param($LiteralPath) [pscustomobject]@{Status='Valid'} }
function Test-Path { param($LiteralPath) $false }
function Get-Item { param($LiteralPath) [pscustomobject]@{VersionInfo=[pscustomobject]@{FileVersion='14.44.35211.0'}} }
function Get-ItemProperty { param($LiteralPath,$ErrorAction) [pscustomobject]@{Installed=1;Version='v14.50.36000.00'} }
function Get-Sha256 { param($Path) 'fixture-sha' }
function Write-Receipt { param($Path,$Value) $script:written=$Value }
function Start-Process { throw 'REAL_INSTALLATION_FORBIDDEN' }
Install-Artifact 'C:\fixture\vc.exe' '/quiet' 'vc-x64'
$script:written | ConvertTo-Json -Compress
"""
    result = run_ps(definition + code)
    assert result.returncode == 0, result.stderr
    receipt = json.loads(result.stdout)
    assert receipt["existing_runtime_satisfied"] is True
    assert receipt["exit_code"] == 0


@pytest.mark.skipif(os.name != "nt" or not PS, reason="Windows PowerShell function mocks required")
def test_invalid_signature_cannot_execute_installer():
    definition = function_definition("Install-Artifact")
    code = r"""
function Get-AuthenticodeSignature { param($LiteralPath) [pscustomobject]@{Status='NotSigned'} }
function Start-Process { throw 'REAL_INSTALLATION_FORBIDDEN' }
try { Install-Artifact 'C:\fixture.exe' '/quiet' 'python313'; throw 'UNEXPECTED_PASS' }
catch { if ($_.Exception.Message -ne 'INSTALLER_SIGNATURE_INVALID') { throw } }
'SIGNATURE_REJECTED'
"""
    result = run_ps(definition + code)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "SIGNATURE_REJECTED"


@pytest.mark.skipif(os.name != "nt" or not PS, reason="Windows PowerShell function mocks required")
@pytest.mark.parametrize("foreign,reused,expected", [
    (False, False, "owned"), (True, False, "MYSQL_LISTENER_OWNER_MISMATCH"),
    (False, True, "MYSQL_LISTENER_OWNER_MISMATCH"),
])
def test_mysql_monitor_child_requires_exact_path_parent_and_creation(foreign, reused, expected):
    definition = function_definition("Assert-OwnedMysqlListener")
    code = r"""
$mysqld='C:\ProBigA\mysql84\bin\mysqld.exe'
$rootTime=[DateTime]'2026-10-01T12:00:00'
$childPath=CHILD_PATH
$childTime=$rootTime.AddSeconds(CHILD_DELTA)
function Assert-OwnedService { [pscustomobject]@{State='Running';ProcessId=10} }
function Get-NetTCPConnection { param($LocalPort,$State,$ErrorAction) [pscustomobject]@{LocalAddress='127.0.0.1';OwningProcess=20} }
function Get-CimInstance {
    param($ClassName,$Filter)
    if ($Filter -eq 'ProcessId=10') { [pscustomobject]@{ProcessId=10;ParentProcessId=1;ExecutablePath=$mysqld;CreationDate=$rootTime} }
    elseif ($Filter -eq 'ProcessId=20') { [pscustomobject]@{ProcessId=20;ParentProcessId=10;ExecutablePath=$childPath;CreationDate=$childTime} }
}
try { if (Assert-OwnedMysqlListener) { 'owned' } else { 'not-ready' } }
catch { $_.Exception.Message }
"""
    code = code.replace("CHILD_PATH", "'D:\\foreign\\mysqld.exe'" if foreign else "$mysqld")
    code = code.replace("CHILD_DELTA", "-1" if reused else "1")
    result = run_ps(definition + code)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == expected


def test_chrome_receipt_seals_msi_not_system_msiexec():
    assert "[string]$PayloadFile=$File" in HELPER
    assert "sha256=(Get-Sha256 $PayloadFile)" in HELPER
    assert "'chrome' $chromeMsi" in HELPER


def test_public_pre_uac_gate_never_reads_protected_source_payload():
    assert "Get-Content -LiteralPath $manifestPath" in ENTRY
    assert "PUBLIC_PACKAGE_SEAL_MISMATCH" in ENTRY
    assert "audit/source-pause" not in ENTRY
    assert "database/metadata" not in ENTRY
    assert "Assert-ColdPackage" not in ENTRY


@pytest.mark.skipif(os.name != "nt" or not PS, reason="Windows PowerShell function mocks required")
@pytest.mark.parametrize("foreign,running,qmt,expected", [
    (False, False, False, "paused"),
    (True, False, False, "TARGET_DATABASE_NOT_PAUSED"),
    (False, True, False, "TARGET_DATABASE_NOT_PAUSED"),
    (False, False, True, "QMT_MUST_REMAIN_STOPPED"),
])
def test_target_paused_gate_requires_exact_service_and_no_clients(foreign, running, qmt, expected):
    definition = function_definition("Assert-TargetPaused", "target_entry")
    code = r'''
$InstallRoot='C:\ProBigA'
$image=IMAGE_VALUE
$state=STATE_VALUE
$pidValue=PID_VALUE
function Get-CimInstance {
    param($ClassName,$Filter,$ErrorAction)
    if ($ClassName -eq 'Win32_Service') { [pscustomobject]@{PathName=$image;State=$state;StartMode='Disabled';ProcessId=$pidValue} }
    elseif (QMT_VALUE) { [pscustomobject]@{ProcessId=99} }
}
function Get-NetTCPConnection { param($LocalPort,$State,$ErrorAction) }
try { Assert-TargetPaused; 'paused' } catch { $_.Exception.Message }
'''
    image = (r'"D:\foreign\mysqld.exe" --defaults-file="C:\ProBigA\my.ini" ProBigA-MySQL84'
             if foreign else r'"C:\ProBigA\mysql84\bin\mysqld.exe" --defaults-file="C:\ProBigA\my.ini" ProBigA-MySQL84')
    code = code.replace("IMAGE_VALUE", "'" + image + "'")
    code = code.replace("STATE_VALUE", "'Running'" if running else "'Stopped'")
    code = code.replace("PID_VALUE", "88" if running else "0")
    code = code.replace("QMT_VALUE", "$true" if qmt else "$false")
    result = run_ps(definition + code)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == expected


@pytest.mark.skipif(os.name != "nt" or not PS, reason="Windows PowerShell function mocks required")
@pytest.mark.parametrize("corrupt,extra,expected", [
    (False, False, "copied:2:12"),
    (True, False, "SOURCE_STATE_ARCHIVE_CONTENT_MISMATCH"),
    (False, True, "SOURCE_STATE_ARCHIVE_EXTRA_OR_MISSING_FILE"),
])
def test_archive_resume_verifies_every_sealed_file_and_rejects_extras(corrupt, extra, expected):
    definition = function_definition("Copy-SealedSourceState")
    code = r'''
$InstallRoot='C:\ProBigA';$PackageRoot='F:\Package';$control='C:\ProBigA\migration-control';$seal='fixture-seal'
$manifest=[pscustomobject]@{files=@(
    [pscustomobject]@{path='audit/source-project-state/development/data/main.db';bytes=5;sha256='five'},
    [pscustomobject]@{path='audit/source-project-state/production/data/cache.json';bytes=7;sha256='seven'})}
function Assert-PlainPath { param($Path) }
function Protect-AdministratorPath { param($Path,$UserRead) if($UserRead){throw 'USER_READ_FORBIDDEN'} }
function Test-Path { param($LiteralPath,$PathType) $true }
function Get-Content { param($LiteralPath,[switch]$Raw,$Encoding) '{"status":"copied","manifest_sha256":"fixture-seal"}' }
function Get-Item { param($LiteralPath,[switch]$Force) [pscustomobject]@{Length=if($LiteralPath.EndsWith('main.db')){5}else{7}} }
function Get-Sha256 { param($Path) if(CORRUPT_VALUE){'bad'}elseif($Path.EndsWith('main.db')){'five'}else{'seven'} }
function Get-ChildItem {
    param($LiteralPath,[switch]$Recurse,[switch]$Force,[switch]$File)
    [pscustomobject]@{FullName='C:\ProBigA\source-state-archive\development\data\main.db'}
    [pscustomobject]@{FullName='C:\ProBigA\source-state-archive\production\data\cache.json'}
    if(EXTRA_VALUE){[pscustomobject]@{FullName='C:\ProBigA\source-state-archive\unsealed.json'}}
}
function Write-Receipt { param($Path,$Value) "$($Value.status):$($Value.file_count):$($Value.bytes)" }
function Copy-Tree { throw 'RESUME_RECOPY_FORBIDDEN' }
try { Copy-SealedSourceState } catch { $_.Exception.Message }
'''
    code = code.replace("CORRUPT_VALUE", "$true" if corrupt else "$false")
    code = code.replace("EXTRA_VALUE", "$true" if extra else "$false")
    result = run_ps(definition + code)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == expected


@pytest.mark.skipif(os.name != "nt" or not PS, reason="Windows PowerShell function mocks required")
@pytest.mark.parametrize("scenario,expected", [
    ("new", "D:\\ProBigA"),
    ("owned", "C:\\ProBigA"),
    ("unknown", "TARGET_NO_SAFE_INTERNAL_VOLUME"),
    ("multiple", "TARGET_MULTIPLE_OWNED_INSTALLATIONS"),
    ("mismatched", "D:\\ProBigA"),
])
def test_automatic_storage_selection_excludes_usb_and_preserves_existing_roots(scenario, expected):
    definition = function_definition("Select-TargetInstallRoot", "target_entry")
    code = r'''
$scenario='SCENARIO_VALUE';$originalSid='S-1-5-21-100-200-300-1001'
$manifest=[pscustomobject]@{minimum_target_free_bytes=[long]250GB;build_sha=('a'*40);source_host='SOURCE'}
function Get-Volume {
    param($ErrorAction)
    [pscustomobject]@{DriveLetter='C';FileSystem='NTFS';HealthStatus='Healthy';DriveType='Fixed';SizeRemaining=[long]100GB}
    [pscustomobject]@{DriveLetter='D';FileSystem='NTFS';HealthStatus='Healthy';DriveType='Fixed';SizeRemaining=[long]500GB}
    [pscustomobject]@{DriveLetter='F';FileSystem='NTFS';HealthStatus='Healthy';DriveType='Fixed';SizeRemaining=[long]900GB}
}
function Get-Partition {
    param($DriveLetter,$ErrorAction)
    [pscustomobject]@{DiskNumber=if($DriveLetter -eq 'F'){2}elseif($DriveLetter -eq 'D'){1}else{0}}
}
function Get-Disk {
    param($Number,$ErrorAction)
    [pscustomobject]@{BusType=if($Number -eq 2){'USB'}else{'SATA'};HealthStatus='Healthy';IsOffline=$false;IsReadOnly=$false}
}
function Test-Path {
    param($LiteralPath,$PathType)
    if($scenario -eq 'unknown'){return $LiteralPath -eq 'D:\ProBigA'}
    if($scenario -eq 'owned' -or $scenario -eq 'mismatched'){return $LiteralPath.StartsWith('C:\ProBigA')}
    if($scenario -eq 'multiple'){return $LiteralPath.StartsWith('C:\ProBigA') -or $LiteralPath.StartsWith('D:\ProBigA')}
    return $false
}
function Get-Item { param($LiteralPath,[switch]$Force,$ErrorAction) [pscustomobject]@{PSIsContainer=$true;Attributes=[IO.FileAttributes]::Normal} }
function Get-Content {
    param($LiteralPath,[switch]$Raw,$Encoding,$ErrorAction)
    $root=$LiteralPath.Substring(0,10)
    [pscustomobject]@{format='probiga.windows-cold-installation.v1';host=$env:COMPUTERNAME;
        original_user_sid=$originalSid;manifest_sha256=if($scenario -eq 'mismatched'){'foreign'}else{'seal'};
        build_sha=('a'*40);install_root=$root;source_host='SOURCE';production_active=$false;restore_requested=$false} | ConvertTo-Json
}
function New-Item { throw 'DISK_MUTATION_FORBIDDEN' }
function Set-Partition { throw 'DISK_MUTATION_FORBIDDEN' }
function Format-Volume { throw 'DISK_MUTATION_FORBIDDEN' }
try { Select-TargetInstallRoot $manifest 'seal' } catch { $_.Exception.Message }
'''
    code = code.replace("SCENARIO_VALUE", scenario)
    result = run_ps(definition + code)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == expected
