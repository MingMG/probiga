"""Cold target lifecycle contracts; no installations, auth, QMT or databases run."""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import uuid

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
         "$ErrorActionPreference='Stop';$env:PSModulePath="
         '"$env:SystemRoot\\System32\\WindowsPowerShell\\v1.0\\Modules;'
         '$env:ProgramFiles\\WindowsPowerShell\\Modules";' + code],
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


def helper_main_try() -> dict:
    path = str(TOOLS / "migrate_target.ps1").replace("'", "''")
    result = run_ps("$t=$null;$e=$null;$a=[Management.Automation.Language.Parser]::ParseFile('" + path
                    + "',[ref]$t,[ref]$e);$s=@($a.EndBlock.Statements|Where-Object {$_ -is "
                    "[Management.Automation.Language.TryStatementAst]})[-1];"
                    "[pscustomobject]@{statement=$s.Extent.Text;finally=$s.Finally.Extent.Text}|ConvertTo-Json -Compress")
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout)


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


@pytest.mark.parametrize("script", [ENTRY, HELPER], ids=["entry", "helper"])
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
    assert "& $appPython -B -m tools.secondary_edge.account_readiness --profile $deepseek" in ENTRY
    assert "Invoke-AuthProbe" not in ENTRY
    assert "$deepseekProbe" not in ENTRY
    assert " -c " not in "\n".join(line for line in ENTRY.splitlines() if not line.lstrip().startswith("#"))
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
function Show-ColdMigrationStage { param($Stage) }
function Write-Host { param($Object) }
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


def test_helper_has_permanent_machine_lock_before_hash_and_diagnostics():
    body = HELPER[HELPER.index("try {\n    # All sealed files"):]
    assert "Global\\ProBigA.ColdMigration.AdminInstaller" in HELPER
    assert body.index("Enter-ColdMigrationHelperLock") < body.index("New-ColdMigrationDiagnosticRun")
    assert body.index("Enter-ColdMigrationHelperLock") < body.index("$manifest = Assert-ColdPackage")
    assert body.index("$manifest = Assert-ColdPackage") < body.index("New-Item -ItemType Directory -Path $InstallRoot")
    busy = body[body.index("if (-not (Enter-ColdMigrationHelperLock))"):body.index("$script:InstallStage = 'diagnostic-initialization'")]
    assert "exit 1618" in busy
    assert "Write-Receipt" not in busy
    assert "Write-ColdMigrationDiagnosticStatus" not in busy
    assert "HELPER_LOCK_ABANDONED_REQUIRES_INSPECTION" in HELPER
    assert "SetAccessRuleProtection($true,$false)" in HELPER
    assert "Exit-ColdMigrationHelperLock" in body[body.rindex("} finally {"):]
    assert "-WindowStyle Normal" in ENTRY
    assert "-WindowStyle Hidden" not in ENTRY
    assert "-Wait -PassThru -WindowStyle Hidden" in HELPER  # silent vendor installer
    assert "-PassThru -WindowStyle Hidden" in HELPER  # private database child


def test_progress_and_early_diagnostics_do_not_weaken_pause_or_seal():
    assert "Assert-ColdPackage $PackageRoot -ProgressAction" in HELPER
    assert "Report-ColdPackageProgress $phase $bytesRead $totalBytes $validFiles $totalFiles" in HELPER
    assert "MigrationDiagnostics" in HELPER
    assert "'status.json'" in HELPER
    assert "[Guid]::NewGuid().ToString('D')" in HELPER
    assert "Assert-ColdMigrationDiagnosticDirectory" in HELPER
    assert "Assert-PlainPath ($script:DiagnosticStatusPath + '.partial')" in HELPER
    assert "installation_authorization=$false" in HELPER
    assert "Start-Transcript" not in ENTRY + HELPER
    assert "$_.Exception.Message" not in ENTRY + HELPER
    assert "Package verification complete. Installation is not complete." in HELPER
    assert "Original-user account readiness is still pending" in HELPER
    assert "Wait-ColdMigrationFailureAcknowledgement" in HELPER
    assert "Wait-ColdMigrationEntryFailureAcknowledgement" in ENTRY
    assert "HELPER_INITIALIZATION_FAILED" in HELPER
    assert "ENTRY_INITIALIZATION_FAILED" in ENTRY
    for stage in ["offline-software", "exact-build-and-python-environments", "copy-business-history-archive",
                  "cold-database-materialization", "private-database-bootstrap-verification",
                  "owned-service-tls-verification"]:
        assert stage in HELPER


@pytest.mark.skipif(os.name != "nt" or not PS, reason="Native PS5 safe function fixtures required")
def test_progress_is_numeric_fixed_phase_and_never_claims_installation_complete():
    code = function_definition("Report-ColdPackageProgress") + r'''
$script:MigrationClock=[Diagnostics.Stopwatch]::StartNew();$script:messages=@();$script:updates=@()
function Write-Host { param($Object) $script:messages += [string]$Object }
function Write-ColdMigrationDiagnosticStatus { param($Status,$Stage,$Code,$BytesRead,$TotalBytes,$ValidFiles,$TotalFiles)
    $script:updates += [pscustomobject]@{status=$Status;stage=$Stage;bytes=$BytesRead;files=$ValidFiles} }
Report-ColdPackageProgress 'hashing-files' 1073741824 2147483648 1 3
Report-ColdPackageProgress 'final-inventory-scanning' 0 0 7 0
Report-ColdPackageProgress 'verified' 2147483648 2147483648 3 3
[pscustomobject]@{messages=$script:messages;updates=$script:updates}|ConvertTo-Json -Depth 4 -Compress
'''
    result = run_ps(code)
    assert result.returncode == 0, result.stdout + result.stderr
    proof = json.loads(result.stdout)
    assert "read=1.00/2.00 GiB" in proof["messages"][0]
    assert "scanned-entries=7; inventory-total=unknown" in proof["messages"][1]
    assert "GiB" not in proof["messages"][1] and "verified-files" not in proof["messages"][1]
    assert all("Installation is not complete" in item for item in proof["messages"])
    assert proof["updates"][0] == {"status": "running", "stage": "package-validation", "bytes": 1073741824, "files": 1}
    assert len(proof["updates"]) == 3


@pytest.mark.skipif(os.name != "nt" or not PS, reason="Native PS5 safe diagnostic-schema fixture required")
@pytest.mark.parametrize("phase", ["inventory-scanning", "hashing-files"])
def test_diagnostic_inventory_entries_are_not_verified_files(phase):
    result = run_ps(function_definition("Write-ColdMigrationDiagnosticStatus") + r'''
$script:DiagnosticStatusPath='C:\FixtureDiagnostics\status.json';$script:DiagnosticRunId='fixture'
$script:DiagnosticManifestHash=$null;$script:MigrationClock=[Diagnostics.Stopwatch]::StartNew()
function Assert-PlainPath { param($Path) }
function Assert-ColdMigrationDiagnosticDirectory { param($Path) }
function Write-Receipt { param($Path,$Value) $Value | ConvertTo-Json -Compress }
Write-ColdMigrationDiagnosticStatus 'running' 'package-validation' '' 0 0 7 0 'PHASE_VALUE'
'''.replace("PHASE_VALUE", phase))
    assert result.returncode == 0, result.stdout + result.stderr
    status = json.loads(result.stdout)
    assert status["progress_phase"] == phase
    assert status["installation_authorization"] is False
    if phase == "inventory-scanning":
        assert status["valid_files"] == status["total_files"] == 0
        assert status["inventory_entries"] == 7
        assert status["inventory_total"] is None
    else:
        assert status["valid_files"] == 7
        assert status["inventory_entries"] is None


@pytest.mark.skipif(os.name != "nt" or not PS, reason="Native PS5 safe function fixtures required")
@pytest.mark.parametrize("script,code", [("migrate_target", "HELPER_INITIALIZATION_FAILED"),
                                        ("target_entry", "ENTRY_INITIALIZATION_FAILED")])
def test_import_failure_displays_fixed_code_without_private_exception(script, code):
    path = str(TOOLS / (script + ".ps1")).replace("'", "''")
    preamble = run_ps("$t=$null;$e=$null;$a=[Management.Automation.Language.Parser]::ParseFile('" + path
                      + "',[ref]$t,[ref]$e);@($a.EndBlock.Statements|Where-Object {$_ -is "
                      "[Management.Automation.Language.TryStatementAst]})[0].Extent.Text")
    assert preamble.returncode == 0, preamble.stderr
    result = run_ps("function Import-Module {throw 'PRIVATE_CREDENTIAL_FILE_SECRET'};"
                    "function Read-Host {throw 'NO_INTERACTION_IN_FIXTURE'};" + preamble.stdout)
    assert result.returncode != 0
    assert "CODE=" + code in result.stdout
    assert "PRIVATE_CREDENTIAL_FILE_SECRET" not in result.stdout + result.stderr


@pytest.mark.skipif(os.name != "nt" or not PS, reason="Native PS5 safe function fixtures required")
@pytest.mark.parametrize("stage,expected", [("package-validation", "PACKAGE_VERIFICATION_FAILED"),
                                          ("offline-software-python313", "OFFLINE_SOFTWARE_FAILED"),
                                          ("private-database-bootstrap-verification", "COLD_DATABASE_VERIFICATION_FAILED")])
def test_failure_code_comes_from_fixed_stage_not_exception(stage, expected):
    result = run_ps(function_definition("Get-ColdMigrationFailureCode")
                    + f"Get-ColdMigrationFailureCode '{stage}'")
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == expected


@pytest.mark.skipif(os.name != "nt" or not PS, reason="Native PS5 mutex fixture required")
def test_native_helper_lock_is_exclusive_across_processes_and_survives_frontend_absence():
    # Only an isolated fixture mutex is created; no installer/power/DB code runs.
    name = "Global\\ProBigA.ColdMigration.Fixture." + uuid.uuid4().hex
    definitions = (function_definition("Enter-ColdMigrationHelperLock")
                   + function_definition("Exit-ColdMigrationHelperLock")).replace(
                       "Global\\ProBigA.ColdMigration.AdminInstaller", name)
    # The fixture host is deliberately not elevated. Use its own SID for the
    # fixture owner/ACL instead of assigning Administrators ownership; the real
    # helper requires an administrator token before creating the real mutex.
    fixture_sid = run_ps("[Security.Principal.WindowsIdentity]::GetCurrent().User.Value").stdout.strip()
    definitions = definitions.replace("S-1-5-32-544", fixture_sid)
    initial = "$script:HelperMutex=$null;$script:HelperLockHeld=$false;"
    holder = subprocess.Popen(
        [PS, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command",
         "$ErrorActionPreference='Stop';" + definitions + initial
         + "try{if(-not (Enter-ColdMigrationHelperLock)){throw 'NOT_ACQUIRED'};"
           "[Console]::WriteLine('LOCKED');[Console]::Out.Flush();"
           "[Console]::ReadLine()|Out-Null}finally{Exit-ColdMigrationHelperLock}"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    try:
        assert holder.stdout.readline().strip() == "LOCKED", holder.stderr.read()
        contender = run_ps(definitions + initial
                           + "try{[bool](Enter-ColdMigrationHelperLock)}finally{Exit-ColdMigrationHelperLock}")
        assert contender.returncode == 0, contender.stdout + contender.stderr
        assert contender.stdout.strip() == "False"
        holder.stdin.write("release\n")
        holder.stdin.flush()
        stdout, stderr = holder.communicate(timeout=10)
        assert holder.returncode == 0, stdout + stderr
        next_run = run_ps(definitions + initial
                          + "try{[bool](Enter-ColdMigrationHelperLock)}finally{Exit-ColdMigrationHelperLock}")
        assert next_run.returncode == 0, next_run.stdout + next_run.stderr
        assert next_run.stdout.strip() == "True"
    finally:
        if holder.poll() is None:
            try:
                holder.communicate(input="release\n", timeout=10)
            except OSError:
                holder.wait(timeout=10)
        for stream in (holder.stdin, holder.stdout, holder.stderr):
            if stream is not None:
                stream.close()


@pytest.mark.skipif(os.name != "nt" or not PS, reason="Native PS5 safe ACL directory fixture required")
@pytest.mark.parametrize("unsafe", [False, True])
def test_diagnostic_run_has_new_guid_safe_acl_and_never_uses_install_root(tmp_path, unsafe):
    definitions = "\n".join(function_definition(name) for name in [
        "Assert-PlainPath", "Assert-ColdMigrationDiagnosticDirectory", "New-ColdMigrationDiagnosticRun"])
    fixture = str(tmp_path).replace("'", "''")
    definitions = definitions.replace(
        "[Environment]::GetFolderPath([Environment+SpecialFolder]::CommonApplicationData)", "'" + fixture + "'")
    fixture_sid = run_ps("[Security.Principal.WindowsIdentity]::GetCurrent().User.Value").stdout.strip()
    definitions = definitions.replace("S-1-5-32-544", fixture_sid)
    if unsafe:
        (tmp_path / "ProBigA").mkdir()  # an unprotected, unowned existing base is never altered
    code = definitions + r'''
$OriginalUserSid='S-1-5-21-100-200-300-1001';$script:DiagnosticStatusPath=$null
$script:DiagnosticRunId=$null;$script:MigrationClock=[Diagnostics.Stopwatch]::StartNew()
function Write-Host { param($Object) }
try { New-ColdMigrationDiagnosticRun; [pscustomobject]@{path=$script:DiagnosticStatusPath;id=$script:DiagnosticRunId}|ConvertTo-Json -Compress }
catch { if (UNSAFE_FIXTURE) { 'DIAGNOSTIC_REFUSED' } else { throw } }
'''
    result = run_ps(code.replace("UNSAFE_FIXTURE", "$true" if unsafe else "$false"))
    assert result.returncode == 0, result.stdout + result.stderr
    if unsafe:
        assert result.stdout.strip() == "DIAGNOSTIC_REFUSED"
        assert not (tmp_path / "ProBigA" / "MigrationDiagnostics").exists()
    else:
        assert result.stdout.strip() != "DIAGNOSTIC_REFUSED", result.stdout + result.stderr
        proof = json.loads(result.stdout)
        assert str(uuid.UUID(proof["id"])) == proof["id"]
        expected = tmp_path / "ProBigA" / "MigrationDiagnostics" / proof["id"] / "status.json"
        assert Path(proof["path"]) == expected
        assert expected.parent.is_dir()
        assert not expected.exists()  # this routine creates diagnostics, not installation state


@pytest.mark.skipif(os.name != "nt" or not PS, reason="Native PS5 no-install lifecycle mocks required")
@pytest.mark.parametrize("busy", [False, True], ids=["early-sha-failure", "busy-no-writes"])
def test_helper_early_sha_failure_is_visible_and_busy_cannot_touch_active_state(tmp_path, busy):
    proof_path = str(tmp_path / "proof.json").replace("'", "''")
    definitions = "\n".join(function_definition(name) for name in [
        "Get-ColdMigrationFailureCode", "Show-ColdMigrationStage", "Report-ColdPackageProgress"])
    code = definitions + r'''
$PackageRoot='C:\FixturePackage';$InstallRoot='D:\ProBigA';$OriginalUserSid='S-1-5-21-100-200-300-1001'
$script:MigrationPowerLease=$null;$script:ServiceOwned=$false;$script:HelperMutex=$null;$script:HelperLockHeld=$false
$script:DiagnosticStatusPath=$null;$script:DiagnosticRunId=$null;$script:MigrationClock=[Diagnostics.Stopwatch]::StartNew()
$script:InstallStage='package-validation';$script:updates=@();$script:shaCalled=$false;$script:powerReleased=$false
function Assert-PlainPath { param($Path) }
function Get-Content { param($LiteralPath,[switch]$Raw,$Encoding)
    '{"format":"probiga.windows-cold-migration.v2","source_host":"fixture-other-host","source_paused":true,"production_activation":false,"restore_requested":false}' }
function Get-CimInstance { param($ClassName)
    if($ClassName -eq 'Win32_OperatingSystem'){[pscustomobject]@{BuildNumber=22000}}
    else{[pscustomobject]@{TotalPhysicalMemory=16GB}} }
function New-ColdMigrationPowerLease { New-Object psobject }
function Remove-ColdMigrationPowerLease { param($Lease) $script:powerReleased=$true }
function Enter-ColdMigrationHelperLock { $script:HelperMutex=New-Object psobject;$script:HelperLockHeld=LOCK_VALUE;return $script:HelperLockHeld }
function Exit-ColdMigrationHelperLock {
    [IO.File]::WriteAllText('PROOF_PATH',([pscustomobject]@{sha_called=$script:shaCalled;updates=$script:updates;
        power_released=$script:powerReleased;lock_disposed=$true}|ConvertTo-Json -Depth 4 -Compress)) }
function New-ColdMigrationDiagnosticRun { $script:DiagnosticStatusPath='C:\FixtureDiagnostics\status.json' }
function Write-ColdMigrationDiagnosticStatus { param($Status,$Stage,$Code,$BytesRead,$TotalBytes,$ValidFiles,$TotalFiles)
    $script:updates += [pscustomobject]@{status=$Status;stage=$Stage;code=$Code} }
function Wait-ColdMigrationFailureAcknowledgement { }
function Assert-ColdPackage { param($Root,$ProgressAction)
    $script:shaCalled=$true;& $ProgressAction 'hashing-files' 123 456 0 1
    throw 'PRIVATE_FILENAME_OR_SECRET_MUST_NOT_APPEAR' }
function New-Item { throw 'REAL_TARGET_WRITE_FORBIDDEN' }
function Write-Receipt { throw 'REAL_TARGET_WRITE_FORBIDDEN' }
function Start-Process { throw 'REAL_INSTALLATION_FORBIDDEN' }
'''
    code = code.replace("PROOF_PATH", proof_path).replace("LOCK_VALUE", "$false" if busy else "$true")
    result = run_ps(code + helper_main_try()["statement"])
    assert result.returncode == (1618 if busy else 1), result.stdout + result.stderr
    proof = json.loads((tmp_path / "proof.json").read_text())
    assert proof["power_released"] and proof["lock_disposed"]
    assert "PRIVATE_FILENAME_OR_SECRET_MUST_NOT_APPEAR" not in result.stdout + result.stderr
    assert "REAL_TARGET_WRITE_FORBIDDEN" not in result.stdout + result.stderr
    if busy:
        assert not proof["sha_called"]
        assert proof["updates"] == []
        assert "CODE=HELPER_ALREADY_RUNNING" in result.stdout
    else:
        assert proof["sha_called"]
        assert proof["updates"][-1] == {"status": "blocked", "stage": "package-validation", "code": "PACKAGE_VERIFICATION_FAILED"}
        assert "CODE=PACKAGE_VERIFICATION_FAILED" in result.stdout
        assert "Diagnostics: C:\\FixtureDiagnostics\\status.json" in result.stdout


@pytest.mark.skipif(os.name != "nt" or not PS, reason="Native PS5 no-install lifecycle mocks required")
@pytest.mark.parametrize("terminal", ["success", "failure", "restart"])
def test_real_helper_finally_disposes_own_lock_after_power_for_every_exit(tmp_path, terminal):
    proof_path = str(tmp_path / "release.json").replace("'", "''")
    action = {"success": "exit 0", "failure": "throw 'FIXTURE_FAILURE'", "restart": "exit 3010"}[terminal]
    code = r'''
$script:MigrationPowerLease=New-Object psobject;$script:HelperMutex=New-Object psobject;$script:calls=@()
function Remove-ColdMigrationPowerLease { param($Lease) $script:calls += 'power' }
function Exit-ColdMigrationHelperLock { $script:calls += 'mutex';[IO.File]::WriteAllText('PROOF_PATH',($script:calls|ConvertTo-Json -Compress)) }
'''.replace("PROOF_PATH", proof_path)
    result = run_ps(code + "try{" + action + "}finally" + helper_main_try()["finally"])
    assert result.returncode == {"success": 0, "failure": 1, "restart": 3010}[terminal], result.stdout + result.stderr
    assert json.loads((tmp_path / "release.json").read_text()) == ["power", "mutex"]


def test_database_supervision_retains_exact_restore_and_pause_gates():
    assert "Invoke-ColdDatabaseMaterialization $qmtPython $InstallRoot $PackageRoot" in HELPER
    assert "$start.UseShellExecute = $false" in HELPER
    assert "$start.CreateNoWindow = $true" in HELPER
    assert "$start.RedirectStandardOutput = $true" in HELPER
    assert "$start.RedirectStandardError = $true" in HELPER
    assert "$start.WorkingDirectory = Join-Path $Root 'code'" in HELPER
    assert "$process.WaitForExit(5000)" in HELPER
    assert "DATABASE_RESTORE_PROCESS_FAILED" in HELPER
    assert "$process.ExitCode -ne 0" in HELPER
    assert "still-running. This is not verified progress or installation completion." in HELPER
    assert "RedirectStandardOutput = '" not in HELPER
    assert "Kill(" not in HELPER
    assert "tools.secondary_edge.cold_database','restore'" in HELPER
    assert HELPER.index("Invoke-ColdDatabaseMaterialization $qmtPython") < HELPER.index("Invoke-DatabaseVerification 'memory'")


@pytest.mark.skipif(os.name != "nt" or not PS, reason="Native PS5 isolated child-process fixture required")
@pytest.mark.parametrize("child_exit,observer_error", [(0, False), (7, False), (0, True)],
                         ids=["success", "real-child-failure", "observer-failure-keeps-waiting"])
def test_database_supervision_waits_discards_child_secrets_and_requires_real_exit(child_exit, observer_error):
    # Only a short, isolated PowerShell sleep/exit fixture runs, never Python,
    # project modules, installers, MySQL or any provider/login process.
    definition = function_definition("Invoke-ColdDatabaseMaterialization")
    executable = PS.replace("'", "''")
    code = definition + r'''
$script:child=$null;$script:messages=@();$script:updates=@();$script:observerCalls=0
function Write-Host { param($Object) $script:messages += [string]$Object }
function Write-ColdMigrationDiagnosticStatus {
    param($Status,$Stage)
    $script:observerCalls++
    $script:updates += [pscustomobject]@{status=$Status;stage=$Stage}
    if(OBSERVER_ERROR){throw 'PRIVATE_DIAGNOSTIC_FAILURE_MUST_NOT_APPEAR'}
}
function New-ColdDatabaseRestoreProcess {
    param($Python,$Root,$SourcePackage)
    $start=New-Object Diagnostics.ProcessStartInfo
    $start.FileName='FIXTURE_EXECUTABLE'
    $start.Arguments='-NoProfile -NonInteractive -Command "[Console]::WriteLine(''CHILD_STDOUT_SECRET'');[Console]::Error.WriteLine(''CHILD_STDERR_SECRET'');Start-Sleep -Seconds 11;exit CHILD_EXIT"'
    $start.UseShellExecute=$false;$start.CreateNoWindow=$true;$start.RedirectStandardOutput=$true;$start.RedirectStandardError=$true
    $script:child=New-Object Diagnostics.Process;$script:child.StartInfo=$start
    [void]$script:child.Start();return $script:child
}
$errorCode='';$clock=[Diagnostics.Stopwatch]::StartNew()
try { Invoke-ColdDatabaseMaterialization 'C:\Fixture\python.exe' 'D:\Fixture' 'F:\Fixture' }
catch {$errorCode=$_.Exception.Message}
$disposed=$false;try{$script:child.WaitForExit(0)|Out-Null}catch{$disposed=$true}
[pscustomobject]@{error=$errorCode;elapsed=$clock.Elapsed.TotalSeconds;messages=$script:messages;
    updates=$script:updates;disposed=$disposed}|ConvertTo-Json -Depth 4 -Compress
'''
    code = (code.replace("FIXTURE_EXECUTABLE", executable).replace("CHILD_EXIT", str(child_exit))
            .replace("OBSERVER_ERROR", "$true" if observer_error else "$false"))
    result = run_ps(code)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "CHILD_STDOUT_SECRET" not in result.stdout + result.stderr
    assert "CHILD_STDERR_SECRET" not in result.stdout + result.stderr
    assert "PRIVATE_DIAGNOSTIC_FAILURE_MUST_NOT_APPEAR" not in result.stdout + result.stderr
    proof = json.loads(result.stdout)
    assert proof["elapsed"] >= 10
    assert len(proof["updates"]) >= 2
    assert proof["disposed"]
    assert all(item == {"status": "running", "stage": "cold-database-materialization"} for item in proof["updates"])
    assert any("still-running" in item and "not verified progress" in item for item in proof["messages"])
    assert proof["error"] == ("DATABASE_RESTORE_SUPERVISION_FAILED" if observer_error else
                             "DATABASE_RESTORE_PROCESS_FAILED" if child_exit else "")


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
