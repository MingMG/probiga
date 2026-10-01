"""Scoped Windows power requests only; no installs, brokers, browsers or databases."""
from pathlib import Path
import json
import os
import re
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "tools/secondary_edge"
COMMON = TOOLS / "package_common.ps1"
PS = shutil.which("powershell.exe")
pytestmark = pytest.mark.skipif(os.name != "nt" or not PS, reason="Native Windows PS5 required")


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def run_ps(code, executable=None):
    return subprocess.run(
        [executable or PS, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command",
         "$ErrorActionPreference='Stop';$ProgressPreference='SilentlyContinue';" + code],
        capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=30,
    )


def helpers(code):
    return run_ps(f". {quote(COMMON)};" + code)


def main_try(script):
    path = TOOLS / (script + ".ps1")
    result = run_ps("$t=$null;$e=$null;$a=[System.Management.Automation.Language.Parser]::ParseFile("
                    + quote(path) + ",[ref]$t,[ref]$e);if($e.Count){throw ($e|Out-String)};"
                    "$s=@($a.EndBlock.Statements|Where-Object {$_ -is "
                    "[System.Management.Automation.Language.TryStatementAst]})[-1];"
                    "[pscustomobject]@{statement=$s.Extent.Text;finally=$s.Finally.Extent.Text}"
                    "|ConvertTo-Json -Compress")
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout)


@pytest.mark.parametrize("pointer_bytes,expected_size", [(8, 32), (4, 24)])
def test_reason_context_full_union_layout_in_native_ps5(pointer_bytes, expected_size):
    executable = PS if pointer_bytes == 8 else str(Path(os.environ["SystemRoot"]) /
                                                  "SysWOW64/WindowsPowerShell/v1.0/powershell.exe")
    if not Path(executable).is_file():
        pytest.skip("This Windows installation has no x86 PowerShell")
    result = run_ps(f". {quote(COMMON)};Initialize-ColdMigrationPowerNative;"
                    "[pscustomobject]@{pointer=[IntPtr]::Size;context="
                    "[Runtime.InteropServices.Marshal]::SizeOf((New-Object "
                    "ProBigA.ColdMigration.PowerReasonContext));status="
                    "[Runtime.InteropServices.Marshal]::SizeOf((New-Object "
                    "ProBigA.ColdMigration.PowerStatus))}|ConvertTo-Json -Compress", executable)
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout) == {"pointer": pointer_bytes, "context": expected_size, "status": 12}


def test_real_native_acquire_and_release_does_not_change_power_scheme():
    result = helpers("$ac=Get-ColdMigrationAcLineStatus;$policy=Get-ColdMigrationAcPolicy;"
                     "if($ac -ne 1 -or $policy -ne 1){throw 'NATIVE_PROBE_REQUIRES_AC_AND_ACCEPTING_POLICY'};"
                     "$lease=$null;try{$lease=New-ColdMigrationPowerLease;"
                     "if(-not $lease.RequestActive -or $lease.IsClosed){throw 'NOT_ACTIVE'}}"
                     "finally{Remove-ColdMigrationPowerLease $lease};"
                     "if(-not $lease.IsClosed -or $lease.RequestActive -or -not $lease.ReleaseSucceeded "
                     "-or $lease.ClearError -ne 0 -or $lease.CloseError -ne 0){throw 'NOT_RELEASED'};"
                     "$lease.Dispose();if((Get-ColdMigrationAcPolicy) -ne $policy){throw 'POLICY_CHANGED'};'released'")
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "released"


@pytest.mark.parametrize("ac,policy,error", [
    (0, 1, "MIGRATION_AC_POWER_REQUIRED"),
    (255, 1, "MIGRATION_AC_POWER_REQUIRED"),
    (2, 1, "MIGRATION_AC_POWER_REQUIRED"),
    (1, 0, "MIGRATION_POWER_POLICY_REFUSES_SYSTEM_REQUESTS"),
    (1, 2, "MIGRATION_POWER_POLICY_REFUSES_SYSTEM_REQUESTS"),
])
def test_battery_unknown_or_refusing_policy_cannot_create_request(ac, policy, error):
    result = helpers(f"function Get-ColdMigrationAcLineStatus{{return {ac}}};"
                     f"function Get-ColdMigrationAcPolicy{{return {policy}}};"
                     "function New-ColdMigrationNativePowerRequest{throw 'UNEXPECTED_CREATE'};"
                     "try{New-ColdMigrationPowerLease}catch{$_.Exception.Message}")
    assert result.returncode == 0, result.stderr
    assert error in result.stdout
    assert "UNEXPECTED_CREATE" not in result.stdout


@pytest.mark.parametrize("failure", ["status", "policy", "create"])
def test_power_api_errors_fail_closed_without_fallback(failure):
    status = "throw 'STATUS_API_ERROR'" if failure == "status" else "return 1"
    policy = "throw 'POLICY_API_ERROR'" if failure == "policy" else "return 1"
    result = helpers(f"function Get-ColdMigrationAcLineStatus{{{status}}};"
                     f"function Get-ColdMigrationAcPolicy{{{policy}}};"
                     "function New-ColdMigrationNativePowerRequest{throw 'CREATE_API_ERROR'};"
                     "try{New-ColdMigrationPowerLease}catch{$_.Exception.Message}")
    assert result.returncode == 0, result.stderr
    assert {"status": "STATUS_API_ERROR", "policy": "POLICY_API_ERROR", "create": "CREATE_API_ERROR"}[failure] in result.stdout


def test_post_acquisition_power_loss_disposes_acquired_request():
    result = helpers("$global:Checks=0;$global:Disposed=0;"
                     "function Get-ColdMigrationAcLineStatus{$global:Checks++;if($global:Checks -eq 1){1}else{0}};"
                     "function Get-ColdMigrationAcPolicy{1};"
                     "function New-ColdMigrationNativePowerRequest{$v=New-Object psobject;"
                     "$v|Add-Member ScriptMethod Dispose {$global:Disposed++};return $v};"
                     "try{New-ColdMigrationPowerLease|Out-Null}catch{};"
                     "if($global:Disposed -ne 1){throw 'REQUEST_LEAKED'};'disposed'")
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip().endswith("disposed")


@pytest.mark.parametrize("script", ["target_entry", "migrate_target"])
@pytest.mark.parametrize("terminal", ["success", "failure", "restart"])
def test_real_outer_finally_releases_lease_for_success_failure_and_3010(tmp_path, script, terminal):
    receipt = tmp_path / "release.json"
    block = main_try(script)["finally"]
    terminal_code = {"success": "exit 0", "failure": "throw 'SIMULATED_FAILURE'", "restart": "exit 3010"}[terminal]
    code = (f". {quote(COMMON)};$script:MigrationMutex=$null;$script:MigrationLockHeld=$false;"
            "$script:MigrationPowerLease=New-Object psobject;"
            "$script:MigrationPowerLease|Add-Member NoteProperty ClearError 0;"
            "$script:MigrationPowerLease|Add-Member NoteProperty CloseError 0;"
            "$script:MigrationPowerLease|Add-Member NoteProperty ReleaseSucceeded $true;"
            "$script:MigrationPowerLease|Add-Member ScriptMethod Dispose {"
            f"[IO.File]::WriteAllText({quote(receipt)},'released')" + "};"
            "try{" + terminal_code + "}finally" + block)
    result = run_ps(code)
    assert (result.returncode == 0) is (terminal == "success"), result.stdout + result.stderr
    assert receipt.read_text() == "released"


@pytest.mark.parametrize("failure", ["ac", "policy", "api", "source"])
def test_real_helper_blocks_before_full_payload_hash_and_target_writes(tmp_path, failure):
    package = tmp_path / "fixture-package"
    package.mkdir()
    manifest = {"format": "probiga.windows-cold-migration.v2", "source_host": "fixture-other-host",
                "source_paused": True, "production_activation": False, "restore_requested": False}
    if failure == "source":
        manifest["source_host"] = os.environ["COMPUTERNAME"]
    (package / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    power_ac = "0" if failure == "ac" else "1"
    power_policy = "0" if failure == "policy" else "1"
    code = (f". {quote(COMMON)};$InstallRoot='C:\\ProBigA-Power-Fixture';$PackageRoot={quote(package)};"
            "$script:MigrationPowerLease=$null;$script:ServiceOwned=$false;$script:InstallStage='package-validation';"
            "function Assert-PlainPath{};"
            "function Get-CimInstance{param($ClassName);if($ClassName -eq 'Win32_OperatingSystem'){"
            "[pscustomobject]@{BuildNumber=22000}}else{[pscustomobject]@{TotalPhysicalMemory=16GB}}};"
            f"function Get-ColdMigrationAcLineStatus{{{power_ac}}};"
            f"function Get-ColdMigrationAcPolicy{{{power_policy}}};"
            "function New-ColdMigrationNativePowerRequest{throw 'EXPECTED_CREATE_API_FAILED'};"
            f"function Assert-ColdPackage{{[IO.File]::WriteAllText({quote(tmp_path / 'unexpected-sha')},'bad');throw 'UNEXPECTED_SHA'}};"
            f"function New-Item{{[IO.File]::WriteAllText({quote(tmp_path / 'unexpected-write')},'bad');throw 'UNEXPECTED_WRITE'}};"
            + main_try("migrate_target")["statement"])
    result = run_ps(code)
    assert result.returncode != 0
    assert not (tmp_path / "unexpected-sha").exists(), result.stdout + result.stderr
    assert not (tmp_path / "unexpected-write").exists(), result.stdout + result.stderr


def test_lifetime_order_no_permanent_policy_or_display_lock_changes():
    common = COMMON.read_text(encoding="utf-8")
    entry = (TOOLS / "target_entry.ps1").read_text(encoding="utf-8")
    helper = (TOOLS / "migrate_target.ps1").read_text(encoding="utf-8")
    entry_body = main_try("target_entry")["statement"]
    helper_body = main_try("migrate_target")["statement"]
    acquire = "New-ColdMigrationPowerLease"
    assert helper_body.index("SOURCE_COMPUTER_BLOCKED") < helper_body.index(acquire)
    assert helper_body.index("Assert-PlainPath $InstallRoot") < helper_body.index(acquire)
    assert helper_body.index(acquire) < helper_body.index("$manifest = Assert-ColdPackage")
    assert helper_body.index(acquire) < helper_body.index("New-Item -ItemType Directory -Path $InstallRoot")
    assert entry_body.index("SOURCE_COMPUTER_BLOCKED") < entry_body.index(acquire)
    assert entry_body.index(acquire) < entry_body.index("-Verb RunAs")
    assert entry_body.index(acquire) < entry_body.index("& $codex login")
    assert "Remove-ColdMigrationPowerLease" in main_try("target_entry")["finally"]
    assert "Remove-ColdMigrationPowerLease" in main_try("migrate_target")["finally"]
    assert "PowerSetRequest(request, 1)" in common
    assert "PowerClearRequest(handle, 1)" in common
    assert "LocalFree(memory)" in common
    assert "SafeHandleZeroOrMinusOneIsInvalid" in common
    assert "SetThreadExecutionState" not in common
    assert not re.search(r"powercfg|PowerWrite|PowerSetActiveScheme|Set-ItemProperty", common + entry + helper, re.I)


def mock_native_source():
    """Compile the actual C# control flow with isolated deterministic API doubles."""
    text = COMMON.read_text(encoding="utf-8")
    source = text.split("Add-Type -Language CSharp -TypeDefinition @'\n", 1)[1].split("\n'@", 1)[0]
    source = source.replace("namespace ProBigA.ColdMigration", "namespace ProBigA.PowerFixture")
    source = source.replace("public static class PowerNative {", r'''
    public static class PowerNative {
        public static string Fault;
        public static int ClearCount, CloseCount, FreeCount, AcCount, PolicyCount;
        public static bool RequestTypeValid = true;
        public static string Calls = "";
''')
    bodies = {
        "GetSystemPowerStatus": r'''private static bool GetSystemPowerStatus(out PowerStatus status) {
            AcCount++; status = new PowerStatus();
            status.ACLineStatus = (byte)(Fault == "battery" ? 0 : Fault == "unknown" ? 255 :
                Fault == "post-ac" && AcCount == 2 ? 0 : 1);
            return Fault != "status-error";
        }''',
        "PowerCreateRequest": r'''private static IntPtr PowerCreateRequest(ref PowerReasonContext context) {
            Calls += "create;";
            if(context.Version != 0 || context.Flags != 1 || context.Reason.SimpleReasonString == IntPtr.Zero)
                throw new InvalidOperationException("WRONG_REASON_CONTEXT");
            return Fault == "create-error" ? new IntPtr(-1) : new IntPtr(123);
        }''',
        "PowerSetRequest": r'''private static bool PowerSetRequest(PowerRequestHandle request, int requestType) {
            Calls += "set;"; RequestTypeValid &= requestType == 1;
            return Fault != "set-error";
        }''',
        "PowerClearRequest": r'''internal static bool PowerClearRequest(IntPtr request, int requestType) {
            Calls += "clear;"; ClearCount++; RequestTypeValid &= requestType == 1;
            return Fault != "clear-error";
        }''',
        "CloseHandle": r'''internal static bool CloseHandle(IntPtr request) {
            Calls += "close;"; CloseCount++; return Fault != "close-error";
        }''',
        "PowerGetActiveScheme": r'''private static uint PowerGetActiveScheme(IntPtr root, out IntPtr memory) {
            memory = Marshal.AllocHGlobal(Marshal.SizeOf(typeof(Guid)));
            Marshal.StructureToPtr(Guid.Empty, memory, false);
            return Fault == "scheme-error" ? 5u : 0u;
        }''',
        "PowerReadACValueIndex": r'''private static uint PowerReadACValueIndex(IntPtr root,
            ref Guid scheme, ref Guid subgroup, ref Guid setting, out uint value) {
            PolicyCount++;
            value = Fault == "policy-zero" || Fault == "post-policy" && PolicyCount == 2 ? 0u :
                Fault == "policy-unknown" ? 255u : 1u;
            return Fault == "policy-error" ? 5u : 0u;
        }''',
        "LocalFree": r'''private static IntPtr LocalFree(IntPtr memory) {
            FreeCount++; Marshal.FreeHGlobal(memory); return IntPtr.Zero;
        }''',
    }
    for name, body in bodies.items():
        pattern = (r'\[DllImport\([^\n]*\)\]\s*(?:\[return:[^\n]*\]\s*)?'
                   r'(?:private|internal) static extern [^;]+?\b' + name + r'\([^;]+;')
        source, count = re.subn(pattern, lambda _: body, source, count=1)
        assert count == 1, name
    assert "DllImport" not in source
    return source


@pytest.mark.parametrize("fault,counts,expected", [
    ("none", (1, 1), "create;set;clear;close;"),
    ("battery", (0, 0), ""),
    ("unknown", (0, 0), ""),
    ("status-error", (0, 0), ""),
    ("policy-zero", (0, 0), ""),
    ("policy-unknown", (0, 0), ""),
    ("scheme-error", (0, 0), ""),
    ("policy-error", (0, 0), ""),
    ("create-error", (0, 0), "create;"),
    ("set-error", (0, 1), "create;set;close;"),
    ("post-ac", (1, 1), "create;set;clear;close;"),
    ("post-policy", (1, 1), "create;set;clear;close;"),
    ("clear-error", (1, 1), "create;set;clear;close;"),
    ("close-error", (1, 1), "create;set;clear;close;"),
])
def test_actual_csharp_failure_cleanup_with_native_api_doubles(fault, counts, expected):
    code = ("Add-Type -Language CSharp -TypeDefinition @'\n" + mock_native_source() + "\n'@\n"
            f"[ProBigA.PowerFixture.PowerNative]::Fault={quote(fault)};$lease=$null;$failed=$false;"
            "try{$lease=[ProBigA.PowerFixture.PowerNative]::Acquire()}catch{$failed=$true}"
            "finally{if($lease){$lease.Dispose();$lease.Dispose()}};"
            "[pscustomobject]@{failed=$failed;clear=[ProBigA.PowerFixture.PowerNative]::ClearCount;"
            "close=[ProBigA.PowerFixture.PowerNative]::CloseCount;free=[ProBigA.PowerFixture.PowerNative]::FreeCount;"
            "types=[ProBigA.PowerFixture.PowerNative]::RequestTypeValid;"
            "calls=[ProBigA.PowerFixture.PowerNative]::Calls;"
            "released=$(if($lease){$lease.ReleaseSucceeded}else{$null})}|ConvertTo-Json -Compress")
    result = run_ps(code)
    assert result.returncode == 0, result.stdout + result.stderr
    proof = json.loads(result.stdout)
    assert (proof["clear"], proof["close"]) == counts
    assert proof["calls"] == expected
    assert proof["types"] is True
    if fault in {"scheme-error", "policy-error", "policy-zero", "policy-unknown"}:
        assert proof["free"] == 1
    if fault in {"none", "clear-error", "close-error"}:
        assert proof["failed"] is False
        assert proof["released"] is (fault == "none")
    else:
        assert proof["failed"] is True

