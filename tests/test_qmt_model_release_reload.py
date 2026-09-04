from __future__ import annotations

import base64
import json
from pathlib import Path
import shutil
import subprocess
import time


ROOT = Path(__file__).resolve().parents[1]


def _source() -> str:
    return (ROOT / "tools" / "reload_big_qmt_strategy.ps1").read_text(
        encoding="utf-8"
    )


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


def _run_powershell_process(program: str) -> subprocess.CompletedProcess[str]:
    encoded = base64.b64encode(program.encode("utf-16-le")).decode("ascii")
    executable = shutil.which("powershell.exe") or shutil.which("pwsh")
    assert executable is not None, "PowerShell is required for release tests"
    return subprocess.run(
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


def _run_powershell(program: str) -> dict[str, bool]:
    completed = _run_powershell_process(program)
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout.strip())


def _powershell_literal(value: Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def test_release_reload_is_bound_to_clean_registered_exact_main() -> None:
    source = _source()

    assert "[string]$RegisteredRoot" in source
    assert "[string]$ExpectedBuildSha" in source
    assert "$Root -ine $ExpectedRoot" in source
    assert '"rev-parse", "--show-toplevel"' in source
    assert '"remote", "get-url", "origin"' in source
    assert '$Branch -cne "main"' in source
    assert "$Head -cne $ExpectedBuild" in source
    assert "$ExpectedBuild`:$StrategyRepositoryPath" in source
    assert "[string]$Release.strategy_git_blob -cne $Blob" in source
    assert '"status", "--porcelain", "--untracked-files=normal"' in source
    assert '"https://github.com/MingMG/probiga.git"' in source


def test_release_reload_requires_one_logged_in_interactive_qmt_client() -> None:
    source = _source()

    assert 'Get-Process -Name "XtItClient"' in source
    assert "$QmtClients.Count -ne 1" in source
    assert "$QmtClient.SessionId -ne [int]$CurrentSession" in source
    assert '$QmtMainTitle -notmatch "^\\s*\\d+\\s*-\\s*.+QMT"' in source
    assert "Assert-NoUnexpectedVisibleQmtWindow" in source
    assert "login, CAPTCHA, confirmation" in source
    assert '"NEEDS_USER_ACTION"' in source
    assert "$FinalExitCode = if ($NeedsUser) { 3 } else { 2 }" in source


def test_release_preflight_rejects_login_and_heartbeat_failures_immediately() -> None:
    source = _source()
    names = (
        "Throw-NeedsUserAction",
        "Get-HeartbeatProperty",
        "Test-HeartbeatProperty",
        "Test-HeartbeatProperties",
        "Assert-QmtInteractiveClientReady",
        "Assert-QmtClientHeartbeatReady",
        "Assert-QmtColdStartEvidence",
    )
    functions = "\n\n".join(_powershell_function(source, name) for name in names)
    program = f"""
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
{functions}
function Get-ReasonCode([scriptblock]$Operation) {{
    try {{
        & $Operation | Out-Null
        return "NONE"
    }}
    catch {{
        $Message = [string]$_.Exception.Message
        return @($Message -split ":", 3)[1]
    }}
}}
$LoggedOut = [pscustomobject]@{{
    Id = 33864
    MainWindowHandle = [IntPtr]1
    Path = "C:/QMT/bin.x64/XtItClient.exe"
    SessionId = 1
    MainWindowTitle = "Guojin QMT Trading Terminal"
}}
$LoggedIn = $LoggedOut | Select-Object *
$LoggedIn.MainWindowTitle = "123456 - Guojin QMT"
$LoggedIn | Add-Member -NotePropertyName StartTime `
    -NotePropertyValue ([DateTimeOffset]::FromUnixTimeSeconds(950).LocalDateTime)
$Mismatch = [pscustomobject]@{{
    status = "running"
    source = "gj_big_qmt_inner"
    pid = 1444
    updated_ts = 1000.0
}}
$Stale = $Mismatch | Select-Object *
$Stale.pid = 33864
$Stale.updated_ts = 900.0
$Fresh = $Stale | Select-Object *
$Fresh.updated_ts = 995.0
$Prior = $Mismatch | Select-Object *
$Prior.updated_ts = 900.0
$NotPrior = $Mismatch | Select-Object *
$NotPrior.updated_ts = 960.0
$Timer = [Diagnostics.Stopwatch]::StartNew()
$LoginReason = Get-ReasonCode {{
    Assert-QmtInteractiveClientReady @($LoggedOut) 1
}}
$PidReason = Get-ReasonCode {{
    Assert-QmtClientHeartbeatReady $Mismatch 33864 30 1000.0
}}
$StaleReason = Get-ReasonCode {{
    Assert-QmtClientHeartbeatReady $Stale 33864 30 1000.0
}}
$FreshReason = Get-ReasonCode {{
    Assert-QmtClientHeartbeatReady $Fresh 33864 30 1000.0
}}
$ColdStartReason = Get-ReasonCode {{
    Assert-QmtColdStartEvidence $Prior $LoggedIn
}}
$InvalidColdStartReason = Get-ReasonCode {{
    Assert-QmtColdStartEvidence $NotPrior $LoggedIn
}}
$Timer.Stop()
[ordered]@{{
    login_reason = $LoginReason
    pid_reason = $PidReason
    stale_reason = $StaleReason
    fresh_reason = $FreshReason
    cold_start_reason = $ColdStartReason
    invalid_cold_start_reason = $InvalidColdStartReason
    elapsed_ms = $Timer.ElapsedMilliseconds
}} | ConvertTo-Json -Compress
"""
    result = _run_powershell(program)

    assert result["login_reason"] == "QMT_LOGIN_REQUIRED"
    assert result["pid_reason"] == "QMT_HEARTBEAT_PID_MISMATCH"
    assert result["stale_reason"] == "QMT_HEARTBEAT_STALE"
    assert result["fresh_reason"] == "NONE"
    assert result["cold_start_reason"] == "NONE"
    assert result["invalid_cold_start_reason"] == (
        "QMT_COLD_START_EVIDENCE_INVALID"
    )
    assert result["elapsed_ms"] < 1000

    validators = "\n".join(
        _powershell_function(source, name)
        for name in (
            "Assert-QmtInteractiveClientReady",
            "Assert-QmtClientHeartbeatReady",
            "Assert-QmtColdStartEvidence",
        )
    )
    for forbidden in (
        "Show-QmtMainWindow",
        "Open-ExactStrategyEditor",
        "Invoke-ExactWindowClick",
        "capabilities",
        "Start-Process",
        "submit",
    ):
        assert forbidden not in validators


def test_updater_preflight_preserves_native_exit_and_only_routes_pid_restart(
    tmp_path: Path,
) -> None:
    updater = (ROOT / "tools" / "update_qmt_windows_edge.ps1").read_text(
        encoding="utf-8"
    )
    helper = _powershell_function(updater, "Invoke-ReadOnlyStrategyPreflight")

    def write_stub(path: Path, payload: dict[str, object], exit_code: int) -> None:
        encoded_payload = json.dumps(payload, separators=(",", ":"))
        path.write_text(
            "\n".join(
                (
                    "param([string]$RegisteredRoot, [string]$ExpectedBuildSha, "
                    "[switch]$PreflightOnly)",
                    f"[Console]::Out.WriteLine('{encoded_payload}')",
                    f"exit {exit_code}",
                )
            ),
            encoding="utf-8",
        )

    base_payload: dict[str, object] = {
        "schema": "probiga.bigqmt-ui-release-reload.v1",
        "mode": "PREFLIGHT_ONLY",
        "status": "NEEDS_USER_ACTION",
        "data_status": "DATA_BLOCKED",
        "expected_build_sha": "a" * 40,
        "qmt_calls": False,
        "database_writes": False,
        "ui_actions_attempted": False,
        "authentication_attempted": False,
        "automatic_order_submission": False,
        "direct_python_strategy_execution": False,
    }
    ready_stub = tmp_path / "ready.ps1"
    pid_stub = tmp_path / "pid-mismatch.ps1"
    persisted_stub = tmp_path / "persisted-recovery.ps1"
    running_finalize_stub = tmp_path / "running-finalize.ps1"
    login_stub = tmp_path / "login-required.ps1"
    write_stub(ready_stub, {"status": "READY"}, 0)
    write_stub(
        pid_stub,
        {**base_payload, "reason_code": "QMT_HEARTBEAT_PID_MISMATCH"},
        3,
    )
    write_stub(
        persisted_stub,
        {**base_payload, "reason_code": "QMT_COLD_START_RETRY_READY"},
        3,
    )
    write_stub(
        running_finalize_stub,
        {
            **base_payload,
            "reason_code": "QMT_COLD_START_RUNNING_FINALIZE_READY",
        },
        3,
    )
    write_stub(
        login_stub,
        {**base_payload, "reason_code": "QMT_LOGIN_REQUIRED"},
        3,
    )

    def invoke_program(stub: Path, emit_json: bool) -> str:
        invocation = (
            "$Result = Invoke-ReadOnlyStrategyPreflight "
            "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"
        )
        trailer = (
            '[ordered]@{ result = $Result } | ConvertTo-Json -Compress'
            if emit_json
            else invocation
        )
        if emit_json:
            trailer = invocation + "\n" + trailer
        return f"""
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$PowerShellExe = (Get-Process -Id $PID).Path
$StrategyReloader = {_powershell_literal(stub)}
$ExpectedRoot = {_powershell_literal(tmp_path)}
function Write-UpdateLog([string]$Message) {{ }}
{helper}
$script:LASTEXITCODE = 99
{trailer}
"""

    ready = _run_powershell_process(invoke_program(ready_stub, True))
    assert ready.returncode == 0, ready.stderr
    assert json.loads(ready.stdout.strip())["result"] == "READY"

    pid_mismatch = _run_powershell_process(invoke_program(pid_stub, True))
    assert pid_mismatch.returncode == 0, pid_mismatch.stderr
    assert json.loads(pid_mismatch.stdout.strip())["result"] == (
        "INITIAL_COLD_START_REQUIRED"
    )

    persisted = _run_powershell_process(invoke_program(persisted_stub, True))
    assert persisted.returncode == 0, persisted.stderr
    assert json.loads(persisted.stdout.strip())["result"] == (
        "PERSISTED_RECOVERY_REQUIRED"
    )

    running_finalize = _run_powershell_process(
        invoke_program(running_finalize_stub, True)
    )
    assert running_finalize.returncode == 0, running_finalize.stderr
    assert json.loads(running_finalize.stdout.strip())["result"] == (
        "PERSISTED_RECOVERY_REQUIRED"
    )

    started = time.monotonic()
    login = _run_powershell_process(invoke_program(login_stub, False))
    elapsed = time.monotonic() - started
    assert login.returncode == 3
    assert elapsed < 5.0
    forwarded = json.loads(login.stdout.strip().splitlines()[-1])
    assert forwarded["reason_code"] == "QMT_LOGIN_REQUIRED"
    assert forwarded["qmt_calls"] is False
    assert forwarded["ui_actions_attempted"] is False


def test_preflight_only_finishes_before_any_qmt_ui_action() -> None:
    source = _source()
    assert 'if (!$PreflightOnly -and -not ("ProBigAQmtReleaseWindow"' in source
    main = source.index(
        '$QmtClients = @(',
        source.index("QMT strategy release reload is already active"),
    )
    unique_client = source.index("Assert-QmtInteractiveClientReady", main)
    heartbeat = source.index("Assert-QmtClientHeartbeatReady", unique_client)
    preflight = source.index("if ($PreflightOnly)", heartbeat)
    show = source.index("Show-QmtMainWindow", preflight)

    assert main < unique_client < heartbeat < preflight < show
    preflight_block = source[preflight:show]
    for contract in (
        'mode = "PREFLIGHT_ONLY"',
        'status = "READY"',
        'qmt_calls = $false',
        'database_writes = $false',
        'ui_actions_attempted = $false',
        'authentication_attempted = $false',
        'automatic_order_submission = $false',
    ):
        assert contract in preflight_block

    failure_block = source[source.index("catch {", preflight):]
    for contract in (
        'status = $Status',
        'data_status = "DATA_BLOCKED"',
        "reason_code = $FailureReasonCode",
        'qmt_calls = $QmtCallsAttempted',
        'database_writes = $false',
        'authentication_attempted = $false',
        'automatic_order_submission = $false',
        "$FinalExitCode = if ($NeedsUser) { 3 } else { 2 }",
    ):
        assert contract in failure_block


def test_activation_pending_exits_four_before_reload_side_effects(
    tmp_path: Path,
) -> None:
    source = _source()
    gate_start = source.index(
        "# The updater that initiated the first coordinated release"
    )
    add_type = source.index(
        'if (!$PreflightOnly -and -not ("ProBigAQmtReleaseWindow"',
        gate_start,
    )
    gate = source[gate_start:add_type]

    assert "Get-QmtReleaseActivation $ExpectedBuild" in gate
    production_binding = gate.index(
        '$env:PROBIGA_DEPLOYMENT_MODE = "production"'
    )
    activation_check = gate.index("Get-QmtReleaseActivation $ExpectedBuild")
    assert production_binding < activation_check
    assert "exit 4" in gate
    for forbidden in (
        "Get-Process",
        "Show-QmtMainWindow",
        "Open-ExactStrategyEditor",
        "Invoke-ExactStrategyInstall",
        "Write-AtomicJson",
        "Write-PersistedRecoveryState",
        "New-Item",
        "Start-Process",
    ):
        assert forbidden not in gate
    assert gate_start < add_type
    assert add_type < source.index('$QmtClients = @(', add_type)
    assert add_type < source.index(
        "$Release = Invoke-ExactStrategyInstall",
        add_type,
    )

    marker = tmp_path / "unexpected-side-effect.txt"
    root = str(tmp_path.resolve()).replace("'", "''")
    marker_literal = _powershell_literal(marker)
    sha = "a" * 40
    program = f"""
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$PreflightOnly = $false
$Root = '{root}'
$ExpectedRoot = '{root}'
$ExpectedOrigin = "https://github.com/MingMG/probiga.git"
$ExpectedBuild = "{sha}"
$PythonExe = "python.exe"
$ReleaseBootstrap = "release-bootstrap.py"
function Assert-OrdinaryDirectory([string]$Path, [string]$Description) {{ }}
function Assert-OrdinaryFile([string]$Path, [string]$Description) {{ }}
function Invoke-Git([string[]]$Arguments) {{
    $Command = $Arguments -join " "
    if ($Command -ceq "rev-parse --show-toplevel") {{ return @($ExpectedRoot) }}
    if ($Command -ceq "remote get-url origin") {{ return @($ExpectedOrigin) }}
    if ($Command -ceq "symbolic-ref --short HEAD") {{ return @("main") }}
    if ($Command -ceq "rev-parse HEAD") {{ return @($ExpectedBuild) }}
    if ($Command -ceq "status --porcelain --untracked-files=normal") {{ return @() }}
    throw "unexpected git read: $Command"
}}
function Get-QmtReleaseActivation([string]$BuildSha) {{
    if ($env:PROBIGA_DEPLOYMENT_MODE -cne "production") {{
        throw "activation check did not bind production mode"
    }}
    return [pscustomobject]@{{
        granted = $false
        payload = [ordered]@{{
            mode = "check-activation"
            status = "PENDING"
            build_sha = $BuildSha
            deployment_attempt_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            activation_granted = $false
            reason_code = "QMT_EDGE_RELEASE_ACTIVATION_PENDING"
            database_writes = $false
        }}
    }}
}}
{gate}
[System.IO.File]::WriteAllText({marker_literal}, "unexpected")
exit 0
"""
    completed = _run_powershell_process(program)

    assert completed.returncode == 4, completed.stderr
    payload = json.loads(completed.stdout.strip())
    assert payload["status"] == "PENDING"
    assert payload["activation_granted"] is False
    assert payload["database_writes"] is False
    assert not marker.exists()


def test_preflight_only_skips_activation_gate() -> None:
    source = _source()
    gate_start = source.index(
        "# The updater that initiated the first coordinated release"
    )
    gate_end = source.index(
        'if (!$PreflightOnly -and -not ("ProBigAQmtReleaseWindow"',
        gate_start,
    )
    gate = source[gate_start:gate_end]
    program = f"""
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$PreflightOnly = $true
function Get-QmtReleaseActivation([string]$BuildSha) {{
    throw "activation gate must not run during preflight"
}}
{gate}
[Console]::Out.WriteLine('{{"preflight_continued":true}}')
"""
    completed = _run_powershell_process(program)

    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout.strip())["preflight_continued"] is True


def test_pid_restart_has_an_explicit_authenticated_cold_start_path() -> None:
    source = _source()
    main = source.index(
        '$QmtClients = @(',
        source.index("QMT strategy release reload is already active"),
    )
    client = source.index("Assert-QmtInteractiveClientReady", main)
    trusted_path = source.index("QMT_CLIENT_PATH_MISMATCH", client)
    heartbeat = source.index("Assert-QmtClientHeartbeatReady", trusted_path)
    restart_gate = source.index(
        '"NEEDS_USER_ACTION:QMT_HEARTBEAT_PID_MISMATCH:"', heartbeat
    )
    evidence = source.index("Assert-QmtColdStartEvidence", restart_gate)
    show = source.index("Show-QmtMainWindow", evidence)
    cold_branch = source.index("if ($ControlledColdStart)", show)
    install = source.index("$Release = Invoke-ExactStrategyInstall", cold_branch)
    reopen = source.index("$NewEditor = Open-ExactStrategyEditor", install)
    start = source.index("$Loaded = Start-ExactStrategy", reopen)
    receipt = source.index(
        "$Receipt = Complete-ColdStartRecovery $Release $Loaded", start
    )

    assert client < trusted_path < heartbeat < restart_gate < evidence < show
    assert show < cold_branch < install < reopen < start < receipt
    assert "[switch]$ColdStartRecovery" in source
    assert "$HeartbeatUpdatedTs -gt ($ClientStartedTs + 5.0)" in source
    completion = _powershell_function(source, "Complete-ColdStartRecovery")
    assert 'status = "COLD_START_COMPLETE"' in completion
    assert 'reason_code = "QMT_CLIENT_RESTART_RECOVERED"' in completion
    cold_block = source[
        cold_branch : source.index("$FinalExitCode = 0", receipt)
    ]
    for contract in (
        'qmt_calls = $QmtCallsAttempted',
        'database_writes = $false',
        'ui_actions_attempted = $UiActionsAttempted',
        'authentication_attempted = $false',
        'automatic_order_submission = $false',
    ):
        assert contract in completion
    start_function = _powershell_function(source, "Start-ExactStrategy")
    assert "$script:QmtCallsAttempted = $true" in start_function

    updater = (ROOT / "tools" / "update_qmt_windows_edge.ps1").read_text(
        encoding="utf-8"
    )
    helper = _powershell_function(updater, "Invoke-ReadOnlyStrategyPreflight")
    assert '"QMT_HEARTBEAT_PID_MISMATCH"' in helper
    assert '"INITIAL_COLD_START_REQUIRED"' in helper
    assert '"PERSISTED_RECOVERY_REQUIRED"' in helper
    assert '"QMT_LOGIN_REQUIRED"' not in helper
    assert 'if ($StrategyColdStartRequired)' in updater
    assert '$StrategyReloadArguments += "-ColdStartRecovery"' in updater


def test_unverified_start_blocks_across_rounds_until_new_stopped_proof() -> None:
    source = _source()
    functions = "\n\n".join(
        _powershell_function(source, name)
        for name in (
            "Get-HeartbeatProperty",
            "Test-HeartbeatProperty",
            "Test-HeartbeatProperties",
            "Test-TrustedStoppedHeartbeat",
        )
    )
    program = f"""
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
{functions}
$Client = [pscustomobject]@{{ Id = 33864 }}
$OldPid = [pscustomobject]@{{
    status = "running"; source = "gj_big_qmt_inner";
    pid = 1444; updated_ts = 1010.0
}}
$OldStopped = [pscustomobject]@{{
    status = "stopped"; source = "gj_big_qmt_inner";
    pid = 33864; updated_ts = 999.0
}}
$NewStopped = [pscustomobject]@{{
    status = "stopped"; source = "gj_big_qmt_inner";
    pid = 33864; updated_ts = 1000.001
}}
$WrongSource = $NewStopped | Select-Object *
$WrongSource.source = "other"
$FutureStopped = $NewStopped | Select-Object *
$FutureStopped.updated_ts = `
    [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() / 1000.0 + 60.0
[ordered]@{{
    old_pid_cannot_unlock = `
        !(Test-TrustedStoppedHeartbeat $OldPid $Client 1000.0)
    pre_click_stopped_cannot_unlock = `
        !(Test-TrustedStoppedHeartbeat $OldStopped $Client 1000.0)
    newer_current_stopped_unlocks = `
        (Test-TrustedStoppedHeartbeat $NewStopped $Client 1000.0)
    wrong_source_cannot_unlock = `
        !(Test-TrustedStoppedHeartbeat $WrongSource $Client 1000.0)
    future_timestamp_cannot_unlock = `
        !(Test-TrustedStoppedHeartbeat $FutureStopped $Client 1000.0)
}} | ConvertTo-Json -Compress
"""
    result = _run_powershell(program)
    assert result and all(result.values()), result

    main = source.index("$PreviousHeartbeat = Get-Heartbeat", source.index("try {"))
    persisted = source.index("Read-PersistedRecoveryState $QmtClient", main)
    generic = source.index("Assert-QmtClientHeartbeatReady", persisted)
    pending = source.index('"QMT_UNVERIFIED_START_PENDING"', persisted)
    assert main < persisted < pending < generic

    marker = source.index('Write-PersistedRecoveryState "UNVERIFIED_START"')
    start_attempted = source.index("$StartAttempted = $true", marker)
    run = source.index("$Loaded = Start-ExactStrategy", start_attempted)
    assert marker < start_attempted < run

    rollback = _powershell_function(source, "Invoke-ColdStartRollback")
    proof = rollback.index("Test-TrustedStoppedHeartbeat")
    close = rollback.index("Close-ExactStrategyEditor", proof)
    restore = rollback.index("Restore-OriginalArtifact", close)
    verify = rollback.index("Assert-OriginalArtifactMatchesBackup", restore)
    safe = rollback.index('"STOPPED_FILES_RESTORED"', verify)
    assert proof < close < restore < verify < safe

    persisted_reader = _powershell_function(source, "Read-PersistedRecoveryState")
    attempted_disk = persisted_reader.index("Assert-AttemptedArtifactMatchesRecovery")
    original_disk = persisted_reader.index("Assert-OriginalArtifactMatchesBackup")
    mixed_disk = persisted_reader.index("!$AttemptedMatches -and !$OriginalMatches")
    assert attempted_disk < original_disk < mixed_disk

    completion_helper = _powershell_function(source, "Complete-ColdStartRecovery")
    completion_write = completion_helper.index("Write-AtomicJson $CompletionPath")
    completion_readback = completion_helper.index(
        "$CompletionReadback = Get-Content", completion_write
    )
    completion_verify = completion_helper.index(
        'throw "QMT cold-start completion receipt readback differs"',
        completion_readback,
    )
    clear_marker = completion_helper.index(
        "Clear-PersistedRecoveryState", completion_verify
    )
    assert completion_write < completion_readback < completion_verify < clear_marker

    persisted_read = source.index("Read-PersistedRecoveryState $QmtClient", main)
    running_recovery = source.index(
        "Read-AttemptedReleaseIdentity $PersistedRecovery", persisted_read
    )
    running_proof = source.index("Test-ExpectedRecoveryHeartbeat", running_recovery)
    read_only_gate = source.index(
        "if ($PreflightOnly -or !$ColdStartRecovery)", running_proof
    )
    finalize_ready = source.index(
        '"QMT_COLD_START_RUNNING_FINALIZE_READY"', read_only_gate
    )
    running_complete = source.index("Complete-ColdStartRecovery", running_proof)
    running_done = source.index("if ($RecoveredRunningIdempotently)", running_complete)
    running_slice = source[running_recovery:running_done]
    assert (
        persisted_read
        < running_recovery
        < running_proof
        < read_only_gate
        < finalize_ready
        < running_complete
        < running_done
    )
    assert "Close-ExactStrategyEditor" not in running_slice
    assert "Restore-OriginalArtifact" not in running_slice
    assert "Invoke-ExactStrategyInstall" not in running_slice
    assert "Show-QmtMainWindow" not in running_slice

    post_run_completion = source.index(
        "$Receipt = Complete-ColdStartRecovery $Release $Loaded", marker
    )
    assert run < post_run_completion


def test_recovery_state_rejects_json_key_tamper_and_untrusted_owner() -> None:
    source = _source()
    strict_keys = _powershell_function(source, "Assert-StrictFlatJsonKeys")
    owner_guard = _powershell_function(source, "Assert-ProtectedPathOwner")
    program = f"""
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
{strict_keys}
function Get-PathOwnerSid([string]$Path, [string]$Description) {{
    if ($Path -eq "root-owned") {{ return "S-1-root" }}
    if ($Path -eq "user-owned") {{ return "S-1-user" }}
    return "S-1-other"
}}
$TrustedReloadStateOwnerSids = @("S-1-root", "S-1-user")
{owner_guard}
function Is-Rejected([scriptblock]$Operation) {{
    try {{ & $Operation; return $false }} catch {{ return $true }}
}}
$ValidKeys = @("schema", "state")
[ordered]@{{
    valid_flat_json = !(Is-Rejected {{
        Assert-StrictFlatJsonKeys '{{"schema":"v1","state":"safe"}}' $ValidKeys
    }})
    duplicate_key_rejected = Is-Rejected {{
        Assert-StrictFlatJsonKeys `
            '{{"schema":"v1","schema":"v2","state":"safe"}}' `
            $ValidKeys
    }}
    case_collision_rejected = Is-Rejected {{
        Assert-StrictFlatJsonKeys `
            '{{"schema":"v1","State":"safe"}}' `
            $ValidKeys
    }}
    root_owner_allowed = !(Is-Rejected {{
        Assert-ProtectedPathOwner "root-owned" "state"
    }})
    current_user_owner_allowed = !(Is-Rejected {{
        Assert-ProtectedPathOwner "user-owned" "state"
    }})
    arbitrary_owner_rejected = Is-Rejected {{
        Assert-ProtectedPathOwner "other-owned" "state"
    }}
}} | ConvertTo-Json -Compress
"""
    result = _run_powershell(program)
    assert result and all(result.values()), result


def test_release_reload_never_targets_an_ambiguous_or_other_model() -> None:
    source = _source()

    assert '$StrategyName = "PROBIGA_BIGQMT_BRIDGE"' in source
    assert "$EditorTitle = \"$StrategyName$EditorSuffix\"" in source
    assert "Assert-NoOtherStrategyEditors" in source
    assert "another QMT strategy editor is open" in source
    assert "target QMT strategy editor is not unique" in source
    assert "GetWindowThreadProcessId" in source
    assert "QMT click target identity changed" in source
    assert "Invoke-ExactWindowClick $Editor $EditorTitle" in source
    assert "Stop-Process" not in source
    assert "Start-Process" not in source


def test_release_reload_locates_the_visible_strategy_pane_and_fails_closed() -> None:
    source = _source()

    assert "FindStrategyPaneLeft" in source
    assert "CreateDIBSection" in source
    assert "BitBlt" in source
    assert "Get-QmtStrategyPaneLayout" in source
    assert "$FullWidthList" in source
    assert "$EmbeddedList" in source
    assert "$PaneLeft + 70" in source
    assert "$PaneLeft + 322" in source
    assert "the visible QMT model-research strategy pane is not unique" in source
    assert "Invoke-ExactScreenPointClick" in source
    assert "QMT point-click target identity changed" in source
    assert "QMT point click escapes the exact target window" in source
    assert "SearchX = 0.325" not in source
    assert "EditX = 0.458" not in source


def test_atomic_install_finishes_before_the_old_model_is_stopped() -> None:
    source = _source()
    spool = (ROOT / "integrations" / "bigqmt" / "spool.py").read_text(
        encoding="utf-8"
    )
    installer = (ROOT / "tools" / "run_big_qmt_bridge.py").read_text(
        encoding="utf-8"
    )

    backup = source.index("$Backup = New-ArtifactBackup")
    install = source.index("$Release = Invoke-ExactStrategyInstall")
    still_old = source.index("$StillOld = Get-Heartbeat", install)
    stop = source.index("Stop-ExactStrategy $Editor", still_old)
    close = source.index("Close-ExactStrategyEditor $Editor", stop)
    reopen = source.index("$NewEditor = Open-ExactStrategyEditor", close)
    start = source.index("$Loaded = Start-ExactStrategy", reopen)
    assert backup < install < still_old < stop < close < reopen < start

    assert "temporary_target = installed_target.with_name" in spool
    assert "_replace_with_retry(temporary_target, installed_target)" in spool
    assert spool.index("_replace_with_retry(temporary_target, installed_target)") < spool.index(
        "return target", spool.index("def install_qmt_strategy")
    )
    assert "manifest_temporary" in installer
    assert "os.fsync(handle.fileno())" in installer
    assert "_replace_with_retry(manifest_temporary, manifest_path)" in installer
    assert "Move-Item -LiteralPath $Temporary -Destination $Path -Force" in source


def test_loaded_identity_must_come_from_the_qmt_process_and_match_all_hashes() -> None:
    source = _source()

    required_fields = (
        "strategy_release_protocol",
        "strategy_identity_protocol",
        "strategy_identity_frozen",
        "strategy_identity_status",
        "strategy_build_sha",
        "strategy_git_blob",
        "strategy_source_sha256",
        "strategy_artifact_sha256",
        "strategy_loaded_identity_sha256",
    )
    for field in required_fields:
        assert f'"{field}"' in source
    assert "Test-HeartbeatProperties $Heartbeat $Required" in source
    assert '[int](Get-HeartbeatProperty $Heartbeat "pid") -eq [int]$QmtClient.Id' in source
    assert '"strategy_build_sha") -ceq $ExpectedBuild' in source
    assert '"strategy_identity_status") -eq "BOUND"' in source
    assert 'direct_python_strategy_execution = $false' in source
    assert 'automatic_order_submission = $false' in source
    assert "Test-ExpectedReleaseHeartbeat $Heartbeat $Release" in source


def test_legacy_and_bound_heartbeat_predicates_are_strictmode_safe() -> None:
    source = _source()
    names = (
        "Get-HeartbeatProperty",
        "Test-HeartbeatProperty",
        "Test-HeartbeatProperties",
        "Get-StrategyIdentityHeartbeatPropertyNames",
        "Test-SameHeartbeatPropertyShape",
        "Test-RunningHeartbeat",
        "Test-ExpectedReleaseHeartbeat",
        "Test-ExpectedRecoveryHeartbeat",
        "Test-OriginalReleaseHeartbeat",
    )
    functions = "\n\n".join(_powershell_function(source, name) for name in names)
    program = f"""
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$ReleaseProtocol = "release-v2"
$IdentityProtocol = "identity-v2"
$ExpectedBuild = "build-123"
$HeartbeatMaxAgeSeconds = 30
$QmtClient = [pscustomobject]@{{ Id = 123 }}
{functions}
function New-LegacyHeartbeat([string]$Status, [string]$UpdatedAt) {{
    return [pscustomobject][ordered]@{{
        schema_version = 2
        bridge_version = "bigqmt_inner_v2"
        source = "gj_big_qmt_inner"
        status = $Status
        updated_at = $UpdatedAt
        updated_ts = 1.0
        pid = 123
        last_error = ""
    }}
}}
$Release = [pscustomobject]@{{
    strategy_git_blob = "blob-123"
    strategy_source_sha256 = "source-123"
    strategy_artifact_sha256 = "artifact-123"
    strategy_loaded_identity_sha256 = "loaded-123"
}}
$LegacyPrevious = New-LegacyHeartbeat "running" "before"
$LegacyCurrent = New-LegacyHeartbeat "running" "after"
$PreviousHeartbeat = $LegacyPrevious
$PartialLegacy = New-LegacyHeartbeat "running" "partial"
$PartialLegacy | Add-Member -NotePropertyName strategy_build_sha `
    -NotePropertyValue "build-123"
$Exact = [pscustomobject][ordered]@{{
    schema_version = 2
    bridge_version = "bigqmt_inner_v2"
    strategy_release_protocol = "release-v2"
    strategy_identity_protocol = "identity-v2"
    strategy_identity_frozen = $true
    strategy_identity_status = "BOUND"
    strategy_build_sha = "build-123"
    strategy_git_blob = "blob-123"
    strategy_source_sha256 = "source-123"
    strategy_artifact_sha256 = "artifact-123"
    strategy_loaded_identity_sha256 = "loaded-123"
    source = "gj_big_qmt_inner"
    status = "running"
    updated_at = "exact"
    updated_ts = `
        [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() / 1000.0
    pid = 123
}}
$Tampered = $Exact | Select-Object *
$Tampered.strategy_artifact_sha256 = "wrong"
$MissingBoundField = $Exact | Select-Object *
$MissingBoundField.PSObject.Properties.Remove("strategy_git_blob")
$AttemptedTs = [double]$Exact.updated_ts - 1.0
$NotNewer = $Exact | Select-Object *
$NotNewer.updated_ts = $AttemptedTs
$FutureExact = $Exact | Select-Object *
$FutureExact.updated_ts = [double]$Exact.updated_ts + 60.0
$StaleExact = $Exact | Select-Object *
$StaleExact.updated_ts = [double]$Exact.updated_ts - 31.0
$Results = [ordered]@{{
    legacy_is_not_new_release = `
        !(Test-ExpectedReleaseHeartbeat $LegacyCurrent $Release)
    identical_legacy_is_valid_rollback = `
        (Test-OriginalReleaseHeartbeat $LegacyCurrent)
    partial_legacy_fails_closed = `
        !(Test-OriginalReleaseHeartbeat $PartialLegacy)
    empty_heartbeat_is_not_running = `
        !(Test-RunningHeartbeat ([pscustomobject]@{{}}))
    exact_bound_release_matches = `
        (Test-ExpectedReleaseHeartbeat $Exact $Release)
    tampered_bound_release_fails = `
        !(Test-ExpectedReleaseHeartbeat $Tampered $Release)
    delayed_exact_running_completes = `
        (Test-ExpectedRecoveryHeartbeat $Exact $Release $AttemptedTs)
    pre_attempt_running_cannot_complete = `
        !(Test-ExpectedRecoveryHeartbeat $NotNewer $Release $AttemptedTs)
    future_running_cannot_complete = `
        !(Test-ExpectedRecoveryHeartbeat $FutureExact $Release $AttemptedTs)
    stale_running_cannot_complete = `
        !(Test-ExpectedRecoveryHeartbeat `
            $StaleExact $Release ([double]$Exact.updated_ts - 120.0))
}}
$PreviousHeartbeat = $Exact
$Results["exact_bound_rollback_matches"] = `
    (Test-OriginalReleaseHeartbeat ($Exact | Select-Object *))
$Results["missing_bound_field_fails_closed"] = `
    !(Test-OriginalReleaseHeartbeat $MissingBoundField)
$Results | ConvertTo-Json -Compress
"""
    results = _run_powershell(program)

    assert results
    assert all(results.values()), results


def test_legacy_heartbeat_compatibility_does_not_use_missing_properties() -> None:
    source = _source()
    expected = _powershell_function(source, "Test-ExpectedReleaseHeartbeat")
    original = _powershell_function(source, "Test-OriginalReleaseHeartbeat")

    assert "$Heartbeat.strategy_" not in expected
    assert "$PreviousHeartbeat.strategy_identity_status" not in original
    assert "Test-SameHeartbeatPropertyShape" in original
    assert "$PreviousHasIdentityStatus" in original


def test_failed_reload_restores_the_previous_artifact_and_model_or_fails_closed() -> None:
    source = _source()

    assert "function New-ArtifactBackup" in source
    assert "function Restore-OriginalArtifact" in source
    assert "function Invoke-ModelRollback" in source
    assert "original-strategy-$Index.bin" in source
    assert "original-manifest.json" in source
    assert "failed-new-manifest.json" in source
    assert "Test-OriginalReleaseHeartbeat" in source
    assert '"OLD_MODEL_RETAINED"' in source
    assert '"OLD_MODEL_RESTORED"' in source
    assert '"FILES_OR_MODEL_UNVERIFIED"' in source
    assert '"FAILED_CLOSED"' in source
    assert '"NEEDS_USER_ACTION"' in source
    assert "Invoke-ModelRollback" in source
    assert "Invoke-ColdStartRollback" in source
    assert '"COLD_START_FILES_RESTORED"' in source
    assert '"COLD_START_MODEL_STOPPED_FILES_RESTORED"' in source
    assert "$FinalExitCode = if ($NeedsUser) { 3 } else { 2 }" in source
    assert source.index("Restore-OriginalArtifact") < source.index(
        "$Editor = Open-ExactStrategyEditor", source.index("function Invoke-ModelRollback")
    )
    rollback = source.index("$RollbackStatus = if ($ControlledColdStart)")
    classify = source.index("$Status = if ($NeedsUser)", rollback)
    assert rollback < classify


def test_updater_reloads_before_restarting_the_writer_and_bootstrap() -> None:
    updater = (ROOT / "tools" / "update_qmt_windows_edge.ps1").read_text(
        encoding="utf-8"
    )
    register = (
        ROOT / "tools" / "register_qmt_windows_edge_scheduler_task.ps1"
    ).read_text(encoding="utf-8")

    ready_preflight = updater.index(
        "Invoke-ReadOnlyStrategyPreflight $TargetSha"
    )
    ready_call = updater.index("--check-ready --expected-build-sha $TargetSha")
    ready_branch = updater.index("if ($ReadyExit -eq 0)", ready_call)
    ready_exit = updater.index("exit 0", ready_branch)
    migration_state = updater.index('$PreparedSha = ""', ready_exit)
    unavailable_branch = updater.index("if ($ReadyExit -ne 4)", ready_exit)
    unavailable_exit = updater.index("exit $ReadyExit", unavailable_branch)
    strategy_preflight = updater.index(
        "Invoke-ReadOnlyStrategyPreflight $CurrentSha",
        migration_state,
    )
    strategy_probe = updater.index(
        "--check-strategy --expected-build-sha $CurrentSha",
        strategy_preflight,
    )
    reload_arguments = updater.index("$StrategyReloadArguments = @(")
    call = updater.index("$StrategyReloadOutput = &", reload_arguments)
    start = updater.index("Start-EdgeScheduler", call)
    bootstrap = updater.index("--bootstrap --expected-build-sha", start)
    assert ready_preflight < ready_call < ready_branch < ready_exit < migration_state
    assert ready_exit < unavailable_branch < unavailable_exit < migration_state
    assert strategy_preflight < strategy_probe < reload_arguments < call < start < bootstrap
    preflight_helper = updater[
        updater.index("function Invoke-ReadOnlyStrategyPreflight"):
        updater.index("function Invoke-Git")
    ]
    assert "-PreflightOnly" in preflight_helper
    assert "$PreflightExit -eq 3" in preflight_helper
    assert "[Console]::Out.WriteLine" in preflight_helper
    assert "$StrategyReloadExit -eq 3" in updater
    assert 'exit 3' in updater
    assert "failed closed" in updater
    assert '"ProBigA\\qmt-model-reload"' in register
    assert "$StrategyReloader" in register

    reloader = (ROOT / "tools" / "reload_big_qmt_strategy.ps1").read_text(
        encoding="utf-8"
    )
    assert "@Arguments 2>$null" in reloader
    assert "@Arguments 2>&1" not in reloader
    assert '$ErrorActionPreference = "Continue"' in reloader
    assert "$ErrorActionPreference = $PreviousPreference" in reloader
    assert "$ExitCode = $LASTEXITCODE" in reloader
    assert "$ExitCode -ne 0" in reloader
