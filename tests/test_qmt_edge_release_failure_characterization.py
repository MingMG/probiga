"""Executable release transition and unresolved-boundary characterizations.

The retained-checkout/PENDING tests exercise the implemented narrow fix. They
do not certify real QMT bootstrap, MySQL authority, or post-schema recovery.
No production services, Git
checkouts, QMT terminals, credentials, or databases are used by this module.

The real PowerShell activation function and code-switch block are executed
with an in-memory scheduler/Git and a local fake read-only checker.  The real
Bash pre-cutover rollback is executed with fake systemd/Git and temporary
files.  See docs/qmt_edge_release_recovery_protocol_v2.md for the release gate.
"""
from __future__ import annotations

import base64
import json
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
PRIOR_SHA = "1" * 40
CANDIDATE_SHA = "2" * 40


def _powershell() -> str:
    executable = shutil.which("powershell") or shutil.which("pwsh")
    if executable is None:
        pytest.skip("PowerShell is required for the Windows fault reproduction")
    return executable


def _ps_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


@pytest.mark.parametrize("selected", [PRIOR_SHA, CANDIDATE_SHA])
def test_authorized_target_selection_never_reads_or_fetches_moving_main(tmp_path, selected):
    source = (ROOT / "tools/update_qmt_windows_edge.ps1").read_text(encoding="utf-8")
    start = source.index('$CurrentSha = ((Invoke-Git @("rev-parse", "HEAD"))')
    end = source.index("# Phase one is deliberately read-only", start)
    checker = tmp_path / "selector.py"
    checker.write_text("import json\nprint(" + repr(json.dumps({
        "mode": "select-update-target", "status": "SELECTED",
        "build_sha": PRIOR_SHA, "target_build_sha": selected,
        "database_writes": False, "writer_authorized": False,
    })) + ")\n", encoding="utf-8")
    program = f'''
$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$PythonExe = {_ps_literal(sys.executable)}
$BootstrapTool = {_ps_literal(checker)}
function Invoke-Git([string[]]$Arguments) {{
    if (($Arguments -join " ") -cne "rev-parse HEAD") {{ throw "network/main tip must not be consulted" }}
    return "{PRIOR_SHA}"
}}
{source[start:end]}
if ($TargetSha -cne "{selected}") {{ throw "authorized target differs" }}
'''
    encoded = base64.b64encode(program.encode("utf-16-le")).decode("ascii")
    result = subprocess.run([_powershell(), "-NoProfile", "-NonInteractive", "-EncodedCommand", encoded],
                            capture_output=True, timeout=30)
    assert result.returncode == 0, result.stderr


def _activation_fault(
    tmp_path: Path,
    *,
    current_sha: str,
    checker_status: str,
    initially_running: bool,
) -> tuple[int, dict[str, object]]:
    source = (ROOT / "tools/update_qmt_windows_edge.ps1").read_text(
        encoding="utf-8"
    )
    function_start = source.index("function Confirm-QmtReleaseActivation(")
    function_end = source.index("\n$TopLevel =", function_start)
    activation_function = source[function_start:function_end]
    phase_start = source.index("# Phase two may quiesce")
    phase_end = source.index(
        "\n$env:PROBIGA_BUILD_COMMIT_SHA = $CurrentSha", phase_start
    )
    switch_block = source[phase_start:phase_end]
    transition_start = source.index("$HandoffReadyToSwitch = $false")
    transition_end = source.index(
        "\nif ($CurrentSha -cne $TargetSha) {\n    # Fetch only for a real forward switch.",
        transition_start,
    )
    transition_block = source[transition_start:transition_end]

    checker = tmp_path / "activation_checker.py"
    checker.write_text(
        "import json, sys\n"
        "status = " + repr(checker_status) + "\n"
        "sha = sys.argv[sys.argv.index('--expected-build-sha') + 1]\n"
        "if status == 'UNAVAILABLE':\n"
        "    print('activation database unavailable', file=sys.stderr)\n"
        "    raise SystemExit(9)\n"
        "if '--check-transition' in sys.argv:\n"
        "    target = sys.argv[sys.argv.index('--target-build-sha') + 1]\n"
        "    print(json.dumps({'mode': 'check-transition', 'status': status,\n"
        "          'build_sha': sha, 'target_build_sha': target,\n"
        "          'database_writes': False, 'writer_authorized': False,\n"
        "          'context': None if status.startswith('LEGACY_') else {'protocol': 'probiga.qmt-edge-precutover-recovery.v1',\n"
        "                      'prior_build_sha': sha, 'prior_running': True, 'prior_pid': 41}}))\n"
        "else:\n"
        "    ready = status in {'RESUME_PRIOR', 'READY_TO_SWITCH', 'LEGACY_READY_TO_SWITCH'}\n"
        "    print(json.dumps({'mode': 'check-activation', 'status': 'READY' if ready else status,\n"
        "                      'build_sha': sha, 'activation_granted': ready, 'database_writes': False}))\n"
        "raise SystemExit(4 if status in {'PENDING', 'LEGACY_PENDING'} else 0)\n",
        encoding="utf-8",
    )
    runtime = tmp_path / "fake-runtime.json"
    runtime.write_text(json.dumps({"pid": 41, "build_sha": current_sha}), encoding="utf-8")
    program = f"""
$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$Events = [System.Collections.Generic.List[string]]::new()
$script:SchedulerRunning = {"$true" if initially_running else "$false"}
$CurrentSha = "{current_sha}"
$TargetSha = "{CANDIDATE_SHA}"
$script:CheckoutSha = $CurrentSha
$PythonExe = {_ps_literal(sys.executable)}
$BootstrapTool = {_ps_literal(checker)}
$SchedulerRuntimePath = {_ps_literal(runtime)}
$SchedulerTaskName = "fake-scheduler"
$PrecutoverRecoveryProtocol = "probiga.qmt-edge-precutover-recovery.v1"
function Get-ScheduledTask([string]$TaskName) {{
    return [PSCustomObject]@{{ State = $(if ($script:SchedulerRunning) {{ "Running" }} else {{ "Ready" }}) }}
}}
function Stop-EdgeScheduler {{
    $script:SchedulerRunning = $false
    [void]$Events.Add("stop-edge")
}}
function Start-EdgeScheduler {{
    $script:SchedulerRunning = $true
    [void]$Events.Add("start-edge")
}}
function Write-UpdateLog([string]$Message) {{ [void]$Events.Add("log:" + $Message) }}
function Invoke-Git([string[]]$Arguments) {{
    if ($Arguments[0] -ceq "merge") {{
        if ($Arguments[2] -cne $TargetSha) {{ throw "must merge the authorized SHA, not main tip" }}
        $script:CheckoutSha = $TargetSha
        [void]$Events.Add("switch-checkout:" + $TargetSha)
        return
    }}
    if ($Arguments[0] -ceq "rev-parse") {{ return $script:CheckoutSha }}
    throw "Unexpected fake Git call"
}}
{activation_function}
try {{
{transition_block}
    if ($CurrentSha -ceq $TargetSha) {{
        Confirm-QmtReleaseActivation $TargetSha
    }}
{switch_block}
}} finally {{
    [ordered]@{{
        checkout_sha = $script:CheckoutSha
        scheduler_running = $script:SchedulerRunning
        events = @($Events)
    }} | ConvertTo-Json -Compress
}}
"""
    encoded = base64.b64encode(program.encode("utf-16-le")).decode("ascii")
    completed = subprocess.run(
        [_powershell(), "-NoProfile", "-NonInteractive", "-EncodedCommand", encoded],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
        check=False,
    )
    payloads = [
        json.loads(line)
        for line in completed.stdout.splitlines()
        if line.startswith('{"checkout_sha":')
    ]
    assert len(payloads) == 1, (completed.returncode, completed.stdout, completed.stderr)
    return completed.returncode, payloads[0]


def test_pending_preserves_prior_checkout_and_returns_nonzero_on_retry(
    tmp_path: Path,
) -> None:
    first_exit, first = _activation_fault(
        tmp_path,
        current_sha=PRIOR_SHA,
        checker_status="PENDING",
        initially_running=True,
    )
    assert first_exit == 4
    assert first["checkout_sha"] == PRIOR_SHA
    assert first["scheduler_running"] is False
    assert first["events"][0] == "stop-edge"
    assert not any(event.startswith("switch-checkout:") for event in first["events"])
    assert "start-edge" not in first["events"]

    second_exit, second = _activation_fault(
        tmp_path,
        current_sha=PRIOR_SHA,
        checker_status="PENDING",
        initially_running=False,
    )
    assert second_exit == 4
    assert second["checkout_sha"] == PRIOR_SHA
    assert second["scheduler_running"] is False
    assert "start-edge" not in second["events"]


@pytest.mark.parametrize("checker_status", ["UNAVAILABLE", "ABORTED"])
def test_characterization_missing_or_unrecognized_authority_stays_fail_closed(
    tmp_path: Path, checker_status: str,
) -> None:
    exit_code, result = _activation_fault(
        tmp_path,
        current_sha=PRIOR_SHA,
        checker_status=checker_status,
        initially_running=True,
    )
    assert exit_code != 0
    assert result["checkout_sha"] == PRIOR_SHA
    assert result["scheduler_running"] is True  # No authority to disturb old process.
    assert "start-edge" not in result["events"]


@pytest.mark.parametrize("transition, expected_sha", [
    ("RESUME_PRIOR", PRIOR_SHA), ("READY_TO_SWITCH", CANDIDATE_SHA),
    ("LEGACY_READY_TO_SWITCH", CANDIDATE_SHA),
])
def test_terminal_transition_selects_exact_checkout_without_starting_a_writer(
    tmp_path: Path, transition: str, expected_sha: str,
) -> None:
    exit_code, result = _activation_fault(
        tmp_path, current_sha=PRIOR_SHA, checker_status=transition,
        initially_running=False,
    )
    assert exit_code == 0
    assert result["checkout_sha"] == expected_sha
    assert "start-edge" not in result["events"]  # Actual schema/QMT/bootstrap follows.


def test_compatibility_pending_keeps_checkout_and_cannot_start(tmp_path):
    exit_code, result = _activation_fault(
        tmp_path, current_sha=PRIOR_SHA, checker_status="LEGACY_PENDING",
        initially_running=True,
    )
    assert exit_code == 4
    assert result["checkout_sha"] == PRIOR_SHA
    assert result["scheduler_running"] is False
    assert "start-edge" not in result["events"]


@pytest.mark.parametrize("handoff, schema_started, api_ok, broker_ok", [
    (False, False, True, True),
    (True, False, True, True),
    (True, True, True, True),
    (True, False, False, True),
    (True, False, True, False),
])
def test_pre_cutover_rollback_calls_protected_abort_only_after_unchanged_old_runtime(
    tmp_path: Path, handoff: bool, schema_started: bool, api_ok: bool, broker_ok: bool,
) -> None:
    bash = shutil.which("bash")
    git_bash = Path(r"C:\Program Files\Git\bin\bash.exe")
    if bash is None and git_bash.is_file():
        bash = str(git_bash)
    if bash is None:
        pytest.skip("Bash is required for the Linux rollback fault reproduction")

    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    start = source.index("\nrollback() {")
    end = source.index("\ntrap 'rollback", start)
    rollback_function = source[start:end]
    trace_path = tmp_path / "rollback-trace.txt"
    trace = trace_path.as_posix()
    state_root = tmp_path.as_posix()
    program = f"""
set -eu
TRACE='{trace}'
DEPLOY_MAIN_BASHPID="$BASHPID"
DEPLOY_SUCCEEDED=0
EXPECTED_SHA={CANDIDATE_SHA}
PREVIOUS_SHA={PRIOR_SHA}
CUTOVER_STARTED=0
CUTOVER_STEP=after_windows_hold_before_cutover
DEFERRED_DB_CUTOVER_STARTED=0
PREPARED_CODE_ROOT='{state_root}/absent-prepared'
PREVIOUS_CODE_ROOT='{state_root}/fake-prior'
DATABASE_WRITER_GUARD_FILE='{state_root}/absent-guard'
DATABASE_WRITER_RESTORE_FILE='{state_root}/absent-restore'
DATABASE_FORWARD_MIGRATION_STARTED={int(schema_started)}
QMT_EDGE_RECOVERABLE_HANDOFF_ATTEMPTED={int(handoff)}
QMT_EDGE_DEPLOYMENT_ATTEMPT_ID={'b' * 32}
PREVIOUS_VENV='{state_root}/fake-prior-venv'
PRE_CUTOVER_SCHEDULER_STOPPED=1
SCHEDULER_UNIT_PRESENT=1
PREVIOUS_SCHEDULER_ACTIVE=1
PREVIOUS_INPUT_LOCK_SHA256=''
PREVIOUS_RESOLVED_FREEZE_SHA256=''
PREVIOUS_ADATA_SHA=''
PREVIOUS_ADATA_TREE_SHA256=''
detach_failure_handler_from_transport() {{ :; }}
persist_deploy_failure_audit() {{ printf '%s' {'a' * 64}; }}
emit_deploy_failure_checkpoint() {{ printf '%s\\n' failure-checkpoint >> "$TRACE"; }}
activation_snapshot_committed_phase_for_release() {{ :; }}
sudo() {{ printf 'sudo %s\\n' "$*" >> "$TRACE"; }}
systemctl() {{ printf 'systemctl %s\\n' "$*" >> "$TRACE"; }}
git() {{ printf '%s' "$PREVIOUS_SHA"; }}
verify_previous_main_health_or_stopped() {{ printf '%s\\n' verify-old-api >> "$TRACE"; return {0 if api_ok else 1}; }}
controlled_guard_run_qmt_activation_tool() {{
  test "$1" = "$PREVIOUS_CODE_ROOT" && test "$2" = "$PREVIOUS_VENV" &&
    test "$3" = "$PREVIOUS_SHA" && test "$4" = --abort-precutover &&
    test "$5" = "$QMT_EDGE_DEPLOYMENT_ATTEMPT_ID" && test "$6" = "$EXPECTED_SHA" || exit 97
  printf '%s\\n' protected-qmt-abort >> "$TRACE"
  return {0 if broker_ok else 1}
}}
write_receipt() {{ printf 'receipt %s\\n' "$*" >> "$TRACE"; }}
{rollback_function}
rollback 23 999
"""
    harness = tmp_path / "rollback-harness.sh"
    harness.write_text(program, encoding="utf-8", newline="\n")
    completed = subprocess.run(
        [bash, "--noprofile", "--norc", harness.as_posix()],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
        check=False,
    )
    assert completed.returncode == 23, (completed.stdout, completed.stderr)
    events = trace_path.read_text(encoding="utf-8").splitlines()
    assert "sudo systemctl start probiga-scheduler" in events
    assert "verify-old-api" in events
    abort_called = handoff and not schema_started and api_ok
    assert ("protected-qmt-abort" in events) is abort_called
    if abort_called:
        assert events.index("verify-old-api") < events.index("protected-qmt-abort")
    blocked = not api_ok or (handoff and (schema_started or not broker_ok))
    receipt = "PREPARATION_FAILED_UNVERIFIED" if blocked else "PREPARATION_FAILED"
    assert f"receipt {receipt} {PRIOR_SHA}" in events
    if blocked and handoff:
        assert "RECOVERY_BLOCKED" in completed.stderr
    # No live Windows start occurs on Linux; the protected terminal must be
    # consumed independently by the updater with its own old-grant/seal checks.
    assert not any("start-edge" in event for event in events)
