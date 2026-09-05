"""Executable reproductions of the unresolved cross-host release outage.

These are CHARACTERIZATION tests, not recovery acceptance tests.  A passing
test proves that today's production code can remain stopped after a failed
release; it does not certify a lifecycle fix.  No production services, Git
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

    checker = tmp_path / "activation_checker.py"
    checker.write_text(
        "import json, sys\n"
        "status = " + repr(checker_status) + "\n"
        "sha = sys.argv[sys.argv.index('--expected-build-sha') + 1]\n"
        "if status == 'UNAVAILABLE':\n"
        "    print('activation database unavailable', file=sys.stderr)\n"
        "    raise SystemExit(9)\n"
        "print(json.dumps({'mode': 'check-activation', 'status': status,\n"
        "                  'build_sha': sha, 'activation_granted': False,\n"
        "                  'database_writes': False}))\n"
        "raise SystemExit(4)\n",
        encoding="utf-8",
    )
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
        $script:CheckoutSha = $TargetSha
        [void]$Events.Add("switch-checkout:" + $TargetSha)
        return
    }}
    if ($Arguments[0] -ceq "rev-parse") {{ return $script:CheckoutSha }}
    throw "Unexpected fake Git call"
}}
{activation_function}
try {{
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


def test_characterization_pending_switches_code_and_remains_stopped_on_retry(
    tmp_path: Path,
) -> None:
    first_exit, first = _activation_fault(
        tmp_path,
        current_sha=PRIOR_SHA,
        checker_status="PENDING",
        initially_running=True,
    )
    assert first_exit == 0  # Current bug: Task Scheduler observes success.
    assert first["checkout_sha"] == CANDIDATE_SHA
    assert first["scheduler_running"] is False
    assert first["events"][:2] == [
        "stop-edge", "switch-checkout:" + CANDIDATE_SHA
    ]
    assert "start-edge" not in first["events"]

    second_exit, second = _activation_fault(
        tmp_path,
        current_sha=CANDIDATE_SHA,
        checker_status="PENDING",
        initially_running=False,
    )
    assert second_exit == 0
    assert second["checkout_sha"] == CANDIDATE_SHA
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
    assert result["checkout_sha"] == CANDIDATE_SHA
    assert result["scheduler_running"] is False
    assert "start-edge" not in result["events"]


def test_characterization_pre_cutover_rollback_restores_linux_not_windows(
    tmp_path: Path,
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
DATABASE_FORWARD_MIGRATION_STARTED=0
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
verify_previous_main_health_or_stopped() {{ printf '%s\\n' verify-old-api >> "$TRACE"; }}
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
    assert f"receipt PREPARATION_FAILED {PRIOR_SHA}" in events
    # This is the demonstrated lifecycle gap, not a desired recovery contract.
    assert not any("qmt" in event.lower() or "windows" in event.lower() for event in events)
