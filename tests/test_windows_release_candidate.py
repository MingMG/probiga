from __future__ import annotations

import base64
from datetime import datetime, timedelta
import json
from pathlib import Path
import shutil
import subprocess
import sys

import pytest
from sqlalchemy import create_engine, text

from server.common import release_candidate as ledger
from tools import validate_windows_release_candidate as validator

ROOT = Path(__file__).resolve().parents[1]
BUILD, PRIOR, TREE = "2" * 40, "1" * 40, "3" * 40
NOW = datetime(2026, 9, 12, 21, 0)


@pytest.fixture
def evidence():
    return ledger.build_candidate(build_sha=BUILD, tree_sha=TREE, prior_build_sha=PRIOR,
        host_name="windows", validated_at=NOW, runtime_fingerprint="a" * 64,
        qmt_client_pid=77, model_instance_id="native-model")


@pytest.fixture
def engine():
    engine = create_engine("sqlite://")
    with engine.begin() as c:
        c.execute(text("CREATE TABLE st_scheduled_tasks (id INTEGER PRIMARY KEY, task_type TEXT)"))
        c.execute(text("INSERT INTO st_scheduled_tasks VALUES (7, 'qmt_reference_incremental')"))
        c.execute(text("CREATE TABLE st_scheduler_runtime (host_name TEXT, executor_role TEXT, build_sha TEXT)"))
        c.execute(text("INSERT INTO st_scheduler_runtime VALUES ('windows', 'qmt_windows_edge', :sha)"), {"sha": PRIOR})
        c.execute(text("CREATE TABLE st_scheduled_task_history (id INTEGER PRIMARY KEY, "
            "run_uid TEXT UNIQUE, task_id INTEGER, task_name TEXT, task_type TEXT, run_at TEXT, "
            "finished_at TEXT, status TEXT, duration INTEGER, exit_code INTEGER, output TEXT, "
            "host_name TEXT, scheduler_instance_id TEXT, build_sha TEXT, trigger_source TEXT)"))
    yield engine
    engine.dispose()


def read(c):
    return ledger.read_candidate(c, build_sha=BUILD, tree_sha=TREE, prior_build_sha=PRIOR, now=NOW)


def test_audit_is_idempotent_and_never_a_runtime_ready_receipt(engine, evidence):
    with engine.begin() as c:
        ledger.append_candidate(c, evidence, now=NOW)
        ledger.append_candidate(c, evidence, now=NOW)
        assert read(c) == evidence
        rows = c.execute(text("SELECT task_type, trigger_source FROM st_scheduled_task_history")).all()
        assert rows == [("qmt_edge_release_bootstrap", "release_candidate")]
        assert evidence["activation_granted"] is False


@pytest.mark.parametrize("changes", [
    {"build_sha": "9" * 40}, {"tree_sha": "9" * 40}, {"prior_build_sha": "9" * 40},
    {"runtime_fingerprint": "9" * 64}, {"qmt_client_pid": True}, {"model_instance_id": ""},
    {"activation_granted": True}, {"checks": {}}, {"extra": 1},
    {"validated_at": (NOW - timedelta(minutes=31)).isoformat()},
    {"validated_at": (NOW + timedelta(seconds=1)).isoformat()},
    {"validated_at": NOW.isoformat() + "+08:00"},
])
def test_changed_code_config_identity_or_expired_evidence_is_rejected(evidence, changes):
    with pytest.raises(ledger.CandidateValidationError):
        ledger.validate_candidate(dict(evidence, **changes), build_sha=BUILD, tree_sha=TREE,
                                  prior_build_sha=PRIOR, now=NOW)


@pytest.mark.parametrize("column,value", [
    ("run_uid", "wrong"), ("task_id", 8), ("scheduler_instance_id", "wrong"),
    ("run_at", "2000-01-01"), ("finished_at", "2000-01-01"),
    ("trigger_source", "release_bootstrap"), ("host_name", "linux"),
    ("status", "running"), ("exit_code", 1), ("output", "[]"),
])
def test_forged_or_unfinished_history_cannot_open_cutover_gate(engine, evidence, column, value):
    with engine.begin() as c:
        ledger.append_candidate(c, evidence, now=NOW)
        c.execute(text(f"UPDATE st_scheduled_task_history SET {column}=:value"), {"value": value})
        with pytest.raises(ledger.CandidateValidationError):
            read(c)


def test_stopped_prior_can_be_revalidated_without_granting_resume(engine, evidence):
    # Preparation uses host provenance, not a live scheduler lease. Existing
    # handoff/grant checks continue to own all permissions to stop/start writers.
    with engine.begin() as c:
        ledger.append_candidate(c, evidence, now=NOW)
        assert read(c)["activation_granted"] is False
        c.execute(text("INSERT INTO st_scheduler_runtime VALUES ('other', 'qmt_windows_edge', :sha)"), {"sha": PRIOR})
        with pytest.raises(ledger.CandidateValidationError, match="not unique"):
            read(c)


def ps(program):
    executable = shutil.which("powershell.exe") or shutil.which("pwsh")
    if not executable:
        pytest.skip("native PowerShell required")
    return subprocess.run([executable, "-NoProfile", "-NonInteractive", "-EncodedCommand",
        base64.b64encode(program.encode("utf-16-le")).decode()], capture_output=True, timeout=30)


def ps_function(file, name):
    # Ask PowerShell's parser for the actual function, avoiding regex bodies.
    return f"""
$tokens = $null; $errors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    '{str(ROOT / file).replace("'", "''")}', [ref]$tokens, [ref]$errors)
if ($errors.Count) {{ throw 'Invalid native PowerShell syntax' }}
$function = $ast.Find({{ param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -ceq '{name}'
}}, $true)
. ([scriptblock]::Create($function.Extent.Text))
"""


@pytest.mark.parametrize("passed", [False, True])
def test_real_updater_checks_candidate_before_first_stop(passed):
    program = """
$ErrorActionPreference = 'Stop'
$script:ForwardGitPreflightReady = $false
$ExpectedRoot = 'fixture'; $TargetSha = 'candidate'; $GitTimeoutSeconds = 45; $GitHubProxy = ''
$Stopped = $false
function Write-DeployGitContext { }
function Assert-DeployRepositoryIdle { }
function Invoke-Git { return 'fixture' }
function Assert-DeployDirectoryWritable { }
""" + ps_function("tools/update_qmt_windows_edge.ps1", "Confirm-ForwardGitPreflight") + f"""
function Confirm-WindowsCandidate {{ if ({'$false' if passed else '$true'}) {{ throw 'native candidate failed' }} }}
try {{ Confirm-ForwardGitPreflight; $Stopped = $true }} catch {{ }}
@{{stopped=$Stopped; ready=$script:ForwardGitPreflightReady}} | ConvertTo-Json -Compress
"""
    result = ps(program)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"stopped": passed, "ready": passed}


@pytest.mark.parametrize("passed", [False, True])
def test_real_linux_gate_failure_never_reaches_service_stop(tmp_path, passed):
    bash = shutil.which("bash") or ("C:/Program Files/Git/bin/bash.exe"
        if Path("C:/Program Files/Git/bin/bash.exe").is_file() else None)
    if not bash:
        pytest.skip("Bash required")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    start = source.index('if [ "$PREVIOUS_SHA" != "$EXPECTED_SHA" ]; then', source.index("CUTOVER_STEP=prepare_release"))
    end = source.index("\nfi", start) + 3
    # Execute the shipping gate; only Git and the external validator are fakes.
    script = tmp_path / "candidate-gate.sh"
    script.write_text("set -eu\nPREVIOUS_SHA=old\nEXPECTED_SHA=new\nCODE_GIT_CACHE=cache\nPREPARED_CODE_ROOT=prepared\n"
        "git() { echo tree; }\nrun_prepared_python_tool() { return " + ("0" if passed else "4") + "; }\n"
        + source[start:end] + "\nprintf SERVICE_STOP_REACHED\n")
    result = subprocess.run([bash, str(script)], capture_output=True, timeout=30)
    assert (b"SERVICE_STOP_REACHED" in result.stdout) is passed
    assert (result.returncode == 0) is passed
    assert source.index("validate_windows_candidate_before_service_stop") < source.index(
        "CUTOVER_STEP=initial_database_schema_preflight")


def test_probe_never_exposes_native_output_with_credentials(monkeypatch):
    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: subprocess.CompletedProcess(
        a, 1, b"mysql://secret:password@host", b"private-token"))
    with pytest.raises(ledger.CandidateValidationError, match="^CANDIDATE_IMPORTS_FAILED$"):
        validator.probe(["python"], env={}, stage="IMPORTS")


def test_runtime_override_can_never_install_dependencies():
    from tools.ensure_qmt_myquant_runtime import ensure_runtime, RuntimeNotReady
    with pytest.raises(RuntimeNotReady, match="MUST_BE_READ_ONLY"):
        ensure_runtime(BUILD, install=True, runtime_root=ROOT)


@pytest.mark.parametrize("fault", ["IMPORTS", "MYQUANT", "POWERSHELL", "RECOVERY_HEALTH", "changed", None])
def test_candidate_receipt_requires_every_real_probe_and_stable_inputs(monkeypatch, fault):
    if sys.platform != "win32":
        pytest.skip("Windows candidate orchestration")
    monkeypatch.setattr(validator, "ordinary", lambda p: p.resolve())
    monkeypatch.setattr(validator, "checkout", lambda *a, **kw: TREE)
    monkeypatch.setattr(validator, "git", lambda *a: "")
    monkeypatch.setattr(validator, "now", lambda: NOW)
    fingerprints = iter(["a" * 64, ("b" if fault == "changed" else "a") * 64])
    monkeypatch.setattr(validator, "runtime_fingerprint", lambda p: next(fingerprints))
    calls = []
    def probe(command, *, env, stage):
        calls.append(stage)
        assert env["PROBIGA_CODE_ROOT"] == str(ROOT)
        assert env["PROBIGA_EXPECTED_GIT_SHA"] == BUILD
        if fault == stage:
            raise ledger.CandidateValidationError("native probe failed")
        if stage == "IMPORTS":
            return {"imports": "PASS"}
        if stage == "MYQUANT":
            assert "--install" not in command
            return dict(status="READY", mode="check", build_sha=BUILD,
                        installed=False, database_writes=False, qmt_calls=False)
        if stage == "POWERSHELL":
            assert "-PreflightOnly" in command
            return dict(schema="probiga.bigqmt-ui-release-reload.v1", mode="PREFLIGHT_ONLY",
                status="READY", expected_build_sha=BUILD, qmt_client_pid=77,
                **{k: False for k in ("qmt_calls", "database_writes", "ui_actions_attempted",
                    "authentication_attempted", "automatic_order_submission", "direct_python_strategy_execution")})
        assert "-CheckOnly" in command
        return dict(schema="probiga.qmt-recovery-health.v1", healthy=True, failed_checks=[],
                    qmt_client_pid=77, model_instance_id="model", database_writes=False,
                    ui_actions_attempted=False)
    monkeypatch.setattr(validator, "probe", probe)
    runtime = Path(sys.executable).parents[2]
    if fault:
        with pytest.raises(ledger.CandidateValidationError):
            validator.validate_native(runtime, BUILD, PRIOR)
    else:
        result = validator.validate_native(runtime, BUILD, PRIOR)
        assert set(result["checks"].values()) == {"PASS"}
        assert result["activation_granted"] is False
    if fault in {"changed", None}:
        assert calls == ["IMPORTS", "MYQUANT", "POWERSHELL", "RECOVERY_HEALTH"]


def test_runtime_config_change_invalidates_fingerprint_without_exposing_values(tmp_path, monkeypatch):
    for name in (".env", ".venv/Scripts/python.exe", "runtime/qmt-py313/Scripts/python.exe"):
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"original")
    monkeypatch.setattr(validator.importlib.metadata, "distributions", lambda: [])
    before = validator.runtime_fingerprint(tmp_path)
    (tmp_path / ".env").write_bytes(b"changed private configuration")
    after = validator.runtime_fingerprint(tmp_path)
    assert len(before) == len(after) == 64
    assert before != after


@pytest.mark.parametrize("name", ["reload_big_qmt_strategy.ps1", "ensure_big_qmt_strategy_running.ps1"])
def test_runtime_override_rejects_mutating_modes(name):
    args = "-RegisteredRoot 'C:\\fixture' -ExpectedBuildSha '" + BUILD + "'" if name.startswith("reload") else ""
    result = ps(f"& '{ROOT / 'tools' / name}' {args} -RuntimeRoot 'C:\\fixture'")
    assert result.returncode != 0
    assert b"RuntimeRoot requires" in result.stderr or b"only for read-only" in result.stderr
