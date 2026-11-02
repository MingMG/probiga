"""Execute the production Bash handoff with real heartbeat/drain decisions."""
from __future__ import annotations

import json
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _function(source, name):
    match = re.search(rf"(?ms)^{name}\(\) \{{\n(.*?)^\}}$", source)
    assert match is not None
    return name + "() {\n" + match.group(1) + "}\n"


# This driver substitutes the clock, database connection and Windows process
# boundary only. Production heartbeat freshness and both drain algorithms run.
DRIVER = r'''import json,sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from server.common.scheduler_tasks import evaluate_fresh_scheduler_writers
from tools import trading_v3_layer4_maintenance as maintenance
from tools import add_trading_v3_tasks as tasks
state_path=Path(sys.argv[2]); state=json.loads(state_path.read_text())
action=sys.argv[3]; args=sys.argv[4:]
def save(): state_path.write_text(json.dumps(state))
if action=="control":
    mode=args[3]; target=args[2]; attempt=args[4]; prior=args[5]
    state["events"].append(mode)
    if mode=="--abort-precutover":
        state["held"]=False; state["aborted"]=True; save()
        print(json.dumps({"status":"inserted","mode":"abort-precutover"})); sys.exit(0)
    base=dict(mode=mode[2:],build_sha=target,prior_build_sha=prior,
              deployment_attempt_id=attempt,activation_granted=False)
    if mode=="--request-forward-quiescence":
        if state["scenario"] in {"wrong_forward","forward"}:
            state["held"]=True; save()
            context=dict(schema="probiga.qmt-edge-forward-only-supersession.v1",
                protocol="probiga.qmt-edge-forward-only-supersession.v1",scope="FORWARD_ONLY_SUPERSESSION",
                build_sha=target,original_prior_build_sha=(prior if state["scenario"]=="forward" else "f"*40),
                deployment_attempt_id=attempt,real_order=False)
            print(json.dumps(dict(base,status="inserted",database_writes=True,context=context)))
        else:
            save()
            print(json.dumps(dict(base,status="not_applicable",database_writes=False,context=None)))
    else:
        state["held"]=True; save()
        context=dict(schema="probiga.qmt-edge-precutover-context.v1",
            protocol="probiga.qmt-edge-precutover-recovery.v1",build_sha=target,
            prior_build_sha=prior,deployment_attempt_id=attempt,prior_running=True,
            prior_host_name="windows",prior_pid=33320,prior_instance_id="windows-33320",real_order=False)
        if state["scenario"]=="bad_identity": context["prior_build_sha"]="f"*40
        if state["scenario"]=="lost_ack":
            print(json.dumps({"status":"UNAVAILABLE","reason":"lost_commit_ack"})); sys.exit(2)
        print(json.dumps(dict(base,status="inserted",database_writes=True,context=context)))
    sys.exit(0)
if action=="active": sys.exit(1 if state["linux_stopped"] else 0)
if action in {"stop","start"}:
    state["linux_stopped"]=action=="stop"; state["events"].append(action); save(); sys.exit(0)
class Clock:
    now=0
    def monotonic(self): return self.now
    def sleep(self,seconds): self.now+=seconds
clock=Clock()
clock.now=state.get("clock",0)
def live(_):
    windows_age=int(clock.now) if state["held"] else 0
    if (state["scenario"]=="drain_timeout" or (state["scenario"]=="late_failure" and "wait_passed" in state["events"])): windows_age=0
    rows=[dict(instance_id="linux-81",heartbeat_age_seconds=0,poll_seconds=60),
          dict(instance_id="windows-33320",heartbeat_age_seconds=windows_age,poll_seconds=60)]
    return evaluate_fresh_scheduler_writers(rows)
if action=="fence":
    tasks.read_fresh_scheduler_writers=live
    result=tasks.wait_for_scheduler_writer_quiescence(None,timeout_seconds=0,poll_seconds=5,
        excluded_instance_ids=frozenset({"linux-81"}),monotonic=clock.monotonic,sleep=clock.sleep)
    assert not result
    state["events"].append("fence_passed"); save(); print("FENCE_PASSED"); sys.exit(0)
class Connection:
    def __enter__(self): return self
    def __exit__(self,*_): pass
class Engine:
    def connect(self): return Connection()
    def dispose(self): pass
maintenance._connection_identity=lambda _: {"server_uuid":"verified"}
maintenance.read_fresh_scheduler_writers_on_connection=live
maintenance.time.monotonic=clock.monotonic; maintenance.time.sleep=clock.sleep
try:
    exclusions=frozenset(args[i+1] for i,a in enumerate(args) if a=="--exclude-instance-id")
    timeout=float(args[args.index("--timeout-seconds")+1])
    result=maintenance.wait_for_writer_quiescence(Engine(),timeout_seconds=timeout,poll_seconds=5,
        fast_exit_if_zero=True,excluded_instance_ids=exclusions)
except maintenance.MaintenanceBlocked as exc:
    print(json.dumps({"status":"blocked","reason_code":str(exc)})); sys.exit(2)
state["clock"]=clock.now; state["events"].append("wait_passed"); save()
print(json.dumps(result))
'''


def _run(tmp_path, scenario, *, request=True):
    bash = shutil.which("bash") or str(Path(r"C:\Program Files\Git\bin\bash.exe"))
    if not Path(bash).is_file(): pytest.skip("Bash required")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    driver = tmp_path / "driver.py"
    driver.write_text(DRIVER, encoding="utf-8")
    state = tmp_path / "state.json"
    state.write_text(json.dumps(dict(scenario=scenario,held=False,linux_stopped=False,
                                    aborted=False,events=[],clock=0)))
    command = " ".join(shlex.quote(str(p).replace("\\","/")) for p in
                       (sys.executable, driver, ROOT, state))
    handoff = _function(source,"prepare_qmt_windows_edge_handoff")
    start = ('if [ "$RELEASE_SCOPE" = COORDINATED ]; then\n'
             '  CUTOVER_STEP=request_qmt_windows_edge_quiescence_before_service_stop') if request else (
             'CUTOVER_STEP=stop_linux_scheduler_before_writer_quiescence')
    stopped = source[source.index(start):]
    stopped = stopped.split("CUTOVER_STEP=migrate_probiga_job_log_legacy_modes_after_writer_quiescence",1)[0]
    rollback = source.split('  if [ "$CUTOVER_STARTED" -eq 0 ]; then\n',1)[1]
    rollback = rollback.split('  echo "Deployment failed; rolling back',1)[0]
    # Remove the closing if of this extracted production pre-cutover branch.
    rollback = rollback.rsplit("  fi\n",1)[0]
    harness = f'''set -Eeuo pipefail
RELEASE_SCOPE=COORDINATED
EXPECTED_SHA={'b'*40}
PREVIOUS_SHA={'a'*40}
PREVIOUS_WINDOWS_SHA={'c'*40}
PREPARED_CODE_ROOT=/target
RELEASE_VENV_ROOT=/venvs
QMT_EDGE_DEPLOYMENT_ATTEMPT_ID={'d'*32}
BOOTSTRAP_PYTHON={shlex.quote(sys.executable.replace(chr(92),'/'))}
QMT_EDGE_RECOVERABLE_HANDOFF_ATTEMPTED=0
LOCAL_SCHEDULER_INSTANCE_ID=linux-81
LOCAL_SCHEDULER_PID=99999999
SCHEDULER_UNIT_PRESENT=1
PREVIOUS_SCHEDULER_ACTIVE=1
PRE_CUTOVER_SCHEDULER_STOPPED=0
CUTOVER_STARTED=0
DATABASE_FORWARD_MIGRATION_STARTED=0
PREVIOUS_CODE_ROOT=/prior
PREVIOUS_VENV=/prior-venv
PREVIOUS_INPUT_LOCK_SHA256=x
PREVIOUS_RESOLVED_FREEZE_SHA256=x
PREVIOUS_ADATA_SHA=x
PREVIOUS_ADATA_TREE_SHA256=x
controlled_guard_run_qmt_activation_tool() {{ {command} control "$@"; }}
run_prepared_python_tool() {{ {command} wait "$@"; }}
sudo() {{ if [ "$2" = stop ]; then {command} stop; elif [ "$2" = start ]; then {command} start; fi; }}
systemctl() {{ if [ "$1" = is-active ]; then {command} active; fi; }}
git() {{ printf '%s\\n' "$PREVIOUS_SHA"; }}
verify_previous_main_health_or_stopped() {{ return 0; }}
write_receipt() {{ printf 'RECEIPT:%s\\n' "$*"; }}
rollback() {{
  trap - ERR
  local failed_status="$1" rollback_failed=0 database_boundary_rollback_failed=0 current_sha
{rollback}
}}
trap 'rollback "$?"' ERR
{handoff}
{stopped}
{command} fence
printf 'DONE\\n'
'''
    (tmp_path / "harness.sh").write_text(harness, encoding="utf-8")
    result = subprocess.run([bash,str(tmp_path / "harness.sh")],capture_output=True,text=True,encoding="utf-8",timeout=30)
    return result,json.loads(state.read_text())


def test_fresh_windows_heartbeat_reproduces_original_deployment_failure(tmp_path):
    result,state = _run(tmp_path,"healthy",request=False)
    assert result.returncode == 2, result.stderr
    assert "LAYER4_FRESH_SCHEDULER_WRITERS_REMAIN:windows-33320" in result.stdout
    assert "RECEIPT:PREPARATION_FAILED " in result.stdout
    assert not state["linux_stopped"]
    assert "fence_passed" not in state["events"]


def test_exact_hold_drains_windows_before_linux_stop_and_both_writer_checks_pass(tmp_path):
    result,state = _run(tmp_path,"healthy")
    assert result.returncode == 0, result.stdout + result.stderr
    assert state["events"] == ["--request-forward-quiescence","--request-recoverable-quiescence","wait_passed","stop","wait_passed","fence_passed"]
    assert state["held"] and not state["aborted"]
    assert "DONE" in result.stdout


@pytest.mark.parametrize("scenario,reason",[("drain_timeout","LAYER4_FRESH_SCHEDULER_WRITERS_REMAIN"),
                                           ("lost_ack","lost_commit_ack"), ("bad_identity",None)])
def test_failed_handoff_keeps_linux_live_and_uses_existing_prior_abort(tmp_path,scenario,reason):
    result,state = _run(tmp_path,scenario)
    assert result.returncode == 2, result.stdout + result.stderr
    if reason: assert reason in result.stdout
    assert "stop" not in state["events"]
    assert state["aborted"] and not state["held"]
    assert "--abort-precutover" in state["events"]
    assert "fence_passed" not in state["events"]


def test_post_stop_failure_prints_reason_then_restores_linux_and_aborts_windows(tmp_path):
    result,state = _run(tmp_path,"late_failure")
    assert result.returncode == 2, result.stdout + result.stderr
    assert "LAYER4_FRESH_SCHEDULER_WRITERS_REMAIN:windows-33320" in result.stdout
    assert state["events"] == ["--request-forward-quiescence","--request-recoverable-quiescence",
                               "wait_passed","stop","start","--abort-precutover"]
    assert state["aborted"] and not state["held"] and not state["linux_stopped"]
    assert "RECEIPT:PREPARATION_FAILED " in result.stdout


def test_forward_hold_from_another_original_prior_fails_without_fresh_fallback(tmp_path):
    result,state = _run(tmp_path,"wrong_forward")
    assert result.returncode == 2, result.stdout + result.stderr
    assert state["events"] == ["--request-forward-quiescence"]
    assert state["held"] and not state["aborted"] and not state["linux_stopped"]
    assert "fence_passed" not in state["events"]


def test_matching_forward_hold_reuses_existing_pending_chain_without_fresh_handoff(tmp_path):
    result,state = _run(tmp_path,"forward")
    assert result.returncode == 0, result.stdout + result.stderr
    assert state["events"] == ["--request-forward-quiescence","wait_passed","stop","wait_passed","fence_passed"]
    assert state["held"] and not state["aborted"]
