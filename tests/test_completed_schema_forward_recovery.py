"""Completed-schema recovery keeps the exact Windows writer and performs no DDL."""
from datetime import datetime
import json
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _function(name):
    source = (ROOT / 'deploy/production_deploy.sh').read_text(encoding='utf-8')
    match = re.search(rf'(?ms)^{name}\(\) \{{\n(.*?)^\}}$', source)
    assert match
    return match.group(0)


@pytest.mark.parametrize('failure', [None, 'missing_schema', 'wrong_seal', 'wrong_windows', 'extra_writer', 'windows_task_change'])
def test_completed_schema_proof_executes_current_validators_and_rejects_unproven_scope(tmp_path, monkeypatch, capsys, failure):
    from tools import check_schema_expected as schema
    from tools import ensure_quality_gate as gate
    from tools import scheduler_task_release as plans
    from server.common import production_runtime_schema_bundle as bundle
    from server.common import scheduler_runtime_health as health
    from server.common import qmt_edge_release_receipt as activation
    from server.engine import strategy_governance as governance
    from server.common import scheduler_runtime_schema, stock_dividend_schema, pit_facts, qmt_history_coverage
    from tools import sync_guojin_qmt_reference_data, prepare_strategy_governance_schema

    target = 'b' * 40
    contracts = [{key: task[key] for key in plans.FIELDS} for task in gate.TASKS]
    previous = json.loads(json.dumps(contracts))
    if failure == 'windows_task_change':
        next(t for t in previous if t['task_type'] == 'qmt_stock_daily_canonical')['description'] = 'old'
    plan = plans.build_plan(previous, contracts, previous_sha='a' * 40, target_sha=target)
    path = tmp_path / 'plan.json'
    path.write_text(json.dumps(plan), encoding='utf-8')
    now = datetime(2026, 11, 3, 7, 0)
    windows = dict(instance_id='windows-54940', host_name='windows', pid=54940,
                   mode='standalone', executor_role='qmt_windows_edge', build_sha=target,
                   heartbeat_age_seconds=1, poll_seconds=60, max_concurrent_tasks=1,
                   heartbeat_at=now, started_at=now)
    if failure == 'wrong_windows': windows['build_sha'] = 'c' * 40
    rows = [windows]
    if failure == 'extra_writer': rows += [{**windows, 'instance_id':'unknown-81', 'host_name':'unknown', 'pid':81, 'executor_role':'linux_standalone'}]

    class Connection:
        def __enter__(self): return self
        def __exit__(self, *_): pass
    # The identity function iterates mappings directly; use a normal sequence.
    class Result:
        def __init__(self, selected): self.selected=selected
        def mappings(self): return self
        def all(self): return self.selected
        def __iter__(self): return iter(self.selected)
    Connection.execute = lambda self, statement, params=None: Result([row for row in rows if not params or row['executor_role']==params['executor_role']])
    engine = SimpleNamespace(connect=Connection, dispose=lambda: None)
    monkeypatch.setattr(schema, '_open_read_only_boundary', lambda: SimpleNamespace(runtime_engine=engine, migrator_engine=engine))
    def validate(_engine):
        if failure == 'missing_schema': raise ValueError('physical column missing')
    monkeypatch.setattr(schema, '_bundle_validators', lambda: tuple((name, validate) for name, _ in bundle._VALIDATORS))
    monkeypatch.setattr(governance, 'validate_prepared_governance_runtime', lambda _: {})
    monkeypatch.setattr(governance, 'validate_privileged_trigger_migration_seal', lambda _, expected_build_sha: {
        'authority':'PRIVILEGED_CUTOVER_TABLE_METADATA_SEAL', 'attested_build_sha':('c'*40 if failure=='wrong_seal' else expected_build_sha)})
    monkeypatch.setattr(activation, 'check_qmt_edge_release_activation', lambda _, expected_build_sha: (True, {'status':'READY','activation_granted':True}))
    monkeypatch.setattr(scheduler_runtime_schema, 'validate_scheduler_runtime_heartbeat_schema', lambda _: {'physical_contract_verified':True})
    monkeypatch.setattr(stock_dividend_schema, 'validate_stock_dividend_schema', lambda _: {'status':'PASS'})
    monkeypatch.setattr(pit_facts, 'pit_fact_schema_health', lambda _: {'valid':True})
    monkeypatch.setattr(sync_guojin_qmt_reference_data, 'validate_reference_tables', lambda *a, **kw: {})
    monkeypatch.setattr(qmt_history_coverage, 'validate_coverage_schema', lambda *a, **kw: {'physical_schema_verified':True})
    monkeypatch.setattr(prepare_strategy_governance_schema, '_direct_acquisition_progress_schema', lambda *a, **kw: {'status':'READY','physical_schema_verified':True})
    code = _function('controlled_guard_completed_forward_windows_instance').split(" -c '\n",1)[1].split("\n' \"$guarded_sha\"",1)[0]
    monkeypatch.setattr(sys, 'argv', ['proof', target, str(path)])
    if failure:
        with pytest.raises(SystemExit) as error: exec(code, {})
        assert error.value.code == 2
        assert 'windows-54940' not in capsys.readouterr().out
    else:
        exec(code, {})
        assert capsys.readouterr().out.strip() == 'windows-54940'


@pytest.mark.parametrize('complete', [True, False])
def test_guard_dispatch_uses_no_schema_resume_only_after_completed_schema_proof(tmp_path, complete):
    bash = shutil.which('bash') or str(Path(r'C:\Program Files\Git\bin\bash.exe'))
    if not Path(bash).is_file(): pytest.skip('Bash required')
    target = 'b'*40
    venv = tmp_path / target
    venv.mkdir()
    (venv / '.adata.gitsha').write_text('c'*40)
    (venv / '.adata.tree.sha256').write_text('d'*64)
    guard = tmp_path / 'guard'
    guard.write_text('\n'.join(['probiga.database-writer-guard.v2','release='+target,
                              'main_unit=loaded,active,enabled','scheduler_unit=loaded,active,enabled',
                              'ai_service_unit=not-found,not-found,not-found','ai_timer_unit=not-found,not-found,not-found'])+'\n', newline='\n')
    q=lambda p: shlex.quote(str(p).replace('\\','/'))
    harness=f'''set -Eeuo pipefail
DATABASE_WRITER_GUARD_FILE={q(guard)}
DATABASE_WRITER_RESTORE_FILE={q(tmp_path / 'absent')}
RELEASE_VENV_ROOT={q(tmp_path)}
CODE_RELEASE_ROOT=/code
ADATA_RUNTIME_ROOT=/adata
ACTIVATION_GOVERNANCE_NEW_SNAPSHOT=/new-governance
controlled_guard_assert_storage() {{ return 0; }}
controlled_guard_assert_marker() {{ return 0; }}
controlled_guard_assert_restore_file() {{ return 0; }}
controlled_guard_completed_forward_windows_instance() {{ {'printf windows-54940' if complete else 'return 2'}; }}
systemctl() {{ printf test-user; }}
controlled_guard_run_writer_fence() {{
  test "$#" -eq 8 && test "$8" = windows-54940 || return 9
  printf FENCE_EXACT_WINDOWS_ONLY
}}
controlled_guard_validate_writer_fence_json() {{ read -r ignored || true; }}
controlled_guard_governance_contract_snapshot() {{ test "$1" = verify; }}
controlled_v2_forward_preserve_no_receipt_recovery() {{ test "$1" = windows-54940 && printf RUNTIME_RESTORED; }}
controlled_guard_certify_database_boundary() {{ printf GENERIC_BLOCKED_BY_FRESH_WINDOWS; return 3; }}
controlled_guard_write_restore_file() {{ return 20; }}
controlled_guard_cleanup() {{ return 21; }}
controlled_guard_restore_and_finalize() {{ return 22; }}
{_function('controlled_database_guard_recovery')}
controlled_database_guard_recovery {target}
'''
    script=tmp_path/'dispatch.sh'; script.write_text(harness, encoding='utf-8', newline='\n')
    result=subprocess.run([bash,str(script)],capture_output=True,text=True,timeout=15)
    if complete:
        assert result.returncode == 0, result.stdout+result.stderr
        assert 'RUNTIME_RESTORED' in result.stdout and 'GENERIC_' not in result.stdout
    else:
        assert result.returncode == 1, result.stdout+result.stderr
        assert 'GENERIC_BLOCKED_BY_FRESH_WINDOWS' in result.stdout
        assert 'RUNTIME_RESTORED' not in result.stdout


def test_runtime_only_forward_refuses_snapshot_drift_without_restoring_windows_configuration(tmp_path):
    bash = shutil.which('bash') or str(Path(r'C:\Program Files\Git\bin\bash.exe'))
    if not Path(bash).is_file(): pytest.skip('Bash required')
    body = _function('controlled_v2_forward_preserve_no_receipt_recovery')
    body = body.split('  V2_RECOVERY_STEP=forward-governance-verify-current\n',1)[1]
    body = body.split('  V2_RECOVERY_STEP=forward-governance-boundary',1)[0]
    harness = 'set -Eeuo pipefail\ncontinuing_windows_instance=windows-54940\nguarded_sha=' + 'b'*40 + '\nACTIVATION_GOVERNANCE_NEW_SNAPSHOT=/snapshot\ncontrolled_guard_governance_contract_snapshot() { test "$1" != restore || printf UNSAFE_WINDOWS_RESTORE; return 1; }\nrun_forward() {\n' + body + '\n}\nrun_forward\n'
    script = tmp_path / 'drift.sh'; script.write_text(harness, encoding='utf-8', newline='\n')
    result = subprocess.run([bash,str(script)], capture_output=True, text=True, timeout=15)
    assert result.returncode == 1
    assert 'Runtime-only recovery refuses' in result.stderr
    assert 'UNSAFE_WINDOWS_RESTORE' not in result.stdout


def test_existing_generic_writer_wait_blocks_current_windows_and_excludes_only_bound_instance(monkeypatch):
    from tools import add_trading_v3_tasks as fence
    from server.common.scheduler_tasks import evaluate_fresh_scheduler_writers
    windows = {'instance_id':'windows-54940', 'heartbeat_age_seconds':1, 'poll_seconds':60}
    other = {'instance_id':'unexpected-linux-81', 'heartbeat_age_seconds':1, 'poll_seconds':60}
    observations = [windows]
    monkeypatch.setattr(fence, 'read_fresh_scheduler_writers', lambda _: evaluate_fresh_scheduler_writers(observations))
    assert fence.wait_for_scheduler_writer_quiescence(None, timeout_seconds=0, poll_seconds=5) == (windows,)
    assert fence.wait_for_scheduler_writer_quiescence(None, timeout_seconds=0, poll_seconds=5,
        excluded_instance_ids=frozenset({'windows-54940'})) == ()
    observations.append(other)
    assert fence.wait_for_scheduler_writer_quiescence(None, timeout_seconds=0, poll_seconds=5,
        excluded_instance_ids=frozenset({'windows-54940'})) == (other,)


def test_old_set_restored_without_verified_windows_identity_cannot_enter_forward_recovery(tmp_path):
    bash = shutil.which('bash') or str(Path(r'C:\Program Files\Git\bin\bash.exe'))
    if not Path(bash).is_file(): pytest.skip('Bash required')
    body = _function('controlled_v2_forward_preserve_no_receipt_recovery')
    # Execute the real admission prefix; no deeper recovery mutation may run.
    body = body.split('  activation_snapshot_validate_new "$guarded_sha"',1)[0]
    harness = '''set -Eeuo pipefail
DEPLOY_OPERATION=recover-database-guard
DEPLOY_ARTIFACT_MODE=static-wheel-lock-v2
activation_snapshot_recorded_release() { printf bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb; }
activation_snapshot_old_release() { printf aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa; }
activation_snapshot_phase() { printf old-set-restored; }
run_forward() {
''' + body.split('\n',1)[1] + '\nprintf UNSAFE_FORWARD_ADMITTED\n}\nrun_forward\n'
    script = tmp_path/'unbound-old-set.sh'; script.write_text(harness,encoding='utf-8',newline='\n')
    result = subprocess.run([bash,str(script)],capture_output=True,text=True,timeout=15)
    assert result.returncode == 1, result.stdout+result.stderr
    assert 'UNSAFE_FORWARD_ADMITTED' not in result.stdout


def test_forward_recovery_branch_emits_numeric_deadline_accepted_by_actual_fenced_gate(tmp_path):
    bash = shutil.which('bash') or str(Path(r'C:\Program Files\Git\bin\bash.exe'))
    if not Path(bash).is_file(): pytest.skip('Bash required')
    verifier = _function('controlled_guard_verify_restored_runtime')
    prefix = '      if [ "$verification_mode" = forward-recovery ]; then\n'
    branch = prefix + verifier.split(prefix, 1)[1].split('\n      fi', 1)[0] + '\n      fi\n'
    deadline_gate = _function('controlled_guard_assert_activation_deadline')
    harness = '''set -Eeuo pipefail
readonly CONTROLLED_RECOVERY_CUTOVER_RESERVE_SECONDS=10800
verification_mode=forward-recovery
RESTORED_RUNTIME_GOVERNANCE_TRADE_DATE=""
RESTORED_RUNTIME_GOVERNANCE_CUTOVER_EPOCH=""
before="$(date +%s)"
produce_forward_result() {
''' + branch + '\nreturn 99\n}\nproduce_forward_result\n' + deadline_gate + '''
after="$(date +%s)"
[[ "$RESTORED_RUNTIME_GOVERNANCE_TRADE_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]
[[ "$RESTORED_RUNTIME_GOVERNANCE_CUTOVER_EPOCH" =~ ^[1-9][0-9]{9,11}$ ]]
test "$RESTORED_RUNTIME_GOVERNANCE_CUTOVER_EPOCH" -ge "$((before + 10800))"
test "$RESTORED_RUNTIME_GOVERNANCE_CUTOVER_EPOCH" -le "$((after + 10800))"
controlled_guard_assert_activation_deadline "$RESTORED_RUNTIME_GOVERNANCE_CUTOVER_EPOCH"
printf 'FORWARD_DEADLINE_ACCEPTED\n'
'''
    script=tmp_path/'forward-deadline.sh'
    script.write_text(harness,encoding='utf-8',newline='\n')
    result=subprocess.run([bash,str(script)],capture_output=True,text=True,timeout=15)
    assert result.returncode==0, result.stdout+result.stderr
    assert result.stdout.strip()=='FORWARD_DEADLINE_ACCEPTED'
