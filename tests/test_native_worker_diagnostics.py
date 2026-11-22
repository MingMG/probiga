import faulthandler
import json
import pytest
from sqlalchemy import create_engine, event, text

from server.engine import strategy_native_backtest as native
from tools.run_trading_v2_job_worker import run_native_backtest


def test_slow_failure_diagnostics_keep_the_business_result_and_stack(tmp_path,monkeypatch):
    from tools import run_ai_recommendation_premarket as diagnostics
    monkeypatch.setenv('PROBIGA_SCHEDULER_HISTORY_RUN_UID','1'*32)
    monkeypatch.setenv('PROBIGA_ANALYSIS_DIAGNOSTICS_ROOT',str(tmp_path))
    def failed(_engine):
        faulthandler.dump_traceback(file=diagnostics._DIAGNOSTIC_STREAM)
        return {'status':'FAILED','run_id':'existing-run','reason':'source deadline'}
    monkeypatch.setattr(native,'native_tick',failed)
    assert run_native_backtest(None)['run_id']=='existing-run'
    assert 'failed' in (tmp_path/(('1'*32)+'.stack.log')).read_text(encoding='utf-8')
    assert diagnostics._DIAGNOSTIC_STREAM is None


def test_fast_native_reads_do_not_accumulate_empty_diagnostic_files(tmp_path,monkeypatch):
    monkeypatch.setenv('PROBIGA_SCHEDULER_HISTORY_RUN_UID','2'*32)
    monkeypatch.setenv('PROBIGA_ANALYSIS_DIAGNOSTICS_ROOT',str(tmp_path))
    monkeypatch.setattr(native,'native_tick',lambda _engine:{'status':'COMPLETE'})
    assert run_native_backtest(None)['status']=='COMPLETE'
    assert not list(tmp_path.glob('*.stack.log'))


def test_unconfigured_diagnostics_use_the_existing_job_log_location(tmp_path,monkeypatch):
    monkeypatch.setenv('PROBIGA_SCHEDULER_HISTORY_RUN_UID','3'*32)
    monkeypatch.setenv('PROBIGA_ANALYSIS_DIAGNOSTICS_ROOT','')
    monkeypatch.setenv('PROBIGA_JOB_LOG_ROOT',str(tmp_path))
    monkeypatch.setattr(native,'native_tick',lambda _engine:{'status':'COMPLETE'})
    assert run_native_backtest(None)['status']=='COMPLETE'
    assert (tmp_path/'analysis-execution-diagnostics').is_dir()


@pytest.mark.parametrize('research_status', ['NOT_STARTED', 'RUNNING'])
def test_pending_model_replay_does_not_block_durable_native_report(monkeypatch, capsys, research_status):
    from tools import run_trading_v2_job_worker as worker
    from server.engine import autonomous_strategy_research as research
    from server.engine import strategy_native_control as control
    from server.engine import strategy_execution_adapters as adapters
    from server.engine.strategy_governance import _append_audit_connection
    from server.engine.shadow_trial_policy import SHADOW_EXECUTION_POLICY

    engine = create_engine('sqlite://')
    @event.listens_for(engine, 'connect')
    def functions(dbapi, _record):
        dbapi.create_function('IS_USED_LOCK', 1, lambda *_: 1)
        dbapi.create_function('CONNECTION_ID', 0, lambda: 1)
    owner = {'scheduler_run_uid': 'worker'}
    monkeypatch.setattr(native, '_owner', lambda *_a, **_k: owner)
    day = '2026-09-07'
    state = {'schema': native.SCHEMA, 'run_id': 'a' * 64, 'rule_hash': 'b' * 64,
             'contract': 'native', 'phase': 'REPLAY', 'status': 'NOT_STARTED',
             'completed_units': 2, 'total_units': 3, 'sessions': [day], 'input_sessions': [day],
             'strategies': [{'strategy_key': 'native', 'source_kind': 'immutable_manifest',
                             'parameters': {'shadow_execution_policy': SHADOW_EXECUTION_POLICY}}],
             'proposals': [{'strategy_key': 'native', 'rule_hash': 'c' * 64, 'validation': {}}]}
    with engine.begin() as c:
        c.execute(text('''CREATE TABLE st_strategy_governance_audit (
            audit_id TEXT PRIMARY KEY,entity_type TEXT,entity_key TEXT,action TEXT,reason TEXT,
            operator_name TEXT,before_json TEXT,after_json TEXT,evidence_json TEXT,payload_json TEXT,
            audit_hash TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP)'''))
        _append_audit_connection(c, **native._part(state, 'S', day,
            {'status': 'BLOCKED', 'signals': {}, 'reason': 'historical source unavailable'}),
            reason='saved source gap', operator='test', before=None, evidence={})
    def native_business_step(source):
        with source.connect() as c:
            native._step(c, state, owner)
        return {'status': state['status'], 'run_id': state['run_id']}
    dispose = engine.dispose
    monkeypatch.setattr(engine, 'dispose', lambda: None)
    monkeypatch.setattr(worker, 'load_project_env', lambda: None)
    monkeypatch.setattr(worker, 'create_batch_engine', lambda: engine)
    monkeypatch.setattr(worker, 'run_one_job', lambda _e: {'status': 'idle'})
    monkeypatch.setattr(adapters, 'bootstrap_strategy_execution_adapter_registry', lambda **_k: None)
    monkeypatch.setattr(research, 'research_tick', lambda _e: {'status': research_status})
    monkeypatch.setattr(control, 'consume_retries', lambda _e: {'status': 'NOT_STARTED'})
    monkeypatch.setattr(worker, 'run_native_backtest', native_business_step)
    try:
        assert worker.main() == 0
        result = json.loads(capsys.readouterr().out)
        assert result['autonomous_research']['status'] == research_status
        with engine.connect() as c:
            saved = native.read_runs(c)[0]
        assert saved['phase'] == 'FINISHED' and saved['completed_units'] == 3
        report = saved['proposals'][0]['validation']
        assert report['available_feature_sessions'] == 0
        assert report['source_gaps'] == [{'trade_date': day, 'reason': 'historical source unavailable'}]
        assert report['report']['net_return_pct'] is None
    finally:
        dispose()
