from copy import deepcopy
from datetime import timedelta

import pytest
from sqlalchemy import text

from server.common.formal_analysis_store import FormalAnalysisStore
from server.db.formal_analysis_schema import privileged_migrate_formal_analysis_schema
from server.engine import strategy_native_backtest as native
from server.engine import native_research_jobs as jobs
from tests.test_native_research_jobs import queue,experiment


def test_source_no_progress_is_not_swallowed_as_a_blocked_day(queue, tmp_path, monkeypatch):
    from server.common import pit_facts as pit
    from server.common import historical_announcement_read as history
    from server.engine import native_research_prepare as prepare
    from server.engine import native_research_identity as identity
    privileged_migrate_formal_analysis_schema(queue)
    monkeypatch.setenv('PROBIGA_NATIVE_DATASET_ROOT', str(tmp_path))
    monkeypatch.setattr(identity, 'algorithm_identity', lambda _config: {'sha256': 'math'})
    monkeypatch.setattr(prepare, 'source_deadline', lambda: -1)
    monkeypatch.setattr(pit, 'load_finance_atomic_batch_seal', lambda *_a, **_k: {'seal': 'verified'})
    state = experiment()
    state.update(phase='PREPARE_DAYS', completed_units=78, total_units=105)
    with queue.begin() as connection:
        jobs.enqueue(connection, state)

    def source(_engine, **kwargs):
        kwargs['metadata_progress'](100, 400, 'fixed-source', True)

    monkeypatch.setattr(history, 'validate_dated_announcement_batch', source)
    plan = {'day': '2026-09-07', 'decision_at': state['requested_at'], 'eligible': ['000001'],
            'finance_observation_at': '2026-09-07 23:59:59', 'snapshot_generation': 'e' * 64,
            'identity': {'status': 'VERIFIED'}}

    def selection_step(connection, state, owner, *, feature_budget=None):
        result = prepare.prepare_selection(connection, state, owner, plan)
        state['incorrect_data_gap'] = result
        state['completed_units'] += 6

    monkeypatch.setattr(native, '_step', selection_step)
    assert native.native_tick(queue)['status'] == 'RUNNING'
    later = jobs.now_shanghai() + timedelta(seconds=2)
    monkeypatch.setattr(jobs, 'now_shanghai', lambda: later)
    result = native.native_tick(queue)
    assert result['status'] == 'FAILED' and 'NO_PROGRESS' in result['reason']
    with queue.connect() as connection:
        saved = native.read_runs(connection)[0]
        assert saved['completed_units'] == 78 and 'incorrect_data_gap' not in saved
        assert saved['source_progress']['completed'] == 100


def test_unexpected_runtime_source_failure_is_not_a_missing_dataset(queue, monkeypatch):
    from server.common import pit_facts as pit
    from server.common import historical_announcement_read as history
    from server.engine import native_research_prepare as prepare
    privileged_migrate_formal_analysis_schema(queue)
    monkeypatch.setattr(pit, 'load_finance_atomic_batch_seal', lambda *_a, **_k: {'seal': 'verified'})

    def source(*_args, **_kwargs):
        raise RuntimeError('source reader failed')

    monkeypatch.setattr(history, 'validate_dated_announcement_batch', source)
    plan = {'day': '2026-09-07', 'decision_at': '2026-11-22 18:05:19', 'eligible': ['000001'],
            'finance_observation_at': '2026-09-07 23:59:59', 'snapshot_generation': 'e' * 64,
            'identity': {'status': 'VERIFIED'}}
    with queue.connect() as connection, pytest.raises(RuntimeError, match='历史事实读取失败'):
        prepare.prepare_selection(connection, experiment(), None, plan)


def test_pit_connection_loss_uses_one_existing_retry_and_keeps_source_checkpoint(queue, tmp_path, monkeypatch):
    from server.engine import native_research_identity as identity
    monkeypatch.setattr(identity, 'algorithm_identity', lambda _config: {'sha256': 'math'})
    monkeypatch.setenv('PROBIGA_NATIVE_DATASET_ROOT', str(tmp_path))
    state = experiment()
    state.update(phase='PREPARE_DAYS', completed_units=78, total_units=105,
                 current_unit='PREPARE selection 2026-11-18',
                 source_progress={'dataset': 'event_metadata', 'completed': 7808, 'total': 15223})
    with queue.begin() as connection:
        jobs.enqueue(connection, state)

    def disconnected(*_args, **_kwargs):
        native._assert_read_succeeded(
            'PIT_FINANCE_ATOMIC_BATCH_INVALID:OperationalError:errno=2013:stage=finance_seal_history')

    monkeypatch.setattr(native, '_step', disconnected)
    first = native.native_tick(queue)
    assert first['status'] == 'RUNNING'
    with queue.connect() as connection:
        saved = native.read_runs(connection)[0]
        assert connection.execute(text('SELECT status FROM st_job_v2')).scalar() == 'PENDING'
        assert saved['completed_units'] == 78 and saved['source_progress'] == state['source_progress']
        assert saved['requested_at'] == state['requested_at'] and saved['price_cutoff'] == state['price_cutoff']
        assert saved['source_error_retries'] == {state['current_unit']: 1}
    later = jobs.now_shanghai() + timedelta(seconds=61)
    monkeypatch.setattr(jobs, 'now_shanghai', lambda: later)
    assert native.native_tick(queue)['status'] == 'FAILED'


@pytest.mark.parametrize('reason', [
    'PIT_FINANCE_ATOMIC_BATCH_INVALID:ValueError',
    'PIT_FINANCE_ATOMIC_BATCH_INVALID:OperationalError:errno=1142:stage=finance_seal_history',
    'PIT_FINANCE_ATOMIC_BATCH_INVALID:OperationalError:errno=2013:unverified-tail',
])
def test_integrity_permission_and_unknown_diagnostics_are_not_transient(reason):
    assert not native._transient_source_error(RuntimeError('历史事实读取失败：' + reason))


@pytest.mark.parametrize('stage',['finance_facts','event_facts','event_metadata'])
def test_source_budget_yields_durable_progress_and_stops_when_the_prefix_does_not_advance(queue,tmp_path,monkeypatch,stage):
    privileged_migrate_formal_analysis_schema(queue)
    monkeypatch.setenv('PROBIGA_NATIVE_DATASET_ROOT',str(tmp_path))
    from server.engine import native_research_identity as identity
    monkeypatch.setattr(identity,'algorithm_identity',lambda _config:{'sha256':'math'})
    state=experiment()
    with queue.begin() as connection:jobs.enqueue(connection,state)
    def bounded(connection,state,owner,*,feature_budget=None):
        FormalAnalysisStore(queue).put('e'*64,'native_source_part','verified-prefix',{'members':['000001']})
        callback=jobs._source_progress(connection,state,owner,'2026-09-07',stage,deadline=-1)
        callback(100,400,'fixed-source',True)
    monkeypatch.setattr(native,'_step',bounded)
    first=native.native_tick(queue)
    assert first['status']=='RUNNING'
    with queue.connect() as connection:
        saved=native.read_runs(connection)[0]
        assert saved['source_progress']['completed']==100
        assert connection.execute(text('SELECT status FROM st_job_v2')).scalar()=='PENDING'
        assert saved['requested_at']=='2026-11-22 18:05:19'
    later=jobs.now_shanghai()+timedelta(seconds=2)
    monkeypatch.setattr(jobs,'now_shanghai',lambda:later)
    second=native.native_tick(queue)
    assert second['status']=='FAILED' and 'NO_PROGRESS' in second['reason']
    assert FormalAnalysisStore(queue).get('e'*64,'native_source_part','verified-prefix')=={'members':['000001']}
    assert not list(tmp_path.glob('*.pin'))


def test_untyped_legacy_audits_are_history_and_never_an_execution_queue(queue,monkeypatch):
    state=experiment();state.pop('job_id',None)
    state.update(checkpoint_sequence=1,status='NOT_STARTED')
    with queue.begin() as connection:native._append_state(connection,state)
    monkeypatch.setattr(native,'_inventory',lambda *_:pytest.fail('audit scans recreated a job'))
    monkeypatch.setattr(native,'_step',lambda *_:pytest.fail('untyped audit was executed'))
    assert native.native_tick(queue)=={'status':'idle'}
