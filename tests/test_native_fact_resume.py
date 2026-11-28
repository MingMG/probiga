from copy import deepcopy
from datetime import timedelta

import pytest
from sqlalchemy import text

from server.common.formal_analysis_store import FormalAnalysisStore
from server.db.formal_analysis_schema import privileged_migrate_formal_analysis_schema
from server.engine import strategy_native_backtest as native
from server.engine import native_research_jobs as jobs
from tests.test_native_research_jobs import queue,experiment


@pytest.mark.parametrize('retained_units',[1,2])
def test_slow_seal_validation_does_not_reject_retained_finance_prefix(monkeypatch,retained_units):
    from server.common import pit_facts as pit
    from tests.test_pit_finance_event_facts import _sealed_common_cutoff_case
    engine,options=_sealed_common_cutoff_case()
    privileged_migrate_formal_analysis_schema(engine)
    monkeypatch.setattr(pit,'FINANCE_ATOMIC_BATCH_QUERY_CODE_LIMIT',1)
    monkeypatch.setattr(native,'_save',lambda *_args,**_kwargs:None)
    store=FormalAnalysisStore(engine)
    expected=pit.resolve_common_fact_cutoff(engine,**options)
    state={}
    callback=jobs._source_progress(None,state,None,'2026-08-25','finance_coverage',deadline=float('inf'))
    def retain(done,total,scope,fresh):
        callback(done,total,scope,fresh)
        if done==retained_units:raise jobs.PrepareYield('retained source prefix')
    with pytest.raises(jobs.PrepareYield):
        pit.resolve_common_fact_cutoff(engine,**options,finance_coverage_store=store,finance_coverage_progress=retain)
    clock=[0.]
    read_seal=pit._load_latest_finance_atomic_seal_evidence
    def slow_seal(*args,**kwargs):
        value=read_seal(*args,**kwargs)
        clock[0]+=146
        return value
    monkeypatch.setattr(pit,'_load_latest_finance_atomic_seal_evidence',slow_seal)
    monkeypatch.setattr(jobs.time,'monotonic',lambda:clock[0])
    callback=jobs._source_progress(None,state,None,'2026-08-25','finance_coverage',deadline=90)
    try:
        actual=pit.resolve_common_fact_cutoff(engine,**options,finance_coverage_store=store,finance_coverage_progress=callback)
        assert actual==expected
        assert state['source_progress']['completed']==state['source_progress']['total']==2
    finally:engine.dispose()


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

    def selection_step(connection, state, owner, *, prepare_budget=None):
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


def test_disconnected_finance_read_retries_each_verified_partition_once(queue, tmp_path, monkeypatch):
    from sqlalchemy import event
    from sqlalchemy.exc import OperationalError
    from pymysql.err import OperationalError as MySQLError
    from server.common import pit_facts as pit
    from server.common.pit_sealed_finance_read import load_sealed_finance_facts
    from server.engine import native_research_identity as identity
    from tests.test_pit_finance_event_facts import _sealed_common_cutoff_case

    source, options = _sealed_common_cutoff_case()
    selection = []
    pit.resolve_common_fact_cutoff(source, **options, capture_finance_selection=selection.append)
    seal = selection[0]
    day = seal.as_of_date.isoformat()
    unit = 'PREPARE finance ' + day
    generation = 'e' * 64
    privileged_migrate_formal_analysis_schema(queue)
    store = FormalAnalysisStore(queue)
    monkeypatch.setenv('PROBIGA_NATIVE_DATASET_ROOT', str(tmp_path))
    monkeypatch.setattr(identity, 'algorithm_identity', lambda _config: {'sha256': 'math'})
    monkeypatch.setattr(pit, 'FINANCE_ATOMIC_BATCH_QUERY_CODE_LIMIT', 1)
    state = experiment()
    state.update(phase='PREPARE_DAYS', current_unit=unit, source_error_retries={unit: 1})
    with queue.begin() as connection:
        jobs.enqueue(connection, state)

    def disconnect(_connection, _cursor, statement, parameters, _context, _many):
        if 'coverage_id IN' in statement and seal.members['002731']['coverage_id'] in parameters:
            raise OperationalError(statement, parameters, MySQLError(2013, 'Lost connection during query'))

    def read(connection, state, owner, **_kwargs):
        load_sealed_finance_facts(source, selection=seal, codes=options['codes'], as_of_date=seal.as_of_date,
            decision_at=seal.decision_at, fact_cutoff_at=seal.fact_cutoff_at, formal_store=store,
            formal_generation_sha256=generation,
            read_progress=jobs._source_progress(connection, state, owner, day, 'finance_facts', deadline=float('inf')))

    monkeypatch.setattr(native, '_step', read)
    event.listen(source, 'before_cursor_execute', disconnect)
    try:
        assert native.native_tick(queue)['status'] == 'RUNNING'
        artifacts = store.list_members(generation, 'finance_issuer_facts')
        assert len(artifacts) == 1
        later = jobs.now_shanghai() + timedelta(seconds=61)
        monkeypatch.setattr(jobs, 'now_shanghai', lambda: later)
        assert native.native_tick(queue)['status'] == 'FAILED'
        assert store.list_members(generation, 'finance_issuer_facts') == artifacts
        with queue.connect() as connection:
            saved = native.read_runs(connection)[0]
            assert saved['source_progress']['completed'] == 1 and saved['source_error_retries'][unit] == 1
            assert saved['requested_at'] == state['requested_at']
    finally:
        event.remove(source, 'before_cursor_execute', disconnect)
        source.dispose()


def test_explicit_recovery_renews_one_retry_without_resetting_progress_or_idempotence(queue,tmp_path,monkeypatch):
    from server.engine import strategy_native_control as control, native_research_identity as identity
    from tests.test_native_research_jobs import start
    monkeypatch.setenv('PROBIGA_NATIVE_DATASET_ROOT',str(tmp_path))
    monkeypatch.setattr(native,'_contract',lambda:'math')
    monkeypatch.setattr(identity,'algorithm_identity',lambda _config:{'sha256':'math'})
    state,owner=start(queue)
    unit='PREPARE selection 2026-11-19'
    prefix={'dataset':'event_metadata','completed':128,'total':15223}
    state.update(status='FAILED',current_unit=unit,source_progress=prefix,source_error_retries={unit:1})
    with queue.connect() as connection:jobs.checkpoint(connection,state,owner)
    result_id='native:'+state['run_id']+':'+('d'*64)
    request=control.request_retry(queue,result_id,'7'*32)
    def disconnected(*_args,**_kwargs):
        raise RuntimeError('历史事实读取失败：QMT_ANNOUNCEMENT_LIVE_BATCH_REPLY_FAILED:signed-historical-native')
    monkeypatch.setattr(native,'_step',disconnected)
    assert native.native_tick(queue)['status']=='RUNNING'
    assert control.request_retry(queue,result_id,'7'*32)==request
    with queue.connect() as connection:
        saved=native.read_runs(connection,state['run_id'])[0]
        assert saved['source_error_retries']=={unit:1}
        for key in ('requested_at','price_cutoff','source_progress','job_id'):
            assert saved[key]==state[key]
        assert connection.execute(text('SELECT COUNT(*) FROM st_job_v2')).scalar()==1
    later=jobs.now_shanghai()+timedelta(seconds=61)
    monkeypatch.setattr(jobs,'now_shanghai',lambda:later)
    assert native.native_tick(queue)['status']=='FAILED'


@pytest.mark.parametrize('reason',[
    'PIT_FINANCE_ATOMIC_BATCH_INVALID:OperationalError:errno=2013:stage=finance_seal_history',
    'QMT_ANNOUNCEMENT_LIVE_BATCH_REPLY_DEADLINE:signed-historical-directory:800:0',
    'QMT_ANNOUNCEMENT_LIVE_BATCH_REPLY_FAILED:signed-historical-head-directory:800:0',
])
def test_pit_connection_loss_uses_one_existing_retry_and_keeps_source_checkpoint(queue, tmp_path, monkeypatch, reason):
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
        raise RuntimeError('历史事实读取失败：'+reason)

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
    def bounded(connection,state,owner,*,prepare_budget=None):
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


def test_cached_event_metadata_and_observational_selection_do_not_yield_or_spam_saves(monkeypatch):
    clock = [100.0]
    saves = []
    monkeypatch.setattr(jobs.time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(native, '_save', lambda *_a, **_k: saves.append(clock[0]))
    assert jobs.source_deadline() == 400.0

    state = {}
    meta = jobs._source_progress(None, state, None, '2026-11-18', 'event_metadata', deadline=120.0)
    sel = jobs._source_progress(None, state, None, '2026-11-18', 'event_selection', deadline=120.0)
    cov = jobs._source_progress(None, state, None, '2026-11-18', 'event_coverage', deadline=120.0)

    # Cached metadata sub-blocks within checkpoint_interval must not trigger audit saves.
    clock[0] = 110.0
    meta(128, 1360, 'sub-block-1', False)
    meta(1360, 1360, 'sub-block-1', False)
    assert saves == []

    # Past deadline, cached metadata and pre-commit observational event_selection must
    # not yield before coverage_store.put commits event_coverage (fresh=True).
    clock[0] = 125.0
    meta(1152, 1360, 'sub-block-2', False)
    sel(1520, 5580, '', None)
    assert saves == []

    with pytest.raises(jobs.PrepareYield, match='event_coverage'):
        cov(1520, 5580, 'cov-gen', True)
    assert state['source_progress']['dataset'] == 'event_coverage'
    assert state['source_progress']['completed'] == 1520
