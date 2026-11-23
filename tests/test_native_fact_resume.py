from copy import deepcopy
from datetime import timedelta

import pytest
from sqlalchemy import text

from server.common.formal_analysis_store import FormalAnalysisStore
from server.db.formal_analysis_schema import privileged_migrate_formal_analysis_schema
from server.engine import strategy_native_backtest as native
from server.engine import native_research_jobs as jobs
from tests.test_native_research_jobs import queue,experiment


@pytest.mark.parametrize('stage',['finance_facts','event_facts','event_metadata'])
def test_source_budget_yields_durable_progress_and_stops_when_the_prefix_does_not_advance(queue,tmp_path,monkeypatch,stage):
    privileged_migrate_formal_analysis_schema(queue)
    monkeypatch.setenv('PROBIGA_NATIVE_DATASET_ROOT',str(tmp_path))
    from server.engine import native_research_identity as identity
    monkeypatch.setattr(identity,'algorithm_identity',lambda _config:{'sha256':'math'})
    state=experiment()
    with queue.begin() as connection:jobs.enqueue(connection,state)
    def bounded(connection,state,owner):
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
