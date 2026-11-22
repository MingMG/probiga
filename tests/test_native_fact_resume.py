from datetime import timedelta

from server.engine import strategy_native_backtest as native
from server.common.formal_analysis_store import FormalAnalysisStore
from server.db.formal_analysis_schema import privileged_migrate_formal_analysis_schema
from tests.test_autonomous_strategy_research import ledger


def test_step_deadline_resumes_only_after_new_durable_finance_progress(ledger,monkeypatch):
    privileged_migrate_formal_analysis_schema(ledger)
    with ledger.connect() as c:
        c.connection.driver_connection.create_function('GET_LOCK',2,lambda *_:1)
        c.connection.driver_connection.create_function('RELEASE_LOCK',1,lambda *_:1)
        owner=native._owner(c)
        state={'schema':native.SCHEMA,'run_id':'a'*64,'phase':'SIGNALS','status':'NOT_STARTED',
            'step_status':'COMPLETE','contract':'code','completed_units':211,'total_units':408,
            'sessions':['2026-09-07'],'current_unit':'historical selection 2026-09-07',
            'deadline_at':str(native.now_shanghai()+timedelta(hours=24)),
            'proposals':[{'validation':{'status':'RUNNING'}}]}
        native._save(c,state,owner)
    monkeypatch.setattr(native,'_contract',lambda:'code')
    monkeypatch.setattr(native.signal,'SIGALRM',14,raising=False)
    monkeypatch.setattr(native.signal,'ITIMER_REAL',0,raising=False)
    monkeypatch.setattr(native.signal,'signal',lambda *_:None)
    monkeypatch.setattr(native.signal,'setitimer',lambda *_:None,raising=False)
    def partial(c,state,owner):
        store=FormalAnalysisStore(ledger)
        store.put('b'*64,'finance_issuer_facts','verified-chunk',{'codes':['000001'],'status':'VERIFIED_TEST_METADATA'})
        state['fact_read_checkpoint']={'trade_date':'2026-09-07','dataset':'finance',
            'completed':1,'total':2,'last_progress_at':str(native.now_shanghai())}
        native._save(c,state,owner)
        raise native.NativeStepDeadline('controlled business step deadline')
    monkeypatch.setattr(native,'_step',partial)
    result=native.native_tick(ledger)
    assert result['status']=='NOT_STARTED'
    with ledger.connect() as c:
        saved=native.read_runs(c)[0]
    assert saved['step_status']=='COMPLETE' and saved['fact_read_checkpoint']['completed']==1
    assert FormalAnalysisStore(ledger).get('b'*64,'finance_issuer_facts','verified-chunk')['codes']==['000001']
    later=native.now_shanghai()+timedelta(minutes=2)
    monkeypatch.setattr(native,'now_shanghai',lambda:later)
    stopped=native.native_tick(ledger)
    assert stopped['status']=='FAILED'
    with ledger.connect() as c:
        assert native.read_runs(c)[0]['status']=='FAILED'
