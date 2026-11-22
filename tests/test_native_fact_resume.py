from datetime import timedelta

from server.engine import strategy_native_backtest as native
from server.common.formal_analysis_store import FormalAnalysisStore
from server.db.formal_analysis_schema import privileged_migrate_formal_analysis_schema
from tests.test_autonomous_strategy_research import ledger


def test_runtime_repair_keeps_same_input_clock_archives_and_total_deadline(ledger):
    now=native.now_shanghai()
    strategy={'strategy_key':'native','strategy_name':'native','current_version':'v1','version_hash':'a'*64}
    days=['2026-09-07','2026-09-08']
    with ledger.begin() as c:
        from sqlalchemy import text
        c.execute(text('CREATE TABLE si_trade_calendar(trade_date TEXT,trade_status INT)'))
    with ledger.connect() as c:
        original={'schema':native.SCHEMA,'run_id':'d'*64,'rule_hash':'e'*64,'phase':'SIGNALS',
            'status':'FAILED','source_hash':'a'*64,'strategies':[strategy],'sessions':days,
            'requested_at':str(now-timedelta(hours=1)),'deadline_at':str(now+timedelta(hours=23)),
            'price_cutoff':str(now-timedelta(hours=2)),'input_archives':[],
            'fact_read_checkpoint':{'trade_date':days[0],'completed':800,'total':4755}}
        native._save(c,original,native._owner(c))
        revised=native._start(c,[strategy],days,{},'a'*64,'different-runtime')
        assert revised['requested_at']==original['requested_at']
        assert revised['price_cutoff']==original['price_cutoff']
        assert revised['deadline_at']==original['deadline_at']
        assert revised['fact_read_checkpoint']==original['fact_read_checkpoint']
        assert revised['run_id']!=original['run_id']
        assert revised['input_archives']==[{'run_id':original['run_id'],'rule_hash':original['rule_hash']}]
        fresh=native._start(c,[strategy],days,{},'b'*64,'different-runtime')
        assert fresh['requested_at']!=original['requested_at']
        assert fresh['input_archives']==[]
        assert fresh['fact_read_checkpoint']=={}


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
