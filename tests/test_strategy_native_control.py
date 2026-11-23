import os
import socket
from datetime import datetime,timedelta

import pytest
from sqlalchemy import create_engine,event,text

from server.engine import strategy_native_backtest as native
from server.engine import strategy_native_control as control
from server.engine.strategy_governance import _append_audit_connection


@pytest.fixture
def ledger(monkeypatch):
    engine=create_engine('sqlite://')
    @event.listens_for(engine,'connect')
    def functions(dbapi,_record):
        for name in ('GET_LOCK','RELEASE_LOCK'):
            dbapi.create_function(name,2 if name=='GET_LOCK' else 1,lambda *_:1)
    with engine.begin() as c:
        c.execute(text('''CREATE TABLE st_strategy_governance_audit (
            audit_id TEXT PRIMARY KEY,entity_type TEXT,entity_key TEXT,action TEXT,reason TEXT,
            operator_name TEXT,before_json TEXT,after_json TEXT,evidence_json TEXT,payload_json TEXT,
            audit_hash TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP)'''))
        c.execute(text('CREATE TABLE st_scheduled_task_history(run_uid TEXT,status TEXT)'))
        c.execute(text('CREATE TABLE si_trade_calendar(trade_date TEXT,trade_status INT)'))
        c.execute(text("INSERT INTO si_trade_calendar VALUES('2026-06-29',1),('2026-06-30',1)"))
        c.execute(text("INSERT INTO st_scheduled_task_history VALUES('old','failed')"))
        state={'schema':native.SCHEMA,'run_id':'a'*64,'rule_hash':'b'*64,'contract':'code',
            'requested_at':'2026-11-22 18:05:19','price_cutoff':'2026-11-22 05:59:24',
            'deadline_at':'2026-11-23 18:05:19',
            'trade_date':'2026-07-03','source_hash':'frozen-source','published':{},'input_archives':[],
            'strategies':[{'strategy_key':'quality_momentum','strategy_name':'质量动量','current_version':1,
                'version_hash':'c'*64,'source_kind':'immutable_v3_sleeve'}],
            'status':'FAILED','step_status':'FAILED','phase':'SIGNALS','checkpoint_sequence':3,
            'cursor':1,'completed_units':141,'sessions':['2026-07-01','2026-07-02','2026-07-03'],
            'proposals':[{'rule_hash':'c'*64,'validation':{'status':'FAILED'}}],
            'owner':{'host':socket.gethostname(),'pid':os.getpid(),'pid_birth':'old-birth','scheduler_run_uid':'old'}}
        _append_audit_connection(c,entity_type=native.ENTITY,entity_key=state['run_id'],action='CHECKPOINT_00000003',
            operator='test',reason='deadlock',before=None,after=state,evidence={})
        for day in state['sessions'][:2]:
            _append_audit_connection(c,**native._part(state,'S',day,{'status':'BLOCKED','reason':'真实来源缺失'}),
                operator='test',reason='saved before deadlock',before=None,evidence={})
    monkeypatch.setattr(native,'_contract',lambda:'code')
    monkeypatch.setattr(control,'process_identity',lambda _pid:'new-birth')
    monkeypatch.setattr(control.os,'kill',lambda *_:None)
    monkeypatch.setattr(control,'_owner',lambda _c:{'scheduler_run_uid':'current'})
    monkeypatch.setattr(control,'now_shanghai',lambda:datetime(2026,11,23,17,45))
    yield engine
    engine.dispose()


def test_retry_idempotency_and_preserved_durable_prefix(ledger,monkeypatch):
    result_id='native:'+('a'*64)+':'+('c'*64)
    first=control.request_retry(ledger,result_id,'1'*32)
    assert control.request_retry(ledger,result_id,'1'*32)==first
    with pytest.raises(ValueError,match='待处理'):control.request_retry(ledger,result_id,'2'*32)
    def save(c,state,owner,events):
        for item in [*events,{'entity_type':native.ENTITY,'entity_key':state['run_id'],
                'action':'CHECKPOINT_00000004','after':state}]:
            _append_audit_connection(c,**item,reason='recovered',operator='test',before=None,evidence={})
        c.commit()
    monkeypatch.setattr(native,'_save',save)
    applied=control.consume_retries(ledger)
    assert applied['resumed_cursor']==2 and applied['business_started'] is False
    with ledger.connect() as c:
        state=native.read_runs(c)[0]
        assert state['run_id']=='a'*64 and state['completed_units']==142
        assert state['requested_at']=='2026-11-22 18:05:19'
        assert state['price_cutoff']=='2026-11-22 05:59:24'
        assert state['deadline_at']=='2026-11-24 17:45:00'
        assert len(native._partitions(c,state,state,'S'))==2
        receipt=control.read_requests(c)[0]
        assert receipt['status']=='COMPLETE'
        assert receipt['previous_deadline_at']=='2026-11-23 18:05:19'
        assert receipt['execution_deadline_at']==state['deadline_at']
    assert control.consume_retries(ledger)['status']=='NOT_STARTED'


def test_retry_cannot_resume_a_live_old_owner(ledger,monkeypatch):
    control.request_retry(ledger,'native:'+('a'*64)+':'+('c'*64),'1'*32)
    monkeypatch.setattr(control,'process_identity',lambda _pid:'old-birth')
    assert control.consume_retries(ledger)['status']=='BLOCKED'
    with ledger.connect() as c:
        state=native.read_runs(c)[0]
        assert state['status']=='FAILED' and state['deadline_at']=='2026-11-23 18:05:19'


def test_expired_retry_is_terminal_without_changing_the_failed_checkpoint(ledger,monkeypatch):
    request=control.request_retry(ledger,'native:'+('a'*64)+':'+('c'*64),'1'*32)
    expired_at=datetime.fromisoformat(request['requested_at'])+timedelta(hours=24)
    monkeypatch.setattr(control,'now_shanghai',lambda:expired_at)
    monkeypatch.setattr(native,'_save',lambda *_:pytest.fail('an expired request cannot change business state'))
    assert control.consume_retries(ledger)['status']=='FAILED'
    with ledger.connect() as c:
        state=native.read_runs(c)[0]
        assert state['status']=='FAILED' and state['cursor']==1 and state['completed_units']==141
        assert state['deadline_at']=='2026-11-23 18:05:19'
        assert len(native._partitions(c,state,state,'S'))==2
        assert control.read_requests(c)[0]['status']=='FAILED'
    assert control.consume_retries(ledger)['status']=='NOT_STARTED'


def test_changed_reader_recomputes_original_frozen_window_without_copying_signals(ledger,monkeypatch):
    with ledger.connect() as c:original=native.read_runs(c)[0]
    monkeypatch.setattr(native,'_contract',lambda:'different-code')
    monkeypatch.setattr(native,'_strategies',lambda _c:original['strategies'])
    monkeypatch.setattr(native,'now_shanghai',lambda:datetime(2026,11,23,19))
    request=control.request_retry(ledger,'native:'+('a'*64)+':'+('c'*64),'1'*32)
    assert request['source_contract']=='code'
    def save(c,state,owner,events):
        state={**state,'owner':owner,'checkpoint_sequence':1}
        for item in [*events,{'entity_type':native.ENTITY,'entity_key':state['run_id'],
                'action':'CHECKPOINT_00000001','after':state}]:
            _append_audit_connection(c,**item,reason='recomputed version',operator='test',before=None,evidence={})
        c.commit()
    monkeypatch.setattr(native,'_save',save)
    applied=control.consume_retries(ledger)
    assert applied['status']=='COMPLETE' and applied['recomputed'] and applied['business_started'] is False
    assert applied['run_id']!=original['run_id']
    with ledger.connect() as c:
        current=native.read_runs(c,run_id=applied['run_id'])[0]
        for field in ('sessions','published','source_hash','requested_at','price_cutoff'):
            assert current[field]==original[field]
        assert current['contract']=='different-code' and current['phase']=='INPUTS' and current['cursor']==0
        assert current['deadline_at']=='2026-11-24 19:00:00'
        assert native._partitions(c,current,current,'S')=={}
        previous=native.read_runs(c,run_id=original['run_id'])[0]
        assert previous['status']=='BLOCKED' and previous['superseded_by_run_id']==current['run_id']
        assert len(native._partitions(c,previous,previous,'S'))==2
        receipt=control.read_requests(c)[0]
        assert receipt['target_run_id']==current['run_id'] and receipt['source_contract']=='code'


def test_pending_version_change_automatically_preserves_frozen_inputs_and_old_signals(ledger,monkeypatch):
    with ledger.connect() as c:
        state=native.read_runs(c)[0]
        state['status']='NOT_STARTED'
        _append_audit_connection(c,entity_type=native.ENTITY,entity_key=state['run_id'],action='CHECKPOINT_00000004',
            operator='test',reason='pending',before=None,after=state,evidence={})
        c.commit()
    with ledger.connect() as c:original=native.read_runs(c)[0]
    monkeypatch.setattr(native,'_contract',lambda:'new-code')
    monkeypatch.setattr(native,'_owner',lambda _c:{'scheduler_run_uid':'new'})
    monkeypatch.setattr(native,'_resume_allowed',lambda *_:True)
    monkeypatch.setattr(native,'now_shanghai',lambda:datetime(2026,11,23,19))
    monkeypatch.setattr(native,'_inventory',lambda *_:pytest.fail('pending work must retain its own frozen inventory'))
    monkeypatch.setattr(native.signal,'SIGALRM',14,raising=False)
    monkeypatch.setattr(native.signal,'ITIMER_REAL',0,raising=False)
    monkeypatch.setattr(native.signal,'signal',lambda *_:None)
    monkeypatch.setattr(native.signal,'setitimer',lambda *_:None,raising=False)
    def save(c,state,owner,events=()):
        state['checkpoint_sequence']=state.get('checkpoint_sequence',0)+1
        state['owner']=owner
        for item in [*events,{'entity_type':native.ENTITY,'entity_key':state['run_id'],
                'action':f"CHECKPOINT_{state['checkpoint_sequence']:08d}",'after':state}]:
            _append_audit_connection(c,**item,reason='version update',operator='test',before=None,evidence={})
        c.commit()
    monkeypatch.setattr(native,'_save',save)
    processed=[]
    def step(c,state,owner):
        assert state['run_id']!=original['run_id'] and state['contract']=='new-code'
        assert all(state[k]==original[k] for k in ('sessions','published','source_hash','requested_at','price_cutoff'))
        assert native._partitions(c,state,state,'S')=={}
        processed.append(state['run_id']);save(c,state,owner)
    monkeypatch.setattr(native,'_step',step)
    result=native.native_tick(ledger)
    assert result['run_id']==processed[0]
    with ledger.connect() as c:
        previous=native.read_runs(c,run_id=original['run_id'])[0]
        assert previous['status']=='BLOCKED' and previous['superseded_by_run_id']==processed[0]
        assert previous['contract']=='code' and previous['cursor']==1 and previous['completed_units']==141
        assert len(native._partitions(c,previous,previous,'S'))==2
    native.native_tick(ledger)
    assert processed==[processed[0],processed[0]]
