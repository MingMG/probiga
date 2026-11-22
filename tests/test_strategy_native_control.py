import os
import socket

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
        c.execute(text("INSERT INTO st_scheduled_task_history VALUES('old','failed')"))
        state={'schema':native.SCHEMA,'run_id':'a'*64,'rule_hash':'b'*64,'contract':'code',
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
        assert len(native._partitions(c,state,state,'S'))==2
        assert control.read_requests(c)[0]['status']=='COMPLETE'
    assert control.consume_retries(ledger)['status']=='NOT_STARTED'


def test_retry_cannot_resume_a_live_old_owner(ledger,monkeypatch):
    control.request_retry(ledger,'native:'+('a'*64)+':'+('c'*64),'1'*32)
    monkeypatch.setattr(control,'process_identity',lambda _pid:'old-birth')
    assert control.consume_retries(ledger)['status']=='BLOCKED'
    with ledger.connect() as c:assert native.read_runs(c)[0]['status']=='FAILED'


def test_retry_refuses_changed_computation_contract(ledger,monkeypatch):
    monkeypatch.setattr(native,'_contract',lambda:'different-code')
    with pytest.raises(ValueError,match='变化'):
        control.request_retry(ledger,'native:'+('a'*64)+':'+('c'*64),'1'*32)
