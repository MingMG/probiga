from copy import deepcopy
from datetime import datetime, timedelta
import json
import socket
import time

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.pool import StaticPool

from server.common.scheduler_task_lease import encode
from server.engine import native_research_jobs as jobs
from server.engine import strategy_native_backtest as native
from server.engine import strategy_native_control as control


@pytest.fixture
def queue(monkeypatch):
    engine=create_engine('sqlite://',connect_args={'check_same_thread':False},poolclass=StaticPool)
    @event.listens_for(engine,'connect')
    def functions(dbapi,_record):
        dbapi.create_function('GET_LOCK',2,lambda *_:1)
        dbapi.create_function('RELEASE_LOCK',1,lambda *_:1)
        dbapi.create_function('IS_USED_LOCK',1,lambda *_:1)
        dbapi.create_function('CONNECTION_ID',0,lambda:1)
    now=jobs.now_shanghai()
    identity={'run_uid':'1'*32,'task_id':70,'task_name':'Worker','task_type':'trading_v2_job_worker',
        'run_at':str(now),'host_name':socket.gethostname(),'scheduler_instance_id':socket.gethostname()+'-123',
        'build_sha':'a'*40,'trigger_source':'scheduled'}
    with engine.begin() as connection:
        connection.execute(text('''CREATE TABLE st_strategy_governance_audit (
            audit_id TEXT PRIMARY KEY,entity_type TEXT,entity_key TEXT,action TEXT,reason TEXT,
            operator_name TEXT,before_json TEXT,after_json TEXT,evidence_json TEXT,payload_json TEXT,
            audit_hash TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP)'''))
        connection.execute(text('''CREATE TABLE st_scheduled_task_history (
            run_uid TEXT PRIMARY KEY,task_id INTEGER,task_name TEXT,task_type TEXT,run_at TEXT,status TEXT,
            host_name TEXT,scheduler_instance_id TEXT,build_sha TEXT,trigger_source TEXT,output TEXT,finished_at TEXT)'''))
        connection.execute(text('''INSERT INTO st_scheduled_task_history VALUES (
            :run_uid,:task_id,:task_name,:task_type,:run_at,'running',:host_name,:scheduler_instance_id,
            :build_sha,:trigger_source,:output,NULL)'''),{**identity,'output':encode(identity,now)})
        connection.execute(text('''CREATE TABLE st_job_v2 (job_id TEXT PRIMARY KEY,job_type TEXT,
            idempotency_key TEXT UNIQUE,request_json TEXT,status TEXT,result_ref TEXT,error_code TEXT,
            error_message TEXT,requested_by TEXT,requested_at DATETIME,started_at DATETIME,
            finished_at DATETIME,next_run_at DATETIME,scheduler_run_uid VARCHAR(64))'''))
        connection.execute(text('CREATE INDEX idx_job_native_ready ON st_job_v2(job_type,status,next_run_at,requested_at,job_id)'))
    for key,value in {'PROBIGA_SCHEDULER_HISTORY_RUN_UID':identity['run_uid'],
        'PROBIGA_SCHEDULER_TASK_ID':'70','PROBIGA_SCHEDULER_TASK_TYPE':'trading_v2_job_worker'}.items():
        monkeypatch.setenv(key,value)
    monkeypatch.setattr(jobs,'process_identity',lambda _pid:'test-process-birth')
    yield engine
    engine.dispose()


def experiment():
    from server.engine.native_research_identity import producer_identity
    return {'schema':native.SCHEMA,'run_id':'b'*64,'request_identity':'b'*64,'rule_hash':'c'*64,
        'architecture':'PREPARED_TYPED_NATIVE_JOB_V2','phase':'PLAN','status':'NOT_STARTED',
        'completed_units':0,'total_units':1,'proposals':[{'rule_hash':'d'*64,'strategy_key':'test','validation':{}}],
        'strategies':[{'source_kind':'immutable_v3_sleeve'}],
        'reports':{},'producer_identity':producer_identity(),'config':{},'deadline_at':str(jobs.now_shanghai()+timedelta(hours=24)),
        'requested_at':'2026-11-22 18:05:19','price_cutoff':'2026-11-22 05:59:24','contract':'math'}


def start(queue):
    with queue.begin() as connection:
        first=jobs.enqueue(connection,experiment())
        assert jobs.enqueue(connection,experiment())==first
    with queue.connect() as connection:
        return jobs.claim(connection)


def test_claim_yield_and_reclaim_preserve_frozen_request(queue,monkeypatch):
    state,owner=start(queue)
    assert state['checkpoint_sequence']==2 and state['status']=='RUNNING'
    with queue.connect() as connection:
        state['completed_units']=1
        jobs.checkpoint(connection,state,owner,yield_job=True)
        row=connection.execute(text('SELECT * FROM st_job_v2')).mappings().one()
        assert row['status']=='PENDING' and row['scheduler_run_uid'] is None
        saved=native.read_runs(connection,run_id=state['run_id'])[0]
        assert saved==state and saved['owner'] is None and saved['status']=='RUNNING'
        assert jobs.claim(connection) is None
    later=jobs.now_shanghai()+timedelta(seconds=2)
    monkeypatch.setattr(jobs,'now_shanghai',lambda:later)
    with queue.connect() as connection:
        resumed,second_owner=jobs.claim(connection)
        assert resumed['completed_units']==1 and resumed['attempt']==2
        assert resumed['requested_at']=='2026-11-22 18:05:19'
        assert resumed['price_cutoff']=='2026-11-22 05:59:24'
        assert second_owner==owner


def test_yielded_long_job_does_not_starve_other_ready_experiments(queue,monkeypatch):
    clock=jobs.now_shanghai()
    monkeypatch.setattr(jobs,'now_shanghai',lambda:clock)
    first=experiment()
    second=deepcopy(first)
    second.update(run_id='c'*64,request_identity='c'*64)
    with queue.begin() as connection:
        jobs.enqueue(connection,first)
    clock+=timedelta(seconds=1)
    with queue.begin() as connection:
        jobs.enqueue(connection,second)
    with queue.connect() as connection:
        state,owner=jobs.claim(connection)
        assert state['run_id']==first['run_id']
        state['completed_units']=1
        jobs.checkpoint(connection,state,owner,yield_job=True)
    clock+=timedelta(seconds=2)
    with queue.connect() as connection:
        next_state,next_owner=jobs.claim(connection)
        assert next_state['run_id']==second['run_id']
        assert next_state['requested_at']==second['requested_at']
        assert next_state['price_cutoff']==second['price_cutoff']
        jobs.checkpoint(connection,next_state,next_owner,yield_job=True)
    clock+=timedelta(seconds=2)
    with queue.connect() as connection:
        resumed,_=jobs.claim(connection)
        assert resumed['run_id']==first['run_id'] and resumed['completed_units']==1


@pytest.mark.parametrize('lost',['queue','sequence','lease'])
def test_late_owner_cannot_publish_partition_or_head(queue,lost):
    state,owner=start(queue)
    with queue.begin() as connection:
        if lost=='queue':connection.execute(text("UPDATE st_job_v2 SET scheduler_run_uid='different'"))
        elif lost=='lease':connection.execute(text("UPDATE st_scheduled_task_history SET status='failed'"))
        else:
            advanced=deepcopy(state);advanced['checkpoint_sequence']+=1
            native._append_state(connection,advanced)
    with queue.connect() as connection:
        count=connection.execute(text('SELECT COUNT(*) FROM st_strategy_governance_audit')).scalar()
        with pytest.raises((RuntimeError,ValueError)):
            jobs.checkpoint(connection,state,owner,[native._part(state,'D','2026-09-07',{'sessions':['2026-09-07']})])
        assert connection.execute(text('SELECT COUNT(*) FROM st_strategy_governance_audit')).scalar()==count
        assert connection.execute(text("SELECT COUNT(*) FROM st_strategy_governance_audit WHERE entity_type='AUTO_REPLAY_PART'")).scalar()==0


def test_queue_cas_failure_rolls_back_the_entire_publication(queue):
    state,owner=start(queue)
    def lose_cas(_connection,_cursor,statement,parameters,_context,_many):
        if statement.startswith('UPDATE st_job_v2 SET status='):
            values=list(parameters);values[-1]='not-the-scheduler-owner'
            return statement,tuple(values)
        return statement,parameters
    event.listen(queue,'before_cursor_execute',lose_cas,retval=True)
    try:
        with queue.connect() as connection:
            with pytest.raises(RuntimeError,match='CAS lost'):
                jobs.checkpoint(connection,state,owner,[native._part(state,'D','2026-09-07',{'sessions':['2026-09-07']})])
    finally:event.remove(queue,'before_cursor_execute',lose_cas)
    with queue.connect() as connection:
        assert native.read_runs(connection)[0]['checkpoint_sequence']==2
        assert connection.execute(text('SELECT status FROM st_job_v2')).scalar()=='RUNNING'
        assert connection.execute(text("SELECT COUNT(*) FROM st_strategy_governance_audit WHERE entity_type='AUTO_REPLAY_PART'")).scalar()==0


def test_terminal_result_and_job_commit_together_and_retry_is_idempotent(queue,monkeypatch):
    state,owner=start(queue)
    state.update(status='FAILED',reason='bounded source failure')
    with queue.connect() as connection:
        jobs.checkpoint(connection,state,owner)
        assert connection.execute(text('SELECT status FROM st_job_v2')).scalar()=='FAILED'
        assert native.read_runs(connection)[0]['owner'] is None
    monkeypatch.setattr(native,'_contract',lambda:'math')
    request=control.request_retry(queue,'native:'+state['run_id']+':'+('d'*64),'2'*32)
    assert request['business_started'] is False and request['target_run_id']==state['run_id']
    assert control.request_retry(queue,'native:'+state['run_id']+':'+('d'*64),'2'*32)==request
    with queue.connect() as connection:
        saved=native.read_runs(connection)[0]
        assert saved['status']=='RUNNING' and saved['requested_at']==state['requested_at']
        assert connection.execute(text('SELECT COUNT(*) FROM st_job_v2')).scalar()==1


def test_running_job_requires_expired_lease_and_proven_process_absence(queue,monkeypatch):
    state,owner=start(queue)
    monkeypatch.setattr(jobs,'_absent',lambda _previous:True)
    with queue.connect() as connection:
        assert jobs.claim(connection) is None
    later=jobs.now_shanghai()+timedelta(minutes=16)
    monkeypatch.setattr(jobs,'now_shanghai',lambda:later)
    with queue.begin() as connection:
        previous=dict(connection.execute(text('SELECT * FROM st_scheduled_task_history')).mappings().one())
        identity={key:previous[key] for key in ('run_uid','task_id','task_name','task_type','run_at','host_name','scheduler_instance_id','build_sha','trigger_source')}
        identity.update(run_uid='3'*32,run_at=str(later))
        connection.execute(text('''INSERT INTO st_scheduled_task_history VALUES(
            :run_uid,:task_id,:task_name,:task_type,:run_at,'running',:host_name,:scheduler_instance_id,
            :build_sha,:trigger_source,:output,NULL)'''),{**identity,'output':encode(identity,later)})
    monkeypatch.setenv('PROBIGA_SCHEDULER_HISTORY_RUN_UID','3'*32)
    with queue.connect() as connection:
        resumed,new_owner=jobs.claim(connection)
        assert resumed['attempt']==2 and new_owner['scheduler_run_uid']=='3'*32
        assert resumed['run_id']==state['run_id']


def test_unexpired_owner_does_not_block_another_pending_experiment(queue):
    state,owner=start(queue)
    second=experiment()
    second.update(run_id='e'*64,request_identity='e'*64)
    with queue.begin() as connection:jobs.enqueue(connection,second)
    with queue.connect() as connection:
        claimed=jobs.claim(connection)
        assert claimed[0]['run_id']==second['run_id']
        assert connection.execute(text('SELECT scheduler_run_uid FROM st_job_v2 WHERE result_ref=:id'),
            {'id':state['run_id']}).scalar()==owner['scheduler_run_uid']


def test_claim_database_stall_rolls_back_before_job_or_head_publication(queue,monkeypatch):
    with queue.begin() as connection:
        first=jobs.enqueue(connection,experiment())
        connection.connection.driver_connection.create_function('claim_stall',0,lambda:time.sleep(.05) or 0)
        connection.execute(text('''CREATE TRIGGER delay_claim BEFORE UPDATE ON st_job_v2
            WHEN NEW.status='RUNNING' BEGIN SELECT claim_stall(); END'''))
    monkeypatch.setattr(jobs,'CLAIM_SECONDS',.01)
    result=native.native_tick(queue)
    assert result['status']=='FAILED' and 'NATIVE_CLAIM_DATABASE_DEADLINE' in result['reason']
    with queue.connect() as connection:
        job=connection.execute(text('SELECT status,scheduler_run_uid FROM st_job_v2 WHERE job_id=:id'),
            {'id':first['job_id']}).mappings().one()
        assert job['status']=='PENDING' and job['scheduler_run_uid'] is None
        state=native.read_runs(connection,run_id=experiment()['run_id'])[0]
        assert state['checkpoint_sequence']==1 and state['status']=='NOT_STARTED'


def test_feature_budget_uses_original_attempt_start_and_remains_bounded(monkeypatch):
    timers=[]
    monkeypatch.setattr(native.signal,'setitimer',lambda _kind,seconds:timers.append(seconds),raising=False)
    monkeypatch.setattr(native.signal,'ITIMER_REAL',0,raising=False)
    monkeypatch.setattr(native.time,'monotonic',lambda:400)
    assert native._feature_compute_deadline(100,arm_timer=True)==1000
    monkeypatch.setattr(native.time,'monotonic',lambda:700)
    assert native._feature_compute_deadline(100,arm_timer=True)==1000
    assert timers==[600,300]
    monkeypatch.setattr(native.time,'monotonic',lambda:1000)
    with pytest.raises(native.NativeStepDeadline,match='FEATURE_ATTEMPT_DEADLINE'):
        native._feature_compute_deadline(100,arm_timer=False)


def test_feature_decode_progress_only_counts_successful_day_reads_per_attempt(monkeypatch):
    first,second,unrelated={},{},{}
    state={'run_id':'b'*64,'attempt':29,'completed_units':101,'feature_windows':{'day':['one','two']},
           'bars':{'one':{'tree':first},'two':{'tree':second}}}
    saved=[]
    monkeypatch.setattr(native,'_save',lambda _c,s,_o:saved.append(deepcopy(s)))
    class Inputs:
        def get_tree(self,tree):
            if tree is second:raise ValueError('invalid content hash')
            return ['exact decoded value']
    monkeypatch.setattr(jobs.time,'monotonic',lambda:0)
    observed=jobs.feature_input_progress(None,state,None,Inputs(),'day',deadline=900)
    assert observed.get_tree(unrelated)==['exact decoded value']
    assert 'source_progress' not in state
    monkeypatch.setattr(jobs.time,'monotonic',lambda:31)
    assert observed.get_tree(first)==['exact decoded value']
    assert saved[-1]['source_progress']['completed']==1
    assert saved[-1]['source_progress']['total']==2
    assert saved[-1]['completed_units']==101
    with pytest.raises(ValueError,match='invalid content hash'):observed.get_tree(second)
    assert state['source_progress']['completed']==1
    observed.get_tree(first)
    assert len(saved)==1
    previous_scope=state['source_progress']['scope']
    state['attempt']=30
    next_attempt=jobs.feature_input_progress(None,state,None,Inputs(),'day',deadline=900)
    next_attempt.get_tree(first)
    assert state['source_progress']['completed']==1
    assert state['source_progress']['scope']!=previous_scope

    # A checkpoint crossing the deadline must fail, rather than auto-yield
    # and restart a RAM-only decode prefix indefinitely on new attempts.
    clock=[31]
    monkeypatch.setattr(jobs.time,'monotonic',lambda:clock[0])
    monkeypatch.setattr(native,'_save',lambda *_:clock.__setitem__(0,1000))
    state['attempt']=31
    expired=jobs.feature_input_progress(None,state,None,Inputs(),'day',deadline=900)
    clock[0]=62
    with pytest.raises(native.NativeStepDeadline,match='FEATURE_ATTEMPT_DEADLINE'):
        expired.get_tree(first)
    assert state['completed_units']==101
