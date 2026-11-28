"""Leased retries and versioned recomputation of the same frozen research inputs."""
import json
import re
import time
from contextlib import contextmanager
from copy import deepcopy
from datetime import date,datetime,timedelta

from sqlalchemy import text

from server.engine import strategy_native_backtest as native
from server.engine.autonomous_strategy_research import now_shanghai
from server.engine.strategy_governance import _append_audit_connection,_digest
from server.common.market_dashboard_worker import process_identity

ENTITY='NATIVE_REPLAY_RETRY'
RETRY_EXECUTION_BUDGET=timedelta(hours=24)


def _experiment_strategies(connection, state):
    from server.engine.native_opening_research import is_opening, opening_strategy
    strategies = native._strategies(connection)
    if is_opening(state['strategies']):
        return [opening_strategy(strategies, state['strategies'][0]['strategy_key'])]
    family = state['strategies'][0]['source_kind']
    return [item for item in strategies if item['source_kind']==family]


def read_requests(c,request_uid=None):
    rows=c.execute(text("""SELECT /*+ MAX_EXECUTION_TIME(4000) */ a.payload_json,a.after_json,a.audit_hash
        FROM st_strategy_governance_audit a JOIN (SELECT entity_key,MAX(action) last_action
        FROM st_strategy_governance_audit WHERE entity_type=:entity GROUP BY entity_key) x
        ON x.entity_key=a.entity_key AND x.last_action=a.action
        WHERE a.entity_type=:entity AND (:uid IS NULL OR a.entity_key=:uid)
        ORDER BY a.created_at,a.audit_id LIMIT 16"""),{'entity':ENTITY,'uid':request_uid}).mappings()
    result=[]
    for row in rows:
        payload,value=json.loads(row['payload_json']),json.loads(row['after_json'])
        if _digest(payload)!=row['audit_hash'] or payload['after']!=value:
            raise ValueError('native retry request integrity differs')
        result.append(value)
    return result


def request_retry(engine,result_id,request_uid):
    """Resume the same typed experiment, or explicitly register changed math."""
    from server.engine.native_research_jobs import enqueue
    if not re.fullmatch(r'[0-9a-f]{32}',request_uid):raise ValueError('invalid retry identity')
    prefix,run_id,version_hash=result_id.split(':')
    if prefix!='native':raise ValueError('only native failed runs support this retry')
    with engine.connect() as connection:
        mysql=connection.dialect.name=='mysql'
        if mysql and connection.execute(text('SELECT GET_LOCK(:name,0)'),{'name':native.LOCK}).scalar()!=1:
            raise ValueError('原回测执行器尚未释放，暂不能登记恢复')
        connection.commit()
        try:
            with connection.begin():
                prior=read_requests(connection,request_uid)
                if prior:
                    if prior[0].get('kind')=='EXPERIMENT':raise ValueError('request identity belongs to an experiment registration')
                    if prior[0]['run_id']!=run_id:raise ValueError('retry identity is already bound to another experiment')
                    return prior[0]
                states=native.read_runs(connection,run_id=run_id)
                if not states or not any(p['rule_hash']==version_hash for p in states[0]['proposals']):raise KeyError(result_id)
                state=states[0]
                if state['status']!='FAILED':raise ValueError('仅失败实验可以恢复，原输入和时间保持不变')
                previous_deadline=state['deadline_at']
                from server.engine.native_research_identity import producer_identity
                same_producer=state.get('producer_identity')==producer_identity(state['strategies'][0]['source_kind'])
                if state.get('architecture')=='PREPARED_TYPED_NATIVE_JOB_V2' and state['contract']==native._contract() and same_producer:
                    suffix=' FOR UPDATE' if mysql else ''
                    job=connection.execute(text('SELECT * FROM st_job_v2 WHERE job_id=:id'+suffix),
                        {'id':state['job_id']}).mappings().one()
                    if job['status']!='FAILED' or job['scheduler_run_uid'] is not None or state.get('owner') is not None:
                        raise ValueError('原生任务未终结或仍有所有者，不能恢复')
                    changed=connection.execute(text('''UPDATE st_job_v2 SET status='PENDING',next_run_at=:now,
                        finished_at=NULL,error_code=NULL,error_message=NULL WHERE job_id=:id AND status='FAILED'
                        AND scheduler_run_uid IS NULL'''),{'id':state['job_id'],'now':now_shanghai()})
                    if changed.rowcount!=1:raise ValueError('原生恢复请求CAS失败')
                    state.get('source_error_retries',{}).pop(state.get('current_unit',state['phase']),None)
                    state.update(status='RUNNING',reason='按原断点恢复；原冻结时间未改变',
                        deadline_at=str(now_shanghai()+RETRY_EXECUTION_BUDGET),checkpoint_sequence=state['checkpoint_sequence']+1)
                    for proposal in state['proposals']:
                        if proposal['strategy_key'] not in state['reports']:proposal['validation'].update(status='RUNNING',reason=state['reason'])
                    native._append_state(connection,state)
                    queued={'job_id':state['job_id'],'result_ref':state['run_id'],'status':'PENDING'}
                else:
                    from server.engine.native_research_jobs import _absent
                    previous=state.get('owner') or state.get('last_owner')
                    if previous:
                        history=connection.execute(text('SELECT status FROM st_scheduled_task_history WHERE run_uid=:uid'),
                            {'uid':previous['scheduler_run_uid']}).scalar()
                        if history in (None,'running') or not _absent(previous):
                            raise ValueError('旧回测进程尚未证明退出，保持原断点')
                    strategies=_experiment_strategies(connection,state)
                    if [(s['strategy_key'],s['version_hash']) for s in strategies]!=[(s['strategy_key'],s['version_hash']) for s in state['strategies']]:
                        raise ValueError('策略登记版本已变化，不能替换原研究规则')
                    candidate=native._start(connection,strategies,state['sessions'],state['published'],state['source_hash'],frozen=state)
                    if any(candidate[key]!=state[key] for key in ('sessions','published','source_hash','requested_at','price_cutoff')):
                        raise ValueError('恢复改变了原冻结输入或可用时间')
                    queued=enqueue(connection,candidate,requested_by='native-research-recovery')
                value={'request_uid':request_uid,'run_id':run_id,'version_hash':version_hash,'status':'COMPLETE',
                    'target_run_id':queued['result_ref'],'job_id':queued['job_id'],'business_started':False,
                    'previous_deadline_at':previous_deadline,'requested_at':str(now_shanghai()),
                    'reason':'已登记到现有任务队列，等待调度从原始输入断点继续'}
                _append_audit_connection(connection,entity_type=ENTITY,entity_key=request_uid,action='RETRY_00000001',
                    reason=value['reason'],operator='native-research-control',before=None,after=value,
                    evidence={'run_id':run_id,'job_id':queued['job_id'],'real_order_authority':False})
                return value
        finally:
            connection.rollback()
            if mysql:connection.execute(text('SELECT RELEASE_LOCK(:name)'),{'name':native.LOCK});connection.commit()


@contextmanager
def _experiment_transaction(engine):
    """Serialize registrations while leaving the execution owner independent."""
    with engine.connect() as connection:
        mysql=connection.dialect.name=='mysql'
        acquired=False
        try:
            if mysql:
                acquired=connection.execute(text("SELECT GET_LOCK('probiga:native-experiment-registration',3)")).scalar()==1
                if not acquired:raise ValueError('另一个实验正在登记，请稍后重试')
            connection.commit()
            with connection.begin():yield connection
        finally:
            connection.rollback()
            if acquired and not connection.invalidated:
                connection.execute(text("SELECT RELEASE_LOCK('probiga:native-experiment-registration')"))
                connection.commit()


def request_experiment(engine,result_id,request_uid,*,evaluation_start,evaluation_end,
                       execution_end_date,allocation_policy='FIXED_SHADOW'):
    """Register a bounded experiment over an existing immutable source freeze."""
    from server.common.analysis_pool_receipt import canonical_sha256
    from server.engine.native_research_jobs import enqueue
    if not re.fullmatch(r'[0-9a-f]{32}',request_uid):raise ValueError('invalid experiment request identity')
    if not re.fullmatch(r'native:[0-9a-f]{64}:[0-9a-f]{64}',result_id):raise ValueError('invalid native source identity')
    _,run_id,version_hash=result_id.split(':')
    for value in (evaluation_start,evaluation_end,execution_end_date):
        if date.fromisoformat(value).isoformat()!=value:raise ValueError('experiment dates must be exact ISO dates')
    if allocation_policy not in ('FIXED_SHADOW','ADAPTIVE_REGIME'):raise ValueError('invalid allocation policy')
    intent=canonical_sha256({'source_result_id':result_id,'evaluation_start':evaluation_start,
        'evaluation_end':evaluation_end,'execution_end_date':execution_end_date,'allocation_policy':allocation_policy})
    deadline=time.monotonic()+15
    def check_deadline(stage):
        if time.monotonic()>deadline:raise TimeoutError('EXPERIMENT_REGISTRATION_DEADLINE:'+stage)
    with _experiment_transaction(engine) as connection:
        prior=read_requests(connection,request_uid)
        if prior:
            if prior[0].get('intent_sha256')!=intent:raise ValueError('request identity is bound to a different experiment')
            return prior[0]
        runs=native.read_runs(connection,run_id=run_id)
        check_deadline('SOURCE')
        if not runs or not any(p['rule_hash']==version_hash for p in runs[0]['proposals']):raise KeyError(result_id)
        source=runs[0]
        if source['status'] not in ('COMPLETE','FAILED','BLOCKED'):
            raise ValueError('来源实验仍在运行，请等待其终结后创建对照实验')
        if not source['sessions'][0]<=evaluation_start<=evaluation_end<=source['sessions'][-1]:
            raise ValueError('评估区间必须位于原冻结实验内')
        sessions=[day for day in source['sessions'] if evaluation_start<=day<=evaluation_end]
        if not sessions:raise ValueError('评估区间没有原实验交易日')
        if not sessions[-1]<=execution_end_date<=source.get('execution_end_date',source['sessions'][-1]):
            raise ValueError('成交窗口必须覆盖评估区间，且不能超出原冻结成交窗口')
        family=source['strategies'][0]['source_kind']
        if allocation_policy=='ADAPTIVE_REGIME' and family!='immutable_v3_sleeve':
            raise ValueError('该策略族不提供市场状态调节入场实验')
        strategies=_experiment_strategies(connection,source)
        check_deadline('STRATEGY')
        if [(s['strategy_key'],s['version_hash']) for s in strategies]!=[(s['strategy_key'],s['version_hash']) for s in source['strategies']]:
            raise ValueError('策略登记版本已变化，不能替换原实验规则')
        frozen=deepcopy(source);frozen['sessions']=sessions
        state=native._start(connection,strategies,sessions,source['published'],source['source_hash'],
            frozen=frozen,allocation_policy=allocation_policy,execution_end_date=execution_end_date)
        check_deadline('PLAN')
        queued=enqueue(connection,state,requested_by='native-research-experiment')
        if queued['status']=='FAILED':raise ValueError('相同冻结输入的实验已失败，请使用从断点重试')
        value={'kind':'EXPERIMENT','request_uid':request_uid,'intent_sha256':intent,'run_id':run_id,
            'target_run_id':queued['result_ref'],'job_id':queued['job_id'],'status':'COMPLETE','business_started':False,
            'queue_status':queued['status'],
            'evaluation_sessions':sessions,'execution_end_date':execution_end_date,'allocation_policy':allocation_policy,
            'data_decision_known_at':state['requested_at'],'price_cutoff':state['price_cutoff'],
            'requested_at':str(now_shanghai()),'reason':('冻结输入实验已登记；等待现有调度器执行'
                if queued['status']=='PENDING' else '已关联同一实验，查看其现有执行进度' if queued['status']=='RUNNING' else '同一冻结输入已有结果，已复用保存的实验')}
        _append_audit_connection(connection,entity_type=ENTITY,entity_key=request_uid,action='REQUEST_00000001',
            operator='native-research-control',reason=value['reason'],before=None,after=value,
            evidence={'source_run_id':run_id,'job_id':queued['job_id'],'real_order_authority':False})
        check_deadline('QUEUE_AND_RECEIPT')
        return value
