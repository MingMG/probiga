"""Leased retries and versioned recomputation of the same frozen research inputs."""
import json
import re
from datetime import datetime,timedelta

from sqlalchemy import text

from server.engine import strategy_native_backtest as native
from server.engine.autonomous_strategy_research import now_shanghai
from server.engine.strategy_governance import _append_audit_connection,_digest
from server.common.market_dashboard_worker import process_identity

ENTITY='NATIVE_REPLAY_RETRY'
RETRY_EXECUTION_BUDGET=timedelta(hours=24)


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
                    family=state['strategies'][0]['source_kind']
                    strategies=[item for item in native._strategies(connection) if item['source_kind']==family]
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
