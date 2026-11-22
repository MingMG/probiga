"""Explicit, idempotent retry requests consumed by the existing leased worker."""
import json
import re
import socket
import os

from sqlalchemy import text

from server.engine import strategy_native_backtest as native
from server.engine.autonomous_strategy_research import _owner,now_shanghai
from server.engine.strategy_governance import _append_audit_connection,_digest
from server.common.market_dashboard_worker import process_identity

ENTITY='NATIVE_REPLAY_RETRY'
REQUEST_LOCK='probiga:native-replay-requests'


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
    if not re.fullmatch(r'[0-9a-f]{32}',request_uid):raise ValueError('invalid retry identity')
    prefix,run_id,version_hash=result_id.split(':')
    if prefix!='native':raise ValueError('only native failed runs support this retry')
    with engine.connect() as c:
        mysql=c.dialect.name=='mysql'
        if mysql and c.execute(text('SELECT GET_LOCK(:name,0)'),{'name':REQUEST_LOCK}).scalar()!=1:
            raise ValueError('恢复请求正在登记，请稍后重试')
        try:
            prior=read_requests(c,request_uid)
            if prior:
                if prior[0]['run_id']!=run_id:raise ValueError('retry identity is already bound to another run')
                return prior[0]
            states=native.read_runs(c,run_id=run_id)
            if not states or not any(p['rule_hash']==version_hash for p in states[0]['proposals']):raise KeyError(result_id)
            state=states[0]
            if state['status']!='FAILED':raise ValueError('仅失败的回测可请求恢复；正在运行或缺少数据的批次继续沿用原状态')
            if state['contract']!=native._contract():raise ValueError('规则计算代码已变化，后台将按新数据身份建立回测')
            if any(r['run_id']==run_id and r['status']=='NOT_STARTED' for r in read_requests(c)):
                raise ValueError('该批次已有待处理的恢复请求')
            value={'request_uid':request_uid,'run_id':run_id,'version_hash':version_hash,'status':'NOT_STARTED',
                'expected_sequence':state['checkpoint_sequence'],'source_contract':state['contract'],
                'requested_at':str(now_shanghai()),'reason':'操作员请求从原断点恢复'}
            _append_audit_connection(c,entity_type=ENTITY,entity_key=request_uid,action='RETRY_00000001',
                reason=value['reason'],operator='native-research-control',before=None,after=value,evidence={'run_id':run_id})
            c.commit()
            return value
        finally:
            c.rollback()
            if mysql:c.execute(text('SELECT RELEASE_LOCK(:name)'),{'name':REQUEST_LOCK});c.commit()


def consume_retries(engine):
    with engine.connect() as c:
        if c.execute(text('SELECT GET_LOCK(:name,0)'),{'name':native.LOCK}).scalar()!=1:
            return {'status':'RUNNING','reason':'原生回测正在写入，恢复请求留待下次处理'}
        try:
            owner=_owner(c)
            for request in read_requests(c):
                if request['status']!='NOT_STARTED':continue
                state=native.read_runs(c,run_id=request['run_id'])[0]
                if (state['status']!='FAILED' or state['checkpoint_sequence']!=request['expected_sequence']
                        or state['contract']!=request['source_contract'] or state['contract']!=native._contract()):
                    raise ValueError('恢复请求与原失败检查点不一致，禁止覆盖新进度')
                old=state['owner']
                history=c.execute(text('SELECT status FROM st_scheduled_task_history WHERE run_uid=:uid'),
                    {'uid':old['scheduler_run_uid']}).mappings().one()
                absent=False
                if old['host']==socket.gethostname() and int(old['pid'])>0:
                    birth=process_identity(int(old['pid']))
                    absent=birth is not None and old['pid_birth'] is not None and birth!=old['pid_birth']
                    if not absent:
                        try:os.kill(int(old['pid']),0)
                        except ProcessLookupError:absent=True
                        except PermissionError:pass
                if history['status']=='running' or not absent:
                    return {'status':'BLOCKED','reason':'旧回测进程尚未证明退出，保持原检查点'}
                kind='S' if state['phase']=='SIGNALS' else 'P' if state['phase']=='PRICES' else None
                if kind:
                    days=state['sessions'] if kind=='S' else state['input_sessions']
                    parts=native._partitions(c,state,state,kind)
                    prefix=0
                    while prefix<len(days) and days[prefix] in parts:prefix+=1
                    if set(parts)!=set(days[:prefix]):raise ValueError('已保存分区不连续，禁止猜测恢复位置')
                    state['completed_units']+=prefix-state['cursor']
                    state['cursor']=prefix
                state.update(status='NOT_STARTED',step_status='NOT_STARTED',retry_count=0,retry_after='',
                    recovery_request_uid=request['request_uid'],reason=request['reason'])
                for proposal in state['proposals']:
                    proposal['validation'].update(status='NOT_STARTED',reason=request['reason'])
                done={**request,'status':'COMPLETE','applied_at':str(now_shanghai()),'scheduler_run_uid':owner['scheduler_run_uid']}
                event={'entity_type':ENTITY,'entity_key':request['request_uid'],'action':'RETRY_00000002','after':done}
                native._save(c,state,owner,[event])
                return {'status':'COMPLETE','request_uid':request['request_uid'],'run_id':state['run_id'],
                    'resumed_cursor':state['cursor'],'business_started':False}
            return {'status':'NOT_STARTED','reason':'没有待处理的原生回测恢复请求'}
        finally:
            c.rollback()
            if not c.invalidated:c.execute(text('SELECT RELEASE_LOCK(:name)'),{'name':native.LOCK});c.commit()
