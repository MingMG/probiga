"""Scheduler-owned native strategy research with durable source/date checkpoints."""
from datetime import date, datetime, timedelta
import json
import logging
import re
import signal
import time

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from server.common.analysis_pool_receipt import canonical_sha256
from server.common.verified_daily_market_truth import DailyPriceSourceUnavailable
from server.engine.native_feature_math import _features, _frame, _assert_read_succeeded
from server.engine.autonomous_strategy_research import now_shanghai
from server.engine.strategy_rule_backtest import _partitions, partition_key, PART_ENTITY
from server.engine.strategy_price_backtest import _group_inputs, _validate_group, _read_bars, _input_parts

ENTITY = 'NATIVE_REPLAY'
SCHEMA = 'probiga.native-strategy-research.v1'
LOCK = 'probiga:native-strategy-research'
logger=logging.getLogger(__name__)
timing = logging.getLogger("server.native_replay.timing")


class NativeStepDeadline(BaseException):
    """Source adapters must not convert the job deadline into a data gap."""


def read_runs(connection, run_id=None):
    from server.engine.strategy_governance import _digest
    from server.common.strategy_audit_heads import latest_head, recent_heads
    head = latest_head(connection, ENTITY, run_id) if run_id is not None else None
    heads = ([head] if head else []) if run_id is not None else recent_heads(connection, ENTITY, 8)
    records=[]
    for head in heads:
        records.extend(connection.execute(text("""SELECT /*+ MAX_EXECUTION_TIME(4000) */ payload_json,audit_hash,after_json
            FROM st_strategy_governance_audit WHERE audit_id=:id"""),
            {'id':head['audit_id']}).mappings().all())
    result=[]
    for r in records:
        payload,after=json.loads(r['payload_json']),json.loads(r['after_json'])
        if _digest(payload)!=r['audit_hash'] or payload['after']!=after or after['schema']!=SCHEMA:
            raise ValueError('native research audit integrity differs')
        result.append(after)
    return result


def _strategies(c):
    rows=c.execute(text("""SELECT /*+ MAX_EXECUTION_TIME(4000) */ r.strategy_key,r.strategy_name,r.current_version,v.version_hash,
        v.source_kind,v.evaluator_type,v.evaluator_config_json,v.parameters_json FROM st_strategy_registry r
        JOIN st_strategy_version v ON v.strategy_key=r.strategy_key AND v.version=r.current_version
        WHERE v.source_kind IN ('immutable_manifest','immutable_v3_sleeve') ORDER BY r.strategy_key""")).mappings()
    result=[{**{k:r[k] for k in ('strategy_key','strategy_name','current_version','version_hash','source_kind','evaluator_type')},
        'evaluator_config':json.loads(r['evaluator_config_json']),'parameters':json.loads(r['parameters_json'])} for r in rows]
    from server.engine.strategy_governance import _strategy_version_digest
    for s in result:
        expected=_strategy_version_digest(strategy_key=s['strategy_key'],version=s['current_version'],evaluator_type=s['evaluator_type'],
            evaluator_config=s['evaluator_config'],parameters=s['parameters'],source_kind=s['source_kind'])
        if expected!=s['version_hash']:raise ValueError('原生策略不可变版本哈希不一致：'+s['strategy_key'])
    return result


def _inventory(c,day):
    from server.engine.autonomous_strategy_research import _historical_inventory
    days,published=_historical_inventory(c,{'trade_date':day,'decision_known_at':str(now_shanghai())})
    members=[dict(r) for r in c.execute(text("""SELECT snapshot_date,industry_hash,captured_at,quality_status
        FROM qmt_membership_snapshot_run WHERE source='gj_big_qmt_inner' AND snapshot_date BETWEEN :a AND :b
        ORDER BY snapshot_date"""),{'a':days[0],'b':days[-1]}).mappings()]
    seals={}
    anchor=c.execute(text("""SELECT /*+ MAX_EXECUTION_TIME(4000) */ MIN(stock_code)
        FROM qmt_industry_member_snapshot WHERE snapshot_date=:d AND source='gj_big_qmt_inner'"""),{'d':day}).scalar()
    for kind,extra in [('finance',"AND stock_code='000000' AND source='probiga.finance.atomic_batch'"),('event','')]:
        if kind=='event' and not anchor:
            seals[kind]=None
            continue
        # An atomic announcement publication updates every member together.
        # This anchor only invalidates caches; the full batch is verified at use.
        if kind=='event':extra="AND stock_code=:anchor AND source IN ('qmt.announcement','cninfo.announcement','eastmoney.notice')"
        row=c.execute(text("SELECT /*+ MAX_EXECUTION_TIME(4000) */ coverage_id,known_at FROM st_pit_source_coverage "
            "WHERE fact_kind=:kind AND coverage_status='COMPLETE' "+extra+
            " ORDER BY known_at DESC,coverage_id DESC LIMIT 1"),{'kind':kind,'anchor':anchor}).mappings().first()
        seals[kind]=dict(row) if row else None
    from server.common.qmt_datadir_result import RECEIPT_TABLE
    price_receipts=c.execute(text(f"""SELECT trade_date,run_id,content_sha256,finished_at
        FROM {RECEIPT_TABLE} WHERE dataset='stock_daily' AND status='COMPLETE'
        AND trade_date BETWEEN :start AND :end AND finished_at<=:clock ORDER BY trade_date"""),
        {'start':days[0],'end':days[-1],'clock':now_shanghai()}).mappings()
    identity=json.loads(json.dumps({'published':published,'membership':members,'seals':seals,
        'price_receipts':[dict(row) for row in price_receipts]},default=str))
    return days,published,canonical_sha256(identity)


def _restore_failure_lock(connection):
    # Interrupted driver reads can invalidate the connection and release its session lock.
    try:
        owned=connection.execute(text('SELECT IS_USED_LOCK(:name)=CONNECTION_ID()'),{'name':LOCK}).scalar()==1
    except DBAPIError as exc:
        if not exc.connection_invalidated:raise
        connection.rollback()
        owned=connection.execute(text('SELECT IS_USED_LOCK(:name)=CONNECTION_ID()'),{'name':LOCK}).scalar()==1
    if not owned:
        if connection.execute(text('SELECT GET_LOCK(:name,0)'),{'name':LOCK}).scalar()!=1:
            raise RuntimeError('原生回测失败回执无法恢复互斥锁')


def _part(state,kind,day,value):
    return {'entity_type':PART_ENTITY,'entity_key':partition_key(state,state,kind,day),
        'action':'PARTITION_COMPLETE','after':{'day':day,**value}}


def _start(connection,strategies,days,published,source_hash,*,frozen=None,
           allocation_policy=None,execution_end_date=None):
    from server.trading_v3.config import load_v3_config
    from server.common.strategy_daily_input_window import V3_DAILY_INPUT_SESSIONS
    from server.engine.strategy_rule_replay import ACCOUNT
    from server.engine.native_research_identity import algorithm_identity,producer_identity,request_identity,feature_algorithm_identity
    if not days or days!=sorted(set(days)):raise ValueError('native evaluation sessions must be ordered and unique')
    versions=[(item['strategy_key'],item['current_version'],item['version_hash']) for item in strategies]
    if frozen is not None and (frozen['source_hash']!=source_hash or frozen['sessions']!=days
            or frozen['published']!=published
            or [(item['strategy_key'],item['current_version'],item['version_hash']) for item in frozen['strategies']]!=versions):
        raise ValueError('冻结回测输入与目标策略及来源身份不一致')
    if frozen is None:
        frozen=next((run for run in read_runs(connection) if run['source_hash']==source_hash and run['sessions']==days
            and [(item['strategy_key'],item['current_version'],item['version_hash']) for item in run['strategies']]==versions),None)
    clock=str(now_shanghai())
    requested=frozen['requested_at'] if frozen else clock
    cutoff=frozen['price_cutoff'] if frozen else requested
    archives=(list(frozen['input_archives'])+[{'run_id':frozen['run_id'],'rule_hash':frozen['rule_hash']}] if frozen else
        [])
    allocation_policy=allocation_policy or (frozen.get('allocation_policy','FIXED_SHADOW') if frozen else 'FIXED_SHADOW')
    execution_end_date=execution_end_date or (frozen.get('execution_end_date',days[-1]) if frozen else days[-1])
    if allocation_policy not in ('FIXED_SHADOW','ADAPTIVE_REGIME'):raise ValueError('native allocation policy differs')
    if not days[-1]<=execution_end_date<=min(requested[:10],cutoff[:10]):
        raise ValueError('native execution window exceeds frozen availability clocks')
    config=json.loads(json.dumps(load_v3_config()))
    algorithm=algorithm_identity(config);producer=producer_identity(strategies[0]['source_kind'])
    request=request_identity(strategies=strategies,sessions=days,published=published,source_hash=source_hash,
        requested_at=requested,price_cutoff=cutoff,algorithm=algorithm,account=dict(ACCOUNT),
        allocation_policy=allocation_policy,producer=producer,execution_end_date=execution_end_date)
    span=int(connection.execute(text('SELECT /*+ MAX_EXECUTION_TIME(4000) */ COUNT(DISTINCT trade_date) FROM si_trade_calendar '
        'WHERE trade_status=1 AND trade_date BETWEEN :start AND :end'),{'start':days[0],'end':execution_end_date}).scalar())
    count=span+V3_DAILY_INPUT_SESSIONS
    if count>5000:raise ValueError('native calendar reference exceeds 5000 sessions')
    calendar=sorted(str(value)[:10] for value in connection.execute(text('SELECT /*+ MAX_EXECUTION_TIME(4000) */ DISTINCT trade_date FROM si_trade_calendar '
        'WHERE trade_status=1 AND trade_date<=:end ORDER BY trade_date DESC LIMIT :count'),
        {'end':execution_end_date,'count':count}).scalars())
    if not set(days)<=set(calendar) or execution_end_date not in calendar:
        raise ValueError('native evaluation and execution dates must be observed trading sessions')
    first=calendar.index(days[0])
    inputs=calendar[max(0,first-20):]
    state={'schema':SCHEMA,'architecture':'PREPARED_TYPED_NATIVE_JOB_V2',
        'run_id':request['sha256'],'request_identity':request['sha256'],'request':request,
        'trade_date':days[-1],'strategies':strategies,'published':published,'sessions':days,
        'requested_at':requested,'price_cutoff':cutoff,'deadline_at':str(now_shanghai()+timedelta(hours=24)),
        'input_archives':archives,'source_run_id':frozen['run_id'] if frozen else None,
        'source_hash':source_hash,'rule_hash':canonical_sha256(versions),'contract':algorithm['sha256'],
        'algorithm_identity':algorithm,'producer_identity':producer,'feature_algorithm_sha256':feature_algorithm_identity(algorithm),
        'config':config,'account':dict(ACCOUNT),'allocation_policy':allocation_policy,
        'experiment_kind':request['experiment_kind'],'execution_end_date':execution_end_date,
        'calendar':calendar,'input_sessions':inputs,'performance_sessions':[day for day in inputs if day>=days[0]],
        'phase':'PLAN','status':'NOT_STARTED','cursor':0,'completed_units':0,'total_units':len(days)+1,
        'last_progress_at':clock,'reason':'','proposals':[],'input_index':None,
        'plans':{},'plan_summary':{},'feature_windows':{},'feature_input_sessions':[],
        'execution_input_sessions':[],'data_sessions':[],'prepared_references':{},
        'bars':{},'day_inputs':{},'signal_parts':{},'reports':{},'base_data_by_day':{},'reused_input_sessions':0,'reused_feature_sessions':0}
    if strategies[0]['source_kind']=='immutable_v3_sleeve':
        from server.trading_v3.sleeves import SLEEVE_BUILDERS,sleeve_required_features,sleeve_input_mode
        from server.trading_v3.daily_feature_math import ENTRY_PIT_STATUS_FIELDS
        from server.trading_v3.regime import REQUIRED as REGIME_REQUIRED
        state['strategy_dependencies']={item['strategy_key']:{
            'builder':SLEEVE_BUILDERS[item['strategy_key']].__name__,
            'required_features':list(sleeve_required_features(item['strategy_key'])),
            'input_mode':sleeve_input_mode(item['strategy_key']),
            'experiment_input_mode':'CLOSED_DAILY','entry_fact_status_fields':list(ENTRY_PIT_STATUS_FIELDS),
            'market_required_features':list(REGIME_REQUIRED),
            'qualification_contract_sha256':algorithm['sha256'],
            'optional_context_policy':'EXPLICIT_AVAILABILITY_WITH_FROZEN_NEUTRAL_DEFAULTS',
        } for item in strategies if item['strategy_key'] in SLEEVE_BUILDERS}
    for item in strategies:
        state['proposals'].append({'strategy_key':item['strategy_key'],'version':item['current_version'],
            'rule_hash':item['version_hash'],'name':item['strategy_name'],
            'validation':{'status':'NOT_STARTED','reason':'原生历史回测已登记，等待来源检查'}})
    return state


def _contract():
    from server.engine.native_research_identity import algorithm_identity
    from server.trading_v3.config import load_v3_config
    return algorithm_identity(load_v3_config())['sha256']


def _failure_reason(state,exc):
    from sqlalchemy.exc import SQLAlchemyError
    chain=exc
    database_error=False
    for _ in range(10):
        if chain is None:break
        database_error=database_error or isinstance(chain,SQLAlchemyError)
        chain=chain.__cause__
    database_error=database_error or any(token in str(exc) for token in ('[SQL:','[parameters:','(pymysql.err.'))
    state['diagnostic_error']=f'{type(exc).__name__}: {exc}'[:4000]
    detail=('数据库读取或写入失败，原断点保留；详见运行诊断' if database_error else str(exc))
    return f"{state.get('current_unit',state['phase'])}: {type(exc).__name__}: {detail}"[:1000]


def _transient_source_error(exc):
    from server.common.batch_db import _transient_db_errno
    chain=exc
    for _ in range(10):
        if chain is None:break
        if _transient_db_errno(chain) in (1205,1213,2003,2006,2013,3024):return True
        # PIT returns a sanitized diagnostic before the feature guard raises;
        # the original SQLAlchemy exception is no longer in this chain.
        if isinstance(chain, RuntimeError) and re.fullmatch(
                r'历史事实读取失败：PIT_[A-Z_]+:OperationalError:errno='
                r'(?:1205|1213|2003|2006|2013|3024)(?::stage=[a-z_]+)?', str(chain)):
            return True
        if isinstance(chain, RuntimeError) and re.fullmatch(
                r'历史事实读取失败：QMT_ANNOUNCEMENT_LIVE_BATCH_REPLY_(?:DEADLINE|FAILED):[A-Za-z0-9:_-]+', str(chain)):
            return True
        chain=chain.__cause__ or chain.__context__
    return False


def _append_state(connection,state,events=()):
    """No commits here: queue CAS, references and head share the caller transaction."""
    from server.engine.strategy_governance import _append_audit_connection
    for event in events:
        _append_audit_connection(connection,**event,reason='原生历史研究分区已处理',
            operator='native-strategy-research',before=None,evidence={'run_id':state['run_id'],'owner':state.get('owner')})
    _append_audit_connection(connection,entity_type=ENTITY,entity_key=state['run_id'],
        action=f"CHECKPOINT_{state['checkpoint_sequence']:08d}",operator='native-strategy-research',
        reason=state.get('reason') or state['phase'],before=None,after=state,
        evidence={'owner':state.get('owner'),'request_identity':state.get('request_identity'),
                  'real_order_authority':False})


def _save(connection,state,owner,events=(),*,yield_job=False):
    from server.engine.native_research_jobs import checkpoint
    checkpoint(connection,state,owner,events,yield_job=yield_job)


def _begin_unit(connection,state,owner,name):
    state.update(current_unit=name,unit_started_at=str(now_shanghai()))
    _progress(state)
    _save(connection,state,owner)


def _put(dataset,store,state,key,value):
    references=dict(state['prepared_references'])
    tree=dataset.put_tree(store,state['run_id'],'native_prepared',key,value,references=references)
    state['prepared_references']=references
    dataset.pin(state['run_id'],references.values())
    return tree


def _finish_plan(state):
    from server.common.strategy_daily_input_window import V3_DAILY_INPUT_SESSIONS
    calendar=state['calendar']
    feasible=[day for day in state['sessions'] if state['plan_summary'][day]['status']=='COMPLETE']
    feature_dates=set()
    for day in feasible:
        index=calendar.index(day)
        window=calendar[max(0,index-(V3_DAILY_INPUT_SESSIONS-1)):index+1]
        if state['strategies'][0]['source_kind']=='immutable_v3_sleeve':
            if len(window)!=V3_DAILY_INPUT_SESSIONS:
                state['plan_summary'][day]={'status':'BLOCKED','reason':'历史特征预热不足70个交易日'}
                continue
            state['feature_windows'][day]=window
            feature_dates.update(window)
    feasible=[day for day in feasible if state['plan_summary'][day]['status']=='COMPLETE']
    execution=[]
    if feasible:
        start=calendar.index(min(feasible))
        execution=calendar[max(0,start-20):calendar.index(state['execution_end_date'])+1]
    state.update(feature_input_sessions=sorted(feature_dates),execution_input_sessions=execution,
        data_sessions=sorted(feature_dates|set(execution)),phase='PREPARE_BARS',cursor=0)
    # PLAN is now final: no implicit feature acquisition changes this total.
    state['total_units']=len(state['sessions'])+len(state['data_sessions'])+len(state['feature_windows'])*6+(len(feasible) if state['strategies'][0]['source_kind']=='immutable_manifest' else 0)+len(state['strategies'])+2


def _prepare_bar(connection,state,owner,dataset,store,day):
    from server.engine.strategy_price_backtest import _source_scope
    inputs=state['input_index']
    if not any(day in part['sessions'] for part in inputs.values()):
        for group in _group_inputs(connection,[day],state['price_cutoff']):
            value=_validate_group(connection,group,state['price_cutoff'])
            inputs[group['sessions'][0]]=value
            _save(connection,state,owner,[_part(state,'D',group['sessions'][0],value)])
    proof=next(part['proofs'][day] for part in inputs.values() if day in part['proofs'])
    with connection.engine.begin() as source:
        scope=_source_scope(source,proof,day,state['price_cutoff'])
    generation=canonical_sha256({'schema':'probiga.native-prepared-daily-bars.v1','day':day,
        'cutoff':state['price_cutoff'],'proof':proof,'scope':scope})
    retained=store.get(generation,'native_bar_handoff','complete')
    if retained is None:
        raw=_read_bars(connection,[day],inputs,state['price_cutoff'])
        references={}
        tree=dataset.put_tree(store,generation,'native_bar_rows','rows',raw,references=references)
        retained={'tree':tree,'references':references,'proof':proof,'scope':scope,'row_count':len(raw),
                  'producer_identity':state['producer_identity']}
        store.put(generation,'native_bar_handoff','complete',retained)
    else:
        state['reused_input_sessions']+=1
    if retained['proof']!=proof or retained['scope']!=scope:
        raise ValueError('prepared bar source identity differs')
    state['prepared_references'].update(retained['references'])
    dataset.pin(state['run_id'],state['prepared_references'].values())
    dataset.prepare(store,list(retained['references'].values()))
    state['bars'][day]={'tree':retained['tree'],'proof':proof,'scope':scope,'row_count':retained['row_count']}


def _prepare_trees(dataset,store,*trees):
    references={}
    for tree in trees:references.update(dataset.tree_references(tree))
    dataset.prepare(store,list(references.values()))


def _context_generation(state,day):
    from server.common.strategy_daily_input_window import V3_DAILY_INPUT_SESSIONS
    from server.trading_v3.theme_features import research_theme_keywords
    window=state['feature_windows'][day]
    if (len(window)!=V3_DAILY_INPUT_SESSIONS or window!=sorted(set(window)) or window[-1]!=day):
        raise ValueError('prepared context feature window differs')
    return canonical_sha256({'schema':'probiga.native-ready-context-input.v1','day':day,
        'known_at':state['requested_at'],'price_cutoff':state['price_cutoff'],
        'producer':state['producer_identity'],'algorithm':state['feature_algorithm_sha256'],
        'config':state['config'],'research_keywords':research_theme_keywords(state['config']),
        'plan':state['plans'][day],
        'facts':{key:state['day_inputs'][day][key] for key in ('selection','finance','events')},
        'bars':[{'day':target,**{field:state['bars'][target][field] for field in ('tree','proof','scope')}}
                for target in window]})


def _context_handoff(state,day,dataset):
    inputs=state['day_inputs'][day]
    if inputs.get('ready')!={key:inputs[key] for key in ('selection','finance','events','context')}:
        raise ValueError('prepared context requires an exact READY completion')
    tree=inputs['context']
    return {'schema':'probiga.native-ready-context-handoff.v1',
        'input_identity':_context_generation(state,day),'tree':tree,'references':dataset.tree_references(tree)}


def retain_ready_context(connection,source_state,day,*,dataset,store):
    """Index an explicitly selected audited READY context without recapturing it."""
    from server.engine import native_research_prepare as prepare
    source=read_runs(connection,run_id=source_state['run_id'])
    if not source or source[0]!=source_state:
        raise ValueError('prepared context source differs from its audited state')
    record=_context_handoff(source_state,day,dataset)
    inputs=source_state['day_inputs'][day]
    _prepare_trees(dataset,store,source_state['plans'][day],*inputs['ready'].values())
    plan=dataset.get_tree(source_state['plans'][day])
    if plan.get('status')!='COMPLETE' or plan.get('day')!=day:
        raise ValueError('prepared context source plan differs')
    values={key:dataset.get_tree(inputs[key]) for key in ('selection','finance','events','context')}
    prepare.validate_ready_inputs(plan,*(values[key] for key in ('selection','finance','events','context')),
        requested_at=source_state['requested_at'],price_cutoff=source_state['price_cutoff'],config=source_state['config'])
    identity=prepare.base_data_identity(plan,values['selection'],values['context'],
        finance_reference=inputs['finance'],event_reference=inputs['events'])
    if source_state.get('base_data_by_day',{}).get(day)!=identity:
        raise ValueError('prepared context source has no matching READY data identity')
    digest=store.put(record['input_identity'],'native_context_handoff','complete',record)
    return {'generation':record['input_identity'],'artifact_sha256':digest}


def _retained_context(state,day,plan,dataset,store):
    from server.engine import native_research_prepare as prepare
    generation=_context_generation(state,day)
    record=store.get(generation,'native_context_handoff','complete')
    if record is None:return None
    if (type(record) is not dict or set(record)!={'schema','input_identity','tree','references'}
            or record['schema']!='probiga.native-ready-context-handoff.v1'
            or record['input_identity']!=generation
            or record['references']!=dataset.tree_references(record['tree'])):
        raise ValueError('prepared context handoff identity differs')
    inputs=state['day_inputs'][day]
    _prepare_trees(dataset,store,*(inputs[key] for key in ('selection','finance','events')),record['tree'])
    prepare.validate_ready_inputs(plan,*(dataset.get_tree(inputs[key]) for key in ('selection','finance','events')),
        dataset.get_tree(record['tree']),requested_at=state['requested_at'],price_cutoff=state['price_cutoff'],config=state['config'])
    return record


def _prepare_day(connection,state,owner,dataset,store,day,*,prepare_budget=None):
    if prepare_budget is not None:prepare_budget()
    from contextlib import nullcontext
    from server.common.formal_analysis_prefetch import prefetch_checkpoint_reads
    from server.engine.native_research_jobs import _source_progress
    from server.engine import native_research_prepare as prepare
    from server.trading_v3.daily_features import prepare_feature_identity_context
    inputs=state['day_inputs'].setdefault(day,{})
    _prepare_trees(dataset,store,state['plans'][day])
    plan=dataset.get_tree(state['plans'][day])
    if plan['status']!='COMPLETE':
        state['signal_parts'][day]=_put(dataset,store,state,day+':signals',
            {'status':'BLOCKED','reason':plan['reason'],'identity':plan.get('identity',{}),'signals':{}})
        return
    for stage,function in [('selection',prepare.prepare_selection),('finance',prepare.prepare_finance),('events',prepare.prepare_events)]:
        if stage in inputs:continue
        _begin_unit(connection,state,owner,'PREPARE '+stage+' '+day)
        inputs=state['day_inputs'][day]
        if stage!='selection':_prepare_trees(dataset,store,inputs['selection'])
        reads = nullcontext()
        if stage == 'finance':
            cache_deadline = time.monotonic() + 45
            reads = prefetch_checkpoint_reads(store, connection.engine, plan['snapshot_generation'],
                'finance_issuer_facts', dataset=dataset, deadline=cache_deadline,
                progress=_source_progress(connection, state, owner, day, 'finance_checkpoint_cache', deadline=cache_deadline))
        with reads:
            value=(function(connection,state,owner,plan) if stage=='selection' else
                   function(connection,state,owner,plan,dataset.get_tree(inputs['selection'])))
        inputs=state['day_inputs'][day]
        inputs[stage]=_put(dataset,store,state,day+':'+stage,value)
        state['completed_units']+=1
        if stage=='selection' and value['status']=='BLOCKED':
            state['signal_parts'][day]=_put(dataset,store,state,day+':signals',{**value,'signals':{}})
            state['completed_units']+=5
        _save(connection,state,owner)
        return
    if 'context' not in inputs:
        _begin_unit(connection,state,owner,'PREPARE context '+day)
        retained=_retained_context(state,day,plan,dataset,store)
        if retained is not None:
            state['prepared_references'].update(retained['references'])
            dataset.pin(state['run_id'],state['prepared_references'].values())
            state['day_inputs'][day]['context']=retained['tree']
            state['completed_units']+=1
            _save(connection,state,owner)
            return
        _prepare_trees(dataset,store,*(state['bars'][target]['tree'] for target in state['feature_windows'][day]))
        frame=_frame(dataset,state,plan,day)
        from server.trading_v3.theme_features import research_theme_keywords
        context=prepare_feature_identity_context(connection.engine,as_of=date.fromisoformat(day),
            dates=[date.fromisoformat(value) for value in state['feature_windows'][day]],frame=frame,
            context_cutoff_at=datetime.fromisoformat(state['requested_at']),entry_scope_codes=plan['eligible'],
            research_keywords=research_theme_keywords(state['config']))
        context['binding']={'day':day,'decision_at':state['requested_at'],'price_cutoff':state['price_cutoff'],
            'identity_sha256':plan['identity']['sha256'],'finance_seal':plan['finance_seal']}
        state['day_inputs'][day]['context']=_put(dataset,store,state,day+':context',context)
        state['completed_units']+=1
        _save(connection,state,owner)
        return
    _prepare_trees(dataset,store,*(inputs[key] for key in ('selection','finance','events','context')))
    prepare.validate_ready_inputs(plan,*(dataset.get_tree(inputs[key]) for key in ('selection','finance','events','context')),
        requested_at=state['requested_at'],price_cutoff=state['price_cutoff'],config=state['config'])
    state['base_data_by_day'][day]=prepare.base_data_identity(plan,dataset.get_tree(inputs['selection']),
        dataset.get_tree(inputs['context']),finance_reference=inputs['finance'],event_reference=inputs['events'])
    inputs['ready']={key:inputs[key] for key in ('selection','finance','events','context')}
    context_handoff=_context_handoff(state,day,dataset)
    store.put(context_handoff['input_identity'],'native_context_handoff','complete',context_handoff)
    generation=_feature_generation(state,day)
    retained=store.get(generation,'native_feature_handoff','complete')
    if retained is not None:
        if retained['input_identity']!=generation:raise ValueError('prepared feature input identity differs')
        state['prepared_references'].update(retained['references'])
        dataset.pin(state['run_id'],state['prepared_references'].values())
        dataset.prepare(store,list(retained['references'].values()))
        inputs['features']=retained['tree']
        state['completed_units']+=1
        state['reused_feature_sessions']+=1
    state['current_unit']='READY '+day
    _save(connection,state,owner)


def _feature_generation(state,day):
    return canonical_sha256({'schema':'probiga.native-prepared-feature.v1',
        'algorithm':state['feature_algorithm_sha256'],'day':day,
        'ready':state['day_inputs'][day]['ready'],
        'bars':{target:{field:state['bars'][target][field] for field in ('tree','proof','scope')}
                for target in state['feature_windows'][day]}})


def _progress(state):
    for proposal in state['proposals']:
        if proposal['strategy_key'] in state['reports']:continue
        proposal['validation'].update(status='RUNNING',completed_units=state['completed_units'],
            total_units=state['total_units'],current_unit=state.get('current_unit'),phase=state['phase'],
            replay_run_id=state['run_id'],reason=state.get('current_unit') or state['phase'])


def _step(connection,state,owner,*,prepare_budget=None):
    from server.common.formal_analysis_store import FormalAnalysisStore
    from server.engine.native_research_dataset import PreparedDataset
    from server.engine.native_research_prepare import plan_v3_day
    from server.engine.native_research_math import evaluate_native_signals
    dataset,store=PreparedDataset(),FormalAnalysisStore(connection.engine)
    if state['input_index'] is None:
        from server.engine.native_research_prepare import validate_input_index
        _begin_unit(connection,state,owner,'PLAN 原始数据断点')
        state['input_index']=validate_input_index(_input_parts(connection,state))
        state['completed_units']+=1
        state['last_progress_at']=str(now_shanghai())
        _save(connection,state,owner)
    active=list(connection.execute(text("SELECT result_ref FROM st_job_v2 WHERE job_type='NATIVE_REPLAY' AND status IN ('PENDING','RUNNING') LIMIT 2001")).scalars())
    if len(active)>2000:raise RuntimeError('PREPARE_ACTIVE_EXPERIMENT_LIMIT_EXCEEDED')
    dataset.reconcile_pins(active)
    dataset.pin(state['run_id'],state['prepared_references'].values())
    deadline=time.monotonic()+100
    while time.monotonic()<deadline and state['phase']!='FINISHED':
        before=state['completed_units']
        failed_reconciliation = None
        published_manifest = False
        if state['phase']=='PLAN':
            if state['cursor']>=len(state['sessions']):
                _finish_plan(state)
            else:
                day=state['sessions'][state['cursor']]
                _begin_unit(connection,state,owner,'PLAN '+day)
                if state['strategies'][0]['source_kind']=='immutable_manifest':
                    published_manifest=bool(state['published'].get(day))
                    if published_manifest and prepare_budget is not None:
                        prepare_budget()
                    from server.engine.native_original_prepare import prepare_original_day
                    value=prepare_original_day(connection,state,day)
                else:value=plan_v3_day(connection,state,day)
                state['plans'][day]=_put(dataset,store,state,'plan:'+day,value)
                if state['strategies'][0]['source_kind']=='immutable_manifest':
                    if value['status']!='COMPLETE':state['signal_parts'][day]=state['plans'][day]
                    if value['status']=='COMPLETE':
                        state['base_data_by_day'][day]=canonical_sha256({'score_snapshot':value['source_sha256'],
                            'source_run_uid':value['source_run_uid'],'identities':value['identities'],
                            'identity':value['identity'],'fixed_market_context':value['fixed_market_context']})
                state['plan_summary'][day]={key:value[key] for key in ('status','reason') if key in value}
                state['cursor']+=1;state['completed_units']+=1
        elif state['phase']=='PREPARE_BARS':
            if state['cursor']>=len(state['data_sessions']):state.update(phase='PREPARE_DAYS',cursor=0)
            else:
                day=state['data_sessions'][state['cursor']]
                _begin_unit(connection,state,owner,'PREPARE bars '+day)
                _prepare_bar(connection,state,owner,dataset,store,day)
                state['cursor']+=1;state['completed_units']+=1
        elif state['phase']=='PREPARE_DAYS':
            days=[day for day in state['sessions'] if day in state['feature_windows']]
            if state['cursor']>=len(days):state.update(phase='FEATURES',cursor=0)
            else:
                day=days[state['cursor']]
                if day in state['signal_parts'] or state['day_inputs'].get(day,{}).get('ready'):state['cursor']+=1
                else:_prepare_day(connection,state,owner,dataset,store,day,prepare_budget=prepare_budget)
        elif state['phase']=='FEATURES':
            days=[day for day in state['sessions'] if day in state['feature_windows'] and day not in state['signal_parts']]
            if state['cursor']>=len(days):state.update(phase='SIGNALS',cursor=0)
            else:
                day=days[state['cursor']]
                feature_deadline=prepare_budget() if prepare_budget is not None else time.monotonic()+900
                _begin_unit(connection,state,owner,'FEATURE '+day)
                if 'features' not in state['day_inputs'][day]:
                    started = time.perf_counter()
                    _prepare_trees(dataset,store,state['plans'][day],*state['day_inputs'][day]['ready'].values(),
                        *(state['bars'][target]['tree'] for target in state['feature_windows'][day]))
                    from server.engine.native_research_jobs import feature_input_progress
                    observed=feature_input_progress(connection,state,owner,dataset,day,deadline=feature_deadline)
                    value=_features(state,observed,day)
                    tree=_put(dataset,store,state,day+':features',value)
                    state['day_inputs'][day]['features']=tree
                    generation=_feature_generation(state,day)
                    store.put(generation,'native_feature_handoff','complete',{'input_identity':generation,
                        'tree':tree,'references':dataset.tree_references(tree)})
                    timing.info("feature-preparation day=%s elapsed=%.3fs", day, time.perf_counter() - started)
                    state['completed_units']+=1
                state['cursor']+=1
        elif state['phase']=='SIGNALS':
            manifest=state['strategies'][0]['source_kind']=='immutable_manifest'
            days=([day for day in state['sessions'] if state['plan_summary'][day]['status']=='COMPLETE'] if manifest else
                [day for day in state['sessions'] if day in state['day_inputs'] and 'features' in state['day_inputs'][day]])
            if state['cursor']>=len(days):state.update(phase='REPLAY',cursor=0)
            else:
                day=days[state['cursor']]
                _begin_unit(connection,state,owner,'SIGNAL '+day)
                started = time.perf_counter()
                _prepare_trees(dataset,store,state['plans'][day])
                plan=dataset.get_tree(state['plans'][day])
                if manifest:
                    from server.engine.native_research_math import evaluate_manifest_signals
                    timing.info("signal-handoff day=%s elapsed=%.3fs", day, time.perf_counter() - started)
                    value=evaluate_manifest_signals(plan,state['strategies'],day=day)
                    value['candidate_universe_count'] = len(plan['identities'])
                else:
                    inputs=state['day_inputs'][day]
                    _prepare_trees(dataset,store,inputs['features'],inputs['selection'])
                    features=dataset.get_tree(inputs['features']);selection=dataset.get_tree(inputs['selection'])
                    timing.info("signal-handoff day=%s elapsed=%.3fs", day, time.perf_counter() - started)
                    value=evaluate_native_signals(features,state['strategies'],config=state['config'],day=day,
                        signal_clock=selection['signal_clock'],allocation_policy=state['allocation_policy'],identities=plan['identity_rows'])
                    value.update(identity=plan['identity'],finance_scope_exclusions=plan['excluded'],
                        candidate_universe_count=len(plan['eligible']),
                        finance_seal_coverage_id=plan['finance_seal'],fact_cutoff=str(selection['fact_clock']),
                        source_cutoff_at=selection['cutoff']['source_cutoff_at'],
                        universe_identity_exclusion_count=max(0,features['market_features']['qmt_daily_input_window']['latest_daily_truth']['attested_row_count']-len(plan['identity_rows'])))
                state['signal_parts'][day]=_put(dataset,store,state,day+':signals',value)
                state['cursor']+=1;state['completed_units']+=1
        elif state['phase']=='REPLAY':
            if state.get('pending_report') is not None:
                _begin_unit(connection,state,owner,'RECONCILE '+state['pending_report']['strategy_key'])
                failed_reconciliation = _reconcile_report(state,dataset,store)
            else:
                if state['cursor']<len(state['strategies']):
                    _begin_unit(connection,state,owner,'REPLAY '+state['strategies'][state['cursor']]['strategy_key'])
                    _prepare_trees(dataset,store,*state['signal_parts'].values(),
                        *(state['bars'][target]['tree'] for target in state['execution_input_sessions']))
                _report_one(state,dataset,store=store)
        else:raise ValueError('unknown native stage: '+state['phase'])
        if state['completed_units']>before:state['last_progress_at']=str(now_shanghai())
        _progress(state)
        _save(connection,state,owner)
        if failed_reconciliation:
            key, checks = failed_reconciliation
            logger.error("Native replay reconciliation blocked publication: run=%s strategy=%s checks=%s",
                         state['run_id'], key, checks)
        if published_manifest:
            break


def _report_one(state,dataset,*,store=None):
    from server.engine.strategy_native_replay import replay_native
    index=state['cursor']
    if index>=len(state['strategies']):
        state.update(phase='FINISHED',status='BLOCKED' if any(value['status']!='COMPLETE' for value in state['reports'].values()) else 'COMPLETE')
        state['completed_units']+=1
        return
    strategy,proposal=state['strategies'][index],state['proposals'][index]
    key=strategy['strategy_key'];state['current_unit']='REPLAY '+key
    parts={day:(dataset.get_tree(state['signal_parts'][day]) if day in state['signal_parts'] else
           {'status':'BLOCKED','reason':state['plan_summary'][day].get('reason','历史特征准备不可用'),'signals':{}})
           for day in state['sessions']}
    gaps=[{'trade_date':day,'reason':part['reason']} for day,part in parts.items() if part['status']=='BLOCKED']
    gaps.extend({'trade_date':day,'reason':f"部分证券历史身份或策略特征不足：身份 {part.get('universe_identity_exclusion_count',0)}，财务覆盖 {len(part.get('finance_scope_exclusions',[]))}，特征 {part.get('missing_feature_stocks',{}).get(key,0)}"}
        for day,part in parts.items() if part['status']=='COMPLETE' and (part.get('universe_identity_exclusion_count',0) or part.get('finance_scope_exclusions') or part.get('missing_feature_stocks',{}).get(key,0)))
    signals=[signal for part in parts.values() for signal in part['signals'].get(key,[])]
    codes={signal['stock_code'] for signal in signals}
    bars=[bar for day in state['execution_input_sessions'] for bar in dataset.get_tree(state['bars'][day]['tree']) if bar['stock_code'] in codes]
    report=replay_native(strategy,signals,bars,state['input_sessions'],evaluation_start=state['sessions'][0],
        source_gaps=gaps,account=state['account'],allocation_policy=state['allocation_policy'])
    usable=lambda part:part['status']=='COMPLETE' and bool(part.get('evaluation_security_sets',{}).get(key))
    available=sum(usable(part) for part in parts.values())
    fully_covered=lambda part:usable(part) and not part.get('missing_feature_stocks',{}).get(key,0) and not part.get('finance_scope_exclusions') and not part.get('universe_identity_exclusion_count',0)
    complete=sum(fully_covered(part) for part in parts.values())
    if not available:
        report.update(status='BLOCKED',net_return_pct=None,verified_input_subset_return_pct=None,nav=[])
        report.pop('ledger')
    # A single comparison base excludes strategy algorithms and their signal outputs.
    base={'prices':{day:{'proof':part['proof'],'scope':part['scope']} for day,part in state['bars'].items()},
          'non_price_sources':state.get('base_data_by_day',{}),
          'evaluation_sessions':state['sessions'],'initial_cash_cny':250000.0,'account':state['account'],
          'allocation_policy':state['allocation_policy'],'price_cutoff':state['price_cutoff'],
          'requested_at':state['requested_at'],'execution_mode':'MODELED_DAILY_BAR'}
    evaluation_set={day:part.get('evaluation_security_sets',{}).get(key,[])
                    for day,part in parts.items() if part['status']=='COMPLETE'}
    base['actual_evaluation_security_sets']=evaluation_set
    validation={'status':report['status'],'sessions':state.get('performance_sessions',state['sessions']),
        'evaluation_sessions':state['sessions'],'source_gaps':gaps,
        'replay_run_id':state['run_id'],'completed_units':state['total_units'],'total_units':state['total_units'],
        'available_feature_sessions':available,'required_sessions':len(state['sessions']),
        'complete_feature_sessions':complete,
        'missing_feature_dates':[day for day,part in parts.items() if not usable(part)],
        'partial_feature_dates':[day for day,part in parts.items() if usable(part) and not fully_covered(part)],
        'identity_covered_sessions':sum(part.get('identity',{}).get('status')=='VERIFIED' for part in parts.values()),
        'source_input_sha256':canonical_sha256(base),'algorithm_sha256':state['contract'],
        'evaluation_set_sha256':canonical_sha256(evaluation_set),
        'reason':f"计算结束·可用{available}/{len(state['sessions'])}日·"+('完整验证未完成；'+gaps[0]['reason'] if gaps else '详见成交、费用和净值'),
        'funding_eligible':False}
    requirements=state.get('strategy_dependencies',{}).get(key)
    if requirements:
        validation['data_requirements']=requirements
        if requirements['input_mode']!=requirements['experiment_input_mode']:
            validation['reason']='当前实验为已收盘日线；该策略需要盘中金额异常、VWAP、成交概率和点差，日线不能替代。'
    if available:
        validation['signal_counts'] = {
            day: {'evaluated_stocks': len(evaluation_set[day]) if day in evaluation_set else None,
                  'signaled_stocks': len({signal['stock_code'] for signal in part['signals'].get(key, [])})
                    if day in evaluation_set else None,
                  'candidate_universe_count': part.get('candidate_universe_count')}
            for day, part in parts.items()
        }
        raw = _put(dataset,store,state,'replay-ledger:'+key,report)
        state['pending_report'] = {'strategy_key': key, 'tree': raw, 'validation': validation}
        return
    validation['report'] = report
    proposal['validation']=validation;state['reports'][key]=validation
    state['cursor']+=1;state['completed_units']+=1


def _reconcile_report(state,dataset,store):
    from server.engine.strategy_native_reporting import native_report_metrics, reconcile_native

    pending = state['pending_report']
    key, tree = pending['strategy_key'], pending['tree']
    _prepare_trees(dataset,store,tree)
    raw = dataset.get_tree(tree)
    prices = {}
    for row in raw['ledger']:
        day = row['trade_date']
        codes = {item['stock_code'] for item in row['fills'] + row['marks']}
        if not codes or day not in state['bars']:
            continue
        price_tree = state['bars'][day]['tree']
        _prepare_trees(dataset,store,price_tree)
        for bar in dataset.get_tree(price_tree):
            if bar['stock_code'] in codes:
                prices[day,bar['stock_code']] = {'open': float(bar['open']), 'close': float(bar['close'])}
    receipt = reconcile_native(raw, prices)
    validation = pending['validation']
    signal_counts = validation.pop('signal_counts')
    validation['reconciliation'] = {**receipt, 'raw_evidence': tree}
    if receipt['status'] == 'PASSED':
        validation['report'] = {name: value for name, value in raw.items() if name != 'ledger'}
        validation['report'].update(native_report_metrics(raw,receipt,signal_counts))
        if validation['evaluation_sessions'] == [
                '2026-11-16', '2026-11-17', '2026-11-18', '2026-11-19', '2026-11-20']:
            validation['report']['window_notes'] = [
                '本轮为五个交易日的管道验证，不构成策略有效性结论。夏普、回撤和盈亏比仅作口径演示。',
                '本轮参考候选上限为 4,750 只，实际评估数以逐日信号回执为准。',
                '北交所 356 只因历史行业映射缺失未进入候选池；历史映射补齐后可参加候选资格检查。',
                '公告空结果原批抽查 20/73。同口径另补查科创板 1 只、北交所 3 只，四个板块合计 24 只，未发现静默失败。',
                '本区间适用 0.05% 卖出印花税。五日窗口无法量化幸存者偏差。',
            ]
            validation['report']['long_window_note'] = (
                '长窗口预查的 485 个交易日中，16 天同时具备行业与财务前置回执。公告和行情待核验。')
    else:
        validation.update(status='BLOCKED', reason='净值对账未通过，正式报告暂停发布。')
    state['proposals'][state['cursor']]['validation'] = validation
    state['reports'][key] = validation
    state.pop('pending_report')
    state['cursor'] += 1
    state['completed_units'] += 1
    if receipt['status'] == 'FAILED':
        return key, sorted({row['check'] for row in receipt['differences']})


def submit_native_experiments(engine):
    """Plan immutable typed jobs; this entry point never executes a replay."""
    from server.common.authoritative_market_clock import authoritative_closed_trade_date
    from server.engine.native_research_jobs import enqueue
    with engine.begin() as connection:
        day=authoritative_closed_trade_date(connection,now=now_shanghai())
        strategies=_strategies(connection);days,published,source_hash=_inventory(connection,day)
        submitted=[]
        for family in ('immutable_manifest','immutable_v3_sleeve'):
            group=[item for item in strategies if item['source_kind']==family]
            if group:
                policies=('FIXED_SHADOW','ADAPTIVE_REGIME') if family=='immutable_v3_sleeve' else ('FIXED_SHADOW',)
                for policy in policies:
                    submitted.append(enqueue(connection,_start(connection,group,days,published,source_hash,
                        allocation_policy=policy)))
    if any(item['strategy_key']=='short_term' for item in strategies):
        from server.engine.native_opening_research import enqueue_opening
        with engine.begin() as connection:
            submitted.append(enqueue_opening(connection, strategies, days))
    return {'status':'COMPLETE','jobs':submitted,'business_started':False}


def native_tick(engine):
    if engine.dialect.name != 'mysql':
        return _native_tick(engine)
    from server.common.engine_factory import create_pooled_engine
    # The announcement adapter bounds a whole call by the tighter socket timeout.
    bounded = create_pooled_engine(engine.url, pool_timeout=5,
        connect_args={'connect_timeout': 5, 'read_timeout': 35, 'write_timeout': 35})
    try:
        return _native_tick(bounded)
    finally:
        bounded.dispose()


def _atomic_prepare_deadline(attempt_started, *, arm_timer):
    # Preparation includes the announcement validator's existing 15-minute
    # budget as well as verified input decoding and durable writes.
    deadline = attempt_started + 900
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise NativeStepDeadline('NATIVE_PREPARE_ATTEMPT_DEADLINE')
    if arm_timer:
        signal.setitimer(signal.ITIMER_REAL, remaining)
    return deadline


def _native_tick(engine):
    """Consume the typed queue only; audit-directory scans never select work."""
    from server.engine.native_research_jobs import claim, CLAIM_SECONDS
    from server.engine.native_research_prepare import PrepareYield
    from server.common.formal_analysis_prefetch import CheckpointPrefetchDeferred
    from server.engine.native_research_dataset import PreparedDataset,PreparedInputMissing
    state=None;previous_handler=None;attempt_started=time.monotonic()
    with engine.connect() as connection:
        try:
            def timeout(_signum,_frame):raise NativeStepDeadline('NATIVE_ATTEMPT_DEADLINE')
            if hasattr(signal,'SIGALRM'):
                previous_handler=signal.signal(signal.SIGALRM,timeout)
                signal.setitimer(signal.ITIMER_REAL,max(.001,75-(time.monotonic()-attempt_started)))
            if connection.dialect.name=='mysql':connection.execute(text('SET SESSION innodb_lock_wait_timeout=5'))
            if connection.execute(text('SELECT GET_LOCK(:name,0)'),{'name':LOCK}).scalar()!=1:
                return {'status':'RUNNING','reason':'已有原生回测持有互斥锁','business_started':False}
            claimed=claim(connection,deadline=attempt_started+CLAIM_SECONDS)
            if claimed is None:return {'status':'idle'}
            state,owner=claimed
            if str(now_shanghai())>state['deadline_at']:raise TimeoutError('原生实验超过24小时执行时限')
            from server.engine.native_research_identity import algorithm_identity
            if algorithm_identity(state['config'])['sha256']!=state['contract']:
                raise ValueError('冻结实验算法在当前执行器不可用，禁止替换版本继续')
            from server.engine.native_research_identity import producer_identity
            if state['phase'] in ('PLAN','PREPARE_BARS','PREPARE_DAYS') and producer_identity(state['strategies'][0]['source_kind'])!=state['producer_identity']:
                raise ValueError('冻结数据转换版本已变化；未完成输入须按新转换身份登记，不能混用或错标版本')
            if previous_handler is not None:
                signal.setitimer(signal.ITIMER_REAL,max(.001,300-(time.monotonic()-attempt_started)))
            _step(connection,state,owner,prepare_budget=lambda: _atomic_prepare_deadline(
                attempt_started,arm_timer=previous_handler is not None))
            if state['phase']!='FINISHED':_save(connection,state,owner,yield_job=True)
            return {key:state.get(key) for key in ('status','job_id','run_id','phase','completed_units','total_units','current_unit','reason')}
        except (Exception,NativeStepDeadline,PrepareYield) as exc:
            if previous_handler is not None:signal.setitimer(signal.ITIMER_REAL,0)
            if state is None:return {'status':'FAILED','reason':f'{type(exc).__name__}: {exc}'}
            connection.rollback();_restore_failure_lock(connection)
            state.update(reason=_failure_reason(state,exc))
            if isinstance(exc,(PrepareYield,PreparedInputMissing,CheckpointPrefetchDeferred)):
                state['status']='RUNNING'
                _save(connection,state,owner,yield_job=True)
            else:
                key=state.get('current_unit',state['phase'])
                retries=state.setdefault('source_error_retries',{})
                if _transient_source_error(exc) and retries.get(key,0)<1:
                    retries[key]=retries.get(key,0)+1
                    state.update(status='RUNNING',retry_after=str(now_shanghai()+timedelta(minutes=1)))
                    _save(connection,state,owner,yield_job=True)
                else:
                    state['status']='BLOCKED' if isinstance(exc,DailyPriceSourceUnavailable) else 'FAILED'
                    for proposal in state['proposals']:
                        if proposal['strategy_key'] not in state['reports']:proposal['validation'].update(status=state['status'],reason=state['reason'])
                    _save(connection,state,owner)
            return {'status':state['status'],'job_id':state['job_id'],'run_id':state['run_id'],
                'phase':state['phase'],'reason':state['reason'],'completed_units':state['completed_units'],'total_units':state['total_units']}
        finally:
            if state is not None and state.get('owner') is None and state.get('status') in ('COMPLETE','BLOCKED','FAILED'):
                try:PreparedDataset().release(state['run_id'])
                except OSError:logger.warning('Native terminal input pin cleanup failed: %s',state['run_id'],exc_info=True)
            if previous_handler is not None:signal.setitimer(signal.ITIMER_REAL,0);signal.signal(signal.SIGALRM,previous_handler)
            connection.rollback()
            if not connection.invalidated:connection.execute(text('SELECT RELEASE_LOCK(:name)'),{'name':LOCK});connection.commit()
