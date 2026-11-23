"""Scheduler-owned native strategy research with durable source/date checkpoints."""
from datetime import date, datetime, timedelta
import hashlib
import json
from pathlib import Path
import signal
import time

import pandas as pd
from sqlalchemy import text

from server.common.analysis_pool_receipt import canonical_sha256
from server.engine.autonomous_strategy_research import _owner, _resume_allowed, now_shanghai
from server.engine.strategy_rule_backtest import _partitions, partition_key, PART_ENTITY
from server.engine.strategy_price_backtest import _group_inputs, _validate_group, _read_bars, _input_parts, _identity_rows, _resolved_scopes

ENTITY = 'NATIVE_REPLAY'
SCHEMA = 'probiga.native-strategy-research.v1'
LOCK = 'probiga:native-strategy-research'


class NativeStepDeadline(BaseException):
    """Source adapters must not convert the job deadline into a data gap."""


def _assert_read_succeeded(reason):
    if any(token in reason for token in ('OperationalError','Timeout','SQLAlchemyError',
            'REPLY_FAILED','REPLY_DEADLINE','VALIDATION_DEADLINE','SCHEMA_UNAVAILABLE','FormalAnalysis')):
        raise RuntimeError('历史事实读取失败：'+reason)


def read_runs(connection, run_id=None):
    from server.engine.strategy_governance import _digest
    # Find recent journal heads through the covering entity/time index before reading bodies.
    scope = " AND entity_key=:uid" if run_id is not None else ""
    heads = connection.execute(text(f"""SELECT /*+ MAX_EXECUTION_TIME(4000) */
        entity_key,MAX(created_at) AS latest_at FROM st_strategy_governance_audit
        WHERE entity_type=:entity{scope} GROUP BY entity_type,entity_key
        ORDER BY latest_at DESC,entity_key DESC LIMIT 8"""), {'entity':ENTITY,'uid':run_id}).mappings().all()
    records=[]
    for head in heads:
        records.extend(connection.execute(text("""SELECT /*+ MAX_EXECUTION_TIME(4000) */ payload_json,audit_hash,after_json
            FROM st_strategy_governance_audit WHERE entity_type=:entity AND entity_key=:uid
            AND created_at=:latest_at ORDER BY action DESC,audit_id DESC LIMIT 1"""),
            {'entity':ENTITY,'uid':head['entity_key'],'latest_at':head['latest_at']}).mappings().all())
    result=[]
    for r in records:
        payload,after=json.loads(r['payload_json']),json.loads(r['after_json'])
        if _digest(payload)!=r['audit_hash'] or payload['after']!=after or after['schema']!=SCHEMA:
            raise ValueError('native research audit integrity differs')
        result.append(after)
    return result


def _contract():
    from server.trading_v3 import daily_features,sleeves,engine,regime,config
    from server.engine import strategy_native_replay,shadow_trial_policy,strategy_native_fact_snapshot
    from server.common import historical_announcement_read,pit_sealed_event_read
    paths=[Path(__file__),*(Path(m.__file__) for m in (daily_features,sleeves,engine,regime,strategy_native_replay,shadow_trial_policy,historical_announcement_read,strategy_native_fact_snapshot,pit_sealed_event_read))]
    root=Path(__file__).resolve().parents[2]
    paths.extend(root/p for p in ('server/engine/strategy_shadow_trials.py','server/engine/strategy_center.py',
        'server/common/pit_facts.py','server/common/versioned_strategy_config.py',
        'server/trading_v2/instrument_registry.py','server/trading_v3/backtest.py','server/engine/strategy_rule_replay.py'))
    return canonical_sha256({'code':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in paths},'config':config.config_hash()})


def _strategies(c):
    rows=c.execute(text("""SELECT r.strategy_key,r.strategy_name,r.current_version,v.version_hash,
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
    identity=json.loads(json.dumps({'published':published,'membership':members,'seals':seals},default=str))
    return days,published,canonical_sha256(identity)


def _save(c,state,owner,events=()):
    from server.engine.strategy_governance import _append_audit_connection
    c.commit()
    if _owner(c,locking=True)!=owner or c.execute(text('SELECT IS_USED_LOCK(:name)=CONNECTION_ID()'),{'name':LOCK}).scalar()!=1:
        raise ValueError('native research owner lease lost')
    state.update(owner=owner,updated_at=str(now_shanghai()),checkpoint_sequence=state.get('checkpoint_sequence',0)+1)
    for event in events:
        _append_audit_connection(c,**event,reason='原生历史研究分区已处理',operator='native-strategy-research',before=None,evidence={'run_id':state['run_id'],'owner':owner})
    _append_audit_connection(c,entity_type=ENTITY,entity_key=state['run_id'],action=f"CHECKPOINT_{state['checkpoint_sequence']:08d}",
        operator='native-strategy-research',reason=state.get('reason') or state['phase'],before=None,after=state,evidence={'owner':owner,'real_order_authority':False})
    c.commit()


def _restore_failure_lock(connection):
    # Interrupted driver reads can invalidate the connection and release its session lock.
    if connection.execute(text('SELECT IS_USED_LOCK(:name)=CONNECTION_ID()'),{'name':LOCK}).scalar()!=1:
        if connection.execute(text('SELECT GET_LOCK(:name,0)'),{'name':LOCK}).scalar()!=1:
            raise RuntimeError('原生回测失败回执无法恢复互斥锁')


def _part(state,kind,day,value):
    return {'entity_type':PART_ENTITY,'entity_key':partition_key(state,state,kind,day),
        'action':'PARTITION_COMPLETE','after':{'day':day,**value}}


def _ensure_inputs(c,state,days,owner):
    inputs=_input_parts(c,state)
    present={d for part in inputs.values() for d in part['sessions']}
    missing=sorted(set(days)-present)
    for group in _group_inputs(c,missing,state['price_cutoff']) if missing else []:
        value=_validate_group(c,group,state['price_cutoff'])
        _save(c,state,owner,[_part(state,'D',group['sessions'][0],value)])
    return _input_parts(c,state)


def _window(c,state,days,inputs):
    proofs={d:r for p in inputs.values() for d,r in p['proofs'].items()}
    scopes=_resolved_scopes(c,inputs,days,state['price_cutoff'])
    return {'schema':'probiga.strategy-daily-input-window.v1','sessions':days,'session_count':len(days),
        'session_set_sha256':canonical_sha256(days),'daily_truth_sha256':canonical_sha256([proofs[d] for d in days]),
        'daily_attested_row_count':sum(scopes[d]['stock_count'] for d in days),
        'catalog_batches_by_session':{d:scopes[d]['catalog'] for d in days},
        'daily_providers_by_session':{d:proofs[d].get('provider','qmt') for d in days},
        'daily_partition_roots':{d:canonical_sha256(proofs[d]) for d in days},
        'latest_daily_truth':proofs[days[-1]],'decision_known_at':state['price_cutoff']}


def _v3_day(c,state,day,owner):
    from server.common.strategy_daily_input_window import V3_DAILY_INPUT_SESSIONS
    from server.common.pit_facts import resolve_strategy_fact_cutoffs,PIT_AVAILABLE
    from server.common.historical_announcement_read import has_dated_announcement_candidate
    from server.trading_v3.daily_features import normalize_daily_bars,build_daily_feature_universe,DailyFactInputs
    from server.trading_v3.regime import classify_regime_probabilities
    from server.trading_v3.engine import TradingV3Engine
    from server.trading_v3.config import load_v3_config
    from server.engine.strategy_shadow_trials import _v3_signal
    identity,identity_proof=_identity_rows(c,day)
    if identity_proof['status']!='VERIFIED':
        return {'status':'BLOCKED','reason':'缺少精确日期证券身份和行业快照','identity':identity_proof,'signals':{}}
    known=datetime.fromisoformat(state['requested_at'])
    target=date.fromisoformat(day)
    from server.common.pit_facts import load_latest_finance_atomic_batch_baseline
    finance_observation=min(known,datetime.combine(target,datetime.max.time()))
    baseline=load_latest_finance_atomic_batch_baseline(c.engine,decision_at=finance_observation)
    if baseline.get('as_of_date')!=day:
        return {'status':'BLOCKED','reason':'PIT_HISTORICAL_FINANCE_BATCH_NOT_FOUND',
            'identity':identity_proof,'finance_observation_at':str(finance_observation),
            'latest_finance_date':baseline.get('as_of_date'),'signals':{}}
    eligible=(set(identity)&set(baseline.get('members',{})))-set(baseline.get('data_excluded_codes',[]))
    excluded=sorted(set(identity)-eligible)
    if not eligible:
        return {'status':'BLOCKED','reason':'历史证券集合没有可验证的财务封存覆盖','identity':identity_proof,'signals':{}}
    if not has_dated_announcement_candidate(c.engine,code=min(eligible),decision_at=known,target=target,window_start=target-timedelta(days=20)):
        return {'status':'BLOCKED','reason':'QMT_ANNOUNCEMENT_COMPLETE_BATCH_NOT_FOUND','identity':identity_proof,'signals':{}}
    from server.engine.strategy_native_fact_snapshot import snapshot_identity,load_snapshot,save_snapshot
    generation=snapshot_identity(day=day,decision=known,identity_hash=identity_proof['sha256'],
        finance_seal=baseline['seal_coverage_id'],codes=eligible)
    fact_inputs=load_snapshot(c.engine,generation)
    from server.common.formal_analysis_store import FormalAnalysisStore
    finances,events=[],[]
    progress_at=time.monotonic()
    def progress(done,total,kind='event'):
        nonlocal progress_at
        if done and (time.monotonic()-progress_at>=5 or done==total):
            state['source_progress']={'trade_date':day,'dataset':kind,'completed':done,'total':total,'last_progress_at':str(now_shanghai())}
            if kind=='finance':
                prior=state.get('fact_read_checkpoint') or {}
                if prior.get('trade_date')!=day or done>prior.get('completed',0):
                    state['fact_read_checkpoint']=dict(state['source_progress'])
            label='财务核对' if kind=='finance' else '公告批次核对'
            for proposal in state['proposals']:
                proposal['validation'].update(status='RUNNING',reason=f'{label} {day}：已处理 {done}/{total} 只')
            state['last_progress_at']=str(now_shanghai());_save(c,state,owner);progress_at=time.monotonic()
    def coverage_progress(done,total,scope,fresh):
        if not fresh:return
        state['coverage_read_checkpoint']={'trade_date':day,'dataset':'event_coverage','completed':done,
            'total':total,'scope':scope,'last_progress_at':str(now_shanghai())}
        state['source_progress']=dict(state['coverage_read_checkpoint'])
        for proposal in state['proposals']:
            proposal['validation'].update(status='RUNNING',reason=f'公告来源核对 {day}：已保存 {done}/{total} 个成员')
        state['last_progress_at']=str(now_shanghai());_save(c,state,owner)
    def finance_coverage_progress(done,total,scope,fresh):
        if not fresh:return
        state['finance_source_read_checkpoint']={'trade_date':day,'dataset':'finance_coverage','completed':done,
            'total':total,'scope':scope,'last_progress_at':str(now_shanghai())}
        state['source_progress']=dict(state['finance_source_read_checkpoint'])
        for proposal in state['proposals']:
            proposal['validation'].update(status='RUNNING',reason=f'财务来源核对 {day}：已保存 {done}/{total} 个成员')
        state['last_progress_at']=str(now_shanghai());_save(c,state,owner)
    def metadata_progress(done,total,scope,fresh):
        nonlocal progress_at
        if not fresh:return
        observation={'trade_date':day,'dataset':'event_chain_metadata','completed':done,'total':total,
            'scope':scope,'last_progress_at':str(now_shanghai())}
        state['event_metadata_read_checkpoint']=observation
        state['source_progress']=dict(observation)
        for proposal in state['proposals']:
            proposal['validation'].update(status='RUNNING',reason=f'公告链元数据 {day}：已保存 {done}/{total} 条')
        state['last_progress_at']=str(now_shanghai())
        if time.monotonic()-progress_at>=5 or done==total:
            _save(c,state,owner);progress_at=time.monotonic()
    if fact_inputs is None:
        cutoff=resolve_strategy_fact_cutoffs(c.engine,codes=eligible,decision_at=known,finance_start_date='1900-01-01',
            finance_end_date=day,event_start_date=target-timedelta(days=20),event_end_date=day,require_qmt_event_batch=True,
            event_trade_date=day,capture_finance_selection=finances.append,capture_historical_selection=events.append,
            finance_observation_at=finance_observation,
            event_progress=progress,
            finance_coverage_store=FormalAnalysisStore(c.engine),finance_coverage_progress=finance_coverage_progress,
            event_coverage_store=FormalAnalysisStore(c.engine),event_coverage_progress=coverage_progress,
            event_metadata_progress=metadata_progress)
    else:
        cutoff=fact_inputs.cutoff
        finances,events=[fact_inputs.finance],[fact_inputs.events]
    if cutoff['status']!=PIT_AVAILABLE:
        reason=cutoff['reason']
        _assert_read_succeeded(reason)
        return {'status':'BLOCKED','reason':reason,'identity':identity_proof,'signals':{}}
    source_clocks={kind:datetime.fromisoformat(value) for kind,value in cutoff['source_cutoff_at'].items()}
    fact_clock=max(source_clocks.values())
    if (any(clock.date()!=target for clock in source_clocks.values())
            or source_clocks['event']<datetime.fromisoformat(day+' 15:35:00')):
        return {'status':'BLOCKED','reason':'历史公告截止时间不能覆盖当日收盘或财务批次日期错误',
            'source_cutoff_at':cutoff['source_cutoff_at'],'signals':{}}
    if len(finances)!=1 or len(events)!=1:raise ValueError('历史事实快照未完整移交')
    signal_clock=max(fact_clock,datetime.fromisoformat(str(identity_proof['captured_at'])),
        datetime.fromisoformat(str(events[0].selection.batch.get('received_at') or fact_clock)))
    if fact_inputs is None:
        fact_inputs=DailyFactInputs(cutoff,finances[0],events[0])
        save_snapshot(c.engine,generation,fact_inputs)
    if (fact_inputs.finance.decision_at!=known or fact_inputs.finance.as_of_date!=target
            or fact_inputs.finance.fact_cutoff_at!=source_clocks['finance'] or set(events[0].selection.members)!=eligible):
        raise ValueError('native cached historical fact scope differs')
    from server.common.pit_sealed_finance_read import load_sealed_finance_facts
    financial=load_sealed_finance_facts(c.engine,selection=finances[0],codes=eligible,decision_at=known,
        fact_cutoff_at=source_clocks['finance'],as_of_date=target,formal_store=FormalAnalysisStore(c.engine),
        formal_generation_sha256=generation,read_progress=lambda done,total:progress(done,total,'finance'))
    from server.common.pit_sealed_event_read import load_sealed_event_facts
    def event_progress(done,total,scope,fresh):
        nonlocal progress_at
        if fresh or time.monotonic()-progress_at>=5 or done==total:
            observation={'trade_date':day,'dataset':'event_facts','completed':done,'total':total,
                'scope':scope,'last_progress_at':str(now_shanghai())}
            state['source_progress']=observation
            if fresh:state['event_read_checkpoint']=dict(observation)
            for proposal in state['proposals']:
                proposal['validation'].update(status='RUNNING',reason=f'公告事实核对 {day}：已处理 {done}/{total} 只')
            state['last_progress_at']=str(now_shanghai());_save(c,state,owner);progress_at=time.monotonic()
    event_facts=load_sealed_event_facts(c.engine,selection=events[0],codes=eligible,
        formal_store=FormalAnalysisStore(c.engine),read_progress=event_progress)
    days=sorted(str(d)[:10] for d in c.execute(text('SELECT DISTINCT trade_date FROM si_trade_calendar WHERE trade_status=1 AND trade_date<=:d ORDER BY trade_date DESC LIMIT :n'),{'d':day,'n':V3_DAILY_INPUT_SESSIONS}).scalars())
    inputs=_ensure_inputs(c,state,days,owner)
    raw=_read_bars(c,days,inputs,state['price_cutoff'])
    frame=pd.DataFrame(raw);del raw
    frame['trade_date']=pd.to_datetime(frame.trade_date).dt.date
    frame['short_name']=[identity.get(code,{}).get('short_name','') if str(d)==day else '' for code,d in zip(frame.stock_code,frame.trade_date)]
    frame.attrs['qmt_daily_input_window']=_window(c,state,days,inputs)
    frame=normalize_daily_bars(frame)
    dataset=build_daily_feature_universe(c.engine,as_of=target,dates=[date.fromisoformat(d) for d in days],frame=frame,
        context_cutoff_at=known,limit=len(eligible),allow_research_industry_last_known=False,entry_scope_codes=eligible,
        fact_inputs=DailyFactInputs(cutoff,finances[0],events[0],financial,event_facts))
    _assert_read_succeeded(str(dataset['market_features'].get('pit_common_cutoff_reason') or ''))
    for stock in dataset['stocks']:
        _assert_read_succeeded(str(stock.get('finance_pit_reason') or '')+';'+str(stock.get('event_pit_reason') or ''))
    market=classify_regime_probabilities(dataset['market_features'])
    if market.quality_status!='PASS':
        return {'status':'BLOCKED','reason':'市场状态输入不足：'+';'.join(market.evidence),'signals':{},'data_snapshot_hash':dataset['data_snapshot_hash']}
    config=load_v3_config()
    strategies=[s for s in state['strategies'] if s['source_kind']=='immutable_v3_sleeve']
    if any(s['evaluator_config']['model_version']!=config['strategy_version'] for s in strategies):
        raise ValueError('native current V3 model identity differs')
    native=TradingV3Engine(calibrations={})
    selected={s['strategy_key']:[] for s in strategies}
    scored={s['strategy_key']:0 for s in strategies}
    missing={s['strategy_key']:0 for s in strategies}
    for stock in dataset['stocks']:
        forecasts,_=native.evaluate_stock_with_theme_signals(stock['stock_code'],stock['stock_name'],stock,dataset['feature_time'],dataset['feature_time']+timedelta(days=30))
        by_key={f.strategy_key:f for f in forecasts}
        for strategy in strategies:
            key=strategy['strategy_key'];f=by_key.get(key)
            if f is None or f.status=='INSUFFICIENT_DATA':
                if not (key=='event_drift' and stock.get('event_authoritative_empty')):missing[key]+=1
                continue
            scored[key]+=1
            forecast={**f.as_dict(),'forecast_status':f.status,'features_json':json.dumps(f.features,default=str),'short_name':f.stock_name}
            candidate=_v3_signal(strategy,forecast,day)
            weights=strategy['evaluator_config']['shadow_trial_routing']['regime_weights']
            multiplier=sum(market.probabilities[k]*weights[k] for k in weights)
            if candidate is None or multiplier<=0 or market.risk_asset_cap<=0:continue
            selected[key].append({**candidate,'industry':stock.get('industry_name'),
                'signal_known_at':str(signal_clock),
                'source_snapshot_hash':dataset['data_snapshot_hash'],'historical_production_decision':False})
    for key,rows in selected.items():
        rows.sort(key=lambda r:(-r['effective_score'],r['stock_code']))
        for rank,row in enumerate(rows):row['rank']=rank+1
    return {'status':'COMPLETE','signals':selected,'scored_stocks':scored,'missing_feature_stocks':missing,
        'finance_scope_exclusions':excluded,'finance_seal_coverage_id':baseline['seal_coverage_id'],
        'universe_identity_exclusion_count':max(0,frame.attrs['qmt_daily_input_window']['latest_daily_truth']['attested_row_count']-len(identity)),
        'data_snapshot_hash':dataset['data_snapshot_hash'],'fact_cutoff':str(fact_clock),
        'source_cutoff_at':cutoff['source_cutoff_at'],'identity':identity_proof,
        'interpretation':'CURRENT_FROZEN_RULES_WITHOUT_FUTURE_CALIBRATIONS'}


def _v2_day(c,state,day):
    from server.common.daily_delivery_control import load_published_analysis_receipt
    from server.common.analysis_pool_receipt import read_score_snapshot_header,decode_score_snapshot
    from server.engine.strategy_center import _fixed_market_snapshot
    from server.engine.strategy_shadow_trials import _manifest_signal
    uid=state['published'].get(day)
    if not uid:return {'status':'BLOCKED','reason':'缺少完整冻结评分及市场上下文批次','signals':{}}
    receipt=load_published_analysis_receipt(c,day,run_uid=uid,decision_at=datetime.fromisoformat(state['requested_at']))
    header=read_score_snapshot_header(receipt['score_snapshot'],trade_date=day,engine=c)
    decision=datetime.fromisoformat(header['decision_at'])
    if decision.date()!=date.fromisoformat(day):
        return {'status':'BLOCKED','reason':'冻结评分实际可用时间晚于目标历史交易日','source_run_uid':uid,'actual_decision_at':str(decision),'signals':{}}
    snapshot=decode_score_snapshot(receipt['score_snapshot'],trade_date=day,engine=c)
    if any(s['evaluator_config']['market_state_config_hash']!=snapshot['fixed_market_context']['market_state_config_hash']
           for s in state['strategies'] if s['source_kind']=='immutable_manifest'):
        raise ValueError('冻结市场状态配置与原生策略版本不一致')
    market=_fixed_market_snapshot(day,snapshot['fixed_market_context'],decision)
    identities,proof=_identity_rows(c,day)
    selected={}
    for strategy in (s for s in state['strategies'] if s['source_kind']=='immutable_manifest'):
        key=strategy['strategy_key'];picked=[]
        for row in snapshot['scored_rows']:
            if row.get(strategy['evaluator_config']['score_field']) is not None and row.get('model_version')!=strategy['evaluator_config']['model_version']:
                raise ValueError('冻结评分模型版本与原生策略不一致')
            candidate=_manifest_signal(strategy,row,target=day,market=market)
            if candidate['signal_status']=='READY':
                member=identities.get(candidate['stock_code'],{})
                picked.append({**candidate,'stock_name':member.get('short_name',''),'industry':member.get('industry_name'),
                    'signal_known_at':str(max(decision,datetime.fromisoformat(str(receipt['published_at'])))), 'source_run_uid':uid})
        picked.sort(key=lambda r:(-r['effective_score'],r['stock_code']))
        for rank,row in enumerate(picked):row['rank']=rank+1
        selected[key]=picked
    return {'status':'COMPLETE','signals':selected,'source_run_uid':uid,'source_sha256':receipt['score_snapshot_sha256'],'identity':proof}


def _start(c,strategies,days,published,source_hash,contract):
    from server.engine.autonomous_strategy_research import _read_states
    prior=next((r.get('price_backtest') for r in _read_states(c) if (r.get('price_backtest') or {}).get('sessions')==days),None)
    warmup=sorted(str(d)[:10] for d in c.execute(text('SELECT DISTINCT trade_date FROM si_trade_calendar WHERE trade_status=1 AND trade_date<:d ORDER BY trade_date DESC LIMIT 20'),{'d':days[0]}).scalars())
    clock=str(now_shanghai());versions=[(s['strategy_key'],s['current_version'],s['version_hash']) for s in strategies]
    frozen=next((r for r in read_runs(c) if r['source_hash']==source_hash and r['sessions']==days
        and [(s['strategy_key'],s['current_version'],s['version_hash']) for s in r['strategies']]==versions),None)
    # Retain input availability clocks and archives; a new execution has its own bounded budget.
    requested=frozen['requested_at'] if frozen else clock
    archives=(list(frozen['input_archives'])+[{'run_id':frozen['run_id'],'rule_hash':frozen['rule_hash']}]
        if frozen else list(prior.get('input_archives',[]))+[{'run_id':prior['run_id'],'rule_hash':prior['rule_hash']}] if prior else [])
    uid=canonical_sha256({'versions':versions,'days':days,'source_hash':source_hash,'contract':contract})
    state={'schema':SCHEMA,'run_id':uid,'trade_date':days[-1],'strategies':strategies,'published':published,
        'sessions':days,'input_sessions':warmup+days,'price_cutoff':frozen['price_cutoff'] if frozen else prior['knowledge_cutoff'] if prior else requested,
        'requested_at':requested,'deadline_at':str(now_shanghai()+timedelta(hours=24)),
        'input_archives':archives,
        'contract':contract,'source_hash':source_hash,'rule_hash':canonical_sha256(versions),'phase':'INPUTS','status':'NOT_STARTED',
        'cursor':0,'completed_units':0,'total_units':(len(warmup)+len(days))*2+len(days)+len(strategies),
        'last_progress_at':clock,'reason':'','proposals':[],
        'fact_read_checkpoint':dict(frozen.get('fact_read_checkpoint') or {}) if frozen else {},
        'finance_source_read_checkpoint':dict(frozen.get('finance_source_read_checkpoint') or {}) if frozen else {},
        'coverage_read_checkpoint':dict(frozen.get('coverage_read_checkpoint') or {}) if frozen else {},
        'event_metadata_read_checkpoint':dict(frozen.get('event_metadata_read_checkpoint') or {}) if frozen else {},
        'event_read_checkpoint':dict(frozen.get('event_read_checkpoint') or {}) if frozen else {}}
    inputs=_input_parts(c,state)
    present={d for p in inputs.values() for d in p['sessions']}
    state['completed_units']=sum(d in present for d in state['input_sessions'])
    state['reused_input_sessions']=state['completed_units']
    for s in strategies:
        state['proposals'].append({'strategy_key':s['strategy_key'],'version':s['current_version'],'rule_hash':s['version_hash'],
            'name':s['strategy_name'],'validation':{'status':'NOT_STARTED','reason':'原生历史回测已登记，等待来源检查'}})
    return state


def _step(c,state,owner):
    state.update(status='RUNNING',step_status='RUNNING',reason='')
    _save(c,state,owner)
    days=state['sessions'];inputs=_input_parts(c,state)
    if state['phase']=='INPUTS':
        present={d for p in inputs.values() for d in p['sessions']}
        missing=[d for d in state['input_sessions'] if d not in present]
        _ensure_inputs(c,state,missing[:16],owner)
        state['completed_units']+=min(16,len(missing))
        if len(missing)<=16:state.update(phase='SIGNALS',cursor=0)
    elif state['phase']=='SIGNALS':
        deadline=time.monotonic()+75
        for _ in range(32):
            if state['cursor']>=len(days) or time.monotonic()>deadline:break
            day=days[state['cursor']];state['current_unit']='原生历史选股 '+day;_save(c,state,owner)
            value=(_v2_day(c,state,day) if state['strategies'][0]['source_kind']=='immutable_manifest'
                   else _v3_day(c,state,day,owner))
            state['cursor']+=1;state['completed_units']+=1;state['last_progress_at']=str(now_shanghai())
            _save(c,state,owner,[_part(state,'S',day,value)])
        if state['cursor']==len(days):state.update(phase='PRICES',cursor=0)
    elif state['phase']=='PRICES':
        parts=_partitions(c,state,state,'S')
        codes={s['stock_code'] for p in parts.values() for picks in p['signals'].values() for s in picks}
        chunk=state['input_sessions'][state['cursor']:state['cursor']+24]
        raw=_read_bars(c,chunk,inputs,state['price_cutoff']) if codes else []
        events=[_part(state,'P',d,{'bars':[r for r in raw if str(r['trade_date'])[:10]==d and r['stock_code'] in codes]}) for d in chunk]
        state['cursor']+=len(chunk);state['completed_units']+=len(chunk);_save(c,state,owner,events)
        if state['cursor']==len(state['input_sessions']):state.update(phase='REPLAY',cursor=0)
    elif state['phase']=='REPLAY':
        from server.engine.strategy_native_replay import replay_native
        parts=_partitions(c,state,state,'S');prices=_partitions(c,state,state,'P')
        source_hash=canonical_sha256({'prices':inputs,'sources':parts,'contract':state['contract']})
        bars=[b for p in prices.values() for b in p['bars']]
        for strategy,proposal in zip(state['strategies'],state['proposals']):
            family=strategy['source_kind'];key=strategy['strategy_key']
            gaps=[{'trade_date':d,'reason':parts[d]['reason']} for d in days if parts[d]['status']=='BLOCKED']
            gaps.extend({'trade_date':d,'reason':f"部分证券历史身份或策略特征不足：身份 {parts[d].get('universe_identity_exclusion_count',0)}，财务覆盖 {len(parts[d].get('finance_scope_exclusions',[]))}，特征 {parts[d].get('missing_feature_stocks',{}).get(key,0)}"}
                for d in days if parts[d]['status']=='COMPLETE' and (parts[d].get('universe_identity_exclusion_count',0) or parts[d].get('finance_scope_exclusions') or parts[d].get('missing_feature_stocks',{}).get(key,0)))
            selected=[s for p in parts.values() for s in p['signals'].get(key,[])]
            report=replay_native(strategy,selected,bars,state['input_sessions'],evaluation_start=days[0],source_gaps=gaps)
            available=sum(parts[d]['status']=='COMPLETE' for d in days)
            if not available:report.update(status='BLOCKED',net_return_pct=None,verified_input_subset_return_pct=None,nav=[])
            proposal['validation']={'status':report['status'],'report':report,'sessions':days,'source_gaps':gaps,
                'replay_run_id':state['run_id'],'completed_units':state['total_units'],'total_units':state['total_units'],
                'available_feature_sessions':available,'required_sessions':len(days),
                'missing_feature_dates':[d for d in days if parts[d]['status']=='BLOCKED'],
                'identity_covered_sessions':sum(parts[d].get('identity',{}).get('status')=='VERIFIED' for d in days),
                'source_input_sha256':canonical_sha256({'source':source_hash,'family':family}),
                'reason':f"原生冻结影子规则已处理；历史输入可用 {available}/{len(days)} 日；"+(gaps[0]['reason'] if gaps else '详见成交、费用和净值'),
                'funding_eligible':False}
            state['completed_units']+=1
        state.update(phase='FINISHED',status='BLOCKED' if any(p['validation']['status'] in ('BLOCKED','PARTIAL') for p in state['proposals']) else 'COMPLETE')
    if state['phase']!='FINISHED':
        state['status']='NOT_STARTED'
        for p in state['proposals']:
            p['validation'].update(status='RUNNING',completed_units=state['completed_units'],total_units=state['total_units'],
                replay_run_id=state['run_id'],reason=state.get('current_unit') or state['phase'])
    state.update(step_status='COMPLETE',last_progress_at=str(now_shanghai()))
    _save(c,state,owner)


def native_tick(engine):
    """One bounded step on the existing Linux worker, fenced by its renewable lease."""
    from server.common.authoritative_market_clock import authoritative_closed_trade_date
    state=None;checkpoint_before={};event_before={};coverage_before={};metadata_before={}
    with engine.connect() as c:
        if c.execute(text('SELECT GET_LOCK(:name,0)'),{'name':LOCK}).scalar()!=1:return {'status':'RUNNING','reason':'已有原生回测持有互斥锁'}
        previous_handler=None
        try:
            owner=_owner(c);runs=read_runs(c)
            pending=[r for r in reversed(runs) if r['status'] in ('RUNNING','NOT_STARTED')]
            contract=_contract()
            for previous in list(pending):
                if previous['contract']==contract:continue
                if not _resume_allowed(c,previous,owner):
                    return {'status':'BLOCKED','reason':'计算规则已更新，等待旧运行所有者退出','run_id':previous['run_id']}
                previous.update(status='BLOCKED',step_status='BLOCKED',reason='计算契约已变化，原检查点保留；使用同源行情建立当前契约研究')
                for p in previous['proposals']:
                    p['validation'].update(status='BLOCKED',reason=previous['reason'])
                _save(c,previous,owner)
                pending.remove(previous)
            state=next((r for r in pending if r.get('retry_after','')<=str(now_shanghai())),None)
            if pending and state is None:return {'status':'BLOCKED','reason':'等待有界数据库重试时间','run_id':pending[0]['run_id']}
            if state is None:
                day=authoritative_closed_trade_date(c,now=now_shanghai())
                strategies=_strategies(c);days,published,source_hash=_inventory(c,day);contract=_contract()
                latest=[]
                for family in ('immutable_manifest','immutable_v3_sleeve'):
                    group=[s for s in strategies if s['source_kind']==family]
                    if not group:continue
                    existing=next((r for r in runs if r['contract']==contract and r['source_hash']==source_hash
                        and r['sessions']==days and [(s['strategy_key'],s['version_hash']) for s in r['strategies']]
                        ==[(s['strategy_key'],s['version_hash']) for s in group]),None)
                    if existing is not None:
                        latest.append(existing)
                        continue
                    candidate=_start(c,group,days,published,source_hash,contract)
                    existing=next((r for r in runs if r['run_id']==candidate['run_id']),None)
                    if existing is None:
                        _save(c,candidate,owner)
                        if state is None:state=candidate
                    else:latest.append(existing)
                if state is None:
                    return {'status':'BLOCKED' if any(r['status']!='COMPLETE' for r in latest) else 'COMPLETE',
                            'run_ids':[r['run_id'] for r in latest],'reused':True}
            if not _resume_allowed(c,state,owner):return {'status':'BLOCKED','reason':'原生回测旧所有者尚未退出','run_id':state['run_id']}
            if state.get('retry_after','')>str(now_shanghai()):
                return {'status':'BLOCKED','run_id':state['run_id'],'reason':state['reason'],'retry_after':state['retry_after']}
            if str(now_shanghai())>state['deadline_at']:raise TimeoutError('原生回测超过24小时总时限')
            checkpoint_before=dict(state.get('fact_read_checkpoint') or {})
            event_before=dict(state.get('event_read_checkpoint') or {})
            coverage_before=dict(state.get('coverage_read_checkpoint') or {})
            finance_source_before=dict(state.get('finance_source_read_checkpoint') or {})
            metadata_before=dict(state.get('event_metadata_read_checkpoint') or {})
            def timeout(_signum,_frame):raise NativeStepDeadline('原生回测单步超过600秒')
            previous_handler=signal.signal(signal.SIGALRM,timeout);signal.setitimer(signal.ITIMER_REAL,600)
            _step(c,state,owner)
            return {k:state.get(k) for k in ('status','run_id','phase','completed_units','total_units','reason')}
        except (Exception,NativeStepDeadline) as ex:
            if state is not None:
                if previous_handler is not None:signal.setitimer(signal.ITIMER_REAL,0)
                c.rollback()
                _restore_failure_lock(c)
                state.update(status='FAILED',step_status='FAILED',reason=f"{state.get('current_unit',state['phase'])}: {type(ex).__name__}: {ex}")
                saved=state.get('fact_read_checkpoint') or {}
                earlier=checkpoint_before.get('completed',0) if checkpoint_before.get('trade_date')==saved.get('trade_date') else 0
                event_saved=state.get('event_read_checkpoint') or {}
                event_earlier=event_before.get('completed',0) if (event_before.get('trade_date'),event_before.get('scope'))==(event_saved.get('trade_date'),event_saved.get('scope')) else 0
                finance_advanced=(saved.get('completed',0)>earlier
                    and (now_shanghai()-datetime.fromisoformat(saved['last_progress_at'])).total_seconds()<=120)
                # New event chunks remain usable when later feature calculation exhausts the step.
                event_advanced=event_saved.get('completed',0)>event_earlier
                coverage_saved=state.get('coverage_read_checkpoint') or {}
                coverage_earlier=coverage_before.get('completed',0) if (coverage_before.get('trade_date'),coverage_before.get('scope'))==(coverage_saved.get('trade_date'),coverage_saved.get('scope')) else 0
                coverage_advanced=coverage_saved.get('completed',0)>coverage_earlier
                source_saved=state.get('finance_source_read_checkpoint') or {}
                source_earlier=finance_source_before.get('completed',0) if (finance_source_before.get('trade_date'),finance_source_before.get('scope'))==(source_saved.get('trade_date'),source_saved.get('scope')) else 0
                source_advanced=source_saved.get('completed',0)>source_earlier
                metadata_saved=state.get('event_metadata_read_checkpoint') or {}
                metadata_earlier=metadata_before.get('completed',0) if (metadata_before.get('trade_date'),metadata_before.get('scope'))==(metadata_saved.get('trade_date'),metadata_saved.get('scope')) else 0
                metadata_advanced=metadata_saved.get('completed',0)>metadata_earlier
                resumable_timeout=isinstance(ex,NativeStepDeadline) or any(token in str(ex) for token in ('REPLY_DEADLINE','VALIDATION_DEADLINE'))
                if resumable_timeout and (finance_advanced or event_advanced or coverage_advanced or source_advanced or metadata_advanced):
                    state.update(status='NOT_STARTED',step_status='COMPLETE',
                        reason='来源事实分块断点已保存；下一步继续',retry_after=str(now_shanghai()+timedelta(minutes=1)))
                original=getattr(ex,'orig',ex);args=getattr(original,'args',())
                transient=(args and args[0] in (1205,1213,2003,2006,2013,3024)) or any(t in str(ex) for t in ('errno=2013','errno=1205','errno=2006'))
                if transient and state.get('retry_count',0)<1:
                    state.update(status='NOT_STARTED',retry_count=1,retry_after=str(now_shanghai()+timedelta(minutes=5)))
                for p in state['proposals']:
                    if p['validation']['status'] in ('RUNNING','NOT_STARTED'):p['validation'].update(status=state['status'],reason=state['reason'])
                _save(c,state,owner)
            return {'status':state['status'] if state else 'FAILED','run_id':state['run_id'] if state else None,'reason':str(ex)}
        finally:
            if previous_handler is not None:signal.setitimer(signal.ITIMER_REAL,0);signal.signal(signal.SIGALRM,previous_handler)
            c.rollback()
            if not c.invalidated:c.execute(text('SELECT RELEASE_LOCK(:name)'),{'name':LOCK});c.commit()
