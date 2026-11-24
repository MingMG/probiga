from copy import deepcopy
from datetime import datetime

import pandas as pd
import pytest
from sqlalchemy import text

from server.common.formal_analysis_store import FormalAnalysisStore
from server.db.formal_analysis_schema import privileged_migrate_formal_analysis_schema
from server.engine import strategy_native_backtest as native
from server.engine import native_research_jobs as jobs
from server.engine import native_research_dataset as storage
from server.engine.strategy_shadow_trials import v3_shadow_policy
from server.engine.shadow_trial_policy import SHADOW_EXECUTION_POLICY
from server.engine.strategy_price_backtest import BAR_COLUMNS
from server.trading_v3 import config as configuration
from tests.test_native_prepared_features import feature_inputs
from tests.test_native_research_jobs import queue


def prepared_experiment(queue,local,store,monkeypatch,policy,*,omit_context=False):
    days,frame,context,finance,events,cutoff=feature_inputs()
    following=list(pd.bdate_range(pd.Timestamp(days[-1])+pd.offsets.BDay(1),periods=23).date)
    calendar=days+following
    with queue.begin() as connection:
        connection.execute(text('CREATE TABLE IF NOT EXISTS si_trade_calendar(trade_date TEXT PRIMARY KEY,trade_status INT)'))
        connection.execute(text('INSERT OR IGNORE INTO si_trade_calendar VALUES(:day,1)'),[{'day':str(day)} for day in calendar])
    config=deepcopy(configuration.load_v3_config())
    config['data_quality']['minimum_market_stock_count']=1
    config['theme_research']={'groups':{'银行':{'keywords':['银行']}}}
    monkeypatch.setattr(configuration,'load_v3_config',lambda:deepcopy(config))
    strategy={'strategy_key':'quality_momentum','strategy_name':'Unit quality','source_kind':'immutable_v3_sleeve',
        'current_version':'unit-test-rule','version_hash':'a'*64,
        'evaluator_config':{'model_version':config['strategy_version'],
                            'shadow_trial_routing':v3_shadow_policy(config,'quality_momentum')},
        'parameters':{'horizon_days':20,'default_risk_weight':.8,
                      'shadow_execution_policy':deepcopy(SHADOW_EXECUTION_POLICY)}}
    # A test rule deliberately admits all scored candidates; production thresholds are unchanged.
    strategy['evaluator_config']['shadow_trial_routing']['minimum_raw_score']=0.
    monkeypatch.setattr(native,'read_runs',native.read_runs)
    from server.engine import autonomous_strategy_research as research
    monkeypatch.setattr(research,'_read_states',lambda *_a,**_k:[])
    with queue.connect() as connection:
        state=native._start(connection,[strategy],[str(days[-1])],{},'e'*64,
            allocation_policy=policy,execution_end_date=str(following[-1]))
    target=str(days[-1])
    from server.trading_v3.context import load_asof_context
    context['market_context']=load_asof_context(None,as_of=days[-1],cutoff_at=datetime.fromisoformat(state['requested_at']))
    cutoff['decision_at']=state['requested_at']
    cutoff['source_cutoff_at']={'finance':target+' 16:00:00','event':target+' 16:00:00'}
    for value in finance.values():
        value.update(finance_decision_at=state['requested_at'],finance_fact_cutoff_at=target+' 16:00:00')
    events.update(decision_at=state['requested_at'],fact_cutoff_at=target+' 16:00:00')
    identity={code:{'short_name':'银行'+str(index),'industry_name':'银行'} for index,code in enumerate(context['codes'])}
    plan={'status':'COMPLETE','day':target,'eligible':context['codes'],'excluded':[],
        'identity_rows':identity,'identity':{'status':'VERIFIED','sha256':'b'*64,'stock_count':4,'captured_at':target+' 16:00:00'},
        'finance_seal':'c'*64,'snapshot_generation':'d'*64}
    context['binding']={'day':target,'decision_at':state['requested_at'],'price_cutoff':state['price_cutoff'],
                        'identity_sha256':'b'*64,'finance_seal':'c'*64}
    state['plans'][target]=native._put(local,store,state,'plan:'+target,plan)
    state['plan_summary'][target]={'status':'COMPLETE'}
    state['feature_windows'][target]=[str(day) for day in days]
    values={'selection':{'status':'COMPLETE','cutoff':cutoff,'signal_clock':datetime.combine(days[-1],datetime.min.time()).replace(hour=18),
                         'fact_clock':datetime.combine(days[-1],datetime.min.time()).replace(hour=16)},
            'finance':finance,'events':{'notices':{},'evidence':events},'context':context}
    state['day_inputs'][target]={key:native._put(local,store,state,target+':'+key,value) for key,value in values.items()}
    raw=frame.copy()
    for field in ('open','high','low','close','pre_close'):raw[field]=frame['raw_'+field]
    rows=raw[list(BAR_COLUMNS)].to_dict('records')
    for code in context['codes']:
        previous=float(frame[frame.stock_code==code].iloc[-1]['raw_close'])
        for day in following:
            close=previous*1.002
            rows.append({'stock_code':code,'trade_date':day,'open':previous,'high':close+.2,'low':previous-.2,
                         'close':close,'pre_close':previous,'amount':1e8,'volume':1e7})
            previous=close
    for day in calendar:
        target_day=str(day)
        truth={'schema':'probiga.qmt-daily-market-consumer-truth.v1','requested_sessions':[target_day],
               'attested_row_count':4,'run_id':'unit-test-prices'}
        tree=native._put(local,store,state,'bars:'+target_day,[row for row in rows if row['trade_date']==day])
        state['bars'][target_day]={'tree':tree,'proof':truth,'scope':{'stock_count':4,'catalog':'unit'},'row_count':4}
    state.update(input_index={},phase='PREPARE_DAYS',cursor=0,feature_input_sessions=[str(day) for day in days],
        execution_input_sessions=state['input_sessions'],data_sessions=[str(day) for day in calendar],
        completed_units=len(calendar)+6,total_units=len(calendar)+10)
    if omit_context:
        state['day_inputs'][target].pop('context')
        state['completed_units']-=1
    with queue.begin() as connection:jobs.enqueue(connection,state)
    return state


def test_ready_inputs_produce_fills_reports_and_reuse_features_across_allocation_policies(queue,tmp_path,monkeypatch):
    privileged_migrate_formal_analysis_schema(queue)
    monkeypatch.setenv('PROBIGA_NATIVE_DATASET_ROOT',str(tmp_path))
    local,store=storage.PreparedDataset(tmp_path),FormalAnalysisStore(queue)
    baseline=prepared_experiment(queue,local,store,monkeypatch,'FIXED_SHADOW')
    budgets=[]
    original_budget=native._feature_compute_deadline
    def budget(started,*,arm_timer):
        budgets.append(started)
        return original_budget(started,arm_timer=arm_timer)
    monkeypatch.setattr(native,'_feature_compute_deadline',budget)
    result=native.native_tick(queue)
    with queue.connect() as connection:
        first=native.read_runs(connection,run_id=baseline['run_id'])[0]
    assert result['status']=='COMPLETE', {key:{field:value.get(field) for field in ('status','reason','source_gaps')} for key,value in first['reports'].items()}
    report=first['reports']['quality_momentum']['report']
    assert report['completed_trade_count']>0 and report['trades'][0]['estimated_fee_cny']>0
    assert first['completed_units']==first['total_units']
    assert first['reports']['quality_momentum']['available_feature_sessions']==1
    assert budgets  # PREPARE_DAYS entered FEATURES in the same attempt.
    assert first['source_progress']['dataset']=='feature_input_decode'
    assert first['source_progress']['completed']==first['source_progress']['total']==70
    assert not list(tmp_path.glob('*.pin'))
    adaptive=prepared_experiment(queue,local,store,monkeypatch,'ADAPTIVE_REGIME')
    assert adaptive['run_id']!=baseline['run_id']
    monkeypatch.setattr(native,'_features',lambda *_a,**_k:pytest.fail('identical frozen feature inputs were recalculated'))
    monkeypatch.setattr(store,'load_artifacts',lambda *_a,**_k:pytest.fail('warm prepared inputs were retransmitted'))
    assert native.native_tick(queue)['status']=='COMPLETE'
    with queue.connect() as connection:
        second=native.read_runs(connection,run_id=adaptive['run_id'])[0]
    assert second['reused_feature_sessions']==1
    assert second['reports']['quality_momentum']['report']['allocation_policy']=='ADAPTIVE_REGIME'
    assert sum(trade['quantity'] for trade in second['reports']['quality_momentum']['report']['trades'])<=sum(trade['quantity'] for trade in report['trades'])


def test_cold_context_stage_builds_the_real_window_and_ignores_unrelated_missing_blocks(queue,tmp_path,monkeypatch):
    from server.trading_v3 import daily_features
    privileged_migrate_formal_analysis_schema(queue)
    monkeypatch.setenv('PROBIGA_NATIVE_DATASET_ROOT',str(tmp_path))
    local,store=storage.PreparedDataset(tmp_path),FormalAnalysisStore(queue)
    state=prepared_experiment(queue,local,store,monkeypatch,'FIXED_SHADOW',omit_context=True)
    target=state['sessions'][0]
    # Read the already frozen original context from the explicitly stored artifact tree.
    context=None
    for reference in state['prepared_references'].values():
        value=local.get(reference)
        if isinstance(value,dict) and 'market_context' in value and 'research_keywords' in value:
            context=value;break
    assert context is not None
    observed=[]
    def load_context(_engine,**kwargs):
        observed.append(kwargs['frame'].attrs['qmt_daily_input_window'])
        return deepcopy(context)
    monkeypatch.setattr(daily_features,'prepare_feature_identity_context',load_context)
    # The removed context artifact is unrelated until its replacement is published.
    # Corrupting another prepared input cannot make the owner scan all history.
    unused=local.put(store,state['run_id'],'unused','not-consumed',{'ignored':True})
    local._path(unused['sha256']).unlink()
    with queue.connect() as connection:
        current=native.read_runs(connection,run_id=state['run_id'])[0]
    current['prepared_references'][unused['sha256']]=unused
    with queue.begin() as connection:
        current['checkpoint_sequence']+=1
        native._append_state(connection,current)
    result=native.native_tick(queue)
    assert result['status']=='COMPLETE',result
    assert observed and observed[0]['session_count']==70
    assert not local._path(unused['sha256']).exists()


def test_verified_ready_context_reuses_exact_inputs_when_live_context_is_unavailable(queue,tmp_path,monkeypatch):
    from server.trading_v3 import daily_features
    privileged_migrate_formal_analysis_schema(queue)
    monkeypatch.setenv('PROBIGA_NATIVE_DATASET_ROOT',str(tmp_path))
    local,store=storage.PreparedDataset(tmp_path),FormalAnalysisStore(queue)
    baseline=prepared_experiment(queue,local,store,monkeypatch,'FIXED_SHADOW')
    assert native.native_tick(queue)['status']=='COMPLETE'
    with queue.connect() as connection:
        source=native.read_runs(connection,run_id=baseline['run_id'])[0]
    target=source['sessions'][0]
    adaptive=prepared_experiment(queue,local,store,monkeypatch,'ADAPTIVE_REGIME',omit_context=True)
    monkeypatch.setattr(daily_features,'prepare_feature_identity_context',
        lambda *_a,**_k:(_ for _ in ()).throw(RuntimeError('live context unavailable')))
    monkeypatch.setattr(native,'_features',lambda *_a,**_k:pytest.fail('verified features were recomputed'))
    result=native.native_tick(queue)
    assert result['status']=='COMPLETE',result
    with queue.connect() as connection:
        restored=native.read_runs(connection,run_id=adaptive['run_id'])[0]
    assert restored['day_inputs'][target]['context']==source['day_inputs'][target]['context']
    assert restored['day_inputs'][target]['ready']==source['day_inputs'][target]['ready']
    assert restored['reused_feature_sessions']==1


def ready_context_source(queue,tmp_path,monkeypatch):
    privileged_migrate_formal_analysis_schema(queue)
    monkeypatch.setenv('PROBIGA_NATIVE_DATASET_ROOT',str(tmp_path))
    local,store=storage.PreparedDataset(tmp_path),FormalAnalysisStore(queue)
    initial=prepared_experiment(queue,local,store,monkeypatch,'FIXED_SHADOW')
    monkeypatch.setattr(native,'_save',lambda *_a,**_k:None)
    with queue.connect() as connection:
        source=native.read_runs(connection,run_id=initial['run_id'])[0]
        native._prepare_day(connection,source,None,local,store,source['sessions'][0])
    source['checkpoint_sequence']+=1
    with queue.begin() as connection:native._append_state(connection,source)
    return source,local,store


def without_context(source):
    consumer=deepcopy(source)
    inputs=consumer['day_inputs'][source['sessions'][0]]
    for key in ('context','ready','features'):inputs.pop(key,None)
    return consumer


@pytest.mark.parametrize('changed',[
    'known','price_cutoff','catalog','facts_tree','bar_tree','bar_proof','bar_scope','config','producer','algorithm'])
def test_context_handoff_never_crosses_its_frozen_input_scope(queue,tmp_path,monkeypatch,changed):
    from datetime import timedelta
    from server.trading_v3 import daily_features
    source,local,store=ready_context_source(queue,tmp_path,monkeypatch)
    target=source['sessions'][0]
    consumer=without_context(source)
    if changed in ('known','price_cutoff'):
        field='requested_at' if changed=='known' else changed
        consumer[field]=str(datetime.fromisoformat(consumer[field])+timedelta(seconds=1))
    elif changed=='catalog':
        plan=local.get_tree(consumer['plans'][target]);plan['identity']['sha256']='9'*64
        consumer['plans'][target]=native._put(local,store,consumer,'different-plan',plan)
    elif changed=='facts_tree':
        events=local.get_tree(consumer['day_inputs'][target]['events']);events['scope_marker']='different'
        consumer['day_inputs'][target]['events']=native._put(local,store,consumer,'different-events',events)
    elif changed in ('bar_tree','bar_proof','bar_scope'):
        bar=consumer['bars'][consumer['feature_windows'][target][0]]
        if changed=='bar_tree':
            rows=local.get_tree(bar['tree']);rows[0]['amount']+=1
            bar['tree']=native._put(local,store,consumer,'different-bar',rows)
        elif changed=='bar_proof':bar['proof']['truth_hash']='8'*64
        else:bar['scope']['catalog']='different'
    elif changed=='config':consumer['config']['context_scope_test']='different'
    elif changed=='producer':consumer['producer_identity']['context_scope_test']='7'*64
    else:consumer['feature_algorithm_sha256']='6'*64
    assert native._context_generation(consumer,target)!=native._context_generation(source,target)
    def cold_context(*_a,**_k):raise RuntimeError('exact context cache miss')
    monkeypatch.setattr(daily_features,'prepare_feature_identity_context',cold_context)
    with queue.connect() as connection:
        with pytest.raises(RuntimeError,match='exact context cache miss'):
            native._prepare_day(connection,consumer,None,local,store,target)
    assert 'context' not in consumer['day_inputs'][target]


def test_context_handoff_restores_corrupt_local_blocks_from_the_same_content_hash(queue,tmp_path,monkeypatch):
    from server.trading_v3 import daily_features
    source,local,store=ready_context_source(queue,tmp_path,monkeypatch)
    target=source['sessions'][0]
    tree=source['day_inputs'][target]['context']
    expected=local.get_tree(tree)
    reference=next(iter(local.tree_references(tree).values()))
    local._path(reference['sha256']).write_bytes(b'corrupt local cache')
    restored=[];original=store.load_artifacts
    def load(digests):restored.extend(digests);return original(digests)
    monkeypatch.setattr(store,'load_artifacts',load)
    monkeypatch.setattr(daily_features,'prepare_feature_identity_context',lambda *_a,**_k:pytest.fail('live context was queried'))
    consumer=without_context(source)
    with queue.connect() as connection:native._prepare_day(connection,consumer,None,local,store,target)
    assert consumer['day_inputs'][target]['context']==tree
    assert local.get_tree(tree)==expected
    assert reference['sha256'] in restored


def test_retain_ready_context_requires_the_explicit_audited_completion(queue,tmp_path,monkeypatch):
    from server.db.formal_analysis_schema import MEMBER_TABLE
    source,local,store=ready_context_source(queue,tmp_path,monkeypatch)
    target=source['sessions'][0]
    generation=native._context_generation(source,target)
    # Model an existing READY audit produced before this completed-context index existed.
    with queue.begin() as connection:
        connection.execute(text(f'DELETE FROM {MEMBER_TABLE} WHERE generation_sha256=:generation AND stage=:stage'),
            {'generation':generation,'stage':'native_context_handoff'})
    assert store.get(generation,'native_context_handoff','complete') is None
    with queue.connect() as connection:
        retained=native.retain_ready_context(connection,source,target,dataset=local,store=store)
        assert native.retain_ready_context(connection,source,target,dataset=local,store=store)==retained
        changed=deepcopy(source);changed['requested_at']='2099-01-01 00:00:00'
        with pytest.raises(ValueError,match='audited state'):
            native.retain_ready_context(connection,changed,target,dataset=local,store=store)
    record=store.get(generation,'native_context_handoff','complete')
    assert retained['generation']==generation
    assert record['tree']==source['day_inputs'][target]['ready']['context']
    incomplete=deepcopy(source);incomplete['day_inputs'][target].pop('ready')
    incomplete['checkpoint_sequence']+=1
    with queue.begin() as connection:native._append_state(connection,incomplete)
    with queue.connect() as connection:
        with pytest.raises(ValueError,match='exact READY completion'):
            native.retain_ready_context(connection,incomplete,target,dataset=local,store=store)


def test_conflicting_context_completion_never_falls_back_to_live_source(queue,tmp_path,monkeypatch):
    from server.common.formal_analysis_store import FormalAnalysisConflict
    from server.db.formal_analysis_schema import MEMBER_TABLE
    from server.trading_v3 import daily_features
    source,local,store=ready_context_source(queue,tmp_path,monkeypatch)
    target=source['sessions'][0];generation=native._context_generation(source,target)
    record=store.get(generation,'native_context_handoff','complete')
    record['input_identity']='f'*64
    with queue.begin() as connection:
        connection.execute(text(f'DELETE FROM {MEMBER_TABLE} WHERE generation_sha256=:generation AND stage=:stage'),
            {'generation':generation,'stage':'native_context_handoff'})
    store.put(generation,'native_context_handoff','complete',record)
    monkeypatch.setattr(daily_features,'prepare_feature_identity_context',lambda *_a,**_k:pytest.fail('live context was queried'))
    with queue.connect() as connection:
        with pytest.raises(ValueError,match='handoff identity differs'):
            native._prepare_day(connection,without_context(source),None,local,store,target)
        with pytest.raises(FormalAnalysisConflict):
            native.retain_ready_context(connection,source,target,dataset=local,store=store)


@pytest.mark.parametrize('invalid_keywords',[False,True])
def test_context_completion_is_published_only_after_ready_validation(queue,tmp_path,monkeypatch,invalid_keywords):
    from server.db.formal_analysis_schema import MEMBER_TABLE
    from server.trading_v3 import daily_features
    privileged_migrate_formal_analysis_schema(queue)
    monkeypatch.setenv('PROBIGA_NATIVE_DATASET_ROOT',str(tmp_path))
    local,store=storage.PreparedDataset(tmp_path),FormalAnalysisStore(queue)
    initial=prepared_experiment(queue,local,store,monkeypatch,'FIXED_SHADOW',omit_context=True)
    context=next(value for reference in initial['prepared_references'].values()
                 if isinstance(value:=local.get(reference),dict) and 'market_context' in value and 'research_keywords' in value)
    # The shared fixture retains the original artifact for constructing a
    # realistic context; a genuinely cold stage has never published its member.
    with queue.begin() as connection:
        connection.execute(text(f'DELETE FROM {MEMBER_TABLE} WHERE generation_sha256=:generation '
            'AND stage=:stage AND unit_key=:key'),
            {'generation':initial['run_id'],'stage':'native_prepared','key':initial['sessions'][0]+':context'})
    if invalid_keywords:context['research_keywords']={'other':('other',)}
    monkeypatch.setattr(daily_features,'prepare_feature_identity_context',lambda *_a,**_k:deepcopy(context))
    monkeypatch.setattr(native,'_save',lambda *_a,**_k:None)
    with queue.connect() as connection:
        current=native.read_runs(connection,run_id=initial['run_id'])[0]
        target=current['sessions'][0];generation=native._context_generation(current,target)
        native._prepare_day(connection,current,None,local,store,target)
        assert 'context' in current['day_inputs'][target] and 'ready' not in current['day_inputs'][target]
        assert store.get(generation,'native_context_handoff','complete') is None
        if invalid_keywords:
            with pytest.raises(ValueError,match='FROZEN_TAXONOMY_DIFFERS'):
                native._prepare_day(connection,current,None,local,store,target)
            assert store.get(generation,'native_context_handoff','complete') is None
        else:
            native._prepare_day(connection,current,None,local,store,target)
            assert store.get(generation,'native_context_handoff','complete')['tree']==current['day_inputs'][target]['ready']['context']
