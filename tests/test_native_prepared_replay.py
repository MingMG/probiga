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
    result=native.native_tick(queue)
    with queue.connect() as connection:
        first=native.read_runs(connection,run_id=baseline['run_id'])[0]
    assert result['status']=='COMPLETE', {key:{field:value.get(field) for field in ('status','reason','source_gaps')} for key,value in first['reports'].items()}
    report=first['reports']['quality_momentum']['report']
    assert report['completed_trade_count']>0 and report['trades'][0]['estimated_fee_cny']>0
    assert first['completed_units']==first['total_units']
    assert first['reports']['quality_momentum']['available_feature_sessions']==1
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
