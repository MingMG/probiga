from copy import deepcopy
from datetime import datetime

import pandas as pd
import pytest
from sqlalchemy import create_engine,text

from server.engine import strategy_native_backtest as native
from server.engine.strategy_native_replay import replay_native
from server.engine.shadow_trial_policy import SHADOW_EXECUTION_POLICY
from server.api.strategy_performance import _research,rank_results


def fixture():
    days=[str(d.date()) for d in pd.bdate_range('2026-07-01',periods=30)]
    bars=[{'stock_code':'000001','trade_date':d,'open':10.,'close':10.,'pre_close':10.,
           'high':10.1,'low':9.9,'amount':1e8,'volume':1e7} for d in days]
    strategy={'strategy_key':'native','current_version':'v1','version_hash':'a'*64,
        'source_kind':'immutable_manifest','parameters':{'max_holding_days':3,
        'shadow_execution_policy':deepcopy(SHADOW_EXECUTION_POLICY)}}
    signal={'stock_code':'000001','stock_name':'证券','industry':'银行','rank':1,
        'signal_known_at':days[20]+' 16:00:00','stop_loss':9.7,'initial_stop_pct':None}
    return days,bars,strategy,signal


def test_native_uses_frozen_one_percent_risk_t1_stop_and_real_fees():
    days,bars,strategy,signal=fixture()
    bars[21].update(open=9.9,close=9.9,low=9.6)
    bars[22].update(open=9.5,pre_close=9.9)
    report=replay_native(strategy,[signal],bars,days,evaluation_start=days[20],source_gaps=[])
    trade=report['trades'][0]
    assert trade['quantity']==200
    assert trade['entry_at']==days[21]+' 09:30:00' and trade['exit_at']==days[22]+' 09:30:00'
    assert trade['exit_reason']=='HARD_STOP' and trade['execution_contract']['signal_stop_price']==9.7
    assert trade['estimated_fee_cny']>0 and trade['net_pnl_cny']<200*(9.5-9.9)
    assert report['net_return_pct']<0 and report['real_order_authority'] is False


def test_missing_data_keeps_full_return_unknown_and_nav_gap_blocks_ranking():
    days,bars,strategy,signal=fixture()
    r=replay_native(strategy,[signal],bars,days,evaluation_start=days[20],source_gaps=[{'trade_date':days[0]}])
    assert r['status']=='PARTIAL' and r['net_return_pct'] is None
    assert r['verified_input_subset_return_pct']<0
    missing=[r for r in bars if r['trade_date']!=days[22]]
    r=replay_native(strategy,[signal],missing,days,evaluation_start=days[20],source_gaps=[])
    assert r['status']=='BLOCKED' and r['verified_input_subset_return_pct'] is None


def test_native_cannot_use_late_reconstructed_scores_as_historical_signals(monkeypatch):
    from server.common import daily_delivery_control
    from server.common import analysis_pool_receipt
    def source(_c,_day,**kwargs):
        return {'published_at':'2026-11-21 21:00:00','score_snapshot':{}}
    monkeypatch.setattr(daily_delivery_control,'load_published_analysis_receipt',source)
    monkeypatch.setattr(analysis_pool_receipt,'read_score_snapshot_header',lambda *_a,**_k:{'decision_at':'2026-11-21 19:05:17'})
    monkeypatch.setattr(analysis_pool_receipt,'decode_score_snapshot',lambda *_a,**_k:pytest.fail('late scores must not be materialized'))
    from server.engine.native_original_prepare import prepare_original_day
    r=prepare_original_day(None,{'published':{'2026-11-20':'receipt'},'requested_at':'2026-11-22 09:00:00'},'2026-11-20')
    assert r['status']=='BLOCKED' and r['signals']=={}
    assert r['actual_decision_at']=='2026-11-21 19:05:17'


def test_native_audit_integrity_and_native_result_namespace():
    from server.engine.strategy_governance import _digest
    engine=create_engine('sqlite://')
    state={'schema':native.SCHEMA,'run_id':'run','proposals':[]}
    import json
    payload={'after':state}
    with engine.begin() as c:
        c.execute(text('CREATE TABLE st_strategy_governance_audit(audit_id INT,entity_type TEXT,entity_key TEXT,action TEXT,created_at TEXT,payload_json TEXT,audit_hash TEXT,after_json TEXT)'))
        c.execute(text('INSERT INTO st_strategy_governance_audit VALUES(1,:entity,\'run\',\'CHECKPOINT_00000001\',\'2026-11-22\',:payload,:hash,:after)'),
            {'entity':native.ENTITY,'payload':json.dumps(payload),'hash':_digest(payload),'after':json.dumps(state)})
        assert native.read_runs(c)==[state]
        c.execute(text("UPDATE st_strategy_governance_audit SET after_json='{}'"))
        with pytest.raises(ValueError,match='integrity'):native.read_runs(c)
    proposal={'name':'native','strategy_key':'native','version':'v1','rule_hash':'a'*64,
        'validation':{'status':'BLOCKED','sessions':['2026-11-20'],'reason':'缺少原始事件证据'}}
    r=_research({'run_id':'run'},proposal,native=True)
    assert r['result_id'].startswith('native:') and r['kind']=='NATIVE_SHADOW_RETROSPECTIVE'
    assert r['net_return_pct'] is None and rank_results([r])==[]


def test_driver_replays_partial_sources_without_fabricating_zero_profit(tmp_path):
    days,bars,strategy,signal=fixture()
    strategy['strategy_name']='native'
    from server.engine.native_research_dataset import PreparedDataset
    from server.engine.strategy_rule_replay import ACCOUNT
    state={'phase':'REPLAY','status':'RUNNING','run_id':'a'*64,'rule_hash':'hash',
        'sessions':days[20:],'input_sessions':days,'strategies':[strategy],
        'contract':'code','total_units':62,'completed_units':60,'cursor':0,'reports':{},
        'plans':{},'plan_summary':{day:{'status':'BLOCKED','reason':'历史身份不可得'} for day in days[20:]},
        'signal_parts':{},'bars':{},'execution_input_sessions':[],
        'account':ACCOUNT,'allocation_policy':'FIXED_SHADOW',
        'price_cutoff':'2026-11-22 05:59:24','requested_at':'2026-11-22 18:05:19',
        'proposals':[{'strategy_key':'native','version':'v1','rule_hash':'a'*64,'name':'native','validation':{}}]}
    native._report_one(state,PreparedDataset(tmp_path))
    native._report_one(state,PreparedDataset(tmp_path))
    validation=state['proposals'][0]['validation']
    assert state['phase']=='FINISHED' and state['status']=='BLOCKED' and state['completed_units']==62
    assert validation['available_feature_sessions']==0 and len(validation['source_gaps'])==10
    assert validation['report']['net_return_pct'] is None and validation['report']['nav']==[]


def test_historical_uncovered_finance_members_do_not_block_other_securities(monkeypatch):
    from types import SimpleNamespace
    from server.common import pit_facts
    from server.common import historical_announcement_read
    identity={str(i).zfill(6):{} for i in range(4755)}
    covered=set(sorted(identity)[4:])
    from server.db.formal_analysis_schema import privileged_migrate_formal_analysis_schema
    engine=create_engine('sqlite://');privileged_migrate_formal_analysis_schema(engine)
    from server.engine import strategy_price_backtest as price
    from server.engine import native_research_prepare as prepare
    monkeypatch.setattr(price,'_identity_rows',lambda *_:(identity,{'status':'VERIFIED','sha256':'a'*64}))
    monkeypatch.setattr(pit_facts,'load_latest_finance_atomic_batch_baseline',lambda *_a,**_k:{'as_of_date':'2026-08-03','members':dict.fromkeys(covered),'seal_coverage_id':'b'*64})
    monkeypatch.setattr(historical_announcement_read,'has_dated_announcement_candidate',lambda *_a,**_k:True)
    def cutoff(_engine,*,codes,**_kwargs):
        if not set(codes)<=covered:raise ValueError('finance atomic batch seal code scope differs')
        assert len(codes)==4751
        return {'status':'DATA_BLOCKED','reason':'公告窗口尚无完成记录'}
    monkeypatch.setattr(pit_facts,'resolve_common_fact_cutoff',cutoff)
    codes=sorted(covered)
    state={'requested_at':'2026-11-22 09:00:00','proposals':[]}
    connection=SimpleNamespace(engine=engine)
    plan=prepare.plan_v3_day(connection,state,'2026-08-03')
    assert len(plan['eligible'])==4751
    result=prepare.prepare_selection(connection,state,None,plan)
    assert result['reason']=='公告窗口尚无完成记录'


def test_entry_scope_filters_stock_features_without_removing_market_bars(monkeypatch):
    from server.trading_v3 import daily_features
    days=[d.date() for d in pd.bdate_range('2026-04-01',periods=70)]
    frame=pd.DataFrame([{'stock_code':code,'trade_date':d} for code in ['000001','000002'] for d in days])
    frame.attrs['qmt_daily_input_window']={'sessions':[str(d) for d in days]}
    class ReachedScope(Exception):pass
    def industries(_engine,codes,**_kwargs):
        assert codes==['000001'] and len(frame)==140
        raise ReachedScope()
    monkeypatch.setattr(daily_features,'_load_industries',industries)
    with pytest.raises(ReachedScope):
        daily_features.build_daily_feature_universe(None,as_of=days[-1],dates=days,frame=frame,entry_scope_codes=['000001'])


@pytest.mark.parametrize('reason',[
    'QMT_ANNOUNCEMENT_LIVE_BATCH_REPLY_FAILED:batch-payload:3000',
    'PIT_COMMON_CUTOFF_SCHEMA_UNAVAILABLE:QMT_ANNOUNCEMENT_LIVE_BATCH_REPLY_FAILED:common-cutoff-coverage:80:size:400',
    'PIT_QMT_EVENT_BATCH_UNAVAILABLE:FormalAnalysisArtifactIntegrityError',
])
def test_observed_reply_failures_are_not_completed_no_data_dates(reason):
    with pytest.raises(RuntimeError,match='历史事实读取失败'):native._assert_read_succeeded(reason)
    native._assert_read_succeeded('QMT_ANNOUNCEMENT_COMPLETE_BATCH_NOT_FOUND')


def test_job_deadline_cannot_be_swallowed_by_source_exception_handlers():
    with pytest.raises(native.NativeStepDeadline):
        try:raise native.NativeStepDeadline('bounded job deadline')
        except Exception:pytest.fail('source error wrapper swallowed the deadline')


def test_failure_lock_recovers_after_connection_invalidation_without_stealing():
    from itertools import count
    from sqlalchemy import event
    from sqlalchemy.pool import QueuePool
    engine=create_engine('sqlite://',poolclass=QueuePool)
    sessions,owners={},{}
    sequence=count(1)
    @event.listens_for(engine,'connect')
    def functions(dbapi,_record):
        session=next(sequence);sessions[id(dbapi)]=session
        def acquire(name,_timeout):
            if name in owners and owners[name]!=session:return 0
            owners[name]=session;return 1
        dbapi.create_function('CONNECTION_ID',0,lambda:session)
        dbapi.create_function('IS_USED_LOCK',1,lambda name:owners.get(name))
        dbapi.create_function('GET_LOCK',2,acquire)
    @event.listens_for(engine,'close')
    def release(dbapi,_record):
        session=sessions.pop(id(dbapi))
        for name in list(owners):
            if owners[name]==session:del owners[name]
    try:
        with engine.connect() as first,engine.connect() as second:
            native._restore_failure_lock(first)
            old=first.execute(text('SELECT CONNECTION_ID()')).scalar()
            first.invalidate();first.rollback()
            native._restore_failure_lock(first)
            assert first.execute(text('SELECT CONNECTION_ID()')).scalar()!=old
            with pytest.raises(RuntimeError,match='互斥锁'):native._restore_failure_lock(second)
            assert first.execute(text('SELECT IS_USED_LOCK(:name)=CONNECTION_ID()'),{'name':native.LOCK}).scalar()==1
    finally:engine.dispose()


def test_native_journal_reads_latest_heads_without_sorting_historical_bodies():
    import json
    from sqlalchemy import event
    from server.engine.strategy_governance import _digest
    engine=create_engine('sqlite://')
    with engine.begin() as c:
        c.execute(text('CREATE TABLE st_strategy_governance_audit(audit_id INT,entity_type TEXT,entity_key TEXT,action TEXT,created_at TEXT,payload_json TEXT,audit_hash TEXT,after_json TEXT)'))
        for number,stamp in [(1,'2026-11-20'),(2,'2026-11-21'),(3,'2026-11-21')]:
            state={'schema':native.SCHEMA,'run_id':'run','checkpoint_sequence':number,'proposals':[]}
            payload={'after':state}
            c.execute(text('INSERT INTO st_strategy_governance_audit VALUES(:n,:entity,\'run\',:action,:stamp,:payload,:hash,:after)'),
                {'n':number,'entity':native.ENTITY,'action':f'CHECKPOINT_{number:08d}',
                 'stamp':stamp,'payload':json.dumps(payload),'hash':_digest(payload),'after':json.dumps(state)})
    def cold_history(_c,_cur,sql,params,_ctx,_many):
        if 'MAX(action)' in sql:
            from sqlalchemy.exc import OperationalError
            raise OperationalError(sql,params,Exception(3024,'observed native journal history deadline'))
        if 'payload_json' in sql:
            assert 'created_at=' in sql, 'historical bodies sorted before limiting to the latest journal head'
    event.listen(engine,'before_cursor_execute',cold_history)
    with engine.connect() as c:
        assert native.read_runs(c)[0]['checkpoint_sequence']==3
        assert native.read_runs(c,run_id='run')[0]['checkpoint_sequence']==3
        assert native.read_runs(c,run_id='absent')==[]
    engine.dispose()


def test_run_directory_seeks_latest_heads_within_the_old_aggregate_work_budget():
    import json
    from sqlalchemy.exc import OperationalError
    from server.engine.strategy_governance import _digest
    engine=create_engine('sqlite://')
    with engine.begin() as c:
        c.execute(text('CREATE TABLE st_strategy_governance_audit(audit_id INT PRIMARY KEY,entity_type TEXT,entity_key TEXT,action TEXT,created_at TEXT,payload_json TEXT,audit_hash TEXT,after_json TEXT)'))
        c.execute(text('CREATE INDEX idx_strategy_governance_audit_entity ON st_strategy_governance_audit(entity_type,entity_key,created_at)'))
        rows=[]
        for run in range(80):
            for checkpoint in range(100):
                state={'schema':native.SCHEMA,'run_id':f'{run:064x}','checkpoint_sequence':checkpoint,'proposals':[]}
                payload={'after':state}
                rows.append({'id':len(rows),'entity':native.ENTITY,'key':state['run_id'],
                    'action':f'CHECKPOINT_{checkpoint:08d}','stamp':f'2026-11-22 00:{checkpoint//60:02d}:{checkpoint%60:02d}',
                    'payload':json.dumps(payload),'hash':_digest(payload),'after':json.dumps(state)})
        c.execute(text('INSERT INTO st_strategy_governance_audit VALUES(:id,:entity,:key,:action,:stamp,:payload,:hash,:after)'),rows)
    old="SELECT entity_key,MAX(created_at) AS latest_at FROM st_strategy_governance_audit WHERE entity_type='NATIVE_REPLAY' GROUP BY entity_type,entity_key ORDER BY latest_at DESC,entity_key DESC LIMIT 8"
    with engine.connect() as c:
        db=c.connection.driver_connection
        count=[0]
        def count_steps():count[0]+=1;return 0
        db.set_progress_handler(count_steps,1)
        expected=c.execute(text(old)).mappings().all()
        old_steps=count[0]
        count[0]=0
        result=native.read_runs(c)
        new_steps=count[0]
        db.set_progress_handler(None,0)
        assert [r['run_id'] for r in result]==[r['entity_key'] for r in expected]
        assert all(r['checkpoint_sequence']==99 for r in result)
        assert new_steps<old_steps*0.75
        budget=int(old_steps*0.75)
        def exhausted():count[0]+=1;return int(count[0]>budget)
        db.set_progress_handler(exhausted,1);count[0]=0
        with pytest.raises(OperationalError,match='interrupted'):c.execute(text(old)).all()
        count[0]=0
        assert len(native.read_runs(c))==8
        db.set_progress_handler(None,0)
    engine.dispose()


def test_original_signals_evaluate_only_historical_scored_identified_securities(monkeypatch):
    from tests.test_native_shadow_trials import _strategy,_analysis_row
    from server.engine.native_research_math import evaluate_manifest_signals
    from server.common import versioned_strategy_config
    strategy=_strategy()
    row=_analysis_row()
    row['model_version']=strategy['evaluator_config']['model_version']
    missing={**row,'stock_code':'000002',strategy['evaluator_config']['score_field']:None}
    no_identity={**row,'stock_code':'000003'}
    prepared={'scored_rows':[row,missing,no_identity],'market':{'market_state':'trend_bullish'},
        'identities':{row['stock_code']:{'short_name':'历史名称','industry_name':'银行'}},
        'identity':{'status':'VERIFIED'},'source_run_uid':'old-receipt','source_sha256':'b'*64,
        'signal_clock':'2026-08-21 18:05:00'}
    monkeypatch.setattr(versioned_strategy_config,'load_stock_manifest',lambda:pytest.fail('pure original math read live rules'))
    result=evaluate_manifest_signals(prepared,[strategy],day='2026-08-21')
    key=strategy['strategy_key']
    assert result['evaluation_security_sets'][key]==[row['stock_code']]
    assert result['missing_feature_stocks'][key]==2
    assert result['signals'][key][0]['industry']=='银行'
    assert result['signals'][key][0]['signal_known_at']==prepared['signal_clock']
