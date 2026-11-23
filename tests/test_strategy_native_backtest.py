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
    r=native._v2_day(None,{'published':{'2026-11-20':'receipt'},'requested_at':'2026-11-22 09:00:00'},'2026-11-20')
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


def test_driver_replays_partial_sources_without_fabricating_zero_profit(monkeypatch):
    days,bars,strategy,signal=fixture()
    strategy['strategy_name']='native'
    state={'phase':'REPLAY','status':'NOT_STARTED','run_id':'run','rule_hash':'hash',
        'sessions':days[20:],'input_sessions':days,'strategies':[strategy],
        'contract':'code','total_units':61,'completed_units':60,
        'proposals':[{'strategy_key':'native','version':'v1','rule_hash':'a'*64,'name':'native','validation':{}}]}
    parts={d:{'day':d,'status':'BLOCKED','reason':'历史身份不可得','signals':{}} for d in days[20:]}
    monkeypatch.setattr(native,'_save',lambda *_a,**_k:None)
    monkeypatch.setattr(native,'_input_parts',lambda *_a:{})
    monkeypatch.setattr(native,'_partitions',lambda _c,_s,_p,kind:parts if kind=='S' else {})
    native._step(None,state,None)
    v=state['proposals'][0]['validation']
    assert state['phase']=='FINISHED' and state['status']=='BLOCKED' and state['completed_units']==61
    assert v['available_feature_sessions']==0 and len(v['source_gaps'])==10
    assert v['report']['net_return_pct'] is None and v['report']['nav']==[]


def test_historical_uncovered_finance_members_do_not_block_other_securities(monkeypatch):
    from types import SimpleNamespace
    from server.common import pit_facts
    from server.common import historical_announcement_read
    identity={str(i).zfill(6):{} for i in range(4755)}
    covered=set(sorted(identity)[4:])
    from server.db.formal_analysis_schema import privileged_migrate_formal_analysis_schema
    engine=create_engine('sqlite://');privileged_migrate_formal_analysis_schema(engine)
    monkeypatch.setattr(native,'_identity_rows',lambda *_:(identity,{'status':'VERIFIED','sha256':'a'*64}))
    monkeypatch.setattr(pit_facts,'load_latest_finance_atomic_batch_baseline',lambda *_a,**_k:{'as_of_date':'2026-08-03','members':dict.fromkeys(covered),'seal_coverage_id':'b'*64})
    monkeypatch.setattr(historical_announcement_read,'has_dated_announcement_candidate',lambda *_a,**_k:True)
    def cutoff(_engine,*,codes,**_kwargs):
        if not set(codes)<=covered:raise ValueError('finance atomic batch seal code scope differs')
        assert len(codes)==4751
        return {'status':'DATA_BLOCKED','reason':'公告窗口尚无完成记录'}
    monkeypatch.setattr(pit_facts,'resolve_common_fact_cutoff',cutoff)
    codes=sorted(covered)
    result=native._v3_day(SimpleNamespace(engine=engine),{'requested_at':'2026-11-22 09:00:00',
        'finance_scope_codes':codes,'finance_scope_sha256':native.canonical_sha256(codes)},'2026-08-03',None)
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
