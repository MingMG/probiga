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
