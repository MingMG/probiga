from copy import deepcopy
from datetime import date, datetime

import pandas as pd
import pytest

from server.trading_v3 import daily_features as reader
from server.trading_v3 import daily_feature_math as math


def feature_inputs():
    days=list(pd.bdate_range('2026-06-01',periods=70).date)
    codes=['000001','000002','000003','000004']
    rows=[]
    for index,code in enumerate(codes):
        for offset,day in enumerate(days):
            previous=10+(offset-1)*(.03-index*.01)
            close=10+offset*(.03-index*.01)
            rows.append({'stock_code':code,'short_name':'银行'+str(index),'trade_date':day,
                'open':previous,'high':close+.2,'low':previous-.2,'close':close,'pre_close':previous,
                'amount':1e8+offset*1e5+index*1e6,'volume':1e7})
    frame=pd.DataFrame(rows)
    frame.attrs['qmt_daily_input_window']={'sessions':[str(day) for day in days],
        'latest_daily_truth':{'schema':'probiga.qmt-daily-market-consumer-truth.v1',
            'requested_sessions':[str(days[-1])],'attested_row_count':4,'run_id':'unit-test-prices'}}
    frame=math.normalize_daily_bars(frame)
    evidence={'status_by_code':dict.fromkeys(codes,'AVAILABLE'),'reason_by_code':dict.fromkeys(codes,''),
        'snapshot_hash':'a'*64,'snapshot_date':str(days[-1]),'source':'unit-test-frozen-industry'}
    context={'codes':codes,'industries':{code:('bank','银行') for code in codes},
        'industry_evidence':evidence,'theme_memberships':{code:[('bank','银行','industry')] for code in codes},
        'concept_snapshot_date':days[-1],'market_context':{'context_theme_scores':{},'context_theme_novelty':{}},
        'research_keywords':{'银行':('银行',)}}
    finance={code:{**{field:float(index+1) for field in math._FINANCE_NUMERIC_FIELDS},
        'finance_pit_status':'AVAILABLE','finance_pit_reason':'','finance_manifest_hash':'b'*64,
        'finance_report_date':date(2026,6,30)} for index,code in enumerate(codes)}
    events={'status':'AVAILABLE','reason':'','manifest_hash':'c'*64,
        'status_by_code':dict.fromkeys(codes,'AVAILABLE'),'reason_by_code':dict.fromkeys(codes,''),
        'coverage_by_code':{code:{'coverage_id':'empty-'+code} for code in codes}}
    cutoff={'status':'AVAILABLE','reason':'','fact_cutoff_at':str(days[-1])+' 16:00:00',
        'decision_at':str(days[-1])+' 18:00:00','source_cutoff_at':{},'receipt_root_hash':'d'*64}
    return days,frame,context,finance,events,cutoff


def calculate(inputs):
    days,frame,context,finance,events,cutoff=deepcopy(inputs)
    state=math.compute_price_features(as_of=days[-1],dates=days,frame=frame,context=context,
        context_cutoff_at=datetime.combine(days[-1],datetime.min.time()).replace(hour=18))
    state=math.compute_finance_features(state,cutoff,finance)
    return math.finish_feature_universe(state,{},events)


def test_prepared_features_preserve_the_original_formula_result(monkeypatch):
    from server.trading_v3 import config,theme_features
    monkeypatch.setattr(config,'load_v3_config',lambda:pytest.fail('calculation read live config'))
    monkeypatch.setattr(theme_features,'_paper_research_theme_keywords',lambda:pytest.fail('calculation read live taxonomy'))
    result=calculate(feature_inputs())
    assert len(result['stocks'])==4
    assert result['data_snapshot_hash']=='d24ee2e7b61a689943fb80d28b7ccd25a56a54fd223183f3e2592498639d45e0'
    assert all(stock['event_authoritative_empty'] for stock in result['stocks'])
    assert all(stock['pit_strategy_status']=='AVAILABLE' for stock in result['stocks'])


def test_preparation_checks_price_evidence_before_querying(monkeypatch):
    days,frame,*_=feature_inputs()
    frame.attrs['qmt_daily_input_window']['sessions']=[]
    monkeypatch.setattr(reader,'_load_industries',lambda *_a,**_k:pytest.fail('queried before evidence validation'))
    with pytest.raises(ValueError,match='window differs'):
        reader.build_daily_feature_universe(None,as_of=days[-1],dates=days,frame=frame)


def test_prepared_keywords_cannot_fall_back_to_live_config():
    inputs=list(feature_inputs());inputs[2]['research_keywords']=None
    with pytest.raises(ValueError,match='taxonomy'):
        calculate(inputs)


def test_feature_cache_binds_window_proofs_and_scope_not_only_rows():
    from server.engine.strategy_native_backtest import _feature_generation
    state={'feature_algorithm_sha256':'a','day_inputs':{'d':{'ready':{}}},
           'feature_windows':{'d':['d']},'bars':{'d':{'tree':{'same':'rows'},'proof':{'root':'old'},'scope':{'catalog':'one'}}}}
    original=_feature_generation(state,'d')
    state['bars']['d']['proof']['root']='new'
    assert _feature_generation(state,'d')!=original
    state['bars']['d']['proof']['root']='old'
    state['bars']['d']['scope']['catalog']='two'
    assert _feature_generation(state,'d')!=original


def test_comparison_base_binds_raw_sources_and_excludes_derived_context_scores():
    from server.engine.native_research_prepare import base_data_identity
    plan={'identity_rows':{'000001':{'short_name':'银行'}},'identity':{'sha256':'a'}}
    selection={'cutoff':{'decision_at':'2026-09-07 18:00:00'}}
    context={'industries':{'000001':['bank','银行']},'industry_evidence':{'sha256':'b'},
        'theme_memberships':{},'concept_snapshot_date':'2026-09-07','market_context':{
            'context_evidence_status':'DATA_BLOCKED','context_strategy_eligible':False,'context_reason':'unavailable'}}
    def identity(finance='f',event='e'):
        return base_data_identity(plan,selection,context,finance_reference=finance,event_reference=event)
    first=identity()
    assert identity(finance='new')!=first and identity(event='new')!=first
    context['market_context']['context_hash']='different derived model'
    assert identity()==first
    context['industries']['000001']=['broker','证券']
    assert identity()!=first
