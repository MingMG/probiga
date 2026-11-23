from copy import deepcopy
from datetime import timedelta

import pandas as pd
import pytest
from sqlalchemy import text

from server.engine import strategy_native_backtest as native
from server.engine import strategy_native_control as control
from server.engine import native_research_jobs as jobs
from server.engine.strategy_rule_backtest import _partitions
from tests.test_native_research_jobs import queue


def legacy(queue,monkeypatch):
    dates=[str(day.date()) for day in pd.bdate_range('2026-06-01',periods=100)]
    strategy={'strategy_key':'native','strategy_name':'native','source_kind':'immutable_v3_sleeve',
              'current_version':'v1','version_hash':'a'*64,'parameters':{},'evaluator_config':{}}
    state={'schema':native.SCHEMA,'run_id':'c'*64,'rule_hash':'d'*64,'contract':'old-control-hash',
        'phase':'SIGNALS','status':'FAILED','source_hash':'e'*64,'strategies':[strategy],
        'sessions':dates[-2:],'published':{},'requested_at':'2026-11-22 18:05:19',
        'price_cutoff':'2026-11-22 05:59:24','deadline_at':'2026-11-23 18:05:19',
        'input_archives':[],'checkpoint_sequence':1,'cursor':1,'completed_units':71,'total_units':408,
        'proposals':[{'rule_hash':'a'*64,'strategy_key':'native','validation':{}}]}
    with queue.begin() as connection:
        connection.execute(text('CREATE TABLE si_trade_calendar(trade_date TEXT,trade_status INT)'))
        connection.execute(text('INSERT INTO si_trade_calendar VALUES(:day,1)'),[{'day':day} for day in dates])
        native._append_state(connection,state,[native._part(state,'S',state['sessions'][0],{'status':'BLOCKED','reason':'verified historical absence'})])
    monkeypatch.setattr(native,'_strategies',lambda _connection:[strategy])
    from server.engine import autonomous_strategy_research as research
    monkeypatch.setattr(research,'_read_states',lambda *_args,**_kwargs:[])
    return state


def test_legacy_recovery_registers_one_typed_job_without_copying_unproven_signals(queue,monkeypatch):
    old=legacy(queue,monkeypatch)
    request=control.request_retry(queue,'native:'+old['run_id']+':'+('a'*64),'2'*32)
    assert request['business_started'] is False and request['target_run_id']!=old['run_id']
    assert control.request_retry(queue,'native:'+old['run_id']+':'+('a'*64),'2'*32)==request
    with queue.connect() as connection:
        current=native.read_runs(connection,run_id=request['target_run_id'])[0]
        for key in ('sessions','published','source_hash','requested_at','price_cutoff'):
            assert current[key]==old[key]
        assert current['phase']=='PLAN' and current['cursor']==0
        assert current['input_archives']==[{'run_id':old['run_id'],'rule_hash':old['rule_hash']}]
        assert _partitions(connection,current,current,'S')=={}
        assert len(_partitions(connection,old,old,'S'))==1
        assert native.read_runs(connection,run_id=old['run_id'])[0]['status']=='FAILED'
        assert connection.execute(text('SELECT COUNT(*) FROM st_job_v2')).scalar()==1


def test_recovery_preserves_original_freeze_across_executor_only_changes(queue,monkeypatch):
    old=legacy(queue,monkeypatch)
    with queue.connect() as connection:
        first=native._start(connection,old['strategies'],old['sessions'],{},old['source_hash'],frozen=old)
        monkeypatch.setenv('PROBIGA_BUILD_COMMIT_SHA','f'*40)
        second=native._start(connection,old['strategies'],old['sessions'],{},old['source_hash'],frozen=old)
    assert first['run_id']==second['run_id']
    assert first['requested_at']==second['requested_at']==old['requested_at']


def test_new_source_transform_has_a_distinct_identity_with_unchanged_raw_clocks(queue,monkeypatch):
    old=legacy(queue,monkeypatch)
    from server.engine import native_research_identity as identity
    with queue.connect() as connection:
        first=native._start(connection,old['strategies'],old['sessions'],{},old['source_hash'],frozen=old)
        producer=identity.producer_identity()
        monkeypatch.setattr(identity,'producer_identity',lambda *_a:{**producer,'changed-reader':'f'*64})
        second=native._start(connection,old['strategies'],old['sessions'],{},old['source_hash'],frozen=old)
    assert first['run_id']!=second['run_id'] and first['contract']==second['contract']
    assert first['requested_at']==second['requested_at']==old['requested_at']
    assert first['price_cutoff']==second['price_cutoff']==old['price_cutoff']
