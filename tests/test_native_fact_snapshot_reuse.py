from datetime import date,datetime,timedelta

import pytest
from sqlalchemy import text

from server.common import pit_facts as pit
from server.common import qmt_announcement_pit as announcements
from server.common import historical_announcement_read as history
from server.trading_v3 import daily_features
from tests.test_qmt_announcement_pit import _engine,_catalog,_install_catalog,_frame


def publish(engine,catalog,cutoff):
    received=cutoff+timedelta(minutes=5)
    start=cutoff.date()-timedelta(days=30)
    results={code:announcements.parse_qmt_announcement_frame(stock_code=code,qmt_code=catalog.qmt_by_code[code],
        frame=_frame(code,'2026-08-25 17:50:00'),fact_cutoff_at=cutoff,window_start=start) for code in catalog.codes}
    batch='batch-'+cutoff.strftime('%Y%m%d')
    root,entries=announcements.build_batch_root(batch_id=batch,fact_cutoff_at=cutoff,received_at=received,
        window_start=start,window_end=cutoff.date(),catalog=catalog,results=results)
    announcements._publish_batch(engine,batch_id=batch,batch_root_hash=root,entries=entries,
        fact_cutoff_at=cutoff,received_at=received,window_start=start,window_end=cutoff.date(),catalog=catalog,results=results)
    return batch


def test_historical_batch_selection_does_not_load_a_later_window():
    engine=_engine();catalog=_install_catalog(engine,_catalog(('000001','000001.SZ'),('000002','000002.SZ')))
    old=publish(engine,catalog,datetime(2026,8,25,18,20))
    future=publish(engine,catalog,datetime(2026,8,26,18,20))
    options={'codes':catalog.codes,'decision_at':datetime(2026,8,27),'window_start':date(2026,8,5),'window_end':date(2026,8,25)}
    assert announcements.validate_complete_qmt_announcement_batch(engine,**options)['batch_id']==future
    assert history.validate_dated_announcement_batch(engine,**options,expected_trade_date='2026-08-25')['batch_id']==old
    assert not history.has_dated_announcement_candidate(engine,code='000001',decision_at=datetime(2026,8,27),
        target=date(2026,8,24),window_start=date(2026,8,4))


def test_full_market_snapshot_reads_fourteen_chunks_with_bounded_artifact_batches():
    # Synthetic identifiers exercise the transport limit; no prices or profits are generated.
    from types import MappingProxyType
    from server.engine.strategy_native_fact_snapshot import save_snapshot,load_snapshot
    from server.db.formal_analysis_schema import privileged_migrate_formal_analysis_schema
    engine=_engine();privileged_migrate_formal_analysis_schema(engine)
    decision=datetime(2026,9,8);cutoff=datetime(2026,9,7,16)
    codes=[str(i).zfill(6) for i in range(5600)]
    financial={code:{'coverage_id':pit.canonical_hash(code)} for code in codes}
    members={code:(MappingProxyType({'stock_code':code,'coverage_id':pit.canonical_hash(code),
        'known_at':cutoff,'window_end':cutoff.date()}),True) for code in codes}
    selection=pit._LiveEventSelection(engine,decision,date(2026,8,18),cutoff.date(),
        MappingProxyType({'coverage_count':len(codes)}),MappingProxyType(members))
    sealed=pit.SealedFinanceInputs(engine,decision,cutoff,cutoff.date(),'a'*64,'b'*64,
        MappingProxyType(financial),MappingProxyType({}))
    inputs=daily_features.DailyFactInputs({'status':pit.PIT_AVAILABLE},sealed,pit.HistoricalEventRead(selection,cutoff))
    save_snapshot(engine,'c'*64,inputs)
    from server.common.formal_analysis_store import FormalAnalysisStore
    store=FormalAnalysisStore(engine)
    header=store.get('c'*64,'native_fact_snapshot','complete')
    assert len(header['chunks'])==14
    with pytest.raises(ValueError,match='artifact read count limit'):
        store.load_artifacts(list(header['chunks'].values()))
    restored=load_snapshot(engine,'c'*64)
    assert len(restored.finance.members)==len(restored.events.selection.members)==5600
    assert restored.finance.members==inputs.finance.members
    assert restored.events.selection.members[codes[-1]][0]['coverage_id']==pit.canonical_hash(codes[-1])


def test_verified_finance_and_event_inputs_are_reused_by_feature_readers(monkeypatch):
    engine=_engine();catalog=_install_catalog(engine,_catalog(('000001','000001.SZ'),('000002','000002.SZ')))
    for code in catalog.codes:
        row={'stock_code':code,'report_date':'2026-03-31','report_type':'Q1','notice_date':'2026-04-25','roe_wtd':14.5}
        revision=pit.append_finance_revision(engine,row,known_at='2026-08-25 18:01:00')
        pit.append_source_coverage(engine,fact_kind='finance',stock_code=code,window_start='1900-01-01',window_end='2026-08-25',
            known_at='2026-08-25 18:01:00',covered_through_at='2026-08-25 18:01:00',watermark_kind='CAPTURED_AT',
            watermark_evidence={'source_call':'success'},source_rows=[row],
            fact_bindings=[{'revision_id':revision.revision_id,'content_hash':revision.content_hash}],
            source='adata.finance.core_index',batch_id='finance-native')
    pit.append_finance_atomic_batch_seal(engine,as_of_date='2026-08-25',completed_known_at='2026-08-25 18:22:00')
    publish(engine,catalog,datetime(2026,8,25,18,20))
    finances,events=[],[];decision=datetime(2026,8,25,19)
    result=pit.resolve_common_fact_cutoff(engine,codes=catalog.codes,decision_at=decision,
        finance_start_date='1900-01-01',finance_end_date='2026-08-25',event_start_date='2026-08-05',event_end_date='2026-08-25',
        require_qmt_event_batch=True,event_trade_date='2026-08-25',
        capture_finance_selection=finances.append,capture_historical_selection=events.append)
    assert result['status']==pit.PIT_AVAILABLE and len(finances)==len(events)==1
    assert finances[0].fact_cutoff_at==datetime.fromisoformat(result['fact_cutoff_at'])
    monkeypatch.setattr(pit,'load_finance_atomic_batch_seal',lambda *_a,**_k:pytest.fail('verified finance was scanned again'))
    monkeypatch.setattr(announcements,'validate_complete_announcement_batch',lambda *_a,**_k:pytest.fail('verified event batch was scanned again'))
    financial=daily_features._load_finance(engine,as_of=date(2026,8,25),codes=['000001'],decision_at=decision,
        fact_cutoff_at=result['fact_cutoff_at'],sealed_finance_inputs=finances[0])
    assert financial['000001']['finance_pit_status']==pit.PIT_AVAILABLE
    subset=pit.subset_historical_event_read(events[0],['000001'])
    notices,evidence=daily_features._load_recent_notices(engine,as_of=date(2026,8,25),codes=['000001'],decision_at=decision,
        fact_cutoff_at=result['fact_cutoff_at'],include_evidence=True,historical_selection=subset)
    assert len(notices['000001'])==1 and evidence['status_by_code']['000001']==pit.PIT_AVAILABLE
    assert len(events[0].selection.members)==2
    with pytest.raises(ValueError,match='SUBSET_DIFFERS'):pit.subset_historical_event_read(events[0],['999999'])
    from server.engine.strategy_native_fact_snapshot import save_snapshot,load_snapshot
    from server.common.formal_analysis_store import FormalAnalysisStore
    from server.common.pit_sealed_finance_read import load_sealed_finance_facts
    from server.db.formal_analysis_schema import privileged_migrate_formal_analysis_schema
    privileged_migrate_formal_analysis_schema(engine)
    inputs=daily_features.DailyFactInputs(result,finances[0],events[0]);generation='d'*64
    save_snapshot(engine,generation,inputs)
    restored=load_snapshot(engine,generation)
    assert restored.finance is not inputs.finance and restored.finance.engine is engine
    assert restored.cutoff==inputs.cutoff and restored.finance.members==inputs.finance.members
    assert restored.events.selection.batch==inputs.events.selection.batch
    financial=load_sealed_finance_facts(engine,selection=restored.finance,codes=catalog.codes,
        decision_at=decision,fact_cutoff_at=result['fact_cutoff_at'],as_of_date=date(2026,8,25),
        formal_store=FormalAnalysisStore(engine),formal_generation_sha256=generation)
    monkeypatch.setattr(daily_features,'load_finance_facts',lambda *_a,**_k:pytest.fail('prepared issuer facts were read again'))
    values=daily_features._load_finance(engine,as_of=date(2026,8,25),codes=['000001'],decision_at=decision,
        fact_cutoff_at=result['fact_cutoff_at'],sealed_finance_inputs=restored.finance,prepared_batch=financial)
    assert values['000001']['roe_wtd']==14.5
    with pytest.raises(ValueError,match='FINANCE_SNAPSHOT_DIFFERS'):
        daily_features._load_finance(engine,as_of=date(2026,8,26),codes=['000001'],decision_at=decision,
            fact_cutoff_at=result['fact_cutoff_at'],sealed_finance_inputs=restored.finance,prepared_batch=financial)
    frozen=pit.subset_historical_event_read(restored.events,['000001'])
    restored_notices,restored_evidence=daily_features._load_recent_notices(engine,as_of=date(2026,8,25),
        codes=['000001'],decision_at=decision,fact_cutoff_at=result['fact_cutoff_at'],
        include_evidence=True,historical_selection=frozen)
    assert restored_notices==notices and restored_evidence==evidence
    assert load_snapshot(engine,'e'*64) is None
    original=FormalAnalysisStore.put
    def interrupted(store,generation,stage,key,payload):
        if stage=='native_fact_snapshot':raise RuntimeError('interrupted before complete snapshot receipt')
        return original(store,generation,stage,key,payload)
    with monkeypatch.context() as partial:
        partial.setattr(FormalAnalysisStore,'put',interrupted)
        with pytest.raises(RuntimeError,match='complete snapshot receipt'):
            save_snapshot(engine,'f'*64,inputs)
    assert load_snapshot(engine,'f'*64) is None


def test_dated_reader_keeps_tampered_primary_batch_visible():
    engine=_engine();catalog=_install_catalog(engine,_catalog(('000001','000001.SZ')))
    publish(engine,catalog,datetime(2026,8,25,18,20))
    with engine.begin() as c:
        c.execute(text('DROP TRIGGER trg_pit_source_coverage_immutable_bu'))
        c.execute(text("UPDATE st_pit_source_coverage SET payload_json='{}'"))
    with pytest.raises(announcements.QMTAnnouncementBlocked,match='ValueError'):
        history.validate_dated_announcement_batch(engine,codes=catalog.codes,decision_at=datetime(2026,8,27),
            window_start=date(2026,8,5),window_end=date(2026,8,25),expected_trade_date=date(2026,8,25))


def test_job_deadline_survives_real_source_validation_wrappers(monkeypatch):
    from server.engine.strategy_native_backtest import NativeStepDeadline
    engine=_engine();catalog=_install_catalog(engine,_catalog(('000001','000001.SZ')))
    publish(engine,catalog,datetime(2026,8,25,18,20))
    def interrupted(*_a,**_k):raise NativeStepDeadline('source read exceeded the job budget')
    monkeypatch.setattr(announcements,'_load_live_announcement_batch_rows',interrupted)
    with pytest.raises(NativeStepDeadline):
        history.validate_dated_announcement_batch(engine,codes=catalog.codes,decision_at=datetime(2026,8,27),
            window_start=date(2026,8,5),window_end=date(2026,8,25),expected_trade_date=date(2026,8,25))


def test_historical_finance_clock_keeps_later_seals_and_restatements_out():
    engine=_engine();catalog=_install_catalog(engine,_catalog(('000001','000001.SZ')))
    for day,roe in ((25,14.5),(26,99.0)):
        clock=datetime(2026,8,day,18,1)
        row={'stock_code':'000001','report_date':'2026-03-31','report_type':'Q1',
            'notice_date':'2026-04-25','roe_wtd':roe}
        revision=pit.append_finance_revision(engine,row,known_at=clock)
        pit.append_source_coverage(engine,fact_kind='finance',stock_code='000001',window_start='1900-01-01',
            window_end=clock.date(),known_at=clock,covered_through_at=clock,watermark_kind='CAPTURED_AT',
            watermark_evidence={'source_call':'success'},source_rows=[row],
            fact_bindings=[{'revision_id':revision.revision_id,'content_hash':revision.content_hash}],
            source='adata.finance.core_index',batch_id='finance-'+str(day))
        pit.append_finance_atomic_batch_seal(engine,as_of_date=clock.date(),completed_known_at=clock+timedelta(minutes=9))
    publish(engine,catalog,datetime(2026,8,25,18,20))
    captured=[];decision=datetime(2026,8,27)
    args=dict(codes=['000001'],decision_at=decision,finance_start_date='1900-01-01',finance_end_date='2026-08-25',
        event_start_date='2026-08-05',event_end_date='2026-08-25',require_qmt_event_batch=True,event_trade_date='2026-08-25')
    latest=[]
    live_clock=pit.resolve_common_fact_cutoff(engine,**args,capture_finance_selection=latest.append)
    with pytest.raises(ValueError,match='COVERAGE_MEMBERSHIP_DIFFERS'):
        daily_features._load_finance(engine,as_of=date(2026,8,25),codes=['000001'],decision_at=decision,
            fact_cutoff_at=live_clock['fact_cutoff_at'],sealed_finance_inputs=latest[0])
    result=pit.resolve_common_fact_cutoff(engine,**args,finance_observation_at=datetime(2026,8,25,23,59),capture_finance_selection=captured.append)
    assert result['status']==pit.PIT_AVAILABLE and len(captured)==1
    assert captured[0].members['000001']['known_at'].startswith('2026-08-25')
    facts=daily_features._load_finance(engine,as_of=date(2026,8,25),codes=['000001'],decision_at=decision,
        fact_cutoff_at=result['fact_cutoff_at'],sealed_finance_inputs=captured[0])
    assert facts['000001']['roe_wtd']==14.5
    missing=pit.resolve_common_fact_cutoff(engine,**args,finance_observation_at=datetime(2026,8,24,23,59))
    assert missing['status']==pit.PIT_DATA_BLOCKED and missing['reason']=='PIT_HISTORICAL_FINANCE_BATCH_NOT_FOUND'
    with pytest.raises(ValueError,match='postdate'):
        pit.resolve_common_fact_cutoff(engine,**args,finance_observation_at=datetime(2026,8,28))
    cutoff=datetime(2026,8,25,18,5);received=datetime(2026,8,25,18,26);start=date(2026,7,26)
    rows={'000001':announcements.parse_qmt_announcement_frame(stock_code='000001',qmt_code='000001.SZ',
        frame=_frame('000001','2026-08-25 17:50:00'),fact_cutoff_at=cutoff,window_start=start)}
    root,entries=announcements.build_batch_root(batch_id='catchup-event',fact_cutoff_at=cutoff,received_at=received,
        window_start=start,window_end=cutoff.date(),catalog=catalog,results=rows)
    announcements._publish_batch(engine,batch_id='catchup-event',batch_root_hash=root,entries=entries,
        fact_cutoff_at=cutoff,received_at=received,window_start=start,window_end=cutoff.date(),catalog=catalog,results=rows)
    earlier=pit.resolve_common_fact_cutoff(engine,**args,finance_observation_at=datetime(2026,8,25,23,59))
    assert earlier['status']==pit.PIT_DATA_BLOCKED and earlier['reason']=='PIT_HISTORICAL_FINANCE_BATCH_NOT_FOUND'
