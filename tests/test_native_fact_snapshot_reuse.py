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


@pytest.mark.parametrize('errno',[3024,2013,1142])
@pytest.mark.parametrize('boundary',['inventory','proof_cache'])
def test_dated_database_fault_preserves_diagnostics_and_retry_classification(monkeypatch,caplog,errno,boundary):
    from sqlalchemy import event
    from sqlalchemy.exc import OperationalError
    from server.common.formal_analysis_store import FormalAnalysisStore
    from server.db.formal_analysis_schema import privileged_migrate_formal_analysis_schema,MEMBER_TABLE
    from server.engine import strategy_native_backtest as native
    engine=_engine();catalog=_install_catalog(engine,_catalog(('000001','000001.SZ')))
    publish(engine,catalog,datetime(2026,8,25,18,20))
    privileged_migrate_formal_analysis_schema(engine)
    options=dict(codes=catalog.codes,decision_at=datetime(2026,8,27),window_start=date(2026,8,5),
        window_end=date(2026,8,25),expected_trade_date='2026-08-25',coverage_store=FormalAnalysisStore(engine))
    faults=[]
    def fail_once(_c,_cursor,statement,parameters,_context,_many):
        matches=("WHERE fact_kind='event' AND stock_code=" in statement if boundary=='inventory' else MEMBER_TABLE in statement)
        if matches and not faults:
            faults.append(statement)
            raise OperationalError(statement,parameters,Exception(errno,'observed database read failure'))
    event.listen(engine,'before_cursor_execute',fail_once)
    try:
        expected_error=announcements.QMTAnnouncementBlocked if errno in (3024,2013) else OperationalError
        with pytest.raises(expected_error) as failure:
            history.validate_dated_announcement_batch(engine,**options)
        reason=pit._announcement_batch_read_reason(failure.value)
        assert native._transient_source_error(RuntimeError('历史事实读取失败：'+reason))==(errno in (3024,2013))
        assert any(str(errno) in record.message and '2026-08-25' in record.message for record in caplog.records)
        assert len(faults)==1
        assert history.validate_dated_announcement_batch(engine,**options)['status']=='COMPLETE'
    finally:
        event.remove(engine,'before_cursor_execute',fail_once)
        engine.dispose()


def test_completed_live_proof_is_reused_before_resuming_ancestry_budget(monkeypatch):
    from server.common.formal_analysis_store import FormalAnalysisStore
    from server.db.formal_analysis_schema import privileged_migrate_formal_analysis_schema
    from server.engine import native_research_jobs as jobs, strategy_native_backtest as native
    engine=_engine()
    catalog=_install_catalog(engine,_catalog(('000001','000001.SZ'),('000002','000002.SZ')))
    publish(engine,catalog,datetime(2026,8,25,18,20))
    privileged_migrate_formal_analysis_schema(engine)
    options=dict(codes=catalog.codes,decision_at=datetime(2026,8,27),window_start=date(2026,8,5),
        window_end=date(2026,8,25),expected_trade_date='2026-08-25')
    expected=[]
    proof=history.validate_dated_announcement_batch(engine,**options,capture_historical_selection=expected.append)
    clock=[0.]
    validations=[]
    validate=history.validate_complete_qmt_announcement_batch
    def expensive_validation(*args,**kwargs):
        value=validate(*args,**kwargs)
        clock[0]+=146  # Production full-batch validation exceeded the 90-second source slice.
        validations.append(value)
        return value
    monkeypatch.setattr(history,'validate_complete_qmt_announcement_batch',expensive_validation)
    monkeypatch.setattr(jobs.time,'monotonic',lambda:clock[0])
    monkeypatch.setattr(native,'_save',lambda *_args,**_kwargs:None)
    monkeypatch.setattr(announcements,'VALIDATION_INVENTORY_CODE_LIMIT',1)
    state={}
    store=FormalAnalysisStore(engine)
    def progress():
        return jobs._source_progress(None,state,None,'2026-08-25','event_coverage',
            deadline=clock[0]+90,checkpoint_interval=0)
    with pytest.raises(jobs.PrepareYield):
        history._bind_dated_live_selection(engine,proof,
            {code:row for code,(row,_head) in expected[0].members.items()},codes=list(catalog.codes),
            decision=options['decision_at'],start=options['window_start'],end=options['window_end'],
            deadline=history.monotonic()+90,coverage_store=store,
            coverage_progress=jobs._source_progress(None,state,None,'2026-08-25','event_coverage',
                deadline=-1,checkpoint_interval=0))
    prior_cursors={key:value['completed'] for key,value in state['source_cursors'].items()}
    with pytest.raises(jobs.PrepareYield):
        callback=progress()
        history.validate_dated_announcement_batch(engine,**options,coverage_store=store,
            capture_historical_selection=lambda _value:None,coverage_progress=callback,metadata_progress=callback)
    assert state['source_progress']['completed']==1
    assert state['source_progress']['scope'].startswith('dated-live-validation:')
    assert all(state['source_cursors'][key]['completed']==value for key,value in prior_cursors.items())
    selected=[]
    callback=progress()
    actual=history.validate_dated_announcement_batch(engine,**options,coverage_store=store,
        capture_historical_selection=selected.append,coverage_progress=callback,metadata_progress=callback)
    assert actual==proof and state['source_progress']['completed']==2
    assert len(validations)==1
    assert pit._json_safe(selected[0].members)==pit._json_safe(expected[0].members)
    engine.dispose()


def test_cached_live_binding_replays_plain_progress_after_slow_preparation(monkeypatch):
    from server.common.formal_analysis_store import FormalAnalysisStore
    from server.db.formal_analysis_schema import privileged_migrate_formal_analysis_schema
    from server.engine import native_research_jobs as jobs, strategy_native_backtest as native
    engine=_engine();catalog=_install_catalog(engine,_catalog(('000001','000001.SZ'),('000002','000002.SZ')))
    publish(engine,catalog,datetime(2026,8,25,18,20))
    privileged_migrate_formal_analysis_schema(engine)
    monkeypatch.setattr(announcements,'VALIDATION_INVENTORY_CODE_LIMIT',1)
    monkeypatch.setattr(native,'_save',lambda *_args,**_kwargs:None)
    options=dict(codes=catalog.codes,decision_at=datetime(2026,8,27),window_start=date(2026,8,5),
        window_end=date(2026,8,25),expected_trade_date='2026-08-25',coverage_store=FormalAnalysisStore(engine))
    state={}
    callback=jobs._source_progress(None,state,None,'2026-08-25','event_selection',deadline=float('inf'))
    def retain(done,total):
        callback(done,total)
        if done==1:raise jobs.PrepareYield('retained binding')
    with pytest.raises(jobs.PrepareYield):
        history.validate_dated_announcement_batch(engine,**options,event_progress=retain,capture_historical_selection=lambda _value:None)
    clock=[0.]
    monkeypatch.setattr(jobs.time,'monotonic',lambda:clock[0])
    callback=jobs._source_progress(None,state,None,'2026-08-25','event_selection',deadline=90)
    clock[0]=146  # Finance validation precedes this observational progress callback.
    captured=[]
    proof=history.validate_dated_announcement_batch(engine,**options,event_progress=callback,capture_historical_selection=captured.append)
    assert proof['status']=='COMPLETE' and set(captured[0].members)==set(catalog.codes)
    assert state['source_progress']['completed']==state['source_progress']['total']==2
    engine.dispose()


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
    inputs=daily_features.DailyFactInputs({'status':pit.PIT_AVAILABLE,
        'source_cutoff_at':{'finance':str(cutoff),'event':str(cutoff)}},sealed,pit.HistoricalEventRead(selection,cutoff))
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


@pytest.mark.parametrize('event_hour',[18,20])
def test_verified_finance_and_event_inputs_are_reused_by_feature_readers(monkeypatch,event_hour):
    engine=_engine();catalog=_install_catalog(engine,_catalog(('000001','000001.SZ'),('000002','000002.SZ')),captured_at='2026-08-25 14:00:00')
    for code in catalog.codes:
        row={'stock_code':code,'report_date':'2026-03-31','report_type':'Q1','notice_date':'2026-04-25','roe_wtd':14.5}
        revision=pit.append_finance_revision(engine,row,known_at='2026-08-25 15:01:00')
        pit.append_source_coverage(engine,fact_kind='finance',stock_code=code,window_start='1900-01-01',window_end='2026-08-25',
            known_at='2026-08-25 15:01:00',covered_through_at='2026-08-25 15:01:00',watermark_kind='CAPTURED_AT',
            watermark_evidence={'source_call':'success'},source_rows=[row],
            fact_bindings=[{'revision_id':revision.revision_id,'content_hash':revision.content_hash}],
            source='adata.finance.core_index',batch_id='finance-native')
    pit.append_finance_atomic_batch_seal(engine,as_of_date='2026-08-25',completed_known_at='2026-08-25 15:11:00')
    publish(engine,catalog,datetime(2026,8,25,event_hour,20))
    finances,events=[],[];decision=datetime(2026,8,25,23)
    result=pit.resolve_strategy_fact_cutoffs(engine,codes=catalog.codes,decision_at=decision,
        finance_start_date='1900-01-01',finance_end_date='2026-08-25',event_start_date='2026-08-05',event_end_date='2026-08-25',
        require_qmt_event_batch=True,event_trade_date='2026-08-25',
        capture_finance_selection=finances.append,capture_historical_selection=events.append)
    assert result['status']==pit.PIT_AVAILABLE and len(finances)==len(events)==1
    assert finances[0].fact_cutoff_at==datetime.fromisoformat(result['source_cutoff_at']['finance'])
    assert events[0].fact_cutoff_at==datetime.fromisoformat(result['source_cutoff_at']['event'])
    blocked=pit.resolve_common_fact_cutoff(engine,codes=catalog.codes,decision_at=decision,
        finance_start_date='1900-01-01',finance_end_date='2026-08-25',
        event_start_date='2026-08-05',event_end_date='2026-08-25',
        require_qmt_event_batch=True,event_trade_date='2026-08-25')
    assert blocked['reason']=='PIT_COMMON_CUTOFF_STALE_OR_BACKFILL'
    monkeypatch.setattr(pit,'load_finance_atomic_batch_seal',lambda *_a,**_k:pytest.fail('verified finance was scanned again'))
    monkeypatch.setattr(announcements,'validate_complete_announcement_batch',lambda *_a,**_k:pytest.fail('verified event batch was scanned again'))
    financial=daily_features._load_finance(engine,as_of=date(2026,8,25),codes=['000001'],decision_at=decision,
        fact_cutoff_at=result['source_cutoff_at']['finance'],sealed_finance_inputs=finances[0])
    assert financial['000001']['finance_pit_status']==pit.PIT_AVAILABLE
    subset=pit.subset_historical_event_read(events[0],['000001'])
    notices,evidence=daily_features._load_recent_notices(engine,as_of=date(2026,8,25),codes=['000001'],decision_at=decision,
        fact_cutoff_at=result['source_cutoff_at']['event'],include_evidence=True,historical_selection=subset)
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
        decision_at=decision,fact_cutoff_at=result['source_cutoff_at']['finance'],as_of_date=date(2026,8,25),
        formal_store=FormalAnalysisStore(engine),formal_generation_sha256=generation)
    monkeypatch.setattr(daily_features,'load_finance_facts',lambda *_a,**_k:pytest.fail('prepared issuer facts were read again'))
    values=daily_features._load_finance(engine,as_of=date(2026,8,25),codes=['000001'],decision_at=decision,
        fact_cutoff_at=result['source_cutoff_at']['finance'],sealed_finance_inputs=restored.finance,prepared_batch=financial)
    assert values['000001']['roe_wtd']==14.5
    with pytest.raises(ValueError,match='FINANCE_SNAPSHOT_DIFFERS'):
        daily_features._load_finance(engine,as_of=date(2026,8,26),codes=['000001'],decision_at=decision,
            fact_cutoff_at=result['source_cutoff_at']['finance'],sealed_finance_inputs=restored.finance,prepared_batch=financial)
    frozen=pit.subset_historical_event_read(restored.events,['000001'])
    restored_notices,restored_evidence=daily_features._load_recent_notices(engine,as_of=date(2026,8,25),
        codes=['000001'],decision_at=decision,fact_cutoff_at=result['source_cutoff_at']['event'],
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

    # The new independent preparation stages consume the same verified selections.
    from server.engine import strategy_native_backtest as native
    from server.engine import strategy_price_backtest as price
    from server.engine import native_research_prepare as prepare
    monkeypatch.undo()
    monkeypatch.setattr(price,'_identity_rows',lambda *_:({code:{} for code in catalog.codes},
        {'status':'VERIFIED','sha256':'a'*64,'captured_at':'2026-08-25 21:47:00'}))
    monkeypatch.setattr(native,'_save',lambda *_:None)
    state={'requested_at':str(decision),'proposals':[]}
    with engine.connect() as connection:
        plan=prepare.plan_v3_day(connection,state,'2026-08-25')
        selection=prepare.prepare_selection(connection,state,None,plan)
        assert selection['status']=='COMPLETE'
        prepared_finance=prepare.prepare_finance(connection,state,None,plan,selection)
        prepared_events=prepare.prepare_events(connection,state,None,plan,selection)
    assert prepared_finance['000001']['roe_wtd']==14.5
    assert prepared_events['notices']['000001']==restored_notices['000001']
    assert set(prepared_events['evidence']['status_by_code'])==set(catalog.codes)
    assert prepared_events['evidence']['status']=='AVAILABLE'


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
