from dataclasses import replace
from datetime import datetime,timedelta

import pytest
from sqlalchemy import event

from server.common import pit_facts as pit
from server.common import qmt_announcement_pit as announcements
from server.common import pit_sealed_event_read as materialize
from server.common.formal_analysis_store import FormalAnalysisStore
from server.db.formal_analysis_schema import privileged_migrate_formal_analysis_schema
from server.engine.strategy_native_backtest import NativeStepDeadline
from server.trading_v3.daily_features import _load_recent_notices
from tests.test_qmt_announcement_pit import _engine,_catalog,_install_catalog,_frame


def source():
    engine=_engine();catalog=_install_catalog(engine,_catalog(('000001','000001.SZ'),('000002','000002.SZ')))
    for code in catalog.codes:
        row={'stock_code':code,'report_date':'2026-03-31','report_type':'Q1','notice_date':'2026-04-25','roe_wtd':14.5}
        revision=pit.append_finance_revision(engine,row,known_at='2026-08-25 18:01:00')
        pit.append_source_coverage(engine,fact_kind='finance',stock_code=code,window_start='1900-01-01',
            window_end='2026-08-25',known_at='2026-08-25 18:01:00',covered_through_at='2026-08-25 18:01:00',
            watermark_kind='CAPTURED_AT',watermark_evidence={'source_call':'success'},source_rows=[row],
            fact_bindings=[{'revision_id':revision.revision_id,'content_hash':revision.content_hash}],
            source='adata.finance.core_index',batch_id='finance')
    pit.append_finance_atomic_batch_seal(engine,as_of_date='2026-08-25',completed_known_at='2026-08-25 18:22:00')
    cutoff=datetime(2026,8,25,18,20);received=cutoff+timedelta(minutes=5);start=cutoff.date()-timedelta(days=30)
    results={code:announcements.parse_qmt_announcement_frame(stock_code=code,qmt_code=catalog.qmt_by_code[code],
        frame=_frame(code,'2026-08-25 17:50:00') if code=='000001' else _frame(code,'2026-08-25 17:50:00').iloc[:0],
        fact_cutoff_at=cutoff,window_start=start) for code in catalog.codes}
    root,entries=announcements.build_batch_root(batch_id='events',fact_cutoff_at=cutoff,received_at=received,
        window_start=start,window_end=cutoff.date(),catalog=catalog,results=results)
    announcements._publish_batch(engine,batch_id='events',batch_root_hash=root,entries=entries,fact_cutoff_at=cutoff,
        received_at=received,window_start=start,window_end=cutoff.date(),catalog=catalog,results=results)
    selected=[]
    resolved=pit.resolve_common_fact_cutoff(engine,codes=catalog.codes,decision_at=datetime(2026,8,25,19),
        finance_start_date='1900-01-01',finance_end_date='2026-08-25',event_start_date='2026-08-05',event_end_date='2026-08-25',
        require_qmt_event_batch=True,event_trade_date='2026-08-25',capture_historical_selection=selected.append)
    assert resolved['status']==pit.PIT_AVAILABLE
    privileged_migrate_formal_analysis_schema(engine)
    return engine,catalog.codes,selected[0]


def test_interrupted_event_materialization_reuses_committed_issuers_and_keeps_exact_pit_result(monkeypatch):
    engine,codes,selection=source();store=FormalAnalysisStore(engine)
    full=pit.load_event_facts(engine,codes=codes,decision_at=selection.selection.decision_at,
        fact_cutoff_at=selection.fact_cutoff_at,start_date=selection.selection.window_start,
        end_date=selection.selection.window_end,require_qmt_complete_batch=True,historical_selection=selection)
    assert full.reason_for('000002')=='PIT_EVENT_AUTHORITATIVE_EMPTY'
    monkeypatch.setattr(materialize,'ISSUER_BATCH_SIZE',1)
    progress=[]
    def interrupt(done,total,generation,fresh):
        progress.append((done,total,generation,fresh))
        raise NativeStepDeadline('interrupted after durable event issuer')
    with pytest.raises(NativeStepDeadline):
        materialize.load_sealed_event_facts(engine,selection=selection,codes=codes,formal_store=store,read_progress=interrupt)
    assert len(store.list_members(progress[0][2],'event_issuer_facts'))==1
    reads=[]
    def record(_c,_cursor,statement,parameters,_context,_many):
        if pit.EVENT_REVISION_TABLE in statement:reads.append(str(parameters))
    event.listen(engine,'before_cursor_execute',record)
    resumed=[]
    try:
        actual=materialize.load_sealed_event_facts(engine,selection=selection,codes=codes,formal_store=store,
            read_progress=lambda *values:resumed.append(values))
        assert actual==full
        assert all('000001' not in parameters for parameters in reads)
        assert [row[3] for row in resumed]==[False,True]
        reads.clear()
        assert materialize.load_sealed_event_facts(engine,selection=selection,codes=codes,formal_store=store)==full
        assert reads==[]
        expected=_load_recent_notices(engine,as_of=selection.selection.window_end,codes=codes,
            decision_at=selection.selection.decision_at,fact_cutoff_at=selection.fact_cutoff_at,
            historical_selection=selection,include_evidence=True)
        reads.clear()
        assert _load_recent_notices(engine,as_of=selection.selection.window_end,codes=codes,
            decision_at=selection.selection.decision_at,fact_cutoff_at=selection.fact_cutoff_at,
            historical_selection=selection,prepared_batch=actual,include_evidence=True)==expected
        assert reads==[]
    finally:event.remove(engine,'before_cursor_execute',record)
    wrong=replace(selection,selection=replace(selection.selection,batch={**selection.selection.batch,'batch_id':'wrong-batch'}))
    with pytest.raises(ValueError,match='PREPARED_SNAPSHOT_DIFFERS'):
        _load_recent_notices(engine,as_of=selection.selection.window_end,codes=codes,
            decision_at=selection.selection.decision_at,fact_cutoff_at=selection.fact_cutoff_at,
            historical_selection=wrong,prepared_batch=actual)
    with pytest.raises(ValueError,match='SCOPE_DIFFERS'):
        materialize.load_sealed_event_facts(engine,selection=selection,codes=['999999'],formal_store=store)


def test_failed_event_read_is_not_published_as_a_completed_issuer(monkeypatch):
    engine,codes,selection=source();store=FormalAnalysisStore(engine);progress=[]
    monkeypatch.setattr(pit,'load_event_facts',lambda *_a,**_k:pit.PITFactBatch(
        global_status=pit.PIT_SCHEMA_UNAVAILABLE,reason_by_code={code:'PIT_EVENT_SCHEMA_UNAVAILABLE:OperationalError' for code in codes}))
    with pytest.raises(RuntimeError,match='PIT_SEALED_EVENT_READ_FAILED'):
        materialize.load_sealed_event_facts(engine,selection=selection,codes=codes,formal_store=store,
            read_progress=lambda *values:progress.append(values))
    assert progress==[]
