"""Date-bound research reads through the existing audited announcement validators."""
from datetime import date,datetime,timedelta
import json
from time import monotonic

from sqlalchemy import text

from server.common.pit_facts import normalize_decision_at
from server.common.qmt_announcement_pit import (
    QMTAnnouncementBlocked,validate_complete_qmt_announcement_batch,
    QMT_ANNOUNCEMENT_SOURCE,CNINFO_ANNOUNCEMENT_SOURCE,EASTMONEY_ANNOUNCEMENT_SOURCE,
    LIVE_ANNOUNCEMENT_BATCH_VALIDATION_BUDGET_SECONDS,
)


def _inventory(engine,*,code,decision_at,target,window_start,source=None):
    with engine.connect() as c:
        rows=c.execute(text("""SELECT /*+ MAX_EXECUTION_TIME(4000) */ source,batch_id,
                covered_through_at,watermark_kind,known_at,coverage_id
            FROM st_pit_source_coverage WHERE fact_kind='event' AND stock_code=:code
            AND source IN ('qmt.announcement','cninfo.announcement','eastmoney.notice')
            AND (:source IS NULL OR source=:source) AND coverage_status='COMPLETE'
            AND known_at<=:decision AND received_at<=:decision AND window_start<=:start AND window_end>=:target
            AND ((watermark_kind='QUERY_CUTOFF' AND covered_through_at>=:opening AND covered_through_at<:ending)
                OR (watermark_kind='HISTORICAL_RECONSTRUCTION' AND window_end=:target))
            ORDER BY known_at DESC,batch_id DESC LIMIT 129"""),
            {'code':code,'source':source,'decision':decision_at,'start':window_start,'target':target,
             'opening':datetime.combine(target,datetime.min.time()),
             'ending':datetime.combine(target+timedelta(days=1),datetime.min.time())}).mappings().all()
        if len(rows)>128:raise ValueError('historical announcement candidate inventory exceeds 128 batches')
        return [dict(row) for row in rows]


def has_dated_announcement_candidate(engine,*,code,decision_at,target,window_start):
    # One missing required member proves that a complete dated batch cannot exist.
    return bool(_inventory(engine,code=code,decision_at=decision_at,target=target,window_start=window_start))


def _reconstruction_counts(connection, *, source, batch_id, deadline):
    from server.common.qmt_announcement_pit import (
        _live_announcement_read_hint, _live_announcement_reply_budget,
        VALIDATION_INVENTORY_CODE_LIMIT,
    )
    from server.common.pit_source_coverage_index import PIT_SOURCE_COVERAGE_BATCH_INDEX_NAME
    index = f' FORCE INDEX ({PIT_SOURCE_COVERAGE_BATCH_INDEX_NAME})' if connection.dialect.name == 'mysql' else ''
    counts, cursor = [], ''
    while True:
        stage = f'historical-header-inventory:{len(counts)}'
        hint = _live_announcement_read_hint(mysql=connection.dialect.name == 'mysql', deadline=deadline, stage=stage)
        with _live_announcement_reply_budget(connection, deadline=deadline, stage=stage):
            rows = connection.execute(text(f'SELECT {hint}stock_code,result_count FROM st_pit_source_coverage{index} '
                "WHERE fact_kind='event' AND source=:source AND batch_id=:batch AND stock_code>:cursor "
                'ORDER BY stock_code LIMIT :limit'),
                {'source':source, 'batch':batch_id, 'cursor':cursor, 'limit':VALIDATION_INVENTORY_CODE_LIMIT}).tuples().all()
        if not rows:
            break
        codes = [row[0] for row in rows]
        if codes != sorted(set(codes)) or codes[0] <= cursor:
            raise ValueError('historical reconstruction inventory has duplicate or unordered members')
        counts.extend(row[1] for row in rows)
        if len(counts) > 10000:
            raise ValueError('historical reconstruction inventory is out of bounds')
        cursor = codes[-1]
        if len(rows) < VALIDATION_INVENTORY_CODE_LIMIT:
            break
    if not counts:
        raise ValueError('historical reconstruction inventory is out of bounds')
    return counts


def _read_reconstruction(engine, candidate, *, codes, decision, start, end, deadline,
        capture_historical_selection, event_progress, coverage_store=None, coverage_progress=None):
    from server.common.pit_facts import _bind_historical_event_snapshot
    from server.common.qmt_announcement_pit import (
        _assert_pit_fact_schema_prepared, _read_live_announcement_payload_rows,
        _read_live_announcement_payload_sizes, _bounded_announcement_payload_keys,
    )

    # Immutable rows permit metadata chain checks without decompressing unrelated batches.
    _assert_pit_fact_schema_prepared(engine)
    source=CNINFO_ANNOUNCEMENT_SOURCE;batch_id=candidate['batch_id']
    with engine.connect() as c:
        keys=[candidate['coverage_id']]
        sizes=_read_live_announcement_payload_sizes(c,source=source,batch_id=batch_id,
            coverage_ids=keys,deadline=deadline,stage='historical-header-size')
        list(_bounded_announcement_payload_keys(list(sizes.items())))
        row,=_read_live_announcement_payload_rows(c,source=source,batch_id=batch_id,
            coverage_ids=keys,deadline=deadline,stage='historical-header-payload')
        evidence=json.loads(row['payload_json'])['watermark']['evidence']
        provenance=evidence['reconstruction_provenance']
        counts = _reconstruction_counts(c, source=source, batch_id=batch_id, deadline=deadline)
    material={
        'status':'COMPLETE','source':source,'primary_source':QMT_ANNOUNCEMENT_SOURCE,
        'fallback_reason':'QMT_ANNOUNCEMENT_HISTORICAL_RECONSTRUCTION_REQUIRED',
        'batch_id':batch_id,'batch_root_hash':evidence['global_batch_root_hash'],
        'window_start':provenance['query_window_start'],'window_end':provenance['query_window_end'],
        'source_query_cutoff_at':provenance['source_query_cutoff_at'],
        'reconstructed_at':provenance['reconstructed_at'],
        'reconstruction_provenance':provenance,'reconstruction_sha256':provenance['reconstruction_sha256'],
        **{field:provenance[field] for field in ('catalog_batch_id','catalog_manifest_hash','catalog_member_set_hash')},
        'stock_count':len(counts),'coverage_count':len(counts),
        'event_count':sum(counts),'empty_stock_count':sum(count==0 for count in counts),
    }
    selected=_bind_historical_event_snapshot(engine,material,codes=codes,decision=decision,
        received=normalize_decision_at(material['reconstructed_at']),frontier=decision,
        window_start=start,window_end=end,event_progress=event_progress,deadline=deadline,
        coverage_store=coverage_store,coverage_progress=coverage_progress)
    if capture_historical_selection is not None:capture_historical_selection(selected)
    return dict(selected.batch)


def validate_dated_announcement_batch(engine,*,codes,decision_at,window_start,window_end,expected_trade_date,
        capture_historical_selection=None,capture_live_reuse=None,event_progress=None,
        coverage_store=None,coverage_progress=None):
    requested=sorted({str(code).zfill(6) for code in codes})
    target=date.fromisoformat(str(expected_trade_date)[:10]);start=date.fromisoformat(str(window_start)[:10])
    end=date.fromisoformat(str(window_end)[:10]);decision=normalize_decision_at(decision_at)
    if not requested or target!=end:raise ValueError('historical announcement target scope differs')
    deadline=monotonic()+LIVE_ANNOUNCEMENT_BATCH_VALIDATION_BUDGET_SECONDS
    for source in (QMT_ANNOUNCEMENT_SOURCE,CNINFO_ANNOUNCEMENT_SOURCE,EASTMONEY_ANNOUNCEMENT_SOURCE):
        candidates=_inventory(engine,code=requested[0],decision_at=decision,target=target,window_start=start,source=source)
        invalid=[]
        for candidate in candidates:
            if candidate['watermark_kind']!='QUERY_CUTOFF':continue
            try:
                proof=validate_complete_qmt_announcement_batch(engine,codes=requested,decision_at=decision,
                    window_start=start,window_end=end,source=source,
                    fact_cutoff_at=candidate['covered_through_at'],_required_batch_id=candidate['batch_id'],
                    _deadline_monotonic=deadline)
            except QMTAnnouncementBlocked as error:
                if error.reason_code=='QMT_ANNOUNCEMENT_COMPLETE_BATCH_NOT_FOUND':
                    if error.detail!='no-common-batch':invalid.append(error)
                    continue
                if error.reason_code=='QMT_ANNOUNCEMENT_FULL_MARKET_ALL_EMPTY_UNPROVEN':continue
                raise
            if normalize_decision_at(proof['fact_cutoff_at']).date()!=target:
                raise ValueError('historical announcement validated cutoff differs from target')
            if capture_live_reuse is not None:capture_live_reuse()
            return proof
        if invalid:raise invalid[-1]
        if source==CNINFO_ANNOUNCEMENT_SOURCE:
            for candidate in candidates:
                if candidate['watermark_kind']=='HISTORICAL_RECONSTRUCTION':
                    return _read_reconstruction(engine,candidate,codes=requested,decision=decision,start=start,end=end,
                        deadline=deadline,capture_historical_selection=capture_historical_selection,event_progress=event_progress,
                        coverage_store=coverage_store,coverage_progress=coverage_progress)
    raise QMTAnnouncementBlocked('QMT_ANNOUNCEMENT_COMPLETE_BATCH_NOT_FOUND','no-common-batch')
