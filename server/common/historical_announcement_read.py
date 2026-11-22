"""Date-bound research reads through the existing audited announcement validators."""
from datetime import date,datetime,timedelta
from time import monotonic

from sqlalchemy import text

from server.common.pit_facts import normalize_decision_at
from server.common.qmt_announcement_pit import (
    QMTAnnouncementBlocked,validate_complete_qmt_announcement_batch,
    validate_complete_historical_reconstruction_batch,
    QMT_ANNOUNCEMENT_SOURCE,CNINFO_ANNOUNCEMENT_SOURCE,EASTMONEY_ANNOUNCEMENT_SOURCE,
    LIVE_ANNOUNCEMENT_BATCH_VALIDATION_BUDGET_SECONDS,
)


def _inventory(engine,*,code,decision_at,target,window_start,source=None):
    with engine.connect() as c:
        rows=c.execute(text("""SELECT /*+ MAX_EXECUTION_TIME(4000) */ source,batch_id,
                covered_through_at,watermark_kind,known_at
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


def validate_dated_announcement_batch(engine,*,codes,decision_at,window_start,window_end,expected_trade_date,
        capture_historical_selection=None,capture_live_reuse=None):
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
        if source==CNINFO_ANNOUNCEMENT_SOURCE and any(c['watermark_kind']=='HISTORICAL_RECONSTRUCTION' for c in candidates):
            try:
                return validate_complete_historical_reconstruction_batch(engine,codes=requested,decision_at=decision,
                    window_start=start,window_end=end,expected_trade_date=target,
                    capture_historical_selection=capture_historical_selection)
            except QMTAnnouncementBlocked as error:
                if error.reason_code!='QMT_ANNOUNCEMENT_COMPLETE_BATCH_NOT_FOUND' or error.detail!='no-common-batch':raise
    raise QMTAnnouncementBlocked('QMT_ANNOUNCEMENT_COMPLETE_BATCH_NOT_FOUND','no-common-batch')
