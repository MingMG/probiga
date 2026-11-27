"""Date-bound research reads through the existing audited announcement validators."""
from datetime import date,datetime,timedelta
import json
import logging
from time import monotonic
from types import MappingProxyType

from sqlalchemy import bindparam, text
from sqlalchemy.exc import OperationalError

from server.common.pit_facts import normalize_decision_at
from server.common.qmt_announcement_pit import (
    QMTAnnouncementBlocked,validate_complete_qmt_announcement_batch,
    QMT_ANNOUNCEMENT_SOURCE,CNINFO_ANNOUNCEMENT_SOURCE,EASTMONEY_ANNOUNCEMENT_SOURCE,
    LIVE_ANNOUNCEMENT_BATCH_VALIDATION_BUDGET_SECONDS,
)


def _read_historical_directory_rows(connection, statement, parameters, *, fields, deadline, stage):
    """Read one bounded directory as one packet, preserving its exact fields."""
    from sqlalchemy import String, cast, column, func, select
    from server.common.pit_facts import _decode_coverage_payload
    from server.common.qmt_announcement_pit import _live_announcement_read_hint, _live_announcement_reply_budget

    mysql = connection.dialect.name == 'mysql'
    hint = _live_announcement_read_hint(mysql=mysql, deadline=deadline, stage=stage)
    if mysql:
        directory = statement.columns(*(column(name) for name in fields)).subquery('directory')
        values = [func.date_format(directory.c[name], '%Y-%m-%d %H:%i:%s.%f')
                  if name == 'known_at' else directory.c[name] for name in fields]
        body = select(func.count().label('row_count'),
            cast(func.json_arrayagg(func.json_array(*values)), String).label('body')
        ).select_from(directory).subquery('packed_directory')
        query = select(body.c.row_count, func.octet_length(body.c.body).label('raw_bytes'),
            func.compress(body.c.body).label('packed')).prefix_with(hint)
    else:
        query = statement
    with _live_announcement_reply_budget(connection, deadline=deadline, stage=stage):
        if not mysql:
            return connection.execute(query, parameters).tuples().all()
        packet = connection.execute(query, parameters).mappings().one()
    count = packet['row_count']
    if type(count) is not int or not 0 <= count <= parameters['limit']:
        raise ValueError('historical announcement directory packet count differs')
    if count == 0:
        if packet['raw_bytes'] is not None or packet['packed'] is not None:
            raise ValueError('historical announcement empty directory packet differs')
        return []
    if type(packet['raw_bytes']) is not int or not 0 < packet['raw_bytes'] <= 32 * 1024 * 1024:
        raise ValueError('historical announcement directory packet exceeds byte budget')
    rows = json.loads(_decode_coverage_payload(packet['packed'], expected_bytes=packet['raw_bytes'], stage=stage))
    if (not isinstance(rows, list) or len(rows) != count
            or any(not isinstance(row, list) or len(row) != len(fields) for row in rows)):
        raise ValueError('historical announcement directory packet membership differs')
    _live_announcement_read_hint(mysql=False, deadline=deadline, stage=stage)
    return rows


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


def _reconstruction_counts(connection, *, source, batch_id, deadline, coverage_store=None, coverage_progress=None):
    from server.common.pit_facts import canonical_hash
    from server.common.qmt_announcement_pit import (
        _live_announcement_read_hint, _live_announcement_reply_budget,
        VALIDATION_INVENTORY_CODE_LIMIT,
    )
    from server.common.pit_source_coverage_index import PIT_SOURCE_COVERAGE_BATCH_INDEX_NAME
    index = f' FORCE INDEX ({PIT_SOURCE_COVERAGE_BATCH_INDEX_NAME})' if connection.dialect.name == 'mysql' else ''
    inventory, cursor = [], ''
    while True:
        stage = f'historical-header-inventory:{len(inventory)}'
        hint = _live_announcement_read_hint(mysql=connection.dialect.name == 'mysql', deadline=deadline, stage=stage)
        with _live_announcement_reply_budget(connection, deadline=deadline, stage=stage):
            rows = connection.execute(text(f'SELECT {hint}stock_code,coverage_id FROM st_pit_source_coverage{index} '
                "WHERE fact_kind='event' AND source=:source AND batch_id=:batch AND stock_code>:cursor "
                'ORDER BY stock_code LIMIT :limit'),
                {'source':source, 'batch':batch_id, 'cursor':cursor, 'limit':VALIDATION_INVENTORY_CODE_LIMIT}).tuples().all()
        if not rows:
            break
        codes = [row[0] for row in rows]
        if codes != sorted(set(codes)) or codes[0] <= cursor:
            raise ValueError('historical reconstruction inventory has duplicate or unordered members')
        inventory.extend([list(row) for row in rows])
        if len(inventory) > 10000:
            raise ValueError('historical reconstruction inventory is out of bounds')
        cursor = codes[-1]
        if len(rows) < VALIDATION_INVENTORY_CODE_LIMIT:
            break
    if not inventory:
        raise ValueError('historical reconstruction inventory is out of bounds')
    # Discover IDs in the covering index before bounded primary-key row reads.
    generation=canonical_hash({'schema':'probiga.historical-header-counts.v1','source':source,'batch':batch_id})
    scope=canonical_hash({'generation':generation,'members':inventory})
    primary=' FORCE INDEX (PRIMARY)' if connection.dialect.name=='mysql' else ''
    counts=[]
    # 128 cold rows exhausted the observed 10-second reply limit.
    for offset in range(0,len(inventory),64):
        if coverage_store is not None and offset % 2048 == 0:
            page_keys=[canonical_hash(inventory[start:start+64])
                for start in range(offset,min(offset+2048,len(inventory)),64)]
            retained_pages=coverage_store.get_many(generation,'historical_header_counts',page_keys)
        selected=inventory[offset:offset+64]
        key=canonical_hash(selected)
        retained=retained_pages.get(key) if coverage_store is not None else None
        fresh=retained is None
        stage=f'historical-header-counts:{offset}'
        hint=_live_announcement_read_hint(mysql=connection.dialect.name=='mysql',deadline=deadline,stage=stage)
        if fresh:
            statement=text(f'SELECT {hint}coverage_id,stock_code,result_count FROM st_pit_source_coverage{primary} '
                "WHERE coverage_id IN :ids AND fact_kind='event' AND source=:source AND batch_id=:batch").bindparams(bindparam('ids',expanding=True))
            with _live_announcement_reply_budget(connection,deadline=deadline,stage=stage):
                rows=connection.execute(statement,{'ids':[row[1] for row in selected],'source':source,'batch':batch_id}).tuples().all()
            retained={'members':selected,'rows':[list(row) for row in rows]}
        rows=retained['rows']
        expected={identifier:code for code,identifier in selected}
        if (retained['members']!=selected or len(rows)!=len(selected)
                or {row[0] for row in rows}!=set(expected)
                or any(len(row)!=3 or row[1]!=expected.get(row[0]) or type(row[2]) is not int or row[2]<0 for row in rows)):
            raise ValueError('historical reconstruction count membership differs')
        values={identifier:count for identifier,_code,count in rows}
        if fresh and coverage_store is not None:coverage_store.put(generation,'historical_header_counts',key,retained)
        counts.extend(values[identifier] for _code,identifier in selected)
        if coverage_progress is not None:coverage_progress(len(counts),len(inventory),scope,fresh and coverage_store is not None)
    return counts


def _read_reconstruction(engine, candidate, *, codes, decision, start, end, deadline,
        capture_historical_selection, event_progress, coverage_store=None, coverage_progress=None, metadata_progress=None):
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
        counts = _reconstruction_counts(c, source=source, batch_id=batch_id, deadline=deadline,
            coverage_store=coverage_store,coverage_progress=coverage_progress)
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
        coverage_store=coverage_store,coverage_progress=coverage_progress,metadata_progress=metadata_progress)
    if capture_historical_selection is not None:capture_historical_selection(selected)
    return dict(selected.batch)


def _bind_dated_live_selection(engine, proof, bound_members, *, codes, decision, start, end, deadline,
        coverage_store=None, coverage_progress=None, event_progress=None, metadata_progress=None):
    from hashlib import sha256
    from pathlib import Path
    from server.common import pit_facts as pit
    from server.common.formal_source_identity import frozen_source_sha256
    from server.common.qmt_announcement_pit import (
        VALIDATION_INVENTORY_CODE_LIMIT, _assert_pit_fact_schema_prepared,
        _live_announcement_reply_budget,
    )

    _assert_pit_fact_schema_prepared(engine)
    if set(bound_members)!=set(codes):
        raise ValueError('dated announcement validated member scope differs')
    received=normalize_decision_at(proof['received_at'])
    source=proof['source'];batch=proof['batch_id']
    generation=pit.canonical_hash({'schema':'probiga.dated-live-event-selection.v1',
        'batch':proof,'codes':codes,'decision':decision,'start':start,'end':end,
        'reader':frozen_source_sha256(Path(__file__)),
        'rules':sha256(Path(pit.__file__).read_bytes()).hexdigest()})
    members={}
    with engine.connect() as c:
        with _live_announcement_reply_budget(c,deadline=deadline,stage='signed-historical-isolation'):
            if c.dialect.name=='mysql' and c.get_isolation_level().upper() not in {'REPEATABLE READ','SERIALIZABLE'}:
                raise ValueError('dated announcement requires a repeatable-read snapshot')
        with c.begin():
            for offset in range(0,len(codes),VALIDATION_INVENTORY_CODE_LIMIT):
                group=codes[offset:offset+VALIDATION_INVENTORY_CODE_LIMIT]
                key=pit.canonical_hash({'codes':group})
                expected={code:pit._json_safe(bound_members[code]) for code in group}
                retained=coverage_store.get(generation,'dated_live_event_coverage',key) if coverage_store else None
                fresh=retained is None
                if fresh:
                    # The complete batch already verified these bodies and its global root.
                    # Check ancestry at its receipt; later observations only determine heads.
                    links=pit._read_signed_historical_coverage_links(c,
                        exact_ids={code:row['coverage_id'] for code,row in expected.items()},received=received,
                        head_frontier=decision,deadline=deadline,offset=offset,source=source,
                        metadata_store=coverage_store,metadata_progress=metadata_progress)
                    retained={}
                    for code,row in expected.items():
                        parent,head=links[row['coverage_id']]
                        if (row['revision_no']!=(parent['revision_no']+1 if parent else 1)
                                or (row['supersedes_coverage_id'] or None)!=(parent['coverage_id'] if parent else None)
                                or (parent and normalize_decision_at(row['known_at'])<pit._row_datetime(parent['known_at']))):
                            raise ValueError('dated announcement selected predecessor differs')
                        retained[code]=[row,head]
                if set(retained)!=set(group):
                    raise ValueError('dated announcement cached member scope differs')
                for code,(row,head) in retained.items():
                    if (type(head) is not bool or row!=expected[code] or row['stock_code']!=code
                            or row['source']!=source or row['batch_id']!=batch or row['watermark_kind']!='QUERY_CUTOFF'
                            or row['scope_hash']!=pit._source_coverage_scope_hash('event',code,source)
                            or not normalize_decision_at(row['received_at'])<=normalize_decision_at(row['known_at'])<=received<=decision):
                        raise ValueError('dated announcement cached member identity differs')
                    members[code]=(MappingProxyType(row),head)
                if fresh and coverage_store:
                    coverage_store.put(generation,'dated_live_event_coverage',key,retained)
                done=offset+len(group)
                if coverage_progress:coverage_progress(done,len(codes),generation,fresh and coverage_store is not None)
                if event_progress:event_progress(done,len(codes))
    return pit._LiveEventSelection(engine,decision,start,end,MappingProxyType(dict(proof)),MappingProxyType(members))


def _validated_live_batch(engine, candidate, *, requested, decision, start, end, source, deadline,
        include_members, coverage_store, progress):
    from pathlib import Path
    from server.common import pit_facts as pit, qmt_announcement_pit as announcement
    from server.common.formal_source_identity import frozen_source_sha256, _source_sha256

    generation=pit.canonical_hash({'schema':'probiga.dated-live-batch-validation.v1',
        'candidate':candidate,'codes':requested,'decision':decision,'start':start,'end':end,
        'reader':frozen_source_sha256(Path(__file__)),'validator':_source_sha256(Path(announcement.__file__))})
    progress_scope='dated-live-validation:'+generation
    keys=[str(offset) for offset in range(0,len(requested),400)]
    retained=None
    if coverage_store is not None:
        announcement._assert_pit_fact_schema_prepared(engine)
        retained=coverage_store.get(generation,'dated_live_batch_proof','complete')
    if retained is None:
        bound=[]
        capture=include_members or coverage_store is not None
        proof=validate_complete_qmt_announcement_batch(engine,codes=requested,decision_at=decision,
            window_start=start,window_end=end,source=source,
            fact_cutoff_at=candidate['covered_through_at'],_required_batch_id=candidate['batch_id'],
            _deadline_monotonic=deadline,_capture_bound_members=bound.append if capture else None)
        if capture and (len(bound)!=1 or set(bound[0])!=set(requested)):
            raise ValueError('dated announcement validated capture differs')
        members=pit._json_safe({code:row for code,(row,_bindings) in bound[0].items()}) if capture else {}
        if coverage_store is not None:
            # A complete proof is reusable before ancestry binding yields its next partition.
            chunks=coverage_store.put_many(generation,'dated_live_batch_members',{
                key:{code:members[code] for code in requested[int(key):int(key)+400]} for key in keys})
            coverage_store.put(generation,'dated_live_batch_proof','complete',{'proof':proof,'chunks':chunks})
            if progress:progress(1,2,progress_scope,True)
        return proof,members,progress_scope
    if (type(retained) is not dict or set(retained)!={'proof','chunks'}
            or type(retained['chunks']) is not dict or type(retained['proof']) is not dict
            or set(retained['chunks'])!=set(keys)):
        raise ValueError('dated announcement cached proof framing differs')
    identities=[retained['chunks'][key] for key in keys]
    artifacts={}
    for offset in range(0,len(identities),8):
        artifacts.update(coverage_store.load_artifacts(identities[offset:offset+8]))
    members={}
    for key in keys:
        part=artifacts[retained['chunks'][key]]
        if type(part) is not dict or set(part)!=set(requested[int(key):int(key)+400]):
            raise ValueError('dated announcement cached proof members differ')
        members.update(part)
    proof=retained['proof']
    if (proof.get('status')!='COMPLETE' or proof.get('source')!=source
            or proof.get('batch_id')!=candidate['batch_id']
            or normalize_decision_at(proof['fact_cutoff_at'])!=normalize_decision_at(candidate['covered_through_at'])):
        raise ValueError('dated announcement cached proof identity differs')
    if progress:progress(1,2,progress_scope,False)
    return proof,members,progress_scope


def validate_dated_announcement_batch(engine,*,codes,decision_at,window_start,window_end,expected_trade_date,
        capture_historical_selection=None,capture_live_reuse=None,event_progress=None,
        coverage_store=None,coverage_progress=None,metadata_progress=None):
    requested=sorted({str(code).zfill(6) for code in codes})
    target=date.fromisoformat(str(expected_trade_date)[:10]);start=date.fromisoformat(str(window_start)[:10])
    end=date.fromisoformat(str(window_end)[:10]);decision=normalize_decision_at(decision_at)
    if not requested or target!=end:raise ValueError('historical announcement target scope differs')
    started=monotonic()
    deadline=started+LIVE_ANNOUNCEMENT_BATCH_VALIDATION_BUDGET_SECONDS
    try:
        for source in (QMT_ANNOUNCEMENT_SOURCE,CNINFO_ANNOUNCEMENT_SOURCE,EASTMONEY_ANNOUNCEMENT_SOURCE):
            stage='inventory'
            candidates=_inventory(engine,code=requested[0],decision_at=decision,target=target,window_start=start,source=source)
            invalid=[]
            for candidate in candidates:
                if candidate['watermark_kind']!='QUERY_CUTOFF':continue
                try:
                    stage='live-proof'
                    proof,bound,validation_scope=_validated_live_batch(engine,candidate,requested=requested,decision=decision,
                        start=start,end=end,source=source,deadline=deadline,
                        include_members=capture_historical_selection is not None,coverage_store=coverage_store,
                        progress=metadata_progress)
                except QMTAnnouncementBlocked as error:
                    if error.reason_code=='QMT_ANNOUNCEMENT_COMPLETE_BATCH_NOT_FOUND':
                        if error.detail!='no-common-batch':invalid.append(error)
                        continue
                    if error.reason_code=='QMT_ANNOUNCEMENT_FULL_MARKET_ALL_EMPTY_UNPROVEN':continue
                    raise
                if normalize_decision_at(proof['fact_cutoff_at']).date()!=target:
                    raise ValueError('historical announcement validated cutoff differs from target')
                if capture_historical_selection is not None:
                    stage='live-binding'
                    capture_historical_selection(_bind_dated_live_selection(engine,proof,bound,codes=requested,
                        decision=decision,start=start,end=end,deadline=deadline,coverage_store=coverage_store,
                        coverage_progress=coverage_progress,event_progress=event_progress,metadata_progress=metadata_progress))
                if coverage_store is not None and metadata_progress:
                    metadata_progress(2,2,validation_scope,False)
                if capture_live_reuse is not None:capture_live_reuse()
                return proof
            if invalid:raise invalid[-1]
            if source==CNINFO_ANNOUNCEMENT_SOURCE:
                for candidate in candidates:
                    if candidate['watermark_kind']=='HISTORICAL_RECONSTRUCTION':
                        stage='reconstruction'
                        return _read_reconstruction(engine,candidate,codes=requested,decision=decision,start=start,end=end,
                            deadline=deadline,capture_historical_selection=capture_historical_selection,event_progress=event_progress,
                            coverage_store=coverage_store,coverage_progress=coverage_progress,metadata_progress=metadata_progress)
        raise QMTAnnouncementBlocked('QMT_ANNOUNCEMENT_COMPLETE_BATCH_NOT_FOUND','no-common-batch')
    except OperationalError as error:
        from server.common.batch_db import _transient_db_errno
        errno=_transient_db_errno(error)
        logging.getLogger(__name__).error(
            'dated-announcement-read date=%s source=%s stage=%s codes=%d elapsed=%.3fs error_type=%s errno=%s error=%s sql=%s',
            target,source,stage,len(requested),monotonic()-started,type(error).__name__,errno,
            str(error.orig)[:500],str(error.statement)[:500])
        if errno in (1969,3024):
            reason='QMT_ANNOUNCEMENT_LIVE_BATCH_REPLY_DEADLINE'
        elif errno in (1205,1213,2003,2006,2013):
            reason='QMT_ANNOUNCEMENT_LIVE_BATCH_REPLY_FAILED'
        else:raise
        raise QMTAnnouncementBlocked(reason,'signed-historical-native') from error
