"""Synthetic metadata stays in the test database; no prices or profits are generated."""
from datetime import datetime, timedelta
import hashlib
import re
from time import monotonic

import pytest
from sqlalchemy import bindparam, create_engine, event, text
from sqlalchemy.exc import OperationalError

from server.common import pit_facts as pit
from server.common.formal_analysis_store import FormalAnalysisStore
from server.db.formal_analysis_schema import MEMBER_TABLE,privileged_migrate_formal_analysis_schema


def _mysql_packets_in_sqlite(engine):
    import json
    import zlib
    class JsonArrayAgg:
        def __init__(self): self.rows=[]
        def step(self,value): self.rows.append(json.loads(value))
        def finalize(self): return json.dumps(self.rows,separators=(',',':'))
    with engine.connect() as connection:
        driver=connection.connection.driver_connection
        driver.create_aggregate('json_arrayagg',1,JsonArrayAgg)
        driver.create_function('octet_length',1,lambda value:len(value.encode()) if value is not None else None)
        driver.create_function('compress',1,lambda value:(len(value.encode()).to_bytes(4,'little')+
            zlib.compress(value.encode())) if value is not None else None)
        driver.create_function('date_format',2,lambda value,format:datetime.fromisoformat(value).strftime(
            format.replace('%i','%M').replace('%s','%S')))


def test_compressed_directory_crosses_same_row_reply_limit_without_losing_identities(monkeypatch):
    from types import SimpleNamespace
    from server.common.historical_announcement_read import _read_historical_directory_rows
    engine=create_engine('sqlite://')
    _mysql_packets_in_sqlite(engine)
    instant=datetime(2026,11,19,0,22,14,952573)
    expected=[(hashlib.sha256(str(i).encode()).hexdigest(),'002283',instant) for i in range(1360)]
    with engine.begin() as c:
        c.execute(text('CREATE TABLE directory_fixture (coverage_id TEXT,stock_code TEXT,known_at TEXT)'))
        c.execute(text('INSERT INTO directory_fixture VALUES (:id,:code,:known)'),
            [dict(id=identifier,code=code,known=str(known)) for identifier,code,known in expected])
    class Result:
        def __init__(self,result): self.result=result
        def tuples(self): return self
        def all(self):
            rows=self.result.tuples().all()
            if len(rows)>64:raise TimeoutError('row packet reply deadline')
            return rows
        def mappings(self): self.result=self.result.mappings();return self
        def one(self): return self.result.one()
    with engine.connect() as c:
        class Connection:
            dialect=SimpleNamespace(name='mysql')
            connection=c.connection
            def execute(self,*args): return Result(c.execute(*args))
        connection=Connection()
        statement=text('SELECT coverage_id,stock_code,known_at FROM directory_fixture LIMIT :limit')
        with pytest.raises(TimeoutError,match='row packet reply deadline'):
            connection.execute(statement,{'limit':200001}).tuples().all()
        actual=_read_historical_directory_rows(connection,statement,{'limit':200001},
            fields=('coverage_id','stock_code','known_at'),deadline=monotonic()+10,stage='signed-historical-directory:800:0')
        assert [(identifier,code,datetime.fromisoformat(known)) for identifier,code,known in actual]==expected
        empty=_read_historical_directory_rows(connection,statement,{'limit':0},
            fields=('coverage_id','stock_code','known_at'),deadline=monotonic()+10,stage='empty-directory')
        assert empty==[]
        c.connection.driver_connection.create_function('compress',1,lambda _value:b'broken-frame')
        with pytest.raises(ValueError,match='compressed payload length differs'):
            _read_historical_directory_rows(connection,statement,{'limit':200001},
                fields=('coverage_id','stock_code','known_at'),deadline=monotonic()+10,stage='corrupted-directory')
    engine.dispose()


def test_historical_payload_size_reads_fit_per_query_budget(monkeypatch, tmp_path):
    from datetime import date
    from types import SimpleNamespace
    from server.common import qmt_announcement_pit as announcement
    from server.common.announcement_provider import CninfoMarketAnnouncementProvider, ProviderBackedAnnouncementAdapter
    from server.common.historical_announcement_read import validate_dated_announcement_batch
    from tests.test_announcement_provider_fallback import _Client, _master, _row_on_date, _historical_authority_v3

    target = date(2026, 9, 1)
    started = datetime(2026, 9, 3, 13)
    received = started + timedelta(seconds=20)
    codes = ('000001', '600519')
    catalog = announcement.AnnouncementCatalog('size-budget', 'a'*64, 'b'*64,
        codes, dict(zip(codes, ('000001.SZ', '600519.SH'))))
    master = _master(*codes)
    client = _Client(masters=[master, master], rows_by_code={
        codes[0]: [_row_on_date(codes[0], 0, target)], codes[1]: []})
    provider = CninfoMarketAnnouncementProvider(client=client, minimum_request_interval=0,
        now_fn=lambda: started+timedelta(seconds=10))
    monkeypatch.setattr(ProviderBackedAnnouncementAdapter, 'bind_capture_deadline',
        lambda adapter, **_kwargs:setattr(adapter, '_deadline_monotonic', monotonic()+30))
    engine = create_engine('sqlite://')
    pit.ensure_pit_fact_schema(engine)
    clocks = iter((started, received))
    publication = announcement.synchronize_historical_cninfo_announcements(engine,
        adapter=ProviderBackedAnnouncementAdapter(provider, workers=1), checkpoint_root=tmp_path,
        catalog=catalog, context=announcement.HistoricalReconstructionContext(
            target_trade_date=target, source_query_cutoff_at=datetime.combine(target, datetime.max.time()),
            reconstruction_started_at=started, scheduler_run_uid='1'*32, build_sha='2'*40,
            authority=_historical_authority_v3(catalog, target)),
        now_fn=lambda: next(clocks, received+timedelta(seconds=1)), batch_size=2)
    assert publication['status'] == 'COMPLETE'
    catalog_proof = SimpleNamespace(batch_id=catalog.batch_id, manifest_hash=catalog.manifest_hash,
        member_set_hash=catalog.member_set_hash, members=[{'stock_code':code,'qmt_code':catalog.qmt_by_code[code]}
            for code in codes], eligible_codes=lambda _day:list(codes))
    monkeypatch.setattr('server.common.qmt_stock_catalog.load_stock_catalog', lambda *_a,**_k:catalog_proof)
    options = dict(codes=list(codes), decision_at=received+timedelta(seconds=1),
        window_start=target-timedelta(days=30), window_end=target, expected_trade_date=target)
    expected = validate_dated_announcement_batch(engine, **options)
    with engine.connect() as connection:
        ids = list(connection.execute(text('SELECT coverage_id FROM st_pit_source_coverage ORDER BY stock_code')).scalars())
        driver = connection.connection.driver_connection
        steps = [0]
        def count_steps():
            steps[0] += 1
            return 0
        driver.set_progress_handler(count_steps, 1)
        try:
            announcement._read_live_announcement_payload_sizes(connection, source=publication['source'],
                batch_id=publication['batch_id'], coverage_ids=ids[:1], deadline=monotonic()+5, stage='size-budget')
        finally:
            driver.set_progress_handler(None, 0)
    budget = steps[0] + steps[0]//4
    def bound_size_read(connection, _cursor, sql, _parameters, _context, _many):
        if 'LENGTH(CAST(payload_json AS BLOB))' in sql:
            used = [0]
            def stop_large_read():
                used[0] += 1
                return int(used[0] > budget)
            connection.connection.driver_connection.set_progress_handler(stop_large_read, 1)
    def clear_budget(connection, *_args):
        connection.connection.driver_connection.set_progress_handler(None, 0)
    event.listen(engine, 'before_cursor_execute', bound_size_read)
    event.listen(engine, 'after_cursor_execute', clear_budget)
    monkeypatch.setattr(announcement, 'VALIDATION_READ_BATCH_SIZE', 1)
    try:
        with engine.connect() as connection:
            try:
                with pytest.raises(OperationalError, match='interrupted'):
                    announcement._read_live_announcement_payload_sizes(connection, source=publication['source'],
                        batch_id=publication['batch_id'], coverage_ids=ids, deadline=monotonic()+5, stage='size-budget')
            finally:
                clear_budget(connection)
        selected = []
        assert validate_dated_announcement_batch(engine, **options, capture_historical_selection=selected.append) == expected
        assert set(selected[0].members) == set(codes)
        assert [selected[0].members[code][0]['result_count'] for code in codes] == [1, 0]
    finally:
        event.remove(engine, 'before_cursor_execute', bound_size_read)
        event.remove(engine, 'after_cursor_execute', clear_budget)
        with engine.connect() as connection:
            clear_budget(connection)
        engine.dispose()


@pytest.mark.parametrize('source',['cninfo.announcement','qmt.announcement','eastmoney.notice'])
def test_historical_chain_filters_future_revisions_before_row_reads(monkeypatch,source):
    engine = create_engine('sqlite://')
    _mysql_packets_in_sqlite(engine)
    codes = ['000001', '000002']
    scopes = {code: pit._source_coverage_scope_hash('event', code, source) for code in codes}
    selected, parents, rows = {}, {}, []
    for code in codes:
        previous = None
        for revision in range(1, 1007):
            clock = datetime(2026, 9, 1 if revision <= 6 else 2) + timedelta(minutes=revision)
            fingerprint = hashlib.sha256(f'{code}:{revision}'.encode()).hexdigest()
            identifier = pit.canonical_hash({'schema': 'probiga.pit-source-coverage-id.v1',
                'scope_hash': scopes[code], 'coverage_fingerprint_hash': fingerprint})
            rows.append({'id': identifier, 'scope': scopes[code], 'code': code, 'rev': revision,
                         'parent': previous, 'known': clock.isoformat(sep=' '), 'fingerprint': fingerprint,'source':source})
            if revision == 6:
                selected[code], parents[code] = identifier, previous
            previous = identifier
    with engine.begin() as c:
        c.execute(text('''CREATE TABLE st_pit_source_coverage(coverage_id TEXT PRIMARY KEY,scope_hash TEXT,
            stock_code TEXT,revision_no INT,supersedes_coverage_id TEXT,known_at TEXT,received_at TEXT,
            covered_through_at TEXT,coverage_status TEXT,coverage_fingerprint_hash TEXT,fact_kind TEXT,source TEXT)'''))
        c.execute(text('CREATE UNIQUE INDEX uk_pit_coverage_scope_revision ON st_pit_source_coverage(scope_hash,revision_no)'))
        c.execute(text('CREATE INDEX idx_pit_coverage_decision ON st_pit_source_coverage(fact_kind,stock_code,known_at)'))
        c.execute(text("INSERT INTO st_pit_source_coverage VALUES(:id,:scope,:code,:rev,:parent,:known,:known,:known,'COMPLETE',:fingerprint,'event',:source)"), rows)
    with engine.connect() as c:
        driver = c.connection.driver_connection
        def bounded_access(_c, _cursor, statement, parameters, _context, _many):
            used = 0
            def progress():
                nonlocal used
                used += 1000
                return int(used > 10000)
            driver.set_progress_handler(progress, 1000)
            return re.sub(r' FORCE INDEX \((\w+)\)',
                lambda match:' INDEXED BY '+('sqlite_autoindex_st_pit_source_coverage_1'
                    if match[1]=='PRIMARY' else match[1]), statement), parameters
        event.listen(c, 'before_cursor_execute', bounded_access, retval=True)
        received = datetime(2026, 9, 1, 0, 6)
        try:
            old = text('''SELECT coverage_id,scope_hash,stock_code,revision_no,supersedes_coverage_id,
                known_at,received_at,covered_through_at,coverage_status,coverage_fingerprint_hash
                FROM st_pit_source_coverage INDEXED BY uk_pit_coverage_scope_revision
                WHERE scope_hash IN :scopes AND fact_kind='event' AND source=:source
                AND known_at<=:received AND received_at<=:received ORDER BY scope_hash,revision_no''').bindparams(bindparam('scopes', expanding=True))
            with pytest.raises(OperationalError, match='interrupted'):
                c.execute(old, {'scopes': list(scopes.values()), 'received': received,'source':source}).all()
            monkeypatch.setattr(engine.dialect, 'name', 'mysql')
            links = pit._read_signed_historical_coverage_links(c, exact_ids=selected, received=received,
                head_frontier=received, deadline=monotonic() + 10, offset=0,source=source)
            assert set(links) == set(selected.values())
            assert all(links[selected[code]][0]['coverage_id'] == parents[code] for code in codes)
            assert all(head for _, head in links.values())
        finally:
            event.remove(c, 'before_cursor_execute', bounded_access)
            driver.set_progress_handler(None, 0)
        with monkeypatch.context() as sqlite:
            sqlite.setattr(engine.dialect, 'name', 'sqlite')
            future = pit._read_signed_historical_coverage_links(c, exact_ids=selected, received=received,
                head_frontier=datetime(2026, 9, 4), deadline=monotonic() + 10, offset=0,source=source)
            assert all(not head for _, head in future.values())
            c.execute(text("UPDATE st_pit_source_coverage SET stock_code='999999' WHERE scope_hash=:scope AND revision_no=3"), {'scope': scopes['000001']})
            with pytest.raises(ValueError, match='chain differs'):
                pit._read_signed_historical_coverage_links(c, exact_ids=selected, received=received,
                    deadline=monotonic() + 10, offset=0,source=source)


def test_cold_historical_metadata_read_resumes_committed_chunks_under_query_budget(tmp_path,monkeypatch):
    url=f"sqlite:///{tmp_path / 'historical-metadata.db'}"
    engine=create_engine(url)
    _mysql_packets_in_sqlite(engine)
    cache_engine=create_engine(url)
    privileged_migrate_formal_analysis_schema(cache_engine)
    source='cninfo.announcement'
    codes=[f'{number:06d}' for number in range(1,401)]
    scopes={code:pit._source_coverage_scope_hash('event',code,source) for code in codes}
    selected,parents,rows={},{},[]
    for code in codes:
        previous=None
        for revision in range(1,33):
            clock=datetime(2026,10,19,15)+timedelta(days=revision)
            fingerprint=hashlib.sha256(f'{code}:{revision}'.encode()).hexdigest()
            identifier=pit.canonical_hash({'schema':'probiga.pit-source-coverage-id.v1',
                'scope_hash':scopes[code],'coverage_fingerprint_hash':fingerprint})
            rows.append({'id':identifier,'scope':scopes[code],'code':code,'revision':revision,
                'parent':previous,'known':str(clock),'fingerprint':fingerprint,'source':source})
            if revision==20:selected[code],parents[code]=identifier,previous
            previous=identifier
    with engine.begin() as c:
        c.execute(text('''CREATE TABLE st_pit_source_coverage(coverage_id TEXT PRIMARY KEY,scope_hash TEXT,
            stock_code TEXT,revision_no INT,supersedes_coverage_id TEXT,known_at TEXT,received_at TEXT,
            covered_through_at TEXT,coverage_status TEXT,coverage_fingerprint_hash TEXT,fact_kind TEXT,source TEXT)'''))
        c.execute(text('CREATE UNIQUE INDEX uk_pit_coverage_scope_revision ON st_pit_source_coverage(scope_hash,revision_no)'))
        c.execute(text('CREATE INDEX idx_pit_coverage_decision ON st_pit_source_coverage(fact_kind,stock_code,known_at)'))
        c.execute(text("INSERT INTO st_pit_source_coverage VALUES(:id,:scope,:code,:revision,:parent,:known,:known,:known,'COMPLETE',:fingerprint,'event',:source)"),rows)
    store=FormalAnalysisStore(cache_engine)
    committed_ids=set()
    resumed=False
    class InterruptedAfterCommit(BaseException):pass
    try:
        with engine.connect() as c:
            driver=c.connection.driver_connection
            def bounded_access(_c,_cursor,sql,parameters,context,_many):
                used=0
                def progress():
                    nonlocal used
                    used+=1000
                    # JSON aggregation adds VM work while returning one wire packet.
                    return int(used>(60000 if 'json_arrayagg' in sql else 20000))
                driver.set_progress_handler(progress,1000)
                if 'coverage_fingerprint_hash,fact_kind,source' in sql:
                    keys=set(context.compiled_parameters[0].values())
                    if resumed:assert not keys&committed_ids,'resuming must not reload committed source records'
                    else:committed_ids.update(keys)
                return re.sub(r' FORCE INDEX \((\w+)\)',lambda m:' INDEXED BY '+
                    ('sqlite_autoindex_st_pit_source_coverage_1' if m[1]=='PRIMARY' else m[1]),sql),parameters
            event.listen(c,'before_cursor_execute',bounded_access,retval=True)
            received=datetime(2026,11,8,18)
            old=text('''SELECT coverage_id,scope_hash,stock_code,revision_no,supersedes_coverage_id,
                known_at,received_at,covered_through_at,coverage_status,coverage_fingerprint_hash
                FROM st_pit_source_coverage INDEXED BY idx_pit_coverage_decision
                WHERE scope_hash IN :scopes AND stock_code IN :codes AND fact_kind='event' AND source=:source
                AND known_at<=:received AND received_at<=:received ORDER BY scope_hash,revision_no''').bindparams(
                    bindparam('scopes',expanding=True),bindparam('codes',expanding=True))
            with pytest.raises(OperationalError,match='interrupted'):
                c.execute(old,{'scopes':list(scopes.values()),'codes':codes,'source':source,'received':received}).all()
            def interrupt(_done,_total,_scope,fresh):
                if fresh:raise InterruptedAfterCommit()
            options={'exact_ids':selected,'received':received,'head_frontier':datetime(2026,11,22,18),
                'metadata_store':store,'offset':0}
            with monkeypatch.context() as mysql:
                mysql.setattr(engine.dialect,'name','mysql')
                with pytest.raises(InterruptedAfterCommit):
                    pit._read_signed_historical_coverage_links(c,deadline=monotonic()+30,
                        metadata_progress=interrupt,**options)
                with cache_engine.connect() as saved:
                    assert saved.execute(text(f"SELECT COUNT(*) FROM {MEMBER_TABLE} WHERE stage='coverage_metadata'")).scalar()==4
                assert len(committed_ids)==512
                resumed=True
                actual=pit._read_signed_historical_coverage_links(c,deadline=monotonic()+30,**options)
                assert actual.keys()==set(selected.values())
                assert all(actual[selected[code]][0]['coverage_id']==parents[code] for code in codes)
                assert all(not head for parent,head in actual.values())
            event.remove(c,'before_cursor_execute',bounded_access)
            driver.set_progress_handler(None,0)
    finally:
        engine.dispose();cache_engine.dispose()
