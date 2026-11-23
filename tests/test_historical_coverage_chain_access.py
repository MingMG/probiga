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


@pytest.mark.parametrize('source',['cninfo.announcement','qmt.announcement','eastmoney.notice'])
def test_historical_chain_filters_future_revisions_before_row_reads(monkeypatch,source):
    engine = create_engine('sqlite://')
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
                    return int(used>20000)
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
