from time import monotonic

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import OperationalError

from server.common.historical_announcement_read import _reconstruction_counts


def test_header_inventory_respects_cold_row_reply_budget_and_preserves_all_counts():
    engine = create_engine('sqlite://')
    with engine.begin() as c:
        c.execute(text('CREATE TABLE raw_coverage(coverage_id TEXT PRIMARY KEY,fact_kind TEXT,source TEXT,batch_id TEXT,stock_code TEXT,result_count INT)'))
        c.execute(text('CREATE INDEX inventory ON raw_coverage(fact_kind,source,batch_id,stock_code,coverage_id)'))
        c.execute(text("INSERT INTO raw_coverage VALUES(:id,'event','cninfo.announcement',:batch,:code,:count)"),
                  [{'id':f'{batch}-{i:06d}','batch': batch, 'code': f'{i:06d}', 'count': i % 7}
                   for batch in ['selected', 'unrelated'] for i in range(5558)])
        c.execute(text('CREATE VIEW st_pit_source_coverage AS SELECT coverage_id,fact_kind,source,batch_id,stock_code,read_count(result_count) AS result_count FROM raw_coverage'))
    with engine.connect() as c:
        driver = c.connection.driver_connection
        payload_reads = 0
        def read_count(value):
            nonlocal payload_reads
            payload_reads += 1
            if payload_reads > 128:raise RuntimeError('cold row reply budget exhausted')
            return value
        driver.create_function('read_count',1,read_count)
        def budget(_c, _cursor, statement, _parameters, _context, _many):
            nonlocal payload_reads
            payload_reads = 0
        event.listen(c, 'before_cursor_execute', budget)
        try:
            with pytest.raises(OperationalError):
                c.execute(text("SELECT result_count FROM st_pit_source_coverage WHERE fact_kind='event' AND source='cninfo.announcement' AND batch_id='selected' LIMIT 10001")).all()
            with pytest.raises(OperationalError):
                c.execute(text("SELECT stock_code,result_count FROM st_pit_source_coverage WHERE fact_kind='event' AND source='cninfo.announcement' AND batch_id='selected' AND stock_code>'' ORDER BY stock_code LIMIT 400")).all()
            counts = _reconstruction_counts(c, source='cninfo.announcement', batch_id='selected', deadline=monotonic() + 30)
            assert counts == [i % 7 for i in range(5558)]
        finally:
            event.remove(c, 'before_cursor_execute', budget)
        with pytest.raises(ValueError, match='out of bounds'):
            _reconstruction_counts(c, source='cninfo.announcement', batch_id='missing', deadline=monotonic() + 30)


def test_header_inventory_does_not_silently_truncate_oversized_batch():
    engine = create_engine('sqlite://')
    with engine.begin() as c:
        c.execute(text('CREATE TABLE st_pit_source_coverage(coverage_id TEXT PRIMARY KEY,fact_kind TEXT,source TEXT,batch_id TEXT,stock_code TEXT,result_count INT)'))
        c.execute(text("INSERT INTO st_pit_source_coverage VALUES(:code,'event','cninfo.announcement','oversized',:code,0)"),
                  [{'code': f'{i:06d}'} for i in range(10001)])
        with pytest.raises(ValueError, match='out of bounds'):
            _reconstruction_counts(c, source='cninfo.announcement', batch_id='oversized', deadline=monotonic() + 30)


def test_header_counts_resume_durable_pages_without_reading_the_verified_prefix(tmp_path):
    from server.common.formal_analysis_store import FormalAnalysisStore
    from server.db.formal_analysis_schema import privileged_migrate_formal_analysis_schema
    engine=create_engine('sqlite:///'+str(tmp_path/'headers.sqlite'))
    privileged_migrate_formal_analysis_schema(engine)
    with engine.begin() as c:
        c.execute(text('CREATE TABLE raw_coverage(coverage_id TEXT PRIMARY KEY,fact_kind TEXT,source TEXT,batch_id TEXT,stock_code TEXT,result_count INT)'))
        c.execute(text('CREATE INDEX inventory ON raw_coverage(fact_kind,source,batch_id,stock_code,coverage_id)'))
        c.execute(text("INSERT INTO raw_coverage VALUES(:code,'event','cninfo.announcement','selected',:code,:count)"),
                  [{'code':f'{i:06d}','count':i%7} for i in range(300)])
        c.execute(text('CREATE VIEW st_pit_source_coverage AS SELECT coverage_id,fact_kind,source,batch_id,stock_code,read_count(stock_code,result_count) AS result_count FROM raw_coverage'))
    forbid_prefix=False
    def functions(dbapi,_record):
        def read_count(code,value):
            if forbid_prefix and int(code)<64:raise RuntimeError('verified counts were read again')
            return value
        dbapi.create_function('read_count',2,read_count)
    event.listen(engine,'connect',functions)
    engine.dispose()
    store=FormalAnalysisStore(engine)
    class Stop(BaseException):pass
    def pause(done,total,scope,fresh):
        assert (done,total,fresh)==(64,300,True)
        raise Stop()
    with engine.connect() as c,pytest.raises(Stop):
        _reconstruction_counts(c,source='cninfo.announcement',batch_id='selected',deadline=monotonic()+30,
            coverage_store=store,coverage_progress=pause)
    forbid_prefix=True
    progress=[]
    with engine.connect() as c:
        counts=_reconstruction_counts(c,source='cninfo.announcement',batch_id='selected',deadline=monotonic()+30,
            coverage_store=store,coverage_progress=lambda *value:progress.append(value))
    assert counts==[i%7 for i in range(300)]
    assert [(done,total,fresh) for done,total,_scope,fresh in progress]==[(64,300,False),(128,300,True),(192,300,True),(256,300,True),(300,300,True)]
    assert len({scope for _done,_total,scope,_fresh in progress})==1
    from server.common.pit_facts import canonical_hash
    bad_engine=create_engine('sqlite:///'+str(tmp_path/'wrong-members.sqlite'))
    privileged_migrate_formal_analysis_schema(bad_engine)
    bad_store=FormalAnalysisStore(bad_engine)
    members=[[f'{i:06d}',f'{i:06d}'] for i in range(64)]
    generation=canonical_hash({'schema':'probiga.historical-header-counts.v1','source':'cninfo.announcement','batch':'selected'})
    bad_store.put(generation,'historical_header_counts',canonical_hash(members),
                  {'members':members,'rows':[[identifier,'999999',0] for _code,identifier in members]})
    with engine.connect() as c,pytest.raises(ValueError,match='count membership differs'):
        _reconstruction_counts(c,source='cninfo.announcement',batch_id='selected',deadline=monotonic()+30,coverage_store=bad_store)
    bad_engine.dispose()
    engine.dispose()
