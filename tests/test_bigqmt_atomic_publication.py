"""Exercise real transaction visibility at the FULL-row/receipt boundary."""
from copy import deepcopy
from datetime import datetime, timedelta
import json
import re

import pytest
from sqlalchemy import create_engine, event, text

from integrations.bigqmt.spool import PROVIDER_ID
from tools import run_big_qmt_bridge as consumer
from tests.test_bigqmt_poll_ingestion import _payload


@pytest.fixture
def publication(tmp_path, monkeypatch):
    engine = create_engine('sqlite:///' + str(tmp_path / 'publication.sqlite'))
    @event.listens_for(engine, 'connect')
    def functions(dbapi, _record):
        dbapi.create_function('GET_LOCK', 2, lambda *_: 1)
        dbapi.create_function('RELEASE_LOCK', 1, lambda *_: 1)
    # Adapt only MySQL insert syntax. The production writer, independent
    # reader, commits and rollbacks still execute against the real database.
    @event.listens_for(engine, 'before_cursor_execute', retval=True)
    def mysql_insert(_conn, _cursor, sql, parameters, _context, _many):
        if ' AS incoming ON DUPLICATE KEY UPDATE ' in sql:
            sql = sql.replace(' AS incoming ON DUPLICATE KEY UPDATE ',
                              ' ON CONFLICT(stock_code) DO UPDATE SET ').replace('incoming.', 'excluded.')
        elif 'ON DUPLICATE KEY UPDATE' in sql:
            sql = sql.replace('ON DUPLICATE KEY UPDATE',
                              'ON CONFLICT(source_provider,source_snapshot_token) DO UPDATE SET')
            sql = re.sub(r'VALUES\((\w+)\)', r'excluded.\1', sql)
        if isinstance(parameters, tuple):
            parameters = tuple(str(value) if isinstance(value, datetime) else value for value in parameters)
        return sql, parameters
    columns = ['stock_code','price','change','change_pct','volume','amount','snapshot_at','etl_sync_at',
               'batch_id','data_source','quality_status','permission_status']
    with engine.begin() as c:
        c.exec_driver_sql('PRAGMA journal_mode=WAL')
        c.exec_driver_sql('CREATE TABLE sm_stock_current(id INTEGER PRIMARY KEY,stock_code TEXT UNIQUE,'
            'price REAL,`change` REAL,change_pct REAL,volume REAL,amount REAL,snapshot_at TEXT,etl_sync_at TEXT,'
            'batch_id TEXT,data_source TEXT,quality_status TEXT,permission_status TEXT)')
        c.exec_driver_sql('CREATE TABLE st_qmt_realtime_sync_receipt_v2(receipt_id TEXT PRIMARY KEY,'
            'source_provider TEXT,source_snapshot_token TEXT,source_full_file_token TEXT,source_generated_at TEXT,'
            'heartbeat_at TEXT,expected_count INT,observed_count INT,coverage REAL,published_at TEXT,'
            'capture_mode TEXT,quality_status TEXT,evidence_json TEXT,created_at TEXT,'
            'UNIQUE(source_provider,source_snapshot_token))')
        c.execute(text("INSERT INTO sm_stock_current(stock_code,price,batch_id) VALUES(:code,10,:batch)"),
                  [{'code':'000001','batch':'old'},{'code':'000002','batch':'old'},{'code':'510300','batch':'etf'}])
        c.execute(text("INSERT INTO st_qmt_realtime_sync_receipt_v2(receipt_id,source_provider,"
            "source_snapshot_token,published_at,created_at,evidence_json) VALUES('old',:provider,'old',:at,:at,:evidence)"),
            {'provider':PROVIDER_ID,'at':str(datetime.now()-timedelta(seconds=5)),
             'evidence':json.dumps({'full_batch_id':'old'})})
    now = datetime.now()
    payload = _payload(now)
    payload['batch_id'] = 'new'
    payload['quotes']['000002.SZ'] = deepcopy(payload['quotes']['000001.SZ'])
    monkeypatch.setattr(consumer,'_snapshot_freshness_required',lambda _engine:True)
    monkeypatch.setattr(consumer,'read_json',lambda _path:{'updated_ts':now.timestamp(),'status':'running'})
    monkeypatch.setattr(consumer,'_read_snapshot_if_changed',lambda kind,**kw:(payload,'new-file') if kind=='full' else ({},''))
    monkeypatch.setattr(consumer,'_capture_limit_up_events',lambda *_a,**_k:([],{'new':{'sealed':True}}))
    monkeypatch.setattr(consumer,'_write_status',lambda *_a,**_k:None)
    monkeypatch.setattr(consumer,'_database_frame',lambda _engine,frame:frame[columns].copy())
    monkeypatch.setattr(consumer,'validate_qmt_realtime_sync_receipt_runtime_schema',lambda _engine:None)
    yield engine, payload
    engine.dispose()


def paired_state(engine):
    with engine.connect() as c:
        receipt = c.execute(text('SELECT evidence_json FROM st_qmt_realtime_sync_receipt_v2 '
            'ORDER BY published_at DESC,created_at DESC LIMIT 1')).scalar_one()
        rows = list(c.execute(text("SELECT stock_code,batch_id,price FROM sm_stock_current ORDER BY stock_code")).tuples())
    return json.loads(receipt)['full_batch_id'], rows


def ingest(engine, tmp_path, tokens):
    return consumer.ingest_once(engine,qmt_home=tmp_path,universe=['000001','000002'],
                               tracked=[],short_name_map={},last_tokens=tokens)


def test_receipt_insert_boundary_exposes_only_the_old_pair_until_commit(publication,tmp_path,monkeypatch):
    engine,_ = publication
    original = consumer._record_realtime_sync_receipt
    observed = []
    writer_connections = set()
    def writer(_connection,_cursor,sql,*_args):
        if 'GET_LOCK' in sql or sql.lstrip().startswith(('INSERT INTO sm_stock_current','INSERT INTO st_qmt_realtime')):
            writer_connections.add(id(_connection))
    event.listen(engine,'before_cursor_execute',writer)
    def before_receipt(connection, **kwargs):
        observed.append(paired_state(engine))
        assert observed[-1][0]=='old'
        assert {row[1] for row in observed[-1][1] if row[0]!='510300'}=={'old'}
        return original(connection,**kwargs)
    monkeypatch.setattr(consumer,'_record_realtime_sync_receipt',before_receipt)
    tokens = {'full':'previous'}
    result = ingest(engine,tmp_path,tokens)
    assert observed and result['full_rows']==2
    receipt,rows = paired_state(engine)
    assert receipt=='new' and {row[1] for row in rows if row[0]!='510300'}=={'new'}
    assert next(row for row in rows if row[0]=='510300')[1:] == ('etf',10)
    assert tokens['full_sync_receipt']['source_batch_id']=='new'
    assert len(writer_connections)==1


def test_receipt_failure_rolls_back_rows_and_keeps_tokens_and_old_pair(publication,tmp_path,monkeypatch):
    engine,_ = publication
    before = paired_state(engine)
    def fail(*_args,**_kwargs):raise RuntimeError('receipt insert failed')
    monkeypatch.setattr(consumer,'_record_realtime_sync_receipt',fail)
    monkeypatch.setattr(consumer,'_limit_event_states',{'old':{'sealed':False}})
    tokens = {'full':'previous','full_file':'previous-file'}
    with pytest.raises(RuntimeError,match='receipt insert failed'):ingest(engine,tmp_path,tokens)
    assert paired_state(engine)==before
    assert tokens=={'full':'previous','full_file':'previous-file'}
    assert consumer._limit_event_states=={'old':{'sealed':False}}


def test_receipt_schema_failure_precedes_any_snapshot_mutation(publication,tmp_path,monkeypatch):
    engine,_ = publication
    before = paired_state(engine)
    def fail(*_args,**_kwargs):raise RuntimeError('receipt schema unavailable')
    monkeypatch.setattr(consumer,'validate_qmt_realtime_sync_receipt_runtime_schema',fail)
    with pytest.raises(RuntimeError,match='receipt schema unavailable'):ingest(engine,tmp_path,{})
    assert paired_state(engine)==before


def test_tracked_updates_only_non_full_securities_without_rebinding_full_rows(publication):
    import pandas as pd
    engine,_ = publication
    frame = pd.DataFrame([dict(stock_code=code,price=99,change=1,change_pct=1,volume=1,amount=1,
        snapshot_at=datetime.now(),etl_sync_at=datetime.now(),batch_id='tracked',
        data_source=PROVIDER_ID,quality_status='VALIDATED',permission_status='SUPPORTED')
        for code in ['000001','510300']])
    assert consumer._replace_tracked_subset(engine,frame,universe=['000001','000002'])==1
    receipt,rows = paired_state(engine)
    assert receipt=='old'
    assert next(row for row in rows if row[0]=='000001')[1:]==('old',10)
    assert next(row for row in rows if row[0]=='510300')[1:]==('tracked',99)


def test_published_timestamp_is_sampled_after_row_dml(publication,tmp_path,monkeypatch):
    engine,_ = publication
    clock = {'now':datetime.now()}
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return clock['now']
    monkeypatch.setattr(consumer,'datetime',Clock)
    original = consumer._upsert_current_rows
    def delayed(connection,frame):
        original(connection,frame)
        clock['now']+=timedelta(seconds=4)
    monkeypatch.setattr(consumer,'_upsert_current_rows',delayed)
    result=ingest(engine,tmp_path,{})
    assert result['full_sync_receipt']['published_at']==clock['now'].replace(microsecond=0).isoformat(sep=' ')


def test_repeated_source_token_returns_the_actual_committed_receipt_id(publication,tmp_path,monkeypatch):
    engine,payload = publication
    clock = {'now':datetime.now().replace(microsecond=0)}
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return clock['now']
    monkeypatch.setattr(consumer,'datetime',Clock)
    first=ingest(engine,tmp_path,{})['full_sync_receipt']
    clock['now']+=timedelta(seconds=2)
    # A new consumer (or a lost previous commit acknowledgement) has no token
    # memory, but must still return the existing unique source/token receipt.
    second=ingest(engine,tmp_path,{})['full_sync_receipt']
    with engine.connect() as connection:
        rows=connection.execute(text('SELECT receipt_id,published_at FROM st_qmt_realtime_sync_receipt_v2 '
            'WHERE source_provider=:source AND source_snapshot_token=:token'),
            {'source':PROVIDER_ID,'token':consumer._snapshot_token(payload)}).all()
    assert len(rows)==1
    assert first['receipt_id']==second['receipt_id']==rows[0][0]
    assert second['published_at']==str(rows[0][1])
    assert second['source_generated_at']==first['source_generated_at']
