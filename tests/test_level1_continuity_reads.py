"""Real indexed reads preserve quote quality evidence and bound the stalled scan."""
from datetime import date, datetime, timedelta
from time import monotonic
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import OperationalError

from server.trading_v2 import quotes
from tools import validate_trading_v2_level1 as validator


@pytest.fixture
def storage():
    engine = create_engine('sqlite://')

    @event.listens_for(engine, 'connect')
    def functions(dbapi, _):
        dbapi.create_function('DATE_FORMAT', 2, lambda value, _: str(value)[11:16])
        dbapi.create_function('TIMESTAMPDIFF', 3, lambda _, start, end: int(
            (datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds()))

    @event.listens_for(engine, 'before_cursor_execute', retval=True)
    def mysql_syntax(connection, cursor, sql, args, context, many):
        sql = sql.replace('TIMESTAMPDIFF(SECOND,', "TIMESTAMPDIFF('SECOND',")
        if 'ON DUPLICATE KEY UPDATE' in sql:
            sql = sql.split('ON DUPLICATE KEY UPDATE')[0] + '''ON CONFLICT(capability_code) DO UPDATE SET
                status=excluded.status, consecutive_trade_days=excluded.consecutive_trade_days,
                evidence_json=excluded.evidence_json, passed_at=excluded.passed_at'''
        return sql, args

    with engine.begin() as connection:
        connection.exec_driver_sql('''CREATE TABLE st_quote_event_v2 (
            stock_code TEXT,quote_at TEXT,received_at TEXT,source_provider TEXT,
            bid1 NUMERIC,ask1 NUMERIC,bid1_volume INTEGER,ask1_volume INTEGER)''')
        connection.exec_driver_sql(f'''CREATE INDEX {quotes.LEVEL1_TIME_INDEX}
            ON st_quote_event_v2 ({','.join(quotes.LEVEL1_TIME_INDEX_COLUMNS)})''')
        connection.exec_driver_sql('''CREATE TABLE si_trade_calendar (trade_date TEXT,trade_status INTEGER)''')
        connection.exec_driver_sql('''CREATE TABLE st_execution_capability_v2 (
            capability_code TEXT PRIMARY KEY,status TEXT,protocol_version TEXT,
            consecutive_trade_days INTEGER,evidence_json TEXT,checked_at TEXT,
            passed_at TEXT,updated_at TEXT)''')
    yield engine
    engine.dispose()


def insert_events(connection, rows):
    connection.execute(text('''INSERT INTO st_quote_event_v2 VALUES
        (:code,:at,:received,:provider,:bid,:ask,:bid_volume,:ask_volume)'''), rows)


def quote(at, *, code='000001', delay=1, provider='gj_big_qmt_inner', ask=10):
    observed = datetime.fromisoformat(at)
    return dict(code=code, at=at, received=str(observed + timedelta(seconds=delay)),
                provider=provider, bid=9, ask=ask, bid_volume=100, ask_volume=100)


def test_bounded_index_range_preserves_all_sources_and_genuine_latency(storage):
    target = date(2026, 11, 25)
    with storage.begin() as connection:
        insert_events(connection, [quote('2026-11-24 09:31:00', code=str(i)) for i in range(15000)])
        insert_events(connection, [
            quote('2026-11-25 09:31:00'), quote('2026-11-25 09:31:01', code='000002'),
            quote('2026-11-25 09:32:00', ask=0), quote('2026-11-25 09:33:00', delay=20),
            quote('2026-11-25 09:34:00', delay=-1),
            quote('2026-11-25 09:35:00', provider='public_snapshot'),
            quote('2026-11-25 15:00:30', delay=86400),
            quote('2026-11-25 12:00:00'), quote('2026-11-26 09:31:00'),
        ])
    with storage.connect() as connection:
        dbapi = connection.connection.dbapi_connection
        dbapi.set_progress_handler(lambda: 1, 1000)
        try:
            with pytest.raises(OperationalError, match='interrupted'):
                connection.execute(text('''SELECT COUNT(*) FROM st_quote_event_v2
                    WHERE DATE(quote_at)='2026-11-25' ''')).scalar()
        finally:
            dbapi.set_progress_handler(None, 0)
        dbapi.set_progress_handler(lambda: 1, 1000)
        try:
            all_sources = quotes._read_level1_day(connection, target, genuine=False, deadline=monotonic()+5)
            genuine = quotes._read_level1_day(connection, target, genuine=True, deadline=monotonic()+5)
        finally:
            dbapi.set_progress_handler(None, 0)
        assert dict(all_sources) == dict(event_count=7, session_minutes=6, complete_events=6,
                                         minimum_ingress_seconds=-1, maximum_ingress_seconds=86400)
        assert dict(genuine) == dict(event_count=3, session_minutes=2, complete_events=2,
                                     minimum_ingress_seconds=1, maximum_ingress_seconds=1)
        plan = str(connection.exec_driver_sql('''EXPLAIN QUERY PLAN SELECT COUNT(*) FROM st_quote_event_v2
            WHERE quote_at>='2026-11-25' AND quote_at<'2026-11-26' ''').all())
        assert 'COVERING INDEX' in plan and 'SEARCH' in plan


def test_five_complete_sessions_pass_and_missing_index_clears_prior_pass(storage, monkeypatch):
    days = [date(2026, 11, 25) - timedelta(days=i) for i in range(5)]
    with storage.begin() as connection:
        for day in days:
            connection.execute(text('INSERT INTO si_trade_calendar VALUES (:day,1)'), {'day': str(day)})
            opening = datetime.combine(day, datetime.min.time()).replace(hour=9, minute=31)
            minutes = [opening + timedelta(minutes=i) for i in range(120)]
            afternoon = opening.replace(hour=13, minute=1)
            minutes += [afternoon + timedelta(minutes=i) for i in range(108)]
            insert_events(connection, [quote(str(at)) for at in minutes])
    passed = quotes.validate_level1_continuity(storage, end_date=days[0])
    assert passed['status'] == 'PASS' and passed['consecutive_trade_days'] == 5
    monkeypatch.setattr(quotes, '_level1_time_index_ready', lambda *_args, **_kwargs: False)
    monkeypatch.setattr(quotes, '_read_level1_day', lambda *_args, **_kwargs: pytest.fail('missing index must not scan'))
    blocked = quotes.validate_level1_continuity(storage, end_date=days[0])
    assert blocked['status'] == 'BLOCK' and blocked['block_reason'] == 'level1_read_index_unavailable'
    assert blocked['evidence']['days'] == [] and blocked['consecutive_trade_days'] == 0
    with storage.connect() as connection:
        assert connection.exec_driver_sql('SELECT status,passed_at FROM st_execution_capability_v2').one() == ('BLOCK', None)


def test_validation_stops_before_secondary_queries_when_index_is_missing(storage, monkeypatch):
    monkeypatch.setattr(validator, 'create_batch_engine', lambda *_args, **_kwargs: storage)
    monkeypatch.setattr(validator, 'get_kline_mysql_url', lambda: 'unused')
    monkeypatch.setattr(validator, 'latest_trade_day', lambda *_args: date(2026, 11, 25))
    monkeypatch.setattr(quotes, '_level1_time_index_ready', lambda *_args, **_kwargs: False)
    monkeypatch.setattr(validator, '_live_receipt_gate', lambda *_args: pytest.fail('must stop'))
    monkeypatch.setattr(validator, '_genuine_level1_event_gate', lambda *_args: pytest.fail('must stop'))
    assert validator.run_validation()['block_reason'] == 'level1_read_index_unavailable'


@pytest.mark.parametrize('fault', [None, 'missing', 'column', 'prefix', 'hidden', 'expression', 'hash'])
def test_physical_index_readiness_checks_complete_definition(storage, fault):
    rows = [dict(COLUMN_NAME=name,SUB_PART=None,INDEX_TYPE='BTREE',IS_VISIBLE='YES',EXPRESSION=None)
            for name in quotes.LEVEL1_TIME_INDEX_COLUMNS]
    if fault == 'missing': rows = []
    elif fault == 'column': rows[0]['COLUMN_NAME'] = 'stock_code'
    elif fault == 'prefix': rows[0]['SUB_PART'] = 4
    elif fault == 'hidden': rows[0]['IS_VISIBLE'] = 'NO'
    elif fault == 'expression': rows[0]['EXPRESSION'] = 'date(quote_at)'
    elif fault == 'hash': rows[0]['INDEX_TYPE'] = 'HASH'
    class Result:
        def mappings(self): return self
        def all(self): return rows
    class Connection:
        dialect = SimpleNamespace(name='mysql')
        def execute(self, *_args): return Result()
    assert quotes._level1_time_index_ready(Connection(), deadline=monotonic()+5) is (fault is None)
