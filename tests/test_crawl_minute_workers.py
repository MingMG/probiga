"""Real threads exercise bounded source work and single-owner publication."""
from __future__ import annotations

from collections import Counter
from datetime import datetime
from threading import Barrier, Lock, get_ident
from contextlib import contextmanager

from sqlalchemy import create_engine, event, text

import pytest

from tools import crawl_minute_kline as crawler


@pytest.fixture(autouse=True)
def fast_pacing(monkeypatch):
    monkeypatch.setattr(crawler, "DELAY", 0)
    monkeypatch.setattr(crawler, "JITTER", 0)
    monkeypatch.setattr(crawler, "BATCH_EVERY", 0)
    monkeypatch.setattr(crawler, "RETRY_DELAY", 0)


def test_parallel_fetch_uses_six_independent_sessions_and_retries_same_stock(monkeypatch):
    barrier = Barrier(crawler.FETCH_WORKERS)
    lock = Lock()
    sessions = {}
    attempts = Counter()
    closed = []
    main_thread = get_ident()

    class Session:
        def close(self):
            closed.append(self)

    monkeypatch.setattr(crawler, "_new_minute_session", Session)

    def fetch(code, market, *, session, **kwargs):
        thread = get_ident()
        assert thread != main_thread
        with lock:
            first = thread not in sessions
            assert sessions.setdefault(thread, session) is session
            attempts[code] += 1
            attempt = attempts[code]
        if first:
            barrier.wait(timeout=5)
        if code == "000001" and attempt == 1:
            return None
        return [code]

    codes = [(f"{number:06}", 0) for number in range(1, 49)]
    with crawler._minute_fetch_workers(codes, fetch) as results:
        received = {code: rows for code, rows, captured in results}
    assert received == {code: [code] for code, _ in codes}
    assert len(sessions) == len({id(value) for value in sessions.values()}) == 6
    assert attempts["000001"] == 2
    assert sum(attempts.values()) == 49
    assert {id(value) for value in closed} == {id(value) for value in sessions.values()}


def test_result_queue_is_bounded(monkeypatch):
    class Session:
        def close(self):
            pass

    monkeypatch.setattr(crawler, "_new_minute_session", Session)
    submitted = []
    consumed = []
    actual_pool = crawler.ThreadPoolExecutor

    class Pool(actual_pool):
        def submit(self, function, *args):
            submitted.append(args[0])
            assert len(submitted) - len(consumed) <= 2 * crawler.FETCH_WORKERS
            return super().submit(function, *args)

    monkeypatch.setattr(crawler, "ThreadPoolExecutor", Pool)
    codes = [(f"{number:06}", 0) for number in range(100)]
    with crawler._minute_fetch_workers(codes, lambda code, market, **kw: [code]) as results:
        for code, _, captured in results:
            consumed.append(code)
    assert set(consumed) == {code for code, _ in codes}


def test_each_worker_preserves_request_and_batch_pacing(monkeypatch):
    actual_event = crawler.threading.Event
    waits = []
    fetched = Counter()
    lock = Lock()

    class Event(actual_event):
        def wait(self, timeout=None):
            if timeout in (0.5, 20):
                with lock:
                    waits.append((get_ident(), timeout))
                return self.is_set()
            return super().wait(timeout)

    monkeypatch.setattr(crawler.threading, "Event", Event)
    monkeypatch.setattr(crawler, "DELAY", 0.5)
    monkeypatch.setattr(crawler, "BATCH_EVERY", 2)
    monkeypatch.setattr(crawler, "BATCH_PAUSE", 20)
    monkeypatch.setattr(crawler.random, "uniform", lambda *_: 0)

    def fetch(code, market, **kwargs):
        with lock:
            fetched[get_ident()] += 1
        return [code]

    with crawler._minute_fetch_workers([("000001", 0)] * 60, fetch) as results:
        assert len(list(results)) == 60
    counted = Counter(waits)
    assert sum(fetched.values()) == 60
    for thread, count in fetched.items():
        assert counted[(thread, 0.5)] == count
        assert counted[(thread, 20)] == count // 2


@pytest.mark.parametrize("kind", ["kline", "flow"])
def test_collections_stage_and_publish_on_main_thread_after_all_results(monkeypatch, kind):
    main_thread = get_ident()
    fetched = set()
    staged = []
    published = []
    total = 32
    lock = Lock()

    def fetch(code, market, *, session, **kwargs):
        assert get_ident() != main_thread
        with lock:
            fetched.add(code)
        return ["2026-09-11 09:31,10,10,10,10,1,10,0,0,0,10"]

    def append(_connection, _stage, rows, *_args):
        assert get_ident() == main_thread
        staged.extend(rows)
        return len(rows)

    def publish(*_args, **_kwargs):
        assert get_ident() == main_thread
        assert len(fetched) == total
        published.append(True)
        return len(staged)

    codes = [(f"{number:06}", 0) for number in range(total)]
    context = {
        "started_at": "2026-09-11 09:32:00", "decision_known_at": "2026-09-11 09:32:00",
        "requested_trade_date": "2026-09-11", "build_sha": "a" * 40, "run_uid": "b" * 32,
        "catalog_batch_id": "catalog", "catalog_manifest_hash": "c" * 64,
        "catalog_captured_at": "2026-09-11 08:00:00", "native_no_trade_evidence": None,
    }
    monkeypatch.setattr(crawler, "_now", lambda: datetime(2026, 9, 11, 9, 33))
    if kind == "kline":
        monkeypatch.setattr(crawler, "fetch_minute_kline", fetch)
        monkeypatch.setattr(crawler, "_create_kline_stage", lambda *a: ("stage", object()))
        monkeypatch.setattr(crawler, "_append_kline_stage", append)
        monkeypatch.setattr(crawler, "_publish_kline_stage", publish)
        monkeypatch.setattr(crawler, "_drop_kline_stage", lambda *a: None)
        result = crawler.crawl_kline(object(), codes, "sm_stock_minute", "stock", 0, 1.0,
                                     trade_date="2026-09-11", context=context)
    else:
        monkeypatch.setattr(crawler, "fetch_minute_flow", fetch)
        monkeypatch.setattr(crawler, "_create_flow_stage", lambda *a: ("stage", object()))
        monkeypatch.setattr(crawler, "_append_flow_stage", append)
        monkeypatch.setattr(crawler, "_publish_flow_stage", publish)
        monkeypatch.setattr(crawler, "_drop_flow_stage", lambda *a: None)
        result = crawler.crawl_flow(object(), codes, 0, 1.0, trade_date="2026-09-11", context=context)
    assert result["collected_count"] == result["written_rows"] == total
    assert published == [True]


def test_worker_failure_aborts_before_publish_and_closes_all_sessions(monkeypatch):
    sessions = []

    class Session:
        closed = False

        def __init__(self):
            sessions.append(self)

        def close(self):
            self.closed = True

    monkeypatch.setattr(crawler, "_new_minute_session", Session)

    def fail(*_args, **_kwargs):
        raise RuntimeError("source failure")

    with pytest.raises(RuntimeError, match="source failure"):
        with crawler._minute_fetch_workers([("000001", 0)] * 50, fail) as results:
            list(results)
    assert sessions and all(session.closed for session in sessions)


def test_worker_capture_precedes_throttle_and_main_queue_delay(monkeypatch):
    before = datetime(2026, 9, 11, 9, 35)
    after = datetime(2026, 9, 11, 9, 45)
    clock = [before]
    actual_event = crawler.threading.Event
    class Event(actual_event):
        def wait(self, timeout=None):
            if timeout == 0.5:
                clock[0] = after
                return False
            return super().wait(timeout)
    monkeypatch.setattr(crawler, "_now", lambda: clock[0])
    monkeypatch.setattr(crawler.threading, "Event", Event)
    monkeypatch.setattr(crawler, "DELAY", 0.5)
    monkeypatch.setattr(crawler.random, "uniform", lambda *_: 0)
    with crawler._minute_fetch_workers([("000001", 0)], lambda *_a, **_kw: ["raw"]) as results:
        received = list(results)
    assert clock[0] == after
    assert received == [("000001", ["raw"], before)]
    assert crawler._required_grid("2026-09-11", received[0][2])[-1] == "09:32"


def test_request_crossing_a_minute_uses_response_time_watermark(monkeypatch):
    clock = [datetime(2026, 9, 11, 9, 34, 59)]
    monkeypatch.setattr(crawler, "_now", lambda: clock[0])
    def fetch(*_args, **_kwargs):
        clock[0] = datetime(2026, 9, 11, 9, 35, 1)
        return ["raw"]
    with crawler._minute_fetch_workers([("000001", 0)], fetch) as results:
        received = list(results)
    assert received[0][2] == datetime(2026, 9, 11, 9, 35, 1)
    assert crawler._required_grid("2026-09-11", received[0][2])[-1] == "09:32"


@pytest.fixture
def flow_publication(tmp_path, monkeypatch):
    """SQLite proves commit boundaries; MySQL access plans need live acceptance."""
    engine = create_engine('sqlite:///' + str(tmp_path / 'flow.db'))
    fields = ','.join(column + (' TEXT' if column not in crawler.VALUE_FIELDS['flow'] else ' NUMERIC')
                      for column in crawler.FLOW_WRITE_COLUMNS)
    stage = 'sm_stock_capital_flow_min_stage_0123456789ab'
    connection = engine.connect()
    connection.execute(text('CREATE TABLE sm_stock_capital_flow_min (id INTEGER PRIMARY KEY,' + fields + ')'))
    connection.execute(text('CREATE INDEX idx_scfm_code_time ON sm_stock_capital_flow_min(stock_code,trade_time)'))
    connection.execute(text('CREATE TEMPORARY TABLE ' + stage + ' (id INTEGER PRIMARY KEY,' + fields + ')'))
    rows = []
    for number in range(1, 34):
        for minute in (31, 32):
            row = {field: 1 for field in crawler.VALUE_FIELDS['flow']}
            row.update(stock_code=f'{number:06}', trade_time=f'2026-09-11 09:{minute}:00',
                       source_time=f'2026-09-11 09:{minute}:00', received_at='2026-09-11 09:33:00',
                       snapshot_at='2026-09-11 09:33:00', etl_sync_at='2026-09-11 09:33:00',
                       data_source=crawler.PUBLIC_MINUTE_SOURCE)
            rows.append(row)
    columns = ','.join(crawler.FLOW_WRITE_COLUMNS)
    values = ','.join(':' + key for key in crawler.FLOW_WRITE_COLUMNS)
    connection.execute(text(f'INSERT INTO {stage} ({columns}) VALUES ({values})'), rows)
    connection.execute(text(f'INSERT INTO sm_stock_capital_flow_min ({columns}) VALUES ({values})'),
                       [{**row, 'main_net_inflow': 0} for row in rows])
    connection.commit()
    @contextmanager
    def lock(_engine, _name, **kwargs):
        assert kwargs['connection'] is connection
        yield kwargs.get('connection', connection)
    monkeypatch.setattr(crawler, 'mysql_named_lock', lock)
    monkeypatch.setattr(crawler, '_now', lambda: datetime(2026, 9, 11, 9, 34))
    # Translate only the original MySQL multi-table DELETE for this transaction regression.
    @event.listens_for(connection, 'before_cursor_execute', retval=True)
    def sqlite_delete(_conn, _cursor, statement, parameters, _context, _many):
        if statement.lstrip().startswith('DELETE target_rows'):
            statement = (f'DELETE FROM sm_stock_capital_flow_min WHERE stock_code IN '
                         f'(SELECT DISTINCT stock_code FROM {stage}) '
                         'AND trade_time >= ? AND trade_time < datetime(?, "+1 day")')
        return statement, parameters
    yield engine, connection, stage, rows
    connection.close()
    engine.dispose()


def flow_proofs(rows):
    return {code: [code, len(values), crawler._code_rows_digest(values, 'flow'), '2026-09-11 09:33:00']
            for code in sorted({row['stock_code'] for row in rows})
            for values in [[row for row in rows if row['stock_code'] == code]]}


def test_flow_later_partition_failure_keeps_earlier_commits(flow_publication):
    engine, connection, stage, rows = flow_publication
    connection.execute(text("CREATE TRIGGER reject_second_batch BEFORE INSERT ON sm_stock_capital_flow_min "
                            "WHEN NEW.stock_code='000017' BEGIN SELECT RAISE(ABORT,'second batch failed'); END"))
    connection.commit()
    progress = []
    with pytest.raises(RuntimeError, match='publication incomplete'):
        crawler._publish_flow_stage(engine, connection, stage, '2026-09-11',
                                   proofs=flow_proofs(rows), progress=progress.append)
    connection.rollback()
    with engine.connect() as reader:
        assert reader.execute(text("SELECT main_net_inflow FROM sm_stock_capital_flow_min "
                                   "WHERE stock_code='000001' LIMIT 1")).scalar_one() == 1
        assert reader.execute(text("SELECT main_net_inflow FROM sm_stock_capital_flow_min "
                                   "WHERE stock_code='000017' LIMIT 1")).scalar_one() == 0
        assert reader.execute(text("SELECT main_net_inflow FROM sm_stock_capital_flow_min "
                                   "WHERE stock_code='000033' LIMIT 1")).scalar_one() == 1
    assert progress[-1]['publication_completed_units'] == 17
    assert progress[-1]['publication_failed_codes'] == [f'{n:06}' for n in range(17, 33)]
    connection.execute(text('DROP TRIGGER reject_second_batch'))
    connection.commit()
    progress.clear()
    assert crawler._publish_flow_stage(engine, connection, stage, '2026-09-11',
                                      proofs=flow_proofs(rows), progress=progress.append) == 66
    assert progress[-1]['publication_written_rows'] == 32
    assert progress[-1]['publication_reused_rows'] == 34


def test_flow_publish_preserves_other_dates_and_missing_securities(flow_publication):
    engine, connection, stage, rows = flow_publication
    columns = ','.join(crawler.FLOW_WRITE_COLUMNS)
    values = ','.join(':' + key for key in crawler.FLOW_WRITE_COLUMNS)
    untouched = [{**rows[0], 'stock_code': '999999', 'main_net_inflow': 7},
                 {**rows[0], 'trade_time': '2026-09-10 09:31:00', 'main_net_inflow': 8}]
    connection.execute(text(f'INSERT INTO sm_stock_capital_flow_min ({columns}) VALUES ({values})'), untouched)
    connection.commit()
    assert crawler._publish_flow_stage(engine, connection, stage, '2026-09-11', proofs=flow_proofs(rows)) == 66
    with engine.connect() as reader:
        assert reader.execute(text("SELECT main_net_inflow FROM sm_stock_capital_flow_min "
                                   "WHERE stock_code='999999'")).scalar_one() == 7
        assert reader.execute(text("SELECT main_net_inflow FROM sm_stock_capital_flow_min "
                                   "WHERE trade_time='2026-09-10 09:31:00'")).scalar_one() == 8


def test_flow_bad_source_stage_does_not_replace_its_batch(flow_publication):
    engine, connection, stage, rows = flow_publication
    connection.execute(text(f"UPDATE {stage} SET main_net_inflow=9 WHERE stock_code='000001'"))
    connection.commit()
    with pytest.raises(RuntimeError, match='publication incomplete'):
        crawler._publish_flow_stage(engine, connection, stage, '2026-09-11', proofs=flow_proofs(rows))
    with engine.connect() as reader:
        assert reader.execute(text("SELECT main_net_inflow FROM sm_stock_capital_flow_min "
                                   "WHERE stock_code='000001' LIMIT 1")).scalar_one() == 0
        assert reader.execute(text("SELECT main_net_inflow FROM sm_stock_capital_flow_min "
                                   "WHERE stock_code='000017' LIMIT 1")).scalar_one() == 1


def test_flow_reuses_only_complete_rows_with_the_original_source_capture(flow_publication):
    engine, connection, stage, rows = flow_publication
    connection.execute(text('UPDATE sm_stock_capital_flow_min SET main_net_inflow=1'))
    connection.execute(text("DELETE FROM sm_stock_capital_flow_min WHERE stock_code='000017' "
                            "AND trade_time='2026-09-11 09:32:00'"))
    connection.execute(text("UPDATE sm_stock_capital_flow_min SET received_at='2026-09-11 09:32:59' "
                            "WHERE stock_code='000018'"))
    retained = list(connection.execute(text("SELECT id FROM sm_stock_capital_flow_min "
                                           "WHERE stock_code='000001' ORDER BY id")).scalars())
    connection.commit()
    progress = []
    assert crawler._publish_flow_stage(engine, connection, stage, '2026-09-11',
                                      proofs=flow_proofs(rows), progress=progress.append) == 66
    assert progress[-1]['publication_written_rows'] == 4
    assert progress[-1]['publication_reused_rows'] == 62
    assert list(connection.execute(text("SELECT id FROM sm_stock_capital_flow_min "
                                        "WHERE stock_code='000001' ORDER BY id")).scalars()) == retained
    assert set(connection.execute(text("SELECT received_at FROM sm_stock_capital_flow_min "
                                       "WHERE stock_code='000018'")).scalars()) == {'2026-09-11 09:33:00'}


def test_flow_oversized_existing_batch_is_not_deleted(flow_publication):
    engine, connection, stage, rows = flow_publication
    columns = ','.join(crawler.FLOW_WRITE_COLUMNS)
    values = ','.join(':' + key for key in crawler.FLOW_WRITE_COLUMNS)
    connection.execute(text(f'INSERT INTO sm_stock_capital_flow_min ({columns}) VALUES ({values})'),
                       [{**rows[0], 'main_net_inflow': 7}] * crawler.FLOW_PUBLISH_ROWS)
    connection.commit()
    with pytest.raises(RuntimeError, match='publication incomplete'):
        crawler._publish_flow_stage(engine, connection, stage, '2026-09-11', proofs=flow_proofs(rows))
    with engine.connect() as reader:
        assert reader.execute(text('SELECT COUNT(*) FROM sm_stock_capital_flow_min '
                                   'WHERE main_net_inflow=7')).scalar_one() == crawler.FLOW_PUBLISH_ROWS


def test_flow_publication_deadline_stops_before_replacement(flow_publication, monkeypatch):
    engine, connection, stage, rows = flow_publication
    monkeypatch.setattr(crawler, 'FLOW_PUBLISH_SECONDS', 0)
    progress = []
    with pytest.raises(TimeoutError, match='publication deadline'):
        crawler._publish_flow_stage(engine, connection, stage, '2026-09-11',
                                   proofs=flow_proofs(rows), progress=progress.append)
    assert progress[-1]['publication_completed_units'] == 0
    assert progress[-1]['publication_deadline_exceeded'] is True
    with engine.connect() as reader:
        assert reader.execute(text('SELECT COUNT(*) FROM sm_stock_capital_flow_min '
                                   'WHERE main_net_inflow=0')).scalar_one() == 66


def test_flow_lost_commit_reply_is_read_back_once_without_success(flow_publication, monkeypatch):
    engine, connection, stage, rows = flow_publication
    commit = engine.dialect.do_commit
    commits = []
    def lose_reply(dbapi_connection):
        commit(dbapi_connection)
        commits.append(True)
        if len(commits) == 1:
            raise RuntimeError('COMMIT reply lost after commit')
    monkeypatch.setattr(engine.dialect, 'do_commit', lose_reply)
    progress = []
    with pytest.raises(RuntimeError, match='COMMIT reply uncertain'):
        crawler._publish_flow_stage(engine, connection, stage, '2026-09-11',
                                   proofs=flow_proofs(rows), progress=progress.append)
    assert progress[-1]['publication_commit_uncertain'] is True
    assert progress[-1]['publication_commit_readback_matches'] is True
    assert progress[-1]['publication_completed_units'] == 16
    assert commits == [True]
    with engine.connect() as reader:
        assert reader.execute(text("SELECT COUNT(*) FROM sm_stock_capital_flow_min "
                                   "WHERE main_net_inflow=1")).scalar_one() == 32


def test_flow_lost_stage_connection_stops_without_restarting_or_losing_commits(flow_publication):
    engine, connection, stage, rows = flow_publication
    deletes = []
    @event.listens_for(connection, 'after_cursor_execute')
    def lose_stage(conn, _cursor, statement, _parameters, _context, _many):
        if statement.startswith('DELETE FROM'):
            deletes.append(True)
            if len(deletes) == 2:
                conn.invalidate()
                raise RuntimeError('stage connection lost')
    with pytest.raises(RuntimeError, match='stage connection lost'):
        crawler._publish_flow_stage(engine, connection, stage, '2026-09-11', proofs=flow_proofs(rows))
    assert len(deletes) == 2
    with engine.connect() as reader:
        assert reader.execute(text("SELECT COUNT(*) FROM sm_stock_capital_flow_min "
                                   "WHERE main_net_inflow=1")).scalar_one() == 32


def run_mysql_flow_publication_acceptance(engine):
    """Explicit operator entry point; only connection-local TEMPORARY targets.

    Call with the authorized minute engine. This does not choose credentials,
    start collectors, acquire the business publisher lock, or write business rows.
    """
    import uuid
    if engine.dialect.name != 'mysql':
        raise ValueError('real MySQL transaction acceptance requires MySQL')
    token = uuid.uuid4().hex[:12]
    target = 'flow_publish_accept_' + token
    original_lock = crawler.mysql_named_lock
    stage, connection = crawler._create_flow_stage(engine)
    @contextmanager
    def isolated_lock(bound_engine, _name, **kwargs):
        assert kwargs['connection'] is connection
        with original_lock(bound_engine, 'probiga:flow-accept:' + token, **kwargs) as bound:
            yield bound
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(crawler, 'FLOW_TABLE', target)
        patch.setattr(crawler, 'mysql_named_lock', isolated_lock)
        patch.setattr(crawler, '_now', lambda: datetime(2026, 9, 11, 9, 34))
        try:
            connection.execute(text(f'CREATE TEMPORARY TABLE `{target}` LIKE sm_stock_capital_flow_min'))
            assert connection.engine is not engine
            dbapi = connection.connection.driver_connection
            assert dbapi._read_timeout == dbapi._write_timeout == 30
            assert connection.execute(text('SELECT @@SESSION.innodb_lock_wait_timeout')).scalar_one() == 5
            rows = []
            for number in range(1, 34):
                for minute in (31, 32):
                    row = {field: 1 for field in crawler.VALUE_FIELDS['flow']}
                    row.update(stock_code=f'{number:06}', trade_time=f'2026-09-11 09:{minute}:00',
                               source_time=f'2026-09-11 09:{minute}:00', received_at='2026-09-11 09:33:00',
                               snapshot_at='2026-09-11 09:33:00', etl_sync_at='2026-09-11 09:33:00',
                               data_source=crawler.PUBLIC_MINUTE_SOURCE)
                    rows.append(row)
            columns = ','.join(crawler.FLOW_WRITE_COLUMNS)
            values = ','.join(':' + key for key in crawler.FLOW_WRITE_COLUMNS)
            connection.execute(text(f'INSERT INTO `{stage}` ({columns}) VALUES ({values})'), rows)
            connection.execute(text(f'INSERT INTO `{target}` ({columns}) VALUES ({values})'),
                               [{**row, 'main_net_inflow': 0} for row in rows])
            connection.commit()
            deletes = []
            def fail_second(_conn, _cursor, statement, _parameters, _context, _many):
                if statement.lstrip().startswith(f'DELETE FROM `{target}`'):
                    deletes.append(True)
                    if len(deletes) == 2:
                        raise RuntimeError('controlled failure after second batch DELETE')
            event.listen(connection, 'after_cursor_execute', fail_second)
            try:
                with pytest.raises(RuntimeError, match='publication incomplete'):
                    crawler._publish_flow_stage(engine, connection, stage, '2026-09-11', proofs=flow_proofs(rows))
            finally:
                event.remove(connection, 'after_cursor_execute', fail_second)
            connection.rollback()
            assert connection.execute(text(f'SELECT COUNT(*) FROM `{target}` WHERE main_net_inflow=1')).scalar_one() == 34
            assert connection.execute(text(f'SELECT COUNT(*) FROM `{target}` WHERE main_net_inflow=0')).scalar_one() == 32
            connection.commit()
            progress = []
            assert crawler._publish_flow_stage(engine, connection, stage, '2026-09-11',
                                              proofs=flow_proofs(rows), progress=progress.append) == 66
            assert progress[-1]['publication_reused_rows'] == 34
            assert progress[-1]['publication_written_rows'] == 32
            return dict(status='COMPLETE', permanent_rows_written=0, real_mysql_transactions=True,
                        temporary_source_rows=66, earlier_and_later_committed_rows_after_failure=34,
                        failed_batch_original_rows_preserved=32, resumed_reused_rows=34,
                        resumed_inserted_rows=32, named_lock_connection='same stage connection',
                        dedicated_engine=True, transport_read_write_seconds=30, lock_wait_seconds=5,
                        data_kind='synthetic isolated transaction fixture, not market observations')
        finally:
            try:
                if not connection.closed and not connection.invalidated:
                    connection.rollback()
                    connection.execute(text(f'DROP TEMPORARY TABLE IF EXISTS `{target}`'))
                    connection.commit()
            finally:
                crawler._drop_flow_stage(connection)
