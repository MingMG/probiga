"""Exercise the installed PyMySQL bulk path and real transaction rollback."""
from __future__ import annotations

from datetime import date, datetime, timedelta
import socket
import threading
import time
from types import SimpleNamespace

import pymysql
import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.dialects.mysql.pymysql import dialect

from server.common import qmt_stock_catalog as catalog
from server.common import qmt_trade_calendar as calendar


SERVER_NOW = datetime(2031, 2, 3, 4, 5, 6)


class _WireCursor(pymysql.cursors.Cursor):
    """Keep real executemany/mogrify; replace only the network execute edge."""

    def __init__(self, connection):
        super().__init__(connection)
        self.sent = []
        self.timeouts = []

    def execute(self, query, args=None):
        wire = self.mogrify(query, args)
        if isinstance(wire, (bytes, bytearray)):
            wire = bytes(wire).decode(self.connection.encoding)
        self.sent.append(wire)
        self.timeouts.append((
            self.connection._read_timeout, self.connection._write_timeout,
        ))
        self.rowcount = wire.count("),(") + 1
        self._executed = wire
        return self.rowcount


class _DriverConnection:
    def __init__(self):
        dbapi = pymysql.Connection(
            defer_connect=True, charset="utf8mb4",
            read_timeout=45, write_timeout=50,
        )
        dbapi.server_status = 0
        self.connection = SimpleNamespace(dbapi_connection=dbapi)
        self.cursor = _WireCursor(dbapi)

    def execute(self, statement, params=None):
        compiled = statement.compile(dialect=dialect())
        query = str(compiled)
        def dbapi_params(values):
            return tuple(values[name] for name in compiled.positiontup)
        if isinstance(params, list):
            self.cursor.executemany(query, [dbapi_params(row) for row in params])
        else:
            self.cursor.execute(query, dbapi_params(params) if params else None)
        return SimpleNamespace(scalar_one=lambda: SERVER_NOW)


def _catalog_arguments(count):
    members = [{
        "qmt_code": f"{index:06d}.SZ",
        "stock_code": f"{index:06d}",
        "list_date": "1991-01-01",
        "expire_date": None,
        "instrument_batch_id": "instrument-proof",
        "instrument_type": "STOCK",
    } for index in range(1, count + 1)]
    return dict(
        batch_id="catalog-test", captured_at="2026-10-11 15:00:00",
        history_complete_from="1990-01-01", members=members,
        discovery=catalog.build_catalog_discovery(
            current_sectors=catalog.NATIVE_A_SHARE_SECTORS,
            expired_sectors=(),
            sector_members=[{
                "sector_name": "深证A股", "qmt_code": member["qmt_code"],
            } for member in members],
        ),
    )


def _calendar_arguments(count):
    sessions = [
        (date(2020, 1, 1) + timedelta(days=index)).isoformat()
        for index in range(count)
    ]
    return dict(
        batch_id="calendar-test", known_at="2026-10-11 15:00:00",
        start_date=sessions[0], end_date=sessions[-1], sessions=sessions,
        source_batch_id=calendar.calendar_source_batch_id(
            start_date=sessions[0], end_date=sessions[-1], sessions=sessions,
        ),
    )


def _publish(connection, kind, count=1213):
    if kind == "catalog":
        return catalog.insert_catalog_batch(connection, **_catalog_arguments(count))
    return calendar.insert_trade_calendar_receipt(
        connection, **_calendar_arguments(count),
    )


def _child_table(kind):
    return (
        "qmt_stock_catalog_member" if kind == "catalog"
        else "qmt_trade_calendar_session"
    )


def _parent_table(kind):
    return (
        "qmt_stock_catalog_batch" if kind == "catalog"
        else "qmt_trade_calendar_batch"
    )


@pytest.mark.parametrize("kind", ["catalog", "calendar"])
def test_real_pymysql_now_values_reproduces_per_row_fallback(kind):
    connection = _DriverConnection()
    columns = (
        "batch_id,qmt_code,created_at" if kind == "catalog"
        else "batch_id,trade_date,created_at"
    )
    connection.cursor.executemany(
        f"INSERT INTO {_child_table(kind)} ({columns}) VALUES (%s,%s,NOW())",
        [("batch", str(index)) for index in range(25)],
    )
    # This runs the installed driver's regex decision and fallback, not a
    # source-text assertion or a mock of executemany itself.
    assert len(connection.cursor.sent) == 25


@pytest.mark.parametrize("kind", ["catalog", "calendar"])
def test_real_pymysql_publication_is_bounded_bulk_with_one_server_clock(kind):
    connection = _DriverConnection()
    result = _publish(connection, kind)
    sent = connection.cursor.sent
    child = [sql for sql in sent if f"INSERT INTO {_child_table(kind)}" in sql]
    assert len(child) == 3  # 500 + 500 + 213 rows, not 1213 WAN queries.
    assert [sql.count("),(") + 1 for sql in child] == [500, 500, 213]
    assert all(len(sql.encode("utf-8")) <= catalog.REFERENCE_INSERT_MAX_BYTES
               for sql in child)
    assert sent.count("SELECT NOW()") == 1
    assert all("NOW()" not in sql for sql in child)
    assert all("2031-02-03 04:05:06" in sql for sql in child)
    assert all(0 < read <= 30 and 0 < write <= 30
               for read, write in connection.cursor.timeouts)
    dbapi = connection.connection.dbapi_connection
    assert (dbapi._read_timeout, dbapi._write_timeout) == (45, 50)
    assert result["manifest_hash"] == catalog.canonical_digest({
        key: value for key, value in result.items() if key != "manifest_hash"
    })


@pytest.mark.parametrize("kind", ["catalog", "calendar"])
def test_bulk_chunks_obey_escaped_byte_cap_before_row_cap(monkeypatch, kind):
    monkeypatch.setattr(catalog, "REFERENCE_INSERT_MAX_BYTES", 4000)
    connection = _DriverConnection()
    _publish(connection, kind, count=100)
    child = [sql for sql in connection.cursor.sent
             if f"INSERT INTO {_child_table(kind)}" in sql]
    assert len(child) > 1
    assert all(len(sql.encode("utf-8")) <= 4000 for sql in child)
    assert sum(sql.count("),(") + 1 for sql in child) == 100


@pytest.mark.parametrize("bound", ["call", "total"])
def test_socket_watchdog_bounds_a_blocked_whole_call_and_restores_limits(monkeypatch, bound):
    # Real local sockets, no server/network credentials or MySQL mutation.
    reader, peer = socket.socketpair()
    dbapi = SimpleNamespace(_sock=reader, _read_timeout=45, _write_timeout=50)
    connection = SimpleNamespace(connection=SimpleNamespace(dbapi_connection=dbapi))
    def driver_read(*_args):
        # Match PyMySQL._read_bytes: it installs _read_timeout on its socket.
        reader.settimeout(dbapi._read_timeout)
        return reader.recv(1)
    connection.execute = driver_read
    if bound == "call":
        monkeypatch.setattr(catalog, "REFERENCE_WRITE_CALL_TIMEOUT_SECONDS", 0.02)
    else:
        monkeypatch.setattr(catalog, "REFERENCE_WRITE_DEADLINE_SECONDS", 0.02)
    started = time.monotonic()
    try:
        reason = "statement timeout" if bound == "call" else "deadline exceeded"
        with pytest.raises(TimeoutError, match=f"{reason} at blocked_insert"):
            with catalog._ReferenceWriteBudget(connection, "publication") as budget:
                budget.execute(text("INSERT"), stage="blocked_insert")
        assert time.monotonic() - started < 1
        assert (dbapi._read_timeout, dbapi._write_timeout) == (45, 50)
        peer.settimeout(1)
        assert peer.recv(1) == b""
    finally:
        reader.close()
        peer.close()


def test_watchdog_stops_fragmented_reply_that_never_hits_per_read_timeout(monkeypatch):
    reader, peer = socket.socketpair()
    dbapi = SimpleNamespace(_sock=reader, _read_timeout=45, _write_timeout=50)
    connection = SimpleNamespace(connection=SimpleNamespace(dbapi_connection=dbapi))

    def stream_fragments():
        until = time.monotonic() + 0.4
        try:
            while time.monotonic() < until:
                peer.sendall(b"x")
                time.sleep(0.005)
        except OSError:
            pass
        finally:
            peer.close()

    def driver_read(*_args):
        reader.settimeout(dbapi._read_timeout)
        while reader.recv(1):
            pass

    connection.execute = driver_read
    monkeypatch.setattr(catalog, "REFERENCE_WRITE_CALL_TIMEOUT_SECONDS", 0.04)
    producer = threading.Thread(target=stream_fragments, daemon=True)
    producer.start()
    started = time.monotonic()
    try:
        with pytest.raises(TimeoutError, match="statement timeout at fragmented_insert"):
            with catalog._ReferenceWriteBudget(connection, "publication") as budget:
                budget.execute(text("INSERT"), stage="fragmented_insert")
        assert time.monotonic() - started < 0.25
    finally:
        reader.close()
        producer.join(timeout=1)


def _sqlite_engine():
    engine = create_engine("sqlite:///:memory:")

    @event.listens_for(engine, "connect")
    def server_time_and_fk(dbapi, _record):
        dbapi.create_function("NOW", 0, lambda: SERVER_NOW.isoformat(sep=" "))
        dbapi.execute("PRAGMA foreign_keys=ON")

    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE qmt_stock_catalog_batch (
                batch_id TEXT PRIMARY KEY, captured_at TEXT,
                history_complete_from TEXT, status TEXT, member_count INTEGER,
                member_set_hash TEXT, manifest_json TEXT, manifest_hash TEXT,
                created_at TEXT NOT NULL
            )
        """))
        connection.execute(text("""
            CREATE TABLE qmt_stock_catalog_member (
                batch_id TEXT, qmt_code TEXT, stock_code TEXT, list_date TEXT,
                expire_date TEXT, instrument_batch_id TEXT, instrument_type TEXT,
                created_at TEXT NOT NULL, PRIMARY KEY(batch_id,qmt_code),
                UNIQUE(batch_id,stock_code),
                FOREIGN KEY(batch_id) REFERENCES qmt_stock_catalog_batch(batch_id)
            )
        """))
        connection.execute(text("""
            CREATE TABLE qmt_trade_calendar_batch (
                batch_id TEXT PRIMARY KEY, source_batch_id TEXT, known_at TEXT,
                start_date TEXT, end_date TEXT, status TEXT, session_count INTEGER,
                session_set_hash TEXT, manifest_json TEXT, manifest_hash TEXT,
                created_at TEXT NOT NULL
            )
        """))
        connection.execute(text("""
            CREATE TABLE qmt_trade_calendar_session (
                batch_id TEXT, trade_date TEXT, created_at TEXT NOT NULL,
                PRIMARY KEY(batch_id,trade_date),
                FOREIGN KEY(batch_id) REFERENCES qmt_trade_calendar_batch(batch_id)
            )
        """))
    return engine


@pytest.mark.parametrize("kind", ["catalog", "calendar"])
def test_mid_chunk_failure_rolls_back_header_and_prior_chunk(kind):
    engine = _sqlite_engine()
    chunks = 0

    @event.listens_for(engine, "before_cursor_execute")
    def fail_second_chunk(_conn, _cursor, statement, _params, _ctx, _many):
        nonlocal chunks
        if f"INSERT INTO {_child_table(kind)}" in statement:
            chunks += 1
            if chunks == 2:
                raise RuntimeError("second chunk failed")

    with pytest.raises(RuntimeError, match="second chunk failed"):
        with engine.begin() as connection:
            _publish(connection, kind)
    assert chunks == 2
    with engine.connect() as connection:
        for name in (_parent_table(kind), _child_table(kind)):
            assert connection.execute(text(f"SELECT COUNT(*) FROM {name}")).scalar() == 0
    engine.dispose()


@pytest.mark.parametrize("kind", ["catalog", "calendar"])
def test_deadline_after_first_chunk_rolls_back_without_another_chunk(monkeypatch, kind):
    engine = _sqlite_engine()
    clock = [0.0]
    monkeypatch.setattr(catalog.time, "monotonic", lambda: clock[0])

    @event.listens_for(engine, "after_cursor_execute")
    def exceed_deadline(_conn, _cursor, statement, _params, _ctx, _many):
        if f"INSERT INTO {_child_table(kind)}" in statement:
            clock[0] = catalog.REFERENCE_WRITE_DEADLINE_SECONDS + 1

    with pytest.raises(TimeoutError, match=f"deadline exceeded at {_child_table(kind)}"):
        with engine.begin() as connection:
            _publish(connection, kind)
    with engine.connect() as connection:
        for name in (_parent_table(kind), _child_table(kind)):
            assert connection.execute(text(f"SELECT COUNT(*) FROM {name}")).scalar() == 0
    engine.dispose()


def test_calendar_failure_also_rolls_back_catalog_in_same_caller_transaction():
    engine = _sqlite_engine()
    chunks = 0

    @event.listens_for(engine, "before_cursor_execute")
    def fail_calendar(_conn, _cursor, statement, _params, _ctx, _many):
        nonlocal chunks
        if "INSERT INTO qmt_trade_calendar_session" in statement:
            chunks += 1
            if chunks == 2:
                raise RuntimeError("calendar chunk failed")

    with pytest.raises(RuntimeError, match="calendar chunk failed"):
        with engine.begin() as connection:
            _publish(connection, "catalog")
            _publish(connection, "calendar")
    with engine.connect() as connection:
        for kind in ("catalog", "calendar"):
            for name in (_parent_table(kind), _child_table(kind)):
                assert connection.execute(text(f"SELECT COUNT(*) FROM {name}")).scalar() == 0
    engine.dispose()


@pytest.mark.parametrize("kind", ["catalog", "calendar"])
def test_publication_roundtrip_preserves_canonical_manifest_and_parent_fk(kind):
    engine = _sqlite_engine()
    with engine.begin() as connection:
        result = _publish(connection, kind, count=12)
    with engine.connect() as connection:
        if kind == "catalog":
            proof = catalog.load_stock_catalog(
                connection, decision_known_at="2026-10-11 15:00:00",
                batch_id=result["batch_id"],
            )
            assert proof.member_count == 12
        else:
            proof = calendar.load_trade_calendar_receipt(
                connection, start_date=result["start_date"],
                end_date=result["end_date"], decision_known_at=result["known_at"],
                batch_id=result["batch_id"],
            )
            assert proof.session_count == 12
        assert proof.manifest_hash == result["manifest_hash"]
        timestamps = connection.execute(text(
            f"SELECT DISTINCT created_at FROM {_child_table(kind)}"
        )).scalars().all()
        assert timestamps == [SERVER_NOW.isoformat(sep=" ")]
    engine.dispose()
