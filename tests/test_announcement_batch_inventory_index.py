"""Execute batch membership queries over indexed multi-source history."""
from __future__ import annotations

from contextlib import contextmanager
from hashlib import sha256
import re
from time import monotonic
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from server.common.qmt_announcement_pit import (
    CNINFO_ANNOUNCEMENT_SOURCE,
    QMT_ANNOUNCEMENT_SOURCE,
    QMTAnnouncementBlocked,
    _load_live_announcement_batch_inventory,
)
from server.common.pit_source_coverage_index import PIT_SOURCE_COVERAGE_BATCH_INDEX_NAME


OLD_INDEX = "idx_pit_coverage_decision"
SOURCES = (QMT_ANNOUNCEMENT_SOURCE, CNINFO_ANNOUNCEMENT_SOURCE)
CODES = tuple(f"{number:06d}" for number in range(2, 804, 2))


def _coverage_id(source, batch, code):
    return sha256(f"{source}:{batch}:{code}".encode()).hexdigest()


def _member(source, batch, code, *, identity=None):
    return {
        "coverage_id": identity or _coverage_id(source, batch, code),
        "fact_kind": "event", "source": source, "batch_id": batch, "stock_code": code,
        "known_at": "2026-11-10 20:00:00", "window_start": "2026-10-11",
        "window_end": "2026-11-10",
    }


def _insert(connection, rows):
    connection.execute(text(
        "INSERT INTO st_pit_source_coverage VALUES "
        "(:coverage_id,:fact_kind,:source,:batch_id,:stock_code,:known_at,:window_start,:window_end)"
    ), rows)


@pytest.fixture(scope="module")
def inventory_engine():
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        # WITHOUT ROWID includes the primary identity in secondary indexes,
        # matching the covering behavior of the InnoDB batch index.
        connection.execute(text(
            "CREATE TABLE st_pit_source_coverage (coverage_id TEXT PRIMARY KEY,"
            "fact_kind TEXT,source TEXT,batch_id TEXT,stock_code TEXT,known_at TEXT,"
            "window_start TEXT,window_end TEXT) WITHOUT ROWID"
        ))
        connection.execute(text(
            f"CREATE INDEX {OLD_INDEX} ON st_pit_source_coverage "
            "(fact_kind,stock_code,known_at,window_start,window_end)"
        ))
        connection.execute(text(
            f"CREATE INDEX {PIT_SOURCE_COVERAGE_BATCH_INDEX_NAME} ON st_pit_source_coverage "
            "(fact_kind,source,batch_id,stock_code)"
        ))
        _insert(connection, [
            _member(source, f"history-{batch:02d}", code)
            for source in SOURCES for batch in range(32) for code in CODES
        ])
        _insert(connection, [
            _member(source, f"target-{count}", code)
            for source in SOURCES for count in (400, 401) for code in CODES[:count]
        ])
    yield engine
    engine.dispose()


@pytest.fixture
def inventory_connection(inventory_engine):
    with inventory_engine.connect() as connection:
        transaction = connection.begin()
        try:
            yield connection
        finally:
            transaction.rollback()


@contextmanager
def _query_work_budget(connection, *, steps=30_000):
    dbapi = connection.connection.driver_connection
    work = {"steps": 0}

    def progress():
        work["steps"] += 100
        return int(work["steps"] > steps)

    dbapi.set_progress_handler(progress, 100)
    try:
        yield work
    finally:
        dbapi.set_progress_handler(None, 0)


class _TupleResult:
    def __init__(self, rows):
        self.rows = rows

    def tuples(self):
        return self

    def all(self):
        return self.rows


class _MysqlIndexedInventoryConnection:
    """Use real SQLite indexes for each forced MySQL access path."""
    dialect = SimpleNamespace(name="mysql")

    def __init__(self, connection, *, override_index=None):
        self.connection = connection
        self.override_index = override_index
        self.calls = []

    def execute(self, statement, parameters):
        sql = str(statement)
        forced = re.search(r" FORCE INDEX \((\w+)\)", sql)
        assert forced is not None
        index = self.override_index or forced.group(1)
        translated = sql[:forced.start()] + " INDEXED BY " + index + sql[forced.end():]
        self.calls.append((index, dict(parameters), self.connection.get_transaction()))
        try:
            rows = self.connection.execute(text(translated), parameters).tuples().all()
        except OperationalError as exc:
            if "interrupted" not in str(exc.orig):
                raise
            # The deterministic VM work budget represents the observed MySQL
            # query deadline, preserving the real reader's 3024 handling.
            raise OperationalError(
                sql, parameters, Exception(3024, "query execution work budget exceeded"),
            ) from exc
        return _TupleResult(rows)


def _read(connection, *, count=401, source=QMT_ANNOUNCEMENT_SOURCE):
    return _load_live_announcement_batch_inventory(
        connection, source=source, batch_id=f"target-{count}",
        catalog_codes=list(reversed(CODES[:count])), deadline=monotonic() + 5,
    )


@pytest.mark.parametrize("source", SOURCES)
def test_batch_index_avoids_first_400_historical_scan_deadline(inventory_connection, source):
    legacy = _MysqlIndexedInventoryConnection(inventory_connection, override_index=OLD_INDEX)
    with _query_work_budget(inventory_connection):
        with pytest.raises(QMTAnnouncementBlocked, match="LIVE_BATCH_REPLY_DEADLINE:batch-inventory:0"):
            _read(legacy, source=source)
    assert len(legacy.calls) == 1
    assert legacy.calls[0][1]["inventory_limit"] == 401
    assert legacy.calls[0][1]["upper_code"] == CODES[400]

    reader = _MysqlIndexedInventoryConnection(inventory_connection)
    with _query_work_budget(inventory_connection):
        actual = _read(reader, source=source)
    assert actual == [(_coverage_id(source, "target-401", code), code) for code in CODES]
    assert len(reader.calls) == 2
    assert all(index == PIT_SOURCE_COVERAGE_BATCH_INDEX_NAME for index, _, _ in reader.calls)
    assert all(active is inventory_connection.get_transaction() for _, _, active in reader.calls)


@pytest.mark.parametrize("count", [400, 401])
def test_batch_membership_keeps_open_ranges_and_exact_400_boundary(inventory_connection, count):
    reader = _MysqlIndexedInventoryConnection(inventory_connection)
    actual = _read(reader, count=count)
    expected_codes = CODES[:count]
    assert actual == [(_coverage_id(SOURCES[0], f"target-{count}", code), code) for code in expected_codes]
    ranges = [
        (params.get("lower_code"), params.get("upper_code"), params["inventory_limit"])
        for _, params, _ in reader.calls
    ]
    assert ranges == (
        [(None, None, 401)] if count == 400
        else [(None, CODES[400], 401), (CODES[400], None, 2)]
    )


@pytest.mark.parametrize("count", [400, 401])
@pytest.mark.parametrize("damage", ["extra-below", "extra-gap", "extra-above", "duplicate", "missing"])
def test_batch_index_cannot_hide_unknown_duplicate_or_missing_members(inventory_connection, count, damage):
    source, batch = SOURCES[0], f"target-{count}"
    if damage == "missing":
        inventory_connection.execute(text(
            "DELETE FROM st_pit_source_coverage WHERE coverage_id=:identity"
        ), {"identity": _coverage_id(source, batch, CODES[200])})
    else:
        code = {
            "extra-below": "000001", "extra-gap": "000403",
            "extra-above": "000999", "duplicate": CODES[200],
        }[damage]
        row = _member(source, batch, code, identity=sha256(f"damage:{damage}".encode()).hexdigest())
        # A future-known extra row must still invalidate complete membership.
        row["known_at"] = "2026-11-12 20:00:00"
        _insert(inventory_connection, [row])
    reader = _MysqlIndexedInventoryConnection(inventory_connection)
    with pytest.raises(ValueError, match="batch stock set differs from catalog"):
        _read(reader, count=count)
    assert reader.calls
    assert all(index == PIT_SOURCE_COVERAGE_BATCH_INDEX_NAME for index, _, _ in reader.calls)
