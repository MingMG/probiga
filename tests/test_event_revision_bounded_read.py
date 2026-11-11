from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
import hashlib
import re

import pytest
from sqlalchemy import event, text

from server.common import pit_facts as pit
from server.common import qmt_announcement_pit as announcements
from tests.test_pit_finance_event_facts import _engine, _mysql_seal_transport


SOURCE = "cninfo.notice"
DECISION = datetime(2026, 11, 10, 20)
CUTOFF = datetime(2026, 11, 10, 14)
START = date(2026, 11, 9)
END = date(2026, 11, 10)


@dataclass(frozen=True)
class EventRead:
    phase: str
    keys: tuple[str, ...]
    sql: str
    connection: object
    transaction: object
    streaming: bool


def _mysql_event_transport(engine, *, isolation_level="REPEATABLE READ"):
    transport = _mysql_seal_transport(engine)

    class EventConnection(transport.connect):
        def get_isolation_level(self):
            return isolation_level

    transport.connect = EventConnection
    return transport


@contextmanager
def _trace_event_reads(engine, *, head_budget=None, after_head=None):
    reads = []
    drivers = set()
    interrupted = []

    def before(connection, cursor, sql, parameters, context, many):
        driver = connection.connection.driver_connection
        drivers.add(driver)
        driver.set_progress_handler(None, 0)
        if f"FROM {pit.EVENT_REVISION_TABLE}" not in sql:
            return sql, parameters
        if "ROW_NUMBER()" in sql:
            phase, bind = "heads", "codes_"
        elif "COMPRESS(payload_json)" in sql:
            phase, bind = "payload", "row_ids_"
        elif "OCTET_LENGTH(payload_json)" in sql:
            phase, bind = "size", "row_ids_"
        else:
            phase, bind = "history", "identities_"
        keys = tuple(
            value for key, value in context.compiled_parameters[0].items() if key.startswith(bind)
        )
        reads.append(EventRead(
            phase, keys, sql, connection, connection.get_transaction(),
            context.execution_options.get("stream_results", False),
        ))
        if phase == "heads" and head_budget is not None:
            steps = 0

            def progress():
                nonlocal steps
                steps += 100
                if steps == head_budget:
                    interrupted.append(keys)
                    return 1
                return 0

            driver.set_progress_handler(progress, 100)
        sql = sql.replace(" FORCE INDEX (idx_pit_event_decision)", " INDEXED BY idx_pit_event_decision")
        sql = sql.replace(
            " FORCE INDEX (uk_pit_event_identity_revision)", " INDEXED BY uk_pit_event_identity_revision",
        )
        return sql, parameters

    def after(_connection, _cursor, sql, _parameters, _context, _many):
        if after_head is not None and f"FROM {pit.EVENT_REVISION_TABLE}" in sql and "ROW_NUMBER()" in sql:
            after_head(reads[-1])

    event.listen(engine, "before_cursor_execute", before, retval=True)
    event.listen(engine, "after_cursor_execute", after)
    try:
        yield reads, interrupted
    finally:
        event.remove(engine, "after_cursor_execute", after)
        event.remove(engine, "before_cursor_execute", before)
        for driver in drivers:
            driver.set_progress_handler(None, 0)


def _append_notice(connection, *, code, key, published, known, title, source=SOURCE):
    return pit.append_event_revision(connection, {
        "stock_code": code, "event_key": key, "notice_date": "2026-11-11",
        "display_time": published, "title": title,
    }, known_at=known, source=source, batch_id=f"capture-{known}")


@pytest.fixture
def market_events():
    engine = _engine()
    codes = [f"{code:06d}" for code in range(1, 121)]
    with engine.begin() as connection:
        connection.execute(text(
            f"CREATE INDEX idx_pit_event_decision ON {pit.EVENT_REVISION_TABLE}"
            "(stock_code,event_date,known_at,published_at)"
        ))
        connection.execute(text(
            f"CREATE INDEX uk_pit_event_identity_revision ON {pit.EVENT_REVISION_TABLE}(identity_hash,revision_no)"
        ))
        for revision in (1, 2, 3):
            pit.append_event_revisions(connection, [
                {
                    "stock_code": code, "event_key": f"market-{identity}",
                    "notice_date": "2026-11-11",
                    "display_time": f"2026-11-{10 if identity < 11 else 1:02d} {8 + revision:02d}:00:00",
                    "title": f"market revision {revision}",
                }
                for code in codes[:-1] for identity in range(12)
            ], known_at=f"2026-11-10 {10 + revision:02d}:00:00", source=SOURCE, batch_id=f"market-{revision}")
        early = _append_notice(
            connection, code=codes[0], key="same-publication", published="2026-11-10 10:00:00",
            known="2026-11-10 10:01:00", title="original before market cutoff",
        )
        _append_notice(
            connection, code=codes[0], key="same-publication", published="2026-11-10 10:00:00",
            known="2026-11-10 16:00:00", title="late correction sharing publication time",
        )
        _append_notice(
            connection, code=codes[0], key="moved-out", published="2026-11-10 10:00:00",
            known="2026-11-10 10:01:00", title="older head inside window",
        )
        _append_notice(
            connection, code=codes[0], key="moved-out", published="2026-11-08 10:00:00",
            known="2026-11-10 12:00:00", title="latest head outside window",
        )
        old_prefix = _append_notice(
            connection, code=codes[40], key="moved-in", published="2026-11-08 10:00:00",
            known="2026-11-10 10:01:00", title="old prefix outside window",
        )
        new_head = _append_notice(
            connection, code=codes[40], key="moved-in", published="2026-11-10 10:00:00",
            known="2026-11-10 12:00:00", title="latest head inside window",
        )
        _append_notice(
            connection, code=codes[0], key="future-knowledge", published="2026-11-10 10:00:00",
            known="2026-11-10 21:00:00", title="not known by decision",
        )
        _append_notice(
            connection, code=codes[0], key="alternate-source", published="2026-11-10 10:00:00",
            known="2026-11-10 12:00:00", title="separate source event", source="eastmoney.notice",
        )
    yield engine, codes, early, old_prefix, new_head
    engine.dispose()


def _event_facts(engine, codes):
    return pit.load_event_facts(
        engine, codes=codes, decision_at=DECISION, fact_cutoff_at=CUTOFF,
        start_date=START, end_date=END,
    )


def test_bounded_event_reads_pass_the_same_real_head_query_budget(market_events, monkeypatch):
    engine, codes, early, old_prefix, new_head = market_events
    mysql_reader = _mysql_event_transport(engine)
    with monkeypatch.context() as full_market:
        full_market.setattr(announcements, "VALIDATION_READ_BATCH_SIZE", len(codes))
        expected = _event_facts(engine, codes)
        with _trace_event_reads(engine, head_budget=180_000) as (_, interrupted):
            unbounded = _event_facts(mysql_reader, codes)
        assert interrupted == [tuple(codes)]
        assert unbounded.global_status == pit.PIT_SCHEMA_UNAVAILABLE
        assert unbounded.facts == {}

    with _trace_event_reads(engine, head_budget=180_000) as (reads, interrupted):
        actual = _event_facts(mysql_reader, list(reversed(codes)) + [codes[0]])
    assert interrupted == []
    assert actual == expected
    assert actual.status_for(codes[-1]) == pit.PIT_NO_ROWS
    assert {fact["event_revision_id"] for fact in actual.facts[codes[0]]} >= {early.revision_id}
    assert {fact["title"] for fact in actual.facts[codes[0]]}.isdisjoint({
        "late correction sharing publication time", "older head inside window", "latest head outside window",
        "not known by decision",
    })
    assert any(fact["event_revision_id"] == new_head.revision_id for fact in actual.facts[codes[40]])
    assert [read.keys for read in reads if read.phase == "heads"] == [
        tuple(codes[offset:offset + 40]) for offset in range(0, len(codes), 40)
    ]
    assert len({read.connection for read in reads}) == len({read.transaction for read in reads}) == 1
    assert all(read.transaction is not None and read.streaming for read in reads)
    for read in reads:
        hint = re.search(r"MAX_EXECUTION_TIME\((\d+)\)", read.sql)
        assert hint and 0 < int(hint[1]) <= announcements.LIVE_ANNOUNCEMENT_BATCH_QUERY_TIMEOUT_MS
    history = [read for read in reads if read.phase == "history"]
    sizes = [read for read in reads if read.phase == "size"]
    payloads = [read for read in reads if read.phase == "payload"]
    assert max(len(read.keys) for read in history) == 400
    assert max(len(read.keys) for read in sizes) == max(len(read.keys) for read in payloads) == 480
    assert {key for read in payloads for key in read.keys} == {key for read in sizes for key in read.keys}
    assert sum(len(read.keys) for read in payloads) == len({key for read in payloads for key in read.keys})
    assert old_prefix.revision_id in {key for read in payloads for key in read.keys}

    with engine.begin() as connection:
        outer_transaction = connection.get_transaction()
        exact_source = pit._query_revisions(
            connection, table_name=pit.EVENT_REVISION_TABLE, codes=[codes[0], codes[40]],
            decision_at=DECISION, start_date=START, end_date=END, source=SOURCE, batch_id="market-3",
        )
        assert connection.get_transaction() is outer_transaction
    assert {row["source"] for row in exact_source} == {SOURCE}
    assert {row["revision_id"] for row in exact_source} >= {old_prefix.revision_id, new_head.revision_id}


def test_event_read_still_rejects_a_corrupt_old_revision_outside_the_window(market_events):
    engine, codes, _, old_prefix, _ = market_events
    with engine.begin() as connection:
        connection.execute(text("DROP TRIGGER trg_pit_event_revision_immutable_bu"))
        connection.execute(text(
            f"UPDATE {pit.EVENT_REVISION_TABLE} SET content_hash='broken' WHERE revision_id=:revision_id"
        ), {"revision_id": old_prefix.revision_id})
    requested = [codes[0], codes[40]]
    expected = _event_facts(engine, requested)
    actual = _event_facts(_mysql_event_transport(engine), requested)
    assert actual == expected
    assert actual.status_for(codes[0]) == pit.PIT_AVAILABLE
    assert actual.status_for(codes[40]) == pit.PIT_DATA_BLOCKED
    assert actual.reason_for(codes[40]).startswith("PIT_EVENT_BAD_CHAIN:")


def test_event_reads_reject_read_committed_before_querying_revisions(market_events):
    engine, codes, _, _, _ = market_events
    mysql_reader = _mysql_event_transport(engine, isolation_level="READ COMMITTED")
    with _trace_event_reads(engine) as (reads, _):
        actual = _event_facts(mysql_reader, codes)
    assert reads == []
    assert actual.global_status == pit.PIT_SCHEMA_UNAVAILABLE
    assert actual.facts == {}
    assert "event-revision-snapshot" in actual.reason_for(codes[0])


def test_event_payload_batch_limit_applies_to_actual_uncompressed_bytes():
    engine = _engine()
    code = "000001"
    body = hashlib.shake_256(b"large event source capture").hexdigest(540_000)
    with engine.begin() as connection:
        for revision in range(5):
            pit.append_event_revision(connection, {
                "stock_code": code, "event_key": "large-disclosure", "notice_date": "2026-11-10",
                "display_time": f"2026-11-10 {8 + revision:02d}:00:00", "title": f"revision {revision}",
                "source_detail": body,
            }, known_at=f"2026-11-10 {9 + revision:02d}:00:00", source=SOURCE)
        sizes = dict(connection.execute(text(
            f"SELECT revision_id,LENGTH(CAST(payload_json AS BLOB)) FROM {pit.EVENT_REVISION_TABLE}"
        )).all())
    expected = _event_facts(engine, [code])
    mysql_reader = _mysql_event_transport(engine)
    with _trace_event_reads(engine) as (reads, _):
        actual = _event_facts(mysql_reader, [code])
    assert actual == expected
    payloads = [read.keys for read in reads if read.phase == "payload"]
    limit = announcements.VALIDATION_READ_PAYLOAD_BYTES
    assert sum(sizes.values()) > limit
    assert [len(keys) for keys in payloads] == [3, 2]
    assert all(sum(sizes[key] for key in keys) <= limit for keys in payloads)
    assert {key for keys in payloads for key in keys} == set(sizes)
    engine.dispose()


def test_later_event_revision_query_exhausting_total_deadline_fails_closed(market_events, monkeypatch):
    engine, codes, _, _, _ = market_events
    mysql_reader = _mysql_event_transport(engine)
    clock = [100.0]
    monkeypatch.setattr(pit, "monotonic", lambda: clock[0])
    monkeypatch.setattr(announcements, "monotonic", lambda: clock[0])

    def expire_on_second_head(read):
        if read.keys[0] == codes[40]:
            clock[0] += 30 * 60

    with _trace_event_reads(engine, after_head=expire_on_second_head) as (reads, _):
        actual = _event_facts(mysql_reader, codes)
    assert [read.keys[0] for read in reads if read.phase == "heads"] == [codes[0], codes[40]]
    assert any(read.phase == "payload" for read in reads)
    assert actual.global_status == pit.PIT_SCHEMA_UNAVAILABLE
    assert actual.facts == actual.coverage_by_code == {}
    assert set(actual.status_by_code) == set(codes)
    assert set(actual.status_by_code.values()) == {pit.PIT_SCHEMA_UNAVAILABLE}
    assert len(set(actual.reason_by_code.values())) == 1
    reason = actual.reason_for(codes[0])
    assert reason.startswith("PIT_EVENT_SCHEMA_UNAVAILABLE:")
    assert "event-revision-identities:40" in reason
    assert announcements.LIVE_ANNOUNCEMENT_BATCH_VALIDATION_DEADLINE in reason
