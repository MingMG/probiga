"""Actual SQLite transactions and SQLAlchemy statements, not MySQL acceptance.

The tables deliberately have a NON-UNIQUE minute identity index. These tests
therefore cannot accidentally validate an ON DUPLICATE KEY implementation.
"""
from copy import deepcopy
from dataclasses import replace
from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import (Column, Date, DateTime, Index, Integer, MetaData, Numeric,
                        String, Table, create_engine, event, select)
from sqlalchemy.dialects import mysql
from sqlalchemy.engine import Connection

from acquisition.datasets import get_spec
from acquisition.minute_grid import grids, proof
from acquisition.models import NormalizedBatch, NormalizedUnit, WorkUnit
from acquisition.store import STATE, SchemaMismatch, StaleRequest, Store


NOW = datetime(2026, 9, 30, 16)
REQUEST = "minute-bulk-request"


def fixture(dataset="stock_minute", codes=("000001.SZ",), *, required=False,
            allowed=False, error=False):
    spec = get_spec(dataset)
    md = MetaData()
    columns = [
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column(spec.code_column, String(16), nullable=False),
        Column("trade_time", DateTime, nullable=False), Column("trade_date", Date, nullable=False),
        Column("price", Numeric(18, 6), nullable=False), Column("volume", Numeric(18, 6)),
        Column("amount", Numeric(18, 6)), Column("note", String(32)),
        Column("data_source", String(32), nullable=False), Column("qmt_code", String(16), nullable=False),
        Column("received_at", DateTime, nullable=False), Column("etl_sync_at", DateTime, nullable=False),
        Column("source_time", DateTime, nullable=False), Column("batch_id", String(64), nullable=False),
        Column("data_version", String(64), nullable=False),
    ]
    if required:
        columns.append(Column("vendor_required", String(32), nullable=False))
    table = Table(spec.table, md, *columns)
    Index("idx_minute_code_time", table.c[spec.code_column], table.c.trade_time)
    engine = create_engine("sqlite:///:memory:")
    md.create_all(engine)
    store = Store(engine)
    store.prepare_progress_schema()
    store.validate_spec(spec)  # Reflection/installation is not commit SQL.
    units = []
    for code in codes:
        unit = WorkUnit(spec.name, spec.source, "2026-09-30", code, "1m", "none")
        required_grid, allowed_grid = grids(spec, code)
        rows = [{spec.code_column: code.split(".")[0],
                 "trade_time": datetime.fromisoformat(unit.target_date + " " + stamp),
                 "trade_date": unit.target_date, "price": Decimal("10"),
                 "volume": Decimal("100"), "amount": Decimal("1000")}
                for stamp in (allowed_grid if allowed else required_grid)]
        if required:
            for row in rows:
                row["vendor_required"] = "known"
        units.append(NormalizedUnit(unit, "complete", rows, detail={
            "minute_grid_proof": proof(spec, unit, rows),
            "missing_expected_rows": 0, "out_of_scope_rows": 0}))
    if error:
        unit = WorkUnit(spec.name, spec.source, "2026-09-30", "000020.SZ", "1m", "none")
        units.append(NormalizedUnit(unit, "error", [], "EMPTY_NATIVE_RESULT", "empty"))
    batch = NormalizedBatch(REQUEST, units, NOW)
    store.begin_request([item.unit for item in units], REQUEST, NOW)
    return engine, store, table, spec, batch


def rows_in(engine, table):
    with engine.connect() as conn:
        return [dict(row) for row in conn.execute(select(table).order_by(table.c.id)).mappings()]


def states_in(store, spec):
    return sorted(store.states(spec.name), key=lambda item: item["partition_key"])


def seed(engine, store, table, spec, item, rows):
    with engine.begin() as conn:
        conn.execute(table.insert(), [store._row_values(spec, row, item.unit, "old", NOW)[1]
                                      for row in rows])


def refresh_proof(spec, item):
    item.detail["minute_grid_proof"] = proof(spec, item.unit, item.rows)


def watch(engine, table):
    calls = []

    def before(conn, cursor, statement, parameters, context, executemany):
        operation = statement.lstrip().split(None, 1)[0].upper()
        calls.append((operation, table.name in statement, parameters, executemany,
                      context.compiled.statement if context.compiled is not None else None))

    event.listen(engine, "before_cursor_execute", before)
    return calls, before


@pytest.mark.parametrize("dataset,code,allowed,count", [
    ("stock_minute", "000001.SZ", False, 241),
    ("index_minute", "000001.SH", False, 241),
    ("index_minute", "000012.SH", False, 271),
    ("index_minute", "980001.SZ", True, 341),
])
def test_both_minute_products_only_use_bounded_full_identity_bulk_path(
        monkeypatch, dataset, code, allowed, count):
    engine, store, table, spec, batch = fixture(dataset, (code,), allowed=allowed)
    monkeypatch.setattr(store, "_upsert_row", lambda *_: pytest.fail("minute fell back to row writer"))
    calls, listener = watch(engine, table)
    try:
        assert store.commit(spec, batch) == {"complete": 1, "no_data": 0, "error": 0, "replayed": 0}
    finally:
        event.remove(engine, "before_cursor_execute", listener)
    business = [item for item in calls if item[1]]
    selects = [item for item in business if item[0] == "SELECT"]
    inserts = [item for item in business if item[0] == "INSERT"]
    assert len(selects) == len(inserts) == (count + 99) // 100
    assert [len(item[2]) for item in inserts] == [100] * (count // 100) + ([count % 100] if count % 100 else [])
    assert all(item[3] for item in inserts)
    for item in selects:
        statement = item[4]
        sql = str(statement.compile(dialect=mysql.dialect()))
        assert "FOR UPDATE" in sql and spec.code_column in sql and "trade_time" in sql
        assert " OR " in sql and statement._limit_clause.value <= 101
        assert "ON DUPLICATE" not in sql
    assert len(rows_in(engine, table)) == count
    state = states_in(store, spec)[0]
    assert state["status"] == "complete" and state["written_rows"] == count
    assert state["request_id"] == REQUEST and state["last_success_at"] == NOW


def test_19_complete_and_one_error_use_154_cursor_calls_not_9198():
    engine, store, table, spec, batch = fixture(
        codes=tuple(f"{number:06}.SZ" for number in range(1, 20)), error=True)
    calls, listener = watch(engine, table)
    try:
        counts = store.commit(spec, batch)
    finally:
        event.remove(engine, "before_cursor_execute", listener)
    assert counts == {"complete": 19, "no_data": 0, "error": 1, "replayed": 0}
    assert len(rows_in(engine, table)) == 4579
    assert len(calls) == 154  # 20 partition SELECT + 20 UPDATE + 57 key SELECT + 57 INSERT.
    assert sum(item[0] == "SELECT" for item in calls) == 77
    assert sum(item[0] == "INSERT" for item in calls) == 57
    assert sum(item[0] == "UPDATE" for item in calls) == 20
    assert sum(item[1] for item in calls) == 114
    calls, listener = watch(engine, table)
    try:
        assert store.commit(spec, batch) == {"complete": 19, "no_data": 0, "error": 1, "replayed": 19}
    finally:
        event.remove(engine, "before_cursor_execute", listener)
    assert len(calls) == 21 and not any(item[1] for item in calls)


@pytest.mark.parametrize("dataset", ["stock_minute", "index_minute"])
def test_update_preserves_ids_full_keys_known_required_metadata_and_optional_omissions(dataset):
    engine, store, table, spec, batch = fixture(dataset, required=True)
    item = batch.units[0]
    old = [{**row, "price": Decimal("9"), "note": "old-note"} for row in item.rows]
    seed(engine, store, table, spec, item, old)
    before = rows_in(engine, table)
    for index, row in enumerate(item.rows):
        row["id"] = before[index]["id"] + 100000  # Existing identity/id must never be SET.
        if index % 2:
            row.pop("vendor_required")
        else:
            row["vendor_required"] = None
        row["data_source"] = None  # Required known value, not an invented replacement.
        if index % 3:
            row["note"] = None
    refresh_proof(spec, item)
    calls, listener = watch(engine, table)
    try:
        store.commit(spec, batch)
    finally:
        event.remove(engine, "before_cursor_execute", listener)
    after = rows_in(engine, table)
    assert len(after) == len(before)
    for index, (previous, current) in enumerate(zip(before, after)):
        assert current["id"] == previous["id"]
        assert all(current[key] == previous[key] for key in spec.key_columns)
        assert current["vendor_required"] == "known"
        assert current["data_source"] == spec.persisted_source
        assert current["price"] == Decimal("10") and current["batch_id"] == REQUEST
        assert current["note"] == (None if index % 3 else "old-note")
    assert all(len(item[2]) <= 100 for item in calls if item[1] and item[3])
    assert any(item[0] == "UPDATE" and item[1] and item[3] for item in calls)


def test_insert_shapes_do_not_drop_nullable_values_or_invent_missing_fields():
    engine, store, table, spec, batch = fixture()
    item = batch.units[0]
    for index, row in enumerate(item.rows):
        if index % 2:
            row["note"] = "supplied"
    refresh_proof(spec, item)
    store.commit(spec, batch)
    saved = rows_in(engine, table)
    by_time = {row["trade_time"]: row for row in saved}
    for index, row in enumerate(item.rows):
        actual = by_time[row["trade_time"]]
        assert actual["note"] == ("supplied" if index % 2 else None)
        assert actual["source_time"] == row["trade_time"]
        assert actual["qmt_code"] == item.unit.code
        assert actual["received_at"] == actual["etl_sync_at"] == NOW
        assert actual["data_version"] and actual["data_source"] == spec.persisted_source


def test_typed_values_are_reused_without_changing_business_identity():
    engine, store, table, spec, batch = fixture()
    item = batch.units[0]
    seed(engine, store, table, spec, item, [item.rows[0]])
    previous_id = rows_in(engine, table)[0]["id"]
    # A UTC source_time is converted by the existing SQL type rule, independently
    # of the already normalized naive minute business key.
    for row in item.rows:
        row["source_time"] = datetime(2026, 9, 30, 1, 30, tzinfo=timezone.utc)
    refresh_proof(spec, item)
    store.commit(spec, batch)
    saved = rows_in(engine, table)
    assert saved[0]["id"] == previous_id
    assert all(row["source_time"] == datetime(2026, 9, 30, 9, 30) for row in saved)
    assert {row["trade_time"] for row in saved} == {row["trade_time"] for row in item.rows}


@pytest.mark.parametrize("kind", ["within-unit", "across-units", "missing-key", "date-only-source"])
def test_invalid_input_is_rejected_before_any_business_or_partition_dml(kind):
    engine, store, table, spec, batch = fixture()
    if kind == "within-unit":
        batch.units[0].rows.append(dict(batch.units[0].rows[0]))
    elif kind == "across-units":
        batch.units.append(deepcopy(batch.units[0]))
    elif kind == "missing-key":
        batch.units[0].rows[0][spec.code_column] = None
    else:
        batch.units[0].rows[0]["source_time"] = "2026-09-30"
    before = states_in(store, spec)
    calls, listener = watch(engine, table)
    try:
        with pytest.raises(SchemaMismatch):
            store.commit(spec, batch)
    finally:
        event.remove(engine, "before_cursor_execute", listener)
    assert not any(item[0] in {"INSERT", "UPDATE", "DELETE"} for item in calls)
    assert rows_in(engine, table) == [] and states_in(store, spec) == before


@pytest.mark.parametrize("duplicate_index", [0, 99, 100, 240])
def test_db_duplicate_rejects_and_rolls_back_prior_chunks_and_other_units(duplicate_index):
    engine, store, table, spec, batch = fixture(codes=("000001.SZ", "000002.SZ"))
    second = batch.units[1]
    seed(engine, store, table, spec, second,
         [{**row, "price": Decimal("9")} for row in second.rows])
    seed(engine, store, table, spec, second, [second.rows[duplicate_index]])
    before_rows, before_states = rows_in(engine, table), states_in(store, spec)
    with pytest.raises(SchemaMismatch, match="duplicate rows"):
        store.commit(spec, batch)
    assert rows_in(engine, table) == before_rows
    assert states_in(store, spec) == before_states


@pytest.mark.parametrize("failure", ["insert-exception", "update-exception", "insert-rowcount", "update-rowcount"])
def test_late_bulk_dml_failure_rolls_back_every_business_row_and_partition(monkeypatch, failure):
    engine, store, table, spec, batch = fixture(codes=("000001.SZ", "000002.SZ"))
    if failure.startswith("update"):
        for item in batch.units:
            seed(engine, store, table, spec, item, [{**row, "price": Decimal("9")} for row in item.rows])
    before_rows, before_states = rows_in(engine, table), states_in(store, spec)
    original = Connection.execute
    seen = 0

    def execute(conn, statement, *args, **kwargs):
        nonlocal seen
        wanted = statement.is_insert if failure.startswith("insert") else statement.is_update
        if wanted and getattr(statement, "table", None) is not None and statement.table.name == table.name:
            seen += 1
            if seen == 5:  # Previous unit and the first chunk of this unit already wrote.
                if failure.endswith("exception"):
                    raise RuntimeError("INJECTED_BULK_FAILURE")
                original(conn, statement, *args, **kwargs)
                return SimpleNamespace(rowcount=0)
        return original(conn, statement, *args, **kwargs)

    monkeypatch.setattr(Connection, "execute", execute)
    with pytest.raises((RuntimeError, SchemaMismatch)):
        store.commit(spec, batch)
    assert seen == 5
    assert rows_in(engine, table) == before_rows and states_in(store, spec) == before_states


def test_unrelated_code_and_date_are_never_selected_updated_or_deleted():
    engine, store, table, spec, batch = fixture()
    item = batch.units[0]
    untouched = [{**item.rows[0], spec.code_column: "999999", "price": Decimal("7")},
                 {**item.rows[0], "trade_time": datetime(2026, 9, 29, 9, 30),
                  "trade_date": date(2026, 9, 29), "price": Decimal("8")}]
    seed(engine, store, table, spec, item, untouched)
    original = rows_in(engine, table)
    store.commit(spec, batch)
    saved = rows_in(engine, table)
    assert saved[:2] == original and len(saved) == 243


def test_missing_required_metadata_on_new_late_chunk_rolls_back_known_updates():
    engine, store, table, spec, batch = fixture(required=True)
    item = batch.units[0]
    seed(engine, store, table, spec, item, [{**row, "price": Decimal("9")} for row in item.rows[:100]])
    before_rows, before_states = rows_in(engine, table), states_in(store, spec)
    item.rows[-1].pop("vendor_required")
    refresh_proof(spec, item)
    with pytest.raises(SchemaMismatch, match="required column"):
        store.commit(spec, batch)
    assert rows_in(engine, table) == before_rows and states_in(store, spec) == before_states


@pytest.mark.parametrize("kind", ["stale-partition", "bad-proof"])
def test_original_ownership_and_minute_proof_still_rollback_prior_units(kind):
    engine, store, table, spec, batch = fixture(codes=("000001.SZ", "000002.SZ"))
    if kind == "stale-partition":
        with engine.begin() as conn:
            conn.execute(STATE.update().where(STATE.c.partition_key == batch.units[1].unit.partition_key)
                         .values(request_id="other"))
    else:
        batch.units[1].rows[0]["price"] = Decimal("99")
    before = states_in(store, spec)
    with pytest.raises((StaleRequest, ValueError)):
        store.commit(spec, batch)
    assert rows_in(engine, table) == [] and states_in(store, spec) == before


def test_same_request_replay_does_not_invoke_either_business_writer(monkeypatch):
    engine, store, table, spec, batch = fixture()
    store.commit(spec, batch)
    before = rows_in(engine, table)
    monkeypatch.setattr(store, "_upsert_minute_rows", lambda *_: pytest.fail("replay bulk DML"))
    monkeypatch.setattr(store, "_upsert_row", lambda *_: pytest.fail("replay generic DML"))
    assert store.commit(spec, batch)["replayed"] == 1
    assert rows_in(engine, table) == before


def test_update_unchanged_values_counts_matched_rows_not_only_changed_values():
    engine, store, table, spec, batch = fixture()
    store.commit(spec, batch)
    before = rows_in(engine, table)
    with engine.begin() as conn:
        conn.execute(STATE.update().values(status="running"))
    assert store.commit(spec, batch)["complete"] == 1
    assert rows_in(engine, table) == before


def test_other_products_keep_their_independent_generic_lane(monkeypatch):
    engine, store, table, minute, _ = fixture()
    # A minimal separate product exercises the actual commit branch rather than
    # treating a patched minute method as proof that the other lane still works.
    spec = replace(minute, name="independent_test_product", period="1d")
    unit = WorkUnit(spec.name, spec.source, "2026-09-30", "000001.SZ", "1d", "none")
    row = {spec.code_column: "000001", "trade_time": datetime(2026, 9, 30, 9, 30),
           "trade_date": "2026-09-30", "price": Decimal("10")}
    store.begin_request([unit], "generic-request", NOW)
    batch = NormalizedBatch("generic-request", [NormalizedUnit(unit, "complete", [row])], NOW)
    monkeypatch.setattr(store, "_upsert_minute_rows", lambda *_: pytest.fail("non-minute entered bulk lane"))
    assert store.commit(spec, batch)["complete"] == 1
    assert len(rows_in(engine, table)) == 1
