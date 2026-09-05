from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import Column, Date, DateTime, Index, Integer, MetaData, Numeric, String, Table, Text, UniqueConstraint, create_engine, select

from acquisition.datasets import get_spec
from acquisition.models import DatasetSpec, NormalizedBatch, NormalizedUnit, WorkUnit
from acquisition.reference import normalize_reference
from acquisition.store import STATE, SchemaMismatch, Store, safe_error


NOW = datetime(2026, 9, 5, 19)


def database(*tables):
    engine = create_engine("sqlite:///:memory:")
    for table in tables:
        table.metadata.create_all(engine)
    store = Store(engine)
    store.prepare_progress_schema()
    return engine, store


def commit(store, spec, rows, request="r1", code="000001.SZ", detail=None):
    unit = WorkUnit(spec.name, spec.source, "2026-09-04", code, spec.period, "none")
    store.begin_request([unit], request, NOW)
    batch = NormalizedBatch(request, [NormalizedUnit(unit, "complete", rows, detail=detail or {})], NOW)
    result = store.commit(spec, batch)
    return result, batch


def finance_tables():
    md = MetaData()
    cache = Table("si_stock_finance", md, Column("stock_code", String, primary_key=True),
                  Column("report_date", Date, primary_key=True), Column("report_type", String),
                  Column("basic_eps", Numeric(18, 6)), Column("source_update_date", Text))
    revisions = Table("st_pit_finance_revision", md,
        Column("revision_id", String, primary_key=True), Column("identity_hash", String),
        Column("stock_code", String), Column("report_date", Date), Column("report_type", String),
        Column("source", String), Column("published_at", DateTime), Column("source_published_text", String),
        Column("publication_time_status", String), Column("known_at", DateTime), Column("received_at", DateTime),
        Column("revision_no", Integer), Column("supersedes_revision_id", String), Column("batch_id", String),
        Column("content_hash", String), Column("revision_fingerprint_hash", String), Column("payload_json", Text),
        Column("created_at", DateTime), UniqueConstraint("identity_hash", "revision_no"),
        UniqueConstraint("identity_hash", "revision_fingerprint_hash"))
    return cache, revisions


def finance_row(value="0.30", updated="2026-09-04 08:00:00"):
    return dict(stock_code="000001", report_date="2026-06-30", report_type="half_year", basic_eps=Decimal(value), source_update_date=updated, notice_date="2026-08-30")


def test_finance_old_fingerprint_replay_preserves_latest_cache_and_revision_count():
    cache, revisions = finance_tables()
    engine, store = database(cache)
    spec = get_spec("finance")
    first = finance_row()
    commit(store, spec, [first], "first")
    commit(store, spec, [finance_row("0.40", "2026-09-04 09:00:00")], "second")
    _, replay = commit(store, spec, [first], "historical-replay")
    assert replay.units[0].detail["cache_not_promoted"] is True
    with engine.connect() as conn:
        assert conn.execute(select(cache.c.basic_eps)).scalar_one() == Decimal("0.4")
        assert len(conn.execute(select(revisions)).all()) == 2


def test_finance_late_unseen_revision_is_kept_without_cache_rollback():
    cache, revisions = finance_tables()
    engine, store = database(cache)
    spec = get_spec("finance")
    commit(store, spec, [finance_row("0.4", "2026-09-04 09:00:00")], "current")
    commit(store, spec, [finance_row("0.2", "2026-09-03 09:00:00")], "older")
    commit(store, spec, [finance_row("0.1", None)], "unverified")
    commit(store, spec, [finance_row("0.5", "2026-09-04 10:00:00")], "newer")
    with engine.connect() as conn:
        assert conn.execute(select(cache.c.basic_eps)).scalar_one() == Decimal("0.5")
        assert len(conn.execute(select(revisions)).all()) == 4


def test_legacy_finance_cache_gets_explicit_first_source_baseline_then_version_guard():
    cache, revisions = finance_tables()
    engine, store = database(cache)
    with engine.begin() as conn:
        conn.execute(cache.insert().values(stock_code="000001", report_date=date(2026, 6, 30), basic_eps=Decimal("0.6")))
    _, batch = commit(store, get_spec("finance"), [finance_row()], "unknown-baseline")
    assert batch.units[0].detail["cache_baseline_established"] is True
    assert batch.units[0].detail["prior_cache_version_unverified"] is True
    commit(store, get_spec("finance"), [finance_row("0.2", "2026-09-04 07:00:00")], "older-after-baseline")
    with engine.connect() as conn:
        assert conn.execute(select(cache.c.basic_eps)).scalar_one() == Decimal("0.3")
        assert len(conn.execute(select(revisions)).all()) == 2


def test_finance_date_only_cannot_order_same_day_updates():
    assert Store._newer_source_update("2026-09-05", "2026-09-04 23:59:59") is True
    assert Store._newer_source_update("2026-09-04 09:00:00", "2026-09-04") is False
    assert Store._newer_source_update(None, "2026-09-04") is False


def test_finance_revision_and_progress_roll_back_when_business_insert_fails():
    cache, revisions = finance_tables()
    cache.append_column(Column("required_business_metadata", String, nullable=False))
    engine, store = database(cache)
    with pytest.raises(SchemaMismatch):
        commit(store, get_spec("finance"), [finance_row()])
    with engine.connect() as conn:
        assert conn.execute(select(cache)).all() == []
        assert conn.execute(select(revisions)).all() == []
    assert store.states("finance")[0]["status"] == "running"


def test_typed_dates_and_datetimes_roundtrip_on_strict_sqlite():
    md = MetaData()
    table = Table("sample_rows", md, Column("code", String, primary_key=True),
                  Column("trade_date", Date), Column("trade_time", DateTime), Column("value", Integer))
    engine, store = database(table)
    spec = DatasetSpec("sample", "guojin_qmt", "sample_rows", "primary", "code", ("code",), "1d", ("none",), "stock", NOW.time())
    _, batch = commit(store, spec, [dict(code="000001", trade_date="2026-09-04", trade_time="2026-09-04T07:00:00Z", value=1)])
    assert store.commit(spec, batch)["replayed"] == 1
    with engine.connect() as conn:
        row = conn.execute(select(table)).mappings().one()
        assert row["trade_date"] == date(2026, 9, 4)
        assert row["trade_time"] == datetime(2026, 9, 4, 15)


def test_update_preserves_required_etf_metadata_but_new_insert_needs_it():
    md = MetaData()
    table = Table("si_etf_code", md, Column("etf_code", String, primary_key=True),
                  Column("short_name", String, nullable=False), Column("asset_class", String, nullable=False))
    engine, store = database(table)
    with engine.begin() as conn:
        conn.execute(table.insert().values(etf_code="510300", short_name="old", asset_class="equity"))
    spec = DatasetSpec("catalog_test", "guojin_qmt", "si_etf_code", "primary", "etf_code", ("etf_code",), "instrument", ("none",), "etf", NOW.time())
    commit(store, spec, [dict(etf_code="510300", short_name="new", asset_class=None)], code="510300.SH")
    with engine.connect() as conn:
        assert conn.execute(select(table.c.asset_class)).scalar_one() == "equity"
    with pytest.raises(SchemaMismatch):
        commit(store, spec, [dict(etf_code="510500", short_name="new")], request="new", code="510500.SH")


def test_validation_cache_is_bound_to_table_and_key_not_reference_name():
    md = MetaData()
    good = Table("good_codes", md, Column("code", String, primary_key=True))
    Table("bad_codes", md, Column("id", Integer, primary_key=True), Column("code", String))
    _, store = database(good)
    spec = DatasetSpec("reference", "guojin_qmt", "good_codes", "primary", "code", ("code",), "instrument", ("none",), "stock", NOW.time())
    store.validate_spec(spec)
    with pytest.raises(SchemaMismatch):
        store.validate_spec(replace(spec, table="bad_codes"))


def test_legacy_nonunique_index_is_reused_but_touched_duplicate_is_rejected():
    md = MetaData()
    table = Table("legacy_rows", md, Column("id", Integer, primary_key=True),
                  Column("code", String), Column("value", Integer))
    Index("idx_legacy_code", table.c.code)
    engine, store = database(table)
    with engine.begin() as conn:
        conn.execute(table.insert(), [{"code": "000001", "value": 1}, {"code": "000001", "value": 2}])
    spec = DatasetSpec("legacy", "guojin_qmt", "legacy_rows", "primary", "code", ("code",),
                       "1d", ("none",), "stock", NOW.time())
    with pytest.raises(SchemaMismatch, match="duplicate rows"):
        commit(store, spec, [{"code": "000001", "value": 3}])


def reference_tables():
    md = MetaData()
    index = Table("si_all_index_code", md, Column("index_code", String, primary_key=True), Column("name", String))
    details = Table("qmt_instrument_detail", md, Column("qmt_code", String, primary_key=True),
                    Column("stock_code", String), Column("short_name", String), Column("exchange", String),
                    Column("list_date", Date), Column("expire_date", Date), Column("permission_status", String), Column("instrument_type", String))
    return index, details


def test_index_uses_exact_instrument_mapping_and_rejects_ambiguity():
    index, details = reference_tables()
    engine, store = database(index)
    with engine.begin() as conn:
        conn.execute(index.insert().values(index_code="980001", name="index"))
        conn.execute(details.insert().values(qmt_code="980001.SZ", stock_code="980001", permission_status="SOURCE_READABLE", instrument_type="INDEX"))
    assert set(store.catalog("index")) == {"980001.SZ"}
    with engine.begin() as conn:
        conn.execute(details.insert().values(qmt_code="980001.SH", stock_code="980001", permission_status="SUPPORTED", instrument_type="INDEX"))
    with pytest.raises(SchemaMismatch, match="ambiguous"):
        store.catalog("index")


def test_reference_commits_mapping_and_business_directory_in_same_transaction():
    index, details = reference_tables()
    engine, store = database(index)
    raw = {"request": dict(request_id="reference-1", dataset="reference", source="guojin_qmt", period="instrument", adjustment="none", codes=["980001.SZ"], start_date="2026-09-04", end_date="2026-09-04"),
           "source_method": "ContextInfo.get_instrument_detail", "received_at": "2026-09-05T19:00:00+08:00",
           "outcomes": {"980001.SZ": {"status": "data", "rows": [dict(InstrumentID="980001", ExchangeID="SZ", InstrumentName="native index", OpenDate="20180101", ExpireDate=0)]}}}
    spec, batch = normalize_reference(raw, "index")
    store.begin_request([batch.units[0].unit], batch.request_id, NOW)
    store.commit(spec, batch)
    assert set(store.catalog("index")) == {"980001.SZ"}
    with engine.connect() as conn:
        assert conn.execute(select(details.c.list_date)).scalar_one() == date(2018, 1, 1)
    assert store.commit(spec, batch)["replayed"] == 1


def test_stable_error_codes_survive_without_leaking_connection_or_sql_text():
    assert safe_error(RuntimeError("QMT_MODEL_UNAVAILABLE")) == "QMT_MODEL_UNAVAILABLE"
    assert safe_error(RuntimeError("connection failed mysql://user:password@host/db")) == "RuntimeError"
    custom = ValueError("private provider response")
    custom.code = "PROVIDER_OFFLINE"
    assert safe_error(custom) == "PROVIDER_OFFLINE"
    custom.code = "secret-token-123"
    assert safe_error(custom) == "ValueError"


def test_index_catalog_uses_asset_metadata_for_stock_digit_collision_and_excludes_statistics():
    index, details = reference_tables()
    engine, store = database(index)
    with engine.begin() as conn:
        conn.execute(index.insert(), [dict(index_code="000001", name="index"), dict(index_code="395001", name="statistics")])
        conn.execute(details.insert(), [dict(qmt_code="000001.SH", stock_code="000001", instrument_type="INDEX", permission_status="SOURCE_READABLE"),
                                        dict(qmt_code="000001.SZ", stock_code="000001", instrument_type="STOCK", permission_status="SUPPORTED")])
    assert set(store.catalog("index")) == {"000001.SH"}


def test_retrying_sources_only_returns_unexpired_source_errors():
    engine, store = database()
    cases = [("SOURCE_UNAVAILABLE", "error", 60),
             ("SOURCE_ACCESS_DENIED", "error", 60),
             ("INVALID_RETRY_AFTER", "error", 60),
             ("NATIVE_CALL_FAILED", "error", 60),
             ("SOURCE_UNAVAILABLE", "error", 0),
             ("SOURCE_UNAVAILABLE", "error", -60),
             ("SOURCE_UNAVAILABLE", "complete", 60),
             ("INCOMPLETE_MINUTES", "error", 60)]
    with engine.begin() as conn:
        for number, (code, status, seconds) in enumerate(cases):
            conn.execute(STATE.insert().values(dataset=f"dataset_{number}", source="guojin_qmt",
                target_date=NOW.date(), partition_key=f"{number}:1d:none", status=status,
                last_error_code=code, next_retry_at=NOW + timedelta(seconds=seconds), updated_at=NOW))
    # UTC and Shanghai represent the same instant; comparisons use DB local time.
    result = store.retrying_sources(NOW.replace(tzinfo=timezone(timedelta(hours=8))).astimezone(timezone.utc))
    assert {row["dataset"] for row in result} == {f"dataset_{n}" for n in range(4)}
    assert all(set(row) == {"dataset", "source", "next_retry_at"} for row in result)
