from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import Column, Date, DateTime, Index, Integer, MetaData, Numeric, String, Table, Text, UniqueConstraint, create_engine, select

from acquisition.datasets import get_spec
from acquisition.models import DatasetSpec, NormalizedBatch, NormalizedUnit, WorkUnit
from acquisition.plan import plan_units
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


def test_state_counts_are_grouped_and_source_isolated():
    engine, store = database()
    with engine.begin() as conn:
        conn.execute(STATE.insert(), [
            {"dataset": "stock_daily", "source": "guojin_qmt",
             "target_date": date(2026, 9, 3), "partition_key": "a:1d:none",
             "status": "complete", "updated_at": NOW},
            {"dataset": "stock_daily", "source": "guojin_qmt",
             "target_date": date(2026, 9, 3), "partition_key": "b:1d:none",
             "status": "error", "updated_at": NOW},
            {"dataset": "stock_daily", "source": "eastmoney",
             "target_date": date(2026, 9, 3), "partition_key": "c:1d:none",
             "status": "complete", "updated_at": NOW},
        ])
    counts = store.state_counts("stock_daily", "guojin_qmt")
    assert {(item["status"], item["unit_count"]) for item in counts} == {
        ("complete", 1), ("error", 1),
    }
    assert {item["source"] for item in counts} == {"guojin_qmt"}


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


def test_index_lookup_prefix_never_collapses_another_k_type():
    md = MetaData()
    table = Table("sm_index_kline", md, Column("id", Integer, primary_key=True),
                  Column("index_code", String), Column("trade_date", Date),
                  Column("k_type", Integer), Column("close", Numeric))
    Index("idx_index_day", table.c.index_code, table.c.trade_date)
    engine, store = database(table)
    with engine.begin() as conn:
        conn.execute(table.insert().values(index_code="000001", trade_date=date(2026, 9, 4),
                                           k_type=2, close=20))
    commit(store, get_spec("index_daily"), [dict(index_code="000001", trade_date="2026-09-04",
                                                   k_type=1, close=10)], code="000001.SH")
    with engine.connect() as conn:
        rows = conn.execute(select(table.c.k_type, table.c.close).order_by(table.c.k_type)).all()
    assert rows == [(1, Decimal("10.0000000000")), (2, Decimal("20.0000000000"))]


def test_alist_detail_replaces_complete_stock_date_without_losing_special_seats():
    md = MetaData()
    table = Table("st_a_list_info", md, Column("id", Integer, primary_key=True),
                  Column("stock_code", String), Column("trade_date", Date),
                  Column("trade_id", String), Column("operate_code", String),
                  Column("operate_name", String), Column("report_side", String),
                  Column("a_buy_amount", Numeric), Column("etl_sync_at", DateTime))
    Index("idx_alist_partition", table.c.stock_code, table.c.trade_date)
    engine, store = database(table)
    with engine.begin() as conn:
        conn.execute(table.insert().values(stock_code="000001", trade_date=date(2026, 9, 4),
                                           trade_id="old", operate_code="0",
                                           operate_name="机构专用", report_side="BUY",
                                           a_buy_amount=1, etl_sync_at=NOW))
    rows = [dict(stock_code="000001", trade_date="2026-09-04", trade_id="106",
                 operate_code="0", operate_name="机构专用", report_side="BUY",
                 a_buy_amount=Decimal(value)) for value in ("20.12", "30.12")]
    commit(store, get_spec("alist_detail"), rows)
    with engine.connect() as conn:
        saved = conn.execute(select(table.c.trade_id, table.c.a_buy_amount)
                             .order_by(table.c.a_buy_amount)).all()
    assert saved == [("106", Decimal("20.1200000000")),
                     ("106", Decimal("30.1200000000"))]


def test_authoritative_empty_event_partition_removes_stale_rows():
    md = MetaData()
    table = Table("st_a_list_daily", md, Column("id", Integer, primary_key=True),
                  Column("stock_code", String), Column("trade_date", Date),
                  Column("trade_id", String))
    Index("idx_alist_daily_partition", table.c.stock_code, table.c.trade_date)
    engine, store = database(table)
    with engine.begin() as conn:
        conn.execute(table.insert().values(stock_code="000001", trade_date=date(2026, 9, 4),
                                           trade_id="stale"))
    spec = get_spec("alist_daily")
    unit = WorkUnit(spec.name, spec.source, "2026-09-04", "000001.SZ", spec.period, "none")
    store.begin_request([unit], "empty-refresh", NOW)
    batch = NormalizedBatch("empty-refresh", [NormalizedUnit(unit, "no_data", [])], NOW)
    assert store.commit(spec, batch)["no_data"] == 1
    with engine.connect() as conn:
        assert conn.execute(select(table)).all() == []


def test_notice_date_correction_updates_stable_article_identity():
    md = MetaData()
    table = Table("si_notice_eastmoney", md, Column("id", Integer, primary_key=True),
                  Column("stock_code", String), Column("art_code", String),
                  Column("notice_date", Date), Column("title", String),
                  Column("etl_sync_at", DateTime),
                  UniqueConstraint("stock_code", "art_code"))
    Index("idx_notice_stock_date", table.c.stock_code, table.c.notice_date)
    engine, store = database(table)
    with engine.begin() as conn:
        conn.execute(table.insert().values(stock_code="000001", art_code="AN1",
                                           notice_date=date(2026, 9, 3), title="old",
                                           etl_sync_at=NOW))
    row = dict(stock_code="000001", art_code="AN1", notice_date="2026-09-04", title="corrected")
    commit(store, get_spec("notices"), [row])
    with engine.connect() as conn:
        saved = conn.execute(select(table)).mappings().one()
    assert saved["notice_date"] == date(2026, 9, 4) and saved["title"] == "corrected"


def test_alist_detail_rejects_unique_constraint_that_would_drop_special_seats():
    md = MetaData()
    table = Table("st_a_list_info", md, Column("id", Integer, primary_key=True),
                  Column("stock_code", String), Column("trade_date", Date),
                  Column("trade_id", String), Column("operate_code", String),
                  Column("operate_name", String), Column("report_side", String),
                  UniqueConstraint("stock_code", "trade_date", "trade_id", "operate_code",
                                   "operate_name", "report_side"))
    _, store = database(table)
    with pytest.raises(SchemaMismatch, match="non-unique date partition"):
        store.validate_spec(get_spec("alist_detail"))


def test_etf_market_write_clears_inherited_validation_and_permission_conclusions():
    md = MetaData()
    table = Table("sm_etf_kline", md, Column("id", Integer, primary_key=True),
                  Column("etf_code", String), Column("trade_date", Date), Column("k_type", Integer),
                  Column("adjust_type", Integer), Column("close", Numeric), Column("data_version", String),
                  Column("validation_source", String), Column("validation_status", String),
                  Column("validation_price_max_delta", Numeric), Column("validation_volume_delta_pct", Numeric),
                  Column("validation_checked_at", DateTime), Column("quality_status", String),
                  Column("permission_status", String, server_default="public"),
                  UniqueConstraint("etf_code", "trade_date", "k_type", "adjust_type"))
    engine, store = database(table)
    old = dict(etf_code="510300", trade_date=date(2026, 9, 4), k_type=1, adjust_type=0,
               close=1, data_version="old", validation_source="legacy", validation_status="passed",
               validation_price_max_delta=0, validation_volume_delta_pct=0,
               validation_checked_at=NOW, quality_status="validated", permission_status="SUPPORTED")
    with engine.begin() as conn:
        conn.execute(table.insert().values(**old))
    row = dict(etf_code="510300", trade_date="2026-09-04", k_type=1, adjust_type=0,
               close=Decimal("2"), data_version="new")
    commit(store, get_spec("etf_daily"), [row], code="510300.SH")
    with engine.connect() as conn:
        saved = conn.execute(select(table)).mappings().one()
    assert saved["close"] == Decimal("2.0000000000") and saved["data_version"] == "new"
    assert all(saved[name] is None for name in (
        "validation_source", "validation_status", "validation_price_max_delta",
        "validation_volume_delta_pct", "validation_checked_at", "quality_status", "permission_status"))


def test_etf_write_rolls_back_if_legacy_validation_column_is_still_required():
    md = MetaData()
    table = Table("sm_etf_kline", md, Column("id", Integer, primary_key=True),
                  Column("etf_code", String), Column("trade_date", Date), Column("k_type", Integer),
                  Column("adjust_type", Integer), Column("close", Numeric),
                  Column("validation_status", String, nullable=False),
                  UniqueConstraint("etf_code", "trade_date", "k_type", "adjust_type"))
    engine, store = database(table)
    old = dict(etf_code="510300", trade_date=date(2026, 9, 4), k_type=1,
               adjust_type=0, close=1, validation_status="passed")
    with engine.begin() as conn:
        conn.execute(table.insert().values(**old))
    with pytest.raises(SchemaMismatch, match="must be nullable"):
        commit(store, get_spec("etf_daily"), [dict(
            etf_code="510300", trade_date="2026-09-04", k_type=1,
            adjust_type=0, close=Decimal("2"),
        )], code="510300.SH")
    with engine.connect() as conn:
        saved = conn.execute(select(table)).mappings().one()
    assert saved["close"] == Decimal("1.0000000000")
    assert saved["validation_status"] == "passed"
    assert store.states("etf_daily")[0]["status"] == "running"


def test_daily_flow_stages_batches_then_replaces_whole_date_atomically():
    md = MetaData()
    table = Table("sm_stock_capital_flow_daily", md,
                  Column("stock_code", String, primary_key=True),
                  Column("trade_date", Date, primary_key=True),
                  Column("data_source", String, nullable=False),
                  *(Column(name, Numeric(24, 6), nullable=False) for name in (
                      "main_net_inflow", "sm_net_inflow", "mid_net_inflow",
                      "lg_net_inflow", "max_net_inflow")))
    engine, store = database(table)
    with engine.begin() as conn:
        conn.execute(table.insert().values(
            stock_code="old", trade_date=date(2026, 9, 4), data_source="push2hist",
            main_net_inflow=1, sm_net_inflow=1, mid_net_inflow=1,
            lg_net_inflow=1, max_net_inflow=1))
    spec = get_spec("capital_flow_daily")
    def row(code, value):
        return dict(stock_code=code, trade_date="2026-09-04",
                    data_source="gj_big_qmt_inner", main_net_inflow=Decimal(value),
                    sm_net_inflow=1, mid_net_inflow=2, lg_net_inflow=3,
                    max_net_inflow=Decimal(value) - 3)
    commit(store, spec, [row("000001", "10")], request="flow-1", code="000001.SZ")
    pending = store.publish_capital_flow_day(
        spec, "2026-09-04", {"000001.SZ", "600000.SH"}, NOW)
    assert pending == {"published": False, "missing": 1}
    with engine.connect() as conn:
        assert conn.execute(select(table.c.stock_code)).scalars().all() == ["old"]
    commit(store, spec, [row("600000", "20")], request="flow-2", code="600000.SH")
    with engine.begin() as conn:
        conn.execute(STATE.insert().values(
            dataset=spec.name, source=spec.source,
            target_date=date(2026, 9, 4), partition_key="999999.SH:1d:none",
            status="complete", written_rows=1, updated_at=NOW))
    published = store.publish_capital_flow_day(
        spec, "2026-09-04", {"000001.SZ", "600000.SH"}, NOW)
    assert published["published"] is True and published["written_rows"] == 2
    with engine.connect() as conn:
        saved = conn.execute(select(table.c.stock_code, table.c.data_source)
                             .order_by(table.c.stock_code)).all()
    assert saved == [("000001", "gj_big_qmt_inner"), ("600000", "gj_big_qmt_inner")]
    assert {item["status"] for item in store.states("capital_flow_daily")} == {"complete"}


def test_daily_flow_expanded_universe_retries_whole_date_without_deleting_partition():
    md = MetaData()
    table = Table("sm_stock_capital_flow_daily", md,
                  Column("stock_code", String, primary_key=True),
                  Column("trade_date", Date, primary_key=True),
                  Column("data_source", String, nullable=False),
                  *(Column(name, Numeric(24, 6), nullable=False) for name in (
                      "main_net_inflow", "sm_net_inflow", "mid_net_inflow",
                      "lg_net_inflow", "max_net_inflow")))
    engine, store = database(table)
    spec = get_spec("capital_flow_daily")

    def row(code, value):
        return dict(stock_code=code, trade_date="2026-09-04",
                    data_source="gj_big_qmt_inner", main_net_inflow=Decimal(value),
                    sm_net_inflow=1, mid_net_inflow=2, lg_net_inflow=3,
                    max_net_inflow=Decimal(value) - 3)

    commit(store, spec, [row("000001", "10")], request="initial", code="000001.SZ")
    store.publish_capital_flow_day(spec, "2026-09-04", {"000001.SZ"}, NOW)
    commit(store, spec, [row("600000", "20")], request="expanded", code="600000.SH")
    pending = store.publish_capital_flow_day(
        spec, "2026-09-04", {"000001.SZ", "600000.SH"}, NOW)
    assert pending == {"published": False, "missing": 1}
    with engine.connect() as conn:
        saved = dict(conn.execute(select(table.c.stock_code, table.c.main_net_inflow)).all())
    assert saved == {"000001": Decimal("10.000000")}
    states = store.states("capital_flow_daily")
    assert {state["status"] for state in states} == {"error"}
    assert {state["last_error_code"] for state in states} == {
        "PARTITION_RESTAGE_REQUIRED"
    }
    assert {unit.code for unit in plan_units(
        spec, "2026-09-04", {
            "000001.SZ": {}, "600000.SH": {},
        }, states, now=NOW,
    )} == {"000001.SZ", "600000.SH"}


def test_daily_flow_detects_published_partition_drift_and_makes_units_retryable():
    md = MetaData()
    table = Table("sm_stock_capital_flow_daily", md,
                  Column("stock_code", String, primary_key=True),
                  Column("trade_date", Date, primary_key=True),
                  Column("data_source", String, nullable=False),
                  *(Column(name, Numeric(24, 6), nullable=False) for name in (
                      "main_net_inflow", "sm_net_inflow", "mid_net_inflow",
                      "lg_net_inflow", "max_net_inflow")))
    engine, store = database(table)
    spec = get_spec("capital_flow_daily")
    row = dict(stock_code="000001", trade_date="2026-09-04",
               data_source="gj_big_qmt_inner", main_net_inflow=10,
               sm_net_inflow=1, mid_net_inflow=2, lg_net_inflow=3,
               max_net_inflow=7)
    commit(store, spec, [row], request="initial", code="000001.SZ")
    store.publish_capital_flow_day(spec, "2026-09-04", {"000001.SZ"}, NOW)
    with engine.begin() as conn:
        conn.execute(table.update().values(data_source="push2hist"))
    outcome = store.publish_capital_flow_day(
        spec, "2026-09-04", {"000001.SZ"}, NOW)
    assert outcome == {"published": False, "missing": 1}
    state = store.states("capital_flow_daily")[0]
    assert state["status"] == "error"
    assert state["last_error_code"] == "PUBLISHED_PARTITION_DRIFT"
    assert [unit.code for unit in plan_units(
        spec, "2026-09-04", {"000001.SZ": {}}, [state], now=NOW)] == ["000001.SZ"]


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
