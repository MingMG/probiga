"""Offline .DAT import: partition proof, replacement and idempotent replay.

These tests use temporary SQLite databases and synthetic native files; they
never touch the production tables or start QMT.
"""
import json
import struct
from datetime import date, datetime

import pytest
from sqlalchemy import (Column, Date, DateTime, Index, Integer, MetaData, Numeric,
                        String, Table, Text, UniqueConstraint, create_engine, select,
                        func)
from zoneinfo import ZoneInfo

import pandas as pd

from acquisition import local_dat_import
from acquisition.datasets import get_spec
from acquisition.minute_grid import grids, proof as minute_grid_proof
from acquisition.models import NormalizedBatch, NormalizedUnit, WorkUnit
from acquisition.sources.local_dat import HEADER, bars_to_rows, core_grid, read_bars
from acquisition.store import STATE, Store

SHANGHAI = ZoneInfo("Asia/Shanghai")
DAY = date(2026, 7, 1)


def _ts(year, month, day, hour, minute=0):
    return int(pd.Timestamp(year=year, month=month, day=day, hour=hour,
                            minute=minute, tz=SHANGHAI).timestamp())


def _record(ts, open_, high, low, close, volume_lots, amount, pre_close):
    body = struct.pack("<5I", ts, open_, high, low, close)
    body += struct.pack("<I", 0)
    body += struct.pack("<I", volume_lots)
    body += struct.pack("<I", 678)
    body += struct.pack("<Q", amount)
    body += struct.pack("<I", 13)
    body += struct.pack("<I", 0x3F800000)
    body += struct.pack("<I", 0x3F800000)
    body += struct.pack("<I", pre_close)
    body += struct.pack("<II", 0, 32761)
    assert len(body) == 64
    return body


def _write(datadir, market, period, code, records):
    folder = datadir / market / period
    folder.mkdir(parents=True, exist_ok=True)
    (folder / f"{code}.DAT").write_bytes(HEADER + b"".join(records))


def _minute_session(day=DAY, *, bars=241, volume_lots=7036, close=10050):
    grid = core_grid()[:bars]
    records = []
    for value in grid:
        hour, minute, _ = (int(part) for part in value.split(":"))
        records.append(_record(_ts(day.year, day.month, day.day, hour, minute),
                               close, close, close, close, volume_lots,
                               7_071_180, close))
    return records


def _schema(engine):
    metadata = MetaData()
    Table(
        "sm_stock_minute", metadata,
        Column("id", Integer, primary_key=True),
        Column("stock_code", String(16), nullable=False),
        Column("trade_time", DateTime),
        Column("trade_date", Date),
        Column("price", Numeric(50, 6)), Column("avg_price", Numeric(50, 6)),
        Column("change", Numeric(50, 6)), Column("change_pct", Numeric(50, 6)),
        Column("volume", Numeric(50, 6)), Column("amount", Numeric(50, 6)),
        Column("etl_sync_at", DateTime, nullable=False),
        Column("qmt_code", String(32)), Column("data_source", String(32)),
        Column("source_time", DateTime), Column("received_at", DateTime),
        Column("batch_id", String(64)), Column("data_version", String(64)),
        Column("quality_status", String(32)), Column("permission_status", String(32)),
        Index("idx_sm_code_time", "stock_code", "trade_time"),
    )
    Table(
        "sm_stock_kline", metadata,
        Column("id", Integer, primary_key=True),
        Column("stock_code", String(16), nullable=False),
        Column("short_name", String(128), nullable=False, server_default=""),
        Column("trade_time", DateTime), Column("trade_date", Date),
        Column("k_type", Integer, nullable=False),
        Column("adjust_type", Integer, nullable=False),
        Column("open", Numeric(50, 6)), Column("close", Numeric(50, 6)),
        Column("high", Numeric(50, 6)), Column("low", Numeric(50, 6)),
        Column("volume", Numeric(50, 6)), Column("amount", Numeric(50, 6)),
        Column("change", Numeric(50, 6)), Column("change_pct", Numeric(50, 6)),
        Column("pre_close", Numeric(50, 6)),
        Column("etl_sync_at", DateTime, nullable=False),
        Column("qmt_code", String(32)), Column("data_source", String(32)),
        Column("source_time", DateTime), Column("received_at", DateTime),
        Column("batch_id", String(64)), Column("data_version", String(64)),
        UniqueConstraint("stock_code", "trade_date", "k_type", "adjust_type",
                         name="uk_sm_stock_kline_business"),
        Index("idx_sk_code_date", "stock_code", "trade_date"),
    )
    Table(
        "si_all_code", metadata,
        Column("stock_code", String(16), primary_key=True),
        Column("qmt_code", String(32)),
    )
    metadata.create_all(engine)
    STATE.create(engine)
    catalog = Table("si_all_code", MetaData(), autoload_with=engine)
    with engine.begin() as conn:
        conn.execute(catalog.delete())
        conn.execute(catalog.insert(), [
            {"stock_code": "000001", "qmt_code": "000001.SZ"},
            {"stock_code": "600000", "qmt_code": "600000.SH"},
        ])


def environment(tmp_path, monkeypatch):
    url = f"sqlite:///{(tmp_path / 'history.db').as_posix()}"
    monkeypatch.setenv("TEST_LOCAL_IMPORT_HISTORY", url)
    config = tmp_path / "config.json"
    config.write_text(json.dumps({
        "write_enabled": True, "state_dir": str(tmp_path.resolve()),
        "start_date": "2026-07-01", "datasets": ["stock_daily_local"],
        "database_env": {"history": "TEST_LOCAL_IMPORT_HISTORY"},
    }), encoding="utf-8")
    engine = create_engine(url)
    _schema(engine)
    return config, engine


def invoke(tmp_path, monkeypatch, datadir, *extra, dataset="stock_minute_local",
           start="2026-07-01", end="2026-07-01"):
    config, engine = environment(tmp_path, monkeypatch)
    report = tmp_path / "report.json"
    code = local_dat_import.main([
        "--config", str(config), "--dataset", dataset, "--datadir", str(datadir),
        "--start", start, "--end", end, "--report", str(report), *extra,
    ])
    return code, engine, json.loads(report.read_text(encoding="utf-8"))


def test_minute_session_writes_a_proven_complete_partition(tmp_path, monkeypatch):
    datadir = tmp_path / "datadir"
    _write(datadir, "SZ", "60", "000001", _minute_session())
    _write(datadir, "SH", "60", "600000", _minute_session())

    code, engine, report = invoke(tmp_path, monkeypatch, datadir, "--execute")
    assert code == 0
    assert report["status"] == "complete"
    assert report["partitions"]["imported"] == 2
    assert report["rows_written"] == 482

    table = Table("sm_stock_minute", MetaData(), autoload_with=engine)
    with engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(table)).scalar() == 482
        row = conn.execute(select(table).where(
            table.c.stock_code == "000001",
            table.c.trade_time == pd.Timestamp("2026-07-01 09:30:00").to_pydatetime(),
        )).mappings().one()
        assert int(row["volume"]) == 7036            # minute volume stays in 手
        assert float(row["price"]) == pytest.approx(10.05)
        assert float(row["avg_price"]) == pytest.approx(10.05)
        assert row["data_source"] == "gj_big_qmt_inner"
        assert row["qmt_code"] == "000001.SZ"
        states = conn.execute(select(STATE)).mappings().all()
        assert len(states) == 2
        assert {item["status"] for item in states} == {"complete"}
        assert {item["written_rows"] for item in states} == {241}
        proof = json.loads(states[0]["detail_json"])["minute_grid_proof"]
        assert proof["dataset"] == "stock_minute_local"
        assert proof["row_count"] == 241


def test_incomplete_source_session_is_never_written(tmp_path, monkeypatch):
    datadir = tmp_path / "datadir"
    _write(datadir, "SZ", "60", "000001", _minute_session(bars=240))

    code, engine, report = invoke(tmp_path, monkeypatch, datadir, "--execute")
    assert code == 0
    assert report["status"] == "partial"
    assert report["partitions"]["source_partial"] == 1
    assert report["partitions"]["imported"] == 0
    table = Table("sm_stock_minute", MetaData(), autoload_with=engine)
    with engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(table)).scalar() == 0
        assert conn.execute(select(func.count()).select_from(STATE)).scalar() == 0


def test_a_complete_database_partition_is_skipped_until_replacement_is_requested(
        tmp_path, monkeypatch):
    datadir = tmp_path / "datadir"
    _write(datadir, "SZ", "60", "000001", _minute_session())
    _, engine = environment(tmp_path, monkeypatch)
    table = Table("sm_stock_minute", MetaData(), autoload_with=engine)
    grid = core_grid()
    with engine.begin() as conn:
        conn.execute(table.insert(), [{
            "stock_code": "000001", "trade_date": DAY,
            "trade_time": pd.Timestamp(f"2026-07-01 {value}").to_pydatetime(),
            "volume": 1, "amount": 1, "price": 1, "avg_price": 1,
            "etl_sync_at": pd.Timestamp("2026-07-01 16:00:00").to_pydatetime(),
            "data_source": "legacy_source",
        } for value in grid])

    # The helper builds a fresh SQLite file, so drive the same engine explicitly.
    config = tmp_path / "config.json"
    report = tmp_path / "report.json"
    args = ["--config", str(config), "--dataset", "stock_minute_local",
            "--datadir", str(datadir), "--start", "2026-07-01", "--end", "2026-07-01",
            "--report", str(report), "--execute"]
    assert local_dat_import.main(args) == 0
    skipped = json.loads(report.read_text(encoding="utf-8"))
    assert skipped["partitions"]["existing_complete"] == 1
    assert skipped["partitions"]["imported"] == 0
    with engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(table)).scalar() == 241
        assert conn.execute(select(func.max(table.c.volume))).scalar() == 1

    assert local_dat_import.main(args + ["--replace-existing"]) == 0
    replaced = json.loads(report.read_text(encoding="utf-8"))
    assert replaced["partitions"]["imported"] == 1
    with engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(table)).scalar() == 241
        assert conn.execute(select(func.max(table.c.volume))).scalar() == 7036
        assert conn.execute(select(func.count()).select_from(table).where(
            table.c.data_source == "legacy_source")).scalar() == 0


def test_rerun_is_an_idempotent_content_replay(tmp_path, monkeypatch):
    datadir = tmp_path / "datadir"
    _write(datadir, "SZ", "60", "000001", _minute_session())
    _, engine = environment(tmp_path, monkeypatch)
    spec = get_spec("stock_minute_local")
    unit = WorkUnit("stock_minute_local", "guojin_qmt", "2026-07-01",
                    "000001.SZ", "1m", "none")
    rows = bars_to_rows(read_bars(datadir / "SZ" / "60" / "000001.DAT", "1m"),
                        "1m", "000001.SZ")
    batch = NormalizedBatch(
        request_id="a" * 64,
        units=[NormalizedUnit(unit=unit, status="complete", rows=rows, detail={
            "minute_grid_proof": minute_grid_proof(spec, unit, rows),
            "missing_expected_rows": 0, "out_of_scope_rows": 0, "traded": True})],
        received_at=datetime.now(SHANGHAI))
    store = Store(engine)
    store.begin_request([unit], batch.request_id, batch.received_at)
    assert store.write_partitions(spec, batch)["complete"] == 1
    second = store.write_partitions(spec, batch)
    assert second["replayed"] == 1 and second["written_rows"] == 0
    table = Table("sm_stock_minute", MetaData(), autoload_with=engine)
    with engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(table)).scalar() == 241


def test_daily_import_is_unadjusted_and_skips_an_existing_fact(tmp_path, monkeypatch):
    datadir = tmp_path / "datadir"
    _write(datadir, "SH", "86400", "600000", [
        _record(_ts(2026, 7, 1, 0), 1197_120, 1200_000, 1190_000, 1198_500,
                41_983, 5_056_689_558, 1197_120),
    ])
    code, engine, report = invoke(tmp_path, monkeypatch, datadir, "--execute",
                                  dataset="stock_daily_local")
    assert code == 0
    assert report["partitions"]["imported"] == 1
    table = Table("sm_stock_kline", MetaData(), autoload_with=engine)
    with engine.connect() as conn:
        row = conn.execute(select(table)).mappings().one()
        assert row["k_type"] == 1 and row["adjust_type"] == 0
        assert int(row["volume"]) == 4_198_300
        assert int(row["amount"]) == 5_056_689_558
        assert float(row["pre_close"]) == pytest.approx(1197.12)

    config = tmp_path / "config.json"
    report_path = tmp_path / "daily-again.json"
    assert local_dat_import.main([
        "--config", str(config), "--dataset", "stock_daily_local",
        "--datadir", str(datadir), "--start", "2026-07-01", "--end", "2026-07-01",
        "--report", str(report_path), "--execute",
    ]) == 0
    again = json.loads(report_path.read_text(encoding="utf-8"))
    assert again["partitions"]["existing_complete"] == 1
    assert again["partitions"]["imported"] == 0


def test_local_specs_share_the_stock_contract():
    spec = get_spec("stock_minute_local")
    assert spec.table == "sm_stock_minute" and spec.persisted_source == "gj_big_qmt_inner"
    assert grids(spec, "000001.SZ")[0] == tuple(sorted(core_grid()))
    assert get_spec("stock_daily_local").table == "sm_stock_kline"
