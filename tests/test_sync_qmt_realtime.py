from contextlib import contextmanager
from datetime import datetime, timedelta
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from integrations.bigqmt import backend as native_backend
from integrations.bigqmt.spool import PROVIDER_ID, snapshot_frame
from integrations.qmt import QmtBackend
from tools import sync_qmt_realtime as task


def _now():
    return datetime.now(task.CHINA_STANDARD_TIME).replace(tzinfo=None, microsecond=0)


def _tick(now, price=10.0):
    return {"stime": now.strftime("%Y%m%d%H%M%S"), "lastPrice": price,
            "lastClose": 9.9, "volume": 100, "amount": 1000}


def _frame(now=None):
    return snapshot_frame({"source": PROVIDER_ID,
                           "quotes": {"000001.SZ": _tick(now or _now())}},
                          require_native_source_time=True)


def _native_snapshots(monkeypatch, *, full, tracked=None):
    monkeypatch.setattr(native_backend, "read_snapshot", lambda kind, **kwargs:
                        full if kind == "full" else (tracked or {}))


def test_task_uses_only_full_qmt_and_preserves_scheduler_arguments(monkeypatch):
    _native_snapshots(monkeypatch, full={"source": PROVIDER_ID,
                                        "quotes": {"000001.SZ": _tick(_now())}})
    monkeypatch.setattr(QmtBackend, "fetch_current", lambda *a, **kw:
                        pytest.fail("MiniQMT must never be invoked"))
    monkeypatch.setattr(task, "_load_short_name_map", lambda engine: {})
    writes = []
    monkeypatch.setattr(task, "_write_current_table", lambda engine, frame, **kwargs:
                        writes.append(frame) or len(frame))
    result = task.sync_qmt_realtime(engine=object(), codes=["000001", "000001"],
                                    skip_closed=False, archive_snapshot=False,
                                    min_coverage=0.60)
    assert result["status"] == "success"
    assert result["requested"] == result["verified_current_rows"] == 1
    assert result["data_source"] == PROVIDER_ID
    assert result["transport"] == "FULL_QMT_LOCAL_SNAPSHOT"
    assert result["quality_status"] == "PENDING"
    assert writes[0]["data_source"].tolist() == [PROVIDER_ID]


def test_missing_native_time_is_not_replaced_with_collection_time(monkeypatch):
    tick = _tick(_now())
    tick.pop("stime")
    _native_snapshots(monkeypatch, full={"source": PROVIDER_ID,
                                        "quotes": {"000001.SZ": tick}})
    result = native_backend.BigQmtBackend().fetch_current(
        ["000001"], require_native_source_time=True)
    assert result.empty


@pytest.mark.parametrize("field,value", [
    ("lastPrice", 0), ("lastPrice", None), ("lastPrice", "NaN"),
    ("lastPrice", float("inf")), ("lastPrice", True),
    ("volume", -100), ("volume", None), ("volume", float("inf")),
    ("amount", -1000), ("amount", None), ("amount", "NaN"),
])
def test_strict_native_reader_drops_invalid_original_values_before_normalization(
    monkeypatch, field, value,
):
    tick = _tick(_now())
    tick[field] = value
    _native_snapshots(monkeypatch, full={"source": PROVIDER_ID,
                                        "quotes": {"000001.SZ": tick}})
    frame = native_backend.BigQmtBackend().fetch_current(
        ["000001"], require_native_source_time=True)
    assert frame.empty


def test_strict_tick_rejection_preserves_legacy_tolerant_consumer_behavior():
    tick = {**_tick(_now()), "lastPrice": 0, "volume": -100, "amount": -1000}
    payload = {"source": PROVIDER_ID, "quotes": {"000001.SZ": tick}}
    assert snapshot_frame(payload, require_native_source_time=True).empty
    legacy = snapshot_frame(payload)
    assert legacy.iloc[0]["price"] == tick["lastClose"]
    assert legacy.iloc[0]["volume"] == legacy.iloc[0]["amount"] == 0


@pytest.mark.parametrize("field", ["lastPrice", "volume", "amount"])
def test_strict_native_reader_rejects_missing_original_values(field):
    tick = _tick(_now())
    tick.pop(field)
    payload = {"source": PROVIDER_ID, "quotes": {"000001.SZ": tick}}
    assert snapshot_frame(payload, require_native_source_time=True).empty


def test_zero_native_volume_and_amount_are_valid_without_repair():
    tick = {**_tick(_now()), "volume": 0, "amount": 0}
    frame = snapshot_frame({"source": PROVIDER_ID, "quotes": {"000001.SZ": tick}},
                           require_native_source_time=True)
    assert len(frame) == 1
    assert frame.iloc[0]["price"] == tick["lastPrice"]


def test_invalid_native_tick_cannot_inflate_task_coverage(monkeypatch):
    tick = {**_tick(_now()), "lastPrice": 0, "volume": -100, "amount": -1000}
    _native_snapshots(monkeypatch, full={"source": PROVIDER_ID,
                                        "quotes": {"000001.SZ": tick}})
    monkeypatch.setattr(task, "_load_short_name_map", lambda engine: {})
    monkeypatch.setattr(task, "_write_current_table", lambda *a, **kw:
                        pytest.fail("invalid original tick must not write"))
    with pytest.raises(RuntimeError, match="no realtime rows"):
        task.sync_qmt_realtime(engine=object(), codes=["000001"],
                               skip_closed=False, archive_snapshot=False)


def test_received_rows_without_verified_targets_cannot_report_success(monkeypatch):
    _native_snapshots(monkeypatch, full={"source": PROVIDER_ID,
                                        "quotes": {"000001.SZ": _tick(_now())}})
    monkeypatch.setattr(task, "_load_short_name_map", lambda engine: {})
    monkeypatch.setattr(task, "_write_current_table", lambda *a, **kw: 0)
    with pytest.raises(RuntimeError, match="verified target count"):
        task.sync_qmt_realtime(engine=object(), codes=["000001"],
                               skip_closed=False, archive_snapshot=False)


def test_native_reader_rejects_wrong_source_and_prefers_newest_event(monkeypatch):
    now = _now()
    _native_snapshots(monkeypatch,
                      full={"source": PROVIDER_ID, "quotes": {"000001.SZ": _tick(now, 11)}},
                      tracked={"source": PROVIDER_ID,
                               "quotes": {"000001.SZ": _tick(now - timedelta(seconds=10), 10)}})
    result = native_backend.BigQmtBackend().fetch_current(
        ["000001"], require_native_source_time=True)
    assert result.iloc[0]["price"] == 11
    _native_snapshots(monkeypatch, full={"source": "gj_qmt",
                                        "quotes": {"000001.SZ": _tick(now)}})
    with pytest.raises(RuntimeError, match="source differs"):
        native_backend.BigQmtBackend().fetch_current(["000001"], require_native_source_time=True)


@pytest.mark.parametrize("seconds", [-121, 3])
def test_stale_or_future_quotes_do_not_count_as_coverage(seconds):
    now = _now()
    frame = _frame(now + timedelta(seconds=seconds))
    assert task._fresh_current_rows(frame, ["000001"], now=now).empty


def test_current_filter_rejects_unexpected_or_duplicate_stock():
    frame = _frame()
    with pytest.raises(RuntimeError, match="stock set"):
        task._fresh_current_rows(frame, ["600000"], now=_now())
    with pytest.raises(RuntimeError, match="stock set"):
        task._fresh_current_rows(pd.concat([frame, frame]), ["000001"], now=_now())


def test_insufficient_fresh_coverage_never_writes(monkeypatch):
    _native_snapshots(monkeypatch, full={"source": PROVIDER_ID,
                                        "quotes": {"000001.SZ": _tick(_now() - timedelta(days=1))}})
    monkeypatch.setattr(task, "_load_short_name_map", lambda engine: {})
    monkeypatch.setattr(task, "_write_current_table", lambda *a, **kw:
                        pytest.fail("stale data must not write"))
    with pytest.raises(RuntimeError, match="coverage below"):
        task.sync_qmt_realtime(engine=object(), codes=["000001"],
                               skip_closed=False, archive_snapshot=False)


def _database(monkeypatch):
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE sm_stock_current (stock_code TEXT PRIMARY KEY, "
                          "data_source TEXT, source_time TEXT, price NUMERIC)"))

    @contextmanager
    def lock(given_engine, name, **kwargs):
        assert given_engine is engine and name == "probiga:stock_current"
        yield None

    monkeypatch.setattr(task, "mysql_named_lock", lock)
    return engine


def test_writer_fails_on_zero_accepted_and_independently_checks_committed_rows(monkeypatch):
    engine = _database(monkeypatch)
    monkeypatch.setattr(task, "safe_upsert_rows", lambda *a, **kw: SimpleNamespace(accepted_rows=0))
    with pytest.raises(RuntimeError, match="accepted-row count"):
        task._write_current_table(engine, _frame())
    monkeypatch.setattr(task, "safe_upsert_rows", lambda *a, **kw: SimpleNamespace(accepted_rows=1))
    with pytest.raises(RuntimeError, match="committed readback"):
        task._write_current_table(engine, _frame())


def test_writer_preserves_newer_full_qmt_rows_without_claiming_an_insert(monkeypatch):
    engine = _database(monkeypatch)
    now = _now()
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO sm_stock_current VALUES (:code,:source,:time,11)"),
                     {"code": "000001", "source": PROVIDER_ID,
                      "time": now.isoformat(sep=" ")})
    monkeypatch.setattr(task, "safe_upsert_rows", lambda *a, **kw:
                        pytest.fail("must preserve newer committed event"))
    assert task._write_current_table(engine, _frame(now - timedelta(seconds=10))) == 1
    with engine.connect() as conn:
        assert conn.execute(text("SELECT price FROM sm_stock_current")).scalar() == 11


@pytest.mark.parametrize("seconds,price", [
    (30, 11),
    (0, 0),
    (0, -1),
    (0, "NaN"),
    (0, "Infinity"),
    (0, None),
])
def test_writer_rejects_invalid_newer_database_rows_without_repair(monkeypatch, seconds, price):
    engine = _database(monkeypatch)
    now = _now()
    source_time = (now + timedelta(seconds=seconds)).isoformat(sep=" ")
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO sm_stock_current VALUES (:code,:source,:time,:price)"),
                     {"code": "000001", "source": PROVIDER_ID,
                      "time": source_time, "price": price})
    monkeypatch.setattr(task, "safe_upsert_rows", lambda *a, **kw:
                        pytest.fail("invalid newer row must not be silently repaired"))
    with pytest.raises(RuntimeError, match="existing newer row is invalid"):
        task._write_current_table(engine, _frame(now - timedelta(seconds=10)))
    with engine.connect() as conn:
        retained = conn.execute(text("SELECT source_time, price FROM sm_stock_current")).first()
    assert retained == (source_time, price)


def test_readback_rejects_invalid_newer_price_even_after_upsert_reports_success(monkeypatch):
    engine = _database(monkeypatch)
    now = _now()

    def corrupt_upsert(given_engine, **kwargs):
        with given_engine.begin() as conn:
            conn.execute(text("INSERT INTO sm_stock_current VALUES ('000001',:source,:time,0)"),
                         {"source": PROVIDER_ID, "time": now.isoformat(sep=" ")})
        return SimpleNamespace(accepted_rows=1)

    monkeypatch.setattr(task, "safe_upsert_rows", corrupt_upsert)
    with pytest.raises(RuntimeError, match="committed readback differs"):
        task._write_current_table(engine, _frame(now - timedelta(seconds=10)))


def test_writer_preserves_native_provenance_and_pending_quality(monkeypatch):
    engine = _database(monkeypatch)

    def upsert(given_engine, *, rows, **kwargs):
        assert kwargs["quality_status"] == "PENDING"
        assert rows[0]["quality_status"] == "PENDING"
        assert rows[0]["data_source"] == PROVIDER_ID
        with given_engine.begin() as conn:
            for row in rows:
                conn.execute(text("INSERT INTO sm_stock_current VALUES (:code,:source,:time,:price)"),
                             {"code": row["stock_code"], "source": row["data_source"],
                              "time": str(row["source_time"]), "price": row["price"]})
        return SimpleNamespace(accepted_rows=len(rows))

    monkeypatch.setattr(task, "safe_upsert_rows", upsert)
    assert task._write_current_table(engine, _frame()) == 1
