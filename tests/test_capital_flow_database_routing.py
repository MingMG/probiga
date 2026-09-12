"""Exercise actual flow SQL with three independent database ownership scopes."""
from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest
from sqlalchemy import create_engine, event, text

from server.common import batch_db, minute_data, scheduler_validation
from tools import crawl_realtime_batch as flow


TARGET = "2026-09-11"
CODES = {"600000", "920001"}


def _rows(*codes, source="east_push2delay", value=10):
    return pd.DataFrame([
        {"stock_code": code, "trade_date": TARGET,
         **{field: value for field in flow.CAPITAL_FLOW_FIELDS},
         "data_source": source, "etl_sync_at": "2026-09-11 18:00:00"}
        for code in codes
    ])


@pytest.fixture
def databases(monkeypatch):
    primary, kline, minute = (create_engine("sqlite://") for _ in range(3))
    locks = []

    @event.listens_for(primary, "connect")
    def calendar_clock(dbapi, _record):
        dbapi.create_function("CURDATE", 0, lambda: "2026-09-12")

    @event.listens_for(minute, "connect")
    def advisory_lock(dbapi, _record):
        def acquire(name, timeout):
            locks.append((name, timeout))
            return 1
        dbapi.create_function("GET_LOCK", 2, acquire)
        dbapi.create_function("RELEASE_LOCK", 1, lambda _name: 1)

    @event.listens_for(minute, "before_cursor_execute", retval=True)
    def sqlite_upsert_dialect(_conn, _cursor, statement, params, _context, _many):
        # Keep the actual transaction, DML and readback; only translate the
        # server's duplicate-key syntax for this independent SQLite database.
        def bind_row(row):
            return tuple(value.to_pydatetime() if isinstance(value, pd.Timestamp) else value
                         for value in row)
        params = [bind_row(row) for row in params] if _many else bind_row(params)
        return statement.replace(
            "ON DUPLICATE KEY UPDATE",
            "ON CONFLICT(stock_code, trade_date) DO UPDATE SET",
        ), params

    with primary.begin() as conn:
        conn.execute(text("CREATE TABLE si_trade_calendar (trade_date TEXT, trade_status INTEGER)"))
        conn.execute(text("INSERT INTO si_trade_calendar VALUES (:day, 1)"), {"day": TARGET})
    with kline.begin() as conn:
        conn.execute(text("CREATE TABLE sm_stock_kline (stock_code TEXT, trade_date TEXT, k_type INTEGER, adjust_type INTEGER, volume REAL, amount REAL)"))
        conn.execute(text("INSERT INTO sm_stock_kline VALUES (:code, :day, 1, 0, 100, 1000)"),
                     [{"code": code, "day": TARGET} for code in sorted(CODES)])
        conn.execute(text("INSERT INTO sm_stock_kline VALUES ('830799', :day, 1, 0, 0, 0)"), {"day": TARGET})
    with minute.begin() as conn:
        fields = ", ".join(f"{field} REAL" for field in flow.CAPITAL_FLOW_FIELDS)
        conn.execute(text(f"CREATE TABLE sm_stock_capital_flow_daily (stock_code TEXT, trade_date TEXT, {fields}, data_source TEXT, etl_sync_at TEXT, PRIMARY KEY(stock_code, trade_date))"))
    monkeypatch.setattr(batch_db, "get_kline_engine", lambda: kline)
    monkeypatch.setattr(minute_data, "get_minute_engine", lambda: minute)
    monkeypatch.setattr(flow, "get_minute_engine", lambda: minute)
    monkeypatch.setattr(flow, "_capital_flow_target_kind", lambda _day: "historical")
    monkeypatch.setattr(flow, "fetch_batch", lambda *_a, **_k: pytest.fail("historical run used live quotes"))
    yield primary, kline, minute, locks
    for engine in (primary, kline, minute):
        engine.dispose()


def _stored(primary):
    return flow._read_existing_flow_partition(primary, TARGET)


def _refresh(primary, **kwargs):
    return flow.refresh_flow(primary, trade_date=TARGET, require_source_date=True,
                             reuse_verified_existing=True, **kwargs)


def test_historical_reuse_and_receipt_readback_use_both_history_databases(databases, monkeypatch):
    primary, _kline, minute, locks = databases
    _rows(*sorted(CODES)).to_sql("sm_stock_capital_flow_daily", minute, if_exists="append", index=False)
    monkeypatch.setattr(flow, "_fetch_missing_flow_rows", lambda *_a, **_k: pytest.fail("complete historical partition refetched"))
    evidence = {}
    assert _refresh(primary, execution_evidence=evidence) == 2
    assert locks == []
    assert evidence["rows_written"] == 0
    assert evidence["source_counts"] == {"east_push2delay": 2}
    payload = {"trade_date": TARGET, "row_count": 2,
               "partition_sha256": evidence["partition_sha256"],
               "source_counts": evidence["source_counts"]}
    valid, reason = scheduler_validation._validate_capital_flow_persisted_receipt(primary, payload)
    assert valid, reason
    with minute.begin() as conn:
        conn.execute(text("UPDATE sm_stock_capital_flow_daily SET main_net_inflow=999 WHERE stock_code='920001'"))
    valid, _ = scheduler_validation._validate_capital_flow_persisted_receipt(primary, payload)
    assert not valid


def test_historical_delta_and_exact_readback_share_minute_database(databases, monkeypatch):
    primary, _kline, minute, locks = databases
    original = _rows("600000", value=123)
    original.to_sql("sm_stock_capital_flow_daily", minute, if_exists="append", index=False)
    _rows("920001", source="unverified_legacy").to_sql("sm_stock_capital_flow_daily", minute, if_exists="append", index=False)
    calls = []
    def dated_fallback(codes, *, trade_date):
        calls.append((set(codes), trade_date))
        return _rows("920001", source="push2his", value=789)
    monkeypatch.setattr(flow, "_fetch_missing_flow_rows", dated_fallback)
    evidence = {}
    assert _refresh(primary, execution_evidence=evidence) == 2
    assert calls == [({"920001"}, TARGET)]
    assert locks == [(flow.CAPITAL_FLOW_DAILY_FREEZE_LOCK_NAME, 30)]
    assert evidence["rows_written"] == 1
    stored = _stored(primary).set_index("stock_code")
    assert stored.loc["600000", "main_net_inflow"] == 123
    assert stored.loc["600000", "data_source"] == "east_push2delay"
    assert stored.loc["920001", "main_net_inflow"] == 789
    assert stored.loc["920001", "data_source"] == "push2his"


def _current_source(monkeypatch):
    monkeypatch.setattr(flow, "_capital_flow_target_kind", lambda _day: "current")
    timestamp = int(datetime(2026, 9, 11, 18, tzinfo=flow.SHANGHAI).timestamp())
    monkeypatch.setattr(flow, "fetch_batch", lambda *_a, **_k: [
        {"f12": code, "f124": timestamp,
         **{field: 456 for field in flow.CAPITAL_FLOW_FIELDS.values()}}
        for code in sorted(CODES)
    ])


@pytest.mark.parametrize("break_readback", [False, True])
def test_current_atomic_replace_and_failed_readback_rollback_stay_in_minute_database(
    databases, monkeypatch, break_readback,
):
    primary, _kline, minute, locks = databases
    _rows(*sorted(CODES), value=123).to_sql("sm_stock_capital_flow_daily", minute, if_exists="append", index=False)
    _rows("600000", value=321).assign(trade_date="2026-09-10").to_sql("sm_stock_capital_flow_daily", minute, if_exists="append", index=False)
    if break_readback:
        with minute.begin() as conn:
            conn.execute(text("CREATE TRIGGER simulate_missing_row AFTER INSERT ON sm_stock_capital_flow_daily WHEN NEW.stock_code='920001' BEGIN DELETE FROM sm_stock_capital_flow_daily WHERE stock_code=NEW.stock_code AND trade_date=NEW.trade_date; END"))
    _current_source(monkeypatch)
    if break_readback:
        with pytest.raises(RuntimeError, match="atomic publication verification"):
            _refresh(primary)
    else:
        assert _refresh(primary) == 2
    stored = _stored(primary)
    assert set(stored["stock_code"]) == CODES
    assert set(stored["main_net_inflow"]) == ({123} if break_readback else {456})
    assert locks == [(flow.CAPITAL_FLOW_DAILY_FREEZE_LOCK_NAME, 30)]
    with minute.connect() as conn:
        previous = conn.execute(text("SELECT main_net_inflow FROM sm_stock_capital_flow_daily WHERE trade_date='2026-09-10'" )).scalar_one()
    assert previous == 321


def test_reference_calendar_remains_authoritative_on_primary(databases):
    primary, kline, _minute, locks = databases
    with primary.begin() as conn:
        conn.execute(text("UPDATE si_trade_calendar SET trade_status=0"))
    with kline.begin() as conn:
        conn.execute(text("CREATE TABLE si_trade_calendar (trade_date TEXT, trade_status INTEGER)"))
        conn.execute(text("INSERT INTO si_trade_calendar VALUES (:day, 1)"), {"day": TARGET})
    with pytest.raises(RuntimeError, match="not one authoritative open session"):
        _refresh(primary)
    assert locks == []
