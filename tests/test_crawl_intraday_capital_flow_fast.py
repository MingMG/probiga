from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime

import pytest
import requests
from sqlalchemy import create_engine, text

from tools import crawl_intraday_capital_flow_fast as fast


class _Response:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class _Session:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, *, params, timeout):
        self.calls.append((url, dict(params), timeout))
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


def _item(code, base=1):
    return {
        "f12": code,
        "f62": base,
        "f66": base + 1,
        "f72": base + 2,
        "f78": base + 3,
        "f84": base + 4,
    }


def test_fetch_retries_and_uses_stable_paginated_code_order():
    session = _Session(
        [
            requests.Timeout("first request timed out"),
            _Response({"data": {"total": 3, "diff": [_item("000002", 20), _item("000001", 10)]}}),
            _Response({"data": {"total": 3, "diff": [_item("000003", 30)]}}),
        ]
    )
    sleeps = []

    rows, metadata = fast.fetch_eastmoney_capital_flow(
        session=session,
        page_size=2,
        attempts=2,
        retry_delay=0.25,
        sleep=sleeps.append,
    )

    assert list(rows) == ["000001", "000002", "000003"]
    assert metadata == {
        "provider_total": 3,
        "provider_seen": 3,
        "provider_valid": 3,
        "pages": 2,
    }
    assert sleeps == [0.25]
    assert [call[1]["pn"] for call in session.calls] == [1, 1, 2]
    assert all(call[1]["fid"] == "f12" and call[1]["po"] == 0 for call in session.calls)
    assert all("m:0+t:81+s:2048" in call[1]["fs"] for call in session.calls)


def test_fetch_fails_closed_when_pagination_stalls():
    repeated = _Response({"data": {"total": 2, "diff": [_item("000001")]}})
    session = _Session([repeated, repeated])

    with pytest.raises(RuntimeError, match="pagination stalled"):
        fast.fetch_eastmoney_capital_flow(
            session=session,
            page_size=1,
            attempts=1,
            sleep=lambda _seconds: None,
        )


def test_outside_session_skips_before_opening_any_engine(monkeypatch):
    monkeypatch.setattr(
        fast,
        "get_kline_engine",
        lambda: pytest.fail("outside-session run must not open the database"),
    )

    result = fast.run_sync(now=datetime(2026, 8, 11, 12, 0, 0))

    assert result["status"] == "skipped"
    assert result["reason"] == "outside_continuous_auction"


def test_run_intersects_active_universe_adds_extras_and_holds_named_lock(monkeypatch):
    kline_engine = object()
    minute_engine = object()
    events = []

    monkeypatch.setattr(fast, "is_trade_day", lambda engine, day: engine is kline_engine)
    monkeypatch.setattr(
        fast,
        "load_latest_active_codes",
        lambda engine: ("2026-08-10", {"000001", "000002"}),
    )
    monkeypatch.setattr(
        fast,
        "fetch_eastmoney_capital_flow",
        lambda **_kwargs: (
            {code: _item(code, index * 10) | {"stock_code": code}
             for index, code in enumerate(("000001", "000002", "000003", "000004"), 1)},
            {"provider_total": 4, "provider_seen": 4, "provider_valid": 4, "pages": 1},
        ),
    )

    @contextmanager
    def fake_lock(engine, name, *, timeout_seconds):
        events.append(("lock", engine, name, timeout_seconds))
        yield object()
        events.append(("unlock",))

    def fake_write(engine, *, trade_time, rows, snapshot_at):
        events.append(("write", engine, trade_time, [row["stock_code"] for row in rows], snapshot_at))
        return len(rows)

    monkeypatch.setattr(fast, "mysql_named_lock", fake_lock)
    monkeypatch.setattr(fast, "write_current_minute_snapshot", fake_write)

    result = fast.run_sync(
        now=datetime(2026, 8, 11, 10, 1, 37),
        kline_engine=kline_engine,
        minute_engine=minute_engine,
        extra_codes=["000003.SZ"],
        session=object(),
    )

    assert result["status"] == "written"
    assert result["coverage"] == 1.0
    assert result["expected_codes"] == 3
    assert result["written_rows"] == 3
    assert fast.LOCK_NAME == "probiga:capital_flow_minute"
    assert events[0] == ("lock", minute_engine, "probiga:capital_flow_minute", 0)
    assert events[1][0:4] == (
        "write",
        minute_engine,
        datetime(2026, 8, 11, 10, 1),
        ["000001", "000002", "000003"],
    )
    assert events[-1] == ("unlock",)


def test_coverage_gate_prevents_any_write(monkeypatch):
    active = {f"{number:06d}" for number in range(1, 101)}
    fetched = {
        code: _item(code) | {"stock_code": code}
        for code in sorted(active)[:97]
    }
    monkeypatch.setattr(fast, "is_trade_day", lambda _engine, _day: True)
    monkeypatch.setattr(fast, "load_latest_active_codes", lambda _engine: ("2026-08-10", active))
    monkeypatch.setattr(
        fast,
        "fetch_eastmoney_capital_flow",
        lambda **_kwargs: (
            fetched,
            {"provider_total": 97, "provider_seen": 97, "provider_valid": 97, "pages": 1},
        ),
    )

    @contextmanager
    def fake_lock(*_args, **_kwargs):
        yield object()

    monkeypatch.setattr(fast, "mysql_named_lock", fake_lock)
    monkeypatch.setattr(
        fast,
        "write_current_minute_snapshot",
        lambda *_args, **_kwargs: pytest.fail("coverage failure must not write"),
    )

    with pytest.raises(fast.CoverageError) as exc_info:
        fast.run_sync(
            now=datetime(2026, 8, 11, 10, 2),
            kline_engine=object(),
            minute_engine=object(),
            min_coverage=0.98,
            session=object(),
        )

    assert exc_info.value.result["coverage"] == 0.97
    assert exc_info.value.result["status"] == "coverage_failed"


def test_write_replaces_only_current_minute_in_one_transaction():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE sm_stock_capital_flow_min ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, stock_code TEXT NOT NULL, "
                "trade_time DATETIME, main_net_inflow REAL, max_net_inflow REAL, "
                "lg_net_inflow REAL, mid_net_inflow REAL, sm_net_inflow REAL, "
                "snapshot_at DATETIME NOT NULL, etl_sync_at DATETIME NOT NULL)"
            )
        )
        for code, trade_time in (
            ("000009", "2026-08-11 10:00:59"),
            ("000008", "2026-08-11 10:01:15"),
            ("000007", "2026-08-11 10:02:00"),
        ):
            conn.execute(
                text(
                    "INSERT INTO sm_stock_capital_flow_min "
                    "(stock_code, trade_time, main_net_inflow, max_net_inflow, "
                    "lg_net_inflow, mid_net_inflow, sm_net_inflow, snapshot_at, etl_sync_at) "
                    "VALUES (:code, :trade_time, 1, 2, 3, 4, 5, :trade_time, :trade_time)"
                ),
                {"code": code, "trade_time": trade_time},
            )

    written = fast.write_current_minute_snapshot(
        engine,
        trade_time=datetime(2026, 8, 11, 10, 1, 42),
        snapshot_at=datetime(2026, 8, 11, 10, 1, 43),
        rows=[
            {
                "stock_code": "000001",
                "main_net_inflow": 10,
                "max_net_inflow": 11,
                "lg_net_inflow": 12,
                "mid_net_inflow": 13,
                "sm_net_inflow": 14,
            }
        ],
    )

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT stock_code, trade_time FROM sm_stock_capital_flow_min "
                "ORDER BY trade_time, stock_code"
            )
        ).fetchall()
    assert written == 1
    assert [(row[0], str(row[1])) for row in rows] == [
        ("000009", "2026-08-11 10:00:59"),
        ("000001", "2026-08-11 10:01:00"),
        ("000007", "2026-08-11 10:02:00"),
    ]
