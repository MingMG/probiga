from __future__ import annotations

from datetime import date
from datetime import datetime

import pytest

from tools.sync_north_flow_daily import parse_ths_day_chart


def test_scheduled_north_flow_target_is_current_closed_session(monkeypatch):
    from tools import sync_north_flow_daily as collector
    from sqlalchemy import create_engine, text
    from server.common.authoritative_market_clock import authoritative_closed_trade_date
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE si_trade_calendar (trade_date TEXT, trade_status INTEGER)"))
        connection.execute(text("INSERT INTO si_trade_calendar VALUES ('2026-11-03',1),('2026-11-04',1)"))
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 11, 4, 15, 35, tzinfo=tz)
    monkeypatch.setattr(collector, "datetime", Clock)
    monkeypatch.setattr(collector, "load_project_env", lambda: None)
    monkeypatch.setattr(collector, "create_tool_engine", lambda: engine)
    monkeypatch.setattr(collector, "authoritative_closed_trade_date",
        lambda engine, **kwargs: authoritative_closed_trade_date(
            engine, now=Clock.now(), **kwargs))
    monkeypatch.setattr(collector, "fetch_ths_day_chart",
        lambda **kwargs: {"time": ["15:00"], "hgt": [1.0], "sgt": [2.0]})
    seen = []
    monkeypatch.setattr(collector, "publish_current_day",
        lambda engine, snapshot, **kwargs: seen.append(snapshot["target_date"]) or {})
    assert collector.main(["--date", "latest", "--apply"]) == 0
    assert seen == ["2026-11-04"]


def test_north_flow_close_snapshot_uses_final_ths_values():
    snapshot = parse_ths_day_chart(
        {
            "time": ["09:30", "15:00"],
            "hgt": [1.25, -9.28],
            "sgt": [-2.5, 379.75],
        },
        target_date="2026-10-28",
    )

    assert snapshot["daily"] == {
        "trade_date": date(2026, 10, 28),
        "net_hgt": -928_000_000,
        "net_sgt": 37_975_000_000,
        "net_tgt": 37_047_000_000,
    }
    assert snapshot["latest"]["trade_time"].strftime("%H:%M") == "15:00"
    assert len(snapshot["minute_rows"]) == 2


def test_north_flow_rejects_unfinished_session():
    with pytest.raises(ValueError, match="completed 15:00"):
        parse_ths_day_chart(
            {
                "time": ["09:30", "14:59"],
                "hgt": [1.0, 2.0],
                "sgt": [-1.0, -2.0],
            },
            target_date="2026-10-28",
        )


def test_north_flow_rejects_misaligned_provider_arrays():
    with pytest.raises(ValueError, match="misaligned"):
        parse_ths_day_chart(
            {"time": ["15:00"], "hgt": [], "sgt": [1.0]},
            target_date="2026-10-28",
        )


def test_north_flow_allows_trailing_shenzhen_window():
    snapshot = parse_ths_day_chart(
        {
            "time": ["09:30", "09:31", "15:00"],
            "hgt": [1.0, 2.0, -9.28],
            "sgt": [401.15, 379.75],
        },
        target_date="2026-10-28",
    )

    assert snapshot["daily"]["net_tgt"] == 37_047_000_000
    assert snapshot["minute_rows"] == []
