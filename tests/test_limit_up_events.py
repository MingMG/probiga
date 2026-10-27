# -*- coding: utf-8 -*-
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta
from decimal import Decimal

import pandas as pd

from server.common.limit_up_events import (
    LIMIT_STATUS_BASIS,
    board_limit_ratio,
    derive_limit_up_events,
    derived_upper_limit,
)
from tools import run_big_qmt_bridge as bridge


def _quote(price: str, *, at: datetime, bid_volume: str = "1000"):
    return {
        "stock_code": "000001",
        "short_name": "平安银行",
        "snapshot_dt": at,
        "qmt_timetag": at.strftime("%Y%m%d %H:%M:%S"),
        "price": Decimal(price),
        "pre_close": Decimal("10.00"),
        "bid_price": [Decimal("11.00")],
        "bid_vol": [Decimal(bid_volume)],
        "ask_price": [Decimal("0")],
        "ask_vol": [Decimal("0")],
        "stale": 0,
        "data_source": "qmt_full_tick_5level",
    }


def test_board_limit_rules_cover_st_growth_star_and_beijing():
    assert board_limit_ratio("000001") == Decimal("0.10")
    assert board_limit_ratio("300001") == Decimal("0.20")
    assert board_limit_ratio("688001") == Decimal("0.20")
    assert board_limit_ratio("920001") == Decimal("0.30")
    assert board_limit_ratio("000001", "*ST测试") == Decimal("0.05")


def test_derived_upper_limit_rounds_to_one_fen():
    assert derived_upper_limit("000001", "测试", "10.01") == Decimal("11.01")
    assert derived_upper_limit("300001", "测试", "10.01") == Decimal("12.01")
    assert derived_upper_limit("000001", "*ST测试", "10.01") == Decimal("10.51")


def test_touch_seal_break_reseal_and_queue_snapshot_have_time_order():
    now = datetime(2026, 10, 27, 9, 30)
    first_events, state = derive_limit_up_events(
        _quote("11.00", at=now),
        None,
        now=now,
    )
    assert [event["event_type"] for event in first_events] == ["TOUCH", "SEAL"]
    assert state is not None
    assert state["ever_sealed"] is True
    assert first_events[0]["limit_status_basis"] == LIMIT_STATUS_BASIS

    queue_time = now + timedelta(seconds=61)
    queue_events, state = derive_limit_up_events(
        _quote("11.00", at=queue_time, bid_volume="1200"),
        state,
        now=queue_time,
    )
    assert [event["event_type"] for event in queue_events] == ["QUEUE_SNAPSHOT"]
    assert queue_events[0]["previous_bid1_volume_raw"] == Decimal("1000")

    break_time = queue_time + timedelta(seconds=5)
    break_events, state = derive_limit_up_events(
        _quote("10.99", at=break_time),
        state,
        now=break_time,
    )
    assert [event["event_type"] for event in break_events] == ["BREAK"]

    reseal_time = break_time + timedelta(seconds=5)
    reseal_events, _ = derive_limit_up_events(
        _quote("11.00", at=reseal_time),
        state,
        now=reseal_time,
    )
    assert [event["event_type"] for event in reseal_events] == ["RESEAL"]


def test_stale_quote_never_creates_an_event():
    now = datetime(2026, 10, 27, 9, 30)
    quote = _quote("11.00", at=now)
    quote["stale"] = 1
    events, state = derive_limit_up_events(quote, None, now=now)
    assert events == []
    assert state is None


def test_bigqmt_capture_uses_native_source_time_and_first_levels(monkeypatch):
    monkeypatch.setattr(
        bridge,
        "validate_limit_up_event_runtime",
        lambda _engine: None,
    )
    bridge._limit_event_schema_checked = False
    bridge._limit_state_trade_date = datetime(2026, 10, 27).date()
    bridge._limit_event_states.clear()
    frame = pd.DataFrame(
        [
            {
                "stock_code": "000001",
                "short_name": "平安银行",
                "source_time": pd.Timestamp("2026-10-27 09:31:00"),
                "price": 11.0,
                "pre_close": 10.0,
                "bid1": 11.0,
                "bid1_volume": 1000,
                "ask1": 0,
                "ask1_volume": 0,
            }
        ]
    )

    events, states = bridge._capture_limit_up_events(
        object(),
        frame,
        now=datetime(2026, 10, 27, 9, 31, 5),
    )

    assert [event["event_type"] for event in events] == ["TOUCH", "SEAL"]
    assert events[0]["event_at"] == datetime(2026, 10, 27, 9, 31)
    assert states["000001"]["sealed"] is True


def test_bigqmt_full_snapshot_writes_limit_events_in_the_same_transaction(
    monkeypatch,
):
    statements = []

    class _Connection:
        def execute(self, statement, params=None):
            statements.append(str(statement))
            return None

    connection = _Connection()

    class _Begin:
        def __enter__(self):
            return connection

        def __exit__(self, *_args):
            return False

    class _Engine:
        def begin(self):
            return _Begin()

    @contextmanager
    def _lock(_engine, _name, timeout_seconds=0):
        yield

    monkeypatch.setattr(bridge, "mysql_named_lock", _lock)
    monkeypatch.setattr(
        bridge,
        "_database_frame",
        lambda _engine, frame: frame,
    )
    frame = pd.DataFrame(
        [{"stock_code": "000001", "price": Decimal("11.000000")}]
    )
    bridge._replace_full_snapshot(
        _Engine(),
        frame,
        limit_up_events=[{"event_key": "event-1"}],
    )

    assert any("INSERT INTO sm_stock_current" in sql for sql in statements)
    assert any("INSERT IGNORE INTO sm_limit_up_event" in sql for sql in statements)
    assert any("DELETE FROM sm_stock_current" in sql for sql in statements)


def test_bigqmt_restart_recovers_last_limit_state():
    class _Result:
        def mappings(self):
            return self

        def all(self):
            return [
                {
                    "stock_code": "000001",
                    "event_type": "BREAK",
                    "event_at": datetime(2026, 10, 27, 10, 10),
                    "bid1_volume_raw": 0,
                    "ever_sealed": 1,
                },
                {
                    "stock_code": "000002",
                    "event_type": "QUEUE_SNAPSHOT",
                    "event_at": datetime(2026, 10, 27, 10, 11),
                    "bid1_volume_raw": 1200,
                    "ever_sealed": 1,
                },
            ]

    class _Connection:
        def execute(self, _statement, _params=None):
            return _Result()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class _Engine:
        def connect(self):
            return _Connection()

    states = bridge._load_limit_event_states(
        _Engine(),
        datetime(2026, 10, 27).date(),
    )

    assert states["000001"]["at_limit"] is False
    assert states["000001"]["ever_sealed"] is True
    assert states["000002"]["at_limit"] is True
    assert states["000002"]["sealed"] is True
