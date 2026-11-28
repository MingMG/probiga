import re
import sqlite3
import json
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, event

from server.trading_v2.domain import InstrumentRule, OrderSide
from server.trading_v2.ledger import FeeProfile
from server.trading_v3 import intraday_pair as study
from server.trading_v3.intraday_pair_report import daily_pnl, metrics

D = Decimal


@pytest.fixture
def book(monkeypatch):
    e = create_engine(
        "sqlite://", connect_args={"detect_types": sqlite3.PARSE_DECLTYPES}
    )

    @event.listens_for(e, "before_cursor_execute", retval=True)
    def indices(conn, cursor, sql, params, context, executemany):
        return re.sub(r" FORCE INDEX\(\w+\)", "", sql), params

    rule = InstrumentRule(
        "002165",
        "paper",
        "A_SHARE",
        "SZ",
        date(2026, 1, 1),
        None,
        True,
        100,
        100,
        1,
        1,
        D(".01"),
        D(".10"),
        fee_profile_version="paper",
    )
    fee = FeeProfile("paper", D(".00025"), D(".00025"), D("5"), D(".0005"))
    monkeypatch.setattr(study, "_specs", lambda *_: (rule, fee))
    with e.begin() as c:
        c.exec_driver_sql("""CREATE TABLE st_quote_event_v2(quote_event_id text,stock_code text,
            quote_at timestamp,received_at timestamp,source_provider text,source_batch_id text,
            bid1 int,ask1 int,bid1_volume int,ask1_volume int,last_price int,
            pre_close int,upper_limit int,lower_limit int,suspended int)""")
        yield c


def add(c, id, at, received, depth=1000, ask=10):
    c.exec_driver_sql(
        "INSERT INTO st_quote_event_v2 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            id,
            "002165",
            at,
            received,
            "gj_big_qmt_inner",
            "batch",
            9,
            ask,
            1000,
            depth,
            10,
            10,
            11,
            9,
            0,
        ),
    )


def test_prospective_same_pair_limit_risk_and_t1(book):
    t = datetime(2026, 11, 30, 10, 5)
    dates = [t.date(), date(2026, 12, 1), date(2026, 12, 2)]
    spec = {**study.contract(), "fees": "paper"}
    q = {"quote_event_id": "decision", "stock_code": "002165", "last_price": D("10")}
    state = study._plan(
        book,
        {"contract": spec, "hash": study._digest(spec)},
        t.date(),
        1,
        dates,
        t,
        q,
        {"fill_probability": None},
        {},
        t,
    )
    assert state["kind"] == "OBSERVATION_ONLY" and not state["original_signal"]
    assert state["limit_price"] == "10.05" and state["quantity"] > 0
    a, b = state["arms"].values()
    assert a["entry_at"] > str(t) and b["entry_at"] == "2026-12-01 09:35:00"
    assert (
        a["exit_at"] == "2026-12-01 09:35:00" and b["exit_at"] == "2026-12-02 09:35:00"
    )


def test_only_first_post_acceptance_quote_and_visible_quantity(book):
    t = datetime(2026, 11, 30, 10, 5)
    state = {"quantity": 200, "stock_code": "002165", "limit_price": "10.05"}
    spec = {"rules": "paper", "fees": "paper", "slippage": "0.0005"}
    arm = {"entry_at": str(t)}
    add(book, "pre", t, t + timedelta(seconds=2))
    add(book, "late", t + timedelta(seconds=1), t + timedelta(seconds=20))
    add(book, "ok", t + timedelta(seconds=3), t + timedelta(seconds=3), depth=100)
    add(book, "later", t + timedelta(seconds=4), t + timedelta(seconds=4))
    out = study._match(book, state, arm, OrderSide.BUY, t + timedelta(seconds=16), spec)
    assert out["quote"]["quote_event_id"] == "ok"
    assert (
        out["status"] == "PARTIALLY_FILLED"
        and out["quantity"] == 20
        and out["fee"] == "5.00"
    )
    arm["entry_at"] = str(t + timedelta(minutes=1))
    assert (
        study._match(book, state, arm, OrderSide.BUY, t + timedelta(minutes=2), spec)[
            "quantity"
        ]
        is None
    )


def test_incomplete_exit_and_missing_mark_excluded_from_net_returns():
    arm = {
        "entry_at": "2026-11-30 10:05:00",
        "exit_at": "2026-12-01 09:35:00",
        "entry": {"quantity": 100, "status": "FILLED", "gross": "1000", "fee": "5"},
        "exit": {"quantity": 100, "gross": "1025", "fee": "6"},
        "entry_close_mark": {"bid1": "9.9"},
    }
    pnl = daily_pnl(arm)
    assert pnl == {"2026-11-30": D("-15"), "2026-12-01": D("29")}
    m = metrics(pnl, D("1000"))
    assert (
        m["net_return_pct"] == 1.4
        and m["max_drawdown_pct"] == 1.5
        and m["worst_day"] == "2026-11-30"
    )
    assert daily_pnl({**arm, "entry_close_mark": None}) is None
    assert daily_pnl({**arm, "exit": {"quantity": 50}}) is None
    assert daily_pnl(
        {**arm, "entry": {"status": "UNFILLED", "quantity": 0}}
    ) == dict.fromkeys(pnl, D(0))


def test_pair_receipt_survives_restart_and_cannot_consume_quote_twice(book):
    book.exec_driver_sql("""CREATE TABLE st_strategy_governance_audit(audit_id text primary key,
        entity_type text,entity_key text,action text,reason text,operator_name text,before_json text,
        after_json text,evidence_json text,payload_json text,audit_hash text,created_at timestamp default current_timestamp)""")
    t = datetime(2026, 11, 30, 10, 5)
    state = {
        "quantity": 100,
        "stock_code": "002165",
        "limit_price": "10.05",
        "key": "pair",
        "status": "PENDING",
        "arms": {
            "A": {"entry_at": str(t), "exit_at": "2026-12-01 09:35:00"},
            "B": {"entry_at": "2026-12-01 09:35:00", "exit_at": "2026-12-02 09:35:00"},
        },
    }
    add(book, "one", t + timedelta(seconds=3), t + timedelta(seconds=3))
    spec = {"rules": "paper", "fees": "paper", "slippage": "0.0005"}
    study._save(book, "pair", "ACCEPTED", state)
    study.advance(book, study._read(book, "pair"), t + timedelta(minutes=1), spec)
    restored = study._read(book, "pair")
    assert (
        restored["arms"]["A"]["entry"]["quantity"] == 100
        and "entry" not in restored["arms"]["B"]
    )
    study.advance(book, restored, t + timedelta(minutes=2), spec)
    assert (
        book.exec_driver_sql(
            "SELECT COUNT(*) FROM st_strategy_governance_audit"
        ).scalar()
        == 2
    )


def test_missing_minute_clock_and_fill_probability_are_preserved(book, monkeypatch):
    from server.trading_v3 import intraday_pair_features as features

    book.exec_driver_sql(
        "CREATE TABLE si_trade_calendar (trade_date date,trade_status int)"
    )
    day = date(2026, 11, 30)
    book.exec_driver_sql("INSERT INTO si_trade_calendar VALUES (?,1)", (day,))
    cutoff = datetime(2026, 11, 30, 10, 10, 10)
    quote = {
        "stock_code": "002165",
        "quote_event_id": "source",
        "quote_at": cutoff,
        "bid1": 9.99,
        "ask1": 10.01,
        "bid1_volume": 200,
        "ask1_volume": 200,
        "last_price": 10,
    }
    monkeypatch.setattr(features, "_sector", lambda *_: ({}, {}))

    def window(*args):
        return {
            day: [
                {
                    "trade_time": datetime(2026, 11, 30, 10, 4) + timedelta(minutes=i),
                    "amount": 10000,
                    "volume": 10,
                    "price": 9.9 if i == 0 else 10,
                    "data_source": "qmt_datadir",
                }
                for i in range(6)
            ]
        }

    monkeypatch.setattr(features, "_minutes", window)
    values, proof = features.snapshot(book, quote, cutoff)
    assert values["price_vs_vwap_pct"] == 0 and values[
        "interval_return_pct"
    ] == pytest.approx(1.010101)
    assert (
        values["fill_probability"] is None
        and "intraday_amount_surprise_z" not in values
    )
    assert proof["window"] == ["2026-11-30 10:04:00", "2026-11-30 10:09:00"]


def test_registration_precedes_two_versioned_arms_and_reuses_original_start(book):
    book.exec_driver_sql("""CREATE TABLE st_strategy_governance_audit(audit_id text primary key,
        entity_type text,entity_key text,action text,reason text,operator_name text,before_json text,
        after_json text,evidence_json text,payload_json text,audit_hash text,created_at timestamp default current_timestamp)""")
    book.exec_driver_sql(
        "CREATE TABLE si_trade_calendar (trade_date date,trade_status int)"
    )
    book.exec_driver_sql(
        "INSERT INTO si_trade_calendar VALUES ('2026-11-30',1),('2026-12-01',1)"
    )
    book.exec_driver_sql(
        "CREATE TABLE st_strategy_registry(strategy_key text,current_version text)"
    )
    book.exec_driver_sql(
        "CREATE TABLE st_strategy_version(strategy_key text,version text,version_hash text,parameters_json text,source_kind text)"
    )
    book.exec_driver_sql(
        "INSERT INTO st_strategy_registry VALUES ('intraday_surprise','parent')"
    )
    book.exec_driver_sql(
        "INSERT INTO st_strategy_version VALUES ('intraday_surprise','parent','source-hash',?,'immutable_v3_sleeve')",
        (
            json.dumps(
                {
                    "shadow_execution_policy": {
                        "entry_timing": "NEXT_AUTHORITATIVE_SESSION"
                    }
                }
            ),
        ),
    )
    head = study.register(book, datetime(2026, 11, 28, 10))
    assert (
        head["first_session"] == "2026-11-30"
        and len({v["hash"] for v in head["arm_versions"].values()}) == 2
    )
    assert study.register(book, datetime(2026, 11, 30, 10)) == head
    assert (
        book.exec_driver_sql(
            "SELECT COUNT(*) FROM st_strategy_governance_audit"
        ).scalar()
        == 1
    )
