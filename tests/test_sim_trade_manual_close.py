from datetime import datetime, timedelta

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event, text
from sqlalchemy.pool import StaticPool

from server.api.routers import sim_trade
from server.engine import sim_trade_engine


NOW = datetime(2026, 11, 6, 10, 30)


@pytest.fixture
def simulated_market(monkeypatch):
    engine = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})

    @event.listens_for(engine, "connect")
    def register_mysql_functions(connection, _record):
        connection.create_function("NOW", 0, lambda: NOW.isoformat(sep=" "))
        connection.create_function("GREATEST", 2, max)

    @event.listens_for(engine, "before_cursor_execute", retval=True)
    def use_sqlite_syntax(_connection, _cursor, statement, params, _context, _executemany):
        return statement.replace("FOR UPDATE", "").replace("LAST_INSERT_ID()", "last_insert_rowid()"), params

    with engine.begin() as connection:
        connection.exec_driver_sql("""
            CREATE TABLE st_sim_position (
                id INTEGER PRIMARY KEY, stock_code TEXT, short_name TEXT, strategy_type TEXT,
                trade_mode TEXT, buy_date TEXT, buy_price REAL, buy_shares INTEGER, ai_score REAL,
                status TEXT, sell_price REAL, sell_date TEXT, sell_time TEXT, sell_reason TEXT,
                profit REAL, profit_rate REAL, holding_days INTEGER, fee_total REAL, exit_order_id INTEGER,
                updated_at TEXT
            )
        """)
        connection.exec_driver_sql("""
            CREATE TABLE st_sim_order (
                id INTEGER PRIMARY KEY, signal_id INTEGER, trade_mode TEXT, order_date TEXT, order_time TEXT,
                stock_code TEXT, short_name TEXT, strategy_type TEXT, side TEXT, order_type TEXT,
                limit_price REAL, target_price REAL, requested_shares INTEGER, remaining_shares INTEGER,
                status TEXT, position_id INTEGER, source_event TEXT, price_source TEXT, risk_budget_amount REAL,
                risk_budget_note TEXT, reason TEXT, created_at TEXT, updated_at TEXT, filled_price REAL,
                filled_shares INTEGER DEFAULT 0, filled_amount REAL DEFAULT 0, fee REAL DEFAULT 0,
                filled_at TEXT, match_count INTEGER DEFAULT 0, last_match_reason TEXT,
                reject_reason TEXT, cancel_reason TEXT
            )
        """)
        connection.exec_driver_sql("""
            CREATE TABLE st_sim_event (
                trade_mode TEXT, event_date TEXT, event_time TEXT, event_type TEXT, signal_id INTEGER,
                order_id INTEGER, position_id INTEGER, stock_code TEXT, strategy_type TEXT,
                severity TEXT, message TEXT, payload TEXT, created_at TEXT
            )
        """)
        connection.exec_driver_sql("""
            CREATE TABLE st_trade_flow (
                order_id INTEGER, stock_code TEXT, short_name TEXT, flow_type TEXT, source TEXT,
                strategy_type TEXT, trade_mode TEXT, trans_type TEXT, price REAL, shares INTEGER,
                amount REAL, fee REAL, reason TEXT, ai_score REAL, trans_date TEXT, trans_time TEXT
            )
        """)
        connection.exec_driver_sql("""
            CREATE TABLE sm_stock_current (
                stock_code TEXT, short_name TEXT, price REAL, volume INTEGER, change_pct REAL,
                source_time TEXT, data_source TEXT
            )
        """)
        connection.exec_driver_sql("""
            CREATE TABLE st_sim_signal (
                id INTEGER PRIMARY KEY, ai_score REAL, short_score REAL, long_score REAL,
                capital_score REAL, technical_score REAL, fundamental_score REAL, event_risk_level TEXT
            )
        """)
        connection.execute(text("""
            INSERT INTO st_sim_position
                (id, stock_code, short_name, strategy_type, trade_mode, buy_date,
                 buy_price, buy_shares, ai_score, status)
            VALUES (1, '000001', '测试', 'short_term', NULL, '2026-11-05', 10, 100, 70, 'holding')
        """))
        connection.execute(text("""
            INSERT INTO sm_stock_current VALUES ('000001', '测试', 11, 10000000, 4, :source_time, :source)
        """), {
            "source_time": (NOW - timedelta(seconds=30)).isoformat(sep=" "),
            "source": sim_trade_engine.PROVIDER_ID,
        })

    monkeypatch.setattr(sim_trade_engine, "_ensure_tables", lambda: None)
    monkeypatch.setattr(sim_trade, "_ensure_tables", lambda: None)
    monkeypatch.setattr(sim_trade_engine, "get_engine", lambda: engine)
    monkeypatch.setattr(sim_trade_engine, "_sell_quote_engine", lambda: engine, raising=False)
    monkeypatch.setattr(sim_trade, "get_engine", lambda: engine)
    monkeypatch.setattr(sim_trade_engine, "_sell_now", lambda: NOW, raising=False)
    monkeypatch.setattr(sim_trade_engine, "_is_trade_date", lambda _: True)
    monkeypatch.setattr(sim_trade_engine, "_is_trading_time", lambda: True)
    monkeypatch.setattr(sim_trade, "_get_current_price", lambda _: {
        "source": "kline_close", "price": 11, "trade_date": "2026-11-05",
    }, raising=False)
    monkeypatch.setattr("requests.get", lambda *args, **kwargs: pytest.fail("Unexpected external quote call"))
    app = FastAPI()
    app.include_router(sim_trade.router, prefix="/api")
    with TestClient(app) as client:
        yield engine, client
    engine.dispose()


def _close(client):
    return client.post("/api/sim-trade/close/1").json()


def _ledger(engine):
    with engine.connect() as connection:
        position = connection.execute(text(
            "SELECT status, exit_order_id, sell_price FROM st_sim_position"
        )).one()
        orders = connection.execute(text("SELECT id, status, source_event FROM st_sim_order")).all()
        flows = connection.execute(text("SELECT order_id, price, trade_mode FROM st_trade_flow")).all()
    return position, orders, flows


@pytest.mark.parametrize("buy_date", ["2026-11-06", None])
def test_manual_close_cannot_sell_before_t_plus_one(simulated_market, buy_date):
    engine, client = simulated_market
    with engine.begin() as connection:
        connection.execute(text("UPDATE st_sim_position SET buy_date=:buy_date"), {"buy_date": buy_date})
    assert _close(client)["reason"] == "t_plus_1"
    assert _ledger(engine) == (("holding", None, None), [], [])


@pytest.mark.parametrize("source_time", ["2026-11-05 15:00:00", "2026-11-06 10:24:00", "2026-11-06 10:31:00"])
def test_manual_close_cannot_fill_stale_or_future_source_quote(simulated_market, monkeypatch, source_time):
    engine, client = simulated_market
    with engine.begin() as connection:
        connection.execute(text("UPDATE sm_stock_current SET source_time=:source_time"), {"source_time": source_time})
    monkeypatch.setattr("requests.get", lambda *args, **kwargs: type("Response", (), {"text": ""})())
    assert _close(client)["reason"] == "stale_price"
    assert _ledger(engine) == (("holding", None, None), [], [])


def test_manual_close_reuses_pending_order_and_scheduler_fills_once(simulated_market, monkeypatch):
    engine, client = simulated_market
    with engine.begin() as connection:
        connection.execute(text("UPDATE sm_stock_current SET change_pct=-10"))
    first = _close(client)
    assert first["status"] == "pending"
    assert first["reason"] == "limit_down"
    assert _close(client)["order_id"] == first["order_id"]
    assert _ledger(engine) == (("holding", None, None), [(first["order_id"], "PENDING", "MANUAL_CLOSE")], [])

    with engine.begin() as connection:
        connection.execute(text("UPDATE sm_stock_current SET source_time='2026-11-05 15:00:00', change_pct=4"))
    monkeypatch.setattr("requests.get", lambda *args, **kwargs: type("Response", (), {"text": ""})())
    matcher = sim_trade_engine.SimTradeEngine()
    assert matcher.match_open_orders(NOW.date().isoformat())[0]["reason"] == "stale_price"
    assert _ledger(engine)[2] == []

    with engine.begin() as connection:
        connection.execute(text("UPDATE sm_stock_current SET source_time='2026-11-06 10:29:30'"))
    assert matcher.match_open_orders(NOW.date().isoformat())[0]["status"] == "filled"
    assert matcher.match_open_orders(NOW.date().isoformat()) == []
    order_id = first["order_id"]
    assert _ledger(engine) == (("sold", order_id, 10.9945), [(order_id, "FILLED", "MANUAL_CLOSE")],
                               [(order_id, 10.9945, "live")])


def test_manual_sell_audit_failure_leaves_one_pending_order_without_partial_fill(simulated_market):
    engine, client = simulated_market
    with engine.begin() as connection:
        connection.exec_driver_sql("""
            CREATE TRIGGER reject_fill_audit BEFORE INSERT ON st_sim_event
            WHEN NEW.event_type = 'ORDER_FILLED'
            BEGIN SELECT RAISE(ABORT, 'test audit failure'); END
        """)
    assert _close(client)["status"] == "error"
    position, orders, flows = _ledger(engine)
    assert position == ("holding", None, None)
    assert len(orders) == 1 and orders[0][1:] == ("PENDING", "MANUAL_CLOSE")
    assert flows == []
    with engine.begin() as connection:
        connection.exec_driver_sql("DROP TRIGGER reject_fill_audit")
    result = _close(client)
    assert (result["status"], result["order_id"]) == ("ok", orders[0][0])
