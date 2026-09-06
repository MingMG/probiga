from __future__ import annotations

from datetime import datetime

import pytest
from sqlalchemy import create_engine, event, text

from tools import verify_direct_capital_flow_daily as verifier


TARGET = "2026-09-04"
BUILD_SHA = "a" * 40


def _engine():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE si_trade_calendar (
                trade_date TEXT PRIMARY KEY,
                trade_status INTEGER NOT NULL
            )
        """))
        connection.execute(text("""
            CREATE TABLE sm_stock_kline (
                stock_code TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                k_type INTEGER NOT NULL,
                adjust_type INTEGER NOT NULL,
                volume NUMERIC NOT NULL,
                amount NUMERIC NOT NULL
            )
        """))
        connection.execute(text("""
            CREATE TABLE sm_stock_capital_flow_daily (
                stock_code TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                main_net_inflow NUMERIC,
                max_net_inflow NUMERIC,
                lg_net_inflow NUMERIC,
                mid_net_inflow NUMERIC,
                sm_net_inflow NUMERIC,
                data_source TEXT
            )
        """))
        connection.execute(
            text("INSERT INTO si_trade_calendar VALUES (:d, 1)"),
            {"d": TARGET},
        )
        connection.execute(text("""
            INSERT INTO sm_stock_kline VALUES
                ('000001', :d, 1, 0, 100, 1000),
                ('600000', :d, 1, 0, 200, 2000),
                ('830001', :d, 1, 0, 300, 3000),
                ('300001', :d, 1, 0, 0, 0)
        """), {"d": TARGET})
        connection.execute(text("""
            INSERT INTO sm_stock_capital_flow_daily VALUES
                ('000001', :d, -10, -4, -6, 3, 7, :source),
                ('600000', :d, 20, 8, 12, -3, -17, :source)
        """), {"d": TARGET, "source": verifier.PROVIDER})
    return engine


def test_direct_qmt_receipt_is_exact_signed_and_read_only(monkeypatch):
    engine = _engine()
    monkeypatch.setattr(
        verifier,
        "routed_read_engine",
        lambda _sql, supplied: supplied,
    )
    monkeypatch.setenv("PROBIGA_SCHEDULER_BUILD_SHA", BUILD_SHA)
    statements = []
    event.listen(
        engine,
        "before_cursor_execute",
        lambda _conn, _cursor, statement, _parameters, _context, _many: (
            statements.append(statement)
        ),
    )

    receipt = verifier.build_receipt(
        engine,
        TARGET,
        now=datetime(2026, 9, 4, 15, 45),
    )

    assert receipt["schema"] == verifier.RECEIPT_SCHEMA
    assert receipt["status"] == "PASS"
    assert receipt["provider"] == verifier.PROVIDER
    assert receipt["row_count"] == receipt["expected_row_count"] == 2
    assert receipt["source_counts"] == {verifier.PROVIDER: 2}
    assert receipt["read_only"] is True
    assert receipt["network_accessed"] is False
    unsigned = dict(receipt)
    supplied = unsigned.pop("receipt_id")
    assert supplied == verifier._sha256_json(unsigned)
    assert statements
    assert all(statement.lstrip().upper().startswith("SELECT") for statement in statements)


@pytest.mark.parametrize(
    "mutation, message",
    [
        (
            "UPDATE sm_stock_capital_flow_daily SET data_source='eastmoney' "
            "WHERE stock_code='000001'",
            "another source",
        ),
        (
            "DELETE FROM sm_stock_capital_flow_daily WHERE stock_code='600000'",
            "coverage differs",
        ),
        (
            "UPDATE sm_stock_capital_flow_daily SET main_net_inflow=NULL "
            "WHERE stock_code='000001'",
            "not numeric",
        ),
        (
            "UPDATE sm_stock_capital_flow_daily SET main_net_inflow=999 "
            "WHERE stock_code='000001'",
            "identity differs",
        ),
    ],
)
def test_direct_qmt_receipt_rejects_wrong_partition(monkeypatch, mutation, message):
    engine = _engine()
    monkeypatch.setattr(
        verifier,
        "routed_read_engine",
        lambda _sql, supplied: supplied,
    )
    with engine.begin() as connection:
        connection.execute(text(mutation))

    with pytest.raises(RuntimeError, match=message):
        verifier.inspect_partition(engine, TARGET)


def test_resolve_trade_date_accepts_exact_closed_open_session(monkeypatch):
    engine = _engine()
    monkeypatch.setattr(
        verifier,
        "routed_read_engine",
        lambda _sql, supplied: supplied,
    )
    monkeypatch.setattr(
        verifier,
        "authoritative_closed_trade_date",
        lambda *_args, **_kwargs: TARGET,
    )

    assert verifier.resolve_trade_date(
        engine,
        TARGET,
        now=datetime(2026, 9, 4, 15, 45),
    ) == TARGET
    assert verifier.resolve_trade_date(
        engine,
        None,
        now=datetime(2026, 9, 4, 15, 45),
    ) == TARGET
