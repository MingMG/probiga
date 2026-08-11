from datetime import datetime

from sqlalchemy import create_engine, text

from server.trading_v2.execution import (
    _sync_v3_execution_plan_states,
    run_execution_tick,
)


def test_stale_paper_order_expires_before_market_opens():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE st_trade_account_v2 (
                    account_id TEXT PRIMARY KEY,
                    real_trading_enabled INTEGER NOT NULL
                )
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TABLE st_order_v2 (
                    order_id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    waiting_reason TEXT,
                    expires_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL
                )
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TABLE si_trade_calendar (
                    trade_date DATE PRIMARY KEY,
                    trade_status INTEGER NOT NULL
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO st_trade_account_v2
                    (account_id, real_trading_enabled)
                VALUES ('paper-main-v2', 0)
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO st_order_v2
                    (order_id, account_id, status, waiting_reason,
                     expires_at, updated_at)
                VALUES
                    ('stale', 'paper-main-v2', 'RISK_APPROVED',
                     'WAITING', '2026-07-29 15:00:00',
                     '2026-07-29 14:59:00'),
                    ('today', 'paper-main-v2', 'QUEUED',
                     'V3_NEXT_SESSION', '2026-07-30 14:45:00',
                     '2026-07-30 08:00:00')
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO si_trade_calendar
                    (trade_date, trade_status)
                VALUES ('2026-07-30', 1)
                """
            )
        )
    result = run_execution_tick(
        engine,
        now=datetime(2026, 7, 30, 8, 20),
    )
    assert result["status"] == "market_closed"
    assert result["expired_orders"] == 1
    with engine.connect() as connection:
        rows = dict(
            connection.execute(
                text(
                    """
                    SELECT order_id, status
                    FROM st_order_v2
                    ORDER BY order_id
                    """
                )
            ).all()
        )
    assert rows == {"stale": "EXPIRED", "today": "QUEUED"}


def test_v3_execution_plan_state_is_projected_from_oms_truth():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_execution_plan_v3 (
                execution_plan_id TEXT PRIMARY KEY,
                run_uid TEXT NOT NULL,
                account_id TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                side TEXT NOT NULL,
                state TEXT NOT NULL,
                updated_at DATETIME NOT NULL
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_trade_intent_v2 (
                intent_id TEXT PRIMARY KEY,
                decision_run_uid TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                action TEXT NOT NULL
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_order_v2 (
                order_id TEXT PRIMARY KEY,
                intent_id TEXT NOT NULL,
                side TEXT NOT NULL,
                status TEXT NOT NULL,
                filled_quantity INTEGER NOT NULL,
                created_at DATETIME NOT NULL
            )
        """))
        for suffix, status, filled in (
            ("filled", "FILLED", 300),
            ("partial", "PARTIALLY_FILLED", 100),
            ("cancelled", "CANCELLED", 0),
            ("partial-cancelled", "CANCELLED", 100),
        ):
            connection.execute(
                text("""
                    INSERT INTO st_execution_plan_v3 VALUES (
                        :plan_id, :run_uid, 'paper-main-v2',
                        :stock_code, 'BUY', 'PAPER_QUEUED', :created_at
                    )
                """),
                {
                    "plan_id": f"plan-{suffix}",
                    "run_uid": f"run-{suffix}",
                    "stock_code": f"code-{suffix}",
                    "created_at": "2026-08-01 09:00:00",
                },
            )
            connection.execute(
                text("""
                    INSERT INTO st_trade_intent_v2 VALUES (
                        :intent_id, :run_uid, :stock_code, 'BUY'
                    )
                """),
                {
                    "intent_id": f"intent-{suffix}",
                    "run_uid": f"run-{suffix}",
                    "stock_code": f"code-{suffix}",
                },
            )
            connection.execute(
                text("""
                    INSERT INTO st_order_v2 VALUES (
                        :order_id, :intent_id, 'BUY', :status,
                        :filled, :created_at
                    )
                """),
                {
                    "order_id": f"order-{suffix}",
                    "intent_id": f"intent-{suffix}",
                    "status": status,
                    "filled": filled,
                    "created_at": "2026-08-01 09:01:00",
                },
            )

    updated = _sync_v3_execution_plan_states(
        engine,
        account_id="paper-main-v2",
        now=datetime(2026, 8, 1, 9, 5),
    )
    assert updated == 4
    with engine.connect() as connection:
        states = dict(connection.execute(text("""
            SELECT execution_plan_id, state
            FROM st_execution_plan_v3
            ORDER BY execution_plan_id
        """)).all())
    assert states == {
        "plan-cancelled": "CANCELLED",
        "plan-filled": "PAPER_FILLED",
        "plan-partial": "PAPER_PARTIALLY_FILLED",
        "plan-partial-cancelled": "PAPER_PARTIAL_CANCELLED",
    }
