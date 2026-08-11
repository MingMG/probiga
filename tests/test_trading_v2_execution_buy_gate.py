from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, event, text

from server.trading_v2 import execution as execution_module
from server.trading_v2.domain import Quote
from server.trading_v2.execution_buy_gate import (
    BuyGateDecision,
    GATE_MODULE,
    append_buy_gate_binding,
    bound_buy_gate,
    build_buy_gate_binding,
    evaluate_buy_gate,
    load_current_buy_gate,
)
from server.trading_v2.ledger import FeeProfile


RUN_UID = "run-gate-1"
STRATEGY = "stock_strategy_v2.0.0:short_term"
STOCK = "000001"
CONTEXT_HASH = "a" * 64
NOW = datetime(2026, 8, 5, 10, 0)
VALID_UNTIL = datetime(2026, 8, 5, 15, 0)


def _binding(**overrides):
    values = {
        "decision_run_uid": RUN_UID,
        "strategy_version": STRATEGY,
        "stock_code": STOCK,
        "context_hash": CONTEXT_HASH,
        "valid_until": VALID_UNTIL,
        "recommendation_data_date": "2026-08-04",
        "recommend_status": "ALLOW",
        "signal_status": "CONFIRM",
        "chase_risk_status": "ALLOW",
        "ordinary_buy_eligible": True,
        "event_risk_level": "LOW",
        "source_health_status": "PASS",
    }
    values.update(overrides)
    return build_buy_gate_binding(**values)


def test_gate_receipt_round_trip_and_allowed_decision():
    binding = _binding()
    evidence = append_buy_gate_binding(({"module": "strategy"},), binding)

    restored = bound_buy_gate(json.dumps(list(evidence)))
    decision = evaluate_buy_gate(
        now=NOW,
        decision_run_uid=RUN_UID,
        strategy_version=STRATEGY,
        stock_code=STOCK,
        bound=restored,
        current=binding,
    )

    assert restored == binding
    assert decision == BuyGateDecision(True)


@pytest.mark.parametrize(
    ("current", "now", "reason"),
    (
        (_binding(chase_risk_status="BLOCK"), NOW, "BUY_GATE_REVOKED"),
        (_binding(event_risk_level="HIGH"), NOW, "BUY_GATE_MAJOR_EVENT"),
        (
            _binding(source_health_status="DATA_BLOCKED"),
            NOW,
            "BUY_GATE_DATA_BLOCKED",
        ),
        (
            _binding(valid_until=datetime(2026, 8, 5, 9, 59)),
            NOW,
            "BUY_GATE_EXPIRED",
        ),
        (
            _binding(context_hash="b" * 64),
            NOW,
            "BUY_CONTEXT_HASH_MISMATCH",
        ),
    ),
)
def test_current_gate_changes_fail_closed(current, now, reason):
    decision = evaluate_buy_gate(
        now=now,
        decision_run_uid=RUN_UID,
        strategy_version=STRATEGY,
        stock_code=STOCK,
        bound=_binding(),
        current=current,
    )

    assert decision.allowed is False
    assert decision.reason_code == reason


def test_gate_with_missing_recommendation_date_fails_closed():
    binding = _binding(recommendation_data_date="")
    decision = evaluate_buy_gate(
        now=NOW,
        decision_run_uid=RUN_UID,
        strategy_version=STRATEGY,
        stock_code=STOCK,
        bound=binding,
        current=binding,
    )

    assert decision.allowed is False
    assert decision.reason_code == "BUY_GATE_STALE_RECOMMENDATION"


def _engine_with_approved_buy(
    *,
    filled_quantity: int = 0,
    signal_valid_until: datetime = VALID_UNTIL,
):
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={
            "detect_types": sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        },
    )

    @event.listens_for(engine, "before_cursor_execute", retval=True)
    def _sqlite_for_update(
        _conn, _cursor, statement, parameters, _context, _executemany
    ):
        return statement.replace(" FOR UPDATE", ""), parameters

    allowed_binding = _binding(valid_until=signal_valid_until)
    intent_evidence = json.dumps(
        list(append_buy_gate_binding(tuple(), allowed_binding)),
        sort_keys=True,
    )
    raw_features = json.dumps(
        {
            "source_recommend_status": "ALLOW",
            "source_signal_status": "CONFIRM",
            "source_chase_risk_status": "ALLOW",
            "source_ordinary_buy_eligible": True,
            "risk_level": "LOW",
            "data_quality_score": 100,
        },
        sort_keys=True,
    )
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_trade_account_v2 (
                account_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                cash_balance NUMERIC NOT NULL,
                fee_profile_version TEXT,
                instrument_rule_version TEXT,
                real_trading_enabled INTEGER NOT NULL
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_trade_intent_v2 (
                intent_id TEXT PRIMARY KEY,
                decision_run_uid TEXT NOT NULL,
                strategy_version TEXT,
                action TEXT,
                theme_code TEXT,
                initial_stop NUMERIC,
                protective_stop NUMERIC,
                invalidation_condition TEXT,
                intent_version INTEGER,
                evidence_json TEXT NOT NULL
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_risk_decision_v2 (
                intent_id TEXT PRIMARY KEY,
                approved_quantity INTEGER NOT NULL
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_order_v2 (
                order_id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL,
                intent_id TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                side TEXT NOT NULL,
                order_type TEXT NOT NULL,
                limit_price NUMERIC NOT NULL,
                quantity INTEGER NOT NULL,
                filled_quantity INTEGER NOT NULL,
                status TEXT NOT NULL,
                waiting_reason TEXT,
                earliest_at TIMESTAMP NOT NULL,
                expires_at TIMESTAMP NOT NULL,
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_data_snapshot_v2 (
                snapshot_id TEXT PRIMARY KEY,
                quality_status TEXT NOT NULL,
                data_snapshot_hash TEXT NOT NULL
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_decision_run_v2 (
                run_uid TEXT PRIMARY KEY,
                snapshot_id TEXT NOT NULL,
                trade_date DATE NOT NULL,
                status TEXT NOT NULL
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_strategy_signal_v2 (
                run_uid TEXT NOT NULL,
                strategy_version TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                action TEXT NOT NULL,
                competition_status TEXT NOT NULL,
                rejection_code TEXT,
                raw_features_json TEXT NOT NULL,
                valid_from TIMESTAMP NOT NULL,
                valid_until TIMESTAMP NOT NULL,
                data_snapshot_hash TEXT NOT NULL
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_recommended_stocks (
                stock_code TEXT NOT NULL,
                pick_date DATE NOT NULL,
                recommend_status TEXT,
                signal_status TEXT,
                chase_risk_status TEXT,
                ordinary_buy_eligible INTEGER,
                event_risk_level TEXT,
                data_quality_score NUMERIC,
                data_quality_flags TEXT
            )
        """))
        connection.execute(
            text("""
                INSERT INTO st_trade_account_v2 VALUES
                ('paper-main-v2', 'ACTIVE', 200000, 'fees-v1', 'rules-v1', 0)
            """)
        )
        connection.execute(
            text("""
                INSERT INTO st_trade_intent_v2 VALUES
                ('intent-1', :run_uid, :strategy, 'OPEN', 'theme-1',
                 9, 9, 'stop', 1, :evidence)
            """),
            {
                "run_uid": RUN_UID,
                "strategy": STRATEGY,
                "evidence": intent_evidence,
            },
        )
        connection.execute(
            text("INSERT INTO st_risk_decision_v2 VALUES ('intent-1', 100)")
        )
        connection.execute(
            text("""
                INSERT INTO st_order_v2 VALUES
                ('order-1', 'paper-main-v2', 'intent-1', :stock, 'BUY',
                 'LIMIT', 10.10, 100, :filled, :status, NULL,
                 '2026-08-05 09:31:00', '2026-08-05 15:00:00',
                 '2026-08-04 15:20:00', '2026-08-04 15:20:00')
            """),
            {
                "stock": STOCK,
                "filled": filled_quantity,
                "status": (
                    "PARTIALLY_FILLED"
                    if filled_quantity
                    else "RISK_APPROVED"
                ),
            },
        )
        connection.execute(
            text(
                "INSERT INTO st_data_snapshot_v2 VALUES "
                "('snapshot-1', 'PASS', :context_hash)"
            ),
            {"context_hash": CONTEXT_HASH},
        )
        connection.execute(
            text(
                "INSERT INTO st_decision_run_v2 VALUES "
                "(:run_uid, 'snapshot-1', '2026-08-04', 'COMPLETED')"
            ),
            {"run_uid": RUN_UID},
        )
        connection.execute(
            text("""
                INSERT INTO st_strategy_signal_v2 VALUES
                (:run_uid, :strategy, :stock, 'BUY', 'ELIGIBLE', NULL,
                 :raw_features, '2026-08-04 15:20:00', :valid_until,
                 :context_hash)
            """),
            {
                "run_uid": RUN_UID,
                "strategy": STRATEGY,
                "stock": STOCK,
                "raw_features": raw_features,
                "valid_until": signal_valid_until,
                "context_hash": CONTEXT_HASH,
            },
        )
        connection.execute(
            text("""
                INSERT INTO st_recommended_stocks VALUES
                (:stock, '2026-08-04', 'ALLOW', 'CONFIRM',
                 'ALLOW', 1, 'LOW', 100, '[]')
            """),
            {"stock": STOCK},
        )
    return engine


def _rule():
    return {
        "permission_confirmed": 1,
        "fee_profile_version": "fees-v1",
        "security_type": "A_SHARE",
        "tick_size": Decimal("0.01"),
        "limit_ratio": Decimal("0.10"),
        "suspended": 0,
    }


@pytest.mark.parametrize(
    ("mutation", "valid_until", "filled", "expected_reason"),
    (
        (
            "UPDATE st_recommended_stocks SET chase_risk_status='BLOCK'",
            VALID_UNTIL,
            40,
            "BUY_GATE_REVOKED",
        ),
        (
            "UPDATE st_recommended_stocks SET event_risk_level='HIGH'",
            VALID_UNTIL,
            0,
            "BUY_GATE_MAJOR_EVENT",
        ),
        (
            "UPDATE st_recommended_stocks SET data_quality_score=0",
            VALID_UNTIL,
            0,
            "BUY_GATE_DATA_BLOCKED",
        ),
        (
            "SELECT 1",
            datetime(2026, 8, 5, 9, 59),
            0,
            "BUY_GATE_EXPIRED",
        ),
    ),
)
def test_approved_buy_cancels_unfilled_remainder_when_gate_changes(
    monkeypatch,
    mutation,
    valid_until,
    filled,
    expected_reason,
):
    engine = _engine_with_approved_buy(
        filled_quantity=filled,
        signal_valid_until=valid_until,
    )
    with engine.begin() as connection:
        connection.execute(text(mutation))
    monkeypatch.setattr(execution_module, "_rule", lambda *_a, **_k: _rule())
    monkeypatch.setattr(
        execution_module, "_fee_profile", lambda *_a, **_k: object()
    )
    monkeypatch.setattr(execution_module, "_record_event", lambda *_a, **_k: None)

    result = execution_module._execute_one(
        engine,
        order_id="order-1",
        account_id="paper-main-v2",
        now=NOW,
    )

    assert result["status"] == "CANCELLED"
    assert result["waiting_reason"] == expected_reason
    assert result["partial_cancelled"] is (filled > 0)
    with engine.connect() as connection:
        stored = connection.execute(
            text(
                "SELECT status, waiting_reason, filled_quantity "
                "FROM st_order_v2 WHERE order_id='order-1'"
            )
        ).one()
    assert stored == ("CANCELLED", expected_reason, filled)


def test_buy_gate_is_checked_again_after_matching_before_fill(monkeypatch):
    engine = _engine_with_approved_buy()
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_quote_event_v2 (
                quote_event_id TEXT PRIMARY KEY,
                pre_close NUMERIC
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_fill_v2 (
                order_id TEXT,
                gross_amount NUMERIC,
                quantity INTEGER
            )
        """))
        connection.execute(
            text("INSERT INTO st_quote_event_v2 VALUES ('quote-1', 10)")
        )
    decisions = [
        BuyGateDecision(True),
        BuyGateDecision(False, "BUY_GATE_REVOKED", "revoked after match"),
    ]
    calls = []

    def _gate(*_args, **_kwargs):
        calls.append(1)
        return decisions.pop(0)

    profile = FeeProfile(
        version="fees-v1",
        buy_commission_rate=Decimal("0"),
        sell_commission_rate=Decimal("0"),
        minimum_commission=Decimal("0"),
        stamp_tax_sell_rate=Decimal("0"),
        transfer_fee_buy_rate=Decimal("0"),
        transfer_fee_sell_rate=Decimal("0"),
    )
    quote = Quote(
        stock_code=STOCK,
        event_id="quote-1",
        quote_at=NOW,
        received_at=NOW,
        bid1=Decimal("9.99"),
        bid1_volume=10000,
        ask1=Decimal("10.00"),
        ask1_volume=10000,
        last_price=Decimal("10.00"),
        upper_limit=Decimal("11.00"),
        lower_limit=Decimal("9.00"),
    )
    monkeypatch.setattr(execution_module, "_rule", lambda *_a, **_k: _rule())
    monkeypatch.setattr(
        execution_module, "_fee_profile", lambda *_a, **_k: profile
    )
    monkeypatch.setattr(execution_module, "latest_quote", lambda *_a, **_k: quote)
    monkeypatch.setattr(
        execution_module, "_sector_entry_wait_reason", lambda *_a, **_k: ""
    )
    monkeypatch.setattr(
        execution_module, "_entry_trend_wait_reason", lambda *_a, **_k: ""
    )
    monkeypatch.setattr(execution_module, "_execution_buy_gate_decision", _gate)
    monkeypatch.setattr(execution_module, "_record_event", lambda *_a, **_k: None)

    result = execution_module._execute_one(
        engine,
        order_id="order-1",
        account_id="paper-main-v2",
        now=NOW,
    )

    assert len(calls) == 2
    assert result["status"] == "CANCELLED"
    assert result["waiting_reason"] == "BUY_GATE_REVOKED"
    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT COUNT(*) FROM st_fill_v2")
        ).scalar_one() == 0


def test_sell_is_exempt_without_reading_buy_gate_evidence():
    class _NoDatabaseAccess:
        @property
        def dialect(self):
            raise AssertionError("SELL attempted to inspect BUY gate data")

        def execute(self, *_args, **_kwargs):
            raise AssertionError("SELL attempted to read BUY gate data")

    decision = execution_module._execution_buy_gate_decision(
        _NoDatabaseAccess(),
        order={"side": "SELL"},
        now=NOW,
    )

    assert decision == BuyGateDecision(True)


def test_v3_nested_receipt_is_supported_but_must_be_unique_and_complete():
    binding = _binding()

    assert bound_buy_gate({GATE_MODULE: binding}) == binding
    assert bound_buy_gate({GATE_MODULE: {"module": GATE_MODULE}}) is None
    duplicate_json = json.dumps({GATE_MODULE: binding})[:-1] + (
        "," + json.dumps(GATE_MODULE) + ":" + json.dumps(binding) + "}"
    )
    assert bound_buy_gate(duplicate_json) is None


def test_hold_signal_cannot_supply_source_pass_for_a_new_buy():
    engine = _engine_with_approved_buy()
    with engine.begin() as connection:
        connection.execute(
            text("UPDATE st_strategy_signal_v2 SET action='HOLD'")
        )
        loaded = load_current_buy_gate(
            connection,
            decision_run_uid=RUN_UID,
            strategy_version=STRATEGY,
            stock_code=STOCK,
            as_of=NOW,
            lock=False,
        )

    assert loaded.binding is not None
    assert loaded.binding["source_health_status"] == "DATA_BLOCKED"
    decision = evaluate_buy_gate(
        now=NOW,
        decision_run_uid=RUN_UID,
        strategy_version=STRATEGY,
        stock_code=STOCK,
        bound=loaded.binding,
        current=loaded.binding,
    )
    assert decision.allowed is False
    assert decision.reason_code == "BUY_GATE_DATA_BLOCKED"


def test_old_allow_recommendation_cannot_revive_a_fresh_buy_signal():
    engine = _engine_with_approved_buy()
    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE st_recommended_stocks "
                "SET pick_date='2026-08-03'"
            )
        )
        loaded = load_current_buy_gate(
            connection,
            decision_run_uid=RUN_UID,
            strategy_version=STRATEGY,
            stock_code=STOCK,
            as_of=NOW,
            lock=False,
        )

    assert loaded.binding is None
    assert loaded.reason_code == "BUY_GATE_STALE_RECOMMENDATION"


def test_newer_recommendation_requires_a_new_matching_decision_run():
    engine = _engine_with_approved_buy()
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO st_recommended_stocks VALUES
            (:stock, '2026-08-05', 'ALLOW', 'CONFIRM',
             'ALLOW', 1, 'LOW', 100, '[]')
        """), {"stock": STOCK})
        loaded = load_current_buy_gate(
            connection,
            decision_run_uid=RUN_UID,
            strategy_version=STRATEGY,
            stock_code=STOCK,
            as_of=NOW,
            lock=False,
        )

    assert loaded.binding is None
    assert loaded.reason_code == "BUY_GATE_STALE_RECOMMENDATION"
