from __future__ import annotations

from datetime import datetime
from decimal import Decimal
import hashlib
import json

import pytest
from sqlalchemy import create_engine, text

from server.trading_v3.forward_evidence import (
    EXECUTED_FORWARD_PROTOCOL,
    reconstruct_executed_forward_records,
)
from server.trading_v3.repository import TradingV3Repository


def _fill(
    *,
    fill_id: str,
    order_id: str,
    side: str,
    quantity: int,
    price: str,
    fee: str,
    filled_at: datetime,
    reason: str,
    evidence: str = "{}",
    decision_run_uid: str = "run-1",
) -> dict:
    gross = Decimal(price) * quantity
    return {
        "fill_id": fill_id,
        "intent_id": f"intent-{order_id}",
        "order_id": order_id,
        "account_id": "paper-main-v2",
        "stock_code": "002380",
        "side": side,
        "quantity": quantity,
        "price": Decimal(price),
        "gross_amount": gross,
        "fee_amount": Decimal(fee),
        "filled_at": filled_at,
        "decision_run_uid": decision_run_uid,
        "intent_reason_code": reason,
        "evidence_json": evidence,
    }


def test_forward_evidence_uses_actual_fills_fees_and_fifo_round_trip():
    rows = [
        _fill(
            fill_id="buy-1",
            order_id="order-buy",
            side="BUY",
            quantity=100,
            price="27.78",
            fee="5.00",
            filled_at=datetime(2026, 8, 3, 9, 31),
            reason="V3_PAPER_DISCOVERY",
            evidence=(
                '{"run_uid":"run-1","signal_strategy_keys":'
                '["oversold_reversal"]}'
            ),
        ),
        _fill(
            fill_id="sell-1",
            order_id="order-sell",
            side="SELL",
            quantity=100,
            price="29.00",
            fee="6.45",
            filled_at=datetime(2026, 8, 6, 9, 31),
            reason="PAPER_DISCOVERY_SIGNAL_ENDED",
        ),
    ]
    result = reconstruct_executed_forward_records(
        rows,
        forecast_ids={
            ("run-1", "002380", "oversold_reversal"): "forecast-1"
        },
    )
    assert len(result) == 1
    trade = result[0]
    assert trade["protocol_version"] == EXECUTED_FORWARD_PROTOCOL
    assert trade["source_forecast_id"] == "forecast-1"
    assert trade["entry_quantity"] == 100
    assert trade["closed_quantity"] == 100
    assert trade["evidence_status"] == "MATURED"
    assert trade["realized_net_pnl_cny"] == Decimal("110.55")
    assert float(trade["realized_net_return_pct"]) == pytest.approx(
        110.55 / 2783 * 100
    )


def test_forward_evidence_does_not_invent_unfilled_or_cancelled_samples():
    # The function consumes immutable fill events, not targets or orders.  With
    # no fill event a queued/cancelled decision cannot become forward evidence.
    assert reconstruct_executed_forward_records([]) == []


def test_forward_evidence_keeps_partial_close_open_until_fully_sold():
    rows = [
        _fill(
            fill_id="buy-1",
            order_id="order-buy",
            side="BUY",
            quantity=100,
            price="10.00",
            fee="5.00",
            filled_at=datetime(2026, 8, 3, 9, 31),
            reason="V3_PAPER_DISCOVERY",
            evidence=(
                '{"run_uid":"run-1","signal_strategy_keys":'
                '["oversold_reversal"]}'
            ),
        ),
        _fill(
            fill_id="sell-1",
            order_id="order-sell",
            side="SELL",
            quantity=40,
            price="10.50",
            fee="5.00",
            filled_at=datetime(2026, 8, 5, 9, 31),
            reason="PAPER_DISCOVERY_SIGNAL_ENDED",
        ),
    ]
    trade = reconstruct_executed_forward_records(
        rows,
        forecast_ids={
            ("run-1", "002380", "oversold_reversal"): "forecast-1"
        },
    )[0]
    assert trade["closed_quantity"] == 40
    assert trade["evidence_status"] == "PARTIALLY_CLOSED"
    assert trade["realized_net_return_pct"] is not None


def test_forward_evidence_uses_intent_run_not_canonical_close_run():
    row = _fill(
        fill_id="manual-buy",
        order_id="manual-order",
        side="BUY",
        quantity=100,
        price="10.00",
        fee="5.00",
        filled_at=datetime(2026, 8, 3, 9, 31),
        reason="V3_PAPER_DISCOVERY",
        evidence=(
            '{"run_uid":"manual-latest","signal_strategy_keys":'
            '["oversold_reversal"]}'
        ),
        decision_run_uid="manual-latest",
    )
    trade = reconstruct_executed_forward_records(
        [row],
        forecast_ids={
            (
                "manual-latest",
                "002380",
                "oversold_reversal",
            ): "forecast-manual"
        },
    )[0]
    assert trade["source_run_uid"] == "manual-latest"
    assert trade["evidence_status"] == "OPEN"


def _snapshot_evidence(
    *,
    run_uid: str = "run-1",
    forecast_id: str = "forecast-primary",
    strategy_key: str = "oversold_reversal",
    supporting: tuple[str, ...] = (
        "oversold_reversal",
        "theme_diffusion",
    ),
) -> str:
    ownership_hash = hashlib.sha256(
        (
            f"{run_uid}|{forecast_id}|002380|{strategy_key}"
        ).encode("utf-8")
    ).hexdigest()
    return json.dumps({
        "run_uid": run_uid,
        "primary_strategy_key": strategy_key,
        "primary_forecast_id": forecast_id,
        "supporting_strategy_keys": list(supporting),
        "sample_owner_role": "PRIMARY",
        "attribution_version": "V3_PRIMARY_FORECAST_SNAPSHOT_V1",
        "ownership_hash": ownership_hash,
    })


def test_multi_strategy_fill_has_exactly_one_frozen_sample_owner():
    row = _fill(
        fill_id="owned-buy",
        order_id="owned-order",
        side="BUY",
        quantity=100,
        price="10.00",
        fee="5.00",
        filled_at=datetime(2026, 8, 3, 9, 31),
        reason="V3_PAPER_DISCOVERY",
        evidence=_snapshot_evidence(),
    )
    result = reconstruct_executed_forward_records(
        [row],
        forecast_ids={
            (
                "run-1",
                "002380",
                "oversold_reversal",
            ): "forecast-primary",
            (
                "run-1",
                "002380",
                "theme_diffusion",
            ): "forecast-support",
        },
    )

    assert len(result) == 1
    assert result[0]["strategy_key"] == "oversold_reversal"
    assert result[0]["source_forecast_id"] == "forecast-primary"
    assert result[0]["sample_owner_role"] == "PRIMARY"
    assert result[0]["attribution_status"] == "VERIFIED_SNAPSHOT"
    assert json.loads(result[0]["supporting_strategy_keys_json"]) == [
        "oversold_reversal",
        "theme_diffusion",
    ]


def test_legacy_multi_strategy_fill_is_quarantined_not_duplicated():
    diagnostics: dict[str, int] = {}
    row = _fill(
        fill_id="ambiguous-buy",
        order_id="ambiguous-order",
        side="BUY",
        quantity=100,
        price="10.00",
        fee="5.00",
        filled_at=datetime(2026, 8, 3, 9, 31),
        reason="V3_PAPER_DISCOVERY",
        evidence=(
            '{"run_uid":"run-1","signal_strategy_keys":'
            '["oversold_reversal","theme_diffusion"]}'
        ),
    )
    result = reconstruct_executed_forward_records(
        [row],
        forecast_ids={
            ("run-1", "002380", "oversold_reversal"): "forecast-1",
            ("run-1", "002380", "theme_diffusion"): "forecast-2",
        },
        diagnostics=diagnostics,
    )

    assert result == []
    assert diagnostics == {"LEGACY_OWNER_AMBIGUOUS": 1}


def test_payload_run_mismatch_cannot_steal_sample_from_relational_run():
    diagnostics: dict[str, int] = {}
    row = _fill(
        fill_id="wrong-run-buy",
        order_id="wrong-run-order",
        side="BUY",
        quantity=100,
        price="10.00",
        fee="5.00",
        filled_at=datetime(2026, 8, 3, 9, 31),
        reason="V3_PAPER_DISCOVERY",
        evidence=_snapshot_evidence(run_uid="payload-run"),
        decision_run_uid="relational-run",
    )

    assert reconstruct_executed_forward_records(
        [row],
        forecast_ids={
            (
                "relational-run",
                "002380",
                "oversold_reversal",
            ): "forecast-primary"
        },
        diagnostics=diagnostics,
    ) == []
    assert diagnostics == {"RUN_UID_MISMATCH": 1}


def test_fifo_exit_keeps_each_entry_fill_with_its_frozen_owner():
    rows = [
        _fill(
            fill_id="owner-a-buy",
            order_id="owner-a-order",
            side="BUY",
            quantity=100,
            price="10.00",
            fee="5.00",
            filled_at=datetime(2026, 8, 3, 9, 31),
            reason="V3_PAPER_DISCOVERY",
            evidence=_snapshot_evidence(
                run_uid="run-a",
                forecast_id="forecast-a",
                strategy_key="oversold_reversal",
                supporting=("oversold_reversal",),
            ),
            decision_run_uid="run-a",
        ),
        _fill(
            fill_id="owner-b-buy",
            order_id="owner-b-order",
            side="BUY",
            quantity=100,
            price="12.00",
            fee="5.00",
            filled_at=datetime(2026, 8, 4, 9, 31),
            reason="V3_PAPER_DISCOVERY",
            evidence=_snapshot_evidence(
                run_uid="run-b",
                forecast_id="forecast-b",
                strategy_key="theme_diffusion",
                supporting=("theme_diffusion",),
            ),
            decision_run_uid="run-b",
        ),
        _fill(
            fill_id="shared-sell",
            order_id="sell-order",
            side="SELL",
            quantity=150,
            price="14.00",
            fee="8.00",
            filled_at=datetime(2026, 8, 6, 9, 31),
            reason="PAPER_DISCOVERY_SIGNAL_ENDED",
        ),
    ]
    result = reconstruct_executed_forward_records(
        rows,
        forecast_ids={
            ("run-a", "002380", "oversold_reversal"): "forecast-a",
            ("run-b", "002380", "theme_diffusion"): "forecast-b",
        },
    )
    by_owner = {item["strategy_key"]: item for item in result}

    assert by_owner["oversold_reversal"]["closed_quantity"] == 100
    assert by_owner["oversold_reversal"]["evidence_status"] == "MATURED"
    assert by_owner["oversold_reversal"]["source_run_uid"] == "run-a"
    assert by_owner["theme_diffusion"]["closed_quantity"] == 50
    assert (
        by_owner["theme_diffusion"]["evidence_status"]
        == "PARTIALLY_CLOSED"
    )
    assert by_owner["theme_diffusion"]["source_run_uid"] == "run-b"


def test_learning_summary_cannot_promote_profitable_shadow_results():
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_forward_trade_evidence_v3 (
                source_forecast_id TEXT,
                source_run_uid TEXT,
                stock_code TEXT,
                strategy_key TEXT,
                evidence_kind TEXT,
                protocol_version TEXT,
                sample_owner_role TEXT,
                attribution_status TEXT,
                evidence_status TEXT,
                exit_reason TEXT,
                realized_net_return_pct REAL,
                realized_mae_pct REAL,
                realized_mfe_pct REAL,
                entry_at TEXT,
                exit_at TEXT,
                updated_at TEXT
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_alpha_forecast_v3 (
                forecast_id TEXT PRIMARY KEY,
                run_uid TEXT,
                stock_code TEXT,
                strategy_key TEXT,
                features_json TEXT
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_counterfactual_v3 (
                strategy_key TEXT,
                evidence_kind TEXT,
                accepted INTEGER,
                realized_net_return_pct REAL,
                missed_opportunity INTEGER,
                false_positive INTEGER
            )
        """))
        connection.execute(text("""
            INSERT INTO st_alpha_forecast_v3 VALUES
                ('actual-1', 'run-1', '002380', 'oversold_reversal',
                 '{"return_20d_pct": 1.0}'),
                ('actual-2', 'run-2', '300001', 'oversold_reversal',
                 '{}')
        """))
        connection.execute(text("""
            INSERT INTO st_forward_trade_evidence_v3 VALUES
                ('actual-1', 'run-1', '002380', 'oversold_reversal',
                 'EXECUTED_PAPER', 'PAPER_EXECUTED_LEDGER_V1',
                 'PRIMARY', 'VERIFIED_SNAPSHOT',
                 'MATURED', 'SIGNAL_ENDED',
                 1.25, -0.8, 2.1, '2026-08-03 09:31:00',
                 '2026-08-06 09:31:00', '2026-08-06 09:31:00'),
                ('actual-2', 'run-2', '300001', 'oversold_reversal',
                 'EXECUTED_PAPER', 'PAPER_EXECUTED_LEDGER_V1',
                 'PRIMARY', 'VERIFIED_SNAPSHOT', 'OPEN', NULL,
                 NULL, NULL, NULL, '2026-08-07 09:31:00',
                 NULL, '2026-08-07 09:31:00')
        """))
        connection.execute(text("""
            INSERT INTO st_counterfactual_v3 VALUES
                ('oversold_reversal', 'SHADOW', 1, 99.0, 7, 3)
        """))

    result = TradingV3Repository(engine).strategy_learning_summary(
        "oversold_reversal"
    )

    assert result["observed_count"] == 2
    assert result["accepted_count"] == 1
    assert result["average_net_return_pct"] == pytest.approx(1.25)
    assert result["shadow_observed_count"] == 1
    assert result["missed_opportunity_count"] == 7
    assert result["false_positive_count"] == 3
    assert result["evidence_source"] == "EXECUTED_PAPER_FILLS_ONLY"
    assert result["shadow_can_activate_model"] is False


def test_latest_recall_aggregates_strategy_attribution_without_collision():
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_opportunity_recall_v3 (
                recall_id TEXT PRIMARY KEY,
                trade_date TEXT,
                horizon_days INTEGER,
                strategy_key TEXT,
                evidence_kind TEXT,
                protocol_version TEXT,
                winner_threshold_pct REAL,
                winner_count INTEGER,
                accepted_winner_count INTEGER,
                missed_winner_count INTEGER,
                recall_at_20 REAL,
                recall_at_50 REAL,
                accepted_average_net_return_pct REAL,
                missed_reason_json TEXT,
                created_at TEXT
            )
        """))
        connection.execute(text("""
            INSERT INTO st_opportunity_recall_v3 VALUES
                ('r1', '2026-07-31', 5, 'oversold_reversal', 'SHADOW',
                 'COUNTERFACTUAL_TECHNICAL_PROXY_V2', 3.0,
                 2, 1, 1, 0.5, 1.0, 2.0, '{"FILTER_A": 1}',
                 '2026-08-01 12:00:00'),
                ('r2', '2026-07-31', 5, 'theme_diffusion', 'SHADOW',
                 'COUNTERFACTUAL_TECHNICAL_PROXY_V2', 3.0,
                 3, 2, 1, 1.0, 1.0, 4.0,
                 '{"FILTER_A": 1, "FILTER_B": 1}',
                 '2026-08-01 12:00:01')
        """))

    repository = TradingV3Repository(engine)
    aggregate = repository.latest_opportunity_recall()
    sleeve = repository.latest_opportunity_recall("oversold_reversal")

    assert aggregate is not None
    assert aggregate["strategy_key"] == "ALL_STRATEGIES"
    assert aggregate["strategy_count"] == 2
    assert aggregate["winner_count"] == 5
    assert aggregate["missed_winner_count"] == 2
    assert aggregate["recall_at_20"] == pytest.approx(0.8)
    assert aggregate["missed_reason_counts"] == {
        "FILTER_A": 2,
        "FILTER_B": 1,
    }
    assert aggregate["evidence_kind"] == "SHADOW"
    assert sleeve is not None
    assert sleeve["strategy_key"] == "oversold_reversal"
    assert sleeve["winner_count"] == 2
