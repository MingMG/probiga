import inspect
from datetime import date, datetime

import pandas as pd

from sqlalchemy import create_engine, text

from server.trading_v3.audit import (
    build_counterfactual_records,
    opportunity_recall,
)
from server.trading_v3.counterfactual_worker import _outcomes, _pending_forecasts
from server.trading_v3.positions import decide_position_transition


def test_trend_failure_exits_without_fixed_holding_days():
    result = decide_position_transition(
        stock_code="002326",
        previous_state="HOLD_TREND",
        current_quantity=1000,
        sellable_quantity=1000,
        current_weight=0.10,
        target_weight=0.08,
        entry_date=date(2026, 7, 24),
        trade_date=date(2026, 7, 28),
        trend_valid=False,
        hard_stop_triggered=False,
        forecast_status="VALIDATED_POSITIVE",
        forecast_improving=False,
        add_count=0,
    )
    assert result.next_state == "EXIT"
    assert result.action == "SELL_ALL"


def test_t1_preserves_exit_intent_instead_of_crashing():
    result = decide_position_transition(
        stock_code="002326",
        previous_state="PROBE",
        current_quantity=1000,
        sellable_quantity=0,
        current_weight=0.05,
        target_weight=0.0,
        entry_date=date(2026, 7, 28),
        trade_date=date(2026, 7, 28),
        trend_valid=False,
        hard_stop_triggered=False,
        forecast_status="RESEARCH_ONLY_PROFIT_GATE_FAILED",
        forecast_improving=False,
        add_count=0,
    )
    assert result.next_state == "EXIT_PENDING_T1"
    assert result.action == "WAIT_SELLABLE"


def test_validated_improving_position_can_add_once():
    result = decide_position_transition(
        stock_code="002326",
        previous_state="CONFIRMED",
        current_quantity=600,
        sellable_quantity=600,
        current_weight=0.06,
        target_weight=0.10,
        entry_date=date(2026, 7, 24),
        trade_date=date(2026, 7, 28),
        trend_valid=True,
        hard_stop_triggered=False,
        forecast_status="VALIDATED_POSITIVE",
        forecast_improving=True,
        add_count=0,
    )
    assert result.next_state == "ADD_ALLOWED"
    assert result.add_count == 1


def test_paper_discovery_position_holds_only_while_signal_remains_active():
    active = decide_position_transition(
        stock_code="002326",
        previous_state="PAPER_DISCOVERY",
        current_quantity=300,
        sellable_quantity=300,
        current_weight=0.03,
        target_weight=0.03,
        entry_date=date(2026, 7, 28),
        trade_date=date(2026, 7, 29),
        trend_valid=True,
        hard_stop_triggered=False,
        forecast_status="PAPER_DISCOVERY_ACTIVE",
        forecast_improving=False,
        add_count=0,
    )
    assert active.action == "HOLD"
    assert active.next_state == "PAPER_DISCOVERY"
    lost = decide_position_transition(
        stock_code="002326",
        previous_state="PAPER_DISCOVERY",
        current_quantity=300,
        sellable_quantity=300,
        current_weight=0.03,
        target_weight=0.0,
        entry_date=date(2026, 7, 28),
        trade_date=date(2026, 7, 29),
        trend_valid=True,
        hard_stop_triggered=False,
        forecast_status="RESEARCH_ONLY_PROFIT_GATE_FAILED",
        forecast_improving=False,
        add_count=0,
    )
    assert lost.action == "SELL_ALL"


def test_paper_discovery_position_does_not_exit_on_unevaluated_signal():
    result = decide_position_transition(
        stock_code="002326",
        previous_state="PAPER_DISCOVERY",
        current_quantity=300,
        sellable_quantity=300,
        current_weight=0.03,
        target_weight=0.0,
        entry_date=date(2026, 7, 28),
        trade_date=date(2026, 7, 29),
        trend_valid=True,
        hard_stop_triggered=False,
        forecast_status="INSUFFICIENT_DATA",
        forecast_improving=False,
        add_count=0,
        signal_evaluation_valid=False,
    )
    assert result.action == "HOLD"
    assert result.next_state == "PAPER_DISCOVERY"
    assert result.reason_code == "PAPER_DISCOVERY_SIGNAL_UNEVALUATED"


def test_hard_stop_still_exits_paper_position_when_signal_is_unevaluated():
    result = decide_position_transition(
        stock_code="002326",
        previous_state="PAPER_DISCOVERY",
        current_quantity=300,
        sellable_quantity=300,
        current_weight=0.03,
        target_weight=0.0,
        entry_date=date(2026, 7, 28),
        trade_date=date(2026, 7, 29),
        trend_valid=False,
        hard_stop_triggered=True,
        forecast_status="INSUFFICIENT_DATA",
        forecast_improving=False,
        add_count=0,
        signal_evaluation_valid=False,
    )
    assert result.action == "SELL_ALL"
    assert result.reason_code == "HARD_STOP_TRIGGERED"


def test_hypothesis_invalidation_keeps_its_own_exit_reason_code():
    result = decide_position_transition(
        stock_code="002326",
        previous_state="HOLD_TREND",
        current_quantity=300,
        sellable_quantity=300,
        current_weight=0.08,
        target_weight=0.0,
        entry_date=date(2026, 7, 20),
        trade_date=date(2026, 7, 29),
        trend_valid=False,
        hard_stop_triggered=False,
        forecast_status="VALIDATED_POSITIVE",
        forecast_improving=False,
        add_count=0,
        explicit_exit_reason="HYPOTHESIS_INVALIDATED",
    )
    assert result.action == "SELL_ALL"
    assert result.reason_code == "HYPOTHESIS_INVALIDATED"


def test_normal_position_holds_when_trend_remains_valid():
    result = decide_position_transition(
        stock_code="002326",
        previous_state="HOLD_TREND",
        current_quantity=300,
        sellable_quantity=300,
        current_weight=0.08,
        target_weight=0.0,
        entry_date=date(2026, 7, 20),
        trade_date=date(2026, 7, 29),
        trend_valid=True,
        hard_stop_triggered=False,
        forecast_status="RESEARCH_ONLY_PROFIT_GATE_FAILED",
        forecast_improving=False,
        add_count=0,
    )
    assert result.action == "HOLD"
    assert result.reason_code == "EDGE_UNCONFIRMED_TREND_VALID"


def test_counterfactual_ledger_marks_missed_winner():
    records = build_counterfactual_records(
        [{
            "stock_code": "600001",
            "strategy_key": "right_side_trend",
            "rank": 10,
            "accepted": False,
            "reason_code": "NET_EDGE_BELOW_COST_BUFFER",
            "return_q10_pct": -2,
        }],
        {"600001": {"net_return_pct": 6, "mae_pct": -1, "mfe_pct": 8}},
    )
    assert records[0]["missed_opportunity"] is True
    assert records[0]["attribution"].startswith("MISSED_BY_")


def test_opportunity_recall_reports_top_k_and_rejection_causes():
    rows = [
        {
            "stock_code": f"{index:06d}",
            "rank": index,
            "accepted": index == 1,
            "reason_code": "LOW_EDGE",
        }
        for index in range(1, 61)
    ]
    outcomes = {
        "000001": {"net_return_pct": 5},
        "000025": {"net_return_pct": 7},
        "000055": {"net_return_pct": 9},
    }
    result = opportunity_recall(rows, outcomes)
    assert result["recall_at_20"] == 1 / 3
    assert result["recall_at_50"] == 2 / 3
    assert result["missed_reason_counts"]["LOW_EDGE"] == 2


def test_counterfactual_uses_one_canonical_run_per_trade_date():
    assert "ROW_NUMBER" not in inspect.getsource(_pending_forecasts)
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text(
            """
            CREATE TABLE st_decision_run_v3 (
                run_uid TEXT PRIMARY KEY,
                trade_date DATE NOT NULL,
                mode TEXT NOT NULL,
                decision_at DATETIME NOT NULL,
                status TEXT NOT NULL,
                portfolio_json TEXT
            )
            """
        ))
        connection.execute(text(
            """
            CREATE TABLE st_alpha_forecast_v3 (
                forecast_id TEXT PRIMARY KEY,
                run_uid TEXT NOT NULL,
                trade_date DATE NOT NULL,
                rank_no INTEGER NOT NULL,
                stock_code TEXT NOT NULL,
                short_name TEXT,
                strategy_key TEXT NOT NULL,
                horizon_days INTEGER NOT NULL,
                expected_return_net_pct REAL,
                return_q10_pct REAL,
                forecast_status TEXT,
                initial_stop_pct REAL NOT NULL DEFAULT -5,
                valid_until DATETIME NOT NULL DEFAULT '2020-02-01 15:00:00'
            )
            """
        ))
        connection.execute(text(
            """
            CREATE TABLE st_target_portfolio_v3 (
                target_id TEXT PRIMARY KEY,
                run_uid TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                strategy_keys_json TEXT NOT NULL DEFAULT '[]'
            )
            """
        ))
        connection.execute(text(
            """
            CREATE TABLE st_counterfactual_v3 (
                counterfactual_id TEXT PRIMARY KEY,
                source_forecast_id TEXT NOT NULL
            )
            """
        ))
        connection.execute(text(
            """
            CREATE TABLE st_counterfactual_queue_v3 (
                forecast_id TEXT PRIMARY KEY,
                next_retry_at DATETIME NOT NULL
            )
            """
        ))
        connection.execute(
            text(
                """
                INSERT INTO st_decision_run_v3
                    (run_uid, trade_date, mode, decision_at, status,
                     portfolio_json)
                VALUES
                    ('manual-newer', '2020-01-02', 'manual',
                     '2020-01-02 16:00:00', 'COMPLETED', '{}'),
                    ('close-canonical', '2020-01-02', 'close',
                     '2020-01-02 15:05:00', 'COMPLETED', '{}'),
                    ('failed-close', '2020-01-03', 'close',
                     '2020-01-03 15:05:00', 'FAILED', '{}'),
                    ('premarket-fallback', '2020-01-03', 'premarket',
                     '2020-01-03 09:00:00', 'COMPLETED', '{}')
                """
            )
        )
        for forecast_id, run_uid, trade_date in (
            ("f-manual", "manual-newer", "2020-01-02"),
            ("f-close", "close-canonical", "2020-01-02"),
            ("f-failed", "failed-close", "2020-01-03"),
            ("f-premarket", "premarket-fallback", "2020-01-03"),
        ):
            connection.execute(
                text(
                    """
                    INSERT INTO st_alpha_forecast_v3 (
                        forecast_id, run_uid, trade_date, rank_no,
                        stock_code, short_name, strategy_key,
                        horizon_days, expected_return_net_pct,
                        return_q10_pct, forecast_status
                    ) VALUES (
                        :forecast_id, :run_uid, :trade_date, 1,
                        '002326', '永太科技', 'right_side_trend',
                        10, 2.0, -1.0, 'VALIDATED_POSITIVE'
                    )
                    """
                ),
                {
                    "forecast_id": forecast_id,
                    "run_uid": run_uid,
                    "trade_date": trade_date,
                },
            )
    rows = _pending_forecasts(engine, limit=100)
    assert {row["forecast_id"] for row in rows} == {
        "f-close",
        "f-premarket",
    }
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO st_target_portfolio_v3 (
                target_id, run_uid, stock_code, strategy_keys_json
            ) VALUES (
                'target-1', 'close-canonical', '002326',
                '["oversold_reversal", "paper_discovery"]'
            )
        """))
        connection.execute(text("""
            INSERT INTO st_alpha_forecast_v3 (
                forecast_id, run_uid, trade_date, rank_no,
                stock_code, short_name, strategy_key,
                horizon_days, expected_return_net_pct,
                return_q10_pct, forecast_status
            ) VALUES (
                'f-oversold', 'close-canonical', '2020-01-02', 2,
                '002326', 'test', 'oversold_reversal',
                5, NULL, NULL, 'PAPER_DISCOVERY_CANDIDATE'
            )
        """))
    attributed = {
        row["forecast_id"]: row["accepted"]
        for row in _pending_forecasts(engine, limit=100)
    }
    assert attributed["f-close"] == 0
    assert attributed["f-oversold"] == 1

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO st_counterfactual_queue_v3 (
                    forecast_id, next_retry_at
                ) VALUES (
                    'f-close', '2020-02-02 16:30:00'
                )
                """
            )
        )
    same_day_rows = _pending_forecasts(
        engine,
        limit=100,
        now=datetime(2020, 2, 1, 16, 30),
    )
    same_day_ids = {
        row["forecast_id"] for row in same_day_rows
    }
    assert "f-close" not in same_day_ids
    assert {"f-oversold", "f-premarket"} <= same_day_ids


def test_counterfactual_outcomes_are_keyed_by_forecast_not_shared_horizon(
    monkeypatch,
):
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text(
            """
            CREATE TABLE sm_stock_kline (
                stock_code TEXT,
                short_name TEXT,
                trade_date DATE,
                open REAL, close REAL, high REAL, low REAL,
                pre_close REAL, amount REAL, change_pct REAL,
                k_type INTEGER
            )
            """
        ))
        connection.execute(text(
            """
            INSERT INTO sm_stock_kline VALUES (
                '002380', 'test', '2020-01-02',
                10, 10, 10, 10, 10, 100000000, 0, 1
            )
            """
        ))

    monkeypatch.setattr(
        "server.trading_v3.counterfactual_worker._build_features",
        lambda frame: pd.DataFrame({
            "stock_code": ["002380"],
            "trade_date": [pd.Timestamp("2020-01-02")],
        }),
    )

    def fake_outcome(_group, *, initial_stop_pct, **_kwargs):
        return {
            "net_return_pct": abs(initial_stop_pct),
            "mae_pct": initial_stop_pct,
            "mfe_pct": abs(initial_stop_pct) + 1,
            "exit_date": pd.Timestamp("2020-01-03"),
        }

    monkeypatch.setattr(
        "server.trading_v3.counterfactual_worker._dynamic_signal_outcome",
        fake_outcome,
    )
    rows = [
        {
            "forecast_id": "theme-forecast",
            "stock_code": "002380",
            "trade_date": date(2020, 1, 2),
            "horizon_days": 5,
            "initial_stop_pct": -5,
            "forecast_status": "RESEARCH_ONLY_UNCALIBRATED",
        },
        {
            "forecast_id": "oversold-forecast",
            "stock_code": "002380",
            "trade_date": date(2020, 1, 2),
            "horizon_days": 5,
            "initial_stop_pct": -6,
            "forecast_status": "PAPER_DISCOVERY_CANDIDATE",
        },
    ]
    outcomes = _outcomes(engine, rows)
    assert outcomes["theme-forecast"]["net_return_pct"] == 5
    assert outcomes["oversold-forecast"]["net_return_pct"] == 6
