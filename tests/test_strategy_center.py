# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import json

from fastapi import FastAPI
from fastapi.testclient import TestClient

from server.api.routers import strategy_center as strategy_center_router
from server.engine import strategy_center as strategy_center_engine
from server.engine.strategy_center import (
    MARKET_STATES,
    STRATEGY_CATALOG,
    _dynamic_shadow_round_robin_plan_ids,
    _dynamic_shadow_trade_session_ordinal,
    adapt_recommendation_row,
    aggregate_candidates,
    _fuse_event_and_tape_risk,
    effective_weight,
    infer_market_state,
    resolve_conflict,
)


def test_dynamic_shadow_round_robin_is_registry_order_independent():
    groups = [
        {"strategy_key": "alpha", "plan_ids": ["a1", "a2", "a3"]},
        {"strategy_key": "beta", "plan_ids": ["b1", "b2"]},
        {"strategy_key": "gamma", "plan_ids": ["g1"]},
    ]
    forward, contract = _dynamic_shadow_round_robin_plan_ids(
        groups,
        trade_date="2026-08-24",
        trade_session_ordinal=100,
        maximum_paper_orders_per_run=2,
        maximum_plans_scanned_per_run=1000,
    )
    reverse, reverse_contract = _dynamic_shadow_round_robin_plan_ids(
        list(reversed(groups)),
        trade_date="2026-08-24",
        trade_session_ordinal=100,
        maximum_paper_orders_per_run=2,
        maximum_plans_scanned_per_run=1000,
    )
    assert reverse == forward
    assert reverse_contract == contract
    by_strategy = {
        "alpha": ["a1", "a2", "a3"],
        "beta": ["b1", "b2"],
        "gamma": ["g1"],
    }
    expected = []
    for candidate_index in range(3):
        for strategy_key in contract["ordered_strategy_keys"]:
            plans = by_strategy[strategy_key]
            if candidate_index < len(plans):
                expected.append(plans[candidate_index])
    assert forward == expected
    assert contract["strategy_count"] == 3
    assert contract["plan_count"] == 6
    assert contract["selection_policy"] == (
        "stable_open_session_capacity_cursor_then_candidate_round_robin"
    )
    assert contract[
        "bounded_wait_maximum_consecutive_competition_runs"
    ] == 2
    assert contract["bounded_wait_contract_status"] == "CONDITIONAL"
    assert "eligible_strategy_set_hash_remains_stable" in contract[
        "bounded_wait_required_conditions"
    ]
    assert contract["bounded_wait_guarantees_order_acceptance"] is False
    assert contract["risk_rejection_consumes_paper_order_capacity"] is False
    assert contract[
        "risk_rejection_counts_as_capacity_underallocation"
    ] is False
    assert contract["automatic_real_order_submission"] is False
    assert contract["real_order_authority"] is False


def test_dynamic_shadow_capacity_cursor_proves_bounded_first_plan_wait():
    groups = [
        {"strategy_key": f"strategy_{index:02d}", "plan_ids": [f"p{index}"]}
        for index in range(7)
    ]
    observed_first_window: set[str] = set()
    contracts = []
    for ordinal in range(501, 505):
        ordered, contract = _dynamic_shadow_round_robin_plan_ids(
            list(reversed(groups)),
            trade_date="2026-08-24",
            trade_session_ordinal=ordinal,
            maximum_paper_orders_per_run=2,
            maximum_plans_scanned_per_run=1000,
        )
        observed_first_window.update(ordered[:2])
        contracts.append(contract)
    assert observed_first_window == {f"p{index}" for index in range(7)}
    assert all(
        item["bounded_wait_maximum_consecutive_competition_runs"] == 4
        for item in contracts
    )
    assert len({item["stable_strategy_set_hash"] for item in contracts}) == 1
    assert [item["strategy_cursor_index"] for item in contracts] == [6, 1, 3, 5]


def test_dynamic_shadow_trade_session_ordinal_requires_exact_open_day():
    class _Mappings:
        def __init__(self, row):
            self._row = row

        def one(self):
            return self._row

    class _Result:
        def __init__(self, row):
            self._row = row

        def mappings(self):
            return _Mappings(self._row)

    class _Connection:
        def __init__(self, row):
            self._row = row
            self.params = None

        def execute(self, statement, params):
            assert "si_trade_calendar" in str(statement)
            self.params = params
            return _Result(self._row)

    valid = _Connection({
        "trade_session_ordinal": 4123,
        "target_open_session_count": 1,
    })
    assert _dynamic_shadow_trade_session_ordinal(
        valid, trade_date="2026-08-24",
    ) == 4123
    assert valid.params["trade_date"].isoformat() == "2026-08-24"

    missing = _Connection({
        "trade_session_ordinal": 4122,
        "target_open_session_count": 0,
    })
    try:
        _dynamic_shadow_trade_session_ordinal(
            missing, trade_date="2026-08-24",
        )
    except RuntimeError as exc:
        assert "权威开市交易日" in str(exc)
    else:
        raise AssertionError("missing target trade session must fail closed")


def test_dynamic_shadow_bootstrap_competes_once_and_only_for_shadow(monkeypatch):
    from server.engine import strategy_governance
    from server.trading_v3 import paper_execution

    registry = [
        {
            "strategy_key": "beta", "current_version": "v1",
            "current_status": "SHADOW", "source_kind": "runtime_registry",
            "enabled": True, "execution_adapter": {"executable": True},
        },
        {
            "strategy_key": "already_active", "current_version": "v1",
            "current_status": "ACTIVE", "source_kind": "runtime_registry",
            "enabled": True, "execution_adapter": {"executable": True},
        },
        {
            "strategy_key": "alpha", "current_version": "v1",
            "current_status": "SHADOW", "source_kind": "runtime_registry",
            "enabled": True, "execution_adapter": {"executable": True},
        },
    ]
    connection = object()
    planned: list[str] = []
    competitions: list[list[str]] = []

    monkeypatch.setattr(strategy_governance, "load_registry", lambda: registry)
    monkeypatch.setattr(
        strategy_center_engine,
        "current_bound_sql_connection",
        lambda: connection,
    )
    monkeypatch.setattr(
        strategy_center_engine,
        "_dynamic_shadow_trade_session_ordinal",
        lambda observed_connection, *, trade_date: (
            500
            if observed_connection is connection and trade_date == "2026-08-24"
            else 0
        ),
    )

    def execute(strategy, _context, *, adapter_status):
        assert adapter_status is strategy["execution_adapter"]
        key = strategy["strategy_key"]
        return {
            "signals": [{"strategy_key": key}],
            "candidate_facts": [],
            "receipt": {
                "receipt_hash": (key[0] * 64),
                "input_hash": "1" * 64,
                "output_hash": "2" * 64,
                "stable_result_hash": "3" * 64,
                "candidate_count": 2,
                "run_uid": (key[0] * 32),
                "completed_at": "2026-08-24T16:00:00+08:00",
            },
        }

    monkeypatch.setattr(
        strategy_center_engine,
        "execute_dynamic_adapter_candidate_batch",
        execute,
    )
    monkeypatch.setattr(
        strategy_center_engine,
        "persist_strategy_adapter_run_receipt",
        lambda observed_connection, _receipt: (
            observed_connection is connection
        ),
    )
    monkeypatch.setattr(
        strategy_center_engine,
        "persist_strategy_adapter_candidate_facts",
        lambda observed_connection, **_kwargs: (
            observed_connection is connection
        ),
    )

    def create_plans(observed_connection, *, strategy, **_kwargs):
        assert observed_connection is connection
        assert strategy["current_status"] == "SHADOW"
        key = strategy["strategy_key"]
        planned.append(key)
        return {
            "plan_count": 2,
            "plan_ids": [f"{key}-1", f"{key}-2"],
            "plan_set_hash": key[0] * 64,
        }

    monkeypatch.setattr(
        strategy_center_engine,
        "create_dynamic_shadow_trial_plans_from_candidate_facts",
        create_plans,
    )

    def materialize(observed_connection, *, plan_ids):
        assert observed_connection is connection
        ordered = list(plan_ids)
        competitions.append(ordered)
        return {
            "status": "ok",
            "created": [
                {"plan_id": value, "idempotent_replay": False}
                for value in ordered[1:3]
            ],
            "skipped": [{
                "plan_id": ordered[0],
                "reason": "MAX_TOTAL_WEIGHT",
            }] + [
                {
                    "plan_id": value,
                    "reason": (
                        "DYNAMIC_SHADOW_BOOTSTRAP_ORDER_CAPACITY_DEFERRED"
                    ),
                }
                for value in ordered[3:]
            ],
            "paper_order_count": 2,
            "new_paper_order_count": 2,
            "idempotent_paper_order_count": 0,
            "scanned_plan_count": 3,
            "scanned_plan_ids": ordered[:3],
            "deferred_plan_count": len(ordered) - 3,
            "maximum_paper_orders_per_run": 20,
            "real_order_count": 0,
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }

    monkeypatch.setattr(
        paper_execution,
        "materialize_dynamic_shadow_bootstrap_orders",
        materialize,
    )
    signals, statuses = strategy_center_engine._dynamic_execution_signals(
        trade_date="2026-08-24",
        recommendation_rows=[],
        market={},
        configs={},
        metrics={},
        persist_receipts=True,
    )
    assert len(signals) == 3
    assert set(planned) == {"alpha", "beta"}
    assert len(competitions) == 1
    assert {
        value.rsplit("-", 1)[0] for value in competitions[0][:2]
    } == {"alpha", "beta"}
    by_key = {row["strategy_key"]: row for row in statuses}
    assert by_key["alpha"]["shadow_bootstrap_paper_order_count"] == 1
    assert by_key["beta"]["shadow_bootstrap_paper_order_count"] == 1
    shadow_results = [
        by_key[key]["shadow_bootstrap_result"]
        for key in ("alpha", "beta")
    ]
    assert sum(
        item["capacity_opportunity_plan_count"]
        for item in shadow_results
    ) == 3
    assert sum(
        item["risk_or_eligibility_rejected_plan_count"]
        for item in shadow_results
    ) == 1
    assert all(
        item["risk_rejection_consumes_paper_order_capacity"] is False
        and item["risk_rejection_counts_as_capacity_underallocation"] is False
        for item in shadow_results
    )
    assert by_key["already_active"]["shadow_trial_plan_count"] == 0
    assert by_key["already_active"]["shadow_bootstrap_result"]["status"] == (
        "NOT_APPLICABLE_LIFECYCLE"
    )
    assert all(
        row["shadow_bootstrap_real_order_count"] == 0
        for row in by_key.values()
    )


def test_dynamic_execution_reuses_one_batch_registry_readiness_snapshot(
    monkeypatch,
):
    from server.engine import strategy_governance

    query_count = 0
    registry = []
    for index in range(25):
        key = f"dynamic_{index:02d}"
        registry.append({
            "strategy_key": key,
            "current_version": "v1",
            "current_status": "SHADOW",
            "source_kind": "runtime_registry",
            "enabled": True,
            "execution_adapter": {
                "executable": True,
                "status": "RESEARCH_READY",
                "funding_pipeline_ready": False,
                "execution_binding_hash": str(index).zfill(64),
            },
        })

    def load_registry_once():
        nonlocal query_count
        query_count += 1
        return registry

    observed_statuses = []

    def execute(strategy, _context, *, adapter_status):
        assert adapter_status is strategy["execution_adapter"]
        observed_statuses.append(adapter_status)
        return {
            "signals": [],
            "candidate_facts": [],
            "receipt": {
                "receipt_hash": "a" * 64,
                "input_hash": "b" * 64,
                "output_hash": "c" * 64,
                "stable_result_hash": "d" * 64,
                "candidate_count": 0,
                "run_uid": "e" * 32,
                "completed_at": "2026-08-24T16:00:00+08:00",
            },
        }

    monkeypatch.setattr(strategy_governance, "load_registry", load_registry_once)
    monkeypatch.setattr(
        strategy_center_engine,
        "execute_dynamic_adapter_candidate_batch",
        execute,
    )
    signals, statuses = strategy_center_engine._dynamic_execution_signals(
        trade_date="2026-08-24",
        recommendation_rows=[],
        market={},
        configs={},
        metrics={},
    )

    assert signals == []
    assert len(statuses) == len(registry)
    assert len(observed_statuses) == len(registry)
    assert query_count == 1


def test_candidate_limit_one_does_not_truncate_751_runtime_strategies(
    monkeypatch,
):
    """The legacy limit bounds stock rows, never strategy discovery."""

    from server.engine import strategy_governance

    registry = [{
        "strategy_key": f"dynamic_{index:04d}",
        "current_version": "v1",
        "current_status": "SHADOW",
        "source_kind": "runtime_registry",
        "enabled": True,
        "execution_adapter": {
            "executable": False,
            "status": "UNDEPLOYED_OR_INVALID",
            "status_label": "执行适配器未部署/无效",
            "reason": "测试未部署适配器",
            "funding_pipeline_ready": False,
        },
    } for index in range(751)]
    observed_candidate_limits = []

    monkeypatch.setattr(strategy_governance, "load_registry", lambda: registry)
    monkeypatch.setattr(
        strategy_center_engine,
        "latest_recommendation_date",
        lambda _value="": "2026-08-24",
    )
    monkeypatch.setattr(
        strategy_center_engine, "load_reference_candidate_pool", lambda _day: {},
    )
    monkeypatch.setattr(
        strategy_center_engine,
        "load_market_snapshot",
        lambda *_args, **_kwargs: {
            "source_status": "fresh",
            "state": {"key": "trend_bullish", "name": "趋势上涨"},
        },
    )
    monkeypatch.setattr(strategy_center_engine, "load_strategy_configs", lambda: {})
    monkeypatch.setattr(
        strategy_center_engine, "load_strategy_metrics", lambda _day: {},
    )

    def load_candidate_rows(_day, limit):
        observed_candidate_limits.append(limit)
        return []

    monkeypatch.setattr(
        strategy_center_engine, "load_recommendation_rows", load_candidate_rows,
    )
    monkeypatch.setattr(strategy_center_engine, "_table_exists", lambda _name: True)
    monkeypatch.setattr(
        strategy_center_engine,
        "_table_columns",
        lambda _name: {"stock_code", "pick_date"},
    )
    monkeypatch.setattr(
        strategy_center_engine,
        "_db_read",
        lambda *_args, **_kwargs: [{"cnt": 0}],
    )
    monkeypatch.setattr(
        strategy_center_engine, "build_strategy_cards", lambda *_args: [],
    )
    monkeypatch.setattr(
        strategy_center_engine,
        "load_stock_manifest",
        lambda: {"manifest_version": "test-v1"},
    )
    monkeypatch.setattr(
        strategy_center_engine, "stock_manifest_hash", lambda: "a" * 64,
    )
    monkeypatch.setattr(
        strategy_center_engine,
        "load_market_state_config",
        lambda: {"config_version": "test-v1"},
    )
    monkeypatch.setattr(
        strategy_center_engine, "market_state_config_hash", lambda: "b" * 64,
    )

    snapshot = strategy_center_engine.build_strategy_center_snapshot(
        "2026-08-24", limit=1,
    )

    assert observed_candidate_limits == [1]
    assert len(snapshot["dynamic_adapter_statuses"]) == 751
    assert snapshot["summary"]["runtime_registry_count"] == 751
    assert snapshot["summary"]["enabled_runtime_count"] == 751
    assert snapshot["summary"]["runtime_registry_discovery_status"] == "COMPLETE"
    assert snapshot["dynamic_adapter_statuses"][-1]["strategy_key"] == (
        "dynamic_0750"
    )


def test_strategy_catalog_contains_only_four_independent_strategies():
    keys = [item["key"] for item in STRATEGY_CATALOG]
    assert len(keys) == 4
    assert len(set(keys)) == 4
    assert set(keys) == {
        "ultra_short", "short_term", "swing", "main_wave",
    }


def test_infer_market_state_extreme_takes_priority():
    result = infer_market_state({
        "market_state": "trend_bullish",
        "risk_score": 90,
        "market_change_pct": -5.1,
    })
    assert result["key"] == "extreme_event"

    inferred = infer_market_state({"risk_score": 90, "market_change_pct": -5.1})
    assert inferred["key"] == "extreme_event"
    assert inferred["confidence"] >= 90


def test_infer_market_state_distinguishes_trend_and_range():
    trend = infer_market_state({"trend_score": 80, "breadth_pct": 65, "risk_score": 20})
    assert trend["key"] == "trend_bullish"

    high_range = infer_market_state({"high_position": True, "breadth_pct": 42, "switch_score": 70})
    assert high_range["key"] == "high_range"


def test_news_event_without_tape_stress_cannot_block_whole_market():
    snapshot = {
        "risk_off_score": 91,
        "tech_risk_score": 100,
        "tech_triggered": True,
    }
    kline = {
        "is_current": True,
        "market_change_pct": 3.15,
        "breadth_pct": 93.9,
        "trend_score": 69.1,
        "switch_score": 91,
        "risk_score": 20,
    }
    fused = _fuse_event_and_tape_risk(
        snapshot,
        kline,
        config={
            "event_fusion": {
                "event_alert_score_gte": 70,
                "systemic_event_score_gte": 82,
                "price_stress_market_change_lte": -1.5,
                "price_stress_breadth_lte": 35,
                "price_stress_kline_risk_gte": 70,
                "unconfirmed_event_risk_score_cap": 54,
                "unconfirmed_event_trend_score_cap": 67,
            }
        },
    )
    assert fused["extreme_event"] is False
    assert fused["risk_score"] == 54
    assert fused["trend_score"] == 67
    assert fused["tech_triggered_raw"] is True
    assert fused["tech_triggered"] is False
    assert fused["event_risk_status"] == (
        "SECTOR_CAUTION_TAPE_NOT_CONFIRMED"
    )


def test_news_event_with_broad_price_stress_is_systemic():
    snapshot = {
        "risk_off_score": 91,
        "tech_risk_score": 100,
        "tech_triggered": True,
    }
    fused = _fuse_event_and_tape_risk(
        snapshot,
        {
            "is_current": True,
            "market_change_pct": -2.2,
            "breadth_pct": 22,
            "trend_score": 30,
            "switch_score": 80,
            "risk_score": 78,
        },
        config={"event_fusion": {}},
    )
    assert fused["extreme_event"] is True
    assert fused["risk_score"] == 100
    assert fused["tech_triggered"] is True
    assert fused["event_risk_status"] == "SYSTEMIC_CONFIRMED"


def test_effective_weight_deweights_risk_mode_and_disabled_strategy():
    normal = effective_weight("trend_breakout", "trend_bullish")
    risk = effective_weight("trend_breakout", "risk_declining")
    disabled = effective_weight("trend_breakout", "trend_bullish", {"enabled": False})
    assert normal["effective_weight"] > risk["effective_weight"]
    assert disabled["effective_weight"] == 0


def test_resolve_conflict_hard_block_wins_over_buy():
    result = resolve_conflict([
        {"strategy_key": "value_quality", "signal_direction": "BUY", "effective_weight": 1, "model_confidence": 90, "effective_score": 80, "gate_status": "PASS", "risk_level": "LOW"},
        {"strategy_key": "event_driven", "signal_direction": "HOLD", "effective_weight": 1, "model_confidence": 95, "effective_score": 70, "gate_status": "BLOCK", "gate_reason": "重大事件", "risk_level": "CRITICAL"},
    ], "trend_bullish")
    assert result["final_status"] == "BLOCKED"
    assert result["final_direction"] == "HOLD"
    assert result["blocking_reasons"] == ["重大事件"]


def test_resolve_conflict_near_tie_becomes_watch_conflict():
    result = resolve_conflict([
        {"strategy_key": "value_quality", "signal_direction": "BUY", "effective_weight": 1, "model_confidence": 60, "effective_score": 70, "gate_status": "PASS", "risk_level": "LOW"},
        {"strategy_key": "event_driven", "signal_direction": "SELL", "effective_weight": 1, "model_confidence": 58, "effective_score": 72, "gate_status": "PASS", "risk_level": "HIGH"},
    ], "trend_bullish")
    assert result["final_status"] == "CONFLICT"
    assert result["final_direction"] == "HOLD"
    assert result["conflict"] is True


def test_extreme_event_blocks_new_buy_after_signal_adaptation():
    row = {
        "stock_code": "600036",
        "short_name": "招商银行",
        "pick_date": "2026-07-17",
        "final_trade_score": 82,
        "swing_score": 82,
        "signal_status": "BUY_READY",
        "recommend_status": "ALLOW",
        "chase_risk_status": "ALLOW",
        "ordinary_buy_eligible": True,
        "event_risk_level": "LOW",
        "confidence_score": 88,
    }
    signal = adapt_recommendation_row(row, "value_quality", {"market_state": "extreme_event"})
    assert signal["strategy_key"] == "swing"
    assert signal["signal_direction"] == "BUY"
    assert signal["gate_status"] == "BLOCK"
    assert signal["signal_status"] == "BLOCKED"


def test_strategy_score_cannot_promote_watch_into_buy_ready():
    row = {
        "stock_code": "600036",
        "short_name": "招商银行",
        "pick_date": "2026-08-05",
        "final_trade_score": 99,
        "swing_score": 99,
        "signal_status": "WATCH",
        "recommend_status": "ALLOW",
        "chase_risk_status": "ALLOW",
        "ordinary_buy_eligible": True,
        "event_risk_level": "LOW",
        "confidence_score": 99,
    }

    signal = adapt_recommendation_row(
        row,
        "swing",
        {"market_state": "trend_bullish"},
    )

    assert signal["signal_direction"] == "HOLD"
    assert signal["signal_status"] == "BLOCKED"
    assert signal["source_signal_status"] == "WATCH"


def test_aggregate_candidates_preserves_all_strategy_signals():
    rows = [{
        "stock_code": "600036", "short_name": "招商银行", "pick_date": "2026-07-17",
        "final_trade_score": 82, "long_term_score": 80, "short_term_score": 75,
        "swing_score": 80, "main_wave_score": 78,
        "suitable_strategies": '["value_quality", "trend_breakout"]',
        "signal_status": "BUY_READY", "recommend_status": "ALLOW", "event_risk_level": "LOW",
        "chase_risk_status": "ALLOW", "ordinary_buy_eligible": True,
        "confidence_score": 85,
    }]
    candidates, conflicts = aggregate_candidates(rows, {"market_state": "trend_bullish", "source_status": "fresh"})
    assert len(candidates) == 1
    assert set(candidates[0]["strategies"]) == {"swing", "main_wave"}
    assert len(candidates[0]["strategy_signals"]) == 2
    assert conflicts == []


def test_strategy_center_overview_endpoint_returns_structured_snapshot(monkeypatch):
    app = FastAPI()
    app.include_router(strategy_center_router.router, prefix="/api")
    expected = {
        "status": "ok", "trade_date": "2026-07-20", "data_date": "2026-07-17",
        "source_status": "fresh", "is_stale": False, "market_state": {"key": "trend_bullish"},
        "global_gate": {"status": "ALLOW_NEW_BUY"}, "strategies": [], "candidates": [], "conflicts": [],
        "summary": {}, "disclaimer": "研究",
    }
    monkeypatch.setattr(strategy_center_router, "build_strategy_center_snapshot", lambda *_args, **_kwargs: expected)
    response = TestClient(app).get("/api/strategy-center/overview?trade_date=2026-07-20")
    assert response.status_code == 200
    assert response.json()["market_state"]["key"] == "trend_bullish"


def test_strategy_center_api_marks_buy_direction_research_only():
    row = strategy_center_router._research_only_candidate({
        "stock_code": "600036",
        "final_direction": "BUY",
        "final_status": "READY",
        "model_confidence": 99,
    })

    assert row["final_direction"] == "BUY"
    assert row["decision_scope"] == "RESEARCH_ONLY"
    assert row["display_action"] == "WATCH"
    assert row["new_buy_eligible"] is False


def test_strategy_center_candidate_compact_view_omits_heavy_signal_payload(monkeypatch):
    app = FastAPI()
    app.include_router(strategy_center_router.router, prefix="/api")
    candidate = {
        "priority": 1,
        "stock_code": "600036",
        "stock_name": "招商银行",
        "final_direction": "HOLD",
        "final_status": "BLOCKED",
        "model_confidence": 82,
        "today_signal": "等待",
        "entry_low": 42.0,
        "entry_high": 42.5,
        "stop_loss": 40.0,
        "risk_level": "HIGH",
        "dominant_strategy": "swing",
        "blocking_reasons": ["市场门禁"],
        "conflict_summary": "",
        "data_date": "2026-07-24",
        "strategies": ["swing"],
        "strategy_signals": [{"evidence_chain": ["x" * 1000]}],
    }
    snapshot = {
        "status": "ok",
        "trade_date": "2026-07-24",
        "data_date": "2026-07-24",
        "source_status": "fresh",
        "is_stale": False,
        "market_state": {"key": "extreme_event"},
        "global_gate": {"status": "BLOCK_NEW_BUY"},
        "candidates": [candidate],
        "conflicts": [{**candidate, "conflict_summary": "冲突"}],
        "disclaimer": "研究",
    }
    monkeypatch.setattr(
        strategy_center_router,
        "load_persisted_strategy_center_compact",
        lambda *_args, **_kwargs: snapshot,
    )
    monkeypatch.setattr(
        strategy_center_router,
        "build_strategy_center_snapshot",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("compact endpoint should use the persisted run")
        ),
    )

    payload = TestClient(app).get(
        "/api/strategy-center/candidates?trade_date=2026-07-24&compact=true"
    ).json()

    assert payload["total"] == 1
    assert "strategy_signals" not in payload["data"][0]
    assert payload["data"][0]["stock_code"] == "600036"
    assert "strategy_signals" not in payload["conflicts"][0]


def test_strategy_center_compact_candidates_fail_closed_without_canonical_binding(
    monkeypatch,
):
    app = FastAPI()
    app.include_router(strategy_center_router.router, prefix="/api")
    monkeypatch.setattr(
        strategy_center_router,
        "load_persisted_strategy_center_compact",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        strategy_center_router,
        "build_strategy_center_snapshot",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("compact route must not fall back to a live snapshot")
        ),
    )

    response = TestClient(app).get(
        "/api/strategy-center/candidates?trade_date=2026-07-24&compact=true"
    )

    assert response.status_code == 503
    payload = response.json()
    assert payload["reason_code"] == (
        "CANONICAL_STRATEGY_CENTER_RUN_UNAVAILABLE"
    )
    assert payload["data"] == []
    assert payload["automatic_real_order_submission"] is False


def test_persisted_compact_snapshot_rebuilds_candidates_without_heavy_json(monkeypatch):
    center_run_uid = "a" * 32
    governance_run_uid = "b" * 32
    governance_result = {
        "run_uid": governance_run_uid,
        "trade_date": "2026-07-24",
        "strategy_center_run_uid": center_run_uid,
        "is_canonical": True,
        "result_mode": "CANONICAL_PERSISTED",
    }
    governance_json = json.dumps(
        governance_result,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    queries = []

    def fake_db_read(sql, _params=None):
        queries.append(sql)
        if "FROM st_strategy_governance_run" in sql:
            return [{
                "governance_run_uid": governance_run_uid,
                "governance_trade_date": "2026-07-24",
                "governance_result_json": governance_json,
                "governance_result_hash": hashlib.sha256(
                    governance_json.encode("utf-8")
                ).hexdigest(),
                "run_uid": center_run_uid,
                "trade_date": "2026-07-24",
                "market_state": "extreme_event",
                "state_confidence": 96,
                "source_status": "fresh",
                "finished_at": "2026-07-24 17:00:00",
            }]
        if "FROM st_strategy_center_signal" in sql:
            return [{
                "stock_code": "600036",
                "stock_name": "招商银行",
                "strategy_key": "swing",
                "market_state": "extreme_event",
                "signal_direction": "BUY",
                "signal_status": "BLOCKED",
                "effective_score": 80,
                "model_confidence": 88,
                "risk_level": "HIGH",
                "gate_status": "BLOCK",
                "gate_reason": "极端行情",
                "entry_low": 42,
                "entry_high": 42.5,
                "stop_loss": 40,
                "today_signal": "等待",
                "data_snapshot_json": '{"data_date":"2026-07-24","large":"ignored"}',
            }]
        raise AssertionError(sql)

    monkeypatch.setattr(strategy_center_engine, "_db_read", fake_db_read)
    payload = strategy_center_engine.load_persisted_strategy_center_compact(
        "2026-07-24",
        20,
    )

    assert payload is not None
    assert payload["persisted_run_uid"] == center_run_uid
    assert payload["candidate_source"]["governance_run_uid"] == governance_run_uid
    assert payload["candidate_source"]["canonical_binding_verified"] is True
    assert payload["global_gate"]["status"] == "BLOCK_NEW_BUY"
    assert payload["candidates"][0]["final_status"] == "BLOCKED"
    assert "strategy_signals" not in payload["candidates"][0]
    assert "LEFT JOIN st_strategy_center_run AS center" in queries[0]
    assert "JSON_EXTRACT" in queries[0]
    assert "ORDER BY trade_date DESC, run_revision DESC" in queries[0]


def test_persisted_compact_snapshot_requires_a_canonical_governance_run(monkeypatch):
    calls = []

    def fake_db_read(sql, params=None):
        calls.append((sql, params))
        return []

    monkeypatch.setattr(strategy_center_engine, "_db_read", fake_db_read)

    assert strategy_center_engine.load_persisted_strategy_center_compact(
        "2026-07-24", 20,
    ) is None
    assert len(calls) == 1
    assert "st_strategy_governance_run" in calls[0][0]


def test_persisted_compact_snapshot_rejects_wrong_bound_run_uid(monkeypatch):
    governance_run_uid = "b" * 32
    bound_run_uid = "a" * 32
    governance_result = {
        "run_uid": governance_run_uid,
        "trade_date": "2026-07-24",
        "strategy_center_run_uid": bound_run_uid,
        "is_canonical": True,
        "result_mode": "CANONICAL_PERSISTED",
    }
    governance_json = json.dumps(
        governance_result,
        sort_keys=True,
        separators=(",", ":"),
    )

    def fake_db_read(sql, _params=None):
        assert "st_strategy_governance_run" in sql
        return [{
            "governance_run_uid": governance_run_uid,
            "governance_trade_date": "2026-07-24",
            "governance_result_json": governance_json,
            "governance_result_hash": hashlib.sha256(
                governance_json.encode("utf-8")
            ).hexdigest(),
            # A newer legacy/manual run cannot satisfy the canonical binding.
            "run_uid": "c" * 32,
            "trade_date": "2026-07-24",
        }]

    monkeypatch.setattr(strategy_center_engine, "_db_read", fake_db_read)

    assert strategy_center_engine.load_persisted_strategy_center_compact(
        "2026-07-24", 20,
    ) is None


def test_market_states_are_the_four_user_facing_states():
    assert set(MARKET_STATES) == {"trend_bullish", "high_range", "risk_declining", "extreme_event"}
