# -*- coding: utf-8 -*-
from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from server.api.routers import strategy_center as strategy_center_router
from server.engine import strategy_center as strategy_center_engine
from server.engine.strategy_center import (
    MARKET_STATES,
    STRATEGY_CATALOG,
    adapt_recommendation_row,
    aggregate_candidates,
    _fuse_event_and_tape_risk,
    effective_weight,
    infer_market_state,
    resolve_conflict,
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


def test_persisted_compact_snapshot_rebuilds_candidates_without_heavy_json(monkeypatch):
    def fake_db_read(sql, _params=None):
        if "FROM st_strategy_center_run" in sql:
            return [{
                "run_uid": "run-1",
                "trade_date": "2026-07-24",
                "market_state": "extreme_event",
                "state_confidence": 96,
                "source_status": "fresh",
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
    assert payload["persisted_run_uid"] == "run-1"
    assert payload["global_gate"]["status"] == "BLOCK_NEW_BUY"
    assert payload["candidates"][0]["final_status"] == "BLOCKED"
    assert "strategy_signals" not in payload["candidates"][0]


def test_market_states_are_the_four_user_facing_states():
    assert set(MARKET_STATES) == {"trend_bullish", "high_range", "risk_declining", "extreme_event"}
