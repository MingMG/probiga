from __future__ import annotations

from datetime import date

from server.api.routers import trading_v3
from server.api.routers.holding_strategy import (
    build_daily_market_holding_context,
)
from server.common import canonical_decision_bridge as bridge


def _snapshot(*, targets=None):
    targets = list(targets or [])
    plan_hash = "c" * 64
    return {
        "status": "ok",
        "run_uid": "a" * 32,
        "canonical_result_hash": "b" * 64,
        "trade_date": "2026-08-28",
        "finished_at": "2026-08-28T16:00:00+08:00",
        "result_mode": "CANONICAL_PERSISTED",
        "is_canonical": True,
        "input_ready": True,
        "summary": {
            "tradable_count": len(targets),
            "market_risk_cap_pct": 20.0,
        },
        "trading_gate": {
            "market_state": "risk_declining",
            "market_risk_cap_pct": 20.0,
        },
        "pools": {
            "observation": [],
            "confirmation": [],
            "tradable": [],
        },
        "paper_execution_plan_hash": plan_hash,
        "paper_execution_plan": {
            "schema": "probiga.governance-paper-execution-plan.v1",
            "trade_date": "2026-08-28",
            "plan_hash": plan_hash,
            "policy": {"reference_capital_cny": 1_000_000},
            "targets": targets,
            "exit_targets": [],
            "target_count": len(targets),
            "invested_bp": sum(int(row["target_bp"]) for row in targets),
            "cash_bp": 10_000 - sum(
                int(row["target_bp"]) for row in targets
            ),
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        },
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def test_canonical_empty_batch_is_a_verified_cash_decision(monkeypatch):
    bridge._SNAPSHOT_CACHE.clear()
    monkeypatch.setattr(
        bridge, "load_canonical_governance_snapshot", lambda **_kwargs: _snapshot()
    )

    projected = bridge.canonical_governance_decision("2026-08-28")

    assert projected is not None
    assert projected["context"]["decision_status"] == "EMPTY"
    assert projected["context"]["data_status"] == "READY"
    assert projected["context"]["decision_integrity_verified"] is True
    assert projected["context"]["target_count"] == 0
    assert projected["pool"]["pool_status"] == "EMPTY"
    assert projected["pool"]["pool_readable"] is True
    assert projected["lineage"]["summary"]["target_count"] == 0
    assert projected["run"]["portfolio"]["targets"] == []

    holding_context = build_daily_market_holding_context(
        projected["run"], "2026-08-28"
    )
    assert holding_context["status"] == "READY"
    assert holding_context["market_action"] == "REDUCE"
    assert holding_context["blockers"] == []


def test_canonical_targets_are_projected_without_order_authority(monkeypatch):
    bridge._SNAPSHOT_CACHE.clear()
    target = {
        "stock_code": "000001",
        "stock_name": "平安银行",
        "strategy_key": "quality_momentum",
        "target_bp": 500,
        "reference_capital_cny": 1_000_000,
        "reference_price": 10.0,
        "reference_board_lot_quantity": 5000,
        "real_order_authority": False,
    }
    snapshot = _snapshot(targets=[target])
    snapshot["pools"]["tradable"] = [{
        "stock_code": "000001",
        "stock_name": "平安银行",
        "strategies": ["quality_momentum"],
        "dominant_strategy": "quality_momentum",
        "opportunity_score": 0.8,
    }]
    monkeypatch.setattr(
        bridge, "load_canonical_governance_snapshot", lambda **_kwargs: snapshot
    )

    projected = bridge.canonical_governance_decision("2026-08-28")

    assert projected is not None
    assert projected["context"]["decision_status"] == "CANDIDATE_AVAILABLE"
    assert projected["context"]["paper_order_authority"] == "NONE"
    assert projected["targets"][0]["target_weight"] == 0.05
    assert projected["targets"][0]["new_buy_eligible"] is False
    assert projected["pool"]["items"][0]["actionability"] == "PAPER_ONLY"
    assert projected["lineage"]["orders"] == []
    assert projected["lineage"]["real_order_authority"] is False


def test_canonical_observation_pool_is_not_reported_as_false_empty(monkeypatch):
    bridge._SNAPSHOT_CACHE.clear()
    snapshot = _snapshot()
    snapshot["pools"]["observation"] = [{
        "stock_code": "000002",
        "stock_name": "观察候选",
        "strategies": ["theme_diffusion"],
        "dominant_strategy": "theme_diffusion",
        "opportunity_score": 0.62,
        "reason": "主线扩散仍需确认",
    }]
    snapshot["strategies"] = [{
        "strategy_key": "theme_diffusion",
        "strategy_name": "板块扩散",
        "current_status": "SHADOW",
    }]
    monkeypatch.setattr(
        bridge, "load_canonical_governance_snapshot", lambda **_kwargs: snapshot
    )

    projected = bridge.canonical_governance_decision("2026-08-28")

    assert projected is not None
    assert projected["context"]["decision_status"] == "CANDIDATE_AVAILABLE"
    assert projected["context"]["target_count"] == 0
    assert projected["pool"]["pool_status"] == "READY"
    assert projected["pool"]["summary"]["strategy_candidate_count"] == 1
    assert projected["pool"]["items"][0]["actionability"] == "RESEARCH_ONLY"
    assert projected["pool"]["strategy_execution"]["strategies"][0][
        "status"
    ] == "COMPLETED_WITH_CANDIDATES"


def test_bridge_rejects_wrong_date_or_open_real_order_boundary(monkeypatch):
    snapshot = _snapshot()
    monkeypatch.setattr(
        bridge, "load_canonical_governance_snapshot", lambda **_kwargs: snapshot
    )
    assert bridge.canonical_governance_decision("2026-08-27") is None

    snapshot["real_order_authority"] = True
    bridge._SNAPSHOT_CACHE.clear()
    assert bridge.canonical_governance_decision("2026-08-28") is None


def test_latest_canonical_batch_can_be_read_as_of_a_later_session(monkeypatch):
    snapshot = _snapshot()

    def load_snapshot(*, trade_date=""):
        return None if str(trade_date or "") else snapshot

    bridge._SNAPSHOT_CACHE.clear()
    monkeypatch.setattr(
        bridge, "load_canonical_governance_snapshot", load_snapshot
    )

    projected = bridge.canonical_governance_decision(
        "2026-08-31", latest_as_of=True
    )

    assert projected is not None
    assert projected["context"]["requested_date"] == "2026-08-31"
    assert projected["context"]["decision_session_date"] == "2026-08-31"
    assert projected["context"]["data_date"] == "2026-08-28"
    assert projected["context"]["context_date_matches"] is True
    assert projected["context"]["historical_read_only"] is False
    assert projected["context"]["is_as_of_fallback"] is True
    assert projected["run"]["requested_as_of"] == "2026-08-31"
    assert projected["run"]["trade_date"] == "2026-08-28"
    assert projected["pool"]["requested_trade_date"] == "2026-08-31"
    assert projected["pool"]["decision_session_date"] == "2026-08-28"
    assert projected["pool"]["is_as_of_fallback"] is True

    holding_context = build_daily_market_holding_context(
        projected["run"], "2026-08-31"
    )
    assert holding_context["status"] == "READY"
    assert holding_context["market_action"] != "WAIT_DATA"
    assert holding_context["blockers"] == []


def test_latest_as_of_never_leaks_a_future_batch(monkeypatch):
    bridge._SNAPSHOT_CACHE.clear()
    monkeypatch.setattr(
        bridge,
        "load_canonical_governance_snapshot",
        lambda **_kwargs: _snapshot(),
    )

    assert (
        bridge.canonical_governance_decision(
            "2026-08-27", latest_as_of=True
        )
        is None
    )


def test_trading_pages_use_the_real_market_clock_route():
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    app = (root / "server/static/js/app.js").read_text(encoding="utf-8")
    v3 = (root / "server/static/js/trading-v3.js").read_text(encoding="utf-8")

    assert "/api/hot-data/market-clock" in app
    assert "/api/hot-data/market-clock" in v3
    assert "fetchJsonWithTimeout('/market-clock'" not in app
    assert "fetchJson('/market-clock')" not in v3
    assert "function normalizedTradingRouteDate(routeDate, tabId)" in app
    assert "routeDate > localToday || isWeekend" in app
    assert "routeDate > latest" not in app


def test_v3_context_uses_canonical_batch_when_legacy_run_is_missing(
    monkeypatch,
):
    class Repository:
        @staticmethod
        def latest_run_metadata(_trade_date):
            return None

    canonical = {
        "context": {
            "run_uid": "a" * 32,
            "decision_status": "EMPTY",
            "data_status": "READY",
            "target_count": 0,
        }
    }
    monkeypatch.setattr(trading_v3, "_repo", lambda: Repository())
    calls = []

    def canonical_projection(day, **kwargs):
        calls.append((day, kwargs))
        return canonical

    monkeypatch.setattr(
        trading_v3, "canonical_governance_decision", canonical_projection
    )

    response = trading_v3.decision_context(date(2026, 8, 28))

    assert response["status"] == "ok"
    assert response["data"] == canonical["context"]
    assert calls == [(date(2026, 8, 28), {"latest_as_of": True})]
