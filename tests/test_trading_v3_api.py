from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest
from fastapi import HTTPException

from server.api.routers import trading_v2, trading_v3


class FakeCalibration:
    model_version = "right_side_trend.v3.4.1-test"
    dataset_hash = "d" * 64

    def has_valid_score_direction(self):
        return True


class FakeRepository:
    def table_readiness(self):
        return {"st_decision_run_v3": True}

    def active_calibrations(self):
        return {"right_side_trend": FakeCalibration()}

    def latest_validations_for_models(self, model_versions):
        assert list(model_versions) == [
            "right_side_trend.v3.4.1-test"
        ]
        return {
            "right_side_trend.v3.4.1-test": {
                "validation_id": "v-pass",
                "result_status": "PASS",
                "created_at": "2026-08-02T10:00:00+08:00",
            }
        }

    def real_trading_guard_readiness(self):
        return {
            "account_insert": True,
            "account_update": True,
            "execution_plan_insert": True,
            "execution_plan_update": True,
        }

    def latest_run_metadata(self):
        today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
        return {
            "run_uid": "run-ready",
            "requested_as_of": today,
            "trade_date": today,
            "decision_at": datetime.combine(today, datetime.min.time()),
            "status": "COMPLETED",
            "lifecycle_status": "PAPER_TRIAL",
            "target_count": 1,
            "decision_integrity_verified": True,
            "decision_integrity_reason": "",
            "portfolio": {
                "decision_snapshot": {"manifest_hash": "a" * 64},
                "decision_truth": {
                    "schema_version": (
                        "probiga.trading-v3.decision-truth.v1"
                    ),
                    "run_status": "COMPLETED",
                    "actionable_status": "PAPER_ACTIONABLE",
                    "paper_order_authority": "V2_GATED",
                    "execution_authority": "V2_CANONICAL_LEDGER",
                    "order_authority": False,
                    "real_order_allowed": False,
                },
            },
        }

    def overview(self):
        return {
            "real_trading_enabled": False,
            "run": {
                "portfolio": {
                    "opportunity_audit": {"large": True},
                    "rejected": [
                        {"stock_code": str(index)}
                        for index in range(20)
                    ],
                }
            },
        }

    def latest_forecasts(
        self,
        *,
        limit,
        status,
        trade_date,
        strategy_key,
        query,
    ):
        return [{
            "limit": limit,
            "status": status,
            "trade_date": trade_date,
            "strategy_key": strategy_key,
            "query": query,
        }]

    def latest_targets(self):
        return []

    def stock_pool(self, *, trade_date, before_session_date=None):
        assert before_session_date is None
        return {
            "trade_date": trade_date,
            "items": [{"stock_code": "000001"}],
            "summary": {"stock_count": 1},
        }

    def latest_hypotheses(
        self,
        *,
        limit,
        trade_date,
        scope_type,
        state,
        query,
    ):
        return [{
            "limit": limit,
            "trade_date": trade_date,
            "scope_type": scope_type,
            "state": state,
            "query": query,
        }]

    def hypothesis_timeline(self, hypothesis_id, *, limit):
        if hypothesis_id == "f" * 32:
            return None
        return {
            "hypothesis": {"hypothesis_id": hypothesis_id},
            "events": [{"limit": limit}],
        }

    def latest_validation(self):
        return {"result_status": "PASS"}

    def latest_opportunity_recall(self):
        return None

    def decision_runs(self, *, limit):
        return [{"limit": limit}]


def test_v3_readiness_reports_calibrated_sleeves(monkeypatch):
    monkeypatch.setattr(trading_v3, "_repo", lambda: FakeRepository())
    result = trading_v3.readiness()
    assert result["data"]["structural_ready"] is True
    assert result["data"]["decision_ready"] is True
    assert result["data"]["execution_ready"] is None
    assert result["data"]["paper_ready"] is False
    assert result["data"]["active_calibrated_sleeves"] == [
        "right_side_trend"
    ]
    assert result["data"]["active_oos_models"] == [{
        "strategy_key": "right_side_trend",
        "model_version": "right_side_trend.v3.4.1-test",
        "dataset_hash": "d" * 64,
        "validation_status": "PASS",
        "validation_id": "v-pass",
        "validation_created_at": "2026-08-02T10:00:00+08:00",
    }]
    assert set(result["data"]["portfolio_limits"]) == {
        "minimum_positions",
        "maximum_positions",
        "maximum_add_count",
        "maximum_paper_discovery_positions",
        "maximum_live_positions",
    }
    assert result["real_trading_enabled"] is False


def test_v3_readiness_blocks_unverified_decision_truth(monkeypatch):
    class Repository(FakeRepository):
        engine = object()

        def latest_run_metadata(self):
            today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
            return {
                "run_uid": "run-without-decision-truth",
                "requested_as_of": today,
                "trade_date": today,
                "decision_at": datetime.combine(
                    today,
                    datetime.min.time(),
                ),
                "status": "COMPLETED",
                "lifecycle_status": "PAPER_TRIAL",
                "target_count": 1,
                "portfolio": {},
            }

    monkeypatch.setattr(trading_v3, "_repo", Repository)
    monkeypatch.setattr(
        trading_v2,
        "readiness",
        lambda: {
            "data": {
                "ready_for_new_positions": True,
                "blocks": [],
            }
        },
    )

    result = trading_v3.readiness()

    assert result["data"]["data_ready"] is False
    assert result["data"]["decision_ready"] is False
    assert result["data"]["execution_ready"] is True
    assert result["data"]["paper_authority_ready"] is False
    assert result["data"]["paper_ready"] is False
    assert result["status"] == "blocked"
    assert "LATEST_DECISION_UNAVAILABLE" in result["data"]["blocks"]


def test_v3_readiness_fails_closed_without_current_context_reader(
    monkeypatch,
):
    class Repository(FakeRepository):
        latest_run_metadata = None

    monkeypatch.setattr(trading_v3, "_repo", Repository)

    result = trading_v3.readiness()

    assert result["data"]["data_ready"] is False
    assert result["data"]["decision_ready"] is False
    assert result["data"]["paper_authority_ready"] is False
    assert result["data"]["paper_ready"] is False
    assert "DECISION_CONTEXT_READER_UNAVAILABLE" in result["data"][
        "blocks"
    ]


def test_v3_compact_overview_omits_heavy_audit_and_caps_rejections(
    monkeypatch,
):
    monkeypatch.setattr(trading_v3, "_repo", lambda: FakeRepository())
    full = trading_v3.overview(compact=False)["data"]
    compact = trading_v3.overview(compact=True)["data"]
    assert "opportunity_audit" in full["run"]["portfolio"]
    assert "opportunity_audit" not in compact["run"]["portfolio"]
    assert len(compact["run"]["portfolio"]["rejected"]) == 12


def test_v3_forecast_history_passes_date_without_recalculation(
    monkeypatch,
):
    monkeypatch.setattr(trading_v3, "_repo", lambda: FakeRepository())
    selected = date(2026, 7, 27)
    result = trading_v3.latest_forecasts(
        limit=100,
        status="VALIDATED_POSITIVE",
        trade_date=selected,
        strategy_key="theme_diffusion",
        q="永太",
    )
    assert result["data"][0]["trade_date"] == selected
    assert result["data"][0]["strategy_key"] == "theme_diffusion"
    assert result["data"][0]["query"] == "永太"


def test_v3_stock_pool_is_a_read_only_per_stock_snapshot(monkeypatch):
    monkeypatch.delenv(
        "PROBIGA_STRATEGY_GOVERNANCE_MODE",
        raising=False,
    )
    monkeypatch.setattr(trading_v3, "_repo", lambda: FakeRepository())
    selected = date(2026, 7, 27)
    result = trading_v3.stock_pool(trade_date=selected)
    assert result["data"]["trade_date"] == selected
    assert result["data"]["items"] == [{"stock_code": "000001"}]


def test_v3_stock_pool_required_mode_preserves_run_native_advisory_plan(
    monkeypatch,
):
    monkeypatch.delenv(
        "PROBIGA_STRATEGY_GOVERNANCE_MODE",
        raising=False,
    )
    native = {
        "run_uid": "run-required",
        "items": [{
            "stock_code": "600036",
            "actionability": "BUY_ZONE",
            "action_plan": {
                "actionability": "BUY_ZONE",
                "buy_range": {"low": 42.0, "high": 42.5},
                "sell_range": {"low": 45.0, "high": 46.0},
                "protective_stop": 40.5,
            },
        }],
        "summary": {"buy_zone_count": 1},
    }

    class Repository:
        def stock_pool(self, *, trade_date, before_session_date):
            return native

    monkeypatch.setattr(trading_v3, "_repo", lambda: Repository())

    result = trading_v3.stock_pool(trade_date=date(2026, 8, 27))["data"]

    assert result is native
    assert result["items"][0]["actionability"] == "BUY_ZONE"
    assert result["items"][0]["action_plan"]["buy_range"] == {
        "low": 42.0,
        "high": 42.5,
    }
    assert "governance_deferred" not in result


def test_v3_stock_pool_forwards_bounded_history_and_rejects_mixed_dates(
    monkeypatch,
):
    class Repository:
        def stock_pool(self, *, trade_date, before_session_date):
            return {
                "trade_date": trade_date,
                "before_session_date": before_session_date,
            }

    monkeypatch.setattr(trading_v3, "_repo", lambda: Repository())
    target = date(2026, 7, 27)

    result = trading_v3.stock_pool(
        trade_date=None,
        before_session_date=target,
    )

    assert result["data"]["trade_date"] is None
    assert result["data"]["before_session_date"] == target
    with pytest.raises(HTTPException) as exc_info:
        trading_v3.stock_pool(
            trade_date=target,
            before_session_date=target,
        )
    assert exc_info.value.status_code == 422


def test_v3_auction_gate_uses_the_same_pool_run_and_has_no_order_authority(
    monkeypatch,
):
    engine = object()

    class Repository:
        def __init__(self):
            self.engine = engine

        def stock_pool(self, *, trade_date, before_session_date):
            assert trade_date == date(2026, 8, 28)
            assert before_session_date is None
            return {
                "run_uid": "same-run",
                "pool_readable": True,
                "items": [{
                    "stock_code": "000001",
                    "is_strategy_candidate": True,
                }],
            }

    monkeypatch.setattr(trading_v3, "_repo", Repository)

    def build(source_engine, pool, *, session_date, cutoff_at):
        assert source_engine is engine
        assert pool["run_uid"] == "same-run"
        assert session_date == date(2026, 8, 28)
        assert cutoff_at == datetime(2026, 8, 28, 9, 25, 59)
        return {
            "status": "COMPLETED",
            "session_date": "2026-08-28",
            "source_run_uid": "same-run",
            "assessments": [],
            "order_authority": False,
            "automatic_substitution": False,
        }

    monkeypatch.setattr(trading_v3, "build_premarket_gate", build)

    result = trading_v3.premarket_auction_gate(
        trade_date=date(2026, 8, 28),
    )["data"]

    assert result["status"] == "COMPLETED"
    assert result["source_run_uid"] == "same-run"
    assert result["order_authority"] is False
    assert result["automatic_substitution"] is False
    assert result["evidence_mode"] == "POINT_IN_TIME_REPLAY"


def test_v3_validation_and_recall_are_read_only_snapshots(monkeypatch):
    monkeypatch.setattr(trading_v3, "_repo", lambda: FakeRepository())
    assert trading_v3.latest_validation()["data"]["result_status"] == "PASS"
    assert trading_v3.latest_opportunity_recall()["status"] == "collecting"


def test_v3_hypothesis_history_and_timeline_are_read_only(monkeypatch):
    monkeypatch.setattr(trading_v3, "_repo", lambda: FakeRepository())
    selected = date(2026, 7, 30)
    result = trading_v3.latest_hypotheses(
        limit=120,
        trade_date=selected,
        scope_type="STOCK",
        state="ACTIVE",
        q="永太",
    )
    assert result["data"][0]["trade_date"] == selected
    assert result["data"][0]["state"] == "ACTIVE"
    timeline = trading_v3.hypothesis_timeline(
        "a" * 32,
        limit=80,
    )
    assert timeline["data"]["events"][0]["limit"] == 80


def test_v3_research_api_does_not_label_hypothesis_or_target_as_buy():
    hypothesis = trading_v3._research_hypothesis_projection({
        "proposed_action": "BUY_OR_HOLD",
        "probability": 0.9,
    })
    target = trading_v3._research_target_projection({
        "stock_code": "000001",
        "target_quantity": 1_000,
    })

    assert hypothesis["source_proposed_action"] == "BUY_OR_HOLD"
    assert hypothesis["proposed_action"] == "WATCH_CLOSELY"
    assert hypothesis["decision_scope"] == "RESEARCH_ONLY"
    assert hypothesis["new_buy_eligible"] is False
    assert target["display_action"] == "WATCH"
    assert target["decision_scope"] == "RESEARCH_ONLY"
    assert target["new_buy_eligible"] is False
