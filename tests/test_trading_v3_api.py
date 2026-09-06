from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine, text

from server.api.routers import trading_v2, trading_v3


_DAILY_BUILD_SHA = "a" * 40


def _healthy_daily_scheduler(build_sha=_DAILY_BUILD_SHA):
    return {
        "status": "HEALTHY",
        "healthy": True,
        "expected_build_sha": build_sha,
        "roles": {
            "linux_standalone": {
                "healthy": True,
                "current": {"build_sha": build_sha},
                "errors": [],
            },
            "qmt_windows_edge": {
                "healthy": True,
                "current": {"build_sha": build_sha},
                "immutable_reference_verified": True,
                "errors": [],
            },
        },
        "reason_codes": [],
    }


def _patch_daily_release_identity(
    monkeypatch,
    *,
    execution_session_date=date(2026, 9, 2),
    build_sha=_DAILY_BUILD_SHA,
):
    monkeypatch.setattr(
        trading_v3,
        "code_version",
        lambda: (build_sha, "test"),
    )
    monkeypatch.setattr(
        trading_v3,
        "_next_execution_session_date",
        lambda _engine, _decision_date: execution_session_date,
    )
    monkeypatch.setattr(
        trading_v3,
        "_daily_scheduler_health",
        lambda *args, **kwargs: _healthy_daily_scheduler(build_sha),
    )


def _daily_empty_pool(
    *,
    trade_date="2026-09-01",
    build_sha=_DAILY_BUILD_SHA,
):
    return {
        "run_uid": f"canonical-{trade_date}",
        "build_commit_sha": build_sha,
        "trade_date": trade_date,
        "decision_date": trade_date,
        "decision_session_date": trade_date,
        "decision_at": f"{trade_date}T22:35:00+08:00",
        "pool_status": "EMPTY",
        "pool_readable": True,
        "run_status": "COMPLETED",
        "decision_integrity_verified": True,
        "source_system": "STRATEGY_GOVERNANCE",
        "decision_scope": "CANONICAL_GOVERNANCE",
        "canonical_result_hash": "e" * 64,
        "is_historical_fallback": False,
        "historical_read_only": False,
        "reason_codes": [],
        "items": [],
        "summary": {
            "stock_count": 0,
            "strategy_candidate_count": 0,
            "target_count": 0,
        },
        "strategy_execution": {
            "strategy_count": 0,
            "strategies": [],
        },
    }


def _verified_daily_real_trading_safety():
    return {
        "status": "SAFE",
        "verified": True,
        "real_trading_enabled": False,
        "account_count": 1,
        "enabled_account_count": 0,
        "accounts": [{
            "account_id": "paper-main-v2",
            "real_trading_enabled": False,
            "updated_at": "2026-09-01T22:30:00+08:00",
        }],
        "guards": {
            "account_insert": True,
            "account_update": True,
            "execution_plan_insert": True,
            "execution_plan_update": True,
        },
        "reason_codes": [],
    }


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


def test_v3_stock_pool_maps_execution_session_for_verified_native_history(
    monkeypatch,
):
    selected = date(2026, 9, 4)
    execution_day = date(2026, 9, 7)
    native = {
        "run_uid": "verified-native-history",
        "trade_date": selected.isoformat(),
        "decision_session_date": selected.isoformat(),
        "pool_status": "READY",
        "pool_readable": True,
        "run_status": "COMPLETED",
        "decision_integrity_verified": True,
        "is_historical_fallback": True,
        "historical_read_only": True,
        "decision_scope": "RESEARCH_ONLY",
        "actionable_output_allowed": False,
        "real_order_authority": False,
        "items": [{"stock_code": "000001", "is_strategy_candidate": True}],
        "summary": {"stock_count": 1, "strategy_candidate_count": 1},
        "reason_codes": [],
    }

    class Repository:
        engine = object()

        def stock_pool(self, *, trade_date, before_session_date):
            assert trade_date is None
            assert before_session_date == date(2026, 9, 7)
            return native

    monkeypatch.delenv("PROBIGA_STRATEGY_GOVERNANCE_MODE", raising=False)
    monkeypatch.setattr(
        trading_v3,
        "_next_execution_session_date",
        lambda engine, decision_date: (
            execution_day
            if engine is Repository.engine and decision_date == selected
            else None
        ),
    )

    result = trading_v3._stock_pool_payload(
        trade_date=None,
        before_session_date=date(2026, 9, 7),
        repository=Repository(),
    )

    assert result["decision_date"] == selected.isoformat()
    assert result["decision_session_date"] == selected.isoformat()
    assert result["execution_session_date"] == execution_day.isoformat()
    assert result["pool_readable"] is True
    assert result["decision_integrity_verified"] is True
    assert result["is_historical_fallback"] is True
    assert result["historical_read_only"] is True
    assert result["decision_scope"] == "RESEARCH_ONLY"
    assert result["actionable_output_allowed"] is False
    assert result["real_order_authority"] is False


def test_v3_stock_pool_blocks_verified_native_history_without_calendar(
    monkeypatch,
):
    native = {
        "run_uid": "verified-native-history",
        "trade_date": "2026-09-04",
        "decision_session_date": "2026-09-04",
        "pool_status": "READY",
        "pool_readable": True,
        "run_status": "COMPLETED",
        "decision_integrity_verified": True,
        "is_historical_fallback": True,
        "historical_read_only": True,
        "decision_scope": "RESEARCH_ONLY",
        "actionable_output_allowed": False,
        "real_order_authority": False,
        "items": [{"stock_code": "000001", "is_strategy_candidate": True}],
        "summary": {"stock_count": 1, "strategy_candidate_count": 1},
        "reason_codes": [],
    }

    class Repository:
        engine = object()

        def stock_pool(self, *, trade_date, before_session_date):
            return native

    monkeypatch.delenv("PROBIGA_STRATEGY_GOVERNANCE_MODE", raising=False)
    monkeypatch.setattr(
        trading_v3,
        "_next_execution_session_date",
        lambda *_args: (_ for _ in ()).throw(
            RuntimeError("NEXT_TRADE_SESSION_UNAVAILABLE")
        ),
    )

    result = trading_v3._stock_pool_payload(
        trade_date=None,
        before_session_date=date(2026, 9, 7),
        repository=Repository(),
    )

    assert result["execution_session_date"] is None
    assert result["pool_readable"] is False
    assert result["decision_integrity_verified"] is False
    assert "EXECUTION_SESSION_DATE_UNAVAILABLE" in result["reason_codes"]
    assert result["is_historical_fallback"] is True
    assert result["historical_read_only"] is True
    assert result["decision_scope"] == "RESEARCH_ONLY"
    assert result["actionable_output_allowed"] is False
    assert result["real_order_authority"] is False


def test_daily_result_returns_one_exact_run_for_first_screen(monkeypatch):
    selected = date(2026, 9, 1)
    pool = {
        "run_uid": "run-20260901",
        "build_commit_sha": _DAILY_BUILD_SHA,
        "trade_date": "2026-09-01",
        "decision_session_date": "2026-09-01",
        "requested_trade_date": "2026-09-01",
        "decision_at": "2026-09-01T22:35:00+08:00",
        "pool_status": "READY",
        "pool_readable": True,
        "run_status": "COMPLETED",
        "decision_integrity_verified": True,
        "source_system": "STRATEGY_GOVERNANCE",
        "decision_scope": "CANONICAL_GOVERNANCE",
        "canonical_result_hash": "A" * 64,
        "is_historical_fallback": False,
        "historical_read_only": False,
        "reason_codes": [],
        "items": [{
            "stock_code": "000001",
            "stock_name": "平安银行",
            "is_strategy_candidate": True,
            "valid_until": "2026-09-02T10:00:00+08:00",
            "target": {"rank_no": 1, "target_weight": 0.05},
            "rejection": None,
        }],
        "summary": {
            "stock_count": 1,
            "strategy_candidate_count": 1,
            "target_count": 1,
        },
        "strategy_execution": {
            "strategy_count": 1,
            "strategies": [{"strategy_key": "right_side_trend"}],
        },
    }

    class Repository:
        engine = object()

        def stock_pool(self, *, trade_date, before_session_date=None):
            assert trade_date == selected
            assert before_session_date is None
            return pool

    monkeypatch.delenv("PROBIGA_STRATEGY_GOVERNANCE_MODE", raising=False)
    monkeypatch.setattr(trading_v3, "_repo", Repository)
    monkeypatch.setattr(
        trading_v3,
        "canonical_governance_decision",
        lambda *args, **kwargs: {"pool": pool},
    )
    monkeypatch.setattr(
        trading_v3,
        "_analysis_runtime_context",
        lambda *args, **kwargs: None,
    )
    _patch_daily_release_identity(monkeypatch)
    monkeypatch.setattr(
        trading_v3,
        "_daily_real_trading_safety",
        lambda *args, **kwargs: _verified_daily_real_trading_safety(),
    )
    trading_v3._DAILY_RESULT_CACHE.clear()

    result = trading_v3.daily_result(selected, force=True)
    data = result["data"]

    assert result["status"] == "ok"
    assert data["delivery_status"] == "COMPLETED"
    assert data["run_uid"] == "run-20260901"
    assert data["context"]["run_uid"] == data["stock_pool"]["run_uid"]
    assert data["overview"]["run"]["run_uid"] == data["run_uid"]
    assert data["strategy_pool"]["run_uid"] == data["run_uid"]
    assert data["strategy_pool"]["pool_readable"] is True
    assert data["canonical_result_hash"] == "a" * 64
    assert data["context"]["canonical_result_hash"] == "a" * 64
    assert data["overview"]["run"]["canonical_result_hash"] == "a" * 64
    assert data["strategy_pool"]["canonical_result_hash"] == "a" * 64
    assert data["stock_pool"]["canonical_result_hash"] == "a" * 64
    assert data["overview"]["real_trading_enabled"] is False
    assert data["overview"]["real_trading_safety_verified"] is True
    assert data["real_trading_safety"]["verified"] is True
    assert data["overview"]["run"]["portfolio"]["targets"] == [{
        "stock_code": "000001",
        "short_name": "平安银行",
        "rank_no": 1,
        "target_weight": 0.05,
    }]
    assert data["acceptance"] == {
        "same_run_uid": True,
        "exact_trade_date": True,
        "canonical_completed": True,
        "strategy_pool_readable": True,
        "stock_pool_readable": True,
        "canonical_pool_build_matches_api": True,
        "both_schedulers_match_api": True,
        "release_build_identity_matches": True,
        "execution_session_mapped": True,
        "scheduler_healthy": True,
        "real_trading_off": True,
        "accepted": True,
    }
    assert data["decision_date"] == "2026-09-01"
    assert data["data_trade_date"] == "2026-09-01"
    assert data["execution_session_date"] == "2026-09-02"
    assert data["build_identity"] == {
        "api_build_sha": _DAILY_BUILD_SHA,
        "canonical_pool_build_sha": _DAILY_BUILD_SHA,
        "linux_scheduler_build_sha": _DAILY_BUILD_SHA,
        "qmt_scheduler_build_sha": _DAILY_BUILD_SHA,
        "canonical_pool_build_matches_api": True,
        "both_schedulers_match_api": True,
        "all_match": True,
        "reason_codes": [],
    }
    assert data["automatic_real_order_submission"] is False
    assert data["real_order_authority"] is False


def test_daily_result_without_date_binds_authoritative_closed_session(
    monkeypatch,
):
    decision_day = date(2026, 9, 1)
    execution_day = date(2026, 9, 2)
    pool = _daily_empty_pool()
    canonical_calls = []

    class Repository:
        engine = object()

    monkeypatch.delenv("PROBIGA_STRATEGY_GOVERNANCE_MODE", raising=False)
    monkeypatch.setattr(trading_v3, "_repo", Repository)
    monkeypatch.setattr(
        trading_v3,
        "authoritative_closed_trade_date",
        lambda engine: (
            decision_day if engine is Repository.engine else None
        ),
    )

    def canonical(trade_date, *, latest_as_of):
        canonical_calls.append((trade_date, latest_as_of))
        return {"pool": pool}

    monkeypatch.setattr(trading_v3, "canonical_governance_decision", canonical)
    _patch_daily_release_identity(
        monkeypatch,
        execution_session_date=execution_day,
    )
    monkeypatch.setattr(
        trading_v3,
        "_daily_real_trading_safety",
        lambda *args, **kwargs: _verified_daily_real_trading_safety(),
    )
    trading_v3._DAILY_RESULT_CACHE.clear()

    result = trading_v3.daily_result()["data"]

    assert canonical_calls == [(decision_day, False)]
    assert result["date_resolution"] == "AUTHORITATIVE_CLOSED_TRADE_DATE"
    assert result["requested_trade_date"] == decision_day.isoformat()
    assert result["decision_date"] == decision_day.isoformat()
    assert result["data_trade_date"] == decision_day.isoformat()
    assert result["execution_session_date"] == execution_day.isoformat()
    assert result["delivery_status"] == "COMPLETED"


def test_daily_result_with_date_exposes_authoritative_closed_session(
    monkeypatch,
):
    selected = date(2026, 9, 1)
    authoritative = date(2026, 9, 2)

    class Repository:
        engine = object()

    monkeypatch.setattr(trading_v3, "_repo", Repository)
    monkeypatch.setattr(
        trading_v3,
        "authoritative_closed_trade_date",
        lambda engine: authoritative if engine is Repository.engine else None,
    )
    monkeypatch.setattr(
        trading_v3,
        "canonical_governance_decision",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        trading_v3,
        "_analysis_runtime_context",
        lambda *args, **kwargs: None,
    )
    _patch_daily_release_identity(monkeypatch)
    monkeypatch.setattr(
        trading_v3,
        "_daily_real_trading_safety",
        lambda *args, **kwargs: _verified_daily_real_trading_safety(),
    )
    trading_v3._DAILY_RESULT_CACHE.clear()

    result = trading_v3.daily_result(selected, force=True)["data"]

    assert result["requested_trade_date"] == selected.isoformat()
    assert result["date_resolution"] == "EXPLICIT_DECISION_DATE"
    assert result["authoritative_closed_trade_date"] == authoritative.isoformat()


def test_daily_result_blocks_when_execution_session_cannot_be_mapped(
    monkeypatch,
):
    selected = date(2026, 9, 1)
    pool = _daily_empty_pool()

    class Repository:
        engine = object()

    monkeypatch.delenv("PROBIGA_STRATEGY_GOVERNANCE_MODE", raising=False)
    monkeypatch.setattr(trading_v3, "_repo", Repository)
    monkeypatch.setattr(
        trading_v3,
        "canonical_governance_decision",
        lambda *args, **kwargs: {"pool": pool},
    )
    monkeypatch.setattr(
        trading_v3,
        "code_version",
        lambda: (_DAILY_BUILD_SHA, "test"),
    )
    monkeypatch.setattr(
        trading_v3,
        "_next_execution_session_date",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("NEXT_TRADE_SESSION_UNAVAILABLE")
        ),
    )
    monkeypatch.setattr(
        trading_v3,
        "_daily_scheduler_health",
        lambda *args, **kwargs: _healthy_daily_scheduler(),
    )
    monkeypatch.setattr(
        trading_v3,
        "_daily_real_trading_safety",
        lambda *args, **kwargs: _verified_daily_real_trading_safety(),
    )
    trading_v3._DAILY_RESULT_CACHE.clear()

    response = trading_v3.daily_result(selected, force=True)

    assert response["status"] == "blocked"
    assert response["data"]["delivery_status"] == "DATA_BLOCKED"
    assert response["data"]["reason_code"] == (
        "DAILY_RESULT_EXECUTION_SESSION_UNAVAILABLE"
    )
    assert response["data"]["execution_session_date"] is None


@pytest.mark.parametrize(
    ("component", "reason_code"),
    [
        ("pool", "CANONICAL_POOL_BUILD_MISMATCH"),
        ("linux", "LINUX_SCHEDULER_BUILD_MISMATCH"),
        ("qmt", "QMT_SCHEDULER_BUILD_INVALID"),
    ],
)
def test_daily_result_blocks_any_release_build_identity_divergence(
    monkeypatch,
    component,
    reason_code,
):
    selected = date(2026, 9, 1)
    pool_build = "b" * 40 if component == "pool" else _DAILY_BUILD_SHA
    pool = _daily_empty_pool(build_sha=pool_build)
    scheduler = _healthy_daily_scheduler()
    if component == "linux":
        scheduler["roles"]["linux_standalone"]["current"][
            "build_sha"
        ] = "b" * 40
    if component == "qmt":
        scheduler["roles"]["qmt_windows_edge"]["current"][
            "build_sha"
        ] = "0" * 40

    class Repository:
        engine = object()

    monkeypatch.delenv("PROBIGA_STRATEGY_GOVERNANCE_MODE", raising=False)
    monkeypatch.setattr(trading_v3, "_repo", Repository)
    monkeypatch.setattr(
        trading_v3,
        "canonical_governance_decision",
        lambda *args, **kwargs: {"pool": pool},
    )
    monkeypatch.setattr(
        trading_v3,
        "code_version",
        lambda: (_DAILY_BUILD_SHA, "test"),
    )
    monkeypatch.setattr(
        trading_v3,
        "_next_execution_session_date",
        lambda *args, **kwargs: date(2026, 9, 2),
    )
    monkeypatch.setattr(
        trading_v3,
        "_daily_scheduler_health",
        lambda *args, **kwargs: scheduler,
    )
    monkeypatch.setattr(
        trading_v3,
        "_daily_real_trading_safety",
        lambda *args, **kwargs: _verified_daily_real_trading_safety(),
    )
    trading_v3._DAILY_RESULT_CACHE.clear()

    result = trading_v3.daily_result(selected, force=True)["data"]

    assert result["delivery_status"] == "DATA_BLOCKED"
    assert result["acceptance"]["release_build_identity_matches"] is False
    assert reason_code in result["build_identity"]["reason_codes"]


def test_daily_result_rejects_forged_pool_counts(monkeypatch):
    selected = date(2026, 9, 1)

    class Repository:
        engine = object()

        def stock_pool(self, *, trade_date, before_session_date=None):
            return {
                "run_uid": "forged-counts",
                "build_commit_sha": _DAILY_BUILD_SHA,
                "trade_date": selected.isoformat(),
                "decision_session_date": selected.isoformat(),
                "pool_status": "READY",
                "pool_readable": True,
                "run_status": "COMPLETED",
                "decision_integrity_verified": True,
                "source_system": "STRATEGY_GOVERNANCE",
                "decision_scope": "CANONICAL_GOVERNANCE",
                "canonical_result_hash": "b" * 64,
                "is_historical_fallback": False,
                "historical_read_only": False,
                "reason_codes": [],
                "items": [{
                    "stock_code": "000001",
                    "is_strategy_candidate": True,
                    "target": None,
                }],
                "summary": {
                    "stock_count": 2,
                    "strategy_candidate_count": 1,
                    "target_count": 0,
                },
                "strategy_execution": {
                    "strategy_count": 1,
                    "strategies": [{"strategy_key": "right_side_trend"}],
                },
            }

    monkeypatch.delenv("PROBIGA_STRATEGY_GOVERNANCE_MODE", raising=False)
    monkeypatch.setattr(trading_v3, "_repo", Repository)
    forged_pool = Repository().stock_pool(trade_date=selected)
    monkeypatch.setattr(
        trading_v3,
        "canonical_governance_decision",
        lambda *args, **kwargs: {"pool": forged_pool},
    )
    monkeypatch.setattr(
        trading_v3,
        "_analysis_runtime_context",
        lambda *args, **kwargs: None,
    )
    _patch_daily_release_identity(monkeypatch)
    monkeypatch.setattr(
        trading_v3,
        "_daily_real_trading_safety",
        lambda *args, **kwargs: _verified_daily_real_trading_safety(),
    )
    trading_v3._DAILY_RESULT_CACHE.clear()

    result = trading_v3.daily_result(selected, force=True)

    assert result["status"] == "unavailable"
    assert result["data"]["delivery_status"] == "UNAVAILABLE"
    assert result["data"]["acceptance"]["stock_pool_readable"] is False
    assert result["data"]["acceptance"]["same_run_uid"] is False
    assert "DAILY_RESULT_STOCK_COUNT_MISMATCH" in (
        result["data"]["context"]["reason_codes"]
    )


def test_daily_result_projection_is_bounded_and_keeps_pool_identity():
    pool = {
        "run_uid": "projection-run",
        "trade_date": "2026-09-01",
        "decision_session_date": "2026-09-01",
        "items": [{
            "stock_code": "000001",
            "is_strategy_candidate": True,
            "rejection": None,
            "features": {
                "theme_name": "银行",
                "unused_large_evidence": "x" * 10_000,
            },
        }, {
            "stock_code": "000002",
            "is_strategy_candidate": False,
            "rejection": None,
            "features": {"unused_large_evidence": "y" * 10_000},
        }, {
            "stock_code": "000003",
            "is_strategy_candidate": False,
            "rejection": {"reason_code": "RISK_REJECTED"},
            "features": {"theme_names": ["保险"], "unused": "z" * 1000},
        }],
        "summary": {
            "stock_count": 3,
            "strategy_candidate_count": 1,
            "target_count": 0,
        },
    }

    projected = trading_v3._daily_stock_pool_projection(pool)

    assert projected["run_uid"] == "projection-run"
    assert [row["stock_code"] for row in projected["items"]] == [
        "000001",
        "000003",
    ]
    assert projected["summary"]["source_stock_count"] == 3
    assert projected["summary"]["stock_count"] == 2
    assert projected["omitted_non_candidate_count"] == 1
    assert projected["items"][0]["features"] == {"theme_name": "银行"}
    assert projected["items"][1]["features"] == {"theme_names": ["保险"]}


def test_daily_context_uses_earliest_expiry_and_is_historical_read_only():
    pool = {
        "run_uid": "historical-run",
        "trade_date": "2020-01-02",
        "decision_session_date": "2020-01-02",
        "execution_session_date": "2020-01-03",
        "pool_status": "READY",
        "pool_readable": True,
        "run_status": "COMPLETED",
        "decision_integrity_verified": True,
        "source_system": "STRATEGY_GOVERNANCE",
        "decision_scope": "CANONICAL_GOVERNANCE",
        "canonical_result_hash": "c" * 64,
        "is_historical_fallback": False,
        "historical_read_only": False,
        "actionable_output_allowed": True,
        "items": [{
            "is_strategy_candidate": True,
            "target": {"rank_no": 1},
            "valid_until": "2020-01-03T10:00:00+08:00",
        }, {
            "is_strategy_candidate": True,
            "target": None,
            "valid_until": "2020-01-03T09:30:00+08:00",
        }],
        "summary": {
            "stock_count": 2,
            "strategy_candidate_count": 2,
            "target_count": 1,
        },
    }

    context = trading_v3._daily_context_from_pool(
        pool,
        requested_date=date(2020, 1, 2),
    )

    assert context["decision_integrity_verified"] is True
    assert context["valid_until"] == "2020-01-03T09:30:00+08:00"
    assert context["historical_read_only"] is True
    assert context["decision_scope"] == "RESEARCH_ONLY"
    assert context["paper_order_authority"] == "NONE"
    assert context["actionable_output_allowed"] is False


def test_decision_and_execution_sessions_use_strict_open_calendar_edges():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE si_trade_calendar (
                trade_date TEXT PRIMARY KEY,
                trade_status INTEGER NOT NULL
            )
        """))
        connection.execute(text("""
            INSERT INTO si_trade_calendar (trade_date, trade_status) VALUES
                ('2026-08-28', 1),
                ('2026-08-29', 0),
                ('2026-08-30', 0),
                ('2026-08-31', 1)
        """))

    assert trading_v3._next_execution_session_date(
        engine,
        date(2026, 8, 28),
    ) == date(2026, 8, 31)
    assert trading_v3._decision_date_for_execution_session(
        engine,
        date(2026, 8, 31),
    ) == date(2026, 8, 28)
    with pytest.raises(RuntimeError, match="NEXT_TRADE_SESSION_UNAVAILABLE"):
        trading_v3._next_execution_session_date(
            engine,
            date(2026, 8, 29),
        )


def test_daily_scheduler_health_requires_qmt_release_receipt(monkeypatch):
    connection = object()

    class Engine:
        def connect(self):
            return self

        def __enter__(self):
            return connection

        def __exit__(self, *args):
            return None

    monkeypatch.setattr(
        trading_v3,
        "get_scheduler_runtime_config",
        lambda: {"poll_seconds": 60},
    )
    monkeypatch.setattr(
        trading_v3,
        "check_linux_standalone_active_release",
        lambda conn, **kwargs: (
            True,
            {"errors": [], "current": {"instance_id": "linux-1"}},
        ),
    )
    monkeypatch.setattr(
        trading_v3,
        "check_qmt_windows_edge_release_receipt",
        lambda conn, **kwargs: (
            False,
            {
                "errors": ["release_receipt_not_unique"],
                "identity": {
                    "errors": [],
                    "current": {
                        "instance_id": "windows-1",
                        "build_sha": "a" * 40,
                    },
                },
                "immutable_reference_verified": False,
            },
        ),
    )

    result = trading_v3._daily_scheduler_health(
        Engine(),
        expected_build_sha="a" * 40,
    )

    assert result["healthy"] is False
    assert result["status"] == "UNHEALTHY"
    assert result["expected_poll_seconds"] == 60
    assert result["roles"]["linux_standalone"]["healthy"] is True
    assert result["roles"]["qmt_windows_edge"]["healthy"] is False
    assert result["roles"]["qmt_windows_edge"]["current"] == {
        "instance_id": "windows-1",
        "build_sha": "a" * 40,
    }
    assert result["roles"]["qmt_windows_edge"][
        "immutable_reference_verified"
    ] is False
    assert result["reason_codes"] == [
        "QMT_WINDOWS_EDGE_RELEASE_RECEIPT_NOT_UNIQUE"
    ]


def test_daily_result_blocks_same_build_qmt_without_release_receipt(monkeypatch):
    selected = date(2026, 9, 1)
    pool = _daily_empty_pool()
    scheduler = _healthy_daily_scheduler()
    scheduler["status"] = "UNHEALTHY"
    scheduler["healthy"] = False
    scheduler["roles"]["qmt_windows_edge"].update({
        "healthy": False,
        "immutable_reference_verified": False,
        "errors": ["release_receipt_not_unique"],
    })
    scheduler["reason_codes"] = [
        "QMT_WINDOWS_EDGE_RELEASE_RECEIPT_NOT_UNIQUE"
    ]

    class Repository:
        engine = object()

    monkeypatch.delenv("PROBIGA_STRATEGY_GOVERNANCE_MODE", raising=False)
    monkeypatch.setattr(trading_v3, "_repo", Repository)
    monkeypatch.setattr(
        trading_v3,
        "canonical_governance_decision",
        lambda *args, **kwargs: {"pool": pool},
    )
    monkeypatch.setattr(
        trading_v3,
        "code_version",
        lambda: (_DAILY_BUILD_SHA, "test"),
    )
    monkeypatch.setattr(
        trading_v3,
        "_next_execution_session_date",
        lambda *args, **kwargs: date(2026, 9, 2),
    )
    monkeypatch.setattr(
        trading_v3,
        "_daily_scheduler_health",
        lambda *args, **kwargs: scheduler,
    )
    monkeypatch.setattr(
        trading_v3,
        "_daily_real_trading_safety",
        lambda *args, **kwargs: _verified_daily_real_trading_safety(),
    )
    trading_v3._DAILY_RESULT_CACHE.clear()

    response = trading_v3.daily_result(selected, force=True)

    assert response["status"] == "blocked"
    assert response["data"]["delivery_status"] == "DATA_BLOCKED"
    assert response["data"]["reason_code"] == (
        "QMT_EDGE_RELEASE_RECEIPT_UNAVAILABLE"
    )
    assert response["data"]["build_identity"]["all_match"] is True
    assert response["data"]["acceptance"]["scheduler_healthy"] is False
    assert response["data"]["acceptance"]["accepted"] is False


@pytest.mark.parametrize(
    ("enabled", "expected_verified", "expected_status", "expected_reason"),
    [
        (0, True, "SAFE", None),
        (1, False, "BLOCKED", "REAL_TRADING_SWITCH_ENABLED"),
    ],
)
def test_daily_real_trading_safety_reads_database_switch_and_guards(
    enabled,
    expected_verified,
    expected_status,
    expected_reason,
):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_trade_account_v2 (
                account_id TEXT PRIMARY KEY,
                real_trading_enabled INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            )
        """))
        connection.execute(
            text("""
                INSERT INTO st_trade_account_v2 (
                    account_id, real_trading_enabled, updated_at
                ) VALUES (
                    'paper-main-v2', :enabled, '2026-09-01 22:30:00'
                )
            """),
            {"enabled": enabled},
        )

    class Repository:
        def __init__(self):
            self.engine = engine

        def real_trading_guard_readiness(self):
            return {
                "account_insert": True,
                "account_update": True,
                "execution_plan_insert": True,
                "execution_plan_update": True,
            }

    result = trading_v3._daily_real_trading_safety(Repository())

    assert result["verified"] is expected_verified
    assert result["status"] == expected_status
    assert result["account_count"] == 1
    assert result["enabled_account_count"] == enabled
    assert result["accounts"] == [{
        "account_id": "paper-main-v2",
        "real_trading_enabled": bool(enabled),
        "updated_at": "2026-09-01T22:30:00+08:00",
    }]
    assert all(result["guards"].values())
    if expected_reason is None:
        assert result["reason_codes"] == []
    else:
        assert expected_reason in result["reason_codes"]


def test_daily_real_trading_safety_query_failure_is_fail_closed():
    class Engine:
        def connect(self):
            raise TimeoutError("database unavailable")

    class Repository:
        engine = Engine()

        def real_trading_guard_readiness(self):
            return {
                name: True
                for name in trading_v3._DAILY_REAL_TRADING_GUARDS
            }

    result = trading_v3._daily_real_trading_safety(Repository())

    assert result["status"] == "UNAVAILABLE"
    assert result["verified"] is False
    assert result["real_trading_enabled"] is None
    assert result["reason_codes"] == [
        "REAL_TRADING_SAFETY_READ_FAILED",
        "TimeoutError",
    ]


@pytest.mark.parametrize(
    "result_hash",
    ["f" * 63, "f" * 65, "g" * 64, "00" * 31 + "ZZ"],
)
def test_daily_context_rejects_non_64_hex_canonical_hash(result_hash):
    selected = date(2026, 9, 1)
    context = trading_v3._daily_context_from_pool(
        {
            "run_uid": "invalid-hash-run",
            "trade_date": selected.isoformat(),
            "decision_session_date": selected.isoformat(),
            "pool_status": "EMPTY",
            "pool_readable": True,
            "run_status": "COMPLETED",
            "decision_integrity_verified": True,
            "source_system": "STRATEGY_GOVERNANCE",
            "decision_scope": "CANONICAL_GOVERNANCE",
            "canonical_result_hash": result_hash,
            "items": [],
            "summary": {
                "stock_count": 0,
                "strategy_candidate_count": 0,
                "target_count": 0,
            },
        },
        requested_date=selected,
    )

    assert context["decision_integrity_verified"] is False
    assert context["canonical_result_hash"] == ""
    assert "DAILY_RESULT_CANONICAL_HASH_INVALID" in context["reason_codes"]


def test_daily_result_blocks_when_database_real_trading_switch_is_enabled(
    monkeypatch,
):
    selected = date(2026, 9, 1)
    pool = {
        "run_uid": "unsafe-switch-run",
        "build_commit_sha": _DAILY_BUILD_SHA,
        "trade_date": selected.isoformat(),
        "decision_session_date": selected.isoformat(),
        "decision_at": "2026-09-01T22:35:00+08:00",
        "pool_status": "EMPTY",
        "pool_readable": True,
        "run_status": "COMPLETED",
        "decision_integrity_verified": True,
        "source_system": "STRATEGY_GOVERNANCE",
        "decision_scope": "CANONICAL_GOVERNANCE",
        "canonical_result_hash": "d" * 64,
        "items": [],
        "summary": {
            "stock_count": 0,
            "strategy_candidate_count": 0,
            "target_count": 0,
        },
        "strategy_execution": {
            "strategy_count": 0,
            "strategies": [],
        },
    }

    class Repository:
        engine = object()

    monkeypatch.delenv("PROBIGA_STRATEGY_GOVERNANCE_MODE", raising=False)
    monkeypatch.setattr(trading_v3, "_repo", Repository)
    monkeypatch.setattr(
        trading_v3,
        "canonical_governance_decision",
        lambda *args, **kwargs: {"pool": pool},
    )
    _patch_daily_release_identity(monkeypatch)
    monkeypatch.setattr(
        trading_v3,
        "_daily_real_trading_safety",
        lambda *args, **kwargs: {
            "status": "BLOCKED",
            "verified": False,
            "real_trading_enabled": True,
            "reason_codes": ["REAL_TRADING_SWITCH_ENABLED"],
        },
    )
    trading_v3._DAILY_RESULT_CACHE.clear()

    result = trading_v3.daily_result(selected, force=True)
    data = result["data"]

    assert result["status"] == "blocked"
    assert data["delivery_status"] == "DATA_BLOCKED"
    assert data["reason_code"] == "REAL_TRADING_SWITCH_ENABLED"
    assert data["acceptance"]["real_trading_off"] is False
    assert data["acceptance"]["accepted"] is False
    assert data["overview"]["real_trading_enabled"] is True
    assert data["overview"]["real_trading_safety_verified"] is False


def test_daily_result_cache_is_copy_isolated_and_expires(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr(trading_v3, "monotonic", lambda: clock[0])
    trading_v3._DAILY_RESULT_CACHE.clear()
    trading_v3._daily_result_cache_set(
        "2026-09-01",
        {"context": {"run_uid": "immutable-run"}},
    )

    first = trading_v3._daily_result_cache_get("2026-09-01")
    assert first is not None
    first["context"]["run_uid"] = "tampered"

    second = trading_v3._daily_result_cache_get("2026-09-01")
    assert second is not None
    assert second["context"]["run_uid"] == "immutable-run"

    clock[0] += trading_v3._DAILY_RESULT_CACHE_SECONDS
    assert trading_v3._daily_result_cache_get("2026-09-01") is None


def test_daily_result_exposes_upstream_data_block_without_empty_pool(
    monkeypatch,
):
    selected = date(2026, 9, 1)

    class Repository:
        engine = object()

        def stock_pool(self, *, trade_date, before_session_date=None):
            return {
                "run_uid": None,
                "trade_date": selected.isoformat(),
                "decision_session_date": selected.isoformat(),
                "pool_status": "UNAVAILABLE",
                "pool_readable": False,
                "run_status": None,
                "decision_integrity_verified": False,
                "reason_codes": ["NO_VERIFIED_COMPLETED_DECISION_RUN"],
                "items": [],
                "summary": {
                    "stock_count": 0,
                    "strategy_candidate_count": 0,
                    "target_count": 0,
                },
            }

    monkeypatch.delenv("PROBIGA_STRATEGY_GOVERNANCE_MODE", raising=False)
    monkeypatch.setattr(trading_v3, "_repo", Repository)
    monkeypatch.setattr(
        trading_v3,
        "canonical_governance_decision",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        trading_v3,
        "_analysis_runtime_context",
        lambda *args, **kwargs: {
            "run_uid": None,
            "requested_date": selected.isoformat(),
            "decision_session_date": selected.isoformat(),
            "data_date": None,
            "run_status": "DATA_BLOCKED",
            "data_status": "DATA_BLOCKED",
            "decision_status": "BLOCKED",
            "decision_integrity_verified": False,
            "reason_codes": ["KLINE_FEATURE_QUERY_TIMEOUT"],
            "data_blocked_reason": "90 日 K 线阶段超时",
            "_envelope_status": "blocked",
        },
    )
    _patch_daily_release_identity(monkeypatch)
    monkeypatch.setattr(
        trading_v3,
        "_daily_real_trading_safety",
        lambda *args, **kwargs: _verified_daily_real_trading_safety(),
    )
    trading_v3._DAILY_RESULT_CACHE.clear()

    result = trading_v3.daily_result(selected, force=True)

    assert result["status"] == "blocked"
    assert result["data"]["delivery_status"] == "DATA_BLOCKED"
    assert result["data"]["reason_code"] == "KLINE_FEATURE_QUERY_TIMEOUT"
    assert result["data"]["stock_pool"]["pool_status"] == "UNAVAILABLE"
    assert result["data"]["acceptance"]["accepted"] is False


def test_daily_result_never_promotes_native_v3_run_to_canonical(monkeypatch):
    selected = date(2026, 9, 1)

    class Repository:
        engine = object()

        def stock_pool(self, **kwargs):
            raise AssertionError("daily-result must not read a native V3 pool")

    monkeypatch.delenv("PROBIGA_STRATEGY_GOVERNANCE_MODE", raising=False)
    monkeypatch.setattr(trading_v3, "_repo", Repository)

    def canonical(trade_date, *, latest_as_of):
        assert trade_date == selected
        assert latest_as_of is False
        return None

    monkeypatch.setattr(trading_v3, "canonical_governance_decision", canonical)
    monkeypatch.setattr(
        trading_v3,
        "_analysis_runtime_context",
        lambda *args, **kwargs: None,
    )
    _patch_daily_release_identity(monkeypatch)
    monkeypatch.setattr(
        trading_v3,
        "_daily_real_trading_safety",
        lambda *args, **kwargs: _verified_daily_real_trading_safety(),
    )
    trading_v3._DAILY_RESULT_CACHE.clear()

    result = trading_v3.daily_result(selected, force=True)

    assert result["status"] == "unavailable"
    assert result["data"]["delivery_status"] == "UNAVAILABLE"
    assert result["data"]["reason_code"] == (
        "EXACT_CANONICAL_POOL_NOT_AVAILABLE"
    )
    assert result["data"]["stock_pool"]["source_system"] == (
        "STRATEGY_GOVERNANCE"
    )
    assert result["data"]["stock_pool"]["run_uid"] is None
    assert result["data"]["acceptance"]["canonical_completed"] is False
    assert result["data"]["acceptance"]["same_run_uid"] is False


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
    decision_day = date(2026, 8, 28)
    execution_day = date(2026, 8, 31)

    class Repository:
        def __init__(self):
            self.engine = engine

    monkeypatch.setattr(trading_v3, "_repo", Repository)
    monkeypatch.setattr(
        trading_v3,
        "_decision_date_for_execution_session",
        lambda source_engine, session_date: (
            decision_day
            if source_engine is engine and session_date == execution_day
            else (_ for _ in ()).throw(AssertionError("wrong session mapping"))
        ),
    )

    def canonical(trade_date, *, latest_as_of):
        assert trade_date == decision_day
        assert latest_as_of is False
        return {"pool": {
            "run_uid": "same-run",
            "trade_date": decision_day.isoformat(),
            "decision_integrity_verified": True,
            "pool_readable": True,
            "items": [{
                "stock_code": "000001",
                "is_strategy_candidate": True,
            }],
        }}

    monkeypatch.setattr(trading_v3, "canonical_governance_decision", canonical)

    def build(source_engine, pool, *, session_date, cutoff_at):
        assert source_engine is engine
        assert pool["run_uid"] == "same-run"
        assert pool["decision_date"] == decision_day.isoformat()
        assert pool["execution_session_date"] == execution_day.isoformat()
        assert session_date == execution_day
        assert cutoff_at == datetime(2026, 8, 31, 9, 25, 59)
        return {
            "status": "COMPLETED",
            "session_date": execution_day.isoformat(),
            "source_run_uid": "same-run",
            "assessments": [],
            "order_authority": False,
            "automatic_substitution": False,
        }

    monkeypatch.setattr(trading_v3, "build_premarket_gate", build)

    result = trading_v3.premarket_auction_gate(
        execution_session_date=execution_day,
    )["data"]

    assert result["status"] == "COMPLETED"
    assert result["decision_date"] == decision_day.isoformat()
    assert result["data_date"] == decision_day.isoformat()
    assert result["execution_session_date"] == execution_day.isoformat()
    assert result["source_run_uid"] == "same-run"
    assert result["order_authority"] is False
    assert result["automatic_substitution"] is False
    assert result["evidence_mode"] == "POINT_IN_TIME_REPLAY"


def test_v3_auction_gate_never_falls_back_from_execution_session_pool(
    monkeypatch,
):
    engine = object()
    decision_day = date(2026, 8, 28)
    execution_day = date(2026, 8, 31)
    canonical_calls = []

    class Repository:
        def __init__(self):
            self.engine = engine

    monkeypatch.setattr(trading_v3, "_repo", Repository)
    monkeypatch.setattr(
        trading_v3,
        "_decision_date_for_execution_session",
        lambda source_engine, session_date: decision_day,
    )

    def canonical(trade_date, *, latest_as_of):
        canonical_calls.append((trade_date, latest_as_of))
        return None

    monkeypatch.setattr(trading_v3, "canonical_governance_decision", canonical)
    monkeypatch.setattr(
        trading_v3,
        "build_premarket_gate",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("gate must not run without exact canonical pool")
        ),
    )

    response = trading_v3.premarket_auction_gate(
        execution_session_date=execution_day,
    )
    result = response["data"]

    assert response["status"] == "blocked"
    assert canonical_calls == [(decision_day, False)]
    assert result["status"] == "DATA_BLOCKED"
    assert result["reason_code"] == (
        "EXACT_CANONICAL_DECISION_POOL_NOT_AVAILABLE"
    )
    assert result["decision_date"] == decision_day.isoformat()
    assert result["execution_session_date"] == execution_day.isoformat()

    with pytest.raises(HTTPException) as exc_info:
        trading_v3.premarket_auction_gate(
            execution_session_date=execution_day,
            trade_date=decision_day,
        )
    assert exc_info.value.status_code == 422


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
