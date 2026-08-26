from __future__ import annotations

import asyncio
import json

import pytest
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse

from server.api import main as api_main
from server.api.routers import health, strategy_center
from server.common.strategy_governance_mode import (
    STRATEGY_GOVERNANCE_BASE_SCHEMA_READY_ENV,
    STRATEGY_GOVERNANCE_MODE_ENV,
    StrategyGovernanceMode,
    StrategyGovernanceModeError,
    get_strategy_governance_mode,
)
from server.trading_v3 import paper_execution


def test_governance_mode_defaults_to_required_and_requires_exact_value(
    monkeypatch,
) -> None:
    monkeypatch.delenv(STRATEGY_GOVERNANCE_MODE_ENV, raising=False)
    assert get_strategy_governance_mode() is StrategyGovernanceMode.REQUIRED

    monkeypatch.setenv(STRATEGY_GOVERNANCE_MODE_ENV, "DEFERRED_DB")
    assert (
        get_strategy_governance_mode()
        is StrategyGovernanceMode.DEFERRED_DB
    )

    monkeypatch.setenv(STRATEGY_GOVERNANCE_MODE_ENV, "deferred_db")
    with pytest.raises(StrategyGovernanceModeError):
        get_strategy_governance_mode()


def _stub_production_health(monkeypatch) -> None:
    monkeypatch.setattr(
        health,
        "_deployed_git_revision",
        lambda: {
            "deployment_mode": "production",
            "expected_sha_configured": True,
            "matches_expected": True,
            "code_worktree_clean": True,
        },
    )
    monkeypatch.setattr(
        health, "_deployed_adata_revision", lambda: {"verified": True},
    )
    monkeypatch.setattr(health, "admin_auth_status", lambda: {"ready": True})
    monkeypatch.setattr(
        health, "_primary_database_readiness",
        lambda: {"status": "ok", "ready": True},
    )
    monkeypatch.setattr(
        health, "_scheduler_script_policy_readiness",
        lambda: {"status": "ok", "ready": True},
    )
    monkeypatch.setattr(
        health,
        "scheduler_runtime_info",
        lambda: {
            "embedded_scheduler_enabled": False,
            "embedded_scheduler_running": False,
        },
    )
    monkeypatch.setattr(health, "scheduler_authority_contract", lambda: {})
    monkeypatch.setattr(
        health,
        "_standalone_scheduler_status",
        lambda: {
            "verified": True,
            "active": True,
            "enabled": True,
            "pid": 4321,
        },
    )
    monkeypatch.setattr(
        health, "_detached_job_log_readiness",
        lambda: {"status": "ok", "ready": True},
    )


def test_deferred_production_health_skips_governance_schema_and_build_heartbeat(
    monkeypatch,
) -> None:
    monkeypatch.setenv(STRATEGY_GOVERNANCE_MODE_ENV, "DEFERRED_DB")
    monkeypatch.setenv(STRATEGY_GOVERNANCE_BASE_SCHEMA_READY_ENV, "true")
    _stub_production_health(monkeypatch)
    monkeypatch.setattr(
        health,
        "_strategy_funding_schema_readiness",
        lambda: (_ for _ in ()).throw(
            AssertionError("deferred health must not query governance schema")
        ),
    )
    monkeypatch.setattr(
        health,
        "_standalone_scheduler_heartbeat_readiness",
        lambda _pid: (_ for _ in ()).throw(
            AssertionError("deferred health must not require same-build heartbeat")
        ),
    )

    payload = health.health()

    assert payload["status"] == "degraded"
    assert payload["strategy_governance_mode"] == "DEFERRED_DB"
    assert payload["schema_ready"] is False
    assert payload["base_schema_ready"] is True
    assert payload["governance_ready"] is False
    assert payload["activation_enabled"] is False
    assert payload["automatic_real_order_submission"] is False
    assert payload["real_order_authority"] is False
    assert payload["database"]["ready"] is True
    assert payload["standalone_scheduler"]["active"] is True
    assert payload["standalone_scheduler"]["enabled"] is True
    assert payload["strategy_funding_schema"]["ready"] is False
    assert payload["standalone_scheduler_heartbeat"]["ready"] is False


def test_deferred_production_health_still_requires_primary_database(
    monkeypatch,
) -> None:
    monkeypatch.setenv(STRATEGY_GOVERNANCE_MODE_ENV, "DEFERRED_DB")
    _stub_production_health(monkeypatch)
    monkeypatch.setattr(
        health, "_primary_database_readiness",
        lambda: {"status": "error", "ready": False},
    )

    with pytest.raises(HTTPException, match="primary database readiness"):
        health.health()


def test_deferred_production_health_still_requires_standalone_scheduler(
    monkeypatch,
) -> None:
    monkeypatch.setenv(STRATEGY_GOVERNANCE_MODE_ENV, "DEFERRED_DB")
    _stub_production_health(monkeypatch)
    monkeypatch.setattr(
        health,
        "_standalone_scheduler_status",
        lambda: {
            "verified": True,
            "active": False,
            "enabled": True,
            "pid": None,
        },
    )

    with pytest.raises(HTTPException, match="standalone scheduler activity"):
        health.health()


def test_deferred_overview_is_cash_only_without_governance_database_access(
    monkeypatch,
) -> None:
    monkeypatch.setenv(STRATEGY_GOVERNANCE_MODE_ENV, "DEFERRED_DB")
    monkeypatch.setenv(STRATEGY_GOVERNANCE_BASE_SCHEMA_READY_ENV, "true")
    monkeypatch.setattr(
        strategy_center,
        "load_canonical_governance_snapshot",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("canonical governance DB must not be read")
        ),
    )
    monkeypatch.setattr(
        strategy_center,
        "canonical_unavailable_context",
        lambda: (_ for _ in ()).throw(
            AssertionError("canonical context DB must not be read")
        ),
    )

    payload = strategy_center.strategy_center_governance("2026-08-26")

    assert payload["reason_code"] == "GOVERNANCE_DATABASE_DEFERRED"
    assert payload["strategies"] == []
    assert payload["combinations"] == []
    assert payload["pools"] == {
        "observation": [], "confirmation": [], "tradable": [],
    }
    assert payload["allocations"] == [{
        "target_type": "CASH",
        "target_key": "cash",
        "name": "现金",
        "simulated_weight_pct": 100.0,
        "reason": "治理数据库迁移待完成，禁止新增买入",
        "real_order_authority": False,
    }]
    assert payload["activation_enabled"] is False
    assert payload["base_schema_ready"] is True


@pytest.mark.parametrize(
    ("method", "path", "blocked"),
    [
        ("GET", "/api/strategy-center/governance", False),
        ("HEAD", "/api/strategy-center/governance", False),
        ("GET", "/api/strategy-center/governance/history", True),
        ("POST", "/api/strategy-center/governance/run", True),
        ("POST", "/api/strategy-center/strategies/a/toggle", True),
        ("GET", "/api/strategy-center/overview", False),
        ("POST", "/api/unrelated", False),
    ],
)
def test_deferred_governance_request_boundary(
    method: str, path: str, blocked: bool,
) -> None:
    assert api_main._deferred_governance_request_blocked(method, path) is blocked


def test_deferred_governance_middleware_returns_explicit_503(
    monkeypatch,
) -> None:
    monkeypatch.setenv(STRATEGY_GOVERNANCE_MODE_ENV, "DEFERRED_DB")
    request = Request({
        "type": "http",
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/api/strategy-center/governance/history",
        "raw_path": b"/api/strategy-center/governance/history",
        "query_string": b"",
        "headers": [],
        "client": ("127.0.0.1", 1234),
        "server": ("testserver", 80),
    })

    async def unexpected_next(_request):
        raise AssertionError("blocked request reached the route")

    response = asyncio.run(
        api_main.enforce_deferred_governance_boundary(
            request, unexpected_next,
        )
    )
    body = json.loads(response.body)

    assert response.status_code == 503
    assert body["error"] == "governance_database_deferred"
    assert body["activation_enabled"] is False
    assert body["real_order_authority"] is False


def test_deferred_buy_receipt_blocks_before_any_database_query(
    monkeypatch,
) -> None:
    monkeypatch.setenv(STRATEGY_GOVERNANCE_MODE_ENV, "DEFERRED_DB")

    class NoDatabaseAccess:
        def execute(self, *_args, **_kwargs):
            raise AssertionError("deferred BUY receipt touched governance DB")

    receipt, reason = paper_execution._canonical_governance_buy_receipt(
        NoDatabaseAccess(),
        trade_date=paper_execution.date(2026, 8, 26),
        stock_code="600036",
        strategy_keys=["right_side_trend"],
    )

    assert receipt is None
    assert reason == "GOVERNANCE_DATABASE_DEFERRED"
