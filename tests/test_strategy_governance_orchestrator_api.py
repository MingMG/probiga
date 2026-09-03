from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from fastapi.responses import JSONResponse

from server.api.routers import strategy_center as router
from server.engine import strategy_governance as governance_module


def _admin_request():
    return SimpleNamespace(state=SimpleNamespace(
        auth_kind="account_session",
        auth_user=SimpleNamespace(
            id=7,
            username="admin",
            role="ADMIN",
            is_active=True,
        ),
    ))


def _completed_orchestration() -> dict:
    run_uid = "a" * 32
    trade_date = "2026-08-21"
    manifest, _candidates = (
        governance_module._build_funding_checkpoint_manifest(
            run_uid=run_uid,
            trade_date=trade_date,
            strategies=[],
            combinations=[],
        )
    )
    coverage = manifest["coverage"]
    return {
        "status": "ok",
        "orchestration_status": "COMPLETED",
        "reason_code": "GOVERNANCE_COMPLETED",
        "target_trade_date": trade_date,
        "trade_date": trade_date,
        "run_uid": run_uid,
        "result_mode": "CANONICAL_PERSISTED",
        "is_canonical": True,
        "build_commit_sha": "WORKTREE_UNVERSIONED",
        "canonical_result_hash": "c" * 64,
        "decision_contract_version": "strategy-governance-decision.v7",
        "statistical_funding_eligible": True,
        "strategies": [],
        "combinations": [],
        "pools": {
            "observation": [],
            "confirmation": [],
            "tradable": [],
        },
        "funding_checkpoint_manifest": manifest,
        "summary": {
            "funding_checkpoint_manifest_hash": manifest["manifest_hash"],
            "funding_checkpoint_eligible_count": coverage["eligible_count"],
            "funding_checkpointed_count": coverage["checkpointed_count"],
            "funding_strategy_checkpoint_count": coverage[
                "strategy_checkpoint_count"
            ],
            "funding_combination_recipe_count": coverage[
                "combination_recipe_count"
            ],
            "funding_ready_count": coverage["funding_ready_count"],
            "funding_checkpoint_ineligible_count": coverage[
                "ineligible_count"
            ],
        },
        "allocations": [{
            "target_type": "CASH",
            "simulated_weight_pct": 100.0,
            "real_order_authority": False,
        }],
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


@pytest.mark.parametrize(
    ("unsafe_payload", "expected_status"),
    [
        ({"automatic_real_order_submission": False}, 500),
        ({
            "automatic_real_order_submission": True,
            "real_order_authority": False,
        }, 500),
        ({
            "automatic_real_order_submission": False,
            "real_order_authority": "false",
        }, 500),
        ({
            "automatic_real_order_submission": False,
            "real_order_authority": False,
            "adapters": [{
                "real_order_submission_enabled": False,
                "automatic_real_order_submission": False,
            }],
            "dynamic_version_readiness": [],
        }, 500),
        ({
            "automatic_real_order_submission": False,
            "real_order_authority": False,
            "adapters": [],
            "dynamic_version_readiness": [{
                "real_order_submission_enabled": False,
                "automatic_real_order_submission": False,
                "real_order_authority": "false",
            }],
        }, 500),
    ],
)
def test_adapter_capability_api_does_not_mask_unsafe_authority_contract(
    monkeypatch, unsafe_payload, expected_status,
):
    monkeypatch.setattr(
        router, "strategy_execution_adapter_capabilities",
        lambda: dict(unsafe_payload),
    )

    response = router.strategy_center_governance_adapter_capabilities()

    assert response.status_code == expected_status
    body = json.loads(response.body.decode("utf-8"))
    assert body["error"] == "invalid_adapter_capability_contract"
    assert body["automatic_real_order_submission"] is False
    assert body["real_order_authority"] is False


def test_adapter_capability_api_preserves_explicit_closed_authority(monkeypatch):
    monkeypatch.setattr(
        router, "strategy_execution_adapter_capabilities",
        lambda: {
            "schema": "probiga.strategy-adapter-capabilities.v1",
            "adapters": [],
            "dynamic_version_readiness": [],
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        },
    )

    response = router.strategy_center_governance_adapter_capabilities()

    assert response["status"] == "ok"
    assert response["automatic_real_order_submission"] is False
    assert response["real_order_authority"] is False


def test_manual_api_delegates_to_shared_orchestrator(monkeypatch):
    captured = {}

    def orchestrate(**kwargs):
        captured.update(kwargs)
        return {
            "status": "not_due",
            "orchestration_status": "NOT_DUE",
            "allocations": [{
                "target_type": "CASH",
                "simulated_weight_pct": 100.0,
                "real_order_authority": False,
            }],
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }

    monkeypatch.setattr(router, "orchestrate_strategy_governance", orchestrate)

    result = router.strategy_center_run_governance(
        router.StrategyRunRequest(trade_date="2026-08-21", limit=321),
        _admin_request(),
    )

    assert result["orchestration_status"] == "NOT_DUE"
    assert captured["requested_trade_date"] == "2026-08-21"
    assert captured["strategy_limit"] == 321
    assert captured["operator"] == "user-id:7"
    assert captured["allow_revision"] is True
    assert captured["governance_runner"] is router.governance_snapshot


def test_manual_api_accepts_only_complete_canonical_completion(monkeypatch):
    monkeypatch.setattr(
        router,
        "orchestrate_strategy_governance",
        lambda **_kwargs: _completed_orchestration(),
    )

    result = router.strategy_center_run_governance(
        router.StrategyRunRequest(trade_date="2026-08-21", limit=10),
        _admin_request(),
    )

    assert result["orchestration_status"] == "COMPLETED"
    assert result["run_uid"] == "a" * 32


@pytest.mark.parametrize(
    ("orchestration_status", "public_status"),
    [
        ("COMPLETED", "blocked"),
        ("NOT_DUE", "ok"),
        ("NOT_READY", "not_due"),
        ("INTEGRITY_ERROR", "ok"),
        ("PROGRAM_ERROR", "not_due"),
    ],
)
def test_manual_api_rejects_mismatched_public_and_orchestration_status(
    monkeypatch, orchestration_status, public_status,
):
    payload = {
        "status": public_status,
        "orchestration_status": orchestration_status,
        "allocations": [{
            "target_type": "CASH",
            "simulated_weight_pct": 100.0,
            "real_order_authority": False,
        }],
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    monkeypatch.setattr(
        router,
        "orchestrate_strategy_governance",
        lambda **_kwargs: payload,
    )

    response = router.strategy_center_run_governance(
        router.StrategyRunRequest(trade_date="2026-08-21", limit=10),
        _admin_request(),
    )

    assert isinstance(response, JSONResponse)
    assert response.status_code == 500
    assert json.loads(response.body)["error"] == (
        "invalid_governance_status_contract"
    )


@pytest.mark.parametrize("field", ["run_uid", "summary", "trade_date"])
def test_manual_api_rejects_incomplete_completed_identity(monkeypatch, field):
    payload = _completed_orchestration()
    if field == "trade_date":
        payload[field] = "2026-08-20"
    else:
        payload.pop(field)
    monkeypatch.setattr(
        router,
        "orchestrate_strategy_governance",
        lambda **_kwargs: payload,
    )

    response = router.strategy_center_run_governance(
        router.StrategyRunRequest(trade_date="2026-08-21", limit=10),
        _admin_request(),
    )

    assert isinstance(response, JSONResponse)
    assert response.status_code == 500
    assert json.loads(response.body)["error"] == (
        "invalid_governance_completion_contract"
    )


def test_canonical_governance_rankings_are_server_paged_and_revision_bound(
    monkeypatch,
):
    snapshot = _completed_orchestration()
    snapshot["strategies"] = [{
        "strategy_key": f"strategy_{index:04d}",
        "strategy_name": f"策略{index:04d}",
        "current_version": "v1",
        "rank": index + 1,
    } for index in range(750)]
    snapshot["combinations"] = [{
        "combination_key": f"combo_{index:04d}",
        "combination_name": f"组合{index:04d}",
        "current_version": "v1",
        "rank": index + 1,
    } for index in range(75)]
    monkeypatch.setattr(
        router,
        "load_canonical_governance_snapshot",
        lambda **_kwargs: snapshot,
    )

    overview = router.strategy_center_governance("2026-08-21")

    assert overview["ranking_response_bounded"] is True
    assert len(overview["strategies"]) == 50
    assert len(overview["combinations"]) == 50
    strategy_page = overview["ranking_pages"]["strategy"]
    assert strategy_page["total_count"] == 750
    assert strategy_page["offset"] == 0
    assert strategy_page["next_cursor"]
    assert strategy_page["page_hash"]

    second = router.strategy_center_governance_rankings(
        "STRATEGY",
        trade_date="2026-08-21",
        run_uid=snapshot["run_uid"],
        canonical_result_hash=snapshot["canonical_result_hash"],
        cursor=strategy_page["next_cursor"],
        limit=50,
        query="",
    )
    assert second["status"] == "ok"
    assert second["page"]["offset"] == 50
    assert len(second["page"]["rows"]) == 50
    assert second["page"]["rows"][0]["rank"] == 51
    assert second["page"]["previous_cursor"]

    tampered = router.strategy_center_governance_rankings(
        "STRATEGY",
        trade_date="2026-08-21",
        run_uid=snapshot["run_uid"],
        canonical_result_hash=snapshot["canonical_result_hash"],
        cursor="50." + "0" * 32,
        limit=50,
        query="",
    )
    assert isinstance(tampered, JSONResponse)
    assert tampered.status_code == 422


def test_ranking_page_rejects_cross_revision_mix(monkeypatch):
    snapshot = _completed_orchestration()
    snapshot["strategies"] = []
    snapshot["combinations"] = []
    monkeypatch.setattr(
        router,
        "load_canonical_governance_snapshot",
        lambda **_kwargs: snapshot,
    )

    response = router.strategy_center_governance_rankings(
        "STRATEGY",
        trade_date="2026-08-21",
        run_uid="b" * 32,
        canonical_result_hash=snapshot["canonical_result_hash"],
        cursor="",
        limit=50,
        query="",
    )

    assert isinstance(response, JSONResponse)
    assert response.status_code == 409
    assert json.loads(response.body)["error"] == (
        "canonical_governance_revision_changed"
    )


@pytest.mark.parametrize(
    ("orchestration_status", "expected_http_status"),
    [
        ("NOT_READY", 503),
        ("INTEGRITY_ERROR", 409),
        ("PROGRAM_ERROR", 500),
    ],
)
def test_manual_api_maps_non_success_orchestration_to_non_200_http(
    monkeypatch, orchestration_status, expected_http_status,
):
    monkeypatch.setattr(
        router,
        "orchestrate_strategy_governance",
        lambda **_kwargs: {
            "status": "blocked",
            "orchestration_status": orchestration_status,
            "reason_code": "fixture_failure",
            "allocations": [{
                "target_type": "CASH",
                "simulated_weight_pct": 100.0,
                "real_order_authority": False,
            }],
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        },
    )

    response = router.strategy_center_run_governance(
        router.StrategyRunRequest(trade_date="2026-08-21", limit=10),
        _admin_request(),
    )

    assert isinstance(response, JSONResponse)
    assert response.status_code == expected_http_status
    body = json.loads(response.body)
    assert body["orchestration_status"] == orchestration_status
    assert body["automatic_real_order_submission"] is False


@pytest.mark.parametrize(
    "unsafe_payload",
    [
        {
            "automatic_real_order_submission": True,
            "allocations": [{
                "simulated_weight_pct": 100.0,
                "real_order_authority": False,
            }],
        },
        {
            "allocations": [{
                "simulated_weight_pct": 100.0,
                "real_order_authority": False,
            }],
        },
        {
            "automatic_real_order_submission": "false",
            "allocations": [{
                "simulated_weight_pct": 100.0,
                "real_order_authority": False,
            }],
        },
        {
            "automatic_real_order_submission": False,
            "real_order_authority": True,
            "allocations": [{
                "simulated_weight_pct": 100.0,
                "real_order_authority": False,
            }],
        },
        {
            "automatic_real_order_submission": False,
            "real_order_authority": "false",
            "allocations": [{
                "simulated_weight_pct": 100.0,
                "real_order_authority": False,
            }],
        },
        {
            "automatic_real_order_submission": False,
            "allocations": [{"simulated_weight_pct": 100.0}],
        },
        {
            "automatic_real_order_submission": False,
            "allocations": [{
                "simulated_weight_pct": 100.0,
                "real_order_authority": "false",
            }],
        },
        {
            "automatic_real_order_submission": False,
            "allocations": [{
                "simulated_weight_pct": 100.0,
                "real_order_authority": False,
                "member_sleeves": [{"real_orders_allowed": True}],
            }],
        },
    ],
)
def test_manual_api_rejects_unsafe_or_missing_order_authority(
    monkeypatch, unsafe_payload,
):
    monkeypatch.setattr(
        router,
        "orchestrate_strategy_governance",
        lambda **_kwargs: {
            "status": "ok",
            "orchestration_status": "COMPLETED",
            **unsafe_payload,
        },
    )

    response = router.strategy_center_run_governance(
        router.StrategyRunRequest(trade_date="2026-08-21", limit=10),
        _admin_request(),
    )

    assert isinstance(response, JSONResponse)
    assert response.status_code == 409
    body = json.loads(response.body)
    assert body["error"] == "unsafe_governance_result"
    assert body["automatic_real_order_submission"] is False


def test_manual_api_rejects_unknown_orchestration_status(monkeypatch):
    monkeypatch.setattr(
        router,
        "orchestrate_strategy_governance",
        lambda **_kwargs: {"orchestration_status": "NEW_UNMAPPED_STATE"},
    )

    response = router.strategy_center_run_governance(
        router.StrategyRunRequest(trade_date="2026-08-21", limit=10),
        _admin_request(),
    )

    assert isinstance(response, JSONResponse)
    assert response.status_code == 500
    body = json.loads(response.body)
    assert body["error"] == "unknown_governance_status"
    assert body["automatic_real_order_submission"] is False


def test_legacy_run_is_http_410_and_never_calls_snapshot_writer(
    monkeypatch,
):
    monkeypatch.setattr(
        router,
        "build_strategy_center_snapshot",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("retired route must not build a snapshot")
        ),
    )
    from server.engine import strategy_center as strategy_center_engine

    monkeypatch.setattr(
        strategy_center_engine,
        "persist_strategy_center_snapshot",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("retired route must not call the writer")
        ),
    )

    response = router.strategy_center_run(
        router.StrategyRunRequest(trade_date="2026-08-21", limit=10),
        _admin_request(),
    )

    assert isinstance(response, JSONResponse)
    assert response.status_code == 410
    body = json.loads(response.body)
    assert body["error"] == "legacy_strategy_center_run_retired"
    assert body["replacement"] == "/api/strategy-center/governance/run"
    assert body["automatic_real_order_submission"] is False


def test_legacy_run_retirement_is_stable_for_an_empty_request():
    response = router.strategy_center_run(
        None,
        _admin_request(),
    )

    assert isinstance(response, JSONResponse)
    assert response.status_code == 410
    body = json.loads(response.body)
    assert body["status"] == "retired"
    assert body["real_order_authority"] is False
    assert body["automatic_real_order_submission"] is False


def test_canonical_unavailable_is_structured_and_hides_raw_db_error(monkeypatch):
    monkeypatch.setattr(
        router,
        "load_canonical_governance_snapshot",
        lambda **_kwargs: (_ for _ in ()).throw(
            RuntimeError("password=do-not-leak database host secret")
        ),
    )
    monkeypatch.setattr(
        router,
        "canonical_unavailable_context",
        lambda: {
            "authoritative_trade_date": "2026-08-21",
            "last_canonical": {
                "run_uid": "a" * 32,
                "trade_date": "2026-08-20",
                "run_revision": 2,
            },
        },
    )

    result = router.strategy_center_governance("2026-08-21")

    assert result["status"] == "degraded"
    assert result["result_mode"] == "CANONICAL_UNAVAILABLE"
    assert result["input_ready"] is False
    assert result["reason_code"] == "CANONICAL_UNAVAILABLE"
    assert result["blocking_stage"] == "CANONICAL_READ"
    assert result["authoritative_trade_date"] == "2026-08-21"
    assert result["last_canonical"]["trade_date"] == "2026-08-20"
    assert result["allocations"][0]["simulated_weight_pct"] == 100.0
    assert "error" not in result
    assert "password" not in str(result)


def test_all_degraded_get_endpoints_hide_database_credentials(
    monkeypatch, caplog,
):
    secret = "password=do-not-leak host=private-db.internal"

    def unavailable(*_args, **_kwargs):
        raise RuntimeError(secret)

    for dependency in (
        "build_strategy_center_snapshot",
        "latest_recommendation_date",
        "versioned_strategy_configuration",
        "governance_history",
        "load_etf_forward_ledger",
        "load_membership_snapshot_history",
        "load_qmt_kline_attestation_status",
    ):
        monkeypatch.setattr(router, dependency, unavailable)

    app = FastAPI()
    app.include_router(router.router, prefix="/api")
    client = TestClient(app)
    paths = (
        "/api/strategy-center/overview?trade_date=2026-08-21",
        "/api/strategy-center/market-state?trade_date=2026-08-21",
        "/api/strategy-center/configuration",
        "/api/strategy-center/governance/history",
        "/api/strategy-center/etf-forward",
        (
            "/api/strategy-center/membership-history"
            "?snapshot_date=2026-08-21&member_type=industry"
        ),
        "/api/strategy-center/qmt-kline-attestation",
        "/api/strategy-center/strategies?trade_date=2026-08-21",
        "/api/strategy-center/candidates?trade_date=2026-08-21",
        "/api/strategy-center/stock/600000?trade_date=2026-08-21",
        "/api/strategy-center/compare?trade_date=2026-08-21",
        "/api/strategy-center/conflicts?trade_date=2026-08-21",
    )

    for path in paths:
        response = client.get(path)
        assert response.status_code == 200
        body = response.json()
        assert body["status"] in {"degraded", "error"}
        assert body["error"] == "strategy_center_read_unavailable"
        assert body["message"] == "策略中心数据暂不可读取，请稍后重试"
        assert "do-not-leak" not in response.text
        assert "private-db.internal" not in response.text

    assert "Strategy center operation failed:" in caplog.text
    assert "exception_type=RuntimeError" in caplog.text
