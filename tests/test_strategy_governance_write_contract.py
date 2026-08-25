import math

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from pydantic import ValidationError

from server.api.routers import strategy_center as router
from server.api.strategy_evidence_request_limit import (
    STRATEGY_GOVERNANCE_REQUEST_MAX_BYTES,
    StrategyEvidenceRequestSizeMiddleware,
    StrategyGovernanceRequestTooLarge,
    strategy_governance_too_large_response,
)
from server.common.canonical_json import validate_canonical_json
from server.engine import strategy_governance as governance


def _overdeep_json():
    root = {}
    current = root
    for _index in range(33):
        current["next"] = {}
        current = current["next"]
    return root


def test_every_governance_write_model_forbids_unknown_fields():
    models = (
        router.StrategyToggleRequest,
        router.StrategyRunRequest,
        router.StrategyRegistrationRequest,
        router.StrategyCombinationMemberRequest,
        router.StrategyCombinationRequest,
        router.StrategyChallengerRegistrationRequest,
        router.StrategyChallengerReviewRequest,
        router.StrategyChallengerEvidenceRequest,
        router.StrategyChallengerPromotionRequest,
        router.LifecycleTransitionRequest,
        router.StrategyMetricEvidenceRequest,
        router.StrategyMetricReviewRequest,
    )
    assert all(model.model_config.get("extra") == "forbid" for model in models)

    with pytest.raises(ValidationError, match="extra_forbidden"):
        router.StrategyRunRequest(limit=1, limti=2)
    with pytest.raises(ValidationError, match="extra_forbidden"):
        router.StrategyCombinationMemberRequest(
            strategy_key="alpha", weight=0.5, unknown_member_field=True,
        )
    limit_description = router.StrategyRunRequest.model_fields[
        "limit"
    ].description
    assert "兼容参数" in limit_description
    assert "不作为任何权威输入的读取上限" in limit_description
    assert "不截断" in limit_description


def test_nested_combination_member_extra_is_http_422():
    app = FastAPI()
    app.include_router(router.router, prefix="/api")
    response = TestClient(app).post(
        "/api/strategy-center/combinations",
        json={
            "combination_key": "combo_alpha",
            "combination_name": "组合A",
            "version": "v1",
            "members": [
                {
                    "strategy_key": "alpha",
                    "weight": 0.5,
                    "unexpected": "silent-drop-must-not-happen",
                },
                {"strategy_key": "beta", "weight": 0.5},
            ],
        },
    )
    assert response.status_code == 422
    assert response.json()["detail"][0]["type"] == "extra_forbidden"


def test_ordinary_governance_write_is_capped_before_json_parsing():
    app = FastAPI()
    app.add_middleware(StrategyEvidenceRequestSizeMiddleware)

    @app.exception_handler(StrategyGovernanceRequestTooLarge)
    async def too_large_handler(_request, _exc):
        return strategy_governance_too_large_response()

    @app.post("/api/strategy-center/governance/run")
    async def receive_governance(request: Request):
        return {"size": len(await request.body())}

    response = TestClient(app).post(
        "/api/strategy-center/governance/run",
        content=b"x" * (STRATEGY_GOVERNANCE_REQUEST_MAX_BYTES + 1),
    )
    assert response.status_code == 413
    assert response.json()["error"] == "strategy_governance_request_too_large"
    assert response.json()["automatic_real_order_submission"] is False
    assert response.json()["real_order_authority"] is False


def test_ordinary_streaming_cap_rejects_forged_small_content_length():
    app = FastAPI()
    app.add_middleware(
        StrategyEvidenceRequestSizeMiddleware, governance_max_bytes=16,
    )

    @app.exception_handler(StrategyGovernanceRequestTooLarge)
    async def too_large_handler(_request, _exc):
        return strategy_governance_too_large_response()

    @app.post("/api/strategy-center/governance/run")
    async def receive_governance(request: Request):
        return {"size": len(await request.body())}

    response = TestClient(app).post(
        "/api/strategy-center/governance/run",
        content=b"x" * 17,
        headers={"content-length": "1"},
    )
    assert response.status_code == 413
    assert response.json()["error"] == "strategy_governance_request_too_large"


@pytest.mark.parametrize(
    "invalid",
    (
        {"value": math.nan},
        {"value": math.inf},
        {1: "non-string-key"},
        {"value": object()},
    ),
)
def test_canonical_governance_json_rejects_noncanonical_values(invalid):
    with pytest.raises(ValueError):
        validate_canonical_json(invalid)


def test_canonical_governance_json_rejects_depth_cycle_and_size():
    deep = {}
    current = deep
    for _index in range(33):
        current["next"] = {}
        current = current["next"]
    cycle = []
    cycle.append(cycle)

    with pytest.raises(ValueError, match="深度"):
        validate_canonical_json(deep)
    with pytest.raises(ValueError, match="循环引用"):
        validate_canonical_json(cycle)
    with pytest.raises(ValueError, match="不得超过"):
        validate_canonical_json({"large": "x" * (1024 * 1024)})


@pytest.mark.parametrize(
    "invalid_config",
    (
        {"bad": math.nan},
        {"bad": math.inf},
        {1: "non-string-key"},
        _overdeep_json(),
        {"large": "x" * (1024 * 1024)},
    ),
)
def test_direct_strategy_registration_rejects_noncanonical_json_before_db(
    monkeypatch, invalid_config,
):
    monkeypatch.setattr(governance, "ensure_and_seed_governance", lambda: None)
    monkeypatch.setattr(
        governance, "_db_read",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("invalid registration reached database")
        ),
    )
    with pytest.raises(ValueError):
        governance.register_strategy({
            "strategy_key": "strict_alpha",
            "strategy_name": "严格策略",
            "version": "v1",
            "evaluator_config": invalid_config,
            "parameters": {
                "max_holding_days": 5,
                "label_horizon_days": 5,
            },
        })


def test_direct_combination_and_lifecycle_reject_noncanonical_json(monkeypatch):
    monkeypatch.setattr(governance, "ensure_and_seed_governance", lambda: None)
    monkeypatch.setattr(
        governance, "_db_read",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("invalid governance JSON reached database")
        ),
    )
    with pytest.raises(ValueError):
        governance.register_combination({
            "combination_key": "strict_combo",
            "combination_name": "严格组合",
            "version": "v1",
            "members": [
                {"strategy_key": "alpha", "weight": 0.5},
                {"strategy_key": "beta", "weight": math.nan},
            ],
        })
    with pytest.raises(ValueError):
        governance.transition_lifecycle(
            "alpha", "SUSPENDED", reason="严格证据",
            operator="test", evidence={"bad": math.inf},
        )
