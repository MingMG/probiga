import json
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from server.api.routers import strategy_center as router_module
from server.engine.strategy_challenger_factory import (
    StrategyAlreadyRegisteredError,
)


def _request(user_id=1):
    user = SimpleNamespace(
        id=user_id,
        role="ADMIN",
        username="operator",
        is_active=True,
    )
    return SimpleNamespace(
        state=SimpleNamespace(auth_user=user, auth_kind="disabled")
    )


def _json(response):
    return json.loads(response.body.decode("utf-8"))


def _registration_payload():
    return router_module.StrategyRegistrationRequest(
        strategy_key="role_test_strategy",
        strategy_name="单人策略",
        version="v1",
    )


def _metric_payload():
    return router_module.StrategyMetricEvidenceRequest(
        strategy_key="role_test_strategy",
        bound_strategy_version="v1",
        as_of_date="2026-08-21",
        window_days=60,
        metrics={},
        evidence_protocol="PURGED_WALK_FORWARD_V2",
        artifact_hash="a" * 64,
        artifact_manifest={},
        evidence_revision_at="2026-08-21T15:00:00",
    )


def test_operator_can_register_submit_and_review(monkeypatch):
    operators = []
    monkeypatch.setattr(
        router_module,
        "register_new_strategy",
        lambda _payload, *, operator: operators.append(operator) or {"ok": True},
    )
    monkeypatch.setattr(
        router_module,
        "record_metric_input",
        lambda _payload, *, operator: operators.append(operator) or {"ok": True},
    )
    monkeypatch.setattr(
        router_module,
        "review_metric_input",
        lambda evidence_id, *, decision, reason, operator: operators.append(operator)
        or {"verification_status": "CONFIRMED"},
    )

    registered = router_module.strategy_center_register_strategy(
        _registration_payload(), _request()
    )
    submitted = router_module.strategy_center_add_metric_evidence(
        _metric_payload(), _request()
    )
    reviewed = router_module.strategy_center_review_metric_evidence(
        router_module.StrategyMetricReviewRequest(
            decision="CONFIRM", reason="单人复核"
        ),
        _request(),
        "e" * 32,
    )

    assert registered["status"] == "ok"
    assert submitted["status"] == "ok"
    assert reviewed["status"] == "ok"
    assert operators == ["user-id:1", "user-id:1", "user-id:1"]


def test_existing_strategy_registry_api_requires_challenger(monkeypatch):
    monkeypatch.setattr(
        router_module,
        "register_new_strategy",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            StrategyAlreadyRegisteredError("已有代码必须走挑战者")
        ),
    )

    response = router_module.strategy_center_register_strategy(
        _registration_payload(), _request()
    )

    assert response.status_code == 409
    assert _json(response)["error"] == "strategy_challenger_required"


def test_challenger_review_contract_rejects_client_metrics_and_sends_decision_only(
    monkeypatch,
):
    with pytest.raises(ValidationError):
        router_module.StrategyChallengerReviewRequest(
            decision="CONFIRM",
            reason="不能夹带自报指标",
            metrics={"profit_factor": 999},
        )
    captured = []
    monkeypatch.setattr(
        router_module,
        "review_strategy_challenger",
        lambda challenger_id, decision, *, operator, reason: captured.append(
            (challenger_id, decision, operator, reason)
        )
        or {"status": "READY"},
    )
    payload = router_module.StrategyChallengerReviewRequest(
        decision="CONFIRM", reason="要求服务器重放冻结产物"
    )

    result = router_module.strategy_center_review_challenger(
        payload, _request(), "c" * 32
    )

    assert result["status"] == "ok"
    assert captured == [
        ("c" * 32, "CONFIRM", "user-id:1", "要求服务器重放冻结产物")
    ]


def test_operator_can_submit_challenger_artifact(monkeypatch):
    calls = []
    monkeypatch.setattr(
        router_module,
        "submit_strategy_challenger_evidence",
        lambda challenger_id, payload, *, operator, reason: calls.append(
            (challenger_id, payload, operator, reason)
        )
        or {"status": "REVIEW_PENDING"},
    )
    payload = router_module.StrategyChallengerEvidenceRequest(
        as_of_date="2026-08-21",
        window_days=120,
        metrics={},
        evidence_protocol="PURGED_WALK_FORWARD_V2",
        artifact_hash="a" * 64,
        artifact_manifest={},
        evidence_revision_at="2026-08-21T15:00:00",
        reason="冻结完整产物",
    )

    accepted = router_module.strategy_center_submit_challenger_evidence(
        payload, _request(), "c" * 32
    )

    assert accepted["status"] == "ok"
    assert len(calls) == 1
    assert calls[0][2] == "user-id:1"
