import json
from types import SimpleNamespace

from server.api.routers import strategy_center as router_module


def _request(role=None, user_id=1, auth_kind="account_session"):
    user = None
    if role is not None:
        user = SimpleNamespace(
            id=user_id,
            role=role,
            username=f"user-{user_id}",
            is_active=True,
        )
    return SimpleNamespace(
        state=SimpleNamespace(auth_user=user, auth_kind=auth_kind)
    )


def _json(response):
    return json.loads(response.body.decode("utf-8"))


def _registration_payload():
    return router_module.StrategyRegistrationRequest(
        strategy_key="role_test_strategy",
        strategy_name="职责分离测试策略",
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


def test_reviewer_and_legacy_token_cannot_mutate_registry(monkeypatch):
    called = []
    monkeypatch.setattr(
        router_module,
        "register_strategy",
        lambda *_args, **_kwargs: called.append(True),
    )

    reviewer = router_module.strategy_center_register_strategy(
        _registration_payload(), _request("EVIDENCE_REVIEWER", user_id=2)
    )
    legacy = router_module.strategy_center_register_strategy(
        _registration_payload(),
        _request(None, auth_kind="legacy_token"),
    )

    assert reviewer.status_code == 403
    assert legacy.status_code == 403
    assert _json(reviewer)["error"] == "strategy_admin_required"
    assert called == []


def test_admin_registers_and_submits_but_cannot_self_assign_reviewer_role(
    monkeypatch,
):
    operators = []
    monkeypatch.setattr(
        router_module,
        "register_strategy",
        lambda _payload, *, operator: operators.append(operator) or {"ok": True},
    )
    monkeypatch.setattr(
        router_module,
        "record_metric_input",
        lambda _payload, *, operator: operators.append(operator) or {"ok": True},
    )

    registered = router_module.strategy_center_register_strategy(
        _registration_payload(), _request("ADMIN")
    )
    submitted = router_module.strategy_center_add_metric_evidence(
        _metric_payload(), _request("ADMIN")
    )
    review_denied = router_module.strategy_center_review_metric_evidence(
        router_module.StrategyMetricReviewRequest(
            decision="CONFIRM", reason="管理员不能代替独立复核员"
        ),
        _request("ADMIN"),
        "e" * 32,
    )

    assert registered["status"] == "ok"
    assert submitted["status"] == "ok"
    assert operators == ["user-id:1", "user-id:1"]
    assert review_denied.status_code == 403
    assert _json(review_denied)["error"] == "metric_reviewer_role_required"


def test_reviewer_can_review_but_cannot_submit(monkeypatch):
    captured = []
    monkeypatch.setattr(
        router_module,
        "review_metric_input",
        lambda evidence_id, *, decision, reason, operator: captured.append(
            (evidence_id, decision, reason, operator)
        ) or {"verification_status": "CONFIRMED"},
    )

    reviewed = router_module.strategy_center_review_metric_evidence(
        router_module.StrategyMetricReviewRequest(
            decision="CONFIRM", reason="已独立核对全部样本"
        ),
        _request("EVIDENCE_REVIEWER", user_id=2),
        "e" * 32,
    )
    submit_denied = router_module.strategy_center_add_metric_evidence(
        _metric_payload(), _request("EVIDENCE_REVIEWER", user_id=2)
    )

    assert reviewed["status"] == "ok"
    assert captured == [
        ("e" * 32, "CONFIRM", "已独立核对全部样本", "user-id:2")
    ]
    assert submit_denied.status_code == 403
    assert _json(submit_denied)["error"] == "metric_evidence_admin_required"
