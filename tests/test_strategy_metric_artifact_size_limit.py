import json
from types import SimpleNamespace

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from server.api.routers import strategy_center as router
from server.api.strategy_evidence_request_limit import (
    StrategyEvidenceRequestSizeMiddleware,
    StrategyEvidenceRequestTooLarge,
    strategy_evidence_too_large_response,
)
from server.engine import strategy_governance as governance


def _request():
    return SimpleNamespace(state=SimpleNamespace(
        auth_kind="account_session",
        auth_user=SimpleNamespace(
            id=7,
            username="admin",
            role="ADMIN",
            is_active=True,
        ),
    ))


def _metric_payload():
    return router.StrategyMetricEvidenceRequest(
        strategy_key="artifact_size_strategy",
        bound_strategy_version="v1",
        as_of_date="2026-08-21",
        window_days=60,
        metrics={},
        evidence_protocol="PURGED_WALK_FORWARD_V2",
        artifact_hash="a" * 64,
        artifact_manifest={},
        evidence_revision_at="2026-08-21T15:00:00",
    )


def _challenger_payload():
    return router.StrategyChallengerEvidenceRequest(
        as_of_date="2026-08-21",
        window_days=120,
        metrics={},
        evidence_protocol="PURGED_WALK_FORWARD_V2",
        artifact_hash="a" * 64,
        artifact_manifest={},
        evidence_revision_at="2026-08-21T15:00:00",
    )


def test_canonical_artifact_bytes_are_bounded_before_deep_replay(monkeypatch):
    monkeypatch.setattr(governance, "METRIC_ARTIFACT_MAX_BYTES", 64)

    try:
        governance._validate_metric_artifact(
            {"oversized": "x" * 100},
            entity_type="STRATEGY",
            entity_key="artifact_size_strategy",
            entity_version="v1",
            as_of_date="2026-08-21",
            window_days=60,
            evidence_protocol="PURGED_WALK_FORWARD_V2",
            evidence_revision_at="2026-08-21T15:00:00",
            metrics={},
            artifact_hash="a" * 64,
            version_created_at="2026-01-01T00:00:00",
            expected_max_holding_days=5,
            expected_label_horizon_days=5,
        )
    except governance.MetricArtifactTooLarge as exc:
        assert "48 MiB" in str(exc)
    else:  # pragma: no cover - explicit negative contract
        raise AssertionError("oversized canonical artifact was accepted")


def test_metric_and_challenger_routes_map_artifact_cap_to_413(monkeypatch):
    def too_large(*_args, **_kwargs):
        raise governance.MetricArtifactTooLarge("产物超过硬上限")

    monkeypatch.setattr(router, "record_metric_input", too_large)
    monkeypatch.setattr(router, "submit_strategy_challenger_evidence", too_large)

    metric = router.strategy_center_add_metric_evidence(
        _metric_payload(), _request(),
    )
    challenger = router.strategy_center_submit_challenger_evidence(
        _challenger_payload(), _request(), "a" * 32,
    )

    for response, error in (
        (metric, "metric_evidence_artifact_too_large"),
        (challenger, "challenger_evidence_artifact_too_large"),
    ):
        assert response.status_code == 413
        body = json.loads(response.body.decode("utf-8"))
        assert body["error"] == error
        assert body["automatic_real_order_submission"] is False
        assert body["real_order_authority"] is False


def test_streaming_request_guard_rejects_forged_small_content_length():
    app = FastAPI()
    app.add_middleware(StrategyEvidenceRequestSizeMiddleware, max_bytes=16)

    @app.exception_handler(StrategyEvidenceRequestTooLarge)
    async def too_large_handler(_request, _exc):
        return strategy_evidence_too_large_response()

    @app.post("/api/strategy-center/metrics")
    async def receive_metric(request: Request):
        return {"size": len(await request.body())}

    client = TestClient(app)
    response = client.post(
        "/api/strategy-center/metrics",
        content=b"x" * 17,
        headers={"content-length": "1"},
    )

    assert response.status_code == 413
    assert response.json()["error"] == "strategy_evidence_request_too_large"
    assert response.json()["automatic_real_order_submission"] is False
    assert response.json()["real_order_authority"] is False


def test_request_guard_does_not_limit_unrelated_routes():
    app = FastAPI()
    app.add_middleware(StrategyEvidenceRequestSizeMiddleware, max_bytes=16)

    @app.post("/api/unrelated")
    async def unrelated(request: Request):
        return {"size": len(await request.body())}

    response = TestClient(app).post("/api/unrelated", content=b"x" * 17)
    assert response.status_code == 200
    assert response.json() == {"size": 17}
