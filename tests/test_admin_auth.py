# -*- coding: utf-8 -*-
from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from server.api import admin_auth as admin_auth_module
from server.api.admin_auth import admin_auth_status, validate_admin_request
from server.api.routers.health import health_security
from server.common.config import get_settings


@pytest.fixture(autouse=True)
def _isolate_auth_environment(monkeypatch):
    # Prevent the production host's /opt/ProBigA/.env from changing unit-test
    # expectations when a setting is intentionally blank.
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "development")
    monkeypatch.setenv("PROBIGA_ADMIN_TOKEN", "")
    monkeypatch.setenv("PROBIGA_AUTH_REGISTRATION_DEADLINE", "")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _client() -> TestClient:
    app = FastAPI()

    @app.middleware("http")
    async def _admin_auth(request: Request, call_next):
        blocked = validate_admin_request(request)
        if blocked is not None:
            return blocked
        return await call_next(request)

    @app.get("/api/health")
    def health():
        return {"status": "ok"}

    @app.get("/api/scheduler/tasks")
    def scheduler_tasks():
        return {"status": "ok"}

    @app.post("/api/portfolio/add")
    def portfolio_add():
        return {"status": "ok"}

    @app.get("/api/strategy-center/dashboard")
    def strategy_center_dashboard():
        return {"status": "ok"}

    @app.post("/api/strategy-center/metrics/{evidence_id}/review")
    def strategy_center_review(evidence_id: str):
        return {"status": "ok", "evidence_id": evidence_id}

    return TestClient(app)


def test_admin_auth_allows_public_health_without_token(monkeypatch):
    monkeypatch.setenv("PROBIGA_ADMIN_AUTH_ENABLED", "true")
    monkeypatch.setenv("PROBIGA_ADMIN_TOKEN", "secret")
    get_settings.cache_clear()
    try:
        response = _client().get("/api/health")
    finally:
        get_settings.cache_clear()

    assert response.status_code == 200


def test_admin_auth_rejects_admin_read_without_account_or_legacy_token(monkeypatch):
    monkeypatch.setenv("PROBIGA_ADMIN_AUTH_ENABLED", "true")
    monkeypatch.setenv("PROBIGA_ADMIN_TOKEN", "")
    get_settings.cache_clear()
    try:
        response = _client().get("/api/scheduler/tasks")
    finally:
        get_settings.cache_clear()

    assert response.status_code == 401
    assert response.json()["error"] == "admin_auth_required"


def test_admin_auth_rejects_missing_token_for_admin_read(monkeypatch):
    monkeypatch.setenv("PROBIGA_ADMIN_AUTH_ENABLED", "true")
    monkeypatch.setenv("PROBIGA_ADMIN_TOKEN", "secret")
    get_settings.cache_clear()
    try:
        response = _client().get("/api/scheduler/tasks")
    finally:
        get_settings.cache_clear()

    assert response.status_code == 401
    assert response.json()["error"] == "admin_auth_required"
    assert response.headers["X-ProBigA-Admin-Auth"] == "required"


def test_admin_auth_accepts_header_token(monkeypatch):
    monkeypatch.setenv("PROBIGA_ADMIN_AUTH_ENABLED", "true")
    monkeypatch.setenv("PROBIGA_ADMIN_TOKEN", "secret")
    get_settings.cache_clear()
    try:
        response = _client().get("/api/scheduler/tasks", headers={"X-ProBigA-Admin-Token": "secret"})
    finally:
        get_settings.cache_clear()

    assert response.status_code == 200


def test_admin_auth_accepts_bearer_token(monkeypatch):
    monkeypatch.setenv("PROBIGA_ADMIN_AUTH_ENABLED", "true")
    monkeypatch.setenv("PROBIGA_ADMIN_TOKEN", "secret")
    get_settings.cache_clear()
    try:
        response = _client().post("/api/portfolio/add", headers={"Authorization": "Bearer secret"})
    finally:
        get_settings.cache_clear()

    assert response.status_code == 200


def _mock_account_session(monkeypatch, *, role: str, active: bool = True) -> None:
    identity = SimpleNamespace(
        user=SimpleNamespace(
            id=17,
            username="reviewer",
            role=role,
            is_active=active,
        )
    )
    monkeypatch.setattr(admin_auth_module, "get_engine", lambda: object())
    monkeypatch.setattr(
        admin_auth_module,
        "resolve_session",
        lambda engine, token: identity if token == "session-token" else None,
    )


def test_evidence_reviewer_is_limited_to_governance_read_and_exact_review_route(
    monkeypatch,
):
    monkeypatch.setenv("PROBIGA_ADMIN_AUTH_ENABLED", "true")
    _mock_account_session(monkeypatch, role="EVIDENCE_REVIEWER")
    get_settings.cache_clear()
    client = _client()
    client.cookies.set("probiga_session", "session-token")
    evidence_id = "a" * 32
    try:
        assert client.get("/api/strategy-center/dashboard").status_code == 200
        assert (
            client.post(f"/api/strategy-center/metrics/{evidence_id}/review").status_code
            == 200
        )

        mutation = client.post("/api/portfolio/add")
        unrelated_read = client.get("/api/scheduler/tasks")
        malformed_review = client.post(
            "/api/strategy-center/metrics/not-an-evidence-id/review"
        )
    finally:
        client.close()
        get_settings.cache_clear()

    assert mutation.status_code == 403
    assert mutation.json()["error"] == "account_role_forbidden"
    assert unrelated_read.status_code == 403
    assert unrelated_read.json()["error"] == "account_role_forbidden"
    assert malformed_review.status_code == 403
    assert malformed_review.json()["error"] == "account_role_forbidden"


def test_admin_account_retains_full_protected_access(monkeypatch):
    monkeypatch.setenv("PROBIGA_ADMIN_AUTH_ENABLED", "true")
    _mock_account_session(monkeypatch, role="ADMIN")
    get_settings.cache_clear()
    client = _client()
    client.cookies.set("probiga_session", "session-token")
    try:
        assert client.get("/api/scheduler/tasks").status_code == 200
        assert client.post("/api/portfolio/add").status_code == 200
    finally:
        client.close()
        get_settings.cache_clear()


def test_admin_auth_can_be_disabled(monkeypatch):
    monkeypatch.setenv("PROBIGA_ADMIN_TOKEN", "")
    monkeypatch.setenv("PROBIGA_ADMIN_AUTH_ENABLED", "false")
    get_settings.cache_clear()
    try:
        response = _client().post("/api/portfolio/add")
    finally:
        get_settings.cache_clear()

    assert response.status_code == 200


def test_production_admin_auth_disabled_fails_closed(monkeypatch):
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_ADMIN_AUTH_ENABLED", "false")
    monkeypatch.setenv("PROBIGA_ADMIN_TOKEN", "")
    get_settings.cache_clear()
    try:
        response = _client().get("/api/scheduler/tasks")
    finally:
        get_settings.cache_clear()

    assert response.status_code == 503
    assert response.json()["error"] == "admin_auth_not_ready"


def test_production_admin_auth_not_ready_fails_closed(monkeypatch):
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_ADMIN_AUTH_ENABLED", "true")
    monkeypatch.setenv("PROBIGA_ADMIN_TOKEN", "secret")
    monkeypatch.setattr(
        admin_auth_module,
        "admin_auth_status",
        lambda: {"enabled": True, "ready": False},
    )
    get_settings.cache_clear()
    try:
        response = _client().get(
            "/api/scheduler/tasks",
            headers={"X-ProBigA-Admin-Token": "secret"},
        )
    finally:
        get_settings.cache_clear()

    assert response.status_code == 503
    assert response.json()["error"] == "admin_auth_not_ready"


def test_admin_auth_status_does_not_expose_token(monkeypatch):
    monkeypatch.setenv("PROBIGA_ADMIN_AUTH_ENABLED", "true")
    monkeypatch.setenv("PROBIGA_ADMIN_TOKEN", "super-secret-token")
    monkeypatch.setattr(
        admin_auth_module,
        "registration_state",
        lambda engine: {"registration_open": False, "user_initialized": True, "user_count": 1},
    )
    monkeypatch.setattr(admin_auth_module, "get_engine", lambda: object())
    get_settings.cache_clear()
    try:
        status = admin_auth_status()
    finally:
        get_settings.cache_clear()

    assert status["enabled"] is True
    assert status["token_configured"] is True
    assert status["ready"] is True
    assert "token" not in status
    assert "super-secret-token" not in repr(status)
    assert "/api/scheduler" in status["protected_read_prefixes"]


def test_production_auth_status_rejects_empty_account_without_token(monkeypatch):
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_ADMIN_AUTH_ENABLED", "true")
    monkeypatch.setenv("PROBIGA_ADMIN_TOKEN", "")
    monkeypatch.setenv("PROBIGA_AUTH_REGISTRATION_DEADLINE", "")
    monkeypatch.setattr(
        admin_auth_module,
        "registration_state",
        lambda engine: {
            "registration_open": True,
            "user_initialized": False,
            "user_count": 0,
        },
    )
    monkeypatch.setattr(admin_auth_module, "get_engine", lambda: object())
    get_settings.cache_clear()
    try:
        status = admin_auth_status()
    finally:
        get_settings.cache_clear()

    assert status["registration_open"] is False
    assert status["credential_ready"] is False
    assert status["ready"] is False


def test_production_auth_status_allows_token_with_default_registration_closed(
    monkeypatch,
):
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_ADMIN_AUTH_ENABLED", "true")
    monkeypatch.setenv("PROBIGA_ADMIN_TOKEN", "secret")
    monkeypatch.setenv("PROBIGA_AUTH_REGISTRATION_DEADLINE", "")
    monkeypatch.setattr(
        admin_auth_module,
        "registration_state",
        lambda engine: {
            "registration_open": True,
            "user_initialized": False,
            "user_count": 0,
        },
    )
    monkeypatch.setattr(admin_auth_module, "get_engine", lambda: object())
    get_settings.cache_clear()
    try:
        status = admin_auth_status()
    finally:
        get_settings.cache_clear()

    assert status["registration_open"] is False
    assert status["registration_deadline_configured"] is False
    assert status["credential_ready"] is True
    assert status["ready"] is True


def test_production_auth_status_accepts_initialized_account_without_token(
    monkeypatch,
):
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_ADMIN_AUTH_ENABLED", "true")
    monkeypatch.setenv("PROBIGA_ADMIN_TOKEN", "")
    monkeypatch.setenv("PROBIGA_AUTH_REGISTRATION_DEADLINE", "")
    monkeypatch.setattr(
        admin_auth_module,
        "registration_state",
        lambda engine: {
            "registration_open": False,
            "user_initialized": True,
            "user_count": 1,
        },
    )
    monkeypatch.setattr(admin_auth_module, "get_engine", lambda: object())
    get_settings.cache_clear()
    try:
        status = admin_auth_status()
    finally:
        get_settings.cache_clear()

    assert status["credential_ready"] is True
    assert status["ready"] is True


def test_health_security_warns_when_admin_token_missing(monkeypatch):
    monkeypatch.setenv("PROBIGA_ADMIN_AUTH_ENABLED", "true")
    monkeypatch.setenv("PROBIGA_ADMIN_TOKEN", "")
    monkeypatch.setattr(admin_auth_module, "registration_state", lambda engine: (_ for _ in ()).throw(RuntimeError()))
    monkeypatch.setattr(admin_auth_module, "get_engine", lambda: object())
    get_settings.cache_clear()
    try:
        response = health_security()
    finally:
        get_settings.cache_clear()

    assert response["status"] == "warn"
    assert response["admin_auth"]["enabled"] is True
    assert response["admin_auth"]["token_configured"] is False


def test_health_security_warns_when_admin_auth_disabled(monkeypatch):
    monkeypatch.setenv("PROBIGA_ADMIN_AUTH_ENABLED", "false")
    monkeypatch.setenv("PROBIGA_ADMIN_TOKEN", "secret")
    monkeypatch.setattr(
        admin_auth_module,
        "registration_state",
        lambda engine: {"registration_open": False, "user_initialized": True, "user_count": 1},
    )
    monkeypatch.setattr(admin_auth_module, "get_engine", lambda: object())
    get_settings.cache_clear()
    try:
        response = health_security()
    finally:
        get_settings.cache_clear()

    assert response["status"] == "warn"
    assert response["admin_auth"]["enabled"] is False
