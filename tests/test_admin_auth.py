# -*- coding: utf-8 -*-
from __future__ import annotations

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from server.api.admin_auth import admin_auth_status, validate_admin_request
from server.common.config import get_settings


@pytest.fixture(autouse=True)
def _isolate_auth_environment(monkeypatch):
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "development")
    monkeypatch.setenv("PROBIGA_ADMIN_TOKEN", "")
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

    @app.get("/api/operator")
    def operator(request: Request):
        return {
            "kind": request.state.auth_kind,
            "user": request.state.auth_user.as_dict(),
        }

    return TestClient(app)


def test_disabled_auth_allows_protected_api_and_injects_operator(monkeypatch):
    monkeypatch.setenv("PROBIGA_ADMIN_AUTH_ENABLED", "false")
    get_settings.cache_clear()
    response = _client().get("/api/operator")

    assert response.status_code == 200
    assert response.json() == {
        "kind": "disabled",
        "user": {
            "id": 1,
            "username": "operator",
            "role": "ADMIN",
            "is_active": True,
        },
    }


def test_enabled_auth_requires_the_operator_token(monkeypatch):
    monkeypatch.setenv("PROBIGA_ADMIN_AUTH_ENABLED", "true")
    monkeypatch.setenv("PROBIGA_ADMIN_TOKEN", "secret")
    get_settings.cache_clear()

    missing = _client().get("/api/scheduler/tasks")
    wrong = _client().get(
        "/api/scheduler/tasks",
        headers={"X-ProBigA-Admin-Token": "wrong"},
    )
    accepted = _client().get(
        "/api/scheduler/tasks",
        headers={"X-ProBigA-Admin-Token": "secret"},
    )

    assert missing.status_code == 401
    assert missing.json()["error"] == "admin_token_required"
    assert wrong.status_code == 401
    assert accepted.status_code == 200


def test_enabled_auth_without_token_is_explicitly_unavailable(monkeypatch):
    monkeypatch.setenv("PROBIGA_ADMIN_AUTH_ENABLED", "true")
    monkeypatch.setenv("PROBIGA_ADMIN_TOKEN", "")
    get_settings.cache_clear()

    response = _client().get("/api/scheduler/tasks")

    assert response.status_code == 503
    assert response.json()["error"] == "admin_token_missing"


def test_auth_status_never_exposes_the_token(monkeypatch):
    monkeypatch.setenv("PROBIGA_ADMIN_AUTH_ENABLED", "true")
    monkeypatch.setenv("PROBIGA_ADMIN_TOKEN", "super-secret-token")
    get_settings.cache_clear()

    payload = admin_auth_status()

    assert payload["required"] is True
    assert payload["token_configured"] is True
    assert "super-secret-token" not in repr(payload)


def test_public_health_remains_public(monkeypatch):
    monkeypatch.setenv("PROBIGA_ADMIN_AUTH_ENABLED", "true")
    monkeypatch.setenv("PROBIGA_ADMIN_TOKEN", "secret")
    get_settings.cache_clear()

    assert _client().get("/api/health").status_code == 200
