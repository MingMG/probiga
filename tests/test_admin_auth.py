# -*- coding: utf-8 -*-
from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from server.api import admin_auth as admin_auth_module
from server.api.admin_auth import admin_auth_status, validate_admin_request
from server.api.routers.health import health_security
from server.common.config import get_settings


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
    monkeypatch.delenv("PROBIGA_ADMIN_TOKEN", raising=False)
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


def test_admin_auth_can_be_disabled(monkeypatch):
    monkeypatch.delenv("PROBIGA_ADMIN_TOKEN", raising=False)
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
    monkeypatch.delenv("PROBIGA_ADMIN_TOKEN", raising=False)
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
    monkeypatch.delenv("PROBIGA_ADMIN_TOKEN", raising=False)
    monkeypatch.delenv("PROBIGA_AUTH_REGISTRATION_DEADLINE", raising=False)
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
    monkeypatch.delenv("PROBIGA_AUTH_REGISTRATION_DEADLINE", raising=False)
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
    monkeypatch.delenv("PROBIGA_ADMIN_TOKEN", raising=False)
    monkeypatch.delenv("PROBIGA_AUTH_REGISTRATION_DEADLINE", raising=False)
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
    monkeypatch.delenv("PROBIGA_ADMIN_TOKEN", raising=False)
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
