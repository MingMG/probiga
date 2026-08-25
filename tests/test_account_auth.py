# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select
from datetime import datetime, timedelta, timezone

import pytest

from server.api import admin_auth
from server.api.routers import auth as auth_router
from server.auth.schema import (
    auth_audit,
    auth_session,
    auth_user,
    privileged_migrate_auth_schema,
    reset_auth_schema_cache,
)
from server.auth.service import registration_window_open
from server.common.config import get_settings


@pytest.fixture(autouse=True)
def _isolate_auth_environment(monkeypatch):
    # Settings intentionally load /opt/ProBigA/.env in production. Override
    # those values so unit tests are deterministic on the production host.
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "development")
    monkeypatch.setenv("PROBIGA_ADMIN_TOKEN", "")
    monkeypatch.setenv("PROBIGA_AUTH_REGISTRATION_DEADLINE", "")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _build_client(monkeypatch, tmp_path) -> tuple[TestClient, object]:
    engine = create_engine(
        f"sqlite:///{tmp_path / 'auth-test.db'}",
        connect_args={"check_same_thread": False},
    )
    reset_auth_schema_cache(engine)
    privileged_migrate_auth_schema(engine)
    monkeypatch.setenv("PROBIGA_ADMIN_AUTH_ENABLED", "true")
    monkeypatch.setenv("PROBIGA_ADMIN_TOKEN", "legacy-secret")
    monkeypatch.setenv("PROBIGA_AUTH_COOKIE_SECURE", "false")
    get_settings.cache_clear()
    monkeypatch.setattr(auth_router, "get_engine", lambda: engine)
    monkeypatch.setattr(admin_auth, "get_engine", lambda: engine)

    app = FastAPI()
    app.include_router(auth_router.router, prefix="/api")

    @app.middleware("http")
    async def require_auth(request: Request, call_next):
        blocked = admin_auth.validate_admin_request(request)
        if blocked is not None:
            return blocked
        return await call_next(request)

    @app.get("/", response_class=HTMLResponse)
    def home():
        return "<h1>private</h1>"

    @app.get("/api/private")
    def private_api():
        return {"status": "ok"}

    return TestClient(app, follow_redirects=False), engine


def test_first_account_registration_login_refresh_and_logout(monkeypatch, tmp_path):
    client, engine = _build_client(monkeypatch, tmp_path)
    try:
        status = client.get("/api/auth/status")
        assert status.status_code == 200
        assert status.json()["required"] is True
        assert status.json()["registration_open"] is True
        assert status.json()["authenticated"] is False

        blocked = client.get("/")
        assert blocked.status_code == 303
        assert blocked.headers["location"].startswith("/login?next=")
        deep_link = client.get("/?tab=stock-list&stock_code=603983")
        assert deep_link.status_code == 303
        assert deep_link.headers["location"] == (
            "/login?next=/%3Ftab%3Dstock-list%26stock_code%3D603983"
        )
        assert client.get("/api/private").status_code == 401

        registered = client.post(
            "/api/auth/register",
            json={"username": "owner", "password": "correct-horse-2026"},
        )
        assert registered.status_code == 201
        assert registered.json()["user"]["role"] == "ADMIN"
        cookie_header = registered.headers["set-cookie"]
        assert "HttpOnly" in cookie_header
        assert "SameSite=strict" in cookie_header
        assert client.get("/").status_code == 200
        assert client.get("/api/private").status_code == 200

        with engine.connect() as conn:
            user_row = conn.execute(select(auth_user)).mappings().one()
            session_row = conn.execute(select(auth_session)).mappings().one()
        assert user_row["password_hash"] != "correct-horse-2026"
        assert "correct-horse-2026" not in user_row["password_hash"]
        assert len(session_row["token_hash"]) == 64

        second = client.post(
            "/api/auth/register",
            json={"username": "other", "password": "another-password-2026"},
        )
        assert second.status_code == 409
        assert second.json()["error"] == "registration_closed"

        old_cookie = client.cookies.get("probiga_session")
        refreshed = client.post("/api/auth/refresh")
        assert refreshed.status_code == 200
        assert client.cookies.get("probiga_session") != old_cookie
        assert client.get("/api/private").status_code == 200

        logged_out = client.post("/api/auth/logout")
        assert logged_out.status_code == 200
        assert client.get("/api/private").status_code == 401

        wrong = client.post(
            "/api/auth/login",
            json={"username": "owner", "password": "definitely-wrong"},
        )
        assert wrong.status_code == 401
        logged_in = client.post(
            "/api/auth/login",
            json={"username": "owner", "password": "correct-horse-2026"},
        )
        assert logged_in.status_code == 200
        assert client.get("/api/private").status_code == 200
    finally:
        client.close()
        engine.dispose()
        get_settings.cache_clear()


def test_admin_can_create_independent_reviewer_but_reviewer_cannot_create_users(
    monkeypatch,
    tmp_path,
):
    client, engine = _build_client(monkeypatch, tmp_path)
    try:
        registered = client.post(
            "/api/auth/register",
            json={"username": "owner", "password": "correct-horse-2026"},
        )
        assert registered.status_code == 201

        created = client.post(
            "/api/auth/users",
            json={
                "username": "reviewer",
                "password": "independent-review-2026",
                "role": "EVIDENCE_REVIEWER",
            },
        )
        assert created.status_code == 201
        assert created.json()["user"] == {
            "id": 2,
            "username": "reviewer",
            "role": "EVIDENCE_REVIEWER",
            "is_active": True,
        }
        assert "password" not in created.text.lower()

        with engine.connect() as conn:
            users = conn.execute(
                select(auth_user).order_by(auth_user.c.id)
            ).mappings().all()
            audit = conn.execute(
                select(auth_audit)
                .where(auth_audit.c.event_type == "USER_CREATED")
            ).mappings().one()
        assert [row["role"] for row in users] == [
            "ADMIN",
            "EVIDENCE_REVIEWER",
        ]
        assert audit["user_id"] == 1
        assert "created_user_id=2" in audit["detail"]

        assert client.post("/api/auth/logout").status_code == 200
        reviewer_login = client.post(
            "/api/auth/login",
            json={
                "username": "reviewer",
                "password": "independent-review-2026",
            },
        )
        assert reviewer_login.status_code == 200
        denied = client.post(
            "/api/auth/users",
            json={
                "username": "thirduser",
                "password": "another-independent-2026",
                "role": "EVIDENCE_REVIEWER",
            },
        )
        assert denied.status_code == 403
        assert denied.json()["error"] == "account_role_forbidden"

        token_denied = client.post(
            "/api/auth/users",
            headers={"X-ProBigA-Admin-Token": "legacy-secret"},
            json={
                "username": "tokenuser",
                "password": "legacy-token-cannot-create-2026",
                "role": "EVIDENCE_REVIEWER",
            },
        )
        assert token_denied.status_code == 403
        assert token_denied.json()["error"] == "account_session_required"
    finally:
        client.close()
        engine.dispose()
        get_settings.cache_clear()


def test_disabled_development_auth_status_does_not_require_login(monkeypatch, tmp_path):
    client, engine = _build_client(monkeypatch, tmp_path)
    monkeypatch.setenv("PROBIGA_ADMIN_AUTH_ENABLED", "false")
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "development")
    get_settings.cache_clear()
    try:
        status = client.get("/api/auth/status")
        home = client.get("/")
    finally:
        client.close()
        engine.dispose()
        get_settings.cache_clear()

    assert status.status_code == 200
    assert status.json()["required"] is False
    assert status.json()["authenticated"] is False
    assert home.status_code == 200


def test_disabled_production_auth_status_remains_fail_closed(monkeypatch, tmp_path):
    client, engine = _build_client(monkeypatch, tmp_path)
    monkeypatch.setenv("PROBIGA_ADMIN_AUTH_ENABLED", "false")
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    get_settings.cache_clear()
    try:
        status = client.get("/api/auth/status")
        home = client.get("/")
    finally:
        client.close()
        engine.dispose()
        get_settings.cache_clear()

    assert status.status_code == 200
    assert status.json()["required"] is True
    assert home.status_code == 503
    assert home.json()["error"] == "admin_auth_not_ready"


def test_browser_auth_guard_redirects_only_when_authentication_is_required():
    root = Path(__file__).resolve().parents[1]
    script = (root / "server" / "static" / "js" / "auth.js").read_text(encoding="utf-8")

    assert "if (data.required !== true) return;" in script
    assert "if (!data._responseOk || !data.authenticated)" in script
    assert "auth.js?v=1" not in (root / "server" / "static" / "index.html").read_text(encoding="utf-8")


def test_production_first_admin_registration_requires_explicit_deadline(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_AUTH_REGISTRATION_DEADLINE", "")
    client, engine = _build_client(monkeypatch, tmp_path)
    try:
        status = client.get("/api/auth/status")
        registered = client.post(
            "/api/auth/register",
            json={"username": "owner", "password": "correct-horse-2026"},
        )
    finally:
        client.close()
        engine.dispose()
        get_settings.cache_clear()

    assert status.status_code == 200
    assert status.json()["registration_open"] is False
    assert registered.status_code == 403
    assert registered.json()["error"] == "registration_window_closed"


def test_legacy_token_still_supports_non_browser_automation(monkeypatch, tmp_path):
    client, engine = _build_client(monkeypatch, tmp_path)
    try:
        response = client.get(
            "/api/private",
            headers={"X-ProBigA-Admin-Token": "legacy-secret"},
        )
        assert response.status_code == 200
    finally:
        client.close()
        engine.dispose()
        get_settings.cache_clear()


def test_cross_site_cookie_mutation_is_rejected(monkeypatch, tmp_path):
    client, engine = _build_client(monkeypatch, tmp_path)
    try:
        registered = client.post(
            "/api/auth/register",
            json={"username": "owner", "password": "correct-horse-2026"},
        )
        assert registered.status_code == 201
        response = client.post(
            "/api/private",
            headers={"Origin": "http://attacker.example"},
        )
        assert response.status_code == 403
        assert response.json()["error"] == "cross_site_request_blocked"
    finally:
        client.close()
        engine.dispose()
        get_settings.cache_clear()


def test_registration_window_deadline_fails_closed():
    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    future = (datetime.now(timezone.utc) + timedelta(minutes=1)).isoformat()
    assert registration_window_open(None) is True
    assert registration_window_open(None, require_explicit=True) is False
    assert registration_window_open(future) is True
    assert registration_window_open(past) is False
    assert registration_window_open("not-a-date") is False
