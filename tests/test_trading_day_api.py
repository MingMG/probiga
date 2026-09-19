from types import SimpleNamespace
from datetime import datetime, timedelta

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
import pytest

from server.api.routers import trading_day
from server.common.trading_day_store import CHINA, TradingDayStore


DAY = "2024-07-12"
URL = f"/api/trading-day/journal?trade_date={DAY}"
NOW = datetime(2024, 7, 12, 15, 1, tzinfo=CHINA)


@pytest.fixture(autouse=True)
def _clock(monkeypatch):
    monkeypatch.setattr("server.common.trading_day_store._now", lambda: NOW)


def _body(revision=0):
    return {"revision": revision, "plans": [{"stock_code": "600519",
        "stock_name": "测试名称", "trigger": "核对确认条件", "invalidation": "核对失效条件",
        "source_as_of": "2024-07-12 09:08:00"}], "review": {"text": "个人笔记"}}


def _client(monkeypatch, root, user_id=7, active=True):
    monkeypatch.setattr(trading_day, "_store", lambda: TradingDayStore(root))
    app = FastAPI()
    app.include_router(trading_day.router, prefix="/api")
    @app.middleware("http")
    async def identity(request: Request, call_next):
        if user_id is not None:
            request.state.auth_user = SimpleNamespace(id=user_id, is_active=active)
        return await call_next(request)
    return TestClient(app)


def test_personal_journal_roundtrip_and_revision_conflict(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path / "journals")
    empty = client.get(URL)
    assert empty.status_code == 200
    assert empty.json() == {"trade_date": DAY, "revision": 0, "plans": [], "review": {"text": "", "updated_at": None}, "updated_at": None}
    response = client.put(URL, json=_body())
    assert response.status_code == 200
    saved = response.json()
    assert saved["revision"] == 1
    assert saved["plans"][0]["status"] == "WATCHING"
    assert saved["plans"][0]["original"]["trigger"] == "核对确认条件"
    assert saved["plans"][0]["created_at"]
    assert "no-store" in response.headers["cache-control"]
    assert client.get(URL).json() == saved
    conflict = client.put(URL, json=_body())
    assert conflict.status_code == 409
    assert conflict.json()["detail"]["revision"] == 1
    assert client.get(URL).json() == saved
    assert client.put(URL, json={"revision": 1, "plans": [], "review": {"text": ""}}).json()["plans"] == []


def test_identity_is_server_owned_and_users_cannot_read_each_other(monkeypatch, tmp_path):
    root = tmp_path / "journals"
    first = _client(monkeypatch, root, 7)
    assert first.put(URL, json=_body()).status_code == 200
    second = _client(monkeypatch, root, 8)
    assert second.get(URL).json()["revision"] == 0
    spoofed = _body()
    spoofed["user_id"] = 7
    assert second.put(URL, json=spoofed).status_code == 422
    assert second.get(URL).json()["revision"] == 0


@pytest.mark.parametrize("user_id,active", [(None, True), (7, False), (True, True), (0, True)])
def test_requires_valid_account_identity(monkeypatch, tmp_path, user_id, active):
    root = tmp_path / "journals"
    client = _client(monkeypatch, root, user_id, active)
    assert client.get(URL).status_code == 401
    assert client.put(URL, json=_body()).status_code == 401
    assert not root.exists()


@pytest.mark.parametrize("mutate", [
    lambda body: body.update(revision=True),
    lambda body: body.update(revision=-1),
    lambda body: body["plans"][0].update(stock_code="../../"),
    lambda body: body["plans"][0].update(source_as_of="2099-01-01"),
    lambda body: body["plans"][0].update(created_at="spoofed"),
    lambda body: body["plans"][0].update(trigger="x" * 1201),
    lambda body: body["plans"].append(dict(body["plans"][0])),
    lambda body: body["review"].update(text="x" * 12001),
])
def test_invalid_input_never_creates_journal(monkeypatch, tmp_path, mutate):
    root = tmp_path / "journals"
    client = _client(monkeypatch, root)
    body = _body()
    mutate(body)
    assert client.put(URL, json=body).status_code == 422
    assert not list(root.glob("*.json"))


def test_storage_failure_is_visible_and_cannot_reset_corrupt_journal(monkeypatch, tmp_path):
    root = tmp_path / "journals"
    client = _client(monkeypatch, root)
    assert client.put(URL, json=_body()).status_code == 200
    path = root / f"trading-day-user-7-{DAY}.json"
    path.write_text("broken", encoding="utf-8")
    for response in (client.get(URL), client.put(URL, json=_body(1))):
        assert response.status_code == 503
        assert response.json()["detail"]["error"] == "journal_store_unavailable"
        assert str(root) not in response.text
    assert path.read_text(encoding="utf-8") == "broken"


def test_date_editing_policy_is_enforced_at_api_boundary(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path / "journals")
    initial = client.put(URL, json=_body()).json()
    monkeypatch.setattr("server.common.trading_day_store._now", lambda: NOW + timedelta(days=1))
    historical = _body(1)
    historical["plans"][0].update(status="REVIEWED", note="次日补记")
    response = client.put(URL, json=historical)
    assert response.status_code == 200
    saved = response.json()
    assert saved["plans"][0]["created_at"] == initial["plans"][0]["created_at"]
    assert saved["plans"][0]["updated_at"].startswith("2024-07-13")
    historical["revision"] = 2
    historical["plans"][0]["trigger"] = "事后改写"
    assert client.put(URL, json=historical).status_code == 422
    assert client.put(URL, json={"revision": 2, "plans": [], "review": {"text": ""}}).status_code == 422
    assert client.get(URL).json() == saved
    future = "/api/trading-day/journal?trade_date=2024-07-14"
    assert client.put(future, json={"revision": 0, "plans": [], "review": {"text": "预填"}}).status_code == 422


def test_real_auth_middleware_requires_account_and_blocks_cross_site_write(monkeypatch, tmp_path):
    from server.api import admin_auth
    from server.common.config import get_settings

    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "development")
    monkeypatch.setenv("PROBIGA_ADMIN_AUTH_ENABLED", "true")
    monkeypatch.setenv("PROBIGA_ADMIN_TOKEN", "legacy-token")
    monkeypatch.setattr(trading_day, "_store", lambda: TradingDayStore(tmp_path / "journals"))
    monkeypatch.setattr(admin_auth, "get_engine", lambda: object())
    account = SimpleNamespace(user=SimpleNamespace(id=7, role="ADMIN", is_active=True))
    monkeypatch.setattr(admin_auth, "resolve_session", lambda engine, token: account if token == "valid-session" else None)
    app = FastAPI()
    app.include_router(trading_day.router, prefix="/api")

    @app.middleware("http")
    async def auth(request: Request, call_next):
        blocked = admin_auth.validate_admin_request(request)
        return blocked if blocked is not None else await call_next(request)

    get_settings.cache_clear()
    try:
        with TestClient(app) as client:
            assert client.get(URL).status_code == 401
            legacy = client.put(URL, json=_body(), headers={"X-ProBigA-Admin-Token": "legacy-token"})
            assert legacy.status_code == 401
            assert legacy.json()["detail"]["error"] == "account_session_required"
            client.cookies.set("probiga_session", "valid-session")
            cross_site = client.put(URL, json=_body(), headers={"Origin": "https://elsewhere.invalid"})
            assert cross_site.status_code == 403
            assert cross_site.json()["error"] == "cross_site_request_blocked"
            assert client.get(URL).json()["revision"] == 0
            assert client.put(URL, json=_body(), headers={"Origin": "http://testserver"}).status_code == 200
    finally:
        get_settings.cache_clear()
