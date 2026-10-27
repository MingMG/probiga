# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from server.briefing_tools.contracts import ToolResult
from server.briefing_tools.router import router
from server.common.config import get_settings


class _FakeReader:
    def index_snapshot(self, trade_date):
        return ToolResult([{"index_code": "000001"}], trade_date.isoformat(), "AVAILABLE")

    def sector_rank(self, trade_date, top_n):
        return ToolResult([{"sector_code": "BK1"}], trade_date.isoformat(), "AVAILABLE")

    def stock_rank(self, trade_date, sector_name, top_n):
        return ToolResult(
            [{"stock_code": "000001", "sector": sector_name}],
            trade_date.isoformat(),
            "AVAILABLE",
        )

    def watchlist(self, trade_date):
        return ToolResult([{"stock_code": "000001"}], trade_date.isoformat(), "AVAILABLE")

    def announcements(self, trade_date, limit):
        return ToolResult([{"stock_code": "000001"}], trade_date.isoformat(), "AVAILABLE")

    def market_breadth(self, trade_date):
        return ToolResult([{"up_count": 1}], trade_date.isoformat(), "AVAILABLE")

    def intraday_snapshot(self, trade_date, time_from, time_to, scope, limit):
        if scope == "bad":
            raise ValueError("scope must be market, stock:<code>, or sector:<name|code>")
        return ToolResult([{"scope": scope}], trade_date.isoformat(), "AVAILABLE")

    def limit_up_events(self, trade_date, stock_code, time_from, time_to, limit):
        return ToolResult([{"event_type": "TOUCH"}], trade_date.isoformat(), "AVAILABLE")

    def sector_linkage(self, trade_date, time_window, sector, limit):
        return ToolResult([{"sector_name": sector or "自动"}], trade_date.isoformat(), "AVAILABLE")

    def market_breadth_intraday(
        self, trade_date, time_from, time_to, interval_minutes,
    ):
        return ToolResult([{"bucket": "09:30:00"}], trade_date.isoformat(), "PARTIAL")


@pytest.fixture(autouse=True)
def _configure_tools(monkeypatch):
    monkeypatch.setenv("PROBIGA_BRIEFING_TOOLS_ENABLED", "true")
    monkeypatch.setenv("PROBIGA_BRIEFING_TOOLS_TOKEN", "briefing-secret")
    monkeypatch.setenv("PROBIGA_BRIEFING_TOOLS_TIMEOUT_SECONDS", "10")
    monkeypatch.setenv("PROBIGA_BRIEFING_TOOLS_MAX_ROWS", "200")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _client() -> TestClient:
    app = FastAPI()
    app.state.briefing_reader = _FakeReader()
    app.include_router(router, prefix="/api")
    return TestClient(app)


def test_requires_the_dedicated_bearer_token():
    client = _client()
    path = "/api/briefing-tools/index-snapshot?trade_date=2026-10-23"

    missing = client.get(path)
    wrong = client.get(path, headers={"Authorization": "Bearer wrong"})
    accepted = client.get(
        path,
        headers={"Authorization": "Bearer briefing-secret"},
    )

    assert missing.status_code == 401
    assert wrong.status_code == 401
    assert accepted.status_code == 200
    assert accepted.json()["tool"] == "get_index_snapshot"
    assert accepted.json()["schema_version"] == "probiga.a-share-briefing-tools.v1"


def test_all_ten_operation_ids_are_exact_and_openapi_is_filtered():
    schema = _client().get("/api/briefing-tools/openapi.json").json()
    operation_ids = {
        operation["operationId"]
        for methods in schema["paths"].values()
        for method, operation in methods.items()
        if method.lower() == "get"
    }
    assert operation_ids == {
        "get_index_snapshot",
        "get_sector_rank",
        "get_stock_rank",
        "get_watchlist",
        "get_announcements",
        "get_market_breadth",
        "get_intraday_snapshot",
        "get_limit_up_events",
        "get_sector_linkage",
        "get_market_breadth_intraday",
    }
    assert all(
        path.startswith("/api/briefing-tools/")
        for path in schema["paths"]
    )
    assert "HTTPBearer" in schema["components"]["securitySchemes"]


def test_endpoint_preserves_request_id_and_rejects_invalid_scope():
    client = _client()
    headers = {
        "Authorization": "Bearer briefing-secret",
        "X-Request-ID": "briefing-test-1",
    }
    accepted = client.get(
        "/api/briefing-tools/intraday-snapshot"
        "?trade_date=2026-10-23&time_from=09:30&time_to=10:00"
        "&scope=stock:000001",
        headers=headers,
    )
    invalid = client.get(
        "/api/briefing-tools/intraday-snapshot"
        "?trade_date=2026-10-23&time_from=09:30&time_to=10:00&scope=bad",
        headers=headers,
    )

    assert accepted.status_code == 200
    assert accepted.json()["request_id"] == "briefing-test-1"
    assert invalid.status_code == 422
    assert invalid.json()["detail"]["error"] == "invalid_tool_argument"


def test_results_are_never_more_than_200_rows(monkeypatch):
    monkeypatch.setenv("PROBIGA_BRIEFING_TOOLS_MAX_ROWS", "1000")
    get_settings.cache_clear()
    from server.common.config import get_briefing_tools_config

    assert get_briefing_tools_config()["max_rows"] == 200


def test_disabled_tool_surface_fails_closed(monkeypatch):
    monkeypatch.setenv("PROBIGA_BRIEFING_TOOLS_ENABLED", "false")
    get_settings.cache_clear()
    response = _client().get(
        "/api/briefing-tools/index-snapshot?trade_date=2026-10-23",
        headers={"Authorization": "Bearer briefing-secret"},
    )
    assert response.status_code == 503
    assert response.json()["detail"]["error"] == "briefing_tools_disabled"
