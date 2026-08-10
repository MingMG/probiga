from datetime import datetime

from fastapi import FastAPI
from fastapi.testclient import TestClient

from server.api.routers import screener


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(screener.router, prefix="/api")
    return TestClient(app)


def test_catalog_exposes_presets_and_all_version_boundaries() -> None:
    body = _client().get("/api/screener/catalog").json()
    assert body["status"] == "ok"
    assert {item["key"] for item in body["presets"]} >= {
        "trend_breakout",
        "capital_support",
        "technical_cross",
    }
    versions = {item["version"]: item for item in body["versions"]}
    assert set(versions) == {"V2", "V3", "V4", "V5", "V6"}
    assert versions["V3"]["production_selector"] is True
    assert versions["V4"]["decision"] == "BLOCK"
    assert versions["V5"]["production_selector"] is False
    assert versions["V6"]["production_selector"] is False
    assert body["execution_boundary"]["real_orders_allowed"] is False


def test_expected_completed_session_handles_intraday_and_weekend() -> None:
    assert screener._expected_completed_session(datetime(2026, 8, 10, 13, 0)).isoformat() == "2026-08-07"
    assert screener._expected_completed_session(datetime(2026, 8, 10, 17, 0)).isoformat() == "2026-08-10"
    assert screener._expected_completed_session(datetime(2026, 8, 9, 12, 0)).isoformat() == "2026-08-07"


def test_normalize_rows_excludes_non_stocks_and_never_makes_actionable() -> None:
    rows = screener._normalize_rows(
        [
            {"stock_code": "1", "short_name": "Alpha", "change_pct": 2, "vol_ratio": 1.5},
            {"stock_code": "2", "short_name": "ST Beta", "change_pct": 8},
            {"stock_code": "899050", "short_name": "指数"},
        ],
        10,
    )
    assert [row["stock_code"] for row in rows] == ["000001"]
    assert rows[0]["stock_name"] == "Alpha"
    assert rows[0]["action"] == "WATCH"
    assert rows[0]["actionable"] is False
    assert rows[0]["score"] > 50


def test_status_fails_closed_when_required_data_is_stale(monkeypatch) -> None:
    monkeypatch.setattr(screener, "_expected_completed_session", lambda: datetime(2026, 8, 10).date())
    monkeypatch.setattr(
        screener,
        "_safe_max_date",
        lambda table, _column: "2026-08-07" if table in {"sm_stock_kline", "sm_stock_capital_flow_daily"} else "",
    )
    body = screener.screener_status()
    assert body["status"] == "blocked"
    assert body["selection_ready"] is False
    assert body["recommendation_ready"] is False
    assert body["gate"] == "DATA_STALE"
    assert body["actionable_output_allowed"] is False


def test_status_separates_base_selection_from_stale_recommendation(monkeypatch) -> None:
    monkeypatch.setattr(screener, "_expected_completed_session", lambda: datetime(2026, 8, 10).date())

    def latest(table: str, _column: str) -> str:
        if table in {"sm_stock_kline", "sm_stock_capital_flow_daily", "si_notice_eastmoney"}:
            return "2026-08-10"
        return "2026-08-07"

    monkeypatch.setattr(screener, "_safe_max_date", latest)
    body = screener.screener_status()
    assert body["status"] == "degraded"
    assert body["selection_ready"] is True
    assert body["recommendation_ready"] is False
    assert body["gate"] == "RESEARCH_WATCH_ONLY_RECOMMENDATION_BLOCKED"
    assert "si_notice_eastmoney" in screener._safe_max_date.__name__ or body["data_dates"]["notice"] == "2026-08-10"


def test_run_uses_legacy_reader_but_preserves_research_boundary(monkeypatch) -> None:
    monkeypatch.setattr(
        screener,
        "_runtime_status",
        lambda: {"status": "ok", "selection_ready": True, "actionable_output_allowed": False, "gate": "RESEARCH_WATCH_ONLY"},
    )
    monkeypatch.setattr(
        screener,
        "_legacy_run",
        lambda _request, _preset: {
            "date": "2026-08-07",
            "data": [{"stock_code": "600000", "short_name": "浦发银行", "change_pct": 1.2}],
        },
    )
    body = screener.screener_run(screener.ScreenerRunRequest(top=10))
    assert body["status"] == "ok"
    assert body["data_date"] == "2026-08-07"
    assert body["actionable_output_allowed"] is False
    assert body["data"][0]["action"] == "WATCH"
    assert body["data"][0]["actionable"] is False


def test_unknown_preset_returns_400() -> None:
    response = _client().post("/api/screener/run", json={"preset": "nope"})
    assert response.status_code == 400
