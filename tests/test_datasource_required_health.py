"""The command page's data chip has to be answerable, and answerable cheaply.

Observed in production: the page requested /api/datasource/required-health with
a 3500ms budget (server/static/js/app.js) and the route did not exist, so the
chip could only ever report a failure.  The route must exist, must speak the
page's ok/warn/fail vocabulary, and must stay a cheap read - a broken check has
to degrade that one row rather than turn the whole chip into a 500.
"""
from fastapi import FastAPI
from fastapi.testclient import TestClient

from server.api.routers import datasource
from tools.data_quality_check import CheckResult


def _client(monkeypatch, results):
    """results: name -> CheckResult, or an exception to raise."""
    def check(name):
        def run(_engine):
            outcome = results[name]
            if isinstance(outcome, Exception):
                raise outcome
            return outcome
        run.__name__ = name
        return run

    monkeypatch.setattr(datasource, "get_engine", lambda: object())
    monkeypatch.setattr(
        "tools.data_quality_check.check_required_tables",
        check("check_required_tables"),
    )
    monkeypatch.setattr(
        "tools.data_quality_check.check_stock_universe",
        check("check_stock_universe"),
    )
    app = FastAPI()
    app.include_router(datasource.router, prefix="/api")
    return TestClient(app)


def _pass(name="check_required_tables"):
    return CheckResult(name=name, status="PASS", message="ok", details={})


def test_required_health_reports_ok_when_every_check_passes(monkeypatch):
    client = _client(monkeypatch, {
        "check_required_tables": _pass(),
        "check_stock_universe": _pass("check_stock_universe"),
    })
    response = client.get("/api/datasource/required-health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["total"] == 2
    assert [item["status"] for item in body["data"]] == ["ok", "ok"]
    # The page must not read a cached verdict for a freshness chip.
    assert response.headers["cache-control"] == "no-store"


def test_required_health_maps_warn_and_fail_into_the_pages_vocabulary(monkeypatch):
    client = _client(monkeypatch, {
        "check_required_tables": CheckResult(
            name="check_required_tables", status="WARN", message="缺名 3 只", details={}),
        "check_stock_universe": CheckResult(
            name="check_stock_universe", status="FAIL", message="股票池缺失", details={}),
    })
    body = client.get("/api/datasource/required-health").json()
    assert body["status"] == "degraded"
    assert [item["status"] for item in body["data"]] == ["warn", "fail"]


def test_a_broken_check_degrades_its_row_instead_of_failing_the_request(monkeypatch):
    client = _client(monkeypatch, {
        "check_required_tables": RuntimeError("probe exploded"),
        "check_stock_universe": _pass("check_stock_universe"),
    })
    response = client.get("/api/datasource/required-health")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "degraded"
    broken = body["data"][0]
    assert broken["status"] == "fail"
    assert "RuntimeError" in broken["message"]


def test_required_health_is_reachable_under_the_api_prefix(monkeypatch):
    client = _client(monkeypatch, {
        "check_required_tables": _pass(),
        "check_stock_universe": _pass("check_stock_universe"),
    })
    assert client.get("/api/datasource/required-health").status_code == 200
    assert client.get("/datasource/required-health").status_code == 404
