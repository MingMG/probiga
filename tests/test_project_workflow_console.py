from __future__ import annotations

import sqlite3
import time

import pytest
from fastapi.testclient import TestClient

from server.api.main import app
from server.common.config import get_settings
from server.project_workflow import store


BASE = "/api/project-workflow/tasks"
HEADERS = {"X-ProBigA-Admin-Token": "isolated-console-test"}


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "development")
    monkeypatch.setenv("PROBIGA_ADMIN_AUTH_ENABLED", "true")
    monkeypatch.setenv("PROBIGA_ADMIN_TOKEN", HEADERS["X-ProBigA-Admin-Token"])
    monkeypatch.delenv("PROBIGA_WORKFLOW_DB", raising=False)
    get_settings.cache_clear()
    # No application lifespan. The workflow read API has no scheduler or
    # trading runtime dependency.
    yield TestClient(app)
    get_settings.cache_clear()


def test_workflow_read_api_reuses_operator_auth_and_does_not_initialize_missing_ledger(
    client, monkeypatch, tmp_path
):
    for route in (BASE, BASE + "/missing"):
        assert client.get(route).status_code == 401
        response = client.get(route, headers=HEADERS)
        assert response.status_code == 503
        assert response.headers["Cache-Control"] == "no-store"
    absent = tmp_path / "workflow.sqlite"
    monkeypatch.setenv("PROBIGA_WORKFLOW_DB", str(absent))
    assert client.get(BASE, headers=HEADERS).status_code == 503
    assert not absent.exists()
    assert client.post(BASE, headers=HEADERS, json={}).status_code == 405


def test_invalid_ledger_returns_bounded_unavailable_message(client, monkeypatch, tmp_path):
    db_path = tmp_path / "workflow.sqlite"
    with sqlite3.connect(db_path) as db:
        db.execute("CREATE TABLE unrelated (id INTEGER)")
    monkeypatch.setenv("PROBIGA_WORKFLOW_DB", str(db_path))
    response = client.get(BASE, headers=HEADERS)
    assert response.status_code == 503
    assert response.json() == {"detail": "编排证据库暂不可用"}
    assert str(db_path) not in response.text


def test_console_reads_actual_persisted_task_without_disclosing_private_plan(
    client, monkeypatch, tmp_path
):
    db_path = tmp_path / "workflow.sqlite"
    store.initialize(db_path)
    monkeypatch.setenv("PROBIGA_WORKFLOW_DB", str(db_path))
    response = client.get(BASE, headers=HEADERS)
    assert response.json() == {"tasks": []}
    store.submit(db_path, {
        "id": "console:isolated-test",
        "title": "隔离验收任务",
        "goal": "INTERNAL_PLAN_MARKER",
        "acceptance": ["INTERNAL_ACCEPTANCE_MARKER"],
        "expires": time.time() + 120,
        "steps": [{"id": "verify", "kind": "verify"}],
    })
    assert client.get(BASE, headers=HEADERS).json()["tasks"][0]["id"] == "console:isolated-test"
    response = client.get(BASE + "/console%3Aisolated-test", headers=HEADERS)
    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "no-store"
    task = response.json()
    assert task["state"] == "NOT_STARTED"
    assert task["steps"][0]["id"] == "verify"
    assert task["evidence"] == []
    assert "INTERNAL_" not in response.text
    assert client.get(BASE + "/missing", headers=HEADERS).status_code == 404


def test_historical_console_keeps_conflicting_checks_and_missing_activation_separate(
    client, monkeypatch, tmp_path
):
    db_path = tmp_path / "workflow.sqlite"
    store.initialize(db_path)
    monkeypatch.setenv("PROBIGA_WORKFLOW_DB", str(db_path))
    now = time.time()
    candidate = "a" * 40
    outcomes = [
        ("merged", "COMPLETE", "github_merge"),
        ("tested", "COMPLETE", "isolated_regression_passed"),
        ("tested", "FAILED", "isolated_http_failed"),
        ("released", "UNKNOWN", "release_receipt_missing"),
        ("active", "UNKNOWN", "runtime_readback_missing"),
    ]
    store.import_history(db_path, {
        "id": "isolated-history",
        "title": "历史验收记录",
        "evidence": [
            {"stage": stage, "state": state, "candidate": candidate, "code": code, "at": now}
            for stage, state, code in outcomes
        ],
    })
    response = client.get(BASE + "/isolated-history", headers=HEADERS)
    assert response.status_code == 200
    task = response.json()
    assert task["historical"] is True
    assert [(item["stage"], item["state"], item["code"]) for item in task["evidence"]] == outcomes
