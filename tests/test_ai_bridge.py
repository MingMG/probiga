# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from sqlalchemy import create_engine

from server.ai_bridge.schema import (
    privileged_migrate_ai_bridge_schema,
    reset_ai_bridge_schema_cache,
)
from server.api.admin_auth import is_admin_protected_path
from server.api.routers import ai_bridge as ai_bridge_router
from server.common.config import get_settings

ROOT = Path(__file__).resolve().parents[1]


def _client(monkeypatch, tmp_path) -> tuple[TestClient, object]:
    engine = create_engine(
        f"sqlite:///{tmp_path / 'ai-bridge.db'}",
        connect_args={"check_same_thread": False},
    )
    reset_ai_bridge_schema_cache(engine)
    privileged_migrate_ai_bridge_schema(engine)
    monkeypatch.setenv("PROBIGA_AI_BRIDGE_TOKEN", "bridge-secret")
    get_settings.cache_clear()
    monkeypatch.setattr(ai_bridge_router, "get_engine", lambda: engine)

    app = FastAPI()
    app.include_router(ai_bridge_router.router, prefix="/api")

    @app.middleware("http")
    async def fake_identity(request: Request, call_next):
        request.state.auth_user = SimpleNamespace(id=int(request.headers.get("X-Test-User", "1")))
        return await call_next(request)

    return TestClient(app), engine


def test_question_round_trip_preserves_answer_and_actual_source(monkeypatch, tmp_path):
    client, engine = _client(monkeypatch, tmp_path)
    worker_headers = {"X-ProBigA-AI-Bridge-Token": "bridge-secret"}
    try:
        question = "  请分析 600519 的主要风险。\n不要省略条件。  "
        submitted = client.post(
            "/api/ai-bridge/questions",
            json={"channel": "stock", "question": question},
        )
        assert submitted.status_code == 202
        request_id = submitted.json()["job"]["request_id"]
        assert submitted.json()["job"]["question"] == question
        assert submitted.json()["job"]["source"] is None

        assert client.post(
            "/api/ai-bridge/worker/claim",
            json={"worker_id": "test-worker"},
        ).status_code == 401
        claimed = client.post(
            "/api/ai-bridge/worker/claim",
            headers=worker_headers,
            json={"worker_id": "test-worker"},
        )
        assert claimed.status_code == 200
        assert claimed.json()["job"]["question"] == question

        progress = client.post(
            f"/api/ai-bridge/worker/{request_id}/progress",
            headers=worker_headers,
            json={"worker_id": "test-worker", "provider_attempt": "codex_gpt"},
        )
        assert progress.status_code == 200

        answer = "第一行。\n\n```text\n原样内容\n```"
        completed = client.post(
            f"/api/ai-bridge/worker/{request_id}/complete",
            headers=worker_headers,
            json={
                "worker_id": "test-worker",
                "status": "completed",
                "answer": answer,
                "source": "codex_gpt",
                "source_label": "GPT（Codex）",
            },
        )
        assert completed.status_code == 200

        fetched = client.get(f"/api/ai-bridge/questions/{request_id}").json()["job"]
        assert fetched["answer"] == answer
        assert fetched["source"] == "codex_gpt"
        assert fetched["source_label"] == "GPT（Codex）"
        assert fetched["status"] == "completed"
    finally:
        client.close()
        engine.dispose()
        get_settings.cache_clear()


def test_questions_are_isolated_by_logged_in_user(monkeypatch, tmp_path):
    client, engine = _client(monkeypatch, tmp_path)
    try:
        submitted = client.post(
            "/api/ai-bridge/questions",
            headers={"X-Test-User": "7"},
            json={"channel": "general", "question": "用户七的问题"},
        )
        request_id = submitted.json()["job"]["request_id"]
        assert client.get(
            f"/api/ai-bridge/questions/{request_id}",
            headers={"X-Test-User": "7"},
        ).status_code == 200
        assert client.get(
            f"/api/ai-bridge/questions/{request_id}",
            headers={"X-Test-User": "8"},
        ).status_code == 404
        assert client.get(
            "/api/ai-bridge/questions?channel=general",
            headers={"X-Test-User": "8"},
        ).json()["jobs"] == []
    finally:
        client.close()
        engine.dispose()
        get_settings.cache_clear()


def test_worker_endpoints_have_separate_public_gate():
    assert is_admin_protected_path("/api/ai-bridge/worker/claim", "POST") is False
    assert is_admin_protected_path("/api/ai-bridge/questions", "POST") is True
    assert is_admin_protected_path("/ai-stock", "GET") is True
    assert is_admin_protected_path("/ai-general", "GET") is True


def test_frontend_displays_source_and_never_injects_answer_html():
    script = (ROOT / "server" / "static" / "js" / "ai-chat.js").read_text(encoding="utf-8")
    assert "GPT（Codex）" in script
    assert "DeepSeek 网页" in script
    assert "answer.textContent" in script
    assert "answer.innerHTML" not in script
