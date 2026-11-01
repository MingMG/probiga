from __future__ import annotations

import base64
import gzip
import hashlib
import json
import random
from types import SimpleNamespace
from datetime import timedelta
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select, update

from server.ai_bridge import research
from server.ai_bridge.schema import ai_bridge_job, metadata, reset_ai_bridge_schema_cache
from server.ai_bridge.service import claim_job, create_job, update_progress, complete_job
from server.api.routers import ai_bridge as router_module
from server.common.config import get_settings


@pytest.fixture
def engine(tmp_path):
    value = create_engine(f"sqlite:///{tmp_path / 'research.db'}", connect_args={"check_same_thread": False})
    metadata.create_all(value)  # Test-only SQLite fixture; production never executes DDL.
    yield value
    reset_ai_bridge_schema_cache(value)
    value.dispose()


def payload(**changes):
    value = {"request_id": str(uuid4()), "stage": "post_close", "rules_sha256": "a" * 64,
             "messages": [{"role": "system", "content": "Return JSON"},
                          {"role": "user", "content": "public market facts"}],
             "response_schema": {"type": "object", "properties": {"answer": {"type": "string"}},
                                 "required": ["answer"], "additionalProperties": False},
             "deadline_at": research._iso(research.utcnow() + timedelta(seconds=120))}
    value.update(changes)
    value["input_sha256"] = research.research_input_sha256(value)
    return value


def receipt(value, raw):
    return {"source": "codex_gpt", "request_id": value["request_id"],
            "input_sha256": value["input_sha256"], "rules_sha256": value["rules_sha256"],
            "raw_sha256": hashlib.sha256(raw.encode("utf-8")).hexdigest(),
            "model": "UNKNOWN", "session_id": "UNKNOWN"}


def enqueue(engine, value=None):
    value = value or payload()
    research._enqueue(engine, value)
    return value


def row(engine, value):
    with engine.connect() as conn:
        return dict(conn.execute(select(ai_bridge_job).where(ai_bridge_job.c.request_uid == value["request_id"])).mappings().one())


def test_large_research_envelope_is_deterministic_and_preserves_exact_unicode():
    value = payload(messages=[{"role": "user", "content": "公开行情与证据" * 5000}])
    first = research.encode_envelope(value)
    assert first == research.encode_envelope(value)
    assert first.isascii() and len(first) <= 60000
    assert research.decode_envelope(first) == value
    assert len(value["messages"][0]["content"]) > 8000


@pytest.mark.parametrize("malformed", ["[]", "{}", "not JSON", "非ASCII"])
def test_bad_envelopes_are_rejected(malformed):
    with pytest.raises(ValueError):
        research.decode_envelope(malformed)


def test_decode_rejects_corruption_and_decompression_bomb():
    wrapper = json.loads(research.encode_envelope(payload()))
    wrapper["sha256"] = "0" * 64
    with pytest.raises(ValueError):
        research.decode_envelope(json.dumps(wrapper))
    bomb = {"schema": research.REQUEST_SCHEMA, "encoding": "gzip+base64", "raw_size": 10,
            "sha256": "0" * 64, "data": base64.b64encode(gzip.compress(b"x" * 1_000_000)).decode()}
    with pytest.raises(ValueError):
        research.decode_envelope(json.dumps(bomb))


def test_both_raw_byte_and_text_column_caps_are_enforced():
    with pytest.raises(ValueError, match="decoded byte"):
        research.encode_envelope({"data": "中" * 90000})
    rng = random.Random(42)
    random_text = "".join(rng.choice("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") for _ in range(80000))
    with pytest.raises(ValueError, match="TEXT byte"):
        research.encode_envelope({"data": random_text})


def test_input_identity_binds_messages_schema_rules_stage_and_scope():
    value = payload(scope={"trade_date": "2026-10-30"})
    changed = dict(value, request_id=str(uuid4()), deadline_at=research._iso(research.utcnow()))
    assert research.research_input_sha256(changed) == value["input_sha256"]
    changed["scope"] = {"trade_date": "2026-10-29"}
    assert research.research_input_sha256(changed) != value["input_sha256"]
    with pytest.raises(ValueError, match="hash mismatch"):
        research.validate_payload(changed)
    with pytest.raises(ValueError, match="aware"):
        research.validate_payload(payload(deadline_at="2026-11-01T12:00:00"))


def test_general_worker_cannot_claim_research_and_research_cannot_claim_general(engine):
    value = enqueue(engine)
    assert claim_job(engine, worker_id="old", lease_seconds=60) is None
    general = create_job(engine, owner_user_id=1, channel="general", question="question")
    claimed = research.claim_research(engine, worker_id="research")
    assert claimed["request_id"] == value["request_id"]
    assert claimed["payload"] == value
    assert claimed["claim_generation"] == 1
    assert research.claim_research(engine, worker_id="other") is None
    assert claim_job(engine, worker_id="old", lease_seconds=60)["request_id"] == general["request_id"]


def test_same_uuid_is_idempotent_and_conflicting_input_is_rejected(engine):
    value = enqueue(engine)
    research._enqueue(engine, value)
    assert row(engine, value)["attempts"] == 0
    changed = payload(request_id=value["request_id"], messages=[{"role": "user", "content": "different"}])
    with pytest.raises(research.ResearchBridgeError, match="input_conflict"):
        research._enqueue(engine, changed)
    with engine.connect() as conn:
        assert len(conn.execute(select(ai_bridge_job)).all()) == 1


def test_expired_queued_job_is_terminal_and_never_claimed(engine):
    value = enqueue(engine, payload(deadline_at=research._iso(research.utcnow() - timedelta(seconds=1))))
    assert research.claim_research(engine, worker_id="worker") is None
    assert row(engine, value)["status"] == "failed"
    research._enqueue(engine, value)
    assert research.claim_research(engine, worker_id="worker") is None
    assert row(engine, value)["attempts"] == 0


def test_expired_processing_lease_is_not_reclaimed_or_renewable(engine):
    value = enqueue(engine)
    research.claim_research(engine, worker_id="first")
    with engine.begin() as conn:
        conn.execute(update(ai_bridge_job).values(lease_expires_at=research.utcnow() - timedelta(seconds=1)))
    assert research.claim_research(engine, worker_id="second") is None
    assert research.progress_research(engine, request_id=value["request_id"], worker_id="first", claim_generation=1) is None
    raw = '{"answer":"test"}'
    assert research.complete_research(engine, request_id=value["request_id"], worker_id="first", claim_generation=1,
        status="completed", raw_response=raw, provider_receipt=receipt(value, raw)) is None
    assert row(engine, value)["attempts"] == 1


def test_owner_generation_deadline_and_receipt_are_all_required(engine):
    value = enqueue(engine)
    research.claim_research(engine, worker_id="first")
    for worker, generation in (("second", 1), ("first", 2)):
        assert research.progress_research(engine, request_id=value["request_id"], worker_id=worker, claim_generation=generation) is None
    raw = '{"answer":"test"}'
    bad = dict(receipt(value, raw), source="deepseek_web")
    with pytest.raises(ValueError, match="codex_gpt"):
        research.complete_research(engine, request_id=value["request_id"], worker_id="first", claim_generation=1,
            status="completed", raw_response=raw, provider_receipt=bad)
    bad = dict(receipt(value, raw), input_sha256="b" * 64)
    with pytest.raises(ValueError, match="input_sha256"):
        research.complete_research(engine, request_id=value["request_id"], worker_id="first", claim_generation=1,
            status="completed", raw_response=raw, provider_receipt=bad)
    assert research.progress_research(engine, request_id=value["request_id"], worker_id="first", claim_generation=1)["status"] == "processing"


def test_completed_receipt_is_exact_and_repeated_completion_is_idempotent(engine):
    value = enqueue(engine)
    research.claim_research(engine, worker_id="first")
    raw = ' {"answer":"原样返回"}\n'
    expected = research.complete_research(engine, request_id=value["request_id"], worker_id="first", claim_generation=1,
        status="completed", raw_response=raw, provider_receipt=receipt(value, raw))
    assert expected["raw_response"] == raw
    repeated = research.complete_research(engine, request_id=value["request_id"], worker_id="first", claim_generation=1,
        status="completed", raw_response=raw, provider_receipt=receipt(value, raw))
    assert repeated == expected
    assert research.research_state(engine, request_id=value["request_id"], worker_id="first", claim_generation=1) == expected
    changed = '{"answer":"different"}'
    assert research.complete_research(engine, request_id=value["request_id"], worker_id="first", claim_generation=1,
        status="completed", raw_response=changed, provider_receipt=receipt(value, changed)) is None
    assert research.research_state(engine, request_id=value["request_id"], worker_id="other", claim_generation=1) is None
    assert research.request_research(engine, payload=value) == expected


def test_request_poll_success_and_failure_have_no_provider_fallback(engine, monkeypatch):
    value = payload()
    events = []
    monkeypatch.setattr(research, "time", SimpleNamespace(monotonic=research.time.monotonic, sleep=lambda _: None))
    def progress(event):
        events.append(event)
        if len(events) == 1:
            research.claim_research(engine, worker_id="first")
            raw = '{"answer":"fresh source"}'
            research.complete_research(engine, request_id=value["request_id"], worker_id="first", claim_generation=1,
                status="completed", raw_response=raw, provider_receipt=receipt(value, raw))
    result = research.request_research(engine, payload=value, on_event=progress)
    assert result["source"] == "codex_gpt"
    assert events[-1]["completed_units"] == 1
    failed = enqueue(engine)
    research.claim_research(engine, worker_id="first")
    research.complete_research(engine, request_id=failed["request_id"], worker_id="first", claim_generation=1,
                              status="failed", error_message="CLI failed; no fallback")
    with pytest.raises(research.ResearchBridgeError, match="CLI failed; no fallback"):
        research.request_research(engine, payload=failed)


def test_request_timeout_terminalizes_original_job_without_requeue(engine):
    value = payload()
    with pytest.raises(research.ResearchBridgeError, match="total_deadline"):
        research.request_research(engine, payload=value, timeout_seconds=0.02)
    saved = row(engine, value)
    assert saved["status"] == "failed" and saved["attempts"] == 0
    with pytest.raises(research.ResearchBridgeError, match="total_deadline"):
        research.request_research(engine, payload=value, timeout_seconds=0.02)
    assert row(engine, value)["status"] == "failed"


def test_worker_research_endpoints_require_token_and_fenced_dtos(engine, monkeypatch):
    value = enqueue(engine)
    monkeypatch.setenv("PROBIGA_AI_BRIDGE_TOKEN", "test-token")
    get_settings.cache_clear()
    monkeypatch.setattr(router_module, "get_engine", lambda: engine)
    app = FastAPI()
    app.include_router(router_module.router, prefix="/api")
    headers = {"X-ProBigA-AI-Bridge-Token": "test-token"}
    with TestClient(app) as client:
        assert client.post("/api/ai-bridge/worker/research/claim", json={"worker_id": "w"}).status_code == 401
        result = client.post("/api/ai-bridge/worker/research/claim", headers=headers, json={"worker_id": "w"})
        assert result.status_code == 200 and result.json()["job"]["payload"] == value
        base = f"/api/ai-bridge/worker/research/{value['request_id']}"
        assert client.post(base + "/progress", headers=headers, json={"worker_id": "w"}).status_code == 422
        assert client.post(base + "/progress", headers=headers, json={"worker_id": "w", "claim_generation": 2}).status_code == 409
        assert client.get(base, headers=headers, params={"worker_id": "w", "claim_generation": 1}).json()["result"]["status"] == "processing"
        raw = '{"answer":"source"}'
        body = {"worker_id": "w", "claim_generation": 1, "status": "completed", "raw_response": raw, "provider_receipt": receipt(value, raw)}
        assert client.post(base + "/complete", headers=headers, json=body).status_code == 200
        assert client.post(base + "/complete", headers=headers, json=body).json()["result"]["raw_response"] == raw
        assert client.get(base, headers=headers, params={"worker_id": "w", "claim_generation": 1}).json()["result"]["provider_receipt"] == receipt(value, raw)
        # The research contract never expands the human-facing public DTO.
        assert client.post("/api/ai-bridge/questions", json={"channel": "research", "question": "x"}).status_code == 422
        assert client.post("/api/ai-bridge/questions", json={"channel": "general", "question": "x" * 8001}).status_code == 422
    get_settings.cache_clear()


def test_legacy_mutations_cannot_bypass_research_generation_lease_or_source(engine):
    value = enqueue(engine)
    research.claim_research(engine, worker_id="same-worker")
    # The legacy protocol has no generation field and supports another provider.
    # Knowing the same owner and UUID must not bypass the research-only fence.
    assert update_progress(engine, request_uid=value["request_id"], worker_id="same-worker",
                           provider_attempt="deepseek_web", lease_seconds=600) is False
    assert complete_job(engine, request_uid=value["request_id"], worker_id="same-worker",
                        status="completed", answer="unfenced answer", source="deepseek_web",
                        source_label="wrong provider", error_message=None) is False
    saved = row(engine, value)
    assert saved["status"] == "processing"
    assert saved["provider_attempt"] == "codex_gpt"
    assert saved["answer"] is None


def test_dead_processing_owner_becomes_failed_at_original_deadline_without_reclaim(engine, monkeypatch):
    value = enqueue(engine)
    research.claim_research(engine, worker_id="gone")
    future = research.utcnow() + timedelta(seconds=180)
    monkeypatch.setattr(research, "utcnow", lambda: future)
    assert research.claim_research(engine, worker_id="new") is None
    saved = row(engine, value)
    assert saved["status"] == "failed"
    assert saved["worker_id"] == "gone" and saved["attempts"] == 1
    assert "deadline" in saved["error_message"]


def test_windows_consumer_and_server_share_the_real_fenced_contract(engine, monkeypatch, tmp_path):
    from tools import run_codex_web_bridge as worker
    value = enqueue(engine)
    monkeypatch.setenv("PROBIGA_AI_BRIDGE_TOKEN", "test-token")
    get_settings.cache_clear()
    monkeypatch.setattr(router_module, "get_engine", lambda: engine)
    app = FastAPI()
    app.include_router(router_module.router, prefix="/api")
    with TestClient(app) as client:
        class LocalApi(worker.ResearchBridgeApi):
            def _request(self, method, path, payload):
                kwargs = {"params": payload} if method == "GET" else {"json": payload}
                response = client.request(method, path, headers=self.headers, **kwargs)
                response.raise_for_status()
                return response.json()
        class LocalProvider:
            calls = 0
            def ask(self, incoming, directory, lost):
                self.calls += 1
                assert incoming == value and not lost.is_set()
                raw = ' {"answer":"跨端合同通过"}\r\n'
                return {"raw_response": raw, "provider_receipt": receipt(incoming, raw)}
        api = LocalApi("http://testserver", "test-token")
        provider = LocalProvider()
        claimed = api.claim_research("windows-research")
        result = worker.process_research_job(api, provider, job=claimed,
            worker_id="windows-research", state_root=tmp_path / "worker-state")
        assert result["status"] == "completed" and result["source"] == "codex_gpt"
        assert provider.calls == 1
        assert result == api.research_status(value["request_id"], "windows-research", 1)
        assert result == research.request_research(engine, payload=value)
        assert result["raw_response"].endswith("\r\n")
        assert result["provider_receipt"]["input_sha256"] == value["input_sha256"]
    get_settings.cache_clear()


def test_research_mysql_pool_is_dedicated_bounded_and_preserves_exact_database_url(monkeypatch):
    from sqlalchemy.engine import make_url
    class EngineIdentity:
        def __init__(self, url):
            self.url = make_url(url)
            self.dialect = SimpleNamespace(name="mysql")
            self.disposed = False
        def dispose(self):
            self.disposed = True
    original = EngineIdentity("mysql+pymysql://test_user:sentinel@db.example:3306/market?charset=utf8mb4&read_timeout=300")
    bounded = EngineIdentity(original.url)
    calls = []
    def factory(**kwargs):
        calls.append(kwargs)
        return bounded
    monkeypatch.setattr(research, "create_batch_engine", factory)
    assert research.get_research_engine(original) is bounded
    assert research.get_research_engine(original) is bounded
    assert research.get_research_engine(bounded) is bounded
    assert len(calls) == 1
    assert calls[0]["url"] == original.url
    assert dict(calls[0]["url"].query) == {"charset": "utf8mb4", "read_timeout": "300"}
    assert calls[0]["connect_args"] == {"connect_timeout": 8, "read_timeout": 15, "write_timeout": 15}
    assert calls[0]["pool_timeout"] == 8


def test_research_pool_refuses_database_identity_drift_and_disposes_the_wrong_pool(monkeypatch):
    from sqlalchemy.engine import make_url
    class EngineIdentity:
        dialect = SimpleNamespace(name="mysql")
        disposed = False
        def __init__(self, database):
            self.url = make_url("mysql+pymysql://user:sentinel@db.example/" + database)
        def dispose(self):
            self.disposed = True
    original, wrong = EngineIdentity("expected"), EngineIdentity("different")
    monkeypatch.setattr(research, "create_batch_engine", lambda **kwargs: wrong)
    with pytest.raises(research.ResearchBridgeError, match="database_identity_mismatch"):
        research.get_research_engine(original)
    assert wrong.disposed is True


def test_sqlite_research_boundaries_use_original_engine_and_public_reads_exclude_internal_jobs(engine):
    from server.ai_bridge.service import get_job, list_jobs
    value = enqueue(engine)
    assert research.get_research_engine(engine) is engine
    assert get_job(engine, owner_user_id=0, request_uid=value["request_id"]) is None
    assert list_jobs(engine, owner_user_id=0) == []
