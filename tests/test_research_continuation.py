from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from uuid import uuid4

import pytest
from sqlalchemy import update

from biz.review import market_decision as core
from server.ai_bridge import research
from server.ai_bridge.schema import ai_bridge_job
from tools import preview_market_decision as cli
from test_research_recovery import recovery
from test_research_contract import NOW


@pytest.fixture
def boundary(recovery):
    value = recovery
    source = value["source"]
    checkpoint = cli._read_json(source / "review_checkpoint.json")
    original = cli._read_json(source / "run_receipt.json")
    old = cli._read_json(source / "stages/review/codex_draft_input.json")
    raw = checkpoint["stages"]["deepseek_draft"]["raw_response"]
    owner = cli._read_json(value["worker_dir"] / "receipt.json")["worker_id"]
    def completed_codex(payload, text):
        provider = {"source": "codex_gpt", "request_id": payload["request_id"], "input_sha256": payload["input_sha256"],
            "rules_sha256": payload["rules_sha256"], "raw_sha256": core._body_hash(text), "ephemeral": True,
            "process_exit_code": 0, "session_id": "unit", "schema_sha256": core._hash(payload["response_schema"]),
            "jsonl_sha256": "a" * 64, "worker_source_sha256": "a" * 64}
        result = {"request_id": payload["request_id"], "status": "completed", "worker_id": owner, "claim_generation": 1,
                  "source": "codex_gpt", "raw_response": text, "raw_sha256": core._body_hash(text), "provider_receipt": provider}
        with value["engine"].begin() as conn:
            conn.execute(update(ai_bridge_job).where(ai_bridge_job.c.request_uid == payload["request_id"]).values(
                status="completed", source="codex_gpt", worker_id=owner, attempts=1, error_message=None,
                answer=research.encode_envelope(result, schema=research.RESULT_SCHEMA)))
        return {**provider, "transport": "live_codex_bridge", "messages_sha256": core._hash(payload["messages"])}
    def save(stage, payload, text, provider):
        item = {"status": "COMPLETE", "request_id": payload["request_id"], "messages_sha256": core._hash(payload["messages"]),
                "raw_response": text, "raw_sha256": core._body_hash(text), "provider_receipt": provider}
        checkpoint["stages"][stage] = item
        cli._atomic(source / f"stages/review/{stage}_input.json", {"messages": payload["messages"], "response_schema": payload["response_schema"]})
        cli._atomic(source / f"stages/review/{stage}_response.json", text.encode(), raw=True)
    save("codex_draft", value["payload"], raw, completed_codex(value["payload"], raw))
    cli._atomic(source / "stages/review/deepseek_draft_input.json", old)
    checkpoint["deadline_at"] = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
    checkpoint["recovery"] = {"schema": cli.RECOVERY_SCHEMA, "recovery_count": 1, "prior_attempt": value["prior"],
                               "request_ids": {"codex_draft": value["payload"]["request_id"]}}
    original.update(error="FAILED total_deadline_exceeded", recovery=deepcopy(checkpoint["recovery"]))
    cli._atomic(source / "review_checkpoint.json", checkpoint)
    cli._atomic(source / "run_receipt.json", original)
    value.update(checkpoint=checkpoint, save=save, completed_codex=completed_codex)
    return value


def add_peer_reviews(value):
    frozen = cli._source_ordered_contexts(cli._read_json(value["source"] / "bundle.json"))
    rules, rule_hash = core._load_rules()
    packet = core.build_packet(frozen["facts"], report_kind="review", next_trade_date="2026-11-02", context=frozen["review_context"],
        previous_research=None, now=NOW, source_digest_sha256=core.source_digest_identity(frozen["facts"]))
    for stage, target in (("deepseek_review", "codex_draft"), ("codex_review", "deepseek_draft")):
        messages, schema = core._peer_request(packet, rules, value["checkpoint"]["stages"][target]["raw_response"])
        raw = core._json({"schema_version": schema["properties"]["schema_version"]["const"], "reviewed_sha256": schema["properties"]["reviewed_sha256"]["const"], "findings": []})
        payload = {"request_id": str(uuid4()), "stage": stage, "messages": messages, "response_schema": schema,
                   "rules_sha256": rule_hash, "deadline_at": value["payload"]["deadline_at"]}
        payload["input_sha256"] = research.research_input_sha256(payload)
        if stage.startswith("codex"):
            research._enqueue(value["engine"], payload)
            provider = value["completed_codex"](payload, raw)
        else:
            provider = {"source": "deepseek_api", "transport": "live_http", "endpoint": core.DEEPSEEK_ENDPOINT,
                "provider_response_id": "unit", "model": core.DEEPSEEK_RESEARCH_POLICY["model"],
                "request_policy": deepcopy(core.DEEPSEEK_RESEARCH_POLICY),
                "raw_sha256": core._body_hash(raw), "messages_sha256": core._hash(messages)}
            wire = core._deepseek_request(messages, schema, core.DEEPSEEK_RESEARCH_POLICY)
            provider.update(response_schema_sha256=core._hash(schema), response_status="tool_calls",
                wire_request_sha256=core._hash(wire), wire_schema_sha256=core._hash(wire["tools"][0]["function"]["parameters"]),
                provider_arguments_sha256=core._body_hash('{"report":' + raw + '}'),
                provider_arguments_prefix='{"report":', provider_arguments_suffix='}')
        value["save"](stage, payload, raw, provider)
    cli._atomic(value["source"] / "review_checkpoint.json", value["checkpoint"])


@pytest.mark.parametrize("count", [2, 4])
def test_expired_complete_boundary_creates_one_new_operation_without_rewriting_success(boundary, tmp_path, count):
    if count == 4:
        add_peer_reviews(boundary)
    source = boundary["source"]
    before = {path.relative_to(source).as_posix(): path.read_bytes() for path in source.rglob("*") if path.is_file()}
    target = cli.prepare_continuation(boundary["engine"], source, output_dir=tmp_path / "continuations")
    saved = cli._read_json(target / "review_checkpoint.json")
    assert saved["stages"] == boundary["checkpoint"]["stages"]
    assert saved["generation_started_at"] == boundary["checkpoint"]["generation_started_at"]
    assert saved["input_sha256"] == boundary["checkpoint"]["input_sha256"]
    assert saved["recovery"]["recovery_count"] == 1
    assert saved["continuation"]["schema"] == "probiga.research-continuation.v1"
    assert saved["continuation"]["continuation_of"] == source.name
    assert saved["continuation"]["continuation_count"] == 1
    assert saved["deadline_at"] != boundary["checkpoint"]["deadline_at"]
    assert all((source / name).read_bytes() == raw for name, raw in before.items())
    assert (target / "continuation/source_checkpoint.json").exists()
    with pytest.raises(RuntimeError, match="already_claimed"):
        cli.prepare_continuation(boundary["engine"], source, output_dir=tmp_path / "another")


@pytest.mark.parametrize("mutation", ["provider_failed", "provider_running", "not_expired", "raw_changed", "unstarted_request", "semantic_final", "second_revision", "already_continued", "active_job"])
def test_invalid_continuation_never_claims_source(boundary, tmp_path, mutation):
    source = boundary["source"]
    checkpoint = cli._read_json(source / "review_checkpoint.json")
    if mutation in {"provider_failed", "provider_running"}:
        checkpoint["stages"]["codex_draft"]["status"] = "FAILED" if mutation == "provider_failed" else "RUNNING"
    elif mutation == "not_expired":
        checkpoint["deadline_at"] = (datetime.now(timezone.utc) + timedelta(seconds=600)).isoformat()
    elif mutation == "raw_changed":
        cli._atomic(source / "stages/review/deepseek_draft_response.json", b"changed", raw=True)
    elif mutation == "unstarted_request":
        cli._atomic(source / "stages/review/deepseek_review_input.json", {"unknown": "attempt"})
    elif mutation == "semantic_final":
        checkpoint["stages"]["deepseek_final_review"] = {"status": "COMPLETE", "raw_response": '{"verdict":"REJECT"}'}
    elif mutation == "second_revision":
        checkpoint["stages"]["codex_final_2"] = {"status": "COMPLETE", "raw_response": '{"draft":{}}'}
    elif mutation == "already_continued":
        checkpoint["continuation"] = {"continuation_count": 1}
    else:
        payload = deepcopy(boundary["payload"])
        payload["request_id"] = str(uuid4())
        research._enqueue(boundary["engine"], payload)
    cli._atomic(source / "review_checkpoint.json", checkpoint)
    with pytest.raises(RuntimeError, match="BLOCKED stage=continuation"):
        cli.prepare_continuation(boundary["engine"], source, output_dir=tmp_path / "continued")
    assert not (source / "continuation_claim.json").exists()


def test_four_stage_continuation_only_calls_unstarted_final_stages(boundary, tmp_path, monkeypatch):
    from test_market_decision import Providers
    add_peer_reviews(boundary)
    target = cli.prepare_continuation(boundary["engine"], boundary["source"], output_dir=tmp_path / "continued")
    checkpoint = cli._read_json(target / "review_checkpoint.json")
    frozen = cli._source_ordered_contexts(cli._read_json(target / "bundle.json"))
    providers = Providers().install(monkeypatch)
    result = core.generate_research(frozen["facts"], report_kind="review", next_trade_date="2026-11-02",
        context=frozen["review_context"], previous_research=None, now=NOW, engine=object(), checkpoint=checkpoint)
    assert set(providers.calls) == {"codex_final", "deepseek_final_review", "codex_arbitration"}
    assert result["status"] == "COMPLETE"
    assert result["continuation"]["continuation_count"] == 1
    assert result["recovery"]["recovery_count"] == 1
