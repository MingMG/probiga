from copy import deepcopy
from datetime import timedelta
import json
from pathlib import Path
import socket
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, select, update

from biz.review import market_decision as core
from server.ai_bridge import research
from server.ai_bridge.schema import ai_bridge_job, metadata, reset_ai_bridge_schema_cache
from tools import preview_market_decision as cli
from test_research_contract import NOW, answer, material


def test_read_registration_written_by_windows_powershell_utf8(tmp_path):
    receipt = tmp_path / "registration.json"
    receipt.write_bytes(b'\xef\xbb\xbf{"status":"REGISTERED"}\r\n')
    assert cli._read_json(receipt) == {"status": "REGISTERED"}


@pytest.fixture
def recovery(tmp_path, monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    metadata.create_all(engine)
    source = tmp_path / str(uuid4())
    source.mkdir()
    facts, context = material()
    original_bytes = {"facts": json.dumps(facts, ensure_ascii=False, default=cli._default).encode("utf-8"),
                      "review_context": json.dumps(context, ensure_ascii=False).encode("utf-8"),
                      "briefing_context": json.dumps(context, ensure_ascii=False).encode("utf-8")}
    bundle = cli.freeze_bundle(facts, context, context, next_trade_date="2026-11-02", source_raw=original_bytes)
    rules, rules_hash = core._load_rules()
    packet = core.build_packet(bundle["facts"], report_kind="review", next_trade_date="2026-11-02",
        context=context, previous_research=None, now=NOW, source_digest_sha256=core.source_digest_identity(bundle["facts"]))
    messages, schema = core._draft_request(packet, rules)
    request_id = str(uuid4())
    payload = {"request_id": request_id, "stage": "codex_draft", "messages": messages, "response_schema": schema,
               "rules_sha256": rules_hash, "deadline_at": (NOW + timedelta(seconds=150)).isoformat()}
    payload["input_sha256"] = research.research_input_sha256(payload)
    research._enqueue(engine, payload)
    owner = "research-" + socket.gethostname() + "-4242-original"
    with engine.begin() as connection:
        connection.execute(update(ai_bridge_job).where(ai_bridge_job.c.request_uid == request_id).values(
            status="failed", worker_id=owner, attempts=1, error_message="request_total_deadline_exceeded"))
    raw = core._json(answer(packet))
    success = {"status": "COMPLETE", "request_id": str(uuid4()), "messages_sha256": core._hash(messages),
        "raw_response": raw, "raw_sha256": core._body_hash(raw), "provider_receipt": {
            "transport": "live_http", "raw_sha256": core._body_hash(raw), "messages_sha256": core._hash(messages),
            "source": "deepseek_api", "endpoint": "https://api.deepseek.com/v1/chat/completions",
            "provider_response_id": "offline-unit", "model": "unit"}}
    prior = {"status": "FAILED", "request_id": request_id, "messages_sha256": core._hash(messages),
             "error_type": "ResearchBridgeError", "error": "ResearchBridgeError"}
    checkpoint = {"run_id": str(uuid4()), "generation_started_at": NOW.isoformat(),
        "deadline_at": (NOW + timedelta(seconds=600)).isoformat(), "input_sha256": core._hash(packet),
        "rules_sha256": rules_hash, "stages": {"deepseek_draft": success, "codex_draft": prior}}
    receipt = {"schema_version": cli.RUN_SCHEMA, "run_id": source.name, "status": "FAILED", "stage": "review:codex_draft",
        "bundle_sha256": bundle["bundle_sha256"], "rules_sha256": rules_hash, "publish": False, "api_base": None,
        "report_started_at": {"review": NOW.isoformat()}, "deadline_at": (NOW + timedelta(seconds=1320)).isoformat(),
        "total_units": 12, "completed_units": 1, "report_database_write_invoked": False, "wecom_delivery_invoked": False}
    for name, value in (("bundle.json", bundle), ("review_checkpoint.json", checkpoint), ("run_receipt.json", receipt)):
        cli._atomic(source / name, value)
    cli._atomic(source / "stages/review/codex_draft_input.json", {"messages": messages, "response_schema": schema})
    cli._atomic(source / "stages/review/deepseek_draft_response.json", raw.encode(), raw=True)
    state_root = tmp_path / "worker"
    worker_dir = state_root / request_id
    local = {"request_id": request_id, "worker_id": owner, "claim_generation": 1, "input_sha256": payload["input_sha256"],
        "status": "COMPLETE", "completion": {"status": "failed", "worker_id": owner, "claim_generation": 1, "provider_receipt": {
            "source": "codex_gpt", "request_id": request_id, "input_sha256": payload["input_sha256"], "rules_sha256": rules_hash}}}
    process = {"status": "TERMINAL", "pid": 99999999, "parent_pid": 4242, "process_exit_code": 1,
        "request_id": request_id, "input_sha256": payload["input_sha256"], "worker_source_sha256": "a" * 64}
    cli._atomic(worker_dir / "receipt.json", local)
    cli._atomic(worker_dir / "process/receipt.json", process)
    cli._atomic(worker_dir / "events.jsonl", b'{"type":"thread.started"}\n{"type":"error","message":"Connection failed: error sending request; waiting for network"}\n', raw=True)
    registration_path = tmp_path / "old_registration.json"
    cli._atomic(registration_path, {"status": "REGISTERED", "registration": {"purpose": "research",
        "task_name": "ProBigA AI Research Worker", "state_root": str(state_root), "worker_sha256": "a" * 64}})
    monkeypatch.setattr(cli, "_pid_absent", lambda pid: True)
    monkeypatch.setattr(cli, "_prior_worker_identity", lambda revision: {"revision": revision, "sha256": "a" * 64})
    result = {"engine": engine, "source": source, "state_root": state_root, "worker_dir": worker_dir,
        "registration": registration_path, "checkpoint": checkpoint, "payload": payload, "prior": prior,
        "kwargs": {"stage": "codex_draft", "worker_state_root": state_root, "registration_path": registration_path,
                   "prior_worker_revision": "b" * 40}}
    yield result
    reset_ai_bridge_schema_cache(engine)
    engine.dispose()


def test_verified_recovery_allocates_once_and_preserves_successful_provider_bytes_and_clock(recovery, tmp_path):
    source = recovery["source"]
    before = {name: (source / name).read_bytes() for name in ("run_receipt.json", "review_checkpoint.json", "stages/review/deepseek_draft_response.json")}
    target = cli.prepare_recovery(recovery["engine"], source, output_dir=tmp_path / "recovered", **recovery["kwargs"])
    checkpoint = cli._read_json(target / "review_checkpoint.json")
    assert checkpoint["generation_started_at"] == recovery["checkpoint"]["generation_started_at"]
    assert checkpoint["input_sha256"] == recovery["checkpoint"]["input_sha256"]
    assert checkpoint["stages"]["deepseek_draft"] == recovery["checkpoint"]["stages"]["deepseek_draft"]
    assert "codex_draft" not in checkpoint["stages"]
    record = checkpoint["recovery"]
    assert record["schema"] == "probiga.research-recovery.v1" and record["recovery_count"] == 1
    assert record["prior_attempt"] == recovery["prior"]
    assert record["request_ids"]["codex_draft"] != recovery["prior"]["request_id"]
    assert record["proof"]["prior_worker_git"]["sha256"] == "a" * 64
    assert checkpoint["deadline_at"] != recovery["checkpoint"]["deadline_at"]
    assert all((source / name).read_bytes() == raw for name, raw in before.items())
    assert (target / "recovery/source_checkpoint.json").exists()
    with pytest.raises(RuntimeError, match="already_claimed"):
        cli.prepare_recovery(recovery["engine"], source, output_dir=tmp_path / "another", **recovery["kwargs"])
    with recovery["engine"].connect() as connection:
        assert len(connection.execute(select(ai_bridge_job)).all()) == 1


@pytest.mark.parametrize("mutation", ["active_pid", "wrong_owner", "wrong_generation", "not_terminal", "wrong_input",
    "schema", "rules", "semantic_rejection", "unknown_timeout", "model_output", "registration", "successful_raw"])
def test_recovery_rejects_unproved_or_semantic_failures_without_claiming_source(recovery, monkeypatch, mutation):
    source, worker_dir = recovery["source"], recovery["worker_dir"]
    if mutation == "active_pid":
        monkeypatch.setattr(cli, "_pid_absent", lambda pid: False)
    elif mutation in {"wrong_owner", "wrong_generation"}:
        value = cli._read_json(worker_dir / "receipt.json")
        value["worker_id" if mutation == "wrong_owner" else "claim_generation"] = "other" if mutation == "wrong_owner" else 2
        cli._atomic(worker_dir / "receipt.json", value)
    elif mutation in {"not_terminal", "wrong_input"}:
        value = cli._read_json(worker_dir / "process/receipt.json")
        value["status" if mutation == "not_terminal" else "input_sha256"] = "RUNNING" if mutation == "not_terminal" else "0" * 64
        cli._atomic(worker_dir / "process/receipt.json", value)
    elif mutation == "schema":
        path = source / "stages/review/codex_draft_input.json"
        value = cli._read_json(path)
        value["response_schema"] = {"type": "object"}
        cli._atomic(path, value)
    elif mutation == "rules":
        value = cli._read_json(source / "run_receipt.json")
        value["rules_sha256"] = "0" * 64
        cli._atomic(source / "run_receipt.json", value)
    elif mutation in {"semantic_rejection", "unknown_timeout"}:
        message = "HTTP400 invalid schema; network" if mutation == "semantic_rejection" else "deadline exceeded"
        cli._atomic(worker_dir / "events.jsonl", (json.dumps({"type": "error", "message": message}) + "\n").encode(), raw=True)
    elif mutation == "model_output":
        cli._atomic(worker_dir / "answer.json", b'{"draft":"already produced"}', raw=True)
    elif mutation == "registration":
        value = cli._read_json(recovery["registration"])
        value["registration"]["worker_sha256"] = "f" * 64  # New worker identity cannot attest old termination.
        cli._atomic(recovery["registration"], value)
    else:
        cli._atomic(source / "stages/review/deepseek_draft_response.json", b'{"changed":true}', raw=True)
    with pytest.raises(RuntimeError, match="BLOCKED stage=recovery"):
        cli.inspect_recovery(recovery["engine"], source, **recovery["kwargs"])
    assert not (source / "recovery_claim.json").exists()


def test_other_active_job_blocks_recovery_and_original_row_is_never_reset(recovery):
    payload = deepcopy(recovery["payload"])
    payload["request_id"] = str(uuid4())
    research._enqueue(recovery["engine"], payload)
    with pytest.raises(RuntimeError, match="another research job is active"):
        cli.inspect_recovery(recovery["engine"], recovery["source"], **recovery["kwargs"])
    with recovery["engine"].connect() as connection:
        row = connection.execute(select(ai_bridge_job).where(ai_bridge_job.c.request_uid == recovery["payload"]["request_id"])).mappings().one()
        assert row["status"] == "failed" and row["attempts"] == 1


def test_recovery_cannot_be_chained_through_a_new_directory(recovery, tmp_path):
    target = cli.prepare_recovery(recovery["engine"], recovery["source"], output_dir=tmp_path / "recovered", **recovery["kwargs"])
    receipt = cli._read_json(target / "run_receipt.json")
    receipt["status"] = "FAILED"
    cli._atomic(target / "run_receipt.json", receipt)
    with pytest.raises(RuntimeError, match="recovery_limit_reached"):
        cli.inspect_recovery(recovery["engine"], target, **recovery["kwargs"])


def test_old_worker_revision_must_be_in_fetched_merged_main_history(monkeypatch):
    calls = []
    class Result:
        returncode, stdout = 1, b""
    def run(command, **kwargs):
        calls.append((command, kwargs))
        return Result()
    monkeypatch.setattr(cli.subprocess, "run", run)
    with pytest.raises(RuntimeError, match="not merged origin/main"):
        cli._prior_worker_identity("b" * 40)
    assert calls[0][0][-3:] == ["--is-ancestor", "b" * 40, "refs/remotes/origin/main"]
    assert calls[0][1]["timeout"] == 15


def test_saved_sorted_manifest_restores_the_original_context_hash_and_packet(recovery):
    # Production 218c failed this boundary: manifest key sorting changed only
    # task.context_sha256 although JSON values were equal. Original bytes win.
    saved = cli._read_json(recovery["source"] / "bundle.json")
    expected = recovery["checkpoint"]["input_sha256"]
    restored = cli._source_ordered_contexts(saved)
    def build(context):
        return core.build_packet(saved["facts"], report_kind="review", next_trade_date="2026-11-02",
            context=context, previous_research=None, now=NOW,
            source_digest_sha256=core.source_digest_identity(saved["facts"]))
    assert saved["review_context"] == restored["review_context"]
    assert core._hash(build(saved["review_context"])) != expected
    assert core._hash(build(restored["review_context"])) == expected


def test_prepared_checkpoint_calls_only_failed_codex_stage_again(recovery, tmp_path, monkeypatch):
    from test_market_decision import Providers
    target = cli.prepare_recovery(recovery["engine"], recovery["source"], output_dir=tmp_path / "recovered", **recovery["kwargs"])
    checkpoint = cli._read_json(target / "review_checkpoint.json")
    frozen = cli._source_ordered_contexts(cli._read_json(target / "bundle.json"))
    providers = Providers().install(monkeypatch)
    result = core.generate_research(frozen["facts"], report_kind="review", next_trade_date="2026-11-02",
        context=frozen["review_context"], previous_research=None, now=NOW, engine=object(), checkpoint=checkpoint)
    assert result["status"] == "COMPLETE"
    assert providers.calls.get("deepseek_draft", []) == []
    assert "codex_draft" in providers.calls
    accepted = {item["stage"]: item for item in result["generation_attempts"]}
    assert accepted["deepseek_draft"]["raw_response"] == recovery["checkpoint"]["stages"]["deepseek_draft"]["raw_response"]
    assert accepted["codex_draft"]["request_id"] == checkpoint["recovery"]["request_ids"]["codex_draft"]
    assert result["recovery"]["prior_attempt"] == recovery["prior"]


def test_default_resume_still_refuses_the_failed_stage_without_a_recovery_claim(recovery, monkeypatch):
    from test_market_decision import Providers
    checkpoint = deepcopy(recovery["checkpoint"])
    checkpoint["deadline_at"] = (core.datetime.now(core.timezone.utc) + timedelta(seconds=600)).isoformat()
    frozen = cli._source_ordered_contexts(cli._read_json(recovery["source"] / "bundle.json"))
    providers = Providers().install(monkeypatch)
    with pytest.raises(core.ResearchError, match="prior_attempt_not_reusable_codex_draft"):
        core.generate_research(frozen["facts"], report_kind="review", next_trade_date="2026-11-02",
            context=frozen["review_context"], previous_research=None, now=NOW, engine=object(), checkpoint=checkpoint)
    assert providers.calls == {}


def test_codex_final_recovery_rebuilds_the_editor_request_from_both_drafts_and_peer_reviews(monkeypatch):
    from unittest.mock import Mock
    packet = {"facts": {"F1": {}}}
    state = {"stages": {"deepseek_draft": {"raw_response": "original DS draft"},
                        "codex_draft": {"raw_response": "original CX draft"},
                        "deepseek_review": {"raw_response": "DS peer review"},
                        "codex_review": {"raw_response": "CX peer review"}}}
    parsed = {"DS peer review": {"findings": [{"id": "DS"}]}, "CX peer review": {"findings": [{"id": "CX"}]}}
    parser = Mock(side_effect=lambda raw, target, facts: parsed[raw])
    editor = Mock(return_value=([{"role": "user", "content": "canonical editor request"}], {"type": "object"}))
    monkeypatch.setattr(core, "_parse_review", parser)
    monkeypatch.setattr(core, "_final_request", editor)
    result = cli._recovery_request(core, packet, "unchanged rules", state, "codex_final")
    assert result == editor.return_value
    assert parser.call_args_list[0].args == ("DS peer review", core._body_hash("original CX draft"), packet)
    assert parser.call_args_list[1].args == ("CX peer review", core._body_hash("original DS draft"), packet)
    assert editor.call_args.args == (packet, "unchanged rules", {"deepseek": "original DS draft", "codex": "original CX draft"},
                                    {"deepseek_review": parsed["DS peer review"], "codex_review": parsed["CX peer review"]})
    assert cli.RECOVERY_STAGES == ("codex_draft", "codex_review", "codex_final")


def test_retired_final_audit_is_not_a_recoverable_execution_path():
    with pytest.raises(RuntimeError, match="unsupported_recovery_stage"):
        cli._recovery_request(core, {}, "rules", {"stages": {}}, "codex_final_review")
