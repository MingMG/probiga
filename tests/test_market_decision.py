"""Automatic v3 research proves six actual provider stages and untouched output."""
from copy import deepcopy
from datetime import timedelta
import json
from threading import Lock
from unittest.mock import Mock
from uuid import uuid4

import pytest

from biz.review import market_decision as decision
from biz.review.research_contract import DRAFT_VERSION
from server.ai_bridge.research import research_input_sha256
from test_research_contract import NOW, answer, material


class Providers:
    """Unit-test provider boundary only; no completion/editorial bypass hook."""
    def __init__(self, *, failed_stage=None, final_reject=False, findings=False, invalid_initial=False):
        self.calls = {}
        self.lock = Lock()
        self.failed_stage = failed_stage
        self.final_reject = final_reject
        self.findings = findings
        self.invalid_initial = invalid_initial

    def install(self, monkeypatch):
        monkeypatch.setattr(decision, "_call_deepseek", self.deepseek)
        monkeypatch.setattr(decision, "_call_codex", self.codex)
        return self

    def deepseek(self, messages, *, timeout_seconds, stage):
        return self.reply(messages, None, stage=stage, timeout_seconds=timeout_seconds)

    def codex(self, engine, messages, schema, *, timeout_seconds, stage, request_id, rules_sha256, progress=None):
        assert engine is not None
        return self.reply(messages, schema, stage=stage, timeout_seconds=timeout_seconds,
                          request_id=request_id, rules_sha256=rules_sha256)

    def reply(self, messages, schema, *, stage, timeout_seconds, request_id=None, rules_sha256=None):
        body = json.loads(messages[1]["content"])
        material = body["material"]
        schema = schema or body["response_schema"]
        packet = material["input"]
        with self.lock:
            assert stage not in self.calls, "duplicate provider attempt"
            self.calls[stage] = {"messages": deepcopy(messages), "schema": deepcopy(schema)}
        assert 0 < timeout_seconds <= 150
        if stage == self.failed_stage:
            raise decision.ResearchError("RESEARCH_BLOCKED stage=" + stage + " reason=unit_provider_failure")
        if stage.endswith("_draft"):
            value = getattr(self, "draft_factory", answer)(packet)
            value["units"][0]["statement"].append({"text": "来自独立甲方研究。" if stage.startswith("deepseek") else "来自独立乙方研究。"})
            if self.invalid_initial:
                value["units"][0]["statement"] = [{"text": "上涨率99.9%。"}]
        elif stage in {"deepseek_review", "codex_review", "codex_final_review"}:
            value = {"schema_version": "probiga.research-review.v3",
                     "reviewed_sha256": schema["properties"]["reviewed_sha256"]["const"], "findings": []}
            if self.findings and stage in {"deepseek_review", "codex_review"}:
                value["findings"] = [{"id": "R1", "severity": "warning", "category": "missed_evidence",
                                     "target": "/units/Uenv", "fact_ids": [next(iter(packet["facts"]))],
                                     "explanation": "请把有效广度证据与环境判断联系得更明确。"}]
            if stage == "codex_final_review":
                value["verdict"] = "REJECT" if self.final_reject else "PASS"
        else:
            assert stage == "deepseek_final"
            final_draft = getattr(self, "draft_factory", answer)(packet)
            final_draft["units"][0]["statement"].append({"text": "最终研究明确保留竞争解释。"})
            dispositions = []
            for name, review in material["peer_reviews"].items():
                for finding in review["findings"]:
                    dispositions.append({"review": name, "finding_id": finding["id"], "resolution": "corrected",
                                         "unit_ids": ["Uenv"], "fact_ids": finding["fact_ids"],
                                         "reason": "最终环境段依据实际广度说明行动边界。"})
            value = {"schema_version": "probiga.research-final.v3", "draft": final_draft, "dispositions": dispositions}
        raw = json.dumps(value, ensure_ascii=False)
        receipt = {"messages_sha256": decision._hash(messages), "raw_sha256": decision._body_hash(raw),
                   "started_at": NOW.isoformat(), "completed_at": NOW.isoformat(),
                   "provider_response_id": "provider-" + stage, "model": "unit-provider"}
        if stage.startswith("deepseek"):
            receipt.update(source="deepseek_api", transport="live_http", endpoint="https://api.deepseek.com/v1/chat/completions")
        else:
            receipt.update(source="codex_gpt", transport="live_codex_bridge", request_id=request_id,
                           rules_sha256=rules_sha256, ephemeral=True, process_exit_code=0, session_id="unit-" + stage,
                           input_sha256=research_input_sha256({"stage": stage, "messages": messages,
                                                              "response_schema": schema, "rules_sha256": rules_sha256}),
                           schema_sha256=decision._body_hash(json.dumps(schema, ensure_ascii=False, sort_keys=True, separators=(",", ":"))),
                           jsonl_sha256=decision._body_hash("unit-jsonl-" + raw),
                           worker_source_sha256=decision._body_hash("unit-worker"))
        with self.lock:
            self.calls[stage].update(raw=raw, receipt=deepcopy(receipt))
        return decision.ProviderReply(raw, receipt)


def completed_research(monkeypatch, **options):
    providers = Providers(**options).install(monkeypatch)
    data, context = material()
    progress = []
    result = decision.generate_research(data, report_kind="review", next_trade_date="2026-11-02",
                                       context=context, now=NOW, engine=object(), progress=progress.append)
    return result, providers, progress, data, context


def test_six_provider_calls_are_independent_then_cross_reviewed_and_final_semantics_are_untouched(monkeypatch):
    result, providers, progress, *_ = completed_research(monkeypatch, findings=True)
    assert set(providers.calls) == set(decision.STAGES)
    ds = providers.calls["deepseek_draft"]["raw"]
    cx = providers.calls["codex_draft"]["raw"]
    assert ds != cx
    for stage, opposite in (("deepseek_review", cx), ("codex_review", ds)):
        call = providers.calls[stage]
        material = json.loads(call["messages"][1]["content"])["material"]
        assert material["target_draft"] == opposite
        assert call["schema"]["properties"]["reviewed_sha256"]["const"] == decision._body_hash(opposite)
    final = json.loads(providers.calls["deepseek_final"]["raw"])
    assert result["model_output"] == final
    assert result["draft"] == final["draft"]
    assert len(final["dispositions"]) == 2
    assert result["content"] == decision.render_research(result)
    assert result["schema_version"] == decision.SCHEMA_VERSION
    assert result["verification"]["raw_semantics_unchanged"] is True
    assert progress[-1]["stage"] == "acceptance" and progress[-1]["completed"] == 6
    decision.verify_automatic_research(result)


@pytest.mark.parametrize("target", ["units.U8", "report.checks.open；units.U29", "roles.博弈与资金分析师 unit_ids=[M3,R1]"])
def test_peer_review_preserves_declared_descriptive_locator_without_undeclared_pointer_rule(monkeypatch, target):
    result, *_ = completed_research(monkeypatch)
    value = {"schema_version": "probiga.research-review.v3", "reviewed_sha256": "0" * 64,
             "findings": [{"id": "R1", "severity": "blocker", "category": "structure", "target": target,
                           "fact_ids": [], "explanation": "定位草稿中需要模型修订的单元。"}]}
    assert decision._parse_review(json.dumps(value, ensure_ascii=False), "0" * 64, result["input_packet"]) == value


def test_invalid_initial_numeric_claims_are_diagnosed_and_only_provider_revision_can_fix_them(monkeypatch):
    result, providers, *_ = completed_research(monkeypatch, invalid_initial=True)
    final_material = json.loads(providers.calls["deepseek_final"]["messages"][1]["content"])["material"]
    assert all("free_text_number_requires_reference" in errors[0] for errors in final_material["deterministic_errors"].values())
    assert "99.9" in providers.calls["deepseek_draft"]["raw"]
    assert "99.9" not in result["content"]
    assert result["draft"] == json.loads(providers.calls["deepseek_final"]["raw"])["draft"]


@pytest.mark.parametrize("stage", decision.STAGES)
def test_each_external_stage_failure_is_terminal_without_fallback_or_duplicate_attempt(monkeypatch, stage):
    providers = Providers(failed_stage=stage).install(monkeypatch)
    data, context = material()
    with pytest.raises(decision.ResearchError) as caught:
        decision.generate_research(data, report_kind="review", next_trade_date="2026-11-02",
                                   context=context, now=NOW, engine=object())
    assert stage in str(caught.value)
    assert caught.value.checkpoint["stages"][stage]["status"] == "FAILED"
    assert len(providers.calls) <= 6
    if stage.endswith("_draft"):
        assert not set(providers.calls).intersection({"deepseek_review", "codex_review", "deepseek_final", "codex_final_review"})
    if stage in {"deepseek_review", "codex_review"}:
        assert "deepseek_final" not in providers.calls
    if stage == "deepseek_final":
        assert "codex_final_review" not in providers.calls


def test_final_independent_rejection_never_publishes_a_generated_body(monkeypatch):
    providers = Providers(final_reject=True).install(monkeypatch)
    data, context = material()
    with pytest.raises(decision.ResearchError, match="codex_rejected_final_draft"):
        decision.generate_research(data, report_kind="review", next_trade_date="2026-11-02",
                                   context=context, now=NOW, engine=object())
    assert set(providers.calls) == set(decision.STAGES)


def test_complete_checkpoint_resume_reuses_every_stage_and_stable_generation_clock(monkeypatch):
    result, _, progress, data, context = completed_research(monkeypatch)
    checkpoint = progress[-1]["checkpoint"]
    no_call = Mock(side_effect=AssertionError("completed checkpoint must not call a provider"))
    monkeypatch.setattr(decision, "_call_deepseek", no_call)
    monkeypatch.setattr(decision, "_call_codex", no_call)
    resumed = decision.generate_research(data, report_kind="review", next_trade_date="2026-11-02",
                                        context=context, engine=object(), checkpoint=checkpoint)
    assert resumed == result
    no_call.assert_not_called()
    with pytest.raises(decision.ResearchError, match="resume"):
        decision.generate_research(data, report_kind="review", next_trade_date="2026-11-02",
                                   context=context, now=NOW + timedelta(minutes=1), engine=object(), checkpoint=checkpoint)


@pytest.mark.parametrize("state", ["RUNNING", "FAILED"])
def test_unconfirmed_or_failed_provider_attempt_is_not_reissued(monkeypatch, state):
    _, _, progress, data, context = completed_research(monkeypatch)
    checkpoint = progress[-1]["checkpoint"]
    checkpoint["stages"]["deepseek_final"]["status"] = state
    no_call = Mock(side_effect=AssertionError("must not duplicate an unconfirmed prior request"))
    monkeypatch.setattr(decision, "_call_deepseek", no_call)
    monkeypatch.setattr(decision, "_call_codex", no_call)
    with pytest.raises(decision.ResearchError, match="prior_attempt_not_reusable_deepseek_final"):
        decision.generate_research(data, report_kind="review", next_trade_date="2026-11-02",
                                   context=context, engine=object(), checkpoint=checkpoint)
    no_call.assert_not_called()


def test_changed_packet_cannot_reuse_a_checkpoint(monkeypatch):
    _, _, progress, data, context = completed_research(monkeypatch)
    context["cutoff_at"] = "2026-10-30T23:58:00+08:00"
    no_call = Mock(side_effect=AssertionError("different facts cannot use old provider receipts"))
    monkeypatch.setattr(decision, "_call_deepseek", no_call)
    monkeypatch.setattr(decision, "_call_codex", no_call)
    with pytest.raises(decision.ResearchError, match="checkpoint_identity_mismatch"):
        decision.generate_research(data, report_kind="review", next_trade_date="2026-11-02",
                                   context=context, engine=object(), checkpoint=progress[-1]["checkpoint"])
    no_call.assert_not_called()


def _terminal_recovery_checkpoint(monkeypatch, failed_stage="codex_draft"):
    providers = Providers(failed_stage=failed_stage).install(monkeypatch)
    data, context = material()
    with pytest.raises(decision.ResearchError) as caught:
        decision.generate_research(data, report_kind="review", next_trade_date="2026-11-02",
                                   context=context, now=NOW, engine=object())
    checkpoint = deepcopy(caught.value.checkpoint)
    prior = checkpoint["stages"].pop(failed_stage)
    assert prior["status"] == "FAILED"
    replacement = str(uuid4())
    checkpoint["recovery"] = {
        "schema": "probiga.research-recovery.v1", "recovery_count": 1,
        "failed_stage": failed_stage, "prior_attempt": prior,
        "request_ids": {failed_stage: replacement},
        # The permanent CLI proves these external terminal boundaries before
        # passing the checkpoint. This unit test exercises only core routing.
        "proof": {"database_status": "failed", "local_terminal": "failed", "pid_absent": True},
    }
    return checkpoint, replacement, data, context


def test_terminal_codex_recovery_uses_new_request_uuid_and_reuses_completed_deepseek_bytes(monkeypatch):
    checkpoint, replacement, data, context = _terminal_recovery_checkpoint(monkeypatch)
    successful_ds = deepcopy(checkpoint["stages"]["deepseek_draft"])
    original_checkpoint = deepcopy(checkpoint)
    assert successful_ds["status"] == "COMPLETE"
    assert checkpoint["recovery"]["prior_attempt"]["request_id"] != replacement
    providers = Providers().install(monkeypatch)
    result = decision.generate_research(data, report_kind="review", next_trade_date="2026-11-02",
                                       context=context, engine=object(), checkpoint=checkpoint)
    assert set(providers.calls) == set(decision.STAGES) - {"deepseek_draft"}
    stages = {stage["stage"]: stage for stage in result["generation_attempts"]}
    assert stages["deepseek_draft"] == {"stage": "deepseek_draft", **successful_ds}
    assert stages["codex_draft"]["request_id"] == replacement
    assert stages["codex_draft"]["provider_receipt"]["request_id"] == replacement
    assert result["recovery"] == original_checkpoint["recovery"]
    assert checkpoint == original_checkpoint
    decision.verify_automatic_research(result)


@pytest.mark.parametrize("mutation,reason", [
    (lambda recovery: recovery.update(recovery_count=2), "invalid_terminal_recovery"),
    (lambda recovery: recovery.update(schema="other-recovery"), "invalid_terminal_recovery"),
    (lambda recovery: recovery["prior_attempt"].update(status="RUNNING"), "invalid_terminal_recovery"),
    (lambda recovery: recovery["request_ids"].update(codex_draft="not-a-uuid"), "invalid_attempt_uuid"),
    (lambda recovery: recovery["request_ids"].update(codex_draft=recovery["request_ids"]["codex_draft"].upper()), "invalid_attempt_uuid"),
    (lambda recovery: recovery["request_ids"].update(codex_draft=recovery["prior_attempt"]["request_id"]), "invalid_terminal_recovery"),
])
def test_terminal_recovery_rejects_second_attempt_nonterminal_prior_or_invalid_uuid(monkeypatch, mutation, reason):
    checkpoint, _, data, context = _terminal_recovery_checkpoint(monkeypatch)
    mutation(checkpoint["recovery"])
    no_call = Mock(side_effect=AssertionError("invalid recovery must not call any provider"))
    monkeypatch.setattr(decision, "_call_deepseek", no_call)
    monkeypatch.setattr(decision, "_call_codex", no_call)
    with pytest.raises(decision.ResearchError, match=reason):
        decision.generate_research(data, report_kind="review", next_trade_date="2026-11-02",
                                   context=context, engine=object(), checkpoint=checkpoint)
    no_call.assert_not_called()


def test_terminal_request_uuid_override_is_not_a_deepseek_retry_channel(monkeypatch):
    checkpoint, _, data, context = _terminal_recovery_checkpoint(monkeypatch, failed_stage="deepseek_draft")
    no_call = Mock(side_effect=AssertionError("DeepSeek override must never issue a retry"))
    monkeypatch.setattr(decision, "_call_deepseek", no_call)
    monkeypatch.setattr(decision, "_call_codex", no_call)
    with pytest.raises(decision.ResearchError, match="invalid_terminal_recovery"):
        decision.generate_research(data, report_kind="review", next_trade_date="2026-11-02",
                                   context=context, engine=object(), checkpoint=checkpoint)
    no_call.assert_not_called()


@pytest.mark.parametrize("mutation,reason", [
    (lambda r: r.update(editorial_review={"status": "PASS"}), "nonautomatic_artifact"),
    (lambda r: r["generation_attempts"][0]["provider_receipt"].update(source="manual"), "deepseek_identity_missing"),
    (lambda r: r["generation_attempts"][1]["provider_receipt"].update(source="manual"), "codex_identity_missing"),
    (lambda r: r["generation_attempts"][1]["provider_receipt"].update(ephemeral=False), "codex_identity_missing"),
    (lambda r: r["generation_attempts"][1]["provider_receipt"].update(request_id="another-request"), "codex_identity_missing"),
    (lambda r: r["generation_attempts"][0].update(stage="manual_draft"), "incomplete_provider_stages"),
    (lambda r: r["generation_attempts"][0].update(raw_response="manually replaced"), "provider_receipt_mismatch"),
    (lambda r: r["draft"]["units"][0]["statement"].append({"text": "人工追加观点。"}), "model_semantics_edited"),
    (lambda r: r.update(content=r["content"] + "人工改稿"), "rendered_content_edited"),
    (lambda r: r["verification"].update(final_review={"verdict": "PASS"}), "review_result_edited"),
])
def test_provenance_verifier_rejects_manual_stage_raw_or_editorial_mutations(monkeypatch, mutation, reason):
    result, *_ = completed_research(monkeypatch)
    changed = deepcopy(result)
    mutation(changed)
    with pytest.raises(decision.ResearchError, match=reason):
        decision.verify_automatic_research(changed)


def test_recomputed_artifact_hash_does_not_authorize_changed_input_or_body(monkeypatch):
    result, *_ = completed_research(monkeypatch)
    result["input_packet"]["facts"]["F1"]["values"]["return_median_pct"] = 88.8
    result["verification"]["input_sha256"] = decision._hash(result["input_packet"])
    result["research_sha256"] = decision._hash({key: value for key, value in result.items() if key != "research_sha256"})
    with pytest.raises(decision.ResearchError, match="message|input|stage"):
        decision.verify_automatic_research(result)


@pytest.mark.parametrize("field,value", [
    ("report_kind", "intraday"), ("review_date", "2026-10-29"),
    ("next_trade_date", "2026-11-03"), ("generated_at", "2026-11-01T21:00:00+08:00"),
    ("data_cutoff_at", "2026-10-30T15:00:00+08:00"), ("context_cutoff_at", "2026-11-02T08:00:00+08:00"),
    ("source_digest_sha256", "another-digest"),
])
def test_top_level_scope_cannot_relabel_genuine_old_model_output_even_after_rerender(monkeypatch, field, value):
    result, *_ = completed_research(monkeypatch)
    original_content = result["content"]
    result[field] = value
    result["content"] = decision.render_research(result)
    result["research_sha256"] = decision._hash({key: item for key, item in result.items() if key != "research_sha256"})
    assert result["content"] == original_content
    with pytest.raises(decision.ResearchError, match="report_scope_changed_" + field):
        decision.verify_automatic_research(result)


def test_automatic_briefing_requires_and_inherits_the_matching_v3_parent(monkeypatch):
    parent, _, _, data, context = completed_research(monkeypatch)
    providers = Providers().install(monkeypatch)
    providers.draft_factory = lambda p: briefing_answer(p)
    result = decision.generate_research(data, report_kind="briefing", next_trade_date="2026-11-02", context=context,
                                       previous_research=parent, now=NOW, engine=object())
    assert result["input_packet"]["baseline"]["parent_report_id"]
    assert result["draft"]["report"]["continuity"][0]["change"] == "inherited"
    assert "A股早报参考" in result["content"]
    changed = deepcopy(data)
    changed["market_structure_json"]["return_median_pct"] = -8.0
    with pytest.raises(decision.ResearchError, match="new_automatic_parent_required"):
        decision.generate_research(changed, report_kind="briefing", next_trade_date="2026-11-02", context=context,
                                   previous_research=parent, now=NOW, engine=object())


def briefing_answer(p):
    draft = answer(p)
    if p.get("baseline"):
        fid = next(iter(p["baseline"]["unit_facts"]))
        draft["units"].append({"id": "Uprior", "kind": "baseline_restatement", "topic": "continuity",
                               "evidence_ids": [fid], "statement": [{"fact_id": fid, "field": "statement"}]})
        draft["report"]["continuity"] = [{"baseline_fact_id": fid, "change": "inherited", "new_fact_ids": [], "reason_units": ["Uprior"]}]
    return draft


def test_fact_identity_ignores_only_persisted_reports_and_execution_ledger():
    data, _ = material()
    before = decision.source_digest_identity(data)
    data["factor_validation_json"].update(market_decision={"content": "report"}, early_briefing={"content": "brief"},
                                          selection_review={"large_execution_ledger": [1, 2, 3]})
    assert decision.source_digest_identity(data) == before
    data["market_structure_json"]["sample_count"] -= 1
    assert decision.source_digest_identity(data) != before
