"""Automatic research proves six, seven or eight bounded provider stages."""
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
    def __init__(self, *, failed_stage=None, final_reject=False, findings=False, invalid_initial=False, oversized_final=False, many_findings=False, first_reject=False,
                 invalid_final=False, repeat_invalid_final=False, invalid_json_final=False):
        self.calls = {}
        self.lock = Lock()
        self.failed_stage = failed_stage
        self.final_reject = final_reject
        self.first_reject = first_reject
        self.findings = findings
        self.invalid_initial = invalid_initial
        self.oversized_final = oversized_final
        self.many_findings = many_findings
        self.invalid_final = invalid_final
        self.repeat_invalid_final = repeat_invalid_final
        self.invalid_json_final = invalid_json_final

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
        assert 0 < timeout_seconds <= (300 if packet['task']['kind'] == 'intraday' else 150)
        if stage == self.failed_stage:
            raise decision.ResearchError("RESEARCH_BLOCKED stage=" + stage + " reason=unit_provider_failure")
        if stage.endswith("_draft"):
            value = getattr(self, "draft_factory", answer)(packet)
            value["units"][0]["statement"].append({"text": "来自独立甲方研究。" if stage.startswith("deepseek") else "来自独立乙方研究。"})
            if self.invalid_initial:
                value["units"][0]["statement"] = [{"text": "上涨率99.9%。"}]
        elif stage in {"deepseek_review", "codex_review", "deepseek_final_review", "deepseek_final_review_2"}:
            value = {"schema_version": "probiga.research-review.v3",
                     "reviewed_sha256": schema["properties"]["reviewed_sha256"]["const"], "findings": []}
            if self.findings and stage in {"deepseek_review", "codex_review"}:
                value["findings"] = [{"id": "R1", "severity": "warning", "category": "missed_evidence",
                                     "target": "/units/Uenv", "fact_ids": [next(iter(packet["facts"]))],
                                     "explanation": "请把有效广度证据与环境判断联系得更明确。"}]
            if self.many_findings and stage in {"deepseek_review", "codex_review"}:
                value["findings"] = [{"id": "R" + str(index), "severity": "warning", "category": "missed_evidence",
                    "target": "units.Uenv", "fact_ids": [next(iter(packet["facts"]))],
                    "explanation": "请保留并逐条处置此审查意见。"}
                    for index in range(12 if stage == "deepseek_review" else 11)]
            if stage in {"deepseek_final_review", "deepseek_final_review_2"}:
                reject = self.final_reject or (self.first_reject and stage == "deepseek_final_review")
                value["verdict"] = "REJECT" if reject else "PASS"
                if reject:
                    value["findings"] = [{"id": "A1", "severity": "blocker", "category": "unsupported_fact",
                        "target": "units.Uenv", "fact_ids": [next(iter(packet["facts"]))],
                        "explanation": "最终原句仍需由作者复核证据并自动修正或反驳。"}]
        else:
            assert stage in {"codex_final", "codex_arbitration"}
            final_draft = getattr(self, "draft_factory", answer)(packet)
            final_draft["units"][0]["statement"].append({"text": "最终研究明确保留竞争解释。"})
            dispositions = {}
            for name, review in material["peer_reviews"].items():
                for finding in review["findings"]:
                    dispositions[name+":"+finding["id"]] = {"review": name, "finding_id": finding["id"], "resolution": "corrected",
                                         "unit_ids": ["Uenv"], "fact_ids": finding["fact_ids"],
                                         "reason": "最终环境段依据实际广度说明行动边界。"}
            if self.oversized_final:
                while len(final_draft["units"]) < 52:
                    unit = deepcopy(final_draft["units"][0])
                    unit["id"] = "Oversize" + str(len(final_draft["units"]))
                    final_draft["units"].append(unit)
            if self.invalid_final and (stage == "codex_final" or self.repeat_invalid_final):
                final_draft["report"]["scenarios"][0].update(trigger_units=["Cmid"], abandon_units=["Cearly"])
            for key,entry in dispositions.items():
                allowed=schema['properties']['dispositions']['properties'][key]['properties']
                dispositions[key]={k:v for k,v in entry.items() if k in allowed}
            value = {"schema_version": schema['properties']['schema_version']['const'], "draft": final_draft, "dispositions": dispositions}
        raw = json.dumps(value, ensure_ascii=False)
        if self.invalid_json_final and stage == "codex_final":
            raw = '{"schema_version":"probiga.research-final.v3","draft":'
        receipt = {"messages_sha256": decision._hash(messages), "raw_sha256": decision._body_hash(raw),
                   "started_at": NOW.isoformat(), "completed_at": NOW.isoformat(),
                   "provider_response_id": "provider-" + stage, "model": "unit-provider"}
        if stage.startswith("deepseek"):
            policy = decision._deepseek_policy(packet["task"]["kind"])
            receipt.update(source="deepseek_api", transport="live_http", endpoint=decision.DEEPSEEK_ENDPOINT,
                           model=policy["model"], request_policy=policy, response_schema_sha256=decision._hash(schema), response_status='completed')
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
    providers=Providers(**options).install(monkeypatch)
    data,context=material()
    result=decision.generate_research(data,report_kind="review",next_trade_date="2026-11-02",context=context,now=NOW,engine=object())
    return result,providers


def test_intraday_call_can_finish_after_ninety_seconds_within_total_deadline(monkeypatch):
    from pathlib import Path
    from types import SimpleNamespace
    from biz.intraday_alert.research import freeze_inputs
    raw = json.loads((Path(__file__).parent / 'fixtures/intraday_consumption_20261102.json').read_text(encoding='utf-8'))
    raw['quality'] = {'status': raw['quality_status']}
    frozen = freeze_inputs(raw, [], [], mode='replay')
    clock = SimpleNamespace(value=1000.)
    monkeypatch.setattr(decision, 'time', SimpleNamespace(monotonic=lambda: clock.value))
    budgets = {}
    def provider(*args, stage, timeout_seconds, **kwargs):
        budgets[stage] = timeout_seconds
        if stage.endswith('_draft'):
            if stage == 'codex_draft':
                if timeout_seconds <= 91:
                    raise decision.ResearchError('premature_per_call_timeout')
                clock.value += 91
            return decision.ProviderReply('{}', {})
        raise decision.ResearchError('peer_boundary_reached_after_slow_draft')
    monkeypatch.setattr(decision, '_call_deepseek', provider)
    monkeypatch.setattr(decision, '_call_codex', provider)
    with pytest.raises(decision.ResearchError, match='peer_boundary_reached_after_slow_draft'):
        decision.generate_research(frozen['digest'], report_kind='intraday', next_trade_date='2026-11-02',
            context=frozen['context'], now=decision._now(), engine=object())
    assert 290 < budgets['codex_draft'] <= 300
    assert 200 < budgets['deepseek_review'] <= 209


def test_fixed_seven_stages_preserve_reject_and_arbitrate(monkeypatch):
    result,providers=completed_research(monkeypatch,findings=True,final_reject=True)
    assert set(providers.calls)==set(decision.STAGES)
    assert result["verification"]["synthesis_review"]["verdict"]=="REJECT"
    assert result["verification"]["consensus"] is False
    assert result["verification"]["stage_count"]==7
    assert result["model_output"]==json.loads(providers.calls["codex_arbitration"]["raw"])
    assert "不代表双方一致" in result["content"]
    decision.verify_automatic_research(result)


@pytest.mark.parametrize("mutation,reason",[
    (lambda r:r["verification"].update(consensus=True),"arbitration_proof_changed"),
    (lambda r:r["draft"]["units"][0]["statement"].append({"text":"人工改稿"}),"model_semantics_edited"),
    (lambda r:r["generation_attempts"].pop(),"incomplete_provider_stages"),
    (lambda r:r.update(content=r["content"]+"伪造正文"),"rendered_content_edited")])
def test_no_consensus_or_manual_rewrite_can_be_fabricated(monkeypatch,mutation,reason):
    result,_=completed_research(monkeypatch)
    mutation(result)
    with pytest.raises(decision.ResearchError,match=reason):decision.verify_automatic_research(result)


def test_unresolved_finding_blocks_even_when_draft_is_valid(monkeypatch):
    result,_=completed_research(monkeypatch,findings=True)
    final=deepcopy(result["model_output"])
    next(iter(final["dispositions"].values()))["resolution"]="unresolved"
    with pytest.raises(decision.ResearchError,match="unresolved_finding"):
        decision._validate_final(json.dumps(final),result["input_packet"],result["verification"]["peer_reviews"])


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
    (lambda r: r["verification"].update(synthesis_review={"verdict": "PASS"}), "arbitration_proof_changed"),
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


def test_real_prior_briefing_remains_auditable_when_research_rules_advance():
    from pathlib import Path
    prior = json.loads((Path(__file__).parent / "fixtures" / "automatic_briefing_20261102_0831.json").read_text(encoding="utf-8"))
    assert prior["rules_sha256"] != decision.rules_identity()
    original = deepcopy(prior)
    decision.verify_automatic_research(prior)
    assert prior == original
    prior["content"] += "事后改成预测成功"
    with pytest.raises(decision.ResearchError, match="rendered_content_edited"):
        decision.verify_automatic_research(prior)


def test_archived_rule_bundle_must_match_its_content_identity(tmp_path, monkeypatch):
    from pathlib import Path
    import shutil
    prior = json.loads((Path(__file__).parent / "fixtures" / "automatic_briefing_20261102_0831.json").read_text(encoding="utf-8"))
    root = tmp_path / "rules"
    shutil.copytree(decision.RULES_DIR, root)
    p = root / "archive" / (prior["rules_sha256"] + ".json")
    changed = json.loads(p.read_text(encoding="utf-8"))
    changed["deepseek_intraday_policy"]["max_tokens"] = 42
    p.write_text(json.dumps(changed), encoding="utf-8")
    monkeypatch.setattr(decision, "RULES_DIR", root)
    with pytest.raises(decision.ResearchError, match="rules_archive_identity_mismatch"):
        decision.verify_automatic_research(prior)


def test_real_duplicate_deepseek_review_is_rejected_without_selecting_a_version():
    from pathlib import Path
    raw = (Path(__file__).parent / 'fixtures/deepseek_duplicate_review_20261102.txt').read_text(encoding='utf-8')
    # Production returned two different reviews despite requesting JSON mode.
    # Neither the first nor last object may be silently selected for publication.
    first, end = json.JSONDecoder().raw_decode(raw)
    assert first['schema_version'] == 'probiga.research-review.v3'
    assert json.loads(raw[end:])['schema_version'] == first['schema_version']
    with pytest.raises(decision.ResearchError, match='peer_review.*Extra data'):
        decision._parse_review(raw, '0' * 64, {'facts': {}}, final=True, require_pass=False)


def test_intraday_schema_bounds_real_oversized_provider_draft_and_keeps_daily_contract():
    from pathlib import Path
    from biz.review.research_contract import _check_schema, ContractError
    raw=json.loads((Path(__file__).parent/'fixtures/deepseek_oversized_intraday_20261102.json').read_text(encoding='utf-8'))
    assert len(raw['units'])==21
    _check_schema(raw,decision.draft_json_schema())
    rules,_=decision._load_rules()
    packet={'task':{'kind':'intraday'}}
    schema=decision.build_stage_request('deepseek_draft',packet,rules)[1]
    with pytest.raises(ContractError,match='schema_array.*units'):
        _check_schema(raw,schema)
    for stage in ('codex_final','codex_arbitration'):
        final=decision.build_stage_request(stage,packet,rules,drafts={},reviews={},final_raw='{}')[1]
        assert final['properties']['draft']==schema
    daily=decision.build_stage_request('deepseek_draft',{'task':{'kind':'review'}},rules)[1]
    assert daily==decision.draft_json_schema()


def test_compact_ledger_keeps_all_real_findings_and_requires_cited_fact_evidence():
    from pathlib import Path
    frozen=json.loads((Path(__file__).parent/'fixtures/intraday_ledger_evidence_20261102.json').read_text(encoding='utf-8'))
    old=frozen['final'];packet=frozen['packet'];reviews=frozen['reviews']
    assert len(old['dispositions'])==30
    final=deepcopy(old);final['schema_version']='probiga.research-final.v4'
    final['dispositions']={k:{field:v[field] for field in ('resolution','unit_ids')} for k,v in old['dispositions'].items()}
    schema=decision._final_schema(reviews,draft_schema=decision._draft_contract(packet,decision._load_rules()[0]),unit_citations=True)
    accepted=decision._validate_final(json.dumps(final,ensure_ascii=False),packet,reviews,schema=schema)
    assert accepted['draft']==old['draft']
    assert set(accepted['dispositions'])==set(old['dispositions'])
    assert len(json.dumps(accepted,ensure_ascii=False)) < len(json.dumps(old,ensure_ascii=False))*0.8
    template={'draft':old['draft'],'input_packet':packet,'verification':{'peer_reviews':{k:v for k,v in reviews.items() if k!='deepseek_final_review'},'synthesis_review':reviews['deepseek_final_review']}}
    assert decision.render_research({**template,'model_output':accepted})==decision.render_research({**template,'model_output':old})
    changed=deepcopy(final);key=next(iter(changed['dispositions']))
    changed['dispositions'][key]={'resolution':'disputed_with_evidence','unit_ids':[]}
    with pytest.raises(decision.ResearchError,match='schema_array'):
        decision._validate_final(json.dumps(changed),packet,reviews,schema=schema)
    changed['dispositions'][key]['unit_ids']=['does_not_exist']
    with pytest.raises(decision.ResearchError,match='unknown_disposition_reference'):
        decision._validate_final(json.dumps(changed),packet,reviews,schema=schema)
