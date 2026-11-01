from copy import deepcopy
from datetime import datetime, timezone
import json
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

from biz.review import market_decision as core, quant_digest, research_flow as flow
from tools import preview_market_decision as acceptance

DAY, NEXT = "2026-10-30", "2026-11-02"


def facts():
    return {"review_date": DAY, "adjust_type": 0, "publish_status": "ready", "generated_at": DAY + "T18:00:00+08:00",
        "data_cutoff_at": DAY + "T15:45:00+08:00", "compact_review": "manual body",
        "quality_json": {"target_date": DAY, "errors": [], "source_dates": {"target_bars": DAY}},
        "market_structure_json": {"sample_count": 2}, "industry_rotation_json": {}, "factor_validation_json": {
            "status": "available", "market_decision": {"editorial_review": True},
            "early_briefing": {"content": "old morning"}, "selection_review": {"content": "old strategy"}}}


def context():
    return {"cutoff_at": DAY + "T23:59:59+08:00", "market_data_asof_at": DAY + "T23:59:59+08:00", "events": {"rows": []}}


def bundle():
    return acceptance.freeze_bundle(facts(), context(), context(), next_trade_date=NEXT)


def test_bundle_preserves_original_bytes_but_strips_all_old_editorial_inputs():
    raw = json.dumps(facts(), ensure_ascii=False, indent=2).encode()
    frozen = acceptance.freeze_bundle(facts(), context(), context(), next_trade_date=NEXT, source_raw={
        "facts": raw, "review_context": acceptance._bytes(context()), "briefing_context": acceptance._bytes(context())})
    assert frozen["source_receipts"]["facts"]["raw_sha256"] == acceptance._sha(raw)
    assert frozen["facts"]["compact_review"] == ""
    assert set(frozen["facts"]["factor_validation_json"]) == {"status"}
    assert frozen["previous_research"] is None


@pytest.mark.parametrize("key", ["facts", "review_context", "source_receipts"])
def test_rehashed_bundle_cannot_change_original_facts_or_context(key):
    frozen = bundle()
    if key == "facts":
        frozen[key]["market_structure_json"]["sample_count"] = 99
    elif key == "review_context":
        frozen[key]["events"]["rows"] = [{"content": "added"}]
    else:
        frozen[key]["facts"]["raw_sha256"] = "0" * 64
    frozen["bundle_sha256"] = flow.artifact_hash({k: v for k, v in frozen.items() if k != "bundle_sha256"})
    with pytest.raises(RuntimeError):
        acceptance.validate_bundle(frozen)


def test_future_context_and_editorial_parent_are_rejected():
    future = context()
    future["cutoff_at"] = "2099-01-01T00:00:00+08:00"
    with pytest.raises(RuntimeError, match="future context cutoff"):
        acceptance.freeze_bundle(facts(), future, context(), next_trade_date=NEXT)
    with pytest.raises(core.ResearchError, match="nonautomatic_artifact"):
        acceptance.freeze_bundle(facts(), context(), context(), next_trade_date=NEXT, previous_research={"status": "COMPLETE"})


@pytest.fixture
def automatic_boundary(monkeypatch):
    # Unit tests isolate the orchestrator boundary; the CLI exposes no provider
    # injection. Real provider provenance is independently tested by core.
    def verify(value):
        if value.get("schema_version") != core.SCHEMA_VERSION or value.get("status") != "COMPLETE":
            raise core.ResearchError("not automatic")
    monkeypatch.setattr(core, "verify_automatic_research", verify)


def report(digest, kind, now=None, *, stage_count=7):
    names = core.STAGES
    value = {"schema_version": core.SCHEMA_VERSION, "status": "COMPLETE", "report_kind": kind,
        "review_date": DAY, "next_trade_date": NEXT, "generated_at": (now or datetime.now(timezone.utc)).isoformat(),
        "rules_sha256": core.rules_identity(), "source_digest_sha256": core.source_digest_identity(digest),
        "content": "自动复盘" if kind == "review" else "自动早报",
        "generation_attempts": [{"stage": stage, "status": "COMPLETE", "raw_response": "raw:" + kind + ":" + stage,
            "raw_sha256": acceptance._sha(("raw:" + kind + ":" + stage).encode())} for stage in names],
        "verification": {"status": "PASS", "stage_count": stage_count}}
    value["research_sha256"] = flow.artifact_hash(value)
    return value


def pair():
    digest = bundle()["facts"]
    parent = report(digest, "review")
    digest["factor_validation_json"]["market_decision"] = parent
    digest["compact_review"] = parent["content"]
    morning = report(digest, "briefing")
    morning.pop("research_sha256")
    morning.update(parent_research=deepcopy(parent), parent_research_sha256=parent["research_sha256"])
    morning["research_sha256"] = flow.artifact_hash(morning)
    return digest, morning


def test_api_requires_exact_date_no_fallback_and_both_untouched_reports(automatic_boundary):
    digest, morning = pair()
    saved = deepcopy(digest)
    saved["factor_validation_json"]["early_briefing"] = morning
    payload = {"date": DAY, "fallback": False, "data": [saved]}
    assert acceptance.verify_api_payload(payload, digest, morning)["status"] == "COMPLETE"
    for mutate in (lambda p: p.update(fallback=True), lambda p: p.update(date="2026-10-29"),
                   lambda p: p["data"][0].update(compact_review="edited")):
        changed = deepcopy(payload)
        mutate(changed)
        with pytest.raises(RuntimeError):
            acceptance.verify_api_payload(changed, digest, morning)


@pytest.mark.parametrize("stage_counts", [(7, 7)])
@pytest.mark.parametrize("publish", [False, True])
def test_uuid_run_resumes_completed_artifacts_without_rereading_or_regenerating(tmp_path, monkeypatch, automatic_boundary, stage_counts, publish):
    frozen, calls = bundle(), []
    def review(engine, digest, **kwargs):
        calls.append(kwargs)
        kwargs["progress"]({"stage": "deepseek_final_review" if stage_counts[0] == 6 else "deepseek_final_review_2", "completed": stage_counts[0], "total": len(core.STAGES)})
        digest["factor_validation_json"]["market_decision"] = report(digest, "review", kwargs["now"], stage_count=stage_counts[0])
        digest["compact_review"] = "自动复盘"
    def morning(engine, target, **kwargs):
        calls.append(kwargs)
        digest = kwargs["digest"]
        saved = acceptance._read_json(directory / "run_receipt.json")
        assert saved["provider_units"]["review"] == {"completed": stage_counts[0], "total": stage_counts[0]}
        kwargs["progress"]({"stage": "deepseek_final_review" if stage_counts[1] == 6 else "deepseek_final_review_2", "completed": stage_counts[1], "total": len(core.STAGES)})
        value = report(digest, "briefing", kwargs["now"], stage_count=stage_counts[1])
        value.pop("research_sha256")
        parent = digest["factor_validation_json"]["market_decision"]
        value.update(parent_research=parent, parent_research_sha256=parent["research_sha256"])
        value["research_sha256"] = flow.artifact_hash(value)
        return value
    monkeypatch.setattr(flow, "attach_review", review)
    monkeypatch.setattr(flow, "generate_briefing", morning)
    monkeypatch.setattr(flow, "collect_context", lambda *_a, **_k: pytest.fail("context reread"))
    monkeypatch.setattr(flow, "publication_snapshot", lambda *_a: {"source_digest_sha256": "frozen"})
    durable = {}
    def persist(_engine, digest, briefing, **_kwargs):
        durable.update(deepcopy(digest))
        durable["factor_validation_json"]["early_briefing"] = deepcopy(briefing)
        return {"status": "DATABASE_COMPLETE", "wecom_delivery_invoked": False}
    monkeypatch.setattr(flow, "persist_report_pair", persist)
    monkeypatch.setattr(flow, "load_digest", lambda *_a: durable)
    monkeypatch.setattr(acceptance, "verify_production_api", lambda *_a: ({"date": DAY}, {"status": "COMPLETE"}))
    directory = tmp_path / str(uuid4())
    options = {"publish": publish, "api_base": "http://example.invalid" if publish else None}
    receipt = acceptance.execute_run(None, frozen, directory, **options)
    assert receipt["completed_units"] == receipt["total_units"] == sum(stage_counts) + (2 if publish else 0)
    for kind, count in zip(("review", "briefing"), stage_counts):
        names = core.STAGES
        for stage in names:
            assert (directory / "stages" / kind / (stage + "_response.json")).read_text() == "raw:" + kind + ":" + stage
        assert (directory / "stages" / kind / "deepseek_final_review_response.json").exists()
    assert acceptance.execute_run(None, frozen, directory, resume=True, **options)["status"] == "COMPLETE"
    assert len(calls) == 2 and calls[0]["previous_research"] is None
    assert all("completion" not in call for call in calls)




def test_failed_stage_checkpoint_is_preserved_and_not_given_a_new_identity(tmp_path, monkeypatch):
    frozen, calls = bundle(), []
    saved = {"run_id": str(uuid4()), "stages": {"deepseek_draft": {"status": "FAILED"}}}
    def fail(engine, digest, **kwargs):
        calls.append(kwargs)
        error = core.ResearchError("prior attempt failed")
        error.checkpoint = saved
        raise error
    monkeypatch.setattr(flow, "attach_review", fail)
    directory = tmp_path / str(uuid4())
    for resume in (False, True):
        with pytest.raises(core.ResearchError):
            acceptance.execute_run(None, frozen, directory, resume=resume)
    assert calls[0]["checkpoint"] is None and calls[1]["checkpoint"] == saved
    assert calls[0]["now"] == calls[1]["now"]
    assert json.loads((directory / "run_receipt.json").read_text())["status"] == "FAILED"


@pytest.fixture
def db(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE st_quant_review_digest(review_date TEXT,adjust_type INTEGER,publish_status TEXT,compact_review TEXT,quality_json TEXT,market_structure_json TEXT,industry_rotation_json TEXT,factor_validation_json TEXT,data_cutoff_at TEXT,generated_at TEXT,PRIMARY KEY(review_date,adjust_type))"))
        conn.execute(text("CREATE TABLE st_scheduled_tasks(id INTEGER,task_type TEXT)"))
        conn.execute(text("CREATE TABLE st_scheduled_task_history(run_uid TEXT,task_type TEXT,run_at TEXT,status TEXT)"))
        conn.execute(text("CREATE TABLE st_job_run_v4(job_id TEXT,job_type TEXT,status TEXT)"))
        conn.execute(text("CREATE TABLE sys_wecom_delivery_receipt(content_sha256 TEXT)"))
    monkeypatch.setattr(quant_digest, "validate_quant_digest_runtime", lambda _engine: None)
    monkeypatch.setattr(quant_digest, "_DIGEST_EXISTING_STATUS", quant_digest._DIGEST_EXISTING_STATUS.replace("FOR UPDATE", ""))
    keys = (*acceptance.FACT_FIELDS, "compact_review")
    updates = ",".join(key + "=excluded." + key for key in keys if key not in {"review_date", "adjust_type"})
    monkeypatch.setattr(quant_digest, "_DIGEST_UPSERT", "INSERT INTO st_quant_review_digest (" + ",".join(keys) +
        ") VALUES (" + ",".join(":" + key for key in keys) + ") ON CONFLICT(review_date,adjust_type) DO UPDATE SET " + updates)
    yield engine
    engine.dispose()


def insert(engine, digest):
    payload = {key: json.dumps(value, ensure_ascii=False) if key in flow.JSON_FIELDS else value for key, value in digest.items()}
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO st_quant_review_digest (" + ",".join(payload) + ") VALUES (" + ",".join(":" + key for key in payload) + ")"), payload)


def test_pair_replaces_old_morning_in_one_transaction_with_independent_readback(db, automatic_boundary):
    insert(db, facts())
    expected = flow.publication_snapshot(db, DAY)
    digest, morning = pair()
    backups = []
    receipt = flow.persist_report_pair(db, digest, morning, expected=expected, backup=backups.append)
    saved = flow.load_digest(db, DAY)
    assert receipt["status"] == "DATABASE_COMPLETE" and receipt["wecom_delivery_invoked"] is False
    assert backups[0]["row"]["compact_review"] == "manual body"
    assert saved["compact_review"] == digest["compact_review"]
    assert saved["factor_validation_json"]["early_briefing"] == morning


def test_pair_second_write_failure_rolls_back_both_documents(db, automatic_boundary):
    insert(db, facts())
    expected = flow.publication_snapshot(db, DAY)
    digest, morning = pair()
    with db.begin() as conn:
        conn.execute(text("CREATE TRIGGER reject_second BEFORE UPDATE OF factor_validation_json ON st_quant_review_digest WHEN NEW.factor_validation_json LIKE '%自动早报%' BEGIN SELECT RAISE(ABORT,'pair rollback'); END"))
    with pytest.raises(IntegrityError, match="pair rollback"):
        flow.persist_report_pair(db, digest, morning, expected=expected, backup=lambda value: None)
    assert flow.load_digest(db, DAY)["compact_review"] == "manual body"


@pytest.mark.parametrize("boundary", ["source_race", "scheduler", "notification"])
def test_pair_guards_ownership_source_and_zero_wecom_before_write(db, automatic_boundary, boundary):
    insert(db, facts())
    expected = flow.publication_snapshot(db, DAY)
    digest, morning = pair()
    with db.begin() as conn:
        if boundary == "source_race":
            conn.execute(text("UPDATE st_quant_review_digest SET compact_review='newer publication'"))
        elif boundary == "scheduler":
            conn.execute(text("INSERT INTO st_scheduled_task_history VALUES('active','early_briefing',:d,'running')"), {"d": DAY})
        else:
            conn.execute(text("INSERT INTO sys_wecom_delivery_receipt VALUES(:h)"), {"h": acceptance._sha(digest["compact_review"].encode())})
    with pytest.raises(RuntimeError, match="BLOCKED stage="):
        flow.persist_report_pair(db, digest, morning, expected=expected, backup=lambda value: None)
    assert flow.load_digest(db, DAY)["compact_review"] != digest["compact_review"]
