"""Input freezing tests use real outbox writes and mocked public source reads."""
from copy import deepcopy
from datetime import datetime, timedelta

import pytest

from biz.intraday_alert import research
from biz.review import market_decision, research_flow, research_inputs, theme_evidence
from tests.test_intraday_research import db, observation, events, final_result, queue, load, retained, NOW
from tests.test_research_flow import _digest, _with_parent, REVIEW, NEXT


def enhanced(context):
    return {**deepcopy(context), "technical_facts": {"status": "UNKNOWN", "baseline_trade_date": "2026-10-29"},
            "fundamental_facts": {"status": "UNKNOWN"}}


def mock_context_sources(monkeypatch):
    monkeypatch.setattr(theme_evidence, "collect_theme_evidence", lambda *args, **kwargs: {"stocks": []})
    monkeypatch.setattr(research_flow, "read_records", lambda *args, **kwargs: [])


@pytest.mark.parametrize("overnight,kind", [(False, "review"), (True, "briefing")])
def test_normal_context_collection_freezes_enriched_evidence_once(monkeypatch, overnight, kind):
    mock_context_sources(monkeypatch)
    calls = []
    at = datetime(2026, 8, 31, 8) if overnight else datetime(2026, 8, 28, 17)

    def enrich(engine, digest, context, **kwargs):
        calls.append((engine, digest, deepcopy(context), kwargs))
        assert kwargs["report_kind"] == kind
        assert digest == {"review_date": REVIEW}
        assert context["cutoff_at"] == at.replace(tzinfo=research.TZ).isoformat()
        return enhanced(context)

    monkeypatch.setattr(research_inputs, "enrich_context", enrich)
    result = research_flow.collect_context(None, REVIEW, now=at, overnight=overnight)
    assert len(calls) == 1
    assert result["technical_facts"]["status"] == "UNKNOWN"


def test_explicit_review_context_is_immutable_and_never_requeries_enrichment(monkeypatch):
    monkeypatch.setattr(research_inputs, "enrich_context", lambda *args, **kwargs: pytest.fail("reread frozen input"))
    frozen = {"explicit_frozen": True}
    original = deepcopy(frozen)

    def generate(digest, **kwargs):
        assert kwargs["context"] == original
        return {"status": "COMPLETE", "content": "automatic"}

    monkeypatch.setattr(market_decision, "generate_research", generate)
    research_flow.attach_review(None, _digest(), context=frozen, next_trade_date=NEXT, previous_research=None)
    assert frozen == original


def test_explicit_briefing_context_does_not_requery_even_when_recovering(monkeypatch):
    monkeypatch.setattr(research_inputs, "enrich_context", lambda *args, **kwargs: pytest.fail("reread frozen input"))
    monkeypatch.setattr(market_decision, "verify_automatic_research", lambda *args: None)
    frozen, checkpoint = {"explicit_frozen": True}, {"stages": {"deepseek_draft": {"status": "COMPLETE"}}}

    def generate(digest, **kwargs):
        assert kwargs["context"] == frozen and kwargs["checkpoint"] == checkpoint
        return {"status": "COMPLETE", "content": "automatic"}

    monkeypatch.setattr(market_decision, "generate_research", generate)
    research_flow.generate_briefing(None, REVIEW, digest=_with_parent(), context=frozen, next_trade_date=NEXT,
                                    checkpoint=checkpoint, persist=False)


@pytest.mark.parametrize("kind", ["review", "briefing"])
def test_resume_without_explicit_context_blocks_before_any_source_read(monkeypatch, kind):
    monkeypatch.setattr(research_flow, "collect_context", lambda *args, **kwargs: pytest.fail("recovery source read"))
    monkeypatch.setattr(research_flow, "load_digest", lambda *args, **kwargs: pytest.fail("recovery source read"))
    monkeypatch.setattr(market_decision, "generate_research", lambda *args, **kwargs: pytest.fail("model"))
    checkpoint = {"stages": {"deepseek_draft": {"status": "COMPLETE"}}}
    with pytest.raises(RuntimeError, match="research_input_resume.*frozen context"):
        if kind == "review":
            research_flow.attach_review(None, _digest(), checkpoint=checkpoint)
        else:
            research_flow.generate_briefing(None, REVIEW, checkpoint=checkpoint)


def test_intraday_enrichment_runs_outside_scan_transaction_then_persists_before_model(db, monkeypatch):
    identity, calls, clock = queue(db), [], [NOW]
    monkeypatch.setattr(market_decision, "verify_automatic_research", lambda *args: None)

    def enrich(engine, digest, context, **kwargs):
        assert db[0].pool.checkedout() == 0
        assert kwargs["report_kind"] == "intraday"
        assert context["cutoff_at"] == NOW.replace(tzinfo=research.TZ).isoformat()
        assert digest["review_date"] == "2026-10-30"
        for seconds in (10, 10):
            clock[0] += timedelta(seconds=seconds)
            kwargs["progress"]({"stage": "technical_source_read", "status": "RUNNING"})
            state = load(db, identity)[1]
            assert research._time(state["analysis"]["lease_expires_at"]) == clock[0] + timedelta(seconds=60)
        calls.append(1)
        return enhanced(context)

    def generate(digest, **kwargs):
        saved, snapshot = load(db, identity)
        assert snapshot["context"] == kwargs["context"]
        assert snapshot["evidence_enrichment"]["status"] == "COMPLETE"
        assert saved["evidence_hash"] == snapshot["input_sha256"]
        assert snapshot["input_sha256"] == research._hash({"digest": digest, "context": kwargs["context"]})
        assert db[0].pool.checkedout() == 0
        return final_result()

    monkeypatch.setattr(research_inputs, "enrich_context", enrich)
    result = research.process_research_batch(db[0], outbox_id=identity, generator=generate, now_fn=lambda: clock[0])
    assert result["status"] == "SHADOW" and calls == [1]
    again = research.process_research_batch(db[0], outbox_id=identity,
        generator=lambda *args, **kwargs: pytest.fail("duplicate model"), now_fn=lambda: clock[0])
    assert again["status"] == "SHADOW" and calls == [1]


def test_enrichment_does_not_extend_original_source_expiration_or_launch_models(db, monkeypatch):
    identity, clock = queue(db), [NOW]

    def too_late(engine, digest, context, **kwargs):
        clock[0] += timedelta(seconds=181)
        return enhanced(context)

    monkeypatch.setattr(research_inputs, "enrich_context", too_late)
    with pytest.raises(research.IntradayResearchError, match="owner_or_deadline_lost"):
        research.process_research_batch(db[0], outbox_id=identity,
            generator=lambda *args, **kwargs: pytest.fail("expired model call"), now_fn=lambda: clock[0])
    saved, state = load(db, identity)
    assert saved["status"] == "EXPIRED" and saved["content_markdown"] == ""


def test_frozen_input_hash_is_checked_and_checkpoint_does_not_reread(monkeypatch):
    monkeypatch.setattr(research_inputs, "enrich_context", lambda *args, **kwargs: pytest.fail("reread"))
    frozen = research.freeze_inputs(observation(), [], events(), mode="replay")
    frozen["checkpoint"] = {"stages": {"deepseek_draft": {"status": "COMPLETE"}}}
    assert research._freeze_research_evidence(None, frozen) is frozen
    altered = deepcopy(frozen)
    altered["context"]["cutoff_at"] = "2026-10-30T13:52:00+08:00"
    with pytest.raises(research.IntradayResearchError, match="input_hash_mismatch"):
        research._freeze_research_evidence(None, altered)


@pytest.mark.parametrize("count", [6, 8])
def test_replay_uses_explicit_new_frozen_context_without_any_source_read(monkeypatch, count):
    frozen = research.freeze_inputs(observation(), [], events(), mode="replay")
    frozen["context"] = enhanced(frozen["context"])
    frozen["input_sha256"] = research._hash({"digest": frozen["digest"], "context": frozen["context"]})
    monkeypatch.setattr(research_inputs, "enrich_context", lambda *args, **kwargs: pytest.fail("reread frozen evidence"))
    monkeypatch.setattr(research, "load_replay_inputs", lambda *args, **kwargs: pytest.fail("reread observation"))
    monkeypatch.setattr(market_decision, "verify_automatic_research", lambda *args: None)

    def generate(digest, **kwargs):
        assert kwargs["context"] == frozen["context"]
        return final_result(count)

    result = research.run_intraday_research_replay(None, observation_id="a" * 64, generator=generate,
        checkpoint={"generation_started_at": NOW.replace(tzinfo=research.TZ).isoformat()}, frozen_inputs=frozen)
    assert result["input_snapshot"] == frozen
    assert result["outbox_write_invoked"] is False
    assert result["completed_units"] == result["total_units"] == count
    assert result["stage_budget"] == 8


def test_fresh_observation_replay_enriches_once_and_emits_snapshot_for_resumption(db, monkeypatch):
    retained(db, observation())
    calls, progress = [], []

    def enrich(engine, digest, context, **kwargs):
        assert db[0].pool.checkedout() == 0
        assert kwargs["report_kind"] == "intraday"
        assert context["mode"] == "replay"
        calls.append(1)
        return enhanced(context)

    monkeypatch.setattr(research_inputs, "enrich_context", enrich)
    monkeypatch.setattr(market_decision, "verify_automatic_research", lambda *args: None)
    result = research.run_intraday_research_replay(db[0], observation_id="a" * 64,
        generator=lambda *args, **kwargs: final_result(), progress=progress.append)
    assert calls == [1]
    frozen = next(event["input_snapshot"] for event in progress if event.get("input_snapshot"))
    assert frozen == result["input_snapshot"]
    research.run_intraday_research_replay(db[0], observation_id="a" * 64, frozen_inputs=frozen,
        generator=lambda *args, **kwargs: final_result(), checkpoint={"generation_started_at": NOW.replace(tzinfo=research.TZ).isoformat()})
    assert calls == [1]


def test_checkpoint_without_retained_input_is_blocked_instead_of_querying_new_evidence(db, monkeypatch):
    retained(db, observation())
    monkeypatch.setattr(research_inputs, "enrich_context", lambda *args, **kwargs: pytest.fail("recovery source read"))
    with pytest.raises(research.IntradayResearchError, match="checkpoint_requires_frozen_inputs"):
        research.run_intraday_research_replay(db[0], observation_id="a" * 64,
            checkpoint={"generation_started_at": NOW.replace(tzinfo=research.TZ).isoformat()},
            generator=lambda *args, **kwargs: pytest.fail("model"))
