from copy import deepcopy
from datetime import datetime, timedelta
import hashlib
import json

import pytest
from sqlalchemy import Column, Date, DateTime, Float, Integer, MetaData, String, Table, Text, create_engine, insert, select, text

from biz.intraday_alert import research

NOW = datetime(2026, 10, 30, 10, 0, 30)


@pytest.fixture(autouse=True)
def public_evidence_boundary(monkeypatch):
    from biz.review import research_inputs
    monkeypatch.setattr(research_inputs, "enrich_context", lambda engine, digest, context, **kwargs:
                        {**context, "technical_facts": {"status": "UNKNOWN"}, "fundamental_facts": {"status": "UNKNOWN"}})


def observation(at=NOW, identifier="a" * 64):
    return {"observation_id": identifier, "trade_date": at.date().isoformat(), "observed_at": at,
            "source_snapshot_at": at - timedelta(seconds=20), "coverage": 0.99,
            "expected_count": 100, "observed_count": 99, "source_provider": "gj_big_qmt_inner",
            "quality": {"status": "PASS", "receipt_id": "source-receipt"},
            "source_receipt": {"receipt_id": "source-receipt", "quality_status": "PASS", "source_generated_at": at - timedelta(seconds=20)},
            "market": {"stock_count": 99, "median_return_pct": -1.2, "positive_breadth_pct": 20.0,
                       "total_amount": 123456, "amount_delta": 50},
            "sectors": {"available": True, "items": [{"code": "industry", "name": "通信", "median_return_pct": -2,
                            "leaders": [{"stock_code": "300502"}]}]},
            "key_stocks": {"leaders": [{"stock_code": "300502", "short_name": "新易盛", "price": 200, "change_pct": -3.0, "amount": 999}]},
            "style": {}, "benchmarks": {"items": [{"instrument_code": "000300", "instrument_name": "沪深300", "instrument_type": "INDEX", "price": 3000, "change_pct": -0.4}]}}


def events():
    return [{"event_key": "MARKET:WEAK", "event_type": "MARKET", "direction": "DOWN", "state": "ENHANCED",
             "transition_name": "ENHANCED", "evidence_hash": "e" * 64, "evidence": {"breadth": 20},
             "inference": "old hard-coded analysis must not enter the research input"},
            {"event_key": "SECTOR:WEAK", "event_type": "SECTOR", "evidence": {"return_pct": -2}}]


@pytest.fixture
def db(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'intraday.db'}")
    meta = MetaData()
    columns = [Column("observation_id", String, primary_key=True), Column("trade_date", Date),
               Column("source_snapshot_at", DateTime), Column("observed_at", DateTime), Column("session_minute", Integer),
               Column("source_provider", String), Column("source_receipt_id", String), Column("expected_count", Integer),
               Column("observed_count", Integer), Column("coverage", Float), Column("quality_status", String), Column("created_at", DateTime)]
    columns += [Column(name, Text) for name in ("market_json", "sector_json", "key_stock_json", "style_json", "benchmark_json")]
    obs = Table(research.OBSERVATION_TABLE, meta, *columns)
    boxes = Table(research.OUTBOX_TABLE, meta,
        Column("outbox_id", String, primary_key=True), Column("trade_date", Date), Column("event_key", String),
        Column("cycle", Integer), Column("transition_name", String), Column("state", String), Column("mode", String),
        Column("status", String), Column("evidence_hash", String), Column("evidence_json", Text),
        Column("content_sha256", String), Column("content_markdown", Text), Column("attempts", Integer),
        Column("next_retry_at", DateTime), Column("claimed_at", DateTime), Column("delivery_id", String),
        Column("error_message", String), Column("created_at", DateTime), Column("sent_at", DateTime), Column("updated_at", DateTime))
    meta.create_all(engine)
    yield engine, obs, boxes
    engine.dispose()


def queue(db, *, mode="shadow"):
    engine, _, _ = db
    with engine.begin() as connection:
        return research.queue_research_batch(connection, observation=observation(), history=[], emissions=events(), mode=mode, now=NOW)


def load(db, identity):
    engine, _, boxes = db
    with engine.connect() as connection:
        row = connection.execute(select(boxes).where(boxes.c.outbox_id == identity)).mappings().one()
        return dict(row), json.loads(row["evidence_json"])


@pytest.fixture
def verifier(monkeypatch):
    from biz.review import market_decision
    def verify(result):
        assert result["status"] == "COMPLETE"
        assert result["verification"]["status"] == "PASS"
        assert len(result["generation_attempts"]) == 6
    monkeypatch.setattr(market_decision, "verify_automatic_research", verify)


def final_result():
    return {"status": "COMPLETE", "content": "已由共享研究流程生成的盘中播报", "verification": {"status": "PASS"},
            "generation_attempts": [{"stage": str(i), "raw_response": '{"reply":"raw"}'} for i in range(6)]}


def test_freeze_excludes_future_history_and_old_fixed_analysis_without_daily_facts():
    past = observation(NOW - timedelta(minutes=1), "b" * 64)
    future = observation(NOW + timedelta(minutes=1), "c" * 64)
    frozen = research.freeze_inputs(observation(), [past, future], events(), mode="shadow")
    assert len(frozen["history"]) == 1
    assert "inference" not in frozen["context"]["intraday_events"][0]
    assert frozen["digest"]["market_structure_json"] == {}
    projected = frozen["context"]["intraday_observation"]
    assert projected["market"]["up_coverage"] == 0.2
    assert projected["market"]["amount_quality"]["status"] == "UNKNOWN"
    assert projected["sectors"][0]["change_pct"] == -2
    assert projected["key_stocks"][0]["sector_names"] == ["通信"]
    assert projected["source_receipt"]["receipt_id"] == "source-receipt"
    assert projected["market"]["indices"][0]["close"] == 3000
    assert frozen["context"].get("theme_evidence") is None


def test_two_emissions_form_one_idempotent_unformed_batch(db):
    identity = queue(db)
    assert queue(db) == identity
    row, state = load(db, identity)
    assert row["status"] == "RESEARCH_PENDING"
    assert row["content_markdown"] == ""
    assert len(state["emissions"]) == 2
    with db[0].connect() as connection:
        assert len(connection.execute(select(db[2])).all()) == 1


def test_model_runs_without_database_transaction_and_success_persists_six_stage_checkpoint(db, verifier):
    identity = queue(db)
    clock = [NOW]
    calls = []
    def generate(digest, **kwargs):
        assert db[0].pool.checkedout() == 0
        assert kwargs["report_kind"] == "intraday" and kwargs["next_trade_date"] == "2026-10-30"
        assert digest["market_structure_json"] == {}
        for index in range(6):
            clock[0] += timedelta(seconds=5)
            checkpoint = {"stages": {str(i): {"status": "COMPLETE", "raw_response": "untouched"} for i in range(index + 1)}}
            kwargs["progress"]({"stage": str(index), "status": "COMPLETE", "completed": index + 1, "total": 6, "checkpoint": checkpoint})
            saved, state = load(db, identity)
            assert state["checkpoint"] == checkpoint
        calls.append(1)
        return final_result()
    result = research.process_research_batch(db[0], outbox_id=identity, generator=generate, now_fn=lambda: clock[0])
    assert result["status"] == "SHADOW"
    saved, state = load(db, identity)
    assert saved["content_markdown"] == result["research"]["content"]
    assert saved["content_sha256"] == hashlib.sha256(saved["content_markdown"].encode()).hexdigest()
    assert state["analysis"]["completed_units"] == 6
    assert len(state["checkpoint"]["stages"]) == 6
    again = research.process_research_batch(db[0], outbox_id=identity, generator=generate, now_fn=lambda: clock[0])
    assert again["status"] == "SHADOW" and calls == [1]


def test_analyzing_owner_is_never_reclaimed_even_after_lease_expiry(db, verifier):
    identity = queue(db)
    clock = [NOW]
    def generate(_digest, **kwargs):
        clock[0] += timedelta(seconds=70)
        # A second scheduler invocation must never start a competing model.
        result = research.process_research_batch(db[0], outbox_id=identity,
                    generator=lambda *a, **k: pytest.fail("parallel model"), now_fn=lambda: clock[0])
        assert result["status"] == "ANALYZING"
        kwargs["progress"]({"checkpoint": {}})
    with pytest.raises(research.IntradayResearchError, match="owner_or_deadline"):
        research.process_research_batch(db[0], outbox_id=identity, generator=generate, now_fn=lambda: clock[0])
    assert load(db, identity)[0]["status"] == "BLOCKED"


def test_late_model_draft_is_expired_with_no_content_or_delivery(db, verifier):
    identity = queue(db, mode="live")
    clock = [NOW]
    def generate(*_args, **_kwargs):
        clock[0] += timedelta(seconds=181)
        return final_result()
    with pytest.raises(research.IntradayResearchError):
        research.process_research_batch(db[0], outbox_id=identity, generator=generate, now_fn=lambda: clock[0])
    saved, _ = load(db, identity)
    assert saved["status"] == "EXPIRED" and saved["content_markdown"] == ""


def test_generation_failure_preserves_successful_checkpoint_and_blocks_automatic_regeneration(db):
    identity = queue(db)
    checkpoint = {"stages": {"deepseek_draft": {"status": "COMPLETE", "raw_response": "actual provider raw"}}}
    def fail(_digest, **kwargs):
        kwargs["progress"]({"stage": "codex_draft", "completed": 1, "checkpoint": checkpoint})
        raise RuntimeError("source failed")
    with pytest.raises(RuntimeError):
        research.process_research_batch(db[0], outbox_id=identity, generator=fail, now_fn=lambda: NOW)
    saved, state = load(db, identity)
    assert saved["status"] == "BLOCKED" and state["checkpoint"] == checkpoint
    assert research.process_research_batch(db[0], outbox_id=identity,
        generator=lambda *a, **k: pytest.fail("regenerated completed stage"), now_fn=lambda: NOW)["status"] == "BLOCKED"


def test_unverified_complete_flag_is_rejected_by_shared_automatic_provenance(db):
    identity = queue(db)
    with pytest.raises(Exception):
        research.process_research_batch(db[0], outbox_id=identity,
            generator=lambda *a, **k: {"status": "COMPLETE", "content": "handwritten"}, now_fn=lambda: NOW)
    assert load(db, identity)[0]["status"] == "BLOCKED"


def retained(db, value, *, created_at=None):
    engine, obs, _ = db
    with engine.begin() as connection:
        connection.execute(insert(obs).values(observation_id=value["observation_id"], trade_date=research._time(value["observed_at"]).date(),
            observed_at=value["observed_at"], source_snapshot_at=value["source_snapshot_at"], session_minute=30,
            source_provider=value["source_provider"], source_receipt_id="retained-receipt", expected_count=100, observed_count=99,
            coverage=0.99, quality_status="PASS", created_at=created_at or value["observed_at"],
            **{field: research._json(value[key]) for field, key in (("market_json", "market"), ("sector_json", "sectors"),
               ("key_stock_json", "key_stocks"), ("style_json", "style"), ("benchmark_json", "benchmarks"))}))


def test_replay_uses_only_retained_cutoff_facts_and_never_writes_live_outbox(db, verifier):
    retained(db, observation())
    retained(db, observation(NOW - timedelta(minutes=1), "b" * 64))
    retained(db, observation(NOW + timedelta(minutes=1), "c" * 64))
    retained(db, observation(NOW - timedelta(minutes=2), "d" * 64), created_at=NOW + timedelta(minutes=1))
    called = []
    def generate(_digest, **kwargs):
        context = kwargs["context"]
        assert context["mode"] == "replay" and context["asof_mode"] == "retained_snapshot"
        history = context["intraday_observation"]["history"]
        assert len(history) == 1 and history[0]["observation_id"] == "b" * 64
        assert context.get("theme_evidence") is None and context.get("events") is None
        assert context["intraday_observation"]["source_receipt"]["full_raw_receipt"] == "UNKNOWN"
        called.append(1)
        return final_result()
    result = research.run_intraday_research_replay(db[0], observation_id="a" * 64, generator=generate)
    assert result["status"] == "COMPLETE" and result["outbox_write_invoked"] is False and result["delivery_invoked"] is False
    assert called == [1]
    with db[0].connect() as connection:
        assert connection.execute(select(db[2])).all() == []


def test_missing_replay_history_is_blocked_instead_of_fabricated(db):
    with pytest.raises(research.IntradayResearchError, match="historical_observation_missing"):
        research.run_intraday_research_replay(db[0], observation_id="missing")


def test_intraday_shared_packet_labels_current_price_and_retained_rule_event_without_daily_window():
    from biz.review.research_contract import build_packet
    from biz.review.market_decision import source_digest_identity
    frozen = research.freeze_inputs(observation(), [], events(), mode="replay")
    packet = build_packet(frozen["digest"], report_kind="intraday", next_trade_date="2026-10-30",
        context=frozen["context"], previous_research=None, now=NOW.replace(tzinfo=research.TZ),
        source_digest_sha256=source_digest_identity(frozen["digest"]))
    assert packet["task"]["kind"] == "intraday" and packet["task"]["condition_horizon"] == "remaining_session"
    stock = next(f for f in packet["facts"].values() if f["metric"] == "stock_prices")
    assert stock["values"]["price"] == 200 and "close" not in stock["values"]
    assert any(f["metric"] == "rule_event" for f in packet["facts"].values())
    assert all(f["window"] != "full_session" for f in packet["facts"].values())


def test_intraday_feed_never_presents_legacy_or_pending_prose_as_formed_research(monkeypatch):
    from server.api.routers import hot_data
    original = [
        {"outbox_id": "legacy", "status": "PENDING", "content_markdown": "old fixed or editorial prose", "evidence_json": "{}"},
        {"outbox_id": "pending", "status": "ANALYZING", "content_markdown": "must not leak", "evidence_json": json.dumps({"schema_version": research.SCHEMA, "analysis": {"completed_units": 2, "total_units": 6}})},
        {"outbox_id": "accepted", "status": "SHADOW", "content_markdown": "accepted model prose", "evidence_json": json.dumps({"schema_version": research.SCHEMA, "research": {"status": "COMPLETE"}})},
    ]
    before = deepcopy(original)
    def read(sql, _params):
        if "FROM st_intraday_alert_outbox" in sql:
            return deepcopy(original)
        return []
    monkeypatch.setattr(hot_data, "_read_sql", read)
    result = hot_data.intraday_alert_feed(trade_date="2026-10-30", limit=30)
    assert not result.get("error")
    legacy, pending, accepted = result["outbox"]
    assert legacy["formed"] is False and legacy["research_status"] == "LEGACY_UNVERIFIED"
    assert legacy["content_markdown"] == "" and legacy["status"] == "PENDING"
    assert pending["formed"] is False and pending["content_markdown"] == ""
    assert pending["research_progress"] == {"completed_units": 2, "total_units": 6, "current_unit": None, "last_progress_at": None}
    assert accepted["formed"] is True and accepted["content_markdown"] == "accepted model prose"
    assert original == before  # No rewrite of retained historical content.


def test_new_observation_scan_terminalizes_a_dead_old_owner_before_loading_new_source(db, monkeypatch):
    from biz.intraday_alert import core
    identity = queue(db)
    saved, state = load(db, identity)
    state["analysis"] = {"owner": "gone-owner", "generation": 1,
                         "lease_expires_at": research._stamp(NOW - timedelta(seconds=1)),
                         "deadline_at": research._stamp(NOW + timedelta(seconds=1))}
    state["checkpoint"] = {"stages": {"deepseek_draft": {"status": "COMPLETE", "raw_response": "preserved"}}}
    with db[0].begin() as connection:
        connection.execute(text(f"UPDATE {research.OUTBOX_TABLE} SET status='ANALYZING',evidence_json=:e WHERE outbox_id=:id"), {"id": identity, "e": research._json(state)})
    monkeypatch.setattr(core, "_is_trade_day", lambda *a: True)
    monkeypatch.setattr(core, "ensure_intraday_alert_tables", lambda *a: None)
    class SourceBoundaryReached(Exception):
        pass
    def next_source(*args, **kwargs):
        assert kwargs["now"] > NOW
        raise SourceBoundaryReached()
    monkeypatch.setattr(core, "_load_validated_snapshot_with_retry", next_source)
    with pytest.raises(SourceBoundaryReached):
        core.run_intraday_scan(db[0], object(), mode="shadow", now=NOW + timedelta(minutes=1))
    terminal, after = load(db, identity)
    assert terminal["status"] == "EXPIRED"
    assert after["checkpoint"] == state["checkpoint"]
    assert after["analysis"]["owner"] == "gone-owner"
    assert research.process_research_batch(db[0], outbox_id=identity,
        generator=lambda *a, **k: pytest.fail("reclaimed dead owner"), now_fn=lambda: NOW + timedelta(minutes=1))["status"] == "EXPIRED"


def test_old_owner_cleanup_is_bounded_to_ten_rows_and_never_overwrites_checkpoints(db):
    engine, _, _ = db
    identities = []
    for index in range(12):
        with engine.begin() as connection:
            identities.append(research.queue_research_batch(connection,
                observation=observation(identifier=f"{index:064x}"), history=[], emissions=events(), mode="shadow", now=NOW))
    for identity in identities:
        _, state = load(db, identity)
        state["analysis"] = {"owner": "gone", "generation": 1, "deadline_at": research._stamp(NOW - timedelta(seconds=1))}
        state["checkpoint"] = {"kept": True}
        with engine.begin() as connection:
            connection.execute(text(f"UPDATE {research.OUTBOX_TABLE} SET status='ANALYZING',evidence_json=:e WHERE outbox_id=:id"), {"id": identity, "e": research._json(state)})
    assert research.expire_research_batches(engine, now=NOW, limit=1000) == 10
    assert sum(load(db, identity)[0]["status"] == "EXPIRED" for identity in identities) == 10
    assert research.expire_research_batches(engine, now=NOW) == 2
    assert all(load(db, identity)[1]["checkpoint"] == {"kept": True} for identity in identities)
