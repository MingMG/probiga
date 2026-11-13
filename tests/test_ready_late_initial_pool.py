from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
import hashlib
import json

import pytest
from sqlalchemy import event, text

from server.common import daily_delivery_control as control
from server.common import pit_facts
from server.common.analysis_pool_receipt import build_score_snapshot, decode_score_snapshot
from server.common.authoritative_market_clock import PRODUCTION_TIMEZONE, resolve_strategy_pool_decision_slot
from server.common.qmt_announcement_pit import QMT_ANNOUNCEMENT_TASK_SCHEMA
from tests.score_snapshot_helpers import scored_publication
from tests.test_daily_delivery_control import BUILD_SHA, STRATEGY_RELEASE_ID, _engine, _legacy_delivery


TARGET = "2026-11-10"
EXECUTION = "2026-11-11"
SCHEDULED = datetime(2026, 11, 10, 22, 20)


@pytest.mark.parametrize("change,accepted", [("clock", True), ("title", False), ("url", False), ("two_seconds", False)])
def test_signed_cninfo_accepts_only_unchanged_disclosure_with_source_clock_correction(change, accepted):
    engine = _engine()
    pit_facts.ensure_pit_fact_schema(engine)
    cutoff = datetime.fromisoformat(TARGET + "T23:59:59.999999")
    received = cutoff + timedelta(hours=3)
    previous_time = datetime.fromisoformat(TARGET + "T19:26:27")
    current_time = previous_time - timedelta(seconds=2 if change == "two_seconds" else 1)
    event_id, code = "1225667697", "301678"
    source = pit_facts.CNINFO_EVENT_SOURCE
    for number, (published, known) in enumerate(((previous_time, previous_time + timedelta(minutes=1)),
                                                 (current_time, received)), start=1):
        title = "Changed title" if number == 2 and change == "title" else "Original title"
        url_date = "2026-11-09" if number == 2 and change == "url" else TARGET
        stamp = published.isoformat(timespec="microseconds")
        raw_hash = str(number) * 64
        payload = {
            "stock_code": code, "event_date": TARGET, "event_key": f"cninfo:{event_id}",
            "published_at": stamp, "source_published_text": TARGET, "source_event_id": event_id,
            "title": title, "source_row_hash": raw_hash,
            "source_fields": {
                "announcementTime": round(published.replace(tzinfo=PRODUCTION_TIMEZONE).timestamp() * 1000),
                "announcement_id": event_id, "provider": source, "stock_code": code, "title": title,
                "detail_url": f"https://static.cninfo.com.cn/finalpage/{url_date}/{event_id}.PDF",
                "published_at": stamp, "source_row_hash": raw_hash,
            },
        }
        pit_facts.append_event_revision(engine, payload, known_at=known, source=source, batch_id="fixture")
    with engine.connect() as connection:
        rows = [dict(row) for row in connection.execute(text(
            "SELECT * FROM st_pit_event_revision ORDER BY revision_no"
        )).mappings()]
    args = dict(fact_kind="event", fact_cutoff_at=cutoff, decision_at=received)
    selected, reason = pit_facts._latest_revision_for_fact_cutoff(rows, **args, signed_event_received_at=received)
    assert selected["revision_id"] == rows[int(accepted)]["revision_id"]
    assert reason == ("" if accepted else "PIT_EVENT_REVISION_PUBLICATION_NOT_ADVANCED")
    unsigned, _ = pit_facts._latest_revision_for_fact_cutoff(rows, **args)
    assert unsigned["revision_id"] == rows[0]["revision_id"]


@pytest.fixture
def pool_calendar():
    engine = _engine()
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE si_trade_calendar (trade_date TEXT, trade_status INTEGER)"))
        connection.execute(text("INSERT INTO si_trade_calendar VALUES ('2026-11-10',1),('2026-11-11',1)"))
    yield engine
    engine.dispose()


def test_initial_cutoff_may_follow_the_plan_but_revision_cutoff_is_fixed(pool_calendar):
    actual = datetime(2026, 11, 11, 1, 40, 3, 123456)
    cutoff = datetime(2026, 11, 10, 17, 40, tzinfo=timezone.utc)
    slot = resolve_strategy_pool_decision_slot(
        pool_calendar, data_trade_date=TARGET, phase="INITIAL", evaluated_at=actual,
        input_cutoff_at=cutoff,
    )
    assert slot.scheduled_at == SCHEDULED.replace(tzinfo=PRODUCTION_TIMEZONE)
    assert slot.input_cutoff_at == datetime(2026, 11, 11, 1, 40, tzinfo=PRODUCTION_TIMEZONE)
    assert slot.evaluated_at == actual.replace(tzinfo=PRODUCTION_TIMEZONE)
    for invalid in (SCHEDULED - timedelta(seconds=1), actual.replace(microsecond=0) + timedelta(seconds=1)):
        with pytest.raises(ValueError, match="input cutoff differs"):
            resolve_strategy_pool_decision_slot(
                pool_calendar, data_trade_date=TARGET, phase="INITIAL", evaluated_at=actual,
                input_cutoff_at=invalid,
            )
    with pytest.raises(ValueError, match="input cutoff differs"):
        resolve_strategy_pool_decision_slot(
            pool_calendar, data_trade_date=TARGET, phase="PREMARKET_REVISION",
            evaluated_at=datetime(2026, 11, 11, 8, 40),
            input_cutoff_at=datetime(2026, 11, 11, 8, 31),
        )


@pytest.mark.parametrize("cross_session", [False, True], ids=["same-session", "compatible-prior-session"])
def test_late_signed_source_obeys_actual_availability_and_round_trips_sealed_pool(pool_calendar, monkeypatch, cross_session):
    from server.common import component_release_attestation

    engine = pool_calendar
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "development")
    source_release = "c" * 40 if cross_session else BUILD_SHA
    contract_digest = "a" * 64
    manifests = {
        sha: {
            "scope": "COORDINATED", "contract_build_sha": sha, "windows_build_sha": sha,
            "contract_sha256": contract_digest, "parent_linux_build_sha": source_release,
            "created_at": created_at,
        }
        for sha, created_at in (
            (source_release, "2026-11-09T01:00:00"), (BUILD_SHA, "2026-11-10T01:00:00"),
        )
    }
    monkeypatch.setattr(component_release_attestation, "load_component_attestation", lambda _connection, sha: manifests[sha])
    stage, source_uid, pool_uid = "qmt_announcement_pit", "1" * 32, "2" * 32
    finished = SCHEDULED + timedelta(minutes=1, microseconds=250_000)
    received = SCHEDULED - timedelta(minutes=1)
    source_cutoff = SCHEDULED - timedelta(minutes=10)
    source_receipt = {
        "schema": QMT_ANNOUNCEMENT_TASK_SCHEMA, "status": "COMPLETE",
        "reason_code": "QMT_ANNOUNCEMENT_FULL_MARKET_COMPLETE", "source": "qmt.announcement",
        "batch_id": "qmt-ann-late-certified-announcement", "batch_root_hash": "c" * 64,
        "window_start": "2026-10-11", "window_end": TARGET, "catalog_batch_id": "catalog",
        "catalog_manifest_hash": "d" * 64, "catalog_member_set_hash": "e" * 64,
        "fact_cutoff_at": source_cutoff.isoformat(), "received_at": received.isoformat(),
        "decision_at": received.isoformat(), "fallback_reason": "",
        "stock_count": 1, "coverage_count": 1,
        "capture_seconds": int((received - source_cutoff).total_seconds()),
        "automatic_real_order_submission": False, "real_order_authority": False,
    }
    replay = json.dumps(source_receipt)
    digest = hashlib.sha256(replay.encode()).hexdigest()
    evidence = {
        "schema": control.SCHEDULER_VALIDATION_EVIDENCE_SCHEMA,
        "run_uid": source_uid, "task_type": stage, "build_sha": source_release,
        "contract_release_id": source_release,
        "status": "success", "exit_code": 0, "validation_checked": True, "validation_ok": True,
        "replay_output": replay, "replay_output_sha256": digest,
        "input_receipt_root_sha256": digest, "target_trade_date": TARGET,
    }
    monkeypatch.setattr(control, "_control_now", lambda: finished - timedelta(seconds=30))
    control.start_daily_stage_attempt(
        engine, scheduler_run_uid=source_uid, stage_name=stage, trade_date=TARGET,
        release_id=source_release, strategy_release_id=STRATEGY_RELEASE_ID, lease_owner="windows-fixture",
    )
    with engine.begin() as connection:
        control.finish_daily_stage_attempt(
            connection, scheduler_run_uid=source_uid, status="success", input_root_sha256=digest,
            checkpoint={**evidence, "evidence_sha256": control.canonical_sha256(evidence)}, now=finished,
        )
    too_early = finished.replace(microsecond=0)
    assert control.select_frozen_initial_announcement_source(engine, trade_date=TARGET, decision_at=too_early) is None

    cutoff = datetime(2026, 11, 11, 1, 40)
    selected = control.select_frozen_initial_announcement_source(engine, trade_date=TARGET, decision_at=cutoff)
    assert selected.receipt["batch_id"] == source_receipt["batch_id"]
    assert selected.score_binding["finished_at"] == finished.isoformat(timespec="microseconds")
    exact = {"trade_date": TARGET, "decision_at": cutoff, "consumer_release_id": BUILD_SHA}
    assert control.select_frozen_initial_announcement_source(engine, required_run_uid=source_uid, **exact) == selected
    assert control.select_frozen_initial_announcement_source(engine, required_run_uid="9" * 32, **exact) is None
    assert selected.score_binding["contract_release_id"] == source_release
    assert selected.score_binding["producer_build_sha"] == source_release
    assert selected.score_binding["input_receipt_root_sha256"] == digest
    replay_uid = "3" * 32
    replay_finished = finished + timedelta(minutes=1)
    replay_evidence = {**evidence, "run_uid": replay_uid, "build_sha": BUILD_SHA, "contract_release_id": BUILD_SHA}
    monkeypatch.setattr(control, "_control_now", lambda: replay_finished - timedelta(seconds=30))
    control.start_daily_stage_attempt(
        engine, scheduler_run_uid=replay_uid, stage_name=stage, trade_date=TARGET,
        release_id=BUILD_SHA, strategy_release_id=STRATEGY_RELEASE_ID, lease_owner="windows-fixture",
    )
    with engine.begin() as connection:
        control.finish_daily_stage_attempt(
            connection, scheduler_run_uid=replay_uid, status="success", input_root_sha256=digest,
            checkpoint={**replay_evidence, "evidence_sha256": control.canonical_sha256(replay_evidence)},
            now=replay_finished,
        )
    assert control.select_frozen_initial_announcement_source(engine, **exact) == selected
    assert control.select_frozen_initial_announcement_source(
        engine, required_run_uid=replay_uid, **exact,
    ).score_binding["scheduler_run_uid"] == replay_uid
    fixture = decode_score_snapshot(scored_publication(trade_date=TARGET)["score_snapshot"])
    core_inputs = {
        "kline": {"stage": "qmt_stock_daily_canonical", "dataset_id": "daily", "root": "1" * 64},
        "turnover": {"stage": "target_turnover_snapshot", "dataset_id": "turnover", "root": "2" * 64},
        "financial": {"stage": "stock_finance", "dataset_id": "finance", "root": "3" * 64},
        "announcement": {"stage": stage, "dataset_id": source_uid,
                         "root": selected.score_binding["evidence_sha256"],
                         "input_root": source_receipt["batch_root_hash"]},
    }

    def score(at):
        return build_score_snapshot(
            trade_date=TARGET, decision_at=at, run_uid=pool_uid, build_sha=BUILD_SHA,
            analysis_rows=fixture["analysis_rows"],
            scored_rows=[{**row, "flow_input_decision_at": at.isoformat()} for row in fixture["scored_rows"]],
            frozen_announcement_source=selected.score_binding, frozen_core_inputs=core_inputs,
        )

    with pytest.raises(ValueError, match="FROZEN_ANNOUNCEMENT_MISMATCH"):
        score(too_early)
    snapshot = score(cutoff)
    sealed = decode_score_snapshot(snapshot, trade_date=TARGET)
    assert sealed["frozen_announcement_source"] == selected.score_binding
    assert sealed["input_decision_at"] == cutoff.isoformat()

    actual = cutoff + timedelta(minutes=1)
    monkeypatch.setattr(control, "_control_now", lambda: actual)
    governance = "strategy_governance_daily"
    control.start_daily_stage_attempt(
        engine, scheduler_run_uid=pool_uid, stage_name=governance, trade_date=TARGET,
        release_id=BUILD_SHA, strategy_release_id=STRATEGY_RELEASE_ID, lease_owner="linux-fixture",
    )
    slot = resolve_strategy_pool_decision_slot(
        engine, data_trade_date=TARGET, phase="INITIAL", evaluated_at=actual, input_cutoff_at=cutoff,
    )
    with engine.begin() as connection:
        control.finish_daily_stage_attempt(connection, scheduler_run_uid=pool_uid, status="success")
        session = control.ensure_daily_delivery_session(
            connection, trade_date=TARGET, release_id=BUILD_SHA, strategy_release_id=STRATEGY_RELEASE_ID,
        )
        draft = control.build_terminal_delivery_receipt(
            session=session, scheduler_run_uid=pool_uid, stage_name=governance, status="PASS",
            strategy_release_id=STRATEGY_RELEASE_ID, exact_core_inputs=core_inputs,
            legacy_receipt={**_legacy_delivery(pool_uid), "target_trade_date": TARGET,
                            "score_snapshot_sha256": snapshot["payload_sha256"]},
        )
        bound = control.bind_frozen_pool_decision(draft, slot=slot, score_snapshot=snapshot)
        persisted = control.persist_terminal_delivery_receipt(connection, receipt=bound)

    decision = persisted["frozen_pool_decision"]
    assert decision == {
        "schema": "probiga.frozen-pool-decision.v1", "phase": "INITIAL", "execution_session_date": EXECUTION,
        "cutoff_at": "2026-11-11T01:40:00+08:00", "evaluated_at": "2026-11-11T01:41:00+08:00",
        "parent_receipt_uid": None, "diff_sha256": None,
    }
    assert control.read_daily_delivery(engine, trade_date=TARGET, release_id=BUILD_SHA)["receipt"] == persisted
    assert control.load_frozen_initial_announcement_parent(engine, TARGET, BUILD_SHA)["batch_id"] == source_receipt["batch_id"]
    if cross_session:
        manifests[BUILD_SHA]["contract_sha256"] = "0" * 64
        assert control.select_frozen_initial_announcement_source(engine, required_run_uid=source_uid, **exact) is None
        with pytest.raises(control.DailyDeliveryControlError, match="result differs"):
            control.load_frozen_initial_announcement_parent(engine, TARGET, BUILD_SHA)
        manifests[BUILD_SHA]["contract_sha256"] = contract_digest

    with engine.begin() as connection:
        connection.execute(text(f"UPDATE {control.ATTEMPT_TABLE} SET finished_at=:finished WHERE scheduler_run_uid=:run"),
                           {"finished": cutoff + timedelta(seconds=1), "run": source_uid})
    with pytest.raises(control.DailyDeliveryControlError, match="completion exceeds its input cutoff"):
        control.load_frozen_initial_announcement_parent(engine, TARGET, BUILD_SHA)


def test_late_cutoff_reads_the_bound_durable_event_batch(tmp_path):
    from server.common.qmt_announcement_pit import synchronize_qmt_announcements
    from tests.test_qmt_announcement_pit import _Clock, _XtData, _catalog, _engine, _frame, _install_catalog

    engine = _engine()
    code = "000001"
    target = date(2026, 8, 25)
    start = target - timedelta(days=30)
    _install_catalog(engine, _catalog((code, f"{code}.SZ")))
    batch = synchronize_qmt_announcements(
        engine, xtdata=_XtData({f"{code}.SZ": _frame(code, "2026-08-21 10:00:00")}),
        checkpoint_root=tmp_path, now_fn=_Clock(datetime(2026, 8, 25, 18, 20)),
    )
    assert batch["status"] == "COMPLETE"
    signed = control.FrozenInitialAnnouncement({
        **batch, "stage_finished_at": "2026-08-25T22:21:00+08:00",
        "stage_attempt_uid": "a" * 64, "stage_evidence_sha256": "b" * 64,
    }, {})
    scope = dict(codes=[code], data_date=target, window_start=start, window_end=target)
    with pytest.raises(ValueError, match="completion scope differs"):
        pit_facts._load_certified_initial_event_selection(
            engine, signed, decision=datetime(2026, 8, 25, 22, 20), **scope,
        )
    actual = datetime(2026, 8, 26, 1, 40)
    selected = pit_facts._load_certified_initial_event_selection(engine, signed, decision=actual, **scope)
    cutoff = datetime.fromisoformat(batch["fact_cutoff_at"])
    facts = pit_facts.load_event_facts(
        engine, codes=[code], decision_at=actual, fact_cutoff_at=cutoff, start_date=start, end_date=target,
        require_qmt_complete_batch=True, historical_selection=pit_facts.HistoricalEventRead(selected, cutoff),
    )
    assert facts.status_for(code) == pit_facts.PIT_AVAILABLE
    assert {item["event_key"] for item in facts.facts[code]} == {"qmt:A-000001"}
    engine.dispose()

@pytest.mark.parametrize("mismatch_run", [False, True], ids=["signed", "wrong-scheduler-run"])
def test_signed_cninfo_reconstruction_is_usable_only_after_actual_completion(
    pool_calendar, tmp_path, monkeypatch, mismatch_run,
):
    from server.common.announcement_provider import (
        CninfoMarketAnnouncementProvider,
        ProviderBackedAnnouncementAdapter,
    )
    from server.common.qmt_announcement_pit import (
        HistoricalReconstructionContext,
        QMT_ANNOUNCEMENT_SCHEDULER_RECOVERY_ORIGIN,
        synchronize_historical_cninfo_announcements,
        validate_task_result,
    )
    from tests.test_announcement_provider_fallback import _Client, _historical_authority_v3, _master, _row_at, _row_on_date
    from tests.test_qmt_announcement_pit import _catalog, _install_catalog

    engine = pool_calendar
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "development")
    pit_facts.ensure_pit_fact_schema(engine)
    target = date.fromisoformat(TARGET)
    start = target - timedelta(days=30)
    scoring_start = target - timedelta(days=14)
    catalog = _install_catalog(
        engine, _catalog(
            ("000001", "000001.SZ"), ("000002", "000002.SZ"), ("000003", "000003.SZ"),
            ("600000", "600000.SH"), ("600519", "600519.SH"),
        ),
        captured_at=f"{TARGET} 15:30:00",
    )
    started = datetime(2026, 11, 11, 1, 20)
    received = started + timedelta(seconds=20)
    finished = received + timedelta(seconds=5, microseconds=250_000)
    source_cutoff = datetime.combine(target, datetime.max.time())
    producer_uid = "1" * 32
    signed_uid = "2" * 32 if mismatch_run else producer_uid
    stage = "qmt_announcement_pit"
    monkeypatch.setattr(control, "_control_now", lambda: started)
    control.start_daily_stage_attempt(
        engine, scheduler_run_uid=signed_uid, stage_name=stage, trade_date=TARGET,
        release_id=BUILD_SHA, strategy_release_id=STRATEGY_RELEASE_ID, lease_owner="windows-fixture",
    )
    prior = started - timedelta(minutes=1)
    unrelated_coverage = pit_facts.append_source_coverage(
        engine, fact_kind="event", stock_code="000001", window_start=start, window_end=target,
        known_at=prior, covered_through_at=prior, watermark_kind="CAPTURED_AT",
        watermark_evidence={"provider": pit_facts.CNINFO_EVENT_SOURCE}, source_rows=[], fact_bindings=[],
        source=pit_facts.CNINFO_EVENT_SOURCE, batch_id="prior-cninfo-coverage",
    )
    date_only = datetime.combine(target, datetime.min.time())
    pit_facts.append_event_revision(
        engine, {
            "stock_code": "000003", "event_key": f"cninfo:event-000003-{target:%Y%m%d}-000",
            "event_date": TARGET, "published_at": None, "source_published_text": TARGET,
            "title": "Earlier content with the same publication date",
        }, known_at=datetime.combine(target, datetime.min.time()) + timedelta(hours=21),
        source=pit_facts.CNINFO_EVENT_SOURCE, batch_id="prior-cninfo-event",
    )

    master = _master(*catalog.codes)
    client = _Client(
        masters=[master, master],
        rows_by_code={
            "000001": [_row_on_date("000001", 0, target)],
            "000002": [_row_at("000002", 0, date_only)],
            "000003": [_row_at("000003", 0, date_only)],
            "600000": [_row_on_date("600000", 0, scoring_start - timedelta(days=1))],
            "600519": [],
        },
    )
    provider = CninfoMarketAnnouncementProvider(
        client=client, minimum_request_interval=0, now_fn=lambda: started + timedelta(seconds=10),
    )
    from time import monotonic

    # The provider's deadline normally uses the real wall clock. Preserve a
    # bounded request budget while the fixture supplies historical timestamps.
    monkeypatch.setattr(
        ProviderBackedAnnouncementAdapter, "bind_capture_deadline",
        lambda adapter, **_: setattr(adapter, "_deadline_monotonic", monotonic() + 30),
    )
    times = iter((started, received))
    batch = synchronize_historical_cninfo_announcements(
        engine,
        adapter=ProviderBackedAnnouncementAdapter(provider, workers=1),
        checkpoint_root=tmp_path,
        catalog=catalog,
        context=HistoricalReconstructionContext(
            target_trade_date=target, source_query_cutoff_at=source_cutoff,
            reconstruction_started_at=started, scheduler_run_uid=producer_uid, build_sha=BUILD_SHA,
            authority=_historical_authority_v3(catalog, target),
            execution_origin=QMT_ANNOUNCEMENT_SCHEDULER_RECOVERY_ORIGIN,
        ),
        now_fn=lambda: next(times, received + timedelta(seconds=1)),
        batch_size=2,
    )
    assert validate_task_result(batch, 0) == "complete"
    assert batch["mode"] == "HISTORICAL_RECONSTRUCTION"
    assert batch["coverage_count"] == 5 and batch["event_count"] == 4 and batch["empty_stock_count"] == 1
    assert datetime.fromisoformat(batch["fact_cutoff_at"]) == source_cutoff
    assert datetime.fromisoformat(batch["received_at"]) == received

    replay = json.dumps(batch)
    digest = hashlib.sha256(replay.encode()).hexdigest()
    evidence = {
        "schema": control.SCHEDULER_VALIDATION_EVIDENCE_SCHEMA,
        "run_uid": signed_uid, "task_type": stage, "build_sha": BUILD_SHA,
        "status": "success", "exit_code": 0, "validation_checked": True, "validation_ok": True,
        "replay_output": replay, "replay_output_sha256": digest,
        "input_receipt_root_sha256": digest, "target_trade_date": TARGET,
    }
    with engine.begin() as connection:
        control.finish_daily_stage_attempt(
            connection, scheduler_run_uid=signed_uid, status="success", input_root_sha256=digest,
            checkpoint={**evidence, "evidence_sha256": control.canonical_sha256(evidence)}, now=finished,
        )

    def select(at):
        return control.select_frozen_initial_announcement_source(engine, trade_date=TARGET, decision_at=at)

    assert select(SCHEDULED) is None
    assert select(received) is None
    assert select(finished.replace(microsecond=0)) is None
    cutoff = finished.replace(microsecond=0) + timedelta(seconds=1)
    signed = select(cutoff)
    if mismatch_run:
        assert signed is None
        return
    assert signed is not None
    assert signed.receipt["batch_id"] == batch["batch_id"]
    assert signed.score_binding["scheduler_run_uid"] == producer_uid
    assert signed.score_binding["received_at"] == received.isoformat(timespec="microseconds")
    assert signed.score_binding["finished_at"] == finished.isoformat(timespec="microseconds")

    selection_scope = dict(codes=list(catalog.codes), data_date=target, window_start=scoring_start, window_end=target)
    with pytest.raises(ValueError, match="completion scope differs"):
        pit_facts._load_certified_initial_event_selection(
            engine, signed, decision=finished.replace(microsecond=0), **selection_scope,
        )
    # Another provider observation can contain a distinct event within the
    # original date window. It is visible by the later scoring decision, but
    # did not belong to the completed announcement batch selected for scoring.
    for code in catalog.codes:
        pit_facts.append_event_revision(
            engine,
            {"stock_code": code, "event_key": f"cninfo:unrelated-{code}",
             "event_date": TARGET, "published_at": f"{TARGET} 20:00:00", "title": "Unrelated observation"},
            known_at=cutoff, received_at=cutoff, source=batch["source"], batch_id="other-cninfo-batch",
        )

    coverage_reads = []

    def record_coverage_read(connection, cursor, statement, parameters, context, executemany):
        if "SELECT * FROM st_pit_source_coverage" in statement:
            coverage_reads.append(parameters)

    event.listen(engine, "before_cursor_execute", record_coverage_read)
    try:
        selected = pit_facts._load_certified_initial_event_selection(engine, signed, decision=cutoff, **selection_scope)
    finally:
        event.remove(engine, "before_cursor_execute", record_coverage_read)
    assert selected.batch["batch_root_hash"] == batch["batch_root_hash"]
    bound_coverage_ids = {row["coverage_id"] for row, _ in selected.members.values()}
    assert {value for params in coverage_reads for value in params} == {batch["source"], batch["batch_id"], *bound_coverage_ids}
    assert selected.members["000001"][0]["supersedes_coverage_id"] == unrelated_coverage.coverage_id
    bad_receipt = control.FrozenInitialAnnouncement(
        {**signed.receipt, "batch_root_hash": "9" * 64}, signed.score_binding,
    )
    with pytest.raises(ValueError, match="durable batch differs"):
        pit_facts._load_certified_initial_event_selection(engine, bad_receipt, decision=cutoff, **selection_scope)
    for code in catalog.codes:
        pit_facts.append_source_coverage(
            engine, fact_kind="finance", stock_code=code, window_start="1900-01-01", window_end=target,
            known_at=source_cutoff, covered_through_at=source_cutoff, watermark_kind="CAPTURED_AT",
            watermark_evidence={"provider": "test.finance"}, source_rows=[], fact_bindings=[],
            source="test.finance", batch_id="finance-fixture",
        )
    event_reads = []
    progress = []
    resolved = pit_facts.resolve_strategy_fact_cutoffs(
        engine, codes=catalog.codes, decision_at=cutoff, finance_start_date="1900-01-01",
        finance_end_date=target, event_start_date=scoring_start, event_end_date=target, require_qmt_event_batch=True,
        required_initial_announcement=signed, capture_historical_selection=event_reads.append,
        event_progress=lambda done, total: progress.append((done, total)),
    )
    assert resolved["status"] == pit_facts.PIT_AVAILABLE, resolved.get("reason")
    assert resolved["qmt_event_batch"]["batch_root_hash"] == batch["batch_root_hash"]
    assert len(event_reads) == 1
    assert progress == [(0, 10), (5, 10), (10, 10)]

    def read_facts(selection, fact_cutoff=resolved["source_cutoff_at"]["event"]):
        return pit_facts.load_event_facts(
            engine, codes=catalog.codes, decision_at=cutoff, fact_cutoff_at=fact_cutoff,
            start_date=scoring_start, end_date=target, require_qmt_complete_batch=True,
            historical_selection=selection,
        )

    facts = read_facts(event_reads[0])
    assert selected.members["600000"][0]["result_count"] == 1
    assert selected.members["600000"][1] is True
    assert facts.status_for("600000") == pit_facts.PIT_AVAILABLE
    assert facts.reason_for("600000") == "PIT_EVENT_AUTHORITATIVE_EMPTY"
    assert facts.facts["600000"] == []
    assert facts.coverage_by_code["600000"]["coverage_id"] == selected.members["600000"][0]["coverage_id"]
    assert facts.status_for("000001") == facts.status_for("600519") == pit_facts.PIT_AVAILABLE
    assert len(facts.facts["000001"]) == 1 and facts.facts["600519"] == []
    assert facts.facts["000001"][0]["event_key"] == f"cninfo:event-000001-{target:%Y%m%d}-000"
    assert facts.coverage_by_code["600519"]["coverage_batch_id"] == batch["batch_id"]
    assert facts.status_for("000002") == pit_facts.PIT_AVAILABLE
    date_only_event, = facts.facts["000002"]
    assert date_only_event["event_publication_precision"] == "DATE_ONLY"
    assert date_only_event["event_publication_time_verified"] is False
    assert date_only_event["event_published_at"][:10] == TARGET
    assert date_only_event["event_observed_available_at"] == received.isoformat(timespec="microseconds")
    assert facts.status_for("000003") == pit_facts.PIT_DATA_BLOCKED
    assert facts.reason_for("000003") == "PIT_EVENT_BOUND_REVISION_NOT_ELIGIBLE"

    fixture = decode_score_snapshot(scored_publication([
        {"stock_code": code,
         "event_revision_ids": [item["event_revision_id"] for item in facts.facts.get(code, [])],
         "event_content_hashes": [item["event_content_hash"] for item in facts.facts.get(code, [])]}
        for code in catalog.codes
    ], trade_date=TARGET)["score_snapshot"])
    core = {
        "kline": {"stage": "qmt_stock_daily_canonical", "dataset_id": "daily", "root": "1" * 64},
        "turnover": {"stage": "target_turnover_snapshot", "dataset_id": "turnover", "root": "2" * 64},
        "financial": {"stage": "stock_finance", "dataset_id": "finance", "root": "3" * 64},
        "announcement": {"stage": stage, "dataset_id": producer_uid,
                         "root": signed.score_binding["evidence_sha256"], "input_root": batch["batch_root_hash"]},
    }
    score = build_score_snapshot(
        trade_date=TARGET, decision_at=cutoff, run_uid="3" * 32, build_sha=BUILD_SHA,
        analysis_rows=fixture["analysis_rows"],
        scored_rows=[{**row, "flow_input_decision_at": cutoff.isoformat()} for row in fixture["scored_rows"]],
        frozen_announcement_source=signed.score_binding, frozen_core_inputs=core,
    )
    decoded = decode_score_snapshot(score, trade_date=TARGET)
    assert decoded["frozen_announcement_source"] == signed.score_binding
    assert decoded["input_decision_at"] == cutoff.isoformat()

    before_bound_publication = datetime.combine(target, datetime.min.time())
    blocked = read_facts(
        replace(event_reads[0], fact_cutoff_at=before_bound_publication), fact_cutoff=before_bound_publication,
    )
    assert blocked.status_for("000001") == pit_facts.PIT_DATA_BLOCKED
    assert blocked.reason_for("000001") == "PIT_EVENT_BOUND_REVISION_NOT_ELIGIBLE"
    assert "000001" not in blocked.coverage_by_code
    assert blocked.status_for("000002") == pit_facts.PIT_DATA_BLOCKED
    assert blocked.reason_for("000002") == "PIT_EVENT_BOUND_REVISION_NOT_ELIGIBLE"
    assert "000002" not in blocked.coverage_by_code

    missing_id = date_only_event["event_revision_id"]

    def omit_bound_revision(_connection, _cursor, sql, parameters, _context, _many):
        if "FROM st_pit_event_revision" in sql and "WHERE revision_id IN" in sql and missing_id in parameters:
            return sql + " AND revision_id<>?", (*parameters, missing_id)
        return sql, parameters

    event.listen(engine, "before_cursor_execute", omit_bound_revision, retval=True)
    try:
        missing = read_facts(event_reads[0])
    finally:
        event.remove(engine, "before_cursor_execute", omit_bound_revision)
    assert missing.status_for("000002") == pit_facts.PIT_SCHEMA_UNAVAILABLE
    assert missing.reason_for("000002").startswith("PIT_EVENT_SCHEMA_UNAVAILABLE:")
    assert "000002" not in missing.coverage_by_code

    pit_facts.append_source_coverage(
        engine, fact_kind="event", stock_code="600000", window_start=start, window_end=target,
        known_at=received, covered_through_at=received, watermark_kind="CAPTURED_AT",
        watermark_evidence={"provider": batch["source"]}, source_rows=[], fact_bindings=[],
        source=batch["source"], batch_id="same-frontier-cninfo-coverage",
    )
    selected = pit_facts._load_certified_initial_event_selection(engine, signed, decision=cutoff, **selection_scope)
    assert selected.members["600000"][0]["batch_id"] == batch["batch_id"]
    assert selected.members["600000"][1] is False
    not_head = read_facts(replace(event_reads[0], selection=selected))
    assert not_head.status_for("600000") == pit_facts.PIT_NO_ROWS
    assert "600000" not in not_head.coverage_by_code
