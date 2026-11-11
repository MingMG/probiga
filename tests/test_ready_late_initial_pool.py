from datetime import date, datetime, timedelta, timezone
import hashlib
import json

import pytest
from sqlalchemy import text

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


def test_late_signed_source_obeys_actual_availability_and_round_trips_sealed_pool(pool_calendar, monkeypatch):
    engine = pool_calendar
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "development")
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
        "run_uid": source_uid, "task_type": stage, "build_sha": BUILD_SHA,
        "status": "success", "exit_code": 0, "validation_checked": True, "validation_ok": True,
        "replay_output": replay, "replay_output_sha256": digest,
        "input_receipt_root_sha256": digest, "target_trade_date": TARGET,
    }
    monkeypatch.setattr(control, "_control_now", lambda: finished - timedelta(seconds=30))
    control.start_daily_stage_attempt(
        engine, scheduler_run_uid=source_uid, stage_name=stage, trade_date=TARGET,
        release_id=BUILD_SHA, strategy_release_id=STRATEGY_RELEASE_ID, lease_owner="windows-fixture",
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
