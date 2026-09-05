from __future__ import annotations

from datetime import datetime, timedelta
import hashlib
import json
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, text

from server.common import daily_delivery_control as control
from server.api import scheduler_runtime
from server.api.routers import scheduler as scheduler_router


BUILD_SHA = "a" * 40
STRATEGY_RELEASE_ID = "b" * 64


def _engine():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    control.privileged_migrate_daily_delivery_schema(engine)
    return engine


def _stage_evidence(run_uid: str, *, stage: str, target: str) -> dict:
    replay_output = json.dumps(
        {
            "schema": "test.daily-stage-result.v1",
            "status": "PASS",
            "trade_date": target,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    input_root = hashlib.sha256(replay_output.encode()).hexdigest()
    core = {
        "schema": control.SCHEDULER_VALIDATION_EVIDENCE_SCHEMA,
        "run_uid": run_uid,
        "task_id": 111,
        "task_name": stage,
        "task_type": stage,
        "build_sha": BUILD_SHA,
        "status": "success",
        "exit_code": 0,
        "started_at": f"{target} 20:00:00",
        "validation_checked": True,
        "validation_ok": True,
        "validation_message": "exact persisted partition verified",
        "machine_output_sha256": input_root,
        "replay_output": replay_output,
        "replay_output_sha256": input_root,
        "input_receipt_root_sha256": input_root,
        "target_trade_date": target,
        "release_target_date": target,
    }
    return {**core, "evidence_sha256": control.canonical_sha256(core)}


def _finish_success_with_evidence(engine, attempt: dict, evidence: dict) -> None:
    with engine.begin() as connection:
        control.finish_daily_stage_attempt(
            connection,
            scheduler_run_uid=attempt["scheduler_run_uid"],
            status="success",
            input_root_sha256=evidence["input_receipt_root_sha256"],
            checkpoint=evidence,
        )


def _legacy_delivery(run_uid: str) -> dict[str, object]:
    receipt = {
        "schema": "probiga.daily-result-delivery-receipt.v1",
        "status": "VERIFIED_DELIVERED",
        "target_trade_date": "2026-09-02",
        "analysis_run_uid": run_uid,
        "analysis_count": 5200,
        "recommendation_count": 80,
        "executable_count": 12,
        "canonical_pool_sha256": "c" * 64,
        "base_data_receipt_root_sha256": "d" * 64,
        "governance_run_uid": "e" * 32,
        "governance_status": "COMPLETED",
        "governance_tradable_count": 20,
        "governance_result_sha256": "f" * 64,
        "strategy_pool_status": "ACTIVE",
        "ticket_pool_status": "ACTIVE",
        "strategy_pool_api_verified": True,
        "ticket_pool_api_verified": True,
        "delivery_receipt_sha256": "1" * 64,
    }
    receipt["base_data_receipts"] = [
        {
            "task_type": task_type,
            "run_uid": f"{index:x}" * 32,
            "evidence_sha256": f"{index + 4:x}" * 64,
            "input_receipt_root_sha256": f"{index + 8:x}" * 64,
        }
        for index, task_type in enumerate(
            (
                "qmt_stock_daily_canonical",
                "target_turnover_snapshot",
                "stock_finance",
                "qmt_announcement_pit",
            ),
            start=1,
        )
    ]
    return receipt


def test_schema_session_and_stage_attempt_are_idempotent_and_fenced():
    engine = _engine()
    first_uid = "1" * 32
    first = control.start_daily_stage_attempt(
        engine,
        scheduler_run_uid=first_uid,
        stage_name="analysis_fast_preliminary",
        trade_date="2026-09-02",
        release_id=BUILD_SHA,
        strategy_release_id=STRATEGY_RELEASE_ID,
        lease_owner="linux-100",
    )
    replay = control.start_daily_stage_attempt(
        engine,
        scheduler_run_uid=first_uid,
        stage_name="analysis_fast_preliminary",
        trade_date="2026-09-02",
        release_id=BUILD_SHA,
        strategy_release_id=STRATEGY_RELEASE_ID,
        lease_owner="linux-100",
    )
    assert first["attempt_uid"] == replay["attempt_uid"]
    assert first["run_id"] == "20260902-aaaaaaaaaaaa"
    with pytest.raises(control.DailyDeliveryLeaseHeld, match="unexpired lease"):
        control.start_daily_stage_attempt(
            engine,
            scheduler_run_uid="2" * 32,
            stage_name="analysis_fast_preliminary",
            trade_date="2026-09-02",
            release_id=BUILD_SHA,
            strategy_release_id=STRATEGY_RELEASE_ID,
            lease_owner="linux-200",
        )
    with engine.begin() as connection:
        connection.execute(
            text(
                f"UPDATE {control.ATTEMPT_TABLE} SET lease_until=:expired "
                "WHERE attempt_uid=:attempt_uid"
            ),
            {
                "expired": datetime.now() - timedelta(seconds=1),
                "attempt_uid": first["attempt_uid"],
            },
        )
    second = control.start_daily_stage_attempt(
        engine,
        scheduler_run_uid="2" * 32,
        stage_name="analysis_fast_preliminary",
        trade_date="2026-09-02",
        release_id=BUILD_SHA,
        strategy_release_id=STRATEGY_RELEASE_ID,
        lease_owner="linux-200",
    )
    assert int(second["attempt_no"]) == 2
    assert int(second["fencing_token"]) > int(first["fencing_token"])
    assert control.renew_daily_stage_lease(
        engine,
        attempt_uid=second["attempt_uid"],
        fencing_token=int(second["fencing_token"]),
        lease_owner="linux-200",
    )
    assert not control.renew_daily_stage_lease(
        engine,
        attempt_uid=first["attempt_uid"],
        fencing_token=int(first["fencing_token"]),
        lease_owner="linux-100",
    )

    with engine.begin() as connection:
        superseded = control.finish_daily_stage_attempt(
            connection,
            scheduler_run_uid=first_uid,
            status="success",
        )
        finished = control.finish_daily_stage_attempt(
            connection,
            scheduler_run_uid="2" * 32,
            status="success",
            output_dataset_id="score-snapshot-2",
        )
    assert superseded["status"] == "SUPERSEDED"
    assert finished["status"] == "SUCCESS"


def _sealed_blocked_strategy(engine):
    attempt = control.start_daily_stage_attempt(
        engine, scheduler_run_uid="f" * 32,
        stage_name="analysis_upper_evidence_prepare", trade_date="2026-09-02",
        release_id=BUILD_SHA, strategy_release_id=STRATEGY_RELEASE_ID,
        lease_owner="linux-strategy",
    )
    with engine.begin() as connection:
        control.finish_daily_stage_attempt(
            connection, scheduler_run_uid="f" * 32, status="blocked",
            error_code="UPPER_PIT_UNAVAILABLE",
        )
        session = control.load_daily_delivery_session(connection, attempt["session_uid"])
        receipt = control.build_terminal_delivery_receipt(
            session=session, scheduler_run_uid="f" * 32,
            stage_name="analysis_upper_evidence_prepare", status="BLOCKED",
            strategy_release_id=STRATEGY_RELEASE_ID, retryable=False,
            error_code="UPPER_PIT_UNAVAILABLE",
        )
        return control.persist_terminal_delivery_receipt(connection, receipt=receipt)


def test_raw_ingestion_and_completed_replay_preserve_signed_terminal_and_fences():
    engine = _engine()
    blocked = _sealed_blocked_strategy(engine)
    arguments = dict(
        stage_name="qmt_stock_daily_canonical", trade_date="2026-09-02",
        release_id=BUILD_SHA, strategy_release_id=STRATEGY_RELEASE_ID,
        lease_owner="windows-data", preserve_session_status=True,
        reuse_completed_stage=True,
    )
    first = control.start_daily_stage_attempt(engine, scheduler_run_uid="1" * 32, **arguments)
    with pytest.raises(control.DailyDeliveryLeaseHeld):
        control.start_daily_stage_attempt(engine, scheduler_run_uid="2" * 32, **arguments)
    evidence = _stage_evidence(
        "1" * 32, stage="qmt_stock_daily_canonical", target="2026-09-02"
    )
    _finish_success_with_evidence(engine, first, evidence)
    replay = control.start_daily_stage_attempt(engine, scheduler_run_uid="2" * 32, **arguments)
    assert replay["idempotent_replay"] is True
    assert replay["fencing_token"] > first["fencing_token"]
    materialized = control.read_daily_delivery(engine, trade_date="2026-09-02", release_id=BUILD_SHA)
    assert materialized["session"]["status"] == "BLOCKED"
    assert materialized["session"]["canonical_receipt_uid"] == blocked["receipt_uid"]
    assert materialized["session"]["latest_generation"] == 1
    assert materialized["receipt"] == blocked
    assert materialized["receipt"]["retryable"] is False
    assert not control.renew_daily_stage_lease(
        engine, attempt_uid=first["attempt_uid"], fencing_token=first["fencing_token"],
        lease_owner="windows-data",
    )
    engine.dispose()


@pytest.mark.parametrize("status", ["success", "blocked", "failed"])
def test_scheduler_raw_ingestion_finish_cannot_replace_signed_strategy_terminal(status):
    engine = _engine()
    blocked = _sealed_blocked_strategy(engine)
    run_uid = "1" * 32
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_scheduled_task_history (
                run_uid TEXT PRIMARY KEY, status TEXT, finished_at DATETIME,
                duration INTEGER, exit_code INTEGER, output TEXT
            )
        """))
        connection.execute(text(
            "INSERT INTO st_scheduled_task_history (run_uid, status) VALUES (:run_uid, 'running')"
        ), {"run_uid": run_uid})
        connection.connection.driver_connection.create_function(
            "NOW", 0, lambda: "2026-09-02 20:01:00"
        )
    control.start_daily_stage_attempt(
        engine, scheduler_run_uid=run_uid, stage_name="qmt_stock_daily_canonical",
        trade_date="2026-09-02", release_id=BUILD_SHA,
        strategy_release_id=STRATEGY_RELEASE_ID, lease_owner="windows-data",
        preserve_session_status=True,
    )
    evidence = _stage_evidence(run_uid, stage="qmt_stock_daily_canonical", target="2026-09-02")
    scheduler_runtime._task_history_finish(
        engine, run_uid, task_type="qmt_stock_daily_canonical", status=status,
        duration=1, exit_code=0 if status == "success" else 2,
        output=json.dumps(evidence) if status == "success" else "source request failed",
    )
    materialized = control.read_daily_delivery(engine, trade_date="2026-09-02", release_id=BUILD_SHA)
    assert materialized["session"]["status"] == "BLOCKED"
    assert materialized["receipt"] == blocked
    with engine.connect() as connection:
        assert connection.execute(text(
            "SELECT status FROM st_scheduled_task_history WHERE run_uid=:run_uid"
        ), {"run_uid": run_uid}).scalar_one() == status
    engine.dispose()


def test_completed_canonical_stage_replays_atomically_without_fence_regression():
    engine = _engine()
    stage = "qmt_stock_daily_canonical"
    first_uid = "a" * 32
    first = control.start_daily_stage_attempt(
        engine,
        scheduler_run_uid=first_uid,
        stage_name=stage,
        trade_date="2026-09-01",
        release_id=BUILD_SHA,
        strategy_release_id=STRATEGY_RELEASE_ID,
        lease_owner="windows-100",
        reuse_completed_stage=True,
    )
    evidence = _stage_evidence(
        first_uid,
        stage=stage,
        target="2026-09-01",
    )
    _finish_success_with_evidence(engine, first, evidence)

    reentered = control.start_daily_stage_attempt(
        engine,
        scheduler_run_uid=first_uid,
        stage_name=stage,
        trade_date="2026-09-01",
        release_id=BUILD_SHA,
        strategy_release_id=STRATEGY_RELEASE_ID,
        lease_owner="windows-100",
        reuse_completed_stage=True,
    )

    second = control.start_daily_stage_attempt(
        engine,
        scheduler_run_uid="c" * 32,
        stage_name=stage,
        trade_date="2026-09-01",
        release_id=BUILD_SHA,
        strategy_release_id=STRATEGY_RELEASE_ID,
        lease_owner="windows-100",
        input_root_sha256=evidence["input_receipt_root_sha256"],
        reuse_completed_stage=True,
    )
    third = control.start_daily_stage_attempt(
        engine,
        scheduler_run_uid="d" * 32,
        stage_name=stage,
        trade_date="2026-09-01",
        release_id=BUILD_SHA,
        strategy_release_id=STRATEGY_RELEASE_ID,
        lease_owner="windows-100",
        input_root_sha256=evidence["input_receipt_root_sha256"],
        reuse_completed_stage=True,
    )

    assert reentered["idempotent_replay"] is True
    assert json.loads(reentered["idempotent_replay_evidence"])[
        "idempotent_replay"
    ]["child_process_started"] is False
    assert second["idempotent_replay"] is True
    assert third["idempotent_replay"] is True
    assert second["status"] == third["status"] == "SUCCESS"
    assert int(first["fencing_token"]) < int(second["fencing_token"])
    assert int(second["fencing_token"]) < int(third["fencing_token"])
    assert int(second["attempt_no"]) == 2
    assert int(third["attempt_no"]) == 3
    replay_evidence = json.loads(third["idempotent_replay_evidence"])
    replay_core = {
        key: value
        for key, value in replay_evidence.items()
        if key != "evidence_sha256"
    }
    assert replay_evidence["run_uid"] == "d" * 32
    assert replay_evidence["target_trade_date"] == "2026-09-01"
    assert replay_evidence["build_sha"] == BUILD_SHA
    assert replay_evidence["input_receipt_root_sha256"] == evidence[
        "input_receipt_root_sha256"
    ]
    assert replay_evidence["evidence_sha256"] == control.canonical_sha256(
        replay_core
    )
    with engine.connect() as connection:
        session = connection.execute(
            text(
                f"SELECT latest_fencing_token, latest_generation, "
                f"canonical_receipt_uid FROM {control.SESSION_TABLE}"
            )
        ).mappings().one()
    assert int(session["latest_fencing_token"]) == int(third["fencing_token"])
    assert int(session["latest_generation"]) == 0
    assert session["canonical_receipt_uid"] is None


def test_completed_canonical_stage_replay_rejects_input_or_checkpoint_drift():
    stage = "qmt_stock_daily_canonical"
    for drift in ("input", "checkpoint"):
        engine = _engine()
        first_uid = "e" * 32
        first = control.start_daily_stage_attempt(
            engine,
            scheduler_run_uid=first_uid,
            stage_name=stage,
            trade_date="2026-09-01",
            release_id=BUILD_SHA,
            strategy_release_id=STRATEGY_RELEASE_ID,
            lease_owner="windows-100",
            reuse_completed_stage=True,
        )
        evidence = _stage_evidence(
            first_uid,
            stage=stage,
            target="2026-09-01",
        )
        _finish_success_with_evidence(engine, first, evidence)
        if drift == "checkpoint":
            with engine.begin() as connection:
                connection.execute(
                    text(
                        f"UPDATE {control.ATTEMPT_TABLE} "
                        "SET checkpoint_json=:checkpoint"
                    ),
                    {"checkpoint": json.dumps({**evidence, "build_sha": "f" * 40})},
                )
        expected_root = (
            "f" * 64
            if drift == "input"
            else evidence["input_receipt_root_sha256"]
        )
        with pytest.raises(
            control.DailyDeliveryFenceLost,
            match=("input identity differs" if drift == "input" else "checkpoint identity differs"),
        ):
            control.start_daily_stage_attempt(
                engine,
                scheduler_run_uid="f" * 32,
                stage_name=stage,
                trade_date="2026-09-01",
                release_id=BUILD_SHA,
                strategy_release_id=STRATEGY_RELEASE_ID,
                lease_owner="windows-100",
                input_root_sha256=expected_root,
                reuse_completed_stage=True,
            )


def test_expired_writer_cannot_publish_success():
    engine = _engine()
    run_uid = "3" * 32
    attempt = control.start_daily_stage_attempt(
        engine,
        scheduler_run_uid=run_uid,
        stage_name="analysis_fast",
        trade_date="2026-09-02",
        release_id=BUILD_SHA,
        strategy_release_id=STRATEGY_RELEASE_ID,
        lease_owner="linux-100",
    )
    expired = datetime.now() - timedelta(seconds=1)
    with engine.begin() as connection:
        connection.execute(
            text(
                f"UPDATE {control.ATTEMPT_TABLE} SET lease_until=:expired "
                "WHERE attempt_uid=:attempt_uid"
            ),
            {"expired": expired, "attempt_uid": attempt["attempt_uid"]},
        )
    with engine.begin() as connection:
        with pytest.raises(control.DailyDeliveryFenceLost, match="lease expired"):
            control.finish_daily_stage_attempt(
                connection,
                scheduler_run_uid=run_uid,
                status="success",
            )


def test_running_stage_cannot_publish_a_terminal_receipt():
    engine = _engine()
    run_uid = "0" * 31 + "1"
    attempt = control.start_daily_stage_attempt(
        engine,
        scheduler_run_uid=run_uid,
        stage_name="strategy_governance_daily",
        trade_date="2026-09-02",
        release_id=BUILD_SHA,
        strategy_release_id=STRATEGY_RELEASE_ID,
        lease_owner="linux-100",
    )
    with engine.begin() as connection:
        session = control.load_daily_delivery_session(
            connection,
            attempt["session_uid"],
        )
        receipt = control.build_terminal_delivery_receipt(
            session=session,
            scheduler_run_uid=run_uid,
            stage_name="strategy_governance_daily",
            status="PASS",
            strategy_release_id=STRATEGY_RELEASE_ID,
            legacy_receipt=_legacy_delivery("2" * 32),
        )
        with pytest.raises(RuntimeError, match="stage attempt differs"):
            control.persist_terminal_delivery_receipt(
                connection,
                receipt=receipt,
            )


def test_blocked_then_pass_receipts_keep_attempt_history_and_advance_generation():
    engine = _engine()
    blocked_uid = "4" * 32
    blocked_attempt = control.start_daily_stage_attempt(
        engine,
        scheduler_run_uid=blocked_uid,
        stage_name="stock_finance",
        trade_date="2026-09-02",
        release_id=BUILD_SHA,
        strategy_release_id=STRATEGY_RELEASE_ID,
        lease_owner="linux-100",
    )
    with engine.begin() as connection:
        control.finish_daily_stage_attempt(
            connection,
            scheduler_run_uid=blocked_uid,
            status="blocked",
            error_code="FINANCE_PIT_UNKNOWN",
            error_detail="one stock has no legal PIT state",
        )
        session = control.ensure_daily_delivery_session(
            connection,
            trade_date="2026-09-02",
            release_id=BUILD_SHA,
            strategy_release_id=STRATEGY_RELEASE_ID,
        )
        blocked = control.build_terminal_delivery_receipt(
            session=session,
            scheduler_run_uid=blocked_uid,
            stage_name="stock_finance",
            status="BLOCKED",
            strategy_release_id=STRATEGY_RELEASE_ID,
            retryable=True,
            error_code="FINANCE_PIT_UNKNOWN",
            error_detail="one stock has no legal PIT state",
        )
        blocked = control.persist_terminal_delivery_receipt(
            connection,
            receipt=blocked,
        )
        blocked_replay = control.persist_terminal_delivery_receipt(
            connection,
            receipt=control.build_terminal_delivery_receipt(
                session=session,
                scheduler_run_uid=blocked_uid,
                stage_name="stock_finance",
                status="BLOCKED",
                strategy_release_id=STRATEGY_RELEASE_ID,
                retryable=True,
                error_code="FINANCE_PIT_UNKNOWN",
                error_detail="one stock has no legal PIT state",
            ),
        )
    assert blocked["status"] == "BLOCKED"
    assert blocked["generation"] == 1
    assert blocked_replay == blocked

    pass_uid = "5" * 32
    control.start_daily_stage_attempt(
        engine,
        scheduler_run_uid=pass_uid,
        stage_name="strategy_governance_daily",
        trade_date="2026-09-02",
        release_id=BUILD_SHA,
        strategy_release_id=STRATEGY_RELEASE_ID,
        lease_owner="linux-100",
    )
    legacy = _legacy_delivery("6" * 32)
    with engine.begin() as connection:
        control.finish_daily_stage_attempt(
            connection,
            scheduler_run_uid=pass_uid,
            status="success",
        )
        session = control.ensure_daily_delivery_session(
            connection,
            trade_date="2026-09-02",
            release_id=BUILD_SHA,
            strategy_release_id=STRATEGY_RELEASE_ID,
        )
        passed = control.build_terminal_delivery_receipt(
            session=session,
            scheduler_run_uid=pass_uid,
            stage_name="strategy_governance_daily",
            status="PASS",
            strategy_release_id=STRATEGY_RELEASE_ID,
            legacy_receipt=legacy,
        )
        passed = control.persist_terminal_delivery_receipt(
            connection,
            receipt=passed,
        )
    assert passed["status"] == "PASS"
    assert passed["generation"] == 2
    assert passed["score_snapshot_id"] == control.score_snapshot_identity(legacy)
    assert set(passed["core_inputs"]) == {
        "kline",
        "turnover",
        "financial",
        "announcement",
    }
    assert passed["core_inputs"]["kline"]["dataset_id"] == "1" * 32

    materialized = control.read_daily_delivery(
        engine,
        trade_date="2026-09-02",
        release_id=BUILD_SHA,
    )
    assert materialized is not None
    assert materialized["session"]["status"] == "PASS"
    assert materialized["receipt"]["receipt_uid"] == passed["receipt_uid"]
    assert len(materialized["attempts"]) == 2
    assert materialized["attempts"][0]["stage_name"] == "strategy_governance_daily"

    # A later manual retry may fail, but it cannot demote an already verified
    # canonical delivery for the same date/release.
    later_uid = "9" * 32
    control.start_daily_stage_attempt(
        engine,
        scheduler_run_uid=later_uid,
        stage_name="stock_finance",
        trade_date="2026-09-02",
        release_id=BUILD_SHA,
        strategy_release_id=STRATEGY_RELEASE_ID,
        lease_owner="linux-100",
    )
    with engine.begin() as connection:
        control.finish_daily_stage_attempt(
            connection,
            scheduler_run_uid=later_uid,
            status="failed",
        )
        session = control.ensure_daily_delivery_session(
            connection,
            trade_date="2026-09-02",
            release_id=BUILD_SHA,
            strategy_release_id=STRATEGY_RELEASE_ID,
        )
        later_block = control.build_terminal_delivery_receipt(
            session=session,
            scheduler_run_uid=later_uid,
            stage_name="stock_finance",
            status="BLOCKED",
            strategy_release_id=STRATEGY_RELEASE_ID,
            retryable=True,
            error_code="UPSTREAM_TEMPORARY_FAILURE",
        )
        control.persist_terminal_delivery_receipt(connection, receipt=later_block)
    still_pass = control.read_daily_delivery(
        engine,
        trade_date="2026-09-02",
        release_id=BUILD_SHA,
    )
    assert still_pass["session"]["status"] == "PASS"
    assert still_pass["receipt"]["receipt_uid"] == passed["receipt_uid"]


def test_strategy_change_cannot_reuse_same_release_session():
    engine = _engine()
    control.start_daily_stage_attempt(
        engine,
        scheduler_run_uid="7" * 32,
        stage_name="analysis_fast",
        trade_date="2026-09-02",
        release_id=BUILD_SHA,
        strategy_release_id=STRATEGY_RELEASE_ID,
        lease_owner="linux-100",
    )
    with pytest.raises(RuntimeError, match="session identity differs"):
        control.start_daily_stage_attempt(
            engine,
            scheduler_run_uid="8" * 32,
            stage_name="analysis_fast",
            trade_date="2026-09-02",
            release_id=BUILD_SHA,
            strategy_release_id="9" * 64,
            lease_owner="linux-100",
        )


def test_degraded_delivery_is_not_demoted_by_a_later_failed_retry():
    engine = _engine()
    degraded_uid = "d" * 32
    control.start_daily_stage_attempt(
        engine,
        scheduler_run_uid=degraded_uid,
        stage_name="strategy_governance_daily",
        trade_date="2026-09-02",
        release_id=BUILD_SHA,
        strategy_release_id=STRATEGY_RELEASE_ID,
        lease_owner="linux-100",
    )
    with engine.begin() as connection:
        control.finish_daily_stage_attempt(
            connection,
            scheduler_run_uid=degraded_uid,
            status="degraded",
        )
        session = control.load_daily_delivery_session(
            connection,
            control.daily_session_identity("2026-09-02", BUILD_SHA)["session_uid"],
        )
        degraded = control.build_terminal_delivery_receipt(
            session=session,
            scheduler_run_uid=degraded_uid,
            stage_name="strategy_governance_daily",
            status="DEGRADED",
            strategy_release_id=STRATEGY_RELEASE_ID,
            legacy_receipt=_legacy_delivery("e" * 32),
            degradations=[
                {
                    "code": "OPTIONAL_SENTIMENT_UNAVAILABLE",
                    "impact": "ranking core unchanged",
                }
            ],
        )
        degraded = control.persist_terminal_delivery_receipt(
            connection,
            receipt=degraded,
        )

    failed_uid = "f" * 32
    control.start_daily_stage_attempt(
        engine,
        scheduler_run_uid=failed_uid,
        stage_name="stock_finance",
        trade_date="2026-09-02",
        release_id=BUILD_SHA,
        strategy_release_id=STRATEGY_RELEASE_ID,
        lease_owner="linux-100",
    )
    with engine.begin() as connection:
        control.finish_daily_stage_attempt(
            connection,
            scheduler_run_uid=failed_uid,
            status="failed",
        )
        session = control.load_daily_delivery_session(
            connection,
            degraded["session_uid"],
        )
        blocked = control.build_terminal_delivery_receipt(
            session=session,
            scheduler_run_uid=failed_uid,
            stage_name="stock_finance",
            status="BLOCKED",
            strategy_release_id=STRATEGY_RELEASE_ID,
            error_code="FINANCE_RETRY_FAILED",
        )
        control.persist_terminal_delivery_receipt(connection, receipt=blocked)

    materialized = control.read_daily_delivery(
        engine,
        trade_date="2026-09-02",
        release_id=BUILD_SHA,
    )
    assert materialized["session"]["status"] == "DEGRADED"
    assert materialized["receipt"]["receipt_uid"] == degraded["receipt_uid"]


@pytest.mark.parametrize("stage", ["stock_finance", "analysis_upper_evidence_prepare"])
def test_scheduler_failure_only_delivery_stage_materializes_terminal_receipt(stage):
    engine = _engine()
    run_uid = "8" * 32
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_scheduled_task_history (
                run_uid TEXT PRIMARY KEY,
                task_type TEXT NOT NULL,
                run_at DATETIME,
                finished_at DATETIME,
                status TEXT NOT NULL,
                duration INTEGER,
                exit_code INTEGER,
                output TEXT
            )
        """))
        connection.execute(text("""
            INSERT INTO st_scheduled_task_history
                (run_uid, task_type, run_at, status, output)
            VALUES (:run_uid, :stage,
                    '2026-09-02 20:00:00', 'running', '')
        """), {"run_uid": run_uid, "stage": stage})
    with engine.connect() as connection:
        connection.connection.driver_connection.create_function(
            "NOW", 0, lambda: "2026-09-02 20:01:00"
        )
    control.start_daily_stage_attempt(
        engine,
        scheduler_run_uid=run_uid,
        stage_name=stage,
        trade_date="2026-09-02",
        release_id=BUILD_SHA,
        strategy_release_id=STRATEGY_RELEASE_ID,
        lease_owner="linux-100",
    )
    blocked_output = json.dumps(
        {
            "status": "blocked",
            "reason_code": "FINANCE_PIT_UNKNOWN",
            "blocking_stage": stage,
            "retryable": False,
            "real_order_authority": False,
        },
        sort_keys=True,
    )
    with patch(
        "server.api.scheduler_runtime.strategy_release_identity",
        return_value=STRATEGY_RELEASE_ID,
    ):
        scheduler_runtime._task_history_finish(
            engine,
            run_uid,
            status="blocked",
            duration=60,
            exit_code=2,
            output=blocked_output,
            task_type=stage,
        )

    materialized = control.read_daily_delivery(
        engine,
        trade_date="2026-09-02",
        release_id=BUILD_SHA,
    )
    if stage == "stock_finance":
        assert materialized["session"]["status"] == "RUNNING"
        assert materialized["receipt"] is None
    else:
        assert materialized["session"]["status"] == "BLOCKED"
        assert materialized["receipt"]["status"] == "BLOCKED"
        assert materialized["receipt"]["error_code"] == "FINANCE_PIT_UNKNOWN"
        assert materialized["receipt"]["retryable"] is False
    assert materialized["attempts"][0]["status"] == "BLOCKED"


def test_scheduler_delivery_api_returns_only_materialized_truth():
    engine = _engine()
    with patch.object(scheduler_router, "get_engine", return_value=engine):
        missing = scheduler_router.daily_delivery_status(
            trade_date="2026-09-02",
            release_id=BUILD_SHA,
            attempt_limit=20,
        )
    assert missing == {
        "status": "NOT_AVAILABLE",
        "trade_date": "2026-09-02",
        "release_id": BUILD_SHA,
        "data": None,
    }

    run_uid = "a" * 32
    control.start_daily_stage_attempt(
        engine,
        scheduler_run_uid=run_uid,
        stage_name="stock_finance",
        trade_date="2026-09-02",
        release_id=BUILD_SHA,
        strategy_release_id=STRATEGY_RELEASE_ID,
        lease_owner="linux-100",
    )
    with engine.begin() as connection:
        control.finish_daily_stage_attempt(
            connection,
            scheduler_run_uid=run_uid,
            status="blocked",
            error_code="FINANCE_PIT_UNKNOWN",
        )
        session = control.ensure_daily_delivery_session(
            connection,
            trade_date="2026-09-02",
            release_id=BUILD_SHA,
            strategy_release_id=STRATEGY_RELEASE_ID,
        )
        receipt = control.build_terminal_delivery_receipt(
            session=session,
            scheduler_run_uid=run_uid,
            stage_name="stock_finance",
            status="BLOCKED",
            strategy_release_id=STRATEGY_RELEASE_ID,
            error_code="FINANCE_PIT_UNKNOWN",
        )
        control.persist_terminal_delivery_receipt(connection, receipt=receipt)

    with patch.object(scheduler_router, "get_engine", return_value=engine):
        response = scheduler_router.daily_delivery_status(
            trade_date="2026-09-02",
            release_id=BUILD_SHA,
            attempt_limit=20,
        )
    assert response["status"] == "BLOCKED"
    assert response["data"]["receipt"]["error_code"] == "FINANCE_PIT_UNKNOWN"
    assert response["data"]["attempts"][0]["status"] == "BLOCKED"

    with engine.begin() as connection:
        connection.execute(
            text(
                f"UPDATE {control.RECEIPT_TABLE} "
                "SET receipt_json='{}' WHERE scheduler_run_uid=:run_uid"
            ),
            {"run_uid": run_uid},
        )
    with pytest.raises(RuntimeError, match="receipt seal differs"):
        control.read_daily_delivery(
            engine,
            trade_date="2026-09-02",
            release_id=BUILD_SHA,
        )
