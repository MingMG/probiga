import hashlib
import json
from datetime import datetime
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, text

from server.api import scheduler_runtime
from server.common import daily_delivery_control as control
from tests.score_snapshot_helpers import scored_publication
from server.common.analysis_pool_receipt import build_publication_receipt, publication_receipt_is_valid


def _publish(engine, receipt):
    started = datetime(2026, 9, 8, 19)
    run_uid = receipt["run_uid"]
    with patch.object(control, "_control_now", return_value=started):
        control.start_daily_stage_attempt(engine, scheduler_run_uid=run_uid, stage_name="analysis_fast",
            trade_date="2026-09-08", release_id=receipt["build_sha"], strategy_release_id="b" * 64,
            lease_owner="test", lease_seconds=3600)
    output = json.dumps({"trade_date": "2026-09-08", "stock_count": receipt["analysis_count"],
                        "publication_receipt": receipt}, separators=(",", ":"))
    receipt = scheduler_runtime._analysis_publication_for_checkpoint(output)
    replay = scheduler_runtime._history_validation_replay_output(output)
    root = hashlib.sha256(replay.encode()).hexdigest()
    evidence = dict(schema=control.SCHEDULER_VALIDATION_EVIDENCE_SCHEMA,
        run_uid=run_uid, task_type="analysis_fast", build_sha=receipt["build_sha"],
        target_trade_date="2026-09-08", status="success", exit_code=0,
        validation_checked=True, validation_ok=True, replay_output=replay,
        replay_output_sha256=root, input_receipt_root_sha256=root)
    evidence["evidence_sha256"] = control.canonical_sha256(evidence)
    checkpoint = control.bind_analysis_publication_checkpoint(evidence, receipt)
    with engine.begin() as connection:
        control.finish_daily_stage_attempt(connection, scheduler_run_uid=run_uid,
            status="success", input_root_sha256=root, output_dataset_id=run_uid,
            checkpoint=checkpoint, now=datetime(2026, 9, 8, 19, 2))
    return replay


def test_full_snapshot_survives_bounded_history_and_loads_without_governance():
    engine = create_engine("sqlite:///:memory:")
    control.privileged_migrate_daily_delivery_schema(engine)
    receipt = scored_publication([{"stock_code": str(600000 + i)} for i in range(1000)])
    assert len(json.dumps(receipt)) > scheduler_runtime._HISTORY_REPLAY_OUTPUT_LIMIT
    replay = _publish(engine, receipt)
    assert len(replay) < scheduler_runtime._HISTORY_REPLAY_OUTPUT_LIMIT
    assert "payload_base64" not in replay
    actual = control.load_published_analysis_receipt(engine, "2026-09-08", datetime(2026, 9, 9, 9, 30))
    assert actual == receipt
    with pytest.raises(control.DailyDeliveryControlError, match="UNAVAILABLE"):
        control.load_published_analysis_receipt(engine, "2026-09-08", datetime(2026, 9, 8, 19))


def test_original_run_remains_addressable_after_same_day_republication():
    engine = create_engine("sqlite:///:memory:")
    control.privileged_migrate_daily_delivery_schema(engine)
    first, second = scored_publication(), scored_publication(run_uid="3" * 32)
    _publish(engine, first)
    _publish(engine, second)
    assert control.load_published_analysis_receipt(engine, "2026-09-08")["run_uid"] == second["run_uid"]
    assert control.load_published_analysis_receipt(engine, "2026-09-08", run_uid=first["run_uid"]) == first


def test_tampered_checkpoint_fails_without_falling_back_to_older_success():
    engine = create_engine("sqlite:///:memory:")
    control.privileged_migrate_daily_delivery_schema(engine)
    _publish(engine, scored_publication())
    _publish(engine, scored_publication(run_uid="3" * 32))
    with engine.begin() as connection:
        connection.execute(text("UPDATE st_daily_stage_attempt SET checkpoint_json='{}' WHERE scheduler_run_uid=:uid"), {"uid": "3" * 32})
    with pytest.raises(control.DailyDeliveryControlError, match="INVALID"):
        control.load_published_analysis_receipt(engine, "2026-09-08")


def test_disabled_morning_publishers_cannot_replace_the_canonical_pool():
    receipt = scored_publication()
    for publisher in ("analysis_morning_strict", "analysis_premarket_external"):
        with pytest.raises(ValueError, match="CANONICAL_PUBLISHER"):
            build_publication_receipt(manifest=receipt, score_snapshot=receipt["score_snapshot"],
                run_uid=receipt["run_uid"], build_sha=receipt["build_sha"],
                publisher_task_type=publisher, published_at=receipt["published_at"])
        forged = dict(receipt, publisher_task_type=publisher)
        forged["receipt_id"] = control.canonical_sha256({key: value for key, value in forged.items() if key != "receipt_id"})
        assert not publication_receipt_is_valid(forged)


def test_checkpoint_extractor_rejects_multiple_nested_publications():
    output = "\n".join(json.dumps({"publication_receipt": receipt}) for receipt in
                       (scored_publication(), scored_publication(run_uid="3" * 32)))
    with pytest.raises(RuntimeError, match="ambiguous"):
        scheduler_runtime._analysis_publication_for_checkpoint(output)


@pytest.mark.parametrize("validator_build", ["3" * 40, "4" * 40])
def test_capture_availability_requires_completed_validator_stage_not_transaction_start(validator_build):
    engine = create_engine("sqlite:///:memory:")
    control.privileged_migrate_daily_delivery_schema(engine)
    run_uid, capture_uid, build = "1" * 32, "2" * 32, "3" * 40
    payload = dict(schema="probiga.market-field-capture.v1", status="COMPLETED", run_id=capture_uid,
        target_date="2026-09-08", collector_build_sha=build, published_at="2026-09-08T19:00:00")
    payload["validated_by_build_sha"] = validator_build
    replay = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    root = hashlib.sha256(replay.encode()).hexdigest()
    evidence = dict(schema=control.SCHEDULER_VALIDATION_EVIDENCE_SCHEMA,
        run_uid=run_uid, task_type="analysis_upper_evidence_prepare", build_sha=validator_build,
        target_trade_date="2026-09-08", status="success", exit_code=0,
        validation_checked=True, validation_ok=True, replay_output=replay,
        replay_output_sha256=root, input_receipt_root_sha256=root)
    evidence["evidence_sha256"] = control.canonical_sha256(evidence)
    with patch.object(control, "_control_now", return_value=datetime(2026, 9, 8, 19)):
        control.start_daily_stage_attempt(engine, scheduler_run_uid=run_uid,
            stage_name="analysis_upper_evidence_prepare", trade_date="2026-09-08",
            release_id=validator_build, strategy_release_id="b" * 64, lease_owner="test", lease_seconds=3600)
    with engine.begin() as connection:
        control.finish_daily_stage_attempt(connection, scheduler_run_uid=run_uid, status="success",
            input_root_sha256=root, checkpoint=evidence, now=datetime(2026, 9, 8, 19, 2))
    args = dict(raw_run_id=capture_uid, stage_name="analysis_upper_evidence_prepare",
                target_date="2026-09-08", build_sha=build)
    with pytest.raises(control.DailyDeliveryControlError, match="COMPLETION_UNAVAILABLE"):
        control.load_completed_market_capture_receipt(engine, decision_at=datetime(2026, 9, 8, 19, 1), **args)
    actual = control.load_completed_market_capture_receipt(engine, decision_at=datetime(2026, 9, 8, 19, 3), **args)
    assert actual["known_at"] == datetime(2026, 9, 8, 19, 2)
    assert actual["receipt"] == payload
    assert actual["validated_by_build_sha"] == validator_build
    with pytest.raises(control.DailyDeliveryControlError, match="COMPLETION_BINDING_INVALID"):
        control.load_completed_market_capture_receipt(engine,
            decision_at=datetime(2026, 9, 8, 19, 3), **{**args, "build_sha": "5" * 40})
