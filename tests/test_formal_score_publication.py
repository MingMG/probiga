import json
from dataclasses import replace
from datetime import datetime

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from biz.analysis import sync_analysis_fast as analysis
from server.common import scheduler_validation
from server.common.analysis_pool_receipt import decode_score_snapshot
from server.common.formal_analysis_store import FormalAnalysisStore
from server.common.formal_score_publication import (
    WORK_KEY,
    WORK_STAGE,
    load_initial_selection,
    pin_initial_cutoff,
    publish_initial_selection,
    seal_initial_selection,
)
from server.common.scheduler_validation import _validate_analysis_strategy_pool, scheduler_output_status
from server.db.formal_analysis_schema import privileged_migrate_formal_analysis_schema
from tests.score_snapshot_helpers import scored_publication


def test_publication_rolls_back_and_replay_reads_same_ranked_universe(monkeypatch, tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'initial.db'}")
    privileged_migrate_formal_analysis_schema(engine)
    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE TABLE pool (code CHAR(6) PRIMARY KEY)")
        connection.exec_driver_sql("INSERT INTO pool VALUES ('600099')")
    build, generation, target = "2" * 40, "a" * 64, "2026-11-13"
    decision_at = f"{target}T19:00:00"
    baseline = decode_score_snapshot(scored_publication([
        {"stock_code": "600001", "final_trade_score": 81, "ranking_score": 81.0},
        {"stock_code": "600002", "final_trade_score": 88, "ranking_score": 88.0},
    ], trade_date=target)["score_snapshot"])
    ranked = sorted(baseline["scored_rows"], key=lambda row: -row["ranking_score"])
    recommendations = [{"stock_code": row["stock_code"]} for row in ranked]
    store = FormalAnalysisStore(engine)
    initial = seal_initial_selection(
        store, generation=generation, trade_date=target, decision_at=decision_at,
        run_uid="1" * 32, build_sha=build, analysis_rows=baseline["analysis_rows"],
        recommendation_rows=recommendations, scored=pd.DataFrame(baseline["scored_rows"]),
        market_mood_score=66, flow_date=target, hot_date=target,
    )["reference"]

    # Reproduce the live ordering failure: the sealed worker has completed,
    # while the scheduler's publication transaction has not written any rows.
    requirements = scheduler_validation.TASK_OUTPUT_REQUIREMENTS
    monkeypatch.setitem(requirements, "analysis_fast", (replace(requirements["analysis_fast"][0], min_rows=2),))
    source_checks = []
    projection_checks = []

    def verify_source(_engine, *, target_date, **_):
        source_checks.append(target_date)
        return True, "source verified"

    def verify_projection(*_args, **_kwargs):
        projection_checks.append(True)
        return False, "projection still has 0 rows"

    monkeypatch.setattr(scheduler_validation, "_validate_daily_universe_coverage", verify_source)
    monkeypatch.setattr(scheduler_validation, "_validate_requirement", verify_projection)
    task = {
        "task_type": "analysis_fast", "_scheduler_pipeline_phase": "INITIAL",
        "_scheduler_pipeline_decision_at": decision_at, "_release_target_date": target,
        "_trigger_source": "release_catchup", "_scheduler_history_run_uid": initial["run_uid"],
        "_scheduler_expected_build_sha": build,
    }
    started = datetime.fromisoformat(decision_at)
    staged = scheduler_validation.validate_scheduler_task_result(
        task, engine=engine, started_at=started, now=started, output=json.dumps(initial),
    )
    assert staged.checked and staged.ok and len(source_checks) == 1
    assert not projection_checks
    legacy = scheduler_validation.validate_scheduler_task_result(
        {**task, "_scheduler_pipeline_phase": ""}, engine=engine, started_at=started,
        now=started, output=json.dumps(scored_publication(trade_date=target)),
    )
    assert legacy.checked and not legacy.ok and projection_checks == [True]

    def save_projection(_engine, rows, recommendations, _, *, publication_connection, decoded_score_snapshot, **__):
        assert rows == baseline["analysis_rows"]
        assert decoded_score_snapshot["scored_rows"] == baseline["scored_rows"]
        assert recommendations == [{"stock_code": "600002"}, {"stock_code": "600001"}]
        publication_connection.exec_driver_sql("DELETE FROM pool")
        publication_connection.execute(text("INSERT INTO pool VALUES (:stock_code)"), recommendations)
        return {"run_uid": decoded_score_snapshot["run_uid"]}

    monkeypatch.setattr(analysis, "save_outputs", save_projection)
    published = []

    def finalize(_engine, *, run_uid, publication_connection, **_):
        assert publication_connection.in_transaction()
        if run_uid == initial["run_uid"]:
            raise RuntimeError("injected failure after staging the new projection")
        published.append(run_uid)

    monkeypatch.setattr(analysis, "_finish_direct_publication_history", finalize)
    with pytest.raises(RuntimeError, match="injected failure"):
        with engine.begin() as connection:
            publish_initial_selection(connection, initial, run_uid=initial["run_uid"], build_sha=build, trade_date=target)

    with engine.connect() as connection:
        assert connection.execute(text("SELECT code FROM pool")).scalars().all() == ["600099"]
    original_artifact = store.list_members(generation, WORK_STAGE)[WORK_KEY]
    engine.dispose()
    replacement_uid = "3" * 32
    replay = load_initial_selection(
        FormalAnalysisStore(engine), generation=generation, run_uid=replacement_uid, build_sha=build,
    )["reference"]
    assert replay["selection_sha256"] == initial["selection_sha256"]
    assert store.list_members(generation, WORK_STAGE)[WORK_KEY] == original_artifact
    with engine.begin() as connection:
        publish_initial_selection(connection, replay, run_uid=replacement_uid, build_sha=build, trade_date=target)
    with engine.connect() as connection:
        assert connection.execute(text("SELECT code FROM pool ORDER BY code")).scalars().all() == ["600001", "600002"]
    assert published == [replacement_uid]
    assert _validate_analysis_strategy_pool(
        engine, target_date=pd.Timestamp(target).date(), started_at=None, now=None,
        scheduler_run_uid=replacement_uid, expected_build_sha=build,
        output=json.dumps(replay), expected_initial_cutoff=decision_at,
    )[0]


def test_retry_pins_first_eligible_cutoff_for_same_source_receipts(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'cutoff.db'}")
    privileged_migrate_formal_analysis_schema(engine)
    identity = {"trade_date": "2026-11-13", "release_id": "a" * 64}
    first, later = "2026-11-15T18:00:00", "2026-11-15T18:14:00"
    assert pin_initial_cutoff(engine, **identity, source_runs={"stock_finance": "1" * 32}, proposed_cutoff=first) == first
    assert pin_initial_cutoff(engine, **identity, source_runs={"stock_finance": "1" * 32}, proposed_cutoff=later) == first
    assert pin_initial_cutoff(engine, **identity, source_runs={"stock_finance": "2" * 32}, proposed_cutoff=later) == later
    blocked = json.dumps({"schema": "probiga.formal-initial-score-blocked.v1", "status": "blocked", "retryable": False})
    assert scheduler_output_status({"task_type": "analysis_fast"}, blocked, return_code=2) == "blocked"
