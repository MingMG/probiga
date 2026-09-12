from __future__ import annotations

import json
from contextlib import ExitStack
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, event, text

from server.api import scheduler_runtime as runtime
from server.common import scheduler_validation as validation


TASK = {
    "id": 75, "task_name": "capital flow",
    "task_type": "intraday_capital_flow_fast",
    "script_path": "tools/crawl_intraday_capital_flow_fast.py",
    "script_args": "--json --min-coverage 0.98", "date_param": "",
    "interval_minutes": 1,
}
CLAIM = datetime(2026, 9, 11, 16, 0)
UID = "a" * 32


def result(*, partial=False):
    return {
        "schema": "probiga.intraday-capital-flow-result.v1",
        "status": "written", "trade_time": "2026-09-11 10:01",
        "catalog_batch_id": "catalog-1", "catalog_manifest_hash": "b" * 64,
        "catalog_captured_at": "2026-09-11 09:00:00",
        "active_codes": 100, "extra_codes": 0,
        "expected_codes": 100, "selected_codes": 99 if partial else 100,
        "written_rows": 99 if partial else 100,
        "missing_codes": ["000100"] if partial else [],
        "coverage": 0.99 if partial else 1.0, "min_coverage": 0.98,
        "acquisition_status": "PARTIAL" if partial else "COMPLETE",
    }


def disposition(payload, *, task=TASK, code=0):
    return validation.scheduler_output_status(task, json.dumps(payload), return_code=code)


@pytest.mark.parametrize("reason", ["outside_continuous_auction", "not_trade_day"])
def test_exact_collector_skip(reason):
    assert disposition({"status": "skipped", "reason": reason}) == "skipped"


@pytest.mark.parametrize("task,reason", [
    ({"task_type": "intraday_realtime", "script_path": "tools/crawl_realtime_batch.py"}, "market_closed"),
    ({"task_type": "jq_minute_gml", "script_path": "tools/sync_jq_minute_gml.py"}, "market_closed"),
])
def test_existing_collector_machine_skip_contracts(task, reason):
    assert disposition({"status": "skipped", "reason": reason}, task=task) == "skipped"


@pytest.mark.parametrize("patch_task,patch_payload,code", [
    ({"task_type": "analysis_fast"}, {}, 0),
    ({"task_type": "strategy_governance_daily"}, {}, 0),
    ({"task_type": "python"}, {}, 0),
    ({"script_path": "tools/other.py"}, {}, 0),
    ({}, {"reason": "provider_failed"}, 0),
    ({}, {"reason": ["outside_continuous_auction"]}, 0),
    ({}, {"written_rows": 1}, 0),
    ({}, {"status": "SKIPPED"}, 0),
    ({}, {}, 1),
])
def test_skip_cannot_hide_failure_or_escape_exact_task(patch_task, patch_payload, code):
    payload = {"status": "skipped", "reason": "outside_continuous_auction", **patch_payload}
    assert disposition(payload, task={**TASK, **patch_task}, code=code) == "failed"


@pytest.mark.parametrize("output", [
    '{"status":"skipped","status":"written","reason":"outside_continuous_auction"}',
    '{"status":"skipped","reason":"outside_continuous_auction"}\n{"status":"written"}',
    '{"wrapper":{"status":"skipped","reason":"outside_continuous_auction"}}',
    "skipped: outside_continuous_auction",
])
def test_skip_requires_unambiguous_top_level_machine_result(output):
    assert validation.scheduler_output_status(TASK, output, return_code=0) == "failed"


@pytest.mark.parametrize("partial,status", [(False, "success"), (True, "degraded")])
def test_complete_and_partial_written_results(partial, status):
    assert disposition(result(partial=partial)) == status


@pytest.mark.parametrize("changes", [
    {"written_rows": 0}, {"written_rows": 98}, {"written_rows": True},
    {"selected_codes": "99"}, {"selected_codes": 100},
    {"expected_codes": 99}, {"expected_codes": 0},
    {"coverage": 1.0}, {"coverage": float("nan")}, {"coverage": True},
    {"min_coverage": 0.5}, {"min_coverage": 1.0},
    {"missing_codes": []}, {"missing_codes": ["000100", "000100"]},
    {"missing_codes": ["invalid"]}, {"acquisition_status": "COMPLETE"},
    {"catalog_manifest_hash": ""}, {"catalog_batch_id": ""},
    {"schema": "probiga.other.v1"},
    {"catalog_captured_at": "2026-09-11 11:00:00"},
    {"status": "coverage_failed"}, {"status": "dry_run"}, {"status": "ready"},
])
def test_partial_written_result_is_self_consistent(changes):
    assert disposition({**result(partial=True), **changes}) == "failed"


def readback(payload, *, rows=None, catalog=None, task=TASK):
    codes = [f"{n:06d}" for n in range(1, 101)]
    if rows is None:
        rows = [
            {
                "stock_code": code, "source_time": datetime(2026, 9, 11, 10, 1),
                "received_at": datetime(2026, 9, 11, 10, 1, 15),
                "etl_sync_at": datetime(2026, 9, 11, 10, 1, 15),
                "data_source": "east_push2delay",
            }
            for code in codes if code not in payload["missing_codes"]
        ]
    catalog = catalog or SimpleNamespace(
        batch_id="catalog-1", manifest_hash="b" * 64,
        captured_at="2026-09-11 09:00:00",
    )
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value.mappings.return_value.all.return_value = rows
    with patch.object(validation, "load_target_stock_catalog", return_value=(catalog, codes)) as load, patch.object(
        validation, "routed_read_engine", return_value=engine,
    ):
        checked = validation.validate_scheduler_task_result(
            task, engine=engine, started_at=datetime(2026, 9, 11, 10, 1, 1),
            now=datetime(2026, 9, 11, 10, 1, 20), output=json.dumps(payload),
        )
    return checked, rows, load


@pytest.mark.parametrize("partial", [False, True])
def test_written_classification_requires_actual_catalog_and_partition(partial):
    checked, _, load = readback(result(partial=partial))
    assert checked.checked and checked.ok
    assert load.call_args.kwargs["batch_id"] == "catalog-1"


@pytest.mark.parametrize("field,value", [
    ("data_source", "unverified"), ("stock_code", "999999"),
    ("source_time", datetime(2026, 9, 11, 9, 55)),
    ("received_at", datetime(2026, 9, 11, 10, 0)),
    ("received_at", datetime(2026, 9, 11, 10, 2)),
    ("etl_sync_at", None),
])
def test_unproven_db_rows_fail(field, value):
    payload = result(partial=True)
    _, rows, _ = readback(payload)
    rows[0][field] = value
    assert not readback(payload, rows=rows)[0].ok


def test_duplicate_persisted_code_and_wrong_catalog_fail():
    payload = result(partial=True)
    _, rows, _ = readback(payload)
    rows[-1] = rows[0]
    assert not readback(payload, rows=rows)[0].ok
    catalog = SimpleNamespace(
        batch_id="catalog-1", manifest_hash="c" * 64,
        captured_at="2026-09-11 09:00:00",
    )
    assert not readback(payload, catalog=catalog)[0].ok


def test_dry_run_and_changed_minimum_cannot_use_existing_partition():
    assert not readback(result(), task={**TASK, "script_args": "--dry-run --json"})[0].ok
    assert not readback(result(), task={**TASK, "script_args": "--min-coverage 1 --json"})[0].ok


def test_explicit_extra_code_is_bound_to_actual_task_arguments():
    payload = {**result(partial=True), "extra_codes": 1, "expected_codes": 101,
               "selected_codes": 100, "written_rows": 100, "coverage": 100 / 101}
    _, rows, _ = readback(payload)
    rows.append({**rows[0], "stock_code": "600000"})
    task = {**TASK, "script_args": "--json --extra-code 600000.SH"}
    assert readback(payload, rows=rows, task=task)[0].ok
    assert not readback(payload, rows=rows)[0].ok


@pytest.fixture
def audit_engine():
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE st_scheduled_tasks (id INTEGER PRIMARY KEY, task_type TEXT, "
            "last_run_at DATETIME, last_triggered_at DATETIME, last_run_status TEXT, "
            "last_run_duration INTEGER, last_run_output TEXT, updated_at DATETIME)"
        ))
        conn.execute(text(
            "CREATE TABLE st_scheduled_task_history (id INTEGER PRIMARY KEY, run_uid TEXT, "
            "task_id INTEGER, task_type TEXT, run_at DATETIME, finished_at DATETIME, "
            "status TEXT, duration INTEGER, exit_code INTEGER, output TEXT)"
        ))
        conn.execute(text(
            "INSERT INTO st_scheduled_tasks VALUES "
            "(75,:kind,:claim,:claim,'running',19,'retained projection',NULL)"
        ), {"kind": TASK["task_type"], "claim": CLAIM})
    yield engine
    engine.dispose()


def history(engine, identity=2, *, status="skipped", kind=None, task_id=75, **changes):
    values = {
        "id": identity, "run_uid": UID if identity == 2 else str(identity) * 32,
        "task_id": task_id, "task_type": kind or TASK["task_type"],
        "run_at": CLAIM if identity == 2 else CLAIM - timedelta(minutes=identity),
        "finished_at": CLAIM + timedelta(seconds=1) if identity == 2 else CLAIM - timedelta(seconds=10),
        "status": status, "duration": 19, "exit_code": 0,
        "output": f"immutable {status} {identity}", **changes,
    }
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO st_scheduled_task_history "
            "(id,run_uid,task_id,task_type,run_at,finished_at,status,duration,exit_code,output) "
            "VALUES (:id,:run_uid,:task_id,:task_type,:run_at,:finished_at,:status,:duration,:exit_code,:output)"
        ), values)


def snapshot(engine):
    with engine.connect() as conn:
        return (
            dict(conn.execute(text("SELECT * FROM st_scheduled_tasks WHERE id=75")).mappings().one()),
            [dict(row) for row in conn.execute(text("SELECT * FROM st_scheduled_task_history ORDER BY id")).mappings()],
        )


@pytest.mark.parametrize("prior_status", ["success", "degraded", "failed", "timeout", "stopped", "blocked"])
def test_skip_restores_latest_real_projection_without_touching_history(audit_engine, prior_status):
    history(audit_engine, 1, status=prior_status)
    history(audit_engine)
    before, audits = snapshot(audit_engine)
    assert runtime._restore_collection_projection_after_skip(audit_engine, TASK, run_uid=UID)
    after, unchanged = snapshot(audit_engine)
    assert unchanged == audits
    assert after["last_triggered_at"] == before["last_triggered_at"]
    assert after["last_run_at"] == audits[0]["run_at"]
    assert after["last_run_status"] == prior_status
    assert after["last_run_duration"] == 19
    assert after["last_run_output"] == audits[0]["output"]


def test_no_real_prior_run_restores_empty_projection(audit_engine):
    history(audit_engine, 1)
    history(audit_engine)
    assert runtime._restore_collection_projection_after_skip(audit_engine, TASK, run_uid=UID)
    after, _ = snapshot(audit_engine)
    assert all(after[key] is None for key in (
        "last_run_at", "last_run_status", "last_run_duration", "last_run_output",
    ))
    assert after["last_triggered_at"] == str(CLAIM)


@pytest.mark.parametrize("changes", [
    {"status": "running"}, {"status": None}, {"finished_at": None},
    {"duration": None}, {"duration": -1},
    {"finished_at": CLAIM + timedelta(seconds=1)},
])
def test_incomplete_latest_real_history_is_not_skipped_over(audit_engine, changes):
    history(audit_engine, 1, **{"status": "success", **changes})
    history(audit_engine)
    before = snapshot(audit_engine)
    assert not runtime._restore_collection_projection_after_skip(audit_engine, TASK, run_uid=UID)
    assert snapshot(audit_engine) == before


@pytest.mark.parametrize("changes", [
    {"run_uid": "b" * 32}, {"status": "running"}, {"exit_code": 1},
    {"finished_at": None}, {"task_type": "other"},
])
def test_skip_restore_requires_exact_current_terminal_history(audit_engine, changes):
    history(audit_engine, 1, status="failed")
    history(audit_engine, **changes)
    before = snapshot(audit_engine)
    assert not runtime._restore_collection_projection_after_skip(audit_engine, TASK, run_uid=UID)
    assert snapshot(audit_engine) == before


def test_new_claim_even_with_same_second_timestamp_is_not_overwritten(audit_engine):
    history(audit_engine, 1, status="failed")
    history(audit_engine)
    history(audit_engine, 3, status="running", run_at=CLAIM, finished_at=None)
    before = snapshot(audit_engine)
    assert not runtime._restore_collection_projection_after_skip(audit_engine, TASK, run_uid=UID)
    assert snapshot(audit_engine) == before


def test_cas_retains_a_changed_claim(audit_engine):
    history(audit_engine, 1, status="success")
    history(audit_engine)
    statements = []

    def change_claim(conn, cursor, statement, parameters, context, many):
        if statement.startswith("UPDATE st_scheduled_tasks SET last_run_at="):
            statements.append(statement)
            cursor.execute("UPDATE st_scheduled_tasks SET last_triggered_at='2026-09-11 16:01:00' WHERE id=75")

    event.listen(audit_engine, "before_cursor_execute", change_claim)
    assert not runtime._restore_collection_projection_after_skip(audit_engine, TASK, run_uid=UID)
    after, _ = snapshot(audit_engine)
    assert after["last_run_status"] == "running"
    assert after["last_run_output"] == "retained projection"
    assert len(statements) == 1


def test_other_task_type_history_does_not_supply_projection(audit_engine):
    history(audit_engine, 1, status="success", kind="analysis_fast")
    history(audit_engine)
    assert runtime._restore_collection_projection_after_skip(audit_engine, TASK, run_uid=UID)
    assert snapshot(audit_engine)[0]["last_run_status"] is None


def test_confirmed_skip_is_terminal_for_owned_daemon_shutdown():
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE st_scheduled_task_history (task_id INTEGER, run_uid TEXT, "
            "scheduler_instance_id TEXT, status TEXT, finished_at DATETIME)"
        ))
        conn.execute(text(
            "INSERT INTO st_scheduled_task_history VALUES (75,:uid,:owner,'skipped',:finished)"
        ), {"uid": UID, "owner": runtime._scheduler_instance_id, "finished": CLAIM})
    try:
        with patch.object(runtime, "get_engine", return_value=engine):
            assert runtime._owned_shutdown_runs_are_terminal({75: UID})
            assert not runtime._owned_shutdown_runs_are_terminal({75: "b" * 32})
            with engine.begin() as conn:
                conn.execute(text("UPDATE st_scheduled_task_history SET finished_at=NULL"))
            assert not runtime._owned_shutdown_runs_are_terminal({75: UID})
        assert "skipped" not in runtime._PIPELINE_TERMINAL_STATUSES
    finally:
        engine.dispose()


@pytest.mark.parametrize("payload,status", [
    ({"status": "skipped", "reason": "outside_continuous_auction"}, "skipped"),
    (result(), "success"), (result(partial=True), "degraded"),
])
def test_run_task_records_disposition_with_replayable_evidence(payload, status):
    proc = MagicMock()
    proc.returncode = 0
    proc.communicate.return_value = (json.dumps(payload), "")
    row = {**TASK, "_history_started": True, "_history_run_uid": UID}
    with ExitStack() as stack:
        for name, value in {
            "_scheduler_build_commit_sha": "b" * 40,
            "resolve_scheduler_script": Path(__file__),
            "build_child_env": {},
            "_build_task_args": ["--json"],
            "_task_dispatch_date": "2026-09-11",
            "_task_history_finish": None,
            "_restore_collection_projection_after_skip": True,
            "update_scheduler_task": None,
            "validate_scheduler_task_result": validation.SchedulerValidationResult(True, True, "ok"),
        }.items():
            stack.enter_context(patch.object(runtime, name, return_value=value))
        stack.enter_context(patch.object(runtime.subprocess, "Popen", return_value=proc))
        finish = runtime._task_history_finish
        restore = runtime._restore_collection_projection_after_skip
        update = runtime.update_scheduler_task
        validate = runtime.validate_scheduler_task_result
        runtime._run_task(row, Path(__file__).parent.parent, MagicMock())
        assert finish.call_args.kwargs["status"] == status
        assert finish.call_args.kwargs["exit_code"] == 0
        if status == "skipped":
            restore.assert_called_once()
            assert all(call.args[2] == {"last_run_status": "running"} for call in update.call_args_list)
            validate.assert_not_called()
        else:
            restore.assert_not_called()
            validate.assert_called_once()
            assert update.call_args.args[2]["last_run_status"] == status
            evidence = runtime._history_validation_evidence(finish.call_args.kwargs["output"])
            assert evidence["status"] == status
            assert evidence["run_uid"] == UID
            assert json.loads(evidence["replay_output"]) == payload
