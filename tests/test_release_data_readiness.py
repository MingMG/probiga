from __future__ import annotations

import json
from datetime import datetime, time, timedelta
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, event, text

from server.api import scheduler_runtime
from server.common.scheduler_validation import SchedulerValidationResult
from server.common import scheduler_validation
from server.common.qmt_attestation_contract import canonical_digest
from tools import ensure_quality_gate


BUILD_SHA = "a" * 40
NOW = datetime(2026, 8, 27, 0, 20, 0)


def _evidence(
    *,
    task: dict,
    run_uid: str,
    replay_output: str = "{}",
    now: datetime = NOW,
    closed_target: str = "2026-08-26",
    target_override: str | None = None,
) -> str:
    core = {
        "schema": ensure_quality_gate.RELEASE_VALIDATION_EVIDENCE_SCHEMA,
        "run_uid": run_uid,
        "task_id": int(task["id"]),
        "task_name": task["task_name"],
        "task_type": task["task_type"],
        "build_sha": BUILD_SHA,
        "status": "success",
        "exit_code": 0,
        "started_at": (now - timedelta(minutes=10)).isoformat(sep=" "),
        "validation_checked": True,
        "validation_ok": True,
        "validation_message": "exact persisted data verified",
        "machine_output_sha256": "b" * 64,
        "replay_output": replay_output,
        "replay_output_sha256": ensure_quality_gate._text_sha256(replay_output),
    }
    task_type = str(task["task_type"])
    target = target_override
    if target is None:
        if task_type in ensure_quality_gate.RELEASE_CATCHUP_CLOSED_TARGET_TASK_TYPES:
            target = closed_target
        elif task_type in ensure_quality_gate.RELEASE_CATCHUP_CURRENT_TARGET_TASK_TYPES:
            target = now.date().isoformat()
    if target is not None:
        core["release_target_date"] = target
    payload = {
        **core,
        "evidence_sha256": ensure_quality_gate._canonical_sha256(core),
    }
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _readiness_engine(
    *,
    now: datetime = NOW,
    closed_target: str = "2026-08-26",
    target_overrides: dict[str, str] | None = None,
):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    definitions = ensure_quality_gate._release_task_definitions()
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_scheduled_tasks (
                id INTEGER PRIMARY KEY,
                task_name TEXT NOT NULL,
                task_type TEXT NOT NULL,
                script_path TEXT NOT NULL,
                script_args TEXT NOT NULL,
                date_param TEXT NOT NULL,
                enabled INTEGER NOT NULL
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_scheduled_task_history (
                id INTEGER PRIMARY KEY,
                run_uid TEXT NOT NULL,
                task_id INTEGER NOT NULL,
                task_name TEXT NOT NULL,
                task_type TEXT NOT NULL,
                run_at DATETIME NOT NULL,
                finished_at DATETIME,
                status TEXT NOT NULL,
                exit_code INTEGER,
                output TEXT,
                build_sha TEXT,
                trigger_source TEXT
            )
        """))
        for sequence, task_type in enumerate(sorted(definitions), start=1):
            definition = definitions[task_type]
            task = {
                "id": sequence,
                "task_name": definition["task_name"],
                "task_type": task_type,
                "script_path": definition["script_path"],
                "script_args": definition["script_args"],
                "date_param": definition.get("date_param", ""),
                "enabled": definition["enabled"],
            }
            connection.execute(text("""
                INSERT INTO st_scheduled_tasks
                (id, task_name, task_type, script_path, script_args,
                 date_param, enabled)
                VALUES (:id, :task_name, :task_type, :script_path,
                        :script_args, :date_param, :enabled)
            """), task)
            run_uid = f"{sequence:032x}"
            connection.execute(text("""
                INSERT INTO st_scheduled_task_history
                (id, run_uid, task_id, task_name, task_type, run_at,
                 finished_at, status, exit_code, output, build_sha,
                 trigger_source)
                VALUES (:id, :run_uid, :task_id, :task_name, :task_type,
                        :run_at, :finished_at, 'success', 0, :output,
                        :build_sha, 'release_catchup')
            """), {
                "id": sequence,
                "run_uid": run_uid,
                "task_id": sequence,
                "task_name": task["task_name"],
                "task_type": task_type,
                "run_at": now - timedelta(minutes=10),
                "finished_at": now - timedelta(minutes=5),
                "output": _evidence(
                    task=task,
                    run_uid=run_uid,
                    now=now,
                    closed_target=closed_target,
                    target_override=(target_overrides or {}).get(task_type),
                ),
                "build_sha": BUILD_SHA,
            })
    return engine


def _validate_ready(
    monkeypatch,
    engine,
    *,
    now: datetime = NOW,
    closed_target: str = "2026-08-26",
    target_resolver=None,
):
    monkeypatch.setattr(
        ensure_quality_gate,
        "authoritative_closed_trade_date",
        target_resolver or (lambda *_args, **_kwargs: closed_target),
    )
    monkeypatch.setattr(
        ensure_quality_gate,
        "scheduler_output_status",
        lambda *_args, **_kwargs: "success",
    )
    monkeypatch.setattr(
        ensure_quality_gate,
        "validate_scheduler_task_result",
        lambda *_args, **_kwargs: SchedulerValidationResult(
            checked=True,
            ok=True,
            message="persisted exact data verified",
        ),
    )
    monkeypatch.setattr(
        ensure_quality_gate,
        "_validate_qmt_strategy_input_window",
        lambda *_args, **_kwargs: {
            "sessions": ["2026-08-20", "2026-08-21", "2026-08-24", "2026-08-25", "2026-08-26"],
            "session_count": 5,
        },
    )
    return ensure_quality_gate.validate_release_data_readiness(
        engine,
        BUILD_SHA,
        now,
    )


def test_release_readiness_requires_every_exact_build_receipt_and_post_validation(
    monkeypatch,
):
    engine = _readiness_engine()
    result = _validate_ready(monkeypatch, engine)
    assert result["status"] == "READY"
    assert result["build_sha"] == BUILD_SHA
    assert (
        "qmt_canonical_history_gap_repair"
        in ensure_quality_gate.RELEASE_DATA_READINESS_TASK_TYPES
    )
    assert (
        "linux_recent_data_gap_repair"
        in ensure_quality_gate.RELEASE_DATA_READINESS_TASK_TYPES
    )
    assert (
        "notice_eastmoney_historical_repair"
        in ensure_quality_gate.RELEASE_DATA_READINESS_TASK_TYPES
    )
    assert result["task_count"] == len(
        ensure_quality_gate.RELEASE_DATA_READINESS_TASK_TYPES
    )
    assert set(result["tasks"]) == set(
        ensure_quality_gate.RELEASE_DATA_READINESS_TASK_TYPES
    )
    assert result["qmt_strategy_input_window"]["session_count"] == 5
    assert result["phase"] == "post_activation_data_readiness"


def test_release_readiness_rejects_mixed_dates_after_closed_session_rollover(
    monkeypatch,
):
    after_rollover = datetime(2026, 8, 27, 18, 1)
    engine = _readiness_engine(
        now=after_rollover,
        closed_target="2026-08-27",
        target_overrides={"analysis_fast": "2026-08-26"},
    )

    with pytest.raises(RuntimeError, match="analysis_fast.*target differs"):
        _validate_ready(
            monkeypatch,
            engine,
            now=after_rollover,
            closed_target="2026-08-27",
        )


@pytest.mark.parametrize(
    ("task_type", "cutoff"),
    (
        ("etf_forward_daily", time(15, 10)),
        ("sector_heat_east", time(15, 10)),
        ("alist_daily", time(16, 30)),
        ("alist_info", time(16, 30)),
        ("eastmoney_concept_current", time(18, 0)),
        ("eastmoney_concept_kline", time(18, 0)),
        ("eastmoney_concept_minute", time(18, 0)),
        ("eastmoney_concept_flow_snapshot", time(18, 0)),
    ),
)
def test_release_scheduler_uses_each_provider_authoritative_rollover(
    monkeypatch,
    task_type,
    cutoff,
):
    observed = []

    def resolve(_engine, *, now, close_ready_time):
        observed.append(close_ready_time)
        return (
            "2026-08-27"
            if now.time() >= close_ready_time
            else "2026-08-26"
        )

    monkeypatch.setattr(
        scheduler_runtime,
        "authoritative_closed_trade_date",
        resolve,
    )
    before = datetime.combine(NOW.date(), cutoff) - timedelta(minutes=1)
    at_cutoff = datetime.combine(NOW.date(), cutoff)

    assert scheduler_runtime._release_catchup_closed_target_date(
        object(),
        task_type=task_type,
        now=before,
    ) == "2026-08-26"
    assert scheduler_runtime._release_catchup_closed_target_date(
        object(),
        task_type=task_type,
        now=at_cutoff,
    ) == "2026-08-27"
    assert observed == [cutoff, cutoff]


def test_release_readiness_recomputes_provider_specific_targets(monkeypatch):
    now = datetime(2026, 8, 27, 16, 31)
    target_overrides = {
        "etf_forward_daily": "2026-08-27",
        "sector_heat_east": "2026-08-27",
        "alist_daily": "2026-08-27",
        "alist_info": "2026-08-27",
    }
    engine = _readiness_engine(
        now=now,
        target_overrides=target_overrides,
    )

    def resolve(_engine, *, now, close_ready_time):
        return (
            "2026-08-27"
            if now.time() >= close_ready_time
            else "2026-08-26"
        )

    result = _validate_ready(
        monkeypatch,
        engine,
        now=now,
        target_resolver=resolve,
    )

    assert result["tasks"]["alist_daily"]["target_date"] == "2026-08-27"
    assert (
        result["tasks"]["eastmoney_concept_current"]["target_date"]
        == "2026-08-26"
    )


def test_release_readiness_waits_for_notice_history_completion_task():
    definitions = ensure_quality_gate._release_task_definitions()

    assert "notice_eastmoney_historical_repair" in definitions
    assert (
        ensure_quality_gate.RELEASE_DATA_READINESS_MAX_AGE_BY_TASK[
            "notice_eastmoney_historical_repair"
        ]
        == timedelta(minutes=45)
    )


@pytest.mark.parametrize(
    ("assignment", "message"),
    [
        ("status='blocked', exit_code=2", "did not succeed"),
        ("build_sha='" + "c" * 40 + "'", "did not succeed"),
        (
            "finished_at='2026-08-20 00:00:00'",
            "latest terminal run is stale",
        ),
        (
            "run_at='2026-08-20 00:00:00'",
            "latest terminal run is stale",
        ),
        ("output='truncated diagnostics only'", "validation evidence envelope"),
    ],
)
def test_release_readiness_rejects_blocked_old_build_stale_or_truncated_history(
    monkeypatch,
    assignment,
    message,
):
    engine = _readiness_engine()
    with engine.begin() as connection:
        connection.execute(text(
            "UPDATE st_scheduled_task_history SET "
            + assignment
            + " WHERE task_type='qmt_stock_daily_canonical'"
        ))
    with pytest.raises(RuntimeError, match=message):
        _validate_ready(monkeypatch, engine)


def test_release_readiness_rejects_installed_task_without_terminal_run(monkeypatch):
    engine = _readiness_engine()
    with engine.begin() as connection:
        connection.execute(text(
            "DELETE FROM st_scheduled_task_history "
            "WHERE task_type='qmt_stock_daily_canonical'"
        ))
    with pytest.raises(RuntimeError, match="has no terminal history row"):
        _validate_ready(monkeypatch, engine)


def test_release_readiness_rejects_scheduler_date_argument_drift(monkeypatch):
    engine = _readiness_engine()
    with engine.begin() as connection:
        connection.execute(text(
            "UPDATE st_scheduled_tasks SET date_param='unexpected' "
            "WHERE task_type='qmt_stock_daily_canonical'"
        ))
    with pytest.raises(RuntimeError, match="drifted fields: date_param"):
        _validate_ready(monkeypatch, engine)


def test_release_readiness_database_path_is_select_only(monkeypatch):
    engine = _readiness_engine()
    statements: list[str] = []

    @event.listens_for(engine, "before_cursor_execute")
    def capture_statement(
        _connection,
        _cursor,
        statement,
        _parameters,
        _context,
        _executemany,
    ):
        statements.append(str(statement))

    _validate_ready(monkeypatch, engine)

    assert statements
    assert all(
        statement.lstrip().upper().startswith("SELECT")
        for statement in statements
    )


def test_release_readiness_rejects_unchecked_post_validation(monkeypatch):
    engine = _readiness_engine()
    monkeypatch.setattr(
        ensure_quality_gate,
        "authoritative_closed_trade_date",
        lambda *_args, **_kwargs: "2026-08-26",
    )
    monkeypatch.setattr(
        ensure_quality_gate,
        "scheduler_output_status",
        lambda *_args, **_kwargs: "success",
    )
    monkeypatch.setattr(
        ensure_quality_gate,
        "validate_scheduler_task_result",
        lambda *_args, **_kwargs: SchedulerValidationResult(
            checked=False,
            ok=True,
            message="no data validation configured",
        ),
    )
    monkeypatch.setattr(
        ensure_quality_gate,
        "_validate_qmt_strategy_input_window",
        lambda *_args, **_kwargs: {},
    )
    with pytest.raises(RuntimeError, match="persisted data is not verified"):
        ensure_quality_gate.validate_release_data_readiness(
            engine,
            BUILD_SHA,
            NOW,
        )


def test_qmt_strategy_window_checks_both_datasets_for_last_five_closed_sessions(
    monkeypatch,
):
    sessions = [
        "2026-08-18",
        "2026-08-19",
        "2026-08-20",
        "2026-08-21",
        "2026-08-24",
        "2026-08-25",
        "2026-08-26",
        "2026-08-27",
    ]
    calendar = MagicMock()
    calendar.sessions_between.return_value = sessions
    connection = MagicMock()
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = connection
    calls: list[tuple[str, str]] = []

    def load_bundle(_connection, *, dataset, trade_date):
        calls.append((dataset, trade_date))
        return {
            "manifest": {
                "manifest_hash": ensure_quality_gate._canonical_sha256(
                    [dataset, trade_date]
                ),
                "status": "EXACT",
                "strategy_eligible": True,
                "provider": "gj_big_qmt_inner",
            },
            "entities": [],
        }

    monkeypatch.setattr(
        "server.common.qmt_trade_calendar.load_trade_calendar_receipt",
        lambda *_args, **_kwargs: calendar,
    )
    def load_daily_truth(_connection, *, start_date, end_date, **_kwargs):
        assert start_date == end_date
        daily_truth = MagicMock()
        daily_truth.requested_sessions = (start_date,)
        daily_truth.attested_row_count = 5_200
        daily_truth.truth_hash = "d" * 64
        return daily_truth

    monkeypatch.setattr(
        "server.common.qmt_daily_market_truth.load_qmt_daily_market_truth",
        load_daily_truth,
    )
    monkeypatch.setattr(
        "server.common.kline_data.get_kline_engine",
        lambda: engine,
    )
    monkeypatch.setattr(
        ensure_quality_gate,
        "_load_qmt_coverage_bundle",
        load_bundle,
    )
    monkeypatch.setattr(
        "server.common.qmt_history_coverage.require_exact_coverage",
        lambda bundle: bundle["manifest"],
    )
    monkeypatch.setattr(
        "server.common.qmt_history_coverage.validate_coverage_authority",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        ensure_quality_gate,
        "_validate_qmt_canonical_strategy_partitions",
        lambda _bundles: None,
    )
    monkeypatch.setattr(
        ensure_quality_gate,
        "authoritative_closed_trade_date",
        lambda *_args, **_kwargs: "2026-08-26",
    )
    proof = ensure_quality_gate._validate_qmt_strategy_input_window(
        engine,
        now=datetime(2026, 8, 27, 16, 0, 0),
    )
    expected_sessions = [
        "2026-08-20",
        "2026-08-21",
        "2026-08-24",
        "2026-08-25",
        "2026-08-26",
    ]
    assert proof["sessions"] == expected_sessions
    assert calls == [
        ("stock_minute", trade_date) for trade_date in expected_sessions
    ]


def test_qmt_strategy_window_does_not_hide_a_middle_partition_gap(monkeypatch):
    calendar = MagicMock()
    calendar.sessions_between.return_value = [
        "2026-08-20",
        "2026-08-21",
        "2026-08-24",
        "2026-08-25",
        "2026-08-26",
    ]
    connection = MagicMock()
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = connection
    monkeypatch.setattr(
        "server.common.qmt_trade_calendar.load_trade_calendar_receipt",
        lambda *_args, **_kwargs: calendar,
    )
    def load_daily_truth(_connection, *, start_date, end_date, **_kwargs):
        assert start_date == end_date
        daily_truth = MagicMock()
        daily_truth.requested_sessions = (start_date,)
        daily_truth.attested_row_count = 5_200
        daily_truth.truth_hash = "d" * 64
        return daily_truth

    monkeypatch.setattr(
        "server.common.qmt_daily_market_truth.load_qmt_daily_market_truth",
        load_daily_truth,
    )
    monkeypatch.setattr(
        "server.common.kline_data.get_kline_engine",
        lambda: engine,
    )

    def missing_middle(_connection, *, dataset, trade_date):
        if dataset == "stock_minute" and trade_date == "2026-08-24":
            raise RuntimeError("QMT stock_minute strategy window is missing 2026-08-24")
        return {
            "manifest": {
                "status": "EXACT",
                "strategy_eligible": True,
                "provider": "gj_big_qmt_inner",
            },
            "entities": [],
        }

    monkeypatch.setattr(
        ensure_quality_gate,
        "_load_qmt_coverage_bundle",
        missing_middle,
    )
    monkeypatch.setattr(
        "server.common.qmt_history_coverage.require_exact_coverage",
        lambda bundle: bundle["manifest"],
    )
    monkeypatch.setattr(
        "server.common.qmt_history_coverage.validate_coverage_authority",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        ensure_quality_gate,
        "authoritative_closed_trade_date",
        lambda *_args, **_kwargs: "2026-08-26",
    )
    with pytest.raises(RuntimeError, match="missing 2026-08-24"):
        ensure_quality_gate._validate_qmt_strategy_input_window(
            engine,
            now=datetime(2026, 8, 27, 0, 20, 0),
        )


def test_scheduler_history_evidence_is_bounded_hash_bound_and_replayable(
    monkeypatch,
):
    receipt = json.dumps(
        {"schema": "probiga.example-result.v1", "status": "PASS"},
        sort_keys=True,
    )
    machine_output = (
        "diagnostic " + "x" * 100_000 + "\n"
        + receipt
        + "\nDATE=2026-08-26\npassword=should-not-persist"
    )
    monkeypatch.setattr(
        scheduler_runtime,
        "_scheduler_build_commit_sha",
        lambda: BUILD_SHA,
    )
    row = {"id": 7, "task_name": "example", "task_type": "example"}
    encoded = scheduler_runtime._build_history_validation_evidence(
        row,
        run_uid="7" * 32,
        machine_output=machine_output,
        status="success",
        exit_code=0,
        started_at=NOW,
        validation_message="verified",
    )
    evidence = ensure_quality_gate._extract_release_validation_evidence(encoded)
    assert receipt.replace(": ", ":").replace(", ", ",") in evidence["replay_output"]
    assert "DATE=2026-08-26" in evidence["replay_output"]
    assert "should-not-persist" not in encoded
    assert len(encoded.encode("utf-8")) < scheduler_runtime._HISTORY_EVIDENCE_LIMIT


def test_scheduler_history_evidence_binds_exact_release_target_date(monkeypatch):
    monkeypatch.setattr(
        scheduler_runtime,
        "_scheduler_build_commit_sha",
        lambda: BUILD_SHA,
    )
    row = {
        "id": 8,
        "task_name": "release analysis",
        "task_type": "analysis_fast",
        "_trigger_source": "release_catchup",
        "_release_target_date": "2026-08-26",
    }
    encoded = scheduler_runtime._build_history_validation_evidence(
        row,
        run_uid="8" * 32,
        machine_output="",
        status="success",
        exit_code=0,
        started_at=NOW,
        validation_message="stock_analysis_result date=2026-08-26",
    )
    evidence = ensure_quality_gate._extract_release_validation_evidence(encoded)

    assert evidence["release_target_date"] == "2026-08-26"


def test_qmt_strategy_window_fails_closed_without_authoritative_session(
    monkeypatch,
):
    monkeypatch.setattr(
        ensure_quality_gate,
        "authoritative_closed_trade_date",
        lambda *_args, **_kwargs: "",
    )
    with pytest.raises(
        RuntimeError,
        match="authoritative closed strategy-input session is unavailable",
    ):
        ensure_quality_gate._validate_qmt_strategy_input_window(
            MagicMock(),
            now=datetime(2026, 8, 27, 3, 5),
        )


def test_scheduler_history_evidence_rejects_oversized_individual_receipt():
    oversized = json.dumps(
        {"schema": "probiga.oversized-result.v1", "payload": "x" * 25_000}
    )
    with pytest.raises(RuntimeError, match="exceeds bounded history evidence"):
        scheduler_runtime._history_validation_replay_output(oversized)


def test_concept_flow_is_a_checked_persisted_release_surface():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    codes = [f"BK{index:03d}" for index in range(100)]
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE sm_concept_capital_flow_east (
                index_code TEXT NOT NULL,
                snapshot_at DATETIME NOT NULL
            )
        """))
        connection.execute(
            text("""
                INSERT INTO sm_concept_capital_flow_east
                (index_code, snapshot_at) VALUES (:code, :snapshot_at)
            """),
            [
                {"code": code, "snapshot_at": "2026-08-26 00:00:00"}
                for code in codes
            ],
        )
    manifest = {
        "schema": "probiga.eastmoney-concept-flow-manifest.v1",
        "provider": "eastmoney.datacenter-web",
        "report_name": "RPT_CONCEPT_FUNDFLOW",
        "source_date": "2026-08-26",
        "row_count": 100,
        "verified_row_count": 100,
        "code_count": 100,
        "code_set_hash": canonical_digest(codes),
        "provider_code_set_hash": "4" * 64,
        "strict_authority": True,
        "captured_at": "2026-08-26 19:30:00",
        "provider_page_count": 1,
        "provider_reported_row_count": 100,
        "provider_pagination_complete": True,
        "calendar_batch_id": "calendar-1",
        "calendar_manifest_hash": "2" * 64,
        "calendar_session_set_hash": "3" * 64,
    }
    payload = {
        "schema": "probiga.eastmoney-concept-flow-result.v1",
        "status": "COMPLETE",
        "source_date": "2026-08-26",
        "provider": "eastmoney.datacenter-web",
        "strict_authority": True,
        "written_rows": 100,
        "db_verified_rows": 100,
        "manifest": manifest,
        "manifest_hash": canonical_digest(manifest),
    }
    result = scheduler_validation.validate_scheduler_task_result(
        {"task_type": "eastmoney_concept_flow_snapshot"},
        engine=engine,
        started_at=datetime(2026, 8, 26, 19, 29, 0),
        now=datetime(2026, 8, 26, 19, 31, 0),
        output=json.dumps(payload),
    )
    assert result.checked is True
    assert result.ok is True
    with engine.begin() as connection:
        connection.execute(text(
            "DELETE FROM sm_concept_capital_flow_east WHERE index_code='BK099'"
        ))
    rejected = scheduler_validation.validate_scheduler_task_result(
        {"task_type": "eastmoney_concept_flow_snapshot"},
        engine=engine,
        started_at=datetime(2026, 8, 26, 19, 29, 0),
        now=datetime(2026, 8, 26, 19, 31, 0),
        output=json.dumps(payload),
    )
    assert rejected.checked is True
    assert rejected.ok is False
    assert "persisted concept-flow snapshot differs" in rejected.message


def test_successful_checked_scheduler_run_persists_replay_envelope(
    monkeypatch,
    tmp_path,
):
    script = tmp_path / "tools" / "job.py"
    script.parent.mkdir(parents=True)
    script.write_text("print('unused')", encoding="utf-8")
    receipt = json.dumps(
        {"schema": "probiga.example-result.v1", "status": "PASS"}
    )
    process = MagicMock()
    process.communicate.return_value = (receipt, "")
    process.returncode = 0
    history_finish = MagicMock()
    monkeypatch.setattr(
        scheduler_runtime,
        "_task_history_start",
        lambda *_args, **_kwargs: "9" * 32,
    )
    monkeypatch.setattr(scheduler_runtime, "_task_history_finish", history_finish)
    monkeypatch.setattr(
        scheduler_runtime,
        "resolve_scheduler_script",
        lambda *_args, **_kwargs: script,
    )
    monkeypatch.setattr(
        scheduler_runtime,
        "build_child_env",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        scheduler_runtime,
        "_build_task_args",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(scheduler_runtime.subprocess, "Popen", lambda *_args, **_kwargs: process)
    monkeypatch.setattr(scheduler_runtime, "update_scheduler_task", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        scheduler_runtime,
        "scheduler_output_status",
        lambda *_args, **_kwargs: "success",
    )
    monkeypatch.setattr(
        scheduler_runtime,
        "validate_scheduler_task_result",
        lambda *_args, **_kwargs: SchedulerValidationResult(
            checked=True,
            ok=True,
            message="persisted exact data verified",
        ),
    )
    monkeypatch.setattr(
        scheduler_runtime,
        "_scheduler_build_commit_sha",
        lambda: BUILD_SHA,
    )
    row = {
        "id": 91,
        "task_name": "checked task",
        "task_type": "stock_kline",
        "script_path": "tools/job.py",
        "script_args": "",
        "date_param": "",
        "interval_minutes": 0,
    }
    scheduler_runtime._run_task(row, tmp_path, MagicMock())
    assert history_finish.call_args.kwargs["status"] == "success"
    evidence = ensure_quality_gate._extract_release_validation_evidence(
        history_finish.call_args.kwargs["output"]
    )
    assert evidence["run_uid"] == "9" * 32
    assert evidence["task_type"] == "stock_kline"
    assert evidence["replay_output"]
