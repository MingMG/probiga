from __future__ import annotations

from contextlib import contextmanager
from socket import gethostname

import pytest

from server.common.scheduler_runtime_health import (
    QMT_WINDOWS_EDGE_EXECUTION_PROOF_TASK_TYPES,
    QMT_WINDOWS_EDGE_TASK_TYPES,
    check_linux_standalone_active_release,
    check_linux_standalone_scheduler_heartbeat,
    check_qmt_windows_edge_executor,
)
from server.common.scheduler_runtime_schema import (
    EXPECTED_COLUMNS,
    migrate_scheduler_runtime_heartbeat,
    preflight_scheduler_runtime_heartbeat_schema,
)
from tools.qmt_host_ownership_contract import WINDOWS_QMT_EDGE_TASKS_BY_TYPE


BUILD_SHA = "a" * 40
PID = 4321
HOST = gethostname()


class _Rows:
    def __init__(self, rows):
        self._rows = list(rows)

    def mappings(self):
        return self

    def __iter__(self):
        return iter(self._rows)


class _HeartbeatConnection:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, statement, params=None):
        assert "executor_role=:executor_role" in str(statement)
        assert params == {"executor_role": "linux_standalone"}
        return _Rows(self.rows)


def _heartbeat_row(**updates):
    row = {
        "instance_id": f"{HOST}-{PID}",
        "mode": "standalone",
        "host_name": HOST,
        "pid": PID,
        "build_sha": BUILD_SHA,
        "executor_role": "linux_standalone",
        "started_at": "2026-08-25 09:00:00",
        "heartbeat_at": "2026-08-25 09:01:00",
        "heartbeat_age_seconds": 5,
        "poll_seconds": 60,
        "max_concurrent_tasks": 2,
    }
    row.update(updates)
    return row


def test_linux_standalone_heartbeat_accepts_one_exact_fresh_executor():
    passed, detail = check_linux_standalone_scheduler_heartbeat(
        _HeartbeatConnection([_heartbeat_row()]),
        expected_build_sha=BUILD_SHA,
        expected_pid=PID,
        expected_host=HOST,
    )

    assert passed is True
    assert detail["fresh_row_count"] == 1
    assert detail["future_row_count"] == 0
    assert detail["errors"] == []


def test_linux_active_release_derives_one_exact_fresh_executor_identity():
    passed, detail = check_linux_standalone_active_release(
        _HeartbeatConnection([_heartbeat_row()]),
        expected_build_sha=BUILD_SHA,
    )

    assert passed is True
    assert detail["fresh_row_count"] == 1
    assert detail["current"] == {
        "instance_id": f"{HOST}-{PID}",
        "mode": "standalone",
        "host_name": HOST,
        "pid": PID,
        "build_sha": BUILD_SHA,
        "executor_role": "linux_standalone",
        "started_at": "2026-08-25T09:00:00",
        "heartbeat_age_seconds": 5,
        "poll_seconds": 60,
        "max_concurrent_tasks": 2,
    }
    assert detail["errors"] == []

    passed, detail = check_linux_standalone_active_release(
        _HeartbeatConnection([_heartbeat_row(build_sha="b" * 40)]),
        expected_build_sha=BUILD_SHA,
    )
    assert passed is False
    assert "build_sha_mismatch" in detail["errors"]


@pytest.mark.parametrize(
    ("rows", "expected_error"),
    [
        ([_heartbeat_row(heartbeat_age_seconds=121)], "fresh_heartbeat_not_unique"),
        ([_heartbeat_row(heartbeat_age_seconds=-1)], "future_heartbeat_present"),
        (
            [_heartbeat_row(), _heartbeat_row(instance_id=f"{HOST}-9999", pid=9999)],
            "fresh_heartbeat_not_unique",
        ),
        ([_heartbeat_row(pid=9999)], "pid_mismatch"),
        ([_heartbeat_row(build_sha="b" * 40)], "build_sha_mismatch"),
    ],
)
def test_linux_standalone_heartbeat_fails_closed_for_identity_or_time_drift(
    rows,
    expected_error,
):
    passed, detail = check_linux_standalone_scheduler_heartbeat(
        _HeartbeatConnection(rows),
        expected_build_sha=BUILD_SHA,
        expected_pid=PID,
        expected_host=HOST,
    )

    assert passed is False
    assert expected_error in detail["errors"]


@pytest.mark.parametrize(
    ("malformed", "expected_error"),
    [
        ({"heartbeat_age_seconds": 1, "poll_seconds": None},
         "fresh_heartbeat_poll_invalid"),
        ({"heartbeat_age_seconds": "bad", "poll_seconds": 60},
         "heartbeat_age_invalid"),
        ({"heartbeat_age_seconds": -60, "poll_seconds": None},
         "future_heartbeat_present"),
        ({"heartbeat_age_seconds": 1, "poll_seconds": 3600},
         "poll_seconds_mismatch"),
        ({"heartbeat_age_seconds": 200, "poll_seconds": 600},
         "poll_seconds_mismatch"),
    ],
)
def test_linux_rejects_malformed_or_wrong_poll_duplicate(
    malformed,
    expected_error,
):
    passed, detail = check_linux_standalone_scheduler_heartbeat(
        _HeartbeatConnection([_heartbeat_row(), _heartbeat_row(**malformed)]),
        expected_build_sha=BUILD_SHA,
        expected_pid=PID,
        expected_host=HOST,
        expected_poll_seconds=60,
    )

    assert passed is False
    assert expected_error in detail["errors"]


EDGE_HOST = "win-qmt-edge-01"
EDGE_PID = 9191
EDGE_INSTANCE = f"{EDGE_HOST}-{EDGE_PID}"
EDGE_TASK_TYPES = QMT_WINDOWS_EDGE_TASK_TYPES
EDGE_PROOF_TASK_TYPES = QMT_WINDOWS_EDGE_EXECUTION_PROOF_TASK_TYPES


def _edge_runtime_row(**updates):
    row = {
        "instance_id": EDGE_INSTANCE,
        "mode": "standalone",
        "host_name": EDGE_HOST,
        "pid": EDGE_PID,
        "build_sha": BUILD_SHA,
        "executor_role": "qmt_windows_edge",
        "started_at": "2026-08-25 00:00:00",
        "heartbeat_at": "2026-08-25 10:00:00",
        "heartbeat_age_seconds": 5,
        "poll_seconds": 60,
        "max_concurrent_tasks": 2,
    }
    row.update(updates)
    return row


def _edge_task_rows():
    rows = []
    for task_id, task_type in enumerate(EDGE_TASK_TYPES, start=101):
        rows.append(
            {
                "id": task_id,
                **WINDOWS_QMT_EDGE_TASKS_BY_TYPE[task_type],
                "last_run_status": "success",
            }
        )
    return rows


def _edge_history_rows():
    task_ids = {
        row["task_type"]: row["id"] for row in _edge_task_rows()
    }
    return [
        {
            "id": index,
            "task_id": task_ids[task_type],
            "task_type": task_type,
            "status": "success",
            "run_at": "2026-08-25 00:00:00",
            "finished_at": "2026-08-25 01:00:00",
            "exit_code": 0,
            "host_name": EDGE_HOST,
            "scheduler_instance_id": EDGE_INSTANCE,
            "success_age_seconds": 3600,
        }
        for index, task_type in enumerate(EDGE_PROOF_TASK_TYPES, start=201)
    ]


class _EdgeConnection:
    def __init__(self, *, runtime=None, tasks=None, history=None):
        self.runtime = [_edge_runtime_row()] if runtime is None else runtime
        self.tasks = _edge_task_rows() if tasks is None else tasks
        self.history = _edge_history_rows() if history is None else history

    def execute(self, statement, params=None):
        sql = str(statement)
        if "FROM st_scheduler_runtime" in sql:
            assert params == {"executor_role": "qmt_windows_edge"}
            return _Rows(self.runtime)
        if "FROM st_scheduled_tasks" in sql:
            assert params == {
                f"task_type_{index}": task_type
                for index, task_type in enumerate(EDGE_TASK_TYPES)
            }
            return _Rows(self.tasks)
        if "FROM st_scheduled_task_history" in sql:
            assert params == {
                f"task_type_{index}": task_type
                for index, task_type in enumerate(EDGE_PROOF_TASK_TYPES)
            }
            return _Rows(self.history)
        raise AssertionError(sql)


def test_qmt_windows_edge_requires_live_identity_and_recent_successes():
    passed, detail = check_qmt_windows_edge_executor(
        _EdgeConnection(),
        expected_build_sha=BUILD_SHA,
    )

    assert passed is True
    assert detail["status"] == "AVAILABLE"
    assert detail["strategy_eligible"] is True
    assert detail["fresh_row_count"] == 1
    assert detail["task_count"] == 3
    assert detail["owned_task_count"] == len(EDGE_TASK_TYPES)
    assert detail["last_success_count"] == 3
    assert detail["required_task_types"] == list(EDGE_PROOF_TASK_TYPES)
    assert detail["owned_task_types"] == list(EDGE_TASK_TYPES)
    assert detail["ownership_contract_verified"] is True
    assert set(detail["owned_tasks"]) == set(EDGE_TASK_TYPES)
    assert detail["errors"] == []


@pytest.mark.parametrize(
    ("connection", "expected_error"),
    [
        (
            _EdgeConnection(runtime=[]),
            "fresh_heartbeat_not_unique",
        ),
        (
            _EdgeConnection(
                runtime=[_edge_runtime_row(build_sha="b" * 40)]
            ),
            "build_sha_mismatch",
        ),
        (
            _EdgeConnection(history=_edge_history_rows()[:-1]),
            "last_success_missing:qmt_reference_incremental",
        ),
        (
            _EdgeConnection(
                tasks=[
                    row for row in _edge_task_rows()
                    if row["task_type"] != "qmt_catalog_capability_refresh"
                ]
            ),
            "task_missing:qmt_catalog_capability_refresh",
        ),
        (
            _EdgeConnection(
                history=[
                    {
                        **row,
                        "success_age_seconds": (
                            400_000
                            if row["task_type"] == "qmt_local_history_2024"
                            else row["success_age_seconds"]
                        ),
                    }
                    for row in _edge_history_rows()
                ]
            ),
            "last_success_stale:qmt_local_history_2024",
        ),
        (
            _EdgeConnection(
                history=[
                    {
                        **row,
                        "host_name": (
                            "other-host"
                            if row["task_type"]
                            == "qmt_local_gap_repair_execute"
                            else row["host_name"]
                        ),
                    }
                    for row in _edge_history_rows()
                ]
            ),
            "last_success_host_mismatch:qmt_local_gap_repair_execute",
        ),
        (
            _EdgeConnection(
                tasks=[
                    {
                        **row,
                        "script_args": (
                            "--start-date 2026-01-01"
                            if row["task_type"] == "qmt_local_history_2024"
                            else row["script_args"]
                        ),
                    }
                    for row in _edge_task_rows()
                ]
            ),
            "task_contract_drift:qmt_local_history_2024",
        ),
    ],
)
def test_qmt_windows_edge_fails_closed_without_execution_proof(
    connection,
    expected_error,
):
    passed, detail = check_qmt_windows_edge_executor(
        connection,
        expected_build_sha=BUILD_SHA,
    )

    assert passed is False
    assert detail["status"] == "UNAVAILABLE"
    assert detail["strategy_eligible"] is False
    assert expected_error in detail["errors"]


def test_qmt_windows_edge_rejects_zero_or_malformed_release_identity():
    passed, detail = check_qmt_windows_edge_executor(
        _EdgeConnection(),
        expected_build_sha="0" * 40,
    )

    assert passed is False
    assert detail["errors"] == ["expected_build_sha_invalid"]


@pytest.mark.parametrize(
    ("malformed", "expected_error"),
    [
        ({"heartbeat_age_seconds": 1, "poll_seconds": None},
         "fresh_heartbeat_poll_invalid"),
        ({"heartbeat_age_seconds": "bad", "poll_seconds": 60},
         "heartbeat_age_invalid"),
        ({"heartbeat_age_seconds": -60, "poll_seconds": None},
         "future_heartbeat_present"),
        ({"heartbeat_age_seconds": 1, "poll_seconds": 3600},
         "poll_seconds_mismatch"),
        ({"heartbeat_age_seconds": 200, "poll_seconds": 600},
         "poll_seconds_mismatch"),
    ],
)
def test_qmt_edge_rejects_malformed_or_wrong_poll_duplicate(
    malformed,
    expected_error,
):
    passed, detail = check_qmt_windows_edge_executor(
        _EdgeConnection(
            runtime=[_edge_runtime_row(), _edge_runtime_row(**malformed)]
        ),
        expected_build_sha=BUILD_SHA,
        expected_poll_seconds=60,
    )

    assert passed is False
    assert detail["status"] == "UNAVAILABLE"
    assert expected_error in detail["errors"]


class _MigrationConnection:
    def __init__(self, columns):
        self.columns = dict(columns)
        self.statements: list[str] = []

    def execute(self, statement, params=None):
        sql = str(statement)
        self.statements.append(sql)
        if "information_schema.COLUMNS" in sql:
            return _Rows(
                {
                    "COLUMN_NAME": name,
                    "DATA_TYPE": spec["data_type"],
                    "CHARACTER_MAXIMUM_LENGTH": spec[
                        "character_maximum_length"
                    ],
                    "IS_NULLABLE": spec["is_nullable"],
                }
                for name, spec in self.columns.items()
            )
        if "ADD COLUMN build_sha" in sql:
            self.columns["build_sha"] = EXPECTED_COLUMNS["build_sha"]
        if "ADD COLUMN executor_role" in sql:
            self.columns["executor_role"] = EXPECTED_COLUMNS["executor_role"]
        return _Rows([])


class _MigrationEngine:
    def __init__(self, connection):
        self.connection = connection

    @contextmanager
    def begin(self):
        yield self.connection

    @contextmanager
    def connect(self):
        yield self.connection


def test_scheduler_runtime_migration_adds_both_legacy_missing_columns():
    connection = _MigrationConnection({})

    result = migrate_scheduler_runtime_heartbeat(_MigrationEngine(connection))

    assert result["status"] == "ok"
    assert result["added_columns"] == ["build_sha", "executor_role"]
    assert result["columns"] == EXPECTED_COLUMNS


def test_scheduler_runtime_migration_is_idempotent_after_privileged_success():
    connection = _MigrationConnection(EXPECTED_COLUMNS)

    result = migrate_scheduler_runtime_heartbeat(_MigrationEngine(connection))

    assert result["added_columns"] == []
    assert result["physical_contract_verified"] is True


def test_scheduler_runtime_preflight_is_read_only_for_legacy_schema():
    connection = _MigrationConnection({})

    result = preflight_scheduler_runtime_heartbeat_schema(
        _MigrationEngine(connection)
    )

    assert result["migration_required"] is True
    assert result["missing_columns"] == ["build_sha", "executor_role"]
    assert not any("CREATE TABLE" in sql for sql in connection.statements)
    assert not any("ALTER TABLE" in sql for sql in connection.statements)


def test_runtime_account_denial_prevents_heartbeat_schema_ddl():
    class _RuntimeDeniedConnection(_MigrationConnection):
        def execute(self, statement, params=None):
            if "ALTER TABLE" in str(statement):
                raise PermissionError("runtime ALTER denied")
            return super().execute(statement, params)

    connection = _RuntimeDeniedConnection({})

    with pytest.raises(PermissionError, match="ALTER denied"):
        migrate_scheduler_runtime_heartbeat(_MigrationEngine(connection))


def test_scheduler_runtime_migration_rejects_wrong_existing_contract():
    connection = _MigrationConnection(
        {
            "build_sha": {
                "data_type": "varchar",
                "character_maximum_length": 40,
                "is_nullable": "YES",
            },
            "executor_role": EXPECTED_COLUMNS["executor_role"],
        }
    )

    with pytest.raises(RuntimeError, match="differ from contract"):
        migrate_scheduler_runtime_heartbeat(_MigrationEngine(connection))
