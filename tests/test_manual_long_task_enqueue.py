from __future__ import annotations

import inspect
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from server.api.routers import commentary, jq_minute, screener
from server.common import manual_scheduler_launch


def _accepted(task_type: str) -> dict:
    return {
        "accepted": True,
        "status": "running",
        "task_type": task_type,
        "task_id": 91,
        "job_id": "a" * 32,
    }


def test_commentary_run_only_enqueues_registered_task() -> None:
    profile = {"id": 7, "profile_name": "test"}
    engine = object()
    with patch.object(commentary, "get_engine", return_value=engine), patch.object(
        commentary, "_profile_row", return_value=profile
    ), patch.object(
        commentary,
        "launch_registered_scheduler_task",
        return_value=_accepted(commentary.TASK_TYPE),
    ) as launch, patch.object(commentary, "_run_profile_assessment") as execute, patch.object(
        commentary, "send_markdown"
    ) as send:
        result = commentary.run_commentary_profile(
            7,
            push=True,
            as_of_date="2026-08-25",
        )

    assert result["queued"] is True
    assert result["job_id"] == "a" * 32
    kwargs = launch.call_args.kwargs
    assert kwargs["task_type"] == commentary.TASK_TYPE
    assert kwargs["expected_script_path"] == commentary.SCRIPT_PATH
    assert kwargs["script_args"] == "--profile-id 7 --push --as-of-date 2026-08-25 --json"
    execute.assert_not_called()
    send.assert_not_called()


def test_commentary_task_ensure_is_gone() -> None:
    with pytest.raises(HTTPException) as captured:
        commentary.ensure_commentary_profile_task(7)
    assert captured.value.status_code == 410


def test_screener_run_only_enqueues_and_token_round_trips() -> None:
    request = screener.ScreenerRunRequest(
        preset="intraday_sector",
        as_of_date="2026-08-25",
        universe="market",
        top=25,
        filters={"exclude_st": True, "min_change": 1.5},
        notify=False,
    )
    engine = object()
    with patch.object(screener, "get_engine", return_value=engine), patch.object(
        screener,
        "launch_registered_scheduler_task",
        return_value=_accepted("screener_intraday_delivery"),
    ) as launch, patch.object(screener, "execute_screener_task") as execute:
        result = screener.screener_run(request)

    assert result["queued"] is True
    kwargs = launch.call_args.kwargs
    assert kwargs["task_type"] == "screener_intraday_delivery"
    assert kwargs["expected_script_path"] == "tools/run_screener_delivery.py"
    prefix, token, json_flag = kwargs["script_args"].split()
    assert prefix == "--request-token"
    assert json_flag == "--json"
    decoded = screener.decode_screener_task_request(token)
    assert decoded.preset == request.preset
    assert decoded.top == request.top
    assert decoded.filters == request.filters
    assert decoded.notify is False
    execute.assert_not_called()


@pytest.mark.parametrize(
    "filters",
    [
        {"unknown": 1},
        {"exclude_st": "yes"},
        {"min_change": float("nan")},
    ],
)
def test_screener_task_payload_rejects_unbounded_or_unknown_input(filters) -> None:
    with pytest.raises(HTTPException) as captured:
        screener.screener_run(
            screener.ScreenerRunRequest(preset="trend_breakout", filters=filters)
        )
    assert captured.value.status_code == 422


def test_jq_sync_only_enqueues_strict_registered_task() -> None:
    engine = object()
    with patch.object(jq_minute, "get_engine", return_value=engine), patch.object(
        jq_minute,
        "launch_registered_scheduler_task",
        return_value=_accepted(jq_minute.TASK_TYPE),
    ) as launch:
        result = jq_minute.sync_jq_minute_once(
            universe="latest-kline",
            codes="000001;600000",
            limit=2,
            count=3,
            batch_size=200,
            include_now=True,
            include_paused=False,
            include_bj=False,
            skip_closed=True,
            min_coverage=0.0,
            dry_run=False,
        )

    assert result["queued"] is True
    kwargs = launch.call_args.kwargs
    assert kwargs["task_type"] == jq_minute.TASK_TYPE
    assert kwargs["expected_script_path"] == jq_minute.SCRIPT_PATH
    assert "--codes 000001,600000" in kwargs["script_args"]
    assert "--skip-closed" in kwargs["script_args"]


def test_jq_sync_rejects_command_like_codes_before_launch() -> None:
    with patch.object(jq_minute, "launch_registered_scheduler_task") as launch:
        with pytest.raises(HTTPException) as captured:
            jq_minute.sync_jq_minute_once(
                universe="latest-kline",
                codes="000001;--json",
                limit=0,
                count=3,
                batch_size=200,
                include_now=True,
                include_paused=False,
                include_bj=False,
                skip_closed=True,
                min_coverage=0.0,
                dry_run=False,
            )
    assert captured.value.status_code == 422
    launch.assert_not_called()


def test_jq_table_ensure_is_production_gone() -> None:
    with pytest.raises(HTTPException) as captured:
        jq_minute.ensure_jq_minute_table()
    assert captured.value.status_code == 410


class _Rows:
    def __init__(self, rows):
        self.rows = list(rows)

    def mappings(self):
        return self

    def all(self):
        return list(self.rows)


class _RegistryConnection:
    def __init__(self, rows):
        self.rows = list(rows)
        self.statements: list[str] = []

    def execute(self, statement, params=None):
        self.statements.append(str(statement))
        return _Rows(self.rows)


class _Context:
    def __init__(self, value):
        self.value = value

    def __enter__(self):
        return self.value

    def __exit__(self, *_args):
        return False


class _RegistryEngine:
    def __init__(self, rows):
        self.connection = _RegistryConnection(rows)

    def connect(self):
        return _Context(self.connection)


def _registered_row(**overrides):
    row = {
        "id": 91,
        "task_name": "registered",
        "task_type": "fixed_task",
        "script_path": "tools/fixed_task.py",
        "script_args": "persisted",
        "date_param": "persisted-date",
        "enabled": 1,
    }
    row.update(overrides)
    return row


def test_manual_launcher_uses_unique_row_copy_and_one_audited_launch() -> None:
    persisted = _registered_row()
    engine = _RegistryEngine([persisted])
    launch_result = _accepted("fixed_task")
    with patch.object(
        manual_scheduler_launch,
        "validate_scheduler_launch_surface",
    ), patch(
        "server.api.scheduler_runtime.launch_scheduler_task",
        return_value=launch_result,
    ) as launch:
        result = manual_scheduler_launch.launch_registered_scheduler_task(
            engine,
            task_type="fixed_task",
            expected_script_path="tools/fixed_task.py",
            script_args="--safe 1",
            root=commentary._ROOT,
        )

    assert result["job_id"] == "a" * 32
    assert "LIMIT 2" in engine.connection.statements[0]
    launched = launch.call_args.args[0]
    assert launched["id"] == persisted["id"]
    assert launched["script_args"] == "--safe 1"
    assert launched["date_param"] == ""
    assert persisted["script_args"] == "persisted"
    launch.assert_called_once()


@pytest.mark.parametrize(
    ("rows", "status"),
    [
        ([], "task_registration_missing"),
        ([_registered_row(), _registered_row(id=92)], "task_registration_ambiguous"),
        ([_registered_row(script_path="tools/wrong.py")], "task_contract_mismatch"),
    ],
)
def test_manual_launcher_fails_closed_on_registration_drift(rows, status) -> None:
    engine = _RegistryEngine(rows)
    with patch.object(
        manual_scheduler_launch,
        "validate_scheduler_launch_surface",
    ), patch("server.api.scheduler_runtime.launch_scheduler_task") as launch:
        result = manual_scheduler_launch.launch_registered_scheduler_task(
            engine,
            task_type="fixed_task",
            expected_script_path="tools/fixed_task.py",
            script_args="--safe 1",
            root=commentary._ROOT,
        )

    assert result["accepted"] is False
    assert result["status"] == status
    assert result["job_id"] == ""
    launch.assert_not_called()


def test_api_entrypoints_have_no_synchronous_long_task_calls() -> None:
    commentary_source = inspect.getsource(commentary.run_commentary_profile)
    screener_source = inspect.getsource(screener.screener_run)
    jq_source = inspect.getsource(jq_minute.sync_jq_minute_once)
    assert "_run_profile_assessment(" not in commentary_source
    assert "execute_screener_task(" not in screener_source
    assert "sync_jq_minute_gml(" not in jq_source
    assert "launch_registered_scheduler_task(" in commentary_source
    assert "launch_registered_scheduler_task(" in screener_source
    assert "launch_registered_scheduler_task(" in jq_source


def test_screener_delivery_script_never_calls_api_route() -> None:
    from tools import run_screener_delivery

    source = inspect.getsource(run_screener_delivery)
    assert "screener_run" not in source
    assert "execute_screener_task" in source


class _SchedulerMetadataConnection:
    def __init__(self, *, missing_column: str = "", engine: str = "InnoDB"):
        self.missing_column = missing_column
        self.engine = engine
        self.statements: list[str] = []

    def execute(self, statement, params=None):
        sql = str(statement)
        self.statements.append(sql)
        normalized = " ".join(sql.lower().split())
        if "from information_schema.tables" in normalized:
            return _Rows([{"engine": self.engine, "table_collation": "utf8mb4_unicode_ci"}])
        if "from information_schema.columns" in normalized:
            specs = {
                "id": ("bigint", None, None, "auto_increment"),
                "task_name": ("varchar", 255, "utf8mb4", ""),
                "task_type": ("varchar", 50, "utf8mb4", ""),
                "script_path": ("varchar", 255, "utf8mb4", ""),
                "script_args": ("varchar", 500, "utf8mb4", ""),
                "date_param": ("varchar", 100, "utf8mb4", ""),
                "enabled": ("tinyint", None, None, ""),
            }
            return _Rows([
                {
                    "column_name": name,
                    "data_type": spec[0],
                    "column_type": spec[0],
                    "is_nullable": "NO",
                    "character_maximum_length": spec[1],
                    "character_set_name": spec[2],
                    "extra": spec[3],
                }
                for name, spec in specs.items()
                if name != self.missing_column
            ])
        if "from information_schema.statistics" in normalized:
            return _Rows([
                {
                    "index_name": "PRIMARY",
                    "non_unique": 0,
                    "seq_in_index": 1,
                    "column_name": "id",
                    "sub_part": None,
                    "index_type": "BTREE",
                }
            ])
        raise AssertionError(sql)


class _SchedulerMetadataEngine:
    def __init__(self, connection):
        self.connection = connection
        self.begin_calls = 0

    def connect(self):
        return _Context(self.connection)

    def begin(self):
        self.begin_calls += 1
        raise AssertionError("runtime validator must not open a write transaction")


def test_scheduler_launch_surface_is_select_only() -> None:
    connection = _SchedulerMetadataConnection()
    engine = _SchedulerMetadataEngine(connection)

    manual_scheduler_launch.validate_scheduler_launch_surface(engine)

    assert engine.begin_calls == 0
    assert len(connection.statements) == 3
    assert all(" ".join(sql.upper().split()).startswith("SELECT ") for sql in connection.statements)


@pytest.mark.parametrize(
    ("connection", "message"),
    [
        (_SchedulerMetadataConnection(missing_column="script_args"), "missing"),
        (_SchedulerMetadataConnection(engine="MyISAM"), "InnoDB"),
    ],
)
def test_scheduler_launch_surface_fails_closed_on_physical_drift(connection, message) -> None:
    engine = _SchedulerMetadataEngine(connection)
    with pytest.raises(RuntimeError, match=message):
        manual_scheduler_launch.validate_scheduler_launch_surface(engine)
    assert engine.begin_calls == 0
