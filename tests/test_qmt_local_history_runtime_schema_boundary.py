from __future__ import annotations

import inspect
import json
from types import SimpleNamespace

from integrations.qmt import local_history
from tools import backfill_guojin_qmt_local_history as backfill_tool


def _assert_no_persistent_ddl(function) -> None:
    source = inspect.getsource(function).upper()
    for keyword in ("CREATE TABLE", "ALTER TABLE", "DROP TABLE", "TRUNCATE TABLE"):
        assert keyword not in source


def test_local_history_runtime_paths_are_read_only_schema_guards():
    for function in (
        local_history.local_history_schema_snapshot,
        local_history.validate_local_history_tables,
        local_history.persist_daily_kline_capture,
        local_history.backfill_daily_kline_local,
        local_history.backfill_minute_local,
        backfill_tool._validate_target_daily_quarantine_table,
        backfill_tool._quarantine_invalid_target_rows_without_native,
    ):
        _assert_no_persistent_ddl(function)


def test_local_history_ddl_is_owned_by_explicit_privileged_migrations():
    local_migration = inspect.getsource(
        local_history.privileged_migrate_local_history_schema
    ).upper()
    quarantine_migration = inspect.getsource(
        backfill_tool._privileged_migrate_target_daily_quarantine_schema
    ).upper()

    assert "CREATE TABLE" in local_migration
    assert "CREATE TABLE" in quarantine_migration
    assert "PRIVILEGED" in local_migration
    assert "PRIVILEGED" in quarantine_migration


def test_privileged_local_migration_applies_only_supported_additive_upgrade(
    monkeypatch,
):
    class Engine:
        url = SimpleNamespace(database="probiga_qmt_history")

        def __init__(self):
            self.statements: list[str] = []

        def begin(self):
            return self

        def __enter__(self):
            return self

        def __exit__(self, _exc_type, _exc, _traceback):
            return False

        def execute(self, statement):
            self.statements.append(str(statement))
            return SimpleNamespace(rowcount=0)

    engine = Engine()
    additive_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        local_history,
        "inspect",
        lambda _engine: SimpleNamespace(has_table=lambda *_args, **_kwargs: True),
    )
    monkeypatch.setattr(
        local_history,
        "local_history_provenance_schema_snapshot",
        lambda *_args, **_kwargs: {
            "missing_columns": [local_history.LOCAL_KLINE_PROVENANCE_COLUMN],
            "errors": ["required columns are missing: pre_close_origin"],
        },
    )
    monkeypatch.setattr(
        local_history,
        "migrate_local_history_provenance_schema",
        lambda target, **kwargs: additive_calls.append(
            {"engine": target, **kwargs}
        ),
    )
    monkeypatch.setattr(
        local_history,
        "validate_local_history_tables",
        lambda *_args, **_kwargs: {"ready": True, "ddl_executed": False},
    )

    result = local_history.privileged_migrate_local_history_schema(engine)

    assert additive_calls == [
        {
            "engine": engine,
            "apply": True,
            "database": "probiga_qmt_history",
        }
    ]
    assert sum("CREATE TABLE IF NOT EXISTS" in sql for sql in engine.statements) == 3
    assert result["migration_boundary"] == "privileged_local_history_release"
    assert result["ddl_executed"] is True


def test_init_is_the_only_cli_path_that_invokes_privileged_migrations(
    monkeypatch,
    capsys,
):
    primary_engine = object()
    history_engine = SimpleNamespace(
        url=SimpleNamespace(database="probiga_qmt_history")
    )
    events: list[tuple[str, object]] = []
    monkeypatch.setattr(
        backfill_tool,
        "_source_engine",
        lambda: primary_engine,
    )
    monkeypatch.setattr(
        backfill_tool,
        "get_local_history_engine",
        lambda _url=None: history_engine,
    )
    monkeypatch.setattr(
        backfill_tool,
        "privileged_migrate_local_history_schema",
        lambda engine: events.append(("local", engine))
        or {"ready": True, "ddl_executed": True},
    )
    monkeypatch.setattr(
        backfill_tool,
        "_privileged_migrate_target_daily_quarantine_schema",
        lambda engine: events.append(("quarantine", engine))
        or {"ready": True, "ddl_executed": True},
    )
    monkeypatch.setattr(
        backfill_tool,
        "validate_local_history_tables",
        lambda _engine: (_ for _ in ()).throw(
            AssertionError("init must not enter the runtime-only validator path")
        ),
    )

    assert backfill_tool.main(["init", "--json"]) == 0

    assert events == [
        ("local", history_engine),
        ("quarantine", primary_engine),
    ]
    assert '"ddl_executed": true' in capsys.readouterr().out


def test_validate_schema_uses_only_runtime_readers_and_disposes(
    monkeypatch,
    capsys,
):
    events: list[tuple[str, object]] = []

    class Engine:
        def __init__(self, name: str):
            self.name = name

        def dispose(self):
            events.append(("dispose", self.name))

    primary_engine = Engine("primary")
    history_engine = Engine("history")
    monkeypatch.setattr(
        backfill_tool,
        "_windows_local_engines",
        lambda: (primary_engine, history_engine),
    )
    monkeypatch.setattr(
        backfill_tool,
        "validate_local_history_tables",
        lambda engine: events.append(("validate-history", engine.name))
        or {"ready": True, "ddl_executed": False},
    )
    monkeypatch.setattr(
        backfill_tool,
        "_validate_target_daily_quarantine_table",
        lambda engine: events.append(("validate-quarantine", engine.name))
        or {"ready": True, "ddl_executed": False},
    )
    monkeypatch.setattr(
        backfill_tool,
        "privileged_migrate_local_history_schema",
        lambda _engine: (_ for _ in ()).throw(
            AssertionError("runtime schema validation must not execute DDL")
        ),
    )
    monkeypatch.setattr(
        backfill_tool,
        "_privileged_migrate_target_daily_quarantine_schema",
        lambda _engine: (_ for _ in ()).throw(
            AssertionError("runtime schema validation must not execute DDL")
        ),
    )

    assert backfill_tool.main(
        ["validate-schema", "--windows-local-option-file", "--json"]
    ) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "ok"
    assert payload["mode"] == "validate-schema"
    assert payload["database_writes"] is False
    assert payload["local_history_schema"]["ready"] is True
    assert payload["target_quarantine_schema"]["ready"] is True
    assert events == [
        ("validate-history", "history"),
        ("validate-quarantine", "primary"),
        ("dispose", "primary"),
        ("dispose", "history"),
    ]


def test_init_rejects_fixed_runtime_identity_before_database_access(
    monkeypatch,
    capsys,
):
    monkeypatch.setattr(
        backfill_tool,
        "_windows_local_engines",
        lambda: (_ for _ in ()).throw(
            AssertionError("runtime engine must not open for privileged init")
        ),
    )

    try:
        backfill_tool.main(
            ["init", "--windows-local-option-file", "--json"]
        )
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("runtime-backed privileged init must be rejected")

    assert "read-only runtime identity" in capsys.readouterr().err
