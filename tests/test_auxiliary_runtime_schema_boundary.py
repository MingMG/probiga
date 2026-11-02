from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, event, text

from integrations.bigqmt import membership_snapshot
from server.common import auxiliary_runtime_schema as schema


ROOT = Path(__file__).resolve().parents[1]


class _TriggerRows:
    def __init__(self, rows) -> None:
        self._rows = list(rows)

    def mappings(self):
        return self

    def all(self):
        return list(self._rows)


class _TriggerConnection:
    def __init__(self, rows) -> None:
        self.rows = list(rows)
        self.statements: list[str] = []

    def execute(self, statement, _params=None):
        sql = str(statement)
        self.statements.append(sql)
        if "information_schema.TRIGGERS" in sql:
            return _TriggerRows(self.rows)
        return _TriggerRows([])


class _ConnectionContext:
    def __init__(self, connection) -> None:
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, *_args):
        return False


class _TriggerEngine:
    def __init__(self, connection, *, dialect: str = "mysql") -> None:
        self.connection = connection
        self.dialect = SimpleNamespace(name=dialect)

    def connect(self):
        return _ConnectionContext(self.connection)

    def begin(self):
        return _ConnectionContext(self.connection)


def _exact_membership_trigger_rows() -> list[dict[str, str]]:
    return [
        {
            "TRIGGER_NAME": name,
            "EVENT_OBJECT_TABLE": table_name,
            "EVENT_MANIPULATION": event_name,
            "ACTION_TIMING": "BEFORE",
            "ACTION_ORIENTATION": "ROW",
            "ACTION_STATEMENT": body,
        }
        for name, (table_name, event_name, body) in (
            schema._expected_qmt_membership_trigger_contracts().items()
        )
    ]


def _create_surface(engine) -> None:
    surfaces = {
        "sm_market_overview_daily": schema.MARKET_OVERVIEW_REQUIRED_COLUMNS,
        "st_hot_stats": schema.HOT_STATS_REQUIRED_COLUMNS,
        "st_qmt_realtime_sync_receipt_v2": (
            schema.QMT_REALTIME_SYNC_RECEIPT_REQUIRED_COLUMNS
        ),
        "si_all_index_code": schema.SI_ALL_INDEX_CODE_REQUIRED_COLUMNS,
        **schema.HOT_RANK_FUSION_REQUIRED_COLUMNS,
        **schema.QMT_MEMBERSHIP_REQUIRED_COLUMNS,
        **(schema.QMT_DATADIR_IMPORT_REQUIRED_COLUMNS),
    }
    with engine.begin() as connection:
        for table_name, columns in surfaces.items():
            definitions = ", ".join(f'"{column}" TEXT' for column in sorted(columns))
            connection.execute(text(f'CREATE TABLE "{table_name}" ({definitions})'))


def test_auxiliary_runtime_validators_are_metadata_reads_only() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    _create_surface(engine)
    with engine.begin() as connection:
        connection.execute(text(
            f"CREATE INDEX {schema.QMT_DATADIR_STAGE_INDEX_NAME} ON "
            "qmt_datadir_minute_stage(run_id,trade_date,stock_code,trade_time)"
        ))
    statements: list[str] = []

    @event.listens_for(engine, "before_cursor_execute")
    def _capture(_conn, _cursor, statement, _parameters, _context, _many):
        statements.append(str(statement))

    result = schema.validate_auxiliary_runtime_schema(engine)

    assert result["read_only"] is True
    membership = result["qmt_membership_snapshot"]
    assert membership["append_only_verified"] is False
    assert membership["immutability_validation_skipped"] == "sqlite"
    assert set(membership["trigger_names"]) == set(
        schema.QMT_MEMBERSHIP_IMMUTABILITY_TRIGGER_NAMES
    )
    assert statements
    normalized = "\n".join(statements).upper()
    assert "CREATE TABLE" not in normalized
    assert "ALTER TABLE" not in normalized
    assert "DROP TABLE" not in normalized
    assert "TRUNCATE TABLE" not in normalized
    assert "INSERT INTO" not in normalized
    assert "UPDATE " not in normalized
    assert "DELETE FROM" not in normalized


def test_auxiliary_runtime_validator_fails_closed_when_table_is_missing() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with pytest.raises(RuntimeError, match="missing_tables"):
        schema.validate_hot_stats_runtime_schema(engine)


def test_qmt_stage_index_migration_preserves_checkpoint_and_is_idempotent(monkeypatch) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _create_surface(engine)
    with engine.begin() as connection:
        connection.execute(text("CREATE INDEX idx_qdms_run_date ON qmt_datadir_minute_stage(run_id,trade_date)"))
        connection.execute(text("INSERT INTO qmt_datadir_minute_stage(run_id,trade_date,stock_code,trade_time,price) VALUES ('retained-run','2026-11-02','600000','2026-11-02 09:30:00','10')"))
    # Exercise the existing migration on actual old tables; SQLite's table DDL
    # uses the same required surface while the production index path stays real.
    monkeypatch.setattr(schema, "_QMT_DATADIR_IMPORT_DDL", tuple(
        f"CREATE TABLE IF NOT EXISTS {table_name} (" + ",".join(f'\"{name}\" TEXT' for name in sorted(columns)) + ")"
        for table_name, columns in schema.QMT_DATADIR_IMPORT_REQUIRED_COLUMNS.items()
    ))
    with pytest.raises(RuntimeError, match="index is not prepared"):
        schema.validate_qmt_datadir_import_runtime_schema(engine)
    statements = []

    @event.listens_for(engine, "before_cursor_execute")
    def capture(_conn, _cursor, statement, _params, _context, _many):
        statements.append(statement)

    first = schema.privileged_migrate_qmt_datadir_import_schema(engine)
    second = schema.privileged_migrate_qmt_datadir_import_schema(engine)
    assert first["minute_stage_index_verified"] is second["stage_rows_preserved"] is True
    assert sum(statement.startswith("CREATE INDEX") for statement in statements) == 1
    assert not any(word in statement.upper() for statement in statements for word in ("TRUNCATE", "DELETE FROM", "DROP "))
    with engine.connect() as connection:
        assert connection.execute(text("SELECT run_id,price FROM qmt_datadir_minute_stage")).one() == ("retained-run", "10")
        assert connection.execute(text("SELECT COUNT(*) FROM sqlite_master WHERE name='idx_qdms_run_date'")).scalar() == 1
    engine.dispose()


@pytest.mark.parametrize("unique,columns,suffix", [
    ("", "run_id,stock_code,trade_date,trade_time", ""),
    ("", "run_id,trade_date,stock_code", ""),
    ("UNIQUE ", "run_id,trade_date,stock_code,trade_time", ""),
    ("", "run_id,trade_date,stock_code,trade_time", " WHERE price IS NOT NULL"),
])
def test_qmt_stage_index_same_name_wrong_shape_is_not_rebuilt(unique, columns, suffix) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _create_surface(engine)
    with engine.begin() as connection:
        connection.execute(text(f"CREATE {unique}INDEX {schema.QMT_DATADIR_STAGE_INDEX_NAME} ON qmt_datadir_minute_stage({columns}){suffix}"))
    with pytest.raises(RuntimeError, match="physical contract differs"):
        schema.validate_qmt_datadir_import_runtime_schema(engine)
    engine.dispose()


@pytest.mark.parametrize("mutation", ["order", "unique", "prefix", "type"])
def test_qmt_stage_mysql_index_requires_exact_physical_shape(mutation) -> None:
    rows = [dict(column_name=column, position=i, non_unique=1, index_type="BTREE", sub_part=None)
            for i, column in enumerate(schema.QMT_DATADIR_STAGE_INDEX_COLUMNS, 1)]
    if mutation == "order":
        rows[1]["column_name"], rows[2]["column_name"] = rows[2]["column_name"], rows[1]["column_name"]
    elif mutation == "unique":
        rows[0]["non_unique"] = 0
    elif mutation == "prefix":
        rows[0]["sub_part"] = 20
    else:
        rows[0]["index_type"] = "HASH"

    class IndexConnection(_TriggerConnection):
        def execute(self, statement, _params=None):
            self.statements.append(str(statement))
            return _TriggerRows(rows)

    connection = IndexConnection(rows)
    with pytest.raises(RuntimeError, match="physical contract differs"):
        schema._qmt_datadir_stage_index_present(_TriggerEngine(connection))
    assert len(connection.statements) == 1
    assert connection.statements[0].startswith("SELECT ")


@pytest.mark.parametrize(
    ("migration_name", "validator_name", "expected_table", "statement_count"),
    (
        (
            "privileged_migrate_market_overview_daily_schema",
            "validate_market_overview_daily_runtime_schema",
            "sm_market_overview_daily",
            1,
        ),
        (
            "privileged_migrate_hot_stats_schema",
            "validate_hot_stats_runtime_schema",
            "st_hot_stats",
            1,
        ),
        (
            "privileged_migrate_qmt_realtime_sync_receipt_schema",
            "validate_qmt_realtime_sync_receipt_runtime_schema",
            "st_qmt_realtime_sync_receipt_v2",
            1,
        ),
        (
            "privileged_migrate_qmt_membership_snapshot_schema",
            "validate_qmt_membership_snapshot_runtime_schema",
            "qmt_membership_snapshot_run",
            3,
        ),
        (
            "privileged_migrate_hot_rank_fusion_schema",
            "validate_hot_rank_fusion_runtime_schema",
            "st_hot_rank_fused",
            2,
        ),
        (
            "privileged_migrate_si_all_index_code_schema",
            "validate_si_all_index_code_runtime_schema",
            "si_all_index_code",
            1,
        ),
        (
            "privileged_migrate_qmt_datadir_import_schema",
            "validate_qmt_datadir_import_runtime_schema",
            "qmt_datadir_import_partition",
            2,
        ),
    ),
)
def test_persistent_ddl_is_exposed_only_through_privileged_migrations(
    monkeypatch,
    migration_name: str,
    validator_name: str,
    expected_table: str,
    statement_count: int,
) -> None:
    captured: list[str] = []
    monkeypatch.setattr(
        schema,
        "_privileged_create",
        lambda _engine, statements: captured.extend(statements),
    )
    monkeypatch.setattr(
        schema,
        validator_name,
        lambda _engine, **_kwargs: {"read_only": True},
    )
    if migration_name == "privileged_migrate_qmt_datadir_import_schema":
        monkeypatch.setattr(schema, "_qmt_datadir_stage_index_present", lambda _engine: True)

    result = getattr(schema, migration_name)(object())

    assert result["privileged_migration"] is True
    assert len(captured) == statement_count
    assert expected_table in "\n".join(captured)
    if migration_name == "privileged_migrate_qmt_datadir_import_schema":
        assert result["stage_rows_preserved"] is True
    assert all("CREATE TABLE IF NOT EXISTS" in statement for statement in captured)


def test_membership_compatibility_guard_is_read_only(monkeypatch) -> None:
    sentinel = {"read_only": True}
    calls: list[tuple[object, bool]] = []

    def _validate(engine, *, require_triggers: bool):
        calls.append((engine, require_triggers))
        return sentinel

    monkeypatch.setattr(
        membership_snapshot,
        "validate_qmt_membership_snapshot_runtime_schema",
        _validate,
    )

    assert membership_snapshot.ensure_membership_snapshot_tables("engine") is sentinel
    assert calls == [("engine", False)]


def test_membership_mysql_runtime_attests_exact_six_trigger_contract(
    monkeypatch,
) -> None:
    connection = _TriggerConnection(_exact_membership_trigger_rows())
    engine = _TriggerEngine(connection)
    monkeypatch.setattr(
        schema,
        "validate_required_table_surface",
        lambda *_args, **_kwargs: {"read_only": True},
    )

    result = schema.validate_qmt_membership_snapshot_runtime_schema(engine)

    assert result["append_only_verified"] is True
    assert set(result["trigger_names"]) == set(
        schema.QMT_MEMBERSHIP_IMMUTABILITY_TRIGGER_NAMES
    )
    assert len(connection.statements) == 1
    assert connection.statements[0].lstrip().upper().startswith("SELECT ")


def test_membership_mysql_runtime_rejects_missing_trigger() -> None:
    rows = _exact_membership_trigger_rows()[:-1]

    with pytest.raises(RuntimeError, match="trigger inventory differs"):
        schema.validate_qmt_membership_snapshot_immutability(
            _TriggerConnection(rows)
        )


def test_membership_mysql_runtime_rejects_noop_trigger_body_mutation() -> None:
    rows = _exact_membership_trigger_rows()
    rows[0] = {**rows[0], "ACTION_STATEMENT": "BEGIN SET @noop=1; END"}

    with pytest.raises(RuntimeError, match="trigger contract differs"):
        schema.validate_qmt_membership_snapshot_immutability(
            _TriggerConnection(rows)
        )


def test_membership_privileged_migration_defers_all_six_triggers_to_broker(
    monkeypatch,
) -> None:
    captured_table_ddl: list[str] = []
    connection = _TriggerConnection(_exact_membership_trigger_rows())
    engine = _TriggerEngine(connection)
    monkeypatch.setattr(
        schema,
        "_privileged_create",
        lambda _engine, statements: captured_table_ddl.extend(statements),
    )
    monkeypatch.setattr(
        schema,
        "validate_qmt_membership_snapshot_runtime_schema",
        lambda _engine, **_kwargs: {
            "read_only": True,
            "append_only_verified": False,
        },
    )

    result = schema.privileged_migrate_qmt_membership_snapshot_schema(engine)

    drops = [
        sql for sql in connection.statements
        if sql.lstrip().upper().startswith("DROP TRIGGER")
    ]
    creates = [
        sql for sql in connection.statements
        if sql.lstrip().upper().startswith("CREATE TRIGGER")
    ]
    assert len(captured_table_ddl) == 3
    assert drops == []
    assert creates == []
    assert result["triggers_installed"] is False
    assert result["trigger_installation"] == "FROZEN_RELEASE_BROKER_REQUIRED"
    assert result["privileged_migration"] is True

    with pytest.raises(RuntimeError, match="frozen release broker"):
        schema.privileged_migrate_qmt_membership_snapshot_schema(
            engine,
            install_triggers=True,
        )


@pytest.mark.parametrize(
    "relative_path",
    (
        "tools/refresh_market_overview_daily.py",
        "tools/hot_data_stats.py",
        "tools/run_big_qmt_bridge.py",
        "integrations/bigqmt/membership_snapshot.py",
        "tools/merge_hot_rank.py",
        "tools/fetch_si_all_index_code_sina.py",
    ),
)
def test_auxiliary_runtime_modules_have_no_persistent_ddl(relative_path: str) -> None:
    source = (ROOT / relative_path).read_text(encoding="utf-8").upper()

    assert "CREATE TABLE" not in source
    assert "ALTER TABLE" not in source
    assert "DROP TABLE" not in source
    assert "TRUNCATE TABLE" not in source


def test_collectors_do_not_reference_missing_or_legacy_ddl_entrypoints() -> None:
    merge_source = (ROOT / "tools/merge_hot_rank.py").read_text(encoding="utf-8")
    assert "replace_table_rows(" in merge_source
    assert "DELETE FROM `st_hot_rank_fused`" not in merge_source
    assert "DELETE FROM `st_hot_rank_multi_day`" not in merge_source
    sina_source = (ROOT / "tools/fetch_si_all_index_code_sina.py").read_text(
        encoding="utf-8"
    )

    assert "02_hot_rank_extra_tables.sql" not in merge_source
    assert "run_ddl" not in merge_source
    assert "run_ddl" not in sina_source
    assert "validate_hot_rank_fusion_runtime_schema" in merge_source
    assert "validate_si_all_index_code_runtime_schema" in sina_source
