from __future__ import annotations

from dataclasses import dataclass
import re
from threading import Lock

import pytest
from sqlalchemy import text

import server.db.migrations_v4 as migrations
from server.trading_v4.domain.enums import (
    AvailabilityStatus,
    CertificationStatus,
    FactorRole,
    QualityStatus,
    ReplayEligibility,
    ResearchStatus,
    ScopeType,
)
from tools import trading_v4_mysql_acceptance as acceptance


_BINARY_REGEXP = re.compile(
    r"\bBINARY\s+[A-Za-z_][A-Za-z0-9_.]*\s+(?:NOT\s+)?REGEXP\b",
    flags=re.IGNORECASE,
)


class _ZeroRow:
    def __getitem__(self, _name: str) -> int:
        return 0


class _ZeroResult:
    def scalar_one(self) -> int:
        return 0

    def mappings(self):
        return self

    def first(self) -> _ZeroRow:
        return _ZeroRow()


class _Mysql84RegexpConnection:
    class Dialect:
        name = "mysql"
        server_version_info = (8, 4, 11)

    dialect = Dialect()

    def __init__(self) -> None:
        self.sql: list[str] = []
        self.commits = 0

    def execute(self, statement, _parameters=None):
        rendered = str(statement)
        if _BINARY_REGEXP.search(rendered):
            raise RuntimeError(
                "ER_CHARACTER_SET_MISMATCH: binary strings are rejected by "
                "MySQL 8.4 REGEXP"
            )
        self.sql.append(rendered)
        return _ZeroResult()

    def commit(self) -> None:
        self.commits += 1


class _Mysql5738Connection(_Mysql84RegexpConnection):
    class Dialect:
        name = "mysql"
        server_version_info = (5, 7, 38)

    dialect = Dialect()

    def execute(self, statement, _parameters=None):
        self.sql.append(str(statement))
        return _ZeroResult()


def test_stage3_migrations_are_expand_only_and_leave_001_004_frozen() -> None:
    declared = tuple(
        (
            str(item["version"]),
            migrations._checksum(tuple(item["statements"])),
            len(tuple(item["statements"])),
        )
        for item in migrations.MIGRATIONS
    )

    assert declared[:4] == acceptance.FROZEN_EXPECTED_MIGRATIONS[:4]
    assert declared[4:] == acceptance.FROZEN_EXPECTED_MIGRATIONS[4:]
    assert tuple(item[0] for item in declared[4:]) == (
        "20260804_005_v4_pit_factor_registry",
        "20260804_006_v4_pit_factor_guards",
        "20260804_007_v4_factor_lineage",
    )
    assert all(
        statement.lstrip().upper().startswith(
            "CREATE TABLE IF NOT EXISTS"
        )
        for statement in migrations.MIGRATIONS[4]["statements"]
    )
    assert all(
        statement.lstrip().upper().startswith("CREATE TRIGGER")
        for statement in migrations.MIGRATIONS[5]["statements"]
    )
    combined = "\n".join(
        str(statement) for item in migrations.MIGRATIONS[4:] for statement in item["statements"]
    ).upper()
    assert "DROP TABLE" not in combined
    assert "DROP COLUMN" not in combined
    assert all(
        statement.lstrip().upper().startswith(("ALTER TABLE", "CREATE TRIGGER"))
        for statement in migrations.MIGRATIONS[6]["statements"]
    )


def test_mysql84_reproduces_binary_regexp_character_set_mismatch() -> None:
    connection = _Mysql84RegexpConnection()
    frozen = migrations._JOB_LEASE_INSERT_TRIGGER_DDL

    with pytest.raises(RuntimeError, match="ER_CHARACTER_SET_MISMATCH"):
        connection.execute(text(frozen))


def test_mysql84_uses_compatible_regexp_without_mutating_frozen_contract() -> None:
    connection = _Mysql84RegexpConnection()
    frozen = migrations._TRANSITION_INSERT_GUARD_DDL
    checksum_before = migrations._checksum(
        tuple(migrations.MIGRATIONS[3]["statements"])
    )

    executable = migrations._mysql_regexp_compatible_statement(
        connection,  # type: ignore[arg-type]
        frozen,
    )
    connection.execute(text(executable))

    assert migrations._TRANSITION_INSERT_GUARD_DDL == frozen
    assert migrations._checksum(
        tuple(migrations.MIGRATIONS[3]["statements"])
    ) == checksum_before
    assert _BINARY_REGEXP.search(executable) is None
    assert (
        "CONVERT(NEW.transition_id USING utf8mb4) "
        "COLLATE utf8mb4_bin NOT REGEXP"
    ) in executable
    assert "BINARY NEW.transition_id <=> BINARY NEW.event_hash" in executable


def test_mysql84_static_trigger_execution_uses_compatible_regexp() -> None:
    connection = _Mysql84RegexpConnection()

    migrations._execute_job_lease_static_statement(
        connection,  # type: ignore[arg-type]
        4,
    )

    assert connection.commits == 1
    assert len(connection.sql) == 1
    assert _BINARY_REGEXP.search(connection.sql[0]) is None
    assert "CONVERT(NEW.job_id USING utf8mb4)" in connection.sql[0]


def test_mysql5738_static_trigger_execution_keeps_frozen_text_exact() -> None:
    connection = _Mysql5738Connection()

    migrations._execute_job_lease_static_statement(
        connection,  # type: ignore[arg-type]
        4,
    )

    assert connection.sql == [migrations._JOB_LEASE_INSERT_TRIGGER_DDL]


def test_mysql84_expected_trigger_signature_matches_compatible_body(
    monkeypatch,
) -> None:
    connection = _Mysql84RegexpConnection()
    monkeypatch.setattr(
        migrations,
        "_job_lease_trigger_context",
        lambda _connection: {
            "schema": "v4_test",
            "definer": "v4_test@localhost",
            "sql_mode": (),
            "character_set_client": "utf8mb4",
            "collation_connection": "utf8mb4_bin",
            "database_collation": "utf8mb4_bin",
        },
    )

    expected = migrations._expected_control_guard_triggers(
        connection  # type: ignore[arg-type]
    )["trg_v4_control_transition_bi"]

    assert _BINARY_REGEXP.search(expected["action_statement"]) is None
    assert "convert(new.transition_id using utf8mb4)" in expected[
        "action_statement"
    ]
    assert "binary new.transition_id <=> binary new.event_hash" in expected[
        "action_statement"
    ]


def test_mysql84_row_auditors_use_compatible_regexp() -> None:
    connection = _Mysql84RegexpConnection()

    migrations._validate_job_lease_row_invariants(
        connection  # type: ignore[arg-type]
    )
    migrations._validate_claim_token_registry_rows(
        connection  # type: ignore[arg-type]
    )
    migrations._validate_control_guard_rows(
        connection  # type: ignore[arg-type]
    )
    migrations._validate_pit_factor_registry_rows(
        connection  # type: ignore[arg-type]
    )

    assert len(connection.sql) == 11
    assert all(_BINARY_REGEXP.search(sql) is None for sql in connection.sql)
    assert all("COLLATE utf8mb4_bin" in sql for sql in connection.sql)


def test_005_freezes_three_table_keys_indexes_and_run_fk() -> None:
    expected = migrations._expected_schema(
        migrations.PIT_FACTOR_REGISTRY_TABLE_DDLS
    )

    assert tuple(expected) == migrations.PIT_FACTOR_REGISTRY_TABLES
    certification = expected["st_data_source_certification_v4"]
    definition = expected["st_factor_definition_v4"]
    snapshot = expected["st_entity_feature_snapshot_v4"]
    assert {
        "replay_eligibility",
        "certification_status",
        "availability_status",
        "research_status",
        "quality_status",
    } <= set(certification["columns"])
    assert {
        "factor_role",
        "scope_type",
        "availability_status",
        "research_status",
        "quality_status",
        "missing_policy",
    } <= set(definition["columns"])
    assert "quality_status" in snapshot["columns"]
    assert certification["indexes"]["PRIMARY"] == {
        "unique": True,
        "columns": ("source_key", "certification_version"),
    }
    assert definition["indexes"]["PRIMARY"] == {
        "unique": True,
        "columns": ("factor_key", "factor_version"),
    }
    assert definition["indexes"]["uk_v4_factor_feature_set"] == {
        "unique": True,
        "columns": ("factor_key", "feature_set_version"),
    }
    assert snapshot["indexes"]["uk_v4_feature_snapshot_identity"] == {
        "unique": True,
        "columns": (
            "run_uid",
            "scope_type",
            "scope_id",
            "feature_set_version",
        ),
    }
    assert snapshot["constraints"]["fk_v4_feature_snapshot_run"] == {
        "columns": ("run_uid",),
        "referenced_table": "st_decision_run_v4",
        "referenced_columns": ("run_uid",),
        "on_delete": "RESTRICT",
    }


def test_006_has_one_insert_update_delete_guard_per_table() -> None:
    specs = migrations.PIT_FACTOR_GUARD_TRIGGER_SPECS

    assert len(specs) == 9
    assert len({name for name, _event, _table, _ddl in specs}) == 9
    for table_name in migrations.PIT_FACTOR_REGISTRY_TABLES:
        table_specs = [item for item in specs if item[2] == table_name]
        assert {item[1] for item in table_specs} == {
            "INSERT",
            "UPDATE",
            "DELETE",
        }
        update_delete = [
            ddl for _name, event, _table, ddl in table_specs if event != "INSERT"
        ]
        assert all("SIGNAL SQLSTATE '45000'" in ddl for ddl in update_delete)
        assert all("IF " not in ddl for ddl in update_delete)

    insert_sql = "\n".join(
        ddl for _name, event, _table, ddl in specs if event == "INSERT"
    )
    assert "PIT_CERTIFIED" in insert_sql
    assert "JSON_VALID" in insert_sql
    assert "^[0-9a-f]{64}$" in insert_sql
    assert "knowledge_cutoff_at" in insert_sql
    assert "lacks exact PIT run" in insert_sql
    assert "$.revision_policy" in migrations._DATA_SOURCE_CERTIFICATION_INSERT_GUARD_DDL
    assert "APPEND_ONLY_REVISION_CHAIN" in insert_sql
    assert "BITEMPORAL_REVISION_CHAIN" in insert_sql
    assert "IMMUTABLE_EVENT_LOG" in insert_sql
    assert "r.status IN ('RUNNING','VALIDATING')" in insert_sql
    assert "c.data_snapshot_hash = NEW.source_manifest_hash" in insert_sql
    assert "'COMMITTED'" not in migrations._ENTITY_FEATURE_SNAPSHOT_INSERT_GUARD_DDL


def test_007_adds_exact_lineage_and_max_age_without_mutating_005_006() -> None:
    assert migrations._checksum(
        tuple(migrations.MIGRATIONS[4]["statements"])
    ) == "14b5b8b2eba30739c897b7c4bb9ba33ab44e132604bcda589f4636c63b5c74db"
    assert migrations._checksum(
        tuple(migrations.MIGRATIONS[5]["statements"])
    ) == "2df39d8dc3cda258a582bb45e1a66b770f402affe14b08d4cdf27b6b232818a0"
    sql = "\n".join(migrations.PIT_FACTOR_LINEAGE_STATEMENTS)
    assert "max_age_seconds" in sql
    assert "required_source_certifications_json" in sql
    assert "factor_definitions_json" in sql
    assert "JSON_LENGTH(NEW.values_json) <> NEW.factor_count" in sql


def test_stage3_refuses_mysql_55_before_executing_any_ddl() -> None:
    class Dialect:
        name = "mysql"
        server_version_info = (5, 5, 20)

    class Engine:
        dialect = Dialect()

    class Connection:
        dialect = Dialect()

        def __init__(self) -> None:
            self.execute_count = 0

        def execute(self, *_args, **_kwargs):
            self.execute_count += 1
            raise AssertionError("no SQL may run on unsupported MySQL")

    connection = Connection()
    with pytest.raises(RuntimeError, match="validated Oracle MySQL"):
        migrations._run_v4_migrations_unlocked(
            Engine(),  # type: ignore[arg-type]
            connection=connection,  # type: ignore[arg-type]
        )
    assert connection.execute_count == 0


@pytest.mark.parametrize("version_info", ((5, 7, 38), (8, 4, 11)))
def test_v4_migration_gate_accepts_each_validated_version(version_info) -> None:
    class Dialect:
        name = "mysql"
        server_version_info = version_info

    class Engine:
        dialect = Dialect()

    class Connection:
        dialect = Dialect()

    migrations._validate_migration_server(
        Engine(),  # type: ignore[arg-type]
        Connection(),  # type: ignore[arg-type]
    )


def test_existing_snapshot_auditor_accepts_committed_but_not_failed_cancelled() -> None:
    class Result:
        def scalar_one(self):
            return 0

    class Connection:
        def __init__(self) -> None:
            self.sql: list[str] = []

        def execute(self, statement):
            self.sql.append(str(statement))
            return Result()

    connection = Connection()
    migrations._validate_pit_factor_registry_rows(connection)  # type: ignore[arg-type]
    snapshot_sql = connection.sql[-1]
    assert "('RUNNING','VALIDATING','COMMITTED')" in snapshot_sql
    assert "FAILED" not in snapshot_sql
    assert "CANCELLED" not in snapshot_sql


def test_stage3_guards_use_domain_owned_enum_values() -> None:
    source_sql = migrations._DATA_SOURCE_CERTIFICATION_INSERT_GUARD_DDL
    factor_sql = migrations._FACTOR_DEFINITION_INSERT_GUARD_DDL
    snapshot_sql = migrations._ENTITY_FEATURE_SNAPSHOT_INSERT_GUARD_DDL

    for enum_type in (
        ReplayEligibility,
        CertificationStatus,
        AvailabilityStatus,
        ResearchStatus,
        QualityStatus,
    ):
        assert all(f"'{item.value}'" in source_sql for item in enum_type)
    for enum_type in (
        FactorRole,
        ScopeType,
        AvailabilityStatus,
        ResearchStatus,
        QualityStatus,
    ):
        assert all(f"'{item.value}'" in factor_sql for item in enum_type)
    assert all(f"'{item.value}'" in snapshot_sql for item in ScopeType)
    assert all(f"'{item.value}'" in snapshot_sql for item in QualityStatus)
    assert all(
        f"'{item}'" in factor_sql
        for item in ("BLOCK", "PROPAGATE_NULL", "DISPLAY_ONLY")
    )

    assert "'CERTIFIED'" not in source_sql
    assert "'RESTRICTED'" not in source_sql
    assert "'ENTRY'" not in factor_sql
    assert "'AVAILABLE'" not in factor_sql
    assert "'STOCK'" not in snapshot_sql


@pytest.mark.parametrize("prefix_length", range(3))
def test_every_005_ddl_prefix_recovers_without_blind_replay(
    monkeypatch: pytest.MonkeyPatch,
    prefix_length: int,
) -> None:
    existing = set(range(prefix_length))
    executed: list[int] = []
    validated: list[int] = []
    table_indexes = {
        table_name: index
        for index, table_name in enumerate(
            migrations.PIT_FACTOR_REGISTRY_TABLES
        )
    }

    monkeypatch.setattr(
        migrations,
        "_pit_factor_registry_table_exists",
        lambda _connection, table_name: table_indexes[table_name] in existing,
    )

    def validate(_connection, statements):
        statement = tuple(statements)[0]
        validated.append(
            tuple(migrations.MIGRATIONS[4]["statements"]).index(statement)
        )

    monkeypatch.setattr(migrations, "_validate_schema_on_connection", validate)

    def execute(_connection, index):
        assert index not in existing
        existing.add(index)
        executed.append(index)

    monkeypatch.setattr(
        migrations,
        "_execute_pit_factor_registry_static_statement",
        execute,
    )

    for index, statement in enumerate(migrations.MIGRATIONS[4]["statements"]):
        migrations._apply_pit_factor_registry_statement(
            object(), index, str(statement)
        )

    assert existing == set(range(3))
    assert validated == list(range(prefix_length))
    assert executed == list(range(prefix_length, 3))


@pytest.mark.parametrize("prefix_length", range(9))
def test_every_006_ddl_prefix_recovers_without_blind_replay(
    monkeypatch: pytest.MonkeyPatch,
    prefix_length: int,
) -> None:
    existing = set(range(prefix_length))
    executed: list[int] = []
    expected = {
        name: {"name": name}
        for name in migrations.PIT_FACTOR_GUARD_TRIGGER_NAMES
    }
    monkeypatch.setattr(
        migrations,
        "_expected_pit_factor_guard_triggers",
        lambda _connection: expected,
    )

    def signatures(_connection):
        return {
            name: expected[name]
            for index, name in enumerate(
                migrations.PIT_FACTOR_GUARD_TRIGGER_NAMES
            )
            if index in existing
        }

    monkeypatch.setattr(
        migrations,
        "_pit_factor_guard_trigger_signatures",
        signatures,
    )

    def execute(_connection, index):
        assert index not in existing
        existing.add(index)
        executed.append(index)

    monkeypatch.setattr(
        migrations,
        "_execute_pit_factor_guard_static_statement",
        execute,
    )

    for index, statement in enumerate(migrations.MIGRATIONS[5]["statements"]):
        migrations._apply_pit_factor_guard_statement(
            object(), index, str(statement)
        )

    assert existing == set(range(9))
    assert executed == list(range(prefix_length, 9))


def test_005_existing_table_drift_blocks_later_ddl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executed: list[int] = []
    monkeypatch.setattr(
        migrations,
        "_pit_factor_registry_table_exists",
        lambda _connection, _table_name: True,
    )
    monkeypatch.setattr(
        migrations,
        "_validate_schema_on_connection",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("column drift")),
    )
    monkeypatch.setattr(
        migrations,
        "_execute_pit_factor_registry_static_statement",
        lambda _connection, index: executed.append(index),
    )

    with pytest.raises(RuntimeError, match="column drift"):
        migrations._validate_pit_factor_registry_existing_prefix_contract(
            object()
        )
    assert executed == []


def test_006_existing_trigger_drift_blocks_later_ddl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trigger_name = migrations.PIT_FACTOR_GUARD_TRIGGER_NAMES[0]
    executed: list[int] = []
    monkeypatch.setattr(
        migrations,
        "_expected_pit_factor_guard_triggers",
        lambda _connection: {trigger_name: {"body": "expected"}},
    )
    monkeypatch.setattr(
        migrations,
        "_pit_factor_guard_trigger_signatures",
        lambda _connection: {trigger_name: {"body": "drifted"}},
    )
    monkeypatch.setattr(
        migrations,
        "_execute_pit_factor_guard_static_statement",
        lambda _connection, index: executed.append(index),
    )

    with pytest.raises(RuntimeError, match="trigger drift blocks recovery"):
        migrations._apply_pit_factor_guard_statement(
            object(), 0, str(migrations.MIGRATIONS[5]["statements"][0])
        )
    assert executed == []


class _ScalarRows:
    def __init__(self, value: int) -> None:
        self.value = value

    def scalar_one(self) -> int:
        return self.value


@pytest.mark.parametrize(
    ("violation_index", "violation_name"),
    (
        (0, "invalid_certifications"),
        (1, "invalid_definitions"),
        (2, "invalid_snapshots"),
    ),
)
def test_stage3_row_auditor_fails_closed_for_each_registry_domain(
    violation_index: int,
    violation_name: str,
) -> None:
    class Connection:
        def __init__(self) -> None:
            self.call = 0

        def execute(self, _statement, _parameters=None):
            value = int(self.call == violation_index)
            self.call += 1
            return _ScalarRows(value)

    with pytest.raises(RuntimeError, match=violation_name):
        migrations._validate_pit_factor_registry_rows(Connection())


@dataclass(frozen=True)
class _ConcurrentResult:
    status: str
    checksum: str


class _AcceptanceEngine:
    def __init__(self) -> None:
        self.disposed = False

    def dispose(self) -> None:
        self.disposed = True


def test_six_migration_chain_supports_concurrent_initial_acceptance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engines: list[_AcceptanceEngine] = []
    call_lock = Lock()
    call_count = 0

    def create_engine(*_args, **_kwargs):
        engine = _AcceptanceEngine()
        engines.append(engine)
        return engine

    def run(_engine):
        nonlocal call_count
        with call_lock:
            writer = call_count == 0
            call_count += 1
        return tuple(
            _ConcurrentResult(
                "applied" if writer else "exists",
                migrations._checksum(tuple(item["statements"])),
            )
            for item in migrations.MIGRATIONS
        )

    identity = (
        "probiga_v4_test_stage3_concurrent",
        "5.7.38",
        "12345678-1234-4234-8234-123456789abc",
        "MySQL Community Server (GPL)",
    )
    monkeypatch.setattr(acceptance, "create_tool_engine", create_engine)
    monkeypatch.setattr(
        acceptance,
        "_preflight_empty_schema",
        lambda *_args: identity,
    )
    monkeypatch.setattr(
        acceptance,
        "_assert_engine_identity",
        lambda *_args: identity,
    )
    monkeypatch.setattr(acceptance, "run_v4_migrations", run)
    monkeypatch.setattr(
        acceptance,
        "_table_names",
        lambda _engine: acceptance.V4_CONTROL_PLANE_TABLES,
    )
    monkeypatch.setattr(
        acceptance,
        "_assert_job_lease_schema",
        lambda _engine: None,
    )

    report = acceptance.run_mysql_concurrent_initial_acceptance(
        "mysql+pymysql://u:p@localhost/"
        "probiga_v4_test_stage3_concurrent",
        expected_server_uuid=identity[2],
        concurrency=3,
    )

    assert len(report.concurrent_initial_runs) == 3
    assert all(len(run_statuses) == 7 for run_statuses in report.concurrent_initial_runs)
    for migration_index in range(6):
        statuses = tuple(
            run_statuses[migration_index]
            for run_statuses in report.concurrent_initial_runs
        )
        assert statuses.count("applied") == 1
        assert statuses.count("exists") == 2
    assert all(engine.disposed for engine in engines)
    assert report.production_activation_allowed is False
    assert report.actionable_output_allowed is False
