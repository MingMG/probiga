from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest

import server.db.migrations_v4 as migrations
from tools import trading_v4_mysql_acceptance as acceptance


@pytest.fixture(autouse=True)
def _stub_stage3_lineage_runner_for_legacy_migration_fixtures(monkeypatch):
    """Keep legacy 001-006 runner fakes focused on their frozen boundaries."""

    monkeypatch.setattr(
        migrations,
        "_validate_pit_factor_lineage_preflight",
        lambda _connection: None,
    )
    monkeypatch.setattr(
        migrations,
        "_apply_pit_factor_lineage_statement",
        lambda _connection, _index: None,
    )


class _Rows:
    def __init__(self, rows=(), scalar=None):
        self.rows = list(rows)
        self._scalar = scalar

    def mappings(self):
        return self

    def first(self):
        return self.rows[0] if self.rows else None

    def __iter__(self):
        return iter(self.rows)

    def scalar_one(self):
        return self._scalar


class _SchemaConnection:
    def __init__(self, signature: dict[str, Any]):
        self.signature = signature

    def execute(self, statement, _parameters=None):
        sql = str(statement)
        if "information_schema.TABLES" in sql:
            return _Rows(
                [{"ENGINE": "InnoDB", "TABLE_COLLATION": "utf8mb4_bin"}]
            )
        if "information_schema.COLUMNS" in sql:
            return _Rows(
                [
                    {
                        "COLUMN_NAME": name,
                        "COLUMN_TYPE": details["type"],
                        "IS_NULLABLE": "YES" if details["nullable"] else "NO",
                        "COLUMN_DEFAULT": details["default"],
                    }
                    for name, details in self.signature["columns"].items()
                ]
            )
        if "information_schema.STATISTICS" in sql:
            return _Rows(
                [
                    {
                        "INDEX_NAME": name,
                        "NON_UNIQUE": 0 if details["unique"] else 1,
                        "SEQ_IN_INDEX": position,
                        "COLUMN_NAME": column,
                    }
                    for name, details in self.signature["indexes"].items()
                    for position, column in enumerate(details["columns"], 1)
                ]
            )
        if "information_schema.KEY_COLUMN_USAGE" in sql:
            return _Rows(
                [
                    {
                        "CONSTRAINT_NAME": name,
                        "COLUMN_NAME": column,
                        "REFERENCED_TABLE_NAME": details["referenced_table"],
                        "REFERENCED_COLUMN_NAME": details["referenced_columns"][
                            position - 1
                        ],
                        "ORDINAL_POSITION": position,
                        "DELETE_RULE": details["on_delete"],
                    }
                    for name, details in self.signature["constraints"].items()
                    for position, column in enumerate(details["columns"], 1)
                ]
            )
        raise AssertionError(f"unexpected query: {sql}")


class _MySQLEngine:
    class _Dialect:
        name = "mysql"
        server_version_info = (5, 7, 38)

    dialect = _Dialect()


class _RunnerConnection:
    def __init__(self, ledger: dict[str, str]):
        self.ledger = ledger
        self.executed: list[str] = []
        self.commits = 0

    def execute(self, statement, parameters=None):
        sql = str(statement)
        self.executed.append(sql)
        if "INSERT IGNORE INTO schema_migration_v4" in sql:
            assert parameters is not None
            self.ledger[str(parameters["version"])] = str(parameters["checksum"])
        return _Rows()

    def commit(self):
        self.commits += 1


def _ledger_record(
    ledger: dict[str, str],
    version: str,
) -> migrations._AppliedMigrationRecord | None:
    checksum = ledger.get(version)
    if checksum is None:
        return None
    statement_count = len(
        tuple(
            next(
                item
                for item in migrations.MIGRATIONS
                if str(item["version"]) == version
            )["statements"]
        )
    )
    return migrations._AppliedMigrationRecord(checksum, statement_count)


def _job_create_statement() -> str:
    return next(
        statement
        for statement in migrations.MIGRATIONS[0]["statements"]
        if "CREATE TABLE IF NOT EXISTS st_job_run_v4" in statement
    )


def test_001_through_004_migration_contracts_are_frozen() -> None:
    first, second, third, fourth = migrations.MIGRATIONS[:4]
    assert migrations._checksum(tuple(first["statements"])) == (
        "49887b8222632a4770fc53f84d4104d425852bb9ba40267f3fd2f4f12863a0ec"
    )
    assert second["version"] == migrations.JOB_LEASE_MIGRATION_VERSION
    assert len(second["statements"]) == 6
    assert migrations._checksum(tuple(second["statements"])) == (
        "d8affcd4f94a14709133b61fc0c87275c55c8067d985412c495f95d466c008a2"
    )
    assert migrations._expected_schema(tuple(second["statements"])) == {}
    assert third["version"] == (
        migrations.CLAIM_TOKEN_REGISTRY_MIGRATION_VERSION
    )
    assert len(third["statements"]) == 4
    assert migrations._checksum(tuple(third["statements"])) == (
        "1ef8d2e44ef17c8b419e737d737e575dad736b04c0b9e7a9ebbf3b0840902c8e"
    )
    registry_schema = migrations._expected_schema(tuple(third["statements"]))
    assert set(registry_schema) == {migrations.CLAIM_TOKEN_REGISTRY_TABLE}
    assert registry_schema[migrations.CLAIM_TOKEN_REGISTRY_TABLE][
        "constraints"
    ] == migrations.CLAIM_TOKEN_REGISTRY_CONSTRAINT_CONTRACT
    assert fourth["version"] == migrations.CONTROL_GUARD_MIGRATION_VERSION
    assert len(fourth["statements"]) == 16
    assert migrations._checksum(tuple(fourth["statements"])) == (
        "3b554ef9a8b637706c6d641bfc2b07c329498fe7a743e38967aa3510a6c79b02"
    )
    assert migrations._expected_schema(tuple(fourth["statements"])) == {}
    acceptance._assert_frozen_migration_contract()
    expected_trigger_hashes = {
        name: body_hash
        for name, _event, _timing, _order, body_hash in (
            acceptance.FROZEN_JOB_LEASE_TRIGGERS
        )
    }
    assert expected_trigger_hashes == {
        "trg_v4_job_lease_bi": acceptance._normalized_trigger_body_hash(
            migrations._trigger_body_from_ddl(
                migrations._JOB_LEASE_INSERT_TRIGGER_DDL
            )
        ),
        "trg_v4_job_lease_bu": acceptance._normalized_trigger_body_hash(
            migrations._trigger_body_from_ddl(
                migrations._JOB_LEASE_UPDATE_TRIGGER_DDL
            )
        ),
    }


def test_002_is_schema_only_and_trigger_contract_fails_closed() -> None:
    statements = tuple(migrations.MIGRATIONS[1]["statements"])
    combined = "\n".join(statements)
    assert "UPDATE st_job_run_v4" not in combined
    assert "DROP " not in combined.upper()
    assert "lease_token CHAR(64) NULL" in statements[0]
    assert "max_attempts INT UNSIGNED NOT NULL DEFAULT 3" in statements[1]
    assert "ADD KEY idx_v4_job_claim_due" in statements[2]
    assert "ADD UNIQUE KEY uk_v4_job_lease_token" in statements[3]

    insert_trigger, update_trigger = statements[4:]
    assert "invalid initial V4 job shape" in insert_trigger
    assert "next_attempt_at <=> NEW.scheduled_for" in insert_trigger
    assert "invalid V4 job exact text contract" in insert_trigger
    assert "BINARY NEW.job_id REGEXP" in insert_trigger
    assert "BINARY NEW.idempotency_key NOT REGEXP" in insert_trigger
    assert "SET NEW." not in insert_trigger
    assert "non-RUNNING V4 job cannot retain lease" in update_trigger
    assert "SET NEW." not in update_trigger
    assert "UTC_TIMESTAMP(6)" in update_trigger
    assert "terminal V4 job is immutable" in update_trigger
    assert "V4 job identity is immutable" in update_trigger
    assert "invalid or active V4 job reclaim" in update_trigger
    assert "V4 heartbeat may only extend its lease" in update_trigger
    assert "expired V4 lease requires exhausted failure" in update_trigger

    from server.trading_v4.infrastructure.job_store import (
        EXHAUSTED_LEASE_ERROR_CODE,
        EXHAUSTED_LEASE_ERROR_MESSAGE,
        JOB_CALLER_CLOCK_MAX_SKEW_SECONDS,
        JOB_LEASE_MAX_DURATION_SECONDS,
    )

    assert EXHAUSTED_LEASE_ERROR_CODE == (
        migrations.JOB_LEASE_EXHAUSTED_ERROR_CODE
    )
    assert EXHAUSTED_LEASE_ERROR_MESSAGE == (
        migrations.JOB_LEASE_EXHAUSTED_ERROR_MESSAGE
    )
    assert EXHAUSTED_LEASE_ERROR_CODE in update_trigger
    assert EXHAUSTED_LEASE_ERROR_MESSAGE in update_trigger
    assert JOB_CALLER_CLOCK_MAX_SKEW_SECONDS == (
        migrations.JOB_LEASE_DB_CLOCK_MAX_SKEW_SECONDS
    )
    assert JOB_LEASE_MAX_DURATION_SECONDS == (
        migrations.JOB_LEASE_MAX_DURATION_SECONDS
    ) == 900
    assert "DATE_ADD(UTC_TIMESTAMP(6), INTERVAL 5 SECOND)" in insert_trigger
    assert "DATE_ADD(UTC_TIMESTAMP(6), INTERVAL 900 SECOND)" in update_trigger
    assert "reserved V4 lease exhaustion shape" in update_trigger
    assert "BINARY NEW.status REGEXP" in update_trigger


def test_003_registry_is_append_only_and_matches_exact_live_claim() -> None:
    table_ddl, insert_trigger, update_trigger, delete_trigger = tuple(
        migrations.MIGRATIONS[2]["statements"]
    )
    assert "CREATE TABLE IF NOT EXISTS st_job_claim_token_v4" in table_ddl
    assert "PRIMARY KEY (lease_token)" in table_ddl
    assert "UNIQUE KEY uk_v4_job_claim_attempt" in table_ddl
    assert "CONSTRAINT fk_v4_job_claim_token_job" in table_ddl
    assert "ON DELETE RESTRICT" in table_ddl
    assert "BINARY NEW.lease_token NOT REGEXP '^[0-9a-f]{64}$'" in (
        insert_trigger
    )
    assert "V4 claim token lacks exact live lease" in insert_trigger
    assert "attempt_count = NEW.attempt_count" in insert_trigger
    assert "lease_token = NEW.lease_token" in insert_trigger
    assert "updated_at = NEW.claimed_at" in insert_trigger
    assert "INTERVAL 900 SECOND" in insert_trigger
    assert "BEFORE UPDATE ON st_job_claim_token_v4" in update_trigger
    assert "BEFORE DELETE ON st_job_claim_token_v4" in delete_trigger
    assert "V4 claim token registry is append-only" in update_trigger
    assert "V4 claim token registry is append-only" in delete_trigger
    assert all(
        "DROP " not in statement.upper()
        for statement in migrations.MIGRATIONS[2]["statements"]
    )


def test_004_guards_every_non_job_control_plane_mutation_boundary() -> None:
    statements = tuple(migrations.MIGRATIONS[3]["statements"])
    combined = "\n".join(statements)

    assert len(statements) == len(migrations.CONTROL_GUARD_TRIGGER_SPECS) == 16
    assert tuple(
        name
        for name, _event, _table, _statement in (
            migrations.CONTROL_GUARD_TRIGGER_SPECS
        )
    ) == migrations.CONTROL_GUARD_TRIGGER_NAMES
    for name, event, table_name, statement in (
        migrations.CONTROL_GUARD_TRIGGER_SPECS
    ):
        assert f"CREATE TRIGGER {name}" in statement
        assert f"BEFORE {event} ON {table_name}" in statement
        assert "SIGNAL SQLSTATE '45000'" in statement

    for table_name in (
        "st_decision_context_v4",
        "st_source_watermark_v4",
        "st_decision_run_v4",
        "st_decision_channel_head_v4",
        "st_runtime_control_v4",
        "st_runtime_control_transition_v4",
    ):
        assert table_name in migrations.CONTROL_GUARD_TABLES

    assert "V4 decision context is append-only" in combined
    assert "V4 source watermark is append-only" in combined
    assert "V4 decision run identity is immutable" in combined
    assert "OLD.status IN ('COMMITTED', 'FAILED', 'CANCELLED')" in combined
    assert "OLD.status <> 'CREATED'" in combined
    assert "OLD.status <> 'RUNNING'" in combined
    assert "OLD.status <> 'VALIDATING'" in combined
    assert "V4 decision run cannot be deleted" in combined
    assert "NEW.head_version <> OLD.head_version + 1" in combined
    assert "NEW.published_at < OLD.published_at" in combined
    assert "nc.decision_at > oc.decision_at" in combined
    assert "V4 channel head cannot be deleted" in combined
    assert "NEW.version <> OLD.version + 1" in combined
    assert "BINARY NEW.control_key <=> BINARY OLD.control_key" in combined
    assert combined.count("V4 runtime control lacks exact transition") == 2
    assert "t.previous_value_json IS NULL" in combined
    assert "BINARY t.previous_value_json" in combined
    assert "V4 runtime control cannot be deleted" in combined
    assert "c.version = NEW.next_version - 1" in combined
    assert "invalid V4 runtime transition genesis" in combined
    assert "p.next_version = NEW.next_version - 1" in combined
    assert combined.count("V4 runtime transition is append-only") == 2
    assert "DROP " not in combined.upper()
    assert "ALTER TABLE" not in combined.upper()
    for forbidden_table in (
        "st_account_v4",
        "st_cash_ledger_v4",
        "st_fill_v4",
        "st_order_v4",
        "st_position_v4",
        "st_risk_ledger_v4",
    ):
        assert forbidden_table not in combined


def test_002_exact_text_guards_cover_all_python_strip_whitespace() -> None:
    exact_text_fields = (
        "job_id",
        "job_type",
        "input_context_id",
        "run_uid",
        "status",
        "lease_owner",
        "error_code",
        "error_message",
    )
    boundary_pattern = "'(^[[:space:]])|([[:space:]]$)'"
    insert_trigger, update_trigger = tuple(
        migrations.MIGRATIONS[1]["statements"]
    )[4:]
    for trigger in (insert_trigger, update_trigger):
        for field in exact_text_fields:
            assert f"BINARY NEW.{field} REGEXP" in trigger
            assert f"OCTET_LENGTH(NEW.{field})" not in trigger
        assert trigger.count(boundary_pattern) == len(exact_text_fields)

    violation_names = (
        "invalid_attempts",
        "invalid_statuses",
        "invalid_running_leases",
        "invalid_released_leases",
        "invalid_pending_shape",
        "invalid_running_shape",
        "invalid_success_shape",
        "invalid_failure_shape",
        "invalid_cancel_shape",
        "invalid_chronology",
        "invalid_future_timestamp",
        "invalid_error_shape",
        "invalid_identity_text",
        "invalid_mutable_text",
    )

    class RowAuditConnection:
        sql = ""

        def execute(self, statement, _parameters=None):
            self.sql = str(statement)
            return _Rows([{name: 0 for name in violation_names}])

    connection = RowAuditConnection()
    migrations._validate_job_lease_row_invariants(connection)
    for field in exact_text_fields:
        assert f"BINARY {field} REGEXP" in connection.sql
        assert f"OCTET_LENGTH({field})" not in connection.sql
    assert connection.sql.count(boundary_pattern) == len(exact_text_fields)


def test_001_gate_allows_only_exact_known_002_forward_expansion() -> None:
    expected = migrations._expected_schema((_job_create_statement(),))[
        migrations.JOB_LEASE_TABLE
    ]
    migrations._validate_schema_on_connection(
        _SchemaConnection(deepcopy(expected)),
        (_job_create_statement(),),
    )

    expanded = deepcopy(expected)
    expanded["columns"].update(
        {
            name: migrations._base_column_contract(details)
            for name, details in migrations.JOB_LEASE_COLUMN_CONTRACT.items()
        }
    )
    expanded["indexes"].update(
        {
            name: migrations._base_index_contract(details)
            for name, details in migrations.JOB_LEASE_INDEX_CONTRACT.items()
        }
    )
    migrations._validate_schema_on_connection(
        _SchemaConnection(deepcopy(expanded)),
        (_job_create_statement(),),
    )

    drifted = deepcopy(expanded)
    drifted["columns"]["max_attempts"]["nullable"] = True
    with pytest.raises(RuntimeError, match="column drift"):
        migrations._validate_schema_on_connection(
            _SchemaConnection(drifted),
            (_job_create_statement(),),
        )

    unknown = deepcopy(expanded)
    unknown["columns"]["parallel_ledger_id"] = {
        "type": "bigint",
        "nullable": True,
        "default": None,
    }
    with pytest.raises(RuntimeError, match="parallel_ledger_id"):
        migrations._validate_schema_on_connection(
            _SchemaConnection(unknown),
            (_job_create_statement(),),
        )


@pytest.mark.parametrize("table_index", (1, 2))
def test_005_gate_allows_only_exact_known_007_forward_expansion(
    table_index: int,
) -> None:
    base_ddl = migrations.PIT_FACTOR_REGISTRY_TABLE_DDLS[table_index]
    table_name = migrations.PIT_FACTOR_REGISTRY_TABLES[table_index]
    final_signature = migrations._expected_schema(
        migrations.PIT_FACTOR_LINEAGE_TABLE_DDLS
    )[table_name]

    migrations._validate_schema_on_connection(
        _SchemaConnection(deepcopy(final_signature)),
        (base_ddl,),
    )

    lineage_columns = (
        {"max_age_seconds", "required_source_certifications_json"}
        if table_name == "st_factor_definition_v4"
        else {"factor_definitions_json"}
    )
    drifted = deepcopy(final_signature)
    drifted["columns"][next(iter(lineage_columns))]["nullable"] = True
    with pytest.raises(RuntimeError, match="column drift"):
        migrations._validate_schema_on_connection(
            _SchemaConnection(drifted),
            (base_ddl,),
        )

    unknown = deepcopy(final_signature)
    unknown["columns"]["unreviewed_forward_column"] = {
        "type": "bigint",
        "nullable": True,
        "default": None,
    }
    with pytest.raises(RuntimeError, match="unreviewed_forward_column"):
        migrations._validate_schema_on_connection(
            _SchemaConnection(unknown),
            (base_ddl,),
        )


def test_dry_run_opened_connection_honors_applied_007_final_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opened = object()

    class Context:
        def __enter__(self):
            return opened

        def __exit__(self, *_args):
            return False

    class Engine:
        def connect(self):
            return Context()

    calls: list[object] = []
    monkeypatch.setattr(
        migrations,
        "_applied_migration_record",
        lambda _engine, version, *, connection: (
            object()
            if version == migrations.PIT_FACTOR_LINEAGE_MIGRATION_VERSION
            and connection is opened
            else None
        ),
    )
    monkeypatch.setattr(
        migrations,
        "_validate_pit_factor_lineage_final_contract",
        lambda connection: calls.append(connection),
    )
    monkeypatch.setattr(
        migrations,
        "_validate_schema_on_connection",
        lambda *_args: (_ for _ in ()).throw(
            AssertionError("earlier migration schema gate must not run")
        ),
    )

    migrations._validate_schema(
        Engine(),
        migrations.PIT_FACTOR_REGISTRY_TABLE_DDLS,
        migration_version=migrations.PIT_FACTOR_REGISTRY_MIGRATION_VERSION,
    )

    assert calls == [opened]


def test_002_validation_never_accepts_an_empty_alter_only_expected_schema(
    monkeypatch,
) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        migrations,
        "_validate_schema_on_connection",
        lambda _connection, _statements: calls.append("create-gate"),
    )
    monkeypatch.setattr(
        migrations,
        "_validate_job_lease_final_contract",
        lambda _connection: calls.append("002-final-gate"),
    )
    migrations._validate_schema(
        _MySQLEngine(),
        tuple(migrations.MIGRATIONS[1]["statements"]),
        connection=object(),
        migration_version=migrations.JOB_LEASE_MIGRATION_VERSION,
    )
    assert calls == ["create-gate", "002-final-gate"]


def test_003_validation_always_invokes_registry_final_gate(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        migrations,
        "_validate_schema_on_connection",
        lambda _connection, _statements: calls.append("create-gate"),
    )
    monkeypatch.setattr(
        migrations,
        "_validate_claim_token_registry_final_contract",
        lambda _connection: calls.append("003-final-gate"),
    )
    migrations._validate_schema(
        _MySQLEngine(),
        tuple(migrations.MIGRATIONS[2]["statements"]),
        connection=object(),
        migration_version=migrations.CLAIM_TOKEN_REGISTRY_MIGRATION_VERSION,
    )
    assert calls == ["create-gate", "003-final-gate"]


def test_004_validation_always_invokes_control_guard_final_gate(
    monkeypatch,
) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        migrations,
        "_validate_schema_on_connection",
        lambda _connection, _statements: calls.append("create-gate"),
    )
    monkeypatch.setattr(
        migrations,
        "_validate_control_guard_final_contract",
        lambda _connection: calls.append("004-final-gate"),
    )
    migrations._validate_schema(
        _MySQLEngine(),
        tuple(migrations.MIGRATIONS[3]["statements"]),
        connection=object(),
        migration_version=migrations.CONTROL_GUARD_MIGRATION_VERSION,
    )
    assert calls == ["create-gate", "004-final-gate"]


def test_004_row_auditor_reads_all_six_non_job_domains() -> None:
    class Connection:
        def __init__(self):
            self.sql: list[str] = []

        def execute(self, statement, _parameters=None):
            self.sql.append(str(statement))
            return _Rows(scalar=0)

    connection = Connection()
    migrations._validate_control_guard_rows(connection)
    assert len(connection.sql) == 6
    combined = "\n".join(connection.sql)
    for table_name in (
        "st_decision_context_v4",
        "st_source_watermark_v4",
        "st_decision_run_v4",
        "st_decision_channel_head_v4",
        "st_runtime_control_v4",
        "st_runtime_control_transition_v4",
    ):
        assert table_name in combined
    assert "JSON_VALID" in combined
    assert "r.status = 'COMMITTED'" in combined
    assert "latest.next_version = c.version" in combined
    assert "p.next_version = t.next_version - 1" in combined


@pytest.mark.parametrize(
    ("violation_index", "violation_name"),
    (
        (0, "invalid_contexts"),
        (1, "invalid_watermarks"),
        (2, "invalid_runs"),
        (3, "invalid_heads"),
        (4, "invalid_controls"),
        (5, "invalid_transitions"),
    ),
)
def test_004_row_auditor_fails_closed_for_each_domain(
    violation_index: int,
    violation_name: str,
) -> None:
    class Connection:
        def __init__(self):
            self.call = 0

        def execute(self, _statement, _parameters=None):
            value = int(self.call == violation_index)
            self.call += 1
            return _Rows(scalar=value)

    with pytest.raises(RuntimeError, match=violation_name):
        migrations._validate_control_guard_rows(Connection())


def test_003_preflight_rejects_unprovable_prior_claim_history() -> None:
    class Connection:
        def __init__(self):
            self.calls: list[str] = []

        def execute(self, statement, _parameters=None):
            self.calls.append(str(statement))
            return _Rows(scalar=1)

    connection = Connection()
    with pytest.raises(RuntimeError, match="cannot prove pre-migration"):
        migrations._validate_claim_token_registry_preflight(connection)
    assert len(connection.calls) == 1
    assert "attempt_count <> 0" in connection.calls[0]


def test_002_preflight_rejects_nonempty_table_without_writes() -> None:
    class Connection:
        def __init__(self):
            self.calls: list[str] = []

        def execute(self, statement, _parameters=None):
            self.calls.append(str(statement))
            return _Rows(scalar=2)

    connection = Connection()
    with pytest.raises(RuntimeError, match="requires an empty"):
        migrations._validate_job_lease_preflight_empty(connection)
    assert connection.calls == ["SELECT COUNT(*) FROM st_job_run_v4"]


def test_nonempty_002_preflight_stops_before_ddl_and_ledger(monkeypatch) -> None:
    first = migrations.MIGRATIONS[0]
    ledger = {
        str(first["version"]): migrations._checksum(tuple(first["statements"]))
    }
    connection = _RunnerConnection(ledger)
    applied_boundaries: list[int] = []
    monkeypatch.setattr(
        migrations,
        "_applied_migration_record",
        lambda _engine, version, connection=None: _ledger_record(
            ledger, version
        ),
    )
    monkeypatch.setattr(migrations, "_validate_schema", lambda *_a, **_k: None)
    monkeypatch.setattr(
        migrations,
        "_validate_job_lease_preflight_empty",
        lambda _connection: (_ for _ in ()).throw(
            RuntimeError("existing rows were not modified")
        ),
    )
    monkeypatch.setattr(
        migrations,
        "_apply_job_lease_statement",
        lambda _connection, index, _statement: applied_boundaries.append(index),
    )
    with pytest.raises(RuntimeError, match="not modified"):
        migrations._run_v4_migrations_unlocked(
            _MySQLEngine(), connection=connection
        )
    assert applied_boundaries == []
    assert migrations.JOB_LEASE_MIGRATION_VERSION not in ledger


def test_004_preflight_stops_before_trigger_ddl_and_ledger(monkeypatch) -> None:
    ledger = {
        str(item["version"]): migrations._checksum(tuple(item["statements"]))
        for item in migrations.MIGRATIONS[:3]
    }
    connection = _RunnerConnection(ledger)
    applied_boundaries: list[int] = []
    monkeypatch.setattr(
        migrations,
        "_applied_migration_record",
        lambda _engine, version, connection=None: _ledger_record(
            ledger, version
        ),
    )
    monkeypatch.setattr(migrations, "_validate_schema", lambda *_a, **_k: None)
    monkeypatch.setattr(
        migrations,
        "_validate_control_guard_preflight",
        lambda _connection: (_ for _ in ()).throw(
            RuntimeError("existing V4 control rows drifted")
        ),
    )
    monkeypatch.setattr(
        migrations,
        "_apply_control_guard_statement",
        lambda _connection, index, _statement: applied_boundaries.append(index),
    )
    with pytest.raises(RuntimeError, match="control rows drifted"):
        migrations._run_v4_migrations_unlocked(
            _MySQLEngine(), connection=connection
        )
    assert applied_boundaries == []
    assert migrations.CONTROL_GUARD_MIGRATION_VERSION not in ledger


def test_first_apply_and_full_chain_replay_report_all_migrations(
    monkeypatch,
) -> None:
    ledger: dict[str, str] = {}
    connection = _RunnerConnection(ledger)
    lease_boundaries: list[int] = []
    registry_boundaries: list[int] = []
    guard_boundaries: list[int] = []
    pit_registry_boundaries: list[int] = []
    pit_guard_boundaries: list[int] = []

    monkeypatch.setattr(
        migrations,
        "_applied_migration_record",
        lambda _engine, version, connection=None: _ledger_record(
            ledger, version
        ),
    )
    monkeypatch.setattr(migrations, "_validate_schema", lambda *_a, **_k: None)
    monkeypatch.setattr(
        migrations,
        "_validate_job_lease_preflight_empty",
        lambda _connection: None,
    )
    monkeypatch.setattr(
        migrations,
        "_validate_job_lease_existing_prefix_contract",
        lambda _connection: None,
    )
    monkeypatch.setattr(
        migrations,
        "_apply_job_lease_statement",
        lambda _connection, index, _statement: lease_boundaries.append(index),
    )
    monkeypatch.setattr(
        migrations,
        "_validate_claim_token_registry_preflight",
        lambda _connection: None,
    )
    monkeypatch.setattr(
        migrations,
        "_validate_claim_token_registry_existing_prefix_contract",
        lambda _connection: None,
    )
    monkeypatch.setattr(
        migrations,
        "_apply_claim_token_registry_statement",
        lambda _connection, index, _statement: registry_boundaries.append(index),
    )
    monkeypatch.setattr(
        migrations,
        "_validate_control_guard_preflight",
        lambda _connection: None,
    )
    monkeypatch.setattr(
        migrations,
        "_validate_control_guard_existing_prefix_contract",
        lambda _connection: None,
    )
    monkeypatch.setattr(
        migrations,
        "_apply_control_guard_statement",
        lambda _connection, index, _statement: guard_boundaries.append(index),
    )
    monkeypatch.setattr(
        migrations,
        "_validate_pit_factor_registry_existing_prefix_contract",
        lambda _connection: None,
    )
    monkeypatch.setattr(
        migrations,
        "_apply_pit_factor_registry_statement",
        lambda _connection, index, _statement: (
            pit_registry_boundaries.append(index)
        ),
    )
    monkeypatch.setattr(
        migrations,
        "_validate_pit_factor_guard_preflight",
        lambda _connection: None,
    )
    monkeypatch.setattr(
        migrations,
        "_validate_pit_factor_guard_existing_prefix_contract",
        lambda _connection: None,
    )
    monkeypatch.setattr(
        migrations,
        "_apply_pit_factor_guard_statement",
        lambda _connection, index, _statement: pit_guard_boundaries.append(
            index
        ),
    )

    applied = migrations._run_v4_migrations_unlocked(
        _MySQLEngine(), connection=connection
    )
    replay = migrations._run_v4_migrations_unlocked(
        _MySQLEngine(), connection=connection
    )

    assert [item.status for item in applied] == ["applied"] * 7
    assert [item.status for item in replay] == ["exists"] * 7
    assert sum(
        "CREATE TABLE IF NOT EXISTS st_" in statement
        for statement in connection.executed
    ) == 7
    assert lease_boundaries == list(range(6))
    assert registry_boundaries == list(range(4))
    assert guard_boundaries == list(range(16))
    assert pit_registry_boundaries == list(range(3))
    assert pit_guard_boundaries == list(range(9))
    assert ledger == {
        str(item["version"]): migrations._checksum(tuple(item["statements"]))
        for item in migrations.MIGRATIONS
    }


def test_applied_ledger_statement_count_drift_fails_closed(monkeypatch) -> None:
    first = migrations.MIGRATIONS[0]
    connection = _RunnerConnection({})
    monkeypatch.setattr(
        migrations,
        "_applied_migration_record",
        lambda _engine, version, connection=None: (
            migrations._AppliedMigrationRecord(
                migrations._checksum(tuple(first["statements"])),
                len(tuple(first["statements"])) - 1,
            )
            if version == str(first["version"])
            else None
        ),
    )
    monkeypatch.setattr(migrations, "_validate_schema", lambda *_a, **_k: None)
    with pytest.raises(RuntimeError, match="statement_count=6"):
        migrations._run_v4_migrations_unlocked(
            _MySQLEngine(), connection=connection
        )


def test_applied_ledger_rejects_bool_statement_count(monkeypatch) -> None:
    class Connection:
        def execute(self, _statement, _parameters=None):
            return _Rows(
                [
                    {
                        "checksum": "a" * 64,
                        "statement_count": True,
                    }
                ]
            )

    monkeypatch.setattr(
        migrations,
        "_migration_table_exists",
        lambda _engine, connection=None: True,
    )
    with pytest.raises(RuntimeError, match="statement_count type"):
        migrations._applied_migration_record(
            _MySQLEngine(), "forged", connection=Connection()
        )


@pytest.mark.parametrize("prefix_length", range(1, 6))
def test_every_002_implicit_ddl_prefix_recovers_without_blind_replay(
    monkeypatch,
    prefix_length: int,
) -> None:
    first = migrations.MIGRATIONS[0]
    second = migrations.MIGRATIONS[1]
    ledger = {
        str(item["version"]): migrations._checksum(tuple(item["statements"]))
        for index, item in enumerate(migrations.MIGRATIONS)
        if index != 1
    }
    connection = _RunnerConnection(ledger)
    objects = set(range(prefix_length))
    executed: list[int] = []
    preflights: list[bool] = []

    monkeypatch.setattr(
        migrations,
        "_applied_migration_record",
        lambda _engine, version, connection=None: _ledger_record(
            ledger, version
        ),
    )
    monkeypatch.setattr(
        migrations,
        "_validate_job_lease_preflight_empty",
        lambda _connection: preflights.append(True),
    )
    monkeypatch.setattr(
        migrations,
        "_job_lease_column_signature",
        lambda _connection, name: (
            migrations.JOB_LEASE_COLUMN_CONTRACT[name]
            if ({"lease_token": 0, "max_attempts": 1}[name] in objects)
            else None
        ),
    )
    monkeypatch.setattr(
        migrations,
        "_job_lease_index_signature",
        lambda _connection, name: (
            migrations.JOB_LEASE_INDEX_CONTRACT[name]
            if (
                {
                    "idx_v4_job_claim_due": 2,
                    "uk_v4_job_lease_token": 3,
                }[name]
                in objects
            )
            else None
        ),
    )
    trigger_contracts = {
        name: {"name": name} for name in migrations.JOB_LEASE_TRIGGER_NAMES
    }
    monkeypatch.setattr(
        migrations,
        "_expected_job_lease_triggers",
        lambda _connection: trigger_contracts,
    )

    def trigger_signatures(_connection):
        return {
            name: trigger_contracts[name]
            for index, name in enumerate(
                migrations.JOB_LEASE_TRIGGER_NAMES, start=4
            )
            if index in objects
        }

    monkeypatch.setattr(
        migrations, "_job_lease_trigger_signatures", trigger_signatures
    )

    statements = tuple(second["statements"])

    def execute_boundary(_connection, index):
        assert index not in objects
        objects.add(index)
        executed.append(index)

    monkeypatch.setattr(
        migrations, "_execute_job_lease_static_statement", execute_boundary
    )

    def validate(_engine, _statements, **kwargs):
        if kwargs.get("migration_version") == str(second["version"]):
            assert objects == set(range(6))

    monkeypatch.setattr(migrations, "_validate_schema", validate)

    result = migrations._run_v4_migrations_unlocked(
        _MySQLEngine(), connection=connection
    )
    assert [item.status for item in result] == [
        "exists",
        "applied",
        "exists",
        "exists",
        "exists",
        "exists",
        "exists",
    ]
    assert preflights == [True]
    assert executed == list(range(prefix_length, 6))
    assert ledger[str(second["version"])] == migrations._checksum(statements)


@pytest.mark.parametrize("prefix_length", range(1, 4))
def test_every_003_implicit_ddl_prefix_recovers_without_blind_replay(
    monkeypatch,
    prefix_length: int,
) -> None:
    third = migrations.MIGRATIONS[2]
    ledger = {
        str(item["version"]): migrations._checksum(tuple(item["statements"]))
        for item in migrations.MIGRATIONS[:2]
    }
    fourth = migrations.MIGRATIONS[3]
    ledger[str(fourth["version"])] = migrations._checksum(
        tuple(fourth["statements"])
    )
    for item in migrations.MIGRATIONS[4:]:
        ledger[str(item["version"])] = migrations._checksum(
            tuple(item["statements"])
        )
    connection = _RunnerConnection(ledger)
    objects = set(range(prefix_length))
    executed: list[int] = []
    preflights: list[bool] = []

    monkeypatch.setattr(
        migrations,
        "_applied_migration_record",
        lambda _engine, version, connection=None: _ledger_record(
            ledger, version
        ),
    )
    monkeypatch.setattr(
        migrations,
        "_validate_claim_token_registry_preflight",
        lambda _connection: preflights.append(True),
    )
    monkeypatch.setattr(
        migrations,
        "_validate_claim_token_registry_existing_prefix_contract",
        lambda _connection: None,
    )
    monkeypatch.setattr(
        migrations,
        "_claim_token_registry_table_exists",
        lambda _connection: 0 in objects,
    )
    monkeypatch.setattr(
        migrations,
        "_validate_schema_on_connection",
        lambda _connection, _statements: None,
    )
    monkeypatch.setattr(
        migrations,
        "_claim_token_registry_column_signature",
        lambda _connection, name: (
            migrations.CLAIM_TOKEN_REGISTRY_COLUMN_CONTRACT[name]
        ),
    )
    monkeypatch.setattr(
        migrations,
        "_claim_token_registry_index_signature",
        lambda _connection, name: (
            migrations.CLAIM_TOKEN_REGISTRY_INDEX_CONTRACT[name]
        ),
    )
    trigger_contracts = {
        name: {"name": name}
        for name in migrations.CLAIM_TOKEN_REGISTRY_TRIGGER_NAMES
    }
    monkeypatch.setattr(
        migrations,
        "_expected_claim_token_registry_triggers",
        lambda _connection: trigger_contracts,
    )

    def trigger_signatures(_connection):
        return {
            name: trigger_contracts[name]
            for index, name in enumerate(
                migrations.CLAIM_TOKEN_REGISTRY_TRIGGER_NAMES,
                start=1,
            )
            if index in objects
        }

    monkeypatch.setattr(
        migrations,
        "_claim_token_registry_trigger_signatures",
        trigger_signatures,
    )

    def execute_boundary(_connection, index):
        assert index not in objects
        objects.add(index)
        executed.append(index)

    monkeypatch.setattr(
        migrations,
        "_execute_claim_token_registry_static_statement",
        execute_boundary,
    )

    def validate(_engine, _statements, **kwargs):
        if kwargs.get("migration_version") == str(third["version"]):
            assert objects == set(range(4))

    monkeypatch.setattr(migrations, "_validate_schema", validate)

    result = migrations._run_v4_migrations_unlocked(
        _MySQLEngine(), connection=connection
    )
    assert [item.status for item in result] == [
        "exists",
        "exists",
        "applied",
        "exists",
        "exists",
        "exists",
        "exists",
    ]
    assert preflights == [True]
    assert executed == list(range(prefix_length, 4))
    assert ledger[str(third["version"])] == migrations._checksum(
        tuple(third["statements"])
    )


def test_002_existing_object_drift_fails_before_execution(monkeypatch) -> None:
    executed: list[str] = []
    monkeypatch.setattr(
        migrations,
        "_job_lease_column_signature",
        lambda _connection, _name: {
            **migrations.JOB_LEASE_COLUMN_CONTRACT["lease_token"],
            "collation": "utf8mb4_general_ci",
        },
    )
    monkeypatch.setattr(
        migrations,
        "_execute_job_lease_static_statement",
        lambda _connection, index: executed.append(str(index)),
    )
    with pytest.raises(RuntimeError, match="column drift blocks recovery"):
        migrations._apply_job_lease_statement(
            object(), 0, tuple(migrations.MIGRATIONS[1]["statements"])[0]
        )
    assert executed == []


@pytest.mark.parametrize("prefix_length", range(1, 16))
def test_every_004_implicit_ddl_prefix_recovers_without_blind_replay(
    monkeypatch,
    prefix_length: int,
) -> None:
    fourth = migrations.MIGRATIONS[3]
    ledger = {
        str(item["version"]): migrations._checksum(tuple(item["statements"]))
        for item in migrations.MIGRATIONS[:3]
    }
    for item in migrations.MIGRATIONS[4:]:
        ledger[str(item["version"])] = migrations._checksum(
            tuple(item["statements"])
        )
    connection = _RunnerConnection(ledger)
    objects = set(range(prefix_length))
    executed: list[int] = []
    preflights: list[bool] = []

    monkeypatch.setattr(
        migrations,
        "_applied_migration_record",
        lambda _engine, version, connection=None: _ledger_record(
            ledger, version
        ),
    )
    monkeypatch.setattr(
        migrations,
        "_validate_control_guard_preflight",
        lambda _connection: preflights.append(True),
    )
    monkeypatch.setattr(
        migrations,
        "_validate_control_guard_existing_prefix_contract",
        lambda _connection: None,
    )
    trigger_contracts = {
        name: {"name": name}
        for name in migrations.CONTROL_GUARD_TRIGGER_NAMES
    }
    monkeypatch.setattr(
        migrations,
        "_expected_control_guard_triggers",
        lambda _connection: trigger_contracts,
    )

    def trigger_signatures(_connection):
        return {
            name: trigger_contracts[name]
            for index, name in enumerate(
                migrations.CONTROL_GUARD_TRIGGER_NAMES
            )
            if index in objects
        }

    monkeypatch.setattr(
        migrations,
        "_control_guard_trigger_signatures",
        trigger_signatures,
    )

    def execute_boundary(_connection, index):
        assert index not in objects
        objects.add(index)
        executed.append(index)

    monkeypatch.setattr(
        migrations,
        "_execute_control_guard_static_statement",
        execute_boundary,
    )

    def validate(_engine, _statements, **kwargs):
        if kwargs.get("migration_version") == str(fourth["version"]):
            assert objects == set(range(16))

    monkeypatch.setattr(migrations, "_validate_schema", validate)

    result = migrations._run_v4_migrations_unlocked(
        _MySQLEngine(), connection=connection
    )
    assert [item.status for item in result] == [
        "exists",
        "exists",
        "exists",
        "applied",
        "exists",
        "exists",
        "exists",
    ]
    assert preflights == [True]
    assert executed == list(range(prefix_length, 16))
    assert ledger[str(fourth["version"])] == migrations._checksum(
        tuple(fourth["statements"])
    )


def test_004_existing_trigger_drift_fails_before_execution(monkeypatch) -> None:
    executed: list[int] = []
    first_name = migrations.CONTROL_GUARD_TRIGGER_NAMES[0]
    monkeypatch.setattr(
        migrations,
        "_expected_control_guard_triggers",
        lambda _connection: {first_name: {"body": "expected"}},
    )
    monkeypatch.setattr(
        migrations,
        "_control_guard_trigger_signatures",
        lambda _connection: {first_name: {"body": "drifted"}},
    )
    monkeypatch.setattr(
        migrations,
        "_execute_control_guard_static_statement",
        lambda _connection, index: executed.append(index),
    )
    with pytest.raises(RuntimeError, match="trigger drift blocks recovery"):
        migrations._apply_control_guard_statement(
            object(),
            0,
            tuple(migrations.MIGRATIONS[3]["statements"])[0],
        )
    assert executed == []
