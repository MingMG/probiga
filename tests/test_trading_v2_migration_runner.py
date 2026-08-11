from __future__ import annotations

from contextlib import contextmanager

import pytest

from server.db import migrations_v2
from server.common import mysql_lock
from server.trading_v2 import execution_evidence_schema_gate as schema_gate


class _Result:
    def __init__(self, rows=(), *, scalar_value=None, rowcount: int = -1) -> None:
        self._rows = tuple(rows)
        self._scalar_value = scalar_value
        self.rowcount = rowcount

    def scalar(self):
        return self._scalar_value

    def mappings(self):
        return self

    def first(self):
        return self._rows[0] if self._rows else None

    def all(self):
        return list(self._rows)

    def __iter__(self):
        return iter(self._rows)


class _Dialect:
    def __init__(self, name: str) -> None:
        self.name = name


class _NoConnectEngine:
    def __init__(self, dialect: str) -> None:
        self.dialect = _Dialect(dialect)

    def connect(self):
        raise AssertionError("non-MySQL dry-run must not open a connection")


class _MySQLEngine:
    def __init__(self, connection) -> None:
        self.dialect = _Dialect("mysql")
        self.connection = connection


class _ExistingLockConnection:
    def __init__(self) -> None:
        self.closed = False
        self.statements: list[str] = []

    def execute(self, statement, _parameters=None):
        sql = " ".join(str(statement).split())
        self.statements.append(sql)
        return _Result(scalar_value=1)

    def close(self) -> None:
        self.closed = True


class _MigrationConnection:
    def __init__(
        self,
        *,
        dialect: str = "mysql",
        server_version: str = "5.7.38-log",
        bad_column_type: tuple[str, str] | None = None,
        bad_row_format_table: str | None = None,
        drift_trigger_body: str | None = None,
        bad_trigger_action_order: str | None = None,
        missing_index: tuple[str, str] | None = None,
        polluted_extension_table: str | None = None,
        fence_activate_rowcount: int = 1,
        fence_deactivate_rowcount: int = 1,
        bad_fence_activate_readback: bool = False,
        bad_fence_deactivate_readback: bool = False,
    ) -> None:
        self.dialect = _Dialect(dialect)
        self.server_version = server_version
        self.bad_column_type = bad_column_type
        self.bad_row_format_table = bad_row_format_table
        self.drift_trigger_body = drift_trigger_body
        self.bad_trigger_action_order = bad_trigger_action_order
        self.missing_index = missing_index
        self.polluted_extension_table = polluted_extension_table
        self.fence_activate_rowcount = fence_activate_rowcount
        self.fence_deactivate_rowcount = fence_deactivate_rowcount
        self.bad_fence_activate_readback = bad_fence_activate_readback
        self.bad_fence_deactivate_readback = bad_fence_deactivate_readback
        self.natural_index_present = False
        self.fence_table_exists = False
        self.fence_state: str | None = None
        self.fence_target_version = migrations_v2.EVIDENCE_BINDING_VERSION
        self.fence_generation = 0
        self._fence_transaction_snapshot: tuple[str | None, str, int] | None = None
        self._bad_fence_readback_pending = False
        self._in_transaction = False
        self.ledger = {
            str(migration["version"]): migrations_v2._checksum(
                tuple(migration["statements"])
            )
            for migration in migrations_v2.MIGRATIONS[:10]
        }
        self.statements: list[str] = []
        self.commit_count = 0
        self.rollback_count = 0

    @staticmethod
    def _schema_signatures():
        signatures = dict(schema_gate._binding_schema_signature())
        signatures.update(schema_gate._authority_schema_signature())
        signatures.update(schema_gate._accounting_schema_signature())
        return signatures

    def _table_is_present(self, table_name: str) -> bool:
        if table_name in schema_gate.EVIDENCE_TABLES:
            version = migrations_v2.EVIDENCE_BINDING_VERSION
        elif table_name in migrations_v2._AUTHORITY_TABLES:
            version = migrations_v2.EVIDENCE_AUTHORITY_VERSION
        elif table_name in migrations_v2._ACCOUNTING_EVIDENCE_TABLES:
            version = migrations_v2.EVIDENCE_ACCOUNTING_VERSION
        else:
            return False
        return version in self.ledger or any(
            statement.startswith(f"CREATE TABLE IF NOT EXISTS {table_name} ")
            for statement in self.statements
        )

    def _trigger_is_present(self, trigger_name: str) -> bool:
        authority_names = set(schema_gate._authority_trigger_contracts()) | (
            set(
                schema_gate._guard_trigger_contracts(
                    include_authority_attestations=True
                )
            )
            - set(
                schema_gate._guard_trigger_contracts(
                    include_authority_attestations=False
                )
            )
        )
        accounting_names = set(schema_gate._accounting_trigger_contracts())
        if trigger_name in authority_names:
            version = migrations_v2.EVIDENCE_AUTHORITY_VERSION
        elif trigger_name in accounting_names:
            version = migrations_v2.EVIDENCE_ACCOUNTING_VERSION
        else:
            version = migrations_v2.EVIDENCE_GUARD_VERSION
        return version in self.ledger or any(
            statement.startswith(f"CREATE TRIGGER {trigger_name} ")
            for statement in self.statements
        )

    def commit(self):
        self.commit_count += 1
        self._fence_transaction_snapshot = None
        self._bad_fence_readback_pending = False
        self._in_transaction = False

    def rollback(self):
        self.rollback_count += 1
        if self._fence_transaction_snapshot is not None:
            (
                self.fence_state,
                self.fence_target_version,
                self.fence_generation,
            ) = self._fence_transaction_snapshot
        self._fence_transaction_snapshot = None
        self._bad_fence_readback_pending = False
        self._in_transaction = False

    def in_transaction(self):
        return self._in_transaction

    def get_isolation_level(self):
        return "REPEATABLE READ"

    def execute(self, statement, parameters=None):
        sql = " ".join(str(statement).split())
        params = dict(parameters or {})
        self.statements.append(sql)
        self._in_transaction = True
        fence_table = migrations_v2.V2_EVIDENCE_MAINTENANCE_FENCE_TABLE
        if sql.startswith(f"CREATE TABLE IF NOT EXISTS {fence_table} "):
            self.fence_table_exists = True
            return _Result()
        if (
            "SELECT COUNT(*) FROM information_schema.TABLES" in sql
            and f"TABLE_NAME = '{fence_table}'" in sql
        ):
            return _Result(scalar_value=int(self.fence_table_exists))
        if (
            "SELECT TABLE_NAME, ENGINE, TABLE_COLLATION, ROW_FORMAT" in sql
            and f"TABLE_NAME = '{fence_table}'" in sql
        ):
            return _Result(
                ()
                if not self.fence_table_exists
                else (
                    {
                        "TABLE_NAME": fence_table,
                        "ENGINE": "InnoDB",
                        "TABLE_COLLATION": "utf8mb4_general_ci",
                        "ROW_FORMAT": "Dynamic",
                    },
                )
            )
        if (
            "FROM information_schema.COLUMNS" in sql
            and f"TABLE_NAME = '{fence_table}'" in sql
        ):
            signature = schema_gate._maintenance_fence_schema_signature()[
                fence_table
            ]
            return _Result(
                {
                    "COLUMN_NAME": column_name,
                    "COLUMN_TYPE": details["type"],
                    "IS_NULLABLE": "YES" if details["nullable"] else "NO",
                    "COLUMN_DEFAULT": details["default"],
                    "COLLATION_NAME": (
                        "ascii_bin"
                        if column_name in {"fence_name", "state", "target_version"}
                        else None
                    ),
                }
                for column_name, details in signature["columns"].items()
            )
        if (
            "FROM information_schema.STATISTICS" in sql
            and f"TABLE_NAME = '{fence_table}'" in sql
        ):
            return _Result(
                (
                    {
                        "INDEX_NAME": "PRIMARY",
                        "NON_UNIQUE": 0,
                        "SEQ_IN_INDEX": 1,
                        "COLUMN_NAME": "fence_name",
                        "SUB_PART": None,
                        "INDEX_TYPE": "BTREE",
                        "COLLATION": "A",
                    },
                )
            )
        if (
            "FROM information_schema.KEY_COLUMN_USAGE" in sql
            and f"TABLE_NAME = '{fence_table}'" in sql
        ):
            return _Result(scalar_value=0)
        if sql.startswith(f"INSERT INTO {fence_table} "):
            self.fence_state = params["state"]
            self.fence_target_version = params["target_version"]
            self.fence_generation = 0
            return _Result()
        if sql.startswith(f"UPDATE {fence_table} "):
            activating = "active_state" in params
            configured_rowcount = (
                self.fence_activate_rowcount
                if activating
                else self.fence_deactivate_rowcount
            )
            cas_matches = (
                self.fence_state == params.get("observed_state")
                and self.fence_generation == params.get("observed_generation")
            )
            rowcount = configured_rowcount if cas_matches else 0
            if rowcount == 1:
                self._fence_transaction_snapshot = (
                    self.fence_state,
                    self.fence_target_version,
                    self.fence_generation,
                )
                if activating:
                    self.fence_generation = params["next_generation"]
                    self.fence_state = params["active_state"]
                    self._bad_fence_readback_pending = (
                        self.bad_fence_activate_readback
                    )
                else:
                    self.fence_state = params["inactive_state"]
                    self._bad_fence_readback_pending = (
                        self.bad_fence_deactivate_readback
                    )
                self.fence_target_version = params["target_version"]
            return _Result(rowcount=rowcount)
        if f"FROM {fence_table}" in sql:
            if not self.fence_table_exists or self.fence_state is None:
                return _Result()
            target_version = self.fence_target_version
            if self._bad_fence_readback_pending:
                target_version = "corrupt_readback"
                self._bad_fence_readback_pending = False
            return _Result(
                (
                    {
                        "fence_name": migrations_v2.V2_EVIDENCE_MAINTENANCE_FENCE_NAME,
                        "state": self.fence_state,
                        "target_version": target_version,
                        "generation": self.fence_generation,
                        "activated_at": "2026-08-04 00:00:00.000000",
                        "updated_at": "2026-08-04 00:00:00.000000",
                    },
                )
            )
        if sql == "SELECT VERSION()":
            return _Result(scalar_value=self.server_version)
        if sql == "SELECT DATABASE()":
            return _Result(scalar_value="probiga_v2_evidence_test")
        if (
            "SELECT COUNT(*) FROM information_schema.TABLES" in sql
            and "schema_migration_v2" in sql
        ):
            return _Result(scalar_value=1)
        if sql.startswith("SELECT checksum FROM schema_migration_v2"):
            checksum = self.ledger.get(params["version"])
            return _Result(() if checksum is None else ({"checksum": checksum},))
        if sql.startswith("INSERT IGNORE INTO schema_migration_v2"):
            self.ledger.setdefault(params["version"], params["checksum"])
            return _Result()
        if sql.startswith("SELECT version, checksum FROM schema_migration_v2"):
            binding_version = params["binding_version"]
            return _Result(
                {
                    "version": version,
                    "checksum": checksum,
                }
                for version, checksum in sorted(self.ledger.items())
                if version >= binding_version
            )
        if (
            "SELECT TABLE_NAME, ENGINE, TABLE_COLLATION" in sql
            and "TABLE_NAME = 'schema_migration_v2'" in sql
        ):
            return _Result(
                (
                    {
                        "TABLE_NAME": "schema_migration_v2",
                        "ENGINE": "InnoDB",
                        "TABLE_COLLATION": "utf8mb4_general_ci",
                    },
                )
            )
        if "SELECT TABLE_NAME, ENGINE, TABLE_COLLATION" in sql:
            table_names = (
                table_name
                for table_name in self._schema_signatures()
                if f"'{table_name}'" in sql
                and self._table_is_present(table_name)
            )
            return _Result(
                {
                    "TABLE_NAME": table_name,
                    "ENGINE": "InnoDB",
                    "TABLE_COLLATION": "utf8mb4_general_ci",
                    "ROW_FORMAT": (
                        "Compact"
                        if table_name == self.bad_row_format_table
                        else "Dynamic"
                    ),
                }
                for table_name in sorted(table_names)
            )
        if (
            "FROM information_schema.COLUMNS" in sql
            and "TABLE_NAME = 'schema_migration_v2'" in sql
        ):
            return _Result(
                (
                    {
                        "COLUMN_NAME": "version",
                        "COLUMN_TYPE": "varchar(80)",
                        "IS_NULLABLE": "NO",
                        "COLUMN_DEFAULT": None,
                        "COLLATION_NAME": "utf8mb4_general_ci",
                    },
                    {
                        "COLUMN_NAME": "checksum",
                        "COLUMN_TYPE": "char(64)",
                        "IS_NULLABLE": "NO",
                        "COLUMN_DEFAULT": None,
                        "COLLATION_NAME": "utf8mb4_general_ci",
                    },
                    {
                        "COLUMN_NAME": "applied_at",
                        "COLUMN_TYPE": "timestamp",
                        "IS_NULLABLE": "NO",
                        "COLUMN_DEFAULT": "CURRENT_TIMESTAMP",
                        "COLLATION_NAME": None,
                    },
                )
            )
        if "FROM information_schema.COLUMNS" in sql:
            return _Result(
                {
                    "TABLE_NAME": table_name,
                    "COLUMN_NAME": column_name,
                    "COLUMN_TYPE": (
                        "varchar(255)"
                        if self.bad_column_type == (table_name, column_name)
                        else details["type"]
                    ),
                    "IS_NULLABLE": "YES" if details["nullable"] else "NO",
                    "COLUMN_DEFAULT": (
                        None
                        if str(details["default"]).upper() == "NULL"
                        else details["default"]
                    ),
                    "COLLATION_NAME": (
                        (
                            "ascii_bin"
                            if table_name in migrations_v2._AUTHORITY_TABLES
                            and column_name != "envelope_json"
                            else "utf8mb4_general_ci"
                        )
                        if details["type"].startswith(("char", "varchar", "longtext"))
                        else None
                    ),
                }
                for table_name, table_signature in self._schema_signatures().items()
                if f"'{table_name}'" in sql
                if self._table_is_present(table_name)
                for column_name, details in table_signature["columns"].items()
            )
        if "FROM information_schema.STATISTICS" in sql:
            if "TABLE_NAME = 'schema_migration_v2'" in sql:
                return _Result(
                    (
                        {
                            "INDEX_NAME": "PRIMARY",
                            "NON_UNIQUE": 0,
                            "SEQ_IN_INDEX": 1,
                            "COLUMN_NAME": "version",
                        },
                    )
                )
            if "INDEX_NAME = 'uk_calendar_evidence_v2_natural'" in sql:
                if (
                    not self.natural_index_present
                    or self.missing_index
                    == (
                        "st_market_calendar_evidence_v2",
                        "uk_calendar_evidence_v2_natural",
                    )
                ):
                    return _Result()
                return _Result(
                    (
                        {
                            "NON_UNIQUE": 0,
                            "SEQ_IN_INDEX": position,
                            "COLUMN_NAME": column_name,
                        }
                        for position, column_name in enumerate(
                            ("market_code", "trade_date", "calendar_version"),
                            start=1,
                        )
                    )
                )
            rows = []
            for table_name, table_signature in self._schema_signatures().items():
                if f"'{table_name}'" not in sql or not self._table_is_present(
                    table_name
                ):
                    continue
                expected_indexes = {
                    index_name: details
                    for index_name, details in table_signature["indexes"].items()
                    if (
                        index_name != "uk_calendar_evidence_v2_natural"
                        or self.natural_index_present
                    )
                }
                for index_name, details in expected_indexes.items():
                    if self.missing_index == (table_name, index_name):
                        continue
                    for position, column_name in enumerate(
                        details["columns"],
                        start=1,
                    ):
                        rows.append(
                            {
                                "TABLE_NAME": table_name,
                                "INDEX_NAME": index_name,
                                "NON_UNIQUE": 0 if details["unique"] else 1,
                                "SEQ_IN_INDEX": position,
                                "COLUMN_NAME": column_name,
                                "SUB_PART": None,
                                "INDEX_TYPE": "BTREE",
                                "COLLATION": "A",
                            }
                        )
                for implicit_number, columns in enumerate(
                    sorted(
                        schema_gate._required_implicit_fk_index_tuples(
                            table_signature,
                            expected_indexes,
                        )
                    ),
                    start=1,
                ):
                    for position, column_name in enumerate(columns, start=1):
                        rows.append(
                            {
                                "TABLE_NAME": table_name,
                                "INDEX_NAME": f"auto_fk_{implicit_number}",
                                "NON_UNIQUE": 1,
                                "SEQ_IN_INDEX": position,
                                "COLUMN_NAME": column_name,
                                "SUB_PART": None,
                                "INDEX_TYPE": "BTREE",
                                "COLLATION": "A",
                            }
                        )
            return _Result(rows)
        if "FROM information_schema.KEY_COLUMN_USAGE" in sql:
            return _Result(
                {
                    "TABLE_NAME": table_name,
                    "CONSTRAINT_NAME": constraint_name,
                    "COLUMN_NAME": column_name,
                    "REFERENCED_TABLE_SCHEMA": "probiga_v2_evidence_test",
                    "REFERENCED_TABLE_NAME": details["referenced_table"],
                    "REFERENCED_COLUMN_NAME": referenced_column,
                    "ORDINAL_POSITION": position,
                    "DELETE_RULE": details["on_delete"],
                    "UPDATE_RULE": details["on_update"],
                }
                for table_name, table_signature in self._schema_signatures().items()
                if f"'{table_name}'" in sql
                if self._table_is_present(table_name)
                for constraint_name, details in table_signature["foreign_keys"].items()
                for position, (column_name, referenced_column) in enumerate(
                    zip(details["columns"], details["referenced_columns"]),
                    start=1,
                )
            )
        if "FROM information_schema.TRIGGERS" in sql:
            contracts = dict(
                schema_gate._guard_trigger_contracts(
                    include_authority_attestations=True
                )
            )
            contracts.update(schema_gate._authority_trigger_contracts())
            contracts.update(schema_gate._accounting_trigger_contracts())
            bodies = dict(
                schema_gate._guard_trigger_bodies(
                    include_authority_attestations=True
                )
            )
            bodies.update(schema_gate._authority_trigger_bodies())
            bodies.update(schema_gate._accounting_trigger_bodies())
            action_orders, _ = schema_gate._trigger_action_order_contracts(
                contracts
            )
            selected_trigger = params.get("trigger_name")
            return _Result(
                {
                    "TRIGGER_NAME": trigger_name,
                    "EVENT_OBJECT_TABLE": table_name,
                    "ACTION_TIMING": "BEFORE",
                    "EVENT_MANIPULATION": event,
                    "ACTION_STATEMENT": (
                        "BEGIN SIGNAL SQLSTATE '45000'; END"
                        if trigger_name == self.drift_trigger_body
                        else bodies[trigger_name]
                    ),
                    "ACTION_ORDER": (
                        action_orders[trigger_name] + 1
                        if trigger_name == self.bad_trigger_action_order
                        else action_orders[trigger_name]
                    ),
                    "SQL_MODE": (
                        "STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,"
                        "ERROR_FOR_DIVISION_BY_ZERO"
                    ),
                    "DEFINER": "probiga_v2_migrator@localhost",
                    "CHARACTER_SET_CLIENT": "utf8mb4",
                    "COLLATION_CONNECTION": "utf8mb4_general_ci",
                    "DATABASE_COLLATION": "utf8mb4_general_ci",
                }
                for trigger_name, (event, table_name) in sorted(contracts.items())
                if selected_trigger == trigger_name or f"'{table_name}'" in sql
                if self._trigger_is_present(trigger_name)
            )
        if sql.startswith("/* v2e:audit_"):
            if (
                self.polluted_extension_table is not None
                and f"audit_empty_{self.polluted_extension_table}" in sql
            ):
                return _Result(({"polluted": 1},))
            return _Result()
        if sql.startswith("ALTER TABLE st_market_calendar_evidence_v2") and (
            "ADD UNIQUE KEY uk_calendar_evidence_v2_natural" in sql
        ):
            self.natural_index_present = True
        return _Result()


class _StatefulMigrationConnection(_MigrationConnection):
    """Fake that models committed CREATE/DROP trigger state across retries."""

    def __init__(self) -> None:
        super().__init__()
        self.active_tables: set[str] = set()
        self.active_triggers: set[str] = set()

    def sync_evidence_objects_from_ledger(self) -> None:
        for table_name in self._schema_signatures():
            if table_name in schema_gate.EVIDENCE_TABLES:
                version = migrations_v2.EVIDENCE_BINDING_VERSION
            elif table_name in migrations_v2._AUTHORITY_TABLES:
                version = migrations_v2.EVIDENCE_AUTHORITY_VERSION
            else:
                version = migrations_v2.EVIDENCE_ACCOUNTING_VERSION
            if version in self.ledger:
                self.active_tables.add(table_name)
        contracts = dict(
            schema_gate._guard_trigger_contracts(
                include_authority_attestations=True
            )
        )
        contracts.update(schema_gate._authority_trigger_contracts())
        contracts.update(schema_gate._accounting_trigger_contracts())
        authority_names = set(schema_gate._authority_trigger_contracts()) | (
            set(
                schema_gate._guard_trigger_contracts(
                    include_authority_attestations=True
                )
            )
            - set(
                schema_gate._guard_trigger_contracts(
                    include_authority_attestations=False
                )
            )
        )
        accounting_names = set(schema_gate._accounting_trigger_contracts())
        for trigger_name in contracts:
            if trigger_name in authority_names:
                version = migrations_v2.EVIDENCE_AUTHORITY_VERSION
            elif trigger_name in accounting_names:
                version = migrations_v2.EVIDENCE_ACCOUNTING_VERSION
            else:
                version = migrations_v2.EVIDENCE_GUARD_VERSION
            if version in self.ledger:
                self.active_triggers.add(trigger_name)
        self.natural_index_present = (
            migrations_v2.EVIDENCE_NATURAL_KEY_VERSION in self.ledger
        )

    def _table_is_present(self, table_name: str) -> bool:
        return table_name in self.active_tables

    def _trigger_is_present(self, trigger_name: str) -> bool:
        return trigger_name in self.active_triggers

    def execute(self, statement, parameters=None):
        sql = " ".join(str(statement).split())
        tokens = sql.split()
        if sql.startswith("CREATE TABLE IF NOT EXISTS "):
            self.active_tables.add(tokens[5])
        elif sql.startswith("DROP TRIGGER IF EXISTS "):
            self.active_triggers.discard(tokens[4])
        elif sql.startswith("CREATE TRIGGER "):
            trigger_name = tokens[2]
            self.active_triggers.add(trigger_name)
            if self.drift_trigger_body == trigger_name:
                self.drift_trigger_body = None
            if self.bad_trigger_action_order == trigger_name:
                self.bad_trigger_action_order = None
        return super().execute(statement, parameters)


def test_non_mysql_dry_run_is_a_pure_plan_and_apply_is_rejected():
    engine = _NoConnectEngine("sqlite")

    results = migrations_v2.run_v2_migrations(engine, dry_run=True)

    assert len(results) == len(migrations_v2.MIGRATIONS)
    assert {item.status for item in results} == {"would_apply"}
    with pytest.raises(RuntimeError, match="require MySQL"):
        migrations_v2.run_v2_migrations(engine)


def test_named_lock_preserves_caller_owned_connection():
    connection = _ExistingLockConnection()
    engine = _NoConnectEngine("mysql")

    with mysql_lock.mysql_named_lock(
        engine,
        "probiga:test:caller-owned",
        timeout_seconds=3,
        connection=connection,
    ) as observed:
        assert observed is connection

    assert connection.closed is False
    assert connection.statements == [
        "SELECT GET_LOCK(:lock_name, :timeout_seconds)",
        "SELECT RELEASE_LOCK(:lock_name)",
    ]


def test_migration_runner_rejects_connection_from_another_engine():
    engine = _MySQLEngine(_MigrationConnection())
    foreign_connection = _MigrationConnection()
    foreign_connection.engine = object()

    with pytest.raises(RuntimeError, match="another engine"):
        migrations_v2.run_v2_migrations(
            engine,
            dry_run=True,
            connection=foreign_connection,
        )


def test_migration_runner_rejects_active_caller_transaction():
    connection = _MigrationConnection()
    engine = _MySQLEngine(connection)
    connection.engine = engine
    connection.in_transaction = lambda: True

    with pytest.raises(RuntimeError, match="active transaction"):
        migrations_v2.run_v2_migrations(
            engine,
            dry_run=True,
            connection=connection,
        )


@pytest.mark.parametrize("value", [1, "false", None])
def test_evidence_gate_requires_an_exact_bool_before_connect(value):
    engine = _NoConnectEngine("mysql")

    with pytest.raises(TypeError, match="allow_execution_evidence must be bool"):
        migrations_v2.run_v2_migrations(
            engine,
            allow_execution_evidence=value,
        )


def test_pending_evidence_migrations_require_explicit_gate(monkeypatch):
    connection = _MigrationConnection()
    engine = _MySQLEngine(connection)
    lock_calls = []

    @contextmanager
    def fake_named_lock(candidate, name, *, timeout_seconds):
        lock_calls.append((candidate, name, timeout_seconds))
        yield connection

    monkeypatch.setattr(migrations_v2, "mysql_named_lock", fake_named_lock)

    with pytest.raises(RuntimeError, match="explicit allow_execution_evidence"):
        migrations_v2.run_v2_migrations(engine)

    assert lock_calls == [(engine, "probiga:trading_v2_schema", 30)]
    assert not any(
        "CREATE TABLE IF NOT EXISTS st_market_calendar_evidence_v2" in item
        for item in connection.statements
    )
    assert migrations_v2.EVIDENCE_BINDING_VERSION not in connection.ledger


def test_legacy_trigger_ddl_is_outside_evidence_recovery_scope() -> None:
    connection = _MigrationConnection()
    legacy_version = "20260726_006_real_trading_hard_guard"
    migration = next(
        item
        for item in migrations_v2.MIGRATIONS
        if str(item["version"]) == legacy_version
    )

    for statement_index in range(1, len(tuple(migration["statements"])) + 1):
        assert migrations_v2._evidence_statement_already_applied(
            connection,
            version=legacy_version,
            statement_index=statement_index,
        ) is False

    assert not any(
        "information_schema.TRIGGERS" in statement
        for statement in connection.statements
    )


def test_explicit_evidence_apply_commits_each_ddl_then_replays(monkeypatch):
    connection = _MigrationConnection()
    engine = _MySQLEngine(connection)

    @contextmanager
    def fake_named_lock(candidate, name, *, timeout_seconds):
        assert candidate is engine
        assert name == "probiga:trading_v2_schema"
        assert timeout_seconds == 30
        yield connection

    monkeypatch.setattr(migrations_v2, "mysql_named_lock", fake_named_lock)

    first = migrations_v2.run_v2_migrations(
        engine,
        allow_execution_evidence=True,
    )

    evidence_migrations = tuple(
        migration
        for migration in migrations_v2.MIGRATIONS
        if str(migration["version"])
        in migrations_v2._EVIDENCE_MIGRATION_VERSIONS
    )
    assert [item.status for item in first[-len(evidence_migrations) :]] == [
        "applied"
    ] * len(evidence_migrations)
    expected_commits = 1 + sum(
        len(migration["statements"]) + 1
        for migration in evidence_migrations
    ) + 4
    assert connection.commit_count == expected_commits
    for migration in evidence_migrations:
        version = str(migration["version"])
        assert connection.ledger[version] == migrations_v2._checksum(
            tuple(migration["statements"])
        )
    trigger_inspections = [
        index
        for index, sql in enumerate(connection.statements)
        if "FROM information_schema.TRIGGERS" in sql
    ]
    guard_creates = [
        index
        for index, sql in enumerate(connection.statements)
        if sql.startswith("CREATE TRIGGER trg_") and "_guard_" in sql
    ]
    assert trigger_inspections
    assert guard_creates
    assert max(trigger_inspections) > max(guard_creates)
    authority_table_inspection = next(
        sql
        for sql in connection.statements
        if "FROM information_schema.TABLES" in sql
        and "st_execution_authority_trust_key_v2" in sql
    )
    assert "ROW_FORMAT" in authority_table_inspection
    authority_trigger_inspection = next(
        sql
        for sql in connection.statements
        if "FROM information_schema.TRIGGERS" in sql
        and "st_execution_authority_trust_key_v2" in sql
    )
    for metadata_column in (
        "ACTION_ORDER",
        "SQL_MODE",
        "DEFINER",
        "CHARACTER_SET_CLIENT",
        "COLLATION_CONNECTION",
        "DATABASE_COLLATION",
    ):
        assert metadata_column in authority_trigger_inspection
    assert any(
        "WHERE version >= :binding_version" in sql
        for sql in connection.statements
    )

    replay = migrations_v2.run_v2_migrations(engine)

    assert {item.status for item in replay} == {"exists"}
    assert connection.commit_count == expected_commits + 1


def test_caller_owned_connection_holds_identity_and_named_lock_scope(monkeypatch):
    connection = _MigrationConnection()
    engine = _MySQLEngine(connection)
    observed: list[object] = []

    @contextmanager
    def fake_named_lock(
        candidate,
        name,
        *,
        timeout_seconds,
        connection: object | None = None,
    ):
        assert candidate is engine
        assert name == "probiga:trading_v2_schema"
        assert timeout_seconds == 30
        observed.append(connection)
        yield connection

    monkeypatch.setattr(migrations_v2, "mysql_named_lock", fake_named_lock)

    results = migrations_v2.run_v2_migrations(
        engine,
        allow_execution_evidence=True,
        connection=connection,
    )

    assert observed == [connection]
    assert [item.status for item in results[-3:]] == [
        "applied",
        "applied",
        "applied",
    ]


def test_013_recovery_skips_exact_committed_index_and_records_missing_ledger(
    monkeypatch,
):
    connection = _MigrationConnection()
    for migration in migrations_v2.MIGRATIONS[10:12]:
        connection.ledger[str(migration["version"])] = migrations_v2._checksum(
            tuple(migration["statements"])
        )
    connection.natural_index_present = True
    engine = _MySQLEngine(connection)

    @contextmanager
    def fake_named_lock(candidate, name, *, timeout_seconds):
        yield connection

    monkeypatch.setattr(migrations_v2, "mysql_named_lock", fake_named_lock)

    results = migrations_v2.run_v2_migrations(
        engine,
        allow_execution_evidence=True,
    )

    result = next(
        item
        for item in results
        if item.version == migrations_v2.EVIDENCE_NATURAL_KEY_VERSION
    )
    assert result.status == "applied"
    assert migrations_v2.EVIDENCE_NATURAL_KEY_VERSION in connection.ledger
    assert not any(
        sql.startswith("ALTER TABLE st_market_calendar_evidence_v2")
        for sql in connection.statements
    )


@pytest.mark.parametrize(
    "partial_version",
    (
        migrations_v2.EVIDENCE_AUTHORITY_VERSION,
        migrations_v2.EVIDENCE_ACCOUNTING_VERSION,
    ),
)
def test_runner_resumes_partial_014_or_015_before_ledger_write(
    monkeypatch,
    partial_version,
):
    connection = _MigrationConnection()
    target_index = next(
        index
        for index, migration in enumerate(migrations_v2.MIGRATIONS)
        if str(migration["version"]) == partial_version
    )
    for migration in migrations_v2.MIGRATIONS[10:target_index]:
        connection.ledger[str(migration["version"])] = migrations_v2._checksum(
            tuple(migration["statements"])
        )
    connection.natural_index_present = True
    first_statement = str(
        migrations_v2.MIGRATIONS[target_index]["statements"][0]
    )
    connection.statements.append(" ".join(first_statement.split()))
    engine = _MySQLEngine(connection)

    @contextmanager
    def fake_named_lock(candidate, name, *, timeout_seconds):
        yield connection

    monkeypatch.setattr(migrations_v2, "mysql_named_lock", fake_named_lock)

    results = migrations_v2.run_v2_migrations(
        engine,
        allow_execution_evidence=True,
    )

    target_result = next(
        item for item in results if item.version == partial_version
    )
    assert target_result.status == "applied"
    assert partial_version in connection.ledger
    assert migrations_v2.EVIDENCE_ACCOUNTING_VERSION in connection.ledger


def test_runner_resumes_partial_014_with_known_forward_evidence_guard(
    monkeypatch,
):
    connection = _MigrationConnection()
    authority_index = next(
        index
        for index, migration in enumerate(migrations_v2.MIGRATIONS)
        if str(migration["version"]) == migrations_v2.EVIDENCE_AUTHORITY_VERSION
    )
    for migration in migrations_v2.MIGRATIONS[10:authority_index]:
        connection.ledger[str(migration["version"])] = migrations_v2._checksum(
            tuple(migration["statements"])
        )
    connection.natural_index_present = True
    authority_statements = tuple(
        migrations_v2.MIGRATIONS[authority_index]["statements"]
    )
    connection.statements.extend(
        " ".join(str(statement).split())
        for statement in authority_statements[:-1]
    )
    engine = _MySQLEngine(connection)

    @contextmanager
    def fake_named_lock(candidate, name, *, timeout_seconds):
        yield connection

    monkeypatch.setattr(migrations_v2, "mysql_named_lock", fake_named_lock)

    results = migrations_v2.run_v2_migrations(
        engine,
        allow_execution_evidence=True,
    )

    authority_result = next(
        item
        for item in results
        if item.version == migrations_v2.EVIDENCE_AUTHORITY_VERSION
    )
    assert authority_result.status == "applied"
    assert migrations_v2.EVIDENCE_ACCOUNTING_VERSION in connection.ledger


@pytest.mark.parametrize(
    ("partial_version", "drop_statement_count", "trigger_name"),
    (
        (
            migrations_v2.EVIDENCE_AUTHORITY_VERSION,
            6,
            "trg_execution_authority_trust_key_v2_guard_bi",
        ),
        (
            migrations_v2.EVIDENCE_ACCOUNTING_VERSION,
            4,
            "trg_fill_accounting_outcome_v2_guard_bi",
        ),
    ),
)
def test_runner_recovers_a_real_drop_create_trigger_gap(
    monkeypatch,
    partial_version,
    drop_statement_count,
    trigger_name,
):
    connection = _StatefulMigrationConnection()
    target_index = next(
        index
        for index, migration in enumerate(migrations_v2.MIGRATIONS)
        if str(migration["version"]) == partial_version
    )
    for migration in migrations_v2.MIGRATIONS[10:target_index]:
        connection.ledger[str(migration["version"])] = migrations_v2._checksum(
            tuple(migration["statements"])
        )
    connection.sync_evidence_objects_from_ledger()
    target_statements = tuple(
        migrations_v2.MIGRATIONS[target_index]["statements"]
    )
    # Reproduce the real DDL boundary: the target tables already exist, but
    # the first guard trigger does not.  The migration therefore skips the
    # exact CREATE TABLE statements, commits the DROP, and is interrupted
    # before the paired CREATE TRIGGER.
    for statement in target_statements[: drop_statement_count - 1]:
        connection.execute(statement)
    assert trigger_name not in connection.active_triggers
    connection.statements.clear()
    engine = _MySQLEngine(connection)

    @contextmanager
    def fake_named_lock(candidate, name, *, timeout_seconds):
        yield connection

    monkeypatch.setattr(migrations_v2, "mysql_named_lock", fake_named_lock)
    fault = migrations_v2.V2MigrationAcceptanceFaultHook(
        version=partial_version,
        phase=migrations_v2.V2_MIGRATION_FAULT_AFTER_DDL_COMMIT,
        committed_statement_count=drop_statement_count,
    )

    with pytest.raises(migrations_v2.V2MigrationAcceptanceFault):
        migrations_v2.run_v2_migrations(
            engine,
            allow_execution_evidence=True,
            acceptance_fault_hook=fault,
        )

    assert trigger_name not in connection.active_triggers
    assert any(
        sql.startswith(f"DROP TRIGGER IF EXISTS {trigger_name}")
        for sql in connection.statements
    )
    assert not any(
        sql.startswith(f"CREATE TRIGGER {trigger_name} ")
        for sql in connection.statements
    )
    assert connection.fence_state == migrations_v2.V2_EVIDENCE_MAINTENANCE_FENCE_ACTIVE
    assert partial_version not in connection.ledger

    recovered = migrations_v2.run_v2_migrations(
        engine,
        allow_execution_evidence=True,
    )

    assert trigger_name in connection.active_triggers
    assert partial_version in connection.ledger
    assert migrations_v2.EVIDENCE_ACCOUNTING_VERSION in connection.ledger
    assert len(connection.active_triggers) == 41
    assert next(item for item in recovered if item.version == partial_version).status == (
        "applied"
    )


@pytest.mark.parametrize(
    "drift_kind",
    ("body", "action_order"),
)
def test_runner_repairs_only_the_drifted_trigger_before_ledger(
    monkeypatch,
    drift_kind,
):
    connection = _StatefulMigrationConnection()
    authority_index = next(
        index
        for index, migration in enumerate(migrations_v2.MIGRATIONS)
        if str(migration["version"]) == migrations_v2.EVIDENCE_AUTHORITY_VERSION
    )
    for migration in migrations_v2.MIGRATIONS[:authority_index]:
        connection.ledger[str(migration["version"])] = migrations_v2._checksum(
            tuple(migration["statements"])
        )
    connection.active_tables.update(connection._schema_signatures())
    contracts = schema_gate._all_trigger_contracts()
    connection.active_triggers.update(contracts)
    connection.natural_index_present = True
    trigger_name = "trg_execution_authority_trust_key_v2_guard_bi"
    if drift_kind == "body":
        connection.drift_trigger_body = trigger_name
    else:
        connection.bad_trigger_action_order = trigger_name
    engine = _MySQLEngine(connection)

    @contextmanager
    def fake_named_lock(candidate, name, *, timeout_seconds):
        yield connection

    monkeypatch.setattr(migrations_v2, "mysql_named_lock", fake_named_lock)

    results = migrations_v2.run_v2_migrations(
        engine,
        allow_execution_evidence=True,
    )

    assert any(
        sql.startswith(f"DROP TRIGGER IF EXISTS {trigger_name}")
        for sql in connection.statements
    )
    assert any(
        sql.startswith(f"CREATE TRIGGER {trigger_name} ")
        for sql in connection.statements
    )
    assert connection.drift_trigger_body is None
    assert connection.bad_trigger_action_order is None
    assert migrations_v2.EVIDENCE_AUTHORITY_VERSION in connection.ledger
    assert migrations_v2.EVIDENCE_ACCOUNTING_VERSION in connection.ledger
    assert {item.status for item in results[-2:]} == {"applied"}
    assert connection.fence_state == (
        migrations_v2.V2_EVIDENCE_MAINTENANCE_FENCE_INACTIVE
    )


def test_ddl_fault_leaves_active_fence_until_explicit_recovery(monkeypatch):
    connection = _MigrationConnection()
    engine = _MySQLEngine(connection)

    @contextmanager
    def fake_named_lock(candidate, name, *, timeout_seconds):
        yield connection

    monkeypatch.setattr(migrations_v2, "mysql_named_lock", fake_named_lock)
    fault = migrations_v2.V2MigrationAcceptanceFaultHook(
        version=migrations_v2.EVIDENCE_BINDING_VERSION,
        phase=migrations_v2.V2_MIGRATION_FAULT_AFTER_DDL_COMMIT,
        committed_statement_count=1,
    )

    with pytest.raises(migrations_v2.V2MigrationAcceptanceFault):
        migrations_v2.run_v2_migrations(
            engine,
            allow_execution_evidence=True,
            acceptance_fault_hook=fault,
        )

    assert connection.fence_state == (
        migrations_v2.V2_EVIDENCE_MAINTENANCE_FENCE_ACTIVE
    )
    assert connection.fence_generation == 1
    assert migrations_v2.EVIDENCE_BINDING_VERSION not in connection.ledger
    with pytest.raises(RuntimeError, match="fence is ACTIVE"):
        migrations_v2.run_v2_migrations(engine)

    recovered = migrations_v2.run_v2_migrations(
        engine,
        allow_execution_evidence=True,
    )

    assert {item.status for item in recovered[-5:]} == {"applied"}
    assert connection.fence_generation == 1
    assert connection.fence_state == (
        migrations_v2.V2_EVIDENCE_MAINTENANCE_FENCE_INACTIVE
    )


@pytest.mark.parametrize(
    ("failure_options", "error"),
    (
        (
            {"fence_activate_rowcount": 0},
            "ACTIVE update did not match one row",
        ),
        (
            {"bad_fence_activate_readback": True},
            "ACTIVE update verification failed",
        ),
    ),
)
def test_activate_fence_cas_failure_rolls_back_without_publishing(
    failure_options,
    error,
):
    connection = _MigrationConnection(**failure_options)
    connection.fence_table_exists = True
    connection.fence_state = migrations_v2.V2_EVIDENCE_MAINTENANCE_FENCE_INACTIVE
    connection.fence_target_version = migrations_v2.EVIDENCE_BINDING_VERSION
    connection.fence_generation = 7

    with pytest.raises(RuntimeError, match=error):
        migrations_v2._activate_maintenance_fence(
            connection,
            target_version=migrations_v2.EVIDENCE_AUTHORITY_VERSION,
        )

    assert connection.rollback_count == 1
    assert connection.commit_count == 0
    assert connection.fence_state == (
        migrations_v2.V2_EVIDENCE_MAINTENANCE_FENCE_INACTIVE
    )
    assert connection.fence_target_version == migrations_v2.EVIDENCE_BINDING_VERSION
    assert connection.fence_generation == 7
    update_sql = next(
        sql
        for sql in connection.statements
        if sql.startswith(
            f"UPDATE {migrations_v2.V2_EVIDENCE_MAINTENANCE_FENCE_TABLE} "
        )
    )
    assert "AND state = :observed_state" in update_sql
    assert "AND generation = :observed_generation" in update_sql
    assert "SET generation = :next_generation" in update_sql
    assert "IF(state = :active_state" not in update_sql


@pytest.mark.parametrize(
    ("failure_options", "error"),
    (
        (
            {"fence_deactivate_rowcount": 0},
            "INACTIVE update did not match one row",
        ),
        (
            {"bad_fence_deactivate_readback": True},
            "INACTIVE update verification failed",
        ),
    ),
)
def test_deactivate_fence_cas_failure_rolls_back_and_remains_active(
    failure_options,
    error,
):
    connection = _MigrationConnection(**failure_options)
    connection.fence_table_exists = True
    connection.fence_state = migrations_v2.V2_EVIDENCE_MAINTENANCE_FENCE_ACTIVE
    connection.fence_target_version = migrations_v2.EVIDENCE_AUTHORITY_VERSION
    connection.fence_generation = 11

    with pytest.raises(RuntimeError, match=error):
        migrations_v2._deactivate_maintenance_fence(connection)

    assert connection.rollback_count == 1
    assert connection.commit_count == 0
    assert connection.fence_state == (
        migrations_v2.V2_EVIDENCE_MAINTENANCE_FENCE_ACTIVE
    )
    assert connection.fence_target_version == migrations_v2.EVIDENCE_AUTHORITY_VERSION
    assert connection.fence_generation == 11
    update_sql = next(
        sql
        for sql in connection.statements
        if sql.startswith(
            f"UPDATE {migrations_v2.V2_EVIDENCE_MAINTENANCE_FENCE_TABLE} "
        )
    )
    assert "AND state = :observed_state" in update_sql
    assert "AND generation = :observed_generation" in update_sql


@pytest.mark.parametrize(
    "isolation_level",
    ("READ COMMITTED", "READ UNCOMMITTED", "AUTOCOMMIT"),
)
def test_evidence_audit_rejects_an_inconsistent_multi_table_snapshot(
    isolation_level,
):
    connection = _MigrationConnection()
    connection.get_isolation_level = lambda: isolation_level

    with pytest.raises(
        RuntimeError,
        match="requires REPEATABLE READ or SERIALIZABLE",
    ):
        migrations_v2._audit_evidence_rows_before_ledger(
            connection,
            version=migrations_v2.EVIDENCE_BINDING_VERSION,
        )

    assert connection.statements == []


def test_failed_authority_row_audit_blocks_ledger_and_keeps_fence_active(
    monkeypatch,
):
    import server.integrations.v2_execution_evidence_authority_audit as authority_audit

    connection = _MigrationConnection()
    authority_index = next(
        index
        for index, migration in enumerate(migrations_v2.MIGRATIONS)
        if str(migration["version"]) == migrations_v2.EVIDENCE_AUTHORITY_VERSION
    )
    for migration in migrations_v2.MIGRATIONS[:authority_index]:
        connection.ledger[str(migration["version"])] = migrations_v2._checksum(
            tuple(migration["statements"])
        )
    connection.natural_index_present = True
    engine = _MySQLEngine(connection)

    @contextmanager
    def fake_named_lock(candidate, name, *, timeout_seconds):
        yield connection

    monkeypatch.setattr(migrations_v2, "mysql_named_lock", fake_named_lock)
    original_audit = authority_audit.audit_v2_execution_evidence_authority_database

    def fail_authority_audit(candidate):
        assert candidate is connection
        raise authority_audit.V2AuthorityStoredRowAuditError(
            "drifted authority row"
        )

    monkeypatch.setattr(
        authority_audit,
        "audit_v2_execution_evidence_authority_database",
        fail_authority_audit,
    )

    with pytest.raises(RuntimeError, match="authority stored-row audit failed"):
        migrations_v2.run_v2_migrations(
            engine,
            allow_execution_evidence=True,
        )

    assert migrations_v2.EVIDENCE_AUTHORITY_VERSION not in connection.ledger
    assert connection.fence_state == (
        migrations_v2.V2_EVIDENCE_MAINTENANCE_FENCE_ACTIVE
    )

    monkeypatch.setattr(
        authority_audit,
        "audit_v2_execution_evidence_authority_database",
        original_audit,
    )
    migrations_v2.run_v2_migrations(
        engine,
        allow_execution_evidence=True,
    )
    assert migrations_v2.EVIDENCE_AUTHORITY_VERSION in connection.ledger
    assert connection.fence_state == (
        migrations_v2.V2_EVIDENCE_MAINTENANCE_FENCE_INACTIVE
    )


def test_failed_accounting_row_audit_blocks_ledger_and_keeps_fence_active(
    monkeypatch,
):
    import server.integrations.v2_accounting_evidence_audit as accounting_audit

    connection = _MigrationConnection()
    accounting_index = next(
        index
        for index, migration in enumerate(migrations_v2.MIGRATIONS)
        if str(migration["version"]) == migrations_v2.EVIDENCE_ACCOUNTING_VERSION
    )
    for migration in migrations_v2.MIGRATIONS[:accounting_index]:
        connection.ledger[str(migration["version"])] = migrations_v2._checksum(
            tuple(migration["statements"])
        )
    connection.natural_index_present = True
    engine = _MySQLEngine(connection)

    @contextmanager
    def fake_named_lock(candidate, name, *, timeout_seconds):
        yield connection

    monkeypatch.setattr(migrations_v2, "mysql_named_lock", fake_named_lock)
    original_audit = accounting_audit.audit_v2_accounting_evidence_database

    def fail_accounting_audit(candidate):
        assert candidate is connection
        raise accounting_audit.V2AccountingEvidenceAuditError(
            "drifted accounting row"
        )

    monkeypatch.setattr(
        accounting_audit,
        "audit_v2_accounting_evidence_database",
        fail_accounting_audit,
    )

    with pytest.raises(RuntimeError, match="accounting stored-row audit failed"):
        migrations_v2.run_v2_migrations(
            engine,
            allow_execution_evidence=True,
        )

    assert migrations_v2.EVIDENCE_ACCOUNTING_VERSION not in connection.ledger
    assert connection.fence_state == (
        migrations_v2.V2_EVIDENCE_MAINTENANCE_FENCE_ACTIVE
    )

    monkeypatch.setattr(
        accounting_audit,
        "audit_v2_accounting_evidence_database",
        original_audit,
    )
    migrations_v2.run_v2_migrations(
        engine,
        allow_execution_evidence=True,
    )
    assert migrations_v2.EVIDENCE_ACCOUNTING_VERSION in connection.ledger
    assert connection.fence_state == (
        migrations_v2.V2_EVIDENCE_MAINTENANCE_FENCE_INACTIVE
    )


def test_applied_checksum_conflict_fails_before_evidence_ddl(monkeypatch):
    connection = _MigrationConnection()
    first_version = str(migrations_v2.MIGRATIONS[0]["version"])
    connection.ledger[first_version] = "0" * 64
    engine = _MySQLEngine(connection)

    @contextmanager
    def fake_named_lock(candidate, name, *, timeout_seconds):
        yield connection

    monkeypatch.setattr(migrations_v2, "mysql_named_lock", fake_named_lock)

    with pytest.raises(RuntimeError, match="checksum changed"):
        migrations_v2.run_v2_migrations(engine)

    assert not any("st_market_calendar_evidence_v2" in item for item in connection.statements)


def test_unvalidated_server_is_rejected_before_evidence_ddl(monkeypatch):
    connection = _MigrationConnection(server_version="8.0.39")
    engine = _MySQLEngine(connection)

    @contextmanager
    def fake_named_lock(candidate, name, *, timeout_seconds):
        yield connection

    monkeypatch.setattr(migrations_v2, "mysql_named_lock", fake_named_lock)

    with pytest.raises(RuntimeError, match="validated Oracle MySQL"):
        migrations_v2.run_v2_migrations(
            engine,
            allow_execution_evidence=True,
        )

    assert not any(
        "CREATE TABLE IF NOT EXISTS st_market_calendar_evidence_v2" in item
        for item in connection.statements
    )
    assert migrations_v2.EVIDENCE_BINDING_VERSION not in connection.ledger


def test_mysql_8411_is_accepted_by_evidence_version_gate():
    connection = _MigrationConnection(server_version="8.4.11")

    migrations_v2._validate_evidence_server(connection)

    assert "SELECT VERSION()" in connection.statements


def test_full_column_validation_runs_before_binding_ledger_write(monkeypatch):
    connection = _MigrationConnection(
        bad_column_type=(
            "st_fill_execution_evidence_v2",
            "fill_execution_evidence_id",
        )
    )
    engine = _MySQLEngine(connection)

    @contextmanager
    def fake_named_lock(candidate, name, *, timeout_seconds):
        yield connection

    monkeypatch.setattr(migrations_v2, "mysql_named_lock", fake_named_lock)

    with pytest.raises(RuntimeError, match="TABLE_COLUMNS_DRIFTED"):
        migrations_v2.run_v2_migrations(
            engine,
            allow_execution_evidence=True,
        )

    assert migrations_v2.EVIDENCE_BINDING_VERSION not in connection.ledger


def test_full_trigger_body_validation_runs_before_guard_ledger_write(monkeypatch):
    trigger_name = sorted(schema_gate._guard_trigger_contracts())[0]
    connection = _MigrationConnection(drift_trigger_body=trigger_name)
    engine = _MySQLEngine(connection)

    @contextmanager
    def fake_named_lock(candidate, name, *, timeout_seconds):
        yield connection

    monkeypatch.setattr(migrations_v2, "mysql_named_lock", fake_named_lock)

    with pytest.raises(RuntimeError, match="TRIGGER_BODY_DRIFTED"):
        migrations_v2.run_v2_migrations(
            engine,
            allow_execution_evidence=True,
        )

    assert migrations_v2.EVIDENCE_BINDING_VERSION in connection.ledger
    assert migrations_v2.EVIDENCE_GUARD_VERSION not in connection.ledger


def test_natural_key_validation_runs_before_013_ledger_write(monkeypatch):
    connection = _MigrationConnection(
        missing_index=(
            "st_market_calendar_evidence_v2",
            "uk_calendar_evidence_v2_natural",
        )
    )
    engine = _MySQLEngine(connection)

    @contextmanager
    def fake_named_lock(candidate, name, *, timeout_seconds):
        yield connection

    monkeypatch.setattr(migrations_v2, "mysql_named_lock", fake_named_lock)

    with pytest.raises(RuntimeError, match="uk_calendar_evidence_v2_natural"):
        migrations_v2.run_v2_migrations(
            engine,
            allow_execution_evidence=True,
        )

    assert migrations_v2.EVIDENCE_BINDING_VERSION in connection.ledger
    assert migrations_v2.EVIDENCE_GUARD_VERSION in connection.ledger
    assert migrations_v2.EVIDENCE_NATURAL_KEY_VERSION not in connection.ledger


@pytest.mark.parametrize(
    ("connection", "error"),
    (
        (
            _MigrationConnection(
                bad_column_type=(
                    "st_execution_authority_receipt_v2",
                    "claim_hash",
                )
            ),
            "TABLE_COLUMNS_DRIFTED:st_execution_authority_receipt_v2",
        ),
        (
            _MigrationConnection(
                drift_trigger_body=(
                    "trg_execution_authority_attestation_v2_guard_bi"
                )
            ),
            "TRIGGER_BODY_DRIFTED:trg_execution_authority_attestation_v2_guard_bi",
        ),
        (
            _MigrationConnection(
                bad_row_format_table="st_execution_authority_receipt_v2"
            ),
            "TABLE_ROW_FORMAT_INVALID:st_execution_authority_receipt_v2",
        ),
    ),
)
def test_authority_validation_runs_before_014_ledger_write(
    monkeypatch, connection, error
):
    engine = _MySQLEngine(connection)

    @contextmanager
    def fake_named_lock(candidate, name, *, timeout_seconds):
        yield connection

    monkeypatch.setattr(migrations_v2, "mysql_named_lock", fake_named_lock)

    with pytest.raises(RuntimeError, match=error):
        migrations_v2.run_v2_migrations(
            engine,
            allow_execution_evidence=True,
        )

    assert migrations_v2.EVIDENCE_NATURAL_KEY_VERSION in connection.ledger
    assert migrations_v2.EVIDENCE_AUTHORITY_VERSION not in connection.ledger


@pytest.mark.parametrize(
    ("connection", "error"),
    (
        (
            _MigrationConnection(
                bad_column_type=(
                    "st_fill_accounting_outcome_finalization_v2",
                    "finalization_hash",
                )
            ),
            "TABLE_COLUMNS_DRIFTED:st_fill_accounting_outcome_finalization_v2",
        ),
        (
            _MigrationConnection(
                drift_trigger_body=(
                    "trg_fill_accounting_finalization_v2_guard_bi"
                )
            ),
            "TRIGGER_BODY_DRIFTED:trg_fill_accounting_finalization_v2_guard_bi",
        ),
        (
            _MigrationConnection(
                bad_row_format_table=(
                    "st_fill_accounting_outcome_finalization_v2"
                )
            ),
            "TABLE_ROW_FORMAT_INVALID:st_fill_accounting_outcome_finalization_v2",
        ),
    ),
)
def test_accounting_validation_runs_before_015_ledger_write(
    monkeypatch, connection, error
):
    engine = _MySQLEngine(connection)

    @contextmanager
    def fake_named_lock(candidate, name, *, timeout_seconds):
        yield connection

    monkeypatch.setattr(migrations_v2, "mysql_named_lock", fake_named_lock)

    with pytest.raises(RuntimeError, match=error):
        migrations_v2.run_v2_migrations(
            engine,
            allow_execution_evidence=True,
        )

    assert migrations_v2.EVIDENCE_AUTHORITY_VERSION in connection.ledger
    assert migrations_v2.EVIDENCE_ACCOUNTING_VERSION not in connection.ledger
