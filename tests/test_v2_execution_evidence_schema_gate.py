from __future__ import annotations

from pathlib import Path
import re

import pytest

from server.db.migrations_v2 import (
    V2_EVIDENCE_MAINTENANCE_FENCE_ACTIVE,
    V2_EVIDENCE_MAINTENANCE_FENCE_INACTIVE,
    V2_EVIDENCE_MAINTENANCE_FENCE_NAME,
    V2_EVIDENCE_MAINTENANCE_FENCE_TABLE,
    _checksum,
)
from server.trading_v2.execution_evidence_schema_gate import (
    ACCOUNTING_EVIDENCE_TABLES,
    AUTHORITY_TABLES,
    EVIDENCE_TABLES,
    V2EvidenceMaintenanceFenceError,
    V2EvidenceSchemaInspectionError,
    _accounting_schema_signature,
    _accounting_trigger_bodies,
    _accounting_trigger_contracts,
    _authority_schema_signature,
    _authority_trigger_bodies,
    _authority_trigger_contracts,
    _binding_schema_signature,
    _declared_column_collation_contracts,
    _expected_migrations,
    _guard_trigger_bodies,
    _guard_trigger_contracts,
    _maintenance_fence_schema_signature,
    _normalize_declared_default,
    _normalize_observed_default,
    _required_implicit_fk_index_tuples,
    _server_is_supported,
    _trigger_action_order_contracts,
    assert_v2_evidence_maintenance_fence_inactive,
    inspect_v2_execution_evidence_schema,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("version", ("5.7.38-log", "8.4.11"))
def test_schema_gate_accepts_only_frozen_oracle_mysql_versions(version):
    assert _server_is_supported("mysql", version) is True


@pytest.mark.parametrize(
    "version",
    ("5.7.39", "8.4.10", "8.4.12", "8.4.11-MariaDB"),
)
def test_schema_gate_rejects_unvalidated_adjacent_versions(version):
    assert _server_is_supported("mysql", version) is False


def _schema_signatures(
    *,
    include_authority: bool = True,
    include_accounting: bool = True,
):
    result = dict(_binding_schema_signature())
    if include_authority:
        result.update(_authority_schema_signature())
    if include_accounting:
        result.update(_accounting_schema_signature())
    return result


def _trigger_contracts(
    *,
    include_authority: bool = True,
    include_accounting: bool = True,
):
    result = dict(
        _guard_trigger_contracts(
            include_authority_attestations=include_authority
        )
    )
    if include_authority:
        result.update(_authority_trigger_contracts())
    if include_accounting:
        result.update(_accounting_trigger_contracts())
    return result


def _trigger_bodies(
    *,
    include_authority: bool = True,
    include_accounting: bool = True,
):
    result = dict(
        _guard_trigger_bodies(
            include_authority_attestations=include_authority
        )
    )
    if include_authority:
        result.update(_authority_trigger_bodies())
    if include_accounting:
        result.update(_accounting_trigger_bodies())
    return result


def _column_collation_contracts():
    from server.trading_v2.execution_evidence_schema_gate import (
        EVIDENCE_ACCOUNTING_MIGRATION,
        EVIDENCE_AUTHORITY_MIGRATION,
        EVIDENCE_BINDING_MIGRATION,
    )

    result = _declared_column_collation_contracts(
        EVIDENCE_BINDING_MIGRATION,
        EVIDENCE_TABLES,
    )
    result.update(
        _declared_column_collation_contracts(
            EVIDENCE_AUTHORITY_MIGRATION,
            AUTHORITY_TABLES,
        )
    )
    result.update(
        _declared_column_collation_contracts(
            EVIDENCE_ACCOUNTING_MIGRATION,
            ACCOUNTING_EVIDENCE_TABLES,
        )
    )
    return result


class _Result:
    def __init__(self, rows=(), *, scalar_value=None) -> None:
        self._rows = tuple(rows)
        self._scalar_value = scalar_value

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
    def __init__(self, name: str = "mysql") -> None:
        self.name = name


class _SchemaConnection:
    def __init__(
        self,
        *,
        dialect: str = "mysql",
        server_version: str = "5.7.38-log",
        missing_table: str | None = None,
        missing_trigger: str | None = None,
        bad_checksum: str | None = None,
        extra_trigger: bool = False,
        bad_column_type: tuple[str, str] | None = None,
        literal_null_default: tuple[str, str] | None = None,
        missing_index: tuple[str, str] | None = None,
        missing_foreign_key: tuple[str, str] | None = None,
        bad_foreign_schema: tuple[str, str] | None = None,
        bad_update_rule: tuple[str, str] | None = None,
        drift_trigger_body: str | None = None,
        bad_trigger_action_order: str | None = None,
        unsafe_trigger_sql_mode: bool = False,
        bad_ledger_primary: bool = False,
        bad_row_format: str | None = None,
        bad_column_collation: tuple[str, str] | None = None,
        bad_table_engine: str | None = None,
        bad_table_collation: str | None = None,
        rogue_nonunique_index: tuple[str, tuple[str, ...]] | None = None,
        bad_index_sub_part: tuple[str, str] | None = None,
        bad_index_type: tuple[str, str] | None = None,
        include_authority: bool = True,
        include_accounting: bool = True,
        fence_present: bool = True,
        fence_state: str = V2_EVIDENCE_MAINTENANCE_FENCE_INACTIVE,
        bad_fence_column: str | None = None,
        bad_fence_index: bool = False,
        extra_fence_row: bool = False,
        transaction_active: bool = True,
    ) -> None:
        self.dialect = _Dialect(dialect)
        self.server_version = server_version
        self.missing_table = missing_table
        self.missing_trigger = missing_trigger
        self.bad_checksum = bad_checksum
        self.extra_trigger = extra_trigger
        self.bad_column_type = bad_column_type
        self.literal_null_default = literal_null_default
        self.missing_index = missing_index
        self.missing_foreign_key = missing_foreign_key
        self.bad_foreign_schema = bad_foreign_schema
        self.bad_update_rule = bad_update_rule
        self.drift_trigger_body = drift_trigger_body
        self.bad_trigger_action_order = bad_trigger_action_order
        self.unsafe_trigger_sql_mode = unsafe_trigger_sql_mode
        self.bad_ledger_primary = bad_ledger_primary
        self.bad_row_format = bad_row_format
        self.bad_column_collation = bad_column_collation
        self.bad_table_engine = bad_table_engine
        self.bad_table_collation = bad_table_collation
        self.rogue_nonunique_index = rogue_nonunique_index
        self.bad_index_sub_part = bad_index_sub_part
        self.bad_index_type = bad_index_type
        self.include_authority = include_authority
        self.include_accounting = include_accounting
        self.fence_present = fence_present
        self.fence_state = fence_state
        self.bad_fence_column = bad_fence_column
        self.bad_fence_index = bad_fence_index
        self.extra_fence_row = extra_fence_row
        self.transaction_active = transaction_active
        self.statements: list[str] = []

    def in_transaction(self):
        return self.transaction_active

    def execute(self, statement, parameters=None):
        sql = " ".join(str(statement).split())
        self.statements.append(sql)
        if (
            "FROM information_schema.TABLES" in sql
            and f"TABLE_NAME = '{V2_EVIDENCE_MAINTENANCE_FENCE_TABLE}'" in sql
        ):
            return _Result(
                ()
                if not self.fence_present
                else (
                    {
                        "TABLE_NAME": V2_EVIDENCE_MAINTENANCE_FENCE_TABLE,
                        "ENGINE": "InnoDB",
                        "TABLE_COLLATION": "utf8mb4_general_ci",
                        "ROW_FORMAT": "Dynamic",
                    },
                )
            )
        if (
            "FROM information_schema.COLUMNS" in sql
            and f"TABLE_NAME = '{V2_EVIDENCE_MAINTENANCE_FENCE_TABLE}'" in sql
        ):
            signature = _maintenance_fence_schema_signature()[
                V2_EVIDENCE_MAINTENANCE_FENCE_TABLE
            ]
            return _Result(
                {
                    "COLUMN_NAME": column_name,
                    "COLUMN_TYPE": (
                        "varchar(255)"
                        if column_name == self.bad_fence_column
                        else details["type"]
                    ),
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
            and f"TABLE_NAME = '{V2_EVIDENCE_MAINTENANCE_FENCE_TABLE}'" in sql
        ):
            return _Result(
                (
                    {
                        "INDEX_NAME": "PRIMARY",
                        "NON_UNIQUE": 0,
                        "SEQ_IN_INDEX": 1,
                        "COLUMN_NAME": (
                            "state" if self.bad_fence_index else "fence_name"
                        ),
                        "SUB_PART": None,
                        "INDEX_TYPE": "BTREE",
                        "COLLATION": "A",
                    },
                )
            )
        if (
            "FROM information_schema.KEY_COLUMN_USAGE" in sql
            and f"TABLE_NAME = '{V2_EVIDENCE_MAINTENANCE_FENCE_TABLE}'" in sql
        ):
            return _Result(scalar_value=0)
        if f"FROM {V2_EVIDENCE_MAINTENANCE_FENCE_TABLE}" in sql:
            if not self.fence_present:
                return _Result()
            rows = [
                {
                    "fence_name": V2_EVIDENCE_MAINTENANCE_FENCE_NAME,
                    "state": self.fence_state,
                    "target_version": str(_expected_migrations()[-1]["version"]),
                    "generation": 1,
                    "activated_at": "2026-08-04 00:00:00.000000",
                    "updated_at": "2026-08-04 00:00:00.000000",
                }
            ]
            if self.extra_fence_row:
                rows.append(
                    {
                        **rows[0],
                        "fence_name": "unexpected_fence",
                    }
                )
            return _Result(rows)
        if sql == "SELECT VERSION()":
            return _Result(scalar_value=self.server_version)
        if sql == "SELECT DATABASE()":
            return _Result(scalar_value="probiga_v2_evidence_test")
        if (
            "FROM information_schema.TABLES" in sql
            and "TABLE_NAME = 'schema_migration_v2'" in sql
            and "SELECT COUNT(*)" not in sql
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
        if (
            "FROM information_schema.TABLES" in sql
            and "TABLE_NAME = 'schema_migration_v2'" in sql
        ):
            return _Result(scalar_value=1)
        if "FROM information_schema.TABLES" in sql:
            rows = [
                {
                    "TABLE_NAME": table_name,
                    "ENGINE": (
                        "MyISAM"
                        if table_name == self.bad_table_engine
                        else "InnoDB"
                    ),
                    "TABLE_COLLATION": (
                        "latin1_swedish_ci"
                        if table_name == self.bad_table_collation
                        else "utf8mb4_general_ci"
                    ),
                    "ROW_FORMAT": (
                        "Compact"
                        if table_name == self.bad_row_format
                        else (
                            "Dynamic"
                            if table_name
                            in AUTHORITY_TABLES | ACCOUNTING_EVIDENCE_TABLES
                            else "Compact"
                        )
                    ),
                }
                for table_name in sorted(
                    _schema_signatures(
                        include_authority=self.include_authority,
                        include_accounting=self.include_accounting,
                    )
                )
                if table_name != self.missing_table
            ]
            return _Result(rows)
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
            rows = []
            collation_contracts = _column_collation_contracts()
            for table_name, signature in _schema_signatures(
                include_authority=self.include_authority,
                include_accounting=self.include_accounting,
            ).items():
                if table_name == self.missing_table:
                    continue
                for column_name, details in signature["columns"].items():
                    column_type = details["type"]
                    if self.bad_column_type == (table_name, column_name):
                        column_type = "varchar(255)"
                    rows.append(
                        {
                            "TABLE_NAME": table_name,
                            "COLUMN_NAME": column_name,
                            "COLUMN_TYPE": column_type,
                            "IS_NULLABLE": "YES" if details["nullable"] else "NO",
                            "COLUMN_DEFAULT": (
                                "NULL"
                                if self.literal_null_default
                                == (table_name, column_name)
                                else (
                                    None
                                    if str(details["default"]).upper() == "NULL"
                                    else details["default"]
                                )
                            ),
                            "COLLATION_NAME": (
                                "latin1_swedish_ci"
                                if self.bad_column_collation
                                == (table_name, column_name)
                                else (
                                    (
                                        contract[1]
                                        if contract[0] == "exact"
                                        else contract[1] + "general_ci"
                                    )
                                    if (
                                        contract := collation_contracts.get(
                                            table_name, {}
                                        ).get(column_name)
                                    )
                                    else None
                                )
                            ),
                        }
                    )
            return _Result(rows)
        if (
            "FROM information_schema.STATISTICS" in sql
            and "TABLE_NAME = 'schema_migration_v2'" in sql
        ):
            return _Result(
                ()
                if self.bad_ledger_primary
                else (
                    {
                        "INDEX_NAME": "PRIMARY",
                        "NON_UNIQUE": 0,
                        "SEQ_IN_INDEX": 1,
                        "COLUMN_NAME": "version",
                    },
                )
            )
        if "FROM information_schema.STATISTICS" in sql:
            rows = []
            for table_name, signature in _schema_signatures(
                include_authority=self.include_authority,
                include_accounting=self.include_accounting,
            ).items():
                if table_name == self.missing_table:
                    continue
                for index_name, details in signature["indexes"].items():
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
                                "SUB_PART": (
                                    1
                                    if self.bad_index_sub_part
                                    == (table_name, index_name)
                                    and position == 1
                                    else None
                                ),
                                "INDEX_TYPE": (
                                    "HASH"
                                    if self.bad_index_type
                                    == (table_name, index_name)
                                    else "BTREE"
                                ),
                                "COLLATION": "A",
                            }
                        )
                for implicit_number, columns in enumerate(
                    sorted(
                        _required_implicit_fk_index_tuples(
                            signature,
                            signature["indexes"],
                        )
                    ),
                    start=1,
                ):
                    index_name = f"auto_fk_{implicit_number}"
                    for position, column_name in enumerate(columns, start=1):
                        rows.append(
                            {
                                "TABLE_NAME": table_name,
                                "INDEX_NAME": index_name,
                                "NON_UNIQUE": 1,
                                "SEQ_IN_INDEX": position,
                                "COLUMN_NAME": column_name,
                                "SUB_PART": (
                                    1
                                    if self.bad_index_sub_part
                                    == (table_name, index_name)
                                    and position == 1
                                    else None
                                ),
                                "INDEX_TYPE": (
                                    "HASH"
                                    if self.bad_index_type
                                    == (table_name, index_name)
                                    else "BTREE"
                                ),
                                "COLLATION": "A",
                            }
                        )
            if self.rogue_nonunique_index is not None:
                table_name, columns = self.rogue_nonunique_index
                for position, column_name in enumerate(columns, start=1):
                    rows.append(
                        {
                            "TABLE_NAME": table_name,
                            "INDEX_NAME": "idx_rogue_nonunique",
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
            rows = []
            for table_name, signature in _schema_signatures(
                include_authority=self.include_authority,
                include_accounting=self.include_accounting,
            ).items():
                if table_name == self.missing_table:
                    continue
                for constraint_name, details in signature["foreign_keys"].items():
                    if self.missing_foreign_key == (table_name, constraint_name):
                        continue
                    for position, (column_name, referenced_column) in enumerate(
                        zip(details["columns"], details["referenced_columns"]),
                        start=1,
                    ):
                        rows.append(
                            {
                                "TABLE_NAME": table_name,
                                "CONSTRAINT_NAME": constraint_name,
                                "COLUMN_NAME": column_name,
                                "REFERENCED_TABLE_SCHEMA": (
                                    "other_schema"
                                    if self.bad_foreign_schema
                                    == (table_name, constraint_name)
                                    else "probiga_v2_evidence_test"
                                ),
                                "REFERENCED_TABLE_NAME": details[
                                    "referenced_table"
                                ],
                                "REFERENCED_COLUMN_NAME": referenced_column,
                                "ORDINAL_POSITION": position,
                                "DELETE_RULE": details["on_delete"],
                                "UPDATE_RULE": (
                                    "CASCADE"
                                    if self.bad_update_rule
                                    == (table_name, constraint_name)
                                    else details["on_update"]
                                ),
                            }
                        )
            return _Result(rows)
        if "FROM information_schema.TRIGGERS" in sql:
            trigger_contracts = _trigger_contracts(
                include_authority=self.include_authority,
                include_accounting=self.include_accounting,
            )
            trigger_bodies = _trigger_bodies(
                include_authority=self.include_authority,
                include_accounting=self.include_accounting,
            )
            action_orders, _ = _trigger_action_order_contracts(
                trigger_contracts
            )
            rows = [
                {
                    "TRIGGER_NAME": trigger_name,
                    "EVENT_OBJECT_TABLE": table_name,
                    "ACTION_TIMING": "BEFORE",
                    "EVENT_MANIPULATION": event,
                    "ACTION_STATEMENT": (
                        "BEGIN SIGNAL SQLSTATE '45000'; END"
                        if trigger_name == self.drift_trigger_body
                        else trigger_bodies[trigger_name]
                    ),
                    "ACTION_ORDER": (
                        action_orders[trigger_name] + 1
                        if trigger_name == self.bad_trigger_action_order
                        else action_orders[trigger_name]
                    ),
                    "SQL_MODE": (
                        "NO_ENGINE_SUBSTITUTION"
                        if self.unsafe_trigger_sql_mode
                        else "STRICT_TRANS_TABLES,NO_ZERO_DATE,"
                        "NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO"
                    ),
                    "DEFINER": "probiga_v2_migrator@localhost",
                    "CHARACTER_SET_CLIENT": "utf8mb4",
                    "COLLATION_CONNECTION": "utf8mb4_general_ci",
                    "DATABASE_COLLATION": "utf8mb4_general_ci",
                }
                for trigger_name, (event, table_name) in sorted(
                    trigger_contracts.items()
                )
                if f"'{table_name}'" in sql
                if trigger_name != self.missing_trigger
                and table_name != self.missing_table
            ]
            if self.extra_trigger:
                rows.append(
                    {
                        "TRIGGER_NAME": "trg_unreviewed_evidence_guard",
                        "EVENT_OBJECT_TABLE": sorted(EVIDENCE_TABLES)[0],
                        "ACTION_TIMING": "BEFORE",
                        "EVENT_MANIPULATION": "UPDATE",
                        "ACTION_STATEMENT": "SIGNAL SQLSTATE '45000'",
                    }
                )
            return _Result(rows)
        if "FROM schema_migration_v2" in sql:
            rows = []
            for migration in _expected_migrations():
                version = str(migration["version"])
                checksum = _checksum(tuple(migration["statements"]))
                if version == self.bad_checksum:
                    checksum = "0" * 64
                rows.append({"version": version, "checksum": checksum})
            return _Result(rows)
        raise AssertionError(f"unexpected SQL: {sql}")


def test_schema_gate_accepts_exact_declared_structure_but_not_activation():
    connection = _SchemaConnection()

    report = inspect_v2_execution_evidence_schema(connection)

    assert report.metadata_preflight_passed is True
    assert report.schema_ready is False
    assert report.structural_blockers == ()
    assert report.production_activation_allowed is False
    assert report.activation_blockers == (
        "ISOLATED_MYSQL_BEHAVIORAL_ACCEPTANCE_MISSING",
        "LEAST_PRIVILEGE_ATTESTATION_MISSING",
        "EVIDENCE_WRITER_NOT_PRODUCTION_WIRED",
        "CANONICAL_HASH_NOT_DATABASE_RECOMPUTABLE",
    )
    assert report.guards_checked is True
    assert report.migration_ledger_checked is True
    assert report.activation_checks_included is True
    assert report.canonical_hash_audit_passed is False
    assert report.phase_scoped_migration_replay is False
    assert report.maintenance_fence_checked is True
    assert report.maintenance_fence_active is False
    assert report.actionable_output_allowed is False
    assert set(report.observed_tables) == set(
        EVIDENCE_TABLES | AUTHORITY_TABLES | ACCOUNTING_EVIDENCE_TABLES
    )
    assert len(report.observed_tables) == 13
    assert len(report.observed_triggers) == len(_trigger_contracts()) == 41
    index_inspection = next(
        sql
        for sql in connection.statements
        if "FROM information_schema.STATISTICS" in sql
        and "schema_migration_v2" not in sql
    )
    for metadata_column in ("SUB_PART", "INDEX_TYPE", "COLLATION"):
        assert metadata_column in index_inspection
    trigger_inspection = next(
        sql
        for sql in connection.statements
        if "FROM information_schema.TRIGGERS" in sql
    )
    assert "ACTION_ORDER" in trigger_inspection


def test_trigger_action_order_freezes_authority_after_base_guard():
    contracts = _trigger_contracts()
    action_orders, references = _trigger_action_order_contracts(contracts)

    assert action_orders["trg_market_calendar_evidence_v2_guard_bi"] == 1
    assert action_orders["trg_market_calendar_evidence_v2_authority_bi"] == 2
    assert action_orders["trg_quote_receipt_evidence_v2_guard_bi"] == 1
    assert action_orders["trg_quote_receipt_evidence_v2_authority_bi"] == 2
    assert references["trg_market_calendar_evidence_v2_authority_bi"] == (
        "FOLLOWS",
        "trg_market_calendar_evidence_v2_guard_bi",
    )
    assert references["trg_quote_receipt_evidence_v2_authority_bi"] == (
        "FOLLOWS",
        "trg_quote_receipt_evidence_v2_guard_bi",
    )


def test_schema_gate_rejects_trigger_action_order_drift():
    trigger_name = "trg_market_calendar_evidence_v2_authority_bi"
    report = inspect_v2_execution_evidence_schema(
        _SchemaConnection(bad_trigger_action_order=trigger_name)
    )

    assert f"TRIGGER_ACTION_ORDER_DRIFTED:{trigger_name}" in (
        report.structural_blockers
    )


def test_public_gate_blocks_active_fence_but_replay_can_validate_it():
    public_report = inspect_v2_execution_evidence_schema(
        _SchemaConnection(fence_state=V2_EVIDENCE_MAINTENANCE_FENCE_ACTIVE)
    )
    replay_report = inspect_v2_execution_evidence_schema(
        _SchemaConnection(fence_state=V2_EVIDENCE_MAINTENANCE_FENCE_ACTIVE),
        maintenance_fence_expected_active=True,
        include_activation_blockers=False,
    )

    assert "MAINTENANCE_FENCE_ACTIVE" in public_report.structural_blockers
    assert public_report.maintenance_fence_active is True
    assert public_report.schema_ready is False
    assert replay_report.structural_blockers == ()
    assert replay_report.maintenance_fence_checked is True
    assert replay_report.maintenance_fence_active is True
    assert replay_report.schema_ready is False


@pytest.mark.parametrize(
    ("connection", "blocker"),
    (
        (
            _SchemaConnection(fence_present=False),
            "MAINTENANCE_FENCE_TABLE_MISSING",
        ),
        (
            _SchemaConnection(bad_fence_column="state"),
            "MAINTENANCE_FENCE_COLUMNS_DRIFTED",
        ),
        (
            _SchemaConnection(bad_fence_index=True),
            "MAINTENANCE_FENCE_INDEX_DRIFTED",
        ),
        (
            _SchemaConnection(extra_fence_row=True),
            "MAINTENANCE_FENCE_ROW_SET_DRIFTED",
        ),
    ),
)
def test_schema_gate_fails_closed_on_fence_contract_drift(connection, blocker):
    report = inspect_v2_execution_evidence_schema(connection)

    assert blocker in report.structural_blockers
    assert report.schema_ready is False


def test_writer_fence_helper_holds_shared_lock_in_same_transaction():
    connection = _SchemaConnection()

    assert_v2_evidence_maintenance_fence_inactive(connection)

    assert connection.statements[-1].endswith("LOCK IN SHARE MODE")
    with pytest.raises(V2EvidenceMaintenanceFenceError, match="active or invalid"):
        assert_v2_evidence_maintenance_fence_inactive(
            _SchemaConnection(
                fence_state=V2_EVIDENCE_MAINTENANCE_FENCE_ACTIVE
            )
        )
    outside_transaction = _SchemaConnection(transaction_active=False)
    with pytest.raises(V2EvidenceMaintenanceFenceError, match="writer transaction"):
        assert_v2_evidence_maintenance_fence_inactive(outside_transaction)
    assert outside_transaction.statements == []


def test_schema_gate_models_all_mysql_implicit_child_fk_indexes():
    signatures = _schema_signatures()
    assert sum(
        len(
            _required_implicit_fk_index_tuples(
                signature,
                signature["indexes"],
            )
        )
        for signature in signatures.values()
    ) == 25


def test_real_canonical_hash_audit_flag_removes_only_its_blocker():
    report = inspect_v2_execution_evidence_schema(
        _SchemaConnection(),
        canonical_hash_audit_passed=True,
    )

    assert report.canonical_hash_audit_passed is True
    assert "CANONICAL_HASH_NOT_DATABASE_RECOMPUTABLE" not in (
        report.activation_blockers
    )
    assert report.activation_blockers == (
        "ISOLATED_MYSQL_BEHAVIORAL_ACCEPTANCE_MISSING",
        "LEAST_PRIVILEGE_ATTESTATION_MISSING",
        "EVIDENCE_WRITER_NOT_PRODUCTION_WIRED",
    )
    assert report.schema_ready is False
    assert report.production_activation_allowed is False
    assert report.actionable_output_allowed is False


@pytest.mark.parametrize(
    "missing_table",
    (
        "st_execution_authority_attestation_v2",
        "st_fill_accounting_outcome_finalization_v2",
    ),
)
def test_complete_ledger_cannot_mask_a_deleted_014_or_015_table(missing_table):
    report = inspect_v2_execution_evidence_schema(
        _SchemaConnection(missing_table=missing_table)
    )

    assert f"TABLE_MISSING:{missing_table}" in report.structural_blockers
    assert not any(
        blocker.startswith("MIGRATION_LEDGER_MISSING:")
        for blocker in report.structural_blockers
    )
    assert report.production_activation_allowed is False
    assert report.actionable_output_allowed is False


@pytest.mark.parametrize(
    ("kwargs", "blocker_prefix"),
    (
        (
            {"missing_table": "st_fill_execution_evidence_v2"},
            "TABLE_MISSING:st_fill_execution_evidence_v2",
        ),
        (
            {"missing_table": "st_execution_authority_receipt_v2"},
            "TABLE_MISSING:st_execution_authority_receipt_v2",
        ),
        (
            {"missing_table": "st_fill_accounting_outcome_finalization_v2"},
            "TABLE_MISSING:st_fill_accounting_outcome_finalization_v2",
        ),
        (
            {"missing_trigger": "__FIRST__"},
            "TRIGGER_MISSING:",
        ),
        (
            {
                "missing_trigger": (
                    "trg_execution_authority_receipt_revocation_v2_guard_bd"
                )
            },
            "TRIGGER_MISSING:trg_execution_authority_receipt_revocation_v2_guard_bd",
        ),
        (
            {
                "missing_trigger": (
                    "trg_fill_accounting_finalization_v2_guard_bd"
                )
            },
            "TRIGGER_MISSING:trg_fill_accounting_finalization_v2_guard_bd",
        ),
        (
            {"bad_checksum": "__LAST__"},
            "MIGRATION_CHECKSUM_DRIFTED:",
        ),
        (
            {"extra_trigger": True},
            "UNDECLARED_EVIDENCE_TRIGGER:",
        ),
        (
            {"server_version": "5.6.51-log"},
            "MYSQL_VERSION_NOT_VALIDATED",
        ),
    ),
)
def test_schema_gate_fails_closed_on_structural_drift(kwargs, blocker_prefix):
    values = dict(kwargs)
    if values.get("missing_trigger") == "__FIRST__":
        values["missing_trigger"] = sorted(_guard_trigger_contracts())[0]
    if values.get("bad_checksum") == "__LAST__":
        values["bad_checksum"] = str(_expected_migrations()[-1]["version"])
    report = inspect_v2_execution_evidence_schema(_SchemaConnection(**values))

    assert report.metadata_preflight_passed is False
    assert report.schema_ready is False
    assert any(
        item.startswith(blocker_prefix) for item in report.structural_blockers
    )
    assert report.production_activation_allowed is False


@pytest.mark.parametrize(
    ("kwargs", "blocker"),
    (
        (
            {
                "bad_column_type": (
                    "st_fill_execution_evidence_v2",
                    "fill_execution_evidence_id",
                )
            },
            "TABLE_COLUMNS_DRIFTED:st_fill_execution_evidence_v2",
        ),
        (
            {
                "bad_column_type": (
                    "st_execution_authority_attestation_v2",
                    "claim_hash",
                )
            },
            "TABLE_COLUMNS_DRIFTED:st_execution_authority_attestation_v2",
        ),
        (
            {
                "bad_column_type": (
                    "st_fill_accounting_outcome_finalization_v2",
                    "finalization_status",
                )
            },
            "TABLE_COLUMNS_DRIFTED:st_fill_accounting_outcome_finalization_v2",
        ),
        (
            {
                "bad_column_collation": (
                    "st_execution_authority_trust_key_v2",
                    "source_provider",
                )
            },
            "COLUMN_COLLATION_INVALID:st_execution_authority_trust_key_v2.source_provider",
        ),
        (
            {"bad_row_format": "st_execution_authority_receipt_v2"},
            "TABLE_ROW_FORMAT_INVALID:st_execution_authority_receipt_v2",
        ),
        (
            {"bad_row_format": "st_fill_accounting_outcome_v2"},
            "TABLE_ROW_FORMAT_INVALID:st_fill_accounting_outcome_v2",
        ),
        (
            {"bad_table_engine": "st_execution_authority_trust_key_v2"},
            "TABLE_ENGINE_INVALID:st_execution_authority_trust_key_v2",
        ),
        (
            {"bad_table_collation": "st_fill_accounting_outcome_v2"},
            "TABLE_COLLATION_INVALID:st_fill_accounting_outcome_v2",
        ),
        (
            {
                "missing_index": (
                    "st_cash_event_binding_v2",
                    "uk_cash_binding_v2_sequence",
                )
            },
            "TABLE_INDEX_DRIFTED:st_cash_event_binding_v2.uk_cash_binding_v2_sequence",
        ),
        (
            {
                "missing_index": (
                    "st_market_calendar_evidence_v2",
                    "uk_calendar_evidence_v2_natural",
                )
            },
            "TABLE_INDEX_DRIFTED:st_market_calendar_evidence_v2.uk_calendar_evidence_v2_natural",
        ),
        (
            {
                "missing_index": (
                    "st_execution_authority_attestation_v2",
                    "uk_authority_attestation_v2_hash",
                )
            },
            "TABLE_INDEX_DRIFTED:st_execution_authority_attestation_v2.uk_authority_attestation_v2_hash",
        ),
        (
            {
                "missing_index": (
                    "st_fill_accounting_outcome_finalization_v2",
                    "uk_fill_accounting_finalization_v2_hash",
                )
            },
            "TABLE_INDEX_DRIFTED:st_fill_accounting_outcome_finalization_v2.uk_fill_accounting_finalization_v2_hash",
        ),
        (
            {
                "rogue_nonunique_index": (
                    "st_market_calendar_evidence_v2",
                    ("payload_hash",),
                )
            },
            "TABLE_IMPLICIT_FK_INDEX_SET_DRIFTED:st_market_calendar_evidence_v2",
        ),
        (
            {
                "bad_index_sub_part": (
                    "st_cash_event_binding_v2",
                    "auto_fk_1",
                )
            },
            "TABLE_IMPLICIT_FK_INDEX_SET_DRIFTED:st_cash_event_binding_v2",
        ),
        (
            {
                "bad_index_type": (
                    "st_market_calendar_evidence_v2",
                    "PRIMARY",
                )
            },
            "TABLE_INDEX_DRIFTED:st_market_calendar_evidence_v2.PRIMARY",
        ),
        (
            {
                "missing_foreign_key": (
                    "st_order_transition_v2",
                    "fk_order_transition_v2_order",
                )
            },
            "TABLE_FOREIGN_KEYS_DRIFTED:st_order_transition_v2",
        ),
        (
            {
                "missing_foreign_key": (
                    "st_execution_authority_attestation_v2",
                    "fk_authority_attestation_v2_receipt",
                )
            },
            "TABLE_FOREIGN_KEYS_DRIFTED:st_execution_authority_attestation_v2",
        ),
        (
            {
                "missing_foreign_key": (
                    "st_fill_accounting_outcome_finalization_v2",
                    "fk_fill_accounting_finalization_v2_outcome",
                )
            },
            "TABLE_FOREIGN_KEYS_DRIFTED:st_fill_accounting_outcome_finalization_v2",
        ),
        (
            {
                "drift_trigger_body": (
                    "trg_quote_receipt_evidence_v2_guard_bi"
                )
            },
            "TRIGGER_BODY_DRIFTED:trg_quote_receipt_evidence_v2_guard_bi",
        ),
        (
            {
                "drift_trigger_body": (
                    "trg_execution_authority_attestation_v2_guard_bi"
                )
            },
            "TRIGGER_BODY_DRIFTED:trg_execution_authority_attestation_v2_guard_bi",
        ),
        (
            {
                "drift_trigger_body": (
                    "trg_fill_accounting_finalization_v2_guard_bi"
                )
            },
            "TRIGGER_BODY_DRIFTED:trg_fill_accounting_finalization_v2_guard_bi",
        ),
        (
            {"server_version": "10.11.6-MariaDB"},
            "MYSQL_VERSION_NOT_VALIDATED",
        ),
        (
            {"server_version": "8.0.39"},
            "MYSQL_VERSION_NOT_VALIDATED",
        ),
        (
            {
                "bad_foreign_schema": (
                    "st_order_transition_v2",
                    "fk_order_transition_v2_order",
                )
            },
            "FOREIGN_KEY_SCHEMA_DRIFTED:st_order_transition_v2.fk_order_transition_v2_order",
        ),
        (
            {
                "bad_update_rule": (
                    "st_cash_event_binding_v2",
                    "fk_cash_binding_v2_event",
                )
            },
            "TABLE_FOREIGN_KEYS_DRIFTED:st_cash_event_binding_v2",
        ),
        (
            {"unsafe_trigger_sql_mode": True},
            "TRIGGER_SQL_MODE_UNSAFE:trg_market_calendar_evidence_v2_guard_bd",
        ),
        (
            {
                "literal_null_default": (
                    "st_market_calendar_evidence_v2",
                    "source_receipt_id",
                )
            },
            "TABLE_COLUMNS_DRIFTED:st_market_calendar_evidence_v2",
        ),
        (
            {
                "literal_null_default": (
                    "st_execution_authority_trust_key_v2",
                    "valid_to",
                )
            },
            "TABLE_COLUMNS_DRIFTED:st_execution_authority_trust_key_v2",
        ),
        (
            {
                "literal_null_default": (
                    "st_fill_accounting_outcome_v2",
                    "authority_receipt_hash",
                )
            },
            "TABLE_COLUMNS_DRIFTED:st_fill_accounting_outcome_v2",
        ),
        (
            {"bad_ledger_primary": True},
            "MIGRATION_LEDGER_INDEX_DRIFTED",
        ),
    ),
)
def test_schema_gate_checks_independent_metadata_dimensions(kwargs, blocker):
    report = inspect_v2_execution_evidence_schema(_SchemaConnection(**kwargs))

    assert blocker in report.structural_blockers
    assert report.metadata_preflight_passed is False


def test_schema_gate_rejects_non_mysql_before_any_query():
    connection = _SchemaConnection(dialect="sqlite")

    with pytest.raises(V2EvidenceSchemaInspectionError, match="MySQL"):
        inspect_v2_execution_evidence_schema(connection)

    assert connection.statements == []


def test_default_null_contract_matches_mysql_information_schema_null():
    signature = _binding_schema_signature()
    assert (
        signature["st_market_calendar_evidence_v2"]["columns"]
        ["source_receipt_id"]["default"]
        is None
    )
    assert _normalize_declared_default("NULL") is None
    assert _normalize_declared_default("'NULL'") == "NULL"
    assert _normalize_observed_default(None) is None
    assert _normalize_observed_default("NULL") == "NULL"


def test_phase_scoped_inspection_cannot_report_global_schema_ready():
    connection = _SchemaConnection()

    report = inspect_v2_execution_evidence_schema(
        connection,
        require_guards=False,
        require_migration_ledger=False,
        include_activation_blockers=False,
    )

    assert report.metadata_preflight_passed is True
    assert report.schema_ready is False
    assert report.guards_checked is False
    assert report.migration_ledger_checked is False
    assert report.activation_checks_included is False
    assert not any("information_schema.TRIGGERS" in sql for sql in connection.statements)
    assert not any(
        "TABLE_NAME = 'schema_migration_v2'" in sql
        or "FROM schema_migration_v2 WHERE version" in sql
        for sql in connection.statements
    )


def test_pre_013_phase_can_omit_natural_key_but_default_gate_requires_it():
    missing = (
        "st_market_calendar_evidence_v2",
        "uk_calendar_evidence_v2_natural",
    )
    phase_report = inspect_v2_execution_evidence_schema(
        _SchemaConnection(missing_index=missing),
        require_guards=False,
        require_natural_keys=False,
        require_migration_ledger=False,
        include_activation_blockers=False,
    )
    final_report = inspect_v2_execution_evidence_schema(
        _SchemaConnection(missing_index=missing)
    )

    assert phase_report.structural_blockers == ()
    assert (
        "TABLE_INDEX_DRIFTED:st_market_calendar_evidence_v2."
        "uk_calendar_evidence_v2_natural"
    ) in final_report.structural_blockers


def test_pre_014_phase_can_omit_extensions_but_default_gate_requires_them():
    pre_014 = _SchemaConnection(
        include_authority=False,
        include_accounting=False,
    )

    phase_report = inspect_v2_execution_evidence_schema(
        pre_014,
        require_migration_ledger=False,
        require_authority_attestations=False,
        require_accounting_evidence=False,
        include_activation_blockers=False,
    )
    default_report = inspect_v2_execution_evidence_schema(
        _SchemaConnection(
            include_authority=False,
            include_accounting=False,
        )
    )

    assert phase_report.structural_blockers == ()
    assert any(
        item.startswith("TABLE_MISSING:st_execution_authority_")
        for item in default_report.structural_blockers
    )
    assert (
        "TABLE_MISSING:st_fill_accounting_outcome_finalization_v2"
        in default_report.structural_blockers
    )


@pytest.mark.parametrize(
    ("connection", "missing_table"),
    (
        (
            _SchemaConnection(include_accounting=False),
            "st_execution_authority_receipt_v2",
        ),
        (
            _SchemaConnection(),
            "st_fill_accounting_outcome_finalization_v2",
        ),
    ),
)
def test_forward_replay_objects_are_exactly_validated_when_flags_are_false(
    connection,
    missing_table,
):
    connection.missing_table = missing_table

    report = inspect_v2_execution_evidence_schema(
        connection,
        require_migration_ledger=False,
        require_authority_attestations=False,
        require_accounting_evidence=False,
        include_activation_blockers=False,
    )

    assert f"TABLE_MISSING:{missing_table}" in report.structural_blockers
    assert report.production_activation_allowed is False
    assert report.actionable_output_allowed is False


def test_phase_scoped_migration_replay_can_resume_a_partial_future_layer():
    connection = _SchemaConnection(
        missing_table="st_execution_authority_receipt_v2",
    )

    report = inspect_v2_execution_evidence_schema(
        connection,
        require_migration_ledger=False,
        require_authority_attestations=False,
        require_accounting_evidence=False,
        phase_scoped_migration_replay=True,
        include_activation_blockers=False,
    )

    assert report.structural_blockers == ()
    assert report.phase_scoped_migration_replay is True
    assert report.production_activation_allowed is False
    assert report.actionable_output_allowed is False


@pytest.mark.parametrize("value", (1, "true", None))
def test_require_natural_keys_must_be_exact_bool(value):
    connection = _SchemaConnection()
    with pytest.raises(TypeError, match="require_natural_keys must be bool"):
        inspect_v2_execution_evidence_schema(
            connection,
            require_natural_keys=value,
        )
    assert connection.statements == []


@pytest.mark.parametrize("value", (1, "true", None))
def test_require_authority_attestations_must_be_exact_bool(value):
    connection = _SchemaConnection()
    with pytest.raises(
        TypeError,
        match="require_authority_attestations must be bool",
    ):
        inspect_v2_execution_evidence_schema(
            connection,
            require_authority_attestations=value,
        )
    assert connection.statements == []


@pytest.mark.parametrize("value", (1, "true", None))
def test_require_accounting_evidence_must_be_exact_bool(value):
    connection = _SchemaConnection()
    with pytest.raises(
        TypeError,
        match="require_accounting_evidence must be bool",
    ):
        inspect_v2_execution_evidence_schema(
            connection,
            require_accounting_evidence=value,
        )
    assert connection.statements == []


@pytest.mark.parametrize("value", (1, "true", None))
def test_phase_scoped_migration_replay_must_be_exact_bool(value):
    connection = _SchemaConnection()
    with pytest.raises(
        TypeError,
        match="phase_scoped_migration_replay must be bool",
    ):
        inspect_v2_execution_evidence_schema(
            connection,
            phase_scoped_migration_replay=value,
        )
    assert connection.statements == []


@pytest.mark.parametrize("value", (1, "true", None))
def test_maintenance_fence_expected_active_must_be_exact_bool(value):
    connection = _SchemaConnection()
    with pytest.raises(
        TypeError,
        match="maintenance_fence_expected_active must be bool",
    ):
        inspect_v2_execution_evidence_schema(
            connection,
            maintenance_fence_expected_active=value,
        )
    assert connection.statements == []


@pytest.mark.parametrize("value", (1, "true", None))
def test_canonical_hash_audit_passed_must_be_exact_bool(value):
    connection = _SchemaConnection()
    with pytest.raises(
        TypeError,
        match="canonical_hash_audit_passed must be bool",
    ):
        inspect_v2_execution_evidence_schema(
            connection,
            canonical_hash_audit_passed=value,
        )
    assert connection.statements == []


def test_schema_gate_source_is_strictly_read_only_and_connection_owned():
    source = (
        PROJECT_ROOT
        / "server/trading_v2/execution_evidence_schema_gate.py"
    ).read_text(encoding="utf-8")

    assert "create_engine" not in source
    assert ".begin(" not in source
    assert ".commit(" not in source
    assert ".rollback(" not in source
    assert not re.search(
        r"\b(?:INSERT\s+INTO|UPDATE\s+[a-z0-9_]+|"
        r"DELETE\s+FROM|CREATE\s+TABLE|DROP\s+TABLE|ALTER\s+TABLE)\b",
        source,
        flags=re.IGNORECASE,
    )
