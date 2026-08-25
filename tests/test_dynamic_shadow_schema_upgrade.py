# -*- coding: utf-8 -*-
"""MySQL-information_schema tests for the dynamic-ledger schema upgrader."""
from __future__ import annotations

from copy import deepcopy
import re

import pytest

from server.engine import dynamic_shadow_ledger_schema as schema


class _Result:
    def __init__(self, rows=()):
        self._rows = [dict(row) for row in rows]

    def mappings(self):
        return self

    def all(self):
        return list(self._rows)

    def first(self):
        return self._rows[0] if self._rows else None


class _MySQLSchemaConnection:
    """Stateful information_schema fake; it never emulates FK ALTER in SQLite."""

    def __init__(self, existing_tables=()):
        self.tables: list[dict] = []
        self.columns: list[dict] = []
        self.indexes: list[dict] = []
        self.foreign_keys: list[dict] = []
        self.checks: list[dict] = []
        self.row_counts = {
            table_name: 0
            for table_name in schema.DYNAMIC_SHADOW_LEDGER_TABLE_NAMES
        }
        self.ddl_statements: list[str] = []
        self.count_queries: list[str] = []
        for table_name in existing_tables:
            self.install_exact_table(table_name)

    def install_exact_table(self, table_name):
        self.tables = [
            row for row in self.tables if row["table_name"] != table_name
        ]
        self.columns = [
            row for row in self.columns if row["table_name"] != table_name
        ]
        self.indexes = [
            row for row in self.indexes if row["table_name"] != table_name
        ]
        self.foreign_keys = [
            row for row in self.foreign_keys
            if row["table_name"] != table_name
        ]
        self.checks = [
            row for row in self.checks if row["table_name"] != table_name
        ]
        self.tables.append({
            "table_name": table_name,
            "engine": "InnoDB",
            "table_collation": "utf8mb4_unicode_ci",
        })
        for ordinal, expected in enumerate(
            schema._DYNAMIC_COLUMN_CONTRACTS[table_name], 1
        ):
            self.columns.append({
                "table_name": table_name,
                "column_name": expected["name"],
                "ordinal_position": ordinal,
                "column_type": expected["column_type"],
                "is_nullable": expected["is_nullable"],
                "column_default": expected["column_default"],
                "extra": expected["extra"],
                "character_set_name": expected["character_set_name"],
                "collation_name": expected["collation_name"],
            })
        for index_name, (unique, columns, _ddl) in (
            schema._DYNAMIC_INDEX_CONTRACTS[table_name].items()
        ):
            for sequence, column_name in enumerate(columns, 1):
                self.indexes.append({
                    "table_name": table_name,
                    "index_name": index_name,
                    "non_unique": 0 if unique else 1,
                    "seq_in_index": sequence,
                    "column_name": column_name,
                    "sub_part": None,
                    "index_type": "BTREE",
                })
        for constraint_name, contract in (
            schema._DYNAMIC_FOREIGN_KEY_CONTRACTS.items()
        ):
            (
                child_table, columns, parent_table, parent_columns,
                update_rule, delete_rule,
            ) = contract
            if child_table != table_name:
                continue
            for ordinal, (column_name, parent_column) in enumerate(
                zip(columns, parent_columns), 1
            ):
                self.foreign_keys.append({
                    "table_name": table_name,
                    "constraint_name": constraint_name,
                    "ordinal_position": ordinal,
                    "column_name": column_name,
                    "referenced_table_name": parent_table,
                    "referenced_column_name": parent_column,
                    "update_rule": update_rule,
                    "delete_rule": delete_rule,
                })
        for constraint_name, (child_table, clause) in (
            schema._DYNAMIC_CHECK_CONTRACTS.items()
        ):
            if child_table == table_name:
                self.checks.append({
                    "table_name": table_name,
                    "constraint_name": constraint_name,
                    "check_clause": f"(({clause}))",
                })

    def remove_column(self, table_name, column_name):
        self.columns = [
            row for row in self.columns
            if not (
                row["table_name"] == table_name
                and row["column_name"] == column_name
            )
        ]
        rows = [
            row for row in self.columns if row["table_name"] == table_name
        ]
        rows.sort(key=lambda row: row["ordinal_position"])
        for ordinal, row in enumerate(rows, 1):
            row["ordinal_position"] = ordinal

    def remove_index(self, table_name, index_name):
        self.indexes = [
            row for row in self.indexes
            if not (
                row["table_name"] == table_name
                and row["index_name"] == index_name
            )
        ]

    def remove_foreign_key(self, constraint_name):
        self.foreign_keys = [
            row for row in self.foreign_keys
            if row["constraint_name"] != constraint_name
        ]

    def remove_check(self, constraint_name):
        self.checks = [
            row for row in self.checks
            if row["constraint_name"] != constraint_name
        ]

    def execute(self, statement, _params=None):
        sql = str(statement).strip()
        lowered = sql.casefold()
        if "from information_schema.tables" in lowered:
            return _Result(deepcopy(self.tables))
        if "from information_schema.columns" in lowered:
            return _Result(deepcopy(self.columns))
        if "from information_schema.statistics" in lowered:
            return _Result(deepcopy(self.indexes))
        if (
            "information_schema.table_constraints" in lowered
            and "information_schema.referential_constraints" in lowered
        ):
            return _Result(deepcopy(self.foreign_keys))
        if (
            "information_schema.table_constraints" in lowered
            and "information_schema.check_constraints" in lowered
        ):
            return _Result(deepcopy(self.checks))
        count_match = re.fullmatch(
            r"select count\(\*\) as row_count from `([a-z0-9_]+)`",
            lowered,
        )
        if count_match:
            table_name = count_match.group(1)
            self.count_queries.append(table_name)
            return _Result([{"row_count": self.row_counts[table_name]}])
        create_match = re.search(
            r"create\s+table\s+if\s+not\s+exists\s+([a-z0-9_]+)",
            lowered,
        )
        alter_match = re.match(r"alter\s+table\s+`([a-z0-9_]+)`", lowered)
        if create_match or alter_match:
            table_name = (
                create_match.group(1) if create_match else alter_match.group(1)
            )
            self.ddl_statements.append(sql)
            # The fake reflects MySQL's post-DDL information_schema.  Tests
            # separately inspect the generated ALTER clauses themselves.
            self.install_exact_table(table_name)
            return _Result()
        raise AssertionError(f"unexpected schema SQL: {sql}")


def _exact_connection():
    return _MySQLSchemaConnection(schema.DYNAMIC_SHADOW_LEDGER_TABLE_NAMES)


def _row(rows, **identity):
    return next(
        row for row in rows
        if all(row.get(key) == value for key, value in identity.items())
    )


def test_read_only_validator_reports_dynamic_scope_counts_and_contract_hash():
    connection = _exact_connection()

    result = schema.validate_dynamic_shadow_ledger_schema(connection)

    assert result == {
        "scope": "dynamic_shadow_ledger",
        "table_count": 4,
        "column_count": 57,
        "index_count": 22,
        "foreign_key_count": 15,
        "check_count": 10,
        "contract_hash": schema.DYNAMIC_SHADOW_LEDGER_SCHEMA_CONTRACT_HASH,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    assert result["contract_hash"] == (
        "25dd0ffab488c22f628a8f8248521ff8e4f337931cfffe856cd069670006d49c"
    )
    assert re.fullmatch(r"[0-9a-f]{64}", result["contract_hash"])
    assert connection.ddl_statements == []


def test_every_foreign_key_has_an_explicit_left_prefix_supporting_index():
    ddl_by_table = dict(zip(
        schema.DYNAMIC_SHADOW_LEDGER_TABLE_NAMES,
        schema.dynamic_shadow_ledger_ddl_statements(),
    ))
    for table_name, indexes in schema._DYNAMIC_INDEX_CONTRACTS.items():
        normalized_ddl = re.sub(r"\s+", " ", ddl_by_table[table_name].casefold())
        for index_name in indexes:
            if index_name == "PRIMARY":
                assert "primary key" in normalized_ddl
            else:
                assert re.search(
                    rf"\b(?:unique\s+)?key\s+{re.escape(index_name.casefold())}\b",
                    normalized_ddl,
                )

    for (
        _constraint_name,
        (child_table, child_columns, *_rest),
    ) in schema._DYNAMIC_FOREIGN_KEY_CONTRACTS.items():
        assert any(
            tuple(index_columns[:len(child_columns)]) == tuple(child_columns)
            for _unique, index_columns, _ddl in (
                schema._DYNAMIC_INDEX_CONTRACTS[child_table].values()
            )
        ), f"{child_table}.{child_columns} would make InnoDB add an implicit index"


def test_read_only_validator_rejects_incomplete_schema_without_mutation():
    connection = _exact_connection()
    connection.remove_foreign_key("fk_dynamic_shadow_chain_risk")

    with pytest.raises(RuntimeError, match="incomplete"):
        schema.validate_dynamic_shadow_ledger_schema(connection)

    assert connection.count_queries == []
    assert connection.ddl_statements == []


def test_read_only_upgrade_preflight_classifies_absent_exact_and_empty_legacy():
    absent = _MySQLSchemaConnection()
    exact = _exact_connection()
    legacy = _exact_connection()
    legacy.remove_column(
        "st_dynamic_shadow_trial_chain", "risk_decision_fact_hash"
    )

    absent_result = schema.preflight_dynamic_shadow_ledger_schema_upgrade(
        absent
    )
    exact_result = schema.preflight_dynamic_shadow_ledger_schema_upgrade(exact)
    legacy_result = schema.preflight_dynamic_shadow_ledger_schema_upgrade(
        legacy
    )

    assert absent_result["status"] == "ABSENT_CREATE_ALLOWED"
    assert exact_result["status"] == "EXACT"
    assert legacy_result["status"] == "EMPTY_ADDITIVE_UPGRADE_ALLOWED"
    assert legacy_result["missing_object_count"] == 1
    assert absent.ddl_statements == exact.ddl_statements == legacy.ddl_statements == []


def test_read_only_upgrade_preflight_rejects_nonempty_incomplete_schema():
    connection = _exact_connection()
    connection.remove_foreign_key("fk_dynamic_shadow_chain_risk")
    connection.row_counts["st_dynamic_shadow_trial_plan"] = 1

    with pytest.raises(RuntimeError, match="切换窗口"):
        schema.preflight_dynamic_shadow_ledger_schema_upgrade(connection)

    assert connection.ddl_statements == []


def test_fresh_schema_is_created_in_dependency_order_then_is_idempotent():
    connection = _MySQLSchemaConnection()

    created = schema.ensure_dynamic_shadow_ledger_schema(
        connection, writers_fenced=True,
    )

    assert created["upgrade_status"] == "CREATED"
    assert created["executed_statement_count"] == 4
    assert [
        re.search(
            r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+([a-z0-9_]+)",
            statement,
            flags=re.IGNORECASE,
        ).group(1)
        for statement in connection.ddl_statements
    ] == list(schema.DYNAMIC_SHADOW_LEDGER_TABLE_NAMES)

    ddl_count = len(connection.ddl_statements)
    unchanged = schema.ensure_dynamic_shadow_ledger_schema(
        connection, writers_fenced=True,
    )
    assert unchanged["upgrade_status"] == "UNCHANGED"
    assert unchanged["executed_statement_count"] == 0
    assert len(connection.ddl_statements) == ddl_count


def test_empty_legacy_chain_receives_only_additive_column_index_fk_and_check():
    connection = _exact_connection()
    table_name = "st_dynamic_shadow_trial_chain"
    connection.remove_column(table_name, "risk_decision_fact_hash")
    connection.remove_index(table_name, "idx_dynamic_shadow_chain_intent")
    connection.remove_foreign_key("fk_dynamic_shadow_chain_risk")
    connection.remove_check("ck_dynamic_shadow_chain_no_real_auto")

    result = schema.ensure_dynamic_shadow_ledger_schema(
        connection, writers_fenced=True,
    )

    assert result["upgrade_status"] == "UPGRADED"
    assert result["executed_statement_count"] == 1
    alter = connection.ddl_statements[0]
    assert alter.startswith("ALTER TABLE `st_dynamic_shadow_trial_chain`")
    assert (
        "ADD COLUMN risk_decision_fact_hash CHAR(64) NOT NULL "
        "AFTER `intent_fact_hash`"
    ) in alter
    assert "ADD KEY idx_dynamic_shadow_chain_intent" in alter
    assert "ADD CONSTRAINT `fk_dynamic_shadow_chain_risk`" in alter
    assert "ON UPDATE RESTRICT ON DELETE RESTRICT" in alter
    assert (
        "ADD CONSTRAINT `ck_dynamic_shadow_chain_no_real_auto` "
        "CHECK (automatic_real_order_submission = 0)"
    ) in alter
    assert " DROP " not in f" {alter.upper()} "
    assert " CHANGE " not in f" {alter.upper()} "
    assert schema.validate_dynamic_shadow_ledger_schema(connection)


def test_incomplete_schema_with_any_dynamic_data_rejects_before_all_ddl():
    connection = _exact_connection()
    connection.remove_column(
        "st_dynamic_shadow_trial_chain", "risk_decision_fact_hash"
    )
    # Data in a different, structurally complete table still blocks the whole
    # four-table upgrade.  There is no table-by-table loophole.
    connection.row_counts["st_dynamic_shadow_trial_plan"] = 1

    with pytest.raises(RuntimeError, match="禁止伪造回填"):
        schema.ensure_dynamic_shadow_ledger_schema(
            connection, writers_fenced=True,
        )

    assert set(connection.count_queries) == set(
        schema.DYNAMIC_SHADOW_LEDGER_TABLE_NAMES
    )
    assert connection.ddl_statements == []


def test_missing_table_with_existing_dynamic_data_does_not_create_any_table():
    existing = list(schema.DYNAMIC_SHADOW_LEDGER_TABLE_NAMES[:-1])
    connection = _MySQLSchemaConnection(existing)
    connection.row_counts[existing[0]] = 2

    with pytest.raises(RuntimeError, match="隔离/导出后"):
        schema.ensure_dynamic_shadow_ledger_schema(
            connection, writers_fenced=True,
        )

    assert connection.ddl_statements == []


@pytest.mark.parametrize(
    "drift_kind",
    (
        "column",
        "index",
        "foreign_parent_column",
        "foreign_update_rule",
        "foreign_delete_rule",
        "check",
    ),
)
def test_wrong_existing_definition_is_rejected_without_drop_or_rewrite(
    drift_kind,
):
    connection = _exact_connection()
    if drift_kind == "column":
        _row(
            connection.columns,
            table_name="st_dynamic_shadow_trial_chain",
            column_name="risk_decision_fact_hash",
        )["column_type"] = "varchar(64)"
    elif drift_kind == "index":
        _row(
            connection.indexes,
            table_name="st_dynamic_shadow_trial_chain",
            index_name="idx_dynamic_shadow_chain_intent",
        )["column_name"] = "entry_order_id"
    elif drift_kind == "foreign_parent_column":
        _row(
            connection.foreign_keys,
            constraint_name="fk_dynamic_shadow_chain_risk",
        )["referenced_column_name"] = "decision_id"
    elif drift_kind == "foreign_update_rule":
        _row(
            connection.foreign_keys,
            constraint_name="fk_dynamic_shadow_chain_risk",
        )["update_rule"] = "CASCADE"
    elif drift_kind == "foreign_delete_rule":
        _row(
            connection.foreign_keys,
            constraint_name="fk_dynamic_shadow_chain_risk",
        )["delete_rule"] = "CASCADE"
    else:
        _row(
            connection.checks,
            constraint_name="ck_dynamic_shadow_chain_no_real_authority",
        )["check_clause"] = "real_order_authority = 1"

    with pytest.raises(RuntimeError, match="drift"):
        schema.ensure_dynamic_shadow_ledger_schema(
            connection, writers_fenced=True,
        )

    assert connection.count_queries == []
    assert connection.ddl_statements == []


def test_missing_authority_check_is_restored_only_as_zero_on_empty_table():
    connection = _exact_connection()
    connection.remove_check("ck_dynamic_shadow_exit_no_real_authority")

    result = schema.ensure_dynamic_shadow_ledger_schema(
        connection, writers_fenced=True,
    )

    assert result["upgrade_status"] == "UPGRADED"
    assert len(connection.ddl_statements) == 1
    assert "CHECK (real_order_authority = 0)" in connection.ddl_statements[0]
    assert "real_order_authority = 1" not in connection.ddl_statements[0]


def test_complete_nonempty_schema_is_read_only_and_idempotent():
    connection = _exact_connection()
    connection.row_counts["st_dynamic_shadow_trial_chain"] = 5

    result = schema.ensure_dynamic_shadow_ledger_schema(
        connection, writers_fenced=True,
    )

    assert result["upgrade_status"] == "UNCHANGED"
    assert connection.count_queries == []
    assert connection.ddl_statements == []


def test_upgrade_requires_explicit_writer_fence_attestation():
    connection = _MySQLSchemaConnection()

    with pytest.raises(RuntimeError, match="writer fence"):
        schema.ensure_dynamic_shadow_ledger_schema(connection)

    assert connection.tables == []
    assert connection.ddl_statements == []
