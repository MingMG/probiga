from __future__ import annotations

import copy

import pandas as pd
import pytest

from tools import sync_guojin_qmt_reference_data as reference_sync


def _table_snapshot() -> dict:
    tables = {}
    character_types = {"char", "varchar", "text", "mediumtext"}
    for table_name in reference_sync._REFERENCE_TRUTH_TABLES:
        columns = []
        for (
            name,
            data_type,
            character_length,
            numeric_precision,
            numeric_scale,
            nullable,
            default,
            extra,
        ) in reference_sync._REFERENCE_COLUMN_CONTRACTS[table_name]:
            is_character = data_type in character_types
            columns.append({
                "name": name,
                "data_type": data_type,
                "character_maximum_length": character_length,
                "numeric_precision": numeric_precision,
                "numeric_scale": numeric_scale,
                "is_nullable": nullable,
                "column_default": default,
                "extra": extra,
                "character_set_name": "utf8mb4" if is_character else None,
                "collation_name": (
                    "utf8mb4_unicode_ci" if is_character else None
                ),
            })
        indexes = {
            name: {
                "non_unique": non_unique,
                "columns": list(index_columns),
                "sub_parts": [None] * len(index_columns),
                "index_type": "BTREE",
            }
            for name, (non_unique, index_columns) in (
                reference_sync._REFERENCE_INDEX_CONTRACTS[table_name].items()
            )
        }
        foreign_keys = {
            name: {
                "columns": list(columns_value),
                "referenced_table": referenced_table,
                "referenced_columns": list(referenced_columns),
                "update_rule": update_rule,
                "delete_rule": delete_rule,
            }
            for name, (
                columns_value,
                referenced_table,
                referenced_columns,
                update_rule,
                delete_rule,
            ) in reference_sync._REFERENCE_FOREIGN_KEY_CONTRACTS.get(
                table_name, {}
            ).items()
        }
        tables[table_name] = {
            "engine": "innodb",
            "table_collation": "utf8mb4_unicode_ci",
            "columns": columns,
            "indexes": indexes,
            "foreign_keys": foreign_keys,
            "checks": {},
        }
    return {
        "schema": "probiga.qmt-reference-table-physical.v1",
        "tables": tables,
    }


def _trigger_snapshot() -> dict:
    rows = []
    for name, (table_name, event, message) in (
        reference_sync._REFERENCE_TRIGGER_CONTRACTS.items()
    ):
        rows.append({
            "trigger_name": name,
            "event_object_table": table_name,
            "event_manipulation": event,
            "action_timing": "BEFORE",
            "action_orientation": "ROW",
            "action_statement": (
                "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='" + message + "'"
            ),
            "sql_mode": "STRICT_TRANS_TABLES",
            "definer": "probiga_migrator@127.0.0.1",
            "character_set_client": "utf8mb4",
            "collation_connection": "utf8mb4_unicode_ci",
            "database_collation": "utf8mb4_unicode_ci",
        })
    return {
        "schema": "probiga.qmt-reference-trigger-physical.v1",
        "triggers": rows,
    }


def test_exact_reference_physical_contract_accepts_only_expected_shape():
    snapshot = _table_snapshot()
    reference_sync._validate_reference_table_snapshot(snapshot)

    mutations = []
    wrong_type = copy.deepcopy(snapshot)
    wrong_type["tables"]["qmt_stock_catalog_batch"]["columns"][0][
        "data_type"
    ] = "text"
    mutations.append(wrong_type)
    wrong_null = copy.deepcopy(snapshot)
    wrong_null["tables"]["qmt_stock_catalog_batch"]["columns"][0][
        "is_nullable"
    ] = "YES"
    mutations.append(wrong_null)
    wrong_collation = copy.deepcopy(snapshot)
    wrong_collation["tables"]["qmt_trade_calendar_batch"][
        "table_collation"
    ] = "utf8mb4_general_ci"
    mutations.append(wrong_collation)
    wrong_order = copy.deepcopy(snapshot)
    wrong_order["tables"]["qmt_stock_catalog_member"]["columns"][0:2] = (
        list(reversed(
            wrong_order["tables"]["qmt_stock_catalog_member"]["columns"][0:2]
        ))
    )
    mutations.append(wrong_order)
    extra_index = copy.deepcopy(snapshot)
    extra_index["tables"]["qmt_trade_calendar_session"]["indexes"][
        "idx_unsealed"
    ] = {
        "non_unique": 1,
        "columns": ["batch_id"],
        "sub_parts": [None],
        "index_type": "BTREE",
    }
    mutations.append(extra_index)
    wrong_fk = copy.deepcopy(snapshot)
    wrong_fk["tables"]["qmt_stock_catalog_member"]["foreign_keys"][
        "fk_qmt_stock_catalog_member_batch"
    ]["delete_rule"] = "CASCADE"
    mutations.append(wrong_fk)
    extra_check = copy.deepcopy(snapshot)
    extra_check["tables"]["qmt_stock_catalog_batch"]["checks"] = {
        "ck_unsealed": "member_count >= 0"
    }
    mutations.append(extra_check)

    for mutation in mutations:
        with pytest.raises(RuntimeError, match="physical schema differs"):
            reference_sync._validate_reference_table_snapshot(mutation)


def test_reference_trigger_body_or_table_tamper_is_rejected():
    snapshot = _trigger_snapshot()
    reference_sync._validate_reference_trigger_snapshot(snapshot)

    wrong_body = copy.deepcopy(snapshot)
    wrong_body["triggers"][0]["action_statement"] = "SET @allowed=1"
    with pytest.raises(RuntimeError, match="trigger differs"):
        reference_sync._validate_reference_trigger_snapshot(wrong_body)

    wrong_table = copy.deepcopy(snapshot)
    wrong_table["triggers"][0]["event_object_table"] = "sm_stock_kline"
    with pytest.raises(RuntimeError, match="trigger differs"):
        reference_sync._validate_reference_trigger_snapshot(wrong_table)


def test_instrument_detail_batch_is_content_addressed_and_type_bound():
    rows = pd.DataFrame([{
        "qmt_code": "000001.SZ",
        "stock_code": "000001",
        "exchange": "SZ",
        "product_type": "STOCK",
        "list_date": "1991-04-03",
        "expire_date": None,
    }])
    first = reference_sync._instrument_detail_source_batch_id(rows)
    second_rows = rows.copy()
    second_rows.loc[0, "product_type"] = "INDEX"
    second = reference_sync._instrument_detail_source_batch_id(second_rows)

    assert len(first) == 64
    assert first != second
    with pytest.raises(RuntimeError, match="payload is incomplete"):
        reference_sync._instrument_detail_source_batch_id(
            rows.drop(columns=["product_type"])
        )


def test_reference_schema_hash_binds_full_ddl_not_only_object_names():
    contracts = reference_sync.reference_table_ddl_contracts()
    migrations = reference_sync.reference_migration_ddl_contracts()
    triggers = reference_sync.reference_trigger_ddl_contracts()

    assert len(triggers) == 10
    assert any("instrument_type VARCHAR(32) NOT NULL" in sql for sql in contracts)
    assert any("utf8mb4_unicode_ci" in sql for sql in migrations)
    assert all("SIGNAL SQLSTATE '45000'" in sql for sql in triggers)
    assert len(reference_sync.REFERENCE_SCHEMA_CONTRACT_HASH) == 64


@pytest.mark.parametrize("columns_exist", (False, True))
def test_reference_additive_ddl_is_mysql_compatible_and_idempotent(
    columns_exist,
):
    class _Result:
        def __init__(self, rows):
            self.rows = rows

        def mappings(self):
            return self

        def all(self):
            return self.rows

    class _Connection:
        def __init__(self):
            self.statements = []

        def execute(self, statement, params=None):
            sql = str(statement).strip()
            self.statements.append(sql)
            if sql.upper().startswith("SELECT COLUMN_NAME"):
                return _Result(
                    [{"COLUMN_NAME": params["column_name"]}]
                    if columns_exist else []
                )
            return _Result([])

    connection = _Connection()
    reference_sync.execute_reference_ddl_contracts(
        connection,
        reference_sync.reference_migration_ddl_contracts(),
    )
    mutations = [
        statement for statement in connection.statements
        if not statement.upper().startswith("SELECT ")
    ]

    assert not any("ADD COLUMN IF NOT EXISTS" in statement.upper()
                   for statement in mutations)
    plain_adds = [
        statement for statement in mutations
        if "ADD COLUMN" in statement.upper()
    ]
    assert len(plain_adds) == (0 if columns_exist else 6)


def test_reference_preflight_is_read_only_for_empty_install(monkeypatch):
    class _Inspector:
        def has_table(self, _name):
            return False

    monkeypatch.setattr(reference_sync, "inspect", lambda _engine: _Inspector())
    result = reference_sync.preflight_reference_tables(object())

    assert result["status"] == "EMPTY"
    assert result["read_only"] is True
    assert result["table_names"] == list(reference_sync.REFERENCE_TABLE_NAMES)
    assert result["trigger_names"] == list(
        reference_sync.REFERENCE_TRIGGER_NAMES
    )
    assert result["trigger_ddl_count"] == 10


def test_reference_preflight_rejects_unknown_controlled_column(monkeypatch):
    class _Type:
        length = 64

        def __str__(self):
            return "VARCHAR(64)"

    class _Inspector:
        def has_table(self, name):
            return name == "qmt_stock_catalog_batch"

        def get_columns(self, _name):
            return [
                {"name": "batch_id", "type": _Type(), "nullable": False},
                {"name": "unknown_truth", "type": _Type(), "nullable": True},
            ]

    monkeypatch.setattr(reference_sync, "inspect", lambda _engine: _Inspector())
    with pytest.raises(RuntimeError, match="unknown_columns"):
        reference_sync.preflight_reference_tables(object())
