from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from integrations.qmt.business_schema import (
    QMT_BUSINESS_COLUMNS,
    migrate_qmt_business_tables,
    missing_qmt_columns,
    qmt_business_tables,
)
from tools import setup_guojin_qmt_business_schema


class _ScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar(self):
        return self.value


class _RowsResult:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


class _Connection:
    def __init__(self, engine):
        self.engine = engine

    def execute(self, statement, params=None):
        sql = str(statement)
        params = params or {}
        table_name = params.get("table_name")
        if "information_schema.TABLES" in sql:
            return _ScalarResult(1 if table_name in self.engine.tables else 0)
        if "information_schema.COLUMNS" in sql:
            return _RowsResult([(column,) for column in self.engine.columns.get(table_name, set())])
        self.engine.alters.append(sql)
        return _ScalarResult(None)


class _Transaction:
    def __init__(self, engine):
        self.connection = _Connection(engine)

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc, traceback):
        return False


class _Engine:
    def __init__(self, tables, columns):
        self.tables = set(tables)
        self.columns = {key: set(value) for key, value in columns.items()}
        self.alters = []

    def begin(self):
        return _Transaction(self)


def test_qmt_business_tables_come_from_catalog_targets():
    tables = qmt_business_tables()

    assert "qmt_raw_manifest" not in tables
    assert "sm_stock_kline" in tables
    assert "si_index_constituent" in tables
    assert tables == sorted(set(tables))


def test_missing_qmt_columns_skips_existing_columns():
    existing = {"stock_code", "data_source", "batch_id"}

    missing = missing_qmt_columns(existing)

    assert "data_source" not in [column.name for column in missing]
    assert "batch_id" not in [column.name for column in missing]
    assert "qmt_code" in [column.name for column in missing]


def test_migration_dry_run_does_not_execute_alter():
    engine = _Engine(tables={"sm_stock_kline"}, columns={"sm_stock_kline": {"stock_code"}})

    results = migrate_qmt_business_tables(engine, tables=["sm_stock_kline"], dry_run=True)

    assert results[0].status == "DRY_RUN"
    assert set(results[0].added_columns) == {column.name for column in QMT_BUSINESS_COLUMNS if column.name != "stock_code"}
    assert engine.alters == []


def test_migration_apply_adds_only_missing_columns():
    engine = _Engine(tables={"sm_stock_kline"}, columns={"sm_stock_kline": {"stock_code", "qmt_code"}})

    results = migrate_qmt_business_tables(engine, tables=["sm_stock_kline"], dry_run=False)

    assert results[0].status == "MIGRATED"
    assert "qmt_code" not in results[0].added_columns
    assert len(engine.alters) == len(QMT_BUSINESS_COLUMNS) - 1


def test_migration_rejects_unsafe_table_name():
    engine = _Engine(tables={"bad;drop"}, columns={})

    results = migrate_qmt_business_tables(engine, tables=["bad;drop"], dry_run=True)

    assert results[0].status == "ERROR"
    assert "Unsafe SQL identifier" in (results[0].error or "")


def test_setup_business_schema_main_uses_batch_engine_for_dry_run():
    engine = object()
    result = SimpleNamespace(status="DRY_RUN")

    with patch.object(
        setup_guojin_qmt_business_schema.sys,
        "argv",
        ["setup_guojin_qmt_business_schema.py", "--tables", "sm_stock_current"],
    ), patch(
        "tools.setup_guojin_qmt_business_schema.create_batch_engine",
        return_value=engine,
    ) as create_batch_engine, patch(
        "tools.setup_guojin_qmt_business_schema.migrate_qmt_business_tables",
        return_value=[result],
    ) as migrate_qmt_business_tables, patch(
        "tools.setup_guojin_qmt_business_schema.result_dicts",
        return_value=[{"status": "DRY_RUN"}],
    ):
        assert setup_guojin_qmt_business_schema.main() == 0

    create_batch_engine.assert_called_once_with(future=True)
    migrate_qmt_business_tables.assert_called_once_with(engine, tables=["sm_stock_current"], dry_run=True)
