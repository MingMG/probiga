# -*- coding: utf-8 -*-
from __future__ import annotations

from server.db.migrations import run_portfolio_collation_migration


class _Result:
    def __init__(self, *, scalar_value=None, row=None):
        self._scalar_value = scalar_value
        self._row = row

    def scalar(self):
        return self._scalar_value

    def mappings(self):
        return self

    def first(self):
        return dict(self._row) if self._row is not None else None


class _Connection:
    def __init__(self, *, column_type="varchar(16)"):
        self.metadata = {
            "COLUMN_TYPE": column_type,
            "IS_NULLABLE": "NO",
            "COLUMN_DEFAULT": None,
            "CHARACTER_SET_NAME": "utf8mb4",
            "COLLATION_NAME": "utf8mb4_general_ci",
            "EXTRA": "",
            "COLUMN_COMMENT": "股票代码",
        }
        self.alters = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement, params=None):
        sql = " ".join(str(statement).split())
        if "information_schema.TABLES" in sql:
            return _Result(scalar_value=1)
        if "information_schema.COLUMNS" in sql:
            return _Result(row=self.metadata)
        if sql.startswith("ALTER TABLE"):
            self.alters.append((sql, dict(params or {})))
            self.metadata["CHARACTER_SET_NAME"] = "utf8mb4"
            self.metadata["COLLATION_NAME"] = "utf8mb4_unicode_ci"
            return _Result()
        raise AssertionError(sql)


class _Engine:
    def __init__(self, connection):
        self.connection = connection

    def begin(self):
        return self.connection


def test_portfolio_collation_migration_dry_run_is_non_mutating():
    connection = _Connection()
    result = run_portfolio_collation_migration(_Engine(connection), dry_run=True)

    assert result[0].status == "would_modify"
    assert connection.alters == []
    assert connection.metadata["COLLATION_NAME"] == "utf8mb4_general_ci"


def test_portfolio_collation_migration_applies_once_and_replays_safely():
    connection = _Connection()
    engine = _Engine(connection)

    first = run_portfolio_collation_migration(engine)
    replay = run_portfolio_collation_migration(engine)

    assert first[0].status == "modified"
    assert replay[0].status == "exists"
    assert len(connection.alters) == 1
    sql, params = connection.alters[0]
    assert "MODIFY COLUMN `stock_code` VARCHAR(16)" in sql
    assert "COLLATE utf8mb4_unicode_ci" in sql
    assert params == {"column_comment": "股票代码"}


def test_portfolio_collation_migration_refuses_unknown_column_shape():
    connection = _Connection(column_type="varchar(20)")
    result = run_portfolio_collation_migration(_Engine(connection))

    assert result[0].status == "contract_mismatch"
    assert connection.alters == []
