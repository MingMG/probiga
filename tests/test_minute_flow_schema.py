from __future__ import annotations

from types import SimpleNamespace

import pytest

from server.common import minute_flow_schema as schema


VALID_COLUMN = {
    "DATA_TYPE": "date",
    "IS_NULLABLE": "YES",
    "EXTRA": "VIRTUAL GENERATED",
    "GENERATION_EXPRESSION": "date(`trade_time`)",
}


def test_minute_flow_date_contract_accepts_exact_generated_index():
    schema.validate_minute_flow_trade_date_metadata(
        VALID_COLUMN,
        (("stock_code", "trade_time"), schema.MINUTE_FLOW_DATE_INDEX_COLUMNS),
    )


@pytest.mark.parametrize(
    "column",
    (
        None,
        {**VALID_COLUMN, "DATA_TYPE": "datetime"},
        {**VALID_COLUMN, "EXTRA": ""},
        {**VALID_COLUMN, "GENERATION_EXPRESSION": "date(`snapshot_at`)"},
        {**VALID_COLUMN, "IS_NULLABLE": "NO"},
    ),
)
def test_minute_flow_date_contract_rejects_column_drift(column):
    with pytest.raises(RuntimeError, match="trade_date"):
        schema.validate_minute_flow_trade_date_metadata(
            column,
            (schema.MINUTE_FLOW_DATE_INDEX_COLUMNS,),
        )


def test_minute_flow_date_contract_requires_daily_index():
    with pytest.raises(RuntimeError, match="missing index"):
        schema.validate_minute_flow_trade_date_metadata(
            VALID_COLUMN,
            (("stock_code", "trade_time"),),
        )


class _Result:
    def __init__(self, value):
        self.value = value

    def scalar_one(self):
        return self.value


class _Connection:
    def __init__(self):
        self.statements: list[str] = []
        self.column_added = False
        self.index_added = False

    def execute(self, statement, _params=None):
        sql = str(statement)
        self.statements.append(sql)
        if "ADD COLUMN" in sql:
            self.column_added = True
        if "ADD INDEX" in sql:
            self.index_added = True
        return _Result(1)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class _Engine:
    dialect = SimpleNamespace(name="mysql")

    def __init__(self, connection):
        self.connection = connection

    def begin(self):
        return self.connection

    def connect(self):
        return self.connection


def test_minute_flow_migration_adds_virtual_date_and_online_index(monkeypatch):
    connection = _Connection()
    engine = _Engine(connection)

    monkeypatch.setattr(
        schema,
        "_column_row",
        lambda _connection: VALID_COLUMN if connection.column_added else None,
    )
    monkeypatch.setattr(
        schema,
        "_indexes",
        lambda _connection: [
            schema.MINUTE_FLOW_DATE_INDEX_COLUMNS
        ] if connection.index_added else [],
    )

    result = schema.privileged_migrate_minute_flow_trade_date(engine)

    assert result == {
        "table": schema.MINUTE_FLOW_TABLE,
        "column": schema.MINUTE_FLOW_DATE_COLUMN,
        "index": schema.MINUTE_FLOW_DATE_INDEX,
        "column_added": True,
        "index_added": True,
        "validated": True,
    }
    assert any(
        "ADD COLUMN `trade_date` DATE GENERATED ALWAYS AS "
        "(DATE(`trade_time`)) VIRTUAL" in sql
        for sql in connection.statements
    )
    assert any(
        "ADD INDEX `idx_scfm_trade_date` "
        "(`trade_date`), ALGORITHM=INPLACE, LOCK=NONE" in sql
        for sql in connection.statements
    )


def test_minute_flow_migration_rejects_non_mysql():
    with pytest.raises(RuntimeError, match="requires MySQL"):
        schema.privileged_migrate_minute_flow_trade_date(
            SimpleNamespace(dialect=SimpleNamespace(name="sqlite"))
        )
