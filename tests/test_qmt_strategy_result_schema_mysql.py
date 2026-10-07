from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

import pytest
from sqlalchemy import CheckConstraint, UniqueConstraint
from sqlalchemy.dialects import mysql

from server.common import qmt_strategy_result_schema as schema


class MySQLReflection:
    """Physical MySQL shape, including reflected effective collation."""

    def __init__(self):
        self.tables = {table.name: table for table in (schema.INPUTS, schema.RESULTS, schema.JOBS)}
        self.columns = {}
        self.options = {}
        self.checks = {}
        self.indexes = {}
        self.uniques = {}
        self.pks = {}
        self.foreign_keys = {}
        dialect = mysql.dialect()
        for name, table in self.tables.items():
            self.columns[name] = []
            for column in table.c:
                expected = column.type.dialect_impl(dialect)
                family = str(expected.compile(dialect=dialect)).split("(", 1)[0].upper()
                reflected_class = {"CHAR": mysql.CHAR, "VARCHAR": mysql.VARCHAR, "LONGTEXT": mysql.LONGTEXT}.get(family)
                actual = (reflected_class(length=expected.length, collation="utf8mb4_unicode_ci")
                          if reflected_class else expected)
                self.columns[name].append({"name": column.name, "type": actual, "nullable": column.nullable})
            self.options[name] = {"mysql_engine": "InnoDB", "mysql_default charset": "utf8mb4",
                                  "mysql_collate": "utf8mb4_unicode_ci"}
            self.checks[name] = [{"name": item.name, "sqltext": str(item.sqltext)}
                                 for item in table.constraints if isinstance(item, CheckConstraint)]
            self.indexes[name] = [{"name": item.name, "column_names": [column.name for column in item.columns]}
                                  for item in table.indexes]
            self.uniques[name] = [{"name": item.name, "column_names": [column.name for column in item.columns]}
                                  for item in table.constraints if isinstance(item, UniqueConstraint)]
            self.pks[name] = {"constrained_columns": list(table.primary_key.columns.keys())}
            self.foreign_keys[name] = ([] if table is schema.INPUTS else [
                {"constrained_columns": ["snapshot_id"], "referred_table": schema.INPUTS.name,
                 "referred_columns": ["snapshot_id"]}])

    def has_table(self, name):
        return name in self.tables

    def get_columns(self, name):
        return self.columns[name]

    def get_table_options(self, name):
        return self.options[name]

    def get_check_constraints(self, name):
        return self.checks[name]

    def get_indexes(self, name):
        return self.indexes[name]

    def get_unique_constraints(self, name):
        return self.uniques[name]

    def get_pk_constraint(self, name):
        return self.pks[name]

    def get_foreign_keys(self, name):
        return self.foreign_keys[name]


@pytest.fixture
def physical_mysql(monkeypatch):
    reader = MySQLReflection()
    monkeypatch.setattr(schema, "inspect", lambda _engine: reader)
    return SimpleNamespace(dialect=mysql.dialect()), reader


@pytest.mark.parametrize("charset", [None, "utf8mb4"])
def test_mysql_effective_column_collation_is_validated_not_type_string(physical_mysql, charset):
    engine, reader = physical_mysql
    for columns in reader.columns.values():
        for column in columns:
            if hasattr(column["type"], "charset"):
                column["type"].charset = charset
    assert str(reader.columns[schema.INPUTS.name][0]["type"]) == 'CHAR(32) COLLATE "utf8mb4_unicode_ci"'
    result = schema.validate_qmt_strategy_result_schema(engine)
    assert result["read_only"] is True and result["runtime_ddl_required"] is False


@pytest.mark.parametrize("field", ["snapshot_id", "trade_date", "snapshot_json"])
@pytest.mark.parametrize("mutation", ["wrong_family", "wrong_length", "wrong_collation", "missing_collation",
                                     "wrong_charset", "binary", "ascii", "unicode", "national", "nullable"])
def test_mysql_physical_string_contract_changes_fail_closed(physical_mysql, field, mutation):
    engine, reader = physical_mysql
    column = next(item for item in reader.columns[schema.INPUTS.name] if item["name"] == field)
    actual = column["type"]
    if mutation == "wrong_family":
        column["type"] = mysql.TEXT(collation="utf8mb4_unicode_ci")
    elif mutation == "wrong_length":
        actual.length = (actual.length or 0) + 1
    elif mutation == "wrong_collation":
        actual.collation = "utf8mb4_bin"
    elif mutation == "missing_collation":
        actual.collation = None
    elif mutation == "wrong_charset":
        actual.charset = "utf8mb3"
    elif mutation == "nullable":
        column["nullable"] = not column["nullable"]
    else:
        setattr(actual, mutation, True)
    with pytest.raises(RuntimeError, match="field contract differs"):
        schema.validate_qmt_strategy_result_schema(engine)


@pytest.mark.parametrize("table", [schema.INPUTS.name, schema.RESULTS.name, schema.JOBS.name])
@pytest.mark.parametrize("option,value", [("mysql_engine", "MyISAM"), ("mysql_default charset", "utf8mb3"),
                                          ("mysql_collate", "utf8mb4_bin"), ("mysql_default charset", None)])
def test_mysql_table_storage_contract_remains_strict(physical_mysql, table, option, value):
    engine, reader = physical_mysql
    reader.options[table][option] = value
    with pytest.raises(RuntimeError, match="storage differs"):
        schema.validate_qmt_strategy_result_schema(engine)


@pytest.mark.parametrize("mutation,error", [("check", "safety constraint"), ("primary", "primary key"),
                                          ("unique", "uniqueness"), ("index", "lookup index"),
                                          ("result_fk", "input/result relationship"),
                                          ("job_fk", "input/job relationship")])
def test_mysql_keys_and_research_checks_are_not_relaxed(physical_mysql, mutation, error):
    engine, reader = physical_mysql
    if mutation == "check":
        reader.checks[schema.INPUTS.name] = deepcopy(reader.checks[schema.INPUTS.name])
        check = next(item for item in reader.checks[schema.INPUTS.name] if item["name"].endswith("research"))
        check["sqltext"] = "simulation_only=0 AND real_order_allowed=1"
    elif mutation == "primary":
        reader.pks[schema.INPUTS.name]["constrained_columns"] = []
    elif mutation == "unique":
        reader.uniques[schema.INPUTS.name] = []
    elif mutation == "index":
        reader.indexes[schema.INPUTS.name] = []
    else:
        reader.foreign_keys[schema.RESULTS.name if mutation == "result_fk" else schema.JOBS.name] = []
    with pytest.raises(RuntimeError, match=error):
        schema.validate_qmt_strategy_result_schema(engine)
