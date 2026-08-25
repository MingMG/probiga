import json
from unittest.mock import patch

import pytest

from server.common import versioned_strategy_config as versioned_config
from server.engine import strategy_center
from server.engine import strategy_governance


class _RowsResult:
    def __init__(self, rows):
        self._rows = list(rows)

    def mappings(self):
        return self

    def all(self):
        return list(self._rows)


class _PreparedRuntimeConnection:
    def __init__(
        self, *, missing_table="", wrong_stock_hash=False,
        tampered_stock_payload=False, duplicate_active_stock=False,
        column_overrides=None,
    ):
        self.missing_table = missing_table
        self.wrong_stock_hash = wrong_stock_hash
        self.tampered_stock_payload = tampered_stock_payload
        self.duplicate_active_stock = duplicate_active_stock
        self.column_overrides = dict(column_overrides or {})
        self.statements = []
        self.bound_params = []

    @staticmethod
    def _column_contracts():
        return {
            **versioned_config._VERSIONED_STRATEGY_COLUMN_CONTRACTS,
            **strategy_center._STRATEGY_CENTER_COLUMN_CONTRACTS,
        }

    @staticmethod
    def _index_contracts():
        return {
            **versioned_config._VERSIONED_STRATEGY_REQUIRED_INDEXES,
            **strategy_center._STRATEGY_CENTER_REQUIRED_INDEXES,
        }

    def _schema_tables(self, params):
        return {
            str(value) for key, value in (params or {}).items()
            if str(key).startswith("table_")
        }

    def _column_rows(self, tables):
        rows = []
        for table, contracts in self._column_contracts().items():
            if table not in tables or table == self.missing_table:
                continue
            for column, contract in contracts.items():
                data_type, nullable, length, precision, scale, auto = contract
                row = {
                    "table_name": table,
                    "column_name": column,
                    "data_type": data_type,
                    "is_nullable": "YES" if nullable else "NO",
                    "character_maximum_length": length,
                    "numeric_precision": precision,
                    "numeric_scale": scale,
                    "extra": "auto_increment" if auto else "",
                }
                row.update(self.column_overrides.get((table, column), {}))
                rows.append(row)
        return rows

    def _index_rows(self, tables):
        rows = []
        for table, indexes in self._index_contracts().items():
            if table not in tables or table == self.missing_table:
                continue
            for index_number, (unique, columns) in enumerate(indexes):
                for position, column in enumerate(columns, start=1):
                    rows.append({
                        "table_name": table,
                        "index_name": f"prepared_{index_number}",
                        "non_unique": 0 if unique else 1,
                        "seq_in_index": position,
                        "column_name": column,
                    })
        return rows

    def _stock_rows(self):
        payload = versioned_config.load_stock_manifest()
        stored_payload = json.loads(json.dumps(payload, ensure_ascii=False))
        if self.tampered_stock_payload:
            stored_payload["status"] = "tampered"
        rows = [{
            "manifest_version": payload["manifest_version"],
            "config_hash": (
                "0" * 64 if self.wrong_stock_hash
                else versioned_config.stock_manifest_hash()
            ),
            "schema_version": payload["schema_version"],
            "model_version": payload["model_version"],
            "manifest_json": json.dumps(stored_payload, ensure_ascii=False),
            "status": payload["status"],
            "active": 1,
        }]
        if self.duplicate_active_stock:
            rows.append({
                **rows[0],
                "manifest_version": "retired-but-still-active",
                "active": 1,
            })
        return rows

    @staticmethod
    def _market_rows():
        payload = versioned_config.load_market_state_config()
        return [{
            "config_version": payload["config_version"],
            "config_hash": versioned_config.market_state_config_hash(),
            "schema_version": payload["schema_version"],
            "config_json": json.dumps(payload, ensure_ascii=False),
            "status": payload["status"],
            "active": 1,
        }]

    @staticmethod
    def _strategy_config_rows():
        registration = {
            "stock_manifest_version": (
                versioned_config.load_stock_manifest()["manifest_version"]
            ),
            "stock_manifest_hash": versioned_config.stock_manifest_hash(),
            "market_state_config_version": (
                versioned_config.load_market_state_config()["config_version"]
            ),
            "market_state_config_hash": (
                versioned_config.market_state_config_hash()
            ),
        }
        expected = strategy_center._expected_strategy_center_configs(registration)
        return [{
            "strategy_key": key,
            "enabled": 1,
            "base_weight": identity["base_weight"],
            "config_json": json.dumps(identity["config"], ensure_ascii=False),
            "version": 2,
        } for key, identity in expected.items()]

    def execute(self, statement, params=None):
        sql = str(statement)
        self.statements.append(sql)
        self.bound_params.append(dict(params or {}))
        lowered = " ".join(sql.lower().split())
        if "from information_schema.columns" in lowered:
            return _RowsResult(self._column_rows(self._schema_tables(params)))
        if "from information_schema.statistics" in lowered:
            return _RowsResult(self._index_rows(self._schema_tables(params)))
        if "from `st_strategy_manifest_registry`" in lowered:
            return _RowsResult(self._stock_rows())
        if "from `st_market_state_config`" in lowered:
            return _RowsResult(self._market_rows())
        if "from st_strategy_center_config" in lowered:
            return _RowsResult(self._strategy_config_rows())
        raise AssertionError(f"unexpected runtime SQL: {sql}")


class _ConnectionContext:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, *_args):
        return False


class _ReadOnlyRuntimeEngine:
    def __init__(self, connection):
        self.connection = connection
        self.begin_calls = 0

    def connect(self):
        return _ConnectionContext(self.connection)

    def begin(self):
        self.begin_calls += 1
        raise AssertionError("runtime validation must never open a write transaction")


def _assert_select_only(statements):
    assert statements
    for sql in statements:
        normalized = " ".join(sql.upper().split())
        assert normalized.startswith("SELECT ")
        assert not any(
            keyword in normalized
            for keyword in ("CREATE TABLE", "ALTER TABLE", "INSERT INTO", "UPDATE ", "DELETE FROM")
        )


def test_runtime_guards_validate_all_nine_tables_without_ddl_or_seed_dml():
    connection = _PreparedRuntimeConnection()
    engine = _ReadOnlyRuntimeEngine(connection)

    strategy_center.validate_strategy_center_runtime(engine)
    versioned_config.ensure_versioned_strategy_tables(engine)
    versioned_config.register_versioned_strategy_configs(engine)

    assert engine.begin_calls == 0
    _assert_select_only(connection.statements)
    schema_tables = {
        str(value)
        for params in connection.bound_params
        for key, value in params.items()
        if str(key).startswith("table_")
    }
    assert schema_tables == set((
        *strategy_center._STRATEGY_CENTER_TABLE_COLUMNS,
        *versioned_config._VERSIONED_STRATEGY_TABLE_COLUMNS,
    ))


def test_runtime_guard_fails_closed_when_one_strategy_center_table_is_missing():
    connection = _PreparedRuntimeConnection(
        missing_table="st_strategy_center_run",
    )
    engine = _ReadOnlyRuntimeEngine(connection)

    with pytest.raises(RuntimeError, match="st_strategy_center_run missing columns"):
        strategy_center.validate_strategy_center_runtime(engine)

    assert engine.begin_calls == 0
    _assert_select_only(connection.statements)


def test_runtime_guard_fails_closed_when_current_manifest_hash_drifts():
    connection = _PreparedRuntimeConnection(wrong_stock_hash=True)
    engine = _ReadOnlyRuntimeEngine(connection)

    with pytest.raises(RuntimeError, match="config identity drift"):
        strategy_center.validate_strategy_center_runtime(engine)

    assert engine.begin_calls == 0
    _assert_select_only(connection.statements)


@pytest.mark.parametrize("kwargs", [
    {"tampered_stock_payload": True},
    {"duplicate_active_stock": True},
])
def test_runtime_guard_fails_closed_on_payload_or_active_singleton_drift(kwargs):
    connection = _PreparedRuntimeConnection(**kwargs)
    engine = _ReadOnlyRuntimeEngine(connection)

    with pytest.raises(RuntimeError, match="config identity drift"):
        strategy_center.validate_strategy_center_runtime(engine)

    assert engine.begin_calls == 0
    _assert_select_only(connection.statements)


@pytest.mark.parametrize("override", [
    {("st_strategy_center_run", "run_uid"): {"data_type": "text"}},
    {("st_strategy_manifest_registry", "config_hash"): {
        "is_nullable": "YES",
    }},
])
def test_runtime_guard_fails_closed_on_type_or_nullability_drift(override):
    connection = _PreparedRuntimeConnection(column_overrides=override)
    engine = _ReadOnlyRuntimeEngine(connection)

    with pytest.raises(RuntimeError, match="schema type drift"):
        strategy_center.validate_strategy_center_runtime(engine)

    assert engine.begin_calls == 0
    _assert_select_only(connection.statements)


def test_persist_validates_read_only_contract_then_writes_facts_but_no_seed():
    connection = _PreparedRuntimeConnection()
    engine = _ReadOnlyRuntimeEngine(connection)
    fact_statements = []
    snapshot = {
        "trade_date": "2026-08-21",
        "source_status": "fresh",
        "market_state": {
            "key": "trend_bullish",
            "confidence": 80,
            "input": {},
            "evidence": [],
        },
        "candidates": [],
        "conflicts": [],
    }

    with patch.object(strategy_center, "get_engine", return_value=engine), patch.object(
        strategy_center, "current_bound_sql_connection", return_value=None,
    ), patch.object(
        strategy_center, "_db_write",
        side_effect=lambda sql, params=None: fact_statements.append(str(sql)),
    ):
        result = strategy_center.persist_strategy_center_snapshot(snapshot)

    assert result["execution_status"] == "done"
    _assert_select_only(connection.statements)
    fact_sql = "\n".join(fact_statements).upper()
    assert "INSERT INTO ST_STRATEGY_CENTER_RUN" in fact_sql
    assert "INSERT INTO ST_MARKET_STATE_DAILY" in fact_sql
    assert "ST_STRATEGY_CENTER_CONFIG" not in fact_sql
    assert "ST_STRATEGY_MANIFEST_REGISTRY" not in fact_sql
    assert "ST_MARKET_STATE_CONFIG" not in fact_sql
    assert "CREATE TABLE" not in fact_sql
    assert "ALTER TABLE" not in fact_sql


def test_persist_missing_schema_blocks_before_any_fact_writer():
    connection = _PreparedRuntimeConnection(
        missing_table="st_strategy_center_signal",
    )
    engine = _ReadOnlyRuntimeEngine(connection)

    with patch.object(strategy_center, "get_engine", return_value=engine), patch.object(
        strategy_center, "current_bound_sql_connection", return_value=None,
    ), patch.object(strategy_center, "_db_write") as writer:
        with pytest.raises(RuntimeError, match="st_strategy_center_signal"):
            strategy_center.persist_strategy_center_snapshot({})

    writer.assert_not_called()


def test_governance_wrong_manifest_hash_blocks_before_writer_lock():
    connection = _PreparedRuntimeConnection(wrong_stock_hash=True)
    engine = _ReadOnlyRuntimeEngine(connection)

    with patch.object(
        strategy_governance, "ensure_and_seed_governance", return_value=None,
    ), patch.object(
        strategy_center, "get_engine", return_value=engine,
    ), patch.object(
        strategy_center, "current_bound_sql_connection", return_value=None,
    ), patch.object(strategy_governance, "get_engine") as governance_engine:
        with pytest.raises(RuntimeError, match="config identity drift"):
            strategy_governance.governance_snapshot(persist=True)

    governance_engine.assert_not_called()
    _assert_select_only(connection.statements)
