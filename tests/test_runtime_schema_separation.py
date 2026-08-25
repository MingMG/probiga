from __future__ import annotations

import inspect

import pytest

from biz.intraday_alert import core as intraday_core
from biz.intraday_alert import schema as intraday_schema
from biz.market_context import external_market
from biz.market_radar import core as radar
from biz.premarket import theme_forecast
from biz.review import quant_digest
from server.common import commentary_profile_schema
from server.common import jq_minute_schema
from server.common import screener_schema


_STRING_TYPES = {"char", "varchar", "tinytext", "text", "mediumtext", "longtext"}


class _Rows:
    def __init__(self, rows):
        self._rows = list(rows)

    def mappings(self):
        return self

    def all(self):
        return list(self._rows)


class _PreparedConnection:
    def __init__(
        self,
        contracts,
        *,
        missing_table="",
        table_overrides=None,
        column_overrides=None,
        missing_index_table="",
    ):
        self.contracts = dict(contracts)
        self.missing_table = missing_table
        self.table_overrides = dict(table_overrides or {})
        self.column_overrides = dict(column_overrides or {})
        self.missing_index_table = missing_index_table
        self.statements = []
        self.bound_params = []

    @staticmethod
    def _requested_tables(params):
        return {
            str(value) for key, value in (params or {}).items()
            if str(key).startswith("table_")
        }

    def _table_rows(self, requested):
        rows = []
        for table, contract in self.contracts.items():
            if table not in requested or table == self.missing_table:
                continue
            row = {
                "table_name": table,
                "engine": contract.engine,
                "table_collation": contract.collation,
            }
            row.update(self.table_overrides.get(table, {}))
            rows.append(row)
        return rows

    def _column_rows(self, requested):
        rows = []
        for table, contract in self.contracts.items():
            if table not in requested or table == self.missing_table:
                continue
            for column, expected in contract.columns.items():
                row = {
                    "table_name": table,
                    "column_name": column,
                    "data_type": expected.data_type,
                    "column_type": (
                        f"{expected.data_type} unsigned"
                        if expected.unsigned else expected.data_type
                    ),
                    "is_nullable": "YES" if expected.nullable else "NO",
                    "character_maximum_length": expected.character_length,
                    "numeric_precision": expected.numeric_precision,
                    "numeric_scale": expected.numeric_scale,
                    "datetime_precision": expected.datetime_precision,
                    "extra": "auto_increment" if expected.auto_increment else "",
                    "character_set_name": (
                        "utf8mb4" if expected.data_type in _STRING_TYPES else None
                    ),
                    "collation_name": (
                        contract.collation
                        if expected.data_type in _STRING_TYPES else None
                    ),
                }
                row.update(self.column_overrides.get((table, column), {}))
                rows.append(row)
        return rows

    def _index_rows(self, requested):
        rows = []
        for table, contract in self.contracts.items():
            if (
                table not in requested
                or table == self.missing_table
                or table == self.missing_index_table
            ):
                continue
            for index_number, index in enumerate(contract.indexes):
                for position, column in enumerate(index.columns, start=1):
                    rows.append({
                        "table_name": table,
                        "index_name": f"prepared_{index_number}",
                        "non_unique": 0 if index.unique else 1,
                        "seq_in_index": position,
                        "column_name": column,
                        "sub_part": None,
                        "index_type": index.index_type,
                    })
        return rows

    def execute(self, statement, params=None):
        sql = str(statement)
        self.statements.append(sql)
        self.bound_params.append(dict(params or {}))
        normalized = " ".join(sql.lower().split())
        requested = self._requested_tables(params)
        if "from information_schema.tables" in normalized:
            return _Rows(self._table_rows(requested))
        if "from information_schema.columns" in normalized:
            return _Rows(self._column_rows(requested))
        if "from information_schema.statistics" in normalized:
            return _Rows(self._index_rows(requested))
        raise AssertionError(f"unexpected runtime SQL: {sql}")


class _Connect:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, *_args):
        return False


class _ReadOnlyEngine:
    def __init__(self, connection):
        self.connection = connection
        self.begin_calls = 0

    def connect(self):
        return _Connect(self.connection)

    def begin(self):
        self.begin_calls += 1
        raise AssertionError("runtime schema validation opened a write transaction")


class _MigrationConnection(_PreparedConnection):
    def execute(self, statement, params=None):
        sql = str(statement)
        normalized = " ".join(sql.upper().split())
        if normalized.startswith("CREATE TABLE") or normalized.startswith("ALTER TABLE"):
            self.statements.append(sql)
            self.bound_params.append(dict(params or {}))
            return _Rows([])
        return super().execute(statement, params)


class _MigrationEngine:
    def __init__(self, connection):
        self.connection = connection
        self.begin_calls = 0

    def begin(self):
        self.begin_calls += 1
        return _Connect(self.connection)

    def connect(self):
        return _Connect(self.connection)


_CASES = (
    ("radar", radar._RADAR_SCHEMA, radar.validate_radar_runtime, radar.ensure_radar_tables),
    (
        "premarket",
        theme_forecast._PREMARKET_THEME_SCHEMA,
        theme_forecast.validate_premarket_theme_runtime,
        theme_forecast.ensure_premarket_theme_tables,
    ),
    (
        "external_market",
        external_market._EXTERNAL_MARKET_SCHEMA,
        external_market.validate_external_market_runtime,
        external_market.ensure_external_market_table,
    ),
    (
        "intraday_alert",
        intraday_schema._INTRADAY_ALERT_SCHEMA,
        intraday_schema.validate_intraday_alert_runtime,
        intraday_schema.ensure_intraday_alert_tables,
    ),
    (
        "quant_digest",
        quant_digest._QUANT_DIGEST_SCHEMA,
        quant_digest.validate_quant_digest_runtime,
        None,
    ),
    (
        "commentary_profile",
        commentary_profile_schema.COMMENTARY_PROFILE_SCHEMA,
        commentary_profile_schema.validate_commentary_profile_runtime,
        commentary_profile_schema.ensure_commentary_profile_table,
    ),
    (
        "screener",
        screener_schema.SCREENER_SCHEMA,
        screener_schema.validate_screener_runtime,
        screener_schema.ensure_screener_tables,
    ),
    (
        "jq_minute",
        jq_minute_schema.JQ_MINUTE_SCHEMA,
        jq_minute_schema.validate_jq_minute_runtime,
        jq_minute_schema.ensure_jq_minute_table,
    ),
)

_MIGRATION_CASES = (
    (radar._RADAR_SCHEMA, radar.privileged_migrate_radar_tables),
    (
        theme_forecast._PREMARKET_THEME_SCHEMA,
        theme_forecast.privileged_migrate_premarket_theme_tables,
    ),
    (
        external_market._EXTERNAL_MARKET_SCHEMA,
        external_market.privileged_migrate_external_market_tables,
    ),
    (
        intraday_schema._INTRADAY_ALERT_SCHEMA,
        intraday_schema.privileged_migrate_intraday_alert_tables,
    ),
    (
        quant_digest._QUANT_DIGEST_SCHEMA,
        quant_digest.privileged_migrate_quant_digest_tables,
    ),
    (
        commentary_profile_schema.COMMENTARY_PROFILE_SCHEMA,
        commentary_profile_schema.privileged_migrate_commentary_profile_table,
    ),
    (
        screener_schema.SCREENER_SCHEMA,
        screener_schema.privileged_migrate_screener_tables,
    ),
    (
        jq_minute_schema.JQ_MINUTE_SCHEMA,
        jq_minute_schema.privileged_migrate_jq_minute_tables,
    ),
)


def _select_only(connection, engine):
    assert connection.statements
    assert engine.begin_calls == 0
    for sql in connection.statements:
        assert " ".join(sql.upper().split()).startswith("SELECT ")
        assert not any(
            keyword in sql.upper()
            for keyword in ("CREATE TABLE", "ALTER TABLE", "INSERT INTO", "UPDATE ", "DELETE FROM")
        )


@pytest.mark.parametrize("_name,contracts,validator,legacy", _CASES)
def test_runtime_validator_and_legacy_ensure_are_select_only(
    _name, contracts, validator, legacy,
):
    connection = _PreparedConnection(contracts)
    engine = _ReadOnlyEngine(connection)

    validator(engine)
    if legacy is not None:
        legacy(engine)

    _select_only(connection, engine)


@pytest.mark.parametrize("contracts,migrate", _MIGRATION_CASES)
def test_privileged_migration_is_explicit_and_self_validating(contracts, migrate):
    connection = _MigrationConnection(contracts)
    engine = _MigrationEngine(connection)

    migrate(engine)

    assert engine.begin_calls == 1
    assert any("CREATE TABLE" in sql.upper() for sql in connection.statements)
    assert any(
        "FROM INFORMATION_SCHEMA.TABLES" in sql.upper()
        for sql in connection.statements
    )


@pytest.mark.parametrize("_name,contracts,validator,_legacy", _CASES)
def test_runtime_validator_fails_closed_on_missing_table(
    _name, contracts, validator, _legacy,
):
    missing = next(iter(contracts))
    connection = _PreparedConnection(contracts, missing_table=missing)
    engine = _ReadOnlyEngine(connection)

    with pytest.raises(RuntimeError, match="missing_tables"):
        validator(engine)

    _select_only(connection, engine)


@pytest.mark.parametrize("_name,contracts,validator,_legacy", _CASES)
def test_runtime_validator_fails_closed_on_physical_column_drift(
    _name, contracts, validator, _legacy,
):
    table = next(iter(contracts))
    column = next(iter(contracts[table].columns))
    connection = _PreparedConnection(
        contracts,
        column_overrides={(table, column): {"is_nullable": "YES"}},
    )
    engine = _ReadOnlyEngine(connection)

    with pytest.raises(RuntimeError, match="column type drift"):
        validator(engine)

    _select_only(connection, engine)


@pytest.mark.parametrize("_name,contracts,validator,_legacy", _CASES)
def test_runtime_validator_fails_closed_on_engine_or_collation_drift(
    _name, contracts, validator, _legacy,
):
    table = next(iter(contracts))
    connection = _PreparedConnection(
        contracts,
        table_overrides={table: {"engine": "MyISAM"}},
    )
    engine = _ReadOnlyEngine(connection)

    with pytest.raises(RuntimeError, match="storage drift"):
        validator(engine)

    _select_only(connection, engine)


@pytest.mark.parametrize("_name,contracts,validator,_legacy", _CASES)
def test_runtime_validator_fails_closed_on_required_index_drift(
    _name, contracts, validator, _legacy,
):
    table = next(iter(contracts))
    connection = _PreparedConnection(contracts, missing_index_table=table)
    engine = _ReadOnlyEngine(connection)

    with pytest.raises(RuntimeError, match="index drift"):
        validator(engine)

    _select_only(connection, engine)


def test_quant_digest_missing_schema_blocks_before_persistence_transaction():
    table = next(iter(quant_digest._QUANT_DIGEST_SCHEMA))
    connection = _PreparedConnection(
        quant_digest._QUANT_DIGEST_SCHEMA,
        missing_table=table,
    )
    engine = _ReadOnlyEngine(connection)

    with pytest.raises(RuntimeError, match="missing_tables"):
        quant_digest.persist_quant_digest(engine, {})

    _select_only(connection, engine)


def test_runtime_entrypoints_do_not_reference_privileged_migration_or_ddl():
    runtime_entrypoints = (
        radar.MarketRadarEngine.scan_once,
        theme_forecast.persist_premarket_theme_forecast,
        theme_forecast.persist_auction_confirmation,
        theme_forecast.mark_forecast_delivery,
        external_market.store_external_market_snapshot,
        external_market.load_latest_external_market_context,
        intraday_core.run_intraday_scan,
        quant_digest.persist_quant_digest,
    )
    for entrypoint in runtime_entrypoints:
        source = inspect.getsource(entrypoint).upper()
        assert "CREATE TABLE" not in source
        assert "ALTER TABLE" not in source
        assert "PRIVILEGED_MIGRATE" not in source
