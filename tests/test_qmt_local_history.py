from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest
from sqlalchemy.engine import make_url

from integrations.qmt import local_history
from integrations.qmt.local_history import (
    LOCAL_KLINE_TABLE,
    LOCAL_KLINE_ATTESTATION_REQUIRED_COLUMNS,
    LOCAL_KLINE_LEGACY_PROVENANCE,
    LOCAL_MINUTE_TABLE,
    LocalHistoryProvenanceSchemaError,
    _data_version,
    _dedicated_tunnel_history_url,
    _has_identity_query_overrides,
    _normalize_date,
    _same_database,
    get_local_history_engine,
    migrate_local_history_provenance_schema,
    validate_local_history_provenance_schema,
)
from tools.backfill_guojin_qmt_local_history import _resolve_limits


def test_local_history_tables_are_dedicated_qmt_tables():
    assert LOCAL_KLINE_TABLE == "qmt_local_stock_kline"
    assert LOCAL_MINUTE_TABLE == "qmt_local_stock_minute"


def test_same_database_blocks_localhost_equivalent_production_url():
    prod = "mysql+pymysql://root:pass@127.0.0.1:3306/probiga?charset=utf8mb4"
    local = "mysql+pymysql://root:pass@localhost:3306/probiga?charset=utf8mb4"

    assert _same_database(local, prod) is True


def test_same_database_allows_different_local_history_database():
    prod = "mysql+pymysql://root:pass@127.0.0.1:3306/probiga?charset=utf8mb4"
    local = "mysql+pymysql://root:pass@127.0.0.1:3306/probiga_qmt_history?charset=utf8mb4"

    assert _same_database(local, prod) is False


def test_same_database_rejects_same_schema_even_with_different_user():
    prod = "mysql+pymysql://runtime:pass@127.0.0.1:3306/probiga"
    local = "mysql+pymysql://history:pass@localhost:3306/probiga"

    assert _same_database(local, prod) is True


def test_same_database_normalizes_mysql_and_mariadb_dialects():
    prod = "mysql+pymysql://runtime:pass@127.0.0.1:3306/probiga"
    local = "mariadb+pymysql://history:pass@localhost:3306/probiga"

    assert _same_database(local, prod) is True


def test_equal_local_tunnel_config_routes_to_dedicated_history_schema(
    monkeypatch,
):
    prod = (
        "mysql+pymysql://runtime:secret@127.0.0.1:13306/"
        "probiga?charset=utf8mb4"
    )
    monkeypatch.setattr(
        "integrations.qmt.local_history.get_mysql_url",
        lambda required=False: prod,
    )
    monkeypatch.setattr(
        "integrations.qmt.local_history.get_qmt_history_mysql_url",
        lambda required=True: prod,
    )

    engine = get_local_history_engine()
    try:
        assert engine.url.database == "probiga_qmt_history"
        assert engine.url.host == "127.0.0.1"
        assert engine.url.port == 13306
        assert engine.url.username == "runtime"
        assert engine.url.password == "secret"
    finally:
        engine.dispose()


def test_equal_local_tunnel_config_preserves_explicit_history_identity(
    monkeypatch,
):
    prod = "mysql+pymysql://runtime:primary@127.0.0.1:13306/probiga"
    history = (
        "mysql+pymysql://history:dedicated@localhost:13306/"
        "probiga?charset=utf8mb4"
    )
    monkeypatch.setattr(
        "integrations.qmt.local_history.get_mysql_url",
        lambda required=False: prod,
    )
    monkeypatch.setattr(
        "integrations.qmt.local_history.get_qmt_history_mysql_url",
        lambda required=True: history,
    )

    engine = get_local_history_engine()
    try:
        assert engine.url.database == "probiga_qmt_history"
        assert engine.url.host == "localhost"
        assert engine.url.port == 13306
        assert engine.url.username == "history"
        assert engine.url.password == "dedicated"
        assert engine.url.query["charset"] == "utf8mb4"
    finally:
        engine.dispose()


def test_missing_history_config_derives_exact_loopback_history_schema(
    monkeypatch,
):
    prod = "mysql+pymysql://runtime:secret@127.0.0.1:13306/probiga"
    monkeypatch.setattr(
        "integrations.qmt.local_history.get_mysql_url",
        lambda required=False: prod,
    )

    def missing_history_url(required=True):
        raise RuntimeError("missing")

    monkeypatch.setattr(
        "integrations.qmt.local_history.get_qmt_history_mysql_url",
        missing_history_url,
    )

    engine = get_local_history_engine()
    try:
        assert engine.url.database == "probiga_qmt_history"
    finally:
        engine.dispose()


def test_local_history_engine_uses_central_tls_factory(monkeypatch):
    prod = "mysql+pymysql://runtime:secret@127.0.0.1:3306/probiga"
    captured = {}
    sentinel = object()
    monkeypatch.setattr(
        "integrations.qmt.local_history.get_mysql_url",
        lambda required=False: prod,
    )

    def missing_history_url(required=True):
        raise RuntimeError("missing")

    monkeypatch.setattr(
        "integrations.qmt.local_history.get_qmt_history_mysql_url",
        missing_history_url,
    )

    def create_tls_engine(url, **kwargs):
        captured["url"] = url
        captured["kwargs"] = kwargs
        return sentinel

    monkeypatch.setattr(
        local_history,
        "create_pooled_engine",
        create_tls_engine,
    )

    assert get_local_history_engine() is sentinel
    assert make_url(captured["url"]).database == "probiga_qmt_history"
    assert captured["kwargs"] == {"pool_pre_ping": True, "future": True}


@pytest.mark.parametrize(
    "base_url",
    [
        "mysql+pymysql://runtime:secret@/probiga",
        "mysql+pymysql://runtime:secret@[::1]:13306/probiga",
        "mysql+pymysql://runtime:secret@127.0.0.1:13306/other",
        "postgresql://runtime:secret@127.0.0.1:13306/probiga",
        "mysql+pyodbc://runtime:secret@127.0.0.1:13306/probiga",
    ],
)
def test_dedicated_tunnel_history_url_fails_closed_outside_exact_contract(
    base_url,
):
    assert _dedicated_tunnel_history_url(base_url) == ""


@pytest.mark.parametrize(
    "query",
    [
        "database=probiga",
        "host=db.example",
        "port=3306",
        "unix_socket=%2Ftmp%2Fmysql.sock",
        "init_command=USE%20probiga",
        "read_default_file=%2Ftmp%2Fmysql.cnf",
        "named_pipe=1",
    ],
)
def test_database_identity_query_overrides_are_rejected(monkeypatch, query):
    prod = "mysql+pymysql://runtime:secret@127.0.0.1:13306/probiga"
    history = (
        "mysql+pymysql://history:secret@127.0.0.1:13306/"
        f"probiga_qmt_history?{query}"
    )
    assert _has_identity_query_overrides(history) is True
    assert _dedicated_tunnel_history_url(f"{prod}?{query}") == ""
    monkeypatch.setattr(
        "integrations.qmt.local_history.get_mysql_url",
        lambda required=False: prod,
    )
    monkeypatch.setattr(
        "integrations.qmt.local_history.get_qmt_history_mysql_url",
        lambda required=True: history,
    )

    with pytest.raises(RuntimeError, match="身份覆盖参数"):
        get_local_history_engine()


def test_non_pymysql_history_driver_is_rejected(monkeypatch):
    prod = "mysql+pymysql://runtime:secret@127.0.0.1:13306/probiga"
    history = (
        "mysql+pyodbc://history:secret@127.0.0.1:13306/"
        "probiga_qmt_history?odbc_connect=hidden"
    )
    monkeypatch.setattr(
        "integrations.qmt.local_history.get_mysql_url",
        lambda required=False: prod,
    )
    monkeypatch.setattr(
        "integrations.qmt.local_history.get_qmt_history_mysql_url",
        lambda required=True: history,
    )

    with pytest.raises(RuntimeError, match=r"mysql\+pymysql"):
        get_local_history_engine()


def test_safe_connection_query_parameters_are_preserved(monkeypatch):
    prod = "mysql+pymysql://runtime:secret@127.0.0.1:13306/probiga"
    history = (
        "mysql+pymysql://history:secret@127.0.0.1:13306/"
        "probiga_qmt_history?charset=utf8mb4&connect_timeout=10"
    )
    monkeypatch.setattr(
        "integrations.qmt.local_history.get_mysql_url",
        lambda required=False: prod,
    )
    monkeypatch.setattr(
        "integrations.qmt.local_history.get_qmt_history_mysql_url",
        lambda required=True: history,
    )

    engine = get_local_history_engine()
    try:
        assert engine.url.query["charset"] == "utf8mb4"
        assert engine.url.query["connect_timeout"] == "10"
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "same_database_url",
    [
        "mysql+pymysql://runtime:secret@/probiga",
        "mysql+pymysql://runtime:secret@[::1]:13306/probiga",
        "mysql+pymysql://runtime:secret@127.0.0.1:13306/other",
        "postgresql://runtime:secret@127.0.0.1:13306/probiga",
    ],
)
def test_equal_non_contract_database_config_remains_blocked(
    monkeypatch,
    same_database_url,
):
    monkeypatch.setattr(
        "integrations.qmt.local_history.get_mysql_url",
        lambda required=False: same_database_url,
    )
    monkeypatch.setattr(
        "integrations.qmt.local_history.get_qmt_history_mysql_url",
        lambda required=True: same_database_url,
    )

    with pytest.raises(RuntimeError):
        get_local_history_engine()


def test_equal_remote_production_config_remains_blocked(monkeypatch):
    prod = "mysql+pymysql://runtime:secret@db.example:3306/probiga"
    monkeypatch.setattr(
        "integrations.qmt.local_history.get_mysql_url",
        lambda required=False: prod,
    )
    monkeypatch.setattr(
        "integrations.qmt.local_history.get_qmt_history_mysql_url",
        lambda required=True: prod,
    )

    with pytest.raises(RuntimeError, match="相同"):
        get_local_history_engine()


def test_normalize_date_accepts_compact_and_datetime_values():
    assert _normalize_date("20260626") == "2026-06-26"
    assert _normalize_date(datetime(2026, 6, 26, 15, 0)) == "2026-06-26"


def test_data_version_ignores_batch_runtime_fields():
    row_a = {"stock_code": "000001", "close": 10.2, "batch_id": "a", "received_at": "x"}
    row_b = {"stock_code": "000001", "close": 10.2, "batch_id": "b", "received_at": "y"}

    assert _data_version(row_a) == _data_version(row_b)


def test_from_gaps_limit_does_not_limit_stock_universe_by_default():
    limits = _resolve_limits("from-gaps", limit=50, stock_limit=None, gap_limit=None)

    assert limits.gap_limit == 50
    assert limits.stock_limit == 0


def test_daily_limit_still_limits_stock_universe():
    limits = _resolve_limits("daily", limit=50, stock_limit=None, gap_limit=None)

    assert limits.stock_limit == 50
    assert limits.gap_limit == 20


def _provenance_columns(
    *,
    include_origin: bool,
    origin_default: str = LOCAL_KLINE_LEGACY_PROVENANCE,
):
    columns = []
    for name in sorted(LOCAL_KLINE_ATTESTATION_REQUIRED_COLUMNS):
        if name == "pre_close_origin" and not include_origin:
            continue
        columns.append(
            {
                "name": name,
                "type": (
                    'VARCHAR(32) COLLATE "utf8mb4_general_ci"'
                    if name == "pre_close_origin"
                    else "TEXT"
                ),
                "nullable": False,
                "default": (
                    f"'{origin_default}'"
                    if name == "pre_close_origin"
                    else None
                ),
            }
        )
    return columns


class _SchemaInspector:
    def __init__(self, engine):
        self.engine = engine

    def has_table(self, table_name, *, schema=None):
        self.engine.inspections.append(("has_table", schema, table_name))
        return self.engine.table_exists

    def get_columns(self, table_name, *, schema=None):
        self.engine.inspections.append(("get_columns", schema, table_name))
        return list(self.engine.columns)


class _SchemaBegin:
    def __init__(self, engine):
        self.engine = engine

    def __enter__(self):
        return self.engine

    def __exit__(self, exc_type, exc, traceback):
        return False


class _SchemaEngine:
    def __init__(self, columns):
        self.url = make_url(
            "mysql+pymysql://history:secret@127.0.0.1:13306/"
            "probiga_qmt_history"
        )
        self.table_exists = True
        self.columns = list(columns)
        self.inspections = []
        self.statements = []

    def begin(self):
        return _SchemaBegin(self)

    def execute(self, statement):
        sql = str(statement)
        self.statements.append(sql)
        if "ADD COLUMN `pre_close_origin`" in sql:
            self.columns.append(
                {
                    "name": "pre_close_origin",
                    "type": 'VARCHAR(32) COLLATE "utf8mb4_general_ci"',
                    "nullable": False,
                    "default": f"'{LOCAL_KLINE_LEGACY_PROVENANCE}'",
                }
            )
        return SimpleNamespace(rowcount=0)


def test_provenance_schema_validation_reads_exact_qualified_table(monkeypatch):
    engine = _SchemaEngine(_provenance_columns(include_origin=True))
    monkeypatch.setattr(
        local_history,
        "inspect",
        lambda bind: _SchemaInspector(bind),
    )

    result = validate_local_history_provenance_schema(engine)

    assert result["ready"] is True
    assert result["qualified_table"] == (
        "`probiga_qmt_history`.`qmt_local_stock_kline`"
    )
    assert result["legacy_rows_default_to"] == "UNVERIFIED_LEGACY"
    assert engine.statements == []


def test_provenance_schema_validation_blocks_missing_origin_without_write(
    monkeypatch,
):
    engine = _SchemaEngine(_provenance_columns(include_origin=False))
    monkeypatch.setattr(
        local_history,
        "inspect",
        lambda bind: _SchemaInspector(bind),
    )

    with pytest.raises(
        LocalHistoryProvenanceSchemaError,
        match="pre_close_origin",
    ):
        validate_local_history_provenance_schema(engine)

    assert engine.statements == []


def test_provenance_migration_is_explicit_instant_and_legacy_ineligible(
    monkeypatch,
):
    engine = _SchemaEngine(_provenance_columns(include_origin=False))
    monkeypatch.setattr(
        local_history,
        "inspect",
        lambda bind: _SchemaInspector(bind),
    )

    planned = migrate_local_history_provenance_schema(engine, apply=False)
    assert planned["status"] == "migration_required"
    assert planned["applied"] is False
    assert engine.statements == []

    applied = migrate_local_history_provenance_schema(engine, apply=True)

    assert applied["status"] == "applied"
    assert applied["ready"] is True
    assert engine.statements[0] == "SET SESSION lock_wait_timeout=30"
    assert len(engine.statements) == 2
    ddl = engine.statements[1]
    assert "`probiga_qmt_history`.`qmt_local_stock_kline`" in ddl
    assert "DEFAULT 'UNVERIFIED_LEGACY'" in ddl
    assert "ALGORITHM=INSTANT" in ddl
    assert "NATIVE_QMT" not in ddl


def test_provenance_schema_rejects_native_default(monkeypatch):
    engine = _SchemaEngine(
        _provenance_columns(
            include_origin=True,
            origin_default="NATIVE_QMT",
        )
    )
    monkeypatch.setattr(
        local_history,
        "inspect",
        lambda bind: _SchemaInspector(bind),
    )

    with pytest.raises(
        LocalHistoryProvenanceSchemaError,
        match="default must be UNVERIFIED_LEGACY",
    ):
        validate_local_history_provenance_schema(engine)
