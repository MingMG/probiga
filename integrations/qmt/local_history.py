from __future__ import annotations

import hashlib
import json
import math
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url

from integrations.bigqmt.spool import PROVIDER_ID as BIGQMT_PROVIDER_ID
from integrations.qmt import bridge
from integrations.qmt.backend import to_qmt_symbol
from integrations.qmt.diagnostics import PROVIDER_ID as LEGACY_PROVIDER_ID
from server.common.config import get_mysql_url, get_qmt_history_mysql_url
from server.common.qmt_stock_catalog import load_stock_catalog
from server.common.qmt_trade_calendar import load_trade_calendar_receipt


CHINA_STANDARD_TIME = timezone(timedelta(hours=8), name="Asia/Shanghai")
LOCAL_KLINE_TABLE = "qmt_local_stock_kline"
LOCAL_MINUTE_TABLE = "qmt_local_stock_minute"
LOCAL_RUN_TABLE = "qmt_local_backfill_run"
LOCAL_KLINE_PROVENANCE_COLUMN = "pre_close_origin"
LOCAL_KLINE_LEGACY_PROVENANCE = "UNVERIFIED_LEGACY"
LOCAL_KLINE_NATIVE_PROVENANCE = "NATIVE_QMT"
LOCAL_HISTORY_MIGRATION_LOCK_WAIT_SECONDS = 30
LOCAL_KLINE_ATTESTATION_REQUIRED_COLUMNS = frozenset(
    {
        "id",
        "provider",
        "qmt_code",
        "stock_code",
        "period",
        "trade_date",
        "adjust_type",
        "open",
        "close",
        "high",
        "low",
        "volume",
        "amount",
        "pre_close",
        LOCAL_KLINE_PROVENANCE_COLUMN,
        "source_time",
        "received_at",
        "batch_id",
        "data_version",
        "permission_status",
    }
)
_IDENTITY_QUERY_KEYS = frozenset(
    {
        "database",
        "db",
        "host",
        "init_command",
        "named_pipe",
        "passwd",
        "password",
        "port",
        "read_default_file",
        "read_default_group",
        "unix_socket",
        "user",
        "username",
    }
)


class LocalHistoryProvenanceSchemaError(RuntimeError):
    """The QMT source table cannot satisfy the frozen provenance contract."""


class LocalHistorySchemaError(LocalHistoryProvenanceSchemaError):
    """The local QMT history schema is absent or physically incompatible."""


_LOCAL_HISTORY_TABLE_CONTRACTS: dict[str, dict[str, Any]] = {
    LOCAL_KLINE_TABLE: {
        "columns": (
            "id", "provider", "qmt_code", "stock_code", "short_name",
            "period", "trade_time", "trade_date", "k_type", "adjust_type",
            "open", "close", "high", "low", "volume", "amount", "change",
            "change_pct", "turnover_ratio", "pre_close",
            "pre_close_origin", "source_time", "received_at", "batch_id",
            "data_version", "quality_status", "permission_status",
            "created_at", "updated_at",
        ),
        "indexes": {
            "uk_qmt_local_kline": (
                True,
                ("provider", "stock_code", "period", "trade_date", "adjust_type"),
            ),
            "idx_qmt_local_kline_date": (False, ("trade_date",)),
            "idx_qmt_local_kline_code_time": (
                False,
                ("stock_code", "trade_time"),
            ),
        },
    },
    LOCAL_MINUTE_TABLE: {
        "columns": (
            "id", "provider", "qmt_code", "stock_code", "short_name",
            "period", "trade_time", "trade_date", "price", "avg_price",
            "change", "change_pct", "volume", "amount", "pre_close",
            "source_time", "received_at", "batch_id", "data_version",
            "quality_status", "permission_status", "created_at", "updated_at",
        ),
        "indexes": {
            "uk_qmt_local_minute": (
                True,
                ("provider", "stock_code", "period", "trade_time"),
            ),
            "idx_qmt_local_minute_date": (False, ("trade_date",)),
            "idx_qmt_local_minute_code_time": (
                False,
                ("stock_code", "trade_time"),
            ),
        },
    },
    LOCAL_RUN_TABLE: {
        "columns": (
            "id", "run_id", "provider", "dataset", "period", "start_date",
            "end_date", "status", "requested_codes", "fetched_rows",
            "written_rows", "error_message", "started_at", "finished_at",
            "extra_json", "created_at",
        ),
        "indexes": {
            "uk_qmt_local_run": (True, ("run_id",)),
            "idx_qmt_local_run_dataset": (
                False,
                ("dataset", "period", "status"),
            ),
        },
    },
}


@dataclass(frozen=True)
class LocalBackfillBatchResult:
    dataset: str
    period: str
    start_date: str
    end_date: str
    requested_codes: int
    fetched_rows: int
    written_rows: int
    skipped: bool
    error: str | None = None
    allowed_missing_codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class LocalBackfillResult:
    run_id: str
    dataset: str
    status: str
    local_database: str
    start_date: str
    end_date: str
    code_count: int
    batch_count: int
    fetched_rows: int
    written_rows: int
    batches: list[LocalBackfillBatchResult]


def now_china() -> datetime:
    return datetime.now(CHINA_STANDARD_TIME).replace(tzinfo=None, microsecond=0)


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))


def _data_version(row: Mapping[str, Any]) -> str:
    keys = sorted(key for key in row if key not in {"received_at", "batch_id", "data_version"})
    payload = {key: row.get(key) for key in keys}
    return hashlib.sha256(_canonical(payload).encode("utf-8")).hexdigest()[:32]


def _clean_value(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return item()
        except Exception:
            return value
    return value


def _normalize_date(value: str | date | datetime) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    if len(digits) >= 8:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
    return str(value or "").strip()[:10]


def _dialect_family(drivername: str) -> str:
    dialect = (drivername or "").split("+", 1)[0].lower()
    return "mysql" if dialect in {"mysql", "mariadb"} else dialect


def _has_identity_query_overrides(raw_url: str) -> bool:
    if not raw_url:
        return False
    query_keys = {str(key).lower() for key in make_url(raw_url).query}
    return bool(query_keys & _IDENTITY_QUERY_KEYS)


def _same_database(url_a: str, url_b: str) -> bool:
    if not url_a or not url_b:
        return False
    a = make_url(url_a)
    b = make_url(url_b)
    return (
        _dialect_family(a.drivername) == _dialect_family(b.drivername)
        and (a.host or "localhost").lower() in {(b.host or "localhost").lower(), "localhost", "127.0.0.1"}
        and (b.host or "localhost").lower() in {(a.host or "localhost").lower(), "localhost", "127.0.0.1"}
        and int(a.port or 3306) == int(b.port or 3306)
        and (a.database or "") == (b.database or "")
    )


def _same_server_and_user(url_a: str, url_b: str) -> bool:
    if not url_a or not url_b:
        return False
    a = make_url(url_a)
    b = make_url(url_b)
    localhost_aliases = {"localhost", "127.0.0.1"}
    host_a = (a.host or "localhost").lower()
    host_b = (b.host or "localhost").lower()
    hosts_match = host_a == host_b or {host_a, host_b}.issubset(
        localhost_aliases
    )
    return (
        _dialect_family(a.drivername) == _dialect_family(b.drivername)
        and hosts_match
        and int(a.port or 3306) == int(b.port or 3306)
        and (a.username or "") == (b.username or "")
    )


def _dedicated_tunnel_history_url(base_url: str) -> str:
    """Derive the dedicated history schema only for the production tunnel."""
    if not base_url:
        return ""
    parsed = make_url(base_url)
    if (
        (parsed.drivername or "").lower() != "mysql+pymysql"
        or (parsed.host or "").lower() not in {"localhost", "127.0.0.1"}
        or (parsed.database or "") != "probiga"
        or _has_identity_query_overrides(base_url)
    ):
        return ""
    return parsed.set(database="probiga_qmt_history").render_as_string(
        hide_password=False
    )


def get_local_history_engine(local_url: str | None = None) -> Engine:
    prod = get_mysql_url(required=False)
    if prod and _has_identity_query_overrides(prod):
        raise RuntimeError(
            "生产 MYSQL_URL 含数据库身份覆盖参数，QMT 历史库已拒绝执行"
        )

    try:
        resolved = (local_url or get_qmt_history_mysql_url(required=True)).strip()
    except RuntimeError:
        resolved = _dedicated_tunnel_history_url(prod)
        if not resolved:
            raise
    if make_url(resolved).drivername.lower() != "mysql+pymysql":
        raise RuntimeError("QMT 历史库仅允许显式 mysql+pymysql URL")
    if _has_identity_query_overrides(resolved):
        raise RuntimeError(
            "QMT 历史库 URL 含数据库身份覆盖参数，已拒绝执行"
        )
    if prod and _same_database(resolved, prod):
        # The production API reaches RDS through a fixed local tunnel.  A
        # legacy environment may therefore point QMT_HISTORY_MYSQL_URL at the
        # primary schema by mistake.  Route that one local-tunnel case to the
        # dedicated, read-only history schema; never reinterpret a remote URL.
        prod_history = _dedicated_tunnel_history_url(prod)
        resolved_history = _dedicated_tunnel_history_url(resolved)
        if not prod_history or not resolved_history:
            raise RuntimeError(
                "QMT 历史本地库配置与生产 MYSQL_URL 相同，已拒绝执行"
            )
        # Derive from the explicitly configured history URL so a dedicated
        # history identity and all connection parameters remain intact.
        resolved = resolved_history
    if prod and _same_database(resolved, prod):
        raise RuntimeError("QMT 历史本地库配置与生产 MYSQL_URL 相同，已拒绝执行")
    return create_engine(resolved, pool_pre_ping=True, future=True)


def _bind_database_name(bind: Any, database: str | None) -> str:
    resolved = str(database or "").strip()
    if not resolved:
        engine = getattr(bind, "engine", bind)
        url = getattr(engine, "url", None)
        resolved = str(getattr(url, "database", "") or "").strip()
    if not resolved:
        raise LocalHistoryProvenanceSchemaError(
            "QMT history database name is required for qualified schema validation"
        )
    return resolved


def _quoted_identifier(value: str) -> str:
    return f"`{str(value).replace('`', '``')}`"


def _normalized_column_default(value: Any) -> str:
    normalized = str(value or "").strip()
    if (
        len(normalized) >= 2
        and normalized[0] == normalized[-1]
        and normalized[0] in {"'", '"'}
    ):
        normalized = normalized[1:-1]
    return normalized


def local_history_provenance_schema_snapshot(
    bind: Any,
    *,
    database: str | None = None,
) -> dict[str, Any]:
    """Read the qualified QMT daily source contract without mutating it."""

    database_name = _bind_database_name(bind, database)
    qualified_table = (
        f"{_quoted_identifier(database_name)}."
        f"{_quoted_identifier(LOCAL_KLINE_TABLE)}"
    )
    inspector = inspect(bind)
    table_exists = bool(
        inspector.has_table(LOCAL_KLINE_TABLE, schema=database_name)
    )
    columns = (
        inspector.get_columns(LOCAL_KLINE_TABLE, schema=database_name)
        if table_exists
        else []
    )
    by_name = {
        str(column.get("name") or ""): column
        for column in columns
        if str(column.get("name") or "")
    }
    missing_columns = sorted(
        LOCAL_KLINE_ATTESTATION_REQUIRED_COLUMNS - set(by_name)
    )
    provenance = by_name.get(LOCAL_KLINE_PROVENANCE_COLUMN)
    provenance_type = (
        " ".join(str(provenance.get("type") or "").lower().split())
        if provenance
        else ""
    )
    provenance_nullable = (
        bool(provenance.get("nullable")) if provenance else None
    )
    provenance_default = (
        _normalized_column_default(provenance.get("default"))
        if provenance
        else ""
    )
    contract_errors: list[str] = []
    if not table_exists:
        contract_errors.append("qualified source table is missing")
    if missing_columns:
        contract_errors.append(
            "required columns are missing: " + ", ".join(missing_columns)
        )
    if provenance is not None:
        if not provenance_type.startswith("varchar(32)"):
            contract_errors.append(
                "pre_close_origin must be VARCHAR(32)"
            )
        if provenance_nullable is not False:
            contract_errors.append("pre_close_origin must be NOT NULL")
        if provenance_default != LOCAL_KLINE_LEGACY_PROVENANCE:
            contract_errors.append(
                "pre_close_origin default must be UNVERIFIED_LEGACY"
            )
    return {
        "database": database_name,
        "table": LOCAL_KLINE_TABLE,
        "qualified_table": qualified_table,
        "table_exists": table_exists,
        "column_count": len(by_name),
        "required_columns": sorted(LOCAL_KLINE_ATTESTATION_REQUIRED_COLUMNS),
        "missing_columns": missing_columns,
        "provenance_column": LOCAL_KLINE_PROVENANCE_COLUMN,
        "provenance_type": provenance_type,
        "provenance_nullable": provenance_nullable,
        "provenance_default": provenance_default,
        "legacy_rows_default_to": LOCAL_KLINE_LEGACY_PROVENANCE,
        "ready": not contract_errors,
        "errors": contract_errors,
    }


def validate_local_history_provenance_schema(
    bind: Any,
    *,
    database: str | None = None,
) -> dict[str, Any]:
    """Fail closed unless the qualified attestation source is provenance-safe."""

    snapshot = local_history_provenance_schema_snapshot(
        bind,
        database=database,
    )
    if not snapshot["ready"]:
        raise LocalHistoryProvenanceSchemaError(
            f"QMT 历史来源表契约未准备: {snapshot['qualified_table']}: "
            + "; ".join(snapshot["errors"])
            + "; 请在获授权的 QMT 历史库迁移边界显式运行 "
            "tools/migrate_qmt_local_history_provenance.py --apply"
        )
    return snapshot


def migrate_local_history_provenance_schema(
    engine: Engine,
    *,
    apply: bool = False,
    database: str | None = None,
) -> dict[str, Any]:
    """Add only the legacy-ineligible provenance column, then read it back."""

    if type(apply) is not bool:
        raise TypeError("apply must be bool")
    snapshot = local_history_provenance_schema_snapshot(
        engine,
        database=database,
    )
    if not snapshot["table_exists"]:
        raise LocalHistoryProvenanceSchemaError(
            f"QMT 历史来源表不存在: {snapshot['qualified_table']}"
        )
    missing = set(snapshot["missing_columns"])
    if not missing:
        validated = validate_local_history_provenance_schema(
            engine,
            database=snapshot["database"],
        )
        return {**validated, "status": "exists", "applied": False}
    if missing != {LOCAL_KLINE_PROVENANCE_COLUMN}:
        raise LocalHistoryProvenanceSchemaError(
            f"QMT 历史来源表缺少不可自动兼容的列: "
            + ", ".join(sorted(missing))
        )
    if not apply:
        return {
            **snapshot,
            "status": "migration_required",
            "applied": False,
        }

    qualified_table = str(snapshot["qualified_table"])
    statement = text(
        f"ALTER TABLE {qualified_table} "
        f"ADD COLUMN {_quoted_identifier(LOCAL_KLINE_PROVENANCE_COLUMN)} "
        "VARCHAR(32) NOT NULL DEFAULT 'UNVERIFIED_LEGACY' AFTER `pre_close`, "
        "ALGORITHM=INSTANT"
    )
    try:
        with engine.begin() as connection:
            connection.execute(text(
                "SET SESSION lock_wait_timeout="
                f"{LOCAL_HISTORY_MIGRATION_LOCK_WAIT_SECONDS}"
            ))
            connection.execute(statement)
    except Exception as exc:
        try:
            validated = validate_local_history_provenance_schema(
                engine,
                database=snapshot["database"],
            )
        except LocalHistoryProvenanceSchemaError:
            raise exc
        return {**validated, "status": "exists", "applied": False}
    validated = validate_local_history_provenance_schema(
        engine,
        database=snapshot["database"],
    )
    return {**validated, "status": "applied", "applied": True}


def local_history_schema_snapshot(
    bind: Any,
    *,
    database: str | None = None,
) -> dict[str, Any]:
    """Read the complete local-history table shape without issuing DDL."""

    database_name = _bind_database_name(bind, database)
    inspector = inspect(bind)
    tables: dict[str, Any] = {}
    errors: list[str] = []
    for table_name, contract in _LOCAL_HISTORY_TABLE_CONTRACTS.items():
        table_errors: list[str] = []
        exists = bool(inspector.has_table(table_name, schema=database_name))
        actual_columns: tuple[str, ...] = ()
        actual_indexes: dict[str, tuple[bool, tuple[str, ...]]] = {}
        primary_key: tuple[str, ...] = ()
        if exists:
            actual_columns = tuple(
                str(row.get("name") or "")
                for row in inspector.get_columns(
                    table_name,
                    schema=database_name,
                )
            )
            primary_key = tuple(
                str(value)
                for value in (
                    inspector.get_pk_constraint(
                        table_name,
                        schema=database_name,
                    ).get("constrained_columns")
                    or ()
                )
            )
            for row in inspector.get_indexes(
                table_name,
                schema=database_name,
            ):
                index_name = str(row.get("name") or "")
                if not index_name or index_name.upper() == "PRIMARY":
                    continue
                actual_indexes[index_name] = (
                    bool(row.get("unique")),
                    tuple(str(value) for value in (row.get("column_names") or ())),
                )
        else:
            table_errors.append("table is missing")
        expected_columns = tuple(contract["columns"])
        expected_indexes = dict(contract["indexes"])
        if exists and actual_columns != expected_columns:
            table_errors.append("column inventory/order differs")
        if exists and primary_key != ("id",):
            table_errors.append("primary key must be exactly id")
        if exists and actual_indexes != expected_indexes:
            table_errors.append("secondary index inventory differs")
        if table_errors:
            errors.extend(
                f"{table_name}: {message}" for message in table_errors
            )
        tables[table_name] = {
            "exists": exists,
            "columns": list(actual_columns),
            "expected_columns": list(expected_columns),
            "primary_key": list(primary_key),
            "indexes": {
                key: {"unique": value[0], "columns": list(value[1])}
                for key, value in sorted(actual_indexes.items())
            },
            "expected_indexes": {
                key: {"unique": value[0], "columns": list(value[1])}
                for key, value in sorted(expected_indexes.items())
            },
            "ready": not table_errors,
            "errors": table_errors,
        }
    return {
        "database": database_name,
        "table_count": sum(
            1 for detail in tables.values() if detail["exists"]
        ),
        "expected_table_count": len(_LOCAL_HISTORY_TABLE_CONTRACTS),
        "tables": tables,
        "ready": not errors,
        "errors": errors,
        "ddl_executed": False,
    }


def validate_local_history_tables(
    bind: Any,
    *,
    database: str | None = None,
) -> dict[str, Any]:
    """Fail closed unless scheduled DML can use the frozen physical schema."""

    snapshot = local_history_schema_snapshot(bind, database=database)
    if not snapshot["ready"]:
        raise LocalHistorySchemaError(
            "QMT 本地历史库物理契约未准备: "
            + "; ".join(snapshot["errors"])
            + "; 常规定时任务禁止 CREATE/ALTER，请由特权账号显式运行 "
            "tools/backfill_guojin_qmt_local_history.py init"
        )
    validate_local_history_provenance_schema(
        bind,
        database=snapshot["database"],
    )
    return snapshot


def privileged_migrate_local_history_schema(engine: Engine) -> dict[str, Any]:
    """Install the frozen local-history schema inside a privileged window.

    Scheduled capture and backfill callers must use
    :func:`validate_local_history_tables` instead.  The only supported
    additive upgrade for an existing database is the provenance column whose
    legacy-safe value is frozen by ``migrate_local_history_provenance_schema``.
    """

    database_name = _bind_database_name(engine, None)
    if inspect(engine).has_table(LOCAL_KLINE_TABLE, schema=database_name):
        provenance = local_history_provenance_schema_snapshot(
            engine,
            database=database_name,
        )
        missing = set(provenance["missing_columns"])
        if missing == {LOCAL_KLINE_PROVENANCE_COLUMN}:
            migrate_local_history_provenance_schema(
                engine,
                apply=True,
                database=database_name,
            )
        elif missing or provenance["errors"]:
            validate_local_history_provenance_schema(
                engine,
                database=database_name,
            )
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {LOCAL_KLINE_TABLE} (
                    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    provider VARCHAR(32) NOT NULL DEFAULT 'gj_qmt',
                    qmt_code VARCHAR(32) NOT NULL,
                    stock_code VARCHAR(16) NOT NULL,
                    short_name VARCHAR(128) NULL,
                    period VARCHAR(16) NOT NULL DEFAULT '1d',
                    trade_time DATETIME NOT NULL,
                    trade_date DATE NOT NULL,
                    k_type INT NOT NULL DEFAULT 1,
                    adjust_type INT NOT NULL DEFAULT 1,
                    open DECIMAL(20,6) NULL,
                    close DECIMAL(20,6) NULL,
                    high DECIMAL(20,6) NULL,
                    low DECIMAL(20,6) NULL,
                    volume DECIMAL(24,6) NULL,
                    amount DECIMAL(24,6) NULL,
                    `change` DECIMAL(20,6) NULL,
                    change_pct DECIMAL(20,6) NULL,
                    turnover_ratio DECIMAL(20,6) NULL,
                    pre_close DECIMAL(20,6) NULL,
                    pre_close_origin VARCHAR(32) NOT NULL
                        DEFAULT 'UNVERIFIED_LEGACY',
                    source_time DATETIME NULL,
                    received_at DATETIME NOT NULL,
                    batch_id VARCHAR(64) NOT NULL,
                    data_version VARCHAR(64) NULL,
                    quality_status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
                    permission_status VARCHAR(32) NOT NULL DEFAULT 'SUPPORTED',
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NULL,
                    UNIQUE KEY uk_qmt_local_kline (provider, stock_code, period, trade_date, adjust_type),
                    KEY idx_qmt_local_kline_date (trade_date),
                    KEY idx_qmt_local_kline_code_time (stock_code, trade_time)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {LOCAL_MINUTE_TABLE} (
                    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    provider VARCHAR(32) NOT NULL DEFAULT 'gj_qmt',
                    qmt_code VARCHAR(32) NOT NULL,
                    stock_code VARCHAR(16) NOT NULL,
                    short_name VARCHAR(128) NULL,
                    period VARCHAR(16) NOT NULL DEFAULT '1m',
                    trade_time DATETIME NOT NULL,
                    trade_date DATE NOT NULL,
                    price DECIMAL(20,6) NULL,
                    avg_price DECIMAL(20,6) NULL,
                    `change` DECIMAL(20,6) NULL,
                    change_pct DECIMAL(20,6) NULL,
                    volume DECIMAL(24,6) NULL,
                    amount DECIMAL(24,6) NULL,
                    pre_close DECIMAL(20,6) NULL,
                    source_time DATETIME NULL,
                    received_at DATETIME NOT NULL,
                    batch_id VARCHAR(64) NOT NULL,
                    data_version VARCHAR(64) NULL,
                    quality_status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
                    permission_status VARCHAR(32) NOT NULL DEFAULT 'SUPPORTED',
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NULL,
                    UNIQUE KEY uk_qmt_local_minute (provider, stock_code, period, trade_time),
                    KEY idx_qmt_local_minute_date (trade_date),
                    KEY idx_qmt_local_minute_code_time (stock_code, trade_time)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
        )
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {LOCAL_RUN_TABLE} (
                    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    run_id VARCHAR(64) NOT NULL,
                    provider VARCHAR(32) NOT NULL DEFAULT 'gj_qmt',
                    dataset VARCHAR(64) NOT NULL,
                    period VARCHAR(16) NOT NULL,
                    start_date DATE NOT NULL,
                    end_date DATE NOT NULL,
                    status VARCHAR(32) NOT NULL,
                    requested_codes INT NOT NULL DEFAULT 0,
                    fetched_rows BIGINT NOT NULL DEFAULT 0,
                    written_rows BIGINT NOT NULL DEFAULT 0,
                    error_message TEXT NULL,
                    started_at DATETIME NOT NULL,
                    finished_at DATETIME NULL,
                    extra_json TEXT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uk_qmt_local_run (run_id),
                    KEY idx_qmt_local_run_dataset (dataset, period, status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
        )
    validated = validate_local_history_tables(
        engine,
        database=database_name,
    )
    return {
        **validated,
        "migration_boundary": "privileged_local_history_release",
        "ddl_executed": True,
    }


def load_stock_codes(source_engine: Engine, *, codes: Sequence[str] | None = None, limit: int = 0) -> list[str]:
    if codes:
        result = []
        for code in codes:
            text_value = str(code or "").strip()
            if not text_value:
                continue
            result.append(text_value.split(".", 1)[0].zfill(6))
        return sorted(set(result))
    with source_engine.begin() as conn:
        catalog = load_stock_catalog(
            conn,
            decision_known_at=datetime.now(CHINA_STANDARD_TIME).replace(
                tzinfo=None, microsecond=0
            ),
        )
    result = catalog.eligible_codes(date.today().isoformat())
    return result[:limit] if limit > 0 else result


def load_trade_dates(source_engine: Engine, *, start_date: str, end_date: str, limit: int = 0) -> list[str]:
    normalized_start = _normalize_date(start_date)
    normalized_end = _normalize_date(end_date)
    decision_time = datetime.now(CHINA_STANDARD_TIME).replace(
        tzinfo=None, microsecond=0
    )
    with source_engine.begin() as conn:
        receipt = load_trade_calendar_receipt(
            conn,
            start_date=normalized_start,
            end_date=normalized_end,
            decision_known_at=decision_time,
        )
    result = receipt.sessions_between(normalized_start, normalized_end)
    return result[:limit] if limit > 0 else result


def _chunked(items: Sequence[str], size: int) -> Iterable[list[str]]:
    chunk_size = max(1, int(size))
    for idx in range(0, len(items), chunk_size):
        yield list(items[idx : idx + chunk_size])


def _short_name_map(source_engine: Engine, codes: Sequence[str]) -> dict[str, str]:
    if not codes:
        return {}
    placeholders = ", ".join(f":code_{idx}" for idx, _ in enumerate(codes))
    params = {f"code_{idx}": code for idx, code in enumerate(codes)}
    with source_engine.begin() as conn:
        rows = conn.execute(
            text(f"SELECT stock_code, short_name FROM si_all_code WHERE stock_code IN ({placeholders})"),
            params,
        ).fetchall()
    return {str(code).zfill(6): str(name or "") for code, name in rows}


def _prepare_kline_rows(
    frame: pd.DataFrame,
    *,
    source_engine: Engine,
    period: str,
    batch_id: str,
    provider: str = LEGACY_PROVIDER_ID,
) -> list[dict[str, Any]]:
    if frame is None or frame.empty:
        return []
    rows = frame.to_dict(orient="records")
    names = _short_name_map(source_engine, [str(row.get("stock_code") or "").zfill(6) for row in rows])
    received_at = now_china()
    prepared: list[dict[str, Any]] = []
    for raw in rows:
        stock_code = str(raw.get("stock_code") or "").zfill(6)
        raw_adjust_type = raw.get("adjust_type")
        try:
            adjust_type = (
                0
                if raw_adjust_type is None or pd.isna(raw_adjust_type)
                else raw_adjust_type
            )
        except (TypeError, ValueError):
            adjust_type = raw_adjust_type
        row = {
            "provider": raw.get("data_source") or provider,
            "qmt_code": raw.get("qmt_code") or to_qmt_symbol(stock_code) or "",
            "stock_code": stock_code,
            "short_name": raw.get("short_name") or names.get(stock_code, ""),
            "period": period,
            "trade_time": raw.get("trade_time"),
            "trade_date": raw.get("trade_date"),
            "k_type": raw.get("k_type") or 1,
            "adjust_type": adjust_type,
            "open": raw.get("open"),
            "close": raw.get("close"),
            "high": raw.get("high"),
            "low": raw.get("low"),
            "volume": raw.get("volume"),
            "amount": raw.get("amount"),
            "change": raw.get("change"),
            "change_pct": raw.get("change_pct"),
            "turnover_ratio": raw.get("turnover_ratio"),
            "pre_close": raw.get("pre_close"),
            "pre_close_origin": (
                raw.get("pre_close_origin") or "UNVERIFIED_LEGACY"
            ),
            "source_time": raw.get("source_time") or raw.get("trade_time"),
            "received_at": raw.get("received_at") or received_at,
            "batch_id": raw.get("batch_id") or batch_id,
            "quality_status": raw.get("quality_status") or "SOURCE_CAPTURED",
            "permission_status": raw.get("permission_status") or "SUPPORTED",
        }
        row = {key: _clean_value(value) for key, value in row.items()}
        row["data_version"] = _data_version(row)
        prepared.append(row)
    return prepared


def _prepare_minute_rows(frame: pd.DataFrame, *, source_engine: Engine, period: str, batch_id: str) -> list[dict[str, Any]]:
    if frame is None or frame.empty:
        return []
    rows = frame.to_dict(orient="records")
    names = _short_name_map(source_engine, [str(row.get("stock_code") or "").zfill(6) for row in rows])
    received_at = now_china()
    prepared: list[dict[str, Any]] = []
    for raw in rows:
        stock_code = str(raw.get("stock_code") or "").zfill(6)
        row = {
            "provider": LEGACY_PROVIDER_ID,
            "qmt_code": raw.get("qmt_code") or to_qmt_symbol(stock_code) or "",
            "stock_code": stock_code,
            "short_name": names.get(stock_code, ""),
            "period": period,
            "trade_time": raw.get("trade_time"),
            "trade_date": raw.get("trade_date"),
            "price": raw.get("price"),
            "avg_price": raw.get("avg_price"),
            "change": raw.get("change"),
            "change_pct": raw.get("change_pct"),
            "volume": raw.get("volume"),
            "amount": raw.get("amount"),
            "pre_close": raw.get("pre_close"),
            "source_time": raw.get("trade_time"),
            "received_at": received_at,
            "batch_id": batch_id,
            "quality_status": "PENDING",
            "permission_status": "SUPPORTED",
        }
        row = {key: _clean_value(value) for key, value in row.items()}
        row["data_version"] = _data_version(row)
        prepared.append(row)
    return prepared


def _upsert_rows(engine: Engine, *, table_name: str, rows: Sequence[Mapping[str, Any]], key_columns: Sequence[str]) -> int:
    if not rows:
        return 0
    table_sql = ".".join(
        f"`{part.replace('`', '``')}`" for part in table_name.split(".")
    )
    columns = list(rows[0].keys())
    col_sql = ", ".join(f"`{column}`" for column in columns)
    val_sql = ", ".join(f":{column}" for column in columns)
    update_columns = [column for column in columns if column not in set(key_columns) and column not in {"id", "created_at"}]
    update_sql = ", ".join(f"`{column}`=VALUES(`{column}`)" for column in update_columns)
    sql = text(
        f"INSERT INTO {table_sql} ({col_sql}) VALUES ({val_sql}) "
        f"ON DUPLICATE KEY UPDATE {update_sql}, updated_at=NOW()"
    )
    with engine.begin() as conn:
        conn.execute(sql, [dict(row) for row in rows])
    return len(rows)


def persist_daily_kline_capture(
    frame: pd.DataFrame,
    *,
    source_engine: Engine,
    local_engine: Engine | None = None,
    batch_id: str = "",
    provider: str = BIGQMT_PROVIDER_ID,
) -> int:
    """Persist raw BigQMT evidence before publishing canonical daily bars."""
    target_engine = local_engine or get_local_history_engine()
    table_name = LOCAL_KLINE_TABLE
    if local_engine is None and _same_server_and_user(
        str(target_engine.url),
        str(source_engine.url),
    ):
        # Connecting with the history schema as the default database can be
        # denied even though the existing production session has explicit
        # cross-schema grants.  Reuse that authenticated session and qualify
        # only the destination table.
        history_database = make_url(str(target_engine.url)).database
        if not history_database:
            raise RuntimeError("QMT history database name is required")
        target_engine = source_engine
        table_name = f"{history_database}.{LOCAL_KLINE_TABLE}"
        validate_local_history_tables(
            target_engine,
            database=history_database,
        )
    else:
        validate_local_history_tables(target_engine)
    rows = _prepare_kline_rows(
        frame,
        source_engine=source_engine,
        period="1d",
        batch_id=(
            batch_id
            or f"qmt_capture_{now_china().strftime('%Y%m%d_%H%M%S')}"
        ),
        provider=provider,
    )
    return _upsert_rows(
        target_engine,
        table_name=table_name,
        rows=rows,
        key_columns=[
            "provider",
            "stock_code",
            "period",
            "trade_date",
            "adjust_type",
        ],
    )


def _record_run_start(
    engine: Engine,
    *,
    run_id: str,
    dataset: str,
    period: str,
    start_date: str,
    end_date: str,
    requested_codes: int,
    provider: str,
    extra: Mapping[str, Any],
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                INSERT INTO {LOCAL_RUN_TABLE} (
                    run_id, provider, dataset, period, start_date, end_date, status,
                    requested_codes, started_at, extra_json
                ) VALUES (
                    :run_id, :provider, :dataset, :period, :start_date, :end_date, 'RUNNING',
                    :requested_codes, :started_at, :extra_json
                )
                """
            ),
            {
                "run_id": run_id,
                "provider": provider,
                "dataset": dataset,
                "period": period,
                "start_date": _normalize_date(start_date),
                "end_date": _normalize_date(end_date),
                "requested_codes": requested_codes,
                "started_at": now_china(),
                "extra_json": _canonical(extra),
            },
        )


def _record_run_finish(
    engine: Engine,
    *,
    run_id: str,
    status: str,
    fetched_rows: int,
    written_rows: int,
    error_message: str | None = None,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                UPDATE {LOCAL_RUN_TABLE}
                SET status=:status, fetched_rows=:fetched_rows, written_rows=:written_rows,
                    error_message=:error_message, finished_at=:finished_at
                WHERE run_id=:run_id
                """
            ),
            {
                "run_id": run_id,
                "status": status,
                "fetched_rows": fetched_rows,
                "written_rows": written_rows,
                "error_message": error_message,
                "finished_at": now_china(),
            },
        )


def backfill_daily_kline_local(
    *,
    source_engine: Engine,
    local_engine: Engine,
    stock_codes: Sequence[str],
    start_date: str,
    end_date: str,
    batch_size: int = 80,
    dividend_type: str = "none",
    provider: str = BIGQMT_PROVIDER_ID,
    dry_run: bool = False,
    allowed_missing_stock_codes: Sequence[str] = (),
    source_batch_id: str = "",
) -> LocalBackfillResult:
    if provider not in {BIGQMT_PROVIDER_ID, LEGACY_PROVIDER_ID}:
        raise ValueError("daily QMT history provider is not supported")
    requested_stock_codes = list(
        dict.fromkeys(
            str(code or "").strip().split(".", 1)[0].zfill(6)
            for code in stock_codes
            if str(code or "").strip()
        )
    )
    if not requested_stock_codes:
        raise ValueError("daily QMT history requires at least one stock code")
    allowed_missing_codes = {
        str(code or "").strip().split(".", 1)[0].zfill(6)
        for code in allowed_missing_stock_codes
        if str(code or "").strip()
    }
    unknown_allowed_codes = sorted(allowed_missing_codes - set(requested_stock_codes))
    if unknown_allowed_codes:
        raise ValueError(
            "allowed missing stock codes must be included in the requested universe: "
            f"count={len(unknown_allowed_codes)}, sample={unknown_allowed_codes[:10]}"
        )
    validate_local_history_tables(local_engine)
    run_id = f"qmt_hist_kline_{now_china().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    normalized_source_batch_id = str(source_batch_id or "").strip()
    if normalized_source_batch_id and (
        len(normalized_source_batch_id) != 64
        or any(ch not in "0123456789abcdef" for ch in normalized_source_batch_id)
    ):
        raise ValueError("daily QMT source batch root must be lower SHA-256")
    batches: list[LocalBackfillBatchResult] = []
    fetched_total = 0
    written_total = 0
    _record_run_start(
        local_engine,
        run_id=run_id,
        dataset=LOCAL_KLINE_TABLE,
        period="1d",
        start_date=start_date,
        end_date=end_date,
        requested_codes=len(requested_stock_codes),
        provider=provider,
        extra={
            "dry_run": dry_run,
            "dividend_type": dividend_type,
            "batch_size": batch_size,
            "provider": provider,
            "allowed_missing_stock_codes": sorted(allowed_missing_codes),
            "allowed_missing_stock_code_count": len(allowed_missing_codes),
            "source_batch_id": normalized_source_batch_id,
        },
    )
    status = "SUCCESS"
    error_message: str | None = None
    try:
        for batch in _chunked(requested_stock_codes, batch_size):
            qmt_codes = [to_qmt_symbol(code) for code in batch]
            qmt_codes = [code for code in qmt_codes if code]
            if len(qmt_codes) != len(batch):
                unsupported = sorted(
                    code for code in batch if not to_qmt_symbol(code)
                )
                raise RuntimeError(
                    "QMT daily batch contains unsupported stock codes: "
                    f"count={len(unsupported)}, sample={unsupported[:10]}"
                )
            if provider == BIGQMT_PROVIDER_ID:
                from integrations.bigqmt.backend import BigQmtBackend

                frame = BigQmtBackend().fetch_kline(
                    list(batch),
                    start_date,
                    end_date,
                    dividend_type=dividend_type,
                    download_history=True,
                )
            else:
                frame = bridge.kline(
                    qmt_codes,
                    start_date=start_date,
                    end_date=end_date,
                    dividend_type=dividend_type,
                    batch_size=batch_size,
                    timeout=900,
                )
            rows = _prepare_kline_rows(
                frame,
                source_engine=source_engine,
                period="1d",
                batch_id=normalized_source_batch_id or run_id,
                provider=provider,
            )
            fetched_codes = {
                str(row.get("stock_code") or "").strip().zfill(6)
                for row in rows
                if str(row.get("stock_code") or "").strip()
            }
            expected_codes = set(batch)
            missing_codes = sorted(expected_codes - fetched_codes)
            allowed_batch_missing_codes = sorted(
                set(missing_codes) & allowed_missing_codes
            )
            fatal_missing_codes = sorted(
                set(missing_codes) - allowed_missing_codes
            )
            unexpected_codes = sorted(fetched_codes - expected_codes)
            if fatal_missing_codes or unexpected_codes:
                raise RuntimeError(
                    "QMT daily batch coverage is incomplete: "
                    f"requested_codes={len(expected_codes)}, "
                    f"fetched_rows={len(rows)}, "
                    f"fetched_codes={len(fetched_codes)}, "
                    f"missing_count={len(missing_codes)}, "
                    f"missing_sample={missing_codes[:10]}, "
                    f"allowed_missing_count={len(allowed_batch_missing_codes)}, "
                    f"fatal_missing_count={len(fatal_missing_codes)}, "
                    f"fatal_missing_sample={fatal_missing_codes[:10]}, "
                    f"unexpected_count={len(unexpected_codes)}, "
                    f"unexpected_sample={unexpected_codes[:10]}"
                )
            fetched_total += len(rows)
            written = 0 if dry_run or not rows else _upsert_rows(
                local_engine,
                table_name=LOCAL_KLINE_TABLE,
                rows=rows,
                key_columns=["provider", "stock_code", "period", "trade_date", "adjust_type"],
            )
            written_total += written
            batches.append(
                LocalBackfillBatchResult(
                    dataset=LOCAL_KLINE_TABLE,
                    period="1d",
                    start_date=_normalize_date(start_date),
                    end_date=_normalize_date(end_date),
                    requested_codes=len(qmt_codes),
                    fetched_rows=len(rows),
                    written_rows=written,
                    skipped=dry_run,
                    allowed_missing_codes=tuple(allowed_batch_missing_codes),
                )
            )
    except Exception as exc:
        status = "FAILED"
        error_message = str(exc)
        raise
    finally:
        _record_run_finish(
            local_engine,
            run_id=run_id,
            status=status,
            fetched_rows=fetched_total,
            written_rows=written_total,
            error_message=error_message,
        )
    return LocalBackfillResult(
        run_id=run_id,
        dataset=LOCAL_KLINE_TABLE,
        status=status,
        local_database=str(make_url(str(local_engine.url)).database or ""),
        start_date=_normalize_date(start_date),
        end_date=_normalize_date(end_date),
        code_count=len(requested_stock_codes),
        batch_count=len(batches),
        fetched_rows=fetched_total,
        written_rows=written_total,
        batches=batches,
    )


def backfill_minute_local(
    *,
    source_engine: Engine,
    local_engine: Engine,
    stock_codes: Sequence[str],
    trade_dates: Sequence[str],
    batch_size: int = 50,
    dry_run: bool = False,
) -> LocalBackfillResult:
    validate_local_history_tables(local_engine)
    start_date = trade_dates[0] if trade_dates else ""
    end_date = trade_dates[-1] if trade_dates else ""
    run_id = f"qmt_hist_minute_{now_china().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    batches: list[LocalBackfillBatchResult] = []
    fetched_total = 0
    written_total = 0
    _record_run_start(
        local_engine,
        run_id=run_id,
        dataset=LOCAL_MINUTE_TABLE,
        period="1m",
        start_date=start_date,
        end_date=end_date,
        requested_codes=len(stock_codes),
        provider=LEGACY_PROVIDER_ID,
        extra={"dry_run": dry_run, "batch_size": batch_size, "trade_dates": list(trade_dates)},
    )
    status = "SUCCESS"
    error_message: str | None = None
    try:
        for trade_date in trade_dates:
            for batch in _chunked(list(stock_codes), batch_size):
                qmt_codes = [to_qmt_symbol(code) for code in batch]
                qmt_codes = [code for code in qmt_codes if code]
                frame = bridge.minute(
                    qmt_codes,
                    trade_date=trade_date,
                    start_date=trade_date,
                    end_date=trade_date,
                    fill_data=False,
                    batch_size=batch_size,
                    timeout=900,
                )
                rows = _prepare_minute_rows(frame, source_engine=source_engine, period="1m", batch_id=run_id)
                if not rows:
                    raise RuntimeError(
                        "QMT minute history returned no native rows: "
                        f"trade_date={trade_date}, requested_codes={len(qmt_codes)}"
                    )
                fetched_total += len(rows)
                written = 0 if dry_run else _upsert_rows(
                    local_engine,
                    table_name=LOCAL_MINUTE_TABLE,
                    rows=rows,
                    key_columns=["provider", "stock_code", "period", "trade_time"],
                )
                written_total += written
                batches.append(
                    LocalBackfillBatchResult(
                        dataset=LOCAL_MINUTE_TABLE,
                        period="1m",
                        start_date=trade_date,
                        end_date=trade_date,
                        requested_codes=len(qmt_codes),
                        fetched_rows=len(rows),
                        written_rows=written,
                        skipped=dry_run,
                    )
                )
    except Exception as exc:
        status = "FAILED"
        error_message = str(exc)
        raise
    finally:
        _record_run_finish(
            local_engine,
            run_id=run_id,
            status=status,
            fetched_rows=fetched_total,
            written_rows=written_total,
            error_message=error_message,
        )
    return LocalBackfillResult(
        run_id=run_id,
        dataset=LOCAL_MINUTE_TABLE,
        status=status,
        local_database=str(make_url(str(local_engine.url)).database or ""),
        start_date=_normalize_date(start_date),
        end_date=_normalize_date(end_date),
        code_count=len(stock_codes),
        batch_count=len(batches),
        fetched_rows=fetched_total,
        written_rows=written_total,
        batches=batches,
    )


def result_dict(result: LocalBackfillResult) -> dict[str, Any]:
    payload = asdict(result)
    payload["batches"] = [asdict(batch) for batch in result.batches]
    return payload
