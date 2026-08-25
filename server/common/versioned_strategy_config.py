# -*- coding: utf-8 -*-
"""Load and register immutable strategy and market-state configuration files."""
from __future__ import annotations

import hashlib
import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine


PROJECT_ROOT = Path(__file__).resolve().parents[2]
STOCK_MANIFEST_PATH = PROJECT_ROOT / "strategies" / "stock_strategy_v2.json"
MARKET_STATE_CONFIG_PATH = PROJECT_ROOT / "strategies" / "market_state_v2.json"


def _canonical_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def config_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"versioned config must be a JSON object: {path}")
    return payload


def _validate_stock_manifest(payload: dict[str, Any]) -> None:
    if payload.get("schema_version") != "probiga.stock-strategy-manifest.v2":
        raise ValueError("unsupported stock strategy manifest schema")
    if not str(payload.get("manifest_version") or "").strip():
        raise ValueError("stock strategy manifest_version is required")
    strategies = payload.get("strategies")
    if not isinstance(strategies, list) or not strategies:
        raise ValueError("stock strategy manifest requires strategies")
    keys: list[str] = []
    for item in strategies:
        if not isinstance(item, dict):
            raise ValueError("strategy entry must be an object")
        key = str(item.get("key") or "").strip()
        if not key:
            raise ValueError("strategy key is required")
        if key in keys:
            raise ValueError(f"duplicate strategy key: {key}")
        keys.append(key)
        params = item.get("parameters")
        if not isinstance(params, dict):
            raise ValueError(f"strategy parameters missing: {key}")
        required = {
            "min_score",
            "confirm_score",
            "base_position_pct",
            "max_holding_days",
            "stop_loss_pct",
            "take_profit_1_pct",
            "take_profit_2_pct",
            "cooldown_days",
        }
        missing = sorted(required - set(params))
        if missing:
            raise ValueError(f"strategy {key} missing parameters: {missing}")
        if float(params["confirm_score"]) < float(params["min_score"]):
            raise ValueError(f"strategy {key} confirm_score must be >= min_score")
        if float(params["stop_loss_pct"]) >= 0:
            raise ValueError(f"strategy {key} stop_loss_pct must be negative")


def _validate_market_config(payload: dict[str, Any]) -> None:
    if payload.get("schema_version") != "probiga.market-state-config.v2":
        raise ValueError("unsupported market-state config schema")
    if not str(payload.get("config_version") or "").strip():
        raise ValueError("market-state config_version is required")
    thresholds = payload.get("thresholds")
    transition = payload.get("transition")
    multipliers = payload.get("strategy_multipliers")
    if not isinstance(thresholds, dict) or not isinstance(transition, dict):
        raise ValueError("market-state thresholds and transition are required")
    for state in payload.get("states") or []:
        if state not in thresholds:
            raise ValueError(f"missing threshold block for market state: {state}")
        if state not in multipliers:
            raise ValueError(f"missing strategy multipliers for market state: {state}")
        if int((transition.get("confirm_days") or {}).get(state, 0)) < 1:
            raise ValueError(f"confirm_days must be positive for {state}")
        if int((transition.get("minimum_state_days") or {}).get(state, 0)) < 1:
            raise ValueError(f"minimum_state_days must be positive for {state}")


@lru_cache(maxsize=1)
def load_stock_manifest() -> dict[str, Any]:
    payload = _read_json(STOCK_MANIFEST_PATH)
    _validate_stock_manifest(payload)
    return payload


@lru_cache(maxsize=1)
def load_market_state_config() -> dict[str, Any]:
    payload = _read_json(MARKET_STATE_CONFIG_PATH)
    _validate_market_config(payload)
    return payload


def stock_manifest_hash() -> str:
    return config_hash(load_stock_manifest())


def market_state_config_hash() -> str:
    return config_hash(load_market_state_config())


def stock_strategy_profiles() -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for strategy in load_stock_manifest()["strategies"]:
        params = strategy["parameters"]
        result[strategy["key"]] = {
            "label": strategy["name"],
            "min_score": float(params["min_score"]),
            "confirm_score": float(params["confirm_score"]),
            "max_holding_days": int(params["max_holding_days"]),
            "extension_days_when_trend_valid": int(
                params.get("extension_days_when_trend_valid", 0)
            ),
            "base_position": float(params["base_position_pct"]),
            "stop_loss_pct": float(params["stop_loss_pct"]),
            "take_profit_1_pct": float(params["take_profit_1_pct"]),
            "take_profit_2_pct": float(params["take_profit_2_pct"]),
            "cooldown_days": int(params["cooldown_days"]),
        }
    return result


def stock_strategy_catalog() -> tuple[dict[str, Any], ...]:
    return tuple(
        {
            "key": str(item["key"]),
            "name": str(item["name"]),
            "category": str(item["category"]),
            "description": str(
                (item.get("score_formula") or {}).get("expression")
                or (item.get("score_formula") or {}).get("reference")
                or ""
            ),
            "base_weight": float(item.get("base_weight", 1.0)),
            "manifest_version": str(load_stock_manifest()["manifest_version"]),
            "config_hash": stock_manifest_hash(),
        }
        for item in load_stock_manifest()["strategies"]
    )


def strategy_score_field_map() -> dict[str, str]:
    return {
        str(item["key"]): str(item["score_field"])
        for item in load_stock_manifest()["strategies"]
    }


def legacy_strategy_merge_map() -> dict[str, str | None]:
    return {
        str(key): (str(value) if value is not None else None)
        for key, value in (load_stock_manifest().get("legacy_merge_map") or {}).items()
    }


_VERSIONED_STRATEGY_TABLE_DDL = (
        """
        CREATE TABLE IF NOT EXISTS st_strategy_manifest_registry (
            manifest_version VARCHAR(80) PRIMARY KEY,
            config_hash CHAR(64) NOT NULL,
            schema_version VARCHAR(80) NOT NULL,
            model_version VARCHAR(80) NOT NULL,
            manifest_json LONGTEXT NOT NULL,
            status VARCHAR(32) NOT NULL,
            frozen_at DATETIME NOT NULL,
            active TINYINT(1) NOT NULL DEFAULT 0,
            registered_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uk_strategy_manifest_hash (config_hash)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS st_market_state_config (
            config_version VARCHAR(80) PRIMARY KEY,
            config_hash CHAR(64) NOT NULL,
            schema_version VARCHAR(80) NOT NULL,
            config_json LONGTEXT NOT NULL,
            status VARCHAR(32) NOT NULL,
            frozen_at DATETIME NOT NULL,
            active TINYINT(1) NOT NULL DEFAULT 0,
            registered_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uk_market_state_config_hash (config_hash)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS st_market_state_daily (
            id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            trade_date DATE NOT NULL,
            run_uid VARCHAR(40) NOT NULL,
            config_version VARCHAR(80) NOT NULL,
            config_hash CHAR(64) NOT NULL,
            input_hash CHAR(64) NOT NULL,
            candidate_state VARCHAR(40) NOT NULL,
            final_state VARCHAR(40) NOT NULL,
            candidate_streak INT NOT NULL DEFAULT 1,
            state_days INT NOT NULL DEFAULT 1,
            cooldown_remaining INT NOT NULL DEFAULT 0,
            source_status VARCHAR(32) NOT NULL,
            input_json LONGTEXT NOT NULL,
            evidence_json LONGTEXT NOT NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uk_market_state_observation
                (trade_date, config_version, input_hash),
            KEY idx_market_state_daily_latest
                (config_version, trade_date, created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
)

_VERSIONED_STRATEGY_TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "st_strategy_manifest_registry": (
        "manifest_version", "config_hash", "schema_version", "model_version",
        "manifest_json", "status", "frozen_at", "active", "registered_at",
    ),
    "st_market_state_config": (
        "config_version", "config_hash", "schema_version", "config_json",
        "status", "frozen_at", "active", "registered_at",
    ),
    "st_market_state_daily": (
        "id", "trade_date", "run_uid", "config_version", "config_hash",
        "input_hash", "candidate_state", "final_state", "candidate_streak",
        "state_days", "cooldown_remaining", "source_status", "input_json",
        "evidence_json", "created_at",
    ),
}

# (data_type, nullable, character_length, numeric_precision, numeric_scale,
#  auto_increment).  Integer display widths are intentionally ignored because
# MySQL 8 removed them; decimal precision and every string bound remain exact.
_VERSIONED_STRATEGY_COLUMN_CONTRACTS: dict[
    str, dict[str, tuple[str, bool, int | None, int | None, int | None, bool]]
] = {
    "st_strategy_manifest_registry": {
        "manifest_version": ("varchar", False, 80, None, None, False),
        "config_hash": ("char", False, 64, None, None, False),
        "schema_version": ("varchar", False, 80, None, None, False),
        "model_version": ("varchar", False, 80, None, None, False),
        "manifest_json": ("longtext", False, None, None, None, False),
        "status": ("varchar", False, 32, None, None, False),
        "frozen_at": ("datetime", False, None, None, None, False),
        "active": ("tinyint", False, None, None, None, False),
        "registered_at": ("datetime", False, None, None, None, False),
    },
    "st_market_state_config": {
        "config_version": ("varchar", False, 80, None, None, False),
        "config_hash": ("char", False, 64, None, None, False),
        "schema_version": ("varchar", False, 80, None, None, False),
        "config_json": ("longtext", False, None, None, None, False),
        "status": ("varchar", False, 32, None, None, False),
        "frozen_at": ("datetime", False, None, None, None, False),
        "active": ("tinyint", False, None, None, None, False),
        "registered_at": ("datetime", False, None, None, None, False),
    },
    "st_market_state_daily": {
        "id": ("bigint", False, None, None, None, True),
        "trade_date": ("date", False, None, None, None, False),
        "run_uid": ("varchar", False, 40, None, None, False),
        "config_version": ("varchar", False, 80, None, None, False),
        "config_hash": ("char", False, 64, None, None, False),
        "input_hash": ("char", False, 64, None, None, False),
        "candidate_state": ("varchar", False, 40, None, None, False),
        "final_state": ("varchar", False, 40, None, None, False),
        "candidate_streak": ("int", False, None, None, None, False),
        "state_days": ("int", False, None, None, None, False),
        "cooldown_remaining": ("int", False, None, None, None, False),
        "source_status": ("varchar", False, 32, None, None, False),
        "input_json": ("longtext", False, None, None, None, False),
        "evidence_json": ("longtext", False, None, None, None, False),
        "created_at": ("datetime", False, None, None, None, False),
    },
}

_VERSIONED_STRATEGY_REQUIRED_INDEXES: dict[
    str, tuple[tuple[bool, tuple[str, ...]], ...]
] = {
    "st_strategy_manifest_registry": (
        (True, ("manifest_version",)),
        (True, ("config_hash",)),
    ),
    "st_market_state_config": (
        (True, ("config_version",)),
        (True, ("config_hash",)),
    ),
    "st_market_state_daily": (
        (True, ("id",)),
        (True, ("trade_date", "config_version", "input_hash")),
        (False, ("config_version", "trade_date", "created_at")),
    ),
}


def _schema_rows(connection, *, kind: str) -> list[dict[str, Any]]:
    tables = tuple(_VERSIONED_STRATEGY_TABLE_COLUMNS)
    placeholders = ", ".join(
        f":table_{index}" for index in range(len(tables))
    )
    if kind == "columns":
        sql = (
            "SELECT table_name AS table_name, column_name AS column_name, "
            "data_type AS data_type, is_nullable AS is_nullable, "
            "character_maximum_length AS character_maximum_length, "
            "numeric_precision AS numeric_precision, "
            "numeric_scale AS numeric_scale, extra AS extra "
            "FROM information_schema.columns "
            "WHERE table_schema=DATABASE() AND table_name IN "
            f"({placeholders})"
        )
    elif kind == "indexes":
        sql = (
            "SELECT table_name AS table_name, index_name AS index_name, "
            "non_unique AS non_unique, seq_in_index AS seq_in_index, "
            "column_name AS column_name FROM information_schema.statistics "
            "WHERE table_schema=DATABASE() AND table_name IN "
            f"({placeholders}) ORDER BY table_name, index_name, seq_in_index"
        )
    else:  # pragma: no cover - internal programming error
        raise ValueError(f"unsupported schema row kind: {kind}")
    return [
        dict(row) for row in connection.execute(
            text(sql),
            {f"table_{index}": table for index, table in enumerate(tables)},
        ).mappings().all()
    ]


def _validate_versioned_strategy_schema(connection) -> None:
    columns_by_table: dict[str, dict[str, dict[str, Any]]] = {
        table: {} for table in _VERSIONED_STRATEGY_TABLE_COLUMNS
    }
    for row in _schema_rows(connection, kind="columns"):
        table = str(row.get("table_name") or "")
        if table in columns_by_table:
            columns_by_table[table][str(row.get("column_name") or "")] = row
    for table, required in _VERSIONED_STRATEGY_TABLE_COLUMNS.items():
        missing = sorted(set(required) - set(columns_by_table[table]))
        if missing:
            raise RuntimeError(
                f"versioned strategy runtime schema is not prepared: "
                f"{table} missing columns {missing}"
            )
    for table, contracts in _VERSIONED_STRATEGY_COLUMN_CONTRACTS.items():
        for column, expected in contracts.items():
            row = columns_by_table[table][column]
            actual = (
                str(row.get("data_type") or "").lower(),
                str(row.get("is_nullable") or "").upper() == "YES",
                (
                    int(row["character_maximum_length"])
                    if row.get("character_maximum_length") is not None else None
                ),
                (
                    int(row["numeric_precision"])
                    if row.get("numeric_precision") is not None else None
                ),
                (
                    int(row["numeric_scale"])
                    if row.get("numeric_scale") is not None else None
                ),
                "auto_increment" in str(row.get("extra") or "").lower(),
            )
            comparable_actual = (
                actual[0], actual[1],
                actual[2] if expected[2] is not None else None,
                actual[3] if expected[3] is not None else None,
                actual[4] if expected[4] is not None else None,
                actual[5],
            )
            if comparable_actual != expected:
                raise RuntimeError(
                    f"versioned strategy runtime schema type drift: "
                    f"{table}.{column} expected={expected} "
                    f"actual={comparable_actual}"
                )

    index_parts: dict[str, dict[str, list[tuple[int, str]]]] = {
        table: {} for table in _VERSIONED_STRATEGY_TABLE_COLUMNS
    }
    index_unique: dict[str, dict[str, bool]] = {
        table: {} for table in _VERSIONED_STRATEGY_TABLE_COLUMNS
    }
    for row in _schema_rows(connection, kind="indexes"):
        table = str(row.get("table_name") or "")
        if table not in index_parts:
            continue
        name = str(row.get("index_name") or "")
        index_parts[table].setdefault(name, []).append((
            int(row.get("seq_in_index") or 0),
            str(row.get("column_name") or ""),
        ))
        index_unique[table][name] = int(row.get("non_unique") or 0) == 0
    for table, required_indexes in _VERSIONED_STRATEGY_REQUIRED_INDEXES.items():
        actual = {
            (
                bool(index_unique[table].get(name)),
                tuple(column for _, column in sorted(parts)),
            )
            for name, parts in index_parts[table].items()
        }
        missing = [spec for spec in required_indexes if spec not in actual]
        if missing:
            raise RuntimeError(
                f"versioned strategy runtime schema is not prepared: "
                f"{table} missing indexes {missing}"
            )


def privileged_migrate_versioned_strategy_tables(engine: Engine) -> None:
    """Create the three versioned-config tables during a privileged deploy step."""

    with engine.begin() as connection:
        for statement in _VERSIONED_STRATEGY_TABLE_DDL:
            connection.execute(text(statement))


def _register_immutable(
    engine: Engine,
    *,
    table: str,
    version_column: str,
    version: str,
    digest: str,
    schema_version: str,
    payload_column: str,
    payload: dict[str, Any],
    status: str,
    frozen_at: str,
    model_version: str = "",
) -> None:
    with engine.begin() as connection:
        existing = connection.execute(
            text(
                f"SELECT config_hash FROM `{table}` "
                f"WHERE `{version_column}` = :version"
            ),
            {"version": version},
        ).scalar()
        if existing and str(existing) != digest:
            raise RuntimeError(
                f"immutable config version collision: {table}.{version} "
                f"existing={existing} requested={digest}"
            )
        if not existing:
            columns = [
                version_column,
                "config_hash",
                "schema_version",
                payload_column,
                "status",
                "frozen_at",
                "active",
            ]
            values = [
                ":version",
                ":config_hash",
                ":schema_version",
                ":payload_json",
                ":status",
                ":frozen_at",
                "1",
            ]
            params: dict[str, Any] = {
                "version": version,
                "config_hash": digest,
                "schema_version": schema_version,
                "payload_json": json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                "status": status,
                "frozen_at": str(frozen_at).replace("T", " ")[:19],
            }
            if table == "st_strategy_manifest_registry":
                columns.insert(3, "model_version")
                values.insert(3, ":model_version")
                params["model_version"] = model_version
            connection.execute(
                text(
                    f"INSERT INTO `{table}` "
                    f"({', '.join(f'`{item}`' for item in columns)}) "
                    f"VALUES ({', '.join(values)})"
                ),
                params,
            )
        connection.execute(
            text(
                f"UPDATE `{table}` SET active = "
                f"CASE WHEN `{version_column}` = :version THEN 1 ELSE 0 END"
            ),
            {"version": version},
        )


def privileged_seed_versioned_strategy_configs(engine: Engine) -> dict[str, Any]:
    """Seed immutable current configs after the privileged schema migration."""

    with engine.connect() as connection:
        _validate_versioned_strategy_schema(connection)
    stock = load_stock_manifest()
    market = load_market_state_config()
    stock_digest = stock_manifest_hash()
    market_digest = market_state_config_hash()
    _register_immutable(
        engine,
        table="st_strategy_manifest_registry",
        version_column="manifest_version",
        version=str(stock["manifest_version"]),
        digest=stock_digest,
        schema_version=str(stock["schema_version"]),
        payload_column="manifest_json",
        payload=stock,
        status=str(stock["status"]),
        frozen_at=str(stock["frozen_at"]),
        model_version=str(stock["model_version"]),
    )
    _register_immutable(
        engine,
        table="st_market_state_config",
        version_column="config_version",
        version=str(market["config_version"]),
        digest=market_digest,
        schema_version=str(market["schema_version"]),
        payload_column="config_json",
        payload=market,
        status=str(market["status"]),
        frozen_at=str(market["frozen_at"]),
    )
    result = {
        "stock_manifest_version": stock["manifest_version"],
        "stock_manifest_hash": stock_digest,
        "market_state_config_version": market["config_version"],
        "market_state_config_hash": market_digest,
    }
    # Seed completion is not accepted until a separate read-only identity check
    # can prove that the database now matches the files shipped in this build.
    validate_versioned_strategy_runtime(engine)
    return result


def _active_config_rows(
    connection,
    *,
    table: str,
    version_column: str,
    version: str,
    selected_columns: tuple[str, ...],
) -> list[dict[str, Any]]:
    columns = ", ".join(f"`{column}`" for column in selected_columns)
    return [
        dict(row) for row in connection.execute(
            text(
                f"SELECT {columns} FROM `{table}` "
                f"WHERE `{version_column}`=:version OR active=1"
            ),
            {"version": version},
        ).mappings().all()
    ]


def _validate_current_config_identity(
    rows: list[dict[str, Any]],
    *,
    table: str,
    version_column: str,
    version: str,
    digest: str,
    schema_version: str,
    payload_column: str,
    payload: dict[str, Any],
    status: str,
    model_version: str = "",
) -> None:
    current = [
        row for row in rows if str(row.get(version_column) or "") == version
    ]
    active = [row for row in rows if int(row.get("active") or 0) == 1]
    if len(current) != 1 or len(active) != 1 or active[0] is not current[0]:
        raise RuntimeError(
            f"versioned strategy config identity drift: {table}.{version} "
            "must be the single active row"
        )
    row = current[0]
    try:
        stored_payload = json.loads(str(row.get(payload_column) or ""))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"versioned strategy config identity drift: invalid {table}.{payload_column}"
        ) from exc
    identity_matches = (
        str(row.get("config_hash") or "") == digest
        and str(row.get("schema_version") or "") == schema_version
        and str(row.get("status") or "") == status
        and stored_payload == payload
        and config_hash(stored_payload) == digest
    )
    if table == "st_strategy_manifest_registry":
        identity_matches = identity_matches and (
            str(row.get("model_version") or "") == model_version
        )
    if not identity_matches:
        raise RuntimeError(
            f"versioned strategy config identity drift: {table}.{version}"
        )


def validate_versioned_strategy_runtime(
    engine: Engine, *, connection=None,
) -> dict[str, Any]:
    """Read-only production guard for schema and active immutable config identity."""

    def validate(bound_connection) -> dict[str, Any]:
        _validate_versioned_strategy_schema(bound_connection)
        stock = load_stock_manifest()
        market = load_market_state_config()
        stock_digest = stock_manifest_hash()
        market_digest = market_state_config_hash()
        stock_rows = _active_config_rows(
            bound_connection,
            table="st_strategy_manifest_registry",
            version_column="manifest_version",
            version=str(stock["manifest_version"]),
            selected_columns=(
                "manifest_version", "config_hash", "schema_version",
                "model_version", "manifest_json", "status", "active",
            ),
        )
        _validate_current_config_identity(
            stock_rows,
            table="st_strategy_manifest_registry",
            version_column="manifest_version",
            version=str(stock["manifest_version"]),
            digest=stock_digest,
            schema_version=str(stock["schema_version"]),
            payload_column="manifest_json",
            payload=stock,
            status=str(stock["status"]),
            model_version=str(stock["model_version"]),
        )
        market_rows = _active_config_rows(
            bound_connection,
            table="st_market_state_config",
            version_column="config_version",
            version=str(market["config_version"]),
            selected_columns=(
                "config_version", "config_hash", "schema_version",
                "config_json", "status", "active",
            ),
        )
        _validate_current_config_identity(
            market_rows,
            table="st_market_state_config",
            version_column="config_version",
            version=str(market["config_version"]),
            digest=market_digest,
            schema_version=str(market["schema_version"]),
            payload_column="config_json",
            payload=market,
            status=str(market["status"]),
        )
        return {
            "stock_manifest_version": stock["manifest_version"],
            "stock_manifest_hash": stock_digest,
            "market_state_config_version": market["config_version"],
            "market_state_config_hash": market_digest,
        }

    if connection is not None:
        return validate(connection)
    with engine.connect() as bound_connection:
        return validate(bound_connection)


def ensure_versioned_strategy_tables(engine: Engine) -> None:
    """Compatibility guard: validate only; never mutate runtime schema."""

    validate_versioned_strategy_runtime(engine)


def register_versioned_strategy_configs(engine: Engine) -> dict[str, Any]:
    """Compatibility guard: validate only; seeding is privileged and explicit."""

    return validate_versioned_strategy_runtime(engine)
