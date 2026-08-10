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


def ensure_versioned_strategy_tables(engine: Engine) -> None:
    ddl = (
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
    with engine.begin() as connection:
        for statement in ddl:
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


def register_versioned_strategy_configs(engine: Engine) -> dict[str, Any]:
    ensure_versioned_strategy_tables(engine)
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
    return {
        "stock_manifest_version": stock["manifest_version"],
        "stock_manifest_hash": stock_digest,
        "market_state_config_version": market["config_version"],
        "market_state_config_hash": market_digest,
    }
