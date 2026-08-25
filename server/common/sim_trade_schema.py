"""Privileged schema migration and read-only runtime contract for sim trading.

The simulation engine is a normal application writer, not a schema owner.
Persistent DDL is therefore exposed only through
``privileged_migrate_sim_trade_schema`` for the fenced release window.  Every
runtime/API entry point calls ``validate_sim_trade_runtime_schema`` and fails
closed before reading or writing if the installed physical contract drifts.
"""
from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from sqlalchemy import text


EXPECTED_ENGINE = "InnoDB"
EXPECTED_COLLATION = "utf8mb4_unicode_ci"


TABLE_DDL: Mapping[str, str] = {
    "st_sim_position": """
        CREATE TABLE IF NOT EXISTS `st_sim_position` (
            `id` BIGINT NOT NULL AUTO_INCREMENT,
            `signal_id` BIGINT DEFAULT NULL,
            `entry_order_id` BIGINT DEFAULT NULL,
            `exit_order_id` BIGINT DEFAULT NULL,
            `stock_code` VARCHAR(10) NOT NULL,
            `short_name` VARCHAR(20) DEFAULT '',
            `strategy_type` VARCHAR(20) NOT NULL,
            `trade_mode` VARCHAR(20) DEFAULT 'live',
            `buy_price` DECIMAL(12,4) NOT NULL,
            `buy_amount` DECIMAL(14,2) NOT NULL,
            `buy_shares` INT NOT NULL,
            `buy_date` DATE NOT NULL,
            `buy_time` VARCHAR(20) DEFAULT '',
            `buy_reason` TEXT,
            `ai_score` DECIMAL(5,2) DEFAULT 0,
            `short_score` DECIMAL(5,2) DEFAULT 0,
            `long_score` DECIMAL(5,2) DEFAULT 0,
            `capital_score` DECIMAL(5,2) DEFAULT 0,
            `technical_score` DECIMAL(5,2) DEFAULT 0,
            `fundamental_score` DECIMAL(5,2) DEFAULT 0,
            `event_risk_level` VARCHAR(10) DEFAULT 'LOW',
            `status` VARCHAR(20) DEFAULT 'holding',
            `sell_price` DECIMAL(12,4) DEFAULT NULL,
            `sell_date` DATE DEFAULT NULL,
            `sell_time` VARCHAR(20) DEFAULT '',
            `sell_reason` TEXT,
            `profit` DECIMAL(14,2) DEFAULT 0,
            `profit_rate` DECIMAL(8,4) DEFAULT 0,
            `holding_days` INT DEFAULT 0,
            `fee_total` DECIMAL(10,2) DEFAULT 0,
            `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            `updated_at` DATETIME DEFAULT NULL,
            PRIMARY KEY (`id`),
            KEY `idx_strategy_status` (`strategy_type`, `status`),
            KEY `idx_trade_mode` (`trade_mode`, `strategy_type`, `status`),
            KEY `idx_stock_code` (`stock_code`),
            KEY `idx_buy_date` (`buy_date`),
            KEY `idx_status` (`status`),
            KEY `idx_sim_position_signal` (`signal_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    "st_trade_flow": """
        CREATE TABLE IF NOT EXISTS `st_trade_flow` (
            `id` BIGINT NOT NULL AUTO_INCREMENT,
            `order_id` BIGINT DEFAULT NULL,
            `stock_code` VARCHAR(10) NOT NULL,
            `short_name` VARCHAR(20) DEFAULT '',
            `flow_type` VARCHAR(20) NOT NULL,
            `source` VARCHAR(20) NOT NULL,
            `strategy_type` VARCHAR(20) DEFAULT '',
            `trade_mode` VARCHAR(20) DEFAULT 'live',
            `trans_type` VARCHAR(10) NOT NULL,
            `price` DECIMAL(12,4) NOT NULL,
            `shares` INT NOT NULL,
            `amount` DECIMAL(14,2) NOT NULL,
            `fee` DECIMAL(10,2) DEFAULT 0,
            `reason` TEXT,
            `ai_score` DECIMAL(5,2) DEFAULT 0,
            `trans_date` DATE NOT NULL,
            `trans_time` VARCHAR(20) DEFAULT '',
            `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            KEY `idx_flow_type` (`flow_type`),
            KEY `idx_source` (`source`),
            KEY `idx_stock_date` (`stock_code`, `trans_date`),
            KEY `idx_trans_date` (`trans_date`),
            KEY `idx_strategy` (`strategy_type`, `trans_date`),
            KEY `idx_trade_mode` (`trade_mode`, `strategy_type`, `trans_date`),
            KEY `idx_trade_flow_order` (`order_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    "st_sim_signal": """
        CREATE TABLE IF NOT EXISTS `st_sim_signal` (
            `id` BIGINT NOT NULL AUTO_INCREMENT,
            `trade_mode` VARCHAR(20) DEFAULT 'live',
            `signal_date` DATE NOT NULL,
            `trade_date` DATE NOT NULL,
            `stock_code` VARCHAR(10) NOT NULL,
            `short_name` VARCHAR(20) DEFAULT '',
            `strategy_type` VARCHAR(20) NOT NULL,
            `status` VARCHAR(20) DEFAULT 'NEW',
            `reason` TEXT,
            `last_check_reason` TEXT,
            `ai_score` DECIMAL(5,2) DEFAULT 0,
            `quality_score` DECIMAL(5,2) DEFAULT 0,
            `entry_score` DECIMAL(5,2) DEFAULT 0,
            `final_trade_score` DECIMAL(5,2) DEFAULT 0,
            `expected_return_pct` DECIMAL(8,4) DEFAULT 0,
            `short_score` DECIMAL(5,2) DEFAULT 0,
            `long_score` DECIMAL(5,2) DEFAULT 0,
            `capital_score` DECIMAL(5,2) DEFAULT 0,
            `technical_score` DECIMAL(5,2) DEFAULT 0,
            `fundamental_score` DECIMAL(5,2) DEFAULT 0,
            `main_wave_score` DECIMAL(5,2) DEFAULT 0,
            `trend_hold_score` DECIMAL(5,2) DEFAULT 0,
            `event_risk_level` VARCHAR(10) DEFAULT 'LOW',
            `entry_price_low` DECIMAL(12,4) DEFAULT NULL,
            `entry_price_high` DECIMAL(12,4) DEFAULT NULL,
            `stop_loss_price` DECIMAL(12,4) DEFAULT NULL,
            `take_profit_1` DECIMAL(12,4) DEFAULT NULL,
            `take_profit_2` DECIMAL(12,4) DEFAULT NULL,
            `intended_amount` DECIMAL(14,2) DEFAULT 0,
            `intended_shares` INT DEFAULT 0,
            `risk_budget_amount` DECIMAL(14,2) DEFAULT 0,
            `risk_budget_note` TEXT,
            `filled_order_id` BIGINT DEFAULT NULL,
            `filled_position_id` BIGINT DEFAULT NULL,
            `pending_order_id` BIGINT DEFAULT NULL,
            `last_check_at` DATETIME DEFAULT NULL,
            `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            `updated_at` DATETIME DEFAULT NULL,
            PRIMARY KEY (`id`),
            UNIQUE KEY `uk_sim_signal` (`trade_mode`, `signal_date`, `trade_date`, `stock_code`, `strategy_type`),
            KEY `idx_sim_signal_status` (`trade_mode`, `trade_date`, `status`),
            KEY `idx_sim_signal_stock` (`stock_code`, `trade_date`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    "st_sim_order": """
        CREATE TABLE IF NOT EXISTS `st_sim_order` (
            `id` BIGINT NOT NULL AUTO_INCREMENT,
            `signal_id` BIGINT DEFAULT NULL,
            `trade_mode` VARCHAR(20) DEFAULT 'live',
            `order_date` DATE NOT NULL,
            `order_time` VARCHAR(20) DEFAULT '',
            `stock_code` VARCHAR(10) NOT NULL,
            `short_name` VARCHAR(20) DEFAULT '',
            `strategy_type` VARCHAR(20) NOT NULL,
            `side` VARCHAR(10) NOT NULL,
            `order_type` VARCHAR(20) DEFAULT 'SIM_LIMIT',
            `limit_price` DECIMAL(12,4) DEFAULT NULL,
            `target_price` DECIMAL(12,4) DEFAULT NULL,
            `requested_shares` INT DEFAULT 0,
            `remaining_shares` INT DEFAULT 0,
            `status` VARCHAR(20) DEFAULT 'PENDING',
            `filled_price` DECIMAL(12,4) DEFAULT NULL,
            `filled_shares` INT DEFAULT 0,
            `filled_amount` DECIMAL(14,2) DEFAULT 0,
            `fee` DECIMAL(10,2) DEFAULT 0,
            `position_id` BIGINT DEFAULT NULL,
            `source_event` VARCHAR(40) DEFAULT '',
            `price_source` VARCHAR(40) DEFAULT '',
            `risk_budget_amount` DECIMAL(14,2) DEFAULT 0,
            `risk_budget_note` TEXT,
            `match_count` INT DEFAULT 0,
            `reason` TEXT,
            `reject_reason` TEXT,
            `last_match_reason` TEXT,
            `cancel_reason` TEXT,
            `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            `updated_at` DATETIME DEFAULT NULL,
            `filled_at` DATETIME DEFAULT NULL,
            PRIMARY KEY (`id`),
            KEY `idx_sim_order_signal` (`signal_id`),
            KEY `idx_sim_order_status` (`trade_mode`, `order_date`, `status`),
            KEY `idx_sim_order_stock` (`stock_code`, `order_date`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    "st_sim_event": """
        CREATE TABLE IF NOT EXISTS `st_sim_event` (
            `id` BIGINT NOT NULL AUTO_INCREMENT,
            `trade_mode` VARCHAR(20) DEFAULT 'live',
            `event_date` DATE NOT NULL,
            `event_time` VARCHAR(20) DEFAULT '',
            `event_type` VARCHAR(40) NOT NULL,
            `signal_id` BIGINT DEFAULT NULL,
            `order_id` BIGINT DEFAULT NULL,
            `position_id` BIGINT DEFAULT NULL,
            `stock_code` VARCHAR(10) DEFAULT '',
            `strategy_type` VARCHAR(20) DEFAULT '',
            `severity` VARCHAR(20) DEFAULT 'INFO',
            `message` TEXT,
            `payload` LONGTEXT,
            `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            KEY `idx_sim_event_date` (`trade_mode`, `event_date`, `event_type`),
            KEY `idx_sim_event_stock` (`stock_code`, `event_date`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    "st_sim_risk_budget": """
        CREATE TABLE IF NOT EXISTS `st_sim_risk_budget` (
            `id` BIGINT NOT NULL AUTO_INCREMENT,
            `trade_mode` VARCHAR(20) DEFAULT 'live',
            `budget_date` DATE NOT NULL,
            `strategy_type` VARCHAR(20) NOT NULL,
            `initial_capital` DECIMAL(14,2) DEFAULT 0,
            `total_equity` DECIMAL(14,2) DEFAULT 0,
            `cash_available` DECIMAL(14,2) DEFAULT 0,
            `max_total_position_amount` DECIMAL(14,2) DEFAULT 0,
            `max_strategy_amount` DECIMAL(14,2) DEFAULT 0,
            `used_strategy_amount` DECIMAL(14,2) DEFAULT 0,
            `pending_strategy_amount` DECIMAL(14,2) DEFAULT 0,
            `available_strategy_amount` DECIMAL(14,2) DEFAULT 0,
            `risk_budget_note` TEXT,
            `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            `updated_at` DATETIME DEFAULT NULL,
            PRIMARY KEY (`id`),
            UNIQUE KEY `uk_sim_risk_budget` (`trade_mode`, `budget_date`, `strategy_type`),
            KEY `idx_sim_risk_budget_date` (`trade_mode`, `budget_date`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    "st_strategy_snapshot": """
        CREATE TABLE IF NOT EXISTS `st_strategy_snapshot` (
            `id` BIGINT NOT NULL AUTO_INCREMENT,
            `snapshot_date` DATE NOT NULL,
            `strategy_type` VARCHAR(20) NOT NULL,
            `total_trades` INT DEFAULT 0,
            `win_count` INT DEFAULT 0,
            `lose_count` INT DEFAULT 0,
            `win_rate` DECIMAL(6,2) DEFAULT 0,
            `total_profit` DECIMAL(14,2) DEFAULT 0,
            `total_fee` DECIMAL(10,2) DEFAULT 0,
            `avg_profit_rate` DECIMAL(8,4) DEFAULT 0,
            `max_profit_rate` DECIMAL(8,4) DEFAULT 0,
            `max_loss_rate` DECIMAL(8,4) DEFAULT 0,
            `holding_count` INT DEFAULT 0,
            `holding_amount` DECIMAL(14,2) DEFAULT 0,
            `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            UNIQUE KEY `uk_date_strategy` (`snapshot_date`, `strategy_type`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
}


# These columns were historically added by application traffic.  They are the
# only missing columns an upgrade may add to an already populated legacy table.
_SAFE_ADDITIVE_COLUMNS = {
    "st_sim_position": {"signal_id", "entry_order_id", "exit_order_id", "trade_mode"},
    "st_trade_flow": {"order_id", "trade_mode"},
    "st_sim_signal": {
        "intended_amount", "intended_shares", "risk_budget_amount",
        "risk_budget_note", "pending_order_id",
    },
    "st_sim_order": {
        "requested_shares", "remaining_shares", "source_event", "price_source",
        "risk_budget_amount", "risk_budget_note", "match_count",
        "last_match_reason", "cancel_reason",
    },
    "st_sim_event": set(),
    "st_sim_risk_budget": set(),
    "st_strategy_snapshot": set(),
}


_COLUMN_RE = re.compile(r"^`(?P<name>[a-z0-9_]+)`\s+(?P<type>[a-z]+(?:\([0-9,]+\))?)(?P<tail>.*)$", re.I)
_INDEX_RE = re.compile(
    r"^(?:(?P<primary>PRIMARY)\s+KEY|(?P<unique>UNIQUE)\s+KEY\s+`[^`]+`|KEY\s+`[^`]+`)\s*\((?P<columns>[^)]+)\)",
    re.I,
)
_DEFAULT_RE = re.compile(
    r"\bDEFAULT\s+(?P<value>NULL|CURRENT_TIMESTAMP(?:\([0-9]+\))?|'(?:''|[^'])*'|-?[0-9]+(?:\.[0-9]+)?)",
    re.I,
)


def _normalize_default(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if normalized.upper() == "NULL":
        return None
    if len(normalized) >= 2 and normalized[0] == normalized[-1] == "'":
        normalized = normalized[1:-1].replace("''", "'")
    timestamp_match = re.fullmatch(
        r"CURRENT_TIMESTAMP(?:\((?P<precision>[0-9]*)\))?",
        normalized,
        re.I,
    )
    if timestamp_match:
        precision = timestamp_match.group("precision")
        return f"current_timestamp({precision})" if precision else "current_timestamp"
    if re.fullmatch(r"-?[0-9]+(?:\.[0-9]+)?", normalized):
        try:
            number = format(Decimal(normalized), "f")
            return number.rstrip("0").rstrip(".") if "." in number else number
        except InvalidOperation:
            pass
    return normalized


def _parse_contract(ddl: str) -> dict[str, Any]:
    columns: dict[str, dict[str, Any]] = {}
    indexes: set[tuple[bool, tuple[str, ...]]] = set()
    for raw_line in ddl.splitlines():
        line = raw_line.strip().rstrip(",")
        column_match = _COLUMN_RE.match(line)
        if column_match:
            name = column_match.group("name")
            tail = column_match.group("tail")
            default_match = _DEFAULT_RE.search(tail)
            base_type = column_match.group("type").split("(", 1)[0].casefold()
            character = base_type in {"char", "varchar", "text", "longtext"}
            columns[name] = {
                "ordinal_position": len(columns) + 1,
                "column_type": column_match.group("type").casefold(),
                "is_nullable": "NO" if re.search(r"\bNOT\s+NULL\b", tail, re.I) else "YES",
                "column_default": _normalize_default(
                    default_match.group("value") if default_match else None
                ),
                "extra": "auto_increment" if re.search(r"\bAUTO_INCREMENT\b", tail, re.I) else "",
                "character_set_name": "utf8mb4" if character else None,
                "collation_name": EXPECTED_COLLATION if character else None,
                "ddl": line,
            }
            continue
        index_match = _INDEX_RE.match(line)
        if index_match:
            index_columns = tuple(
                item.strip().strip("`").casefold()
                for item in index_match.group("columns").split(",")
            )
            indexes.add((bool(index_match.group("primary") or index_match.group("unique")), index_columns))
    if not columns or not indexes:
        raise RuntimeError("sim trade schema source contract is invalid")
    return {"columns": columns, "indexes": indexes}


EXPECTED_CONTRACTS = {
    table_name: _parse_contract(ddl)
    for table_name, ddl in TABLE_DDL.items()
}


def _mapping_value(row: Mapping[str, Any], name: str) -> Any:
    return row.get(name) if name in row else row.get(name.casefold())


def _load_inventory(connection) -> dict[str, Any]:
    table_rows = connection.execute(
        text(
            "SELECT TABLE_NAME, ENGINE, TABLE_COLLATION "
            "FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() "
            "AND TABLE_NAME IN (" + ",".join(f"'{name}'" for name in TABLE_DDL) + ")"
        )
    ).mappings().all()
    tables = {
        str(_mapping_value(row, "TABLE_NAME")): {
            "engine": str(_mapping_value(row, "ENGINE") or ""),
            "collation": str(_mapping_value(row, "TABLE_COLLATION") or ""),
        }
        for row in table_rows
    }
    column_rows = connection.execute(
        text(
            "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_TYPE, IS_NULLABLE, "
            "COLUMN_DEFAULT, EXTRA, CHARACTER_SET_NAME, COLLATION_NAME "
            "FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN ("
            + ",".join(f"'{name}'" for name in TABLE_DDL) + ")"
        )
    ).mappings().all()
    columns: dict[str, dict[str, dict[str, Any]]] = {name: {} for name in TABLE_DDL}
    for row in column_rows:
        table_name = str(_mapping_value(row, "TABLE_NAME"))
        column_name = str(_mapping_value(row, "COLUMN_NAME"))
        columns.setdefault(table_name, {})[column_name] = {
            "ordinal_position": int(_mapping_value(row, "ORDINAL_POSITION") or 0),
            "column_type": str(_mapping_value(row, "COLUMN_TYPE") or "").casefold(),
            "is_nullable": str(_mapping_value(row, "IS_NULLABLE") or "").upper(),
            "column_default": _normalize_default(_mapping_value(row, "COLUMN_DEFAULT")),
            "extra": re.sub(
                r"\bdefault_generated\b",
                "",
                str(_mapping_value(row, "EXTRA") or "").casefold(),
            ).strip(),
            "character_set_name": (
                str(_mapping_value(row, "CHARACTER_SET_NAME") or "").casefold()
                or None
            ),
            "collation_name": (
                str(_mapping_value(row, "COLLATION_NAME") or "").casefold()
                or None
            ),
        }
    index_rows = connection.execute(
        text(
            "SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME "
            "FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() "
            "AND TABLE_NAME IN (" + ",".join(f"'{name}'" for name in TABLE_DDL) + ") "
            "ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX"
        )
    ).mappings().all()
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in index_rows:
        table_name = str(_mapping_value(row, "TABLE_NAME"))
        index_name = str(_mapping_value(row, "INDEX_NAME"))
        item = grouped.setdefault(
            (table_name, index_name),
            {"unique": int(_mapping_value(row, "NON_UNIQUE") or 0) == 0, "columns": []},
        )
        item["columns"].append(
            (int(_mapping_value(row, "SEQ_IN_INDEX") or 0), str(_mapping_value(row, "COLUMN_NAME")).casefold())
        )
    indexes: dict[str, set[tuple[bool, tuple[str, ...]]]] = {name: set() for name in TABLE_DDL}
    for (table_name, _index_name), item in grouped.items():
        indexes.setdefault(table_name, set()).add(
            (bool(item["unique"]), tuple(column for _seq, column in sorted(item["columns"])))
        )
    return {"tables": tables, "columns": columns, "indexes": indexes}


def validate_sim_trade_runtime_schema(engine, *, connection=None) -> dict[str, Any]:
    """Validate all seven tables without issuing persistent DDL or DML."""

    owns_connection = connection is None
    bound_connection = connection or engine.connect()
    try:
        inventory = _load_inventory(bound_connection)
    finally:
        if owns_connection:
            bound_connection.close()

    errors: list[str] = []
    for table_name, expected in EXPECTED_CONTRACTS.items():
        table = inventory["tables"].get(table_name)
        if table is None:
            errors.append(f"{table_name}:missing-table")
            continue
        if str(table.get("engine") or "").casefold() != EXPECTED_ENGINE.casefold():
            errors.append(f"{table_name}:engine")
        if str(table.get("collation") or "") != EXPECTED_COLLATION:
            errors.append(f"{table_name}:collation")
        actual_columns = inventory["columns"].get(table_name, {})
        unexpected_columns = sorted(set(actual_columns) - set(expected["columns"]))
        if unexpected_columns:
            errors.append(
                f"{table_name}:unexpected-columns:{'|'.join(unexpected_columns)}"
            )
        for column_name, expected_column in expected["columns"].items():
            actual = actual_columns.get(column_name)
            if actual is None:
                errors.append(f"{table_name}.{column_name}:missing")
                continue
            for field in (
                "ordinal_position", "column_type", "is_nullable",
                "column_default", "extra", "character_set_name", "collation_name",
            ):
                if actual.get(field) != expected_column.get(field):
                    errors.append(f"{table_name}.{column_name}:{field}")
        actual_indexes = inventory["indexes"].get(table_name, set())
        for index_shape in expected["indexes"]:
            if index_shape not in actual_indexes:
                errors.append(f"{table_name}:index:{index_shape}")
    if errors:
        raise RuntimeError("sim trade physical schema differs: " + ",".join(sorted(errors)))
    return {
        "schema": "probiga.sim-trade-physical-contract.v1",
        "status": "HEALTHY",
        "table_count": len(TABLE_DDL),
        "tables": sorted(TABLE_DDL),
        "physical_schema_verified": True,
        "runtime_ddl_required": False,
        "read_only": True,
    }


def _actual_index_shapes(connection, table_name: str) -> set[tuple[bool, tuple[str, ...]]]:
    inventory = _load_inventory(connection)
    return set(inventory["indexes"].get(table_name, set()))


def privileged_migrate_sim_trade_schema(engine) -> dict[str, Any]:
    """Create or safely upgrade sim-trade tables in a writer-fenced window."""

    added_columns: list[str] = []
    added_indexes: list[str] = []
    with engine.begin() as connection:
        for table_name, ddl in TABLE_DDL.items():
            connection.execute(text(ddl))
            inventory = _load_inventory(connection)
            actual_columns = inventory["columns"].get(table_name, {})
            expected_columns = EXPECTED_CONTRACTS[table_name]["columns"]
            missing = set(expected_columns) - set(actual_columns)
            unsafe_missing = missing - _SAFE_ADDITIVE_COLUMNS[table_name]
            if unsafe_missing:
                raise RuntimeError(
                    f"legacy sim trade table {table_name} misses non-additive columns: "
                    + ",".join(sorted(unsafe_missing))
                )
            ordered_names = list(expected_columns)
            for column_name in (name for name in ordered_names if name in missing):
                definition = expected_columns[column_name]["ddl"]
                previous_name = ordered_names[ordered_names.index(column_name) - 1]
                connection.execute(
                    text(
                        f"ALTER TABLE `{table_name}` ADD COLUMN {definition} "
                        f"AFTER `{previous_name}`"
                    )
                )
                added_columns.append(f"{table_name}.{column_name}")

            # Two historic VARCHAR fields were intentionally widened to TEXT.
            if table_name == "st_sim_position":
                sell_reason = actual_columns.get("sell_reason", {})
                if sell_reason.get("column_type") != "text":
                    connection.execute(text("ALTER TABLE `st_sim_position` MODIFY COLUMN `sell_reason` TEXT"))
            if table_name == "st_trade_flow":
                reason = actual_columns.get("reason", {})
                if reason.get("column_type") != "text":
                    connection.execute(text("ALTER TABLE `st_trade_flow` MODIFY COLUMN `reason` TEXT"))

            inventory = _load_inventory(connection)
            actual_shapes = set(inventory["indexes"].get(table_name, set()))
            for unique, columns in sorted(EXPECTED_CONTRACTS[table_name]["indexes"]):
                shape = (unique, columns)
                if shape in actual_shapes:
                    continue
                if columns == ("id",) and unique:
                    raise RuntimeError(f"legacy sim trade table {table_name} has no primary id")
                base_name = "uk" if unique else "idx"
                generated_name = f"{base_name}_{table_name[3:]}_{'_'.join(columns)}"[:64]
                kind = "UNIQUE INDEX" if unique else "INDEX"
                quoted_columns = ", ".join(f"`{column}`" for column in columns)
                connection.execute(
                    text(f"ALTER TABLE `{table_name}` ADD {kind} `{generated_name}` ({quoted_columns})")
                )
                actual_shapes.add(shape)
                added_indexes.append(f"{table_name}.{generated_name}")

    validated = validate_sim_trade_runtime_schema(engine)
    return {
        **validated,
        "migration_status": "ok",
        "added_columns": added_columns,
        "added_indexes": added_indexes,
    }


__all__ = [
    "EXPECTED_COLLATION",
    "EXPECTED_CONTRACTS",
    "EXPECTED_ENGINE",
    "TABLE_DDL",
    "privileged_migrate_sim_trade_schema",
    "validate_sim_trade_runtime_schema",
]
