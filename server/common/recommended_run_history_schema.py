"""Privileged physical contract for AI recommendation execution history.

Application and worker processes are DML-only. They may validate and append
history rows, but creation, legacy upgrades and index repair belong to the
fenced release migration.
"""
from __future__ import annotations

from typing import Any

from sqlalchemy import text


TABLE_NAME = "st_recommended_run_history"
EXPECTED_ENGINE = "InnoDB"
EXPECTED_COLLATION = "utf8mb4_unicode_ci"
_UID_PATTERN = "^[0-9a-f]{32}$"

REQUIRED_COLUMNS = (
    "id", "run_uid", "trade_date", "status", "min_score", "top_n",
    "strict_prev_trade_day", "execution_time", "started_at", "finished_at",
    "duration_seconds", "progress_percent", "done_count", "total", "passed",
    "flow_date", "hot_date", "market_mood_score", "message", "error",
    "trigger_source", "scheduler_job_id", "host_name", "build_sha",
    "created_at", "updated_at",
)


def _column_spec(
    column_type: str,
    nullable: bool,
    default: str | None = None,
    *,
    character: bool = False,
    extra_contains: tuple[str, ...] = (),
) -> dict[str, Any]:
    return {
        "column_type": column_type,
        "is_nullable": "YES" if nullable else "NO",
        "column_default": default,
        "character_set_name": "utf8mb4" if character else None,
        "collation_name": EXPECTED_COLLATION if character else None,
        "extra_contains": extra_contains,
    }


EXPECTED_COLUMN_CONTRACT = {
    "id": _column_spec("bigint", False, extra_contains=("auto_increment",)),
    "run_uid": _column_spec("char(32)", False, character=True),
    "trade_date": _column_spec("date", True),
    "status": _column_spec("varchar(20)", False, "running", character=True),
    "min_score": _column_spec("decimal(8,2)", True),
    "top_n": _column_spec("int", True),
    "strict_prev_trade_day": _column_spec("tinyint(1)", False, "0"),
    "execution_time": _column_spec("datetime", True),
    "started_at": _column_spec("datetime", True),
    "finished_at": _column_spec("datetime", True),
    "duration_seconds": _column_spec("int", True),
    "progress_percent": _column_spec("int", True),
    "done_count": _column_spec("int", True),
    "total": _column_spec("int", True),
    "passed": _column_spec("int", True),
    "flow_date": _column_spec("varchar(20)", True, character=True),
    "hot_date": _column_spec("varchar(20)", True, character=True),
    "market_mood_score": _column_spec("decimal(8,2)", True),
    "message": _column_spec("varchar(500)", True, character=True),
    "error": _column_spec("varchar(500)", True, character=True),
    "trigger_source": _column_spec(
        "varchar(32)", False, "scheduled", character=True
    ),
    "scheduler_job_id": _column_spec("char(32)", True, character=True),
    "host_name": _column_spec("varchar(128)", True, character=True),
    "build_sha": _column_spec("char(40)", True, character=True),
    "created_at": _column_spec("datetime", False, "current_timestamp"),
    "updated_at": _column_spec(
        "datetime", False, "current_timestamp",
        extra_contains=("on update current_timestamp",),
    ),
}

_COLUMN_DDL = {
    "id": "BIGINT NOT NULL AUTO_INCREMENT",
    "run_uid": (
        "CHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL"
    ),
    "trade_date": "DATE NULL",
    "status": (
        "VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci "
        "NOT NULL DEFAULT 'running'"
    ),
    "min_score": "DECIMAL(8,2) NULL",
    "top_n": "INT NULL",
    "strict_prev_trade_day": "TINYINT(1) NOT NULL DEFAULT 0",
    "execution_time": "DATETIME NULL",
    "started_at": "DATETIME NULL",
    "finished_at": "DATETIME NULL",
    "duration_seconds": "INT NULL",
    "progress_percent": "INT NULL",
    "done_count": "INT NULL",
    "total": "INT NULL",
    "passed": "INT NULL",
    "flow_date": (
        "VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL"
    ),
    "hot_date": (
        "VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL"
    ),
    "market_mood_score": "DECIMAL(8,2) NULL",
    "message": (
        "VARCHAR(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL"
    ),
    "error": (
        "VARCHAR(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL"
    ),
    "trigger_source": (
        "VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci "
        "NOT NULL DEFAULT 'scheduled'"
    ),
    "scheduler_job_id": (
        "CHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL"
    ),
    "host_name": (
        "VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL"
    ),
    "build_sha": (
        "CHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL"
    ),
    "created_at": "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP",
    "updated_at": (
        "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
    ),
}

_ADDITIVE_COLUMNS = {
    name: ddl for name, ddl in _COLUMN_DDL.items()
    if name not in {"id", "run_uid"}
}

_REQUIRED_INDEX_SHAPES = {
    (True, ("id",)),
    (True, ("run_uid",)),
    (False, ("trade_date", "started_at")),
    (False, ("status", "started_at")),
}


def _row_value(row: Any, *names: str) -> Any:
    for name in names:
        try:
            if name in row:
                return row[name]
        except TypeError:
            pass
    return None


def _normalize_default(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().strip("'").lower()
    if normalized.endswith("()"):
        normalized = normalized[:-2]
    return normalized


def _column_inventory(connection) -> dict[str, dict[str, Any]]:
    rows = connection.execute(
        text(
            "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, "
            "EXTRA, CHARACTER_SET_NAME, COLLATION_NAME "
            "FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:table_name"
        ),
        {"table_name": TABLE_NAME},
    ).mappings().all()
    inventory: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = str(_row_value(row, "COLUMN_NAME", "column_name") or "")
        if not name:
            continue
        inventory[name] = {
            "column_type": str(
                _row_value(row, "COLUMN_TYPE", "column_type") or ""
            ).lower(),
            "is_nullable": str(
                _row_value(row, "IS_NULLABLE", "is_nullable") or ""
            ).upper(),
            "column_default": _normalize_default(
                _row_value(row, "COLUMN_DEFAULT", "column_default")
            ),
            "extra": str(_row_value(row, "EXTRA", "extra") or "").lower(),
            "character_set_name": (
                str(_row_value(
                    row, "CHARACTER_SET_NAME", "character_set_name"
                ) or "").lower() or None
            ),
            "collation_name": (
                str(_row_value(row, "COLLATION_NAME", "collation_name") or "")
                .lower() or None
            ),
        }
    return inventory


def _table_metadata(connection) -> dict[str, Any] | None:
    row = connection.execute(
        text(
            "SELECT ENGINE, TABLE_COLLATION FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:table_name"
        ),
        {"table_name": TABLE_NAME},
    ).mappings().first()
    if row is None:
        return None
    return {
        "engine": str(_row_value(row, "ENGINE", "engine") or ""),
        "table_collation": str(
            _row_value(row, "TABLE_COLLATION", "table_collation") or ""
        ),
    }


def _index_inventory(
    connection,
) -> tuple[set[tuple[bool, tuple[str, ...]]], set[str]]:
    rows = connection.execute(text(f"SHOW INDEX FROM {TABLE_NAME}")).mappings().all()
    indexes: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = str(_row_value(row, "Key_name", "key_name") or "")
        if not name:
            continue
        non_unique = _row_value(row, "Non_unique", "non_unique")
        entry = indexes.setdefault(
            name, {"unique": int(non_unique or 0) == 0, "columns": []}
        )
        entry["columns"].append((
            int(_row_value(row, "Seq_in_index", "seq_in_index") or 0),
            str(_row_value(row, "Column_name", "column_name") or ""),
        ))
    return (
        {
            (
                bool(entry["unique"]),
                tuple(column for _position, column in sorted(entry["columns"])),
            )
            for entry in indexes.values()
        },
        set(indexes),
    )


def _index_shape_available(
    actual: set[tuple[bool, tuple[str, ...]]],
    required: tuple[bool, tuple[str, ...]],
) -> bool:
    if required in actual:
        return True
    required_unique, required_columns = required
    return not required_unique and (True, required_columns) in actual


def _available_index_name(used_names: set[str], preferred: str) -> str:
    if preferred not in used_names:
        used_names.add(preferred)
        return preferred
    suffix = 2
    while f"{preferred}_{suffix}" in used_names:
        suffix += 1
    result = f"{preferred}_{suffix}"
    used_names.add(result)
    return result


def _column_drift(
    actual: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any] | None]:
    drift: dict[str, dict[str, Any] | None] = {}
    for name, expected in EXPECTED_COLUMN_CONTRACT.items():
        observed = actual.get(name)
        if observed is None:
            drift[name] = None
            continue
        expected_base = {
            key: value for key, value in expected.items()
            if key != "extra_contains"
        }
        observed_base = {key: observed.get(key) for key in expected_base}
        extra = str(observed.get("extra") or "").lower()
        if observed_base != expected_base or any(
            token not in extra for token in expected["extra_contains"]
        ):
            drift[name] = observed
    return drift


def _history_data_contract(connection) -> dict[str, int]:
    row = connection.execute(
        text(
            f"SELECT COUNT(*) AS row_count, "
            "SUM(CASE WHEN run_uid IS NULL OR "
            f"BINARY run_uid NOT REGEXP '{_UID_PATTERN}' "
            "THEN 1 ELSE 0 END) AS invalid_run_uid_count, "
            "COUNT(*) - COUNT(DISTINCT run_uid) AS duplicate_run_uid_count, "
            "SUM(CASE WHEN scheduler_job_id IS NOT NULL AND "
            "(BINARY scheduler_job_id NOT REGEXP "
            f"'{_UID_PATTERN}' OR BINARY scheduler_job_id <> BINARY run_uid) "
            "THEN 1 ELSE 0 END) AS invalid_scheduler_job_count, "
            "SUM(CASE WHEN status IN ('queued','submitted','running') "
            "AND scheduler_job_id IS NULL THEN 1 ELSE 0 END) "
            "AS unbound_active_count, "
            "SUM(CASE WHEN status IN ('queued','submitted','running') "
            "AND build_sha IS NULL THEN 1 ELSE 0 END) "
            "AS unbuilt_active_count, "
            "SUM(CASE WHEN build_sha IS NOT NULL AND "
            "BINARY build_sha NOT REGEXP '^[0-9a-f]{40}$' "
            "THEN 1 ELSE 0 END) AS invalid_build_sha_count "
            f"FROM {TABLE_NAME}"
        )
    ).mappings().one()
    names = (
        "row_count", "invalid_run_uid_count", "duplicate_run_uid_count",
        "invalid_scheduler_job_count", "unbound_active_count",
        "unbuilt_active_count", "invalid_build_sha_count",
    )
    return {name: int(_row_value(row, name, name.upper()) or 0) for name in names}


def _assert_history_data_contract(data: dict[str, int]) -> None:
    failures = {
        name: value for name, value in data.items()
        if name != "row_count" and value
    }
    if failures:
        raise RuntimeError(
            "recommendation run history identity data differs: "
            f"{failures}; active legacy rows must be terminally reconciled "
            "before migration"
        )


def validate_recommended_run_history_schema(engine) -> dict[str, Any]:
    """Read-only proof that recommendation launches can be audited exactly."""

    with engine.connect() as connection:
        columns = _column_inventory(connection)
        metadata = _table_metadata(connection)
        index_shapes, _index_names = _index_inventory(connection)
        data_contract = _history_data_contract(connection) if columns else {}

    missing_columns = sorted(set(REQUIRED_COLUMNS) - set(columns))
    column_drift = _column_drift(columns)
    table_drift: dict[str, Any] = {}
    if metadata is None:
        table_drift["table"] = "missing"
    else:
        if metadata["engine"].lower() != EXPECTED_ENGINE.lower():
            table_drift["engine"] = metadata["engine"]
        if metadata["table_collation"].lower() != EXPECTED_COLLATION.lower():
            table_drift["table_collation"] = metadata["table_collation"]
    missing_indexes = sorted(
        required for required in _REQUIRED_INDEX_SHAPES
        if not _index_shape_available(index_shapes, required)
    )
    data_failures = {
        name: value for name, value in data_contract.items()
        if name != "row_count" and value
    }
    if (
        missing_columns or column_drift or table_drift
        or missing_indexes or data_failures
    ):
        raise RuntimeError(
            "recommended run history physical contract differs: "
            f"missing_columns={missing_columns}, column_drift={column_drift}, "
            f"table_drift={table_drift}, missing_indexes={missing_indexes}, "
            f"data_failures={data_failures}"
        )
    return {
        "table": TABLE_NAME,
        "column_names": list(REQUIRED_COLUMNS),
        "required_index_count": len(_REQUIRED_INDEX_SHAPES),
        "row_count": data_contract["row_count"],
        "identity_data_verified": True,
        "physical_contract_verified": True,
        "runtime_ddl_required": False,
        "read_only": True,
    }


def migrate_recommended_run_history(engine) -> dict[str, Any]:
    """Create or safely upgrade history with a privileged fenced engine."""

    added_columns: list[str] = []
    normalized_columns: list[str] = []
    added_indexes: list[str] = []
    with engine.begin() as connection:
        connection.execute(text(
            f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} ("
            "id BIGINT NOT NULL AUTO_INCREMENT, "
            "run_uid CHAR(32) CHARACTER SET utf8mb4 "
            "COLLATE utf8mb4_unicode_ci NOT NULL, "
            "trade_date DATE NULL, "
            "status VARCHAR(20) CHARACTER SET utf8mb4 "
            "COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'running', "
            "min_score DECIMAL(8,2) NULL, top_n INT NULL, "
            "strict_prev_trade_day TINYINT(1) NOT NULL DEFAULT 0, "
            "execution_time DATETIME NULL, started_at DATETIME NULL, "
            "finished_at DATETIME NULL, duration_seconds INT NULL, "
            "progress_percent INT NULL, done_count INT NULL, "
            "total INT NULL, passed INT NULL, "
            "flow_date VARCHAR(20) NULL, hot_date VARCHAR(20) NULL, "
            "market_mood_score DECIMAL(8,2) NULL, "
            "message VARCHAR(500) NULL, error VARCHAR(500) NULL, "
            "trigger_source VARCHAR(32) NOT NULL DEFAULT 'scheduled', "
            "scheduler_job_id CHAR(32) NULL, "
            "host_name VARCHAR(128) NULL, build_sha CHAR(40) NULL, "
            "created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, "
            "updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP "
            "ON UPDATE CURRENT_TIMESTAMP, "
            "PRIMARY KEY (id), UNIQUE KEY uk_rec_run_uid (run_uid), "
            "KEY idx_rec_run_date (trade_date, started_at), "
            "KEY idx_rec_status_started (status, started_at)"
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 "
            "COLLATE=utf8mb4_unicode_ci"
        ))
        columns = _column_inventory(connection)
        missing_core = sorted({"id", "run_uid"} - set(columns))
        if missing_core:
            raise RuntimeError(
                "legacy recommendation history core columns differ: "
                + ",".join(missing_core)
            )
        for name, ddl in _ADDITIVE_COLUMNS.items():
            if name in columns:
                continue
            connection.execute(text(
                f"ALTER TABLE {TABLE_NAME} ADD COLUMN `{name}` {ddl}"
            ))
            added_columns.append(name)

        columns = _column_inventory(connection)
        metadata = _table_metadata(connection)
        if metadata is None:
            raise RuntimeError("recommendation history table metadata is unavailable")
        data_contract = _history_data_contract(connection)
        _assert_history_data_contract(data_contract)
        row_count = data_contract["row_count"]

        table_drift = (
            metadata["engine"].lower() != EXPECTED_ENGINE.lower()
            or metadata["table_collation"].lower() != EXPECTED_COLLATION.lower()
        )
        if table_drift:
            if row_count:
                raise RuntimeError(
                    "nonempty recommendation history engine/collation differs; "
                    "offline verified migration is required"
                )
            connection.execute(text(
                f"ALTER TABLE {TABLE_NAME} ENGINE={EXPECTED_ENGINE}, "
                "DEFAULT CHARACTER SET utf8mb4 "
                f"COLLATE {EXPECTED_COLLATION}"
            ))

        drift = _column_drift(columns)
        safe_nonempty_normalizations = {"run_uid", "scheduler_job_id"}
        unsafe_nonempty = sorted(set(drift) - safe_nonempty_normalizations)
        if row_count and unsafe_nonempty:
            raise RuntimeError(
                "nonempty recommendation history column drift cannot be "
                "modified in place: " + ",".join(unsafe_nonempty)
            )
        for name in sorted(drift):
            if name not in _COLUMN_DDL:
                raise RuntimeError(f"unsupported recommendation column drift: {name}")
            connection.execute(text(
                f"ALTER TABLE {TABLE_NAME} MODIFY COLUMN "
                f"`{name}` {_COLUMN_DDL[name]}"
            ))
            normalized_columns.append(name)

        index_shapes, used_names = _index_inventory(connection)
        if (True, ("id",)) not in index_shapes:
            raise RuntimeError("recommendation history primary id index differs")
        index_specs = (
            ((True, ("run_uid",)), "uk_rec_run_uid", "UNIQUE INDEX", "`run_uid`"),
            (
                (False, ("trade_date", "started_at")),
                "idx_rec_run_date", "INDEX", "`trade_date`, `started_at`",
            ),
            (
                (False, ("status", "started_at")),
                "idx_rec_status_started", "INDEX", "`status`, `started_at`",
            ),
        )
        for shape, preferred_name, index_type, columns_sql in index_specs:
            if _index_shape_available(index_shapes, shape):
                continue
            name = _available_index_name(used_names, preferred_name)
            connection.execute(text(
                f"ALTER TABLE {TABLE_NAME} ADD {index_type} "
                f"`{name}` ({columns_sql})"
            ))
            added_indexes.append(name)
            index_shapes.add(shape)

    validated = validate_recommended_run_history_schema(engine)
    return {
        **validated,
        "status": "ok",
        "added_columns": sorted(added_columns),
        "normalized_columns": sorted(normalized_columns),
        "added_indexes": sorted(added_indexes),
    }


__all__ = [
    "EXPECTED_COLLATION", "EXPECTED_COLUMN_CONTRACT", "EXPECTED_ENGINE",
    "REQUIRED_COLUMNS", "TABLE_NAME", "migrate_recommended_run_history",
    "validate_recommended_run_history_schema",
]
