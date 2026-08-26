"""Privileged physical contract for AI recommendation execution history.

Application and worker processes are DML-only. They may validate and append
history rows, but creation, legacy upgrades and index repair belong to the
fenced release migration.
"""
from __future__ import annotations

from typing import Any

from sqlalchemy import text

from server.common.schema_recovery_evidence import (
    ensure_evidence_table,
    load_pending_physical_rewrite_plan,
    make_evidence_record,
    persist_and_verify_evidence,
    plan_sha256,
    row_payload,
    row_sha256,
    table_content_fingerprint,
    verify_pending_plan_content,
)


TABLE_NAME = "st_recommended_run_history"
EXPECTED_ENGINE = "InnoDB"
EXPECTED_COLLATION = "utf8mb4_unicode_ci"
_UID_PATTERN = "^[0-9a-f]{32}$"
RECOVERY_VERSION = "recommended-run-history-legacy-terminalization.v1"
PHYSICAL_RECOVERY_VERSION = "recommended-run-history-physical-normalization.v1"
LEGACY_TERMINAL_ERROR = (
    "LEGACY_UNBOUND_RUN_TERMINATED_DURING_PRIVILEGED_SCHEMA_RECOVERY"
)
_ACTIVE_STATUSES = frozenset({"queued", "submitted", "running"})
_LEGACY_UTF8MB4_COLLATIONS = frozenset(
    {"utf8mb4_general_ci", EXPECTED_COLLATION}
)

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
            f"NOT REGEXP_LIKE(run_uid, _utf8mb4'{_UID_PATTERN}', 'c') "
            "THEN 1 ELSE 0 END) AS invalid_run_uid_count, "
            "COUNT(*) - COUNT(DISTINCT run_uid) AS duplicate_run_uid_count, "
            "SUM(CASE WHEN scheduler_job_id IS NOT NULL AND "
            "(NOT REGEXP_LIKE(scheduler_job_id, "
            f"_utf8mb4'{_UID_PATTERN}', 'c') OR "
            "scheduler_job_id COLLATE utf8mb4_bin <> "
            "run_uid COLLATE utf8mb4_bin) "
            "THEN 1 ELSE 0 END) AS invalid_scheduler_job_count, "
            "SUM(CASE WHEN status IN ('queued','submitted','running') "
            "AND scheduler_job_id IS NULL THEN 1 ELSE 0 END) "
            "AS unbound_active_count, "
            "SUM(CASE WHEN status IN ('queued','submitted','running') "
            "AND build_sha IS NULL THEN 1 ELSE 0 END) "
            "AS unbuilt_active_count, "
            "SUM(CASE WHEN build_sha IS NOT NULL AND "
            "NOT REGEXP_LIKE(build_sha, "
            "_utf8mb4'^[0-9a-f]{40}$', 'c') "
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


def _legacy_terminal_finished_at(row: dict[str, Any]) -> Any:
    for name in (
        "finished_at", "started_at", "execution_time", "created_at", "updated_at",
    ):
        value = row.get(name)
        if value is not None:
            return value
    raise RuntimeError(
        "legacy active recommendation run has no deterministic terminal timestamp"
    )


def _legacy_active_rows(connection, *, for_update: bool = False) -> list[dict[str, Any]]:
    suffix = " FOR UPDATE" if for_update else ""
    rows = connection.execute(text(
        f"SELECT * FROM `{TABLE_NAME}` "
        "WHERE LOWER(status) IN ('queued','submitted','running') "
        f"ORDER BY id{suffix}"
    )).mappings().all()
    result: list[dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        # Bound runs created by the current scheduler are not legacy recovery
        # candidates.  The fenced operator must resolve those through normal
        # scheduler ownership instead of silently terminating them here.
        if row.get("scheduler_job_id") and row.get("build_sha"):
            continue
        result.append(row)
    return result


def _build_legacy_terminal_plan(rows: list[dict[str, Any]]) -> dict[str, Any]:
    actions: list[dict[str, Any]] = []
    source_rows: dict[int, dict[str, Any]] = {}
    for row in rows:
        source_id = int(row.get("id") or 0)
        run_uid = str(row.get("run_uid") or "")
        status = str(row.get("status") or "").casefold()
        if source_id <= 0 or not run_uid or status not in _ACTIVE_STATUSES:
            raise RuntimeError("legacy recommendation run recovery identity differs")
        finished_at = _legacy_terminal_finished_at(row)
        source_rows[source_id] = row
        actions.append({
            "action": "TERMINALIZE",
            "source_row_id": source_id,
            "run_uid": run_uid,
            "source_status": status,
            "terminal_status": "error",
            "terminal_error": LEGACY_TERMINAL_ERROR,
            "terminal_finished_at": row_payload({"value": finished_at})["value"],
            "source_row_sha256": row_sha256(row),
        })
    actions.sort(key=lambda item: (item["source_row_id"], item["run_uid"]))
    payload = {"actions": actions}
    return {
        "schema": "probiga.recommended-run-history-recovery-plan.v1",
        "recovery_version": RECOVERY_VERSION,
        "terminalize_count": len(actions),
        "destructive_changes_planned": bool(actions),
        "actions": actions,
        "plan_sha256": plan_sha256(
            recovery_version=RECOVERY_VERSION,
            payload=payload,
        ),
        "read_only": True,
        "_plan_payload": payload,
        "_source_rows": source_rows,
    }


def _legacy_terminal_plan(connection, *, for_update: bool = False) -> dict[str, Any]:
    return _build_legacy_terminal_plan(
        _legacy_active_rows(connection, for_update=for_update)
    )


def _public_recovery_plan(plan: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in plan.items() if not key.startswith("_")}


def plan_recommended_run_history_recovery(engine) -> dict[str, Any]:
    """Build a read-only terminalization/physical-rewrite dry-run plan."""

    with engine.connect() as connection:
        columns = _column_inventory(connection)
        metadata = _table_metadata(connection)
        if not columns or metadata is None:
            terminal_plan = _build_legacy_terminal_plan([])
            fingerprint = None
            rewrite_required = True
        else:
            terminal_plan = _legacy_terminal_plan(connection)
            drift = _column_drift(columns)
            rewrite_required = (
                metadata["engine"].casefold() != EXPECTED_ENGINE.casefold()
                or metadata["table_collation"].casefold()
                != EXPECTED_COLLATION.casefold()
                or bool(drift)
            )
            safe_drift = {
                name: observed for name, observed in drift.items()
                if not (observed is None and name in _ADDITIVE_COLUMNS)
            }
            try:
                _assert_safe_nonempty_physical_rewrite(
                    columns=columns,
                    metadata=metadata,
                    drift=safe_drift,
                )
                safe_automatic_rewrite = True
            except RuntimeError:
                safe_automatic_rewrite = False
            fingerprint = (
                table_content_fingerprint(connection, TABLE_NAME)
                if rewrite_required and safe_automatic_rewrite else None
            )
        if not columns or metadata is None:
            safe_automatic_rewrite = True
    physical = {
        "table_exists": bool(columns),
        "engine": metadata["engine"] if metadata else None,
        "table_collation": metadata["table_collation"] if metadata else None,
        "rewrite_required": rewrite_required,
        "safe_automatic_rewrite": safe_automatic_rewrite,
        "before_fingerprint": fingerprint,
    }
    public = _public_recovery_plan(terminal_plan)
    combined_hash = plan_sha256(
        recovery_version=RECOVERY_VERSION,
        payload={"terminal": public["actions"], "physical": physical},
    )
    return {
        **public,
        "physical_rewrite": physical,
        "ready_for_privileged_apply": safe_automatic_rewrite,
        "recovery_bundle_sha256": combined_hash,
    }


def _only_collation_diff(
    observed: dict[str, Any], expected: dict[str, Any]
) -> bool:
    for field, value in expected.items():
        if field == "extra_contains":
            continue
        actual = observed.get(field)
        if field == "collation_name":
            if actual not in _LEGACY_UTF8MB4_COLLATIONS or value != EXPECTED_COLLATION:
                return False
            continue
        if actual != value:
            return False
    extra = str(observed.get("extra") or "").casefold()
    return all(token in extra for token in expected["extra_contains"])


def _safe_uid_drift(name: str, observed: dict[str, Any]) -> bool:
    expected = EXPECTED_COLUMN_CONTRACT[name]
    allowed_types = {
        "run_uid": {"char(32)", "varchar(32)", "varchar(40)", "varchar(64)"},
        "scheduler_job_id": {
            "char(32)", "varchar(32)", "varchar(40)", "varchar(64)",
        },
    }
    if observed.get("column_type") not in allowed_types[name]:
        return False
    for field, value in expected.items():
        if field in {"extra_contains", "column_type", "collation_name"}:
            continue
        if observed.get(field) != value:
            return False
    if observed.get("collation_name") not in _LEGACY_UTF8MB4_COLLATIONS:
        return False
    extra = str(observed.get("extra") or "").casefold()
    return all(token in extra for token in expected["extra_contains"])


def _assert_safe_nonempty_physical_rewrite(
    *,
    columns: dict[str, dict[str, Any]],
    metadata: dict[str, Any],
    drift: dict[str, dict[str, Any] | None],
) -> None:
    if metadata["engine"].casefold() != EXPECTED_ENGINE.casefold():
        raise RuntimeError("nonempty recommendation history engine differs")
    if metadata["table_collation"].casefold() not in _LEGACY_UTF8MB4_COLLATIONS:
        raise RuntimeError("nonempty recommendation history charset differs")
    for column in columns.values():
        charset = column.get("character_set_name")
        collation = column.get("collation_name")
        if charset is not None and (
            charset != "utf8mb4" or collation not in _LEGACY_UTF8MB4_COLLATIONS
        ):
            raise RuntimeError(
                "nonempty recommendation history character encoding differs"
            )
    unsafe: list[str] = []
    for name, observed in drift.items():
        if observed is None:
            unsafe.append(name)
        elif name in {"run_uid", "scheduler_job_id"}:
            if not _safe_uid_drift(name, observed):
                unsafe.append(name)
        elif not _only_collation_diff(observed, EXPECTED_COLUMN_CONTRACT[name]):
            unsafe.append(name)
    if unsafe:
        raise RuntimeError(
            "nonempty recommendation history column drift cannot be modified "
            "in place: " + ",".join(sorted(unsafe))
        )


def _persist_legacy_terminal_plan(connection, plan: dict[str, Any]) -> dict[str, Any]:
    records = []
    source_rows = plan["_source_rows"]
    for action in plan["actions"]:
        source_id = int(action["source_row_id"])
        records.append(make_evidence_record(
            recovery_version=RECOVERY_VERSION,
            source_table=TABLE_NAME,
            source_row_id=source_id,
            action="TERMINALIZE",
            business_key={"run_uid": action["run_uid"]},
            source_row=source_rows[source_id],
            plan_payload=plan["_plan_payload"],
            plan_hash=plan["plan_sha256"],
        ))
    return persist_and_verify_evidence(connection, records)


def _apply_legacy_terminal_plan(connection, plan: dict[str, Any]) -> int:
    source_rows = plan["_source_rows"]
    for action in plan["actions"]:
        result = connection.execute(text(
            f"UPDATE `{TABLE_NAME}` SET status='error', "
            "finished_at=:finished_at, trigger_source='legacy_schema_recovery', "
            "error=:terminal_error "
            "WHERE id=:source_row_id AND run_uid=:run_uid "
            "AND LOWER(status)=:source_status "
            "AND (scheduler_job_id IS NULL OR build_sha IS NULL)"
        ), {
            "finished_at": _legacy_terminal_finished_at(
                source_rows[int(action["source_row_id"])]
            ),
            "terminal_error": LEGACY_TERMINAL_ERROR,
            "source_row_id": action["source_row_id"],
            "run_uid": action["run_uid"],
            "source_status": action["source_status"],
        })
        if int(result.rowcount or 0) != 1:
            raise RuntimeError(
                "legacy recommendation terminalization changed after evidence capture"
            )
    return len(plan["actions"])


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


def _history_physical_contract_ready(connection) -> bool:
    columns = _column_inventory(connection)
    metadata = _table_metadata(connection)
    if metadata is None or _column_drift(columns):
        return False
    if (
        metadata["engine"].casefold() != EXPECTED_ENGINE.casefold()
        or metadata["table_collation"].casefold()
        != EXPECTED_COLLATION.casefold()
    ):
        return False
    index_shapes, _names = _index_inventory(connection)
    if any(
        not _index_shape_available(index_shapes, required)
        for required in _REQUIRED_INDEX_SHAPES
    ):
        return False
    data_contract = _history_data_contract(connection)
    return not any(
        value for name, value in data_contract.items() if name != "row_count"
    )


def migrate_recommended_run_history(engine) -> dict[str, Any]:
    """Create or safely upgrade history with a privileged fenced engine."""

    added_columns: list[str] = []
    normalized_columns: list[str] = []
    added_indexes: list[str] = []
    recovery_plan = _build_legacy_terminal_plan([])
    recovery_evidence: dict[str, Any] = {
        "evidence_row_count": 0,
        "evidence_verified": True,
    }
    terminalized_count = 0
    physical_rewrite: dict[str, Any] = {
        "required": False,
        "content_verified": True,
    }
    physical_context: dict[str, Any] | None = None
    with engine.begin() as connection:
        ensure_evidence_table(connection)
        pending_physical = load_pending_physical_rewrite_plan(
            connection,
            recovery_version=PHYSICAL_RECOVERY_VERSION,
            source_table=TABLE_NAME,
        )
        if pending_physical is not None:
            if (
                pending_physical["business_key"] != {"table": TABLE_NAME}
                or int(pending_physical["record"]["source_row_id"]) != 0
                or pending_physical["plan_payload"].get("table") != TABLE_NAME
            ):
                raise RuntimeError(
                    "recommendation history pending physical PLAN differs"
                )
            verify_pending_plan_content(connection, pending_physical)
            physical_context = pending_physical
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
        if "status" not in columns:
            legacy_count = int(connection.execute(text(
                f"SELECT COUNT(*) FROM `{TABLE_NAME}`"
            )).scalar() or 0)
            if legacy_count:
                raise RuntimeError(
                    "nonempty recommendation history has no status column; "
                    "automatic terminal recovery is unsafe"
                )
        else:
            recovery_plan = _legacy_terminal_plan(
                connection, for_update=True
            )
            # This write and its exact hash verification deliberately happen
            # before any source-table ALTER or destructive DML.
            recovery_evidence = _persist_legacy_terminal_plan(
                connection, recovery_plan
            )
        for name, ddl in _ADDITIVE_COLUMNS.items():
            if name in columns:
                continue
            connection.execute(text(
                f"ALTER TABLE {TABLE_NAME} ADD COLUMN `{name}` {ddl}"
            ))
            added_columns.append(name)

        terminalized_count = _apply_legacy_terminal_plan(
            connection, recovery_plan
        )

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
        drift = _column_drift(columns)
        rewrite_required = table_drift or bool(drift)
        if row_count and rewrite_required:
            _assert_safe_nonempty_physical_rewrite(
                columns=columns,
                metadata=metadata,
                drift=drift,
            )
            before = table_content_fingerprint(connection, TABLE_NAME)
            rewrite_manifest = {
                "before_fingerprint": before,
                "fingerprint_columns": list(columns),
                "column_drift": sorted(drift),
                "engine": metadata["engine"],
                "table_collation": metadata["table_collation"],
            }
            physical_plan_payload = {"table": TABLE_NAME, **rewrite_manifest}
            physical_plan_hash = plan_sha256(
                recovery_version=PHYSICAL_RECOVERY_VERSION,
                payload=physical_plan_payload,
            )
            if physical_context is None:
                plan_record = make_evidence_record(
                    recovery_version=PHYSICAL_RECOVERY_VERSION,
                    source_table=TABLE_NAME,
                    source_row_id=0,
                    action="PHYSICAL_REWRITE_PLAN",
                    business_key={"table": TABLE_NAME},
                    source_row=rewrite_manifest,
                    plan_payload=physical_plan_payload,
                    plan_hash=physical_plan_hash,
                )
                persist_and_verify_evidence(connection, [plan_record])
                physical_context = {
                    "record": plan_record,
                    "business_key": {"table": TABLE_NAME},
                    "source_row": rewrite_manifest,
                    "plan_payload": physical_plan_payload,
                    "plan_sha256": physical_plan_hash,
                }
            else:
                rewrite_manifest = dict(physical_context["source_row"])
                before = rewrite_manifest["before_fingerprint"]
                physical_plan_payload = dict(physical_context["plan_payload"])
                physical_plan_hash = str(physical_context["plan_sha256"])

            collation_rewrite = (
                metadata["table_collation"].casefold()
                != EXPECTED_COLLATION.casefold()
                or any(
                    column.get("character_set_name") is not None
                    and column.get("collation_name") != EXPECTED_COLLATION
                    for column in columns.values()
                )
            )
            if collation_rewrite:
                connection.execute(text(
                    f"ALTER TABLE `{TABLE_NAME}` ENGINE={EXPECTED_ENGINE}, "
                    "CONVERT TO CHARACTER SET utf8mb4 "
                    f"COLLATE {EXPECTED_COLLATION}"
                ))
            columns = _column_inventory(connection)
            remaining_drift = _column_drift(columns)
            for name in sorted(remaining_drift):
                observed = remaining_drift[name]
                if (
                    observed is None
                    or name not in {"run_uid", "scheduler_job_id"}
                    or not _safe_uid_drift(name, observed)
                ):
                    raise RuntimeError(
                        f"unsupported recommendation column drift: {name}"
                    )
                connection.execute(text(
                    f"ALTER TABLE `{TABLE_NAME}` MODIFY COLUMN "
                    f"`{name}` {_COLUMN_DDL[name]}"
                ))
                normalized_columns.append(name)
        elif rewrite_required:
            connection.execute(text(
                f"ALTER TABLE `{TABLE_NAME}` ENGINE={EXPECTED_ENGINE}, "
                "CONVERT TO CHARACTER SET utf8mb4 "
                f"COLLATE {EXPECTED_COLLATION}"
            ))
            columns = _column_inventory(connection)
            for name in sorted(_column_drift(columns)):
                if name not in _COLUMN_DDL:
                    raise RuntimeError(
                        f"unsupported recommendation column drift: {name}"
                    )
                connection.execute(text(
                    f"ALTER TABLE `{TABLE_NAME}` MODIFY COLUMN "
                    f"`{name}` {_COLUMN_DDL[name]}"
                ))
                normalized_columns.append(name)
            physical_rewrite = {
                "required": True,
                "empty_table": True,
                "content_verified": True,
            }

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

        if physical_context is not None:
            if not _history_physical_contract_ready(connection):
                raise RuntimeError(
                    "recommendation history physical PLAN is not fully applied"
                )
            manifest = physical_context["source_row"]
            before = manifest["before_fingerprint"]
            after = table_content_fingerprint(
                connection,
                TABLE_NAME,
                columns=manifest.get("fingerprint_columns") or None,
            )
            if after != before:
                raise RuntimeError(
                    "recommendation history content fingerprint changed during "
                    "physical normalization"
                )
            verified_record = make_evidence_record(
                recovery_version=PHYSICAL_RECOVERY_VERSION,
                source_table=TABLE_NAME,
                source_row_id=0,
                action="PHYSICAL_REWRITE_VERIFIED",
                business_key={"table": TABLE_NAME},
                source_row={
                    "before_fingerprint": before,
                    "after_fingerprint": after,
                    "fingerprint_columns": manifest.get("fingerprint_columns"),
                },
                plan_payload=physical_context["plan_payload"],
                plan_hash=str(physical_context["plan_sha256"]),
            )
            persist_and_verify_evidence(connection, [verified_record])
            physical_rewrite = {
                "required": True,
                "plan_sha256": str(physical_context["plan_sha256"]),
                "before_fingerprint": before,
                "after_fingerprint": after,
                "content_verified": True,
                "resumed_pending_plan": pending_physical is not None,
            }

    validated = validate_recommended_run_history_schema(engine)
    return {
        **validated,
        "status": "ok",
        "added_columns": sorted(added_columns),
        "normalized_columns": sorted(normalized_columns),
        "added_indexes": sorted(added_indexes),
        "recovery_plan": _public_recovery_plan(recovery_plan),
        "recovery_evidence": recovery_evidence,
        "terminalized_count": terminalized_count,
        "physical_rewrite": physical_rewrite,
    }


__all__ = [
    "EXPECTED_COLLATION", "EXPECTED_COLUMN_CONTRACT", "EXPECTED_ENGINE",
    "LEGACY_TERMINAL_ERROR", "RECOVERY_VERSION", "REQUIRED_COLUMNS",
    "TABLE_NAME", "migrate_recommended_run_history",
    "plan_recommended_run_history_recovery",
    "validate_recommended_run_history_schema",
]
