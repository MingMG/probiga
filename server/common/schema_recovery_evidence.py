"""Evidence helpers for fenced, privileged legacy-schema recovery.

The runtime account must never call this module's write helpers.  Recovery
callers first build a read-only deterministic plan.  A fenced migrator then
persists the complete pre-change source rows (or physical rewrite manifest)
and verifies the persisted hashes before applying any destructive DML.
"""
from __future__ import annotations

import base64
import hashlib
import json
import re
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Iterable, Mapping, Sequence

from sqlalchemy import text


EVIDENCE_TABLE = "st_privileged_schema_recovery_evidence"
PLAN_ACTION = "PHYSICAL_REWRITE_PLAN"
VERIFIED_ACTION = "PHYSICAL_REWRITE_VERIFIED"
EXPECTED_ENGINE = "InnoDB"
EXPECTED_COLLATION = "utf8mb4_unicode_ci"
EXPECTED_TRIGGER_DEFINER = "probiga_migrator@127.0.0.1"
EXPECTED_TRIGGER_SQL_MODE = (
    "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,"
    "ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
)
EXPECTED_CHARACTER_SET_CLIENT = "utf8mb4"
EXPECTED_COLLATION_CONNECTION = "utf8mb4_general_ci"
TRIGGER_STATEMENTS = {
    "trg_privileged_schema_recovery_evidence_immutable_bu": (
        f"CREATE TRIGGER trg_privileged_schema_recovery_evidence_immutable_bu "
        f"BEFORE UPDATE ON {EVIDENCE_TABLE} FOR EACH ROW "
        "BEGIN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = "
        "'schema recovery evidence is append only'; END"
    ),
    "trg_privileged_schema_recovery_evidence_immutable_bd": (
        f"CREATE TRIGGER trg_privileged_schema_recovery_evidence_immutable_bd "
        f"BEFORE DELETE ON {EVIDENCE_TABLE} FOR EACH ROW "
        "BEGIN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = "
        "'schema recovery evidence cannot be deleted'; END"
    ),
}
EVIDENCE_TABLE_DDL = f"""
CREATE TABLE IF NOT EXISTS `{EVIDENCE_TABLE}` (
    `event_id` BIGINT NOT NULL AUTO_INCREMENT,
    `recovery_key` CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    `recovery_version` VARCHAR(80) NOT NULL,
    `source_table` VARCHAR(64) NOT NULL,
    `source_row_id` BIGINT NOT NULL,
    `action` VARCHAR(32) NOT NULL,
    `business_key_json` LONGTEXT NOT NULL,
    `source_row_json` LONGTEXT NOT NULL,
    `source_row_sha256` CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    `plan_payload_json` LONGTEXT NOT NULL,
    `plan_sha256` CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    `created_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (`event_id`),
    UNIQUE KEY `uk_schema_recovery_key` (`recovery_key`),
    KEY `idx_schema_recovery_source` (`source_table`, `source_row_id`),
    KEY `idx_schema_recovery_plan` (`recovery_version`, `plan_sha256`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_HASH = re.compile(r"^[0-9a-f]{64}$")
_LOGICAL_FIELDS = (
    "recovery_key",
    "recovery_version",
    "source_table",
    "source_row_id",
    "action",
    "business_key_json",
    "source_row_json",
    "source_row_sha256",
    "plan_payload_json",
    "plan_sha256",
)


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        # JSON has no representation for NaN/Infinity.  Persist their exact
        # Python spelling as a string rather than emitting non-standard JSON.
        return value if value == value and abs(value) != float("inf") else str(value)
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.isoformat(timespec="microseconds")
    if isinstance(value, (date, time)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {
            "encoding": "base64",
            "value": base64.b64encode(bytes(value)).decode("ascii"),
        }
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return str(value)


def canonical_json(value: Any) -> str:
    """Return stable UTF-8 JSON suitable for hashing and durable evidence."""

    return json.dumps(
        _json_value(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def row_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): _json_value(value) for key, value in row.items()}


def row_sha256(row: Mapping[str, Any]) -> str:
    return sha256_json(row_payload(row))


def plan_sha256(*, recovery_version: str, payload: Any) -> str:
    return sha256_json({"recovery_version": recovery_version, "payload": payload})


def make_evidence_record(
    *,
    recovery_version: str,
    source_table: str,
    source_row_id: int,
    action: str,
    business_key: Mapping[str, Any],
    source_row: Mapping[str, Any],
    plan_payload: Mapping[str, Any],
    plan_hash: str | None = None,
) -> dict[str, Any]:
    source = row_payload(source_row)
    source_hash = sha256_json(source)
    business_json = canonical_json(dict(business_key))
    plan_payload_json = canonical_json(dict(plan_payload))
    computed_plan_hash = plan_sha256(
        recovery_version=recovery_version,
        payload=json.loads(plan_payload_json),
    )
    if plan_hash is not None and plan_hash != computed_plan_hash:
        raise RuntimeError("schema recovery plan hash differs from payload")
    key_payload = {
        "recovery_version": recovery_version,
        "source_table": source_table,
        "source_row_id": int(source_row_id),
        "action": action,
        "business_key_sha256": hashlib.sha256(
            business_json.encode("utf-8")
        ).hexdigest(),
        "source_row_sha256": source_hash,
        "plan_sha256": computed_plan_hash,
    }
    record = {
        "recovery_key": sha256_json(key_payload),
        "recovery_version": recovery_version,
        "source_table": source_table,
        "source_row_id": int(source_row_id),
        "action": action,
        "business_key_json": business_json,
        "source_row_json": canonical_json(source),
        "source_row_sha256": source_hash,
        "plan_payload_json": plan_payload_json,
        "plan_sha256": computed_plan_hash,
    }
    return _strict_evidence_record(record)


def _parse_canonical_json(value: Any, *, field: str) -> Any:
    if type(value) is not str:
        raise RuntimeError(f"schema recovery {field} must be canonical JSON")
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"schema recovery {field} must be canonical JSON"
        ) from exc
    if canonical_json(parsed) != value:
        raise RuntimeError(f"schema recovery {field} is not canonical JSON")
    return parsed


def _strict_evidence_record(record: Mapping[str, Any]) -> dict[str, Any]:
    if set(record) != set(_LOGICAL_FIELDS):
        raise RuntimeError("schema recovery evidence fields differ")
    normalized = {field: record[field] for field in _LOGICAL_FIELDS}
    recovery_version = normalized["recovery_version"]
    source_table = normalized["source_table"]
    action = normalized["action"]
    if (
        type(recovery_version) is not str
        or not recovery_version
        or len(recovery_version) > 80
        or type(source_table) is not str
        or _IDENTIFIER.fullmatch(source_table) is None
        or len(source_table) > 64
        or type(action) is not str
        or not action
        or len(action) > 32
        or type(normalized["source_row_id"]) is not int
        or int(normalized["source_row_id"]) < 0
    ):
        raise RuntimeError("schema recovery evidence identity differs")
    business = _parse_canonical_json(
        normalized["business_key_json"], field="business_key_json"
    )
    source = _parse_canonical_json(
        normalized["source_row_json"], field="source_row_json"
    )
    plan_payload = _parse_canonical_json(
        normalized["plan_payload_json"], field="plan_payload_json"
    )
    if not isinstance(business, dict) or not isinstance(source, dict) or not isinstance(
        plan_payload, dict
    ):
        raise RuntimeError("schema recovery evidence JSON shape differs")
    source_hash = sha256_json(source)
    computed_plan_hash = plan_sha256(
        recovery_version=recovery_version,
        payload=plan_payload,
    )
    expected_key = sha256_json({
        "recovery_version": recovery_version,
        "source_table": source_table,
        "source_row_id": int(normalized["source_row_id"]),
        "action": action,
        "business_key_sha256": hashlib.sha256(
            str(normalized["business_key_json"]).encode("utf-8")
        ).hexdigest(),
        "source_row_sha256": source_hash,
        "plan_sha256": computed_plan_hash,
    })
    if (
        normalized["source_row_sha256"] != source_hash
        or normalized["plan_sha256"] != computed_plan_hash
        or normalized["recovery_key"] != expected_key
        or _HASH.fullmatch(str(normalized["source_row_sha256"] or "")) is None
        or _HASH.fullmatch(str(normalized["plan_sha256"] or "")) is None
        or _HASH.fullmatch(str(normalized["recovery_key"] or "")) is None
    ):
        raise RuntimeError("schema recovery evidence cryptographic fields differ")
    return normalized


_EXPECTED_COLUMNS = {
    "event_id": ("bigint", "NO", None, None, None, ("auto_increment",)),
    "recovery_key": ("char(64)", "NO", None, "ascii", "ascii_bin", ()),
    "recovery_version": (
        "varchar(80)", "NO", None, "utf8mb4", EXPECTED_COLLATION, (),
    ),
    "source_table": (
        "varchar(64)", "NO", None, "utf8mb4", EXPECTED_COLLATION, (),
    ),
    "source_row_id": ("bigint", "NO", None, None, None, ()),
    "action": (
        "varchar(32)", "NO", None, "utf8mb4", EXPECTED_COLLATION, (),
    ),
    "business_key_json": (
        "longtext", "NO", None, "utf8mb4", EXPECTED_COLLATION, (),
    ),
    "source_row_json": (
        "longtext", "NO", None, "utf8mb4", EXPECTED_COLLATION, (),
    ),
    "source_row_sha256": (
        "char(64)", "NO", None, "ascii", "ascii_bin", (),
    ),
    "plan_payload_json": (
        "longtext", "NO", None, "utf8mb4", EXPECTED_COLLATION, (),
    ),
    "plan_sha256": ("char(64)", "NO", None, "ascii", "ascii_bin", ()),
    "created_at": (
        "datetime(6)", "NO", "current_timestamp(6)", None, None, (),
    ),
}
_EXPECTED_INDEXES = {
    "PRIMARY": (True, ("event_id",)),
    "uk_schema_recovery_key": (True, ("recovery_key",)),
    "idx_schema_recovery_source": (
        False, ("source_table", "source_row_id"),
    ),
    "idx_schema_recovery_plan": (
        False, ("recovery_version", "plan_sha256"),
    ),
}


def _normalize_default(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().strip("'").casefold()
    if normalized in {"current_timestamp", "current_timestamp()"}:
        return "current_timestamp"
    return normalized


def _evidence_column_inventory(connection) -> dict[str, tuple[Any, ...]]:
    rows = connection.execute(text(
        "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA, "
        "CHARACTER_SET_NAME, COLLATION_NAME FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:table_name "
        "ORDER BY ORDINAL_POSITION"
    ), {"table_name": EVIDENCE_TABLE}).mappings().all()
    return {
        str(row.get("COLUMN_NAME") or row.get("column_name") or ""): (
            str(row.get("COLUMN_TYPE") or row.get("column_type") or "").casefold(),
            str(row.get("IS_NULLABLE") or row.get("is_nullable") or "").upper(),
            _normalize_default(
                row.get("COLUMN_DEFAULT")
                if "COLUMN_DEFAULT" in row else row.get("column_default")
            ),
            (
                str(row.get("CHARACTER_SET_NAME") or row.get("character_set_name") or "")
                .casefold() or None
            ),
            (
                str(row.get("COLLATION_NAME") or row.get("collation_name") or "")
                .casefold() or None
            ),
            tuple(sorted(
                token for token in ("auto_increment",)
                if token in str(row.get("EXTRA") or row.get("extra") or "").casefold()
            )),
        )
        for row in rows
    }


def _normalized_trigger_body(value: Any) -> str:
    normalized = str(value or "").replace("`", "")
    normalized = re.sub(r"\s+", " ", normalized).strip().casefold()
    normalized = re.sub(r"\s*;\s*", ";", normalized)
    normalized = re.sub(r"\s*=\s*", "=", normalized)
    return normalized


def _expected_trigger_body(statement: str) -> str:
    marker = re.search(r"\bFOR\s+EACH\s+ROW\b", statement, re.IGNORECASE)
    if marker is None:
        raise RuntimeError("schema recovery trigger source cannot be parsed")
    return statement[marker.end():].strip()


def validate_recovery_evidence_schema(
    engine,
    *,
    connection=None,
    require_triggers: bool = True,
) -> dict[str, Any]:
    """Validate the exact evidence table and its broker-owned immutable guards."""

    owns_connection = connection is None
    bound = connection or engine.connect()
    try:
        table = bound.execute(text(
            "SELECT ENGINE, TABLE_COLLATION FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:table_name"
        ), {"table_name": EVIDENCE_TABLE}).mappings().one_or_none()
        if table is None:
            raise RuntimeError("schema recovery evidence table is unavailable")
        engine_name = str(table.get("ENGINE") or table.get("engine") or "")
        collation = str(
            table.get("TABLE_COLLATION") or table.get("table_collation") or ""
        )
        if engine_name.casefold() != EXPECTED_ENGINE.casefold() or (
            collation.casefold() != EXPECTED_COLLATION.casefold()
        ):
            raise RuntimeError("schema recovery evidence table storage differs")
        columns = _evidence_column_inventory(bound)
        if columns != _EXPECTED_COLUMNS:
            raise RuntimeError("schema recovery evidence columns differ")

        index_rows = bound.execute(text(
            "SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME "
            "FROM information_schema.STATISTICS "
            "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:table_name "
            "ORDER BY INDEX_NAME, SEQ_IN_INDEX"
        ), {"table_name": EVIDENCE_TABLE}).mappings().all()
        grouped: dict[str, dict[str, Any]] = {}
        for row in index_rows:
            name = str(row.get("INDEX_NAME") or row.get("index_name") or "")
            item = grouped.setdefault(name, {"unique": True, "columns": []})
            non_unique = row.get("NON_UNIQUE")
            if non_unique is None:
                non_unique = row.get("non_unique")
            item["unique"] = int(non_unique or 0) == 0
            sequence = row.get("SEQ_IN_INDEX")
            if sequence is None:
                sequence = row.get("seq_in_index")
            item["columns"].append((
                int(sequence or 0),
                str(row.get("COLUMN_NAME") or row.get("column_name") or ""),
            ))
        indexes = {
            name: (
                bool(item["unique"]),
                tuple(column for _sequence, column in sorted(item["columns"])),
            )
            for name, item in grouped.items()
        }
        if indexes != _EXPECTED_INDEXES:
            raise RuntimeError("schema recovery evidence indexes differ")

        trigger_rows = bound.execute(text(
            "SELECT TRIGGER_NAME, DEFINER, ACTION_TIMING, EVENT_MANIPULATION, "
            "EVENT_OBJECT_TABLE, ACTION_ORIENTATION, ACTION_STATEMENT, SQL_MODE, "
            "CHARACTER_SET_CLIENT, COLLATION_CONNECTION, DATABASE_COLLATION "
            "FROM information_schema.TRIGGERS "
            "WHERE TRIGGER_SCHEMA=DATABASE() AND EVENT_OBJECT_TABLE=:table_name "
            "ORDER BY BINARY TRIGGER_NAME"
        ), {"table_name": EVIDENCE_TABLE}).mappings().all()
        observed_names = {
            str(row.get("TRIGGER_NAME") or row.get("trigger_name") or "")
            for row in trigger_rows
        }
        if require_triggers and observed_names != set(TRIGGER_STATEMENTS):
            raise RuntimeError("schema recovery evidence triggers differ")
        if not require_triggers and observed_names - set(TRIGGER_STATEMENTS):
            raise RuntimeError("schema recovery evidence has unexpected triggers")
        for row in trigger_rows:
            name = str(row.get("TRIGGER_NAME") or row.get("trigger_name") or "")
            expected_event = "UPDATE" if name.endswith("_bu") else "DELETE"
            def value(upper: str) -> Any:
                return row.get(upper) if upper in row else row.get(upper.casefold())
            if (
                name not in TRIGGER_STATEMENTS
                or str(value("DEFINER") or "") != EXPECTED_TRIGGER_DEFINER
                or str(value("ACTION_TIMING") or "").upper() != "BEFORE"
                or str(value("EVENT_MANIPULATION") or "").upper() != expected_event
                or str(value("EVENT_OBJECT_TABLE") or "") != EVIDENCE_TABLE
                or str(value("ACTION_ORIENTATION") or "").upper() != "ROW"
                or _normalized_trigger_body(value("ACTION_STATEMENT"))
                != _normalized_trigger_body(
                    _expected_trigger_body(TRIGGER_STATEMENTS[name])
                )
                or str(value("SQL_MODE") or "") != EXPECTED_TRIGGER_SQL_MODE
                or str(value("CHARACTER_SET_CLIENT") or "")
                != EXPECTED_CHARACTER_SET_CLIENT
                or str(value("COLLATION_CONNECTION") or "")
                != EXPECTED_COLLATION_CONNECTION
                or str(value("DATABASE_COLLATION") or "") != EXPECTED_COLLATION
            ):
                raise RuntimeError(
                    "schema recovery evidence trigger definition differs"
                )
        return {
            "table": EVIDENCE_TABLE,
            "trigger_names": sorted(observed_names),
            "trigger_count": len(observed_names),
            "append_only_verified": require_triggers,
            "physical_contract_verified": True,
            "read_only": True,
        }
    finally:
        if owns_connection:
            bound.close()


def ensure_evidence_table(
    connection,
    *,
    require_triggers: bool = True,
) -> None:
    """Create/verify the sink; trigger creation remains broker-controlled."""

    connection.execute(text(EVIDENCE_TABLE_DDL))
    columns = _evidence_column_inventory(connection)
    if "plan_payload_json" not in columns:
        row_count = int(connection.execute(text(
            f"SELECT COUNT(*) FROM `{EVIDENCE_TABLE}`"
        )).scalar() or 0)
        if row_count:
            raise RuntimeError(
                "legacy schema recovery evidence cannot reconstruct plan payload"
            )
        connection.execute(text(
            f"ALTER TABLE `{EVIDENCE_TABLE}` ADD COLUMN `plan_payload_json` "
            "LONGTEXT NOT NULL AFTER `source_row_sha256`"
        ))
    validate_recovery_evidence_schema(
        None,
        connection=connection,
        require_triggers=require_triggers,
    )


def persist_and_verify_evidence(
    connection,
    records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Persist idempotent records and prove the stored hashes before mutation."""

    if not records:
        return {"evidence_row_count": 0, "evidence_verified": True}
    ensure_evidence_table(connection)
    expected: dict[str, dict[str, Any]] = {}
    for candidate in records:
        record = _strict_evidence_record(candidate)
        recovery_key = str(record["recovery_key"])
        existing = expected.get(recovery_key)
        if existing is not None and existing != record:
            raise RuntimeError("duplicate schema recovery key has different evidence")
        expected[recovery_key] = record

    statement = text(
        f"INSERT IGNORE INTO `{EVIDENCE_TABLE}` ("
        "recovery_key, recovery_version, source_table, source_row_id, action, "
        "business_key_json, source_row_json, source_row_sha256, "
        "plan_payload_json, plan_sha256"
        ") VALUES ("
        ":recovery_key, :recovery_version, :source_table, :source_row_id, "
        ":action, :business_key_json, :source_row_json, :source_row_sha256, "
        ":plan_payload_json, :plan_sha256)"
    )
    for record in expected.values():
        connection.execute(statement, record)

    lookup = text(
        f"SELECT event_id, created_at, {', '.join(_LOGICAL_FIELDS)} "
        f"FROM `{EVIDENCE_TABLE}` WHERE recovery_key=:recovery_key"
    )
    for recovery_key, expected_record in expected.items():
        row = connection.execute(
            lookup, {"recovery_key": recovery_key}
        ).mappings().one_or_none()
        if row is None:
            raise RuntimeError("schema recovery evidence row is unavailable")
        if int(row.get("event_id") or 0) <= 0 or row.get("created_at") is None:
            raise RuntimeError("schema recovery evidence storage identity differs")
        stored_record = _strict_evidence_record({
            field: row.get(field) for field in _LOGICAL_FIELDS
        })
        if stored_record != expected_record:
            raise RuntimeError("schema recovery evidence row differs from plan")
    return {
        "evidence_row_count": len(expected),
        "evidence_verified": True,
        "evidence_table": EVIDENCE_TABLE,
    }


def load_pending_physical_rewrite_plan(
    connection,
    *,
    recovery_version: str,
    source_table: str,
) -> dict[str, Any] | None:
    """Return one durable unverified PLAN, rejecting an ambiguous journal."""

    rows = connection.execute(text(
        f"SELECT event_id, created_at, {', '.join(_LOGICAL_FIELDS)} "
        f"FROM `{EVIDENCE_TABLE}` "
        "WHERE recovery_version=:recovery_version "
        "AND source_table=:source_table "
        "AND action IN (:plan_action, :verified_action) "
        "ORDER BY event_id"
    ), {
        "recovery_version": recovery_version,
        "source_table": source_table,
        "plan_action": PLAN_ACTION,
        "verified_action": VERIFIED_ACTION,
    }).mappings().all()
    grouped: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for row in rows:
        if int(row.get("event_id") or 0) <= 0 or row.get("created_at") is None:
            raise RuntimeError("schema recovery evidence storage identity differs")
        record = _strict_evidence_record({
            field: row.get(field) for field in _LOGICAL_FIELDS
        })
        if (
            record["recovery_version"] != recovery_version
            or record["source_table"] != source_table
        ):
            raise RuntimeError("schema recovery evidence query boundary differs")
        plan_group = grouped.setdefault(
            str(record["plan_sha256"]),
            {PLAN_ACTION: [], VERIFIED_ACTION: []},
        )
        plan_group[str(record["action"])].append(record)

    pending: list[dict[str, Any]] = []
    for plan_hash, actions in grouped.items():
        plans = actions[PLAN_ACTION]
        verified = actions[VERIFIED_ACTION]
        if len(plans) != 1 or len(verified) > 1:
            raise RuntimeError(
                "schema recovery physical journal is ambiguous: " + plan_hash
            )
        if verified and any(
            verified[0][field] != plans[0][field]
            for field in (
                "recovery_version",
                "source_table",
                "source_row_id",
                "business_key_json",
                "plan_payload_json",
                "plan_sha256",
            )
        ):
            raise RuntimeError(
                "schema recovery physical PLAN/VERIFIED identity differs"
            )
        if not verified:
            pending.append(plans[0])
    if len(pending) > 1:
        raise RuntimeError("multiple unverified physical recovery plans exist")
    if not pending:
        return None
    record = pending[0]
    return {
        "record": record,
        "business_key": json.loads(record["business_key_json"]),
        "plan_sha256": record["plan_sha256"],
        "source_row": json.loads(record["source_row_json"]),
        "plan_payload": json.loads(record["plan_payload_json"]),
    }


def verify_pending_plan_content(
    connection,
    pending: Mapping[str, Any],
) -> dict[str, Any]:
    """Prove current content still equals the PLAN's durable fingerprint."""

    record = pending.get("record")
    source_row = pending.get("source_row")
    if not isinstance(record, Mapping) or not isinstance(source_row, Mapping):
        raise RuntimeError("schema recovery pending plan shape differs")
    before = source_row.get("before_fingerprint")
    fingerprint_columns = source_row.get("fingerprint_columns")
    if (
        not isinstance(before, Mapping)
        or set(before) != {"row_count", "content_sha256"}
        or type(before.get("row_count")) is not int
        or int(before["row_count"]) < 0
        or _HASH.fullmatch(str(before.get("content_sha256") or "")) is None
        or not isinstance(fingerprint_columns, list)
        or not all(type(column) is str for column in fingerprint_columns)
        or len(set(fingerprint_columns)) != len(fingerprint_columns)
        or bool(before.get("row_count")) and not fingerprint_columns
    ):
        raise RuntimeError("schema recovery PLAN fingerprint manifest differs")
    current = table_content_fingerprint(
        connection,
        str(record["source_table"]),
        columns=(fingerprint_columns or None),
    )
    expected = {
        "row_count": int(before["row_count"]),
        "content_sha256": str(before["content_sha256"]),
    }
    if current != expected:
        raise RuntimeError(
            "schema recovery source content changed after physical PLAN"
        )
    return current


def _safe_identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"unsafe SQL identifier: {value!r}")
    return value


def table_content_fingerprint(
    connection,
    table_name: str,
    *,
    order_by: Iterable[str] = ("id",),
    columns: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Hash every selected row in deterministic order without changing data."""

    table = _safe_identifier(table_name)
    ordered = tuple(_safe_identifier(column) for column in order_by)
    if not ordered:
        raise ValueError("table fingerprint requires a deterministic order")
    selected = (
        tuple(_safe_identifier(column) for column in columns)
        if columns is not None else ()
    )
    select_sql = (
        ", ".join(f"`{column}`" for column in selected)
        if selected else "*"
    )
    order_sql = ", ".join(f"`{column}`" for column in ordered)
    result = connection.execute(
        text(f"SELECT {select_sql} FROM `{table}` ORDER BY {order_sql}")
    ).mappings()
    digest = hashlib.sha256()
    row_count = 0
    for row in result:
        encoded = canonical_json(row_payload(row)).encode("utf-8")
        digest.update(len(encoded).to_bytes(8, byteorder="big", signed=False))
        digest.update(encoded)
        row_count += 1
    return {"row_count": row_count, "content_sha256": digest.hexdigest()}


__all__ = [
    "EVIDENCE_TABLE",
    "EVIDENCE_TABLE_DDL",
    "PLAN_ACTION",
    "TRIGGER_STATEMENTS",
    "VERIFIED_ACTION",
    "canonical_json",
    "ensure_evidence_table",
    "load_pending_physical_rewrite_plan",
    "make_evidence_record",
    "persist_and_verify_evidence",
    "plan_sha256",
    "row_payload",
    "row_sha256",
    "sha256_json",
    "table_content_fingerprint",
    "validate_recovery_evidence_schema",
    "verify_pending_plan_content",
]
