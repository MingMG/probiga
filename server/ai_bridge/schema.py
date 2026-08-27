# -*- coding: utf-8 -*-
from __future__ import annotations

from threading import Lock
from weakref import WeakKeyDictionary

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    inspect,
    text,
)
from sqlalchemy.engine import Engine

from server.common.schema_recovery_evidence import (
    ensure_evidence_table,
    load_pending_physical_rewrite_plan,
    make_evidence_record,
    persist_and_verify_evidence,
    plan_sha256,
    table_content_fingerprint,
    verify_pending_plan_content,
)

metadata = MetaData()
BIGINT_PK = BigInteger().with_variant(Integer, "sqlite")

ai_bridge_job = Table(
    "st_ai_bridge_job",
    metadata,
    Column("id", BIGINT_PK, primary_key=True, autoincrement=True),
    Column("request_uid", String(36), nullable=False, unique=True),
    Column("owner_user_id", Integer, nullable=False),
    Column("channel", String(16), nullable=False),
    Column("question", Text, nullable=False),
    Column("answer", Text, nullable=True),
    Column("status", String(20), nullable=False),
    Column("provider_attempt", String(32), nullable=True),
    Column("source", String(32), nullable=True),
    Column("source_label", String(80), nullable=True),
    Column("error_message", String(1000), nullable=True),
    Column("worker_id", String(120), nullable=True),
    Column("attempts", Integer, nullable=False, default=0),
    Column("lease_expires_at", DateTime, nullable=True),
    Column("created_at", DateTime, nullable=False),
    Column("started_at", DateTime, nullable=True),
    Column("completed_at", DateTime, nullable=True),
    Column("updated_at", DateTime, nullable=False),
    mysql_charset="utf8mb4",
    mysql_collate="utf8mb4_unicode_ci",
    mysql_engine="InnoDB",
)
Index("ix_st_ai_bridge_job_owner_created", ai_bridge_job.c.owner_user_id, ai_bridge_job.c.created_at)
Index("ix_st_ai_bridge_job_queue", ai_bridge_job.c.status, ai_bridge_job.c.created_at)

_schema_lock = Lock()
_initialized_engines: WeakKeyDictionary[Engine, bool] = WeakKeyDictionary()


_AI_BRIDGE_COLUMN_CONTRACT = {
    "id": ("bigint", "NO", "auto_increment"),
    "request_uid": ("varchar(36)", "NO", ""),
    "owner_user_id": ("int", "NO", ""),
    "channel": ("varchar(16)", "NO", ""),
    "question": ("text", "NO", ""),
    "answer": ("text", "YES", ""),
    "status": ("varchar(20)", "NO", ""),
    "provider_attempt": ("varchar(32)", "YES", ""),
    "source": ("varchar(32)", "YES", ""),
    "source_label": ("varchar(80)", "YES", ""),
    "error_message": ("varchar(1000)", "YES", ""),
    "worker_id": ("varchar(120)", "YES", ""),
    "attempts": ("int", "NO", ""),
    "lease_expires_at": ("datetime", "YES", ""),
    "created_at": ("datetime", "NO", ""),
    "started_at": ("datetime", "YES", ""),
    "completed_at": ("datetime", "YES", ""),
    "updated_at": ("datetime", "NO", ""),
}
_AI_BRIDGE_INDEX_CONTRACT = {
    (True, ("id",)),
    (True, ("request_uid",)),
    (False, ("owner_user_id", "created_at")),
    (False, ("status", "created_at")),
}
_AI_BRIDGE_TABLE = "st_ai_bridge_job"
_AI_BRIDGE_COLLATION = "utf8mb4_unicode_ci"
_AI_BRIDGE_LEGACY_COLLATION = "utf8mb4_general_ci"
_AI_BRIDGE_RECOVERY_VERSION = "ai-bridge-physical-normalization.v1"
_AI_BRIDGE_TARGET_CONTRACT_VERSION = "ai-bridge-target-contract.v1"
_AI_BRIDGE_CONVERT_ACTION = "CONVERT_COLLATION:utf8mb4_unicode_ci"
_AI_BRIDGE_CREATE_ACTION = "CREATE_TARGET_TABLE"


def _index_shapes(inspector) -> set[tuple[bool, tuple[str, ...]]]:
    shapes: set[tuple[bool, tuple[str, ...]]] = set()
    primary = inspector.get_pk_constraint("st_ai_bridge_job").get("constrained_columns") or []
    if primary:
        shapes.add((True, tuple(str(value) for value in primary)))
    for item in inspector.get_unique_constraints("st_ai_bridge_job"):
        columns = item.get("column_names") or []
        if columns:
            shapes.add((True, tuple(str(value) for value in columns)))
    for item in inspector.get_indexes("st_ai_bridge_job"):
        columns = item.get("column_names") or []
        if columns:
            shapes.add((bool(item.get("unique")), tuple(str(value) for value in columns)))
    return shapes


def _ai_bridge_target_contract_sha256() -> str:
    columns = []
    for ordinal, (name, (column_type, nullable, extra)) in enumerate(
        _AI_BRIDGE_COLUMN_CONTRACT.items(),
        1,
    ):
        character = column_type.split("(", 1)[0] in {"varchar", "text"}
        columns.append({
            "name": name,
            "ordinal_position": ordinal,
            "column_type": column_type,
            "is_nullable": nullable,
            "column_default": None,
            "extra": extra,
            "character_set_name": "utf8mb4" if character else None,
            "collation_name": _AI_BRIDGE_COLLATION if character else None,
        })
    indexes = [
        {"unique": unique, "columns": list(index_columns)}
        for unique, index_columns in sorted(_AI_BRIDGE_INDEX_CONTRACT)
    ]
    return plan_sha256(
        recovery_version=_AI_BRIDGE_TARGET_CONTRACT_VERSION,
        payload={
            "table": _AI_BRIDGE_TABLE,
            "engine": "InnoDB",
            "table_collation": _AI_BRIDGE_COLLATION,
            "columns": columns,
            "indexes": indexes,
        },
    )


def _validate_pending_ai_bridge_plan(pending) -> None:
    record = pending.get("record")
    if (
        not isinstance(record, dict)
        or pending.get("business_key") != {"table": _AI_BRIDGE_TABLE}
        or int(record.get("source_row_id", -1)) != 0
    ):
        raise RuntimeError("AI bridge pending physical PLAN identity differs")
    manifest = pending.get("source_row")
    payload = pending.get("plan_payload")
    if not isinstance(manifest, dict) or not isinstance(payload, dict):
        raise RuntimeError("AI bridge pending physical PLAN shape differs")
    if set(manifest) != {
        "before_fingerprint",
        "fingerprint_columns",
        "source_collation",
        "target_contract_sha256",
        "allowed_actions",
    } or payload != {"table": _AI_BRIDGE_TABLE, **manifest}:
        raise RuntimeError("AI bridge pending physical PLAN manifest differs")
    before = manifest.get("before_fingerprint")
    if (
        not isinstance(before, dict)
        or type(before.get("row_count")) is not int
        or int(before["row_count"]) < 0
        or manifest.get("fingerprint_columns")
        != list(_AI_BRIDGE_COLUMN_CONTRACT)
        or manifest.get("source_collation") != _AI_BRIDGE_LEGACY_COLLATION
        or manifest.get("target_contract_sha256")
        != _ai_bridge_target_contract_sha256()
        or manifest.get("allowed_actions") != [_AI_BRIDGE_CONVERT_ACTION]
    ):
        raise RuntimeError("AI bridge pending physical PLAN contract differs")


def _mysql_table_collation(connection) -> str:
    table_row = connection.execute(text(
        "SELECT ENGINE,TABLE_COLLATION FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='st_ai_bridge_job'"
    )).mappings().one_or_none()
    if table_row is None:
        raise RuntimeError("AI bridge physical table is missing")
    engine_name = str(
        table_row.get("ENGINE") or table_row.get("engine") or ""
    )
    collation = str(
        table_row.get("TABLE_COLLATION")
        or table_row.get("table_collation")
        or ""
    ).casefold()
    if engine_name.casefold() != "innodb":
        raise RuntimeError("AI bridge table engine differs")
    return collation


def _validate_mysql_physical_contract(
    connection,
    *,
    expected_collation: str,
) -> dict[str, tuple[object, ...]]:
    collation = _mysql_table_collation(connection)
    if collation != expected_collation:
        raise RuntimeError("AI bridge table collation differs")
    rows = connection.execute(text(
        "SELECT COLUMN_NAME,ORDINAL_POSITION,COLUMN_TYPE,IS_NULLABLE,"
        "COLUMN_DEFAULT,EXTRA,"
        "CHARACTER_SET_NAME,COLLATION_NAME FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='st_ai_bridge_job'"
    )).mappings().all()
    actual: dict[str, tuple[object, ...]] = {}
    for row in rows:
        get = lambda key: row.get(key) if key in row else row.get(key.casefold())
        name = str(get("COLUMN_NAME") or "")
        actual[name] = (
            int(get("ORDINAL_POSITION") or 0),
            str(get("COLUMN_TYPE") or "").casefold(),
            str(get("IS_NULLABLE") or "").upper(),
            get("COLUMN_DEFAULT"),
            str(get("EXTRA") or "")
            .casefold()
            .replace("default_generated", "")
            .strip(),
            str(get("CHARACTER_SET_NAME") or "").casefold() or None,
            str(get("COLLATION_NAME") or "").casefold() or None,
        )
    if set(actual) != set(_AI_BRIDGE_COLUMN_CONTRACT):
        raise RuntimeError("AI bridge physical columns differ")
    for ordinal, (name, (column_type, nullable, extra)) in enumerate(
        _AI_BRIDGE_COLUMN_CONTRACT.items(), 1
    ):
        character = column_type.split("(", 1)[0] in {"varchar", "text"}
        expected = (
            ordinal,
            column_type,
            nullable,
            None,
            extra,
            "utf8mb4" if character else None,
            expected_collation if character else None,
        )
        if actual[name] != expected:
            raise RuntimeError(f"AI bridge physical column differs: {name}")
    return actual


def _request_uid_has_target_collation_duplicates(connection) -> bool:
    return connection.execute(text(
        "SELECT 1 FROM `st_ai_bridge_job` "
        "GROUP BY (`request_uid` COLLATE utf8mb4_unicode_ci) "
        "HAVING COUNT(*)>1 LIMIT 1"
    )).first() is not None


def plan_ai_bridge_recovery(engine: Engine) -> dict[str, object]:
    """Build a read-only plan for the one supported legacy collation."""

    inspector = inspect(engine)
    table_exists = _AI_BRIDGE_TABLE in set(inspector.get_table_names())
    target_contract_sha256 = _ai_bridge_target_contract_sha256()
    state = "MISSING"
    safe = True
    rewrite_required = True
    allowed_actions: list[str] = (
        [] if table_exists else [_AI_BRIDGE_CREATE_ACTION]
    )
    before_fingerprint = None
    fingerprint_columns: list[str] = []
    blocked_reason = None
    if table_exists and engine.dialect.name != "mysql":
        try:
            validate_ai_bridge_runtime_schema(engine)
            state = "TARGET"
            rewrite_required = False
        except RuntimeError as exc:
            state = "UNSUPPORTED"
            safe = False
            blocked_reason = str(exc)
    elif table_exists:
        if _index_shapes(inspector) != _AI_BRIDGE_INDEX_CONTRACT:
            state = "UNSUPPORTED"
            safe = False
            blocked_reason = "AI bridge job indexes differ"
        else:
            with engine.connect() as connection:
                try:
                    observed_collation = _mysql_table_collation(connection)
                    if observed_collation == _AI_BRIDGE_COLLATION:
                        _validate_mysql_physical_contract(
                            connection,
                            expected_collation=_AI_BRIDGE_COLLATION,
                        )
                        state = "TARGET"
                        rewrite_required = False
                    elif observed_collation == _AI_BRIDGE_LEGACY_COLLATION:
                        _validate_mysql_physical_contract(
                            connection,
                            expected_collation=_AI_BRIDGE_LEGACY_COLLATION,
                        )
                        if _request_uid_has_target_collation_duplicates(connection):
                            raise RuntimeError(
                                "AI bridge request_uid collides under target "
                                "collation"
                            )
                        state = "LEGACY_GENERAL_CI"
                        allowed_actions = [_AI_BRIDGE_CONVERT_ACTION]
                        fingerprint_columns = list(_AI_BRIDGE_COLUMN_CONTRACT)
                        before_fingerprint = table_content_fingerprint(
                            connection,
                            _AI_BRIDGE_TABLE,
                            columns=fingerprint_columns,
                        )
                    else:
                        raise RuntimeError("AI bridge table collation differs")
                except RuntimeError as exc:
                    state = "UNSUPPORTED"
                    safe = False
                    blocked_reason = str(exc)
    plan_payload = {
        "table": _AI_BRIDGE_TABLE,
        "state": state,
        "rewrite_required": rewrite_required,
        "target_contract_sha256": target_contract_sha256,
        "allowed_actions": allowed_actions,
        "before_fingerprint": before_fingerprint,
        "fingerprint_columns": fingerprint_columns,
        "safe_automatic_rewrite": safe,
        "blocked_reason": blocked_reason,
    }
    return {
        "schema": "probiga.ai-bridge-recovery-plan.v1",
        "recovery_version": _AI_BRIDGE_RECOVERY_VERSION,
        "table_exists": table_exists,
        **plan_payload,
        "ready_for_privileged_apply": safe,
        "plan_sha256": plan_sha256(
            recovery_version=_AI_BRIDGE_RECOVERY_VERSION,
            payload=plan_payload,
        ),
        "read_only": True,
    }


def validate_ai_bridge_runtime_schema(engine: Engine) -> dict[str, object]:
    """Read-only worker queue schema validation."""
    inspector = inspect(engine)
    if "st_ai_bridge_job" not in set(inspector.get_table_names()):
        raise RuntimeError("AI bridge job table is missing")
    columns = {str(item["name"]) for item in inspector.get_columns("st_ai_bridge_job")}
    if columns != set(_AI_BRIDGE_COLUMN_CONTRACT):
        raise RuntimeError("AI bridge job columns differ")
    if _index_shapes(inspector) != _AI_BRIDGE_INDEX_CONTRACT:
        raise RuntimeError("AI bridge job indexes differ")
    if engine.dialect.name == "mysql":
        with engine.connect() as connection:
            _validate_mysql_physical_contract(
                connection,
                expected_collation=_AI_BRIDGE_COLLATION,
            )
    return {
        "schema": "probiga.ai-bridge-physical-contract.v1",
        "status": "HEALTHY",
        "table_count": 1,
        "physical_schema_verified": True,
        "runtime_ddl_required": False,
        "read_only": True,
    }


def privileged_migrate_ai_bridge_schema(engine: Engine) -> dict[str, object]:
    """Create the queue table in a writer-fenced release window."""
    metadata.create_all(engine, checkfirst=True)
    rewrite_evidence = None
    if engine.dialect.name == "mysql":
        inspector = inspect(engine)
        if _index_shapes(inspector) != _AI_BRIDGE_INDEX_CONTRACT:
            raise RuntimeError("AI bridge job indexes differ")
        with engine.begin() as connection:
            ensure_evidence_table(connection)
            pending = load_pending_physical_rewrite_plan(
                connection,
                recovery_version=_AI_BRIDGE_RECOVERY_VERSION,
                source_table=_AI_BRIDGE_TABLE,
            )
            context = None
            resumed_pending_plan = pending is not None
            if pending is not None:
                _validate_pending_ai_bridge_plan(pending)
                verify_pending_plan_content(connection, pending)
                context = pending
            observed_collation = _mysql_table_collation(connection)
            if observed_collation == _AI_BRIDGE_LEGACY_COLLATION:
                _validate_mysql_physical_contract(
                    connection,
                    expected_collation=_AI_BRIDGE_LEGACY_COLLATION,
                )
                if _request_uid_has_target_collation_duplicates(connection):
                    raise RuntimeError(
                        "AI bridge request_uid collides under target collation"
                    )
                fingerprint_columns = list(_AI_BRIDGE_COLUMN_CONTRACT)
                if context is None:
                    before = table_content_fingerprint(
                        connection,
                        _AI_BRIDGE_TABLE,
                        columns=fingerprint_columns,
                    )
                    manifest = {
                        "before_fingerprint": before,
                        "fingerprint_columns": fingerprint_columns,
                        "source_collation": _AI_BRIDGE_LEGACY_COLLATION,
                        "target_contract_sha256": (
                            _ai_bridge_target_contract_sha256()
                        ),
                        "allowed_actions": [_AI_BRIDGE_CONVERT_ACTION],
                    }
                    physical_plan_payload = {
                        "table": _AI_BRIDGE_TABLE,
                        **manifest,
                    }
                    physical_plan_hash = plan_sha256(
                        recovery_version=_AI_BRIDGE_RECOVERY_VERSION,
                        payload=physical_plan_payload,
                    )
                    plan_record = make_evidence_record(
                        recovery_version=_AI_BRIDGE_RECOVERY_VERSION,
                        source_table=_AI_BRIDGE_TABLE,
                        source_row_id=0,
                        action="PHYSICAL_REWRITE_PLAN",
                        business_key={"table": _AI_BRIDGE_TABLE},
                        source_row=manifest,
                        plan_payload=physical_plan_payload,
                        plan_hash=physical_plan_hash,
                    )
                    context = {
                        "record": plan_record,
                        "business_key": {"table": _AI_BRIDGE_TABLE},
                        "source_row": manifest,
                        "plan_payload": physical_plan_payload,
                        "plan_sha256": physical_plan_hash,
                    }
                    _validate_pending_ai_bridge_plan(context)
                    persist_and_verify_evidence(connection, [plan_record])
                connection.execute(text(
                    "ALTER TABLE `st_ai_bridge_job` "
                    "CONVERT TO CHARACTER SET utf8mb4 "
                    "COLLATE utf8mb4_unicode_ci"
                ))
            elif observed_collation == _AI_BRIDGE_COLLATION:
                _validate_mysql_physical_contract(
                    connection,
                    expected_collation=_AI_BRIDGE_COLLATION,
                )
            else:
                raise RuntimeError("AI bridge table collation differs")
            if context is not None:
                manifest = context["source_row"]
                before = manifest["before_fingerprint"]
                fingerprint_columns = manifest["fingerprint_columns"]
                _validate_mysql_physical_contract(
                    connection,
                    expected_collation=_AI_BRIDGE_COLLATION,
                )
                after = table_content_fingerprint(
                    connection,
                    _AI_BRIDGE_TABLE,
                    columns=fingerprint_columns,
                )
                if after != before:
                    raise RuntimeError(
                        "AI bridge content fingerprint changed during "
                        "collation migration"
                    )
                persist_and_verify_evidence(connection, [make_evidence_record(
                    recovery_version=_AI_BRIDGE_RECOVERY_VERSION,
                    source_table=_AI_BRIDGE_TABLE,
                    source_row_id=0,
                    action="PHYSICAL_REWRITE_VERIFIED",
                    business_key={"table": _AI_BRIDGE_TABLE},
                    source_row={
                        "before_fingerprint": before,
                        "after_fingerprint": after,
                        "fingerprint_columns": fingerprint_columns,
                    },
                    plan_payload=context["plan_payload"],
                    plan_hash=str(context["plan_sha256"]),
                )])
                rewrite_evidence = {
                    "plan_sha256": str(context["plan_sha256"]),
                    "before_fingerprint": before,
                    "after_fingerprint": after,
                    "fingerprint_columns": fingerprint_columns,
                    "content_verified": True,
                    "resumed_pending_plan": resumed_pending_plan,
                }
    validated = validate_ai_bridge_runtime_schema(engine)
    return {
        **validated,
        "normalized_legacy_collation": rewrite_evidence is not None,
        "physical_rewrite_evidence": rewrite_evidence,
    }


def ensure_ai_bridge_schema(engine: Engine) -> None:
    """Compatibility runtime guard; never performs DDL."""
    with _schema_lock:
        if _initialized_engines.get(engine):
            return
        validate_ai_bridge_runtime_schema(engine)
        _initialized_engines[engine] = True


def reset_ai_bridge_schema_cache(engine: Engine | None = None) -> None:
    with _schema_lock:
        if engine is None:
            _initialized_engines.clear()
        else:
            _initialized_engines.pop(engine, None)
