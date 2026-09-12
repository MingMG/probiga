"""Final dividend event cache and retained native-source revision audit.

Runtime validation performs no DDL. Only the coordinated privileged schema
cutover calls ``prepare_stock_dividend_schema`` with the migrator engine.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
from socket import gethostname

from sqlalchemy import inspect, text

EVENT_COLUMNS = {
    "event_id": "CHAR(64) NULL", "report_period": "DATE NULL",
    "plan_notice_date": "DATE NULL", "assign_progress": "VARCHAR(64) NULL",
    "source_payload_json": "LONGTEXT NULL",
}
BASE_COLUMNS = {
    "id", "stock_code", "report_date", "dividend_plan", "ex_dividend_date",
    "etl_sync_at", "qmt_code", "data_source", "received_at", "batch_id",
    "data_version", "quality_status",
}
AUDIT_COLUMNS = {
    "sm_dividend_source_revision": {
        "event_id", "source_hash", "source_payload_json", "first_received_at",
    },
    "sm_dividend_source_snapshot": {
        "batch_id", "observed_at", "manifest_json", "manifest_hash",
    },
    "sm_dividend_task_cutover_audit": {
        "task_id", "old_identity_hash", "old_identity_json", "old_projection_json", "new_identity_json", "migrated_at",
    },
}
TYPE_CONTRACTS = {
    "sm_dividend": {"event_id": ("CHAR", 64, True), "report_period": ("DATE", None, True),
                    "plan_notice_date": ("DATE", None, True), "assign_progress": ("VARCHAR", 64, True),
                    "source_payload_json": ("LONGTEXT", None, True)},
    "sm_dividend_source_revision": {"event_id": ("CHAR", 64, False), "source_hash": ("CHAR", 64, False),
                                    "source_payload_json": ("LONGTEXT", None, False), "first_received_at": ("DATETIME", None, False)},
    "sm_dividend_source_snapshot": {"batch_id": ("CHAR", 64, False), "manifest_hash": ("CHAR", 64, False),
                                    "manifest_json": ("LONGTEXT", None, False), "observed_at": ("DATETIME", None, False)},
    "sm_dividend_task_cutover_audit": {"task_id": ("BIGINT", None, False), "old_identity_hash": ("CHAR", 64, False),
                                      "old_identity_json": ("LONGTEXT", None, False), "old_projection_json": ("LONGTEXT", None, False),
                                      "new_identity_json": ("LONGTEXT", None, False), "migrated_at": ("DATETIME", None, False)},
}
CREATE_AUDIT = {
    "sm_dividend_source_revision": """CREATE TABLE sm_dividend_source_revision (
        event_id CHAR(64) NOT NULL, source_hash CHAR(64) NOT NULL,
        source_payload_json LONGTEXT NOT NULL, first_received_at DATETIME NOT NULL,
        PRIMARY KEY(event_id, source_hash)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin""",
    "sm_dividend_source_snapshot": """CREATE TABLE sm_dividend_source_snapshot (
        batch_id CHAR(64) NOT NULL, observed_at DATETIME NOT NULL,
        manifest_json LONGTEXT NOT NULL, manifest_hash CHAR(64) NOT NULL,
        PRIMARY KEY(batch_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin""",
    "sm_dividend_task_cutover_audit": """CREATE TABLE sm_dividend_task_cutover_audit (
        task_id BIGINT NOT NULL, old_identity_hash CHAR(64) NOT NULL,
        old_identity_json LONGTEXT NOT NULL, old_projection_json LONGTEXT NOT NULL,
        new_identity_json LONGTEXT NOT NULL, migrated_at DATETIME NOT NULL,
        PRIMARY KEY(task_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin""",
}
AUDIT_PRIMARY_KEYS = {"sm_dividend_source_revision": ("event_id", "source_hash"),
                      "sm_dividend_source_snapshot": ("batch_id",), "sm_dividend_task_cutover_audit": ("task_id",)}
def inspect_stock_dividend_schema(engine) -> dict:
    reader = inspect(engine)
    if not reader.has_table("sm_dividend"):
        raise RuntimeError("DIVIDEND_BASE_SCHEMA_MISSING")
    columns = {c["name"] for c in reader.get_columns("sm_dividend")}
    if BASE_COLUMNS - columns:
        raise RuntimeError("DIVIDEND_BASE_SCHEMA_DRIFT")
    for table, contracts in TYPE_CONTRACTS.items():
        if not reader.has_table(table):
            continue
        for column in reader.get_columns(table):
            if column["name"] not in contracts:
                continue
            name, length, nullable = contracts[column["name"]]
            observed_type = column["type"]
            if (type(observed_type).__name__.upper() != name
                or getattr(observed_type, "length", None) != length
                or bool(column["nullable"]) != nullable):
                raise RuntimeError("DIVIDEND_FIELD_STORAGE_CONTRACT_DRIFT")
    unique = [tuple(v["column_names"]) for v in reader.get_indexes("sm_dividend") if v.get("unique")]
    unique += [tuple(v["column_names"]) for v in reader.get_unique_constraints("sm_dividend")]
    # Never silently remove an unexpected key that could collapse real events.
    if any(set(v) <= {"stock_code", "report_date"} for v in unique):
        raise RuntimeError("DIVIDEND_LEGACY_UNIQUE_COLLAPSES_EVENTS")
    missing_audit = []
    for table, required in AUDIT_COLUMNS.items():
        if not reader.has_table(table):
            missing_audit.append(table)
        elif required - {c["name"] for c in reader.get_columns(table)}:
            raise RuntimeError("DIVIDEND_AUDIT_SCHEMA_DRIFT")
        else:
            primary = tuple(reader.get_pk_constraint(table)["constrained_columns"])
            expected = AUDIT_PRIMARY_KEYS[table]
            if primary != expected:
                raise RuntimeError("DIVIDEND_AUDIT_PRIMARY_KEY_DRIFT")
    result = {"missing_event_columns": sorted(set(EVENT_COLUMNS) - columns),
              "missing_event_unique": ("event_id",) not in unique,
              "missing_audit_tables": missing_audit}
    return {"status": "MIGRATION_REQUIRED" if any(result.values()) else "PASS", **result}


def validate_stock_dividend_schema(engine) -> dict:
    result = inspect_stock_dividend_schema(engine)
    if result["status"] != "PASS":
        raise RuntimeError("DIVIDEND_EVENT_SCHEMA_MIGRATION_REQUIRED")
    return result


def prepare_stock_dividend_schema(engine) -> dict:
    """Called only after writer fencing by the existing privileged cutover."""
    plan = inspect_stock_dividend_schema(engine)
    with engine.begin() as connection:
        for column in plan["missing_event_columns"]:
            connection.exec_driver_sql(f"ALTER TABLE sm_dividend ADD COLUMN {column} {EVENT_COLUMNS[column]}")
        if plan["missing_event_unique"]:
            connection.exec_driver_sql("ALTER TABLE sm_dividend ADD UNIQUE KEY uq_dividend_event_id (event_id)")
        for table in plan["missing_audit_tables"]:
            connection.exec_driver_sql(CREATE_AUDIT[table])
    return {**validate_stock_dividend_schema(engine), "legacy_rows_preserved": True}


OLD_TASK_TYPE = "stock_dividend_baidu"
OLD_SCRIPT = "biz/stock_market/sync_dividend_baidu.py"
NEW_IDENTITY = {"task_type": "stock_dividend_eastmoney", "script_path": "biz/stock_market/sync_dividend_eastmoney.py",
                "script_args": "--execute", "task_name": "东财全市场分红原始事件同步"}
PROJECTION_COLUMNS = ("last_run_status", "last_run_at", "last_run_duration", "last_run_output")


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _task_plan(connection, *, lock=False):
    suffix = " FOR UPDATE" if lock and connection.dialect.name == "mysql" else ""
    rows = [dict(row) for row in connection.execute(text(
        "SELECT id,task_name,task_type,script_path,script_args," + ",".join(PROJECTION_COLUMNS) +
        " FROM st_scheduled_tasks WHERE task_type IN (:old,:new) OR script_path IN (:old_path,:new_path) ORDER BY id" + suffix
    ), {"old": OLD_TASK_TYPE, "new": NEW_IDENTITY["task_type"], "old_path": OLD_SCRIPT, "new_path": NEW_IDENTITY["script_path"]}).mappings()]
    if not rows:
        return "INSTALL", None
    if len(rows) != 1:
        raise RuntimeError("DIVIDEND_TASK_IDENTITY_AMBIGUOUS")
    row = rows[0]
    if row["task_type"] == OLD_TASK_TYPE and row["script_path"] == OLD_SCRIPT:
        return "MIGRATE", row
    if all(row[key] == value for key, value in NEW_IDENTITY.items()):
        return "PASS", row
    raise RuntimeError("DIVIDEND_TASK_IDENTITY_DRIFT")


def inspect_stock_dividend_task_identity(engine):
    """Read-only preflight; ambiguity fails before any cutover DDL/DML."""
    with engine.connect() as connection:
        status, row = _task_plan(connection)
    return {"status": status, "task_id": row["id"] if row else None}


def _dead_scheduler_owner_evidence(history):
    """Observe the exact old local Linux owner; never infer death from fencing."""
    host = gethostname()
    instance = str(history.get("scheduler_instance_id") or "")
    match = re.fullmatch(re.escape(host) + r"-([1-9][0-9]*)", instance)
    if (os.name != "posix" or history.get("host_name") != host or match is None
        or history.get("task_type") != OLD_TASK_TYPE
        or not re.fullmatch(r"[0-9a-f]{32,64}", str(history.get("run_uid") or ""))
        or not re.fullmatch(r"[0-9a-f]{40}", str(history.get("build_sha") or ""))):
        raise RuntimeError("DIVIDEND_TASK_RUNNING_OWNER_NOT_PROVEN_ABSENT")
    from server.api.scheduler_runtime import _owner_pid_is_absent
    pid = int(match.group(1))
    if not _owner_pid_is_absent(instance, host_name=host):
        raise RuntimeError("DIVIDEND_TASK_RUNNING_OWNER_NOT_PROVEN_ABSENT")
    try:
        # A missing /proc mount or a permission error is not absence evidence.
        Path("/proc/self/status").stat()
        Path(f"/proc/{pid}").stat()
    except FileNotFoundError as exc:
        if str(exc.filename) != f"/proc/{pid}":
            raise RuntimeError("DIVIDEND_TASK_RUNNING_OWNER_NOT_PROVEN_ABSENT") from exc
    except OSError as exc:
        raise RuntimeError("DIVIDEND_TASK_RUNNING_OWNER_NOT_PROVEN_ABSENT") from exc
    else:
        raise RuntimeError("DIVIDEND_TASK_RUNNING_OWNER_NOT_PROVEN_ABSENT")
    return {"run_uid": history["run_uid"], "build_sha": history["build_sha"], "host_name": host,
            "scheduler_instance_id": instance, "owner_pid": pid, "owner_absent": True,
            "observed_at": datetime.now().isoformat(), "method": "exact_owner_pid_and_proc_absent",
            "history_status_unchanged": True}


def migrate_stock_dividend_task_identity(engine):
    """Fenced cutover only: preserve the existing id, schedule and history."""
    with engine.begin() as connection:
        status, row = _task_plan(connection, lock=True)
        if status != "MIGRATE":
            return {"status": status, "task_id": row["id"] if row else None, "history_preserved": True}
        suffix = " FOR UPDATE" if connection.dialect.name == "mysql" else ""
        running = [dict(item) for item in connection.execute(text(
            "SELECT run_uid,task_type,host_name,scheduler_instance_id,build_sha FROM st_scheduled_task_history "
            "WHERE task_id=:id AND LOWER(status)='running' ORDER BY id" + suffix
        ), {"id": row["id"]}).mappings()]
        owner_evidence = None
        if running:
            if len(running) != 1:
                raise RuntimeError("DIVIDEND_TASK_RUNNING_OWNER_NOT_DRAINED")
            owner_evidence = _dead_scheduler_owner_evidence(running[0])
        elif str(row["last_run_status"] or "").lower() == "running":
            raise RuntimeError("DIVIDEND_TASK_RUNNING_OWNER_NOT_DRAINED")
        old_identity = {key: row[key] for key in ("task_type", "task_name", "script_path", "script_args")}
        old_json = _json(old_identity)
        connection.execute(text(
            "INSERT INTO sm_dividend_task_cutover_audit (task_id,old_identity_hash,old_identity_json,old_projection_json,new_identity_json,migrated_at) "
            "VALUES (:id,:old_hash,:old_json,:projection,:new_json,:at)"
        ), {"id": row["id"], "old_hash": hashlib.sha256(old_json.encode()).hexdigest(), "old_json": old_json,
            "projection": _json({**{key: row[key] for key in PROJECTION_COLUMNS}, "running_owner_evidence": owner_evidence}),
            "new_json": _json(NEW_IDENTITY), "at": datetime.now()})
        changed = connection.execute(text(
            "UPDATE st_scheduled_tasks SET task_type=:task_type,task_name=:task_name,script_path=:script_path,script_args=:script_args,"
            "last_run_status=NULL,last_run_at=NULL,last_run_duration=NULL,last_run_output=NULL "
            "WHERE id=:id AND task_type=:old_type AND script_path=:old_path"
        ), {**NEW_IDENTITY, "id": row["id"], "old_type": OLD_TASK_TYPE, "old_path": OLD_SCRIPT})
        if changed.rowcount != 1 or _task_plan(connection)[0] != "PASS":
            raise RuntimeError("DIVIDEND_TASK_CUTOVER_COMPARE_AND_SWAP_FAILED")
    return {"status": "PASS", "task_id": row["id"], "history_preserved": True, "previous_projection_retained": True}
