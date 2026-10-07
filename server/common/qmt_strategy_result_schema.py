"""Fenced schema for issued QMT simulation inputs and append-only results.

Only the release migrator calls the DDL entrypoint. API and edge processes
validate this contract and append facts; they never create or repair tables.
"""
from __future__ import annotations

from typing import Any

from sqlalchemy import (
    CHAR, CheckConstraint, Column, DateTime, ForeignKey, Index, Integer,
    MetaData, String, Table, Text, UniqueConstraint, inspect,
)
from sqlalchemy.dialects.mysql import LONGTEXT


SCHEMA = "probiga.qmt-strategy-result-storage.v1"
METADATA = MetaData()
_JSON_TEXT = Text().with_variant(LONGTEXT(), "mysql")
_STORAGE = {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4",
            "mysql_collate": "utf8mb4_unicode_ci"}

INPUTS = Table(
    "st_qmt_strategy_input", METADATA,
    Column("snapshot_id", CHAR(32), primary_key=True),
    Column("trade_date", String(10), nullable=False),
    Column("run_mode", String(8), nullable=False),
    Column("edge_build_sha", CHAR(40), nullable=False),
    Column("executor_sha256", CHAR(64), nullable=False),
    Column("input_hash", CHAR(64), nullable=False),
    Column("snapshot_sha256", CHAR(64), nullable=False),
    Column("snapshot_json", _JSON_TEXT, nullable=False),
    Column("issued_at", DateTime(), nullable=False),
    Column("simulation_only", Integer(), nullable=False),
    Column("real_order_allowed", Integer(), nullable=False),
    UniqueConstraint("edge_build_sha", "snapshot_sha256", name="uk_qmt_strategy_input_hash"),
    CheckConstraint("run_mode IN ('DAILY','REPLAY')", name="ck_qmt_strategy_input_mode"),
    CheckConstraint("simulation_only=1 AND real_order_allowed=0", name="ck_qmt_strategy_input_research"),
    CheckConstraint("LENGTH(snapshot_id)=32 AND LENGTH(input_hash)=64 AND LENGTH(snapshot_sha256)=64", name="ck_qmt_strategy_input_identity"),
    Index("idx_qmt_strategy_input_date", "trade_date", "issued_at"),
    **_STORAGE,
)

RESULTS = Table(
    "st_qmt_strategy_result", METADATA,
    Column("run_uid", CHAR(32), primary_key=True),
    Column("snapshot_id", CHAR(32), ForeignKey(INPUTS.c.snapshot_id), nullable=False),
    Column("trade_date", String(10), nullable=False),
    Column("run_mode", String(8), nullable=False),
    Column("origin", String(16), nullable=False),
    Column("status", String(24), nullable=False),
    Column("input_hash", CHAR(64), nullable=False),
    Column("result_hash", CHAR(64), nullable=False),
    Column("execution_hash", CHAR(64), nullable=False),
    Column("result_json", _JSON_TEXT, nullable=False),
    Column("execution_json", _JSON_TEXT, nullable=False),
    Column("strategy_count", Integer(), nullable=False),
    Column("combination_count", Integer(), nullable=False),
    Column("selected_count", Integer(), nullable=False),
    Column("blocked_count", Integer(), nullable=False),
    Column("completed_at", DateTime(), nullable=False),
    Column("received_at", DateTime(), nullable=False),
    Column("simulation_only", Integer(), nullable=False),
    Column("real_order_allowed", Integer(), nullable=False),
    UniqueConstraint("snapshot_id", name="uk_qmt_strategy_result_snapshot"),
    CheckConstraint("run_mode IN ('DAILY','REPLAY')", name="ck_qmt_strategy_result_mode"),
    CheckConstraint("origin IN ('QMT_ENTRY','WINDOWS_DAILY')", name="ck_qmt_strategy_result_origin"),
    CheckConstraint("status IN ('COMPLETED','COMPLETED_EMPTY','DATA_BLOCKED','PARTIAL')", name="ck_qmt_strategy_result_status"),
    CheckConstraint("simulation_only=1 AND real_order_allowed=0", name="ck_qmt_strategy_result_research"),
    CheckConstraint("strategy_count=10 AND combination_count=4 AND selected_count>=0 AND blocked_count>=0", name="ck_qmt_strategy_result_counts"),
    CheckConstraint("LENGTH(run_uid)=32 AND LENGTH(result_hash)=64 AND LENGTH(execution_hash)=64", name="ck_qmt_strategy_result_identity"),
    Index("idx_qmt_strategy_result_date", "trade_date", "received_at"),
    **_STORAGE,
)

JOBS = Table(
    "st_qmt_strategy_input_job", METADATA,
    Column("request_id", CHAR(32), primary_key=True),
    Column("request_hash", CHAR(64), nullable=False),
    Column("binding_hash", CHAR(64), nullable=False),
    Column("request_json", _JSON_TEXT, nullable=False),
    Column("trade_date", String(10), nullable=False),
    Column("run_mode", String(8), nullable=False),
    Column("edge_build_sha", CHAR(40), nullable=False),
    Column("status", String(12), nullable=False),
    Column("snapshot_id", CHAR(32), ForeignKey(INPUTS.c.snapshot_id), nullable=True),
    Column("snapshot_existing", Integer(), nullable=True),
    Column("lease_token", CHAR(32), nullable=True),
    Column("lease_expires_at", DateTime(), nullable=True),
    Column("heartbeat_at", DateTime(), nullable=True),
    Column("attempt_count", Integer(), nullable=False),
    Column("error_code", String(64), nullable=True),
    Column("created_at", DateTime(), nullable=False),
    Column("updated_at", DateTime(), nullable=False),
    CheckConstraint("run_mode IN ('DAILY','REPLAY')", name="ck_qmt_strategy_job_mode"),
    CheckConstraint("status IN ('QUEUED','PREPARING','ISSUED','FAILED')", name="ck_qmt_strategy_job_status"),
    CheckConstraint("attempt_count>=0", name="ck_qmt_strategy_job_attempts"),
    CheckConstraint("LENGTH(request_id)=32 AND LENGTH(request_hash)=64 AND LENGTH(binding_hash)=64", name="ck_qmt_strategy_job_identity"),
    CheckConstraint("(status='ISSUED' AND snapshot_id IS NOT NULL) OR (status<>'ISSUED' AND snapshot_id IS NULL)", name="ck_qmt_strategy_job_snapshot"),
    CheckConstraint("(status='ISSUED' AND snapshot_existing IS NOT NULL AND snapshot_existing IN (0,1)) OR (status<>'ISSUED' AND snapshot_existing IS NULL)", name="ck_qmt_strategy_job_existing"),
    CheckConstraint("(status='PREPARING' AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL AND heartbeat_at IS NOT NULL) OR (status<>'PREPARING' AND lease_token IS NULL AND lease_expires_at IS NULL)", name="ck_qmt_strategy_job_lease"),
    CheckConstraint("(status='FAILED' AND error_code IS NOT NULL) OR (status<>'FAILED' AND error_code IS NULL)", name="ck_qmt_strategy_job_error"),
    Index("idx_qmt_strategy_job_lease", "status", "lease_expires_at"),
    **_STORAGE,
)


def validate_qmt_strategy_result_schema(engine: Any) -> dict[str, Any]:
    """Inspect physical fields, keys, constraints and storage without DDL."""
    reader = inspect(engine)
    dialect = engine.dialect
    for table in (INPUTS, RESULTS, JOBS):
        if not reader.has_table(table.name):
            raise RuntimeError("QMT simulation result schema is not installed")
        columns = {item["name"]: item for item in reader.get_columns(table.name)}
        if set(columns) != set(table.c.keys()):
            raise RuntimeError("QMT simulation result columns differ")
        for column in table.c:
            actual = columns[column.name]
            expected_type = str(column.type.compile(dialect=dialect)).lower()
            actual_type = str(actual["type"]).lower()
            if (actual_type != expected_type or bool(actual["nullable"]) != column.nullable):
                raise RuntimeError("QMT simulation result field contract differs")
        if reader.get_pk_constraint(table.name)["constrained_columns"] != list(table.primary_key.columns.keys()):
            raise RuntimeError("QMT simulation result primary key differs")
        expected_unique = {
            tuple(column.name for column in item.columns)
            for item in table.constraints if isinstance(item, UniqueConstraint)
        }
        actual_unique = {tuple(item["column_names"]) for item in reader.get_unique_constraints(table.name)}
        if not expected_unique.issubset(actual_unique):
            raise RuntimeError("QMT simulation result uniqueness differs")
        expected_indexes = {index.name: tuple(column.name for column in index.columns) for index in table.indexes}
        actual_indexes = {item["name"]: tuple(item["column_names"]) for item in reader.get_indexes(table.name)}
        if any(actual_indexes.get(name) != fields for name, fields in expected_indexes.items()):
            raise RuntimeError("QMT simulation result lookup index differs")
        actual_checks = {item["name"]: item.get("sqltext", "") for item in reader.get_check_constraints(table.name)}
        normalize = lambda value: "".join(str(value).lower().replace("_utf8mb4", "").replace("_utf8mb3", "").replace("`", "").replace("\"", "").split()).replace("(", "").replace(")", "")
        for constraint in table.constraints:
            if isinstance(constraint, CheckConstraint) and normalize(actual_checks.get(constraint.name)) != normalize(constraint.sqltext):
                raise RuntimeError("QMT simulation result safety constraint differs")
        if dialect.name == "mysql":
            options = reader.get_table_options(table.name)
            if (str(options.get("mysql_engine", "")).lower() != "innodb"
                    or options.get("mysql_collate") != "utf8mb4_unicode_ci"):
                raise RuntimeError("QMT simulation result storage differs")
    foreign_keys = reader.get_foreign_keys(RESULTS.name)
    if not any(item["constrained_columns"] == ["snapshot_id"] and item["referred_table"] == INPUTS.name
               and item["referred_columns"] == ["snapshot_id"] for item in foreign_keys):
        raise RuntimeError("QMT simulation input/result relationship differs")
    if not any(item["constrained_columns"] == ["snapshot_id"] and item["referred_table"] == INPUTS.name
               and item["referred_columns"] == ["snapshot_id"] for item in reader.get_foreign_keys(JOBS.name)):
        raise RuntimeError("QMT simulation input/job relationship differs")
    return {"schema": SCHEMA, "tables": [INPUTS.name, RESULTS.name, JOBS.name],
            "read_only": True, "runtime_ddl_required": False,
            "simulation_only": True, "real_order_allowed": False}


def privileged_migrate_qmt_strategy_result_schema(engine: Any) -> dict[str, Any]:
    """Create only absent final tables inside the established release fence."""
    METADATA.create_all(engine, checkfirst=True)
    return {**validate_qmt_strategy_result_schema(engine), "privileged_migration": True}
