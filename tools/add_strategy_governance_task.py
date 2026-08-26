#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Install or update the production daily strategy-governance scheduler row."""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import quote_identifier
from server.common.scheduler_tasks import table_columns, upsert_scheduler_task
from server.common.strategy_governance_mode import (
    StrategyGovernanceMode,
    get_strategy_governance_mode,
)
from tools.env_config import create_tool_engine, load_project_env
from tools.strategy_governance_task_contract import TASK


def _matching_tasks(engine) -> list[dict[str, Any]]:
    with engine.connect() as connection:
        return [
            dict(row)
            for row in connection.execute(
                text(
                    "SELECT * FROM st_scheduled_tasks "
                    "WHERE task_type=:task_type OR script_path=:script_path "
                    "ORDER BY id"
                ),
                {
                    "task_type": TASK["task_type"],
                    "script_path": TASK["script_path"],
                },
            ).mappings().all()
        ]


def _require_unique_task(engine) -> list[dict[str, Any]]:
    rows = _matching_tasks(engine)
    if len(rows) > 1:
        raise RuntimeError(
            "strategy governance scheduler identity is not unique: "
            f"{len(rows)} matching rows"
        )
    return rows


def _require_exact_deferred_task(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Require the one immutable scheduler identity that may be disabled."""

    if len(rows) != 1:
        raise RuntimeError(
            "deferred strategy governance disable requires exactly one matching "
            f"scheduler row; observed {len(rows)}"
        )
    row = rows[0]
    if (
        str(row.get("task_type") or "") != TASK["task_type"]
        or str(row.get("script_path") or "") != TASK["script_path"]
    ):
        raise RuntimeError(
            "deferred strategy governance scheduler identity is not exact"
        )
    if str(row.get("last_run_status") or "").strip().lower() == "running":
        raise RuntimeError(
            "cannot disable the strategy governance task while it is running"
        )
    try:
        enabled = int(row.get("enabled") or 0)
    except (TypeError, ValueError, OverflowError) as exc:
        raise RuntimeError(
            "strategy governance scheduler enabled bit is invalid"
        ) from exc
    if enabled not in {0, 1}:
        raise RuntimeError(
            "strategy governance scheduler enabled bit is invalid"
        )
    return row


def _deferred_disable_task(
    engine,
    *,
    snapshot_path: Path | None = None,
) -> dict[str, Any]:
    """Atomically disable the exact non-running governance task without DDL."""

    identity = {
        "task_type": TASK["task_type"],
        "script_path": TASK["script_path"],
    }
    select_sql = text(
        "SELECT * FROM st_scheduled_tasks "
        "WHERE task_type=:task_type OR script_path=:script_path "
        "ORDER BY id FOR UPDATE"
    )
    with engine.begin() as connection:
        before_rows = [
            dict(row)
            for row in connection.execute(
                select_sql,
                identity,
            ).mappings().all()
        ]
        before = _require_exact_deferred_task(before_rows)
        if snapshot_path is not None:
            _write_snapshot(snapshot_path, before_rows)

        task_id = int(before.get("id") or 0)
        if task_id <= 0:
            raise RuntimeError(
                "strategy governance scheduler row has no valid primary key"
            )
        update_result = connection.execute(
            text(
                "UPDATE st_scheduled_tasks SET enabled=0 "
                "WHERE id=:task_id AND task_type=:task_type "
                "AND script_path=:script_path "
                "AND COALESCE(LOWER(TRIM(last_run_status)), '') <> 'running'"
            ),
            {**identity, "task_id": task_id},
        )
        changed = int(update_result.rowcount or 0)
        if int(before.get("enabled") or 0) == 1 and changed != 1:
            raise RuntimeError(
                "strategy governance task was not atomically disabled"
            )
        if changed not in {0, 1}:
            raise RuntimeError(
                "strategy governance disable changed an unexpected row count"
            )

        after_rows = [
            dict(row)
            for row in connection.execute(
                select_sql,
                identity,
            ).mappings().all()
        ]
        after = _require_exact_deferred_task(after_rows)
        if int(after.get("id") or 0) != task_id:
            raise RuntimeError(
                "strategy governance scheduler identity changed during disable"
            )
        if int(after.get("enabled") or 0) != 0:
            raise RuntimeError(
                "strategy governance task remained enabled after deferred disable"
            )
        before_unchanged = {
            key: value for key, value in before.items() if key != "enabled"
        }
        after_unchanged = {
            key: value for key, value in after.items() if key != "enabled"
        }
        if after_unchanged != before_unchanged:
            raise RuntimeError(
                "strategy governance task changed outside the enabled bit"
            )

    return {
        "action": (
            "disabled" if int(before.get("enabled") or 0) == 1
            else "already_disabled"
        ),
        "id": task_id,
        "enabled": 0,
        "snapshot_file": str(snapshot_path) if snapshot_path is not None else None,
        "schema_preparation_performed": False,
    }


def _write_snapshot(path: Path, rows: list[dict[str, Any]]) -> None:
    if path.exists() and path.stat().st_size:
        raise RuntimeError(f"refusing to overwrite non-empty snapshot: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "format_version": 1,
        "task_type": TASK["task_type"],
        "script_path": TASK["script_path"],
        "rows": json.loads(
            json.dumps(rows, ensure_ascii=False, default=str, sort_keys=True)
        ),
    }
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, default=str)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary_name, 0o600)
        os.replace(temporary_name, path)
    finally:
        if os.path.exists(temporary_name):
                os.unlink(temporary_name)


def _read_snapshot(path: Path) -> dict[str, Any]:
    # ``-`` is the standard stream sentinel.  The root deployment broker can
    # open a sealed root:root 0600 snapshot and hand its read-only stdin to the
    # unprivileged service process without weakening the file's permissions.
    raw_text = (
        sys.stdin.buffer.read().decode("utf-8")
        if str(path) == "-"
        else path.read_text(encoding="utf-8")
    )
    raw = json.loads(raw_text)
    if (
        not isinstance(raw, dict)
        or raw.get("format_version") != 1
        or raw.get("task_type") != TASK["task_type"]
        or raw.get("script_path") != TASK["script_path"]
        or not isinstance(raw.get("rows"), list)
        or len(raw["rows"]) > 1
    ):
        raise RuntimeError("invalid strategy governance task snapshot")
    return raw


def _verify_snapshot(engine, path: Path) -> dict[str, Any]:
    raw = _read_snapshot(path)
    observed = json.loads(
        json.dumps(
            _require_unique_task(engine),
            ensure_ascii=False,
            default=str,
            sort_keys=True,
        )
    )
    if observed != raw["rows"]:
        raise RuntimeError("strategy governance task differs from sealed snapshot")
    return {"verified": True, "row_count": len(observed)}


def _restore_snapshot(engine, path: Path) -> dict[str, Any]:
    raw = _read_snapshot(path)
    prior_rows = raw["rows"]
    current_rows = _require_unique_task(engine)
    columns = table_columns(engine)
    if not columns:
        raise RuntimeError("st_scheduled_tasks does not exist")
    predicate = "task_type=:task_type OR script_path=:script_path"
    identity = {
        "task_type": TASK["task_type"],
        "script_path": TASK["script_path"],
    }
    with engine.begin() as connection:
        if not prior_rows:
            deleted = connection.execute(
                text(f"DELETE FROM st_scheduled_tasks WHERE {predicate}"),
                identity,
            ).rowcount
            return {"action": "deleted_new_task", "row_count": int(deleted or 0)}

        prior = prior_rows[0]
        if not isinstance(prior, dict) or "id" not in prior:
            raise RuntimeError("scheduler snapshot row is missing its id")
        compatible = {
            str(key): value for key, value in prior.items() if str(key) in columns
        }
        prior_id = int(compatible["id"])
        if current_rows:
            current_id = int(current_rows[0].get("id") or 0)
            if current_id != prior_id:
                raise RuntimeError(
                    "scheduler task identity changed during deployment rollback"
                )
            update_payload = {
                key: value for key, value in compatible.items() if key != "id"
            }
            assignments = ", ".join(
                f"{quote_identifier(key)}=:{key}" for key in update_payload
            )
            connection.execute(
                text(
                    f"UPDATE st_scheduled_tasks SET {assignments} "
                    "WHERE id=:restore_id"
                ),
                {**update_payload, "restore_id": prior_id},
            )
            return {"action": "restored_existing_task", "id": prior_id}

        names = ", ".join(quote_identifier(key) for key in compatible)
        values = ", ".join(f":{key}" for key in compatible)
        connection.execute(
            text(f"INSERT INTO st_scheduled_tasks ({names}) VALUES ({values})"),
            compatible,
        )
        return {"action": "reinserted_previous_task", "id": prior_id}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="安装、禁用或恢复动态策略治理每日调度任务"
    )
    parser.add_argument(
        "--disabled",
        action="store_true",
        help="安装任务但保持禁用，供生产切换窗口使用",
    )
    parser.add_argument(
        "--deferred-disable",
        action="store_true",
        help=(
            "仅在PROBIGA_STRATEGY_GOVERNANCE_MODE=DEFERRED_DB时，"
            "原子停用已存在且未运行的唯一治理任务；不执行任何结构准备"
        ),
    )
    parser.add_argument(
        "--snapshot-file",
        default="",
        help="变更前将唯一治理任务行写入此回滚快照",
    )
    parser.add_argument(
        "--restore-snapshot",
        default="",
        help="从部署前快照精确恢复任务定义；传 - 时从标准输入读取",
    )
    parser.add_argument(
        "--capture-snapshot",
        default="",
        help="只读捕获当前唯一治理任务行，不安装或更新任务",
    )
    parser.add_argument(
        "--verify-snapshot",
        default="",
        help="只读验证当前治理任务行与密封快照逐字段一致；传 - 时从标准输入读取",
    )
    parser.add_argument(
        "--schema-prepared",
        action="store_true",
        help=(
            "只读验证已提前安装的治理表、索引和迁移标记；"
            "不传时由本部署工具自动执行RDS安全的无触发器结构准备"
        ),
    )
    parser.add_argument(
        "--writers-fenced-schema-preparation",
        action="store_true",
        help=(
            "仅在所有生产写入器已停止时，允许执行严格证明的RDS安全QMT排序规则迁移"
        ),
    )
    args = parser.parse_args()
    read_or_restore_modes = [
        bool(args.restore_snapshot),
        bool(args.capture_snapshot),
        bool(args.verify_snapshot),
    ]
    if sum(read_or_restore_modes) > 1:
        parser.error("snapshot capture, verification and restore are mutually exclusive")
    if any(read_or_restore_modes) and (
        args.disabled
        or args.deferred_disable
        or args.snapshot_file
        or args.schema_prepared
        or args.writers_fenced_schema_preparation
    ):
        parser.error("snapshot-only modes cannot be combined with install options")
    if args.deferred_disable and (
        args.disabled
        or args.schema_prepared
        or args.writers_fenced_schema_preparation
    ):
        parser.error(
            "--deferred-disable can only be combined with --snapshot-file"
        )
    if args.writers_fenced_schema_preparation and (
        args.schema_prepared or not args.disabled
    ):
        parser.error(
            "writers-fenced schema preparation requires --disabled and cannot "
            "be combined with --schema-prepared"
        )
    if (
        os.environ.get("PROBIGA_DEPLOYMENT_MODE") == "production"
        and not any(read_or_restore_modes)
        and not args.deferred_disable
        and not args.schema_prepared
    ):
        parser.error(
            "production task installation requires --schema-prepared; "
            "persistent DDL belongs to the fenced migration account"
        )
    load_project_env()
    if (
        args.deferred_disable
        and get_strategy_governance_mode()
        is not StrategyGovernanceMode.DEFERRED_DB
    ):
        parser.error(
            "--deferred-disable requires "
            "PROBIGA_STRATEGY_GOVERNANCE_MODE=DEFERRED_DB"
        )
    engine = create_tool_engine()
    try:
        if args.restore_snapshot:
            result = _restore_snapshot(engine, Path(args.restore_snapshot))
            print(
                json.dumps(
                    {"status": "ok", "restored": True, "result": result},
                    ensure_ascii=False,
                    default=str,
                )
            )
            return 0
        if args.capture_snapshot:
            rows = _require_unique_task(engine)
            _write_snapshot(Path(args.capture_snapshot), rows)
            print(
                json.dumps(
                    {"status": "ok", "captured": True, "row_count": len(rows)},
                    ensure_ascii=False,
                )
            )
            return 0
        if args.verify_snapshot:
            result = _verify_snapshot(engine, Path(args.verify_snapshot))
            print(
                json.dumps(
                    {"status": "ok", "snapshot_verified": True, "result": result},
                    ensure_ascii=False,
                )
            )
            return 0
        if args.deferred_disable:
            result = _deferred_disable_task(
                engine,
                snapshot_path=(
                    Path(args.snapshot_file) if args.snapshot_file else None
                ),
            )
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "deferred_disabled": True,
                        "result": result,
                    },
                    ensure_ascii=False,
                    default=str,
                )
            )
            return 0

        # Capture the scheduler state before any forward-only database
        # migration.  A later deployment failure can then always restore the
        # task definition even though additive schema changes intentionally
        # remain installed.
        existing_rows = _require_unique_task(engine)
        if args.snapshot_file:
            _write_snapshot(Path(args.snapshot_file), existing_rows)

        from server.engine.strategy_governance import (
            ensure_strategy_governance_tables,
            seed_governance_registry,
            validate_prepared_governance_runtime,
        )
        from server.db.migrations_v3 import run_v3_migrations
        from tools.attest_qmt_daily_kline import (
            ensure_attestation_tables,
            migrate_legacy_attestation_collation,
            validate_attestation_schema,
        )
        from tools.prepare_strategy_governance_qmt_history import (
            apply_legacy_completed_run_binding,
        )

        # The governance ledger consumes the exact strategy_version written by
        # the V3 paper-evidence path. Apply the additive, rollback-compatible
        # V3 expansion before creating/enabling the daily governance task.
        if args.schema_prepared:
            migration_plan = run_v3_migrations(engine, dry_run=True)
            pending = [
                item.version for item in migration_plan if item.status != "exists"
            ]
            if pending:
                raise RuntimeError(
                    "strategy governance schema was not prepared before cutover: "
                    + ", ".join(pending)
                )
            # The deployment boundary already applied and fully validated the
            # table/index migrations while every writer was fenced. Reusing
            # the read-only plan here closes a check/apply race during task
            # installation without requiring database triggers.
            migration_results = migration_plan
            qmt_attestation_schema = validate_attestation_schema(engine)
            governance_schema = validate_prepared_governance_runtime(engine)
        else:
            migration_results = run_v3_migrations(engine)
            qmt_collation_migration = None
            qmt_legacy_binding_migration = None
            if args.writers_fenced_schema_preparation:
                qmt_collation_migration = migrate_legacy_attestation_collation(
                    engine,
                    writers_fenced=True,
                )
                qmt_legacy_binding_migration = (
                    apply_legacy_completed_run_binding(engine)
                )
            ensure_attestation_tables(engine)
            qmt_attestation_schema = validate_attestation_schema(engine)
            ensure_strategy_governance_tables(engine=engine)
            seed_governance_registry()
            governance_schema = validate_prepared_governance_runtime(engine)
        task = {**TASK, "enabled": 0 if args.disabled else 1}
        result = upsert_scheduler_task(
            engine,
            task,
            lookup_where=(
                "task_type = :task_type OR script_path = :script_path"
            ),
            lookup_params={
                "task_type": TASK["task_type"],
                "script_path": TASK["script_path"],
            },
        )
    finally:
        engine.dispose()
    print(
        json.dumps(
            {
                "status": "ok",
                "task": task,
                "v3_migrations": [
                    {
                        "version": item.version,
                        "status": item.status,
                        "statement_count": item.statement_count,
                    }
                    for item in migration_results
                ],
                "snapshot_file": args.snapshot_file or None,
                "qmt_attestation_schema": qmt_attestation_schema,
                "qmt_collation_migration": (
                    qmt_collation_migration
                    if not args.schema_prepared
                    else None
                ),
                "qmt_legacy_binding_migration": (
                    qmt_legacy_binding_migration
                    if not args.schema_prepared
                    else None
                ),
                "governance_trigger_count": int(
                    governance_schema["trigger_count"]
                ),
                "database_triggers_required": bool(
                    governance_schema["database_triggers_required"]
                ),
                "governance_schema": governance_schema,
                "schema_prepared": bool(args.schema_prepared),
                "result": result,
            },
            ensure_ascii=False,
            default=str,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
