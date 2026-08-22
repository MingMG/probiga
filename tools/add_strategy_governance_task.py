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


def _verify_snapshot(engine, path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if (
        not isinstance(raw, dict)
        or raw.get("format_version") != 1
        or raw.get("task_type") != TASK["task_type"]
        or raw.get("script_path") != TASK["script_path"]
        or not isinstance(raw.get("rows"), list)
        or len(raw["rows"]) > 1
    ):
        raise RuntimeError("invalid strategy governance task snapshot")
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
    raw = json.loads(path.read_text(encoding="utf-8"))
    if (
        not isinstance(raw, dict)
        or raw.get("format_version") != 1
        or raw.get("task_type") != TASK["task_type"]
        or raw.get("script_path") != TASK["script_path"]
        or not isinstance(raw.get("rows"), list)
        or len(raw["rows"]) > 1
    ):
        raise RuntimeError("invalid strategy governance task snapshot")
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
        "--snapshot-file",
        default="",
        help="变更前将唯一治理任务行写入此回滚快照",
    )
    parser.add_argument(
        "--restore-snapshot",
        default="",
        help="从部署前快照精确恢复任务定义",
    )
    parser.add_argument(
        "--capture-snapshot",
        default="",
        help="只读捕获当前唯一治理任务行，不安装或更新任务",
    )
    parser.add_argument(
        "--verify-snapshot",
        default="",
        help="只读验证当前治理任务行与密封快照逐字段一致",
    )
    parser.add_argument(
        "--schema-prepared",
        action="store_true",
        help=(
            "只接受已由root部署迁移边界完整安装并验证的治理结构；"
            "该模式绝不创建触发器"
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
        args.disabled or args.snapshot_file or args.schema_prepared
    ):
        parser.error("snapshot-only modes cannot be combined with install options")
    if (
        os.environ.get("PROBIGA_DEPLOYMENT_MODE", "").strip().lower()
        == "production"
        and not any(read_or_restore_modes)
        and not args.schema_prepared
    ):
        parser.error(
            "production task installation requires --schema-prepared; "
            "runtime schema DDL is forbidden"
        )

    load_project_env()
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

        # Capture the scheduler state before any forward-only database
        # migration.  A later deployment failure can then always restore the
        # task definition even though additive schema changes intentionally
        # remain installed.
        existing_rows = _require_unique_task(engine)
        if args.snapshot_file:
            _write_snapshot(Path(args.snapshot_file), existing_rows)

        from server.engine.strategy_governance import (
            ensure_and_seed_governance,
            validate_governance_append_only_triggers,
            validate_metric_input_review_triggers,
        )
        from server.db.migrations_v3 import run_v3_migrations
        from tools.attest_qmt_daily_kline import (
            ensure_attestation_tables,
            validate_attestation_schema,
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
            # The root deployment boundary already applied and fully validated
            # these migrations while every writer was fenced. Reusing the
            # read-only plan here removes any runtime-account CREATE TRIGGER
            # path and closes a check/apply race during task installation.
            migration_results = migration_plan
            qmt_attestation_schema = validate_attestation_schema(engine)
            with engine.connect() as connection:
                metric_trigger_schema = validate_metric_input_review_triggers(
                    connection
                )
                seeded_strategy_count = int(connection.execute(text(
                    "SELECT COUNT(*) FROM st_strategy_registry"
                )).scalar() or 0)
            append_only_schema = validate_governance_append_only_triggers(engine)
            if seeded_strategy_count <= 0:
                raise RuntimeError(
                    "strategy governance seed registry is empty after preparation"
                )
        else:
            migration_results = run_v3_migrations(engine)
            ensure_attestation_tables(engine)
            qmt_attestation_schema = validate_attestation_schema(engine)
            ensure_and_seed_governance()
            with engine.connect() as connection:
                metric_trigger_schema = validate_metric_input_review_triggers(
                    connection
                )
            append_only_schema = validate_governance_append_only_triggers(engine)
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
                "governance_trigger_count": (
                    int(metric_trigger_schema.get("trigger_count") or 0)
                    + int(append_only_schema.get("trigger_count") or 0)
                ),
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
