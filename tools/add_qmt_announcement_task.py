#!/usr/bin/env python3
"""Install the Windows-owned full-market QMT announcement scheduler task."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import tempfile
from typing import Any

from sqlalchemy import text


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import quote_identifier
from server.common.scheduler_tasks import (
    table_columns,
    upsert_scheduler_task,
)
from tools.qmt_announcement_task_contract import (
    ANALYSIS_FAST_CRON,
    ANALYSIS_UPPER_EVIDENCE_CRON,
    STRATEGY_GOVERNANCE_CRON,
    TASK,
    validate_pipeline_order,
)
from tools.qmt_operations_task_contract import TASKS as QMT_OPERATIONS_TASKS


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
            ).mappings()
        ]


def _require_unique_task(engine) -> list[dict[str, Any]]:
    rows = _matching_tasks(engine)
    if len(rows) > 1:
        raise RuntimeError(
            "QMT announcement scheduler identity is not unique: "
            f"{len(rows)} matching rows"
        )
    return rows


def _matching_operation_tasks(engine) -> list[dict[str, Any]]:
    predicates: list[str] = []
    params: dict[str, str] = {}
    for index, task in enumerate(QMT_OPERATIONS_TASKS):
        predicates.append(
            f"task_type=:task_type_{index} OR script_path=:script_path_{index}"
        )
        params[f"task_type_{index}"] = str(task["task_type"])
        params[f"script_path_{index}"] = str(task["script_path"])
    with engine.connect() as connection:
        return [
            dict(row)
            for row in connection.execute(
                text(
                    "SELECT * FROM st_scheduled_tasks WHERE "
                    + " OR ".join(f"({item})" for item in predicates)
                    + " ORDER BY id"
                ),
                params,
            ).mappings()
        ]


def _require_unique_operation_tasks(engine) -> list[dict[str, Any]]:
    rows = _matching_operation_tasks(engine)
    matched_ids: list[int] = []
    for task in QMT_OPERATIONS_TASKS:
        matches = [
            row
            for row in rows
            if str(row.get("task_type") or "") == task["task_type"]
            or str(row.get("script_path") or "") == task["script_path"]
        ]
        if len(matches) > 1:
            raise RuntimeError(
                "QMT operations scheduler identity is not unique: "
                f"{task['task_type']} has {len(matches)} rows"
            )
        if matches:
            matched_ids.append(int(matches[0].get("id") or 0))
    if len(matched_ids) != len(set(matched_ids)) or any(
        item <= 0 for item in matched_ids
    ):
        raise RuntimeError("QMT operations scheduler identities overlap")
    return rows


def _write_snapshot(
    path: Path,
    rows: list[dict[str, Any]],
    operation_rows: list[dict[str, Any]] | None = None,
) -> None:
    if path.exists() and path.stat().st_size:
        raise RuntimeError(f"refusing to overwrite non-empty snapshot: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema": "probiga.qmt-announcement-task-snapshot.v1",
        "task_type": TASK["task_type"],
        "script_path": TASK["script_path"],
        "rows": json.loads(
            json.dumps(rows, ensure_ascii=False, default=str, sort_keys=True)
        ),
        "operations": {
            "task_types": sorted(
                str(task["task_type"]) for task in QMT_OPERATIONS_TASKS
            ),
            "script_paths": sorted(
                str(task["script_path"]) for task in QMT_OPERATIONS_TASKS
            ),
            "rows": json.loads(
                json.dumps(
                    operation_rows or [],
                    ensure_ascii=False,
                    default=str,
                    sort_keys=True,
                )
            ),
        },
    }
    fd, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _read_snapshot(path: Path) -> dict[str, Any]:
    raw_text = (
        sys.stdin.buffer.read().decode("utf-8")
        if str(path) == "-"
        else path.read_text(encoding="utf-8")
    )
    payload = json.loads(raw_text)
    if (
        not isinstance(payload, dict)
        or payload.get("schema")
        != "probiga.qmt-announcement-task-snapshot.v1"
        or payload.get("task_type") != TASK["task_type"]
        or payload.get("script_path") != TASK["script_path"]
        or not isinstance(payload.get("rows"), list)
        or len(payload["rows"]) > 1
        or not isinstance(payload.get("operations"), dict)
        or payload["operations"].get("task_types")
        != sorted(str(task["task_type"]) for task in QMT_OPERATIONS_TASKS)
        or payload["operations"].get("script_paths")
        != sorted(str(task["script_path"]) for task in QMT_OPERATIONS_TASKS)
        or not isinstance(payload["operations"].get("rows"), list)
        or len(payload["operations"]["rows"]) > len(QMT_OPERATIONS_TASKS)
    ):
        raise RuntimeError("invalid QMT announcement task snapshot")
    return payload


def _verify_snapshot(engine, path: Path) -> dict[str, Any]:
    payload = _read_snapshot(path)
    expected = payload["rows"]
    expected_operations = payload["operations"]["rows"]
    observed = json.loads(
        json.dumps(
            _require_unique_task(engine),
            ensure_ascii=False,
            default=str,
            sort_keys=True,
        )
    )
    if observed != expected:
        raise RuntimeError("QMT announcement task differs from sealed snapshot")
    observed_operations = json.loads(
        json.dumps(
            _require_unique_operation_tasks(engine),
            ensure_ascii=False,
            default=str,
            sort_keys=True,
        )
    )
    if observed_operations != expected_operations:
        raise RuntimeError("QMT operations tasks differ from sealed snapshot")
    return {
        "verified": True,
        "row_count": len(observed),
        "operation_row_count": len(observed_operations),
    }


def _restore_snapshot(engine, path: Path) -> dict[str, Any]:
    payload = _read_snapshot(path)
    prior_rows = payload["rows"]
    prior_operation_rows = payload["operations"]["rows"]
    current_rows = _require_unique_task(engine)
    current_operation_rows = _require_unique_operation_tasks(engine)
    columns = table_columns(engine)
    if not columns:
        raise RuntimeError("st_scheduled_tasks does not exist")
    identities = ((TASK, prior_rows, current_rows),) + tuple(
        (
            task,
            [
                row
                for row in prior_operation_rows
                if str(row.get("task_type") or "") == task["task_type"]
                or str(row.get("script_path") or "") == task["script_path"]
            ],
            [
                row
                for row in current_operation_rows
                if str(row.get("task_type") or "") == task["task_type"]
                or str(row.get("script_path") or "") == task["script_path"]
            ],
        )
        for task in QMT_OPERATIONS_TASKS
    )
    actions: dict[str, str] = {}
    with engine.begin() as connection:
        for task, prior_matches, current_matches in identities:
            task_type = str(task["task_type"])
            predicate = "task_type=:task_type OR script_path=:script_path"
            identity = {
                "task_type": task_type,
                "script_path": str(task["script_path"]),
            }
            if len(prior_matches) > 1 or len(current_matches) > 1:
                raise RuntimeError(
                    f"QMT task rollback identity is not unique: {task_type}"
                )
            if not prior_matches:
                connection.execute(
                    text(f"DELETE FROM st_scheduled_tasks WHERE {predicate}"),
                    identity,
                )
                actions[task_type] = "deleted_new_task"
                continue
            prior = prior_matches[0]
            if not isinstance(prior, dict) or "id" not in prior:
                raise RuntimeError(
                    f"QMT task snapshot row is missing its id: {task_type}"
                )
            compatible = {
                str(key): value
                for key, value in prior.items()
                if str(key) in columns
            }
            prior_id = int(compatible["id"])
            if current_matches:
                current_id = int(current_matches[0].get("id") or 0)
                if current_id != prior_id:
                    raise RuntimeError(
                        f"QMT task identity changed during rollback: {task_type}"
                    )
                update_payload = {
                    key: value
                    for key, value in compatible.items()
                    if key != "id"
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
                actions[task_type] = "restored_existing_task"
                continue
            names = ", ".join(quote_identifier(key) for key in compatible)
            values = ", ".join(f":{key}" for key in compatible)
            connection.execute(
                text(
                    f"INSERT INTO st_scheduled_tasks ({names}) VALUES ({values})"
                ),
                compatible,
            )
            actions[task_type] = "reinserted_previous_task"
    return {
        "action": actions[TASK["task_type"]],
        "actions": actions,
        "operation_row_count": len(prior_operation_rows),
    }


def install(engine, *, disabled: bool = False) -> dict:
    _require_unique_task(engine)
    with engine.connect() as connection:
        rows = [
            dict(row)
            for row in connection.execute(
                text(
                    "SELECT task_type, cron_time FROM st_scheduled_tasks "
                    "WHERE task_type IN "
                    "('analysis_upper_evidence_prepare','analysis_fast',"
                    "'strategy_governance_daily') "
                    "ORDER BY task_type"
                )
            ).mappings()
        ]
    by_type: dict[str, list[str]] = {}
    for row in rows:
        task_type = str(row.get("task_type") or "")
        by_type.setdefault(task_type, []).append(
            str(row.get("cron_time") or "")[:5]
        )
    duplicates = sorted(
        task_type for task_type, values in by_type.items() if len(values) != 1
    )
    if duplicates:
        raise RuntimeError(
            "duplicate scheduler dependency: " + ", ".join(duplicates)
        )
    expected_crons = {
        "analysis_upper_evidence_prepare": ANALYSIS_UPPER_EVIDENCE_CRON,
        "analysis_fast": ANALYSIS_FAST_CRON,
        "strategy_governance_daily": STRATEGY_GOVERNANCE_CRON,
    }
    observed_crons = {
        task_type: values[0] for task_type, values in by_type.items()
    }
    order = validate_pipeline_order()
    if not disabled:
        drift = {
            task_type: {
                "expected": expected,
                "actual": observed_crons.get(task_type),
            }
            for task_type, expected in expected_crons.items()
            if observed_crons.get(task_type) != expected
        }
        if drift:
            raise RuntimeError(
                "daily strategy pipeline scheduler contract differs: "
                + json.dumps(drift, ensure_ascii=False, sort_keys=True)
            )
    task = {**TASK, "enabled": 0 if disabled else 1}
    result = upsert_scheduler_task(
        engine,
        task,
        lookup_where="task_type=:task_type OR script_path=:script_path",
        lookup_params={
            "task_type": TASK["task_type"],
            "script_path": TASK["script_path"],
        },
    )
    return {
        "schema": "probiga.qmt-announcement-task-install.v1",
        "status": "ok",
        "task": task,
        "pipeline_order": order,
        "pipeline_schedule": {
            "expected": expected_crons,
            "observed_before_install": observed_crons,
            "validated": not disabled,
        },
        "result": result,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--disabled", action="store_true")
    parser.add_argument(
        "--capture-snapshot",
        default="",
        help="只读捕获变更前唯一QMT公告任务行",
    )
    parser.add_argument(
        "--verify-snapshot",
        default="",
        help="逐字段验证当前任务与密封快照一致；传 - 从stdin读取",
    )
    parser.add_argument(
        "--restore-snapshot",
        default="",
        help="精确恢复变更前任务；传 - 从stdin读取",
    )
    args = parser.parse_args(argv)
    snapshot_modes = [
        bool(args.capture_snapshot),
        bool(args.verify_snapshot),
        bool(args.restore_snapshot),
    ]
    if sum(snapshot_modes) > 1:
        parser.error("snapshot capture, verification and restore are exclusive")
    if args.disabled and any(snapshot_modes):
        parser.error("snapshot-only modes cannot be combined with --disabled")
    from tools.env_config import create_tool_engine, load_project_env

    load_project_env()
    engine = create_tool_engine()
    try:
        if args.capture_snapshot:
            rows = _require_unique_task(engine)
            operation_rows = _require_unique_operation_tasks(engine)
            _write_snapshot(Path(args.capture_snapshot), rows, operation_rows)
            payload = {
                "schema": "probiga.qmt-announcement-task-snapshot-result.v1",
                "status": "ok",
                "action": "captured",
                "row_count": len(rows),
                "operation_row_count": len(operation_rows),
            }
        elif args.verify_snapshot:
            payload = {
                "schema": "probiga.qmt-announcement-task-snapshot-result.v1",
                "status": "ok",
                "action": "verified",
                "result": _verify_snapshot(
                    engine, Path(args.verify_snapshot)
                ),
            }
        elif args.restore_snapshot:
            payload = {
                "schema": "probiga.qmt-announcement-task-snapshot-result.v1",
                "status": "ok",
                "action": "restored",
                "result": _restore_snapshot(
                    engine, Path(args.restore_snapshot)
                ),
            }
        else:
            payload = install(engine, disabled=args.disabled)
    finally:
        engine.dispose()
    print(json.dumps(payload, ensure_ascii=False, default=str, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
