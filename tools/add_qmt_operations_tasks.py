#!/usr/bin/env python3
"""Install or restore the five frozen QMT foundation scheduler tasks."""
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
from server.common.scheduler_tasks import table_columns, upsert_scheduler_task
from tools.qmt_operations_task_contract import TASKS


SNAPSHOT_SCHEMA = "probiga.qmt-operations-task-snapshot.v1"


def _matching_tasks(engine) -> list[dict[str, Any]]:
    predicates: list[str] = []
    params: dict[str, str] = {}
    for index, task in enumerate(TASKS):
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


def _require_unique_tasks(engine) -> list[dict[str, Any]]:
    rows = _matching_tasks(engine)
    matched_ids: list[int] = []
    for task in TASKS:
        matches = [
            row
            for row in rows
            if str(row.get("task_type") or "") == task["task_type"]
            or str(row.get("script_path") or "") == task["script_path"]
        ]
        if len(matches) > 1:
            raise RuntimeError(
                "QMT operations scheduler identity is not unique: "
                f"{task['task_type']} has {len(matches)} matching rows"
            )
        if matches:
            matched_ids.append(int(matches[0].get("id") or 0))
    if len(matched_ids) != len(set(matched_ids)) or any(
        item <= 0 for item in matched_ids
    ):
        raise RuntimeError("QMT operations scheduler identities overlap")
    return rows


def _snapshot_payload(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema": SNAPSHOT_SCHEMA,
        "task_types": sorted(str(task["task_type"]) for task in TASKS),
        "script_paths": sorted(str(task["script_path"]) for task in TASKS),
        "rows": json.loads(
            json.dumps(rows, ensure_ascii=False, default=str, sort_keys=True)
        ),
    }


def _write_snapshot(path: Path, rows: list[dict[str, Any]]) -> None:
    if path.exists() and path.stat().st_size:
        raise RuntimeError(f"refusing to overwrite non-empty snapshot: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(
                _snapshot_payload(rows),
                handle,
                ensure_ascii=False,
                sort_keys=True,
            )
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
        or payload.get("schema") != SNAPSHOT_SCHEMA
        or payload.get("task_types")
        != sorted(str(task["task_type"]) for task in TASKS)
        or payload.get("script_paths")
        != sorted(str(task["script_path"]) for task in TASKS)
        or not isinstance(payload.get("rows"), list)
        or len(payload["rows"]) > len(TASKS)
    ):
        raise RuntimeError("invalid QMT operations task snapshot")
    return payload


def _verify_snapshot(engine, path: Path) -> dict[str, Any]:
    expected = _read_snapshot(path)["rows"]
    observed = _snapshot_payload(_require_unique_tasks(engine))["rows"]
    if observed != expected:
        raise RuntimeError("QMT operations tasks differ from sealed snapshot")
    return {"verified": True, "row_count": len(observed)}


def _restore_snapshot(engine, path: Path) -> dict[str, Any]:
    prior_rows = _read_snapshot(path)["rows"]
    current_rows = _require_unique_tasks(engine)
    columns = table_columns(engine)
    if not columns:
        raise RuntimeError("st_scheduled_tasks does not exist")
    actions: dict[str, str] = {}
    with engine.begin() as connection:
        for task in TASKS:
            task_type = str(task["task_type"])
            script_path = str(task["script_path"])
            prior = [
                row
                for row in prior_rows
                if str(row.get("task_type") or "") == task_type
                or str(row.get("script_path") or "") == script_path
            ]
            current = [
                row
                for row in current_rows
                if str(row.get("task_type") or "") == task_type
                or str(row.get("script_path") or "") == script_path
            ]
            if len(prior) > 1 or len(current) > 1:
                raise RuntimeError(
                    f"QMT operations rollback identity is not unique: {task_type}"
                )
            predicate = "task_type=:task_type OR script_path=:script_path"
            identity = {"task_type": task_type, "script_path": script_path}
            if not prior:
                connection.execute(
                    text(f"DELETE FROM st_scheduled_tasks WHERE {predicate}"),
                    identity,
                )
                actions[task_type] = "deleted_new_task"
                continue
            row = prior[0]
            if not isinstance(row, dict) or "id" not in row:
                raise RuntimeError(
                    f"QMT operations snapshot row has no id: {task_type}"
                )
            compatible = {
                str(key): value
                for key, value in row.items()
                if str(key) in columns
            }
            prior_id = int(compatible["id"])
            if current:
                if int(current[0].get("id") or 0) != prior_id:
                    raise RuntimeError(
                        f"QMT operations task identity changed: {task_type}"
                    )
                update = {
                    key: value for key, value in compatible.items() if key != "id"
                }
                assignments = ", ".join(
                    f"{quote_identifier(key)}=:{key}" for key in update
                )
                connection.execute(
                    text(
                        f"UPDATE st_scheduled_tasks SET {assignments} "
                        "WHERE id=:restore_id"
                    ),
                    {**update, "restore_id": prior_id},
                )
                actions[task_type] = "restored_existing_task"
            else:
                names = ", ".join(
                    quote_identifier(key) for key in compatible
                )
                values = ", ".join(f":{key}" for key in compatible)
                connection.execute(
                    text(
                        f"INSERT INTO st_scheduled_tasks ({names}) "
                        f"VALUES ({values})"
                    ),
                    compatible,
                )
                actions[task_type] = "reinserted_previous_task"
    return {"actions": actions, "row_count": len(prior_rows)}


def install(engine, *, disabled: bool = False) -> dict[str, Any]:
    _require_unique_tasks(engine)
    results: dict[str, Any] = {}
    for frozen in TASKS:
        task = {**frozen, "enabled": 0 if disabled else 1}
        results[str(task["task_type"])] = upsert_scheduler_task(
            engine,
            task,
            lookup_where="task_type=:task_type OR script_path=:script_path",
            lookup_params={
                "task_type": task["task_type"],
                "script_path": task["script_path"],
            },
        )
    return {
        "schema": "probiga.qmt-operations-task-install.v1",
        "status": "ok",
        "enabled": not disabled,
        "task_count": len(TASKS),
        "results": results,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--disabled", action="store_true")
    parser.add_argument("--capture-snapshot", default="")
    parser.add_argument("--verify-snapshot", default="")
    parser.add_argument("--restore-snapshot", default="")
    args = parser.parse_args(argv)
    snapshot_modes = [
        bool(args.capture_snapshot),
        bool(args.verify_snapshot),
        bool(args.restore_snapshot),
    ]
    if sum(snapshot_modes) > 1:
        parser.error("snapshot capture, verification and restore are exclusive")
    if args.disabled and any(snapshot_modes):
        parser.error("snapshot modes cannot be combined with --disabled")
    from tools.env_config import create_tool_engine, load_project_env

    load_project_env()
    engine = create_tool_engine()
    try:
        if args.capture_snapshot:
            rows = _require_unique_tasks(engine)
            _write_snapshot(Path(args.capture_snapshot), rows)
            payload = {"status": "ok", "action": "captured", "row_count": len(rows)}
        elif args.verify_snapshot:
            payload = {
                "status": "ok",
                "action": "verified",
                "result": _verify_snapshot(engine, Path(args.verify_snapshot)),
            }
        elif args.restore_snapshot:
            payload = {
                "status": "ok",
                "action": "restored",
                "result": _restore_snapshot(engine, Path(args.restore_snapshot)),
            }
        else:
            payload = install(engine, disabled=args.disabled)
    finally:
        engine.dispose()
    print(json.dumps(payload, ensure_ascii=False, default=str, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
