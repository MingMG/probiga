#!/usr/bin/env python3
"""Apply the public-market data profile without changing historical receipts."""
from __future__ import annotations

import argparse
from collections.abc import Mapping
from pathlib import Path
import json
import os
import sys
import tempfile

from sqlalchemy import inspect, text


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.env_config import create_tool_engine, load_project_env
from tools.qmt_host_ownership_contract import (
    LINUX_QMT_TASKS_BY_TYPE,
    LINUX_PROVIDER_TASKS,
    WINDOWS_QMT_EDGE_TASKS_BY_TYPE,
)


PROFILE = "public_market_only"
PUBLIC_ENV: dict[str, str] = {
    "MINUTE_DATA_SOURCE": "legacy",
    "DATA_SOURCE_KLINE": "adata",
    "DATA_SOURCE_MINUTE": "adata",
    "DATA_SOURCE_CURRENT": "adata",
    "DATA_SOURCE_INDEX_KLINE": "adata",
    "DATA_SOURCE_INDEX_MINUTE": "adata",
    "DATA_SOURCE_INDEX_CURRENT": "adata",
    "DATA_SOURCE_CONCEPT_KLINE": "east",
    "DATA_SOURCE_CONCEPT_MINUTE": "east",
    "DATA_SOURCE_CONCEPT_CURRENT": "east",
    "DATA_SOURCE_CONCEPT_FLOW": "east",
    "DATA_SOURCE_CODE_LIST": "adata",
    "DATA_SOURCE_INDEX_LIST": "sina",
    "DATA_SOURCE_CONCEPT_LIST": "east",
    "DATA_SOURCE_FLOW_DAILY": "adata",
    "DATA_SOURCE_FLOW_MIN": "adata",
    "SI_ALL_CODE_SOURCE": "adata",
    "SI_ALL_INDEX_CODE_SOURCE": "sina",
    "SI_INDEX_CONSTITUENT_SOURCE": "sina",
    "SI_CONCEPT_SOURCE": "east",
    "SI_INDUSTRY_SOURCE": "adata",
    "BIG_QMT_BRIDGE_ENABLED": "false",
    "BIG_QMT_STRATEGY_AUTO_RECOVER": "false",
    "QMT_CLIENT_AUTO_RESTART": "false",
    "LEGACY_MINIQMT_ENABLED": "false",
    "QMT_GATEWAY_ENABLED": "0",
    "QMT_GATEWAY_REQUIRED": "0",
    "QMT_LIVE_POLL_ENABLED": "false",
    "LIVE_QUOTE_POLL_ENABLED": "false",
    "API_QMT_LIVE_RUNTIME_ENABLED": "false",
}
PUBLIC_TASK_TYPES = frozenset(
    {
        "portfolio_quote_refresh",
        "public_quote_failover",
        "intraday_realtime",
        "intraday_minute_kline",
        "intraday_minute_flow",
        "free_stock_daily_canonical",
        "free_index_daily_canonical",
        "stock_minute",
        "stock_minute_flow",
        "eastmoney_concept_current",
        "eastmoney_concept_kline",
        "eastmoney_concept_minute",
        "eastmoney_concept_flow_snapshot",
        "linux_recent_data_gap_repair",
    }
)
NON_QMT_WINDOWS_TASK_TYPES = frozenset(
    {
        # MyQuant upper-limit evidence is independent of QMT inner Python.
        "analysis_upper_evidence_prepare",
    }
)
RETIRED_QMT_TASK_TYPES = frozenset(
    set(WINDOWS_QMT_EDGE_TASKS_BY_TYPE)
    - NON_QMT_WINDOWS_TASK_TYPES
) | frozenset(LINUX_QMT_TASKS_BY_TYPE)
RETIRED_QMT_TASKS: dict[str, Mapping[str, object]] = {
    task_type: WINDOWS_QMT_EDGE_TASKS_BY_TYPE[task_type]
    for task_type in RETIRED_QMT_TASK_TYPES
    if task_type in WINDOWS_QMT_EDGE_TASKS_BY_TYPE
}
RETIRED_QMT_TASKS.update(
    {
        task_type: LINUX_QMT_TASKS_BY_TYPE[task_type]
        for task_type in RETIRED_QMT_TASK_TYPES
        if task_type in LINUX_QMT_TASKS_BY_TYPE
    }
)
_TASK_COLUMNS = (
    "task_name",
    "task_type",
    "group_name",
    "script_path",
    "script_args",
    "cron_time",
    "interval_minutes",
    "enabled",
    "date_param",
    "sort_order",
    "description",
)


def update_env_profile(
    path: Path,
    updates: Mapping[str, str] = PUBLIC_ENV,
) -> list[str]:
    """Atomically update profile keys while retaining every unrelated value."""

    source_lines = path.read_text(encoding="utf-8").splitlines()
    output: list[str] = []
    found: set[str] = set()
    changed: list[str] = []
    for line in source_lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            output.append(line)
            continue
        raw_key = stripped.split("=", 1)[0].strip()
        key = raw_key[7:].lstrip() if raw_key.startswith("export ") else raw_key
        if key not in updates:
            output.append(line)
            continue
        found.add(key)
        replacement = f"{key}={updates[key]}"
        current_value = stripped.split("=", 1)[1].strip()
        if current_value != updates[key]:
            changed.append(key)
            output.append(replacement)
        else:
            output.append(line)
    for key, value in updates.items():
        if key not in found:
            output.append(f"{key}={value}")
            changed.append(key)
    if not changed:
        return []
    temporary: Path | None = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            dir=path.parent,
            prefix=f".{path.name}.",
            text=True,
        )
        temporary = Path(temporary_name)
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write("\n".join(output) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, path.stat().st_mode & 0o777)
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return changed


def canonical_public_tasks() -> dict[str, Mapping[str, object]]:
    from tools.ensure_quality_gate import TASKS

    definitions: dict[str, Mapping[str, object]] = {}
    for task in (*TASKS, *LINUX_PROVIDER_TASKS):
        task_type = str(task.get("task_type") or "")
        if task_type not in PUBLIC_TASK_TYPES:
            continue
        if task_type in definitions and dict(definitions[task_type]) != dict(task):
            raise RuntimeError(f"public task definition differs: {task_type}")
        definitions[task_type] = task
    missing = PUBLIC_TASK_TYPES - set(definitions)
    if missing:
        raise RuntimeError(
            "public task definitions are missing: " + ", ".join(sorted(missing))
        )
    return definitions


def _task_payload(task: Mapping[str, object], columns: set[str]) -> dict:
    return {
        key: value
        for key, value in task.items()
        if key in _TASK_COLUMNS and key in columns
    }


def _upsert_public_task(connection, task: Mapping[str, object], columns: set[str]) -> str:
    payload = _task_payload(task, columns)
    rows = connection.execute(
        text(
            "SELECT id,task_type,script_path FROM st_scheduled_tasks "
            "WHERE task_type=:task_type"
        ),
        {"task_type": task["task_type"]},
    ).mappings().all()
    if len(rows) > 1:
        raise RuntimeError(f"duplicate public task identity: {task['task_type']}")
    if rows:
        payload["enabled"] = 1
        assignments = ", ".join(f"{key}=:{key}" for key in payload)
        if "updated_at" in columns:
            assignments += ", updated_at=CURRENT_TIMESTAMP"
        result = connection.execute(
            text(
                f'UPDATE st_scheduled_tasks SET {assignments} WHERE id=:task_id'
            ),
            {**payload, "task_id": rows[0]["id"]},
        )
        if result.rowcount != 1:
            raise RuntimeError(f"public task update failed: {task['task_type']}")
        return "updated"
    payload["enabled"] = 1
    quoted = ", ".join(payload)
    values = ", ".join(f":{key}" for key in payload)
    if "updated_at" in columns:
        quoted += ", updated_at"
        values += ", CURRENT_TIMESTAMP"
    connection.execute(
        text(f"INSERT INTO st_scheduled_tasks ({quoted}) VALUES ({values})"),
        payload,
    )
    return "inserted"


def apply_task_profile(connection) -> dict:
    inspector = inspect(connection)
    columns = {
        str(column["name"])
        for column in inspector.get_columns("st_scheduled_tasks")
    }
    retired = []
    for task_type, expected in sorted(RETIRED_QMT_TASKS.items()):
        rows = connection.execute(
            text(
                "SELECT id,task_type,script_path,enabled FROM st_scheduled_tasks "
                "WHERE task_type=:task_type"
            ),
            {"task_type": task_type},
        ).mappings().all()
        if len(rows) > 1:
            raise RuntimeError(f"duplicate QMT task identity: {task_type}")
        if not rows:
            continue
        row = rows[0]
        expected_script = str(expected["script_path"]).replace("\\", "/")
        observed_script = str(row["script_path"] or "").replace("\\", "/")
        if observed_script != expected_script:
            raise RuntimeError(f"QMT task script identity differs: {task_type}")
        if int(row["enabled"] or 0) == 1:
            result = connection.execute(
                text(
                    "UPDATE st_scheduled_tasks SET enabled=0"
                    + (", updated_at=CURRENT_TIMESTAMP" if "updated_at" in columns else "")
                    + " WHERE id=:task_id AND task_type=:task_type "
                    "AND script_path=:script_path AND enabled=1"
                ),
                {
                    "task_id": row["id"],
                    "task_type": task_type,
                    "script_path": row["script_path"],
                },
            )
            if result.rowcount != 1:
                raise RuntimeError(f"QMT task retirement identity changed: {task_type}")
            retired.append({"id": int(row["id"]), "task_type": task_type})
    public_actions = {}
    for task_type, task in sorted(canonical_public_tasks().items()):
        public_actions[task_type] = _upsert_public_task(connection, task, columns)
    return {
        "status": "PASS",
        "profile": PROFILE,
        "retired": retired,
        "public_tasks": public_actions,
        "history_preserved": True,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    env_path = Path(args.env_file).resolve()
    planned_env_changes = [
        key
        for key, value in PUBLIC_ENV.items()
        if key not in env_path.read_text(encoding="utf-8")
        or next(
            (
                line.split("=", 1)[1].strip()
                for line in env_path.read_text(encoding="utf-8").splitlines()
                if line.strip().startswith(f"{key}=")
            ),
            None,
        )
        != value
    ]
    result = {
        "status": "PLANNED" if not args.apply else "PASS",
        "profile": PROFILE,
        "env_file": str(env_path),
        "env_changes": planned_env_changes,
        "public_task_types": sorted(PUBLIC_TASK_TYPES),
        "retired_qmt_task_types": sorted(RETIRED_QMT_TASK_TYPES),
    }
    if not args.apply:
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0
    changed = update_env_profile(env_path, PUBLIC_ENV)
    load_project_env(env_path)
    engine = create_tool_engine()
    try:
        with engine.begin() as connection:
            result.update(apply_task_profile(connection))
    finally:
        engine.dispose()
    result["env_changes"] = changed
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
