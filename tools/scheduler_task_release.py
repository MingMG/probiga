"""Content-scoped scheduler contract publication and exact before-image recovery."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import threading

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SCHEMA = "probiga.scheduler-task-release-plan.v1"
SNAPSHOT_SCHEMA = "probiga.scheduler-task-release-before.v1"
FIELDS = frozenset({"task_name", "task_type", "group_name", "script_path", "script_args",
                    "cron_time", "interval_minutes", "enabled", "description", "sort_order", "date_param"})
MAX_BYTES = 1024 * 1024


def _hash(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def normalize_tasks(tasks):
    if isinstance(tasks, dict):
        if any(key != value.get("task_type") for key, value in tasks.items()):
            raise ValueError("TASK_RELEASE_MAP_IDENTITY_DIFFERS")
        tasks = list(tasks.values())
    if not isinstance(tasks, (list, tuple)):
        raise ValueError("TASK_RELEASE_CONTRACTS_MALFORMED")
    result, names = {}, set()
    for task in tasks:
        if not isinstance(task, dict) or set(task) != FIELDS:
            raise ValueError("TASK_RELEASE_CONTRACT_FIELDS_DIFFER")
        kind = task["task_type"]
        if (not isinstance(kind, str) or re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,63}", kind) is None
                or kind in result or not isinstance(task["task_name"], str)
                or not task["task_name"] or task["task_name"] in names):
            raise ValueError("TASK_RELEASE_CONTRACT_IDENTITY_AMBIGUOUS")
        if any(not isinstance(task[key], str) for key in FIELDS - {"enabled", "interval_minutes", "sort_order"}):
            raise ValueError("TASK_RELEASE_CONTRACT_VALUE_INVALID")
        if (type(task["enabled"]) is not int or task["enabled"] not in {0, 1}
                or any(type(task[key]) is not int for key in ("interval_minutes", "sort_order"))):
            raise ValueError("TASK_RELEASE_CONTRACT_INTEGER_INVALID")
        names.add(task["task_name"])
        result[kind] = {key: task[key] for key in sorted(FIELDS)}
    return dict(sorted(result.items()))


def build_plan(previous_tasks, target_tasks, *, previous_sha, target_sha):
    if any(re.fullmatch(r"[0-9a-f]{40}", str(value)) is None or value == "0" * 40 for value in (previous_sha, target_sha)):
        raise ValueError("TASK_RELEASE_BUILD_IDENTITY_INVALID")
    base, target = normalize_tasks(previous_tasks), normalize_tasks(target_tasks)
    changed = sorted(key for key in base.keys() & target.keys() if base[key] != target[key])
    added, retired = sorted(target.keys() - base.keys()), sorted(base.keys() - target.keys())
    plan = {"schema": SCHEMA, "previous_sha": previous_sha, "target_sha": target_sha,
            "base_tasks": base, "target_tasks": target, "base_task_sha256": _hash(base),
            "target_task_sha256": _hash(target), "changed_task_types": changed,
            "new_task_types": added, "retired_task_types": retired,
            "upsert_task_types": sorted(changed + added)}
    plan["plan_sha256"] = _hash(plan)
    return plan


def validate_plan(plan):
    if not isinstance(plan, dict) or plan != build_plan(plan.get("base_tasks"), plan.get("target_tasks"),
            previous_sha=plan.get("previous_sha"), target_sha=plan.get("target_sha")):
        raise ValueError("TASK_RELEASE_PLAN_SEAL_OR_SCOPE_DIFFERS")
    return plan


def _scope(plan):
    validate_plan(plan)
    return sorted(plan["upsert_task_types"] + plan["retired_task_types"])


def _current(plan):
    validate_plan(plan)
    expected = os.environ.get("PROBIGA_EXPECTED_GIT_SHA", "")
    if expected and expected != plan["target_sha"]:
        raise ValueError("TASK_RELEASE_TARGET_BUILD_DIFFERS")
    from tools import ensure_quality_gate as gate
    current = normalize_tasks([{key: task[key] for key in FIELDS} for task in gate.TASKS])
    if current != plan["target_tasks"]:
        raise ValueError("TASK_RELEASE_TARGET_SOURCE_CONTRACT_DIFFERS")
    return gate


def _read_rows(connection, plan, *, lock=False):
    from sqlalchemy import bindparam, text
    scope = _scope(plan)
    names = sorted({plan[which][key]["task_name"] for key in scope for which in ("base_tasks", "target_tasks") if key in plan[which]})
    suffix = " FOR UPDATE" if lock and connection.dialect.name == "mysql" else ""
    query = text("SELECT id," + ",".join(sorted(FIELDS)) + " FROM st_scheduled_tasks "
                 "WHERE task_type IN :types OR task_name IN :names ORDER BY id" + suffix).bindparams(
                     bindparam("types", expanding=True), bindparam("names", expanding=True))
    result = {}
    for row in connection.execute(query, {"types": scope, "names": names}).mappings():
        row = dict(row)
        kind = row["task_type"]
        if kind not in scope or kind in result:
            raise ValueError("TASK_RELEASE_DATABASE_IDENTITY_AMBIGUOUS")
        allowed_paths = {plan[which][kind]["script_path"] for which in ("base_tasks", "target_tasks") if kind in plan[which]}
        if row["script_path"] not in allowed_paths:
            raise ValueError("TASK_RELEASE_DATABASE_EXECUTOR_IDENTITY_DIFFERS")
        result[kind] = row
    return {key: result.get(key) for key in scope}


def capture_before(engine, plan):
    scope = _scope(plan)
    _current(plan)
    if scope:
        with engine.connect() as connection:
            rows = _read_rows(connection, plan)
    else:
        rows = {}
    result = {"schema": SNAPSHOT_SCHEMA, "plan_sha256": plan["plan_sha256"], "rows": rows}
    result["snapshot_sha256"] = _hash(result)
    return result


def _snapshot(plan, snapshot):
    if (not isinstance(snapshot, dict) or snapshot.get("schema") != SNAPSHOT_SCHEMA
            or snapshot.get("plan_sha256") != plan["plan_sha256"]
            or set(snapshot.get("rows", {})) != set(_scope(plan))
            or snapshot.get("snapshot_sha256") != _hash({k:v for k,v in snapshot.items() if k != "snapshot_sha256"})):
        raise ValueError("TASK_RELEASE_BEFORE_IMAGE_DIFFERS")
    return snapshot


def _retired_rows(connection, plan, *, disable=False):
    from sqlalchemy import text
    rows = _read_rows(connection, plan, lock=disable)
    audit = []
    for kind in plan["retired_task_types"]:
        row = rows[kind]
        if row is None:
            raise ValueError("TASK_RELEASE_RETIRED_IDENTITY_MISSING")
        expected = dict(plan["base_tasks"][kind])
        actual = {key: row[key] for key in FIELDS}
        if any(actual[key] != expected[key] for key in FIELDS - {"enabled"}) or actual["enabled"] not in {0,1}:
            raise ValueError("TASK_RELEASE_RETIRED_IDENTITY_UNPROVEN")
        if disable and row["enabled"]:
            changed = connection.execute(text("UPDATE st_scheduled_tasks SET enabled=0 WHERE id=:id AND task_type=:kind AND script_path=:path AND enabled=1"),
                {"id":row["id"],"kind":kind,"path":row["script_path"]})
            if changed.rowcount != 1:
                raise ValueError("TASK_RELEASE_RETIRED_CAS_LOST")
        elif not disable and row["enabled"] != 0:
            raise ValueError("TASK_RELEASE_RETIRED_STILL_ENABLED")
        audit.append({"task_type": kind, "id": row["id"], "before_enabled": row["enabled"], "after_enabled": 0})
    return audit


def apply_plan(engine, plan):
    gate = _current(plan)
    if not _scope(plan):
        return {"status": "NOOP", "plan_sha256": plan["plan_sha256"], "upserted": [], "retired": []}
    # Validate retirement identities before any target-contract mutation.
    if plan["retired_task_types"]:
        with engine.begin() as connection:
            retired = _retired_rows(connection, plan, disable=True)
    else:
        retired = []
    if plan["upsert_task_types"]:
        gate.run(engine, task_types=plan["upsert_task_types"])
    validate_applied_plan(engine, plan)
    return {"status": "APPLIED", "plan_sha256": plan["plan_sha256"], "upserted": plan["upsert_task_types"], "retired": retired}


def validate_applied_plan(engine, plan):
    gate = _current(plan)
    if not _scope(plan):
        return {"status": "NOOP", "plan_sha256": plan["plan_sha256"]}
    if plan["upsert_task_types"]:
        gate.validate_managed_task_contracts(engine, task_types=plan["upsert_task_types"])
    if plan["retired_task_types"]:
        with engine.connect() as connection:
            _retired_rows(connection, plan)
    return {"status": "VALIDATED", "plan_sha256": plan["plan_sha256"]}


def restore_before(engine, plan, snapshot):
    from sqlalchemy import text
    scope = _scope(plan)
    _snapshot(plan, snapshot)
    _current(plan)
    if not scope:
        return {"status": "NOOP", "plan_sha256": plan["plan_sha256"]}
    with engine.begin() as connection:
        current = _read_rows(connection, plan, lock=True)
        for kind in scope:
            original, row = snapshot["rows"][kind], current[kind]
            if original == row:
                continue
            if row is None:
                raise ValueError("TASK_RELEASE_ROLLBACK_IDENTITY_MISSING")
            actual = {key:row[key] for key in FIELDS}
            target = plan["target_tasks"].get(kind)
            if target is not None:
                target = dict(target)
                from tools.qmt_host_ownership_contract import WINDOWS_QMT_EDGE_TASK_TYPES
                if kind in WINDOWS_QMT_EDGE_TASK_TYPES:
                    target["enabled"] = original["enabled"] if original is not None else 0
                if original is None and actual == {**target, "enabled":0}:
                    continue
            retired = {**plan["base_tasks"].get(kind, {}), "enabled":0}
            if actual not in (target, retired) or (original is not None and row["id"] != original["id"]):
                raise ValueError("TASK_RELEASE_ROLLBACK_EXTERNAL_DRIFT")
            if original is None:
                # Preserve a newly created task's identity/audit; it cannot run after rollback.
                payload = {"enabled":0}
            else:
                payload = {key:original[key] for key in FIELDS}
            statement = "UPDATE st_scheduled_tasks SET " + ",".join(f"`{key}`=:{key}" for key in payload) + " WHERE id=:row_id AND task_type=:identity"
            changed = connection.execute(text(statement), {**payload,"row_id":row["id"],"identity":kind})
            if changed.rowcount != 1:
                raise ValueError("TASK_RELEASE_ROLLBACK_CAS_LOST")
    return {"status":"RESTORED", "plan_sha256":plan["plan_sha256"], "restored":scope}


def _load(stream):
    value = stream.read(MAX_BYTES + 1)
    if len(value.encode("utf-8")) > MAX_BYTES:
        raise ValueError("TASK_RELEASE_INPUT_BUDGET_EXCEEDED")
    return json.loads(value)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("build","bundle","check-source","capture","apply","validate","restore"))
    parser.add_argument("--base")
    parser.add_argument("--target")
    parser.add_argument("--previous-sha")
    parser.add_argument("--target-sha")
    parser.add_argument("--plan")
    parser.add_argument("--before")
    args = parser.parse_args(argv)
    if args.action == "build":
        with Path(args.base).open(encoding="utf-8") as stream: base = _load(stream)
        with Path(args.target).open(encoding="utf-8") as stream: target = _load(stream)
        result = build_plan(base, target, previous_sha=args.previous_sha, target_sha=args.target_sha)
    elif args.action == "bundle":
        with Path(args.plan).open(encoding="utf-8") as stream: plan = validate_plan(_load(stream))
        result = {"plan":plan}
        if args.before:
            with Path(args.before).open(encoding="utf-8") as stream: result["before"] = _snapshot(plan, _load(stream))
    else:
        packet = _load(sys.stdin)
        plan = validate_plan(packet["plan"])
        _current(plan)
        if args.action == "check-source":
            print(json.dumps({"status":"SOURCE_VALIDATED", "plan_sha256":plan["plan_sha256"],
                              "upsert_task_types":plan["upsert_task_types"], "retired_task_types":plan["retired_task_types"]}))
            return 0
        # An empty immutable delta is deliberately zero database work.
        if not _scope(plan):
            engine = None
        else:
            from server.common.batch_db import create_batch_engine
            engine = create_batch_engine(connect_args={"connect_timeout":5,"read_timeout":30,"write_timeout":30})
        try:
            if args.action == "capture": result = capture_before(engine,plan)
            elif args.action == "apply":
                if packet.get("before") != capture_before(engine,plan):
                    raise ValueError("TASK_RELEASE_DATABASE_CHANGED_AFTER_CAPTURE")
                result = apply_plan(engine,plan)
            elif args.action == "validate": result = validate_applied_plan(engine,plan)
            else: result = restore_before(engine,plan,packet["before"])
        finally:
            if engine is not None: engine.dispose()
    print(json.dumps(result,ensure_ascii=False,sort_keys=True,separators=(",",":")))
    return 0


if __name__ == "__main__":
    def deadline():
        stage = sys.argv[1] if len(sys.argv) > 1 else "operation"
        print(f"TASK_RELEASE_{stage.upper()}_DEADLINE_EXCEEDED", file=sys.stderr, flush=True)
        os._exit(124)
    timer = threading.Timer(90, deadline)
    timer.daemon = True
    timer.start()
    raise SystemExit(main())
