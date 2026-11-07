"""Transactional task ledger. Only the engineering service writes this database."""

from contextlib import contextmanager
from hashlib import sha256
import json
from pathlib import Path
import re
import sqlite3
import time
import uuid


STAGES = {"merge": "merged", "test": "tested", "deploy": "released", "readback": "active"}
KINDS = {"codex", "test", "merge", "classify", "deploy", "readback", "rollback", "acquire",
         "coverage", "research", "simulation", "verify", "probe"}
MUTATING = {"codex", "rework", "merge", "deploy", "rollback", "acquire", "simulation", "research"}
RETRYABLE = {"test", "classify", "verify", "probe", "coverage", "readback", "research"}
LIVE = {"NOT_STARTED", "RUNNING", "BLOCKED"}
ID = re.compile(r"[A-Za-z0-9_.:-]{1,96}\Z")
SHA = re.compile(r"[a-f0-9]{40}\Z")
LEASE = 20
NO_PROGRESS = 1800
SCHEMA = """
CREATE TABLE IF NOT EXISTS task (
 id TEXT PRIMARY KEY, title TEXT NOT NULL, goal TEXT NOT NULL, acceptance TEXT NOT NULL,
 priority INTEGER NOT NULL, expires REAL NOT NULL, created REAL NOT NULL,
 progress REAL NOT NULL, session TEXT NOT NULL, worktree TEXT UNIQUE, candidate TEXT,
 paused INTEGER NOT NULL DEFAULT 0, canceled INTEGER NOT NULL DEFAULT 0, historical TEXT
);
CREATE TABLE IF NOT EXISTS step (
 task TEXT NOT NULL REFERENCES task(id), id TEXT NOT NULL, ordinal INTEGER NOT NULL,
 spec TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'NOT_STARTED', phase TEXT NOT NULL DEFAULT 'execute',
 code TEXT, receipt TEXT, checkpoint TEXT NOT NULL DEFAULT '{}', next REAL NOT NULL DEFAULT 0,
 progress REAL NOT NULL, failures TEXT NOT NULL DEFAULT '[]', PRIMARY KEY(task,id)
);
CREATE TABLE IF NOT EXISTS attempt (
 id TEXT PRIMARY KEY, task TEXT NOT NULL, step TEXT NOT NULL, fence INTEGER NOT NULL,
 operation TEXT NOT NULL, fingerprint TEXT NOT NULL, resource TEXT, state TEXT NOT NULL,
 mode TEXT NOT NULL DEFAULT 'execute', deadline REAL NOT NULL, lease REAL NOT NULL DEFAULT 0,
 launch REAL NOT NULL DEFAULT 0, owner TEXT, result TEXT, reconcile_until REAL,
 FOREIGN KEY(task,step) REFERENCES step(task,id), UNIQUE(task,step,fence)
);
CREATE UNIQUE INDEX IF NOT EXISTS active_resource ON attempt(resource)
 WHERE state IN ('QUEUED','RUNNING','RECONCILE','QUARANTINED') AND resource IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS active_step ON attempt(task,step)
 WHERE state IN ('QUEUED','RUNNING','RECONCILE','QUARANTINED');
CREATE TABLE IF NOT EXISTS event (
 seq INTEGER PRIMARY KEY, task TEXT NOT NULL REFERENCES task(id), at REAL NOT NULL,
 step TEXT, operation TEXT, code TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS release_hold (
 environment TEXT NOT NULL, endpoint TEXT NOT NULL, task TEXT NOT NULL REFERENCES task(id),
 step TEXT NOT NULL, candidate TEXT NOT NULL, PRIMARY KEY(environment,endpoint)
);
CREATE TABLE IF NOT EXISTS grant_record (
 task TEXT NOT NULL REFERENCES task(id), kind TEXT NOT NULL, environment TEXT NOT NULL,
 resource TEXT NOT NULL, endpoint TEXT NOT NULL, candidate TEXT NOT NULL,
 expires REAL NOT NULL, decision TEXT NOT NULL, reference TEXT NOT NULL,
 PRIMARY KEY(task,kind,environment,resource,endpoint,candidate)
);
"""


def pack(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def unpack(value, default=None):
    return default if value is None else json.loads(value)


@contextmanager
def db(path, *, write=False):
    path = Path(path).resolve()
    if not path.is_file():
        raise FileNotFoundError(path)
    con = sqlite3.connect(path.as_uri() + ("?mode=rw" if write else "?mode=ro"), uri=True, timeout=3)
    con.row_factory = sqlite3.Row
    try:
        if write:
            con.execute("PRAGMA foreign_keys=ON")
            con.execute("BEGIN IMMEDIATE")
        else:
            con.execute("BEGIN")
        yield con
        con.commit()
    except BaseException:
        con.rollback()
        raise
    finally:
        con.close()


def initialize(path):
    path = Path(path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
    con = sqlite3.connect(path, timeout=3)
    try:
        # DELETE journaling allows the existing API to open the ledger read-only.
        con.executescript(SCHEMA)
    finally:
        con.close()


def event(con, task, code, step=None, operation=None):
    con.execute("INSERT INTO event(task,at,step,operation,code) VALUES(?,?,?,?,?)",
                (task, time.time(), step, operation, code))


def key(value):
    if not isinstance(value, str) or not ID.fullmatch(value):
        raise ValueError("invalid identifier")
    return value


def text_value(value, limit):
    if not isinstance(value, str) or not value.strip() or len(value) > limit:
        raise ValueError("invalid text field")
    return value.strip()


def submit(path, manifest):
    now = time.time()
    task_id = key(manifest.get("id", uuid.uuid4().hex))
    expires = float(manifest["expires"])
    priority = int(manifest.get("priority", 0))
    if not now < expires <= now + 30 * 86400 or not -10 <= priority <= 10:
        raise ValueError("invalid deadline or priority")
    title = text_value(manifest["title"], 160)
    goal = text_value(manifest["goal"], 10000)
    acceptance = manifest["acceptance"]
    if not isinstance(acceptance, list) or not 1 <= len(acceptance) <= 20:
        raise ValueError("acceptance checks required")
    acceptance = [text_value(a, 2000) for a in acceptance]
    session = manifest.get("session", {})
    if not isinstance(session, dict) or set(session) - {"worktree", "branch", "thread_id", "turn_id",
                                                          "ownership_released", "base", "candidate"}:
        raise ValueError("invalid session")
    tree = str(Path(session["worktree"]).resolve()) if session.get("worktree") else None
    if tree:
        session = {**session, "worktree": tree}
    if session.get("thread_id") and (not tree or session.get("ownership_released") is not True or not session.get("base")):
        raise ValueError("handoff requires worktree, base and explicit exclusive ownership")
    candidate = manifest.get("candidate")
    if candidate is not None and (not isinstance(candidate, str) or not SHA.fullmatch(candidate)):
        raise ValueError("candidate must be a full commit SHA")
    raw = manifest["steps"]
    if not isinstance(raw, list) or not 1 <= len(raw) <= 64:
        raise ValueError("expected one to 64 steps")
    specs = {}
    for item in raw:
        step_id, kind = key(item["id"]), item["kind"]
        if step_id in specs or kind not in KINDS:
            raise ValueError("duplicate step or unsupported action")
        if set(item) - {"id", "kind", "inputs", "endpoint", "resource", "timeout", "depends",
                        "on_failure_of", "rework", "profile"}:
            raise ValueError("unsupported step field")
        inputs = item.get("inputs", {})
        if not isinstance(inputs, dict) or len(pack(inputs)) > 16000:
            raise ValueError("invalid action inputs")
        environment = inputs.get("environment", "isolated")
        if environment not in {"isolated", "development", "production"}:
            raise ValueError("invalid environment")
        endpoint = item.get("endpoint")
        if endpoint not in {None, "linux", "windows"}:
            raise ValueError("invalid endpoint")
        if (kind in {"deploy", "rollback", "readback"} or kind == "acquire" and environment == "production") and not endpoint:
            raise ValueError("release operations require an endpoint")
        if kind in {"codex", "test", "research", "simulation", "merge"} and environment == "production":
            raise ValueError("action cannot execute in production")
        timeout = int(item.get("timeout", 120))
        if not 1 <= timeout <= 3600:
            raise ValueError("invalid step timeout")
        resource = item.get("resource")
        if resource is not None:
            key(resource)
        if kind in MUTATING and kind != "codex" and not resource:
            raise ValueError("mutating step requires a resource")
        if kind in {"codex", "merge"} or (kind == "test" and tree):
            if not tree:
                raise ValueError("engineering action requires a dedicated worktree")
            resource = "worktree:" + sha256(str(Path(tree)).casefold().encode()).hexdigest()
        if item.get("rework") and (kind != "test" or not tree):
            raise ValueError("rework requires a test step and an owned worktree")
        if bool(item.get("on_failure_of")) != (kind == "rollback"):
            raise ValueError("rollback must be triggered by failed production readback")
        deps = item.get("depends", [])
        if not isinstance(deps, list) or len(set(deps)) != len(deps) or any(d not in specs for d in deps):
            raise ValueError("dependencies must be listed earlier")
        failure = item.get("on_failure_of")
        if failure and (failure not in specs or specs[failure]["kind"] != "readback"
                        or specs[failure]["endpoint"] != endpoint
                        or specs[failure]["inputs"]["environment"] != environment):
            raise ValueError("rollback trigger must be readback on the same environment and endpoint")
        specs[step_id] = {**item, "resource": resource, "endpoint": endpoint, "depends": deps,
                          "timeout": timeout, "profile": key(item.get("profile", kind)),
                          "inputs": {**inputs, "environment": environment}}
    for spec in specs.values():
        if spec["kind"] in {"deploy", "rollback"} and not any(
            c["kind"] == "readback" and c["endpoint"] == spec["endpoint"]
            and c["inputs"]["environment"] == spec["inputs"]["environment"] and spec["id"] in c["depends"]
            for c in specs.values()
        ):
            raise ValueError("every deployment requires dependent endpoint readback")
        if spec["kind"] == "acquire" and not any(c["kind"] == "coverage" and spec["id"] in c["depends"]
                                                  for c in specs.values()):
            raise ValueError("acquisition requires dependent coverage and durable readback")
    with db(path, write=True) as con:
        con.execute("INSERT INTO task(id,title,goal,acceptance,priority,expires,created,progress,session,worktree,candidate) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (task_id, title, goal, pack(acceptance), priority, expires, now, now, pack(session), tree, candidate))
        con.executemany("INSERT INTO step(task,id,ordinal,spec,progress) VALUES(?,?,?,?,?)",
                        [(task_id, s, i, pack(spec), now) for i, (s, spec) in enumerate(specs.items())])
        event(con, task_id, "task_registered")
    return task_id


def control(path, task, action):
    with db(path, write=True) as con:
        row = con.execute("SELECT paused,canceled,historical FROM task WHERE id=?", (task,)).fetchone()
        if not row or row["canceled"] or row["historical"]:
            raise ValueError("task cannot be changed")
        if action == "cancel":
            con.execute("UPDATE task SET canceled=1 WHERE id=?", (task,))
        elif action in {"pause", "resume"}:
            con.execute("UPDATE task SET paused=? WHERE id=?", (int(action == "pause"), task))
        else:
            raise ValueError("unsupported control")
        event(con, task, "task_" + action)


def grant(path, task, *, kind, environment, resource, endpoint, candidate, expires, decision, reference):
    if (kind not in {"deploy", "rollback", "acquire"} or environment != "production" or
            decision not in {"allow", "deny"} or not SHA.fullmatch(candidate) or
            endpoint not in {"linux", "windows"} or not time.time() < expires):
        raise ValueError("invalid production decision")
    with db(path, write=True) as con:
        row = con.execute("SELECT expires,canceled,historical FROM task WHERE id=?", (task,)).fetchone()
        if not row or row["canceled"] or row["historical"] or expires > row["expires"]:
            raise ValueError("decision must be within an existing task deadline")
        con.execute("INSERT OR REPLACE INTO grant_record VALUES(?,?,?,?,?,?,?,?,?)",
                    (task, kind, environment, key(resource), endpoint, candidate, expires, decision,
                     text_value(reference, 300)))
        event(con, task, "authorization_" + decision)


def authorization(con, task, op, now):
    if op["kind"] not in MUTATING or op["inputs"]["environment"] != "production":
        return None
    row = con.execute("SELECT expires,decision,reference FROM grant_record WHERE "
                      "task=? AND kind=? AND environment=? AND resource=? AND endpoint=? AND candidate=?",
                      (task, op["kind"], "production", op["resource"], op["endpoint"],
                       op["inputs"].get("candidate"))).fetchone()
    if not row:
        return "authorization_missing"
    if row["decision"] != "allow":
        return "authorization_denied"
    if row["expires"] <= now:
        return "authorization_expired"
    return None


def authorization_reference(con, task, op):
    return con.execute("SELECT reference FROM grant_record WHERE task=? AND kind=? AND environment=? "
                       "AND resource=? AND endpoint=? AND candidate=? AND decision='allow' AND expires>?",
                       (task, op["kind"], op["inputs"]["environment"], op["resource"], op["endpoint"],
                        op["inputs"].get("candidate"), time.time())).fetchone()


def set_step(con, task, step, state, code, *, phase=None, next_at=0):
    con.execute("UPDATE step SET state=?,code=?,next=?,phase=COALESCE(?,phase) WHERE task=? AND id=?",
                (state, code, next_at, phase, task, step))
    event(con, task, code, step)


def materialize(spec, task, phase):
    op = {**spec, "inputs": dict(spec["inputs"])}
    if phase != "execute":
        op.update(kind=phase, profile=phase, endpoint=None)
        op["inputs"]["environment"] = "isolated"
    if op["inputs"].get("candidate") == "$candidate" or ("candidate" not in op["inputs"] and task["candidate"]):
        op["inputs"]["candidate"] = task["candidate"]
    return op


def ancestors(spec, rows):
    todo, seen = list(spec["depends"]), set()
    while todo:
        sid = todo.pop()
        if sid not in seen:
            seen.add(sid)
            row = rows[sid]
            dep = unpack(row["spec"])
            yield dep, unpack(row["receipt"], {}).get("evidence", {})
            todo.extend(dep["depends"])


def acceptance_gate(op, rows):
    if op["kind"] not in {"deploy", "readback", "rollback", "coverage"}:
        return None
    candidate = op["inputs"].get("candidate")
    deps = list(ancestors(op, rows))
    matching = [(d, e) for d, e in deps if candidate and e.get("candidate") == candidate]
    if op["kind"] == "deploy":
        classified = [e for d, e in matching if d["kind"] == "classify"]
        planned = {unpack(row["spec"])["endpoint"] for row in rows.values()
                   if unpack(row["spec"])["kind"] == "deploy" and
                   unpack(row["spec"])["inputs"]["environment"] == op["inputs"]["environment"]}
        if not any(set(e.get("endpoints", [])) == planned for e in classified):
            return "release_scope_unverified"
        if len(planned) == 2 and {e.get("endpoint") for d, e in deps if d["kind"] == "probe"
                                  and e.get("application_ok") is True} != planned:
            return "cross_end_preflight_unverified"
        if not any(d["kind"] == "test" and e.get("environment") in {"isolated", "development"} for d, e in matching):
            return "release_tests_unverified"
        if op["inputs"]["environment"] == "production" and not any(
                d["kind"] == "merge" and e.get("merged") is True for d, e in matching):
            return "release_merge_unverified"
    if op["kind"] == "readback" and not any(
            d["kind"] in {"deploy", "rollback"} and e.get("endpoint") == op["endpoint"] and
            d["inputs"]["environment"] == op["inputs"]["environment"] for d, e in matching):
        return "release_receipt_unverified"
    if op["kind"] == "rollback":
        source = unpack(rows[op["on_failure_of"]]["spec"])
        deployments = [e for d, e in ancestors(source, rows) if d["kind"] == "deploy"
                       and d["endpoint"] == op["endpoint"]]
        if len(deployments) != 1 or deployments[0].get("rollback_candidate") != candidate:
            return "rollback_source_unverified"
        op["inputs"]["rollback_from"] = deployments[0]["candidate"]
    if op["kind"] == "coverage":
        acquired = [e for d, e in deps if d["kind"] == "acquire" and
                    d["inputs"]["environment"] == op["inputs"]["environment"]]
        if len(acquired) != 1 or not acquired[0].get("external_id"):
            return "acquisition_receipt_unverified"
        op["inputs"]["external_id"] = acquired[0]["external_id"]
    return None


def schedule(path, slots):
    now = time.time()
    with db(path, write=True) as con:
        count = con.execute("SELECT COUNT(*) FROM attempt WHERE state IN ('QUEUED','RUNNING','RECONCILE')").fetchone()[0]
        tasks = con.execute("SELECT * FROM task WHERE historical IS NULL AND paused=0 AND canceled=0 AND EXISTS "
                            "(SELECT 1 FROM step WHERE task=task.id AND state IN ('NOT_STARTED','BLOCKED')) "
                            "ORDER BY priority DESC,created").fetchall()
        for task in tasks:
            tid = task["id"]
            rows = {s["id"]: s for s in con.execute("SELECT * FROM step WHERE task=? ORDER BY ordinal", (tid,))}
            for sid, step in rows.items():
                if step["state"] not in {"NOT_STARTED", "BLOCKED"} or step["next"] > now:
                    continue
                if con.execute("SELECT 1 FROM attempt WHERE task=? AND step=? AND state='QUARANTINED'", (tid, sid)).fetchone():
                    continue
                spec = unpack(step["spec"])
                if task["expires"] <= now:
                    set_step(con, tid, sid, "FAILED", "task_deadline")
                    continue
                if spec.get("on_failure_of"):
                    cause = rows[spec["on_failure_of"]]
                    if cause["state"] == "COMPLETE":
                        set_step(con, tid, sid, "COMPLETE", "rollback_not_needed")
                        continue
                    evidence = unpack(cause["receipt"], {}).get("evidence", {})
                    if cause["state"] == "FAILED" and (evidence.get("business_verified") is not False or
                            evidence.get("endpoint") != spec["endpoint"] or not evidence.get("candidate")):
                        set_step(con, tid, sid, "FAILED", "rollback_trigger_unverified")
                        continue
                    if cause["state"] != "FAILED":
                        continue
                dep_states = [rows[d]["state"] for d in spec["depends"]]
                if "FAILED" in dep_states:
                    set_step(con, tid, sid, "FAILED", "dependency_failed")
                    continue
                if any(d != "COMPLETE" for d in dep_states):
                    continue
                if spec["kind"] == "readback" and any(rows[d]["code"] == "rollback_not_needed" for d in spec["depends"]):
                    set_step(con, tid, sid, "COMPLETE", "rollback_not_needed")
                    continue
                op = materialize(spec, task, step["phase"])
                reason = authorization(con, tid, op, now) or acceptance_gate(op, rows)
                if reason:
                    if step["code"] != reason:
                        set_step(con, tid, sid, "BLOCKED", reason)
                    continue
                if count >= slots:
                    continue
                environment, endpoint = op["inputs"]["environment"], op["endpoint"]
                if op["kind"] in {"deploy", "rollback"}:
                    hold = con.execute("SELECT task,step FROM release_hold WHERE environment=? AND endpoint=?",
                                       (environment, endpoint)).fetchone()
                    if hold and (hold["task"] != tid or (op["kind"] == "deploy" and hold["step"] != sid)):
                        if step["code"] != "release_waiting_for_readback":
                            set_step(con, tid, sid, "BLOCKED", "release_waiting_for_readback")
                        continue
                    if op["kind"] == "deploy" and any(
                        r["state"] == "FAILED" and unpack(r["spec"])["kind"] == "readback" and
                        unpack(r["spec"])["inputs"]["environment"] == environment for r in rows.values()
                    ):
                        if step["code"] != "release_frozen_after_readback":
                            set_step(con, tid, sid, "BLOCKED", "release_frozen_after_readback")
                        continue
                resource = op["resource"]
                if resource and con.execute("SELECT 1 FROM attempt WHERE resource=? AND state IN "
                                            "('QUEUED','RUNNING','RECONCILE','QUARANTINED')", (resource,)).fetchone():
                    if step["code"] != "resource_busy":
                        set_step(con, tid, sid, "BLOCKED", "resource_busy")
                    continue
                fence = con.execute("SELECT COALESCE(MAX(fence),0)+1 FROM attempt WHERE (task=? AND step=?) OR resource=?",
                                    (tid, sid, resource)).fetchone()[0]
                aid = uuid.uuid4().hex
                serialized = pack(op)
                con.execute("INSERT INTO attempt(id,task,step,fence,operation,fingerprint,resource,state,deadline) "
                            "VALUES(?,?,?,?,?,?,?,'QUEUED',?)",
                            (aid, tid, sid, fence, serialized, sha256(serialized.encode()).hexdigest(), resource,
                             min(now + op["timeout"], task["expires"])))
                if op["kind"] == "deploy":
                    con.execute("INSERT OR IGNORE INTO release_hold VALUES(?,?,?,?,?)",
                                (environment, endpoint, tid, sid, op["inputs"]["candidate"]))
                set_step(con, tid, sid, "RUNNING", "operation_queued")
                count += 1


def checked_result(op, result):
    if not isinstance(result, dict) or result.get("status") not in {"success", "failure", "waiting", "unknown"}:
        return {"status": "unknown", "code": "malformed_receipt"}
    code = result.get("code")
    if not isinstance(code, str) or not ID.fullmatch(code):
        return {"status": "unknown", "code": "malformed_receipt_code"}
    evidence = result.get("evidence", {})
    checkpoint = result.get("checkpoint", {})
    if not isinstance(evidence, dict) or not isinstance(checkpoint, dict):
        return {"status": "unknown", "code": "malformed_receipt_evidence"}
    if len(pack(result)) > 96000:
        return {"status": "unknown", "code": "oversize_receipt"}
    if result["status"] != "success":
        return result
    kind, inp = op["kind"], op["inputs"]
    cand = inp.get("candidate")
    if kind in {"codex", "rework", "merge", "test", "classify", "deploy", "readback", "rollback"}:
        actual = evidence.get("candidate", "")
        if not isinstance(actual, str) or not SHA.fullmatch(actual) or (
                cand and kind not in {"codex", "rework", "merge"} and cand != actual):
            return {"status": "unknown" if kind in MUTATING else "failure", "code": "candidate_unverified"}
    checks = {
        "test": lambda: evidence.get("environment") == inp["environment"],
        "merge": lambda: evidence.get("merged") is True,
        "classify": lambda: isinstance(evidence.get("endpoints"), list) and
        set(evidence["endpoints"]) <= {"linux", "windows"} and bool(evidence.get("contract")),
        "deploy": lambda: evidence.get("released") is True and evidence.get("endpoint") == op["endpoint"],
        "rollback": lambda: evidence.get("released") is True and evidence.get("endpoint") == op["endpoint"],
        "readback": lambda: evidence.get("active") is True and evidence.get("business_verified") is True
        and evidence.get("endpoint") == op["endpoint"],
        "acquire": lambda: bool(evidence.get("external_id")),
        "coverage": lambda: all(evidence.get(k) is True for k in
                                ("coverage_verified", "receipt_verified", "readback_verified")) and
        evidence.get("external_id") == inp.get("external_id"),
        "simulation": lambda: evidence.get("isolated") is True and type(evidence.get("real_orders")) is int
        and evidence["real_orders"] == 0,
    }
    if kind in checks and not checks[kind]():
        return {"status": "unknown" if kind in MUTATING else "failure", "code": "acceptance_evidence_missing"}
    return result


def reopen_reconciliation(path, aid, seconds=120):
    key(aid)
    if not 10 <= seconds <= 1800:
        raise ValueError("invalid reconciliation budget")
    with db(path, write=True) as con:
        a = con.execute("SELECT * FROM attempt WHERE id=? AND state='QUARANTINED'", (aid,)).fetchone()
        if not a:
            raise ValueError("only quarantined operations can be reopened")
        con.execute("UPDATE attempt SET state='RECONCILE',mode='reconcile',launch=0,reconcile_until=? WHERE id=?",
                    (time.time() + seconds, aid))
        event(con, a["task"], "operator_read_only_reconciliation", a["step"], aid)


def get_launches(path, *, owner_absent, cap):
    now = time.time()
    with db(path, write=True) as con:
        rows = con.execute("SELECT a.*,t.expires,t.paused,t.canceled FROM attempt a JOIN task t ON t.id=a.task "
                           "WHERE a.state IN ('QUEUED','RUNNING','RECONCILE') ORDER BY a.launch LIMIT 128").fetchall()
        launch = []
        for a in rows:
            aid, tid, sid = a["id"], a["task"], a["step"]
            state, launch_at = a["state"], a["launch"]
            if state == "RUNNING" and now >= a["lease"] and owner_absent(aid):
                con.execute("UPDATE attempt SET state='RECONCILE',mode='reconcile',owner=NULL,launch=0 WHERE id=?", (aid,))
                event(con, tid, "worker_absent_reconcile", sid, aid)
                state, launch_at = "RECONCILE", 0
            elif state == "RUNNING" and now > a["deadline"]:
                current = con.execute("SELECT code FROM step WHERE task=? AND id=?", (tid, sid)).fetchone()[0]
                if current != "worker_deadline_waiting_for_owner":
                    set_step(con, tid, sid, "RUNNING", "worker_deadline_waiting_for_owner")
            if state == "RECONCILE" and now >= (a["reconcile_until"] or a["expires"]):
                con.execute("UPDATE attempt SET state='QUARANTINED',owner=NULL WHERE id=?", (aid,))
                set_step(con, tid, sid, "BLOCKED", "external_outcome_unverified")
                continue
            if state == "QUEUED" and (now >= a["deadline"] or a["paused"] or a["canceled"]):
                con.execute("UPDATE attempt SET state='DONE' WHERE id=?", (aid,))
                release_unsubmitted(con, a, unpack(a["operation"]))
                stopped = bool(a["canceled"] or now >= a["deadline"])
                code = "task_cancel" if a["canceled"] else ("dispatch_deadline" if stopped else "task_pause")
                set_step(con, tid, sid, "FAILED" if stopped else "NOT_STARTED", code)
                continue
            if state in {"QUEUED", "RECONCILE"} and len(launch) < cap and launch_at <= now:
                con.execute("UPDATE attempt SET launch=? WHERE id=?", (now + 5, aid))
                launch.append(aid)
        return launch


def release_unsubmitted(con, a, op):
    if op["kind"] == "deploy":
        con.execute("DELETE FROM release_hold WHERE task=? AND step=? AND environment=? AND endpoint=?",
                    (a["task"], a["step"], op["inputs"]["environment"], op["endpoint"]))


def claim(path, aid, owner):
    now = time.time()
    with db(path, write=True) as con:
        a = con.execute("SELECT * FROM attempt WHERE id=? AND state IN ('QUEUED','RECONCILE')", (aid,)).fetchone()
        if not a:
            return None
        tid, sid = a["task"], a["step"]
        task = con.execute("SELECT * FROM task WHERE id=?", (tid,)).fetchone()
        step = con.execute("SELECT * FROM step WHERE task=? AND id=?", (tid, sid)).fetchone()
        op = unpack(a["operation"])
        auth = None
        if a["mode"] == "execute":
            if task["paused"] or task["canceled"] or a["deadline"] <= now:
                return None
            reason = authorization(con, tid, op, now)
            if reason:
                con.execute("UPDATE attempt SET state='DONE' WHERE id=?", (aid,))
                release_unsubmitted(con, a, op)
                set_step(con, tid, sid, "BLOCKED", reason)
                return None
            ref = authorization_reference(con, tid, op)
            auth = ref[0] if ref else None
        deadline = a["deadline"] if a["mode"] == "execute" else min(a["reconcile_until"] or task["expires"], now + min(op["timeout"], 120))
        if deadline <= now:
            return None
        con.execute("UPDATE attempt SET state='RUNNING',owner=?,lease=?,deadline=? WHERE id=?",
                    (owner, now + LEASE, deadline, aid))
        set_step(con, tid, sid, "RUNNING", "operation_" + a["mode"])
        return op, {"operation_id": aid, "task_id": tid, "step_id": sid, "fence": a["fence"],
                    "deadline": deadline, "mode": a["mode"], "checkpoint": unpack(step["checkpoint"]),
                    "failure": unpack(step["receipt"], {}), "session": unpack(task["session"]),
                    "goal": task["goal"], "acceptance": unpack(task["acceptance"]), "authorization": auth}


def renewed(path, aid, owner):
    with db(path, write=True) as con:
        return bool(con.execute("UPDATE attempt SET lease=? WHERE id=? AND owner=? AND state='RUNNING'",
                                (time.time() + LEASE, aid, owner)).rowcount)


def progress(path, aid, owner, *, completed, total, unit, checkpoint):
    if type(completed) is not int or type(total) is not int or not 0 <= completed <= total:
        raise ValueError("invalid progress units")
    key(unit)
    if not isinstance(checkpoint, dict) or len(pack(checkpoint)) > 16000:
        raise ValueError("invalid checkpoint")
    with db(path, write=True) as con:
        a = con.execute("SELECT * FROM attempt WHERE id=? AND owner=? AND state='RUNNING' AND lease>?",
                        (aid, owner, time.time())).fetchone()
        if not a:
            return False
        row = con.execute("SELECT checkpoint FROM step WHERE task=? AND id=?", (a["task"], a["step"])).fetchone()
        old = unpack(row[0])
        if old.get("progress", {}).get("completed", -1) >= completed:
            return False
        state = {**checkpoint, "progress": {"completed": completed, "total": total, "unit": unit}}
        now = time.time()
        con.execute("UPDATE step SET checkpoint=?,progress=? WHERE task=? AND id=?", (pack(state), now, a["task"], a["step"]))
        con.execute("UPDATE task SET progress=? WHERE id=?", (now, a["task"]))
        event(con, a["task"], "business_progress", a["step"], aid)
        return True


def finish(path, aid, owner, result):
    now = time.time()
    with db(path, write=True) as con:
        a = con.execute("SELECT * FROM attempt WHERE id=? AND owner=? AND state='RUNNING' AND lease>?",
                        (aid, owner, now)).fetchone()
        if not a:
            return False
        op = unpack(a["operation"])
        result = checked_result(op, result)
        status, code = result["status"], result["code"]
        tid, sid = a["task"], a["step"]
        event(con, tid, "receipt_" + status, sid, aid)
        s = con.execute("SELECT * FROM step WHERE task=? AND id=?", (tid, sid)).fetchone()
        task = con.execute("SELECT session,expires FROM task WHERE id=?", (tid,)).fetchone()
        kind = op["kind"]
        if status == "unknown" or a["mode"] == "reconcile" and status == "waiting":
            terminal = now >= (a["reconcile_until"] or task["expires"])
            con.execute("UPDATE attempt SET state=?,mode='reconcile',result=?,owner=NULL,launch=? WHERE id=?",
                        ("QUARANTINED" if terminal else "RECONCILE", pack(result), now + 10, aid))
            set_step(con, tid, sid, "BLOCKED" if terminal else "RUNNING", code)
            return True
        con.execute("UPDATE attempt SET state='DONE',result=?,owner=NULL WHERE id=?", (pack(result), aid))
        checkpoint = {**unpack(s["checkpoint"]), **result.get("checkpoint", {})}
        con.execute("UPDATE step SET checkpoint=? WHERE task=? AND id=?", (pack(checkpoint), tid, sid))
        if status == "waiting":
            release_unsubmitted(con, a, op)
            set_step(con, tid, sid, "BLOCKED", code, next_at=min(now + 5, task["expires"]))
            return True
        if kind == "supervise":
            set_step(con, tid, sid, "FAILED", "supervisor_diagnosed" if status == "success" else "supervisor_unavailable")
            return True
        if status == "success":
            evidence = result.get("evidence", {})
            env, endpoint = op["inputs"]["environment"], op["endpoint"]
            if kind == "rollback":
                con.execute("UPDATE release_hold SET candidate=? WHERE task=? AND environment=? AND endpoint=?",
                            (evidence["candidate"], tid, env, endpoint))
            if kind == "readback":
                con.execute("DELETE FROM release_hold WHERE task=? AND environment=? AND endpoint=? AND candidate=?",
                            (tid, env, endpoint, evidence["candidate"]))
            if kind in {"codex", "rework", "merge"}:
                previous = unpack(task["session"])
                proposed = checkpoint.get("session", {}) if kind != "merge" else {}
                session = {**previous, **{k: proposed[k] for k in ("thread_id", "turn_id", "candidate") if k in proposed}}
                con.execute("UPDATE task SET candidate=?,session=? WHERE id=?", (evidence["candidate"], pack(session), tid))
            con.execute("UPDATE task SET progress=? WHERE id=?", (now, tid))
            con.execute("UPDATE step SET progress=?,receipt=? WHERE task=? AND id=?", (now, pack(result), tid, sid))
            if kind == "rework":
                con.execute("UPDATE step SET spec=? WHERE task=? AND id=?", (pack({**unpack(s["spec"]), "inputs": {
                    **unpack(s["spec"])["inputs"], "candidate": "$candidate"}}), tid, sid))
                set_step(con, tid, sid, "NOT_STARTED", "rework_finished_retest", phase="execute")
            else:
                set_step(con, tid, sid, "COMPLETE", code)
            return True
        failed_spec = unpack(s["spec"])
        failures = unpack(s["failures"])
        failure_key = result.get("failure_key", code)
        if not isinstance(failure_key, str) or len(failure_key) > 160:
            failure_key = code
        failures.append(failure_key)
        con.execute("UPDATE step SET failures=?,receipt=? WHERE task=? AND id=?", (pack(failures), pack(result), tid, sid))
        can_retry = (now - s["progress"] < NO_PROGRESS and len(failures) < 3 and failures.count(failure_key) < 2)
        if kind == "test" and failed_spec.get("rework") and code in {"test_failed", "tests_failed"} and can_retry:
            set_step(con, tid, sid, "NOT_STARTED", "test_failed_rework", phase="rework")
        elif kind in RETRYABLE and code in {"runtime_offline", "transient_readback", "transient_failure"} and can_retry:
            set_step(con, tid, sid, "BLOCKED", code, next_at=now + 5)
        elif kind != "readback" and (kind in {"test", "codex", "rework"} or len(failures) > 1):
            set_step(con, tid, sid, "NOT_STARTED", "supervisor_needed", phase="supervise")
        else:
            set_step(con, tid, sid, "FAILED", code)
        return True


EVIDENCE_STATES = {"COMPLETE", "FAILED", "BLOCKED", "UNKNOWN", "NOT_STARTED"}
PUBLIC_EVIDENCE = {"candidate", "environment", "endpoint", "external_id", "instance_id", "model_instance_id", "build", "run_id"}


def public_evidence(value):
    return {k: v for k, v in value.items() if k in PUBLIC_EVIDENCE and isinstance(v, str) and ID.fullmatch(v)}


def import_history(path, manifest):
    task_id = key(manifest["id"])
    evidence = []
    for item in manifest["evidence"]:
        if item["stage"] not in STAGES.values() or item["state"] not in EVIDENCE_STATES:
            raise ValueError("invalid historical evidence state")
        if item["state"] == "COMPLETE" and not SHA.fullmatch(item.get("candidate", "")):
            raise ValueError("completed historical evidence requires exact candidate")
        evidence.append({**public_evidence(item), "stage": item["stage"], "state": item["state"],
                         "code": key(item["code"]), "at": float(item["at"])})
    if len(evidence) > 64 or set(e["stage"] for e in evidence) != set(STAGES.values()):
        raise ValueError("all four independent historical stages must be recorded")
    now = time.time()
    with db(path, write=True) as con:
        old = con.execute("SELECT historical FROM task WHERE id=?", (task_id,)).fetchone()
        if old:
            if old[0] == pack(evidence):
                return task_id
            raise ValueError("historical evidence is append-only; use a new import identity")
        con.execute("INSERT INTO task(id,title,goal,acceptance,priority,expires,created,progress,session,historical) "
                    "VALUES(?,?,?,'[]',0,?,?,?,'{}',?)",
                    (task_id, text_value(manifest["title"], 160), "Historical acceptance", now, now, now, pack(evidence)))
        event(con, task_id, "historical_evidence_imported")
    return task_id


def _view(con, task, detail):
    tid = task["id"]
    rows = con.execute("SELECT * FROM step WHERE task=? ORDER BY ordinal", (tid,)).fetchall()
    historical = unpack(task["historical"])
    states = [s["state"] for s in rows]
    if historical:
        states = [e["state"] for e in historical]
    if "RUNNING" in states:
        state = "RUNNING"
    elif task["canceled"]:
        state = "FAILED"
    elif states and all(s == "COMPLETE" for s in states):
        state = "COMPLETE"
    elif task["paused"] or "BLOCKED" in states or "UNKNOWN" in states:
        state = "BLOCKED"
    elif "FAILED" in states:
        state = "FAILED"
    else:
        state = "NOT_STARTED"
    view = {"id": tid, "title": task["title"], "state": state, "updated_at": task["progress"],
            "expires_at": task["expires"], "total": len(states), "completed": states.count("COMPLETE"),
            "current": next((s["id"] for s in rows if s["state"] == "RUNNING"),
                            next((s["id"] for s in rows if s["state"] != "COMPLETE"), None)),
            "paused": bool(task["paused"]), "canceled": bool(task["canceled"]), "historical": historical is not None}
    if not detail:
        return view
    view["steps"] = []
    view["evidence"] = historical or []
    for s in rows:
        spec = unpack(s["spec"])
        attempts = con.execute("SELECT * FROM attempt WHERE task=? AND step=? ORDER BY fence DESC LIMIT 32",
                               (tid, s["id"])).fetchall()
        check = unpack(s["checkpoint"]).get("progress", {})
        view["steps"].append({"id": s["id"], "kind": spec["kind"], "endpoint": spec["endpoint"],
                              "state": s["state"], "phase": s["phase"], "code": s["code"], "next_at": s["next"],
                              "updated_at": s["progress"], "progress": check,
                              "attempts": con.execute("SELECT COUNT(*) FROM attempt WHERE task=? AND step=?",
                                                      (tid, s["id"])).fetchone()[0],
                              "operation_id": attempts[0]["id"] if attempts else None,
                              "runtime": public_evidence(unpack(s["receipt"], {}).get("endpoint_evidence", {}))})
        for a in attempts:
            op, result = unpack(a["operation"]), unpack(a["result"], {})
            stage = STAGES.get(op["kind"])
            if not stage or not result or result["status"] == "waiting":
                continue
            occurred = con.execute("SELECT MAX(at) FROM event WHERE task=? AND step=? AND operation=?",
                                   (tid, s["id"], a["id"])).fetchone()[0]
            status = {"success": "COMPLETE", "failure": "FAILED", "unknown": "UNKNOWN"}[result["status"]]
            proof = public_evidence(result.get("evidence", {}))
            view["evidence"].append({**proof, "stage": stage, "state": status, "code": result["code"],
                                     "environment": op["inputs"]["environment"], "endpoint": op["endpoint"],
                                     "candidate": proof.get("candidate") or op["inputs"].get("candidate"), "at": occurred})
    view["events"] = [dict(e) for e in con.execute("SELECT at,step,code FROM event WHERE task=? ORDER BY seq DESC LIMIT 100", (tid,))]
    return view


def list_tasks(path, limit=50):
    with db(path) as con:
        rows = con.execute("SELECT * FROM task ORDER BY created DESC LIMIT ?", (max(1, min(int(limit), 100)),)).fetchall()
        return [_view(con, row, False) for row in rows]


def task_view(path, task_id):
    with db(path) as con:
        task = con.execute("SELECT * FROM task WHERE id=?", (task_id,)).fetchone()
        return _view(con, task, True) if task else None
