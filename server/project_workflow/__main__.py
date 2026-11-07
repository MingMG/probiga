"""Engineering service, worker entrypoint and operator CLI."""

import argparse
from contextlib import contextmanager
from functools import partial
import json
import logging
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
from threading import Event, Thread
import time
import uuid

from . import store


LOG = logging.getLogger("project_workflow")


def load(path, cap=1024 * 1024):
    with Path(path).open("rb") as stream:
        data = stream.read(cap + 1)
    if len(data) > cap:
        raise ValueError("configuration or manifest exceeds maximum size")
    result = json.loads(data)
    if not isinstance(result, dict):
        raise ValueError("expected JSON object")
    return result


@contextmanager
def worker_lock(database, aid):
    store.key(aid)
    directory = database.parent / "locks"
    directory.mkdir(mode=0o700, exist_ok=True)
    with (directory / aid).open("a+b") as handle:
        # An OS-owned lock proves absence even after a PID is recycled or the machine restarts.
        if os.name == "nt":
            import msvcrt

            handle.seek(0)
            try:
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError:
                yield False
                return
        else:
            import fcntl

            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                yield False
                return
        yield True


def absent(database, aid):
    with worker_lock(database, aid) as acquired:
        return acquired


def adapter(op):
    if op["kind"] in {"codex", "rework", "supervise"}:
        from . import codex_runner

        return codex_runner
    from . import adapters

    return adapters


def probe_first(op, context):
    if not op["endpoint"] or op["kind"] == "probe" or context["mode"] != "execute":
        return None
    from . import adapters

    probe = {**op, "kind": "probe", "profile": "probe-" + op["endpoint"], "resource": None}
    receipt = adapters.execute(probe, context)
    evidence = receipt.get("evidence", {})
    if (receipt.get("status") == "success" and evidence.get("endpoint") == op["endpoint"] and
            evidence.get("application_ok") is True and (op["endpoint"] != "windows" or
            (evidence.get("instance_id") and evidence.get("build") and evidence.get("no_order_ok") is True))):
        context["endpoint_evidence"] = store.public_evidence(evidence)
        return None
    return {"status": "waiting", "code": "endpoint_identity_unavailable", "evidence": evidence}


def worker(database, settings_path, aid):
    with worker_lock(database, aid) as acquired:
        if not acquired:
            return
        owner = uuid.uuid4().hex
        claimed = store.claim(database, aid, owner)
        if not claimed:
            return
        op, context = claimed
        stop = Event()

        def renew():
            while not stop.wait(store.LEASE / 4):
                try:
                    if not store.renewed(database, aid, owner):
                        return
                except sqlite3.OperationalError:
                    LOG.warning("lease heartbeat could not access ledger: %s", aid)

        heartbeat = Thread(target=renew, daemon=True)
        heartbeat.start()
        evidence_dir = database.parent / "evidence" / aid
        evidence_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        def pre_dispatch(operation):
            if time.time() >= context["deadline"]:
                return "dispatch_deadline"
            with store.db(database) as con:
                task = con.execute("SELECT paused,canceled FROM task WHERE id=?",
                                   (context["task_id"],)).fetchone()
                active = con.execute("SELECT 1 FROM attempt WHERE id=? AND state='RUNNING' AND owner=? AND lease>?",
                                     (aid, owner, time.time())).fetchone()
                auth = store.authorization(con, context["task_id"], operation, time.time())
                ref = store.authorization_reference(con, context["task_id"], operation)
            if not active or task["paused"] or task["canceled"]:
                return "task_stopped_before_execution"
            if auth:
                return auth
            context["authorization"] = ref[0] if ref else None
            return None

        context.update(evidence_dir=evidence_dir, progress=partial(store.progress, database, aid, owner),
                       pre_dispatch=pre_dispatch)
        try:
            try:
                context["settings"] = load(settings_path)
                impl = adapter(op)
                if context["mode"] == "reconcile":
                    result = impl.reconcile(op, context)
                else:
                    result = probe_first(op, context)
                    if result is None:
                        if time.time() >= context["deadline"]:
                            result = {"status": "waiting", "code": "endpoint_probe_consumed_budget"}
                        else:
                            blocker = pre_dispatch(op)
                            if blocker:
                                result = {"status": "waiting", "code": blocker}
                            else:
                                result = impl.execute(op, context)
                                if context.get("endpoint_evidence"):
                                    result = {**result, "endpoint_evidence": context["endpoint_evidence"]}
            except Exception as exc:
                LOG.error("worker %s failed in %s: %s", aid, op["kind"], type(exc).__name__)
                result = {"status": "unknown", "code": "worker_adapter_exception"}
            if not store.finish(database, aid, owner, result):
                LOG.warning("worker receipt fenced out: %s", aid)
        finally:
            stop.set()
            heartbeat.join(timeout=2)


def serve(database, settings_path, slots, poll, run_seconds):
    load(settings_path)
    store.initialize(database)
    if not 1 <= slots <= 32 or not 0.1 <= poll <= 10:
        raise ValueError("invalid worker count or poll interval")
    end = time.monotonic() + run_seconds if run_seconds else float("inf")
    children = {}
    while time.monotonic() < end:
        children = {aid: p for aid, p in children.items() if p.poll() is None}
        try:
            store.schedule(database, slots)
            for aid in store.get_launches(database, owner_absent=partial(absent, database), cap=slots):
                if aid in children:
                    continue
                command = [sys.executable, "-m", "server.project_workflow", "--db", str(database),
                           "--settings", str(settings_path), "worker", aid]
                options = ({"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP} if os.name == "nt"
                           else {"start_new_session": True})
                try:
                    children[aid] = subprocess.Popen(command, stdin=subprocess.DEVNULL, **options)
                except OSError as exc:
                    LOG.error("dispatch worker failed for %s: %s", aid, type(exc).__name__)
        except sqlite3.OperationalError as exc:
            LOG.warning("ledger poll failed: %s", type(exc).__name__)
        time.sleep(poll)


def submit_engineering(database, settings, manifest):
    """Register a goal on a new conversation branch of the fetched production main."""
    if set(manifest) - {"id", "title", "goal", "acceptance", "expires", "priority", "test_profile", "timeout"}:
        raise ValueError("unsupported engineering submission field")
    config = load(settings)
    repo = Path(config["adapters"]["repository"]).resolve(strict=True)
    root = Path(config["codex_worktrees_root"]).resolve(strict=True)
    if not root.is_dir() or not repo.is_dir():
        raise ValueError("engineering repository and worktree root must be provisioned")
    tid = store.key(manifest.get("id", uuid.uuid4().hex))
    branch = "codex/workflow-" + tid
    target = root / ("workflow-" + tid)
    if ":" in tid or target.exists():
        raise ValueError("invalid or existing engineering worktree identity")
    profile = store.key(manifest.get("test_profile", "test"))
    if profile not in config["adapters"].get("bindings", {}):
        raise ValueError("trusted test binding is not provisioned")
    env = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
    git = partial(subprocess.run, cwd=repo, check=True, env=env, capture_output=True, text=True)
    base, ref = git(["git", "ls-remote", "--exit-code", "origin", "refs/heads/main"], timeout=15).stdout.split()
    if ref != "refs/heads/main" or not store.SHA.fullmatch(base):
        raise ValueError("production main identity unavailable")
    git(["git", "fetch", "--no-tags", "origin", base], timeout=45)
    git(["git", "worktree", "add", "-b", branch, str(target), base], timeout=60)
    spec = {k: v for k, v in manifest.items() if k not in {"test_profile", "timeout"}}
    timeout = manifest.get("timeout", 300)
    try:
        return store.submit(database, {**spec, "id": tid, "candidate": base,
                            "session": {"worktree": str(target), "branch": branch, "base": base},
                            "steps": [
                                {"id": "implement", "kind": "codex", "timeout": timeout},
                                {"id": "test", "kind": "test", "profile": profile, "timeout": timeout,
                                 "depends": ["implement"], "inputs": {"candidate": "$candidate"}, "rework": True},
                            ]})
    except BaseException:
        # Keep the named branch/worktree if the registration fails; it is
        # outside the caller's checkout and can be inspected without data loss.
        LOG.error("engineering registration failed after creating branch %s", branch)
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=os.environ.get("PROBIGA_WORKFLOW_DB"))
    parser.add_argument("--settings", type=Path, default=os.environ.get("PROBIGA_WORKFLOW_SETTINGS"))
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init")
    commands.add_parser("list")
    commands.add_parser("status").add_argument("task")
    commands.add_parser("submit").add_argument("manifest", type=Path)
    commands.add_parser("submit-engineering").add_argument("manifest", type=Path)
    commands.add_parser("import-history").add_argument("manifest", type=Path)
    for action in ("pause", "resume", "cancel"):
        commands.add_parser(action).add_argument("task")
    decision = commands.add_parser("authorize")
    decision.add_argument("task")
    for name in ("kind", "environment", "resource", "endpoint", "candidate", "reference", "decision"):
        decision.add_argument("--" + name, required=True)
    decision.add_argument("--expires", type=float, required=True)
    run = commands.add_parser("serve")
    run.add_argument("--slots", type=int, default=4)
    run.add_argument("--poll", type=float, default=1)
    run.add_argument("--run-seconds", type=int, default=0)
    reconcile = commands.add_parser("reconcile")
    reconcile.add_argument("attempt")
    reconcile.add_argument("--seconds", type=int, default=120)
    commands.add_parser("worker").add_argument("attempt")
    a = parser.parse_args(argv)
    if not a.db:
        parser.error("--db or PROBIGA_WORKFLOW_DB is required")
    db = a.db.resolve()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if a.command in {"serve", "worker"}:
        if not a.settings:
            parser.error("trusted --settings or PROBIGA_WORKFLOW_SETTINGS is required")
        settings = a.settings.resolve()
        if a.command == "serve":
            serve(db, settings, a.slots, a.poll, a.run_seconds)
        else:
            worker(db, settings, a.attempt)
    elif a.command == "init":
        store.initialize(db)
    elif a.command == "submit-engineering":
        if not a.settings:
            parser.error("trusted --settings or PROBIGA_WORKFLOW_SETTINGS is required")
        print(submit_engineering(db, a.settings, load(a.manifest)))
    elif a.command in {"submit", "import-history"}:
        fn = store.submit if a.command == "submit" else store.import_history
        print(fn(db, load(a.manifest)))
    elif a.command == "list":
        print(store.pack(store.list_tasks(db)))
    elif a.command == "status":
        view = store.task_view(db, a.task)
        if view is None:
            parser.error("task does not exist")
        print(store.pack(view))
    elif a.command in {"pause", "resume", "cancel"}:
        store.control(db, a.task, a.command)
    elif a.command == "reconcile":
        store.reopen_reconciliation(db, a.attempt, a.seconds)
    elif a.command == "authorize":
        args = vars(a).copy()
        for attr in ("db", "command", "settings", "task"):
            args.pop(attr)
        store.grant(db, a.task, **args)


if __name__ == "__main__":
    main()
