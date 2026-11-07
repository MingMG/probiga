"""Exercise real Codex rework using a separately provisioned engineering App Server."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.project_workflow import store
from server.project_workflow.__main__ import load


def git(worktree, *args):
    command = ["git", "-c", f"core.hooksPath={os.devnull}", "-c", "commit.gpgsign=false", *args]
    return subprocess.run(command, cwd=worktree, capture_output=True, check=True, text=True, timeout=15).stdout.strip()


def fixture(root):
    directory = Path(tempfile.mkdtemp(prefix="probiga-codex-acceptance-", dir=root)).resolve()
    worktree = directory / "worktree"
    worktree.mkdir()
    git(worktree, "init", "-b", "codex/workflow-acceptance-drill")
    git(worktree, "config", "core.autocrlf", "false")
    (worktree / "calculation.py").write_text("def calculated():\n    return 1\n", encoding="utf-8")
    (worktree / "verify.py").write_text("from calculation import calculated\n"
                                       "assert calculated() == 2, 'calculated() must return 2'\n", encoding="utf-8")
    git(worktree, "add", "-A")
    git(worktree, "-c", "user.name=Workflow Drill", "-c", "user.email=workflow@example.invalid",
        "commit", "-m", "Isolated failing test fixture")
    return directory, worktree, git(worktree, "rev-parse", "HEAD")


def start(database, settings):
    return subprocess.Popen([sys.executable, "-m", "server.project_workflow", "--db", str(database), "--settings",
                             str(settings), "serve", "--slots", "1", "--poll", "0.2", "--run-seconds", "290"],
                            cwd=ROOT, stdin=subprocess.DEVNULL)


def stop(coordinator):
    if coordinator.poll() is None:
        coordinator.terminate()
        try:
            coordinator.wait(timeout=5)
        except subprocess.TimeoutExpired:
            coordinator.kill()
            coordinator.wait(timeout=5)


def attempts(database, task):
    with store.db(database) as connection:
        rows = connection.execute("SELECT id,operation,state,result FROM attempt WHERE task=? ORDER BY rowid", (task,)).fetchall()
    return [{"id": row["id"], "kind": store.unpack(row["operation"])["kind"], "state": row["state"],
             "result": store.unpack(row["result"], {})} for row in rows]


def submitted_journal(directory, operation_id):
    key = hashlib.sha256(operation_id.encode()).hexdigest()
    path = directory / "evidence" / operation_id / (key + ".codex.json")
    if not path.is_file():
        return {}
    journal = load(path)
    return journal if journal.get("phase") == "submitted" else {}


def run(source):
    provided = load(source)
    worktrees_root = Path(provided["codex_worktrees_root"]).resolve(strict=True)
    directory, worktree, initial = fixture(worktrees_root)
    print(f"Isolated drill evidence: {directory}", flush=True)
    settings = {key: value for key, value in provided.items() if key.startswith("codex_")}
    resource = "worktree:" + hashlib.sha256(str(worktree).casefold().encode()).hexdigest()
    settings["adapters"] = {"bindings": {"acceptance-drill": {"kind": "test", "endpoint": None,
                            "environment": "isolated", "resource": resource, "cwd": str(worktree),
                            "argv": [sys.executable, "-B", "verify.py"], "protocol": "exit"}}}
    settings_file = directory / "settings.json"
    settings_file.write_text(store.pack(settings), encoding="utf-8")
    database = directory / "tasks.sqlite"
    store.initialize(database)
    task = store.submit(database, {"title": "Isolated real Codex rework drill",
                        "goal": "Change calculation.py so calculated() returns 2. Leave verify.py unchanged.",
                        "acceptance": ["verify.py exits zero", "Only calculation.py changes"],
                        "expires": time.time() + 270, "candidate": initial,
                        "session": {"worktree": str(worktree), "branch": "codex/workflow-acceptance-drill", "candidate": initial},
                        "steps": [{"id": "verify", "kind": "test", "profile": "acceptance-drill", "rework": True,
                                   "timeout": 150, "inputs": {"candidate": "$candidate", "environment": "isolated"}}]})
    coordinator = start(database, settings_file)
    restart = None
    end = time.monotonic() + 270
    current = []
    try:
        while time.monotonic() < end and coordinator.poll() is None:
            current = attempts(database, task)
            if restart is None:
                for attempt in current:
                    if attempt["kind"] != "rework" or attempt["state"] != "RUNNING":
                        continue
                    journal = submitted_journal(directory, attempt["id"])
                    if journal.get("turn_id") and not journal.get("result"):
                        restart = {"operation_id": attempt["id"], "turn_id": journal["turn_id"]}
                        stop(coordinator)
                        coordinator = start(database, settings_file)
                        break
            if store.task_view(database, task)["state"] in {"COMPLETE", "FAILED"}:
                break
            if any(item["result"].get("code") in {"CODEX_MODEL_UNAUTHORIZED", "CODEX_SANDBOX_NOT_VERIFIED",
                                                 "CODEX_ENDPOINT_NOT_PROVISIONED"} for item in current):
                break
            time.sleep(0.2)
        current = attempts(database, task)
    finally:
        stop(coordinator)
    codes = [item["result"].get("code") for item in current]
    candidate = git(worktree, "rev-parse", "HEAD")
    passed = (codes == ["test_failed", "CODEX_CANDIDATE_READY", "test_passed"] and restart is not None
              and current[1]["result"].get("external_id") == restart["turn_id"]
              and current[2]["result"].get("evidence", {}).get("candidate") == candidate
              and git(worktree, "diff", "--name-only", initial, candidate) == "calculation.py")
    report = {"task_id": task, "status": "COMPLETE" if passed else "UNVERIFIED", "initial_candidate": initial,
              "candidate": candidate, "coordinator_restart": restart, "attempt_codes": codes}
    (directory / "drill-result.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report))
    return 0 if passed else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--settings", type=Path, required=True, help="Trusted isolated engineering App Server settings")
    sys.exit(run(parser.parse_args().settings))
