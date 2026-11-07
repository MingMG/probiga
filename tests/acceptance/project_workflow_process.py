"""Process-level isolation drill: no production network, broker, or account."""
from contextlib import closing
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
from urllib.request import ProxyHandler, build_opener

ROOT = Path(__file__).resolve().parents[2]
SHA = "7" * 40


def reply(request, status, code, evidence=None):
    print(json.dumps({"operation_id": request["operation_id"], "fence": request["fence"],
                      "status": status, "code": code, "evidence": evidence or {}}), flush=True)


def endpoint(mode, remote):
    req = json.load(sys.stdin)
    if mode == "probe":
        if not remote.with_suffix(".online").exists():
            reply(req, "waiting", "isolated_windows_offline")
        else:
            reply(req, "success", "isolated_windows_ready", {
                "endpoint": "windows", "build": SHA, "instance_id": "isolated-edge",
                "model_instance_id": "isolated-qmt", "application_ok": True, "no_order_ok": True})
        return
    if mode in {"linux", "windows"}:
        remote.with_suffix("." + mode).write_text(req["operation_id"], encoding="utf-8")
        reply(req, "success", "isolated_" + mode + "_verified")
        return
    with closing(sqlite3.connect(remote, timeout=3)) as con, con:
        if mode == "submit":
            con.execute("INSERT INTO writes VALUES(?,?,?)", (req["operation_id"], req["fence"], os.getppid()))
        else:
            identity = con.execute("SELECT fence FROM writes WHERE id=?", (req["operation_id"],)).fetchone()
            if not identity or identity[0] != req["fence"]:
                reply(req, "unknown", "isolated_receipt_unavailable")
                return
    if mode == "submit":
        # A real external write committed before the workflow worker gets its receipt.
        time.sleep(5)
    reply(req, "success", "isolated_simulation_verified", {"isolated": True, "real_orders": 0})


def wait(fn, seconds, stage):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = fn()
        if result:
            return result
        time.sleep(0.15)
    raise RuntimeError("drill deadline: " + stage)


def process_drill(directory):
    sys.path.insert(0, str(ROOT))
    from server.project_workflow import store

    path = directory / "tasks.sqlite3"
    remote = directory / "isolated.sqlite3"
    config = directory / "settings.json"
    store.initialize(path)
    with closing(sqlite3.connect(remote)) as con, con:
        con.execute("CREATE TABLE writes(id TEXT,fence INTEGER,worker INTEGER)")
    actions = {}

    def binding(profile, mode, kind, target=None, resource=None):
        args = [sys.executable, str(Path(__file__).resolve())]
        actions[profile] = {"kind": kind, "endpoint": target, "resource": resource,
                            "environment": "isolated", "argv": [*args, mode, str(remote)]}
        return actions[profile]

    binding("probe-windows", "probe", "probe", "windows")
    binding("linux-fixture", "linux", "verify")
    binding("windows-fixture", "windows", "verify", "windows")
    sim = binding("simulation", "submit", "simulation", resource="simulation:isolated")
    sim.update(isolated=True, reconcile_argv=[*sim["argv"][:2], "reconcile", str(remote)])
    config.write_text(json.dumps({"adapters": {"bindings": actions}}), encoding="utf-8")

    def submit(id, steps):
        store.submit(path, {"id": id, "title": id, "goal": "Validate isolated workflow process recovery",
                            "acceptance": ["Durable receipt and exactly one submission"],
                            "expires": time.time() + 100, "steps": steps})

    submit("endpoints", [
        {"id": "linux", "kind": "verify", "profile": "linux-fixture"},
        {"id": "windows", "kind": "verify", "endpoint": "windows", "profile": "windows-fixture"},
    ])

    def launch():
        return subprocess.Popen([sys.executable, "-m", "server.project_workflow", "--db", str(path),
                                 "--settings", str(config), "serve", "--poll", "0.2"],
                                cwd=ROOT, stdin=subprocess.DEVNULL)

    def status(id):
        return store.task_view(path, id)

    def writes():
        with closing(sqlite3.connect(remote, timeout=3)) as con, con:
            return con.execute("SELECT id,fence,worker FROM writes").fetchall()

    orchestrator = launch()
    worker = None
    try:
        def independent_done():
            view = status("endpoints")
            return view if view["steps"][0]["state"] == "COMPLETE" and view["steps"][1]["state"] == "BLOCKED" else None
        wait(independent_done, 15, "independent Linux phase while isolated Windows offline")
        assert not remote.with_suffix(".windows").exists()
        linux_id = remote.with_suffix(".linux").read_text(encoding="utf-8")
        remote.with_suffix(".online").touch()
        wait(lambda: status("endpoints")["state"] == "COMPLETE", 20, "isolated Windows identity recovery")
        assert remote.with_suffix(".linux").read_text(encoding="utf-8") == linux_id
        windows_id = remote.with_suffix(".windows").read_text(encoding="utf-8")
        assert windows_id != linux_id
        submit("external", [{"id": "simulate", "kind": "simulation", "resource": "simulation:isolated", "timeout": 30}])
        durable = wait(writes, 15, "external write committed")
        aid, fence, worker = durable[0]
        assert len(durable) == 1
        assert status("external")["state"] == "RUNNING"
        orchestrator.kill()
        orchestrator.wait(timeout=5)
        os.kill(worker, signal.SIGTERM if os.name == "nt" else signal.SIGKILL)
        worker = None
        orchestrator = launch()
        wait(lambda: status("external")["state"] == "COMPLETE", 40, "post-crash read-only reconciliation")
        assert writes() == durable
        assert "worker_absent_reconcile" in {e["code"] for e in status("external")["events"]}
        print(json.dumps({"environment": "isolated process drill; fixture endpoint; no production connection",
                          "linux_operation": linux_id, "windows_operation": windows_id,
                          "reconciled_operation": aid, "fence": fence,
                          "durable_external_writes": len(durable), "real_order_calls": 0}))
    finally:
        if orchestrator.poll() is None:
            orchestrator.kill()
        orchestrator.wait(timeout=5)
        if worker is not None:
            try:
                os.kill(worker, signal.SIGTERM)
            except OSError:
                pass


if __name__ == "__main__":
    if len(sys.argv) == 3:
        endpoint(sys.argv[1], Path(sys.argv[2]))
    else:
        with tempfile.TemporaryDirectory(prefix="probiga-workflow-drill-") as work:
            process_drill(Path(work))
