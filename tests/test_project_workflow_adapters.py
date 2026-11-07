from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import subprocess
import sys
import threading
import time

from server.project_workflow.adapters import Registry

CANDIDATE = "a" * 40
OTHER = "b" * 40


def _context(tmp_path, *, operation_id="operation-1", fence=1, authorization=None):
    return {
        "operation_id": operation_id,
        "task_id": "workflow-1",
        "fence": fence,
        "deadline": time.time() + 20,
        "evidence_dir": tmp_path / "evidence",
        "authorization": authorization,
    }


def _operation(kind, *, endpoint=None, environment="isolated", resource=None, **inputs):
    return {
        "kind": kind,
        "profile": "probe" if kind == "probe" else kind,
        "endpoint": endpoint,
        "resource": resource,
        "timeout": 10,
        "inputs": {"environment": environment, **inputs},
    }


def _fixture(tmp_path):
    script = tmp_path / "operator_entrypoint.py"
    script.write_text(
        """import json, os, sys
from pathlib import Path
run_file = Path(sys.argv[2])
request = json.load(sys.stdin)
def reply(status, code, **evidence):
    print(json.dumps({
        "status": status, "code": code, "evidence": evidence,
        "operation_id": request["operation_id"], "fence": request["fence"],
    }))
action = sys.argv[1]
if action == "deploy":
    with run_file.open("a") as runs:
        runs.write(request["operation_id"] + "\\n")
    os._exit(5)  # lost acknowledgement after the external write
if action == "reconcile":
    runs = run_file.read_text().splitlines()
    assert request["operation_id"] in runs
    reply("success", "receipt_verified", candidate=request["candidate"],
          endpoint=request["endpoint"], released=True)
if action == "probe":
    if not run_file.exists():
        reply("waiting", "qmt_unavailable")
    else:
        reply("success", "qmt_read_only_verified", endpoint="windows",
              build="a"*40, instance_id="host-123", model_instance_id="model-456",
              application_ok=True, no_order_ok=True)
""",
        encoding="utf-8",
    )
    return script


def test_deploy_receipt_is_reconciled_across_fences_without_second_execution(tmp_path):
    script = _fixture(tmp_path)
    runs = tmp_path / "external-runs"
    repo = tmp_path / "trusted-main"
    repo.mkdir()
    _git(repo, "init", "-b", "main")
    _git(repo, "config", "user.name", "Isolated Workflow")
    _git(repo, "config", "user.email", "isolated@example.invalid")
    _git(repo, "commit", "--allow-empty", "-m", "merged candidate")
    candidate = _git(repo, "rev-parse", "HEAD")
    config = {"trusted_main_remote": str(repo), "bindings": {
        "deploy": {
            "kind": "deploy", "endpoint": "linux", "resource": "production:linux",
            "environment": "production",
            "argv": [sys.executable, str(script), "deploy", str(runs)],
            "reconcile_argv": [sys.executable, str(script), "reconcile", str(runs)],
        },
    }}
    op = _operation(
        "deploy", endpoint="linux", environment="production",
        resource="production:linux", candidate=candidate,
    )
    assert Registry(config).execute(op, _context(tmp_path))["status"] == "waiting"
    assert not runs.exists()
    ctx = _context(tmp_path, authorization="approved-1")
    wrong = _operation(
        "deploy", endpoint="linux", environment="production",
        resource="production:linux", candidate=CANDIDATE,
    )
    assert Registry(config).execute(wrong, ctx)["code"] == "production_candidate_is_not_trusted_main"
    assert not runs.exists()
    expired = {**ctx, "pre_dispatch": lambda _: "authorization_expired"}
    assert Registry(config).execute(op, expired)["code"] == "authorization_expired"
    assert not runs.exists()
    result = Registry(config).execute(op, ctx)
    assert result["status"] == "unknown"
    restored = Registry(config)
    result = restored.reconcile(op, {**ctx, "fence": 2})
    assert result["evidence"] == {
        "candidate": candidate, "endpoint": "linux", "released": True,
    }
    assert restored.execute(op, {**ctx, "fence": 2})["status"] == "success"
    assert runs.read_text().splitlines() == [ctx["operation_id"]]


def test_windows_probe_waits_for_application_and_checks_recovered_identity(tmp_path):
    script = _fixture(tmp_path)
    online = tmp_path / "online"
    config = {"bindings": {
        "probe-windows": {
            "kind": "probe", "endpoint": "windows", "resource": None,
            "environment": "isolated",
            "argv": [sys.executable, str(script), "probe", str(online)],
        },
    }}
    op = _operation("probe", endpoint="windows", expected_instance_id="host-123")
    assert Registry(config).execute(op, _context(tmp_path))["code"] == "qmt_unavailable"
    online.touch()
    result = Registry(config).execute(op, _context(tmp_path))
    assert result["status"] == "success"
    assert result["evidence"]["model_instance_id"] == "model-456"
    op["inputs"]["expected_instance_id"] = "host-321"
    assert Registry(config).execute(op, _context(tmp_path))["status"] == "waiting"


def _git(root, *args):
    return subprocess.run(
        ["git", "-C", str(root), *args], check=True,
        capture_output=True, text=True, timeout=10,
    ).stdout.strip()


def test_real_failed_command_forwards_case_and_candidate_without_secret(tmp_path, monkeypatch):
    repo = tmp_path / "candidate"
    repo.mkdir()
    _git(repo, "init")
    _git(repo, "config", "user.name", "Isolated Workflow")
    _git(repo, "config", "user.email", "isolated@example.invalid")
    (repo / "tracked.txt").write_text("candidate")
    _git(repo, "add", "tracked.txt")
    _git(repo, "commit", "-m", "candidate")
    candidate = _git(repo, "rev-parse", "HEAD")
    monkeypatch.setenv("PROBIGA_TEST_PRIVATE_VALUE", "fixture-secret")
    script = tmp_path / "tests.py"
    script.write_text(
        """import os
assert "PROBIGA_TEST_PRIVATE_VALUE" not in os.environ
print("FAILED tests/test_candidate.py::test_gate - mysql://user:private@example.invalid")
print("E   AssertionError: password=private")
raise SystemExit(1)
""", encoding="utf-8",
    )
    config = {"bindings": {
        "test": {
            "kind": "test", "endpoint": None, "resource": None, "environment": "isolated",
            "argv": [sys.executable, str(script)], "protocol": "exit", "managed_worktree": True,
        },
    }}
    op = _operation("test", candidate=candidate)
    ctx = {**_context(tmp_path), "session": {"worktree": str(repo)}}
    result = Registry(config).execute(op, ctx)
    assert result["status"] == "failure"
    assert result["code"] == "test_failed"
    assert result["evidence"]["candidate"] == candidate
    assert result["log_tail"] == "tests/test_candidate.py::test_gate\nE   AssertionError"
    assert len(result["failure_key"]) == 64
    assert "private" not in json.dumps(result)
    op["inputs"]["candidate"] = OTHER
    assert Registry(config).execute(op, ctx)["code"] == "test_worktree_candidate_mismatch"


def test_linux_native_application_probe_keeps_observed_old_version(tmp_path):
    payload = {
        "status": "ok",
        "release_revision": {
            "actual_git_sha": OTHER, "deployment_mode": "production", "matches_expected": True,
        },
        "component_release": {"ready": True, "linux_build_sha": OTHER},
        "standalone_scheduler_heartbeat": {"ready": True},
    }

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            assert self.path == "/api/health"
            self.send_response(200)
            self.end_headers()
            self.wfile.write(json.dumps(payload).encode())

        def log_message(self, *args):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            config = {"linux_health_url": f"http://127.0.0.1:{server.server_port}/api/health"}
            op = _operation("probe", endpoint="linux", candidate=CANDIDATE)
            result = Registry(config).execute(op, _context(tmp_path))
            assert result["status"] == "success"
            assert result["evidence"]["build"] == OTHER
            assert result["evidence"]["production"] is True
            payload["component_release"]["linux_build_sha"] = CANDIDATE
            result = Registry(config).execute(op, _context(tmp_path))
            assert result["status"] == "waiting"
        finally:
            server.shutdown()
            thread.join(timeout=2)


def test_operation_payload_cannot_select_program_or_unisolated_simulation(tmp_path):
    ctx = _context(tmp_path)
    op = _operation("simulation", candidate=CANDIDATE, argv=[sys.executable])
    binding = {
        "kind": "simulation", "endpoint": None, "environment": "isolated", "resource": None,
        "argv": [sys.executable, "-c", "raise RuntimeError('cannot run')"],
    }
    result = Registry({"bindings": {"simulation": binding}}).execute(op, ctx)
    assert result["code"] == "isolated_simulation_binding_required"
    assert not (Path(ctx["evidence_dir"]) / "dispatch").exists()



def test_classification_exercises_actual_git_contract_and_windows_consumers(tmp_path):
    repo = tmp_path / "release-history"
    repo.mkdir()
    _git(repo, "init")
    _git(repo, "config", "user.name", "Isolated Workflow")
    _git(repo, "config", "user.email", "isolated@example.invalid")

    def commit(path, source):
        file = repo / path
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text(source, encoding="utf-8")
        _git(repo, "add", path)
        _git(repo, "commit", "-m", path)
        return _git(repo, "rev-parse", "HEAD")

    windows_path = "tools/import_qmt_datadir_daily.py"
    base = commit(windows_path, "BATCH_LIMIT = 100\n\ndef read():\n    return 1\n")
    linux = commit("server/static/page.html", "<p>Linux UI</p>")
    windows = commit(windows_path, "BATCH_LIMIT = 100\n\ndef read():\n    return 2\n")
    cross_end = commit("server/db/example.sql", "CREATE TABLE st_workflow_check (id INT);")
    config = {"repository": str(repo), "windows_only_runtime_paths": [windows_path]}
    registry = Registry(config)
    ctx = _context(tmp_path)

    def classify(first, last):
        return registry.execute(_operation("classify", base=first, candidate=last), ctx)

    assert classify(base, linux)["evidence"]["endpoints"] == ["linux"]
    assert classify(linux, windows)["evidence"]["endpoints"] == ["windows"]
    assert classify(windows, cross_end)["evidence"]["endpoints"] == ["linux", "windows"]
    assert Registry({"repository": str(repo)}).execute(
        _operation("classify", base=linux, candidate=windows), ctx,
    )["code"] == "windows_runtime_scope_requires_verified_consumer_inventory"
