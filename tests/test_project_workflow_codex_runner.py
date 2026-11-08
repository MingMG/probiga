from contextlib import contextmanager
import errno
import json
from pathlib import Path
import socket
import subprocess
import sys
import time

import pytest

from server.project_workflow import codex_runner


@pytest.mark.parametrize(("mode", "write_error", "exit_code"), [
    ("read", errno.EROFS, 0),
    ("write", errno.EROFS, 94),
    ("read", errno.EIO, None),
])
def test_probe_distinguishes_linux_read_only_mount_from_other_write_errors(monkeypatch, mode, write_error, exit_code):
    def sandbox_open(path, access):
        if (path, access) == ("hidden", "rb"):
            raise FileNotFoundError(path)
        assert (path, access) == ("write-canary", "x")
        raise OSError(write_error, "sandbox write denied")

    def blocked_network(*args, **kwargs):
        raise PermissionError(errno.EACCES, "sandbox network denied")

    monkeypatch.setattr(sys, "argv", ["probe", "hidden", "endpoint.sock", "12345", "write-canary", mode])
    for name in ("OPENAI_API_KEY", "GH_TOKEN", "GITHUB_TOKEN", "SSH_AUTH_SOCK"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(socket, "create_connection", blocked_network)
    monkeypatch.setattr(socket, "AF_UNIX", 1, raising=False)
    monkeypatch.setattr(socket, "socket", blocked_network)
    if exit_code is None:
        with pytest.raises(OSError) as error:
            exec(codex_runner._PROBE, {"open": sandbox_open})
        assert error.value.errno == write_error
    else:
        with pytest.raises(SystemExit) as result:
            exec(codex_runner._PROBE, {"open": sandbox_open})
        assert result.value.code == exit_code


def git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True,
                          check=True, timeout=15).stdout.strip()


@pytest.fixture
def owned_context(tmp_path, monkeypatch):
    checkout = tmp_path / "worktrees" / "isolated"
    checkout.mkdir(parents=True)
    git(checkout, "init", "-b", "codex/isolated-rework")
    (checkout / "value.py").write_text("VALUE = 1\n", encoding="utf-8")
    (checkout / "check.py").write_text("from value import VALUE\nassert VALUE == 2\n", encoding="utf-8")
    git(checkout, "add", "-A")
    git(checkout, "-c", "user.name=Workflow Test", "-c", "user.email=test@example.invalid",
        "-c", "commit.gpgsign=false", "commit", "-m", "test fixture")
    canary = tmp_path / "sandbox-canary"
    canary.write_text("isolated sentinel", encoding="utf-8")
    endpoint = tmp_path / "dedicated-app-server.sock"
    settings = {"codex_worktrees_root": str(checkout.parent)}
    monkeypatch.setattr(codex_runner, "_settings", lambda _op, _ctx: codex_runner.WorkerConfig(settings, endpoint, canary, False))
    return {"operation_id": "rework-1", "fence": 1, "deadline": time.time() + 60,
            "task_id": "workflow-test", "goal": "Fix VALUE to 2", "acceptance": ["check.py passes"],
            "session": {"worktree": str(checkout), "branch": "codex/isolated-rework",
                        "candidate": git(checkout, "rev-parse", "HEAD")},
            "evidence_dir": tmp_path / "evidence",
            "settings": settings}


def test_recovery_after_lost_turn_start_response_checks_external_turn_once(monkeypatch, owned_context):
    context = owned_context
    checkout = Path(context["session"]["worktree"])
    failed = subprocess.run([sys.executable, "-B", "check.py"], cwd=checkout, capture_output=True, text=True, timeout=10)
    assert failed.returncode == 1
    context["failure"] = {"tail": failed.stderr}
    operations = []
    received_prompt = []
    turn_state = ["inProgress"]

    class Server:
        def request(self, method, params):
            operations.append(method)
            if method == "command/exec":
                return {"exitCode": 0}
            if method == "thread/start":
                return {"thread": {"id": "managed-thread"}}
            if method == "turn/start":
                received_prompt.append(params["input"][0])
                raise codex_runner.RunnerError("CONNECTION_LOST", "Receipt lost", status="unknown")
            if method == "thread/turns/list":
                return {"data": [{"id": "older", "status": "completed"},
                                 {"id": "own-turn", "status": turn_state[0]}]}
            if method == "thread/items/list":
                content = received_prompt if params["turnId"] == "own-turn" else [{"text": "unrelated turn"}]
                return {"data": [
                    {"turnId": "another-turn", "item": {"type": "userMessage", "content": received_prompt}},
                    {"turnId": params["turnId"], "item": {"type": "userMessage", "content": content}},
                ]}
            pytest.fail(f"Unexpected App Server method {method}")

    @contextmanager
    def connect(*_args):
        yield Server()

    monkeypatch.setattr(codex_runner, "_connect", connect)
    operation = {"kind": "rework"}
    unknown = codex_runner.execute(operation, context)
    assert unknown["status"] == "unknown"
    assert "AssertionError" in received_prompt[0]["text"]
    active = codex_runner.reconcile(operation, context)
    assert active["status"] == "unknown"
    assert active["external_id"] == "own-turn"
    (checkout / "value.py").write_text("VALUE = 2\n", encoding="utf-8")
    turn_state[0] = "completed"
    completed = codex_runner.reconcile(operation, context)
    assert completed["status"] == "success"
    assert completed["evidence"]["candidate"] == git(checkout, "rev-parse", "HEAD")
    assert git(checkout, "rev-list", "--count", "HEAD") == "2"
    assert subprocess.run([sys.executable, "-B", "check.py"], cwd=checkout, timeout=10).returncode == 0
    assert codex_runner.reconcile(operation, context) == completed
    assert operations.count("turn/start") == 1
    assert operations.count("thread/items/list") == 2


@pytest.mark.parametrize("has_matching_entry", [True, False])
def test_supervisor_reads_diagnosis_from_its_own_wrapped_turn_item(has_matching_entry):
    report = {
        "root_cause_hypothesis": "Read-only filesystem",
        "evidence": "The write returned EROFS",
        "recommended_action": "Check the selected profile",
        "verification": "Confirm write mode rejects the read-only mount",
    }

    class Server:
        def request(self, method, params):
            assert method == "thread/items/list"
            assert params["turnId"] == "supervisor-turn"
            entries = [{"turnId": "another-turn", "item": {
                "type": "agentMessage", "text": json.dumps({**report, "evidence": "Unrelated failure"}),
            }}]
            if has_matching_entry:
                entries.append({"turnId": "supervisor-turn", "item": {
                    "type": "agentMessage", "text": json.dumps(report),
                }})
            return {"data": entries}

    evidence = {"thread_id": "managed-thread", "turn_id": "supervisor-turn"}
    if has_matching_entry:
        assert codex_runner._diagnosis(Server(), evidence) == report
    else:
        with pytest.raises(codex_runner.RunnerError) as error:
            codex_runner._diagnosis(Server(), evidence)
        assert error.value.code == "CODEX_DIAGNOSIS_MISSING"


def test_credentials_probe_blocks_before_thread_creation(monkeypatch, owned_context):
    class ExposedCredentialServer:
        def request(self, method, _params):
            assert method == "command/exec"
            return {"exitCode": 91}

    @contextmanager
    def connect(*_args):
        yield ExposedCredentialServer()

    monkeypatch.setattr(codex_runner, "_connect", connect)
    response = codex_runner.execute({"kind": "codex"}, owned_context)
    assert response["status"] == "failure"
    assert response["code"] == "CODEX_SANDBOX_NOT_VERIFIED"
    assert not list(owned_context["evidence_dir"].glob("*.json"))


def test_no_coding_outside_owned_worktree_root(owned_context):
    owned_context["settings"]["codex_worktrees_root"] = owned_context["evidence_dir"].parent / "different-root"
    response = codex_runner.execute({"kind": "codex"}, owned_context)
    assert response["status"] == "failure"
    assert response["code"] == "CODEX_WORKTREE_UNOWNED"


def test_unprovisioned_socket_blocks_before_opening_worktree():
    context = {"settings": {"codex_ws_url": "ws://127.0.0.1:9999"}}
    response = codex_runner.execute({"kind": "codex"}, context)
    assert response["status"] == "waiting"
    assert response["code"] == "CODEX_ENDPOINT_NOT_PROVISIONED"
