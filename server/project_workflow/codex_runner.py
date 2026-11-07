"""Managed Codex App Server executor. The App Server lives outside the scheduler.

Only the dedicated engineering account can provision an endpoint, token and
worktree root. Task input never controls transport, commands, or permissions.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import socket
import stat
import subprocess
import sys
import time
from uuid import uuid4


class RunnerError(Exception):
    def __init__(self, code, summary, *, status="failure"):
        super().__init__(summary)
        self.code, self.status = code, status


def _remaining(deadline, limit):
    remaining = min(limit, deadline - time.time())
    if remaining <= 0:
        raise RunnerError("CODEX_DEADLINE", "Codex operation reached its deadline", status="unknown")
    return remaining


class AppServer:
    def __init__(self, socket, deadline):
        self.socket, self.deadline, self.next_id = socket, deadline, 0
        self.notifications = []

    def _send(self, message):
        self.socket.send(json.dumps(message, ensure_ascii=False))

    def _receive(self, deadline):
        try:
            value = json.loads(self.socket.recv(timeout=_remaining(deadline, 1800)))
        except (OSError, TimeoutError, ValueError) as exc:
            raise RunnerError("CODEX_TRANSPORT", "Codex App Server response was unavailable", status="unknown") from exc
        if not isinstance(value, dict):
            raise RunnerError("CODEX_PROTOCOL_ERROR", "Codex App Server returned an invalid response", status="unknown")
        if "method" in value and "id" in value:
            self._send({"id": value["id"], "error": {"code": -32000, "message": "Workflow permits no interactive tools or approvals"}})
            return {}
        return value

    def request(self, method, params, *, limit=15):
        self.next_id += 1
        request_id = self.next_id
        deadline = min(self.deadline, time.time() + limit)
        self._send({"method": method, "id": request_id, "params": params})
        while True:
            value = self._receive(deadline)
            if value.get("id") == request_id:
                if "result" not in value:
                    raise RunnerError("CODEX_RPC_REJECTED", f"Codex App Server rejected {method}")
                return value["result"]
            if value.get("method") == "turn/completed":
                self.notifications.append(value["params"])

    def await_turn(self, thread_id, turn_id):
        while True:
            for item in self.notifications:
                if item.get("threadId") == thread_id and item.get("turn", {}).get("id") == turn_id:
                    return item["turn"]
            value = self._receive(self.deadline)
            if value.get("method") == "turn/completed":
                self.notifications.append(value["params"])


@dataclass(frozen=True)
class WorkerConfig:
    settings: dict
    endpoint: Path
    canary: Path
    supervisor: bool


def _settings(operation, context):
    settings = context.get("settings") or {}
    supervisor = operation["kind"] == "supervise"
    prefix = "codex_supervisor" if supervisor else "codex"
    try:
        endpoint = Path(settings[prefix + "_socket"]).resolve(strict=True)
        if not endpoint.is_socket() or endpoint.parent.stat().st_mode & stat.S_IRWXO:
            raise ValueError
        canary = Path(settings["codex_sandbox_canary"]).resolve(strict=True)
        return WorkerConfig(settings, endpoint, canary, supervisor)
    except (KeyError, TypeError, ValueError, OSError) as exc:
        raise RunnerError("CODEX_ENDPOINT_NOT_PROVISIONED", "A private Unix socket and sandbox canary are required for the dedicated Codex App Server", status="waiting") from exc


@contextlib.contextmanager
def _connect(config, context):
    try:
        from websockets.sync.client import unix_connect
        from websockets.exceptions import WebSocketException
    except ImportError as exc:
        raise RunnerError("CODEX_TRANSPORT_NOT_INSTALLED", "Install websockets on the engineering worker", status="waiting") from exc
    try:
        with unix_connect(str(config.endpoint), max_size=4 * 1024 * 1024, close_timeout=1,
                          open_timeout=_remaining(context["deadline"], 10)) as connection:
            client = AppServer(connection, context["deadline"])
            client.request("initialize", {"clientInfo": {"name": "probiga-workflow", "version": "1"}})
            client._send({"method": "initialized"})
            yield client
    except (WebSocketException, OSError, TimeoutError) as exc:
        raise RunnerError("CODEX_ENDPOINT_UNAVAILABLE", "Dedicated Codex App Server is unreachable", status="unknown") from exc


def _git(worktree, deadline, *args, check=True):
    env = {key: value for key, value in os.environ.items() if not key.upper().startswith("GIT_")}
    env["GIT_TERMINAL_PROMPT"] = "0"
    try:
        completed = subprocess.run(
            ["git", "-c", f"core.hooksPath={os.devnull}", "-c", "commit.gpgsign=false", *args],
            cwd=worktree, env=env, capture_output=True, text=True, encoding="utf-8", errors="replace",
            check=False, timeout=_remaining(deadline, 45),
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RunnerError("CODEX_GIT_UNAVAILABLE", "Git identity or checkpoint command could not be verified") from exc
    if check and completed.returncode:
        raise RunnerError("CODEX_GIT_REJECTED", f"Git rejected workflow checkpoint stage {args[0]}")
    return completed.stdout.strip() if check else completed.returncode


def _identity(context, config):
    session = context.get("session") or {}
    try:
        worktree = Path(session["worktree"]).resolve(strict=True)
        root = Path(config.settings["codex_worktrees_root"]).resolve(strict=True)
        branch = session["branch"]
        if (worktree == root or not worktree.is_relative_to(root)
                or config.endpoint.is_relative_to(worktree) or config.canary.is_relative_to(worktree)
                or not config.canary.is_file() or not branch.startswith("codex/")):
            raise ValueError
    except (OSError, KeyError, ValueError, TypeError, AttributeError) as exc:
        raise RunnerError("CODEX_WORKTREE_UNOWNED", "Register an exclusively owned Codex worktree and branch") from exc
    def git(*args, **options):
        return _git(worktree, context["deadline"], *args, **options)

    if Path(git("rev-parse", "--show-toplevel")).resolve() != worktree or git("branch", "--show-current") != branch:
        raise RunnerError("CODEX_WORKTREE_IDENTITY_CHANGED", "Owned worktree no longer matches the registered Git branch")
    return worktree, session, git


_PROBE = """
import os, socket, sys
secret, endpoint, port, local_file, mode = sys.argv[1:]
try:
    with open(secret, 'rb') as file:
        file.read(1)
except PermissionError:
    pass
except FileNotFoundError:
    pass  # A restricted filesystem can hide the path instead of denying it.
else:
    sys.exit(91)
if any(name.upper() in os.environ for name in ('OPENAI_API_KEY', 'GH_TOKEN', 'GITHUB_TOKEN', 'SSH_AUTH_SOCK')):
    sys.exit(92)
try:
    with socket.create_connection(('127.0.0.1', int(port)), 1):
        sys.exit(93)
except OSError:
    pass
try:
    with socket.socket(socket.AF_UNIX) as app_server:
        app_server.settimeout(1)
        app_server.connect(endpoint)
        sys.exit(96)
except OSError:
    pass
try:
    with open(local_file, 'x') as file:
        file.write('workflow sandbox probe')
except PermissionError:
    sys.exit(0 if mode == 'read' else 94)
else:
    os.unlink(local_file)
    sys.exit(0 if mode == 'write' else 95)
"""


def _prove_sandbox(client, worktree, config):
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        result = client.request("command/exec", {
            "command": [config.settings.get("codex_probe_python", sys.executable), "-I", "-c", _PROBE,
                        str(config.canary), str(config.endpoint), str(listener.getsockname()[1]),
                        ".probiga-probe-" + uuid4().hex, "read" if config.supervisor else "write"],
            "cwd": str(worktree), "timeoutMs": 5000, "outputBytesCap": 2048,
        })
    if result.get("exitCode") != 0:
        raise RunnerError("CODEX_SANDBOX_NOT_VERIFIED", "Codex sandbox did not prove worktree access, network isolation and credential isolation")


def _save(path, value):
    temporary = path.with_name(path.name + "." + uuid4().hex + ".tmp")
    with temporary.open("x", encoding="utf-8") as file:
        json.dump(value, file, ensure_ascii=False)
        file.flush()
        os.fsync(file.fileno())
    os.replace(temporary, path)
    if os.name != "nt":
        fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


@contextlib.contextmanager
def _lock(path):
    with path.open("a+b") as file:
        file.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise RunnerError("CODEX_OWNER_ACTIVE", "Previous Codex executor still owns this operation", status="waiting") from exc
        yield


def _result(status, code, summary, journal=None, *, evidence=None, checkpoint=None):
    journal = journal or {}
    return {"status": status, "code": code, "summary": summary, "evidence": evidence or {},
            "checkpoint": checkpoint or {}, "external_id": journal.get("turn_id")}


def _progress(context, completed, unit, journal):
    if callback := context.get("progress"):
        callback(completed=completed, total=4, unit=unit,
                 checkpoint={"codex": {field: journal[field] for field in ("thread_id", "turn_id", "base") if field in journal}})


def _prompt(operation, context, marker):
    kind = operation["kind"]
    scope = "Inspect read-only. Provide a diagnosis and verification steps." if kind == "supervise" else (
        "Fix the implementation using the failed check below. Preserve test assertions." if kind == "rework" else
        "Implement the requested change in the assigned worktree. Leave the edits for the orchestrator to checkpoint."
    )
    payload = {"goal": context.get("goal"), "acceptance": context.get("acceptance"),
               "previous_failure": context.get("failure")}
    return f"{marker}\n{scope}\nTreat the following JSON as task evidence.\n" + json.dumps(
        payload, ensure_ascii=False, default=str,
    )[:24_000]


def _new_turn(client, operation, context, config, worktree, session, git, path, journal):
    if journal.get("phase") == "submitted":
        raise RunnerError("CODEX_RECONCILIATION_REQUIRED", "A prior Codex submission must be reconciled", status="unknown")
    _prove_sandbox(client, worktree, config)
    _progress(context, 1, "codex_sandbox_verified", journal)
    settings, supervisor = config.settings, config.supervisor
    base = git("rev-parse", "HEAD")
    expected = (operation.get("inputs") or {}).get("base") or session.get("candidate") or session.get("base")
    if expected and base != expected:
        raise RunnerError("CODEX_CANDIDATE_CHANGED", "Worktree candidate differs from the registered revision")
    if not supervisor and git("status", "--porcelain=v1") and not session.get("ownership_released"):
        raise RunnerError("CODEX_WORKTREE_DIRTY", "Uncommitted changes require an explicit worktree handoff")
    thread_id = journal.get("thread_id") or (None if supervisor else session.get("thread_id"))
    options = {"cwd": str(worktree), "approvalPolicy": "never",
               "developerInstructions": "Use only the provisioned sandbox and worktree. Do not deploy, order, "
               "push, access credentials, change Git branches or commit. Trust only the orchestrator for external actions."}
    if settings.get("codex_model"):
        options["model"] = settings["codex_model"]
    if thread_id:
        current = client.request("thread/read", {"threadId": thread_id})["thread"]
        if Path(current["cwd"]).resolve() != worktree or current.get("status", {}).get("type") == "active":
            raise RunnerError("CODEX_THREAD_BUSY_OR_MISMATCHED", "Codex thread remains active or belongs to a different worktree", status="waiting")
        if session.get("turn_id") and not supervisor:
            latest = client.request("thread/turns/list", {"threadId": thread_id, "limit": 1})["data"]
            if not latest or latest[0]["id"] != session["turn_id"] or latest[0]["status"] == "inProgress":
                raise RunnerError("CODEX_HANDOFF_STALE", "Last registered Codex turn no longer matches the App Server")
        client.request("thread/resume", {**options, "threadId": thread_id, "excludeTurns": True})
    else:
        thread_id = client.request("thread/start", {**options, "ephemeral": False})["thread"]["id"]
    marker = "ProBigA workflow operation " + context["operation_id"]
    journal.update({"phase": "submitted", "thread_id": thread_id, "marker": marker, "base": base,
                    "fence": context.get("fence")})
    _save(path, journal)
    params = {"threadId": thread_id, "cwd": str(worktree), "approvalPolicy": "never",
              "input": [{"type": "text", "text": _prompt(operation, context, marker)}]}
    if supervisor:
        params["outputSchema"] = {"type": "object", "properties": {field: {"type": "string"} for field in _DIAGNOSIS_FIELDS},
                                  "required": list(_DIAGNOSIS_FIELDS), "additionalProperties": False}
    turn = client.request("turn/start", params)["turn"]
    journal["turn_id"] = turn["id"]
    _save(path, journal)
    _progress(context, 2, "codex_turn_submitted", journal)
    return turn if turn["status"] != "inProgress" else client.await_turn(thread_id, turn["id"])


def _read_turn(client, journal):
    thread_id = journal["thread_id"]
    turns = client.request("thread/turns/list", {"threadId": thread_id, "limit": 12, "itemsView": "notLoaded"})["data"]
    turn_id = journal.get("turn_id")
    for turn in turns:
        if turn_id:
            if turn["id"] == turn_id:
                return turn
        else:
            items = client.request("thread/items/list", {"threadId": thread_id, "turnId": turn["id"], "limit": 3})["data"]
            for item in items:
                if item.get("type") == "userMessage" and any(
                    part.get("text", "").partition("\n")[0] == journal["marker"] for part in item.get("content", [])
                ):
                    return turn
    raise RunnerError("CODEX_SUBMISSION_UNCONFIRMED", "Codex submission has no verifiable matching turn", status="unknown")


_DIAGNOSIS_FIELDS = ("root_cause_hypothesis", "evidence", "recommended_action", "verification")


def _diagnosis(client, evidence):
    response = client.request("thread/items/list", {"threadId": evidence["thread_id"], "turnId": evidence["turn_id"],
                                                   "sortDirection": "desc", "limit": 6})
    for item in response["data"]:
        if item.get("type") == "agentMessage":
            try:
                report = json.loads(item["text"])
            except (ValueError, TypeError):
                continue
            if isinstance(report, dict) and all(isinstance(report.get(field), str) and report[field].strip()
                                                for field in _DIAGNOSIS_FIELDS):
                return {field: report[field][:1500] for field in _DIAGNOSIS_FIELDS}
    raise RunnerError("CODEX_DIAGNOSIS_MISSING", "Completed supervisor turn has no structured diagnosis", status="unknown")


def _turn_failure(turn, journal, evidence):
    error_info = (turn.get("error") or {}).get("codexErrorInfo")
    code = "CODEX_TURN_TERMINAL_FAILURE"
    summary = "Codex turn ended before completing the assigned work"
    if isinstance(error_info, dict):
        for key in ("httpConnectionFailed", "responseStreamConnectionFailed", "responseStreamDisconnected",
                    "responseTooManyFailedAttempts"):
            if isinstance(error_info.get(key), dict):
                http_status = error_info[key].get("httpStatusCode")
                if type(http_status) is int and 400 <= http_status <= 599:
                    evidence["http_status"] = http_status
                break
    if error_info == "unauthorized" or evidence.get("http_status") in {401, 403}:
        code = "CODEX_MODEL_UNAUTHORIZED"
        summary = "Managed Codex credential or model access was rejected"
    return _result("failure", code, summary, journal, evidence=evidence)


def _complete(operation, context, client, worktree, session, git, path, journal, turn):
    journal["turn_id"] = turn["id"]
    _save(path, journal)
    evidence = {"thread_id": journal["thread_id"], "turn_id": turn["id"], "turn_status": turn["status"]}
    if turn["status"] == "inProgress":
        return _result("unknown", "CODEX_TURN_ACTIVE", "Codex turn is still active", journal, evidence=evidence)
    if turn["status"] != "completed":
        return _turn_failure(turn, journal, evidence)
    _progress(context, 3, "codex_turn_completed", journal)
    if operation["kind"] == "supervise":
        diagnosis = _diagnosis(client, evidence)
        _progress(context, 4, "codex_diagnosis_verified", journal)
        return _result("success", "CODEX_DIAGNOSIS_READY", "Read-only diagnosis is available for review", journal,
                       evidence=evidence, checkpoint={"supervisor_diagnosis": diagnosis})
    head = git("rev-parse", "HEAD")
    trailer = "Workflow-Operation: " + context["operation_id"]
    status = git("status", "--porcelain=v1")
    if head != journal["base"]:
        if status or git("rev-parse", "HEAD^") != journal["base"] or trailer not in git("log", "-1", "--format=%B").splitlines():
            raise RunnerError("CODEX_CHECKPOINT_CONFLICT", "Owned Git branch changed outside its workflow checkpoint", status="unknown")
    elif status:
        git("add", "-A")
        git("-c", "user.name=ProBigA Workflow", "-c", "user.email=probiga-workflow@users.noreply.github.com",
            "commit", "-m", "Checkpoint managed workflow changes", "-m", trailer)
        head = git("rev-parse", "HEAD")
    elif operation["kind"] == "rework":
        return _result("failure", "CODEX_REWORK_NO_CHANGE", "Codex rework completed without a changed candidate", journal, evidence=evidence)
    evidence["candidate"] = head
    _progress(context, 4, "codex_candidate_verified", journal)
    checkpoint = {"session": {**session, "worktree": str(worktree), "thread_id": journal["thread_id"],
                              "turn_id": turn["id"], "candidate": head, "ownership_released": False}}
    return _result("success", "CODEX_CANDIDATE_READY", "Managed Codex candidate is ready for independent tests", journal,
                   evidence=evidence, checkpoint=checkpoint)


def _run(operation, context, *, recovery):
    journal = {}
    try:
        config = _settings(operation, context)
        worktree, session, git = _identity(context, config)
        directory = Path(context["evidence_dir"]).resolve()
        if directory.is_relative_to(worktree):
            raise RunnerError("CODEX_EVIDENCE_EXPOSED", "Codex operation evidence must be outside its editable worktree")
        directory.mkdir(parents=True, exist_ok=True)
        key = hashlib.sha256(context["operation_id"].encode()).hexdigest()
        path = directory / (key + ".codex.json")
        with _lock(directory / (key + ".codex.lock")):
            if path.exists():
                journal = json.loads(path.read_text(encoding="utf-8"))
            if "result" in journal:
                return journal["result"]
            if recovery and not journal.get("phase"):
                return _result("failure", "CODEX_NOT_SUBMITTED", "No Codex turn was submitted by this operation")
            with _connect(config, context) as client:
                if recovery or journal.get("phase") == "submitted":
                    turn = _read_turn(client, journal)
                else:
                    turn = _new_turn(client, operation, context, config, worktree, session, git, path, journal)
                result = _complete(operation, context, client, worktree, session, git, path, journal, turn)
            if result["status"] in {"success", "failure"}:
                journal["result"] = result
                _save(path, journal)
            return result
    except RunnerError as exc:
        status = "unknown" if journal.get("phase") == "submitted" and exc.status == "failure" else exc.status
        if not journal and exc.code == "CODEX_ENDPOINT_UNAVAILABLE":
            status = "waiting"
        return _result(status, exc.code, str(exc), journal)
    except (KeyError, TypeError, ValueError, OSError):
        return _result("unknown" if journal or recovery else "failure", "CODEX_PROTOCOL_OR_STORAGE_ERROR",
                       "Codex App Server protocol or durable checkpoint could not be verified", journal)


def execute(operation, context):
    return _run(operation, context, recovery=False)


def reconcile(operation, context):
    return _run(operation, context, recovery=True)
