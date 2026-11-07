"""Bounded adapters for the engineering workflow's trusted runtime boundaries."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import time
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, build_opener

ROOT = Path(__file__).resolve().parents[2]
SHA = re.compile(r"[0-9a-f]{40}\Z")
TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:/,-]{0,191}\Z")
NAME = re.compile(r"[A-Za-z][A-Za-z0-9_]{0,63}\Z")
MAX_OUTPUT = 256 * 1024
TRUSTED_MAIN_REMOTES = frozenset({
    "https://github.com/MingMG/probiga.git",
    "ssh://git@ssh.github.com:443/MingMG/probiga.git",
})
SYSTEM_ENV = frozenset({
    "PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "TMP", "TEMP",
    "LANG", "LC_ALL", "TZ", "USERPROFILE", "LOCALAPPDATA", "APPDATA",
})
MUTATING = frozenset({"deploy", "rollback", "acquire", "simulation", "research", "merge"})
STATUSES = frozenset({"success", "failure", "waiting", "unknown"})
SAFE_EVIDENCE = frozenset({
    "candidate", "endpoint", "environment", "build", "instance_id",
    "model_instance_id", "strategy_build", "source_sha256", "application_ok",
    "no_order_ok", "receipt", "receipt_verified", "coverage_verified",
    "readback_verified", "released", "active", "business_verified",
    "isolated", "real_orders", "merged", "external_id", "tests", "passed",
    "failed", "skipped", "total", "completed", "current_unit", "contract",
    "endpoints", "out_of_sample", "data_snapshot", "data_available_at",
    "strategy_version", "production", "scope", "reason_codes", "exit_code",
    "failure_signature", "log_sha256", "run_id", "run_uid", "rollback_candidate",
})


class AdapterError(ValueError):
    """Fixed, safe failure code for the workflow event stream."""


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False)


def _result(status, code, *, evidence=None, checkpoint=None, external_id=None):
    return {
        "status": status,
        "code": code,
        "summary": code.replace("_", " "),
        "evidence": evidence or {},
        "checkpoint": checkpoint or {},
        "external_id": external_id,
    }


def _mapping(value):
    return value if isinstance(value, dict) else {}


def _token(value):
    return isinstance(value, str) and TOKEN.fullmatch(value) is not None


def _sha(value):
    return isinstance(value, str) and SHA.fullmatch(value) is not None


def _safe_value(value):
    if type(value) is bool or (type(value) is int and abs(value) < 2**53):
        return value
    if _token(value):
        return value
    if isinstance(value, list) and len(value) <= 50 and all(_token(v) for v in value):
        return value
    return None


def _response(payload, request):
    if not isinstance(payload, dict) or payload.get("status") not in STATUSES:
        raise AdapterError("invalid_adapter_response")
    if payload.get("operation_id") != request["operation_id"]:
        raise AdapterError("adapter_operation_identity_mismatch")
    if payload.get("fence") != request["fence"]:
        raise AdapterError("adapter_fence_mismatch")
    code = payload.get("code")
    if not isinstance(code, str) or not NAME.fullmatch(code):
        raise AdapterError("invalid_adapter_result_code")
    evidence = {
        key: safe
        for key, value in _mapping(payload.get("evidence")).items()
        if key in SAFE_EVIDENCE and (safe := _safe_value(value)) is not None
    }
    checkpoint = {
        key: safe
        for key, value in _mapping(payload.get("checkpoint")).items()
        if key in {"external_id", "cursor", "completed", "current_unit", "total"}
        and (safe := _safe_value(value)) is not None
    }
    external_id = payload.get("external_id")
    if external_id is not None and not _token(external_id):
        raise AdapterError("invalid_external_run_identity")
    if external_id is not None:
        checkpoint["external_id"] = external_id
        evidence.setdefault("external_id", external_id)
    return _result(
        payload["status"], code, evidence=evidence, checkpoint=checkpoint,
        external_id=external_id,
    )


def _safe_test_failure(raw):
    # Pytest node IDs and exception types are useful for rework. Omit assertion
    # values, parameters, fixture logs, absolute paths and environment dumps.
    frames = []
    for line in raw.decode("utf-8", "replace").splitlines():
        match = re.match(
            r"^(?:FAILED |ERROR )?(tests/[A-Za-z0-9_/.-]+[.]py)"
            r"((?:::[A-Za-z0-9_]+){1,3}|:[0-9]+)", line,
        )
        if match:
            frames.append(match.group(1) + match.group(2))
        elif re.fullmatch(r"E\s+[A-Za-z0-9_.]*(?:Error|Exception):.*", line):
            frames.append(line.split(":", 1)[0].strip())
    return "\n".join(dict.fromkeys(frames))[-6000:]


def _remaining(context, operation):
    remaining = min(float(operation.get("timeout", 60)), float(context["deadline"]) - time.time())
    if remaining <= 0:
        raise AdapterError("stage_deadline_elapsed")
    return remaining


def _terminate(process):
    if process.poll() is not None:
        return
    if os.name == "posix":
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    else:
        process.kill()
    process.wait(timeout=2)


def _run(argv, *, cwd, timeout, request=None, env=None):
    """Bound execution, response size and retained output for a trusted command."""
    deadline = time.monotonic() + timeout
    with tempfile.TemporaryFile() as stdin, tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
        if request is not None:
            body = _json(request).encode()
            if len(body) > 64 * 1024:
                raise AdapterError("adapter_request_too_large")
            stdin.write(body)
            stdin.seek(0)
        try:
            process = subprocess.Popen(
                argv, cwd=cwd, env=env, stdin=stdin if request is not None else subprocess.DEVNULL,
                stdout=stdout, stderr=stderr, start_new_session=os.name == "posix",
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
            )
        except OSError as exc:
            raise AdapterError("adapter_command_unavailable") from exc
        try:
            while process.poll() is None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise AdapterError("adapter_command_timeout")
                if any(os.fstat(handle.fileno()).st_size > MAX_OUTPUT for handle in (stdout, stderr)):
                    raise AdapterError("adapter_response_too_large")
                try:
                    process.wait(timeout=min(remaining, 0.1))
                except subprocess.TimeoutExpired:
                    continue
        finally:
            _terminate(process)
        if any(os.fstat(handle.fileno()).st_size > MAX_OUTPUT for handle in (stdout, stderr)):
            raise AdapterError("adapter_response_too_large")
        stdout.seek(0)
        stderr.seek(0)
        return process.returncode, stdout.read(MAX_OUTPUT + 1), stderr.read(MAX_OUTPUT + 1)


def _parse(raw):
    try:
        value = json.loads(raw)
    except (UnicodeError, ValueError) as exc:
        raise AdapterError("invalid_adapter_response") from exc
    if not isinstance(value, dict):
        raise AdapterError("invalid_adapter_response")
    return value


def _sync_directory(path):
    if os.name == "posix":
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def _write_marker(path, payload):
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        return False
    with os.fdopen(fd, "w", encoding="utf-8") as file:
        file.write(_json(payload))
        file.flush()
        os.fsync(file.fileno())
    _sync_directory(path.parent)
    return True


def _save_receipt(path, payload):
    fd, temporary = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            file.write(_json(payload))
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, path)
        _sync_directory(path.parent)
    finally:
        Path(temporary).unlink(missing_ok=True)


def _read(path):
    try:
        with path.open("rb") as file:
            raw = file.read(MAX_OUTPUT + 1)
    except FileNotFoundError:
        return None
    if len(raw) > MAX_OUTPUT:
        raise AdapterError("invalid_adapter_journal")
    return _parse(raw)


class Registry:
    """Only the service operator supplies executable paths and permitted fields."""

    def __init__(self, config=None):
        self.config = _mapping(config)

    def _config(self, context):
        return self.config or _mapping(_mapping(context.get("settings")).get("adapters"))

    def _binding(self, operation, context):
        inputs = _mapping(operation.get("inputs"))
        name = operation.get("profile") or operation["kind"]
        if not _token(name):
            raise AdapterError("invalid_adapter_binding")
        bindings = _mapping(self._config(context).get("bindings"))
        if operation["kind"] == "probe" and name == "probe":
            name = f"probe-{operation.get('endpoint')}"
        binding = _mapping(bindings.get(name))
        if not binding:
            return None
        if (
            binding.get("kind") != operation["kind"]
            or binding.get("endpoint") != operation.get("endpoint")
            or binding.get("environment") != inputs.get("environment")
            or binding.get("resource") != operation.get("resource")
        ):
            raise AdapterError("adapter_binding_scope_mismatch")
        return binding

    def _request(self, operation, context, binding):
        fields = binding.get("input_fields", [])
        if not isinstance(fields, list) or not all(isinstance(key, str) and NAME.fullmatch(key) for key in fields):
            raise AdapterError("invalid_adapter_input_fields")
        inputs = _mapping(operation.get("inputs"))
        public_fields = {key: inputs[key] for key in fields if key in inputs}
        return {
            "operation_id": context["operation_id"],
            "task_id": context["task_id"],
            "fence": context["fence"],
            "kind": operation["kind"],
            "endpoint": operation.get("endpoint"),
            "resource": operation.get("resource"),
            "candidate": inputs.get("candidate"),
            "environment": inputs.get("environment"),
            "deadline": context["deadline"],
            "authorization": context.get("authorization"),
            "endpoint_evidence": _mapping(context.get("endpoint_evidence")),
            "inputs": public_fields,
            "checkpoint": _mapping(context.get("checkpoint")),
        }

    def _command(self, binding, stage, request):
        argv = binding.get(stage)
        if not isinstance(argv, list) or not argv or not all(isinstance(arg, str) for arg in argv):
            raise AdapterError("adapter_command_not_configured")
        if not Path(argv[0]).is_absolute() or "{" in argv[0] or "}" in argv[0]:
            raise AdapterError("adapter_executable_must_be_absolute")
        values = {**request["checkpoint"], **request["inputs"], **request}
        expanded = [argv[0]]
        for arg in argv[1:]:
            if arg.startswith("{") and arg.endswith("}"):
                value = values.get(arg[1:-1])
                if type(value) is int:
                    value = str(value)
                if not _token(value):
                    raise AdapterError("invalid_adapter_argument")
                expanded.append(value)
            elif "{" in arg or "}" in arg:
                raise AdapterError("invalid_adapter_command_template")
            else:
                expanded.append(arg)
        cwd = Path(binding.get("cwd") or ROOT)
        if not cwd.is_absolute() or not cwd.is_dir():
            raise AdapterError("invalid_adapter_working_directory")
        return expanded, cwd

    def _environment(self, binding):
        inherited = binding.get("env_allowlist", [])
        if not isinstance(inherited, list) or not all(isinstance(name, str) and NAME.fullmatch(name) for name in inherited):
            raise AdapterError("invalid_adapter_environment")
        allowed = SYSTEM_ENV | set(inherited)
        return {name: value for name, value in os.environ.items() if name.upper() in allowed}

    def _test_identity(self, cwd, candidate, timeout):
        deadline = time.monotonic() + timeout
        from tools.classify_release_scope import Git, ScopeError

        if not _sha(candidate):
            raise AdapterError("test_candidate_required")
        try:
            git = Git(repository=cwd)
        except ScopeError as exc:
            raise AdapterError("test_git_identity_unavailable") from exc
        exit_code, raw, _ = _run(
            git.command + ["status", "--porcelain=v1", "--branch", "--untracked-files=normal"],
            cwd=cwd, timeout=min(15, max(0.001, deadline - time.monotonic())), env=git.environment,
        )
        if exit_code or len(raw.splitlines()) != 1:
            raise AdapterError("test_worktree_has_tracked_changes")
        exit_code, raw, _ = _run(
            git.command + ["rev-parse", "--verify", "HEAD"],
            cwd=cwd, timeout=min(15, max(0.001, deadline - time.monotonic())), env=git.environment,
        )
        if exit_code or raw.strip() != candidate.encode("ascii"):
            raise AdapterError("test_worktree_candidate_mismatch")

    def _test_result(self, context, request, exit_code, stdout, stderr):
        # The worker alone can read this file. Console evidence contains its
        # digest and outcome; test output can include sensitive fixture values.
        path = Path(context["evidence_dir"]) / "test-output.json"
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        log = stdout + b"\n" + stderr
        _save_receipt(
            path,
            {"log": log.decode("utf-8", "replace")[-32_768:]},
        )
        result = _result(
            "success" if exit_code == 0 else "failure",
            "test_passed" if exit_code == 0 else "test_failed",
            evidence={
                "candidate": request["candidate"], "environment": request["environment"],
                "exit_code": exit_code, "log_sha256": hashlib.sha256(log).hexdigest(),
            },
            checkpoint={"log_file": path.name},
        )
        if exit_code:
            result["log_tail"] = _safe_test_failure(log)
            if result["log_tail"]:
                result["failure_key"] = hashlib.sha256(result["log_tail"].encode()).hexdigest()
        return result

    @staticmethod
    def _json_result(raw, exit_code, request):
        result = _response(_parse(raw), request)
        if exit_code and result["status"] == "success":
            raise AdapterError("adapter_exit_and_receipt_conflict")
        return result

    def _paths(self, context):
        key = str(context["operation_id"])
        if not _token(key):
            raise AdapterError("invalid_operation_identity")
        base = Path(context["evidence_dir"]) / "dispatch"
        base.mkdir(mode=0o700, parents=True, exist_ok=True)
        path = base / hashlib.sha256(key.encode()).hexdigest()
        return path.with_suffix(".dispatch.json"), path.with_suffix(".receipt.json")

    def _identity(self, operation, request):
        fields = {
            key: request[key] for key in ("operation_id", "task_id", "kind", "endpoint", "resource")
        }
        fields["operation"] = operation
        return hashlib.sha256(_json(fields).encode()).hexdigest()

    def execute(self, operation, context):
        try:
            kind = operation["kind"]
            if kind == "classify":
                return self._classify(operation, context)
            binding = self._binding(operation, context)
            if kind == "probe" and binding is None:
                return self._probe(operation, context)
            if binding is None:
                return _result("waiting", "trusted_adapter_binding_required")
            return self._execute_binding(operation, context, binding)
        except (OSError, subprocess.SubprocessError):
            status = "waiting" if operation.get("kind") == "probe" else "failure"
            return _result(status, "adapter_runtime_unavailable")
        except AdapterError as exc:
            status = "waiting" if operation.get("kind") == "probe" else "failure"
            return _result(status, str(exc))

    def _trusted_main(self, operation, context, candidate):
        from tools.classify_release_scope import ScopeError, _git_environment, _git_executable

        remote = self._config(context).get("trusted_main_remote")
        if not isinstance(remote, str) or not remote:
            return _result("waiting", "trusted_main_source_required")
        is_local_mirror = Path(remote).is_absolute()
        if not is_local_mirror and remote not in TRUSTED_MAIN_REMOTES:
            raise AdapterError("invalid_trusted_main_source")
        if is_local_mirror and not Path(remote).is_dir():
            return _result("waiting", "trusted_main_source_unavailable")
        try:
            git = _git_executable()
        except ScopeError as exc:
            raise AdapterError("trusted_git_unavailable") from exc
        env = _git_environment()
        env.update({name: os.environ[name] for name in ("SSH_AUTH_SOCK",) if name in os.environ})
        protocol = "file" if is_local_mirror else remote.split(":", 1)[0]
        try:
            exit_code, raw, _ = _run(
                [
                    git, "--no-replace-objects", "-c", "protocol.allow=never",
                    "-c", f"protocol.{protocol}.allow=always",
                    "-c", "core.sshCommand=ssh -o BatchMode=yes -o ConnectTimeout=8",
                    "ls-remote", "--exit-code", remote, "refs/heads/main",
                ],
                cwd=ROOT, env=env, timeout=min(15, _remaining(context, operation)),
            )
        except AdapterError:
            return _result("waiting", "trusted_main_source_unavailable")
        if exit_code:
            return _result("waiting", "trusted_main_source_unavailable")
        if raw.splitlines() != [candidate.encode("ascii") + b"\trefs/heads/main"]:
            return _result("failure", "production_candidate_is_not_trusted_main")
        return None

    def _execute_binding(self, operation, context, binding):
        request = self._request(operation, context, binding)
        kind = operation["kind"]
        mode = binding.get("protocol", "json")
        if (
            mode not in {"json", "submit", "exit", "observe"}
            or (mode == "exit" and kind != "test")
            or (mode == "submit" and kind not in MUTATING)
            or (mode == "observe" and kind not in {"deploy", "rollback"})
        ):
            raise AdapterError("invalid_adapter_protocol")
        if kind == "simulation" and (
            binding.get("isolated") is not True or request["environment"] in {None, "production"}
        ):
            return _result("waiting", "isolated_simulation_binding_required")
        if kind in {"deploy", "rollback"} and not _sha(request["candidate"]):
            raise AdapterError("release_candidate_required")
        if kind in MUTATING and mode != "observe":
            marker, receipt = self._paths(context)
            if marker.exists():
                return self.reconcile(operation, context)
        if (
            request["environment"] == "production"
            and kind in MUTATING
            and not _token(context.get("authorization"))
        ):
            return _result("waiting", "production_operation_authorization_required")
        if kind in MUTATING and not binding.get("reconcile_argv"):
            return _result("waiting", "read_only_reconciliation_binding_required")
        if kind == "deploy" and request["environment"] == "production" and mode != "observe":
            blocked = self._trusted_main(operation, context, request["candidate"])
            if blocked:
                return blocked
        if mode == "observe":
            return self.reconcile(operation, context)
        if kind in {"deploy", "rollback"} and mode == "submit" and "{candidate}" not in binding.get("argv", []):
            raise AdapterError("release_command_must_bind_candidate")
        argv, cwd = self._command(binding, "argv", request)
        if kind == "test" and binding.get("managed_worktree") is True:
            cwd = Path(_mapping(context.get("session")).get("worktree") or ".")
            if not cwd.is_absolute() or not cwd.is_dir():
                raise AdapterError("managed_test_worktree_required")
        timeout = _remaining(context, operation)
        env = self._environment(binding)
        if kind == "test":
            self._test_identity(cwd, request["candidate"], timeout)
            timeout = _remaining(context, operation)
        if kind not in MUTATING:
            exit_code, raw, diagnostics = _run(
                argv, cwd=cwd, timeout=timeout, request=request if mode == "json" else None, env=env,
            )
            if mode == "exit":
                self._test_identity(cwd, request["candidate"], _remaining(context, operation))
                return self._test_result(context, request, exit_code, raw, diagnostics)
            result = self._json_result(raw, exit_code, request)
            if kind == "test":
                self._test_identity(cwd, request["candidate"], _remaining(context, operation))
                if result["status"] == "failure":
                    result["log_tail"] = _safe_test_failure(diagnostics)
                    if result["log_tail"]:
                        result["failure_key"] = hashlib.sha256(result["log_tail"].encode()).hexdigest()
            if kind == "probe" and result["status"] == "success":
                return self._verified_probe(result, operation)
            return result

        if pre_dispatch := context.get("pre_dispatch"):
            if reason := pre_dispatch(operation):
                return _result("waiting", reason)
            request["authorization"] = context.get("authorization")
        timeout = _remaining(context, operation)
        identity = self._identity(operation, request)
        if not _write_marker(marker, {"identity": identity, "fence": request["fence"], "submitted_at": time.time()}):
            return self.reconcile(operation, context)
        try:
            exit_code, raw, _ = _run(
                argv, cwd=cwd, timeout=timeout, request=request if mode == "json" else None, env=env,
            )
            if mode == "submit":
                return self.reconcile(operation, context)
            result = self._after_dispatch(self._json_result(raw, exit_code, request))
            _save_receipt(receipt, {"identity": identity, "result": result})
            self._report_progress(result, context)
            return result
        except (AdapterError, OSError, subprocess.SubprocessError):
            return _result("unknown", "submission_requires_reconciliation")

    def reconcile(self, operation, context):
        try:
            binding = self._binding(operation, context)
            if operation["kind"] not in MUTATING and (binding is None or not binding.get("reconcile_argv")):
                return self.execute(operation, context)
            if binding is None:
                return _result("unknown", "reconciliation_binding_required")
            request = self._request(operation, context, binding)
            marker, receipt = self._paths(context)
            identity = self._identity(operation, request)
            saved = _read(receipt)
            if saved is not None:
                if saved.get("identity") != identity:
                    raise AdapterError("adapter_journal_identity_mismatch")
                last = _mapping(saved.get("result"))
                if last.get("status") in {"success", "failure"}:
                    return last
                request["checkpoint"] = {**_mapping(last.get("checkpoint")), **request["checkpoint"]}
            recorded = _read(marker)
            if recorded is not None and recorded.get("identity") != identity:
                raise AdapterError("adapter_journal_identity_mismatch")
            if not binding.get("reconcile_argv"):
                return _result("unknown", "reconciliation_binding_required")
            argv, cwd = self._command(binding, "reconcile_argv", request)
            exit_code, raw, _ = _run(
                argv, cwd=cwd, timeout=_remaining(context, operation),
                request=request, env=self._environment(binding),
            )
            result = self._json_result(raw, exit_code, request)
            if recorded is not None:
                result = self._after_dispatch(result)
                _save_receipt(receipt, {"identity": identity, "result": result})
            self._report_progress(result, context)
            return result
        except AdapterError as exc:
            return _result("unknown", str(exc))
        except (OSError, subprocess.SubprocessError):
            return _result("unknown", "reconciliation_unavailable")

    @staticmethod
    def _report_progress(result, context):
        callback = context.get("progress")
        if callback is None:
            return
        checkpoint = result["checkpoint"]
        completed, total, unit = (
            checkpoint.get("completed"), checkpoint.get("total"), checkpoint.get("current_unit"),
        )
        if (
            type(completed) is int and type(total) is int and 0 <= completed <= total
            and isinstance(unit, str) and re.fullmatch(r"[A-Za-z0-9_.:-]{1,96}", unit)
        ):
            callback(completed=completed, total=total, unit=unit, checkpoint=checkpoint)

    @staticmethod
    def _after_dispatch(result):
        if result["status"] == "waiting":
            return {**result, "status": "unknown"}
        return result

    def _classify(self, operation, context):
        inputs = _mapping(operation.get("inputs"))
        base, candidate = inputs.get("base"), inputs.get("candidate")
        windows_base = inputs.get("windows_base")
        if not (_sha(base) and _sha(candidate)) or (windows_base is not None and not _sha(windows_base)):
            raise AdapterError("release_baseline_and_candidate_required")
        repository = self._config(context).get("repository")
        if not repository:
            return _result("waiting", "trusted_release_repository_required")
        argv = [
            sys.executable, str(ROOT / "tools" / "classify_release_scope.py"),
            "--repository", str(repository), "--base-sha", base, "--target-sha", candidate,
        ]
        if windows_base:
            argv += ["--windows-base-sha", windows_base]
        exit_code, raw, _ = _run(argv, cwd=ROOT, timeout=_remaining(context, operation))
        proof = _parse(raw)
        if exit_code or proof.get("target_sha") != candidate:
            raise AdapterError("trusted_release_classification_failed")
        scope = proof.get("scope")
        if scope not in {"LINUX", "COORDINATED"}:
            raise AdapterError("invalid_release_scope")
        endpoints = ["linux"] if scope == "LINUX" else ["linux", "windows"]
        # Windows-only activation needs both proof of ownership and an existing
        # Windows-only production entrypoint. The broker's COORDINATED label
        # alone cannot authorize Windows-only or widen a single-end release.
        if (
            scope == "COORDINATED"
            and proof.get("base_contract_sha256") == proof.get("contract_sha256")
            and proof.get("windows_runtime_changed_paths")
        ):
            windows_owned = set(self._config(context).get("windows_only_runtime_paths", []))
            changed = set(proof.get("changed_paths", []))
            if changed and changed <= windows_owned and changed <= set(proof["windows_runtime_changed_paths"]):
                endpoints = ["windows"]
                scope = "WINDOWS"
            else:
                return _result(
                    "waiting", "windows_runtime_scope_requires_verified_consumer_inventory",
                    evidence={
                        "candidate": candidate, "contract": proof.get("contract_sha256"),
                        "reason_codes": proof.get("reason_codes"), "scope": scope,
                    },
                )
        return _result(
            "success", "release_scope_classified",
            evidence={
                "candidate": candidate, "endpoints": endpoints, "scope": scope,
                "contract": proof.get("contract_sha256"), "reason_codes": proof.get("reason_codes"),
                "changed_paths": proof.get("changed_paths"),
            },
        )

    def _verified_probe(self, result, operation):
        proof = result["evidence"]
        endpoint = operation.get("endpoint")
        expected_instance = _mapping(operation.get("inputs")).get("expected_instance_id")
        if (
            proof.get("endpoint") != endpoint
            or proof.get("application_ok") is not True
            or not _sha(proof.get("build"))
            or (expected_instance and proof.get("instance_id") != expected_instance)
            or (endpoint == "windows" and (
                not _token(proof.get("instance_id"))
                or not _token(proof.get("model_instance_id"))
                or proof.get("no_order_ok") is not True
            ))
        ):
            return _result("waiting", "endpoint_runtime_identity_unverified")
        return result

    def _probe(self, operation, context):
        endpoint = operation.get("endpoint")
        config = self._config(context)
        if endpoint == "linux":
            url = config.get("linux_health_url")
            if not url:
                return _result("waiting", "linux_health_binding_required")
            argv = ["linux-probe", url]
        elif endpoint == "windows":
            build = config.get("windows_build")
            if os.name != "nt" or not _sha(build):
                return _result("waiting", "windows_application_probe_binding_required")
            argv = ["windows-probe", build]
        else:
            raise AdapterError("invalid_probe_endpoint")
        exit_code, raw, _ = _run(
            [sys.executable, "-m", __name__, *argv], cwd=ROOT,
            timeout=min(30, _remaining(context, operation)),
        )
        if exit_code != 0:
            return _result("waiting", f"{endpoint}_application_probe_unavailable")
        proof = _parse(raw)
        if (
            endpoint == "linux"
            and _mapping(operation.get("inputs")).get("environment") == "production"
            and proof.get("production") is not True
        ):
            return _result("waiting", "linux_production_identity_unverified")
        return self._verified_probe(
            _result("success", "endpoint_application_verified", evidence=proof), operation,
        )


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _linux_probe(url):
    parts = urlsplit(url)
    if (
        parts.path != "/api/health" or parts.username or parts.password
        or parts.query or parts.fragment or not parts.hostname
        or (parts.scheme != "https" and not (
            parts.scheme == "http" and parts.hostname in {"127.0.0.1", "::1", "localhost"}
        ))
    ):
        raise AdapterError("invalid_linux_health_url")
    with build_opener(ProxyHandler({}), _NoRedirect()).open(url, timeout=8) as response:
        payload = _parse(response.read(MAX_OUTPUT + 1))
    release = _mapping(payload.get("release_revision"))
    component = _mapping(payload.get("component_release"))
    heartbeat = _mapping(payload.get("standalone_scheduler_heartbeat"))
    observed = release.get("actual_git_sha")
    production = release.get("deployment_mode") == "production"
    if (
        payload.get("status") != "ok"
        or not _sha(observed)
        or (production and (
            release.get("matches_expected") is not True
            or component.get("ready") is not True
            or component.get("linux_build_sha") != observed
            or heartbeat.get("ready") is not True
        ))
    ):
        raise AdapterError("linux_runtime_identity_unverified")
    return {
        "endpoint": "linux", "build": observed, "application_ok": True,
        "production": production,
    }


def _windows_probe(build):
    if os.name != "nt" or not _sha(build):
        raise AdapterError("invalid_windows_probe")
    from integrations.bigqmt import bridge
    from integrations.bigqmt.health import evaluate_spool_health
    from integrations.bigqmt.spool import resolve_big_qmt_home
    from server.common.scheduler_runtime_health import check_qmt_windows_edge_identity
    from tools.env_config import create_tool_engine, load_project_env
    from tools.run_qmt_windows_edge_release_bootstrap import validate_bigqmt_strategy_release

    load_project_env()
    engine = create_tool_engine()
    try:
        with engine.connect() as connection:
            ready, detail = check_qmt_windows_edge_identity(
                connection, expected_build_sha=build,
            )
    finally:
        engine.dispose()
    current = _mapping(detail.get("current"))
    if not ready or current.get("build_sha") != build or not _token(current.get("instance_id")):
        raise AdapterError("windows_scheduler_identity_unverified")
    home = resolve_big_qmt_home(required=True)
    health = evaluate_spool_health(home)
    if not health.get("healthy") or not _token(health.get("model_instance_id")):
        raise AdapterError("qmt_application_unavailable")
    capabilities = bridge.capabilities(timeout=10)
    proof = validate_bigqmt_strategy_release(capabilities, expected_build_sha=build)
    if (
        capabilities.get("model_instance_id") != health["model_instance_id"]
        or proof.get("read_only") is not True
        or proof.get("automatic_real_order_submission") is not False
        or proof.get("real_order_authority") is not False
    ):
        raise AdapterError("qmt_no_order_capability_unverified")
    return {
        "endpoint": "windows", "build": build, "instance_id": current["instance_id"],
        "model_instance_id": health["model_instance_id"], "application_ok": True,
        "no_order_ok": True, "strategy_build": proof["strategy_build_sha"],
        "source_sha256": proof["strategy_source_sha256"],
    }


def execute(operation, context):
    return Registry().execute(operation, context)


def reconcile(operation, context):
    return Registry().reconcile(operation, context)


if __name__ == "__main__":
    try:
        mode, value = sys.argv[1:]
        handlers = {"linux-probe": _linux_probe, "windows-probe": _windows_probe}
        print(_json(handlers[mode](value)))
    except Exception:
        raise SystemExit(2)
