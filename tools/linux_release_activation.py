#!/usr/bin/env python3
"""Durable Linux API/scheduler activation; never controls Windows or SQL.

The root deployment broker owns the outer deployment lock. This controller owns
an independent durable journal so SIGKILL/power loss cannot require the cross-end
rollback machinery. All installed paths and service names are fixed here.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import sys
import tempfile
import time
import urllib.request
from urllib.parse import unquote, urlsplit

SHA = re.compile(r"[0-9a-f]{40}\Z")
SCHEMA = "probiga.linux-activation.v1"
UNITS = ("probiga.service", "probiga-scheduler.service")
AI_UNITS = ("probiga-ai-recommendation-worker.service", "probiga-ai-recommendation-worker.timer")
FILES = (
    "/etc/systemd/system/probiga.service.d/scheduler.conf",
    "/etc/systemd/system/probiga-scheduler.service",
    "/etc/systemd/system/probiga-scheduler.service.d/release.conf",
    "/etc/systemd/system/probiga-ai-recommendation-worker.service.d/release-runtime.conf",
)
LINK = "/opt/ProBigA-current"
JOURNAL = "/var/lib/probiga/linux-activation/transaction.json"


class ActivationError(RuntimeError):
    pass


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def checked_sha(value):
    if not isinstance(value, str) or not SHA.fullmatch(value) or value == "0" * 40:
        raise ActivationError("invalid_build_identity")
    return value


class Filesystem:
    """Root-confined, durable storage. Alternate roots are test injection only."""
    def __init__(self, root=Path("/"), owner=0):
        self.root = Path(root)
        self.owner = owner

    def path(self, name):
        if not name.startswith("/") or any(p in {".", ".."} for p in name.split("/")[1:]):
            raise ActivationError("unsafe_path")
        return self.root / name.lstrip("/")

    def trusted(self, path, *, directory=False):
        info = path.lstat()
        kind = stat.S_ISDIR if directory else stat.S_ISREG
        if not kind(info.st_mode) or info.st_uid != self.owner or info.st_mode & 0o022:
            raise ActivationError("untrusted_storage")
        if not directory and info.st_nlink != 1:
            raise ActivationError("untrusted_storage")
        return info

    def parents(self, path):
        chain = []
        parent = path.parent
        while parent != self.root:
            if self.root not in parent.parents:
                raise ActivationError("unsafe_path")
            chain.append(parent)
            parent = parent.parent
        self.trusted(self.root, directory=True)
        for parent in reversed(chain):
            if not parent.exists():
                parent.mkdir(mode=0o755)
            self.trusted(parent, directory=True)

    def sync(self, directory):
        if os.name == "posix":
            fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

    def read(self, name):
        path = self.path(name)
        self.parents(path)
        info = self.trusted(path)
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_BINARY", 0))
        try:
            opened = os.fstat(fd)
            if (opened.st_ino, opened.st_dev, opened.st_nlink) != (info.st_ino, info.st_dev, 1):
                raise ActivationError("storage_identity_changed")
            with os.fdopen(fd, "rb", closefd=False) as stream:
                data = stream.read(4 * 1024 * 1024 + 1)
            if len(data) > 4 * 1024 * 1024:
                raise ActivationError("storage_limit_exceeded")
            return data
        finally:
            os.close(fd)

    def snapshot(self, name):
        path = self.path(name)
        self.parents(path)
        if not path.exists() and not path.is_symlink():
            return {"kind": "absent"}
        if name == LINK:
            info = path.lstat()
            if not stat.S_ISLNK(info.st_mode) or info.st_uid != self.owner:
                raise ActivationError("unsafe_current_link")
            target = os.readlink(path)
            if not re.fullmatch(r"/opt/ProBigA-releases/[0-9a-f]{40}", target):
                raise ActivationError("unsafe_current_link")
            return {"kind": "link", "target": target}
        info = self.trusted(path)
        return {"kind": "file", "mode": stat.S_IMODE(info.st_mode),
                "data": base64.b64encode(self.read(name)).decode()}

    def write(self, name, data, mode=0o600):
        path = self.path(name)
        self.parents(path)
        if path.exists() or path.is_symlink():
            self.trusted(path)
        fd, tmp = tempfile.mkstemp(prefix=".activation-", dir=path.parent)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            os.chmod(tmp, mode)
            os.replace(tmp, path)
            self.sync(path.parent)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)

    def restore(self, name, item):
        path = self.path(name)
        self.parents(path)
        if item["kind"] == "file":
            self.write(name, base64.b64decode(item["data"], validate=True), item["mode"])
        elif item["kind"] == "link" and name == LINK:
            if path.exists() or path.is_symlink():
                self.snapshot(name)
            temporary = path.with_name(".ProBigA-current-activation")
            if temporary.exists() or temporary.is_symlink():
                if not temporary.is_symlink():
                    raise ActivationError("unsafe_link_staging")
                temporary.unlink()
            os.symlink(item["target"], temporary)
            os.replace(temporary, path)
            self.sync(path.parent)
        elif item == {"kind": "absent"}:
            if path.exists() or path.is_symlink():
                self.snapshot(name)
                path.unlink()
                self.sync(path.parent)
        else:
            raise ActivationError("invalid_snapshot")

    def manifest(self, sha):
        checked_sha(sha)
        value = json.loads(self.read(f"/var/lib/probiga/release-artifacts/{sha}/component-release.json"))
        required = {"schema", "linux_build_sha", "windows_build_sha", "contract_build_sha",
                    "contract_sha256", "parent_linux_build_sha", "scope", "created_at", "manifest_sha256"}
        if not isinstance(value, dict) or set(value) != required:
            raise ActivationError("invalid_component_manifest")
        for key in ("linux_build_sha", "windows_build_sha", "contract_build_sha", "parent_linux_build_sha"):
            checked_sha(value[key])
        seal = value["manifest_sha256"]
        core = {key: item for key, item in value.items() if key != "manifest_sha256"}
        if (value["schema"] != "probiga.component-release.v1" or value["linux_build_sha"] != sha
                or value["windows_build_sha"] != value["contract_build_sha"]
                or not re.fullmatch(r"[0-9a-f]{64}", value["contract_sha256"])
                or value["scope"] not in {"LINUX", "COORDINATED"}
                or hashlib.sha256(encoded(core)).hexdigest() != seal):
            raise ActivationError("invalid_component_manifest")
        return value


class Services:
    def open_http(self, request, *, timeout):
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, request, fp, code, msg, headers, newurl):
                raise ActivationError("unexpected_probe_redirect")
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
        return opener.open(request, timeout=timeout)

    def runtime_environment(self, pid):
        return Path(f"/proc/{pid}/environ").read_bytes().split(b"\0")

    def health(self):
        with self.open_http("http://127.0.0.1/api/health", timeout=20) as response:
            return json.load(response)

    def reload_static(self):
        result = subprocess.run(["/usr/sbin/nginx", "-t"],
                                env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LANG": "C"},
                                stdin=subprocess.DEVNULL, capture_output=True, timeout=30)
        if result.returncode:
            raise ActivationError("nginx_configuration_failed")
        self.run("reload", "nginx.service")
        self.run("is-active", "--quiet", "nginx.service")

    def static_root(self, manifest):
        return Path("/opt/ProBigA-releases") / manifest["linux_build_sha"] / "server/static"

    def verify_static(self, manifest):
        root = self.static_root(manifest)
        html = (root / "index.html").read_text(encoding="utf-8")
        assets = {"/static/js/app.js", "/static/css/style.css"}
        assets.update(re.findall(r'''(?:src|href)\s*=\s*["'](/static/[^"']+)["']''', html))
        for asset in sorted(assets):
            path = unquote(urlsplit(asset).path)
            if "\\" in path or any(part in {"..", ".", ""} for part in path.split("/")[1:]):
                raise ActivationError("unsafe_static_asset")
            expected = (root / path.removeprefix("/static/")).read_bytes()
            request = urllib.request.Request("http://127.0.0.1" + asset,
                                             headers={"Cache-Control": "no-cache"})
            matched = False
            for attempt in range(15):
                try:
                    with self.open_http(request, timeout=15) as response:
                        matched = response.read(len(expected) + 1) == expected
                    if matched:
                        break
                except OSError:
                    pass
                time.sleep(1)
            if not matched:
                raise ActivationError("static_release_bytes_differ")

    def run(self, *args):
        try:
            result = subprocess.run(["/usr/bin/systemctl", *args],
                                    env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LANG": "C"},
                                    stdin=subprocess.DEVNULL, capture_output=True, timeout=180)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise ActivationError("systemd_failed") from exc
        if result.returncode:
            raise ActivationError("systemd_failed")
        return result.stdout.decode("utf-8").strip()

    def states(self):
        result = {}
        for unit in (*UNITS, *AI_UNITS):
            if unit in AI_UNITS and self.run("show", unit, "--property=LoadState", "--value") == "not-found":
                continue
            active = self.run("show", unit, "--property=ActiveState", "--value")
            enabled = self.run("show", unit, "--property=UnitFileState", "--value")
            allowed_enabled = {"enabled", "disabled", "static"} if unit == AI_UNITS[0] else {"enabled", "disabled"}
            allowed_active = {"active", "inactive", "activating"} if unit == AI_UNITS[0] else {"active", "inactive"}
            if active not in allowed_active or enabled not in allowed_enabled:
                raise ActivationError("unsupported_unit_state")
            result[unit] = {"active": active, "enabled": enabled}
        return result

    def stop(self, units):
        ordered = [unit for unit in (*UNITS, *AI_UNITS) if unit in units]
        self.run("stop", *reversed(ordered))
        for unit in units:
            if self.run("show", unit, "--property=ActiveState", "--value") != "inactive":
                raise ActivationError("unit_not_stopped")

    def start(self, states, *, original):
        self.run("daemon-reload")
        # JSON canonicalization sorts object keys; never let that reorder the
        # runtime dependency sequence when a fresh process restores a journal.
        for unit in (*UNITS, *AI_UNITS):
            if unit not in states:
                continue
            if states[unit]["enabled"] != "static":
                self.run("enable" if states[unit]["enabled"] == "enabled" else "disable", unit)
            if (not original and unit in UNITS) or states[unit]["active"] in {"active", "activating"}:
                self.run("start", unit)

    def verify(self, manifest, states, *, original=False):
        sha = manifest["linux_build_sha"]
        current = self.states()
        scheduler_pid = None
        if set(current) != set(states):
            raise ActivationError("unit_inventory_differs")
        for unit in states:
            expected_active = states[unit]["active"] if original or unit in AI_UNITS else "active"
            if unit == AI_UNITS[0]:
                # --once completion and a normal timer firing are both allowed.
                # Preserve scheduling/enablement policy, then prove any running
                # worker's actual identity instead of freezing an instant state.
                expected_active = current[unit]["active"]
                if expected_active == "inactive" and (
                        self.run("show", unit, "--property=Result", "--value") != "success"
                        or self.run("show", unit, "--property=ExecMainStatus", "--value") != "0"):
                    raise ActivationError("ai_worker_failed")
            if current[unit] != {"active": expected_active, "enabled": states[unit]["enabled"]}:
                raise ActivationError("unit_state_differs")
            if expected_active not in {"active", "activating"} or unit.endswith(".timer"):
                continue
            pid = self.run("show", unit, "--property=MainPID", "--value")
            if unit == AI_UNITS[0]:
                for _ in range(30):
                    if pid.isdigit() and int(pid) > 0:
                        break
                    if (self.run("show", unit, "--property=ActiveState", "--value") == "inactive"
                            and self.run("show", unit, "--property=Result", "--value") == "success"
                            and self.run("show", unit, "--property=ExecMainStatus", "--value") == "0"):
                        pid = "completed"
                        break
                    time.sleep(0.1)
                    pid = self.run("show", unit, "--property=MainPID", "--value")
                if pid == "completed":
                    continue
            if not pid.isdigit() or int(pid) < 1:
                raise ActivationError("runtime_pid_missing")
            env = self.runtime_environment(pid)
            expected = {
                "PROBIGA_EXPECTED_GIT_SHA": sha, "PROBIGA_BUILD_COMMIT_SHA": sha,
                "PROBIGA_CODE_ROOT": f"/opt/ProBigA-releases/{sha}",
                "PROBIGA_COMPONENT_RELEASE_PATH": f"/var/lib/probiga/release-artifacts/{sha}/component-release.json",
            }
            if unit in UNITS:
                expected["API_EMBEDDED_SCHEDULER_ENABLED"] = "false"
            if unit == UNITS[1]:
                expected["PROBIGA_SCHEDULER_EXECUTOR_ROLE"] = "linux_standalone"
                scheduler_pid = int(pid)
            if any(f"{key}={value}".encode() not in env for key, value in expected.items()):
                raise ActivationError("runtime_identity_differs")
        if original and states[UNITS[0]]["active"] != "active":
            self.verify_static(manifest)
            return
        for attempt in range(30):
            try:
                health = self.health()
                identity = health.get("component_release", {})
                if identity != {"ready": True, **manifest}:
                    raise ActivationError("health_component_identity_differs")
                heartbeat = health.get("standalone_scheduler_heartbeat", {})
                if (not original or states[UNITS[1]]["active"] == "active") and heartbeat.get("ready") is not True:
                    raise ActivationError("scheduler_heartbeat_not_ready")
                if scheduler_pid is not None and health.get("standalone_scheduler", {}).get("pid") != scheduler_pid:
                    raise ActivationError("scheduler_instance_differs")
                break
            except (OSError, ValueError, ActivationError):
                if attempt == 29:
                    raise ActivationError("runtime_health_failed") from None
                time.sleep(2)
        self.verify_static(manifest)


class Activation:
    def __init__(self, fs=None, services=None):
        self.fs = fs or Filesystem()
        self.services = services or Services()

    def save(self, value, phase):
        value["phase"] = phase
        value["journal_sha256"] = hashlib.sha256(encoded({key: item for key, item in value.items()
                                                        if key != "journal_sha256"})).hexdigest()
        self.fs.write(JOURNAL, encoded(value))

    def load(self):
        path = self.fs.path(JOURNAL)
        if not path.exists() and not path.is_symlink():
            return None
        value = json.loads(self.fs.read(JOURNAL))
        if value.get("schema") != SCHEMA:
            raise ActivationError("invalid_activation_journal")
        if set(value) != {"schema", "transaction_id", "target_sha", "previous_sha", "units",
                          "old_files", "new_files", "phase", "journal_sha256"}:
            raise ActivationError("invalid_activation_journal")
        core = {key: item for key, item in value.items() if key != "journal_sha256"}
        if (hashlib.sha256(encoded(core)).hexdigest() != value["journal_sha256"]
                or not re.fullmatch(r"[0-9a-f]{32}", value["transaction_id"])):
            raise ActivationError("invalid_activation_seal")
        for key in ("previous_sha", "target_sha"):
            checked_sha(value[key])
        expected_paths = {*FILES, LINK, self.receipt(value["target_sha"])}
        if set(value["old_files"]) != expected_paths or set(value["new_files"]) != expected_paths:
            raise ActivationError("invalid_activation_paths")
        if not set(UNITS) <= set(value["units"]) <= set((*UNITS, *AI_UNITS)):
            raise ActivationError("invalid_activation_units")
        for collection in (value["old_files"], value["new_files"]):
            for name, item in collection.items():
                if item == {"kind": "absent"}:
                    continue
                if (name == LINK and set(item) == {"kind", "target"} and item["kind"] == "link"
                        and re.fullmatch(r"/opt/ProBigA-releases/[0-9a-f]{40}", item["target"])):
                    continue
                if (name != LINK and set(item) == {"kind", "mode", "data"} and item["kind"] == "file"
                        and type(item["mode"]) is int and 0 <= item["mode"] <= 0o777):
                    base64.b64decode(item["data"], validate=True)
                    continue
                raise ActivationError("invalid_activation_snapshot")
        for unit, item in value["units"].items():
            allowed_enabled = ("enabled", "disabled", "static") if unit == AI_UNITS[0] else ("enabled", "disabled")
            allowed_active = ("active", "inactive", "activating") if unit == AI_UNITS[0] else ("active", "inactive")
            if item not in ({"active": active, "enabled": enabled}
                            for active in allowed_active for enabled in allowed_enabled):
                raise ActivationError("invalid_activation_units")
        if value["phase"] not in {"PREPARED", "ACTIVATING", "VERIFYING", "COMMITTED", "ROLLING_BACK", "ROLLED_BACK"}:
            raise ActivationError("invalid_activation_phase")
        return value

    @staticmethod
    def receipt(sha):
        return f"/var/lib/probiga/deploy-receipts/linux-{checked_sha(sha)}.json"

    def retire(self, value):
        archive = f"/var/lib/probiga/linux-activation/history/{value['target_sha']}-{value['transaction_id']}.json"
        self.fs.write(archive, encoded(value), 0o400)
        self.fs.restore(JOURNAL, {"kind": "absent"})

    def restore(self, value):
        self.save(value, "ROLLING_BACK")
        self.services.stop(value["units"])
        for name, item in value["old_files"].items():
            self.fs.restore(name, item)
        self.services.reload_static()
        self.services.start(value["units"], original=True)
        self.services.verify(self.fs.manifest(value["previous_sha"]), value["units"], original=True)
        for name, item in value["old_files"].items():
            if self.fs.snapshot(name) != item:
                raise ActivationError("rollback_file_differs")
        self.save(value, "ROLLED_BACK")
        self.retire(value)

    def recover(self):
        value = self.load()
        if value is None:
            return "NO_TRANSACTION"
        if value["phase"] == "COMMITTED":
            for name, item in value["new_files"].items():
                if self.fs.snapshot(name) != item:
                    raise ActivationError("committed_file_differs")
            self.retire(value)
            return "COMMITTED"
        self.restore(value)
        return "ROLLED_BACK"

    def activate(self, target_sha, previous_sha, sources):
        checked_sha(target_sha)
        checked_sha(previous_sha)
        self.recover()
        target = self.fs.manifest(target_sha)
        previous = self.fs.manifest(previous_sha)
        if target_sha == previous_sha:
            self.services.verify(target, self.services.states())
            return "ALREADY_ACTIVE"
        if (target["scope"] != "LINUX" or target["parent_linux_build_sha"] != previous_sha
                or any(target[key] != previous[key] for key in
                       ("windows_build_sha", "contract_build_sha", "contract_sha256"))):
            raise ActivationError("linux_contract_differs")
        old_files = {name: self.fs.snapshot(name) for name in (*FILES, LINK, self.receipt(target_sha))}
        if old_files[LINK] != {"kind": "link", "target": f"/opt/ProBigA-releases/{previous_sha}"}:
            raise ActivationError("previous_current_link_differs")
        new_files = {}
        for name, source in zip(FILES, sources, strict=True):
            if source is None and name == FILES[3]:
                new_files[name] = old_files[name]
                continue
            # These root-owned preparation files are produced by the existing
            # sealed release/dependency gate, never by a caller-supplied shell.
            source = Path(source)
            self.fs.parents(source)
            self.fs.trusted(source)
            data = source.read_bytes()
            if len(data) > 1024 * 1024 or not data:
                raise ActivationError("invalid_prepared_unit")
            if name in (FILES[0], FILES[1], FILES[3]):
                marker = f"PROBIGA_COMPONENT_RELEASE_PATH=/var/lib/probiga/release-artifacts/{target_sha}/component-release.json".encode()
                if marker not in data or f"PROBIGA_EXPECTED_GIT_SHA={target_sha}".encode() not in data:
                    raise ActivationError("prepared_unit_identity_differs")
            new_files[name] = {"kind": "file", "mode": 0o644, "data": base64.b64encode(data).decode()}
        new_files[LINK] = {"kind": "link", "target": f"/opt/ProBigA-releases/{target_sha}"}
        receipt = {"schema": SCHEMA, "status": "DEPLOYED", "previous_sha": previous_sha,
                   "component_release": target}
        new_files[self.receipt(target_sha)] = {"kind": "file", "mode": 0o600,
                                            "data": base64.b64encode(encoded(receipt)).decode()}
        units = self.services.states()
        if (AI_UNITS[0] in units) != (sources[3] is not None):
            raise ActivationError("prepared_ai_inventory_differs")
        self.services.verify(previous, units, original=True)
        value = {"schema": SCHEMA, "transaction_id": os.urandom(16).hex(),
                 "target_sha": target_sha, "previous_sha": previous_sha,
                 "units": units, "old_files": old_files, "new_files": new_files}
        self.save(value, "PREPARED")
        try:
            self.save(value, "ACTIVATING")
            self.services.stop(units)
            for name in (*FILES, LINK):
                self.fs.restore(name, new_files[name])
            self.services.reload_static()
            self.services.start(units, original=False)
            self.save(value, "VERIFYING")
            self.services.verify(target, units)
            self.fs.restore(self.receipt(target_sha), new_files[self.receipt(target_sha)])
            self.save(value, "COMMITTED")
        except Exception:
            # Ordinary failures converge now; uncatchable SIGKILL/power loss
            # leave the same evidence for recover() before the next deployment.
            self.restore(value)
            raise
        self.retire(value)
        return "DEPLOYED"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("recover", "activate"))
    parser.add_argument("--target-sha")
    parser.add_argument("--previous-sha")
    parser.add_argument("--main-source")
    parser.add_argument("--scheduler-source")
    parser.add_argument("--resources-source")
    parser.add_argument("--ai-source")
    args = parser.parse_args(argv)
    if not sys.platform.startswith("linux") or os.geteuid() != 0:
        parser.exit(2, "linux activation: root Linux execution required\n")
    def interrupted(signum, frame):
        raise ActivationError("activation_interrupted")
    for number in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
        signal.signal(number, interrupted)
    try:
        activation = Activation()
        if args.operation == "recover":
            status = activation.recover()
        else:
            status = activation.activate(args.target_sha, args.previous_sha,
                                         [args.main_source, args.scheduler_source, args.resources_source, args.ai_source])
        print(json.dumps({"schema": SCHEMA, "status": status}))
    except Exception:
        print(json.dumps({"schema": SCHEMA, "error": "linux_activation_failed"}), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
