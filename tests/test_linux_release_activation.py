"""Durable activation fault tests use real journal/files and injected services."""
import importlib.util
import json
import os
from pathlib import Path
import stat
from io import BytesIO

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("linux_activation", ROOT / "tools/linux_release_activation.py")
activation = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(activation)
OLD, NEW, WINDOWS = "1" * 40, "2" * 40, "3" * 40


class Crash(BaseException):
    """A killed process bypasses Exception rollback, like SIGKILL/power loss."""


class TestFilesystem(activation.Filesystem):
    __test__ = False

    def write(self, name, data, mode=0o600):
        super().write(name, data, mode)
        if os.name == "nt":
            if not hasattr(self, "_posix_modes"):
                self._posix_modes = {}
            self._posix_modes[name] = mode

    def trusted(self, path, *, directory=False):
        if os.name != "nt":
            return super().trusted(path, directory=directory)
        # NTFS does not implement Unix owner/mode bits. Retain shape/link checks
        # for portability; production always executes the unchanged POSIX guard.
        info = path.lstat()
        assert not path.is_symlink()
        assert stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
        return info

    def snapshot(self, name):
        if os.name == "nt" and name == activation.LINK:
            path = self.path(name + ".test-link")
            if path.exists():
                return {"kind": "link", "target": path.read_text()}
            return {"kind": "absent"}
        result = super().snapshot(name)
        if os.name == "nt" and result["kind"] == "file":
            result["mode"] = self._posix_modes[name]
        return result

    def restore(self, name, item):
        if os.name == "nt" and name == activation.LINK:
            path = self.path(name + ".test-link")
            self.parents(path)
            if item["kind"] == "absent":
                path.unlink(missing_ok=True)
            else:
                path.write_text(item["target"])
            return
        return super().restore(name, item)


class FakeServices:
    def __init__(self):
        self.units = {unit: {"active": "active", "enabled": "enabled"} for unit in activation.UNITS}
        self.units[activation.AI_UNITS[0]] = {"active": "inactive", "enabled": "static"}
        self.units[activation.AI_UNITS[1]] = {"active": "active", "enabled": "enabled"}
        self.calls = []
        self.failure = None
        self.crash = None

    def action(self, name):
        self.calls.append(name)
        if self.failure == name:
            self.failure = None
            raise activation.ActivationError("injected_failure")
        if self.crash == name:
            self.crash = None
            raise Crash()

    def states(self):
        return json.loads(json.dumps(self.units))

    def stop(self, units):
        self.action("stop")
        for state in self.units.values():
            state["active"] = "inactive"

    def reload_static(self):
        self.action("reload_static")

    def start(self, states, *, original):
        self.action("start_old" if original else "start_new")
        self.units = json.loads(json.dumps(states))
        if not original:
            for unit in activation.UNITS:
                self.units[unit]["active"] = "active"

    def verify(self, manifest, states, *, original=False):
        self.action("verify_old" if original else "verify_new")
        self.calls.append("verified:" + manifest["linux_build_sha"])


def manifest(sha, parent, scope="LINUX", windows=WINDOWS):
    core = {"schema": "probiga.component-release.v1", "linux_build_sha": sha,
            "windows_build_sha": windows, "contract_build_sha": windows,
            "contract_sha256": "a" * 64, "parent_linux_build_sha": parent,
            "scope": scope, "created_at": "2026-09-19T12:00:00Z"}
    return {**core, "manifest_sha256": activation.hashlib.sha256(activation.encoded(core)).hexdigest()}


@pytest.fixture
def fixture(tmp_path):
    root = tmp_path / "host"
    root.mkdir(mode=0o700)
    fs = TestFilesystem(root, owner=os.getuid() if hasattr(os, "getuid") else 0)
    for sha, value in ((OLD, manifest(OLD, WINDOWS)), (NEW, manifest(NEW, OLD))):
        fs.write(f"/var/lib/probiga/release-artifacts/{sha}/component-release.json", activation.encoded(value), 0o644)
    for index, name in enumerate(activation.FILES):
        fs.write(name, f"old-unit-{index}".encode(), 0o644)
    fs.restore(activation.LINK, {"kind": "link", "target": f"/opt/ProBigA-releases/{OLD}"})
    sources = []
    for index in range(4):
        name = f"/var/lib/probiga/prepared/{index}"
        fs.write(name, (f"[Service]\nPROBIGA_EXPECTED_GIT_SHA={NEW}\n"
                        f"PROBIGA_COMPONENT_RELEASE_PATH=/var/lib/probiga/release-artifacts/{NEW}/component-release.json\n").encode())
        sources.append(fs.path(name))
    services = FakeServices()
    return fs, services, sources


def capture(fs, sha=NEW):
    return {name: fs.snapshot(name) for name in (*activation.FILES, activation.LINK, activation.Activation.receipt(sha))}


def test_success_preserves_windows_contract_and_ai_states(fixture):
    fs, services, sources = fixture
    ctl = activation.Activation(fs, services)
    assert ctl.activate(NEW, OLD, sources) == "DEPLOYED"
    assert fs.snapshot(activation.LINK)["target"].endswith(NEW)
    receipt = json.loads(fs.read(ctl.receipt(NEW)))
    assert receipt["component_release"]["windows_build_sha"] == WINDOWS
    assert services.units[activation.AI_UNITS[0]] == {"active": "inactive", "enabled": "static"}
    assert services.units[activation.AI_UNITS[1]] == {"active": "active", "enabled": "enabled"}
    assert services.calls.count("stop") == 1
    assert services.calls.count("verify_new") == 1
    assert ctl.recover() == "NO_TRANSACTION"


@pytest.mark.parametrize("step", ["stop", "reload_static", "start_new", "verify_new"])
def test_service_failures_restore_exact_files_units_link_and_receipt(fixture, step):
    fs, services, sources = fixture
    before = capture(fs)
    states = services.states()
    services.failure = step
    ctl = activation.Activation(fs, services)
    with pytest.raises(activation.ActivationError):
        ctl.activate(NEW, OLD, sources)
    assert capture(fs) == before
    assert services.states() == states
    assert services.calls[-2:] == ["verify_old", "verified:" + OLD]
    assert ctl.recover() == "NO_TRANSACTION"


@pytest.mark.parametrize("step", ["stop", "reload_static", "start_new", "verify_new"])
def test_crash_reentry_uses_durable_snapshot_and_fresh_old_verification(fixture, step):
    fs, services, sources = fixture
    before = capture(fs)
    services.crash = step
    with pytest.raises(Crash):
        activation.Activation(fs, services).activate(NEW, OLD, sources)
    # A new controller has no in-memory state from the process that was killed.
    assert activation.Activation(fs, services).recover() == "ROLLED_BACK"
    assert capture(fs) == before
    assert services.calls[-2:] == ["verify_old", "verified:" + OLD]


def test_partial_file_install_crash_is_recovered(fixture, monkeypatch):
    fs, services, sources = fixture
    before = capture(fs)
    restore = fs.restore
    def fail(name, value):
        restore(name, value)
        if name == activation.FILES[0]:
            raise Crash()
    monkeypatch.setattr(fs, "restore", fail)
    with pytest.raises(Crash):
        activation.Activation(fs, services).activate(NEW, OLD, sources)
    monkeypatch.setattr(fs, "restore", restore)
    assert activation.Activation(fs, services).recover() == "ROLLED_BACK"
    assert capture(fs) == before


def test_crash_after_receipt_before_commit_removes_unverified_receipt(fixture, monkeypatch):
    fs, services, sources = fixture
    before = capture(fs)
    ctl = activation.Activation(fs, services)
    save = ctl.save
    def crash(value, phase):
        if phase == "COMMITTED":
            raise Crash()
        save(value, phase)
    monkeypatch.setattr(ctl, "save", crash)
    with pytest.raises(Crash):
        ctl.activate(NEW, OLD, sources)
    assert fs.path(ctl.receipt(NEW)).exists()
    assert activation.Activation(fs, services).recover() == "ROLLED_BACK"
    assert capture(fs) == before


def test_crash_after_commit_preserves_new_release_and_same_sha_retry(fixture, monkeypatch):
    fs, services, sources = fixture
    ctl = activation.Activation(fs, services)
    monkeypatch.setattr(ctl, "retire", lambda value: (_ for _ in ()).throw(Crash()))
    with pytest.raises(Crash):
        ctl.activate(NEW, OLD, sources)
    fresh = activation.Activation(fs, services)
    assert fresh.recover() == "COMMITTED"
    calls_before = list(services.calls)
    assert fresh.activate(NEW, NEW, sources) == "ALREADY_ACTIVE"
    assert services.calls[len(calls_before):] == ["verify_new", "verified:" + NEW]


def test_rollback_failure_leaves_retryable_journal(fixture):
    fs, services, sources = fixture
    services.crash = "verify_new"
    with pytest.raises(Crash):
        activation.Activation(fs, services).activate(NEW, OLD, sources)
    services.failure = "verify_old"
    with pytest.raises(activation.ActivationError):
        activation.Activation(fs, services).recover()
    assert json.loads(fs.read(activation.JOURNAL))["phase"] == "ROLLING_BACK"
    assert activation.Activation(fs, services).recover() == "ROLLED_BACK"


def test_tampered_journal_and_component_drift_fail_before_services(fixture):
    fs, services, sources = fixture
    value = manifest(NEW, OLD, windows="4" * 40)
    fs.write(f"/var/lib/probiga/release-artifacts/{NEW}/component-release.json", activation.encoded(value))
    with pytest.raises(activation.ActivationError, match="linux_contract_differs"):
        activation.Activation(fs, services).activate(NEW, OLD, sources)
    assert services.calls == []
    fs.write(activation.JOURNAL, activation.encoded({"schema": activation.SCHEMA}))
    with pytest.raises(activation.ActivationError):
        activation.Activation(fs, services).recover()
    assert services.calls == []


def test_missing_prepared_ai_unit_cannot_leave_future_worker_on_old_code(fixture):
    fs, services, sources = fixture
    sources[3] = None
    with pytest.raises(activation.ActivationError, match="prepared_ai_inventory_differs"):
        activation.Activation(fs, services).activate(NEW, OLD, sources)
    assert "stop" not in services.calls


def test_linux_dispatch_exits_before_coordinated_mutations():
    script = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    start = script.index("CUTOVER_STEP=prepare_release\nprepare_release")
    dispatch = script[start:script.index("# PREPARE DATABASE:", start)]
    assert 'if [ "$RELEASE_SCOPE" = LINUX ]' in dispatch
    assert "activate_linux_release\n" in dispatch and "exit 0" in dispatch
    assert "--activation-grant" not in dispatch
    controller = (ROOT / "tools/linux_release_activation.py").read_text(encoding="utf-8")
    assert "prepare_strategy_governance_schema" not in controller
    assert "bootstrap_qmt_windows_edge" not in controller
    assert 'PROBIGA_PREVIOUS_GIT_SHA="$PREVIOUS_CONTRACT_SHA"' in script
    assert '--request-recoverable-quiescence "$QMT_EDGE_DEPLOYMENT_ATTEMPT_ID" "$PREVIOUS_WINDOWS_SHA"' in script


class ObservedServices(activation.Services):
    def __init__(self):
        self.snapshot = FakeServices().states()
        self.snapshot[activation.AI_UNITS[0]]["active"] = "active"
        self.completed = False
        self.triggered = False
        self.heartbeat_pid = 102
        self.actual = manifest(NEW, OLD)
        self.static_verified = False

    def states(self):
        result = json.loads(json.dumps(self.snapshot))
        if self.completed:
            result[activation.AI_UNITS[0]]["active"] = "inactive"
        if self.triggered:
            result[activation.AI_UNITS[0]]["active"] = "activating"
        return result

    def run(self, *args):
        if "--property=MainPID" in args:
            return str(101 + (*activation.UNITS, *activation.AI_UNITS).index(args[1]))
        if "--property=Result" in args:
            return "success"
        if "--property=ExecMainStatus" in args:
            return "0"
        raise AssertionError(args)

    def runtime_environment(self, pid):
        values = {"PROBIGA_EXPECTED_GIT_SHA": NEW, "PROBIGA_BUILD_COMMIT_SHA": NEW,
                  "PROBIGA_CODE_ROOT": f"/opt/ProBigA-releases/{NEW}",
                  "PROBIGA_COMPONENT_RELEASE_PATH": f"/var/lib/probiga/release-artifacts/{NEW}/component-release.json"}
        if pid in {"101", "102"}:
            values["API_EMBEDDED_SCHEDULER_ENABLED"] = "false"
        if pid == "102":
            values["PROBIGA_SCHEDULER_EXECUTOR_ROLE"] = "linux_standalone"
        return [f"{key}={value}".encode() for key, value in values.items()]

    def health(self):
        return {"component_release": {"ready": True, **self.actual},
                "standalone_scheduler_heartbeat": {"ready": True},
                "standalone_scheduler": {"pid": self.heartbeat_pid}}

    def verify_static(self, manifest):
        self.static_verified = True


@pytest.mark.parametrize("completed", [False, True])
def test_active_ai_once_worker_or_successful_completion_verifies(completed):
    service = ObservedServices()
    service.completed = completed
    service.verify(service.actual, service.snapshot)
    assert service.static_verified


def test_timer_may_start_previously_inactive_ai_worker_during_verification():
    service = ObservedServices()
    service.snapshot[activation.AI_UNITS[0]]["active"] = "inactive"
    service.triggered = True
    service.verify(service.actual, service.snapshot)
    assert service.static_verified


def test_recovery_starts_api_and_scheduler_before_ai_after_json_key_sorting(monkeypatch):
    service = activation.Services()
    states = FakeServices().states()
    states[activation.AI_UNITS[0]]["active"] = "active"
    states = json.loads(json.dumps(states, sort_keys=True))
    calls = []
    monkeypatch.setattr(service, "run", lambda *args: calls.append(args))
    service.start(states, original=True)
    starts = [args[1] for args in calls if args[0] == "start"]
    assert starts == [*activation.UNITS, *activation.AI_UNITS]


def test_health_receipt_must_belong_to_current_scheduler_instance(monkeypatch):
    service = ObservedServices()
    service.heartbeat_pid = 999
    monkeypatch.setattr(activation.time, "sleep", lambda duration: None)
    with pytest.raises(activation.ActivationError, match="runtime_health_failed"):
        service.verify(service.actual, service.snapshot)
    assert not service.static_verified


def test_health_manifest_must_identify_actual_windows_contract(monkeypatch):
    service = ObservedServices()
    expected = service.actual
    service.actual = manifest(NEW, OLD, windows="4" * 40)
    monkeypatch.setattr(activation.time, "sleep", lambda duration: None)
    with pytest.raises(activation.ActivationError, match="runtime_health_failed"):
        service.verify(expected, service.snapshot)


def test_nginx_verification_includes_new_page_assets_and_rejects_cached_old_bytes(tmp_path, monkeypatch):
    (tmp_path / "js").mkdir()
    (tmp_path / "css").mkdir()
    (tmp_path / "index.html").write_text('<script src="/static/js/trading-day.js?v=2"></script>')
    values = {"js/app.js": b"app", "css/style.css": b"style", "js/trading-day.js": b"new-page"}
    for name, value in values.items():
        (tmp_path / name).write_bytes(value)
    services = activation.Services()
    monkeypatch.setattr(services, "static_root", lambda manifest: tmp_path)
    monkeypatch.setattr(activation.time, "sleep", lambda duration: None)
    requested = []
    def open_http(request, *, timeout):
        requested.append(request.full_url)
        path = activation.urlsplit(request.full_url).path.removeprefix("/static/")
        return BytesIO(values[path])
    monkeypatch.setattr(services, "open_http", open_http)
    services.verify_static(manifest(NEW, OLD))
    assert "http://127.0.0.1/static/js/trading-day.js?v=2" in requested
    values["js/trading-day.js"] = b"cached-old-page"
    with pytest.raises(activation.ActivationError, match="static_release_bytes_differ"):
        services.verify_static(manifest(NEW, OLD))
