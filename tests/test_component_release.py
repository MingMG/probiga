from __future__ import annotations

import hashlib
import json
import os
import stat
import ctypes
import errno
from pathlib import Path
from types import SimpleNamespace

import pytest

from server.common import component_release as component


LINUX = "a" * 40
ANCHOR = "b" * 40
PARENT = "c" * 40
DIGEST = "d" * 64


@pytest.fixture(autouse=True)
def _clean_identity(monkeypatch):
    for key in (*component._BUILD_ENV, "PROBIGA_COMPONENT_RELEASE_PATH", "PROBIGA_DEPLOYMENT_MODE", "PROBIGA_SCHEDULER_EXECUTOR_ROLE"):
        monkeypatch.delenv(key, raising=False)


def _manifest(**overrides):
    values = dict(
        linux_build_sha=LINUX, windows_build_sha=ANCHOR,
        contract_build_sha=ANCHOR, contract_sha256=DIGEST,
        parent_linux_build_sha=PARENT, scope="LINUX",
        created_at="2026-09-19T02:00:00Z",
    )
    values.update(overrides)
    return component.build_component_release(**values)


def _reseal(payload):
    core = {key: value for key, value in payload.items() if key != "manifest_sha256"}
    return {**core, "manifest_sha256": hashlib.sha256(json.dumps(core, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()}


def _runtime_file(monkeypatch, tmp_path, payload=None):
    value = payload or _manifest()
    path = component.write_component_release(tmp_path / "manifest.json", value)
    monkeypatch.setattr(component.sys, "platform", "linux")
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", LINUX)
    monkeypatch.setenv("PROBIGA_COMPONENT_RELEASE_PATH", f"{component.COMPONENT_RELEASE_ROOT}/{LINUX}/component-release.json")
    # Keep the production location check; only the root-owned OS trust boundary
    # is substituted because this test suite also runs on Windows workstations.
    monkeypatch.setattr(component, "_trusted_read", lambda _path: component._read_regular(path))
    return path


def test_seal_has_exact_canonical_content_and_round_trips():
    manifest = _manifest()
    assert component.validate_component_release(manifest) == manifest
    assert _reseal(manifest) == manifest
    coordinated = _manifest(linux_build_sha=ANCHOR, scope="COORDINATED")
    assert coordinated["linux_build_sha"] == coordinated["windows_build_sha"] == coordinated["contract_build_sha"]


@pytest.mark.parametrize(("field", "value"), [
    ("linux_build_sha", "0" * 40), ("linux_build_sha", "A" * 40),
    ("windows_build_sha", "b" * 39), ("parent_linux_build_sha", ""),
    ("parent_linux_build_sha", "0" * 40), ("contract_build_sha", "x" * 40),
    ("contract_sha256", "x" * 64), ("scope", "WINDOWS"),
    ("created_at", "2026-09-19T02:00:00"), ("created_at", "invalid"),
])
def test_invalid_fields_cannot_be_built(field, value):
    with pytest.raises(component.ComponentReleaseError):
        _manifest(**{field: value})


@pytest.mark.parametrize("overrides", [
    {"scope": "COORDINATED"}, {"windows_build_sha": PARENT},
    {"contract_build_sha": PARENT},
])
def test_invalid_component_combinations_rejected(overrides):
    with pytest.raises(component.ComponentReleaseError):
        _manifest(**overrides)


def test_resealed_unknown_and_missing_fields_still_rejected():
    payload = _manifest()
    payload["allow_compatibility"] = "yes"
    with pytest.raises(component.ComponentReleaseError, match="fields"):
        component.validate_component_release(_reseal(payload))
    payload = _manifest()
    del payload["parent_linux_build_sha"]
    with pytest.raises(component.ComponentReleaseError, match="fields"):
        component.validate_component_release(_reseal(payload))
    payload = _manifest()
    payload["linux_build_sha"] = 1
    with pytest.raises(component.ComponentReleaseError, match="strings"):
        component.validate_component_release(_reseal(payload))


def test_file_is_immutable_including_creation_timestamp(tmp_path):
    target = tmp_path / "release" / component.COMPONENT_RELEASE_FILENAME
    value = _manifest()
    assert component.write_component_release(target, value) == target
    initial = target.read_bytes()
    assert component.write_component_release(target, value) == target
    assert target.read_bytes() == initial
    assert target.stat().st_nlink == 1
    assert target.stat().st_mode & stat.S_IWUSR == 0
    with pytest.raises(component.ComponentReleaseError, match="immutable"):
        component.write_component_release(target, _manifest(created_at="2026-09-19T03:00:00Z"))
    assert target.read_bytes() == initial
    assert sorted(item.name for item in target.parent.iterdir()) == [target.name]
    with pytest.raises(component.ComponentReleaseError, match="absolute"):
        component.write_component_release("relative.json", value)


def test_existing_tampered_and_duplicate_json_files_rejected(tmp_path):
    path = tmp_path / "manifest.json"
    manifest = _manifest()
    manifest["contract_sha256"] = "e" * 64
    path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(component.ComponentReleaseError, match="seal"):
        component.write_component_release(path, _manifest())
    valid = json.dumps(_manifest())
    path.write_text('{"schema":"probiga.component-release.v1",' + valid[1:], encoding="utf-8")
    with pytest.raises(component.ComponentReleaseError, match="duplicate"):
        component._read_regular(path)


@pytest.mark.parametrize("contents", ["[]", "null", "NaN", "\"text\"", "{}", " " * 16385])
def test_malformed_or_oversized_files_rejected(tmp_path, contents):
    path = tmp_path / "manifest.json"
    path.write_text(contents, encoding="utf-8")
    with pytest.raises(component.ComponentReleaseError):
        component._read_regular(path)


def test_hard_link_is_not_a_sealed_file(tmp_path):
    target = component.write_component_release(tmp_path / "manifest.json", _manifest())
    alias = tmp_path / "alias.json"
    os.link(target, alias)
    try:
        with pytest.raises(component.ComponentReleaseError, match="single-link"):
            component._read_regular(target)
    finally:
        if os.name == "nt":
            os.chmod(alias, 0o600)
        alias.unlink()


def test_symlink_is_not_a_sealed_file(tmp_path):
    target = component.write_component_release(tmp_path / "manifest.json", _manifest())
    alias = tmp_path / "alias.json"
    try:
        alias.symlink_to(target)
    except OSError:
        pytest.skip("symlink creation unavailable on this platform")
    with pytest.raises(component.ComponentReleaseError, match="single-link"):
        component._read_regular(alias)


def test_production_linux_resolves_roles_without_overwriting_actual_build(monkeypatch, tmp_path):
    _runtime_file(monkeypatch, tmp_path)
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", LINUX)
    monkeypatch.setenv("EXPECTED_GIT_SHA", LINUX)
    assert component.load_runtime_component_release() == _manifest()
    assert component.runtime_component_build_sha("linux", LINUX) == LINUX
    assert component.runtime_component_build_sha("windows", LINUX) == ANCHOR
    assert component.runtime_contract_build_sha(LINUX) == ANCHOR
    assert os.environ["PROBIGA_BUILD_COMMIT_SHA"] == LINUX


@pytest.mark.parametrize("environment", ["PROBIGA_EXPECTED_GIT_SHA", "EXPECTED_GIT_SHA"])
def test_every_configured_build_must_agree(monkeypatch, tmp_path, environment):
    _runtime_file(monkeypatch, tmp_path)
    monkeypatch.setenv(environment, ANCHOR)
    with pytest.raises(component.ComponentReleaseError, match="disagree"):
        component.load_runtime_component_release()


def test_wrong_manifest_linux_build_and_expected_argument_rejected(monkeypatch, tmp_path):
    _runtime_file(monkeypatch, tmp_path, _manifest(linux_build_sha=PARENT))
    with pytest.raises(component.ComponentReleaseError, match="differs"):
        component.load_runtime_component_release()
    with pytest.raises(component.ComponentReleaseError, match="differs"):
        component.runtime_contract_build_sha(ANCHOR)


@pytest.mark.parametrize("path", [
    "", "relative.json", "/tmp/component-release.json",
    f"/var/lib/probiga/release-artifacts/{ANCHOR}/component-release.json",
    f"/var/lib/probiga/release-artifacts/{LINUX}/../{LINUX}/component-release.json",
    f"/var/lib/probiga/release-artifacts//{LINUX}/component-release.json",
])
def test_runtime_rejects_missing_map_or_alternate_paths(monkeypatch, tmp_path, path):
    _runtime_file(monkeypatch, tmp_path)
    monkeypatch.setenv("PROBIGA_COMPONENT_RELEASE_PATH", path)
    with pytest.raises(component.ComponentReleaseError, match="fixed path"):
        component.runtime_contract_build_sha(LINUX)


def test_runtime_reloads_and_detects_real_file_tampering(monkeypatch, tmp_path):
    path = _runtime_file(monkeypatch, tmp_path)
    assert component.runtime_contract_build_sha() == ANCHOR
    os.chmod(path, 0o600)
    tampered = _manifest()
    tampered["windows_build_sha"] = PARENT
    path.write_text(json.dumps(tampered), encoding="utf-8")
    with pytest.raises(component.ComponentReleaseError):
        component.runtime_contract_build_sha()


@pytest.mark.parametrize(("node", "mode", "uid"), [
    ("parent", stat.S_IFDIR | 0o777, 0),
    ("parent", stat.S_IFDIR | 0o755, 1000),
    ("parent", stat.S_IFLNK | 0o755, 0),
    ("file", stat.S_IFREG | 0o666, 0),
    ("file", stat.S_IFREG | 0o444, 1000),
])
def test_root_trust_boundary_rejects_unsafe_parent_or_file(monkeypatch, tmp_path, node, mode, uid):
    path = tmp_path / "manifest.json"
    def fake_lstat(candidate):
        selected = candidate == path if node == "file" else candidate == path.parent
        return SimpleNamespace(st_mode=mode if selected else stat.S_IFDIR | 0o755, st_uid=uid if selected else 0)
    monkeypatch.setattr(Path, "lstat", fake_lstat)
    def forbidden_read(_):
        raise AssertionError("untrusted file must never be parsed")
    monkeypatch.setattr(component, "_read_regular", forbidden_read)
    with pytest.raises(component.ComponentReleaseError, match="root-owned"):
        component._trusted_read(path)


def test_production_needs_configured_build_even_with_expected(monkeypatch):
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    with pytest.raises(component.ComponentReleaseError, match="required"):
        component.runtime_contract_build_sha(LINUX)


def test_windows_uses_own_build_without_reading_linux_manifest(monkeypatch):
    monkeypatch.setattr(component.sys, "platform", "win32")
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", ANCHOR)
    monkeypatch.setenv("PROBIGA_SCHEDULER_EXECUTOR_ROLE", "qmt_windows_edge")
    monkeypatch.setenv("PROBIGA_COMPONENT_RELEASE_PATH", "should-not-be-read")
    monkeypatch.setattr(component, "load_runtime_component_release", lambda: pytest.fail("Windows read a Linux manifest"))
    assert component.runtime_contract_build_sha(ANCHOR) == ANCHOR
    assert component.runtime_component_build_sha("windows", ANCHOR) == ANCHOR
    with pytest.raises(component.ComponentReleaseError, match="cannot establish"):
        component.runtime_component_build_sha("linux", ANCHOR)
    monkeypatch.delenv("PROBIGA_SCHEDULER_EXECUTOR_ROLE")
    with pytest.raises(component.ComponentReleaseError, match="QMT edge role"):
        component.runtime_contract_build_sha(ANCHOR)


def test_development_has_no_implicit_compatibility_mapping(monkeypatch):
    assert component.runtime_contract_build_sha(LINUX) == LINUX
    assert component.runtime_contract_build_sha() == ""
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", ANCHOR)
    assert component.runtime_contract_build_sha() == ANCHOR
    with pytest.raises(component.ComponentReleaseError, match="differs"):
        component.runtime_contract_build_sha(LINUX)
    with pytest.raises(component.ComponentReleaseError, match="role"):
        component.runtime_component_build_sha("guess-peer")


def test_cli_build_writes_only_manifest(tmp_path, capsys):
    output = tmp_path / "component-release.json"
    assert component.main([
        "build", "--linux-build-sha", LINUX, "--windows-build-sha", ANCHOR,
        "--contract-build-sha", ANCHOR, "--contract-sha256", DIGEST,
        "--parent-linux-build-sha", PARENT, "--scope", "LINUX",
        "--created-at", "2026-09-19T02:00:00Z", "--output", str(output),
    ]) == 0
    assert capsys.readouterr().out.strip() == str(output)
    assert json.loads(output.read_text(encoding="utf-8")) == _manifest()


def test_interruption_after_atomic_install_leaves_one_complete_reusable_file(monkeypatch, tmp_path):
    target = tmp_path / "component-release.json"
    install = component._atomic_install_noreplace
    def interrupted(source, destination):
        install(source, destination)
        assert not source.exists()
        assert destination.stat().st_nlink == 1
        raise KeyboardInterrupt("simulated termination after rename")
    monkeypatch.setattr(component, "_atomic_install_noreplace", interrupted)
    with pytest.raises(KeyboardInterrupt):
        component.write_component_release(target, _manifest())
    original = target.read_bytes()
    monkeypatch.setattr(component, "_atomic_install_noreplace", install)
    assert component.write_component_release(target, _manifest()) == target
    assert target.read_bytes() == original
    assert target.stat().st_nlink == 1
    assert list(tmp_path.iterdir()) == [target]


def test_linux_publication_uses_renameat2_noreplace_and_never_emulates_with_links(monkeypatch, tmp_path):
    calls = []
    class Operation:
        def __call__(self, source_directory, source, target_directory, target, flags):
            calls.append((source_directory, source, target_directory, target, flags))
            return -1
    operation = Operation()
    monkeypatch.setattr(ctypes, "CDLL", lambda *_, **__: SimpleNamespace(renameat2=operation))
    monkeypatch.setattr(ctypes, "get_errno", lambda: errno.EEXIST)
    monkeypatch.setattr(os, "link", lambda *_: pytest.fail("publication must not create hardlinks"))
    target = tmp_path / "target"
    target.write_bytes(b"original")
    with pytest.raises(FileExistsError):
        component._linux_rename_noreplace(tmp_path / "source", target)
    assert calls == [(-100, os.fsencode(tmp_path / "source"), -100, os.fsencode(target), 1)]
    assert target.read_bytes() == b"original"
    monkeypatch.setattr(ctypes, "CDLL", lambda *_, **__: SimpleNamespace())
    with pytest.raises(component.ComponentReleaseError, match="unavailable"):
        component._linux_rename_noreplace(tmp_path / "source", target)
