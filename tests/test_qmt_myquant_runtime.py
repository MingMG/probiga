"""Locked SDK repair and real updater failure gates, without production access."""
from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from tools import ensure_qmt_myquant_runtime as runtime


ROOT = Path(__file__).resolve().parents[1]
BUILD = "1" * 40
LOCK = "\n".join(f"{name}=={version} --hash=sha256:{'a' * 64}" for name, version in
                 (("gm", "3.0.186"), ("numpy", "2.3.2"), ("pandas", "2.3.1")))


@pytest.fixture
def locked_runtime(tmp_path, monkeypatch):
    lock = tmp_path / "requirements.lock"
    lock.write_text(LOCK, encoding="utf-8")
    monkeypatch.setattr(runtime, "LOCK_PATH", lock)
    monkeypatch.setattr(runtime, "validate_runtime", Mock())
    monkeypatch.setattr(runtime, "verify_import", Mock())
    monkeypatch.setattr(runtime, "install_locked_dependencies", Mock())
    return lock


def test_complete_matching_runtime_does_not_install_or_change_versions(locked_runtime, monkeypatch):
    monkeypatch.setattr(runtime, "version_mismatches", Mock(return_value=[]))
    result = runtime.ensure_runtime(BUILD, install=True)
    assert result["status"] == "READY" and result["installed"] is False
    runtime.install_locked_dependencies.assert_not_called()
    runtime.verify_import.assert_called_once_with()


def test_missing_sdk_check_does_not_install_or_import(locked_runtime, monkeypatch):
    monkeypatch.setattr(runtime, "version_mismatches", Mock(return_value=["gm"]))
    result = runtime.ensure_runtime(BUILD)
    assert result["status"] == "NEEDS_INSTALL"
    assert result["mismatched_packages"] == ["gm"]
    runtime.install_locked_dependencies.assert_not_called()
    runtime.verify_import.assert_not_called()


def test_install_rechecks_versions_and_import_before_ready(locked_runtime, monkeypatch):
    monkeypatch.setattr(runtime, "version_mismatches", Mock(side_effect=[["gm"], []]))
    result = runtime.ensure_runtime(BUILD, install=True)
    assert result["status"] == "READY" and result["installed"] is True
    runtime.install_locked_dependencies.assert_called_once_with()
    runtime.verify_import.assert_called_once_with()


@pytest.mark.parametrize("failure", ["pip", "version", "import", "lock_changed"])
def test_failed_install_never_claims_ready(locked_runtime, monkeypatch, failure):
    monkeypatch.setattr(runtime, "version_mismatches", Mock(side_effect=[
        ["gm"], ["gm"] if failure == "version" else [],
    ]))
    if failure == "pip":
        runtime.install_locked_dependencies.side_effect = runtime.RuntimeNotReady("MYQUANT_LOCKED_INSTALL_FAILED")
    elif failure == "import":
        runtime.verify_import.side_effect = runtime.RuntimeNotReady("MYQUANT_SDK_IMPORT_FAILED")
    elif failure == "lock_changed":
        runtime.install_locked_dependencies.side_effect = lambda: locked_runtime.write_text(LOCK + "\n# changed")
    with pytest.raises(runtime.RuntimeNotReady):
        runtime.ensure_runtime(BUILD, install=True)


@pytest.mark.parametrize("invalid", [
    LOCK.replace("gm==", "gm>="), LOCK + "\n--extra-index-url https://example.invalid",
    LOCK.replace(" --hash=sha256:" + "a" * 64, "", 1),
    LOCK + "\ngm==3.0.186 --hash=sha256:" + "a" * 64,
    "numpy==2.3.2 --hash=sha256:" + "a" * 64,
])
def test_unpinned_or_ambiguous_lock_is_rejected(invalid):
    with pytest.raises(runtime.RuntimeNotReady):
        runtime.parse_lock(invalid)


def test_release_lock_pins_entire_sdk_environment():
    requirements = runtime.parse_lock((ROOT / "deploy/qmt_myquant_requirements.lock").read_text())
    assert requirements["gm"] == "3.0.186"
    assert requirements["numpy"] == "2.3.2"
    assert requirements["pandas"] == "2.3.1"
    assert runtime.parse_lock(LOCK.replace(" --hash", " \\\n    --hash"))["gm"] == "3.0.186"


def test_duplicate_installed_distribution_cannot_satisfy_lock(monkeypatch):
    distributions = [SimpleNamespace(metadata={"Name": name}, version=version)
                     for name, version in (("gm", "3.0.186"), ("gm", "3.0.185"),
                                           ("numpy", "2.3.2"), ("pandas", "2.3.1"))]
    monkeypatch.setattr(runtime.metadata, "distributions", lambda: distributions)
    assert runtime.version_mismatches(runtime.parse_lock(LOCK)) == ["gm"]


def test_wrong_runtime_platform_is_rejected_before_git_or_pip(monkeypatch):
    monkeypatch.setattr(runtime.sys, "version_info", (3, 14))
    child = Mock()
    monkeypatch.setattr(runtime.subprocess, "run", child)
    with pytest.raises(runtime.RuntimeNotReady, match="MYQUANT_RUNTIME_PLATFORM_DIFFERS"):
        runtime.validate_runtime(BUILD)
    child.assert_not_called()


def test_pip_uses_binary_hash_lock_without_upgrade_and_hides_child_failure(monkeypatch):
    child = Mock(return_value=SimpleNamespace(returncode=1, stdout="token=secret", stderr="secret"))
    monkeypatch.setattr(runtime.subprocess, "run", child)
    monkeypatch.setattr(runtime, "package_source", lambda: ["--index-url", "https://pypi.org/simple"])
    with pytest.raises(runtime.RuntimeNotReady, match="^MYQUANT_LOCKED_INSTALL_FAILED$"):
        runtime.install_locked_dependencies()
    args = child.call_args.args[0]
    assert args[:5] == [sys.executable, "-I", "-m", "pip", "--isolated"]
    assert "--require-hashes" in args and "--only-binary=:all:" in args
    assert "--upgrade" not in args
    assert args[-2:] == ["-r", str(runtime.LOCK_PATH)]


@pytest.mark.parametrize("code,output,ready", [
    (0, b"PROBIGA_MYQUANT_IMPORT_READY\r\n", True),
    (0, b"", False), (0, b"other marker\n", False),
    (1, b"PROBIGA_MYQUANT_IMPORT_READY\n", False),
])
def test_import_requires_flushed_success_marker_even_with_zero_exit(monkeypatch, code, output, ready):
    child = Mock(return_value=SimpleNamespace(returncode=code, stdout=output))
    monkeypatch.setattr(runtime.subprocess, "run", child)
    if ready:
        runtime.verify_import()
    else:
        with pytest.raises(runtime.RuntimeNotReady, match="MYQUANT_SDK_IMPORT_FAILED"):
            runtime.verify_import()
    assert "flush=True" in child.call_args.args[0][-1]


@pytest.mark.parametrize("fail_import", [False, True])
def test_actual_sdk_atexit_zero_cannot_hide_failed_import(tmp_path, monkeypatch, fail_import):
    package = tmp_path / "gm"
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "api.py").write_text(
        "import atexit, os\natexit.register(lambda: os._exit(0))\n"
        + ("raise RuntimeError('import did not finish')\n" if fail_import else ""),
        encoding="utf-8",
    )
    real_run = subprocess.run
    results = []

    def fake_sdk_run(args, **kwargs):
        invocation = [*args[:-1], f"import sys; sys.path.insert(0, {str(tmp_path)!r}); " + args[-1]]
        result = real_run(invocation, **kwargs)
        results.append(result)
        return result

    monkeypatch.setattr(runtime.subprocess, "run", fake_sdk_run)
    if fail_import:
        with pytest.raises(runtime.RuntimeNotReady, match="MYQUANT_SDK_IMPORT_FAILED"):
            runtime.verify_import()
    else:
        runtime.verify_import()
    assert results[0].returncode == 0


def test_complete_fixed_wheelhouse_is_hash_verified_and_used_offline(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime, "ROOT", tmp_path)
    lock = tmp_path / "lock"
    monkeypatch.setattr(runtime, "LOCK_PATH", lock)
    wheelhouse = tmp_path / "runtime/myquant-wheels"
    wheelhouse.mkdir(parents=True)
    payload = b"fixed wheel payload"
    (wheelhouse / "gm.whl").write_bytes(payload)
    lock.write_text(LOCK.replace("a" * 64, hashlib.sha256(payload).hexdigest()))
    assert runtime.package_source() == ["--no-index", "--find-links", str(wheelhouse)]
    (wheelhouse / "gm.whl").write_bytes(b"tampered")
    assert runtime.package_source() == ["--index-url", "https://pypi.org/simple"]


@pytest.mark.parametrize("scenario", ["ready", "missing", "install_failed", "unavailable"])
def test_real_powershell_dependency_gate_preserves_stop_and_activation_order(tmp_path, scenario):
    ps = shutil.which("powershell.exe")
    if not ps or os.name != "nt":
        pytest.skip("Windows PowerShell 5.1 is required")
    checker = tmp_path / "checker.py"
    checker.write_text(
        "import json,sys\n"
        f"scenario={scenario!r}\n"
        "install='--install' in sys.argv\n"
        "status='READY' if install or scenario=='ready' else 'NEEDS_INSTALL'\n"
        "code=0 if status=='READY' else 4\n"
        "if scenario=='unavailable' or (scenario=='install_failed' and install):\n"
        "    status='BLOCKED'; code=2\n"
        f"print(json.dumps({{'schema':{runtime.SCHEMA!r}, 'build_sha':{BUILD!r},\n"
        f"    'lock_sha256':{'a' * 64!r}, 'mode':'install' if install else 'check',\n"
        "    'status':status,'qmt_calls':False,'database_writes':False}))\n"
        "raise SystemExit(code)\n", encoding="utf-8",
    )
    source = (ROOT / "tools/update_qmt_windows_edge.ps1").read_text(encoding="utf-8")
    function = source[source.index("function Invoke-QmtMyQuantRuntime("):source.index("\n$TopLevel =")]
    gate = source[source.index("# Install only from the now-selected checkout"):source.index("# The local schema receipt")]
    literal = lambda value: "'" + str(value).replace("'", "''") + "'"
    program = f"""
$ErrorActionPreference='Stop'
Set-StrictMode -Version Latest
$QmtPythonExe={literal(sys.executable)}
$MyQuantRuntimeTool={literal(checker)}
$CurrentSha='{BUILD}'
$Events=[Collections.Generic.List[string]]::new()
function Stop-EdgeScheduler {{ $Events.Add('stop') }}
function Confirm-QmtReleaseActivation([string]$Sha) {{ $Events.Add('activation') }}
function Write-UpdateLog([string]$Message) {{ }}
{function}
$Ready=$false
try {{
{gate}
$Ready=$true
}} catch {{ }}
@{{ready=$Ready;events=@($Events.ToArray())}} | ConvertTo-Json -Compress
"""
    result = subprocess.run([ps, "-NoProfile", "-NonInteractive", "-EncodedCommand",
                             base64.b64encode(program.encode("utf-16-le")).decode("ascii")],
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    proof = json.loads(result.stdout)
    assert proof["ready"] is (scenario in {"ready", "missing"})
    assert proof["events"] == (["stop", "activation"] if scenario in {"missing", "install_failed"} else [])


def test_equal_sha_missing_sdk_cannot_take_existing_ready_receipt_shortcut(tmp_path):
    ps = shutil.which("powershell.exe")
    if not ps or os.name != "nt":
        pytest.skip("Windows PowerShell 5.1 is required")
    source = (ROOT / "tools/update_qmt_windows_edge.ps1").read_text(encoding="utf-8")
    gate = source[source.index("# An equal-SHA retry"):source.index("# Phase two may quiesce")]
    (tmp_path / "tools").mkdir()
    (tmp_path / "tools/initialize_qmt_windows_state.ps1").write_text(
        "param($StateInitializationRoot, $StateInitializationBuildSha)\n", encoding="utf-8")
    program = f"""
$ErrorActionPreference='Stop'
Set-StrictMode -Version Latest
$CurrentSha='{BUILD}'
$TargetSha=$CurrentSha
$ExpectedRoot='{str(tmp_path).replace("'", "''")}'
function Confirm-QmtReleaseActivation([string]$Sha) {{ }}
function Invoke-ReadOnlyStrategyPreflight([string]$Sha) {{ return 'READY' }}
function Invoke-QmtMyQuantRuntime([string]$Sha) {{ return $false }}
function Write-UpdateLog([string]$Message) {{ throw 'must reach the installation gate' }}
{gate}
@{{proceed_to_install=$true}} | ConvertTo-Json -Compress
"""
    result = subprocess.run([ps, "-NoProfile", "-NonInteractive", "-EncodedCommand",
                             base64.b64encode(program.encode("utf-16-le")).decode("ascii")],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"proceed_to_install": True}
