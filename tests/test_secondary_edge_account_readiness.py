"""Account readiness contracts; never open Chrome or access a real account."""
from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from tools.secondary_edge import account_readiness as readiness


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def chrome_stub(monkeypatch):
    calls = []
    controls = {"state": {"ready": True, "captcha": False}}

    class Session:
        def __init__(self, profile):
            calls.append(("profile", profile))

        def page(self):
            if controls.get("page_error"):
                raise RuntimeError("private provider page failure")
            return {"webSocketDebuggerUrl": "stub-endpoint"}

    class Connection:
        def __init__(self, endpoint):
            calls.append(("connect", endpoint))

        def evaluate(self, script):
            calls.append(("evaluate", script))
            if controls.get("evaluate_error"):
                raise RuntimeError("private provider evaluation failure")
            return controls["state"]

        def close(self):
            calls.append(("close",))
            if controls.get("close_error"):
                raise RuntimeError("private provider close failure")

    monkeypatch.setattr(readiness, "_load_chrome_api", lambda: (Session, Connection, "read-only-dom"))
    return controls, calls


@pytest.mark.parametrize("state,expected", [
    ({"ready": True, "captcha": False}, 0),
    ({"ready": True, "captcha": True}, 10),
    ({"ready": False, "captcha": False}, 10),
    ({}, 10),
    (None, 10),
    ("invalid-dom-state", 20),
])
def test_login_readiness_codes_and_connection_close(chrome_stub, tmp_path, state, expected):
    controls, calls = chrome_stub
    controls["state"] = state
    assert readiness.check_deepseek_readiness(tmp_path) == expected
    assert calls == [("profile", tmp_path), ("connect", "stub-endpoint"),
                     ("evaluate", "read-only-dom"), ("close",)]


@pytest.mark.parametrize("failure", ["page_error", "evaluate_error", "close_error"])
def test_provider_errors_are_private_and_fail_closed(chrome_stub, tmp_path, capsys, failure):
    controls, calls = chrome_stub
    controls[failure] = True
    assert readiness.check_deepseek_readiness(tmp_path) == 20
    assert calls.count(("close",)) == (0 if failure == "page_error" else 1)
    output = capsys.readouterr()
    assert not output.out and not output.err


def test_dependency_import_errors_are_private(monkeypatch, tmp_path, capsys):
    def fail_import():
        raise ImportError("private environment details")

    monkeypatch.setattr(readiness, "_load_chrome_api", fail_import)
    assert readiness.check_deepseek_readiness(tmp_path) == 20
    output = capsys.readouterr()
    assert not output.out and not output.err


def test_main_passes_profile_as_path_without_other_actions(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(readiness, "check_deepseek_readiness", lambda profile: calls.append(profile) or 10)
    assert readiness.main(["--profile", str(tmp_path)]) == 10
    assert calls == [tmp_path]


def test_help_does_not_import_chrome_or_perform_account_readiness(monkeypatch, capsys):
    def forbidden():
        raise AssertionError("BROWSER_IMPORT_FORBIDDEN")

    monkeypatch.setattr(readiness, "_load_chrome_api", forbidden)
    with pytest.raises(SystemExit) as result:
        readiness.main(["--help"])
    assert result.value.code == 0
    assert "--profile" in capsys.readouterr().out


@pytest.mark.skipif(os.name != "nt" or not shutil.which("powershell.exe"),
                    reason="native Windows PowerShell 5.1 required")
def test_native_powershell_module_help_avoids_python_c_quoting_and_browser():
    program = sys.executable.replace("'", "''")
    result = subprocess.run(
        [shutil.which("powershell.exe"), "-NoProfile", "-NonInteractive", "-Command",
         f"$ErrorActionPreference='Stop';& '{program}' -B -m tools.secondary_edge.account_readiness --help;exit $LASTEXITCODE"],
        cwd=ROOT, capture_output=True, text=True, timeout=15,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "--profile" in result.stdout
    assert "without generating a response" in result.stdout
