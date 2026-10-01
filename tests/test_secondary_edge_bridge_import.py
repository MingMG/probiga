"""Local browser library import must not configure or start production."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys

import pytest

from tools import run_codex_web_bridge as bridge
from tools.remote_support import UnsafeProductionSshError


ROOT = Path(__file__).resolve().parents[1]


def test_real_account_chrome_api_import_requires_no_production_host():
    environment = os.environ.copy()
    environment.pop("PROBIGA_REMOTE_SSH_HOST", None)
    environment.pop("PROBIGA_AI_BRIDGE_SERVER_URL", None)
    code = """
import subprocess
import socket
import urllib.request

def forbidden(*args, **kwargs):
    raise AssertionError('NETWORK_OR_PROCESS_START_FORBIDDEN')

subprocess.Popen = forbidden
socket.create_connection = forbidden
urllib.request.urlopen = forbidden
from tools.secondary_edge.account_readiness import _load_chrome_api
session_type, connection_type, state_script = _load_chrome_api()
assert session_type.__name__ == 'DeepSeekChromeSession'
assert connection_type.__name__ == 'CdpConnection'
assert isinstance(state_script, str) and state_script
print('import-ready')
"""
    result = subprocess.run(
        [sys.executable, "-B", "-c", code], cwd=ROOT, env=environment,
        capture_output=True, text=True, timeout=15,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "import-ready"
    assert not result.stderr


@pytest.mark.parametrize("server_override", [None, "https://example.invalid/bridge"])
def test_production_parser_still_requires_remote_host(monkeypatch, server_override):
    monkeypatch.delenv("PROBIGA_REMOTE_SSH_HOST", raising=False)
    if server_override is None:
        monkeypatch.delenv("PROBIGA_AI_BRIDGE_SERVER_URL", raising=False)
    else:
        monkeypatch.setenv("PROBIGA_AI_BRIDGE_SERVER_URL", server_override)
    with pytest.raises(UnsafeProductionSshError, match="PROBIGA_REMOTE_SSH_HOST is required for remote SSH"):
        bridge.build_parser()


def test_production_parser_default_is_resolved_from_current_host(monkeypatch):
    monkeypatch.setenv("PROBIGA_REMOTE_SSH_HOST", "example.invalid")
    monkeypatch.delenv("PROBIGA_AI_BRIDGE_SERVER_URL", raising=False)
    assert bridge.build_parser().parse_args([]).server_url == "http://example.invalid"


def test_production_parser_preserves_explicit_server_environment(monkeypatch):
    monkeypatch.setenv("PROBIGA_REMOTE_SSH_HOST", "example.invalid")
    monkeypatch.setenv("PROBIGA_AI_BRIDGE_SERVER_URL", "https://example.invalid/bridge")
    assert bridge.build_parser().parse_args([]).server_url == "https://example.invalid/bridge"
