from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from integrations.qmt import bridge
from integrations.qmt import diagnostics as qmt_diagnostics


def test_iter_client_candidates_prefers_guojin_environment(monkeypatch, tmp_path: Path):
    exe = tmp_path / "bin.x64" / "XtMiniQmt.exe"
    monkeypatch.setenv("GJ_QMT_EXE", str(exe))

    candidates = qmt_diagnostics.iter_client_candidates()

    assert candidates[0] == exe


def test_resolve_client_path_returns_existing_candidate(monkeypatch, tmp_path: Path):
    exe = tmp_path / "XtMiniQmt.exe"
    exe.write_bytes(b"")
    monkeypatch.setattr(qmt_diagnostics, "iter_client_candidates", lambda: [exe])

    assert qmt_diagnostics.resolve_client_path() == exe.resolve()


def test_client_status_reports_installed_and_running(monkeypatch, tmp_path: Path):
    exe = tmp_path / "XtMiniQmt.exe"
    exe.write_bytes(b"")
    monkeypatch.setattr(qmt_diagnostics, "resolve_client_path", lambda: exe)
    monkeypatch.setattr(qmt_diagnostics, "_windows_process_rows", lambda: [{"pid": "42"}])

    result = qmt_diagnostics.client_status()

    assert result["provider"] == "gj_qmt"
    assert result["installed"] is True
    assert result["running"] is True


def test_bridge_status_normalizes_timeout(monkeypatch, tmp_path: Path):
    python = tmp_path / "python.exe"
    python.write_bytes(b"")
    monkeypatch.setattr(bridge, "python_path", lambda: python)
    monkeypatch.setattr(bridge, "is_configured", lambda: True)
    monkeypatch.setattr(
        bridge,
        "ping",
        lambda **_: (_ for _ in ()).throw(bridge.QmtBridgeError("QMT worker timed out after 2s")),
    )

    result = qmt_diagnostics.bridge_status(timeout=2)

    assert result["connected"] is False
    assert result["error_code"] == "CONNECT_TIMEOUT"


def test_bridge_run_wraps_subprocess_timeout(monkeypatch, tmp_path: Path):
    python = tmp_path / "python.exe"
    worker = tmp_path / "worker.py"
    python.write_bytes(b"")
    worker.write_text("", encoding="utf-8")
    monkeypatch.setattr(bridge, "python_path", lambda: python)
    monkeypatch.setattr(bridge, "WORKER", worker)
    monkeypatch.setenv("QMT_GATEWAY_ENABLED", "0")
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            subprocess.TimeoutExpired(cmd=args[0] if args else "qmt", timeout=kwargs.get("timeout", 1))
        ),
    )

    with pytest.raises(bridge.QmtBridgeError, match="timed out after 3s"):
        bridge.ping(timeout=3)


def test_capabilities_uses_worker_action(monkeypatch):
    monkeypatch.setattr(bridge, "_run", lambda payload, timeout=None: {"ok": True, "action": payload["action"]})

    result = bridge.capabilities(timeout=4)

    assert result["action"] == "capabilities"


def test_probe_core_uses_worker_action(monkeypatch):
    monkeypatch.setattr(bridge, "_run", lambda payload, timeout=None: {"ok": True, "action": payload["action"]})

    result = bridge.probe_core(timeout=4)

    assert result["action"] == "probe_core"


def test_refresh_reference_data_uses_worker_action(monkeypatch):
    monkeypatch.setattr(bridge, "_run", lambda payload, timeout=None: {"ok": True, **payload})

    result = bridge.refresh_reference_data(["download_sector_data"], timeout=4)

    assert result["action"] == "refresh_reference_data"
    assert result["operations"] == ["download_sector_data"]
