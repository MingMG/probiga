from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from integrations.qmt import bridge
from integrations.qmt import diagnostics as qmt_diagnostics
from tools import archive_guojin_qmt_probe


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


def test_windows_process_rows_accepts_new_qmt_runtime_name(monkeypatch):
    monkeypatch.setattr(qmt_diagnostics.os, "name", "nt")
    monkeypatch.setattr(
        qmt_diagnostics.subprocess,
        "run",
        lambda args, **_kwargs: subprocess.CompletedProcess(
            args,
            0,
            '[{"image_name":"XtItClient.exe","pid":"42","memory":"123456"}]',
            "",
        ),
    )

    assert qmt_diagnostics._windows_process_rows() == [
        {"image_name": "XtItClient.exe", "pid": "42", "memory": "123456"}
    ]


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


def test_archive_probe_main_uses_batch_engine():
    engine = object()
    diag = {
        "status": "ok",
        "client": {"client_version": "1.0"},
        "sdk": {"sdk_module": "xtquant"},
    }

    with patch("tools.archive_guojin_qmt_probe.create_batch_engine", return_value=engine) as create_batch_engine, \
         patch("tools.archive_guojin_qmt_probe.ensure_audit_tables") as ensure_audit_tables, \
         patch("tools.archive_guojin_qmt_probe.diagnostics", return_value=diag) as diagnostics, \
         patch("tools.archive_guojin_qmt_probe.capabilities", return_value={"status": "cap"}) as capabilities, \
         patch("tools.archive_guojin_qmt_probe.core_probe", return_value={"status": "core"}) as core_probe, \
         patch("tools.archive_guojin_qmt_probe.archive_payload", return_value=object()) as archive_payload, \
         patch("tools.archive_guojin_qmt_probe.result_dict", side_effect=[{"a": 1}, {"b": 2}]):
        assert archive_guojin_qmt_probe.main() == 0

    create_batch_engine.assert_called_once_with(future=True)
    ensure_audit_tables.assert_called_once_with(engine)
    diagnostics.assert_called_once_with(force=True)
    capabilities.assert_called_once_with(force=True)
    core_probe.assert_called_once_with(force=True)
    assert archive_payload.call_count == 2
    assert all(call.args[0] is engine for call in archive_payload.call_args_list)
