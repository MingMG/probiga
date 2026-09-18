from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from server.api import scheduler_runtime as runtime


@pytest.mark.parametrize("boot_offset,absent,foreign,expected", [
    (3600, True, False, True),
    (None, True, False, False),
    (-3600, True, False, False),
    (30, True, False, False),
    (3600, False, False, False),
    (3600, True, True, False),
])
def test_same_build_needs_preboot_same_host_and_absent_pid(
    monkeypatch, boot_offset, absent, foreign, expected,
):
    started = datetime(2026, 9, 18, 1, 28, 16)
    history = dict(run_uid="reboot-run", run_at=started, status="running",
                   host_name="other" if foreign else "windows-host",
                   scheduler_instance_id="windows-host-42376",
                   build_sha="c" * 40, trigger_source="scheduled")
    selected = MagicMock()
    selected.mappings.return_value.all.return_value = [history]
    connection = MagicMock()
    connection.execute.side_effect = [selected, MagicMock(rowcount=1), MagicMock(rowcount=1)]
    engine = MagicMock()
    engine.begin.return_value.__enter__.return_value = connection
    monkeypatch.setattr(runtime, "gethostname", lambda: "windows-host")
    monkeypatch.setattr(runtime, "_scheduler_build_commit_sha", lambda: "c" * 40)
    monkeypatch.setattr(runtime, "_owner_pid_is_absent", lambda *a, **kw: absent)
    monkeypatch.setattr(runtime, "_windows_boot_started_at", lambda:
                        None if boot_offset is None else started + timedelta(seconds=boot_offset))
    assert runtime._recover_interrupted_manual_claim(engine, {"id": 114}, started) is expected
    assert connection.execute.call_count == (3 if expected else 1)
    if expected:
        history_sql = str(connection.execute.call_args_list[1].args[0])
        assert "status='failed'" in history_sql
        assert "previous_windows_boot" in connection.execute.call_args_list[2].args[1]["output"]


@pytest.mark.parametrize("clock_delta,offset,aware,expected", [
    (0, 3600, True, True), (600, 3600, True, False),
    (0, 30, True, False), (0, 3600, False, False),
])
def test_boot_evidence_requires_cim_uptime_agreement(monkeypatch, clock_delta, offset, aware, expected):
    now = datetime(2026, 9, 18, 9, 0)
    boot = now - timedelta(seconds=offset)
    monkeypatch.setattr(runtime, "os", SimpleNamespace(name="nt", environ={"SystemRoot": "C:/Windows"}))
    monkeypatch.setattr(runtime, "_now_shanghai_naive", lambda: now)
    monkeypatch.setattr(runtime, "_scheduler_started_at", now - timedelta(seconds=10))
    monkeypatch.setattr(runtime.subprocess, "CREATE_NO_WINDOW", 0, raising=False)
    run = MagicMock(return_value=SimpleNamespace(stdout=boot.isoformat() + ("+08:00" if aware else "")))
    monkeypatch.setattr(runtime.subprocess, "run", run)
    kernel = MagicMock()
    kernel.GetTickCount64.return_value = (offset + clock_delta) * 1000
    monkeypatch.setattr(runtime.ctypes, "WinDLL", lambda *a, **kw: kernel, raising=False)
    assert runtime._windows_boot_started_at() == (boot if expected else None)
    assert run.call_args.kwargs["timeout"] == 10


def test_boot_read_failure_keeps_claim(monkeypatch):
    monkeypatch.setattr(runtime, "os", SimpleNamespace(name="nt", environ={}))
    assert runtime._windows_boot_started_at() is None
