from __future__ import annotations

import os

from tools import run_scheduler_daemon


def test_windows_env_loader_overrides_runtime_and_removes_launcher_controls(
    monkeypatch,
):
    monkeypatch.setattr(run_scheduler_daemon.os, "name", "nt")
    monkeypatch.setattr(
        "dotenv.dotenv_values",
        lambda _path: {
            "MYSQL_URL": "mysql://runtime",
            "SCHEDULER_INTRADAY_START": "09:20",
        },
    )
    monkeypatch.setenv("MYSQL_URL", "mysql://stale")
    monkeypatch.setenv("PROBIGA_SCHEDULER_STDOUT", "must-not-leak")

    loaded = run_scheduler_daemon._load_windows_runtime_env()

    assert loaded == 2
    assert os.environ["MYSQL_URL"] == "mysql://runtime"
    assert os.environ["SCHEDULER_INTRADAY_START"] == "09:20"
    assert "PROBIGA_SCHEDULER_STDOUT" not in os.environ


def test_non_windows_env_loader_is_a_noop(monkeypatch):
    monkeypatch.setattr(run_scheduler_daemon.os, "name", "posix")
    monkeypatch.setenv("PROBIGA_SCHEDULER_STDOUT", "untouched")

    assert run_scheduler_daemon._load_windows_runtime_env() == 0
    assert os.environ["PROBIGA_SCHEDULER_STDOUT"] == "untouched"
