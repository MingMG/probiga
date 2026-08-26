from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from tools import run_big_qmt_bridge as bridge


def _task(**overrides):
    task = {
        "id": 69,
        "enabled": 1,
        "cron_time": "15:20",
        "last_run_at": None,
        "last_triggered_at": None,
        "last_run_status": None,
    }
    task.update(overrides)
    return task


def test_etf_forward_delegation_waits_for_due_time():
    engine = MagicMock()
    with patch.object(
        bridge,
        "_etf_forward_task",
        return_value=_task(),
    ), patch.object(
        bridge,
        "_calendar_trade_day",
    ) as trade_day:
        result = bridge.maybe_run_etf_forward_daily(
            engine,
            now=datetime(2026, 7, 27, 15, 19),
        )
    assert result["status"] == "not_due"
    trade_day.assert_not_called()


def test_etf_forward_delegation_runs_once_on_qmt_host():
    engine = MagicMock()
    command_result = {
        "returncode": 0,
        "stdout_tail": '{"status":"success"}',
        "stderr_tail": "",
    }
    with patch.object(
        bridge,
        "_etf_forward_task",
        return_value=_task(),
    ), patch.object(
        bridge,
        "_calendar_trade_day",
        return_value=True,
    ), patch.object(
        bridge,
        "claim_scheduler_task_run",
        return_value=True,
    ) as claim, patch.object(
        bridge,
        "update_scheduler_task",
    ) as update, patch(
        "server.common.scheduler_validation.scheduler_output_status",
        return_value="success",
    ), patch(
        "server.common.scheduler_validation.validate_scheduler_task_result",
        return_value=MagicMock(checked=True, ok=True, message="verified"),
    ):
        result = bridge.maybe_run_etf_forward_daily(
            engine,
            now=datetime(2026, 7, 27, 15, 20),
            runner=lambda: command_result,
        )

    assert result["status"] == "success"
    claim.assert_called_once_with(engine, 69)
    assert update.call_args.args[2]["last_run_status"] == "success"
    assert "windows_big_qmt_bridge" in (
        update.call_args.args[2]["last_run_output"]
    )


def test_etf_forward_command_does_not_invoke_legacy_ssh_promotion():
    completed = MagicMock(returncode=0, pid=1234)
    receipt = {
        "schema": "probiga.etf-forward-daily-receipt.v1",
        "status": "PASS",
    }
    completed.communicate.return_value = (json.dumps(receipt), "")

    with patch.object(bridge.subprocess, "Popen", return_value=completed) as popen:
        result = bridge._run_etf_forward_command()

    assert popen.call_count == 1
    completed.communicate.assert_called_once_with(timeout=1200)
    assert result["returncode"] == 0
    assert result["machine_receipt"] == receipt
    assert result["delivery_mode"] == "CONFIGURED_DATABASE_DIRECT"
    assert result["promotion_stderr_tail"] == ""


def test_etf_forward_command_parses_receipt_before_diagnostic_truncation():
    receipt = {
        "schema": "probiga.etf-forward-daily-receipt.v1",
        "status": "PASS",
        "diagnostic": "x" * 4000,
    }
    completed = MagicMock(returncode=0, pid=1234)
    completed.communicate.return_value = (json.dumps(receipt), "")

    with patch.object(bridge.subprocess, "Popen", return_value=completed):
        result = bridge._run_etf_forward_command()

    assert result["machine_receipt"] == receipt
    assert len(result["daily_stdout_tail"]) == 1000


def test_etf_forward_delegation_does_not_repeat_success():
    engine = MagicMock()
    with patch.object(
        bridge,
        "_etf_forward_task",
        return_value=_task(
            last_run_at=datetime(2026, 7, 27, 15, 20),
            last_triggered_at=datetime(2026, 7, 27, 15, 20),
            last_run_status="success",
        ),
    ), patch.object(
        bridge,
        "_calendar_trade_day",
        return_value=True,
    ), patch.object(
        bridge,
        "claim_scheduler_task_run",
    ) as claim:
        result = bridge.maybe_run_etf_forward_daily(
            engine,
            now=datetime(2026, 7, 27, 16, 0),
        )
    assert result["status"] == "current"
    claim.assert_not_called()


def test_etf_forward_delegation_retries_failed_run_after_cooldown():
    engine = MagicMock()
    failed_at = datetime(2026, 7, 27, 15, 20)
    base_task = _task(
        last_run_at=failed_at,
        last_triggered_at=failed_at,
        last_run_status="failed",
    )
    with patch.object(
        bridge,
        "_etf_forward_task",
        return_value=base_task,
    ), patch.object(
        bridge,
        "_calendar_trade_day",
        return_value=True,
    ), patch.object(
        bridge,
        "claim_scheduler_task_run",
        return_value=True,
    ) as claim, patch.object(
        bridge,
        "update_scheduler_task",
    ), patch(
        "server.common.scheduler_validation.scheduler_output_status",
        return_value="success",
    ), patch(
        "server.common.scheduler_validation.validate_scheduler_task_result",
        return_value=MagicMock(checked=True, ok=True, message="verified"),
    ):
        waiting = bridge.maybe_run_etf_forward_daily(
            engine,
            now=failed_at + timedelta(minutes=9),
        )
        retried = bridge.maybe_run_etf_forward_daily(
            engine,
            now=failed_at + timedelta(minutes=10),
            runner=lambda: {
                "returncode": 0,
                "stdout_tail": "",
                "stderr_tail": "",
            },
        )
    assert waiting["status"] == "retry_wait"
    assert retried["status"] == "success"
    claim.assert_called_once_with(engine, 69)


def test_daemon_publishes_quote_receipt_before_optional_slow_jobs(
    monkeypatch,
    tmp_path,
):
    events: list[str] = []
    watchlist = {
        "universe": ["000001"],
        "tracked": ["000001"],
        "short_name_map": {"000001": "Ping An"},
    }

    monkeypatch.setattr(bridge.signal, "signal", lambda *_args: None)
    monkeypatch.setattr(bridge, "create_batch_engine", lambda **_kwargs: object())
    monkeypatch.setattr(bridge, "refresh_watchlist", lambda *_args, **_kwargs: watchlist)
    monkeypatch.setattr(bridge.time, "monotonic", lambda: 1_000.0)
    monkeypatch.setattr(
        bridge,
        "ingest_once",
        lambda *_args, **_kwargs: (
            events.append("ingest")
            or {"status": "success", "freshness_required": False}
        ),
    )
    monkeypatch.setattr(
        bridge,
        "maybe_run_etf_forward_daily",
        lambda *_args, **_kwargs: (
            events.append("etf_forward") or {"status": "not_due"}
        ),
    )
    monkeypatch.setattr(
        bridge,
        "maybe_sync_membership_snapshot",
        lambda *_args, **_kwargs: (
            events.append("membership") or {"status": "not_due"}
        ),
    )
    monkeypatch.setattr(bridge, "_write_status", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        bridge,
        "_launch_maintenance_job",
        lambda state, *, name, runner: (
            state["results"].__setitem__(name, runner()) is None
        ),
    )

    def stop_after_first_cycle(_seconds):
        raise KeyboardInterrupt

    monkeypatch.setattr(bridge.time, "sleep", stop_after_first_cycle)

    with pytest.raises(KeyboardInterrupt):
        bridge.run_daemon(
            qmt_home=tmp_path,
            poll_seconds=1.0,
            tracked_limit=10,
        )

    assert events == ["ingest", "etf_forward", "membership"]


def test_maintenance_job_slot_serializes_slow_jobs():
    state: dict = {"results": {}}
    started = threading.Event()
    release = threading.Event()

    def slow_job():
        started.set()
        assert release.wait(2)
        return {"status": "success", "rows": 1}

    assert bridge._launch_maintenance_job(
        state,
        name="slow",
        runner=slow_job,
    )
    assert started.wait(1)
    assert not bridge._launch_maintenance_job(
        state,
        name="second",
        runner=lambda: {"status": "success"},
    )

    release.set()
    state["thread"].join(2)
    assert not state["thread"].is_alive()
    assert state["thread"].daemon
    assert state["results"]["slow"] == {"status": "success", "rows": 1}
    assert "second" not in state["results"]


def test_shutdown_terminates_tracked_maintenance_process_tree():
    process = MagicMock(pid=4321)
    process.poll.return_value = None
    with bridge._maintenance_process_lock:
        bridge._maintenance_processes.add(process)
    try:
        with patch.object(bridge.os, "name", "nt"), patch.object(
            bridge.subprocess,
            "run",
        ) as taskkill:
            bridge._terminate_active_maintenance_processes()
        taskkill.assert_called_once()
        command = taskkill.call_args.args[0]
        assert command == ["taskkill.exe", "/PID", "4321", "/T", "/F"]
        process.kill.assert_called_once()
    finally:
        with bridge._maintenance_process_lock:
            bridge._maintenance_processes.discard(process)


def test_shutdown_falls_back_when_taskkill_is_unavailable():
    process = MagicMock(pid=9876)
    process.poll.return_value = None
    with patch.object(bridge.os, "name", "nt"), patch.object(
        bridge.subprocess,
        "run",
        side_effect=FileNotFoundError("taskkill missing"),
    ):
        bridge._terminate_maintenance_process_tree(process)
    process.kill.assert_called_once()


def test_daemon_keeps_ingesting_while_maintenance_is_slow(
    monkeypatch,
    tmp_path,
):
    ingests: list[int] = []
    maintenance_started = threading.Event()
    maintenance_release = threading.Event()
    maintenance_done = threading.Event()
    membership_calls: list[int] = []
    watchlist = {
        "universe": ["000001"],
        "tracked": ["000001"],
        "short_name_map": {"000001": "Ping An"},
    }

    monkeypatch.setattr(bridge.signal, "signal", lambda *_args: None)
    monkeypatch.setattr(bridge, "create_batch_engine", lambda **_kwargs: object())
    monkeypatch.setattr(bridge, "refresh_watchlist", lambda *_args, **_kwargs: watchlist)
    monkeypatch.setattr(
        bridge,
        "ingest_once",
        lambda *_args, **_kwargs: (
            ingests.append(len(ingests) + 1)
            or {"status": "success", "freshness_required": False}
        ),
    )

    def slow_maintenance(_engine):
        maintenance_started.set()
        try:
            assert maintenance_release.wait(2)
            return {"status": "success"}
        finally:
            maintenance_done.set()

    monkeypatch.setattr(bridge, "maybe_run_etf_forward_daily", slow_maintenance)
    monkeypatch.setattr(
        bridge,
        "maybe_sync_membership_snapshot",
        lambda _engine: membership_calls.append(1) or {"status": "success"},
    )
    monkeypatch.setattr(bridge, "_write_status", lambda *_args, **_kwargs: None)

    def stop_after_three_cycles(_seconds):
        assert maintenance_started.wait(1)
        if len(ingests) >= 3:
            maintenance_release.set()
            raise KeyboardInterrupt

    monkeypatch.setattr(bridge.time, "sleep", stop_after_three_cycles)

    with pytest.raises(KeyboardInterrupt):
        bridge.run_daemon(
            qmt_home=tmp_path,
            poll_seconds=1.0,
            tracked_limit=10,
        )

    assert ingests == [1, 2, 3]
    assert membership_calls == []
    assert maintenance_done.wait(1)


def test_process_identity_distinguishes_the_current_process():
    alive, start_token = bridge._process_identity(os.getpid())
    assert alive
    assert start_token


def test_malformed_owner_payload_falls_back_to_bounded_legacy_lease():
    assert bridge._parse_bridge_task_owner(
        json.dumps(
            {
                "executor": "windows_big_qmt_bridge",
                "lease_version": "not-a-number",
                "pid": "not-a-pid",
            }
        )
    ) is None


def test_dead_maintenance_owner_is_recovered_and_reclaimed():
    engine = MagicMock()
    connection = engine.begin.return_value.__enter__.return_value
    connection.execute.return_value.rowcount = 1
    task = _task(
        last_run_at=datetime(2026, 8, 17, 15, 20),
        last_triggered_at=datetime(2026, 8, 17, 15, 20),
        last_run_status="running",
        last_run_output=json.dumps(
            {
                "executor": "windows_big_qmt_bridge",
                "lease_version": 1,
                "host": "test-host",
                "pid": 1234,
                "process_start_token": "old-token",
            }
        ),
    )

    with patch.object(
        bridge,
        "_bridge_task_owner_is_alive",
        return_value=False,
    ), patch.object(
        bridge,
        "claim_scheduler_task_run",
        return_value=True,
    ) as claim, patch.object(
        bridge,
        "update_scheduler_task",
    ) as update:
        claimed = bridge._claim_bridge_task_run(
            engine,
            task,
            task_type=bridge.ETF_FORWARD_TASK_TYPE,
        )

    assert claimed
    assert connection.execute.call_count == 1
    claim.assert_called_once_with(engine, 69)
    owner = json.loads(update.call_args.args[2]["last_run_output"])
    assert owner["lease_version"] == 1
    assert owner["pid"] == os.getpid()
    assert owner["process_start_token"]


def test_etf_running_row_reaches_owner_lease_recovery():
    engine = MagicMock()
    task = _task(
        last_run_at=datetime(2026, 8, 17, 15, 20),
        last_triggered_at=datetime(2026, 8, 17, 15, 20),
        last_run_status="running",
    )
    with patch.object(
        bridge,
        "_etf_forward_task",
        return_value=task,
    ), patch.object(
        bridge,
        "_calendar_trade_day",
        return_value=True,
    ), patch.object(
        bridge,
        "_claim_bridge_task_run",
        return_value=False,
    ) as claim:
        result = bridge.maybe_run_etf_forward_daily(
            engine,
            now=datetime(2026, 8, 17, 15, 25),
        )

    assert result["status"] == "already_running"
    claim.assert_called_once_with(
        engine,
        task,
        task_type=bridge.ETF_FORWARD_TASK_TYPE,
    )


def test_live_maintenance_owner_is_never_stolen():
    engine = MagicMock()
    task = _task(
        last_run_at=datetime(2026, 8, 17, 15, 20),
        last_triggered_at=datetime(2026, 8, 17, 15, 20),
        last_run_status="running",
        last_run_output=json.dumps(
            {
                "executor": "windows_big_qmt_bridge",
                "lease_version": 1,
                "host": "test-host",
                "pid": 1234,
                "process_start_token": "live-token",
            }
        ),
    )

    with patch.object(
        bridge,
        "_bridge_task_owner_is_alive",
        return_value=True,
    ), patch.object(bridge, "claim_scheduler_task_run") as claim:
        claimed = bridge._claim_bridge_task_run(
            engine,
            task,
            task_type=bridge.ETF_FORWARD_TASK_TYPE,
        )

    assert not claimed
    engine.begin.assert_not_called()
    claim.assert_not_called()


def test_expired_legacy_running_task_has_bounded_recovery():
    engine = MagicMock()
    connection = engine.begin.return_value.__enter__.return_value
    connection.execute.return_value.rowcount = 1
    task = _task(
        last_run_at=datetime(2026, 8, 17, 12, 0),
        last_triggered_at=datetime(2026, 8, 17, 12, 0),
        last_run_status="running",
        last_run_duration=0,
        last_run_output="legacy output without an owner lease",
    )

    recovered = bridge._recover_abandoned_bridge_task(
        engine,
        task,
        task_type=bridge.ETF_FORWARD_TASK_TYPE,
        now=datetime(2026, 8, 17, 13, 0),
    )

    assert recovered
    params = connection.execute.call_args.args[1]
    assert "legacy maintenance lease expired" in params["output"]
