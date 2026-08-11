from __future__ import annotations

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
    ) as update:
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
