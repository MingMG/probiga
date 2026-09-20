"""Ordinary post-close dependency recovery must pass both dispatch gates."""

from datetime import datetime, timedelta
import json

import pytest

from server.api import scheduler_runtime as runtime


def _stages(*, task_type, status, target, upstream_finished, downstream_started):
    receipt = json.dumps({"target_trade_date": target})
    upstream = {
        "task_type": "qmt_stock_daily_canonical",
        "enabled": 1,
        "last_triggered_at": upstream_finished - timedelta(minutes=10),
        "last_run_at": upstream_finished - timedelta(minutes=10),
        "last_run_duration": 600,
        "last_run_status": "success",
        "last_run_output": receipt,
    }
    downstream = {
        "task_type": task_type,
        "cron_time": "15:20" if task_type == "capital_flow_batch_fast" else "15:50",
        "enabled": 1,
        "_scheduler_target_available": True,
        "_scheduler_target_trade_date": target,
        "last_triggered_at": downstream_started,
        "last_run_at": downstream_started,
        "last_run_duration": 60,
        "last_run_status": status,
        "last_run_output": receipt,
    }
    return upstream, downstream


def _dispatch_gates(row, *, now):
    # A long-lived daemon cannot use the restart grace period to hide an
    # inconsistent critical-cron decision.
    return (
        runtime._cron_due(row, now=now),
        runtime._overdue_cron_allowed(
            row,
            now=now,
            cron_time=row["cron_time"],
            startup_time=datetime(2026, 9, 20, 8),
        ),
    )


@pytest.mark.parametrize("task_type", ["capital_flow_batch_fast", "target_turnover_snapshot"])
@pytest.mark.parametrize("status", ["success", "blocked", "failed", "timeout", "stopped"])
@pytest.mark.parametrize("now", [datetime(2026, 9, 21, 18), datetime(2026, 9, 22, 0, 5)])
def test_new_exact_target_input_wakes_a_completed_ordinary_attempt(task_type, status, now):
    upstream, downstream = _stages(
        task_type=task_type,
        status=status,
        target="2026-09-21",
        upstream_finished=now - timedelta(minutes=1),
        downstream_started=now - timedelta(minutes=3),
    )

    runtime._attach_daily_dependency_recovery([upstream, downstream], now=now)

    assert downstream["_dependency_recovery_due"] is True
    assert _dispatch_gates(downstream, now=now) == (True, True)


@pytest.mark.parametrize("status", ["success", "blocked"])
def test_same_input_does_not_reopen_a_terminal_attempt(status):
    now = datetime(2026, 9, 21, 18)
    upstream, downstream = _stages(
        task_type="capital_flow_batch_fast",
        status=status,
        target="2026-09-21",
        upstream_finished=now - timedelta(minutes=30),
        downstream_started=now - timedelta(minutes=20),
    )

    runtime._attach_daily_dependency_recovery([upstream, downstream], now=now)

    assert _dispatch_gates(downstream, now=now) == (False, False)


@pytest.mark.parametrize("status", ["failed", "timeout", "stopped"])
def test_current_input_retry_waits_from_completion_not_start(status, monkeypatch):
    monkeypatch.setattr(runtime, "CRON_RETRY_INTERVAL_MINUTES", 15)
    started = datetime(2026, 9, 21, 17)
    upstream, downstream = _stages(
        task_type="capital_flow_batch_fast",
        status=status,
        target="2026-09-21",
        upstream_finished=started - timedelta(minutes=1),
        downstream_started=started,
    )
    downstream["last_run_duration"] = 20 * 60
    before_retry = started + timedelta(minutes=34, seconds=59)

    runtime._attach_daily_dependency_recovery([upstream, downstream], now=before_retry)

    assert downstream["_dependency_recovery_due"] is True
    assert _dispatch_gates(downstream, now=before_retry) == (False, False)
    assert _dispatch_gates(
        downstream, now=started + timedelta(minutes=35)
    ) == (True, True)


@pytest.mark.parametrize("upstream_patch", [
    {"last_run_status": "failed"},
    {"last_run_output": json.dumps({"target_trade_date": "2026-09-18"})},
    {"_qmt_daily_truth_ready": False},
    {"enabled": 0},
])
def test_unready_or_wrong_target_input_does_not_reopen_success(upstream_patch):
    now = datetime(2026, 9, 21, 18)
    upstream, downstream = _stages(
        task_type="capital_flow_batch_fast",
        status="success",
        target="2026-09-21",
        upstream_finished=now - timedelta(minutes=1),
        downstream_started=now - timedelta(minutes=3),
    )
    upstream.update(upstream_patch)

    runtime._attach_daily_dependency_recovery([upstream, downstream], now=now)

    assert downstream.get("_dependency_recovery_due") is not True
    assert _dispatch_gates(downstream, now=now) == (False, False)
