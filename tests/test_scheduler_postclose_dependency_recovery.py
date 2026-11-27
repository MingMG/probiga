"""Ordinary post-close dependency recovery must pass both dispatch gates."""

from datetime import datetime, timedelta
import json

import pytest

from server.api import scheduler_runtime as runtime
from server.common.qmt_attestation_contract import canonical_digest
from server.common.qmt_datadir_result import build_unavailable_result
from tools.qmt_host_ownership_contract import QMT_STOCK_DAILY_CANONICAL_TASK


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


def _partial_daily_attempt(target="2026-11-27"):
    payload = build_unavailable_result(dataset="stock_daily", sessions=[target], build_sha="3"*40,
                                       reason=f"QMT_DAILY_COVERAGE_INCOMPLETE target={target} unresolved=2")
    payload["session_results"] = [{
        "session": target, "status": "BLOCKED", "run_id": "qdd_20261127234017_b9c89d5a",
        "verified_code_count": 5580,
        "unresolved_codes": dict.fromkeys(("301661", "600381"), "OFFICIAL_LISTING_EVIDENCE_FAILED: stage=worker"),
    }]
    return payload


def _daily_row(monkeypatch, now, payload, *, finished, target="2026-11-27", corrupt=False, enabled=1):
    payload.pop("receipt_id", None)
    payload["receipt_id"] = "a"*64 if corrupt else canonical_digest(payload)
    out = json.dumps(payload)
    row = {**QMT_STOCK_DAILY_CANONICAL_TASK, "enabled": enabled, "last_run_status": "blocked",
           "last_triggered_at": finished - timedelta(seconds=52), "last_run_at": finished - timedelta(seconds=52),
           "last_run_duration": 52, "last_run_output": out + "\n---MACHINE-RECEIPTS---\n" + out}
    monkeypatch.setattr(runtime, "authoritative_closed_trade_date", lambda *a, **kw: target)
    runtime._attach_daily_recovery_targets(None, [row], now=now)
    return row


@pytest.mark.parametrize(("finished", "now", "expected"), [
    ("2026-11-27 16:10:09", "2026-11-27 16:25:08", False),
    ("2026-11-27 16:10:09", "2026-11-27 16:25:09", True),
    ("2026-11-27 23:55:00", "2026-11-28 00:08:00", False),
    ("2026-11-27 23:40:56", "2026-11-28 00:08:00", True),
])
def test_ordinary_daily_partial_resumes_through_existing_backoff(monkeypatch, finished, now, expected):
    from server.common.scheduler_args import build_scheduler_task_args

    monkeypatch.setattr(runtime, "CRON_RETRY_INTERVAL_MINUTES", 15)
    now = datetime.fromisoformat(now)
    row = _daily_row(monkeypatch, now, _partial_daily_attempt(), finished=datetime.fromisoformat(finished))
    assert _dispatch_gates(row, now=now) == (expected, expected)
    assert build_scheduler_task_args(row, row["script_path"], row["_scheduler_target_trade_date"])[-4:] == [
        "--start", "2026-11-27", "--end", "2026-11-27",
    ]


@pytest.mark.parametrize("case", ["tampered", "other_day", "other_task", "other_provider", "nonretryable",
                                  "zero_build", "no_checkpoint", "wrong_inner_day", "no_gaps", "disabled", "expired"])
def test_daily_partial_rejects_unrelated_or_unverified_receipts(monkeypatch, case):
    now = datetime(2026, 11, 28, 0, 8) if case != "expired" else datetime(2026, 12, 6, 0, 8)
    payload = _partial_daily_attempt(target="2026-11-26" if case == "other_day" else "2026-11-27")
    if case == "other_task":
        payload["task_type"] = "qmt_stock_minute_canonical"
        payload["dataset"] = "stock_minute"
    if case == "other_provider":
        payload["provider"] = "another"
    if case == "nonretryable":
        payload["retryable"] = False
    if case == "zero_build":
        payload["build_sha"] = "0"*40
    if case == "no_checkpoint":
        del payload["session_results"]
    if case == "wrong_inner_day":
        payload["session_results"][0]["session"] = "2026-11-26"
    if case == "no_gaps":
        payload["session_results"][0]["unresolved_codes"] = {}
    row = _daily_row(monkeypatch, now, payload, finished=datetime(2026, 11, 27, 23, 40, 56),
                     corrupt=case == "tampered", enabled=int(case != "disabled"))
    assert not runtime._task_status_is_retryable(row)
