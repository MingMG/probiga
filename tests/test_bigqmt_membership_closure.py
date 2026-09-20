from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import pytest

from tools import run_big_qmt_bridge as consumer
from tools import sync_bigqmt_reference as membership


TARGET = date(2026, 9, 18)
BUILD = "a" * 40


@pytest.fixture
def runtime(monkeypatch, tmp_path):
    monkeypatch.setenv("BIG_QMT_MEMBERSHIP_CAPTURE_DIR", str(tmp_path / "captures"))
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value.scalar.return_value = 1
    task = {"id": 65, "enabled": 1, "cron_time": "15:12"}
    monkeypatch.setattr(consumer, "_membership_snapshot_task", lambda _e: task)
    monkeypatch.setattr(membership, "authoritative_closed_trade_date", lambda *_a, **_k: TARGET.isoformat())
    monkeypatch.setattr(membership, "read_sql_rows", lambda *_a, **_k: [{"open_count": 1}])
    monkeypatch.setattr(consumer, "_claim_bridge_task_run", lambda *_a, **_k: True)
    update = MagicMock()
    monkeypatch.setattr(consumer, "update_scheduler_task", update)
    return engine, task, update


@pytest.mark.parametrize("now", [
    datetime(2026, 9, 19, 0, 0),
    datetime(2026, 9, 20, 12, 0),
    datetime(2026, 9, 21, 9, 0),
])
def test_late_start_verifies_existing_snapshot_without_native_capture(monkeypatch, runtime, now):
    engine, _, update = runtime
    exists = MagicMock(return_value=True)
    run = MagicMock()
    monkeypatch.setattr(consumer, "_membership_snapshot_exists", exists)
    monkeypatch.setattr(consumer, "_run_membership_snapshot", run)
    result = consumer.maybe_sync_membership_snapshot(engine, expected_build_sha=BUILD, now=now)
    assert result == {"status": "current", "snapshot_date": TARGET.isoformat()}
    exists.assert_called_once_with(engine, TARGET, decision_known_at=now)
    run.assert_not_called()
    update.assert_not_called()


def test_same_day_failure_retries_until_verified_publication(monkeypatch, runtime):
    engine, _, update = runtime
    exists = MagicMock(side_effect=[False, False, True])
    run = MagicMock(side_effect=[TimeoutError("native timeout"), {"snapshot": {"status": "created"}}])
    monkeypatch.setattr(consumer, "_membership_snapshot_exists", exists)
    monkeypatch.setattr(consumer, "_run_membership_snapshot", run)
    states = [consumer.maybe_sync_membership_snapshot(
        engine, expected_build_sha=BUILD, now=datetime(2026, 9, 18, 16, minute),
    )["status"] for minute in (0, 5, 10)]
    assert states == ["error", "success", "current"]
    assert run.call_count == 2
    assert [call.args[2]["last_run_status"] for call in update.call_args_list] == ["failed", "success"]


def test_corrupt_existing_snapshot_is_blocked_without_replacement(monkeypatch, runtime):
    engine, _, update = runtime
    exists = MagicMock(side_effect=RuntimeError("count/hash proof differs"))
    run = MagicMock()
    monkeypatch.setattr(consumer, "_membership_snapshot_exists", exists)
    monkeypatch.setattr(consumer, "_run_membership_snapshot", run)
    result = consumer.maybe_sync_membership_snapshot(
        engine, expected_build_sha=BUILD, now=datetime(2026, 9, 18, 16),
    )
    assert result["status"] == "error"
    assert "DATA_BLOCKED" in result["error"] and "count/hash" in result["error"]
    run.assert_not_called()
    assert update.call_args.args[2]["last_run_status"] == "failed"


@pytest.mark.parametrize("now", [
    datetime(2026, 9, 18, 15, 9, 59),
    datetime(2026, 9, 19, 0, 0),
    datetime(2026, 9, 20, 12, 0),
    datetime(2026, 9, 21, 9, 0),
])
def test_closed_capture_window_rejected_before_publication_dml(runtime, now):
    engine, _, _ = runtime
    with pytest.raises(RuntimeError, match="capture window is closed"):
        membership.validate_membership_publication_target(engine, snapshot_date=TARGET, now=now)
    engine.begin.assert_not_called()


@pytest.mark.parametrize("now", [
    datetime(2026, 9, 18, 15, 10),
    datetime(2026, 9, 18, 23, 59, 59),
    datetime(2026, 9, 18, 15, 59, 59, tzinfo=timezone.utc),
])
def test_capture_window_accepts_only_current_shanghai_session(runtime, now):
    engine, _, _ = runtime
    assert membership.validate_membership_publication_target(engine, snapshot_date=TARGET, now=now) == TARGET


def test_direct_capture_rejects_expired_target_before_calling_qmt(monkeypatch, runtime):
    engine, _, _ = runtime
    monkeypatch.setattr(consumer, "_assert_membership_runtime", lambda *_a: None)
    monkeypatch.setattr(membership, "_membership_decision_time", lambda _now=None: datetime(2026, 9, 19, 0, 0))
    fetch = MagicMock()
    monkeypatch.setattr(membership, "fetch_and_validate", fetch)
    with pytest.raises(RuntimeError, match="capture window is closed"):
        consumer._run_membership_snapshot(engine, TARGET, expected_build_sha=BUILD)
    fetch.assert_not_called()
    engine.begin.assert_not_called()


def test_bad_old_capture_does_not_block_today_or_other_pending_days(monkeypatch, runtime):
    engine, _, update = runtime
    old = date(2026, 9, 16)
    other = date(2026, 9, 17)
    monkeypatch.setattr(consumer, "_membership_pending_dates", lambda: [old, other])
    monkeypatch.setattr(consumer, "_membership_snapshot_exists", lambda *_a, **_k: False)
    claim = MagicMock(return_value=True)
    monkeypatch.setattr(consumer, "_claim_bridge_task_run", claim)
    calls = []
    def run(_engine, target, **_kwargs):
        update.assert_not_called()
        calls.append(target)
        if target == old:
            raise RuntimeError("DATA_BLOCKED: corrupt old source checksum")
        return {"snapshot": {"status": "created"}}
    monkeypatch.setattr(consumer, "_run_membership_snapshot", run)
    result = consumer.maybe_sync_membership_snapshot(
        engine, expected_build_sha=BUILD, now=datetime(2026, 9, 18, 16),
    )
    assert calls == [TARGET, old, other]
    assert result["status"] == "error"
    assert [item["status"] for item in result["results"]] == ["success", "error", "success"]
    claim.assert_called_once()
    update.assert_called_once()
    assert update.call_args.args[2]["last_run_status"] == "success"
    assert '"snapshot_date": "2026-09-18"' in update.call_args.args[2]["last_run_output"]
    assert '"backlog_status": "BLOCKED"' in update.call_args.args[2]["last_run_output"]


def test_invalid_pending_inventory_is_reported_after_today_capture(monkeypatch, runtime):
    engine, _, _ = runtime
    monkeypatch.setattr(consumer, "_membership_pending_dates", MagicMock(side_effect=RuntimeError("invalid filename")))
    monkeypatch.setattr(consumer, "_membership_snapshot_exists", lambda *_a, **_k: False)
    run = MagicMock(return_value={"snapshot": {"status": "created"}})
    monkeypatch.setattr(consumer, "_run_membership_snapshot", run)
    result = consumer.maybe_sync_membership_snapshot(
        engine, expected_build_sha=BUILD, now=datetime(2026, 9, 18, 16),
    )
    run.assert_called_once()
    assert result["results"][0]["status"] == "success"
    assert result["status"] == "error"


def test_failed_initial_claim_never_reclaims_old_pending_or_overwrites_new_owner(monkeypatch, runtime):
    engine, _, update = runtime
    monkeypatch.setattr(consumer, "_membership_pending_dates", lambda: [date(2026, 9, 17)])
    monkeypatch.setattr(consumer, "_membership_snapshot_exists", lambda *_a, **_k: False)
    claim = MagicMock(side_effect=[False, True])
    monkeypatch.setattr(consumer, "_claim_bridge_task_run", claim)
    run = MagicMock()
    monkeypatch.setattr(consumer, "_run_membership_snapshot", run)
    result = consumer.maybe_sync_membership_snapshot(
        engine, expected_build_sha=BUILD, now=datetime(2026, 9, 18, 16),
    )
    assert result["status"] == "already_running"
    claim.assert_called_once()
    run.assert_not_called()
    update.assert_not_called()
