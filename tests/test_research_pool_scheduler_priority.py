from datetime import datetime, timedelta
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

from server.api import scheduler_runtime


NOW = datetime(2026, 9, 7, 10, 0)


def research_task(**changes):
    return {
        "id": 134,
        "task_type": "trading_v3_research_pool",
        "script_path": "tools/run_trading_v3_research_pool.py",
        "script_args": "",
        "date_param": "",
        "interval_minutes": 0,
        "cron_time": "09:59",
        **changes,
    }


def ordered(rows):
    with patch.object(
        scheduler_runtime, "_release_build_catchup_allowed",
        side_effect=lambda row, **_: row.get("release_catchup", False),
    ):
        return sorted(rows, key=lambda row: scheduler_runtime._scheduler_task_sort_key(row, now=NOW))


def test_due_pool_follows_raw_repair_but_precedes_bulk_release_replays():
    repair = {"id": 116, "task_type": "linux_recent_data_gap_repair", "release_catchup": True}
    finance = {"id": 123, "task_type": "stock_finance", "release_catchup": True}
    pool = research_task()
    assert [row["id"] for row in ordered([finance, pool, repair])] == [116, 134, 123]


def test_future_or_completed_pool_is_not_promoted_to_due_work():
    repair = {"id": 116, "task_type": "linux_recent_data_gap_repair", "release_catchup": True}
    for pool in (
        research_task(cron_time="22:10"),
        research_task(last_triggered_at="2026-09-07 09:59:00", last_run_status="success"),
    ):
        assert ordered([pool, repair])[-1] is pool
        assert scheduler_runtime._scheduler_task_sort_key(pool, now=NOW)[0] == 1


def test_priority_does_not_apply_to_a_different_script_or_arguments():
    finance = {"id": 123, "task_type": "stock_finance", "release_catchup": True}
    for pool in (
        research_task(script_path="tools/other.py"),
        research_task(script_args="--from-packaged-seed 2026-09-04"),
        research_task(date_param="2026-09-04"),
    ):
        assert ordered([pool, finance])[0] is finance


def test_actual_ordinary_raw_repair_precedes_exclusive_research():
    # This task was removed from release replay. Mocking it as a release job
    # masked the production starvation at the ordinary cron boundary.
    repair = {
        "id": 116, "task_type": "linux_recent_data_gap_repair",
        "cron_time": "00:45", "interval_minutes": 0,
        "last_run_status": "failed",
        "last_triggered_at": NOW - timedelta(hours=1),
        "last_run_at": NOW - timedelta(hours=1),
        "last_run_duration": 180,
    }
    assert not scheduler_runtime._release_build_catchup_pending(repair)
    assert [row["id"] for row in ordered([research_task(), repair])] == [116, 134]


def test_notice_history_recovery_precedes_exclusive_research_and_bulk_replays():
    repair = {
        "id": 122, "task_type": "notice_eastmoney_historical_repair",
        "release_catchup": True, "interval_minutes": 5,
    }
    dividend = {"id": 121, "task_type": "stock_dividend_eastmoney", "release_catchup": True}
    assert [row["id"] for row in ordered([research_task(), dividend, repair])] == [122, 134, 121]


def test_raw_repair_priority_preserves_retry_backoff_and_future_cron():
    for repair in (
        {"id": 116, "task_type": "linux_recent_data_gap_repair", "cron_time": "22:00"},
        {"id": 116, "task_type": "linux_recent_data_gap_repair", "cron_time": "00:45",
         "last_triggered_at": NOW - timedelta(minutes=1), "last_run_status": "failed"},
        {"id": 122, "task_type": "notice_eastmoney_historical_repair", "interval_minutes": 5,
         "last_triggered_at": NOW - timedelta(minutes=1)},
    ):
        assert ordered([research_task(), repair])[0]["id"] == 134


def test_history_priority_does_not_override_unavailable_release_authorization():
    repair = {
        "id": 122, "task_type": "notice_eastmoney_historical_repair",
        "interval_minutes": 5, "last_triggered_at": NOW - timedelta(minutes=30),
    }
    with patch.object(scheduler_runtime, "_release_build_catchup_pending", return_value=True):
        assert ordered([research_task(), repair])[0]["id"] == 134


def test_research_reserves_pending_notice_slot_without_authorizing_it(monkeypatch):
    repair = {
        "id": 122, "task_type": "notice_eastmoney_historical_repair",
        "interval_minutes": 5, "last_triggered_at": NOW - timedelta(minutes=30),
        "last_run_status": "failed", "_release_catchup_authorized": False,
    }
    monkeypatch.setattr(scheduler_runtime, "_should_skip_task_for_host", lambda _: False)
    monkeypatch.setattr(scheduler_runtime, "strategy_governance_task_block_reason", lambda _: "")
    monkeypatch.setattr(scheduler_runtime, "_release_build_catchup_pending", lambda row: row.get("pending", True))
    monkeypatch.setattr(scheduler_runtime, "_release_build_catchup_allowed", lambda row, **_: row.get("allowed", False))
    pool = research_task()
    assert scheduler_runtime._research_waits_for_notice_acquisition(pool, [repair, pool], now=NOW)
    assert repair["_release_catchup_authorized"] is False
    assert not scheduler_runtime._release_build_catchup_allowed(repair, now=NOW)
    for changes in (
        {"enabled": False}, {"pending": False},
        {"last_triggered_at": NOW - timedelta(minutes=1)},
        {"_release_catchup_authorized": True},  # Genuine failure backoff permits other work.
        {"last_run_status": "blocked", "last_run_output": '{"retryable":false}'},
    ):
        assert not scheduler_runtime._research_waits_for_notice_acquisition(
            pool, [{**repair, **changes}], now=NOW,
        )
    assert scheduler_runtime._research_waits_for_notice_acquisition(
        pool, [{**repair, "_release_catchup_authorized": True, "allowed": True}], now=NOW,
    )
    assert not scheduler_runtime._research_waits_for_notice_acquisition(
        research_task(script_args="--from-packaged-seed 2026-09-04"), [repair], now=NOW,
    )


def test_real_dispatch_loop_does_not_launch_research_during_notice_authorization_gap():
    pool = {"task_name": "research", **research_task()}
    repair = {
        "id": 122, "task_name": "notice repair",
        "task_type": "notice_eastmoney_historical_repair",
        "script_path": "biz/notice/sync_notice_em.py", "script_args": "--repair-history",
        "cron_time": "00:00", "interval_minutes": 5,
        "last_triggered_at": NOW - timedelta(minutes=30), "last_run_status": "failed",
    }
    columns = sorted(set(pool) | set(repair))
    result = MagicMock()
    result.keys.return_value = columns
    result.fetchall.return_value = [tuple(row.get(key) for key in columns) for row in (pool, repair)]
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value = result
    stop = MagicMock()
    stop.is_set.return_value = False
    with ExitStack() as stack:
        replacements = {
            "get_engine": engine,
            "get_scheduler_runtime_config": {"poll_seconds": 15, "max_concurrent_tasks": 1},
            "_qmt_windows_loop_activation_ready": (True, "ready"),
            "_standalone_heartbeat_allows_dispatch": (True, {}),
            "_qmt_windows_dispatch_preflight": (True, "ready"),
            "_attach_release_catchup_authorization": (False, "activation grant pending"),
            "_release_catchup_disabled_for_deferred_database": False,
            "_release_build_catchup_allowed": False,
            "_should_skip_task_for_host": False,
            "strategy_governance_task_block_reason": "",
            "_now_shanghai_naive": NOW,
            "_wait_for_scheduler_poll": True,
        }
        for name, value in replacements.items():
            stack.enter_context(patch.object(scheduler_runtime, name, return_value=value))
        for name in (
            "_write_scheduler_heartbeat", "_cleanup_stale_running_tasks", "_maybe_cleanup_history",
            "_attach_daily_recovery_targets", "_attach_research_pool_recovery_target",
            "_attach_release_catchup_history", "_attach_release_catchup_expected_targets",
        ):
            stack.enter_context(patch.object(scheduler_runtime, name))
        stack.enter_context(patch.object(
            scheduler_runtime, "_release_build_catchup_pending",
            side_effect=lambda row: row.get("task_type") == "notice_eastmoney_historical_repair",
        ))
        hold = stack.enter_context(patch.object(
            scheduler_runtime, "_research_waits_for_notice_acquisition",
            wraps=scheduler_runtime._research_waits_for_notice_acquisition,
        ))
        claim = stack.enter_context(patch.object(scheduler_runtime, "_claim_task_run"))
        worker = stack.enter_context(patch.object(scheduler_runtime.threading, "Thread"))
        scheduler_runtime._check_and_run_tasks(mode="embedded", stop_event=stop)
    assert hold.call_count == 1
    assert hold.call_args.args[0]["id"] == 134
    claim.assert_not_called()
    worker.assert_not_called()
