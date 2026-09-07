from datetime import datetime
from unittest.mock import patch

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
