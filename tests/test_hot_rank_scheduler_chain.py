from __future__ import annotations

from datetime import datetime

import pytest

from server.api import scheduler_runtime
from tools import ensure_quality_gate


HOT_PIPELINE = (
    ("hot_concept", "17:10"),
    ("hot_rank_ths", "17:12"),
    ("hot_pop_east", "17:14"),
    ("hot_fused", "17:20"),
    ("hot_fused_3", "17:22"),
    ("hot_fused_5", "17:24"),
)


def test_release_installs_exact_hot_and_daily_derived_task_contracts() -> None:
    expected = {
        "hot_concept": ("17:10", "tools/fetch_hot_concept_ths_daily.py", ""),
        "hot_rank_ths": ("17:12", "tools/fetch_hot_rank_ths.py", ""),
        "hot_pop_east": ("17:14", "tools/fetch_hot_pop_rank_east.py", ""),
        "hot_fused": ("17:20", "tools/merge_hot_rank.py", "--top 100"),
        "hot_fused_3": (
            "17:22", "tools/merge_hot_rank.py", "--top 100 --days 3"
        ),
        "hot_fused_5": (
            "17:24", "tools/merge_hot_rank.py", "--top 100 --days 5"
        ),
        "market_overview_daily": (
            "18:20", "tools/refresh_market_overview_daily.py", ""
        ),
        "stock_snapshot_daily": (
            "18:25", "biz/stock_market/sync_stock_snapshot.py", ""
        ),
    }
    installed = {
        str(task["task_type"]): (
            task["cron_time"], task["script_path"], task["script_args"]
        )
        for task in ensure_quality_gate.TASKS
        if task["task_type"] in expected
    }
    assert installed == expected
    assert all(
        int(task["enabled"]) == 1 and int(task["interval_minutes"]) == 0
        for task in ensure_quality_gate.TASKS
        if task["task_type"] in expected
    )
    sina = next(
        task
        for task in ensure_quality_gate.TASKS
        if task["task_type"] == "hot_rank_sina"
    )
    assert int(sina["enabled"]) == 0
    assert "不参与融合或发布门禁" in sina["description"]


def _task(task_id: int, task_type: str, cron_time: str) -> dict:
    return {
        "id": task_id,
        "task_type": task_type,
        "cron_time": cron_time,
        "interval_minutes": 0,
        "enabled": 1,
        "last_triggered_at": "2026-08-25 17:24:00",
        "last_run_status": "success",
    }


def _completed(task_type: str, at: str, status: str = "success") -> dict:
    return {
        "task_type": task_type,
        "enabled": 1,
        "last_triggered_at": at,
        "last_run_status": status,
    }


def test_busy_worker_can_catch_up_whole_hot_pipeline_in_source_order() -> None:
    """Missing the former three-minute window must not drop this chain."""

    now = datetime(2026, 8, 26, 20, 35)
    rows = [
        _task(index, task_type, cron_time)
        for index, (task_type, cron_time) in enumerate(HOT_PIPELINE, start=1)
    ]

    for row in rows:
        assert scheduler_runtime._critical_cron_catchup_allowed(
            row,
            now=now,
            cron_time=row["cron_time"],
        )

    ordered = sorted(
        rows,
        key=lambda row: scheduler_runtime._scheduler_task_sort_key(row, now=now),
    )
    assert [row["task_type"] for row in ordered] == [
        task_type for task_type, _cron_time in HOT_PIPELINE
    ]


def test_hot_pipeline_catchup_remains_same_day_and_bounded() -> None:
    row = _task(1, "hot_fused", "17:20")

    assert not scheduler_runtime._critical_cron_catchup_allowed(
        row,
        now=datetime(2026, 8, 27, 1, 30),
        cron_time="17:20",
    )
    assert not scheduler_runtime._critical_cron_catchup_allowed(
        row,
        now=datetime(2026, 8, 27, 3, 21),
        cron_time="17:20",
    )


def test_daily_fusion_requires_every_frozen_source_to_succeed_today() -> None:
    now = datetime(2026, 8, 26, 20, 35)
    rows = [
        _completed("hot_rank_ths", "2026-08-26 20:30:00"),
        _completed("hot_pop_east", "2026-08-26 20:31:00"),
    ]

    assert scheduler_runtime.evaluate_hot_rank_pipeline_dependencies(
        "hot_fused", rows, now=now
    ) == (True, "ready")

    for bad_status in ("failed", "blocked", "running", ""):
        failed_rows = [dict(item) for item in rows]
        failed_rows[1]["last_run_status"] = bad_status
        assert scheduler_runtime.evaluate_hot_rank_pipeline_dependencies(
            "hot_fused", failed_rows, now=now
        ) == (False, "hot_pop_east:not_success_today")

    stale_rows = [dict(item) for item in rows]
    stale_rows[0]["last_triggered_at"] = "2026-08-25 20:30:00"
    assert scheduler_runtime.evaluate_hot_rank_pipeline_dependencies(
        "hot_fused", stale_rows, now=now
    ) == (False, "hot_rank_ths:not_run_today")


@pytest.mark.parametrize(
    ("task_type", "dependency"),
    (("hot_fused_3", "hot_fused"), ("hot_fused_5", "hot_fused_3")),
)
def test_multiday_fusion_waits_for_previous_stage_today(
    task_type: str,
    dependency: str,
) -> None:
    now = datetime(2026, 8, 26, 20, 35)
    assert scheduler_runtime.evaluate_hot_rank_pipeline_dependencies(
        task_type,
        [_completed(dependency, "2026-08-26 20:34:00")],
        now=now,
    ) == (True, "ready")
    assert scheduler_runtime.evaluate_hot_rank_pipeline_dependencies(
        task_type,
        [],
        now=now,
    ) == (False, f"{dependency}:missing_or_duplicate")


def test_fusion_is_deferred_when_it_preceded_a_late_source_retry() -> None:
    now = datetime(2026, 8, 26, 20, 35)
    rows = [
        _completed("hot_rank_ths", "2026-08-26 20:30:00"),
        _completed("hot_pop_east", "2026-08-26 20:31:00"),
        _completed("hot_fused", "2026-08-26 20:20:00"),
    ]

    assert scheduler_runtime.evaluate_hot_rank_pipeline_dependencies(
        "hot_fused", rows, now=now
    ) == (False, "hot_fused:ran_before_dependency")
