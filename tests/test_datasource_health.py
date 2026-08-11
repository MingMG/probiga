from datetime import datetime
from unittest.mock import patch

from server.api.routers import datasource


def test_required_task_health_marks_missing_tasks():
    with patch("server.api.routers.datasource._read_sql", return_value=[]):
        health = datasource._required_task_health(now=datetime(2026, 6, 28, 10, 0, 0))

    assert [item["status"] for item in health] == ["missing_task", "missing_task", "missing_task"]
    assert all(item["configured"] is False for item in health)


def test_latest_required_trade_date_waits_until_task_ready_time():
    with patch("server.api.routers.datasource._read_sql", return_value=[{"latest_trade_date": "2026-07-08"}]) as read_sql:
        target = datasource._latest_required_trade_date(
            datetime(2026, 7, 9, 0, 30, 0),
            ready_time="17:30",
        )

    assert target.isoformat() == "2026-07-08"
    assert "trade_date < :today" in read_sql.call_args.args[0]


def test_required_task_health_ok_when_task_and_latest_data_are_present():
    tasks = [
        {
            "id": 1,
            "task_name": "新浪热股TOP100",
            "task_type": "hot_rank_sina",
            "script_path": "tools/fetch_hot_rank_sina.py",
            "script_args": "",
            "enabled": 1,
            "last_run_status": "success",
            "last_run_at": "2026-06-28 09:30:00",
            "last_run_duration": 12,
            "last_run_output": "ok",
        },
        {
            "id": 2,
            "task_name": "个股资金流向(全量)",
            "task_type": "capital_flow",
            "script_path": "tools/run_single_table.py",
            "script_args": "sm_stock_capital_flow_daily",
            "enabled": 1,
            "last_run_status": "success",
            "last_run_at": "2026-06-28 17:30:00",
            "last_run_duration": 30,
            "last_run_output": "ok",
        },
        {
            "id": 3,
            "task_name": "概念资金流向",
            "task_type": "concept_flow",
            "script_path": "tools/run_single_table.py",
            "script_args": "sm_concept_capital_flow_east",
            "enabled": 1,
            "last_run_status": "success",
            "last_run_at": "2026-06-28 19:30:00",
            "last_run_duration": 8,
            "last_run_output": "ok",
        },
    ]

    def freshness(table, date_col):
        return {
            "table_exists": True,
            "max_data_time": "2026-06-28 15:00:00",
            "row_count_latest": 500 if table == "sm_stock_capital_flow_daily" else 20,
        }

    with patch("server.api.routers.datasource._read_sql", return_value=tasks), \
         patch("server.api.routers.datasource._table_freshness", side_effect=freshness):
        health = datasource._required_task_health(now=datetime(2026, 6, 28, 20, 0, 0))

    assert [item["status"] for item in health] == ["ok", "ok", "ok"]
    assert health[1]["row_count_latest"] == 500


def test_required_task_health_surfaces_failed_task_before_data_age():
    tasks = [{
        "id": 2,
        "task_name": "个股资金流向(全量)",
        "task_type": "capital_flow",
        "script_path": "tools/run_single_table.py",
        "script_args": "sm_stock_capital_flow_daily",
        "enabled": 1,
        "last_run_status": "failed",
        "last_run_at": "2026-06-28 17:30:00",
        "last_run_duration": 3,
        "last_run_output": "boom",
    }]

    with patch("server.api.routers.datasource._read_sql", return_value=tasks), \
         patch("server.api.routers.datasource._table_freshness", return_value={
             "table_exists": True,
             "max_data_time": "2026-06-28 15:00:00",
             "row_count_latest": 500,
         }):
        health = datasource._required_task_health(now=datetime(2026, 6, 28, 20, 0, 0))

    capital = next(item for item in health if item["task_type"] == "capital_flow")
    assert capital["status"] == "failed"
    assert capital["message"] == "最近一次执行失败"


def test_required_task_health_accepts_fast_capital_flow_task():
    tasks = [
        {
            "id": 2,
            "task_name": "个股资金流向(全量)",
            "task_type": "capital_flow",
            "script_path": "tools/run_single_table.py",
            "script_args": "sm_stock_capital_flow_daily",
            "enabled": 1,
            "last_run_status": "failed",
            "last_run_at": "2026-06-28 17:30:00",
            "last_run_duration": 3,
            "last_run_output": "qmt empty",
        },
        {
            "id": 22,
            "task_name": "盘后快速资金流同步",
            "task_type": "capital_flow_batch_fast",
            "script_path": "tools/crawl_realtime_batch.py",
            "script_args": "--only flow --min-coverage 0.70 --json",
            "enabled": 1,
            "last_run_status": "success",
            "last_run_at": "2026-06-28 17:03:00",
            "last_run_duration": 21,
            "last_run_output": "ok",
        },
    ]

    def freshness(table, date_col):
        return {
            "table_exists": True,
            "max_data_time": "2026-06-28 15:00:00",
            "row_count_latest": 500,
        }

    with patch("server.api.routers.datasource._read_sql", return_value=tasks), \
         patch("server.api.routers.datasource._table_freshness", side_effect=freshness):
        health = datasource._required_task_health(now=datetime(2026, 6, 28, 20, 0, 0))

    capital = next(item for item in health if item["task_type"] == "capital_flow")
    assert capital["status"] == "ok"
    assert capital["selected_task_type"] == "capital_flow_batch_fast"


def test_required_task_health_requires_latest_trade_date():
    tasks = [{
        "id": 2,
        "task_name": "个股资金流向(全量)",
        "task_type": "capital_flow",
        "script_path": "tools/crawl_realtime_batch.py",
        "script_args": "--only flow --min-coverage 0.70 --json",
        "enabled": 1,
        "last_run_status": "success",
        "last_run_at": "2026-06-28 17:30:00",
        "last_run_duration": 3,
        "last_run_output": "ok",
    }]

    with patch("server.api.routers.datasource._read_sql", return_value=tasks), \
         patch("server.api.routers.datasource._table_freshness", return_value={
             "table_exists": True,
             "max_data_time": "2026-06-27 15:00:00",
             "row_count_latest": 500,
         }):
        health = datasource._required_task_health(now=datetime(2026, 6, 28, 20, 0, 0))

    capital = next(item for item in health if item["task_type"] == "capital_flow")
    assert capital["status"] == "stale_target_date"
    assert "2026-06-28" in capital["message"]


def test_get_stats_includes_required_health():
    stats_row = {
        "total": 10,
        "success": 7,
        "failed": 1,
        "running": 1,
        "pending": 1,
        "enabled": 9,
        "disabled": 1,
    }

    with patch("server.api.routers.datasource._read_sql", return_value=[stats_row]), \
         patch("server.api.routers.datasource._required_task_health_cached", return_value=([{"status": "ok"}], False, 0)):
        stats = datasource.get_stats()

    assert stats["total"] == 10
    assert stats["required_health"] == [{"status": "ok"}]


def test_get_required_health_uses_cache_unless_forced():
    datasource._required_health_cache = None
    with patch(
        "server.api.routers.datasource._required_task_health",
        side_effect=[
            [{"status": "ok", "label": "first"}],
            [{"status": "failed", "label": "forced"}],
        ],
    ) as health:
        first = datasource.get_required_health()
        second = datasource.get_required_health()
        forced = datasource.get_required_health(force=True)

    assert first["cached"] is False
    assert second["cached"] is True
    assert second["data"][0]["label"] == "first"
    assert forced["cached"] is False
    assert forced["bad_count"] == 1
    assert health.call_count == 2
    datasource._required_health_cache = None
