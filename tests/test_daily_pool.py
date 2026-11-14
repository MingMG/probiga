from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from sqlalchemy import create_engine, text

from server.api import scheduler_runtime
from tools import run_daily_pool
from tools.run_daily_pool import limit_pct_for, select_daily_pool


def _row(code: str, name: str, *, change: float, amount: float,
         close: float = 10.0, high: float = 10.2, low: float = 9.6):
    return {
        "stock_code": code,
        "stock_name": name,
        "close": close,
        "high": high,
        "low": low,
        "change_pct": change,
        "amount": amount,
    }


def test_selects_liquid_strong_close_and_excludes_st():
    rows = [
        _row("600001", "强势股份", change=4.0, amount=800_000_000),
        _row("600002", "ST风险", change=4.0, amount=900_000_000),
        _row("600003", "弱收盘", change=4.0, amount=900_000_000,
             close=9.7),
        _row("600004", "低成交", change=4.0, amount=100_000_000),
        _row("600005", "下跌", change=-1.0, amount=900_000_000),
    ]
    pool = select_daily_pool(rows, {}, top=10)
    assert [item["stock_code"] for item in pool] == ["600001"]


def test_board_specific_limit_allows_growth_board_but_not_main_board():
    rows = [
        _row("300001", "创业板强势", change=12.0, amount=800_000_000),
        _row("600001", "主板过热", change=12.0, amount=900_000_000),
    ]
    pool = select_daily_pool(rows, {}, top=10)
    assert [item["stock_code"] for item in pool] == ["300001"]
    assert limit_pct_for("300001") == 19.5
    assert limit_pct_for("600001") == 9.5


def test_negative_main_flow_is_excluded_and_positive_flow_ranks_first():
    rows = [
        _row("600001", "流入", change=4.0, amount=800_000_000),
        _row("600002", "流出", change=4.0, amount=900_000_000),
    ]
    pool = select_daily_pool(
        rows, {"600001": 300_000_000.0, "600002": -1.0}, top=10
    )
    assert [item["stock_code"] for item in pool] == ["600001"]


def test_ranking_is_deterministic_for_equal_scores():
    rows = [
        _row("600002", "乙", change=4.0, amount=800_000_000),
        _row("600001", "甲", change=4.0, amount=800_000_000),
    ]
    pool = select_daily_pool(rows, {}, top=10)
    assert [item["stock_code"] for item in pool] == ["600001", "600002"]

def test_scheduled_pool_waits_for_its_closed_session_without_overwriting_prior_pool(monkeypatch):
    db = create_engine("sqlite://")
    with db.begin() as connection:
        connection.exec_driver_sql("CREATE TABLE si_trade_calendar (trade_date TEXT, trade_status INT)")
        connection.exec_driver_sql(
            "INSERT INTO si_trade_calendar VALUES ('2026-11-12', 1), ('2026-11-13', 1), ('2026-11-14', 0)"
        )
        connection.exec_driver_sql(
            "CREATE TABLE sm_stock_kline (trade_date TEXT, stock_code TEXT, short_name TEXT, "
            "k_type INT, adjust_type INT, close REAL, change_pct REAL, amount REAL, "
            "high REAL, low REAL, volume REAL)"
        )
        connection.exec_driver_sql(
            "CREATE TABLE sm_stock_capital_flow_daily (trade_date TEXT, stock_code TEXT, main_net_inflow REAL)"
        )
        connection.exec_driver_sql(
            "CREATE TABLE st_screener_candidate_pool (as_of_date TEXT, source TEXT, status TEXT, "
            "stock_code TEXT, stock_name TEXT, screen_name TEXT, score REAL, reason TEXT, payload_json TEXT)"
        )
        connection.exec_driver_sql(
            "INSERT INTO st_screener_candidate_pool VALUES "
            "('2026-11-12', 'daily_pool', 'PUBLISHED', '600001', 'prior pool', '', 0, '', '{}')"
        )
        connection.execute(
            text("INSERT INTO sm_stock_kline VALUES "
                 "(:day, '600001', 'candidate', 1, 0, 10, 4, 800000000, 10.2, 9.6, 5000)"),
            {"day": "2026-11-12"},
        )

    monkeypatch.setattr(run_daily_pool, "MIN_SESSION_ROWS", 1)
    monkeypatch.setattr(run_daily_pool, "create_batch_engine", lambda: SimpleNamespace(
        begin=db.begin, dispose=lambda: None,
    ))
    task = {"task_type": "daily_pool", "script_args": "--json"}
    script_path = "tools/run_daily_pool.py"
    scheduled_at = datetime(2026, 11, 13, 18, 31)
    target = scheduler_runtime._task_dispatch_date(task, db, now=scheduled_at)
    argument_row = scheduler_runtime._task_argument_row(task, now=scheduled_at, target_date=target, engine=db)
    cli_args = scheduler_runtime._build_task_args(argument_row, script_path, target)
    assert cli_args == ["--json", "--date", "2026-11-13"]
    assert scheduler_runtime._task_dispatch_date(task, db, now=datetime(2026, 11, 14, 8)) == target

    args = SimpleNamespace(install_task=False, date=cli_args[-1], top=30, dry_run=False)
    result = run_daily_pool.run(args)
    assert result["status"] == "blocked"
    assert result["stage"] == "daily_kline"
    assert result["target_trade_date"] == "2026-11-13"

    with db.begin() as connection:
        assert connection.exec_driver_sql(
            "SELECT stock_name FROM st_screener_candidate_pool"
        ).scalars().all() == ["prior pool"]
        connection.exec_driver_sql(
            "INSERT INTO sm_stock_kline SELECT '2026-11-13', stock_code, short_name, k_type, adjust_type, "
            "close, change_pct, amount, high, low, volume FROM sm_stock_kline WHERE trade_date='2026-11-12'"
        )

    result = run_daily_pool.run(args)
    assert result["status"] == "ok"
    assert result["pool_date"] == result["data_date"] == "2026-11-13"
    assert result["candidate_count"] == 1
    with db.connect() as connection:
        assert connection.exec_driver_sql(
            "SELECT as_of_date, stock_name FROM st_screener_candidate_pool ORDER BY as_of_date"
        ).all() == [("2026-11-12", "prior pool"), ("2026-11-13", "candidate")]
