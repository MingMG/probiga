from datetime import date

from sqlalchemy import create_engine, text

from server.trading_v3.daily_features import _load_bars
from tools.add_trading_v3_tasks import TASKS
from tools.run_trading_v3_decision import (
    DEFAULT_PER_SLEEVE_LIMIT,
    DEFAULT_UNIVERSE_LIMIT,
)


def test_v3_daily_tasks_use_bounded_production_universe() -> None:
    daily_tasks = {
        item["task_type"]: item
        for item in TASKS
        if item["script_path"] == "tools/run_trading_v3_decision.py"
    }

    assert set(daily_tasks) == {
        "trading_v3_close_decision",
        "trading_v3_premarket_review",
    }
    for task in daily_tasks.values():
        assert (
            f"--universe-limit {DEFAULT_UNIVERSE_LIMIT}"
            in task["script_args"]
        )
        assert (
            f"--per-sleeve-limit {DEFAULT_PER_SLEEVE_LIMIT}"
            in task["script_args"]
        )
    assert daily_tasks["trading_v3_close_decision"]["cron_time"] == "22:05"


def test_daily_bar_loader_streams_rows_into_frame() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE sm_stock_kline (
                    stock_code TEXT, short_name TEXT, trade_date DATE,
                    k_type INTEGER, open REAL, close REAL, high REAL,
                    low REAL, pre_close REAL, amount REAL,
                    change_pct REAL
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO sm_stock_kline VALUES
                ('002240', '盛新锂能', '2026-08-10', 1,
                 10, 10.2, 10.3, 9.9, 10, 100000000, 2),
                ('002240', '盛新锂能', '2026-08-11', 1,
                 10.2, 10.5, 10.6, 10.1, 10.2, 120000000, 2.94)
                """
            )
        )

    frame = _load_bars(
        engine,
        dates=[date(2026, 8, 10), date(2026, 8, 11)],
    )

    assert len(frame) == 2
    assert set(frame["stock_code"]) == {"002240"}
    assert frame.iloc[-1]["short_name"] == "盛新锂能"
    engine.dispose()
