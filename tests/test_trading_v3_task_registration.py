from datetime import date, datetime

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
    assert daily_tasks["trading_v3_premarket_review"]["cron_time"] == "09:26"


def test_daily_bar_loader_streams_rows_into_frame(monkeypatch) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE sm_stock_kline (
                    stock_code TEXT, short_name TEXT, trade_date DATE,
                    k_type INTEGER, adjust_type INTEGER,
                    open REAL, close REAL, high REAL,
                    low REAL, pre_close REAL, amount REAL,
                    change_pct REAL, received_at DATETIME
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO sm_stock_kline VALUES
                ('002240', '盛新锂能', '2026-08-10', 1, 0,
                 10, 10.2, 10.3, 9.9, 10, 100000000, 2, '2026-08-10 16:00:00'),
                ('002240', '盛新锂能', '2026-08-11', 1, 0,
                 10.2, 10.5, 10.6, 10.1, 10.2, 120000000, 2.94, '2026-08-11 16:00:00')
                """
            )
        )
        connection.execute(text("CREATE TABLE qmt_stock_catalog_member (batch_id TEXT, stock_code TEXT, instrument_type TEXT, list_date TEXT, expire_date TEXT)"))
        connection.execute(text("INSERT INTO qmt_stock_catalog_member VALUES ('catalog','002240','STOCK','2020-01-01',NULL)"))
    proof = {
        "sessions": ["2026-08-10", "2026-08-11"],
        "catalog_batches_by_session": {day: "catalog" for day in ("2026-08-10", "2026-08-11")},
        "decision_known_at": "2026-08-11 18:00:00",
    }
    monkeypatch.setattr("server.trading_v3.daily_features.validate_daily_input_sessions", lambda *_args, **_kwargs: proof)

    frame = _load_bars(
        engine,
        dates=[date(2026, 8, 10), date(2026, 8, 11)],
        decision_known_at=datetime(2026, 8, 11, 18),
    )

    assert len(frame) == 2
    assert set(frame["stock_code"]) == {"002240"}
    assert frame.iloc[-1]["short_name"] == "盛新锂能"
    engine.dispose()
