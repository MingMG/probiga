"""Stock minutes select today's close without relabeling other data products."""

from datetime import datetime
import json
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import create_engine, text

from server.api import scheduler_runtime as runtime
from server.common import authoritative_market_clock as market_clock
from server.common.qmt_daily_market_truth import QMT_DAILY_CAPTURE_READY_TIME
from tools.qmt_host_ownership_contract import (
    QMT_INDEX_MINUTE_TASK,
    QMT_STOCK_MINUTE_CANONICAL_TASK,
)


@pytest.fixture
def calendar_engine():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE si_trade_calendar (trade_date TEXT, trade_status INTEGER)"))
        connection.execute(text("INSERT INTO si_trade_calendar VALUES ('2026-09-18',1),('2026-09-19',0),('2026-09-20',0),('2026-09-21',1),('2026-09-22',0)"))
    yield engine
    engine.dispose()


@pytest.mark.parametrize(("now", "target"), [
    (datetime(2026, 9, 21, 15, 34, 59), "2026-09-18"),
    (datetime(2026, 9, 21, 15, 35), "2026-09-21"),
    (datetime(2026, 9, 21, 7, 35, tzinfo=ZoneInfo("UTC")), "2026-09-21"),
    (datetime(2026, 9, 22, 0, 5), "2026-09-21"),
    (datetime(2026, 9, 22, 17), "2026-09-21"),
    (datetime(2026, 9, 20, 17), "2026-09-18"),
])
def test_stock_minute_candidate_uses_shanghai_close_boundary(calendar_engine, now, target):
    assert market_clock.STOCK_MINUTE_CAPTURE_READY_TIME == QMT_DAILY_CAPTURE_READY_TIME
    assert market_clock.authoritative_stock_minute_trade_date(calendar_engine, now=now) == target


def test_stock_minute_dispatch_closes_today_while_index_retains_own_contract(calendar_engine):
    now = datetime(2026, 9, 21, 16)
    stock = dict(QMT_STOCK_MINUTE_CANONICAL_TASK)
    index = dict(QMT_INDEX_MINUTE_TASK)

    assert runtime._attach_daily_recovery_targets(calendar_engine, [stock, index], now=now)
    assert stock["_scheduler_target_trade_date"] == "2026-09-21"
    assert stock["_scheduler_historical_recovery"] is False
    assert index["_scheduler_target_trade_date"] == "2026-09-18"
    assert index["_scheduler_historical_recovery"] is True
    assert runtime._task_dispatch_date(dict(QMT_STOCK_MINUTE_CANONICAL_TASK), calendar_engine, now=now) == "2026-09-21"
    assert runtime._task_dispatch_date(dict(QMT_INDEX_MINUTE_TASK), calendar_engine, now=now) == "2026-09-18"


def test_prior_session_success_does_not_consume_today_postclose_run(calendar_engine):
    now = datetime(2026, 9, 21, 16)
    row = {
        **QMT_STOCK_MINUTE_CANONICAL_TASK,
        "last_triggered_at": datetime(2026, 9, 21, 8),
        "last_run_at": datetime(2026, 9, 21, 8),
        "last_run_status": "success",
        "last_run_output": json.dumps({"trade_date": "2026-09-18"}),
    }

    assert runtime._attach_daily_recovery_targets(calendar_engine, [row], now=now)
    assert runtime._cron_due(row, now=now)
    assert runtime._overdue_cron_allowed(
        row, now=now, cron_time=row["cron_time"],
        startup_time=datetime(2026, 9, 20, 8),
    )
    args = runtime._build_task_args(row, row["script_path"], "2026-09-21")
    assert "--latest-session" not in args
    assert args[-4:] == ["--start-date", "2026-09-21", "--end-date", "2026-09-21"]
    # The source publisher's verified success still consumes this exact day.
    row.update(last_triggered_at=now, last_run_at=now,
               last_run_output=json.dumps({"trade_date": "2026-09-21"}))
    assert not runtime._cron_due(row, now=datetime(2026, 9, 21, 18))


@pytest.mark.parametrize(("now", "invalid_target"), [
    (datetime(2026, 9, 21, 15, 34), "2026-09-21"),
    (datetime(2026, 9, 21, 16), "2026-09-22"),
    (datetime(2026, 9, 21, 16), ""),
    (datetime(2026, 9, 21, 16), "2026-9-21"),
])
def test_invalid_minute_clock_never_claims_a_partition(monkeypatch, now, invalid_target):
    monkeypatch.setattr(runtime, "authoritative_stock_minute_trade_date", lambda *_args, **_kwargs: invalid_target)
    row = dict(QMT_STOCK_MINUTE_CANONICAL_TASK)

    assert not runtime._attach_daily_recovery_targets(object(), [row], now=now)
    assert row["_scheduler_target_available"] is False
    with pytest.raises(runtime.ReleaseCatchupDataBlocked, match="stock minute closed-session authority"):
        runtime._task_dispatch_date(row, object(), now=now)
