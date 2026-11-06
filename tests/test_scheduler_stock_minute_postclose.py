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
    PUBLIC_STOCK_MINUTE_TASK,
    PUBLIC_STOCK_MINUTE_FLOW_TASK,
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


@pytest.mark.parametrize("task", [
    PUBLIC_STOCK_MINUTE_TASK,
    PUBLIC_STOCK_MINUTE_FLOW_TASK,
])
def test_public_minute_tasks_roll_over_to_the_last_closed_session(
    calendar_engine,
    task,
):
    assert runtime._task_dispatch_date(
        dict(task),
        calendar_engine,
        now=datetime(2026, 9, 22, 0, 5),
    ) == "2026-09-21"


def test_degraded_public_flow_resumes_after_midnight_with_completion_backoff(calendar_engine):
    with calendar_engine.begin() as connection:
        connection.execute(text("INSERT INTO si_trade_calendar VALUES ('2026-11-02',1),('2026-11-03',1)"))
    row = {
        **PUBLIC_STOCK_MINUTE_FLOW_TASK,
        "id": 73,
        "last_triggered_at": datetime(2026, 11, 2, 19, 49),
        "last_run_at": datetime(2026, 11, 2, 19, 49),
        "last_run_duration": 8 * 60 * 60,
        "last_run_status": "degraded",
        "last_run_output": json.dumps({"trade_date": "2026-11-02"}),
    }
    for now, due in ((datetime(2026, 11, 3, 3, 50), False),
                     (datetime(2026, 11, 3, 4, 4), True)):
        assert runtime._attach_daily_recovery_targets(calendar_engine, [row], now=now)
        assert row["_scheduler_target_trade_date"] == "2026-11-02"
        assert row["_scheduler_historical_recovery"] is True
        assert runtime._cron_due(row, now=now) is due
        assert runtime._overdue_cron_allowed(
            row, now=now, cron_time=row["cron_time"],
            startup_time=datetime(2026, 11, 3, 3, 45),
        ) is due
    args = runtime._build_task_args(row, row["script_path"], "2026-11-02")
    assert args[-2:] == ["--trade-date", "2026-11-02"]
    # A verified complete result consumes the same session across restarts.
    row.update(last_triggered_at=datetime(2026, 11, 3, 4, 4),
               last_run_at=datetime(2026, 11, 3, 4, 4), last_run_duration=60,
               last_run_status="success")
    now = datetime(2026, 11, 3, 6)
    assert runtime._attach_daily_recovery_targets(calendar_engine, [row], now=now)
    assert not runtime._cron_due(row, now=now)


def test_public_minute_recovery_uses_latest_close_without_old_delivery_dependency(calendar_engine, monkeypatch):
    rows = [dict(PUBLIC_STOCK_MINUTE_TASK), dict(PUBLIC_STOCK_MINUTE_FLOW_TASK)]
    monkeypatch.setattr(runtime, "_daily_result_recovery_target",
                        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unrelated delivery")))
    assert runtime._attach_daily_recovery_targets(
        calendar_engine, rows, now=datetime(2026, 9, 22, 0, 5),
    )
    assert all(row["_scheduler_target_trade_date"] == "2026-09-21" for row in rows)


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
    assert args[-4:] == ["--start", "2026-09-21", "--end", "2026-09-21"]
    # The source publisher's verified success still consumes this exact day.
    row.update(last_triggered_at=now, last_run_at=now,
               last_run_output=json.dumps({"trade_date": "2026-09-21"}))
    assert not runtime._cron_due(row, now=datetime(2026, 9, 21, 18))


@pytest.mark.parametrize(("new_proof", "due"), [
    (("b" * 64, "2026-09-21 18:00:00"), True),
    (("a" * 64, "2026-09-21 18:00:00"), False),
    (("b" * 64, "2026-09-21 21:00:00"), False),
    (None, False),
])
def test_successful_stock_minutes_reopen_only_for_a_visible_daily_scope(
    calendar_engine, new_proof, due,
):
    target = "2026-09-21"
    now = datetime(2026, 9, 21, 20)
    previous_start = datetime(2026, 9, 21, 15, 55)
    row = {
        **QMT_STOCK_MINUTE_CANONICAL_TASK,
        "last_triggered_at": previous_start,
        "last_run_at": previous_start,
        "last_run_status": "success",
        "last_run_output": json.dumps({
            "schema": "probiga.qmt-datadir-import-result.v1", "target_trade_date": target,
        }),
    }
    with calendar_engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE qmt_datadir_import_partition (
                dataset TEXT, trade_date TEXT, source_root TEXT, status TEXT,
                code_count INTEGER, row_count INTEGER, content_sha256 TEXT,
                run_id TEXT, detail_json TEXT
            )
        """))
        connection.execute(text("""
            INSERT INTO qmt_datadir_import_partition
            VALUES ('stock_minute', :target, 'cache', 'COMPLETE', 2, 482, '', 'old', :detail)
        """), {"target": target, "detail": json.dumps({"full_market_manifest_hash": "a" * 64})})
        connection.execute(text("""
            CREATE TABLE qmt_history_coverage_manifest (
                manifest_hash TEXT, dataset TEXT, trade_date TEXT, provider TEXT,
                status TEXT, schema_version TEXT, captured_at TEXT, strategy_eligible INTEGER
            )
        """))
        if new_proof:
            connection.execute(text("""
                INSERT INTO qmt_history_coverage_manifest VALUES (
                    :hash, 'stock_daily', :target, 'qmt_datadir', 'EXACT',
                    'probiga.qmt-datadir-daily-coverage.v1', :captured, 1
                )
            """), {"hash": new_proof[0], "captured": new_proof[1], "target": target})

    def is_due(candidate, now):
        assert runtime._attach_daily_recovery_targets(calendar_engine, [candidate], now=now)
        runtime._attach_qmt_minute_scope_recovery(calendar_engine, [candidate], now=now)
        return runtime._cron_due(candidate, now=now) and runtime._overdue_cron_allowed(
            candidate, now=now, cron_time=candidate["cron_time"], startup_time=now,
        )

    assert is_due(row, now) is due
    if due:
        assert runtime._build_task_args(row, row["script_path"], target)[-4:] == [
            "--start", target, "--end", target,
        ]
        assert not is_due({**row, "last_run_status": "running", "last_triggered_at": now}, now)
        with calendar_engine.begin() as connection:
            connection.execute(text("""
                UPDATE qmt_datadir_import_partition SET detail_json=:detail
                WHERE dataset='stock_minute' AND trade_date=:target
            """), {"target": target, "detail": json.dumps({"full_market_manifest_hash": new_proof[0]})})
        row.update(last_triggered_at=now, last_run_at=now)
        assert not is_due(row, datetime(2026, 9, 21, 20, 20))


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
