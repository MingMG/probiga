from datetime import datetime
import json
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import create_engine, text

from server.api import scheduler_runtime
from server.common.scheduler_task_retirement import (
    is_retired_provider_task, retire_superseded_provider_tasks,
    restore_calendar_skip_projections,
)


def _engine(status="failed", task_type="stock_current", script="tools/sync_qmt_primary.py"):
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text("""CREATE TABLE st_scheduled_tasks (
            id INTEGER PRIMARY KEY, task_type TEXT, script_path TEXT, enabled INTEGER,
            last_run_status TEXT, last_run_at TEXT, last_triggered_at TEXT,
            last_run_output TEXT, last_run_duration INTEGER)"""))
        conn.execute(text("""INSERT INTO st_scheduled_tasks VALUES
            (1,:task_type,:script,1,:status,'2026-09-11 15:03:00',
             '2026-09-11 15:03:00','exact provider receipt',120)"""),
            {"status": status, "task_type": task_type, "script": script})
    return engine


def _read(engine):
    with engine.connect() as conn:
        return dict(conn.execute(text("SELECT * FROM st_scheduled_tasks")).mappings().one())


@pytest.mark.parametrize("status", ["failed", "degraded", "timeout", "success"])
def test_closed_session_preserves_last_real_execution(status):
    engine = _engine(status)
    before = _read(engine)
    scheduler_runtime._mark_non_trading_day_skip(before, engine, datetime(2026, 9, 12, 8, 0))
    after = _read(engine)
    assert after["last_triggered_at"].startswith("2026-09-12 08:00")
    assert {k: v for k, v in after.items() if k != "last_triggered_at"} == {
        k: v for k, v in before.items() if k != "last_triggered_at"
    }


def test_closed_session_never_changes_running_owner_or_raced_execution():
    for running in (False, True):
        engine = _engine("running" if running else "failed")
        observed = _read(engine)
        if not running:
            with engine.begin() as conn:
                conn.execute(text("UPDATE st_scheduled_tasks SET last_triggered_at='2026-09-11 16:00:00'"))
        before = _read(engine)
        scheduler_runtime._mark_non_trading_day_skip(observed, engine, datetime(2026, 9, 12, 8, 0))
        assert _read(engine) == before


def test_retirement_preserves_running_identity_and_historical_receipt():
    engine = _engine("running")
    before = _read(engine)
    result = retire_superseded_provider_tasks(engine)
    assert result["retired_tasks"] == [{"id": 1, "task_type": "stock_current",
        "script_path": "tools/sync_qmt_primary.py", "previous_enabled": 1}]
    assert _read(engine) == {**before, "enabled": 0}
    assert retire_superseded_provider_tasks(engine)["retired_tasks"] == []


@pytest.mark.parametrize("task_type", ["intraday_minute_kline", "intraday_minute_flow"])
def test_retirement_keeps_formal_public_minute_sources(task_type):
    engine = _engine(task_type=task_type, script="tools/crawl_minute_kline.py")
    before = _read(engine)
    assert not is_retired_provider_task(before)
    assert retire_superseded_provider_tasks(engine)["retired_tasks"] == []
    assert _read(engine) == before


def test_retirement_covers_unknown_generic_script_alias():
    assert is_retired_provider_task({"task_type": "stock_minute_flow",
                                     "script_path": r"tools\run_single_table.py"})


@pytest.mark.parametrize("router_name", ["scheduler", "datasource"])
def test_retired_task_cannot_be_reenabled_through_management_api(monkeypatch, router_name):
    from server.api.routers import scheduler, datasource
    router = {"scheduler": scheduler, "datasource": datasource}[router_name]
    monkeypatch.setattr(router, "_read_sql", lambda *args: [{
        "id": 1, "enabled": 0, "task_type": "stock_current",
        "script_path": "tools/sync_qmt_primary.py",
    }])
    def unexpected(*args, **kwargs):
        raise AssertionError("retired task must not reach a write")
    monkeypatch.setattr(router, "get_engine", unexpected)
    assert router.toggle_task(1) == {
        "id": 1, "enabled": 0, "status": "retired_provider_task",
        "error": "旧数据源任务已退役，请使用正式采集任务",
    }


def _calendar_engine():
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE si_trade_calendar (trade_date TEXT, trade_status INTEGER)"))
        conn.execute(text("INSERT INTO si_trade_calendar VALUES (:day,1)"), [
            {"day": day} for day in ("2026-09-10", "2026-09-11", "2026-09-14")
        ])
    return engine


@pytest.mark.parametrize("now,expected", [
    (datetime(2026, 9, 11, 16), "2026-09-10"),
    (datetime(2026, 9, 12, 0, 1), "2026-09-11"),
    (datetime(2026, 9, 14, 16), "2026-09-11"),
    (datetime(2026, 9, 14, 23, 59, 59, 999999), "2026-09-11"),
    (datetime(2026, 9, 11, 16, 1, tzinfo=ZoneInfo("UTC")), "2026-09-11"),
])
def test_native_minute_finality_uses_elapsed_shanghai_calendar_day(now, expected):
    from server.common.authoritative_market_clock import authoritative_elapsed_trade_date
    assert authoritative_elapsed_trade_date(_calendar_engine(), now=now) == expected


@pytest.mark.parametrize("task_type", ["qmt_index_minute", "qmt_stock_minute_canonical"])
def test_minute_backfill_runs_next_morning_with_exact_target_and_retry_budget(monkeypatch, task_type):
    engine = _calendar_engine()
    now = datetime(2026, 9, 12, 8)
    row = {"id": 112, "task_type": task_type, "cron_time": "15:55", "script_args":
           "--dataset minute --latest-session --apply --json", "date_param": "",
           "last_run_at": datetime(2026, 9, 11, 16),
           "last_triggered_at": datetime(2026, 9, 11, 16), "last_run_status": "blocked",
           "last_run_output": ""}
    if task_type == "qmt_stock_minute_canonical":
        from tools.sync_qmt_stock_edge import _failure
        payload = _failure("minute", ValueError("SAME_DAY_SOURCE_NOT_FINAL"))
    else:
        from tools.sync_qmt_index_edge import RESULT_SCHEMA, PROVIDER
        payload = {"schema": RESULT_SCHEMA, "status": "BLOCKED", "dataset": "minute",
                   "provider": PROVIDER, "reason": "DATA_BLOCKED: SAME_DAY_SOURCE_NOT_FINAL"}
    row["last_run_output"] = json.dumps(payload)
    def forbidden(*args, **kwargs):
        raise AssertionError("minute collection cannot depend on a strategy delivery")
    monkeypatch.setattr(scheduler_runtime, "_daily_result_recovery_target", forbidden)
    assert scheduler_runtime._attach_daily_recovery_targets(engine, [row], now=now)
    assert row["_scheduler_target_trade_date"] == "2026-09-11"
    assert scheduler_runtime._cron_due(row, now=now)
    assert scheduler_runtime._overdue_cron_allowed(row, now=now, cron_time="15:55", startup_time=now)
    assert not scheduler_runtime._should_skip_non_trading_day(row, engine, now)
    args = scheduler_runtime._build_task_args(row, "tools/sync_qmt_stock_edge.py", "2026-09-11")
    assert "--latest-session" not in args
    assert args[-4:] == ["--start-date", "2026-09-11", "--end-date", "2026-09-11"]
    row.update(last_run_at=now, last_triggered_at=now, last_run_duration=10)
    assert not scheduler_runtime._cron_due(row, now=datetime(2026, 9, 12, 8, 10))
    assert scheduler_runtime._cron_due(row, now=datetime(2026, 9, 12, 8, 16))
    row["last_run_status"] = "success"
    row["last_run_output"] = json.dumps({"target_trade_date": "2026-09-11"})
    assert not scheduler_runtime._cron_due(row, now=datetime(2026, 9, 12, 8, 16))


def test_minute_retry_rejects_free_text_and_explicit_terminal_policy():
    row = {"task_type": "qmt_index_minute", "last_run_status": "blocked"}
    assert not scheduler_runtime._task_status_is_retryable({**row, "last_run_output": "DATA_BLOCKED retryable true"})
    assert not scheduler_runtime._task_status_is_retryable({**row, "last_run_output": json.dumps({
        "retryable": False, "status": "BLOCKED", "reason": "requires user action"})})


def test_finalized_minute_missing_calendar_fails_closed():
    engine = create_engine("sqlite://")
    row = {"task_type": "qmt_stock_minute_canonical"}
    assert not scheduler_runtime._attach_daily_recovery_targets(engine, [row], now=datetime(2026, 9, 12, 8))
    assert row["_scheduler_target_available"] is False


def _overwritten_skip_engine(status="failed"):
    engine = _engine(status="success")
    with engine.begin() as conn:
        conn.execute(text("""CREATE TABLE st_scheduled_task_history (
            id INTEGER PRIMARY KEY,task_id INTEGER,task_type TEXT,run_uid TEXT,build_sha TEXT,
            run_at TEXT,finished_at TEXT,status TEXT,duration INTEGER,output TEXT)"""))
        conn.execute(text("""INSERT INTO st_scheduled_task_history VALUES
            (10,1,'stock_current',:uid,:sha,'2026-09-11 15:00:00',:finished,:status,120,'native run receipt')"""),
            {"uid": "a"*32, "sha": "b"*40, "status": status,
             "finished": None if status == "running" else "2026-09-11 15:02:00"})
        conn.execute(text("""UPDATE st_scheduled_tasks SET
            last_run_at='2026-09-12 08:00:00',last_triggered_at='2026-09-12 08:00:00',
            last_run_duration=0,last_run_output='Skipped automatically: 2026-09-12 is not a trading day.'"""))
    return engine


@pytest.mark.parametrize("status", ["running", "degraded", "failed", "success"])
def test_migration_reconstructs_only_real_run_projection_and_retains_audit(status):
    engine = _overwritten_skip_engine(status)
    with engine.connect() as conn:
        history = list(conn.execute(text("SELECT * FROM st_scheduled_task_history")))
    result = restore_calendar_skip_projections(engine)
    assert result["status"] == "PASS"
    assert result["restored"][0]["source_run_uid"] == "a" * 32
    assert "native run receipt" not in json.dumps(result)
    after = _read(engine)
    assert after["last_run_status"] == status
    assert after["last_run_at"] == "2026-09-11 15:00:00"
    assert after["last_triggered_at"] == "2026-09-12 08:00:00"
    assert after["last_run_output"] == "native run receipt"
    with engine.connect() as conn:
        assert list(conn.execute(text("SELECT * FROM st_scheduled_task_history"))) == history
    assert restore_calendar_skip_projections(engine)["restored"] == []


@pytest.mark.parametrize("drift", ["wrong_type", "unfinished_terminal", "newer_run", "not_exact_skip"])
def test_projection_repair_rejects_ambiguous_history_and_preserves_task(drift):
    engine = _overwritten_skip_engine()
    with engine.begin() as conn:
        if drift == "wrong_type":
            conn.execute(text("UPDATE st_scheduled_task_history SET task_type='other'"))
        elif drift == "unfinished_terminal":
            conn.execute(text("UPDATE st_scheduled_task_history SET finished_at=NULL"))
        elif drift == "newer_run":
            conn.execute(text("UPDATE st_scheduled_task_history SET run_at='2026-09-12 09:00:00'"))
        else:
            conn.execute(text("UPDATE st_scheduled_tasks SET last_run_output=last_run_output || ' custom text'"))
    before = _read(engine)
    result = restore_calendar_skip_projections(engine)
    assert result["restored"] == []
    assert _read(engine) == before
    if drift != "not_exact_skip":
        assert result["status"] == "BLOCKED"
