import json
from contextlib import contextmanager
from datetime import date, datetime

import pytest
from sqlalchemy import create_engine, text

from server.common import market_close_ladder as ladder


DAY = date(2026, 11, 6)
OBSERVED = datetime(2026, 11, 7, 2, 0, 0, 123456)


def _raw(*, qdate=20261106, count=2, duplicate=False):
    return json.dumps({"rc": 0, "data": {"qdate": qdate, "tc": count, "pool": [
        {"c": "000001", "n": "样本甲", "p": 10100, "lbc": 2},
        {"c": "000001" if duplicate else "000002", "n": "样本乙", "p": 12300, "lbc": 1},
    ]}})


@pytest.fixture
def source(monkeypatch, tmp_path):
    monkeypatch.setenv("PROBIGA_JOB_LOG_ROOT", str(tmp_path))
    monkeypatch.setattr(ladder, "_now", lambda: OBSERVED)
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE si_trade_calendar (trade_date TEXT, trade_status INTEGER)"))
        conn.execute(text("INSERT INTO si_trade_calendar VALUES ('2026-11-06',1)"))
        conn.execute(text("""CREATE TABLE sm_stock_kline (
            stock_code TEXT, trade_date TEXT, close REAL, data_source TEXT, quality_status TEXT,
            permission_status TEXT, k_type INTEGER, adjust_type INTEGER, etl_sync_at TEXT)"""))
        conn.execute(text("""INSERT INTO sm_stock_kline VALUES
            ('000001','2026-11-06',10.10,'qmt_datadir','VERIFIED','SUPPORTED',1,0,'2026-11-06 17:00:00'),
            ('000002','2026-11-06',12.30,'qmt_datadir','VERIFIED','SUPPORTED',1,0,'2026-11-06 17:00:00')"""))

    @contextmanager
    def lease(selected, name):
        assert selected is engine and name.endswith(DAY.isoformat())
        with engine.connect() as conn:
            yield conn

    monkeypatch.setattr(ladder, "mysql_named_lock", lease)
    yield engine
    engine.dispose()


@pytest.mark.parametrize("raw", [_raw(qdate=20261105), _raw(count=3), _raw(duplicate=True)])
def test_rejects_wrong_date_incomplete_pool_and_duplicates(raw):
    with pytest.raises(ladder.MarketCloseLadderBlocked):
        ladder._pool(raw, DAY)


def test_capture_is_exact_immutable_and_uses_actual_availability(source, monkeypatch):
    calls = []

    def fetch(day, **kwargs):
        assert day == DAY and kwargs["timeout_seconds"] == 8 and kwargs["max_payload_bytes"] > 0
        calls.append(day)
        return [], _raw()

    monkeypatch.setattr(ladder, "_fetch_pool", fetch)
    first = ladder.capture_market_close_ladder(source, source, DAY)
    second = ladder.capture_market_close_ladder(source, source, DAY)
    assert first["status"] == second["status"] == "COMPLETE"
    assert first["receipt_sha256"] == second["receipt_sha256"]
    assert not first["reused"] and second["reused"] and calls == [DAY]
    quotes = ladder._load_qmt_closes(source, DAY, ladder._pool(_raw(), DAY), OBSERVED)
    result = ladder.load_market_close_ladder(DAY, quotes=quotes, now=OBSERVED)
    assert result["available_at"] == "2026-11-07T02:00:00.123456+08:00"
    assert result["limit_up_count"] == 2 and result["max_streak"] == 2
    assert result["rows"] == [{"streak": 2, "count": 1, "stocks": ["样本甲"]},
                              {"streak": 1, "count": 1, "stocks": ["样本乙"]}]
    with pytest.raises(ladder.MarketCloseLadderBlocked, match="availability"):
        ladder.load_market_close_ladder(DAY, quotes=quotes, now=datetime(2026, 11, 6, 21))
    quotes[0]["price"] += 0.01
    with pytest.raises(ladder.MarketCloseLadderBlocked, match="differs from QMT"):
        ladder.load_market_close_ladder(DAY, quotes=quotes, now=OBSERVED)


def test_mismatched_close_cannot_be_published(source, monkeypatch):
    monkeypatch.setattr(ladder, "_fetch_pool", lambda *_args, **_kwargs: ([], _raw().replace("10100", "10110")))
    with pytest.raises(ladder.MarketCloseLadderBlocked, match="differs from QMT"):
        ladder.capture_market_close_ladder(source, source, DAY)
    assert not ladder._path(DAY).exists()


def test_exact_explicit_empty_pool_does_not_create_streaks(source, monkeypatch):
    raw = json.dumps({"rc": 0, "data": {"qdate": 20261106, "tc": 0, "pool": []}})
    monkeypatch.setattr(ladder, "_fetch_pool", lambda *_args, **_kwargs: ([], raw))
    assert ladder.capture_market_close_ladder(source, source, DAY)["sample_count"] == 0
    result = ladder.load_market_close_ladder(DAY, quotes=[], now=OBSERVED)
    assert result["limit_up_count"] == 0 and result["max_streak"] is None and result["rows"] == []


def test_manual_ladder_dispatch_binds_closed_friday_without_overview_write(
    source, monkeypatch, capsys,
):
    from fastapi import HTTPException
    from server.api.routers import scheduler
    from server.api.scheduler_runtime import _cron_due, _task_dispatch_date
    from server.common import authoritative_market_clock, kline_data
    from server.common.scheduler_args import build_scheduler_task_args
    from server.common.scheduler_validation import scheduler_output_status, validate_scheduler_task_result
    from tools import refresh_market_overview_daily as overview

    registered = {"id": 400, "task_type": "market_overview_daily",
                  "script_path": "tools/refresh_market_overview_daily.py",
                  "script_args": "", "date_param": "", "cron_time": "18:20"}
    monkeypatch.setattr(scheduler, "_read_sql", lambda query, params: [registered])
    monkeypatch.setattr(scheduler, "get_engine", lambda: source)
    closed_date = authoritative_market_clock.authoritative_closed_trade_date
    monkeypatch.setattr(authoritative_market_clock, "authoritative_closed_trade_date",
        lambda engine, **kwargs: closed_date(engine, now=OBSERVED, **kwargs))
    claimed = []
    monkeypatch.setattr(scheduler, "launch_scheduler_task", lambda row, **kwargs:
        claimed.append(row) or {"accepted": True})

    assert scheduler.run_task_now(400, trade_date=DAY)["accepted"]
    for invalid in ({"trade_date": OBSERVED.date()}, {"trade_date": DAY, "index_codes": "000001"}):
        with pytest.raises(HTTPException) as rejected:
            scheduler.run_task_now(400, **invalid)
        assert rejected.value.status_code == 422
    assert len(claimed) == 1
    task = {**claimed[0], "_trigger_source": "manual_ladder"}
    selected = _task_dispatch_date(task, source, now=OBSERVED)
    assert selected == DAY.isoformat()
    args = build_scheduler_task_args(task, registered["script_path"], selected)
    assert args == ["--dates", "2026-11-06", "--limit-ladder-only"]
    with pytest.raises(ValueError, match="exact manual"):
        build_scheduler_task_args({**task, "_trigger_source": "scheduled"}, registered["script_path"], selected)

    monkeypatch.setattr(ladder, "_fetch_pool", lambda *_args, **_kwargs: ([], _raw()))
    monkeypatch.setattr(overview, "load_project_env", lambda: None)
    monkeypatch.setattr(overview, "create_batch_engine", lambda **kwargs: source)
    monkeypatch.setattr(overview, "get_kline_engine", lambda: source)
    monkeypatch.setattr(kline_data, "get_kline_engine", lambda: source)
    monkeypatch.setattr(source, "dispose", lambda: None)
    assert overview.main(args) == 0
    output = capsys.readouterr().out
    assert json.loads(output)["trade_date"] == DAY.isoformat()
    assert scheduler_output_status(task, output, return_code=0) == "success"
    assert validate_scheduler_task_result(task, engine=source, now=OBSERVED, output=output).ok
    assert _cron_due({**registered, "last_triggered_at": "2026-11-06 18:21:00",
                      "last_run_status": "success", "last_run_output": output},
                     now=datetime(2026, 11, 6, 18, 22))
    ladder._path(DAY).unlink()
    assert not validate_scheduler_task_result(task, engine=source, now=OBSERVED, output=output).ok


def test_ladder_only_history_does_not_replace_full_overview_release_evidence(source):
    from server.api.scheduler_runtime import _attach_release_catchup_history
    from tools.ensure_quality_gate import _latest_release_history_row

    with source.begin() as conn:
        conn.execute(text("""CREATE TABLE st_scheduled_task_history (
            id INT, run_uid TEXT, task_id INT, task_type TEXT, task_name TEXT, status TEXT,
            build_sha TEXT, run_at TEXT, finished_at TEXT, exit_code INT, output TEXT, trigger_source TEXT
        )"""))
        conn.execute(text("""INSERT INTO st_scheduled_task_history VALUES
            (1,'overview',400,'market_overview_daily','overview','success','build',
             '2026-11-06 18:20','2026-11-06 18:21',0,'overview proof','scheduled'),
            (2,'ladder',400,'market_overview_daily','overview','success','build',
             '2026-11-07 02:00','2026-11-07 02:01',0,'ladder proof','manual_ladder')
        """))
    task = {"id": 400, "task_type": "market_overview_daily", "task_name": "overview"}
    assert _attach_release_catchup_history(source, [task])
    assert task["_release_terminal_run_uid"] == "overview"
    assert _latest_release_history_row(source, task=task)["run_uid"] == "overview"
