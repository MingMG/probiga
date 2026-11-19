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


def _fetch_result(raw, kwargs):
    return [], (raw if kwargs.get("pool_kind") == "limit_up" else
                json.dumps({"rc": 0, "data": {"qdate": 20261106, "tc": 0, "pool": []}}))


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
        return _fetch_result(_raw(), kwargs)

    monkeypatch.setattr(ladder, "_fetch_pool", fetch)
    first = ladder.capture_market_close_ladder(source, source, DAY)
    second = ladder.capture_market_close_ladder(source, source, DAY)
    assert first["status"] == second["status"] == "COMPLETE"
    assert first["receipt_sha256"] == second["receipt_sha256"]
    assert not first["reused"] and second["reused"] and calls == [DAY] * 3
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
    monkeypatch.setattr(ladder, "_fetch_pool", lambda *_args, **kwargs: _fetch_result(_raw().replace("10100", "10110"), kwargs))
    with pytest.raises(ladder.MarketCloseLadderBlocked, match="differs from QMT"):
        ladder.capture_market_close_ladder(source, source, DAY)
    assert not ladder._path(DAY).exists()


def test_historical_pool_with_latest_quote_date_is_bound_to_exact_qmt_closes(source, monkeypatch):
    monkeypatch.setattr(ladder, "_fetch_pool", lambda *_args, **kwargs: _fetch_result(_raw(qdate=20261107), kwargs))
    result = ladder.capture_market_close_ladder(source, source, DAY)
    assert result["status"] == "COMPLETE" and result["sample_count"] == 2
    assert result["trade_date"] == DAY.isoformat()
    record, pool = ladder._load_record(DAY, OBSERVED)
    assert json.loads(record["raw_payload"])["data"]["qdate"] == 20261107
    assert len(pool) == 2


@pytest.mark.parametrize("raw", [
    _raw(qdate=20261107).replace("10100", "10110"),
    _raw(qdate=20261108),
    json.dumps({"rc": 0, "data": {"qdate": 20261107, "tc": 0, "pool": []}}),
])
def test_historical_quote_date_cannot_publish_wrong_prices_future_or_ambiguous_empty(source, monkeypatch, raw):
    monkeypatch.setattr(ladder, "_fetch_pool", lambda *_args, **_kwargs: ([], raw))
    with pytest.raises(ladder.MarketCloseLadderBlocked):
        ladder.capture_market_close_ladder(source, source, DAY)
    assert not ladder._path(DAY).exists()


def test_exact_explicit_empty_pool_does_not_create_streaks(source, monkeypatch):
    raw = json.dumps({"rc": 0, "data": {"qdate": 20261106, "tc": 0, "pool": []}})
    monkeypatch.setattr(ladder, "_fetch_pool", lambda *_args, **_kwargs: ([], raw))
    assert ladder.capture_market_close_ladder(source, source, DAY)["sample_count"] == 0
    result = ladder.load_market_close_ladder(DAY, quotes=[], now=OBSERVED)
    assert result["limit_up_count"] == 0 and result["max_streak"] is None and result["rows"] == []


def test_down_and_opened_pools_are_independently_verified_and_resume(source, monkeypatch):
    with source.begin() as conn:
        conn.execute(text("""INSERT INTO sm_stock_kline VALUES
            ('000003','2026-11-06',9.50,'qmt_datadir','VERIFIED','SUPPORTED',1,0,'2026-11-06 17:00:00'),
            ('000004','2026-11-06',13.40,'qmt_datadir','VERIFIED','SUPPORTED',1,0,'2026-11-06 17:00:00')
        """))
    bad_down = True
    calls = []

    def fetch(day, **kwargs):
        kind = kwargs["pool_kind"]
        calls.append(kind)
        if kind == "limit_up":
            return [], _raw()
        code, price = ("000003", 9510 if bad_down else 9500) if kind == "limit_down" else ("000004", 13400)
        return [], json.dumps({"rc": 0, "data": {"qdate": 20261107, "tc": 1, "pool": [
            {"c": code, "n": "实际收盘样本", "p": price},
        ]}})

    monkeypatch.setattr(ladder, "_fetch_pool", fetch)
    with pytest.raises(ladder.MarketCloseLadderBlocked, match="differs from QMT"):
        ladder.capture_market_close_ladder(source, source, DAY)
    assert ladder._path(DAY).exists()
    assert not ladder._path(DAY, "limit_down").exists()
    bad_down = False
    captured = ladder.capture_market_close_ladder(source, source, DAY)
    assert calls == ["limit_up", "limit_down", "limit_down", "opened_limit_up"]
    verified = ladder.verify_market_close_ladder_capture(source, DAY, now=OBSERVED)
    assert captured["pool_receipts"] == verified["pool_receipts"]
    pools = [ladder._load_record(DAY, OBSERVED, kind)[1] for kind in ladder._POOL_ENDPOINTS]
    quotes = ladder._load_qmt_closes(source, DAY, sum(pools, []), OBSERVED)
    result = ladder.load_market_close_ladder(DAY, quotes=quotes, now=OBSERVED)
    assert result["limit_up_count"] == 2
    assert result["limit_down_count"] == result["opened_limit_up_count"] == 1
    assert result["seal_rate_pct"] == pytest.approx(200 / 3)
    ladder._path(DAY, "opened_limit_up").unlink()
    with pytest.raises(ladder.MarketCloseLadderBlocked, match="durable receipt is missing"):
        ladder.verify_market_close_ladder_capture(source, DAY, now=OBSERVED)


def test_capture_failure_preserves_the_source_stage_reason(monkeypatch, capsys):
    from server.common.eastmoney_upper_limit_history import EastmoneyUpperLimitHistoryBlocked
    from tools import refresh_market_overview_daily as overview

    reason = "DATA_BLOCKED: Eastmoney limit pool failed for 2026-11-16"
    def unavailable(*_args):
        raise EastmoneyUpperLimitHistoryBlocked(reason)
    monkeypatch.setattr(overview, "capture_market_close_ladder", unavailable)
    assert overview._publish_limit_ladder(None, None, "2026-11-16", exact=True) is False
    receipt = json.loads(capsys.readouterr().out)
    assert receipt["status"] == "BLOCKED" and receipt["reason"] == reason


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
    assert scheduler.run_task_now(400, trade_date=DAY, full_overview=True)["accepted"]
    overview_task = {**claimed[1], "_trigger_source": "manual"}
    assert "_limit_ladder_only" not in overview_task
    assert _task_dispatch_date(overview_task, source, now=OBSERVED) == selected
    assert build_scheduler_task_args(overview_task, registered["script_path"], selected) == [DAY.isoformat()]

    monkeypatch.setattr(ladder, "_fetch_pool", lambda *_args, **kwargs: _fetch_result(_raw(), kwargs))
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
