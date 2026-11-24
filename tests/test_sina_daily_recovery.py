from datetime import date, datetime

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from tools import sync_free_stock_daily as collector


def test_completed_source_survives_publication_failure_without_recapture(tmp_path, monkeypatch):
    from biz.stock_market import stock_kline_akshare as provider
    monkeypatch.setenv("PROBIGA_JOB_LOG_ROOT", str(tmp_path))
    calls = []
    def fetch(code, start, end, adjust, **kwargs):
        calls.append((code, end))
        return pd.DataFrame([{"stock_code": code, "trade_date": datetime.strptime(end, "%Y%m%d").date(),
                              "trade_time": datetime.strptime(end, "%Y%m%d"), "volume": 100, "close": 10}])
    monkeypatch.setattr(provider, "fetch_stock_daily_kline", fetch)
    monkeypatch.setattr(provider, "akshare_daily_to_sm_kline", lambda frame, *_: frame)
    first, evidence = collector.fetch_sina_history_day(["000001"], target_date="2026-11-23")
    # The next attempt has no working source, as after a failed database publication.
    monkeypatch.setattr(provider, "fetch_stock_daily_kline", lambda *_a, **_kw: pytest.fail("source recaptured"))
    second, retained = collector.fetch_sina_history_day(["000001"], target_date="2026-11-23")
    assert calls == [("000001", "20261123")]
    assert second.to_dict("records") == first.to_dict("records")
    assert retained == evidence
    monkeypatch.setattr(provider, "fetch_stock_daily_kline", fetch)
    collector.fetch_sina_history_day(["000001"], target_date="2026-11-20")
    assert len(calls) == 2


def test_manual_daily_repair_binds_date_without_changing_registration(monkeypatch):
    from server.api.routers import scheduler
    from server.common import authoritative_market_clock
    from server.common.scheduler_args import build_scheduler_task_args
    engine = create_engine("sqlite://")
    with engine.begin() as c:
        c.execute(text("CREATE TABLE si_trade_calendar (trade_date TEXT, trade_status INTEGER)"))
        c.execute(text("INSERT INTO si_trade_calendar VALUES ('2026-11-23',1),('2026-11-24',1)"))
    registered = {"id": 138, "task_type": "free_stock_daily_canonical", "enabled": 0,
                  "script_path": "tools/sync_free_stock_daily.py", "script_args": "--date latest --json", "date_param": ""}
    monkeypatch.setattr(scheduler, "_read_sql", lambda *_: [registered])
    monkeypatch.setattr(scheduler, "get_engine", lambda: engine)
    monkeypatch.setattr(authoritative_market_clock, "authoritative_closed_trade_date", lambda *_a, **_kw: "2026-11-24")
    claimed = []
    monkeypatch.setattr(scheduler, "launch_scheduler_task", lambda row, **kw: claimed.append(row) or {"accepted": True})
    assert scheduler.run_task_now(138, trade_date=date(2026, 11, 23))["accepted"]
    assert registered["script_args"] == "--date latest --json" and registered["enabled"] == 0
    row = {**claimed[0], "_trigger_source": "manual"}
    assert row["_scheduler_target_trade_date"] == "2026-11-23"
    assert build_scheduler_task_args(row, row["script_path"], "2026-11-23") == ["--date", "2026-11-23", "--backfill-sina", "--json"]
    from fastapi import HTTPException
    with pytest.raises(HTTPException):
        scheduler.run_task_now(138, trade_date=date(2026, 11, 25))
    engine.dispose()


def test_write_progress_does_not_claim_commit_when_publication_rolls_back(monkeypatch, capsys):
    from types import SimpleNamespace
    from server.common import batch_db
    import json
    engine = create_engine("sqlite://")
    frame = pd.DataFrame([dict(stock_code=code, short_name="sample", trade_date=date(2026, 11, 23),
        trade_time=datetime(2026, 11, 23), k_type=1, adjust_type=0, open=10, close=11, high=11,
        low=10, volume=100, amount=1050, pre_close=10, change=1, change_pct=10, turnover_ratio=1)
        for code in ("000001", "000002")])
    monkeypatch.setattr(collector, "_latest_catalog", lambda *_a, **_kw: SimpleNamespace(
        eligible_codes=lambda _: ["000001", "000002"], batch_id="catalog", manifest_hash="a" * 64, member_set_hash="b" * 64))
    monkeypatch.setattr(collector, "_source_counts", lambda *_a, **_kw: {})
    monkeypatch.setattr(collector, "fetch_sina_history_day", lambda *_a, **_kw: (frame, {
        "coverage_ratio": 1, "captured_at": "2026-11-24 10:00:00"}))
    def interrupted(rows, table_name, _engine, **kwargs):
        with engine.begin() as c:
            batch_db.write_frame(rows, table_name, c, if_exists="append", chunksize=1, method=kwargs["method"])
            raise RuntimeError("publication interrupted")
    monkeypatch.setattr(collector, "replace_table_rows_exact_keys", interrupted)
    with pytest.raises(RuntimeError, match="publication interrupted"):
        collector.publish_sina_history(engine, target_date="2026-11-23", now=datetime(2026, 11, 24, 10))
    messages = [json.loads(x) for x in capsys.readouterr().err.splitlines()]
    progress = [x for x in messages if x["stage"] == "database_write_progress"]
    assert [x["attempted_rows"] for x in progress] == [1, 2]
    assert all(x["total_rows"] == 2 and x["committed"] is False for x in progress)
    with engine.connect() as c:
        assert c.execute(text("SELECT COUNT(*) FROM sm_stock_kline")).scalar() == 0
    engine.dispose()
