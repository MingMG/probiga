from __future__ import annotations

from contextlib import contextmanager
import json
from datetime import datetime

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from biz.stock_market import sync_stock_market
from tools import sync_free_index_daily as collector


@pytest.fixture
def index_repair_database():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE si_trade_calendar (trade_date TEXT,trade_status INT)"))
        connection.execute(text("INSERT INTO si_trade_calendar VALUES ('2026-10-09',1),('2026-11-06',1)"))
        connection.execute(text("""CREATE TABLE qmt_datadir_import_partition (
            dataset TEXT,status TEXT,trade_date TEXT,row_count INT,code_count INT,
            content_sha256 TEXT,finished_at TEXT,detail_json TEXT)"""))
        connection.execute(text("""CREATE TABLE sm_index_kline (
            index_code TEXT,trade_time TEXT,trade_date TEXT,k_type INT,
            `open` REAL,`close` REAL,high REAL,low REAL,volume REAL,amount REAL,
            `change` REAL,change_pct REAL,etl_sync_at TEXT,
            PRIMARY KEY(index_code,trade_date,k_type))"""))
        connection.execute(text("""CREATE TABLE acquisition_partition_state (
            dataset TEXT,source TEXT,target_date TEXT,partition_key TEXT,status TEXT,
            request_id TEXT,written_rows INT,last_attempt_at TEXT,last_success_at TEXT,
            detail_json TEXT,updated_at TEXT,PRIMARY KEY(dataset,source,target_date,partition_key))"""))
        connection.execute(text("""INSERT INTO sm_index_kline VALUES (
            '000001','2026-11-06 15:00:00','2026-11-06',1,3640,3650,3660,3630,
            120,1200,10,0.28,'2026-11-06 18:17:14')"""))
        connection.execute(text("""INSERT INTO acquisition_partition_state VALUES (
            'index_daily','tencent_east_verified','2026-11-06','__index_daily_partition__',
            'complete','original',1,'2026-11-06 18:17:16','2026-11-06 18:17:16',
            '{}','2026-11-06 18:17:16')"""))
    yield engine
    engine.dispose()


def test_exact_index_repair_preserves_original_rows_and_scope(index_repair_database, monkeypatch):
    from server.common.index_daily_publication import FREE_BAR_RECEIPT_SCHEMA, free_index_bar_hash

    class RepairClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 11, 7, 0, 15, tzinfo=tz)

    monkeypatch.setattr(collector, "datetime", RepairClock)
    target = "2026-11-06"
    source_row = dict(index_code="000300", trade_time=f"{target} 15:00:00", trade_date=target,
        k_type=1, open=4010, close=4054.27, high=4060, low=4001, volume=100, amount=float("nan"),
        change=54.27, change_pct=1.36)
    monkeypatch.setattr(collector, "_fetch_verified_external_index_kline",
        lambda code, start, end: (pd.DataFrame([source_row]), "tencent_only"))
    variant, values = collector._repair_source("000300", target)
    assert values["amount"] is None
    candidate = {"000300": (variant, values), "000001": (variant, {**values, "index_code": "000001"})}
    with index_repair_database.begin() as connection:
        published, = collector._repair_publish(connection, target, candidate)
        assert published["index_code"] == "000300"
    with index_repair_database.begin() as connection:
        rows = collector._repair_rows(connection, target, ["000001", "000300"])
        assert rows["000001"]["close"] == 3650
        assert rows["000300"]["amount"] is None
        assert free_index_bar_hash(rows["000300"]) == published["row_hash"]
        assert published["schema"] == FREE_BAR_RECEIPT_SCHEMA
        assert published["source_variant"] == "tencent_only"
        assert published["available_at"] > "2026-11-06 18:17:16"
        assert connection.execute(text("SELECT etl_sync_at FROM sm_index_kline WHERE index_code='000001'")).scalar_one() == "2026-11-06 18:17:14"
        saved = connection.execute(text("""SELECT request_id,last_success_at,detail_json FROM acquisition_partition_state
            WHERE partition_key LIKE 'index_bar:000300:%'""")).mappings().one()
        assert saved["request_id"] == published["receipt_id"]
        assert saved["last_success_at"] == published["available_at"]
        assert json.loads(saved["detail_json"]) == published
        assert connection.execute(text("SELECT request_id FROM acquisition_partition_state WHERE partition_key='__index_daily_partition__'")).scalar_one() == "original"
        assert collector._repair_publish(connection, target, candidate) == []


def test_index_repair_rolls_back_if_receipt_cannot_be_saved(index_repair_database):
    with index_repair_database.begin() as connection:
        connection.execute(text("""CREATE TRIGGER reject_repair_receipt BEFORE INSERT ON acquisition_partition_state
            BEGIN SELECT RAISE(ABORT, 'receipt storage failed'); END"""))
    target = "2026-11-06"
    values = dict(index_code="000300", trade_time=f"{target} 15:00:00", trade_date=target,
        k_type=1, open=4010, close=4054.27, high=4060, low=4001, volume=100, amount=None,
        change=54.27, change_pct=1.36)
    from sqlalchemy.exc import IntegrityError
    with pytest.raises(IntegrityError, match="receipt storage failed"):
        with index_repair_database.begin() as connection:
            collector._repair_publish(connection, target, {"000300": ("tencent_only", values)})
    with index_repair_database.connect() as connection:
        assert collector._repair_rows(connection, target, ["000300"]) == {}
        assert connection.execute(text("SELECT COUNT(*) FROM acquisition_partition_state")).scalar_one() == 1


def test_scheduled_index_target_is_today_after_1535(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE si_trade_calendar (trade_date TEXT, trade_status INTEGER)"))
        connection.execute(text("INSERT INTO si_trade_calendar VALUES ('2026-11-03',1),('2026-11-04',1)"))
    from server.common.authoritative_market_clock import authoritative_closed_trade_date
    monkeypatch.setattr(collector, "load_project_env", lambda: None)
    monkeypatch.setattr(collector, "create_batch_engine", lambda: engine)
    monkeypatch.setattr(collector, "authoritative_closed_trade_date",
        lambda engine, **kwargs: authoritative_closed_trade_date(
            engine, now=datetime(2026, 11, 4, 17, 10), **kwargs))
    seen = []
    monkeypatch.setattr(collector, "publish", lambda target, **kwargs:
        seen.append((target, kwargs)) or {"status": "COMPLETE"})
    from shlex import split
    from tools.qmt_host_ownership_contract import FREE_INDEX_DAILY_TASK

    assert collector.main(split(FREE_INDEX_DAILY_TASK["script_args"])) == 0
    assert seen == [("2026-11-04", {"apply": True,
                    "codes": list(collector.PUBLIC_INDEX_DAILY_CODES)})]


def test_public_index_sources_map_beijing_index_to_bj_market():
    assert sync_stock_market._tencent_index_symbol("899050") == "bj899050"
    assert sync_stock_market._east_index_secid("899050") == "0.899050"
    assert sync_stock_market._tencent_index_symbol("000001") == "sh000001"
    assert sync_stock_market._east_index_secid("399001") == "0.399001"


def test_beijing_index_accepts_target_day_cross_check(monkeypatch):
    target = pd.Timestamp("2026-10-28")
    columns = [
        "index_code", "trade_time", "trade_date", "k_type", "open", "close",
        "high", "low", "volume", "amount", "change", "change_pct",
    ]
    values = [899050, target + pd.Timedelta(hours=15), target, 1,
              935.86, 946.83, 949.08, 926.56, 6789320, 13857839224,
              15.19, 1.63]
    tencent = pd.DataFrame([values], columns=columns)
    east = pd.concat([
        pd.DataFrame([[899050, target - pd.Timedelta(days=1), target - pd.Timedelta(days=1),
                       1, 968.41, 931.64, 972.06, 930.59, 1, 1, 1, 1]],
                     columns=columns),
        pd.DataFrame([values], columns=columns),
    ], ignore_index=True)
    monkeypatch.setattr(
        sync_stock_market, "_fetch_tencent_index_kline",
        lambda *_args, **_kwargs: tencent,
    )
    monkeypatch.setattr(
        sync_stock_market, "_fetch_east_index_kline",
        lambda *_args, **_kwargs: east,
    )

    frame, source = sync_stock_market._fetch_verified_external_index_kline(
        "899050", "2026-10-28", "2026-10-28",
    )

    assert source == "cross_checked"
    assert frame["trade_date"].max() == target
    assert frame.loc[frame["trade_date"].eq(target), "amount"].iloc[0] == 13857839224


def test_current_index_quote_fills_missing_amount_and_change(monkeypatch):
    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "data": {
                    "f43": 94683,
                    "f48": 13857839224.0,
                    "f86": int(
                        pd.Timestamp("2026-10-28 15:32:23", tz="Asia/Shanghai").timestamp()
                    ),
                    "f169": 1519,
                    "f170": 163,
                }
            }

    monkeypatch.setattr(sync_stock_market.requests, "get", lambda *_a, **_k: Response())
    frame = pd.DataFrame([{
        "index_code": "899050",
        "trade_date": pd.Timestamp("2026-10-28"),
        "close": 946.83,
        "amount": None,
        "change": float("nan"),
        "change_pct": float("nan"),
    }])

    filled = sync_stock_market._fill_index_close_quote(
        frame, "899050", "2026-10-28",
    )

    assert filled.iloc[0]["amount"] == 13857839224.0
    assert filled.iloc[0]["change"] == 15.19
    assert filled.iloc[0]["change_pct"] == 1.63


def test_current_index_quote_falls_back_to_tencent(monkeypatch):
    class TencentResponse:
        text = (
            'v_bj899050="62~北证50~899050~946.83~931.64~935.86~6789320~'
            '3394660~3394660~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~'
            '20261028153211~15.19~1.63~949.08~926.56~946.83/6789320/13857839224~'
            '6789320~1385783.92~1.65";'
        )

        def raise_for_status(self):
            return None

    calls = []

    def fake_get(url, **_kwargs):
        calls.append(url)
        if "push2.eastmoney.com" in url:
            raise sync_stock_market.requests.ConnectionError("blocked")
        return TencentResponse()

    monkeypatch.setattr(sync_stock_market.requests, "get", fake_get)
    frame = pd.DataFrame([{
        "index_code": "899050",
        "trade_date": pd.Timestamp("2026-10-28"),
        "close": 946.83,
        "amount": None,
        "change": float("nan"),
        "change_pct": float("nan"),
    }])

    filled = sync_stock_market._fill_index_close_quote(
        frame, "899050", "2026-10-28",
    )

    assert len(calls) == 2
    assert filled.iloc[0]["amount"] == 13_857_839_200
    assert filled.iloc[0]["change"] == 15.19
    assert filled.iloc[0]["change_pct"] == 1.63



def test_exact_index_job_checkpoints_partial_october_scope_and_reuses_proof(
    index_repair_database, monkeypatch,
):
    from server.common.authoritative_market_clock import authoritative_closed_trade_date
    from server.common.scheduler_validation import (
        scheduler_output_status, validate_scheduler_task_result,
    )
    from server.common import kline_data

    engine = index_repair_database
    monkeypatch.setattr(engine, "dispose", lambda: None)
    monkeypatch.setattr(collector, "create_batch_engine", lambda: engine)
    monkeypatch.setattr(collector, "get_kline_engine", lambda: engine)
    monkeypatch.setattr(kline_data, "get_kline_engine", lambda: engine)
    monkeypatch.setattr(collector, "_DATABASE_IDENTITY", text("SELECT 'test-db','test'"))
    target = "2026-10-09"

    class RepairClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 11, 7, 0, 15, tzinfo=tz)

    monkeypatch.setattr(collector, "datetime", RepairClock)
    monkeypatch.setattr(collector, "authoritative_closed_trade_date",
        lambda conn, **kwargs: authoritative_closed_trade_date(
            conn, now=RepairClock.now(), **kwargs))

    @contextmanager
    def lock(engine, key, timeout_seconds):
        assert key == collector.INDEX_WRITE_LOCK
        with engine.connect() as conn:
            yield conn

    monkeypatch.setattr(collector, "mysql_named_lock", lock)
    called = []

    def source(code, day):
        assert day == target
        called.append(code)
        if code == "899050":
            raise ValueError("missing in both deployed public providers")
        return "tencent_only", {
            "index_code": code, "trade_time": f"{day} 15:00:00", "trade_date": day,
            "k_type": 1, "open": 10, "close": 11, "high": 12, "low": 9, "volume": 100,
            "amount": None, "change": 1, "change_pct": 10,
        }

    monkeypatch.setattr(collector, "_repair_source", source)
    expected = ["000001", "000688", "399001", "399006"]
    task = {"task_type": "free_index_daily_canonical", "_scheduler_target_trade_date": target,
            "_trigger_source": "manual", "_index_repair_codes": tuple(expected)}
    first = collector.publish(target, apply=True, codes=expected + ["899050"])
    assert first["status"] == "PARTIAL" and first["database_writes"]
    assert first["verified_codes"] == expected and first["missing_codes"] == ["899050"]
    assert scheduler_output_status(task, json.dumps(first), return_code=2) == "failed"

    called.clear()
    partial_again = collector.publish(target, apply=True, codes=expected + ["899050"])
    assert called == ["899050"] and not partial_again["database_writes"]
    assert partial_again["proof_sha256"] == first["proof_sha256"]

    called.clear()
    reused = collector.publish(target, apply=True, codes=expected)
    assert called == []
    assert reused["status"] == "COMPLETE" and not reused["database_writes"]
    assert scheduler_output_status(task, json.dumps(reused), return_code=0) == "success"
    checked = validate_scheduler_task_result(
        task, engine=engine, started_at=RepairClock.now(), now=RepairClock.now(),
        output=json.dumps(reused),
    )
    assert checked.ok, checked.message
    assert not validate_scheduler_task_result(
        {**task, "_index_repair_codes": ()}, engine=engine,
        started_at=RepairClock.now(), now=RepairClock.now(), output=json.dumps(reused),
    ).ok

    with engine.begin() as conn:
        conn.execute(text("UPDATE sm_index_kline SET close=10 WHERE trade_date=:day AND index_code='000001'"),
                     {"day": target})
    assert not validate_scheduler_task_result(
        task, engine=engine, started_at=RepairClock.now(), now=RepairClock.now(),
        output=json.dumps(reused),
    ).ok


def test_exact_index_dispatch_uses_existing_manual_task_and_preserves_normal_cron():
    from server.api.scheduler_runtime import _cron_due
    from server.common.scheduler_args import build_scheduler_task_args
    from tools.qmt_host_ownership_contract import FREE_INDEX_DAILY_TASK

    target = "2026-10-09"
    task = {**FREE_INDEX_DAILY_TASK, "_trigger_source": "manual",
            "_scheduler_target_trade_date": target, "_index_repair_codes": ("000001", "000688")}
    assert build_scheduler_task_args(task, task["script_path"], target) == [
        "--date", target, "--codes", "000001,000688", "--repair-missing", "--json",
    ]
    with pytest.raises(ValueError, match="exact manual scope"):
        build_scheduler_task_args({**task, "_trigger_source": "scheduled"}, task["script_path"], target)

    finished = {**FREE_INDEX_DAILY_TASK, "last_triggered_at": "2026-11-06 17:14:00",
                "last_run_status": "success", "last_run_output": json.dumps({"target_date": target})}
    assert _cron_due(finished, now=datetime(2026, 11, 6, 17, 30))
    scheduled = {**finished, "last_run_output": json.dumps({
        "schema": collector.RESULT_SCHEMA, "target_date": "2026-11-06",
        "requested_codes": list(collector.PUBLIC_INDEX_DAILY_CODES),
    })}
    assert not _cron_due(scheduled, now=datetime(2026, 11, 6, 17, 30))


def test_index_api_claims_existing_task_for_exact_closed_scope(index_repair_database, monkeypatch):
    from datetime import date

    from fastapi import HTTPException
    from server.api.admin_auth import is_admin_protected_path
    from server.api.routers import scheduler
    from server.common import authoritative_market_clock
    from tools.qmt_host_ownership_contract import FREE_INDEX_DAILY_TASK

    engine = index_repair_database
    registered = {"id": 139, **FREE_INDEX_DAILY_TASK}
    monkeypatch.setattr(scheduler, "_read_sql", lambda query, params: [registered])
    monkeypatch.setattr(scheduler, "get_engine", lambda: engine)
    monkeypatch.setattr(authoritative_market_clock, "authoritative_closed_trade_date",
                        lambda connection, **kwargs: "2026-11-06")
    dispatched = []
    monkeypatch.setattr(scheduler, "launch_scheduler_task", lambda row, **kwargs:
        dispatched.append(row) or {"accepted": True})
    assert is_admin_protected_path("/api/scheduler/tasks/139/run", "POST")
    route = next(route for route in scheduler.router.routes if route.endpoint is scheduler.run_task_now)
    assert {param.name for param in route.dependant.query_params} == {"trade_date", "index_codes"}
    params = {"trade_date": date(2026, 10, 9), "index_codes": "399001,000001,399001"}
    assert scheduler.run_task_now(139, **params) == {"accepted": True}
    for invalid in ({"trade_date": date(2026, 10, 10)}, {"index_codes": "987654"}):
        with pytest.raises(HTTPException) as rejected:
            scheduler.run_task_now(139, **{**params, **invalid})
        assert rejected.value.status_code == 422
    assert dispatched == [{**registered, "_scheduler_target_trade_date": "2026-10-09",
                            "_index_repair_codes": ("000001", "399001")}]
