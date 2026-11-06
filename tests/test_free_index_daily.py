from __future__ import annotations

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
                    "codes": ["000001", "399001", "399006", "000688", "899050"]})]


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


def test_public_index_scope_excludes_unavailable_fixed_codes():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE sm_index_kline ("
                "index_code TEXT, trade_date TEXT, k_type INTEGER)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO sm_index_kline VALUES "
                "('000001','2026-10-26',1),"
                "('988006','2026-10-26',1),"
                "('988007','2026-10-26',1),"
                "('988106','2026-10-26',1),"
                "('988107','2026-10-26',1),"
                "('988201','2026-10-26',1)"
            )
        )
    try:
        assert collector._expected_codes(engine, "2026-10-27") == [
            "000001"
        ]
    finally:
        engine.dispose()


def test_public_index_publisher_uses_persisted_scope_and_verified_rows(
    monkeypatch,
):
    class Engine:
        def dispose(self):
            pass

    calls = []
    rows = [
        {
            "index_code": "000001",
            "trade_date": "2026-10-26",
            "open": 1,
            "close": 2,
            "high": 3,
            "low": 1,
            "volume": 10,
            "amount": 20,
        }
    ]
    monkeypatch.setattr(collector, "create_batch_engine", Engine)
    monkeypatch.setattr(collector, "get_kline_engine", Engine)
    monkeypatch.setattr(
        collector,
        "_expected_codes",
        lambda *_args, **_kwargs: ["000001"],
    )
    monkeypatch.setattr(
        collector,
        "_read_target",
        lambda *_args, **_kwargs: rows,
    )
    monkeypatch.setattr(
        collector,
        "step_index_kline",
        lambda *_args, **_kwargs: calls.append("fetch"),
    )
    monkeypatch.setattr(
        collector,
        "_write_receipt",
        lambda *_args, **_kwargs: {"receipt_id": "a" * 64},
    )

    result = collector.publish("2026-10-26")

    assert calls == ["fetch"]
    assert result["status"] == "COMPLETE"
    assert result["expected_count"] == result["code_count"] == 1
    assert result["missing_codes"] == []
    assert result["qmt_attestation_claimed"] is False


def test_public_index_publisher_reports_missing_codes_without_claiming_full(
    monkeypatch,
):
    class Engine:
        def dispose(self):
            pass

    monkeypatch.setattr(collector, "create_batch_engine", Engine)
    monkeypatch.setattr(collector, "get_kline_engine", Engine)
    monkeypatch.setattr(
        collector,
        "_expected_codes",
        lambda *_args, **_kwargs: ["000001", "399001"],
    )
    monkeypatch.setattr(
        collector,
        "_read_target",
        lambda *_args, **_kwargs: [
            {
                "index_code": "000001",
                "trade_date": "2026-10-26",
                "open": 1,
                "close": 2,
                "high": 3,
                "low": 1,
                "volume": 10,
                "amount": 20,
            }
        ],
    )
    monkeypatch.setattr(collector, "step_index_kline", lambda *_args: None)
    monkeypatch.setattr(
        collector,
        "_write_receipt",
        lambda *_args, **_kwargs: {"receipt_id": "b" * 64},
    )

    result = collector.publish("2026-10-26")

    assert result["status"] == "PARTIAL"
    assert result["missing_codes"] == ["399001"]


def test_public_index_publisher_accepts_explicit_core_scope(monkeypatch):
    class Engine:
        def dispose(self):
            pass

    calls = []
    monkeypatch.setattr(collector, "create_batch_engine", Engine)
    monkeypatch.setattr(collector, "get_kline_engine", Engine)
    monkeypatch.setattr(
        collector,
        "_expected_codes",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("persisted scope must not be enumerated")
        ),
    )
    monkeypatch.setattr(
        collector,
        "_read_target",
        lambda *_args, **_kwargs: [
            {
                "index_code": "000001",
                "trade_date": "2026-10-28",
                "open": 1,
                "close": 2,
                "high": 3,
                "low": 1,
                "volume": 10,
                "amount": 20,
            }
        ],
    )
    monkeypatch.setattr(
        collector,
        "step_index_kline",
        lambda _engine, codes, *_args, **_kwargs: calls.append(codes),
    )
    monkeypatch.setattr(
        collector,
        "_write_receipt",
        lambda *_args, **_kwargs: {"receipt_id": "c" * 64},
    )

    result = collector.publish(
        "2026-10-28",
        codes=["000001"],
    )

    assert calls == [["000001"]]
    assert result["status"] == "COMPLETE"
    assert result["expected_count"] == 1
