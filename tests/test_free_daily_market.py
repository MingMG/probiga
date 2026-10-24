from datetime import datetime
import sys
from types import ModuleType

import pandas as pd
from sqlalchemy import create_engine, text

from server.common.verified_daily_market_truth import (
    is_suspended_daily_row,
    load_source_daily_rows,
)
from tools import sync_free_stock_daily as collector


def test_snapshot_marks_zero_trade_quote_as_suspended(monkeypatch):
    def quote_batch(_session, _codes, *, timeout):
        del timeout
        return [
            {
                "stock_code": "000016",
                "short_name": "*ST康佳A",
                "source_symbol": "sz000016",
                "source_time": "20261023091000",
                "open": "0.00",
                "high": "0.00",
                "low": "0.00",
                "close": "2.46",
                "pre_close": "2.46",
                "volume_hands": "0",
                "amount_cny": "0",
                "change": "0",
                "change_pct": "0",
                "turnover_ratio": "0",
            }
        ]

    monkeypatch.setattr(collector, "_fetch_quote_batch", quote_batch)
    monkeypatch.setattr(collector, "FREE_DAILY_MIN_COVERAGE", 0.90)
    frame, evidence = collector.fetch_target_snapshot(
        ["000016"],
        target_date="2026-10-23",
    )

    assert len(frame) == 1
    row = frame.iloc[0]
    assert row["quality_status"] == "SUSPENDED"
    assert row["permission_status"] == "NO_TRADE"
    assert evidence["suspended_count"] == 1
    assert evidence["traded_count"] == 0
    assert is_suspended_daily_row(row.to_dict()) is True


def test_zero_ohlc_with_volume_is_not_mislabeled_suspended():
    row = {
        "quality_status": "SUSPENDED",
        "permission_status": "NO_TRADE",
        "open": 0,
        "high": 0,
        "low": 0,
        "volume": 100,
        "amount": 1000,
    }

    assert is_suspended_daily_row(row) is False


def test_retained_qmt_daily_rows_are_a_supported_verified_source():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE sm_stock_kline (
                stock_code TEXT, trade_date TEXT, `open` NUMERIC, `close` NUMERIC,
                high NUMERIC, low NUMERIC, volume NUMERIC, amount NUMERIC,
                pre_close NUMERIC, data_source TEXT, quality_status TEXT,
                permission_status TEXT, k_type INTEGER, adjust_type INTEGER
            )
        """))
        connection.execute(text("""
            INSERT INTO sm_stock_kline VALUES (
                '000001','2026-09-28',10,10.5,10.6,9.9,100,1050,10,
                'gj_big_qmt_inner','QMT_ATTESTED','SUPPORTED',1,0
            )
        """))
    with engine.connect() as connection:
        rows = load_source_daily_rows(
            connection,
            source="gj_big_qmt_inner",
            target_date="2026-09-28",
            codes=["000001"],
        )
    assert len(rows) == 1
    assert rows[0]["data_source"] == "gj_big_qmt_inner"
    assert rows[0]["quality_status"] == "QMT_ATTESTED"


def test_baostock_primary_uses_tencent_for_bj_and_missing_codes(monkeypatch):
    fallback = pd.DataFrame(
        [
            {
                "stock_code": "000001", "short_name": "平安银行",
                "trade_time": "2026-10-23 15:00:00", "trade_date": "2026-10-23",
                "k_type": 1, "adjust_type": 0, "open": 11.27, "high": 11.35,
                "low": 11.17, "close": 11.20, "pre_close": 11.30,
                "volume": 144880560, "amount": 1628277173.0,
                "change": -0.1, "change_pct": -0.88, "turnover_ratio": 0.75,
                "data_source": "tencent_qt", "quality_status": "SOURCE_VERIFIED",
                "permission_status": "SUPPORTED",
                "received_at": datetime(2026, 10, 23, 16, 15),
                "etl_sync_at": datetime(2026, 10, 24),
            },
            {
                "stock_code": "601326", "short_name": "秦港股份",
                "trade_time": "2026-10-23 15:00:00", "trade_date": "2026-10-23",
                "k_type": 1, "adjust_type": 0, "open": 0.0, "high": 0.0,
                "low": 0.0, "close": 3.57, "pre_close": 3.57,
                "volume": 0, "amount": 0.0, "change": 0.0,
                "change_pct": 0.0, "turnover_ratio": 0.0,
                "data_source": "tencent_qt", "quality_status": "SUSPENDED",
                "permission_status": "NO_TRADE",
                "received_at": datetime(2026, 10, 23, 16, 15),
                "etl_sync_at": datetime(2026, 10, 24),
            },
            {
                "stock_code": "920001", "short_name": "纬达光电",
                "trade_time": "2026-10-23 15:00:00", "trade_date": "2026-10-23",
                "k_type": 1, "adjust_type": 0, "open": 10.0, "high": 10.5,
                "low": 9.9, "close": 10.39, "pre_close": 10.03,
                "volume": 1881500, "amount": 18821120.0,
                "change": 0.36, "change_pct": 3.59, "turnover_ratio": 1.0,
                "data_source": "tencent_qt", "quality_status": "SOURCE_VERIFIED",
                "permission_status": "SUPPORTED",
                "received_at": datetime(2026, 10, 23, 16, 15),
                "etl_sync_at": datetime(2026, 10, 24),
            },
        ]
    )

    class Result:
        error_code = "0"
        error_msg = "success"

        def __init__(self, row):
            self._row = row
            self._read = False

        def next(self):
            if self._read:
                return False
            self._read = True
            return True

        def get_row_data(self):
            return self._row

    response_rows = {
        "sz.000001": [
            "2026-10-23", "sz.000001", "11.27", "11.35", "11.17", "11.20",
            "11.30", "144880560", "1628277173.38", "3", "1", "-0.88", "0.75",
        ],
        "sh.601326": [
            "2026-10-23", "sh.601326", "3.57", "3.57", "3.57", "3.57",
            "3.57", "", "", "3", "0", "", "",
        ],
    }
    fake = ModuleType("baostock")
    fake.login = lambda: Result([])
    fake.logout = lambda: Result([])
    fake.query_history_k_data_plus = lambda symbol, *_args, **_kwargs: Result(
        response_rows.get(symbol, [])
    )
    monkeypatch.setitem(sys.modules, "baostock", fake)
    monkeypatch.setattr(
        collector,
        "fetch_target_snapshot",
        lambda *_args, **_kwargs: (
            fallback.copy(),
            {
                "coverage_ratio": 1.0,
                "captured_at": "2026-10-23 16:15:00",
            },
        ),
    )

    frame, evidence = collector.fetch_baostock_primary_snapshot(
        ["000001", "601326", "920001"],
        target_date="2026-10-23",
    )

    rows = frame.set_index("stock_code")
    assert rows.loc["000001", "data_source"] == "baostock"
    assert rows.loc["601326", "quality_status"] == "SUSPENDED"
    assert rows.loc["601326", "permission_status"] == "NO_TRADE"
    assert rows.loc["920001", "data_source"] == "tencent_qt"
    assert evidence["baostock_count"] == 2
    assert evidence["tencent_fallback_count"] == 1
