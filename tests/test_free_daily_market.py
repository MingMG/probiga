from datetime import datetime

from server.common.verified_daily_market_truth import is_suspended_daily_row
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
