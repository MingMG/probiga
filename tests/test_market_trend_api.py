# -*- coding: utf-8 -*-
from datetime import datetime as real_datetime, timedelta
import json

from server.api.routers import hot_data
from server.engine.market_trend import DEFAULT_INDEX_NAMES


class _IntradayDatetime(real_datetime):
    @classmethod
    def now(cls, tz=None):
        value = cls(2026, 9, 4, 10, 30, 0)
        return value if tz is None else value.replace(tzinfo=tz)


def _index_rows():
    rows = []
    start = real_datetime(2026, 1, 2)
    dates = []
    current = start
    while current.date().isoformat() <= "2026-09-04":
        if current.weekday() < 5:
            dates.append(current.date().isoformat())
        current += timedelta(days=1)
    for code_offset, code in enumerate(DEFAULT_INDEX_NAMES):
        for index, trade_date in enumerate(dates):
            rows.append(
                {
                    "index_code": code,
                    "trade_date": trade_date,
                    "close": 2000 + code_offset * 100 + index,
                }
            )
    return rows


def test_market_trend_api_keeps_intraday_same_day_bar_provisional(monkeypatch):
    hot_data._cache_drop("market_trend_2026-09-04")

    def fake_read(sql, params=None, **kwargs):
        if "FROM sm_index_kline" in sql:
            return _index_rows()
        if "FROM si_trade_calendar" in sql:
            return [{"next_trade_date": "2026-09-07"}]
        if "FROM st_market_state_daily" in sql:
            return []
        raise AssertionError(sql)

    monkeypatch.setattr(hot_data, "_read_sql", fake_read)
    monkeypatch.setattr(hot_data, "datetime", _IntradayDatetime)
    monkeypatch.setattr(hot_data, "_market_clock_trade_date_from_calendar", lambda _day: "2026-09-04")
    monkeypatch.setattr(hot_data, "_market_phase", lambda *_args: ("intraday", "盘中", True))

    result = hot_data._market_trend_result("2026-09-04")
    assert result["status"] == "ok"
    assert result["source"]["daily_close_confirmed"] is False
    assert result["source"]["daily_close_basis"] == "market_clock:intraday"
    assert result["data_cutoff"] == "2026-09-04"
    assert result["methodology"]["indicators"][0]["formula"]
    assert all(
        item["periods"]["daily"]["confirmation_status"] == "provisional"
        for item in result["indices"]
    )
    assert all(
        item["periods"]["weekly"]["confirmation_status"] == "provisional"
        for item in result["indices"]
    )
    assert all(item["periods"]["daily"]["evidence"] for item in result["indices"])
    assert result["retained_history_status"] == "not_yet_recorded"


def test_retained_history_returns_the_original_summary_and_later_price_change(monkeypatch):
    observation = {
        "evidence_type": "market_trend_snapshot",
        "schema_version": "market-trend-observation-v1",
        "indices": [
            {
                "index_code": "000300",
                "summary": {"daily": "日线：当时下行。"},
                "periods": {"daily": {"metrics": {"close": 1000}}},
            }
        ],
    }
    current = {
        "indices": [
            {
                "index_code": "000300",
                "periods": {"daily": {"metrics": {"close": 1100}}},
            }
        ]
    }
    monkeypatch.setattr(
        hot_data,
        "_read_sql",
        lambda *_args, **_kwargs: [
            {
                "trade_date": "2026-09-01",
                "run_uid": "a" * 32,
                "created_at": "2026-09-01 18:00:00",
                "evidence_json": json.dumps(["原市场证据", observation], ensure_ascii=False),
            }
        ],
    )

    status, history = hot_data._retained_market_trend_history(
        "2026-09-04", current
    )

    assert status == "available"
    assert history[0]["indices"][0]["summary"]["daily"] == "日线：当时下行。"
    assert history[0]["indices"][0]["subsequent_change_pct"] == 10.0
