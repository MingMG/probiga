from datetime import datetime, timedelta
from time import monotonic
from zoneinfo import ZoneInfo

import pytest

from server.api.routers import market_home
from server.common import index_live

DAY = "2026-11-19"
NOW = datetime(2026, 11, 19, 10, 30)


def snapshot(stamp=NOW):
    return [{"f12": code, "f13": 0 if code.startswith("399") else 1,
             "f2": 101, "f18": 100, "f3": 1,
             "f124": int(stamp.replace(tzinfo=ZoneInfo("Asia/Shanghai")).timestamp())}
            for code in market_home.INDEX_NAMES]


def test_capture_all_seven_indices_and_reuse_durable_snapshot(tmp_path, monkeypatch):
    monkeypatch.setenv("PROBIGA_JOB_LOG_ROOT", str(tmp_path))
    calls = []
    def fetch(names, timeout):
        calls.append(timeout)
        return snapshot()
    monkeypatch.setattr(index_live, "_fetch", fetch)
    first = index_live.read_live_indices(market_home.INDEX_NAMES, DAY, now=NOW, deadline=monotonic()+8)
    again = index_live.read_live_indices(market_home.INDEX_NAMES, DAY, now=NOW+timedelta(seconds=1), deadline=monotonic()+8)
    assert first == again and len(first) == 7
    assert calls == [3]
    assert {row["source"] for row in first} == {"eastmoney.push2.index"}
    assert {row["as_of"] for row in first} == {DAY + "T10:30:00+08:00"}


@pytest.mark.parametrize("stamp,count", [(NOW-timedelta(minutes=4), 0),
                                         (NOW-timedelta(days=1), 0),
                                         (NOW+timedelta(seconds=1), 0), (NOW, 7)])
def test_date_future_and_staleness_contract(stamp, count):
    assert len(index_live._normalize(snapshot(stamp), market_home.INDEX_NAMES, DAY, NOW)) == count


def test_lunch_retains_same_session_pause_and_invalid_member_is_isolated():
    rows = snapshot(NOW.replace(hour=11, minute=30))
    rows[0]["f13"] = 0
    rows[1]["f3"] = 20
    rows[2]["f2"] = float("nan")
    usable = index_live._normalize(rows, market_home.INDEX_NAMES, DAY, NOW.replace(hour=12))
    assert len(usable) == 4


def test_bulk_transport_requests_all_explicit_index_markets(monkeypatch):
    calls = []
    def request(url, params, **kwargs):
        calls.append((url, params, kwargs))
        return {"rc": 0, "data": {"total": 7, "diff": snapshot()}}
    monkeypatch.setattr(index_live, "_resolved_curl_json", request)
    assert len(index_live._fetch(market_home.INDEX_NAMES, 3)) == 7
    assert len(calls) == 1
    assert '1.000001' in calls[0][1]['secids'] and '0.399001' in calls[0][1]['secids']
    assert '1.000680' in calls[0][1]['secids'] and '1.000688' in calls[0][1]['secids']


def test_same_day_intraday_board_exposes_indices_even_without_stock_quotes(monkeypatch):
    monkeypatch.setattr(market_home, "_trading_day", lambda *_: True)
    monkeypatch.setattr(market_home, "_live_quotes", lambda *_: ([], None))
    monkeypatch.setattr(market_home, "_limit_up_events", lambda *_: [])
    monkeypatch.setattr(market_home, "_market_events", lambda *_: [])
    monkeypatch.setattr(market_home, "read_live_indices", lambda *args, **kwargs:
                        index_live._normalize(snapshot(), market_home.INDEX_NAMES, DAY, NOW))
    result = market_home.build_market_board(NOW.date(), now=NOW)
    assert result["status"] == "partial"
    assert len(result["indices"]) == 7
    assert result["as_of_basis"] == "public_index_snapshot"
    assert result["as_of"] == DAY + "T10:30:00+08:00"
    assert "indices" not in result["unavailable_fields"]
