from datetime import datetime, timedelta
from time import monotonic
from zoneinfo import ZoneInfo

import pytest

from server.common import liquidity_live

NOW = datetime(2026, 11, 19, 10, 30)
DAY = NOW.date().isoformat()


def rows(stamp=NOW, count=20):
    return [{"f12": f"{i:06d}", "f2": 10, "f8": i, "f10": i/10,
             "f124": int(stamp.replace(tzinfo=ZoneInfo("Asia/Shanghai")).timestamp())}
            for i in range(count)]


def test_liquidity_full_source_capture_is_durable_and_reused(tmp_path, monkeypatch):
    monkeypatch.setenv("PROBIGA_JOB_LOG_ROOT", str(tmp_path))
    calls = []
    def fetch(deadline, **kwargs):
        calls.append(deadline)
        return rows()
    monkeypatch.setattr(liquidity_live, "_fetch", fetch)
    first = liquidity_live.read_live_liquidity(DAY, now=NOW, deadline=monotonic()+22)
    again = liquidity_live.read_live_liquidity(DAY, now=NOW+timedelta(seconds=1), deadline=monotonic()+22)
    assert first == again and len(calls) == 1
    assert first["turnover_median_pct"] == 9.5
    liquidity_live.read_live_liquidity(DAY, now=NOW+timedelta(seconds=20), deadline=monotonic()+22)
    assert len(calls) == 2
    assert first["volume_ratio_median"] == pytest.approx(0.95)
    assert first["provider_total"] == first["dated_quote_count"] == 20


def test_missing_metric_preserves_other_valid_values():
    data = rows()
    data[0]["f8"] = "-"
    data[1]["f10"] = "-"
    result = liquidity_live._normalize(data, DAY, NOW)
    assert result["turnover_sample_count"] == result["volume_sample_count"] == 19
    assert result["turnover_median_pct"] == 10


@pytest.mark.parametrize("age_seconds", [59, 90])
def test_old_noncontributing_quote_does_not_expire_fresh_aggregate(age_seconds):
    data = rows()
    data[0].update(f8="-", f10="-", f124=int(
        (NOW-timedelta(seconds=age_seconds)).replace(tzinfo=ZoneInfo("Asia/Shanghai")).timestamp()))
    result = liquidity_live._normalize(data, DAY, NOW)
    assert result["as_of"] == NOW.replace(tzinfo=ZoneInfo("Asia/Shanghai")).isoformat()
    assert result["fresh_quote_count"] == 19
    assert result["excluded_quote_count"] == 1
    assert result["provider_total"] == 20


def test_old_contributors_are_excluded_and_source_range_uses_actual_contributors():
    data = rows(NOW-timedelta(seconds=5))
    data[0].update(f8=900, f10=900, f124=int(
        (NOW-timedelta(seconds=61)).replace(tzinfo=ZoneInfo("Asia/Shanghai")).timestamp()))
    data[1]["f124"] = int((NOW-timedelta(seconds=60)).replace(
        tzinfo=ZoneInfo("Asia/Shanghai")).timestamp())
    data[2].update(f8="-", f10="-", f124=int(
        (NOW-timedelta(seconds=59)).replace(tzinfo=ZoneInfo("Asia/Shanghai")).timestamp()))
    result = liquidity_live._normalize(data, DAY, NOW)
    earliest = (NOW-timedelta(seconds=60)).replace(tzinfo=ZoneInfo("Asia/Shanghai")).isoformat()
    latest = (NOW-timedelta(seconds=5)).replace(tzinfo=ZoneInfo("Asia/Shanghai")).isoformat()
    assert result["as_of"] == result["source_time_min"] == earliest
    assert result["source_time_max"] == latest
    assert result["fresh_quote_count"] == 18
    assert result["excluded_quote_count"] == 2
    assert result["dated_quote_count"] == 19
    assert result["turnover_sample_count"] == result["volume_sample_count"] == 18
    assert result["turnover_median_pct"] == 10.5


@pytest.mark.parametrize("missing_metrics", [False, True])
def test_less_than_ninety_percent_fresh_contributors_is_rejected(missing_metrics):
    data = rows()
    for item in data[:3]:
        if missing_metrics:
            item.update(f8="-", f10="-")
        else:
            item["f124"] = int((NOW-timedelta(seconds=61)).replace(
                tzinfo=ZoneInfo("Asia/Shanghai")).timestamp())
    with pytest.raises(ValueError, match="coverage"):
        liquidity_live._normalize(data, DAY, NOW)


@pytest.mark.parametrize("stamp", [NOW-timedelta(days=1), NOW-timedelta(minutes=4), NOW+timedelta(seconds=1)])
def test_widespread_wrong_date_stale_or_future_rows_fail(stamp):
    with pytest.raises(ValueError, match="coverage"):
        liquidity_live._normalize(rows(stamp), DAY, NOW)


def test_liquidity_retains_valid_lunch_observations():
    result = liquidity_live._normalize(rows(NOW.replace(hour=11, minute=30)), DAY, NOW.replace(hour=12))
    assert result["dated_quote_count"] == 20


def test_liquidity_uses_time_after_waiting_for_capture_owner(tmp_path, monkeypatch):
    elapsed = 0
    class Owner:
        def acquire(self, **kwargs):
            nonlocal elapsed
            elapsed = 10
            return True
        def release(self):
            pass
    monkeypatch.setenv("PROBIGA_JOB_LOG_ROOT", str(tmp_path))
    monkeypatch.setattr(liquidity_live, "_LOCK", Owner())
    monkeypatch.setattr(liquidity_live, "monotonic", lambda: elapsed)
    monkeypatch.setattr(liquidity_live, "_fetch", lambda deadline, **kwargs: rows(NOW+timedelta(seconds=8)))
    result = liquidity_live.read_live_liquidity(DAY, now=NOW, deadline=22)
    assert result["dated_quote_count"] == 20


def test_bulk_capture_requires_complete_unique_pagination(tmp_path, monkeypatch):
    monkeypatch.setenv("PROBIGA_JOB_LOG_ROOT", str(tmp_path))
    calls = []
    def request(url, params, **kwargs):
        calls.append(params["pn"])
        start = (params["pn"]-1)*100
        data = [{"f12": f"{i:06d}"} for i in range(start, start+100)]
        return {"rc": 0, "data": {"total": 3000, "diff": data}}
    monkeypatch.setattr(liquidity_live, "_resolved_curl_json", request)
    assert len(liquidity_live._fetch(monotonic()+22, selected=DAY, now=NOW)) == 3000
    assert sorted(calls) == list(range(1, 31))

    def duplicate(url, params, **kwargs):
        return {"rc": 0, "data": {"total": 3000, "diff": [{"f12": f"{i:06d}"} for i in range(100)]}}
    monkeypatch.setattr(liquidity_live, "_resolved_curl_json", duplicate)
    with pytest.raises(ValueError, match="duplicate"):
        liquidity_live._fetch(monotonic()+22, selected=DAY, now=NOW)


def test_bulk_failure_resumes_only_missing_pages(tmp_path, monkeypatch):
    monkeypatch.setenv("PROBIGA_JOB_LOG_ROOT", str(tmp_path))
    calls, fail = [], True
    def request(url, params, **kwargs):
        number = params["pn"]
        calls.append(number)
        if number == 9 and fail:
            raise TimeoutError("source page deadline")
        start = (number-1)*100
        return {"rc": 0, "data": {"total": 3000, "diff": [{"f12": f"{i:06d}"} for i in range(start, start+100)]}}
    monkeypatch.setattr(liquidity_live, "_resolved_curl_json", request)
    with pytest.raises(RuntimeError, match="missing pages.*9"):
        liquidity_live._fetch(monotonic()+22, selected=DAY, now=NOW)
    assert calls.count(9) == 2
    fail = False
    calls.clear()
    result = liquidity_live._fetch(monotonic()+22, selected=DAY, now=NOW+timedelta(seconds=2))
    assert sorted(calls) == [1, 9]
    assert len(result) == 3000


def test_liquidity_route_binds_intraday_requests_to_exact_session(monkeypatch):
    from server.api.routers import market_home
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 11, 19, 10, 30, tzinfo=tz)
    monkeypatch.setattr(market_home, "datetime", Clock)
    monkeypatch.setattr(market_home, "_trading_day", lambda *_: True)
    monkeypatch.setattr(market_home, "read_live_liquidity", lambda selected, **kwargs: {"trade_date": selected, "status": "available"})
    assert market_home._capture_liquidity(DAY)["trade_date"] == DAY
    assert market_home._capture_liquidity("2026-11-18")["status"] == "not_intraday"


def test_liquidity_failure_preserves_the_failed_stage_and_safe_source_reason(monkeypatch):
    from fastapi import HTTPException
    from server.api.routers import market_home
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 11, 19, 10, 30, tzinfo=tz)
    monkeypatch.setattr(market_home, "datetime", Clock)
    monkeypatch.setattr(market_home, "_trading_day", lambda *_: True)
    def failed(*args, **kwargs):
        raise ValueError("liquidity_live: insufficient current-session source coverage")
    monkeypatch.setattr(market_home, "read_live_liquidity", failed)
    with pytest.raises(HTTPException) as error:
        market_home._capture_liquidity(DAY)
    assert error.value.status_code == 503
    assert error.value.detail["stage"] == "liquidity_snapshot"
    assert error.value.detail["reason"] == "liquidity_live: insufficient current-session source coverage"
