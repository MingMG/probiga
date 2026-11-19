from datetime import datetime, timedelta
from time import monotonic
from zoneinfo import ZoneInfo

import pytest

from server.common import turnover_live

NOW = datetime(2026, 11, 19, 10, 30)
DAY = NOW.date().isoformat()


def payload(stamp=NOW):
    return {"total": 5931, "rows": [
        {"f12": f"{i:06d}", "f14": f"stock{i}", "f2": 10, "f3": 1, "f8": 50-i,
         "f124": int(stamp.replace(tzinfo=ZoneInfo("Asia/Shanghai")).timestamp())}
        for i in range(1, 21)]}


def test_live_turnover_persists_ranked_source_and_reuses_checkpoint(tmp_path, monkeypatch):
    monkeypatch.setenv("PROBIGA_JOB_LOG_ROOT", str(tmp_path))
    calls = []
    def capture(timeout):
        calls.append(timeout)
        return payload()
    monkeypatch.setattr(turnover_live, "_fetch", capture)
    first = turnover_live.read_live_turnover(DAY, now=NOW, deadline=monotonic()+8)
    again = turnover_live.read_live_turnover(DAY, now=NOW+timedelta(seconds=1), deadline=monotonic()+8)
    assert first == again and calls == [3]
    assert first["rows"][0]["turnover_rate"] == 49
    turnover_live.read_live_turnover(DAY, now=NOW+timedelta(seconds=20), deadline=monotonic()+22)
    assert len(calls) == 2
    assert first["rows"][0]["turnover_rate_source"] == "eastmoney.push2.f8"
    assert first["provider_total"] == 5931 and first["observed_count"] == 20


@pytest.mark.parametrize("stamp", [NOW-timedelta(days=1), NOW-timedelta(minutes=4), NOW+timedelta(seconds=1)])
def test_wrong_date_stale_and_future_turnover_cannot_enter_ranking(stamp):
    with pytest.raises(ValueError, match="no fresh dated"):
        turnover_live._normalize(payload(stamp), DAY, NOW)


def test_one_invalid_turnover_does_not_block_valid_rows():
    raw = payload()
    raw["rows"][0]["f8"] = "-"
    result = turnover_live._normalize(raw, DAY, NOW)
    assert len(result["rows"]) == 19 and result["excluded_count"] == 1


def test_invalid_vendor_sort_order_is_rejected():
    raw = payload()
    raw["rows"][2]["f8"] = 99
    with pytest.raises(ValueError, match="order differs"):
        turnover_live._normalize(raw, DAY, NOW)
