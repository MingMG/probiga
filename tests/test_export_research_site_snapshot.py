from datetime import datetime, timezone
from urllib.parse import parse_qs, urlsplit

import pytest

from tools import export_research_site_snapshot as exporter


def test_dated_capture_keeps_source_gaps_and_labels_current_observation(monkeypatch):
    day = "2026-11-06"
    old = "2026-11-05"
    build = "a" * 40
    origin = "http://127.0.0.1"
    responses = {
        "/api/health": {"status": "ok", "release_revision": {"actual_git_sha": build,
                         "deployment_mode": "production", "matches_expected": True}},
        f"/api/monitor/board?date={day}": {"status": "available", "trade_date": day, "sample_size": 5573,
                                             "as_of": day + "T15:00:00+08:00", "operator_details": "private"},
        f"/api/hot-data/sector-rotation?date={day}": {"trade_date": old, "rising_sectors": [{"name": "old"}]},
        f"/api/market-stage/current?trade_date={day}": {"trade_date": day, "status": "DATA_BLOCKED"},
        "/api/market-stage/current": {"trade_date": day, "status": "READY", "evidence_mode": "CURRENT_OBSERVATION",
                                       "snapshot": {"state": "UPTREND"}},
        f"/api/hot-data/premarket-theme-forecast?session_date={day}": {"session_date": day, "themes": []},
        f"/api/v3/premarket/auction-gate?execution_session_date={day}": {
            "status": "blocked", "data": {"execution_session_date": day, "status": "DATA_BLOCKED", "reason": "无前日正式票池"}},
    }
    monkeypatch.setattr(exporter, "request_json", lambda url, **_: responses[url.removeprefix(origin)])
    snapshot = exporter.capture(day)
    sections = snapshot["sections"]
    assert snapshot["source_build"] == build
    assert sections["market_board"]["data"]["sample_size"] == 5573
    assert "operator_details" not in sections["market_board"]["data"]
    assert sections["sector_rotation"]["status"] == "DATA_BLOCKED"
    assert sections["sector_rotation"]["data"] == {}
    assert sections["market_stage"]["status"] == "PARTIAL"
    assert sections["market_stage"]["data"]["evidence_mode"] == "CURRENT_OBSERVATION"
    assert sections["premarket_forecast"]["status"] == "DATA_BLOCKED"
    assert sections["auction"]["reason"] == "无前日正式票池"


def test_uncertain_site_write_reconciles_exactly_once(monkeypatch):
    capture = {"trade_date": "2025-11-06", "captured_at": "2025-11-08T00:50:00Z", "sections": {}}
    response = {"snapshot": capture, "recorded_at": "2025-11-08T00:50:01Z"}
    calls = []

    def request(url, *, stage, deadline, body=None, token=None):
        calls.append(stage)
        assert url.startswith("https://research.example.chatgpt.site/api/probiga-snapshot")
        assert token == "site-scoped-test-token"
        if body:
            raise RuntimeError("site_write: timeout")
        assert parse_qs(urlsplit(url).query) == {
            "trade_date": [capture["trade_date"]], "captured_at": [capture["captured_at"]],
        }
        return response

    monkeypatch.setenv("PROBIGA_RESEARCH_SITE_ORIGIN", "https://research.example.chatgpt.site")
    monkeypatch.setenv("PROBIGA_RESEARCH_SITE_TOKEN", "site-scoped-test-token")
    monkeypatch.setattr(exporter, "request_json", request)
    assert exporter.publish(capture)["status"] == "COMPLETE"
    assert calls == ["site_write", "site_readback"]
    response["snapshot"] = {**capture, "trade_date": "2025-11-05"}
    with pytest.raises(RuntimeError, match="site_readback"):
        exporter.publish(capture)


def test_future_time_is_not_an_availability_claim():
    cutoff = datetime(2026, 11, 8, 0, 50, tzinfo=timezone.utc)
    assert exporter._time("2026-11-08 09:00:00", cutoff) is None
    assert exporter._time("2026-11-08 08:40:00", cutoff) == "2026-11-08T08:40:00+08:00"


def test_ladder_projection_keeps_exact_verified_stock_names():
    day = "2026-11-06"
    ladder = {"trade_date": day, "source": "东财收盘涨停池 · QMT 收盘价复核", "receipt_sha256": "a" * 64,
              "rows": [{"streak": 5, "count": 2, "stocks": ["梯队股票甲", "梯队股票乙"]}]}
    output = exporter._market_board({"trade_date": day, "limit_ladder": ladder})["limit_ladder"]
    assert output["source"] == ladder["source"]
    assert output["receipt_sha256"] == ladder["receipt_sha256"]
    assert output["rows"] == [{"streak": 5, "count": 2,
                               "stocks": [{"short_name": "梯队股票甲"}, {"short_name": "梯队股票乙"}]}]
    assert not exporter._market_board({"trade_date": "2026-11-05", "limit_ladder": ladder})["limit_ladder"]["rows"]
