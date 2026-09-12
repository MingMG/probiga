from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from biz.market_context import external_market as external

from biz.market_context.external_market import (
    EXTERNAL_MARKET_SYMBOLS,
    _YAHOO_FALLBACK_MAP,
    _parse_eastmoney_quote,
    _parse_yahoo_chart_payload,
    _score_snapshot,
)


def _item(symbol: str, change_pct: float) -> dict:
    return {
        "symbol": symbol,
        "display_name": symbol,
        "price": 100.0,
        "change_pct": change_pct,
        "availability": "available",
        "source": "eastmoney.quote.ulist",
    }


AS_OF = datetime(2026, 9, 12, 8, 30)


def _quote(symbol, **changes):
    market, code = external._EASTMONEY_QUOTE_IDS[symbol].split(".", 1)
    row = {"f12": code, "f13": int(market), "f2": 101.0, "f18": 100.0,
           "f3": 1.0, "f124": int(AS_OF.replace(tzinfo=ZoneInfo("Asia/Shanghai")).timestamp()) - 3600}
    return {**row, **changes}


def test_global_index_identity_includes_japan_and_korea() -> None:
    for symbol in ("nasdaq", "nikkei", "kospi"):
        item = _parse_eastmoney_quote(symbol, _quote(symbol), captured_at=AS_OF)
        assert item["symbol"] == symbol
        assert item["market_time"] == "2026-09-12 07:30:00"
        assert item["raw_code"] == external._EASTMONEY_QUOTE_IDS[symbol]


def test_external_market_score_is_supportive_or_risk_sensitive() -> None:
    supportive = [_item(symbol, 3.0) for symbol in ("nasdaq", "sp500", "dow", "nikkei", "kospi", "hang_seng")]
    risk = [_item(symbol, -3.0) for symbol in ("nasdaq", "sp500", "dow", "nikkei", "kospi", "hang_seng")]
    support_score, support_status, _ = _score_snapshot(supportive)
    risk_score, risk_status, _ = _score_snapshot(risk)
    assert support_score is not None and support_score > 50
    assert support_status == "SUPPORT"
    assert risk_score is not None and risk_score < 50
    assert risk_status == "RISK"


def test_external_market_score_is_unknown_without_core_changes() -> None:
    score, status, reason = _score_snapshot([_item("vix", 2.0)])
    assert score is None
    assert status == "UNKNOWN"
    assert "暂无" in reason


def test_external_market_score_uses_equity_futures_as_proxy() -> None:
    items = [
        _item("sp500_futures", 0.8),
        _item("nasdaq_futures", 1.2),
        _item("dow_futures", 0.4),
        _item("a50", 0.2),
    ]
    score, status, reason = _score_snapshot(items)
    assert score is not None and score > 50
    assert status == "SUPPORT"
    assert "代理" in reason


def test_eastmoney_vix_quote_is_scaled_to_index_units() -> None:
    item = _parse_eastmoney_quote("vix", _quote("vix", f2=16.82, f18=17.05, f3=-1.35), captured_at=AS_OF)
    assert item is not None
    assert item["price"] == 16.82
    assert item["previous_close"] == 17.05
    assert item["change_pct"] == -1.35


def test_high_vix_adds_external_risk_pressure() -> None:
    items = [_item(symbol, 0.0) for symbol in ("nasdaq", "sp500", "dow", "nikkei", "kospi", "hang_seng")]
    items.append({"symbol": "vix", "price": 30.0, "change_pct": 10.0, "availability": "available"})
    score, status, _ = _score_snapshot(items)
    assert score is not None and score <= 47.0
    assert status == "RISK"


def test_yahoo_fallback_uses_live_quote_before_cutoff() -> None:
    item = _parse_yahoo_chart_payload(
        "kospi",
        "韩国KOSPI",
        "^KS11",
        {
            "chart": {
                "result": [{
                    "meta": {
                        "symbol": "^KS11",
                        "regularMarketPrice": 2810.0,
                        "chartPreviousClose": 2782.0,
                        "regularMarketTime": 1786671000,
                        "exchangeTimezoneName": "Asia/Seoul",
                    },
                    "timestamp": [1786498200, 1786584600],
                    "indicators": {"quote": [{"close": [2750.0, 2782.0]}]},
                }]
            }
        },
        captured_at=datetime.fromtimestamp(1786671060, ZoneInfo("Asia/Shanghai")),
    )
    assert item is not None
    assert item["price"] == 2810.0
    assert round(item["change_pct"], 2) == 1.01
    assert item["source"] == "yahoo.finance.chart"


def test_yahoo_fallback_rejects_future_live_quote_during_replay() -> None:
    item = _parse_yahoo_chart_payload(
        "nasdaq",
        "美股纳斯达克",
        "^IXIC",
        {
            "chart": {
                "result": [{
                    "meta": {
                        "symbol": "^IXIC",
                        "regularMarketPrice": 30000.0,
                        "chartPreviousClose": 29000.0,
                        "regularMarketTime": 1787000000,
                    },
                    "timestamp": [1786400000, 1786486400, 1786572800],
                    "indicators": {"quote": [{"close": [27000.0, 27200.0, 27500.0]}]},
                }]
            }
        },
        captured_at=datetime.fromtimestamp(1786500000, ZoneInfo("Asia/Shanghai")),
    )
    assert item is None


def test_external_snapshot_includes_theme_specific_us_korea_japan_proxies() -> None:
    expected = {
        "us_lithium", "us_semiconductor", "us_ai", "us_robotics",
        "kr_battery", "kr_semiconductor", "jp_battery", "jp_semiconductor",
        "jp_robotics", "jp_auto", "taiwan_semiconductor",
    }
    assert expected <= {symbol for symbol, _name in EXTERNAL_MARKET_SYMBOLS}
    assert expected <= set(_YAHOO_FALLBACK_MAP)


def test_yahoo_tnx_is_normalized_to_percentage_points() -> None:
    item = _parse_yahoo_chart_payload(
        "us10y",
        "美国10年期国债收益率",
        "^TNX",
        {
            "chart": {
                "result": [{
                    "meta": {
                        "symbol": "^TNX",
                        "regularMarketPrice": 46.43,
                        "regularMarketTime": 1786671000,
                    },
                    "timestamp": [1786498200, 1786584600],
                    "indicators": {"quote": [{"close": [46.20, 46.41]}]},
                }]
            }
        },
        captured_at=datetime.fromtimestamp(1786671060, ZoneInfo("Asia/Shanghai")),
    )
    assert item is not None
    assert item["price"] == 4.643
    assert item["previous_close"] == 4.62


@pytest.mark.parametrize("change", [
    {"f13": 999}, {"f124": None}, {"f124": 1786671000},
    {"f124": int((AS_OF + timedelta(hours=1)).replace(tzinfo=ZoneInfo("Asia/Shanghai")).timestamp())},
    {"f2": "-"}, {"f2": float("nan")}, {"f18": 0},
])
def test_native_quote_rejects_wrong_identity_stale_clock_and_unusable_prices(change):
    with pytest.raises(ValueError):
        _parse_eastmoney_quote("sp500", _quote("sp500", **change), captured_at=AS_OF)


def test_native_provider_retries_transient_failure_and_keeps_partial_evidence(monkeypatch):
    responses = iter([ConnectionResetError("connection closed"), {"rc": 0, "data": {"diff": [_quote("sp500"), _quote("dow", f2="-")]}}])
    calls = []
    def get(request, **kwargs):
        calls.append(request.full_url)
        result = next(responses)
        if isinstance(result, Exception):
            raise result
        return result
    monkeypatch.setattr(external, "_public_json", get)
    monkeypatch.setattr(external.time, "sleep", lambda _: None)
    items, errors = external._load_eastmoney_quote_items(captured_at=AS_OF)
    assert set(items) == {"sp500"}
    assert len(calls) == 2
    assert all("fltt=2" in url for url in calls)
    assert any("ConnectionResetError" in error for error in errors)
    assert any("dow" in error and "price" in error for error in errors)
    assert any("nasdaq" in error and "missing" in error for error in errors)


def test_unsuccessful_native_payload_cannot_supply_quotes(monkeypatch):
    monkeypatch.setattr(external, "_public_json", lambda *_a, **_kw: {"rc": 1, "data": {"diff": [_quote("sp500")]}})
    monkeypatch.setattr(external.time, "sleep", lambda _: None)
    items, errors = external._load_eastmoney_quote_items(captured_at=AS_OF)
    assert not items
    assert errors


@pytest.mark.parametrize("available_count, expected_status", [(0, "FAILED"), (10, "PARTIAL"), (41, "COMPLETE")])
def test_snapshot_accounts_for_every_configured_symbol(monkeypatch, available_count, expected_status):
    available = {symbol: _item(symbol, 1.0) for symbol, _ in EXTERNAL_MARKET_SYMBOLS[:available_count]}
    monkeypatch.setattr(external, "_load_eastmoney_quote_items", lambda **_kw: (available, []))
    monkeypatch.setattr(external, "_load_twse_quote_item", lambda **_kw: (None, []))
    monkeypatch.setattr(external, "_load_yahoo_fallback_items", lambda *_a, **_kw: ({}, ["yahoo: HTTPError: 403"]))
    snapshot = external.fetch_external_market_snapshot(AS_OF)
    assert snapshot["acquisition_status"] == expected_status
    assert snapshot["available_count"] == available_count
    assert len(snapshot["items"]) == snapshot["expected_count"] == 41
    assert len(snapshot["missing_symbols"]) == 41 - available_count
    assert (snapshot["external_market_data_quality"] == "PASS") == (available_count == 41)
    if available_count < 41:
        assert "403" in snapshot["items"][-1]["payload"]["error"]


def _twse_payload(**changes):
    return {"rtcode": "0000", "msgArray": [{
        "c": "2330", "ch": "2330.tw", "ex": "tse", "d": "20260911", "t": "13:30:00",
        "tlong": "1789108200000", "z": "2410.0000", "y": "2450.0000", **changes,
    }]}


def test_twse_uses_exact_taiwan_listing_and_native_trade_clock():
    item = external._parse_twse_quote(_twse_payload(), captured_at=AS_OF)
    assert item["raw_code"] == "tse_2330.tw"
    assert item["price"] == 2410
    assert item["previous_close"] == 2450
    assert item["market_time"] == "2026-09-11 13:30:00"
    assert item["payload"]["source_updated_at"] == "2026-09-11 14:30:00"
    assert item["change_pct"] == pytest.approx(-1.6326530612)


@pytest.mark.parametrize("changes", [{"c": "2317"}, {"ex": "otc"}, {"z": "-"},
    {"y": "0"}, {"d": "20260801"}, {"tlong": None}, {"tlong": "1789156800000"}])
def test_twse_rejects_wrong_identity_unavailable_values_or_inconsistent_clock(changes):
    with pytest.raises(ValueError):
        external._parse_twse_quote(_twse_payload(**changes), captured_at=AS_OF)


def test_twse_source_completes_native_batch_without_contacting_unneeded_fallback(monkeypatch):
    items = {symbol: _parse_eastmoney_quote(symbol, _quote(symbol), captured_at=AS_OF)
             for symbol in external._EASTMONEY_QUOTE_IDS}
    monkeypatch.setattr(external, "_load_eastmoney_quote_items", lambda **_kw: (items, []))
    monkeypatch.setattr(external, "_load_twse_quote_item", lambda **_kw: (external._parse_twse_quote(_twse_payload(), captured_at=AS_OF), []))
    monkeypatch.setattr(external, "_fetch_yahoo_fallback_item", lambda *_a, **_kw: pytest.fail("all instruments have verified native quotes"))
    snapshot = external.fetch_external_market_snapshot(AS_OF)
    assert snapshot["acquisition_status"] == "COMPLETE"
    assert snapshot["available_count"] == snapshot["expected_count"] == 41
    assert snapshot["source_warnings"] == []
