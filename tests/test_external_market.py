from __future__ import annotations

from datetime import datetime

from biz.market_context.external_market import (
    EXTERNAL_MARKET_SYMBOLS,
    _YAHOO_FALLBACK_MAP,
    _index_items,
    _parse_eastmoney_vix_payload,
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
    }


def test_global_index_code_mapping_includes_japan_and_korea() -> None:
    items = _index_items([
        {"代码": "NDX", "名称": "纳斯达克", "最新价": 100, "涨跌幅": 1.2},
        {"代码": "N225", "名称": "日经225", "最新价": 100, "涨跌幅": -0.4},
        {"代码": "KS11", "名称": "韩国综合指数", "最新价": 100, "涨跌幅": 0.7},
    ])
    mapped = {item["symbol"]: item for item in items}
    assert mapped["nasdaq"]["change_pct"] == 1.2
    assert mapped["nikkei"]["change_pct"] == -0.4
    assert mapped["kospi"]["change_pct"] == 0.7


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
    item = _parse_eastmoney_vix_payload({
        "data": {"f43": 1682, "f60": 1705, "f170": -135, "f86": 1784735761},
    })
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
        captured_at=datetime.fromtimestamp(1786671060),
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
                        "regularMarketPrice": 30000.0,
                        "chartPreviousClose": 29000.0,
                        "regularMarketTime": 1787000000,
                    },
                    "timestamp": [1786400000, 1786486400, 1786572800],
                    "indicators": {"quote": [{"close": [27000.0, 27200.0, 27500.0]}]},
                }]
            }
        },
        captured_at=datetime.fromtimestamp(1786500000),
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
                        "regularMarketPrice": 46.43,
                        "regularMarketTime": 1786671000,
                    },
                    "timestamp": [1786498200, 1786584600],
                    "indicators": {"quote": [{"close": [46.20, 46.41]}]},
                }]
            }
        },
        captured_at=datetime.fromtimestamp(1786671060),
    )
    assert item is not None
    assert item["price"] == 4.643
    assert item["previous_close"] == 4.62
