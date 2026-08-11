from __future__ import annotations

from biz.market_context.external_market import _index_items, _parse_eastmoney_vix_payload, _score_snapshot


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
