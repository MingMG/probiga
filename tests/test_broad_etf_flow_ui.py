# -*- coding: utf-8 -*-
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _text(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def test_broad_etf_flow_page_is_wired_into_both_sidebar_layouts():
    index = _text("server/static/index.html")
    script = _text("server/static/js/app.js")

    assert 'data-tab="broad-etf-flow"' in index
    assert 'id="tab-broad-etf-flow"' in index
    assert "'broad-etf-flow':'🏛 宽基资金监测'" in index
    assert script.count("{id:'broad-etf-flow'") == 2
    assert "'broad-etf-flow': function (d, c)" in script
    assert "PAGE_TITLES['broad-etf-flow'] = '🏛 宽基资金监测'" in script


def test_broad_etf_flow_ui_uses_the_documented_api_and_honest_method_copy():
    script = _text("server/static/js/app.js")

    assert "fetchJsonWithTimeout('/broad-etf-flow?trade_date='" in script
    assert "'&days=20', 45000)" in script
    assert "份额变化 × 前一交易日收盘价" in script
    assert "这是代理信号，不能识别最终持有人" in script
    assert "不能确认申购或赎回方身份" in script
    assert "不能据此断言国家队正在出货" not in script  # API evidence owns this dynamic wording.
    assert "var netValues = history.map(function (row) { return broadEtfFinite(row.net_amount); });" in script
    assert "['degraded', 'insufficient', 'error']" in script


def test_broad_etf_flow_styles_are_scoped_and_responsive():
    style = _text("server/static/css/style.css")

    assert "#tab-broad-etf-flow .bef-page" in style
    assert "#tab-broad-etf-flow .bef-chart-wrap" in style
    assert "@media (max-width: 1180px)" in style
    assert "@media (max-width: 768px)" in style
