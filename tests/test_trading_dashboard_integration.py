from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from server.api.admin_auth import PROTECTED_PAGE_PATHS
from server.api.main import app, trading_v2_page, trading_v3_page


ROOT = Path(__file__).resolve().parents[1]


def test_legacy_trading_pages_redirect_into_main_dashboard():
    for response in (trading_v2_page(), trading_v3_page()):
        assert response.status_code == 307
        assert response.headers["location"] == "/?tab=trading"


def test_trading_routes_remain_login_protected():
    assert "/trading-v2" in PROTECTED_PAGE_PATHS
    assert "/trading-v3" in PROTECTED_PAGE_PATHS


def test_main_page_owns_strategy_and_paper_trading_dashboard():
    html = (ROOT / "server" / "static" / "index.html").read_text(encoding="utf-8")
    app_js = (ROOT / "server" / "static" / "js" / "app.js").read_text(encoding="utf-8")

    assert 'id="tab-trading"' in html
    assert html.index("交易决策") < html.index("自选管理") < html.index("市场概览")
    assert "策略与模拟" in html
    assert 'href="/trading-v2"' not in html
    assert "function tradingDeskName" in app_js
    assert "row.stock_name" in app_js
    assert "function loadTradingModulePage" in app_js
    assert "window.openTradingModule" in app_js
    assert "id=\"tradeFullWorkbench\"" not in app_js
    assert "fetch('/static/trading-' + modulePage + '.html?embedded=1'" in app_js
    assert "frame.srcdoc = html" in app_js
    assert "'probiga-trading-' + modulePage + '-view'" in app_js
    assert "window.openTradingV2Module" in app_js
    assert 'scrolling="auto"' in app_js
    assert "frame._contentResizeObserver" in app_js
    assert "Math.min(2600" not in app_js
    assert html.count('data-trading-view=') == 12
    for view in (
        "overview", "hypotheses", "candidates", "intraday", "portfolio",
        "positions", "orders", "validation", "missed", "evidence",
    ):
        assert f"data-tab=\"trading-v3-{view}\"" in html
        assert f"data-trading-view=\"{view}\"" in html
        assert f"id:'trading-v3-{view}', modulePage:'v3', tradingView:'{view}'" in app_js
    for view in ("etf", "operations"):
        assert f"data-tab=\"trading-shared-{view}\"" in html
        assert f"id:'trading-shared-{view}', modulePage:'v2', tradingView:'{view}'" in app_js
    assert "这只股票走到哪一步了" in app_js
    assert "策略选了什么" in app_js
    assert "模拟买了什么" in app_js
    assert "入选 → 委托 → 成交 → 持仓" in app_js

    old_layout = app_js.split("var LAYOUT_OLD = [", 1)[1].split("var LAYOUT_NEW = [", 1)[0]
    new_layout = app_js.split("var LAYOUT_NEW = [", 1)[1].split("function ensureLayoutItem", 1)[0]
    assert old_layout.index("group:'交易决策'") < old_layout.index("group:'自选管理'") < old_layout.index("group:'市场分析'")
    assert new_layout.index("group:'交易决策'") < new_layout.index("group:'自选管理'") < new_layout.index("group:'市场概览'")
    assert old_layout.count("id:'portfolio'") == 1
    assert new_layout.count("id:'portfolio'") == 1


def test_v3_truth_modules_replace_old_v2_production_tabs():
    app_js = (ROOT / "server" / "static" / "js" / "app.js").read_text(encoding="utf-8")
    html = (ROOT / "server" / "static" / "index.html").read_text(encoding="utf-8")
    v3_html = (ROOT / "server" / "static" / "trading-v3.html").read_text(encoding="utf-8")
    v3_js = (ROOT / "server" / "static" / "js" / "trading-v3.js").read_text(encoding="utf-8")
    v3_css = (ROOT / "server" / "static" / "css" / "trading-v3.css").read_text(encoding="utf-8")
    legacy_html = (ROOT / "server" / "static" / "trading-v2.html").read_text(encoding="utf-8")
    legacy_js = (ROOT / "server" / "static" / "js" / "trading-v2.js").read_text(encoding="utf-8")
    production_views = [
        "overview", "hypotheses", "candidates", "intraday", "portfolio",
        "positions", "orders", "validation", "missed", "evidence",
    ]

    for view in production_views:
        assert f'data-view="{view}"' in v3_html
        assert f'id="view-{view}"' in v3_html
        assert f"id:'trading-v3-{view}'" in app_js
    assert 'data-tab="trading-v2-trust"' not in html
    assert "LOADERS[item.id]" in app_js
    assert "v3-embedded" in v3_html
    assert "probiga-trading-v3-resize" in v3_js
    assert 'id="candidateCards"' in v3_html
    assert 'id="candidatePager"' in v3_html
    assert 'data-embedded-view="candidates"' in v3_css
    assert ".candidate-list{display:grid" in v3_css
    assert ".candidate-table-wrap{display:none}" in v3_css
    assert "new URL(document.referrer).origin" in v3_js
    assert "},targetOrigin)" in v3_js
    assert "},location.origin)" not in v3_js
    assert "(frame.title || '交易模块') + '已加载'" in app_js
    assert "v2-embedded" in legacy_html
    assert "probiga-trading-v2-resize" in legacy_js
    assert "window.parent!==window" in legacy_html and "window.parent!==window" in v3_html
    assert "正式组合最多 6 只" not in v3_html
    assert "— / 4" not in legacy_html
    assert "{'511880':1}" not in legacy_js


def test_only_embedded_trading_pages_allow_same_origin_framing():
    client = TestClient(app, follow_redirects=False)

    for page in ("v2", "v3"):
        embedded = client.get(f"/static/trading-{page}.html?embedded=1")
        ordinary = client.get(f"/static/trading-{page}.html")
        assert embedded.headers["x-frame-options"] == "SAMEORIGIN"
        assert embedded.headers["content-security-policy"] == "frame-ancestors 'self'"
        assert ordinary.headers["x-frame-options"] == "DENY"


def test_only_embedded_ai_pages_allow_same_origin_framing():
    client = TestClient(app, follow_redirects=False)

    for path in ("/ai-stock", "/ai-general"):
        embedded = client.get(f"{path}?embedded=1")
        ordinary = client.get(path)
        assert embedded.headers["x-frame-options"] == "SAMEORIGIN"
        assert embedded.headers["content-security-policy"] == "frame-ancestors 'self'"
        assert ordinary.headers["x-frame-options"] == "DENY"


def test_recommendation_ui_never_upgrades_score_or_signal_without_all_buy_gates():
    app_js = (ROOT / "server" / "static" / "js" / "app.js").read_text(
        encoding="utf-8"
    )

    assert "function hasExplicitNewBuyGate(row)" in app_js
    assert "recommend === 'ALLOW'" in app_js
    assert "chase === 'ALLOW' && ordinary" in app_js
    assert "rows.filter(hasExplicitNewBuyGate)" in app_js
    assert "四门确认（成交前复验）" in app_js
    assert "✅ 推荐买入" not in app_js
    assert "⚡ 谨慎买入" not in app_js
    assert (
        "/BUY_READY|CONFIRM/.test(String(r.signal_status || r.recommend_status || ''))"
        not in app_js
    )
