from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_ai_navigation_is_prioritized_and_groups_are_collapsible():
    html = (ROOT / "server" / "static" / "index.html").read_text(encoding="utf-8")
    app_js = (ROOT / "server" / "static" / "js" / "app.js").read_text(encoding="utf-8")
    css = (ROOT / "server" / "static" / "css" / "style.css").read_text(encoding="utf-8")

    assert "var PRIMARY_NAV_ORDER = ['ai-stock', 'portfolio', 'trading', 'command'];" in app_js
    assert "arrangePrimaryNavigation(LAYOUT_OLD);" in app_js
    assert "arrangePrimaryNavigation(LAYOUT_NEW);" in app_js
    assert "window.toggleSidebarGroup" in app_js
    assert "probiga_sidebar_group_state_v1" in app_js
    assert 'class="sidebar-group-title sidebar-group-toggle"' in app_js
    assert "LOADERS['ai-stock']" in app_js
    assert "LOADERS['ai-general']" in app_js
    assert "/ai-stock?embedded=1" in app_js
    assert "/ai-general?embedded=1" in app_js
    assert 'data-tab="ai-stock"' in html
    assert 'data-tab="ai-general"' in html
    assert 'id="tab-ai-stock"' in html
    assert 'id="tab-ai-general"' in html
    assert ".sidebar-group.collapsed .sidebar-group-items" in css
    assert ".ai-embedded-frame" in css
    assert '/static/css/style.css?v=36' in html
    assert '/static/js/app.js?v=85' in html
