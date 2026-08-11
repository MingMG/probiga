from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_watch_advice_modal_keeps_its_own_vertical_scroll_area():
    css = (ROOT / "server" / "static" / "css" / "style.css").read_text(encoding="utf-8")
    scroll_rule = css.split(".modal-body.pf-watch-modal-body {", 1)[1].split("}", 1)[0]

    assert "overflow-y: auto" in scroll_rule
    assert "overflow-x: hidden" in scroll_rule
    assert "-webkit-overflow-scrolling: touch" in scroll_rule


def test_portfolio_market_bar_uses_backend_total_and_discloses_freshness():
    js = (ROOT / "server" / "static" / "js" / "app.js").read_text(encoding="utf-8")

    assert "md.total_count" in js
    assert "md.flat_count" in js
    assert "md.data_time" in js
    assert "md.is_realtime" in js
    assert "cache:'no-store'" in js
    assert "fetchRawJsonWithTimeout('/api/monitor/data?_=' + now, 8000" in js
    assert "upc + dnc + flt" not in js
    assert "±1%内" in js


def test_portfolio_funds_do_not_present_stale_rows_as_current_attitude():
    js = (ROOT / "server" / "static" / "js" / "app.js").read_text(encoding="utf-8")

    assert "暂无今日资金" in js
    assert "已过期" in js
    assert "今流 " in js
    assert "基线建立中" in js
    assert "正在积累5分钟变化基线" in js
    assert "过期数据不参与判断" in js
    assert "最近日资金" not in js


def test_portfolio_live_polling_pauses_when_hidden_or_market_closed():
    js = (ROOT / "server" / "static" / "js" / "app.js").read_text(encoding="utf-8")
    portfolio_js = js.split("portfolio: function", 1)[1].split("datasource: function", 1)[0]

    assert "function pfPageIsHidden()" in portfolio_js
    assert "!hidden && pfIsActiveTab() && isTradingTime()" in portfolio_js
    assert "setTimeout(tick, pfAutoRefreshDelayMs())" in portfolio_js
    assert "document.addEventListener('visibilitychange'" in portfolio_js
    assert "setInterval(function()" not in portfolio_js


def test_portfolio_missing_quotes_are_not_rendered_as_zero_or_qmt():
    js = (ROOT / "server" / "static" / "js" / "app.js").read_text(encoding="utf-8")
    portfolio_js = js.split("portfolio: function", 1)[1].split("datasource: function", 1)[0]

    assert "function pfNumberOrNull(v)" in portfolio_js
    assert "if (!s) return '来源未标注';" in portfolio_js
    assert "String(source || 'gj_qmt')" not in portfolio_js
    assert "Number(r.cur_price || 0)" not in portfolio_js
    assert "Number(r.cur_price||0)" not in portfolio_js
    assert "chg == null ? '-'" in portfolio_js


def test_portfolio_mutations_force_a_full_reconciliation():
    js = (ROOT / "server" / "static" / "js" / "app.js").read_text(encoding="utf-8")

    assert "function pfReloadAfterMutation()" in js
    assert js.count("return pfReloadAfterMutation();") >= 4
    assert "fetchRawJsonWithTimeout('/api/portfolio/remove/'" in js
