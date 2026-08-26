from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_strategy_pool_defaults_to_strategy_selected_stocks_without_hiding_research_rows():
    script = (ROOT / "server/static/js/trading-v3.js").read_text(encoding="utf-8")
    page = (ROOT / "server/static/trading-v3.html").read_text(encoding="utf-8")

    assert "STRATEGY-SELECTED STOCK POOL" in page
    assert "策略选股池" in page
    assert 'value="RESEARCH_ONLY">研究观察（不可执行）' in page
    assert "item.is_strategy_candidate===true" in script
    assert "st==='REJECTED'&&item.actionability==='REJECTED'" in script
    assert "x.actionability!=='RESEARCH_ONLY'&&x.actionability!=='REJECTED'" in script
    assert "researchDefaultFallback=!st&&!preferredDefaultRows.length" in script
    assert "preferredDefaultRows.length?preferredDefaultRows:researchDefaultRows" in script
    assert ".filter(function(item){return item.actionability!=='RESEARCH_ONLY'})" not in script
    assert "研究观察（不可执行）" in script


def test_strategy_pool_uses_only_an_older_readable_batch_as_historical_fallback():
    script = (ROOT / "server/static/js/trading-v3.js").read_text(encoding="utf-8")

    assert "function stockPoolWithHistoricalFallback(requestedDate)" in script
    assert "return api3('/stock-pool').then" in script
    assert "latestSession>=target" in script
    assert "is_historical_fallback:true" in script
    assert "exact_run_missing:true" in script
    assert "历史只读 · 原" in script
    assert "HISTORICAL_READ_ONLY" in script
    assert "全部不可执行，也不会创建模拟或真实订单" in script
    assert "历史保护位 " in script
    assert "不可作为当前指令" in script


def test_strategy_pool_summary_exposes_each_decision_layer():
    script = (ROOT / "server/static/js/trading-v3.js").read_text(encoding="utf-8")
    page = (ROOT / "server/static/trading-v3.html").read_text(encoding="utf-8")

    assert 'id="candidateHistoryNotice"' in page
    assert 'id="candidateResearchNotice"' in page
    assert 'id="candidatePoolStats"' in page
    assert "summary.strategy_candidate_count" in script
    assert "summary.wait_trigger_count" in script
    assert "summary.target_count" in script
    assert "summary.rejected_count" in script
    assert "研究目标（不可直接下单）" in page
