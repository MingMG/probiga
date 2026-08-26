from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]


def _javascript_block(script: str, start: str, end: str) -> str:
    return script.split(start, 1)[1].split(end, 1)[0]


def test_stock_detail_renders_current_watchlist_execution_strategy():
    script = (ROOT / "server/static/js/app.js").read_text(encoding="utf-8")
    renderer = _javascript_block(
        script,
        "    function renderStockDetailExecutionStrategy(d) {",
        "    function renderStockDetail(body, d) {",
    )

    assert 'id="stockDetailExecutionStrategy"' in renderer
    assert "d.holding_strategy" in renderer
    assert "d.watch_analysis" in renderer
    assert "d.holding_strategy_context" in renderer
    for field in (
        "strategy.action",
        "strategy.reason",
        "strategy.sell_plan",
        "strategy.emergency_exit",
        "strategy.next_session_plan",
        "strategy.knowledge_cutoff",
        "strategy.execution_authority",
    ):
        assert field in renderer
    assert "当前执行策略" in renderer
    assert "未持仓盯盘建议" in renderer
    assert "持仓执行策略暂不可用" in renderer
    assert "页面不会用旧分析冒充今日执行策略" in renderer
    assert "不会自动下单" in renderer
    assert "escHtml(strategy.action" in renderer
    assert "escHtml(stockDetailStrategyReason" in renderer
    assert "analysis_snapshot" not in renderer
    assert "ai_analysis" not in renderer


def test_stock_detail_places_execution_strategy_before_stale_ai_analysis():
    script = (ROOT / "server/static/js/app.js").read_text(encoding="utf-8")
    detail_renderer = _javascript_block(
        script,
        "    function renderStockDetail(body, d) {",
        "    window.closeStockDetail = function",
    )

    strategy_position = detail_renderer.index(
        "h += renderStockDetailExecutionStrategy(d);"
    )
    ai_position = detail_renderer.index("// ── 七、AI投资分析（置顶）──")
    assert strategy_position < ai_position


def test_stock_detail_context_failure_runtime_renders_explicit_unavailable_card():
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required for the JavaScript runtime regression")

    script = (ROOT / "server/static/js/app.js").read_text(encoding="utf-8")
    renderer_start = "    function stockDetailStrategyRange(plan) {"
    renderer = (
        "function escHtml(value) { return String(value == null ? '' : value)"
        ".replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }\n"
        + renderer_start
        + _javascript_block(
            script,
            renderer_start,
            "    function renderStockDetail(body, d) {",
        )
        + "\nconst html = renderStockDetailExecutionStrategy({"
        "watchlist_member: null, portfolio_context_status: 'unavailable', "
        "holding_strategy_context: {reason_code: 'PORTFOLIO_CONTEXT_UNAVAILABLE'}"
        "});\n"
        "if (!html.includes('stockDetailExecutionStrategy') || "
        "!html.includes('执行策略不可用') || !html.includes('页面已清除缓存中的旧持仓和旧动作')) "
        "throw new Error('portfolio context failure rendered an empty strategy area');\n"
    )
    completed = subprocess.run(
        [node, "-e", renderer],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    assert completed.returncode == 0, completed.stderr


def test_stock_detail_unverified_quote_runtime_freezes_action_without_hiding_holding():
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required for the JavaScript runtime regression")

    script = (ROOT / "server/static/js/app.js").read_text(encoding="utf-8")
    renderer_start = "    function stockDetailStrategyRange(plan) {"
    renderer = (
        "function escHtml(value) { return String(value == null ? '' : value)"
        ".replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }\n"
        + renderer_start
        + _javascript_block(
            script,
            renderer_start,
            "    function renderStockDetail(body, d) {",
        )
        + "\nconst html = renderStockDetailExecutionStrategy({"
        "watchlist_member: true, portfolio_snapshot_stale: true, "
        "holding: {shares: 1200}, holding_strategy_context: "
        "{reason_code: 'PORTFOLIO_QUOTE_UNVERIFIED'}"
        "});\n"
        "if (!html.includes('动作冻结') || !html.includes('持仓信息已读取') || "
        "!html.includes('不会基于未验证价格生成或复用买卖动作') || "
        "!html.includes('最近一次缓存快照')) "
        "throw new Error('unverified quote did not render the fail-closed state');\n"
    )
    completed = subprocess.run(
        [node, "-e", renderer],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    assert completed.returncode == 0, completed.stderr
