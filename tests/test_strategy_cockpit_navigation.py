from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_strategy_navigation_is_reduced_to_five_user_tasks():
    index = (ROOT / "server/static/index.html").read_text(encoding="utf-8")
    app = (ROOT / "server/static/js/app.js").read_text(encoding="utf-8")
    desk = (ROOT / "server/static/trading-v3.html").read_text(encoding="utf-8")

    expected = ["今日策略", "我的持仓", "策略池", "盘中应急", "连续跟踪"]
    for label in expected:
        assert label in index
        assert label in app
        assert label in desk

    visible_desk_routes = desk.split("</nav>", 1)[0]
    assert visible_desk_routes.count('class="nav') == 5
    assert "目标组合" not in visible_desk_routes
    assert "模拟订单" not in visible_desk_routes
    assert "回测验收" not in visible_desk_routes
    assert "数据与系统" not in visible_desk_routes


def test_today_strategy_cockpit_prioritizes_holdings_and_actions():
    desk = (ROOT / "server/static/trading-v3.html").read_text(encoding="utf-8")
    script = (ROOT / "server/static/js/trading-v3.js").read_text(encoding="utf-8")

    for text in [
        "先处理持仓，再看新机会",
        "今日决策依据",
        "今天第一动作",
        "自选股真实持仓",
        "今天需动作",
        "允许新买",
        "盘中退出红线",
        "买入 / 加仓范围",
        "卖出范围",
        "盘中突发退出",
        "下一交易日",
    ]:
        assert text in desk

    assert "todayFirstAction" in script
    assert "urgentActionCount" in script
    assert "intradayRedlineCount" in script
    assert "/api/portfolio/holding-strategy" in script


def test_research_and_system_pages_are_details_not_primary_navigation():
    desk = (ROOT / "server/static/trading-v3.html").read_text(encoding="utf-8")

    assert '<details class="system-tools">' in desk
    assert '<details class="research-details">' in desk
    assert "查看研究依据、实验与目标明细" in desk
    assert "查看批次、时间和系统证据" in desk
