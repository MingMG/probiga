from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_strategy_governance_has_a_dedicated_navigation_page():
    index = (ROOT / "server/static/index.html").read_text(encoding="utf-8")
    assert "🏆 动态策略竞技场" in index
    assert "旧候选中心（研究）" not in index
    assert "style.css?v=44" in index
    assert "app.js?v=104" in index


def test_strategy_governance_page_uses_chinese_lifecycle_values():
    script = (ROOT / "server/static/js/app.js").read_text(encoding="utf-8")
    for label in ("正常运行", "降权运行", "影子观察", "暂停使用", "已淘汰"):
        assert label in script
    assert "/api/strategy-center/governance" in script
    assert "/api/strategy-center/registry" in script
    assert "无合格策略保持现金" in script
    assert "'strategy-center': function (d, c)" in script
    loader = script.split("'strategy-center': function (d, c)", 1)[1].split("},", 1)[0]
    assert "loadStrategyCenterPage(d, c)" in loader
    assert "loadCandidateCenterPage(d, c)" not in loader
    assert "trade_date:''" in script
    assert "strategyLifecycleLabel(row.current_status)" in script
    assert "strategyLifecycleLabel(row.previous_status)" in script
    assert "row.status_label || '影子观察'" not in script


def test_strategy_governance_page_exposes_both_arenas_and_three_pools():
    script = (ROOT / "server/static/js/app.js").read_text(encoding="utf-8")
    assert "单策略竞技场" in script
    assert "组合策略竞技场" in script
    assert "新增组合 / 新版本" in script
    assert "_strategyCombinationRegister" in script
    assert "观察池" in script
    assert "等待确认池" in script
    assert "可交易池" in script
    assert "20/60/120窗口证据" in script
    assert "strategyGovernanceWindowSummary(row)" in script
    assert "strategyCombinationConstraintSummary(row)" in script
    assert "相关性/个股重叠/行业" in script
    assert "行业侧重：" in script
    assert "正期望、盈亏比和利润因子只说明已确认的历史前向证据" in script
    assert "不代表未来一定盈利" in script


def test_governance_document_matches_the_executable_twenty_day_gate():
    document = (
        ROOT / "docs/dynamic_strategy_governance_v1.md"
    ).read_text(encoding="utf-8")
    for contract in (
        "内部成熟交易不少于 20 笔",
        "内部组合净值覆盖不少于 20 个权威交易日",
        "选择验证不少于 20 笔且覆盖不少于 20 个权威交易日",
        "利润因子严格大于 1.00",
        "毛期望 - 1.5 × 内部账本实际成本",
        "按生成端四位小数口径重算",
        "策略和组合均调用同一个规范窗口门槛",
        "不代表未来一定盈利",
    ):
        assert contract in document


def test_strategy_governance_page_exposes_independent_evidence_ledger():
    script = (ROOT / "server/static/js/app.js").read_text(encoding="utf-8")
    assert "独立证据复核台账" in script
    assert "等待独立复核" in script or "verification_status_label" in script
    assert "source_dataset_hash" in script
    assert "等待暂停后新证据自动恢复" in script
    assert "add('恢复影子'" not in script
    assert "EVIDENCE_REVIEWER" in script
    assert "仅管理员可治理" in script
    assert "请由证据复核员登录处理" in script
    assert "/api/auth/status" in script


def test_governance_history_shows_canonical_revision_state_in_chinese():
    script = (ROOT / "server/static/js/app.js").read_text(encoding="utf-8")
    engine = (
        ROOT / "server/engine/strategy_governance.py"
    ).read_text(encoding="utf-8")
    assert "当前生效" in script
    assert "已被替代" in script
    assert "修订号" in script
    assert "supersedes_run_uid" in script
    assert "run_revision, supersedes_run_uid" in engine
    assert "is_canonical, market_state" in engine
