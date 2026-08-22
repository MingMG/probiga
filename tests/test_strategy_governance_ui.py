from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _javascript_block(script: str, start: str, end: str) -> str:
    return script.split(start, 1)[1].split(end, 1)[0]


def test_strategy_governance_has_a_dedicated_navigation_page():
    index = (ROOT / "server/static/index.html").read_text(encoding="utf-8")
    assert "🏆 动态策略竞技场" in index
    assert "旧候选中心（研究）" not in index
    assert "style.css?v=44" in index
    assert "app.js?v=105" in index


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


def test_default_visible_pool_never_falls_back_to_overview_candidates():
    """Overview stock A may exist, but the normative pool reads canonical only."""

    script = (ROOT / "server/static/js/app.js").read_text(encoding="utf-8")
    render = _javascript_block(
        script,
        "    function renderStrategyCenter(container, data, governance, history) {",
        "    window._renderStrategyCenterCandidates = function",
    )
    pool_renderer = _javascript_block(
        script,
        "    window._strategyPoolShow = function (level, button) {",
        "\n    };\n\n    window._strategyRegistrationToggle",
    )

    canonical_position = render.index("strategyGovernanceHtml(governance, history)")
    diagnostic_position = render.index(
        '<details id="scResearchInputDiagnostics" class="sc-research-diagnostics">'
    )
    assert canonical_position < diagnostic_position
    assert "研究输入诊断（非规范结果，默认收起）" in render
    assert '<details id="scResearchInputDiagnostics" class="sc-research-diagnostics" open>' not in render
    assert "不参与默认排名、规范票池或资金分配展示" in render
    assert "未治理研究输入明细（非规范）" in render
    assert "<span>候选股票池</span>" not in render
    assert "<span>策略状态</span>" not in render

    # Executable negative contract: even if overview candidates contain stock A,
    # the normative renderer has no path to overview/data and selects only the
    # canonical governance pool.
    overview = {"candidates": [{"stock_code": "A"}]}
    canonical = {"pools": {"observation": []}}
    assert overview["candidates"][0]["stock_code"] == "A"
    assert canonical["pools"]["observation"] == []
    assert "window._strategyCenterData" not in pool_renderer
    assert "var governance = window._strategyCenterGovernance || {};" in pool_renderer
    assert "var selectedPool = (governance.pools || {})[level];" in pool_renderer


def test_canonical_pool_renders_all_101_rows_without_silent_cap():
    script = (ROOT / "server/static/js/app.js").read_text(encoding="utf-8")
    pool_renderer = _javascript_block(
        script,
        "    window._strategyPoolShow = function (level, button) {",
        "\n    };\n\n    window._strategyRegistrationToggle",
    )
    canonical_rows = [
        {"stock_code": str(index).zfill(6)} for index in range(101)
    ]

    assert len(canonical_rows) == 101
    assert "slice(0, 100)" not in pool_renderer
    assert "slice(0,100)" not in pool_renderer
    assert "var rows = Array.isArray(selectedPool) ? selectedPool : [];" in pool_renderer
    assert "rows.forEach(function (row)" in pool_renderer
    assert "规范票池完整展示 ' + rows.length + ' 条" in pool_renderer


def test_strategy_governance_page_exposes_execution_and_profit_contracts():
    script = (ROOT / "server/static/js/app.js").read_text(encoding="utf-8")
    for label in (
        "执行适配器",
        "执行与资金证据链已就绪",
        "影子候选执行已就绪",
        "执行适配器未部署/无效",
        "资金证据链：",
        "未接通，仅可影子观察",
        "影子候选可运行，资金证据链未接通",
        "当前规范结果",
        "实时预览（不生效）",
        "动态适配器运行回执",
        "零候选已留痕",
        "逐成员",
        "制品 SHA-256",
        "成本模型代码",
        "市场门禁：",
        "风险上限：",
        "胜率",
        "盈亏比",
        "行业侧重",
        "正常新增风险",
        "降权新增风险",
        "暂停新增风险",
        "数据未就绪",
        "等待人工复核",
    ):
        assert label in script
    for contract in (
        "execution_binding",
        "artifact_sha256",
        "strategy_version: payload.version",
        "commission_pct",
        "stamp_tax_pct",
        "slippage_pct",
        "transfer_fee_pct",
        "row.lane_rank || row.rank",
        "row.industry_focus",
        "funding_pipeline_ready",
        "governance.adapter_capabilities",
        "history.adapter_run_receipts",
        "row.member_sleeves",
        "sleeve_row_hash",
        "scRegEvaluatorType",
        "执行适配器、版本、制品或评估器类型不在服务器可信发布清单中",
    ):
        assert contract in script
    assert "未通过执行绑定及内部模拟成交验证前不会进入票池" in script
    assert "strategyTradingGateLabel(gate.status)" in script
    assert "evaluator_type: 'external_evidence'" not in script
    assert "Number((el('scRegRouteTrend') || {}).value)" not in script
    assert "result_mode:'CANONICAL_UNAVAILABLE'" in script
    assert "is_canonical:false" in script
    assert "governance.adapter_capabilities = capabilityPayload.adapters" in script
    assert "/api/strategy-center/governance/adapter-capabilities" in script


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
