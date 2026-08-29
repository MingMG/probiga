from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _javascript_block(script: str, start: str, end: str) -> str:
    return script.split(start, 1)[1].split(end, 1)[0]


def test_strategy_governance_has_a_dedicated_navigation_page():
    index = (ROOT / "server/static/index.html").read_text(encoding="utf-8")
    assert "🏆 动态策略竞技场" in index
    assert "旧候选中心（研究）" not in index
    assert "style.css?v=45" in index
    assert "app.js?v=117" in index


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
    assert "模拟可交易池" in script
    assert "INTERNAL_PORTFOLIO_CHECKPOINT_FACT_LEDGER_V3" in script
    assert "旧版内部账本，仅供历史展示" in script
    assert "20/60/120窗口证据" in script
    assert "strategyGovernanceWindowSummary(row)" in script
    assert "strategyCombinationConstraintSummary(row)" in script
    assert "相关性/个股重叠/行业" in script
    assert "行业侧重：" in script
    assert "客户端声明信号榜只比较经双人复核、结构可重算的外部提交" in script
    assert "提交来源未与权威行情逐行认证" in script
    assert "成交实证榜只比较内部模拟成交、实际费用和逐日净值" in script
    assert "共享账户里同票未成交不会被伪造成 fill" in script
    assert "信号榜不授予模拟资金" in script
    assert "不代表未来一定盈利" in script
    assert "成员事实链复算配方" in script
    assert "不生成独立组合现金事实" in script
    assert "recipe.recipe_hash" in script
    assert "观察池始终可用" not in script
    assert "观察池用于展示可审计研究候选且允许为空" in script
    assert "精确日期行业/概念成分归属" in script
    assert "来源特定板块热度" in script
    assert "按成分股聚合的强弱" in script
    assert "QMT原生 .BKZS 板块指数" in script
    assert "未认证的 .BKZS 不用合成曲线补齐" in script
    assert "strategyStatisticalProofSummary(row, metric, windowDays)" in script
    assert "单侧95%下界" in script
    assert "有效样本" in script
    assert "正式健康分" in script
    assert "点估计" in script
    assert "服务端BY多重检验" in script
    assert "全族" in script
    assert "内部非重叠时序" in script
    assert "精确间隔确认" in script
    assert "不可变日历回执已绑定" in script


def test_paper_execution_plan_distinguishes_blocked_unavailable_and_canonical():
    script = (ROOT / "server/static/js/app.js").read_text(encoding="utf-8")
    renderer = _javascript_block(
        script,
        "    function strategyPaperExecutionPlanHtml(governance) {",
        "    function strategyGovernanceHtml(governance, history) {",
    )

    assert 'data-execution-plan-state="blocked"' in renderer
    assert 'data-execution-plan-state="unavailable"' in renderer
    assert 'data-execution-plan-state="canonical"' in renderer
    assert "这不是一个已经验证的“0只空仓”结论" in renderer
    assert "页面不会把研究候选升级为执行目标" in renderer
    assert "规范空计划已验证" in renderer
    for contract in (
        "governance.is_canonical === true",
        "resultMode === 'CANONICAL_PERSISTED'",
        "probiga.governance-paper-execution-plan.v1",
        "planHash === topHash",
        "plan.automatic_real_order_submission === false",
        "plan.real_order_authority === false",
        "governance.automatic_real_order_submission === false",
        "governance.real_order_authority === false",
    ):
        assert contract in renderer


def test_paper_execution_plan_renders_every_target_and_exit_without_truncation():
    script = (ROOT / "server/static/js/app.js").read_text(encoding="utf-8")
    renderer = _javascript_block(
        script,
        "    function strategyPaperExecutionPlanHtml(governance) {",
        "    function strategyGovernanceHtml(governance, history) {",
    )

    assert "targets.forEach(function (row)" in renderer
    assert "exits.forEach(function (row)" in renderer
    assert "targets.slice(" not in renderer
    assert "exits.slice(" not in renderer
    for field in (
        "row.stock_code",
        "row.stock_name",
        "row.industry_name",
        "row.strategy_key",
        "row.strategy_version",
        "row.target_bp",
        "row.previous_target_bp",
        "row.new_buy_delta_bp",
        "row.reference_price",
        "row.reference_board_lot_quantity",
        "row.opportunity_score",
        "row.execution_score",
        "row.planned_risk_reward_ratio",
        "row.stop_loss_price",
        "row.take_profit_1",
        "row.take_profit_2",
    ):
        assert field in renderer
    assert "window._strategyCenterData" not in renderer


def test_large_rankings_use_revision_bound_server_pagination():
    script = (ROOT / "server/static/js/app.js").read_text(encoding="utf-8")
    governance_renderer = _javascript_block(
        script,
        "    function strategyGovernanceHtml(governance, history) {",
        "    function renderStrategyCenter(container, data, governance, history) {",
    )

    assert "strategyGovernancePaginationHtml" in governance_renderer
    assert "rankingPages.strategy" in governance_renderer
    assert "rankingPages.combination" in governance_renderer
    assert "每页最多 ' + limit + ' 条" in script
    assert "/api/strategy-center/governance/rankings/" in script
    assert "canonical_result_hash:String(governance.canonical_result_hash" in script
    assert "window._strategyGovernanceRankingPage" in script
    assert "window._strategyGovernanceRankingSearch" in script


def test_lifecycle_and_audit_history_are_independently_server_paged():
    script = (ROOT / "server/static/js/app.js").read_text(encoding="utf-8")
    governance_renderer = _javascript_block(
        script,
        "    function strategyGovernanceHtml(governance, history) {",
        "    function loadStrategyCenterPage(d, container) {",
    )

    assert "/api/strategy-center/governance/history/lifecycle?limit=50" in script
    assert "/api/strategy-center/governance/history/audit?limit=50" in script
    assert "window._strategyGovernanceHistoryPage" in script
    assert "strategyGovernanceHistoryPaginationHtml" in governance_renderer
    assert "historyPages.lifecycle" in governance_renderer
    assert "historyPages.audit" in governance_renderer
    assert "较新一页" in script
    assert "更早一页" in script
    assert "对象代码筛选" in script
    assert "lifecycle.slice(0, 30)" not in governance_renderer
    assert "audits.slice(0, 30)" not in governance_renderer
    assert "page.raw_payload_inline !== false" in script
    assert "result.automatic_real_order_submission !== false" in script
    assert "result.real_order_authority !== false" in script


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
        "执行适配器与模拟链成熟证据已就绪",
        "模拟链结构已就绪，证据积累中",
        "执行适配器未部署",
        "内部模拟链：",
        "模拟链校验失败",
        "成熟证据已通过复算",
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
        "双榜成绩",
        "成交实证",
        "独立信号",
        "行业侧重",
        "正常新增风险",
        "降权新增风险",
        "暂停新增风险",
        "数据未就绪",
        "等待人工复核",
    ):
        assert label in script
    for stale_label in (
        "执行与资金证据链已就绪",
        "影子候选执行已就绪",
        "执行适配器未部署/无效",
        "未接通，仅可影子观察",
        "资金证据链未接通",
    ):
        assert stale_label not in script
    for contract in (
        "execution_binding",
        "artifact_sha256",
        "strategy_version: payload.version",
        "commission_pct",
        "stamp_tax_pct",
        "slippage_pct",
        "transfer_fee_pct",
        "row.execution_evidence_rank",
        "row.signal_validation_rank",
        "row.independent_evidence_rank",
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
    assert "只有后续执行适配器在内部模拟账户产生可重算成交、费用和逐日净值" in script
    assert "才会进入模拟可交易池" in script
    assert "strategyTradingGateLabel(gate.status)" in script
    assert "evaluator_type: 'external_evidence'" not in script
    assert "Number((el('scRegRouteTrend') || {}).value)" not in script
    assert "result_mode:'CANONICAL_UNAVAILABLE'" in script
    assert "is_canonical:false" in script
    assert "governance.adapter_capabilities = capabilityPayload.adapters" in script
    assert "/api/strategy-center/governance/adapter-capabilities" in script
    assert "row.lane_rank || row.rank" not in script
    assert "盈利日占比（资金口径）" in script
    assert "日均净收益（资金口径）" in script
    assert "日频盈亏比" in script
    assert "日频PF" in script
    assert "逐笔胜率/盈亏比仅属于交易诊断或独立信号研究口径" in script


def test_governance_document_matches_the_executable_twenty_day_gate():
    document = (
        ROOT / "docs/dynamic_strategy_governance_v1.md"
    ).read_text(encoding="utf-8")
    for contract in (
        "内部成熟交易不少于 20 笔",
        "内部组合净值覆盖不少于 20 个权威交易日",
        "利润因子严格大于 1.00",
        "日均毛收益 - 1.5 × 内部账本实际日均成本",
        "按生成端四位小数口径重算",
        "策略和组合均调用同一个规范窗口门槛",
        "不代表未来一定盈利",
        "最佳 5 个盈利日贡献不超过 70%",
        "逐笔胜率、逐笔盈亏比和逐笔头部贡献",
    ):
        assert contract in document
    assert "外部 v3 声明既不是必要条件" in document
    assert "选择验证不少于" not in document


def test_strategy_governance_page_exposes_unattested_research_ledger():
    script = (ROOT / "server/static/js/app.js").read_text(encoding="utf-8")
    assert "外部研究声明复核台账" in script
    assert "等待独立复核" in script or "verification_status_label" in script
    assert "source_dataset_hash" in script
    assert "该哈希不是权威行情源认证" in script
    assert "不是内部账本资金资格的必要条件" in script
    assert "等待暂停后新证据自动恢复" in script
    assert "add('恢复影子'" not in script
    assert "EVIDENCE_REVIEWER" in script
    assert "仅管理员可治理" in script
    assert "请由证据复核员登录处理" in script
    assert "/api/auth/status" in script


def test_governance_rank_and_dynamic_evidence_labels_fail_closed():
    script = (ROOT / "server/static/js/app.js").read_text(encoding="utf-8")
    for contract in (
        "officialStrategyRank",
        "officialCombinationRank",
        "row.execution_evidence_comparable === true",
        "row.execution_evidence_rank",
        "row.signal_validation_rank",
        "row.has_independent_evidence === true ? row.independent_evidence_rank : null",
        "未入榜",
        "模拟链为空，正在积累首批证据",
        "影子试验已产生，等待成熟闭环",
        "模拟链证据无效，已阻断",
        "成熟证据已通过复算",
        "基础设施就绪、动态执行未启用",
        "真实下单权限保持关闭",
    ):
        assert contract in script
    assert "row.has_independent_evidence === true ? (row.lane_rank || row.rank) : null" not in script


def test_challenger_ui_uses_two_stage_artifact_replay_without_fake_claims():
    script = (ROOT / "server/static/js/app.js").read_text(encoding="utf-8")
    for contract in (
        "_strategyChallengerEvidenceSubmit",
        "/challengers/' + encodeURIComponent(challengerId) + '/evidence",
        "提交可重算产物",
        "REVIEW_PENDING",
        "decision:decision",
        "服务器重算结构与哈希",
        "复核通过最多晋级为无资金影子版本",
        "注册入口只允许从未存在过的新策略代码",
    ):
        assert contract in script
    for removed_claim in (
        "deflated_sharpe_probability",
        "probability_of_backtest_overfitting",
        "false_discovery_rate_q",
        "Deflated Sharpe",
        "DSR ",
        "PBO ",
        "FDR q",
    ):
        assert removed_claim not in script
    review_block = _javascript_block(
        script,
        "    window._strategyChallengerReview = function (challengerId) {",
        "\n    };\n\n    window._strategyChallengerPromote",
    )
    assert "metrics:" not in review_block
    assert "artifact_hash" not in review_block


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


def test_governance_ui_disables_mutations_during_deferred_database_mode():
    script = (ROOT / "server/static/js/app.js").read_text(encoding="utf-8")
    assert "governance.strategy_governance_mode || ''" in script
    assert "governance.activation_enabled === false" in script
    assert "authRole === 'ADMIN' && !governanceDeferred" in script
    assert "数据库防篡改门禁待完成" in script
    assert "基础表、字段、索引、初始化数据和版本标记已上线" in script
    assert "governance.base_schema_ready === true" in script
    assert "模拟资金保持 100% 现金" in script
    assert "真实下单与新买入均关闭" in script
