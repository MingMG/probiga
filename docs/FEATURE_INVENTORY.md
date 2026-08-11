# ProBigA Feature Inventory

> 当前项目 A 的可比较功能清单。Feature ID 是跨项目比较、迁移和冲突分析的稳定引用键。  
> 页面/API/表名以当前工作区源码为准；未现场连接数据库的字段以源码查询和DDL为证据。

| 功能ID | 一级模块 | 二级/三级模块 | 功能名称 | 用户价值 | 使用角色 | 页面入口 | 后端接口 | 数据来源/存储 | 前置条件 | 输出结果 | 关联功能 | 当前问题 | 优化建议 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| F-001 | 市场研究 | 监控 | 市场监控 | 快速判断市场温度、宽度、主线和可信度 | 研究员/盘中观察者 | `monitor` | `/api/monitor/data`、`/api/hot-data/command-monitor` | `sm_market_overview_daily`、K线、复盘、质量表 | 有最新交易日数据 | 市场总览、情绪、宽度、主线、系统可信度 | F-002,F-013,F-026 | 依赖多组数据日期一致 | 统一 `DataContext` 和预聚合快照 |
| F-002 | 市场研究 | 盘中 | 盘中作战 | 将实时板块、候选和持仓放到同一执行页 | 盘中观察者 | `intraday-battle` | `/api/monitor/data`、`/api/sim-trade/*`、`/api/portfolio/*` | 实时快照、分钟K、推荐、组合 | 交易时段与实时源可用 | 候选、持仓、板块异动、QMT状态 | F-011,F-019,F-026 | 历史分钟覆盖需门禁 | 任务化刷新、WebSocket/增量推送 |
| F-003 | 热点分析 | 热榜 | 热股排行 | 观察市场关注度和热度扩散 | 研究员 | `fused` | `/api/hot-data/fused`、`fused-live`、`rank-*` | `st_hot_rank_*`、`st_hot_rank_fused` | 热榜任务成功 | 多源热榜、融合分、来源 | F-011,F-013 | 热榜源时效和日期可能不同 | 热榜统一快照批次 |
| F-004 | 热点分析 | 板块 | 板块分析/轮动 | 判断资金和情绪主线，先板块后个股 | 研究员/策略员 | `sector` | `/api/hot-data/sector-rotation`、`sector-heat-matrix`、`/api/sector/movement` | `si_concept_*`、`sm_concept_*`、板块热度/资金 | 板块代码、资金和K线可用 | 板块排名、资金、宽度、延续性、异动 | F-010,F-011,F-013 | 部分来源存在替代/停用任务 | 统一板块实体和历史成分版本 |
| F-005 | 热点分析 | 概念/行业 | 概念与行业成分 | 从概念/行业反查股票和行情 | 研究员 | `concept` | `/api/hot-data/concept-ths*`、`concept-stocks`、`concept-multi-day` | `si_concept_code_*`、`si_concept_constituent_*` | 概念目录和成分同步 | 概念行情、成分列表、多日变化 | F-004,F-011 | QMT概念目录与东财/同花顺可能不一致 | 统一概念主键和来源映射 |
| F-006 | 市场行为 | 龙虎榜 | 龙虎榜及机构席位 | 识别机构参与和资金席位行为 | 研究员 | `alist` | `/api/hot-data/a-list-daily`、`a-list-info` | `st_a_list_daily`、`st_a_list_info` | 龙虎榜同步 | 日期、股票、席位、净买卖 | F-007,F-011 | 机构拆分依赖字段兼容 | 建立标准席位实体和席位类型 |
| F-007 | 市场行为 | 资金 | 个股资金流与主力行为 | 判断资金承接、净流入和盘口深度 | 研究员/盘中观察者 | `capital`、`mainforce` | `/api/hot-data/capital-flow*`、`mainforce-*` | `sm_stock_capital_flow*`、`sm_stock_five_level` | 资金/盘口源有覆盖 | 日/实时资金、主力分析、全市场扫描 | F-002,F-011,F-019 | 分钟资金流曾不完整 | 资金指标统一单位、日期和来源质量 |
| F-008 | 个股研究 | 个股详情 | 个股详情与分钟走势 | 集中查看股票行情、技术、事件和分析 | 研究员 | 股票详情弹窗/全市场 | `/api/hot-data/stock-detail`、`stock-minute`、`stock-notices` | K线、分钟、财务、公告、推荐 | 股票代码有效 | 详情、技术、公告、风险、分钟数据 | F-011,F-017 | 重查询可能慢 | 详情快照和按需加载 |
| F-009 | 选股 | 规则选股 | 规则筛选与候选池 | 用可解释条件缩小全市场范围 | 策略员 | `screen`、强势股 | `/api/hot-data/screen-stocks`、`ai-screen` | K线、资金、热度、板块 | 目标交易日和字段完整 | 趋势/低位/量能/换手候选 | F-004,F-011 | 参数多、前端状态复杂 | 保存筛选方案和版本化条件 |
| F-010 | 选股 | 聚宽 | 聚宽策略与分钟同步 | 使用策略结果和分钟源辅助研究 | 策略员 | `jq-picks` | `/api/strategy/picks/*`、`/api/jq/minute/*` | `jq_strategy_meta`、`jq_strategy_picks`、`sm_stock_minute_gml` | 聚宽凭证/额度可用 | 策略列表、选股、分钟同步状态 | F-009,F-019 | 外部额度/数据覆盖不稳定 | 记录策略运行批次和数据快照 |
| F-011 | AI投研 | 推荐 | 批量AI推荐 | 从全市场生成结构化推荐池 | 研究员/策略员 | `recommended` | `/api/hot-data/recommended-stocks`、`run`、`gate`、`progress`、`run-history` | `stock_analysis_result`、`st_recommended_stocks` | 核心数据质量通过 | 推荐池、状态、得分、理由、交易计划 | F-004,F-007,F-013,F-019 | 规则多且字段演进快 | 版本化特征/模型/参数快照 |
| F-012 | AI投研 | 个股分析 | 统一四层分析 | 保证不同页面看到同一股票的分析口径 | 研究员 | 推荐/自选/详情共用 | `/api/hot-data/analysis-result`、`portfolio/analyze` | `StockDataLoader` + 多源表 | 个股数据可加载 | 长线、短线、事件、闸门、总结 | F-008,F-011,F-017 | 轻量/全量/批量路径可能漂移 | 统一 DTO 和黄金样本测试 |
| F-013 | 复盘 | 日报 | 市场复盘/专业复盘 | 将盘面事实转为主线、情绪、仓位和执行计划 | 研究员 | 复盘区域/命令行 | `/api/hot-data/daily-review*` | `st_daily_review`、`st_daily_review_pro` | 当日市场数据和表字段 | 结构化复盘、Markdown、图表 | F-001,F-004,F-011 | 历史验收出现过列缺失与日期滞后 | 集中schema迁移、固定复盘快照 |
| F-014 | 资讯 | 新闻 | 快讯与重要新闻 | 获取实时资讯并作为事件/复盘输入 | 研究员 | `news` | `/api/hot-data/news-flash`、`news-important`、`news-history` | `st_news_flash`、新浪/东财/财联社 | 网络和新闻表 | 新闻流、重要性、分类、推送 | F-011,F-013,F-015 | 新闻源重复、时效差异 | 统一新闻ID、发布时间上界与去重 |
| F-015 | 资讯 | 公告 | 个股公告 | 识别公告风险、利好和未来事件 | 研究员 | `notice`、个股详情 | `/api/hot-data/stock-notices` | `si_notice_eastmoney` | 公告同步且按分析日过滤 | 公告列表、风险关键词 | F-008,F-011 | 历史生产报告发现未来公告风险 | 强制 `notice_date <= as_of_date` |
| F-016 | AI投研 | 雷达 | 研报主题雷达 | 将主题、验证点和风险映射到股票 | 研究员 | `research-radar` | `/api/hot-data/research-radar` | `biz/research_radar`、新闻/市场数据 | 主题配置和市场命中 | 主题、证据等级、股票角色、风险 | F-004,F-011 | 主题证据不能单独构成买入理由 | 主题配置版本化和来源引用 |
| F-017 | 组合管理 | 自选 | 自选股与手工交易 | 管理关注列表、成本和人工交易记录 | 研究员 | `portfolio` | `/api/portfolio/list/add/remove/reorder/transact/live/refresh-prices` | `st_user_portfolio`、`st_portfolio_trans_log`、`st_trade_flow` | 股票代码和成本/股数合法 | 自选、成本、持仓、实时盈亏 | F-008,F-012,F-019 | 写入接口需鉴权；页面状态有历史残留 | 统一组合服务和写入审计 |
| F-018 | 组合管理 | 分析 | 自选股分析历史 | 跟踪组合股票的统一分析变化 | 研究员 | 自选详情 | `/api/portfolio/analyze/{stock_code}`、`analysis-history` | `st_portfolio_analysis_log`、统一引擎 | 股票在组合或可分析 | 当前分析、历史分析 | F-012,F-017 | 日志和推荐结果需要明确版本 | 绑定分析批次和模型版本 |
| F-019 | 模拟交易 | 信号池 | 模拟候选和买点 | 将推荐转成可执行/等待/卖出信号 | 策略员 | `sim-trade`、盘中作战 | `/api/sim-trade/candidates`、`recommendation-summary`、`scan` | `st_recommended_stocks`、`st_sim_signal` | 推荐状态、交易日、实时价格 | BUY_READY/WAIT/SELL_ALERT、理由 | F-011,F-020 | WATCH到BUY_READY偏保守是历史问题 | 增加观察池跟踪与二阶段确认 |
| F-020 | 模拟交易 | 执行 | 多策略委托/成交 | 在不触达真实账户的前提下验证执行纪律 | 策略员 | `sim-trade` | `/api/sim-trade/orders`、`positions`、`events`、`close` | `st_sim_order`、`st_sim_position`、`st_sim_event`、`st_trade_flow` | 交易时段、行情、风险预算 | 委托、成交、持仓、事件和盈亏 | F-019,F-021 | 依赖分钟/实时数据质量 | 抽象撮合器并保存输入快照 |
| F-021 | 模拟交易 | 回测/前向 | 回测和前向模拟 | 验证策略收益、回撤和可重复性 | 策略员 | `sim-trade` | `/api/sim-trade/backtest`、`backtest/report`、`forward/*` | 模拟交易表、日K/分钟K | 历史推荐和历史价格完整 | 胜率、收益、回撤、夏普、Profit Factor | F-019,F-020 | 历史分钟回放曾不完整，样本量需谨慎 | 独立run_id、防未来函数、固定数据集 |
| F-022 | AI投研 | 学习 | 失败归因与阈值校准 | 将推荐表现反馈到策略参数 | 策略员/负责人 | 推荐后台/任务 | 主要由批处理完成，参数查询 `/api/hot-data/strategy-runtime-params` | `st_ai_failure_samples`、`st_strategy_threshold_calibration`、`st_strategy_runtime_params` | 推荐后有足够交易日样本 | 失败标签、建议、当前参数 | F-011,F-021 | 自动发布需严格稳定性门槛 | 增加参数版本、审批和回滚 |
| F-023 | 运营 | 股评 | 股评画像与评估 | 管理外部股评画像、评估和推送 | 研究员/管理员 | `commentary` | `/api/commentary/*` | 画像表、新闻/市场数据、调度表 | 画像配置 | 评估、运行、启停、推送 | F-013,F-014 | 外部文本质量与失败降级需监控 | 保存原文、提示词版本和评分证据 |
| F-024 | 平台运维 | 数据源 | 数据源管理 | 查看各任务、提供商、统计、日志和健康 | 管理员 | `datasource` | `/api/datasource/*` | `st_scheduled_tasks`、history | 管理Token、调度表 | 分组状态、运行记录、日志 | F-026,F-025 | 任务状态可能混合空/禁用/失败 | 明确状态机和任务契约 |
| F-025 | 平台运维 | 调度 | 调度管理 | 管理Cron、间隔任务、手动运行和停止 | 管理员 | `scheduler` | `/api/scheduler/*` | `st_scheduled_tasks`、`st_scheduler_runtime` | 调度进程/内嵌线程 | 任务状态、心跳、质量、下一次运行 | F-024,F-026 | 单实例、补漏、时区边界复杂 | 统一JobRun和时区/交易日服务 |
| F-026 | 平台运维 | 健康 | 数据质量与系统健康 | 在推荐和交易前判断系统是否可信 | 管理员/研究员 | 监控、数据源、调度 | `/api/health/*`、`/api/scheduler/quality`、`/api/datasource/required-health` | `sys_data_*`、QMT诊断、调度心跳 | 数据库和任务状态可读 | PASS/WARN/FAIL、QMT能力、盘中准备度 | 所有核心功能 | 历史报告显示过质量门禁与实际数据不同步 | 将门禁作为所有下游入口的硬依赖 |
| F-027 | 平台运维 | QMT | QMT接入与审计 | 使用QMT行情/基础数据并可追溯能力与质量 | 管理员/数据工程师 | QMT健康/脚本 | `/api/health/qmt-*`、QMT工具链 | `qmt_*`、`sys_data_*`、本地历史库 | QMT客户端、SDK、权限、网关 | 能力台账、原始归档、覆盖率、缺口 | F-026,F-028 | 部分官方能力/权限仍待探测 | 按 capability_key 做契约和数据准入 |
| F-028 | 数据工程 | 同步 | 基础/行情/资讯同步 | 维护所有研究所需的数据资产 | 数据工程师/管理员 | 调度/命令行 | 多个 `biz.*`、`tools/*` 命令 | `si_*`、`sm_*`、`st_*` | 数据源配置、数据库、交易日 | 入库、覆盖率、任务状态 | F-003,F-007,F-011,F-026 | 同步脚本多、来源优先级复杂 | 声明式数据集目录和统一同步框架 |
| F-029 | 平台运维 | 部署 | 部署与远程运维 | 发布、查看部署运行和重启服务 | 管理员 | `deploy` | `/api/deploy/*`、GitHub Actions、`deploy/*` | 部署运行记录、日志 | SSH/SCP/GitHub secret/systemd | 部署状态、运行详情 | F-025,F-026 | 历史生产SSH/HTTP曾不可用；发布可复现性不足 | 制品化发布、版本哈希、回滚与灰度 |

## 功能迁移使用方式

比较项目 A/B 时，按 `功能ID` 对齐；若 B 没有同名功能但有等价能力，保留 A 的 ID 并记录 `equivalent_feature_id`。迁移决策至少补充：A/B存在性、数据契约、接口契约、权限依赖、冲突、复杂度、风险、优先级和验证用例。
