# ProBigA Database Inventory

> 本清单按源码中的 SQL、查询字段、建表和迁移逻辑整理。当前没有连接数据库做现场 `SHOW TABLES/SHOW COLUMNS`；部署迁移前必须对实际库做差异核对。

## 表族总览

| 表族 | 表 | 作用 | 典型写入者 | 典型消费者 |
|---|---|---|---|---|
| `si_*` 基础 | `si_all_code` | 股票代码、简称、市场和状态主数据 | `biz.stock_info.sync_*`、QMT参考同步 | 全市场、K线、分析、组合 |
| `si_*` 基础 | `si_all_index_code` | 指数代码和基础信息 | 股票信息/QMT参考同步 | 指数行情、市场监控 |
| `si_*` 基础 | `si_trade_calendar` | 交易日历/交易状态 | 股票信息/QMT | 所有日期解析、调度、回测 |
| `si_*` 基础 | `si_index_constituent` | 指数成分/权重 | 股票信息/QMT | 指数、板块、分析 |
| `si_*` 基础 | `si_concept_code_east`、`si_concept_code_ths` | 概念目录 | 股票信息、概念同步 | 概念页、板块分析 |
| `si_*` 基础 | `si_concept_constituent_east`、`si_concept_constituent_ths` | 概念成分 | 股票信息、QMT | 概念成分、板块聚合 |
| `si_*` 基础 | `si_stock_concept_east`、`si_stock_concept_ths`、`si_stock_concept_map` | 股票—概念映射 | 股票信息/QMT | 分析、选股、主题 |
| `si_*` 基础 | `si_industry_sw`、`si_stock_plate_east` | 行业/板块归属 | 股票信息/QMT | 行业估值、板块先行 |
| `si_*` 基础 | `si_stock_finance` | 财务核心指标 | 财务同步/QMT | 长线、基本面、估值 |
| `si_*` 基础 | `si_stock_holder` | 股东人数 | 股东同步 | 筹码风险、事件引擎 |
| `si_*` 基础 | `si_stock_shares` | 总股本/流通股本 | 股票信息/QMT | 市值、流动性、仓位 |
| `si_*` 基础 | `si_notice_eastmoney` | 个股公告 | 公告同步 | 事件风险、公告页 |
| `sm_*` 行情 | `sm_stock_kline` | 股票日/周期K线 | 行情同步、QMT、日K补数 | 技术特征、复盘、回测、详情 |
| `sm_*` 行情 | `sm_stock_current` | 当前/快照行情 | 实时批量/QMT | 监控、模拟交易、组合 |
| `sm_*` 行情 | `sm_rt_quote_snapshot` | 带时间的实时快照归档 | 实时批量 | 实时兜底、模拟成交、审计 |
| `sm_*` 行情 | `sm_stock_minute`、`sm_stock_minute_gm`、`sm_stock_minute_gml` | 分钟K线 | 聚宽/QMT/爬取工具 | 盘中、前向模拟、技术确认 |
| `sm_*` 行情 | `sm_stock_capital_flow`、`sm_stock_capital_flow_daily`、`sm_stock_capital_flow_min` | 个股资金流 | 多源资金工具 | 资金页、分析、质量门禁 |
| `sm_*` 行情 | `sm_stock_five_level` | 五档盘口 | QMT/实时工具 | 盘口深度、流动性门禁 |
| `sm_*` 行情 | `sm_index_current`、`sm_index_kline` | 指数行情 | 行情/QMT | 市场风格、复盘、监控 |
| `sm_*` 行情 | `sm_concept_east_current`、`sm_concept_east_kline`、`sm_concept_ths_current`、`sm_concept_ths_kline` | 概念行情 | 概念工具/QMT | 板块、概念页 |
| `sm_*` 行情 | `sm_concept_capital_flow_east` | 概念资金流 | 东财数据中心 | 板块先行、概念资金 |
| `sm_*` 行情 | `sm_dividend` | 分红方案/现金分红 | 行情/QMT | 价值评分、股息证据 |
| `sm_*` 衍生 | `sm_market_overview_daily` | 日市场概览 | `refresh_market_overview_daily.py` | 监控、质量、新鲜度 |
| `st_*` 热点 | `st_hot_rank_ths`、`st_hot_rank_sina`、`st_hot_rank_xq`、`st_hot_pop_rank_east` | 多源热股排行 | 热榜工具 | 热榜页、情绪、推荐 |
| `st_*` 热点 | `st_hot_rank_fused`、`st_hot_rank_multi_day` | 融合/多日热榜 | `merge_hot_rank.py` | 热榜、短线情绪 |
| `st_*` 热点 | `st_hot_concept_ths_daily`、`st_hot_concept_ths_rt` | 热门概念 | 同花顺工具 | 概念页、市场情绪 |
| `st_*` 热点 | `st_hot_stats` | 热点统计 | 热点任务 | 统计/诊断 |
| `st_*` 市场资讯 | `st_a_list_daily`、`st_a_list_info` | 龙虎榜列表和明细 | 舆情/QMT/同步任务 | 龙虎榜、资金、分析 |
| `st_*` 市场资讯 | `st_news_flash` | 多源快讯 | 新闻同步/API刷新 | 新闻、复盘、事件、AI文本 |
| `st_*` 市场资讯 | `st_north_flow_daily` | 北向流向 | 舆情/QMT | 市场风格、资金证据 |
| `st_*` 市场资讯 | `st_securities_margin` | 两融 | 舆情/QMT | 筹码资金、风险 |
| `st_*` 市场资讯 | `st_mine_clearance_tdx` | 扫雷/风险 | 舆情同步 | 事件/推荐闸门 |
| `st_*` 市场资讯 | `st_stock_lifting_last_month` | 解禁 | 舆情同步 | 解禁风险 |
| `stock_analysis_result` | 统一分析结果 | 单股长线、短线、事件、推荐总结 | `sync_analysis_fast.py` | 分析页、组合、推荐 |
| `st_recommended_stocks` | 推荐池快照 | 推荐、策略、买点、风险、证据、收益回填 | `sync_analysis_fast.py` | 推荐页、模拟交易 |
| `st_daily_review` | 基础复盘 | 市场统计、情绪周期、主线、仓位和摘要 | `biz.review.generate` | 复盘页、监控 |
| `st_daily_review_pro` | 专业复盘 | 详细Markdown/结构化专业结论 | `biz.review.generate` | 专业复盘、晚报 |
| `st_ai_failure_samples` | AI失败样本 | 推荐失败、标签、收益、原因 | 分析批处理 | 阈值校准、诊断 |
| `st_event_impact_relations` | 事件产业链关系 | 公告/新闻触发词到受益/受损/替代对象 | 分析初始化/配置 | 事件风险、推荐解释 |
| `st_strategy_threshold_calibration` | 阈值校准 | 窗口样本、收益、胜率、建议 | 分析批处理 | 参数发布 |
| `st_strategy_runtime_params` | 生效参数 | 当前阈值、来源、生效日、元数据 | 分析批处理 | 推荐批量读取 |
| `st_strategy_snapshot` | 策略统计快照 | 交易次数、胜率、收益、持仓 | 模拟交易 | 模拟交易统计 |
| `st_user_portfolio` | 自选/组合 | 股票、排序、持仓、成本等 | API | 自选页、组合分析 |
| `st_portfolio_trans_log` | 组合交易日志 | 手工买卖、价格、股数、日期 | API | 组合回顾、流水 |
| `st_portfolio_analysis_log` | 组合分析日志 | 自选股票的分析历史 | API | 分析历史 |
| `st_trade_flow` | 交易流水 | 模拟/手工交易统一流水 | 模拟交易/API | 统计、审计 |
| `st_sim_signal` | 模拟信号 | 推荐转策略信号、评分、买卖计划 | 模拟交易准备 | 扫描/候选 |
| `st_sim_order` | 模拟订单 | 委托、成交、价格、费用、拒绝原因 | 模拟交易引擎 | 订单页、统计 |
| `st_sim_position` | 模拟持仓 | 买入、卖出、盈亏、持仓天数 | 模拟交易引擎 | 持仓/回测 |
| `st_sim_event` | 模拟事件 | 买卖、风控、异常和解释 | 模拟交易引擎 | 事件页、诊断 |
| `st_sim_risk_budget` | 风险预算 | 总资产、现金、策略预算、已用/可用 | 模拟交易引擎 | 风险预算页 |
| `st_scheduled_tasks` | 调度定义 | 任务、脚本、Cron/间隔、启停、最近状态 | 调度工具/API | 调度、数据源管理 |
| `st_scheduled_task_history` | 调度历史 | 任务执行记录/日志 | 调度器 | 日志、质量 |
| `st_scheduler_runtime` | 调度心跳 | 实例、主机、进程、心跳、并发 | 调度器 | 健康、API重启安全 |
| `sys_data_sync_run` | 数据同步批次 | provider、dataset、run、状态、时间 | QMT/质量工具 | 对账/质量 |
| `sys_data_coverage` | 覆盖率 | 期望/实际/缺失/覆盖率/状态 | QMT reconciliation | 质量门禁 |
| `sys_data_gap` | 数据缺口 | 数据集、日期、范围、原因、修复状态 | QMT reconciliation | 缺口队列/补数 |
| `sys_data_quality_result` | 质量结果 | 规则、状态、消息、批次 | QMT/质量工具 | 质量门禁、诊断 |
| `qmt_raw_manifest` | QMT原始归档清单 | API参数、版本、哈希、文件、批次 | QMT raw store | 审计、重放 |
| `qmt_api_registry` | QMT API注册表 | capability、权限、目标表、消费者 | QMT catalog | 能力台账 |
| `qmt_api_capability` | QMT能力结果 | 支持状态、返回字段、错误、版本、探测时间 | QMT diagnostics/catalog | 数据准入、健康 |
| `qmt_sector_list`、`qmt_sector_member` | QMT板块参考 | 板块目录、成分 | QMT参考同步 | 板块映射 |
| `qmt_instrument_detail`、`qmt_index_weight` | QMT合约/指数参考 | 合约详情、指数权重 | QMT参考同步 | 基础数据/指数 |
| `qmt_local_stock_kline` | 本地QMT日K历史 | 本地大体量历史K线 | `local_history.py` | 补数、研究 |
| `qmt_local_stock_minute` | 本地QMT分钟历史 | 本地大体量分钟线 | `local_history.py` | 前向/回放 |
| `qmt_local_backfill_run` | 本地补数批次 | 补数运行状态、数据集、进度 | 本地补数工具 | QMT历史诊断 |

## 关键业务表字段

### `stock_analysis_result`

源码写入字段包括：

`stock_code`、`stock_name`、`analysis_date`、`last_news_time`、`long_term_score`、`fundamental_score`、`growth_score`、`valuation_score`、`risk_score`、`short_term_score`、`capital_score`、`technical_score`、`sentiment_score`、`event_score`、`event_risk_score`、`event_risk_level`、`event_risk_detail`、`recommend_status`、`recommend_reason`、`summary`、`recommendation`、`strengths`、`risks`、`data_quality_score`、`data_quality_flags`、`flow_trade_date`、`hot_trade_date`、`model_version`、`created_at`、`updated_at`。

### `st_recommended_stocks`

除股票、日期、基础分数和推荐理由外，源码确保/写入：

- 状态：`recommend_status`、`signal_status`、`investment_rating`、`sector_gate_status`。
- 评分：`ai_score`、`long_term_score`、`short_term_score`、`quality_score`、`entry_score`、`final_trade_score`、`main_wave_score`、`trend_hold_score`、`confidence_score`、`heat_overload_score`、`data_quality_score`。
- 交易计划：`entry_price_low/high`、`stop_loss_price`、`take_profit_1/2`、`position_weight`、`max_holding_days`、`risk_reward_ratio`、`expected_return_pct`、`resistance_price`。
- 策略：`primary_strategy`、`strategy_profile`、`suitable_strategies`、`main_wave_stage/signal/reason`、`trend_stop_price`、`trend_reduce_price`。
- 证据：`technical_evidence_json`、`evidence_chain_json`、`entry_conditions_json`、`sell_rules_json`、`invalidation_reason`、`data_quality_flags`。
- 学习：`review_1d_pct`、`review_3d_pct`、`review_5d_pct`、`review_10d_pct`、`failure_tags_json`、`cooldown_days_left`、`cooldown_until`、`model_version`。

### 模拟交易表族

| 表 | 关键字段 |
|---|---|
| `st_sim_signal` | `trade_mode`、`signal_date`、`trade_date`、`stock_code`、`strategy_type`、`status`、评分、风险、买入区间、止损、止盈、成交关联ID |
| `st_sim_order` | `signal_id`、模式、订单日期/时间、股票、策略、方向、限价/目标价、请求/剩余/成交股数、状态、成交价、费用、拒绝原因、风险预算 |
| `st_sim_position` | 股票、策略、模式、买入价格/金额/股数、买入日期、原因、分析分、风险、状态、卖出价格/日期/原因、利润、收益率、持仓天数、费用 |
| `st_sim_event` | 模式、日期/时间、事件类型、信号/订单/持仓ID、股票、策略、严重度、消息、payload |
| `st_sim_risk_budget` | 模式、预算日、策略、初始资金、总权益、现金、总仓位上限、策略上限、已用/待用/可用预算 |

## 关系与迁移注意

```text
si_all_code -> sm_stock_kline / sm_stock_current / sm_stock_snapshot
si_trade_calendar -> 日线分析 / 调度 / 模拟交易 / 回测
si_concept_* -> sm_concept_* -> sector_rotation -> st_recommended_stocks
sm_stock_kline + si_stock_finance + capital/news/risk -> stock_analysis_result
stock_analysis_result -> st_recommended_stocks -> st_sim_signal -> st_sim_order -> st_sim_position
st_recommended_stocks + 后续K线 -> failure_samples -> threshold_calibration -> runtime_params
QMT raw/capability/coverage/gap -> data quality gate -> recommendation/trading readiness
```

迁移顺序建议：

1. `si_trade_calendar`、`si_all_code`、基础概念/行业/股本。
2. 日K、实时快照、分钟读库、资金流和公告新闻。
3. `stock_analysis_result`、`st_recommended_stocks` 及字段版本。
4. 复盘、组合和模拟交易表族。
5. 质量、调度、QMT审计和本地历史表。

禁止事项：

- 不在未确认目标库和索引前执行全表 `TRUNCATE`。
- 不把 QMT 大体量历史直接写入生产业务库。
- 不在分析日期之后读取公告/新闻/行情作为历史回测输入。
- 不把字段缺失静默当作“有数据”，必须写入质量状态或兼容标记。
