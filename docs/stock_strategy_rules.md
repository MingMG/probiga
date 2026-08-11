# stock.txt 策略拆解与系统落地正式版

本文把 `stock.txt` 的要求拆成可执行规则，用于统一推荐、复盘、个股详情和模拟交易口径。系统目标不是替用户直接交易，而是输出可验证、可追踪、可复盘的 A 股投研辅助结论。

## 1. 总体原则

- 数据必须标注来源、交易日和是否滞后。
- 推荐必须先过硬性风险闸门，再进入评分和排序。
- 每个分数都要能回溯到数据、规则和证据链。
- 盘前、盘中、盘后使用不同口径，不能混用实时和收盘数据。
- 输出必须包含核心数据、技术结构、事件影响、风险提示和操作作废条件。
- 复盘结果要进入失败样本和阈值校准，稳定有效后发布为运行时参数。

## 2. 模块落地清单

| 模块 | stock.txt 要求 | 落地位置 | 当前状态 |
|---|---|---|---|
| 数据校验 | 北京时间对齐，价格至少两源交叉验证 | `sync_analysis_fast.attach_price_crosscheck` | 已落地，支持运行时偏差阈值 |
| 交易范围 | 过滤港股、688、ST，买卖按 100 股整数倍 | 推荐闸门、模拟交易、手工组合交易 | 已落地 |
| 市场复盘 | 指数、量能、涨跌停、炸板率、连板、主线、仓位 | `biz.review.generate` | 已增强主线纯正性 |
| 板块先行 | 板块资金大于 5 亿，延续性通过后筛个股 | `load_sector_rotation_features` | 已落地，阈值可发布覆盖 |
| 个股硬过滤 | 经营/自由现金流为负、EBIT利润率为负、ROIC/应收/预付/关联交易雷区、下降趋势、连续资金流出、风险公告剔除 | `load_finance`、`build_rule_flags`、`choose_recommend_status` | 已落地 |
| 技术分析 | EMA、SMA、趋势时钟、抵扣价、成交密集支撑/压力、BBI/BIAS/MTM/LWR/DMI/MACD/KDJ/RSI/BOLL、MA5回调 | `load_kline_features`、`technical_evidence_json` | 已落地 |
| 缠论结构 | 中枢、背驰、一二三买卖点 | `build_chan_structure`、`build_minute_chan_structure` | 已落地日线与 30/60 分钟级 |
| 筹码资金 | 股东人数变化、龙虎榜净买、两融方向、解禁、质押、减持和扫雷风险 | `load_chip_capital_features`、`load_market_margin_features`、`chip_capital_score` | 已落地 |
| 市场宽度 | 全市场站上 MA20 比例大于 85% 或小于 15% | `compute_market_breadth_features`、`market_breadth`证据链 | 已落地 |
| 盈亏比 | 目标/止损量化，低于 3:1 不执行 | `build_strategy_trade_plan` | 已落地，阈值可发布覆盖 |
| 仓位上限 | 低/中/高风险单票上限 30%/20%/10%，系统默认再收紧到 12%；模拟交易总仓位不超过 80%，预留 20% 现金 | `_position_weight`、`derive_position_risk_level`、`SIM_RISK_CONFIG` | 已落地 |
| 热度风险 | 热榜过前、单日涨幅过大、板块宽度过热降级 | `heat_overload_score`、板块闸门 | 已落地 |
| 推荐分层 | 买点就绪、观察池、回调等待、风险警报、剔除池 | `signal_status`、前端推荐表 | 已落地 |
| 五档评级 | 买入、增持、中性、减持、卖出 | `derive_investment_rating`、`investment_rating`、前端推荐表 | 已落地 |
| 失败归因 | 推荐后跟踪 1/3/5/10 日收益与失败原因 | `backfill_recommendation_reviews` | 已落地 |
| 事件分析 | 5W2H、受益/受损标的、替代范围、时效性 | `event_risk_detail`、`st_event_impact_relations` | 已增强产业链关系规则 |
| 阈值学习 | 根据历史推荐表现校准策略阈值 | `st_strategy_threshold_calibration`、`st_strategy_runtime_params` | 已落地校准与自动发布 |

## 3. 强制闸门

| 规则 | 判定 | 处理 |
|---|---|---|
| 688 开头 | `stock_code.startswith("688")` | `BLOCK` |
| ST/退市 | 名称包含 `ST` 或 `退` | `BLOCK` |
| 非沪深主代码 | 不以 `0/3/6` 开头 | `BLOCK` |
| 重大公告风险 | 立案、退市、重大违法等 | `BLOCK` |
| 经营现金流为负 | `oper_cf_ps < 0` 或 `cash_flow_ratio < 0` | `BLOCK` |
| 自由现金流为负 | 库内存在 `free_cash_flow/fcf` 且小于 0 | `BLOCK` |
| EBIT 利润率为负 | 库内存在 `ebit_margin` 或可由 `ebit/total_rev` 计算且小于 0 | `BLOCK` |
| 连续 3 日主力净流出 | 最近 3 个资金日主力净流入均小于 0 | `BLOCK` |
| 明显下降趋势 | 收盘低于 MA20 且 MA5 < MA10 < MA20，并弱于 MA60 | `BLOCK` |
| 近一周涨幅过大 | `pct_5 >= 20%` | `SUSPENDED` |
| 未来 30 日大额解禁 | `lifting_max_ratio_30d >= 10%` 或解禁金额/有效市值 `>= 5%` | `BLOCK` |
| 未来 30 日小额解禁 | 存在解禁但未达到大额阈值 | `SUSPENDED` |
| 大股东质押偏高 | `pledge_ratio >= 50%` | `SUSPENDED` |
| 股东减持比例偏高 | 近 90 日 `reduction_max_ratio_90d >= 2%` | `SUSPENDED` |
| 商誉占净资产偏高 | `goodwill_to_net_asset_pct >= 20%`，其中 `>=30%` 为高风险 | `SUSPENDED` |
| 扫雷高风险 | `mine_clearance_score >= 70` | `BLOCK` |
| 股东人数明显增加 | `holder_num_ratio >= 10%` | `SUSPENDED` |
| 市场宽度过热 | 全市场 MA20 宽度超过 85%，且个股已扩张 | `SUSPENDED` |
| 弱板块 | 板块资金、宽度或轮动分不合格 | `SUSPENDED/BLOCK` |
| 数据缺失 | 财务、资金流或价格校验核心数据缺失 | `SUSPENDED` |
| 双源价格偏差过大 | K 线收盘价与第二行情源偏差超过阈值 | `SUSPENDED` |
| 非主升浪盈亏比不足 | 预期上行空间 / 止损空间小于运行阈值 | `BLOCK` |

## 4. 运行时参数

默认参数：

- `min_risk_reward = 3.0`
- `min_sector_flow_amount_3d = 500000000`
- `min_sector_rotation_score = 50`
- `price_crosscheck_tolerance_pct = 1.0`

发布表：

- `st_strategy_runtime_params` 保存当前生效参数。
- `st_strategy_threshold_calibration` 保存复盘校准建议。
- `publish_strategy_runtime_params` 只有在连续多天、样本数足够、整体建议方向一致时才发布参数。

发布策略：

- 连续建议收紧：提高最低盈亏比、板块资金阈值和板块轮动分。
- 连续建议放宽：在下限内降低最低盈亏比、板块资金阈值和板块轮动分。
- 建议不稳定或样本不足：只记录，不发布。

## 5. 证据链输出

推荐结果的 `evidence_chain_json` 至少覆盖：

- 数据质量与来源日期。
- 价格双源交叉校验。
- 板块资金、宽度、轮动闸门。
- 全市场宽度和拥挤/恐慌状态。
- 宏观政策与宏观硬数据。
- ETF 资金流向与市场风险偏好。
- 散户看涨/看跌情绪极值与机构/北向趋势倒挂验证。
- 日线技术证据。
- 经典顶底结构。
- 日线缠论结构。
- 30/60 分钟缠论结构。
- 资金流向。
- 个股北向持仓与近期增减持。
- 股东人数、龙虎榜、两融、解禁、质押、减持、扫雷组成的筹码资金证据。
- 机构持仓/评级/调研画像。
- 投资者互动问答里的订单、客户、产能和风险验证。
- 主营纯正性与行业景气硬指标。
- 公告/新闻事件影响。
- 交易计划、入场区间、止损、盈亏比。
- 最终五档评级：买入、增持、中性、减持、卖出。

`technical_evidence_json` 同步保留趋势时钟、MA5 回调、成交密集支撑/压力、EMA/MACD/BBI/BIAS/MTM/LWR/KDJ/RSI/BOLL/DMI、日线缠论和分钟缠论摘要，供前端详情和推荐表展示。

## 6. 事件关系图谱

`st_event_impact_relations` 用结构化规则补充产业链影响：

- `trigger_keyword`：公告/新闻触发词。
- `source_scope/source_key`：匹配行业、板块、概念或个股。
- `target_type/target_key/target_name`：受影响对象。
- `impact_type`：`beneficiary`、`damaged`、`alternative`。
- `reason`：关系命中原因。

系统保留原有公告受益/受损判断，并叠加关系表命中的产业链对象。没有关系表或没有命中时，行为保持原样。

## 7. 市场复盘口径

主线不再只取涨幅第一，而是综合：

- 板块涨幅排名。
- 板块资金流入排名。
- 涨停家数代表的情绪扩散。
- 是否同时出现在走弱方向。

输出 `purity_score`、`purity_level`、`purity_reasons`，并写入复盘正文的“主线纯正性”段落。

## 8. 验收标准

- 批处理能加载运行参数，失败时回落默认值。
- 推荐生成后能补充 30/60 分钟缠论证据。
- 推荐生成能补充 MACD/KDJ/RSI/BOLL、筹码资金证据和市场宽度证据。
- 解禁、扫雷高风险、近一周过热、股东人数明显扩散能进入推荐门禁。
- 事件影响能读取产业链关系规则并输出受益、受损或替代范围。
- 复盘能输出主线纯正性。
- 失败样本、阈值校准、运行参数发布形成闭环。
- 单元测试覆盖新增规则，核心 Python 文件编译通过。

## 9. 最新落地补充

- 日线 K 形态：新增看涨/看跌吞没、早晨星、黄昏星、锤头线、射击星、红三兵、黑三鸦识别，进入 `technical_evidence_json`；高位看空形态触发 `bearish_kline_pattern` 暂缓。
- 均线逐项表：`technical_evidence_json` 新增 `moving_average_table`，按 EMA12、EMA26、SMA20、SMA60、SMA120、SMA250 输出当前值、现价位置、偏离幅度、五步法阶段、方向和抵扣价；同步输出 `ema_sma_divergence`，识别“EMA领先拐头，SMA待验证”等分歧。
- 抵扣价推演：`technical_evidence_json` 新增 `deduction_projection`，对 SMA20/SMA60 输出抵扣日期、抵扣价、当前收盘价、差值、方向和“未来3日维持现价附近”的拐头推演。
- 经典顶底形态：新增 `detect_classic_top_bottom_structure`，识别双顶/双底、头肩顶/头肩底、圆弧顶/圆弧底；确认跌破颈线或高位转弱时触发 `classic_top_breakdown` 暂缓，并写入 `classic_pattern` 技术证据；若无明确顶底结构，也输出当前波段高点、低点、日期、涨跌幅和“当前无明确顶底结构”的兜底说明。
- 缠论结构：日线和 30/60 分钟线输出一买/二买/三买、一卖/二卖/三卖观察信号，并补充支撑、压力、失效价。
- 分时行为：分钟线新增 `intraday_behavior`，识别吸筹、出货、洗盘、弱势杀跌，用于盘中二次确认。
- 盘中执行时间窗：模拟交易新开仓只允许 09:37-09:45、14:26-14:28；09:30-09:37、13:15-13:30 标记出场窗口，14:00-14:05、14:57-15:00 标记 T 窗口。
- 情绪周期：复盘按涨停数、炸板率、赚钱效应和跌停扩散划分复苏期、主升期、分化期、退潮期，并写入基础复盘与专业复盘。
- PEG/行业估值：推荐评分按行业映射成长股、稳定增长股、周期股、低速价值股；成长/稳定/价值股使用 stock.txt 的 PEG 分层，周期股不硬套 PEG，回退 PE/PB 与供需周期观察；严重高估进入 `valuation_overpriced` 暂缓和 `valuation_expensive` 失败标签。
- 行业相对估值：计算个股 PE(TTM) 相对行业中位数倍数，超过 1.5 倍开始扣减 `valuation_score`，达到 1.8 倍且估值分偏低时触发 `industry_relative_overvalued` 暂缓。
- 题材延续性 10 分制：板块轮动分派生 `theme_continuity_score_10` 和 `theme_continuity_level`，8 分以上为高延续性、6-7.9 为中延续性、低于 6 为低延续性；低于 5 分触发 `theme_continuity_low` 暂缓，并写入证据链。
- PS/营收估值：读取财务表 `total_rev`，用有效市值/营收估算 `ps_ratio`，计算行业 PS 中位数与 `ps_industry_multiple`；超过 2.0 倍开始扣分，达到 2.5 倍且估值分偏低时触发 `industry_relative_ps_overvalued` 暂缓，并写入估值证据链。
- 历史估值分位：用近 250 日收盘价分位作为 PE/PB 历史估值拥挤度代理，输出 `valuation_history_percentile_250d`、`pe_percentile_250d`、`pb_percentile_250d`；80 分位以上扣减估值分，85 分位以上且估值分偏低时触发 `valuation_history_percentile_high` 暂缓。
- 行情风格适配：读取沪深300与创业板指数趋势，输出 `market_style` 证据；牛市按成长/大盘价值偏向加权，熊市暂停非防御成长股，震荡市强化量能与筹码因子。
- 板块内位置：在 `load_sector_rotation_features` 内按同板块近 3 日成交额、主力净流入和涨幅排名计算 `sector_leadership_score`，划分 `leader/front/middle/follower`；龙头/前排获得轻量加权，后排跟风只作观察并在风险中提示。
- 宏观政策压力：读取 `st_news_flash` 近 3 日新闻，统计降准/降息/稳增长等支持词与制裁/通胀/汇率贬值/监管趋严等压力词，输出 `macro_policy` 证据；熊市或过热市场叠加宏观压力时暂缓新推荐。
- 宏观硬数据：新增 `load_macro_indicator_context`，兼容读取 `st_macro_indicator`、`st_macro_economic_data`、`st_macro_china_daily`、`st_macro_calendar` 等表，综合 PMI、GDP、CPI、PPI、社融/M2、汇率形成 `macro_indicator` 证据；熊市或过热市场叠加硬数据压力时触发 `macro_indicator_pressure` 暂缓。
- 宏观周期标签：`macro_indicator` 同步输出 `macro_cycle`，按 PMI/GDP/CPI/PPI/社融流动性粗分复苏、过热、滞胀、衰退或中性，用于市场风格和风险偏好修正。
- 相对大盘强弱：用个股 20 日涨幅减沪深300 20 日涨幅生成 `relative_strength` 证据；跑赢 10 个百分点以上加分，跑输 10 个百分点且自身为负时暂缓。
- 流动性门控：按 `amount_ma20`、当日成交额和换手率输出 `liquidity` 证据；低于 1 亿成交额硬底线直接阻断，熊市将 20 日日均成交额阈值收紧至 8 亿，换手率不在 5%-15% 区间进入暂缓。
- 盘口深度：读取 `sm_stock_five_level` 五档盘口，按买一至买五/卖一至卖五价格与手数折算 `bid5_amount`、`ask5_amount` 和 `order_book_depth_amount`；五档合计低于 1 亿或买卖盘严重失衡时触发 `order_book_depth_low/order_book_imbalance` 暂缓，并写入 `liquidity` 证据。
- 量能温度：按 `amount_ratio_20`、换手率、日涨跌和 5 日涨幅输出 `volume_temperature` 证据；0.8-2.5 倍视为温和放量，天量换手/高位放量/放量下跌触发暂缓。
- 流通市值门控：读取 `sm_stock_snapshot` 和 `si_stock_shares` 计算 `float_market_cap`，输出 `size_liquidity` 证据；流通/有效市值低于 50 亿进入暂缓，缺数据只标记未知不误杀。
- 政策利空预警：公告/消息负面关键词新增 `集采`、`限产`、`监管趋严`、`补贴退坡`、`产能过剩`、`价格管制` 等，用于识别政策转向风险。
- 业绩公告语义：公告正面关键词覆盖 `业绩预增`、`业绩快报`、`预盈`、`净利润增长`、`扣非增长`、`高增长`；负面关键词覆盖 `业绩变脸`、`大额减值`、`商誉减值`、`坏账准备`、`存货跌价` 等，用于强化业绩打底和扫雷。
- 利好兑现识别：新增 `classify_event_fulfillment`，正向事件叠加 5 日涨幅、当日涨幅或距 MA20 乖离过高时标记 `PRICED_IN`，触发 `positive_event_priced_in` 暂缓，避免把已兑现利好当作新买点。
- 基本面阈值：按成长/价值/周期类型校验 ROE/扣非 ROE、ROA、毛利率、营收/利润同比、营收/利润环比、速动比率和资产负债率，输出 `fundamental_quality` 证据；业绩亏损或收入利润同步恶化直接阻断，环比业绩掉头、利润动能弱、偿债或盈利效率不达标进入暂缓。
- 财务雷区：`load_finance` 可选读取 `roic`、`acct_recv_to_rev`、`prepayment_yoy_gr`、`related_transaction_to_rev`；ROIC 低于 15%、应收账款/营收超过 30%、预付账款同比增长超过 50%、关联交易/营收超过 20% 时进入 `fundamental_quality` 暂缓和 `fundamental_weak` 失败标签。
- 分红能力：读取 `sm_dividend` 分红方案，解析现金分红并计算近 3 年连续性、最新股息率和平均股息率，输出 `dividend` 证据；价值/稳定/防御风格获得小比例加权，缺少分红记录列为风险提示。
- 研报主题证据：复用 `research_radar` 的主题股票池，输出 `research_theme` 证据，包含主题、角色、验证点与风险；核心验证标的只做轻量加权，不能单独构成买入理由。
- ETF 资金流向：新增 `load_etf_flow_context`，兼容 `st_etf_flow_daily`、`st_market_etf_flow`、`st_fund_etf_flow` 等表，汇总 ETF 1/3/5 日净流入；熊市叠加 3 日 ETF 净流出超过 30 亿元时触发 `etf_flow_pressure`。
- 散户情绪倒挂：新增 `load_retail_sentiment_context` 与 `classify_retail_sentiment_context`，兼容散户看涨/看跌调查表；散户极度看多且机构/北向走弱时触发 `retail_institution_contrarian_risk`，散户极度看空但机构承接时只做轻量反向情绪支持。
- 资金席位增强：读取 `st_a_list_info` 拆分近 20 日龙虎榜机构席位净买/净卖，进入 `chip_capital` 证据和暂停规则；读取 `st_securities_margin` 计算个股两融近 3 日余额变化、融资买入额和扩张/收缩天数，连续去杠杆进入暂缓；读取 `st_north_flow_daily` 计算北向 1/3/5 日流向，进入 `north_flow` 证据、资金分和情绪分；新增 `load_stock_north_holding_features` 读取个股北向持股比例、持股变化和近 3/5 日净买入，低于 1% 且未增持触发 `north_stock_underweight`，明显减持或净卖出触发 `north_stock_outflow`。
- 机构画像：新增 `load_institutional_features`，兼容公募/QFII/RQFII/社保/私募/机构持仓、券商评级/目标价、机构调研表和券商金股池，形成 `institutional_profile` 证据；评级下调、机构认可度弱或目标价下修时触发 `institutional_profile_weak`。
- 投资者互动验证：新增 `load_investor_interaction_features` 与 `evaluate_investor_interaction_profile`，读取互动问答/IR 记录，统计订单、客户、量产、产能等验证词和亏损、延迟、监管、问询等风险词，形成 `investor_interaction` 证据；风险问答集中时触发 `investor_interaction_risk`。
- 主营纯正性：新增 `load_business_purity_features` 与 `evaluate_business_purity`，读取主营业务、经营范围、公司简介、概念字段，与行业/研报主题/角色做匹配；主营与题材不匹配或弱相关业务过多时触发 `business_purity_low`。
- 行业景气硬指标：新增 `load_industry_prosperity_features` 与 `evaluate_industry_prosperity`，读取订单/合同、产品价格、产能利用率、行业景气分等结构化数据，形成 `industry_prosperity` 证据；产品价格下行、产能利用率低且缺少订单承接时触发 `industry_prosperity_weak`。
- 质押/解禁/减持/商誉扫雷：解禁不再一刀切，`evaluate_unlock_pressure` 按 30 日最大解禁比例 10% 或解禁金额/有效市值 5% 分为大额阻断、小额暂缓；`load_chip_capital_features` 兼容读取质押表候选字段并以 50% 质押率作为风控线，兼容读取股东减持表并以近 90 日最大减持比例 2% 作为暂缓线；`load_finance` 可选读取 `goodwill/net_assets`，或用每股净资产与股本估算净资产，计算 `goodwill_to_net_asset_pct`，20% 以上暂缓、30% 以上标为高风险。
- 3/5/10 日资金承接：`load_flow_features` 增加 10 日主力净流入和 5/10 日流入流出天数，`capital` 证据输出 3/5/10/20 日主力趋势；10 日内 7 天净流出且累计为负时暂缓。
- 仓位纪律：`derive_position_risk_level` 将重大风险、过热、流动性、基本面、两融去杠杆等信号映射为 LOW/MEDIUM/HIGH；`_position_weight` 对低/中/高风险执行 30%/20%/10% 单票上限，并叠加系统单票 12% 保守上限，阻断/卖出警报仓位为 0。
- 执行纪律：`portfolio_calc_next_position` 对手工组合交易也强制校验 100 股整数倍；模拟交易 `SIM_RISK_CONFIG` 按总仓位 80%、现金缓冲 20%、单票 10% 进行风险预算，所有自动买入股数按整手向下取整。
- 成交密集区：`load_kline_features` 按近 90 日典型价和成交额分箱估算 `volume_profile_peak/support/resistance`，写入 `volume_profile` 技术证据和预期空间阻力价，避免只用前高前低判断支撑压力。
