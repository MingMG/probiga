# 判断与展示准确性审查记录 · 2026-09-19

本记录覆盖本次系统审查中独立完成的判断、持仓建议、股评和模拟交易指标修复。它不是对整个系统不存在缺陷、策略能够盈利或预测已经校准的保证。首页、监控页和其他页面的改动由本次任务的其他工作项记录。

## 发布边界

**Linux/server。** 修改为 `server/engine/strategy_center.py`、`server/api/commentary_utils.py`、`server/api/routers/{strategy_center,holding_strategy,commentary,sim_trade}.py` 及相应测试、本文档。没有修改数据采集、QMT 客户端、共享数据库结构、调度归属或下单协议。这里新增的是读接口的解释字段，不赋予交易权限。Windows/QMT 不需要因这些修改而部署或重启。

## 已复现并修复的问题

| 问题与复现条件 | 原行为 | 修复后的行为 |
| --- | --- | --- |
| 数据质量为 0；或至少 10 个样本、胜率为 0 | `or` 将质量 0 改成 1、胜率 0 改成 50 | 保留真实零值；质量为 0 不获得有效权重 |
| 在历史日期查询已经回填的推荐后 5 日收益 | 只限制推荐日，读取当时尚未发生的结果 | 对应交易日窗口须成熟，并要求 `updated_at` 在当次截止前；缺少日期或更新时间证据时不使用 |
| 在历史日期读取后来才生成的指标快照 | `as_of_date` 符合即采用 | 同时要求 `created_at` 在当次截止前 |
| 模拟交易、历史回放在同一表中 | 策略卡混合统计多种运行模式 | 策略卡的平仓样本仅采用实时模拟模式，回放另行展示 |
| 将多个个股收益相加，或将最差单笔收益当作回撤 | 显示为策略收益率、最大回撤 | 单笔样本只展示样本数、胜率、单笔均值、对应盈利因子；没有组合净值时组合收益、回撤为空 |
| 缺少收益的交易行 | 当作 0 收益进入统计 | 排除缺失或非有限值，给出有效样本数、缺失数和状态；持平交易单独计数 |
| `sellable_shares=0` | 回退为全部持仓可卖 | 保留 0，显示待可卖后退出/减仓；T+1 保持下一交易日处理 |
| 过期推荐带有 `SELL_ALERT`，而当前判断为 HOLD/WAIT_DATA | 旧告警、旧阈值或展示字段重新触发立即退出 | 当前有效判断决定行动；过期推荐不重新授权卖出，当前成本保护、MA20 和当前市场风险继续生效 |
| 推荐已刷新，但旧个股分析仍带 CRITICAL/HIGH 风险 | 旧分析的风险先于过期检查触发卖出或减仓 | 推荐和分析分别按自身时效过滤；旧分析不能绕过当前判断，当前硬止损仍生效 |
| 历史盘中股评仅提供日期 | 混用最新实时行情和历史日线 | 明确拒绝缺少历史时刻证据的盘中评估，不将今天行情代入历史 |
| 历史盘前股评、缺失启动日 | 可读到当日收盘；找不到锚点时替换为其他日期 | 盘前只用上一完整交易日日线；锚点必须精确匹配，找不到则标记缺证据 |
| 盘中旧行情、缺少关键均线或量能 | 可能继续给出结构确认 | 盘中要求截止前 5 分钟内报价；关键证据缺失不能得到 TRACK；不把昨日量比显示为盘中量比 |
| 股评新闻没有时间截止 | 历史评估展示未来资讯 | 新闻按评估时刻截止，只有日期的公告采用更保守的前一日边界；资讯仍标记未核验，不参与资金/下单判断 |
| 全盈利模拟样本 | 盈利金额被填入盈利因子，平均盈利百分比被填入盈亏比 | 无亏损分母时返回空值和 `NO_LOSING_TRADES`；全亏损盈利因子为真实 0，条件盈亏比因无盈利样本而为空 |
| 将可能重叠的单笔交易顺序复利 | 冒充近 3 月组合最大回撤和夏普 | 没有日频组合净值时这两个指标为空，标记 `PORTFOLIO_NAV_UNAVAILABLE` |
| 模拟归档现金减去持仓市值，权益漏掉浮盈亏 | 市场上涨反而使显示现金下降，权益不变 | 使用现有 `portfolio_state` 单一核算并传入页面报价；缺少持仓报价时不伪造权益或浮盈亏 |
| 缺少持仓报价，或报价为无穷/非数值 | 明细为空但策略市值、账户预算仍使用买入成本；非有限值可能破坏 JSON | 拒绝非有限报价，持仓估值及其依赖的账户/预算字段一致为空；现金与成本核算保留 |
| GET 查询风险预算 | 查询时按无报价的成本估值覆盖预算快照 | 查询只返回已存预算及其日期；预算更新仍由既有 `SimTradeEngine.run_event_tick` 两处写入负责，不改变调度归属 |
| 多策略统计与偶数样本中位数 | 总体分布和每日损益只剩最后一个策略；中位数取上中位 | 跨策略累计有效结果，偶数中位数取中间两值均值；缺失不计零、持平不计亏，无盈利/亏损样本时对应极值为空 |
| 固定状态评分或评分距离启发值显示为百分比置信度 | 容易被理解为成功概率 | 新增规则评分的 basis、label、semantics；不修改分数数值、入选阈值或主要评分公式 |

## API 口径

- 策略卡：新增 `avg_return_pct`、`metric_scope`、`performance_weight_eligible`。`CLOSED_LIVE_SIMULATION_TRADES` 和 `MATURED_RECOMMENDATION_OUTCOMES` 是描述性样本，不改变策略绩效权重；它们的 `return_pct`、`max_drawdown_pct` 为 null。`metric_note` 解释统计口径。
- 状态和候选：分别新增 `confidence_basis/label/semantics` 与 `model_confidence_basis/label/semantics`。语义为 `UNCALIBRATED_RULE_SCORE`；紧凑候选接口也保留解释字段。页面应显示“规则分 /100”，不能显示“成功概率”。
- 股评：新增 `evaluation_date`、`knowledge_cutoff`、`price_basis`、`price_observed_at`，以及 `current.price_trade_date`、`current.daily_data_date`。日线数据日和评估时刻必须分开阅读。
- 模拟指标：新增 `profit_factor_status`、`profit_loss_ratio_status`，近 90 日字段带 `_3m` 后缀。`NO_SAMPLES`、`NO_LOSING_TRADES`、`NO_WINNING_TRADES` 都不能显示成数值 0。
- 模拟样本：`evaluated_count`、`missing_outcome_count`、`breakeven_count`、`metric_status` 明确分母。已实现损益曲线仍可供归档查看，但标记 `REALIZED_PNL_ONLY`，不等同于包含持仓浮盈亏的组合净值。
- 模拟估值：`valuation_status` 和 `missing_valuation_codes` 标记报价缺口。核算失败或估值缺失时权益字段为空，不回填虚假确定数值；各策略、`portfolio_state` 和依赖估值的风险预算保持一致。`accounting_basis=CURRENT_POSITION_LEDGER` 表明账户是当前账本，不是所选历史预算日的账户回放。
- 风险预算查询：`budgets_basis=PERSISTED_RISK_BUDGET_SNAPSHOTS`，预算按请求日读取已存快照；账户估值在没有报价时保持未知。GET 不再写入预算。现有写入仍在 `server/engine/sim_trade_engine.py` 的 `run_event_tick` 内，未改变任务所有权。

## 验证

使用项目虚拟环境 **Python 3.14.3** 运行以下回归命令，结果 **142 passed**。4 条警告为 SQLite 默认 datetime adapter 自 Python 3.12 起的弃用提示。

```powershell
& 'E:/My Code/ProBigA/.venv/Scripts/python.exe' -m pytest tests/test_sim_trade_summary.py tests/test_sim_trade_rules.py tests/test_sim_trade_runtime_schema.py tests/test_decision_review_accuracy.py tests/test_watchlist_holding_strategy.py tests/test_commentary_assess.py tests/test_strategy_center.py tests/test_pit_strategy_reader_contract.py tests/test_manual_long_task_enqueue.py -q
```

测试覆盖交易日成熟窗口与晚写入结果、零值、亏损/盈利/持平/缺失样本、不可卖持仓、旧退出信号与旧风险分析、历史盘中拒绝、盘前日期、新闻截止、缺少新鲜报价、紧凑 API 评分解释字段，以及模拟现金/权益核算、报价缺失/非有限值、风险预算查询无写入和跨策略统计。

调用方搜索确认 `_return_metrics`、`_calc_trade_metrics` 及 `_3m` 比率字段用于此模拟 API 的读模型和前端；没有发现推荐或下单评分算法消费这些 `_3m` 字段。原 `strategy_governance.calculate_return_metrics` 是独立实现，此次未改。`tests/test_sim_trade_rules.py` 回归通过，比例变空不会改动模拟买入规则。

## 未覆盖与解释限制

- 本记录中的测试使用本地、合成或模拟数据库证据，不能代替合并后生产环境的接口和视觉验收；部署结果由主任务记录。
- 没有验证采集的准确性和覆盖率，也没有重建历史证券主数据、复权、退市样本或资讯版本；这些属于本次明确排除的数据获取及证据来源范围。
- 历史读取依赖现有交易日历及行更新时间。后来改写的行会保守排除，不能还原它在每个历史时刻的旧版本。因此历史样本可能减少；这比显示未来数据更诚实，但不等于完整的时间点数据库。
- 无组合净值证据的归档样本不提供组合收益、回撤或夏普。已实现损益曲线不衡量未平仓风险。
- 规则分尚未进行概率校准；新增标签不代表提高了预测能力。本次不声称任何策略具有稳定超额收益。
- 日线估值可能不是实时成交价；模拟交易归档报价的来源字段说明使用实时、分钟或日线价格，不代表在该价可成交。
- 未执行真实交易、没有检查全部实时成交路径，也未对所有页面和所有并发场景做穷尽测试。其他模块仍需要以复现证据继续审查，不能据此宣称“全系统已无问题”。
