# ProBigA 股票选股 V4 OOS 研发与生产最终验收

- 验收日期：2026-08-01
- 研发数据：2020-01-02 至 2026-07-31
- 生产模式：仅 PAPER，真实下单关闭
- 最终裁决：**BLOCK**

## 1. 总结论

本轮共执行 7 个冻结研究活动、28 个新候选，累计候选搜索计数由 49 增至 77。趋势、反转、市场状态、嵌套岭回归、有限持有期、QFBR、月频质量/价值/低波动等路线均已交叉验证。

没有任何股票模型同时达到锁定的 OOS 门槛：至少 80 笔组合交易、净期望大于 0、Profit Factor 不低于 1.30、Payoff Ratio 不低于 1.00、最大回撤不高于 12%、至少 4/5 个外层窗口为正，并通过校准和多重检验。

因此：

- 不注册任何 V4 候选为 `PAPER_ACTIVE`；
- 不写入 active calibration；
- 不把候选接入正式目标组合；
- 不允许真实委托；
- 当前生产验收严格保持 14/16，而不是伪造为 16/16。

## 2. 七轮交叉研发

| 活动 | 新候选 | 结果 |
|---|---:|---|
| 固定趋势/反转/双状态 | 7 | 全部 BLOCK，校准方向失败 |
| 嵌套岭回归趋势模型 | 3 | 全部 BLOCK，未修复负 OOS 基线 |
| 有限持有期多策略 | 6 | 全部 BLOCK；QFBR 覆盖最好但 PF 不足 |
| 中等市场状态 QFBR | 3 | 全部 BLOCK；利润较好但样本和窗口不足 |
| 宽样本打分反转 | 3 | 全部 BLOCK，原始 PF 低于 1 |
| 嵌套岭回归反转模型 | 3 | 全部 BLOCK，原始期望为负 |
| 月频质量/价值/低波动 | 3 | 全部 BLOCK，最佳 PF 约 1.00 |

原始经济指标最强的候选是 `broad_qfbr_moderate_5_v1`：

- 组合交易 46 笔，低于 80 笔门槛；
- 净期望 +1.654188%；
- Profit Factor 2.160154；
- Payoff Ratio 1.661657；
- 净利润 +7,955.10 元；
- 最大回撤 1.493482%；
- 正向外层窗口 3/5，低于 4/5 门槛；
- Max-T 调整后 p=0.128436，不显著；
- 2022 年 PF=0.100419，2025 上半年 0 笔交易；
- 校准后组合 0 笔交易。

覆盖度更高的 `bounded_qfbr_5_v1` 有 121 笔、4/5 个正向窗口，但 PF 仅 1.101807，低于 1.30；最后窗口 PF=0.198369，Max-T 调整后 p=0.871564。

## 3. 已完成的代码处理

- 财务特征增加 `report_date <= as_of` 和 `notice_date >= report_date` 时点约束，排除未来报告及异常公告日期。
- 决策 Top10 在存在兼容的有效校准时，仅允许可校准状态进入，避免高分阻塞状态占位。
- 回测入场在停牌或一字涨停时延后，退出在一字跌停时延后，采用下一可成交开盘价。
- 新增 V4 研究模块、7 份冻结协议、7 个可复现研究入口及对应 OOS 产物。
- 生产部署备份：`/opt/ProBigA/.codex_backups/acceptance_20260801_104101`。

## 4. 验证证据

- 本地 Trading V3/V4 全相关测试：85 passed。
- 生产新增相关测试：29 passed。
- 生产文件 SHA256 与本地一致。
- `probiga`、`probiga-scheduler`：均为 active。
- 生产验收：14/16。
- 唯一失败项：`active_oos_model_present`、`latest_validation_pass`。
- 纸面账户 active，`real_trading_enabled=0`。
- 四个数据库真单保护触发器存在。
- 生产主动越权测试：账户真单开关和执行计划真单开关均被 SQLSTATE 1644 拒绝。
- `real_trading_enabled_count=0`、`real_order_allowed_count=0`，测试无持久修改。

## 5. 独立终审核查出的后续硬门槛

以下问题不改变本次 BLOCK，但必须在任何未来 PASS 前解决：

1. 多重检验必须覆盖累计 77 次搜索，并作为正式 gate，而不能只把活动内 Max-T p 值写进产物。
2. 产物必须锁定完整代码/数据实现哈希；协议哈希不能替代实现版本证明。
3. 基础 OOS 通过后，必须实际执行 1.5/2 倍成本、延迟一日、删除最佳 5 笔、参数、TopN 和资金敏感度压力测试。
4. 一字板、ST、不同板块涨跌幅制度、停牌和缺失估值应统一到同一成交规则实现。
5. 财务数据只有公告日期而没有公告时间；无法证明同日盘后披露在决策时刻已知时，应采用更保守的时点规则。

## 6. 前瞻纸面验收缺口

截至 2026-07-31 的历史已被反复查看，历史结果最高只能授予 `PAPER_CANDIDATE`。最终 PASS 必须从 2026-08-03 开始重新累计未经查看的前瞻纸面证据：

- 至少 120 个交易日；
- 至少 80 笔成熟组合交易；
- 规则、TopN、成本、成交与退出协议冻结；
- 净期望、PF、Payoff、回撤、窗口稳定性、多重检验和压力测试全部达标；
- 任何实质调参后重新计时和累计样本。

当前前瞻证据为 0 个交易日、0 笔成熟组合交易。

## 7. 证据文件

- `artifacts/trading_v4/oos_campaign_20260801.json`
- `artifacts/trading_v4/ml_oos_campaign_20260801.json`
- `artifacts/trading_v4/bounded_oos_campaign_20260801.json`
- `artifacts/trading_v4/qfbr_oos_campaign_20260801.json`
- `artifacts/trading_v4/scored_reversal_oos_20260801.json`
- `artifacts/trading_v4/reversal_ml_oos_20260801.json`
- `artifacts/trading_v4/monthly_factor_oos_20260801.json`

ETF `daily_vol_stop` 的稳健回测只能作为工程旁证，不能替代股票选股模型验收。
