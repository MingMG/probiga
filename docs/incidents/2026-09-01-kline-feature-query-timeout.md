# 2026-09-01 日 K 特征查询超时证据

## 影响

- 目标交易日：`2026-08-31`
- 生产版本：`2cbed9a44bf353d93aa654f1d6c8e40317a910c0`
- 推荐任务运行标识：`be2570dff8a742e8b49542bbfcd4de34`
- 任务耗时：`2373` 秒（约 39 分 33 秒）
- 终态：失败
- 用户侧表现：今日策略页面依赖的多项接口均在 `15000ms` 后超时，页面只能显示 `UNAVAILABLE`，没有形成可验证的策略池和票池。

## 原始失败证据

异常：

```text
pandas.errors.DatabaseError
(pymysql.err.OperationalError) (2013, 'Lost connection to MySQL server during query')
```

失败查询一次读取并排序全市场 90 个交易日 K 线，日期范围为 `2026-04-21` 至 `2026-08-31`：

```sql
SELECT k.stock_code, k.short_name, k.trade_date,
       k.open, k.high, k.low, k.close,
       k.volume, k.amount, k.change_pct,
       k.turnover_ratio, k.pre_close
FROM sm_stock_kline k
LEFT JOIN si_all_code a ON a.stock_code = k.stock_code
WHERE k.k_type = 1
  AND k.adjust_type = 0
  AND k.trade_date >= '2026-04-21'
  AND k.trade_date <= '2026-08-31'
  AND k.received_at <= '2026-08-31 22:20:00'
ORDER BY k.stock_code, k.trade_date;
```

该批次失败后没有继续运行策略治理和 V3 收盘决策。生产策略池仍指向 `2026-08-28` 的旧批次，其 `0` 行及 `AS_OF_POOL_DATE_INVALID` 不能解释为 `2026-08-31` 主动空仓。

## 修复边界与验收

1. K 线特征读取优先使用日期索引聚合；回退路径按少量交易日分批读取，不再执行一次性 90 日全量排序。
2. 每条 K 线查询有独立执行上限，整个 K 线阶段有总运行上限。
3. 超时、连接中断、分批读取失败或数据为空必须形成带原因码的 `DATA_BLOCKED` 终态，不能显示为有效空池。
4. 页面先读取统一决策上下文，再渐进加载辅助数据；单个辅助接口超时不能拖住决策真值的首屏展示。
5. 完整链路重跑后，应同时验证分析历史、策略治理批次、V3 决策上下文及页面展示日期均为 `2026-08-31`。
