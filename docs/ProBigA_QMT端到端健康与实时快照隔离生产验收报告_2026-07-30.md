# ProBigA QMT 端到端健康与实时快照隔离生产验收报告

验收时间：2026-07-30 21:19—22:12（Asia/Shanghai）  
生产主机：`47.113.123.190`  
生产目录：`/opt/ProBigA`  
QMT：国金 QMT 2.1.19，标准内置 Python 策略 `PROBIGA_BIGQMT_BRIDGE`

## 一、结论

本次四项改造已完成代码、数据库、Windows QMT 恢复链路和生产发布。

收盘后的生产验收结果为通过：

- QMT 策略心跳、全市场快照和数据库同步凭证三段链路均为 `PASS`；
- QMT 进程存活不再等同于行情健康；
- 心跳阈值为 30 秒，恢复任务每 5 秒检查一次；
- 自动登录和策略恢复改为 30—900 秒指数退避、持续重试、无每日三次上限；
- 页面已把“当前实时状态”和“最新历史快照”分开；
- 盘中结果超过 120 秒会显示“结果已过期”，并关闭可执行状态；
- 收盘后返回 `MARKET_CLOSED`，不会把最后一份历史快照冒充实时结果；
- 旧分钟凭证全部冻结为 `LEGACY_UNCLASSIFIED + forward_eligible=0`；
- 盘后补数只能写成 `AFTER_CLOSE_BACKFILL + forward_eligible=0`；
- 只有真实盘中采集才能写成 `LIVE_FORWARD + forward_eligible=1`；
- 真实下单开关仍为关闭。

由于验收发生在收盘后，本次产生的 73 份全市场同步凭证均正确标记为
`OFF_SESSION_SNAPSHOT`。它们只证明数据链正常，不会通过盘中交易门禁。
下一交易时段产生首份 `LIVE_FORWARD` 后，才形成真实盘中前向证据；该记录
不能在盘后伪造或回填。

## 二、改造内容

### 1. QMT 端到端健康检查

新增统一健康检查，必须同时满足：

1. QMT 内置策略心跳有效，年龄不超过 30 秒；
2. 全市场快照存在、股票数大于 0、年龄不超过 75 秒；
3. 最近完成入库的同步凭证为 `PASS`，凭证证明的源快照年龄不超过 75 秒。

全市场入库约需数秒至数十秒。生产者在入库期间可能已经生成下一代文件，
因此门禁允许“当前文件领先最近完成凭证一代”的有限流水线重叠，但凭证源
快照必须仍在 75 秒内。旧凭证、任意伪造凭证或停滞消费者仍会失败关闭。

相关文件：

- `integrations/bigqmt/health.py`
- `tools/check_big_qmt_end_to_end_health.py`
- `tools/run_big_qmt_bridge.py`
- `server/trading_v2/public_quote_failover.py`
- `server/trading_v2/intraday_activation.py`

### 2. 持续恢复、退避和报警

- 本地监督器检查周期由 30 秒缩短到 5 秒；
- 心跳超过 30 秒立即进入恢复流程；
- QMT 自动登录和策略启动不再“失败三次后当天放弃”；
- 连续失败按 30、60、120、240 秒递增，最高 900 秒，并持续重试；
- 每次失败、重试、消费者重启和恢复均写入
  `data/qmt_health_alerts.jsonl`；
- 配置企业微信或 QMT 告警 Webhook 后会同步推送；
- QMT 2.1.19 当前界面的搜索框、筛选后首行“编辑”和运行按钮已按真实
  界面重新校准，解决“策略编辑器打不开”的实际故障。

相关文件：

- `tools/start_local_live_services.ps1`
- `tools/run_local_live_supervisor.ps1`
- `tools/ensure_big_qmt_strategy_running.ps1`
- `tools/run_qmt_client_watchdog.ps1`

### 3. 实时状态与历史快照隔离

V2/V3 页面和只读仓储返回两个独立对象：

- `current_realtime_state`：只允许展示当前交易时段且年龄不超过 120 秒的
  实时状态；
- `latest_historical_snapshot`：只读复盘快照，永远不能据此创建当前买单。

盘中超过 120 秒时：

- 状态为 `STALE`；
- 页面显示“结果已过期”；
- `actionable=false`；
- 当前实时快照返回 `null`；
- 历史快照仍可查看。

收盘后：

- 状态为 `MARKET_CLOSED`；
- 当前实时快照返回 `null`；
- 最新历史快照单独展示。

相关文件：

- `server/trading_v2/repository.py`
- `server/static/trading-v2.html`
- `server/static/trading-v3.html`
- `server/static/js/trading-v2.js`
- `server/static/js/trading-v3.js`

### 4. 盘后补数与盘中前向记录隔离

分钟同步凭证新增：

- `capture_mode`
- `forward_eligible`

判定规则：

- 交易日当天；
- 采集发生于 09:30—11:32 或 13:00—15:02；
- 最新分钟线相对采集时间的滞后为 -5—120 秒；

三项全部满足才是 `LIVE_FORWARD + forward_eligible=1`。其他情况一律为
`AFTER_CLOSE_BACKFILL + forward_eligible=0`。历史旧记录迁移后统一为
`LEGACY_UNCLASSIFIED + forward_eligible=0`。

盘中决策查询显式要求：

```sql
capture_mode = 'LIVE_FORWARD'
AND forward_eligible = 1
```

因此盘后补齐数据不能替代真实盘中前向记录。

## 三、生产验收证据

### 1. 服务与迁移

| 项目 | 结果 |
|---|---|
| `probiga` | `active` |
| `probiga-scheduler` | `active` |
| `mysql` | `active` |
| 迁移版本 | `20260730_010_qmt_end_to_end_health` 已应用 |
| 生产发布时间 | 2026-07-30 22:06 |
| 最后回滚备份 | `/opt/ProBigA/.codex_backups/acceptance_20260730_220603` |

### 2. 真实 QMT 链路

最终一次检查：

| 检查项 | 结果 |
|---|---|
| 策略心跳 | `PASS`，年龄 3.2 秒 |
| 全市场快照 | `PASS`，5532 只，年龄 23.1 秒 |
| 同步凭证 | `PASS`，覆盖率 100% |
| 凭证模式 | `OFF_SESSION_SNAPSHOT` |
| 自动恢复状态 | `success`，连续失败 0 |

连续 12 次、约 40 秒采样：

- 12/12 次健康结果为 `PASS`；
- 实际观察到 3 次“新文件已生成、上一代凭证刚完成”的流水线重叠；
- 重叠期间仍保持健康；
- 21:34:05 之后不再出现错误的消费者重启报警。

### 3. 数据面路由

验收发现并修复了一项生产路由错误：

- QMT 行情与同步凭证位于 Windows 行情数据面；
- 生产通过反向 MySQL 通道读取该数据面；
- 原实现错误地从生产业务库查同步凭证，导致凭证永久为 0；
- 修复后同步凭证和 `sm_stock_current` 从同一 `current_engine` 读取；
- 生产业务库只负责股票全集和交易账本。

生产从行情数据面实读：

| 数据 | 结果 |
|---|---|
| QMT 当前行情 | 5532 只 |
| 实时同步凭证 | 73 条 |
| 凭证模式 | 全部 `OFF_SESSION_SNAPSHOT + PASS` |
| 生产业务库同名凭证表 | 0 条，符合数据面隔离设计 |

### 4. 分钟凭证隔离

生产通过分钟数据面实读：

| 模式 | `forward_eligible` | 条数 |
|---|---:|---:|
| `LEGACY_UNCLASSIFIED` | 0 | 12 |

非 `LIVE_FORWARD` 却被标记可前向的记录为 0。

### 5. 页面与只读仓储

收盘后生产返回：

| 字段 | 结果 |
|---|---|
| `status` | `historical` |
| `current_realtime_state.status` | `MARKET_CLOSED` |
| 当前是否可执行 | 否 |
| 当前实时快照 | `null` |
| 最新历史快照 | 存在 |
| 自动真实下单 | `false` |

V2/V3 生产静态文件均包含“结果已过期”、当前实时状态和最新历史快照的
独立渲染逻辑；本地与生产文件 SHA-256 完全一致。

真实浏览器访问生产交易页会被登录鉴权拦截并跳转到：

`/login?next=%2Ftrading-v3`

说明公网登录门禁仍在生效。

## 四、自动化回归

最终相关测试：

```text
27 passed
```

覆盖：

- 三段式 QMT 健康检查；
- 旧凭证和停滞消费者失败关闭；
- 正常流水线重叠不误杀消费者；
- 心跳 30 秒阈值和持续退避；
- 生产业务库与行情数据面分离；
- 盘中前向/盘后补齐分类；
- 实时状态与历史快照隔离；
- QMT 主源与公共替补门禁。

## 五、次日盘中复验项

以下不是代码遗留，而是只能在下一交易时段形成的真实前向证据：

1. 首条 `LIVE_FORWARD` 全市场同步凭证；
2. 首条 `LIVE_FORWARD + forward_eligible=1` 分钟凭证；
3. 生产页面 `current_realtime_state.status=LIVE`；
4. QMT 主源健康时不启用公共替补；
5. 任一心跳超过 30 秒时，在下一次 5 秒检查中进入恢复并报警；
6. 任一实时页面结果超过 120 秒时立即变为“结果已过期”。

在这些证据出现前，系统会失败关闭，不会用收盘快照或盘后补数冒充盘中
可交易信号。
