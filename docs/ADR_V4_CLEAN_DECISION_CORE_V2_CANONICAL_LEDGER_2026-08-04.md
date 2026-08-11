# ADR：V4 决策净室复用 V2 唯一交易账本

- 状态：Accepted for code structure and non-production Stage 3 research; production activation prohibited
- 日期：2026-08-04
- 决策范围：ProBigA Trading V2 / V3 / V4
- 取代：把 V4 投资逻辑直接增量写入 V3 决策链的方案
- 关联方案：`ProBigA动态交易决策系统V4净室重构与迁移实施方案_2026-08-03.md` V2.6

## 背景

V3 的市场状态、特征、校准、题材评分、组合与退出逻辑会污染 V4 的研究归因；但重新建设账户、订单、成交、现金、持仓 lot、费用和风控账本，又会制造双重真相、双重对账和不可接受的资金风险。

系统已完成隔离 Oracle MySQL 5.7.38 的非生产行为验收，但尚未完成正式 V3 冻结基线、PIT 因子认证、独立 OOS 校准或 paper 交易批准；数据库验收不构成生产激活。因此，代码结构裁决与生产激活必须分离。

## 决策

1. V4 使用独立的决策科学净室。V4 自有 universe、数据 lineage、因子、标签、模型、校准、预测、组合、动作和研究产物，不读取 V2/V3 的投资观点、评分、候选、权重或退出结论。
2. V2 继续拥有唯一 canonical 交易事实。账户、订单、成交、现金、持仓 lot、费用、证券规则、T+1、风控和对账不在 V4 重建。
3. V3 按架构定位为外部对照，正式 baseline 仍须人工确认并在仓库外存证。V3 只消费 V2 canonical 事件建立 read model，不得反向写入或取代 V2 账本；V3 与 V4 不做决策双写。
4. V4 可使用独立 paper account，但该账户仍是 V2 唯一账户表中的隔离账户，不形成第二套账务系统。
5. `011`～`015` execution evidence、authority attestation 和 accounting finalization 是绑定既有 V2 事实的 append-only 证据层，不是新的事实账本。
6. canonical commit 必须在调用方拥有的同一事务中完成 V2 facts、execution/accounting evidence 和强制 V3 transition outbox 回执；任何一步失败都由调用方回滚整笔事务。
7. V3 transition outbox 四表不进入 V2 `MIGRATIONS`；它们已作为 `20260804_001_v3_execution_projection_outbox` 纳入 V3 forward-only migration，并完成隔离 MySQL 验收。worker 仍然禁用，数据库验收本身不授予部署或运行权限。
8. V2 execution-evidence 迁移使用单行 maintenance fence。writer 在同一事务持有共享锁；runner 排空 writer 后以 CAS 发布 `ACTIVE`，中断时保持 `ACTIVE`，完整结构、ledger 和行审计通过后才以 CAS 发布 `INACTIVE`。
9. 阶段 2 的唯一 concrete decision kernel 是固定身份、固定阻断原因的 `BlockedDecisionKernel`；它只能产生无 forecast/action/intent 的 `DATA_BLOCKED` 结果。runtime control 只能通过 append-only transition + CAS 改变非授权配置，永久生产/actionable/paper-outbox 硬门不可放宽。
10. forward-only `20260804_002_v4_job_lease_repair` 在不改写 `001` 的前提下补齐独立 lease token、持久化最大尝试数、due/active-token 索引和状态/租约 trigger；caller-owned JobStore 采用数据库 UTC 时钟、最长 900 秒租约和完整 owner/token/attempt/until/updated CAS。隔离 MySQL 并发/恢复/权限验收及 `003` append-only 历史 token registry 已完成，但它们不构成 worker，也不授予调度、paper 或 production 权限；启用仍须正式 V3 baseline、独立 paper/worker 授权和部署接线。

## 不变量

```text
one_account_ledger = V2
one_order_ledger = V2
one_fill_ledger = V2
one_cash_ledger = V2
one_position_lot_ledger = V2
one_risk_ledger = V2

OOS = BLOCK
production_activation_allowed = false
actionable_output_allowed = false
paper_buy_outbox = closed
```

任何 migration 参数、TEST/CI capability、运行时配置、离线测试或 schema-ready 报告都不得改变上述运行状态。短期进程内 HMAC capability 只防普通误接线，不是抵抗同一受信进程内恶意代码的安全边界。

## 允许复用与禁止复用

允许复用：

- V2 canonical 账户、订单、成交、现金、lot、费用、证券规则、日历和机械风险事实；
- 经只读 facade 映射并带来源、时点、版本和哈希的事实；
- 策略中立的 OMS、matcher、保护 supervisor 和绝对风险包络；
- V3 作为冻结对照的输出和评价结果，但不得作为 V4 输入。

禁止复用：

- V2/V3 的市场状态、题材评分、候选排名、模型概率、组合权重、开仓比例和退出结论；
- 默认账户、隐式 fallback、无 lineage 的数据或无法证明 knowledge time 的历史数据；
- 任何绕过 canonical commit、maintenance fence、schema gate 或 V2 账本直接写入的路径。

## 后果

正面后果：

- V4 的预测改善可以与执行差异分开归因；
- 不增加第二套账户和资金真相；
- V3/V4 可在同一机械执行合同下公平双跑；
- 证据层和 read model 的失败不会被误认为 canonical 事实成功。

代价与限制：

- V2 执行中立化与隔离 MySQL 非生产验收已完成；正式 V3 冻结、PIT/OOS 认证和独立 paper 批准仍是阶段 3 及运行接线前的硬前置；
- V4 必须重新认证 PIT 数据、因子、标签、模型和校准，不能沿用 V3 结果快速上线；
- 事件、行业、预测、组合和盘中 worker 在前置门未通过前只能设计或做纯离线实验，不能产生 actionable 输出；已有 job lease schema/JobStore 只解决控制面持久化合同，不等于 worker 可启用。

## 验证与解除门槛

进入 PIT 因子实现前，至少需要：

1. 正式、人工确认并外部保存 hash 的 V3 baseline manifest；
2. 隔离 Oracle MySQL 5.7.38 上的 V2 `001`～`015`、V3 outbox、V4 `001`～`004` 串行、并发和中断恢复验收已经完成；这些测试不等于生产库授权；
3. V2 core/authority/accounting 与 V4 的负向 DML、三层非空 stored-row auditor、数据库 `SHA2` 和共享锁验收已经完成；密钥运营、外部信任锚和生产权限仍需独立人员复核；
4. 五个运行身份的精确表级与 allowlist 列级权限正向审计，以及额外表、全局、库级、未声明列、routine、`IS_GRANTABLE` 与 `PROXY` 越权检查已经完成；projection worker 的 payload、plan identity、无关 V4 表和 canonical order UPDATE 真实低权负测均被 MySQL 拒绝；
5. 固定 62 文件矩阵通过 `1302 passed, 659 warnings`，包含它的 76 文件扩展矩阵通过 `1576 passed, 659 warnings`；两组不得相加，也不得用本地回归替代真实数据库验收。

解除 paper buy、actionable 或 production 门需要新的独立 ADR 和显式批准；本 ADR 不授予该权限。

## 被拒绝的替代方案

- 直接升级 V3 为 V4：拒绝，因为旧特征、模型、组合和持仓会污染净室归因。
- 新建完整 V4 交易系统：拒绝，因为会产生平行账户、订单、现金、持仓和风控账本。
- 先启用 paper buy 再补数据与验收：拒绝，因为当前 OOS、PIT、正式 V3 冻结与 paper 批准仍未通过；MySQL 非生产验收和 TEST/CI runtime seam 虽已通过，但不授予运行权限。
- 用配置跳过 gate：拒绝，因为配置不能证明数据、结构、事务或生产授权成立。

## 2026-08-04 阶段 3 增量裁决

本轮按用户授权继续在既有 V2/V3 底座上增量实现，但没有把 V4 投资判断写回 V2/V3。该选择不会降低选股结果质量：V4 因子、事件、追高风险和截止时点证据在独立决策边界内计算；V2 只在动作形成后提供唯一账户、订单、成交、现金、持仓、T+1、费用和风险事实，并在实际成交前复验买入资格。这样既能保持预测归因独立，又能避免第二套账本导致的重复订单、双重现金和持仓漂移。

新增不变量如下：

1. 新开仓和加仓必须同时满足推荐闸门、信号确认闸门、追高闸门和普通买入资格；缺字段、过期、哈希不一致、数据源不健康或不可成交均失败关闭。分数、主升浪、题材热度和历史推荐本身不能构成买入资格。
2. 上述证据必须绑定股票、交易日、决策截止时点、有效期和内容哈希，并由 V2 在撮合前、成交前于同一事务中再次读取验证。部分成交后复验失败只能取消剩余数量，不能回滚已经合法完成的成交。
3. 持仓退出不依赖候选池，也不被新买入数据门阻断。止损、止盈、趋势破坏、重大负面事件和风险收缩可以输出 `REDUCE`/`SELL`；数据暂缺只能阻止新的风险暴露，不能强制把风险持仓锁死为 `HOLD`。
4. 新闻和大事件已进入风险与数据健康因子：在截止时点只读取当时可见记录，校验来源水位、发布时间、采集时间和有效期。当前实现能做负面/高风险阻断与持仓退出，但尚未形成经 OOS 校准的正向“新闻预测收益”模型，因此不得把新闻热度直接解释为买入信号。
5. 没有 acquisition cutoff 和 lineage 证明的当前态旧因子一律中性化，并在 `disabled_factor_inventory` 中留证；不能通过运行参数、互动记录、历史推荐、失败样本或当前持仓快照回填历史决策。
6. 模拟交易可以复用同一决策合同做非生产验收，但仍不成为新的 canonical 账本，也不授予 paper worker、可交易输出或真实下单权限。

当前业务 MySQL 为 5.5.20，而 Stage 3 forward-only DDL 的最低支持版本是 MySQL 5.7。迁移器必须在执行任何 V4 ledger/table DDL 前拒绝该版本；在生产数据库拓扑升级或迁移并完成独立审批前，业务库不能应用 V4 migration 或写 FactorStore。旧模拟盘为绑定执行闸门做过一次已留证的加法式兼容 DDL，不改变这一 V4 部署阻断。该操作是历史留证，不是现行运行时 schema 策略。隔离 MySQL 验收、SQLite 单元测试或模拟盘成功都不能绕过这个部署前置。

模拟盘 schema 生命周期的现行裁决如下：

1. import、`SimTradeEngine` 构造以及 GET 路径中的 `_ensure_tables()` 只能调用 `_require_sim_execution_schema()` 做只读 metadata 验证，不得自动 `CREATE`/`ALTER`。
2. 缺表、缺必需列或非 InnoDB 必须 fail-close 并向调用方报错；禁止以旧 schema fallback 继续执行或由读路径补表。
3. 唯一运维迁移入口是 `python tools/migrate_sim_trade.py --allow-schema-change`。CLI 必须显式收到该开关，内部迁移函数也必须显式收到 `allow_schema_change=True`；它不能被 import、构造或 GET 隐式触发。
4. 列/索引 DDL、metadata 检查或迁移后复验任一失败都必须原样向上抛出，不得 catch-and-continue、记录后吞异常或宣告 schema ready。

本裁决对应的最终代码验收为：全仓 `2696 passed, 1 skipped`、0 failed；固定 62+14+20 门禁共 96 个文件，`2050 passed, 1 skipped`、0 failed，且三份 manifest 的固定 SHA-256 校验通过。测试通过不构成生产激活授权。

因此本 ADR 的最终运行状态仍为：

```text
production_activation_allowed=false
actionable_output_allowed=false
prepared_commit_runtime_enabled=false
v3_projection_worker=disabled
paper_buy_outbox=closed
```
