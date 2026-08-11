# Trading V2 `001`～`015` migration runbook

## 1. 当前结论与硬边界

当前代码声明完整迁移链 `001`～`015`，冻结口径为 **15 个编号 migration / 150 条
migration-owned statement**。其中 `001`～`010` 是既有 V2
事实账本底座，`011`～`015` 是在同一底座上追加的执行证据、外部权威和会计结果证据。
它们不会创建第二套账户、订单、成交、现金、持仓或风控账本：新增证据都引用并核验既有
V2 canonical facts，事实所有权仍在既有 V2 表。

现行 `001`～`015` 已在隔离、全空的 Oracle MySQL Community 5.7.38 测试库完成串行、并发、
行为和 11 个中断恢复场景的非生产验收；验收实例为 `127.0.0.1:33578`，业务 MySQL 3306 未访问。
该结果只证明本轮 artifact 的测试边界，不授权生产迁移、writer 接线或交易输出。当前状态必须始终是：

```text
production_activation_allowed=false
actionable_output_allowed=false
actionable=false
```

`actionable=false` 是上述输出门的业务解释；代码报告中的正式字段名是
`actionable_output_allowed`。`--allow-execution-evidence` 只允许显式应用或恢复
`011`～`015` DDL；当维护围栏因中断保持 `ACTIVE` 时，它也是唯一允许继续恢复的显式开关。
它不是生产授权、实盘授权或 writer 接线授权。

## 2. `001`～`015` 迁移清单

所有版本都是 forward-only。已经写入 `schema_migration_v2` 的版本及 checksum 不得修改；
同版本 checksum 不一致必须立即停止并新增后续 migration。

| 版本 | 作用 | 结构增量 |
|---|---|---:|
| `20260725_001_trading_v2_core` | V2 账户、策略、快照、决策、计划、意图、风控、订单、成交、lot、现金、权益、对账和事件核心事实 | 15 tables |
| `20260725_002_trading_v2_jobs_and_lifecycle` | 作业与策略生命周期 | 2 tables |
| `20260725_003_trading_v2_execution_research_ops` | 行情事件、执行能力、费用/证券规则、回测、健康、故障演练和 worker 心跳 | 9 tables |
| `20260725_004_trading_v2_etf_truth_and_forward` | ETF 基础事实、K 线和前向观察 | 4 tables |
| `20260725_005_trading_v2_theme_risk_chain` | 在 signal、intent、position lot 上追加 `theme_code` 风险链及索引 | 4 ALTER statements |
| `20260726_006_real_trading_hard_guard` | 数据库层禁止把 V2 账户切到真实交易 | 2 triggers |
| `20260726_007_market_regime_transition_state` | 决策运行表的市场状态迁移字段 | 5 ALTER statements |
| `20260727_008_intraday_dynamic_activation` | 盘中市场状态、动态激活、QMT 分钟回执和观察行情 | 4 tables |
| `20260730_009_public_quote_failover` | 公共行情当前值与接收回执 | 2 tables |
| `20260730_010_qmt_end_to_end_health` | QMT 实时同步回执及分钟回执健康字段 | 1 table + 4 ALTER statements |
| `20260803_011_v2_execution_evidence_bindings` | 五类 canonical execution-evidence 绑定 | 5 tables / 5 statements |
| `20260803_012_v2_execution_evidence_guards` | 五张执行证据表的 `BEFORE INSERT/UPDATE/DELETE` fail-closed 与 append-only 保护 | 15 triggers / 30 DROP+CREATE statements |
| `20260803_013_v2_execution_evidence_natural_keys` | 补齐 calendar `(market_code, trade_date, calendar_version)` 自然键唯一约束 | 1 ALTER statement |
| `20260803_014_v2_execution_authority_attestations` | 外部权威 trust key、签名 receipt、两类撤销和 cryptographic attestation，并把权威证明绑定到 evidence INSERT | 5 tables + 17 triggers / 39 statements |
| `20260803_015_v2_accounting_outcome_evidence` | fill 会计结果、逐 lot effect 和唯一 `FINAL` 完成标记 | 3 tables + 9 triggers / 21 statements |

编号 migration 完成后的冻结库存必须精确等于：

| 分段 | Tables | Triggers |
|---|---:|---:|
| `schema_migration_v2` + `001`～`010` | 38 | 2 |
| `011`～`013` execution evidence | +5 | +15 |
| `014` authority | +5 | +17 |
| `015` accounting evidence | +3 | +9 |
| **编号 migration-owned 合计** | **51** | **43** |

`schema_migration_v2_maintenance_fence` 是 migration runner 在编号 migration 之外创建的
bootstrap 控制表，不属于 `MIGRATIONS`、150 条 statement 或 migration checksum ledger。
因此最终物理 V2 库存必须是 **51 张 migration-owned 表 + 1 张非编号 bootstrap 控制表 =
52 张物理表**，trigger 仍然精确为 **43 个**。围栏表只能有一行
`fence_name=execution_evidence_011_015` 的控制记录；完整迁移或重放结束后该行必须为
`INACTIVE`，缺行、多行或 `ACTIVE` 都不是可接受的最终状态。

这里的 **17 个 evidence-table triggers** 专指五张核心执行证据表上的 15 个
`INSERT/UPDATE/DELETE` guard，加上 `014` 在 calendar 和 quote INSERT 上追加的 2 个
authority guard。它不包含 authority 自有五张表的 15 个 trigger，也不包含 accounting
三张表的 9 个 trigger。不要把 `server.trading_v2.repository.V2_TABLES` 的核心仓储子集
数量误当成完整迁移库存。

公开的 execution-evidence Schema Gate 默认必须同时精确检查 `011`～`015` 的 **13 张证据相关表**
和其上的 **41 个相关 trigger**，包括 column/default/collation、`ROW_FORMAT`、index/unique、FK、
trigger body、执行上下文与围栏状态。公开 gate 一旦看见任意 `014` 或 `015` 对象，就把该未来层
整体提升为精确校验，不能忽略残缺前向对象。只有 migration runner 的
`phase_scoped_migration_replay` 恢复路径，才可在校验当前目标阶段时暂时忽略尚未写入 ledger 的
未来层残留；该例外不得暴露给普通 public gate，也不得弱化最终全层检查。ledger 已记录但表或
trigger 被删除时必须失败关闭，不能只凭 checksum 判定结构就绪。

冻结 DDL 需要的 25 个 MySQL 隐式外键支持索引按 **table + 有序 child columns** 校验合同，
不冻结 MySQL 自动生成的索引名称。每个额外非唯一索引都必须一一对应所需 FK child-column tuple，
列顺序、完整列、BTREE 与升序元数据必须精确；缺失、重复、前缀索引、错误类型或无关额外索引
都必须失败关闭。

`server/integrations/v3_execution_projection_outbox/schema.py` 中的四张表不接入 V2 `MIGRATIONS`：
outbox、worker checkpoint、order audit baseline 与 dead-letter reconciliation。它们不属于上述
51 张 migration-owned 表、52 张物理 V2 表或 43 个 trigger，也不得由本 V2 runbook 顺带部署。
四表已作为 `20260804_001_v3_execution_projection_outbox` 纳入 V3 forward-only 末尾 migration，
并完成隔离 MySQL 结构、串行重放、并发首次迁移和 partial-DDL 恢复验收。canonical commit 仍必须
获得严格 V3 持久化回执；worker 必须核对订单审计基线和连续序号，dead-letter 重入必须使用
TEST/CI capability、完整审计与 compare-and-swap。常驻 worker 继续禁用，数据库验收不构成生产激活。

### 2.1 维护围栏与恢复语义

所有 execution-evidence writer 必须在将要追加证据的**同一事务**内读取维护围栏并持有
`LOCK IN SHARE MODE`；只允许唯一控制行为 `INACTIVE` 时继续。migration runner 在执行
`011`～`015` 前使用排他锁等待既有 writer 事务排空，把同一控制行持久化为 `ACTIVE`，然后才允许
进入会隐式提交的 DDL 边界。

任何 DDL、结构校验、ledger 写入或行审计故障都必须让围栏继续保持 `ACTIVE`；不得在 `finally`
或异常清理中自动解除。此时普通运行失败关闭，只有显式
`--allow-execution-evidence` 才能在同一 artifact、同一数据库身份与已知中断边界上继续恢复。
runner 只有在 `001`～`015` ledger/checksum、52 张物理表、43 个 trigger、结构合同以及迁移期
行状态全部审计通过后，才能把唯一控制行提交为 `INACTIVE`。围栏只能协调写入与迁移窗口，不能
把 `production_activation_allowed` 或 `actionable_output_allowed` 改为 `true`。

## 3. `011`～`013`：五类执行证据

`011` 按依赖顺序新增：

1. `st_market_calendar_evidence_v2` (`MARKET_CALENDAR`)
2. `st_quote_receipt_evidence_v2` (`QUOTE_RECEIPT`)
3. `st_fill_execution_evidence_v2` (`FILL_EXECUTION`)
4. `st_cash_event_binding_v2` (`CASH_EVENT`)
5. `st_order_transition_v2` (`ORDER_TRANSITION`)

`012` 为每张表建立三个 MySQL 5.7 trigger。`BEFORE INSERT` 重验关键跨字段、JSON、SHA-256、
父表和链关系；`BEFORE UPDATE`、`BEFORE DELETE` 一律拒绝，从数据库层落实 append-only。
MySQL 5.7 不执行 `CHECK`，不能把 DDL 中的 `CHECK` 当作保护证据，关键不变量必须由
trigger、唯一键、外键、writer 校验和 exact read-back 共同成立。

`013` 补齐 calendar 自然键。五个公共 writer 对同一自然键执行锁读：同内容重放返回
`IDEMPOTENT`，异内容返回 `EvidenceAppendConflictError`；不得用
`ON DUPLICATE KEY UPDATE` 把异内容覆盖成“幂等”。

## 4. `014`：外部权威边界

内容 hash 只能证明内容身份，不能证明来源权威。`014` 新增以下五张 append-only 表：

- `st_execution_authority_trust_key_v2`
- `st_execution_authority_receipt_v2`
- `st_execution_authority_key_revocation_v2`
- `st_execution_authority_receipt_revocation_v2`
- `st_execution_authority_attestation_v2`

`EXTERNAL_RECEIPT_VERIFIED` 必须经过 registry-backed Ed25519 路径：在调用方同一事务中读取并
核对 exact claim、provider/key/version、receipt/envelope、replay nonce、有效期、签名和
key/receipt 撤销状态，再追加 `CRYPTOGRAPHIC` attestation。calendar/quote 的 authority
INSERT trigger 还会重验 attestation、receipt、trust key、时点和当前未撤销状态；缺失或不唯一
均以 SQLSTATE `45000` 失败关闭。默认 verifier 拒绝全部权威声明，低层调用方不得用自定义
verifier 绕过 registry。

权威证明的代码和数据库边界已完成隔离 MySQL 行为验收，包括 trust key/receipt 登记、nonce、
签名失败、key/receipt 撤销、并发登记、PIT 时序和历史重验；这不代表生产信任源登记、私钥管理、
轮换/撤销运营或外部信任锚已经获批。

`014` 的五张 authority 表已有独立 stored-row auditor。它在调用方事务内按固定顺序全表
`LOCK IN SHARE MODE`，同时核对数据库 `SHA2`、Python canonical preimage、Ed25519 签名、
parent evidence、唯一性与撤销时序；合法空表和合法非空表都可通过。任何一行无法重建时，
runner 不写入该 migration ledger，maintenance fence 保持 `ACTIVE`。隔离 MySQL 5.7.38 上的
非空扫描、数据库 `SHA2`、共享锁和并发/撤销行为已经验收；密钥运营和生产权限仍须独立复核。

## 5. `015`：会计结果证据与最终完成语义

`015` 新增：

- `st_fill_accounting_outcome_v2`：把既有 fill evidence、`BUY_FILL`/`SELL_FILL` cash binding、
  `FILL_APPLIED` order transition、账户现金 before/after 和 lot effect 汇总绑定起来；
- `st_lot_transition_evidence_v2`：BUY 必须是完整的单个 `BUY_CREATE`，SELL 必须按确定性 FIFO
  顺序记录全部 `SELL_FIFO_CONSUME` before/after transition；
- `st_fill_accounting_outcome_finalization_v2`：每个 outcome 唯一、append-only 的强完成标记，
  唯一合法状态是 `FINAL`。

accounting writer 不拥有 engine 或事务，也不计算、更新或替代既有账户余额、订单、fill、现金
和 lot。它在调用方已经开启的同一事务中按固定顺序锁读 canonical rows，先追加 outcome，
再追加全部 lot effects，最后追加 `FINAL` marker 并 exact read-back。任何数据库异常都交给
调用方回滚整个事务。

三张 `015` 表都显式使用 `ROW_FORMAT=DYNAMIC`，不能依赖实例默认行格式承载 utf8mb4 复合
唯一键。所有进入 accounting hash 的时间字段遵循既有 V2 `DATETIME` 整秒合同；领域对象在
生成 hash 前即拒绝非零微秒，writer 和数据库层继续做防御性复核。

只有通过 `st_fill_accounting_outcome_finalization_v2` 内连接，且 parent、fill evidence、
lot-effect root/hash/count/quantity 和 provenance 全部一致、状态为 `FINAL` 的 outcome 才是有效
读取结果。只有 parent 或部分 effect 而没有 `FINAL` 的记录一律是 pending，不得被用于会计、
交易或 actionable 输出。这一层是现有 V2 事实的不可变审计证据，不是平行 accounting ledger。

`015` 三张 accounting 表也已有独立 stored-row auditor。它会共享锁读 outcome、lot effects、
`FINAL` marker 及所引用的 core evidence、account/order/fill/cash/lot 父事实，并同时核对数据库
`SHA2` 与 Python 重建的 provenance、FIFO effect 链、数量、root 和唯一完成语义。合法空表和合法
非空表都可通过；不完整或漂移记录会在 ledger 之前失败关闭并保持 fence 为 `ACTIVE`。隔离
MySQL 上的非空扫描、锁、表达式、完整/中断回滚、重放、冲突和 FIFO 已完成非生产验收；生产负载
下的性能容量仍须单独评估。

core、authority、accounting 三层全局 stored-row 扫描必须处于同一个稳定事务快照。runner 在进入
任一层审计前强制核对隔离级别，只接受 `REPEATABLE READ` 或 `SERIALIZABLE`；`READ COMMITTED`、
`READ UNCOMMITTED` 和 `AUTOCOMMIT` 一律在写 migration ledger 前失败关闭，避免跨表父子事实来自
不同提交时点。

## 6. 专用 MySQL 验收前提

基础验收的 `serial-replay`、`concurrent-initial`、`behavioral` 必须分别使用三座不同、外部可销毁、
完全为空的 Oracle MySQL Community/Enterprise 5.7.38 数据库。每个 recovery scenario 也必须
再使用一座独立空库，不能在前一个场景迁移后的库上继续。数据库名必须包含
`_v2_evidence_test` 或 `_v2_evidence_ci`，不得复用应用库、staging 库或历史验收库。

每座库使用独立最小权限账户，并通过独立 secret/config channel 记录预期 `@@server_uuid`。
验收工具不会清库，也没有放宽空库检查的开关。URL 与 UUID 必须来自两个显式 TEST/CI 环境变量；
禁止回退到 `MYSQL_URL` 或 `DATABASE_URL`。URL path 必须直接给出数据库名且不得带 query
parameter，防止 driver 参数改库或在身份检查前执行 SQL。

DDL 前必须 fail closed 地确认：

- URL database、runtime `DATABASE()` 和外部预期 `@@server_uuid` 三者一致；
- `VERSION()`、`@@version_comment` 和 dialect 精确识别 Oracle MySQL
  Community/Enterprise 5.7.38，而不是 MariaDB 或 Percona；
- `SHOW GRANTS FOR CURRENT_USER()` 仅允许目标 schema 上的 `SELECT`、`INSERT`、`UPDATE`、
  `DELETE`、`CREATE`、`ALTER`、`INDEX`、`REFERENCES`、`TRIGGER`，以及 global `USAGE`；
- 拒绝 global privilege、其他 schema privilege、`ALL PRIVILEGES`、grant option 和任何额外
  target-schema privilege，尤其不能授予 `DROP`；
- 目标 schema 的 table、stored routine 和 scheduled event 数量全部为零。

## 7. 基础验收命令

三种模式不得共用数据库：

```powershell
$env:V2_EVIDENCE_TEST_MYSQL_URL = '<dedicated empty serial DB URL>'
$env:V2_EVIDENCE_TEST_MYSQL_SERVER_UUID = '<expected @@server_uuid>'
python tools/trading_v2_evidence_mysql_acceptance.py `
  --mode serial-replay `
  --url-env V2_EVIDENCE_TEST_MYSQL_URL `
  --server-uuid-env V2_EVIDENCE_TEST_MYSQL_SERVER_UUID

$env:V2_EVIDENCE_CI_CONCURRENT_MYSQL_URL = '<different empty concurrent DB URL>'
$env:V2_EVIDENCE_CI_CONCURRENT_MYSQL_SERVER_UUID = '<expected @@server_uuid>'
python tools/trading_v2_evidence_mysql_acceptance.py `
  --mode concurrent-initial `
  --concurrency 2 `
  --url-env V2_EVIDENCE_CI_CONCURRENT_MYSQL_URL `
  --server-uuid-env V2_EVIDENCE_CI_CONCURRENT_MYSQL_SERVER_UUID

$env:V2_EVIDENCE_TEST_BEHAVIORAL_MYSQL_URL = '<third empty behavioral DB URL>'
$env:V2_EVIDENCE_TEST_BEHAVIORAL_MYSQL_SERVER_UUID = '<expected @@server_uuid>'
python tools/trading_v2_evidence_mysql_acceptance.py `
  --mode behavioral `
  --url-env V2_EVIDENCE_TEST_BEHAVIORAL_MYSQL_URL `
  --server-uuid-env V2_EVIDENCE_TEST_BEHAVIORAL_MYSQL_SERVER_UUID
```

`serial-replay` 必须得到一次完整 all-`applied`、一次完整 all-`exists` 重放；
`concurrent-initial` 必须得到一个完整 all-`applied` owner 和完整 all-`exists` followers，不能接受
不同 runner 按 migration 交替取得所有权。两种模式最终都必须核对 15 条 ledger、150 条冻结
statement、51 张 migration-owned 表、1 张 bootstrap fence 表、合计 52 张物理表、43 个 trigger、
17 个 evidence-table trigger、业务表全空、唯一 `INACTIVE` 围栏行和完整 metadata shape。

### 7.1 固定本地回归清单

本地回归不再通过临时 glob 推导文件集合，而使用两个纳入代码审查的显式 manifest：

- `tests/manifests/trading_v234_regression_62.txt`：固定 62 个 V2/V3/V4、集成与架构测试文件；
- `tests/manifests/trading_core_compatibility_14.txt`：在上述基础上追加 14 个 trading-core
  compatibility 文件，扩展矩阵合计 76 个文件。

统一入口会检查 manifest 文件数、重复路径和缺失文件，并打印 Git HEAD 与 manifest SHA-256：

```powershell
# 固定 62 文件矩阵
powershell -ExecutionPolicy Bypass -File tools/run_trading_v4_regression.ps1

# 固定 76 文件扩展矩阵；它包含前述 62 文件，结果不得与 62 文件矩阵相加
powershell -ExecutionPolicy Bypass -File tools/run_trading_v4_regression.ps1 -IncludeTradingCore
```

本轮最终执行结果为：固定 62 文件矩阵 `1302 passed, 659 warnings`；包含它的 76 文件扩展矩阵
`1576 passed, 659 warnings`，两组不得相加。执行时的 base Git HEAD 为
`d00dd09caba7ae962b3ce66e85eac34c503b035e`，62 文件清单 SHA-256 为
`e2fcabf530efab10dd7724a6d742cd2976735c0e6783650219dd5fa1de85024f`，14 文件补充清单
SHA-256 为 `30322345896fccf67c0473cf8d741ba0e41fd3ac79259ea435a88aa18ec73e93`。当前工作树不是
clean artifact，HEAD 只标识基线提交，不能单独证明工作树内容。650 条 warning 均为 Python 3.14.3
运行环境下 SQLite 默认 date/datetime/timestamp adapter 的弃用提示，不是测试失败。这两组本地
回归不能替代数据库验收；本轮另在独立 `127.0.0.1:33578` Oracle MySQL 5.7.38 测试实例完成
V2/V3/V4 串行、并发、中断恢复、行为与最小权限验收，未访问业务 MySQL 3306。数据库验收仍不
授予生产或可交易权限，`production_activation_allowed=false` 且 `actionable_output_allowed=false`。

## 8. 当前 `behavioral` 完整覆盖

当前 `behavioral` 声明并实际验收七类范围：

```text
MARKET_CALENDAR -> QUOTE_RECEIPT -> FILL_EXECUTION -> CASH_EVENT -> ORDER_TRANSITION
-> EXTERNAL_AUTHORITY_REGISTRY -> ACCOUNTING_OUTCOME_FINALIZATION
```

五类 core execution evidence 均覆盖：

- 合法 INSERT、新事务 exact replay 和外层事务整批 rollback；
- no-op UPDATE、DELETE、`INVALID_INSERT`、`REPLACE`、`ON_DUPLICATE_KEY_UPDATE` 的
  SQLSTATE `45000` / errno `1644` 拒绝及原行保留；
- 两连接相同内容 double-writer 收敛为一个 `INSERTED`、一个 `IDEMPOTENT`、只保留一行；
- 同一自然键异内容 double-writer 只保留 winner，loser 在新事务中得到
  `EvidenceAppendConflictError`；
- shared row locks 下由 MySQL `SHA2` 重算 canonical payload，随后由 Python 重建对象及
  cash/order chain。

`014` authority 扩展矩阵覆盖 trust key 与 receipt 登记、nonce replay、错误签名、key/receipt
撤销、并发 `INSERTED+IDEMPOTENT`、PIT chronology 和历史重验。`015` accounting 扩展矩阵覆盖
outcome -> ordered lot effects -> 唯一 `FINAL` 顺序、中断和整批回滚、exact replay、异内容冲突、
SELL FIFO 以及“只有 finalized outcome 可见”。core、authority、accounting 三层 auditor 均要求
非空表、数据库 `SHA2`、共享锁和 Python canonical 重建全部通过。

当前报告中的精确声明是：

```text
behavioral_not_covered = ()
behavioral_probes_not_covered = ()
all_declared_evidence_types_covered = true
```

`TRUNCATE` 不触发 DELETE trigger，因此保护依赖运行身份没有 `DROP` 权限。隔离 MySQL 的最小权限
已直接验收；生产账号仍必须用同一 grant 合同和只读 audit CLI 独立复核。上述结果证明非生产行为
边界，不等于生产信任源、容量、部署或交易授权已经完成。

## 9. `011`～`015` DDL 中断恢复验收

MySQL DDL 会隐式提交。named lock 负责串行 migration runner；bootstrap maintenance fence 则通过
writer 的共享锁与 runner 的排他锁阻止迁移窗口内新增 execution-evidence 写入。
`DROP TRIGGER` 与配对 `CREATE TRIGGER` 之间仍可能出现临时保护缺口，因此恢复验收必须在独立
空库中注入中断，并核对中断时 ledger 精确停在目标 migration 之前、部分对象库存符合预期、
业务表仍为空且唯一围栏行持续为 `ACTIVE`。故障后必须换新连接，并由 recovery harness 对 runner
显式传入 `allow_execution_evidence=true`（部署 CLI 对应 `--allow-execution-evidence`）后恢复；
runner 的 phase-scoped 检查只在此恢复阶段忽略未记账的未来层残留。恢复后再重放一次
all-`exists`，并在最终全层结构、ledger 与空行审计通过后回到 15 ledger /
150 statements / 51 migration-owned tables + 1 bootstrap fence table / 43 triggers /
17 evidence-table triggers，唯一围栏行为 `INACTIVE`。

当前定义了以下 **11 个**场景：

| Scenario | 注入边界 |
|---|---|
| `011-ddl-prefix` | `011` 已提交前 2 个 table DDL 后中断 |
| `012-drop-create-boundary` | 先在第 2 条语句后留下一个已创建 trigger，再于 fresh retry 的第 1 条 DROP 后中断，验证最终恢复配对 CREATE |
| `012-before-ledger` | `012` 全部 trigger DDL 已提交、写 migration ledger 前中断 |
| `013-after-ddl` | `013` 唯一 ALTER 已提交后中断 |
| `013-before-ledger` | `013` 结构完成、写 migration ledger 前中断 |
| `014-ddl-prefix` | `014` 已提交前 2 张 authority table 后中断 |
| `014-drop-create-boundary` | 先在第 7 条语句后留下首个 authority trigger，再于 fresh retry 的第 6 条 DROP 后中断 |
| `014-before-ledger` | `014` 的 5 张表和 17 个 trigger 完成、写 ledger 前中断 |
| `015-ddl-prefix` | `015` 已提交前 2 张 accounting table 后中断 |
| `015-drop-create-boundary` | 先在第 5 条语句后留下首个 accounting trigger，再于 fresh retry 的第 4 条 DROP 后中断 |
| `015-before-ledger` | `015` 的 3 张表和 9 个 trigger 完成、写 ledger 前中断 |

每次 invocation 只运行一个场景，而且必须换一座全新空库：

```powershell
$env:V2_EVIDENCE_TEST_RECOVERY_MYSQL_URL = '<one fresh empty DB for one scenario>'
$env:V2_EVIDENCE_TEST_RECOVERY_MYSQL_SERVER_UUID = '<expected @@server_uuid>'
python tools/trading_v2_evidence_mysql_recovery_acceptance.py `
  --scenario 011-ddl-prefix `
  --url-env V2_EVIDENCE_TEST_RECOVERY_MYSQL_URL `
  --server-uuid-env V2_EVIDENCE_TEST_RECOVERY_MYSQL_SERVER_UUID
```

把 `--scenario` 替换为上表其余名称，并为每次运行重新 provision 空库。本轮已为上述 11 个场景
分别使用独立验收库注入一次真实中断，并全部恢复至 15 条 ledger、52 张物理表、43 个 trigger，
唯一 maintenance fence 行为 `INACTIVE`。这些库只用于隔离验收，不能复用为生产部署证据。

## 10. 安全部署顺序

当前不允许生产执行 `011`～`015` 或接入 writer。隔离 MySQL 与测试身份权限验收已经完成；后续
只有在冻结 artifact、目标库变更审批、生产身份独立复核和显式生产裁决全部完成后，才能按以下
顺序进行一次 future cutover：

1. 冻结待部署 artifact，从同一份代码生成 `001`～`015` version/checksum manifest，并外部保存。
2. 运行 `python tools/backup_trading_v2_schema.py`；确认备份包含数据库版本、全部现存 V2 DDL、
   trigger、migration ledger 和 row counts。
3. 读目标库 `schema_migration_v2`，逐版本核对 manifest。缺版本才能继续；同版本异 checksum
   立即停止，禁止覆盖 ledger 或修改旧 migration。
4. 停止所有 V2/V3 canonical writers，或撤销其 `INSERT/UPDATE/DELETE` 权限；确认没有活动写事务。
5. 运行 `python tools/migrate_trading_v2.py --dry-run --json`，人工核对只包含预期 forward changes。
6. 只有正式批准后才运行
   `python tools/migrate_trading_v2.py --allow-execution-evidence --json`。runner 必须先等待 writer
   共享锁排空并持久化 `ACTIVE`；若任一步失败，保持 `ACTIVE` 并停止，禁止人工改为 `INACTIVE`。
7. 核对 15 条 migration ledger、150 条冻结 statement、51 张 migration-owned 表、1 张非编号
   bootstrap fence 表、合计 52 张物理表、43 个 trigger、17 个 evidence-table trigger、全部
   checksum、column/index/FK/trigger body、definer、SQL mode、charset/collation 和预期 row state。
   25 个隐式 FK 索引按 table + ordered child columns 校验，不按自动生成名称校验；最终围栏必须
   恰好一行为 `INACTIVE`。
8. 再运行一次 migration；15 个结果必须全部是 `exists`。
9. 运行 bootstrap 两次；第二次必须没有新增 account/strategy 和 metadata correction，并核对
   `paper-main-v2` 只有一个 `INITIAL_DEPOSIT`、初始现金与余额均为 CNY 200,000.00。
10. 核对 `real_trading_enabled=0`、股票策略为 `RESEARCH`、ETF 策略为 `SHADOW`，GET endpoint
    不写数据、不执行 DDL。
11. TEST/CI runtime seam 已接入现有 V2 caller-owned transaction：facts、evidence 和强制 V3
    transition outbox 任一缺少持久化回执或失败都回滚整笔。实际 `_execute_one` 测试覆盖 expiry、
    waiting-reason change 与 outbox 失败回滚；fill 已覆盖 prepared adapter、evidence、accounting
    finalization 和 outbox 编排，但尚不是 `_execute_one` 端到端 fill 验收。preparer 只接收不可变
    mutation，不获得 caller 的裸 connection，且 adapter 在回调后重新验证 transaction identity。
    authority/accounting 三层
    非空 MySQL 审计，以及 V3 `20260804_001_v3_execution_projection_outbox` 的结构、并发和恢复验收
    已完成；production cutover、常驻 worker、fill caller E2E 和目标库性能仍须独立批准与验收。
12. 即使上述结构和行为验收通过，也继续输出
    `production_activation_allowed=false`、`actionable_output_allowed=false`，直到另一项独立、显式的
    生产激活变更获得批准。不得通过配置或本 migration flag 绕过该门。

## 11. 回滚与故障处理

应用回滚时恢复 `/opt/ProBigA/.codex_backups` 中的上一版文件并重启服务。保留 V2 表、
migration ledger 和不可变证据供审计，不得 drop 或 truncate。

schema 修正必须发布新的 forward-only migration。不得改写已应用 checksum。成交、现金事件、
已完成决策、对账和证据错误通过 reversal event 或新版本纠正，不能原地 UPDATE/DELETE。

若 migration 中断：保持停写，记录当前 connection identity、ledger、table/trigger inventory 和错误；
确认唯一 maintenance-fence 行仍为 `ACTIVE`，不要手工补写 ledger、删除控制行、强制改为
`INACTIVE` 或跳过结构门。仅在同一 artifact、同一目标数据库身份和已知中断边界下，通过显式
`allow_execution_evidence` 恢复调用执行幂等恢复；最终全量结构、ledger 与迁移期行审计成功后由
runner 自行提交 `INACTIVE`。随后重新完成 inventory、checksum、behavioral/recovery 和权限验收。

## 12. Stage 3 PIT/因子与买入复验增量运行手册

本节不改变 V2 `001`～`015` 的所有权。V4 `005`～`007` 只创建数据源认证、因子定义、因子快照及其 lineage/guard；不得创建账户、订单、成交、现金、持仓 lot 或风险账本。V2 继续是唯一机械事实源。

### 12.1 当前禁止在业务库执行

业务 MySQL 当前为 5.5.20。V4 runner 在任何 migration ledger/table DDL 前检查版本，低于 5.7 抛出 `V4 Stage3 migrations require MySQL 5.7 or newer`。因此当前业务库在 V4/FactorStore 范围内只允许有界只读源核验；禁止运行 `005`～`007`、创建 V4 表/trigger 或写 FactorStore。本轮为旧模拟盘 fail-closed 执行合同已单独在 `st_sim_order`/`st_sim_signal` 增加 10 个 gate 列，并把 `st_sim_position.sell_reason`、`st_trade_flow.reason` 保持为 `TEXT`；该一次性兼容 DDL 已在实施文档第 22.3 节留证，不代表允许继续对 5.5 应用 V4 migration。

上述模拟盘 DDL 是历史留证，现行运行边界必须按以下合同执行：

- import、`SimTradeEngine` 构造和所有 GET 路径中的 `_ensure_tables()` 仅调用 `_require_sim_execution_schema()` 做只读 metadata 验证，不得执行 `CREATE`/`ALTER`；
- 任一必需表/列缺失或存储引擎不是 InnoDB 时立即 fail-close 报错，不得 fallback 到旧 schema、自动补表或继续撮合；
- 唯一模拟盘 schema 运维迁移入口是 `python tools/migrate_sim_trade.py --allow-schema-change`，不得用 V2 runner、应用启动、构造器或 GET 请求代替；CLI 内部还必须显式传入 `allow_schema_change=True`；
- DDL、列/索引 metadata 检查或迁移后 schema 复验任一失败都必须向上抛出并使运维命令失败，禁止吞异常、跳过失败项或在不完整 schema 上恢复运行。

隔离 acceptance 与未来生产部署是两个不同门：

- 普通 runner 的最低结构版本为 MySQL 5.7；
- 隔离 acceptance 必须是 Oracle MySQL 5.7.38、专用空 schema、预期 database/server UUID、`V4_TEST_*` 或 `V4_CI_*` URL；
- acceptance 拒绝 `MYSQL_URL`、`DATABASE_URL` 和任何非专用库名；
- 隔离通过不授权在业务库迁移，更不授权 paper/production 激活。

### 12.2 冻结增量清单

| Migration | Statements | 结构增量 | SHA-256 |
|---|---:|---|---|
| `20260804_005_v4_pit_factor_registry` | 3 | 3 表 | `14b5b8b2eba30739c897b7c4bb9ba33ab44e132604bcda589f4636c63b5c74db` |
| `20260804_006_v4_pit_factor_guards` | 9 | 9 triggers | `2df39d8dc3cda258a582bb45e1a66b770f402affe14b08d4cdf27b6b232818a0` |
| `20260804_007_v4_factor_lineage` | 5 | 3 ALTER + 2 triggers | `c39a1fccb2228d55eded94e08c846229764f9766f0786a97279a3112bbf4e75f` |

Stage 3 增量共 17 statements、3 张新表、11 个 trigger、3 个 ALTER。完整 `001`～`007` migration body 为 50 statements、11 张业务/控制表和 32 个 trigger；runner 另建 `schema_migration_v4`，所以验收物理表总数为 12。不得把 ledger 表误算为 migration body 新建的业务表。

### 12.3 未来部署前置与顺序

只有在数据库升级/迁移审批完成后，才能执行以下未来流程：

1. 冻结同一代码 artifact 和 7 条 version/checksum/statement-count manifest，外部保存哈希。
2. 备份目标 schema、migration ledger、表/trigger/权限清单和 row counts；证明目标不是 5.5。
3. 停止 V4 writer；核对没有活动 FactorStore 事务。
4. 以专用 migration 身份先执行 dry-run；人工核对仅包含 `005`～`007` forward-only 增量，不允许 DROP、TRUNCATE 或修改 `001`～`006` 已应用 checksum。
5. 应用 migration 后核对 7 条 ledger、50 statements、12 张物理表、32 个 trigger、三列 lineage 合同及所有 column/index/trigger body。
6. 再执行一次，7 条结果必须全为 `exists`；同版本异 checksum 或 statement count 立即停止。
7. 用最小权限 FactorStore 身份做 source/factor/snapshot 正向写、精确重放、异内容冲突、缺父记录、过期和并发验收。运行身份只授予声明表的必要 `SELECT/INSERT`，不得授予 UPDATE/DELETE/DROP/ALTER。
8. MySQL 1205/1213 只能通过 `run_factor_store_transaction` 在全新事务中重放完整 caller unit；禁止在失败事务中继续 SQL，也禁止 repository 自行 commit/rollback。
9. 验证六个 source adapter 的实际 certification。默认 4 个 `FORWARD_ONLY`、2 个 `DISPLAY_ONLY`、0 个 `PIT_CERTIFIED/BACKTEST_READY`；在外部证据完成前不得手工提升状态。
10. 最后运行固定 62+14+20 CI 矩阵和真实并发 acceptance；任何失败均停止，不得开启下游 worker。

### 12.4 订单切换与兼容行为

新 schema-v2 买入 receipt 绑定 `run_uid/strategy/stock`、决策交易日、推荐日期、推荐状态、信号状态、追高状态、普通成交资格、事件风险、来源健康、上下文哈希和有效期。V2 在撮合前与 fill 前同事务复验；旧 schema-v1 receipt、HOLD 信号、旧推荐、次日新推荐错配、重复/不完整 JSON receipt 均失败关闭。部署新代码时仍在途的旧 BUY 可能被取消，这是有意的安全切换；已完成的合法部分成交不回滚，只取消剩余量。

V3 paper materializer 不得再直接入队无证据 BUY/ADD。只有能取得同一 V2 executor 可再次验证的 canonical receipt 才能写入既有 V2 intent/order；否则返回 `RESEARCH_ONLY`/`BUY_GATE_*` 且不创建订单。该限制不适用于 SELL/REDUCE，持仓退出仍走 V2 canonical 路径。

模拟盘只用于非生产行为验证。订单以数据库 CAS 从 `PENDING/PARTIAL` 认领到 `MATCHING`，并在同一事务写 position/flow/order/signal/event；不同 BUY 复用现有风险预算行做数据库串行化并在锁内重算现金。live/forward 的部分减仓必须有稳定 order id，SELL 锁持仓并校验 stock/mode/strategy/T+1，风险退出订单跨日保持 GTC。它不是第二套 canonical 账本，也不能成为真实下单授权。

### 12.5 仍保持关闭

FactorStore 当前没有生产调用点；六源没有 PIT-certified/backtest-ready 数据链；模型注册只有 Protocol，没有 active model/calibration 实现；正向新闻收益模型和正式 OOS 均未完成。因此最终状态必须继续为：

```text
OOS=BLOCK
production_activation_allowed=false
actionable_output_allowed=false
prepared_commit_runtime_enabled=false
v3_projection_worker=disabled
paper_buy_outbox=closed
```

### 12.6 本次验收基线

- 全仓：`2696 passed, 1 skipped, 966 warnings, 25 subtests passed`，0 failed。
- 正式固定门禁：`python tools/run_trading_v4_ci_gate.py --suite all`，62+14+20 共 96 个文件，`2050 passed, 1 skipped, 926 warnings, 23 subtests passed`，0 failed。
- manifest SHA-256：base `e2fcabf530efab10dd7724a6d742cd2976735c0e6783650219dd5fa1de85024f`；core `30322345896fccf67c0473cf8d741ba0e41fd3ac79259ea435a88aa18ec73e93`；research `52c58824467f6acb04be75bd43792ed6998d2d880da812910895a732271c597c`。
- 运行时 schema 只读检查、显式迁移授权、迁移失败传播、模拟盘事务原子性、V2 成交前四门复验、V3 无 canonical receipt 禁止写单、SELL/REDUCE 退出豁免均在固定门禁内。

该基线用于代码交付复核，不得被解释为第 12.5 节关闭项已经解除。
