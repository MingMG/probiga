# QMT 采集发布失败恢复：v2 最小协议与上线阻断

日期：2026-09-05。状态：**新增“未切换、schema 未变”的恢复候选，首次兼容安装仍 BLOCKED；候选未完成，不可合并到生产 main 或部署，不能宣称每日采集已稳定。**

本方案只处理“应用发布失败使 Windows 采集永久停止”的生命周期问题。交易日历、数据源质量、补数与策略依赖由其他专项处理。这里不新增消息队列、调度平台或交易权限，不允许降低数据库写入围栏、身份验证、SHA 来源证据及 QMT 登录要求。

## 0. 本轮收敛：已实现候选与真实缺口

为避免扩成发布平台，本轮只实现 **PRE_CUTOVER_UNCHANGED_SCHEMA**：Windows 尚保留旧 checkout、Linux 尚未 cutover、迁移未开始、旧 API 和精确旧 seal 均重新验证成功时，才能终结本次 hold 并恢复原来运行的旧采集。schema 中途失败、原本已停采、旧 checkout 已被替换、旧真实 grant 缺失、跨 host 或证据不全一律 `RECOVERY_BLOCKED`。本轮不新增任意 schema 回滚能力。

已实现的闭环候选：

- 新 `server/common/qmt_edge_release_recovery.py` 在现有 `qmt_edge_release_request / release_activation` 受保护通道追加严格 context 和 ABORT；不改表、trigger、权限或假冒签名。
- context 冻结实际旧 Windows SHA、host/PID/instance、原运行状态、原 seal 摘要及失败 hold 的 UID/hash。新旧所有 hold/grant/abort 使用同一 MySQL 命名锁、同一物理连接事务；读取全局最新 hold，不按单个 SHA 隔离排序。
- ABORT 与已有合法 v1 grant 共用唯一 `qmt-edge-grant-{attempt}` 终态键，但使用不同严格 schema；旧 reader 会拒绝 ABORT，绝不把它当 grant。新 reader 恢复旧版时仍验证旧版原有真实 grant 和原 seal；更新的全局 hold 撤销旧 ABORT。
- `tools/run_qmt_windows_edge_release_bootstrap.py` 的受控 root/migrator 写入口支持 `--request-recoverable-quiescence`、`--abort-precutover`；只读 Windows 查询为 `--check-transition`。root 账本连接与受保护固定运行配置的数据库 UUID/名称必须相同。
- updater 在 PENDING 时停止已冻结的原实例，保留旧 checkout 并返回 4；ABORT 只选择保留的旧版，仍经过原 schema/QMT/真实 grant/启动检查；只有合法最终 grant 才允许快进，快进后仍用候选真实代码验证候选 seal。`READY_TO_SWITCH` 本身明确不授予写权限。
- Linux pre-cutover 回滚在旧 API/schema 证明通过后写入同尝试 ABORT。其他失败分支不越权恢复 Windows，而是保持围栏并返回失败。

**未完成的三个放行条件：**

1. **首次兼容 bootstrap 没有可执行入口。** 当前生产旧发行制品没有新模块；新 broker 的能力门禁会拒绝普通发布。这个门禁是安全阻断，不是安装方案。不能把新文件放入旧 SHA 目录再冒称旧制品，不能先写新 context 让旧 daemon 理解，也不能伪造旧 grant。即使 edge 已停、没有新 context、有存量合法 grant，仍必须完成一个前向、保留历史 hold、实际加载新 controller/reader 且不伪造来源证明的受控安装入口与验收；本轮没有实现，禁止用手工改 SHA/重启替代。
2. **真实 MySQL 与双机验证未完成。** SQLite 和连接替身证明协议状态/调用顺序，不证明生产 trigger 权限、GET_LOCK 实际竞争、断线释放或实际 Windows/QMT 重启恢复。还需 runtime 用户不能写任一新行、两终态并发只成功一个、断线重试、无双写和实际补齐数据验证。
3. **全生命周期未覆盖。** 进入 cutover/schema 变更后不自动恢复旧 Windows；这不是任意发布失败都可自动恢复的最终方案。采集/页面完全独立版本也未实现。

因此这组 release 修改必须与已完成的核心采集修复分开保存为候选；可上传评审，不能作为生产 ready 合并。下文第 2—8 节保留完整 v2 后续设计，不能把那些目标视为本轮完成项。

候选验证记录：生产发布边界完整文件为 **126 passed、7 skipped（57.89 秒）**；恢复、故障分支、grant authority、bootstrap 四个专项文件合计 **79 passed（10.39 秒）**，包含真实 SQLite 终态/全局顺序测试、实际 PowerShell/Bash 分支故障注入、同连接命名锁调用顺序替身和 root/runtime 数据库身份拒绝测试。跳过项、SQLite 与锁替身均不替代第 0 节的真实环境放行条件。两个原 hold 测试随新的受控 broker 调用调整定位，仍校验旧真实 SHA、候选 SHA、精确 attempt、拒绝 activation 授权及先停写后停 API 的顺序，不是删除安全断言。

### 成功发布的顺序核对：纠正“所有正常发布必然互等”的推断

实际顺序是 Linux 发布并验证运行身份 → `controlled_guard_finalize_successful_activation` → 最终精确 attempt grant → Windows 切换/验证/启动 → 新 edge bootstrap receipt。正常末尾 finalize 只执行 HTTP/runtime、unit/snapshot/journal 验证，并不调用完整 governance health；`QMT_EDGE_DEPLOY_BLOCKING=0` 也跳过前面的直接新 edge receipt 等待。不能仅因 full checker 的 required inventory 包含新 edge receipt，就声称正常主路径必然产生 grant 互等。

确实存在额外耦合：`prepared_active_runtime_matches_current_request` 的同 SHA / preserved 恢复路径及 full restored-runtime health 会检查新 edge receipt；发布主路径还在 grant 前强绑行情补齐、分析/策略完成、策略页面新 build 结果。已有 `RELEASE_DATA_VALIDATION_BLOCKING=0` 的 code-release 模式可以移除这些业务阻塞，schema/seal/身份/围栏/final grant 不可提前或删除。它同时跳过部分全库业务账本审计，所以必须保留独立且明确失败的后置 observer，并分别报告 DEPLOYED 与 DATA_READY。该模式本身不能解决上述首次兼容 bootstrap。

## 1. 已确认的基线故障链（6c503，非候选当前行为）

| 位置 | 当前行为 | 后果 |
| --- | --- | --- |
| `deploy/production_deploy.sh`，`request_qmt_windows_edge_quiescence_before_service_stop` | 正式 cutover 标记前发布 hold | 准备阶段也可能已经停止远端采集 |
| `tools/update_qmt_windows_edge.ps1`，Phase two | 停采后直接快进唯一 production checkout，再确认 activation | 授权未完成时已失去原活动代码位置 |
| 同脚本，`Confirm-QmtReleaseActivation` | PENDING 停采并 `exit 0` | 重试仍停采，任务结果掩盖长时间未完成状态 |
| `deploy/production_deploy.sh`，`rollback()` 的 pre-cutover 分支 | 恢复 Linux 调度器并验证旧 API | Windows hold 未终结，也没有授权旧采集恢复 |
| `server/common/qmt_edge_release_receipt.py` | request、hold、grant；按 build 选择最新 hold | 没有跨 build 的全局发布决议、ABORT/RESUME 终态 |
| `tools/run_scheduler_daemon.py`、`server/api/scheduler_runtime.py` | 仅接受当前 build 的激活与 seal | 不能靠手动启动、假 grant 或忽略校验补救 |

现有安全检查本身有必要：新旧程序同时写入、schema 不兼容、陈旧发布授权重放都会损坏数据。问题是只做了“失败时不写”，没有实现“失败后证明安全并恢复写入”。

### 可执行故障/恢复分支测试

```powershell
& 'E:\My Code\ProBigA-qmt-production\.venv\Scripts\python.exe' -m pytest -q tests/test_qmt_edge_release_failure_characterization.py
```

该测试运行实际 PowerShell 激活/切换分支和实际 Bash 准备阶段回滚函数，外部 Git、systemd、调度器、激活查询全部使用临时沙盒替身。不会连接生产或启动 QMT。

当前文件已更新为执行候选保留旧 checkout、PENDING 非 0、终态选择和受控 pre-cutover ABORT 调用测试，并保留未知授权/未支持边界的拒绝行为。**测试通过只证明这些隔离分支，不是跨端实际恢复验收，不得作为发布放行依据。** 需要 PowerShell 与 Bash；跳过也不构成跨平台验证。

## 2. 方案取舍

| 方案 | 优点 | 缺点 / 风险 | 是否采用 |
| --- | --- | --- | --- |
| 只加预检查、超时重试 | 改动小，减少可提前发现的发布失败 | 无法修复停采之后的故障；仍可能永久 PENDING | 作为辅助，不算修复 |
| 超时直接启动旧版、删除 hold、放松 SHA | 看似恢复快 | 可能在 schema 已变更或新 writer 尚存时双写；失去授权与来源证据 | 禁止 |
| 有终态的发布决议 + 授权恢复 | 复用现有 broker、账本、围栏，正面处理停采后的恢复 | 必须有跨端兼容安装、旧制品保留、故障测试；不兼容 schema 必须阻断 | **必要；本轮先实现第 0 节窄边界候选，完整 v2 不强行扩入本轮** |
| 采集与页面使用独立发布版本 | 页面/分析失败不打断采集，进一步减少故障面 | 需兼容合同、混合版本验收 | v2 稳定后推进，不借此绕过当前检查 |

“所有检查都提前做”不可能覆盖发布过程中断电、网络断开或迁移失败。因此完整失败恢复有必要，不是过度设计。

## 3. 尽量不改特权数据库合同

### 3.1 可以复用现有受保护通道，但不能复用 v1 grant 语义

`server/common/scheduler_task_history_schema.py` 的 INSERT trigger 已对以下整行限制为固定 migrator 身份写入，并未依赖 JSON schema：

```text
task_type = qmt_edge_release_request
trigger_source = release_activation
```

因此 v2 控制消息可使用**同一受保护 task_type / trigger_source**、新的严格 envelope schema 与不同 run_uid。这样不必增加 runtime 权限、修改 trigger DDL 或改变触发器源码合同哈希。账本原有 append-only 与唯一 run_uid 约束继续适用。

例如新增 `probiga.qmt-edge-release-decision.v2`，消息种类明确区分 `ATTEMPT`、`COMMIT`、`ABORT_RESUME`。这是设计名称，不是当前可执行接口。

- ATTEMPT：`qmt-edge-attempt-{attempt_id}`。
- 唯一终态：`qmt-edge-decision-{attempt_id}`，COMMIT 与 ABORT_RESUME 共享这个唯一键，不允许一个尝试产生两个终态。
- v1 的 `qmt-edge-grant-{attempt_id}` 不能改名冒充恢复。新 reader 必须验证 v2 决议，且明确拒绝在 v2 attempt 上只靠 v1 grant 启动。
- JSON 摘要仅用于完整性校验，不是签名；真正的授权来自经过验证的固定数据库端点、privileged trigger seal、受保护账本行、固定 migrator 身份及可信 root broker。

**反例：**直接新增 `trigger_source=release_abort` / `release_resume` 不在当前 INSERT trigger 的保护范围内，runtime 可伪造。禁止作为“简单修复”实施。

### 3.2 首次启用仍有反伪造与混合版本要求

需要在可信 broker 下确认当前保护 trigger 的真实结构和 seal。若其安装/验证历史不可信，不能因为行内 hash 正确就接受存量 v2 行。协议首次启用前检查不存在无法归因的 v2 控制行，记录 root 管理的启用基线。首次反伪造检查应复用 `tools/prepare_strategy_governance_schema.py` 的保护原则，但首选路径不改 trigger DDL。

旧 daemon 严格接受 READY/PENDING；旧 updater 固定 main 且只能快进；旧 broker 不理解 v2 终态互斥。因此不能先写 v2 决议再期待旧程序恢复。

## 4. 协议内容与唯一授权顺序

### ATTEMPT 必须冻结的事实

- 随机且非零的 attempt ID、受保护账本序号、前一全局 attempt 的 ID/hash。
- 候选 app/edge SHA；原活动 edge SHA、不可变制品摘要、依赖锁摘要、QMT bridge 身份摘要。
- 原活动 schema/trigger seal 与数据库身份；原运行/启用状态，不把原本停用的采集擅自改为启用。
- 可信 release journal 的身份与摘要，明确归属同一生产环境。
- 协议版本及最小 reader/controller 能力；实际 SHA 不允许写成另一版本。

### COMMIT

只有现有特权迁移/验证与最终围栏移交成功后，root broker 才在同一逻辑事务写入该 attempt 的 COMMIT 决议。候选 writer 仍需本机制品、QMT 模型、schema seal、调度器身份和业务派发检查，不以单一 JSON 代替它们。

### ABORT_RESUME

只有以下条件同时得到证明，root broker 才能签发：

1. 这是当前全局 attempt，尚无 COMMIT，且没有后来的发布已获得写入所有权。
2. 新 writer 已停止并完成既有 quiescence/围栏证明。心跳超时不是单独足够的死亡证明，需沿用现有进程/所有权核查。
3. 原活动制品仍完整可用，依赖与 QMT 身份可恢复；不是根据 `origin/main` 猜测旧版。
4. 数据库实际结构与原 writer 的精确合同兼容；原 seal 经可信 broker 校验/恢复。不可仅复制旧 seal 文件或改 metadata 来假称兼容。
5. 若原运行状态为 enabled/running，则恢复它；否则保持原停用状态并如实回执。
6. ABORT_RESUME 绑定该原 edge SHA、原制品摘要、当前实际 schema seal、attempt 和写入围栏代际。

**不兼容的前向 schema 变更不能盲目回退代码。** 若不能验证原 writer 兼容，则保持围栏，标为 `RECOVERY_BLOCKED_SCHEMA`，由受控前向恢复处理，并显式通知。超时只触发恢复和告警，绝不产生权限。

### 状态序列

```text
原采集运行 → 准备候选 → ATTEMPT / hold → 原采集停写
                                      ├─ COMMIT → 候选采集验证、启动、补数
                                      └─ ABORT_RESUME → 原采集验证、恢复、补数
                                         无法证明安全 → 明确阻断并告警
```

## 5. 全局 attempt 顺序、锁与竞态

当前 `_latest_qmt_edge_release_quiescence_rows` 按 build 筛选，不足以阻止另一个 build 的旧恢复授权。不得将它直接复用于全局恢复决议。

推荐复用现有 root 部署 flock，常规部署、失败恢复、人工受控恢复均通过同一 broker。Windows 只读控制账本，不写授权。bootstrap CLI 本身当前没有该锁，不能把“外层一般会加锁”当作不变量。

在此基础上，v2 所有 ATTEMPT/COMMIT/ABORT_RESUME 写入均使用同一个固定的 MySQL 命名锁（同一物理连接持有；固定数据库/环境名，不能由请求方任意传入），锁内开启短事务：

1. 查询最新的**全局受保护 ATTEMPT**，比较 expected previous ID/hash 或 expected current attempt。
2. 校验本次 attempt 全部不可变字段，检查唯一 terminal run_uid。
3. 首次写入终态；重复请求只允许字节规范化后内容完全相同的幂等读取；不同终态或不同恢复版本一律拒绝。
4. 提交后立即回读，然后在 finally 释放命名锁。连接断开后锁释放，下一次根据已提交账本收敛。

账本自增 ID 可作为已序列化提交的顺序证据；时间戳不能替代顺序。在所有 writers 采用上述锁之前，不能假定 ID 等于安全的全局发布顺序。单用 SELECT MAX(id) 再 INSERT 存在竞态；只对空结果做 FOR UPDATE 也不够。

新增 attempt 的第一步必须撤销上一 writer 的全局派发资格并完成 quiescence 才能迁移。恢复进程在启动前和每次派发前都核对最新受保护决议/所有权，迟到的 A 恢复不得与 B 发布并行写入。现有进程租约及围栏继续保留。

v2 启用后，可信 broker 必须拒绝旧控制工具继续写 v1-only 决议；兼容路径只读既有 v1 历史。这不是对恶意 root 的防护承诺，而是限制受支持运维入口，防止正常操作发生协议混写。

## 6. Windows 制品与控制器：不能先覆写唯一活动 checkout

保持受控 main 来源规则，但区分代码来源 checkout、候选不可变制品和活动运行位置。用独立、受保护的控制器/启动器解析活动指针，拒绝 reparse point、越界路径、非注册生产目录、脏树和非预期摘要。

- 正常准备：拉取候选、校验制品与可提前检查的环境；旧采集继续运行。
- hold：只停止当前明确实例，保留旧制品及活动指针。
- COMMIT：原子切换到候选制品，验证后启动。
- ABORT_RESUME：指向原制品，验证后恢复。
- 重启：先读取持久状态和全局受保护决议，不能仅根据 Git HEAD/Task Scheduler 上次退出码判断。
- controller 更新自身也要保留可信旧副本；不能依赖已经被替换的脚本理解旧恢复标记。

阶段一不意味着“停采零秒”，而是将停写变成可证明、安全、有终态的有界操作。页面发布完全不触碰采集，需后续独立 ingest release 合同实现。

## 7. 完整 v2 后续目标的文件边界与兼容上线次序

本表是完整 v2 的后续工作清单，不是第 0 节窄边界候选的实现状态。候选通过 `qmt_edge_release_receipt.py` 的原 activation 查询接入现有 daemon/runtime 每轮围栏，不另改 daemon/runtime 入口。

| 文件 / 模块 | 必须做的变化 | 当前是否完成 |
| --- | --- | --- |
| `server/common/qmt_edge_release_receipt.py` | v2 严格消息校验、全局 attempt、唯一终态、恢复决议读取 | 否 |
| `tools/run_qmt_windows_edge_release_bootstrap.py` | 同一 root/migrator 通道的 v2 写入、锁、逐项回读与只读决议查询 | 否 |
| `deploy/production_deploy.sh` | 可信 attempt/journal、所有失败分支终结、恢复 broker 入口、最终核验 | 否 |
| `tools/update_qmt_windows_edge.ps1` | 候选与活动位置分离、消费 COMMIT/ABORT、恢复原运行状态 | 否 |
| `tools/run_scheduler_daemon.py` | v1/v2 reader、拒绝混合授权、本机及全局所有权验证 | 否 |
| `server/api/scheduler_runtime.py` | 每轮派发核对全局 writer 授权，与现有租约/围栏兼容 | 否 |
| `tools/run_local_scheduler_task.ps1`、`tools/register_qmt_windows_edge_scheduler_task.ps1` | 活动制品启动入口与受保护控制器路径；路径/账户/参数回读 | 否 |
| `server/common/scheduler_task_history_schema.py` | 首选方案无需改 DDL / 权限；需要实库保护验证 | 未执行实库验证 |
| `tools/prepare_strategy_governance_schema.py`、`server/engine/strategy_governance.py` | 首选不改触发器合同；复用真实 schema/seal 证明，扩展受控恢复核验而非跳过 | 未实现 |
| `tests/test_qmt_edge_release_failure_characterization.py` | 当前故障复现 | 已添加，不是验收 |

### A. 离线完成完整实现与故障测试

把所有 writer 行为置于默认关闭的受控 v2 协议开关后。该开关属于 broker 管理的已验证部署状态，不是随意可传入的环境变量。完整状态机与下文验收先在隔离 Linux/Windows、MySQL 实例验证。

### B. 兼容 reader/controller 先行

可以安全编写 v1 兼容 reader：保留 v1 原语义；新 schema 不改变原 JSON 的严格字段集合；未知/不完整消息继续拒绝；无 v2 消息时输出与现有实现等价。不改 trigger DDL 是更小的路线。

但**部署兼容 reader 仍会经过当前有缺陷的发布流程**。其第一次安装必须作为明确维护窗口，保留受控恢复入口和完整旧制品，预先完成环境/数据检查；不能声称这次发布已经由尚未安装的 v2 自动兜底。若无法证明第一次受控安装可恢复，则停止上线。

确认 Linux broker、Windows controller/daemon、派发 reader 全部安装并具备一致能力。只有 reader 兼容，不等于恢复已经可用。

### C. 启用受保护 v2 writer

可信 broker 获取统一锁，验证真实 trigger/seal、无无法归因的存量 v2 行、受支持入口全部受控；记录协议启用基线。先做无业务写入的往返回执验证，再对隔离 canary 执行 COMMIT 和 ABORT_RESUME。所有验证成功才允许生产 v2 attempt。

### D. 生产故障演练及观察

在维护窗口模拟准备失败和受控中断，验证实际恢复原采集、没有双写、自动补齐停止期间缺口；观察至少一个完整收盘采集与跨日恢复周期。禁止先频繁发布再补检查。

存量失败 hold 不能凭人工推测补写“当时已经 ABORTED”的历史。通过当前可信 broker，以现在的真实证据创建受控恢复决议并保留原失败记录。

## 8. 恢复验收矩阵：全部必要，不得以局部测试替代

| 故障点 | 预期恢复 | 必须验证 |
| --- | --- | --- |
| ATTEMPT 之前 | 旧采集不受影响 | 原 PID/制品、心跳和数据写入持续 |
| ATTEMPT 已提交，输出丢失 | 重试读取同一尝试 | 不重复创建 attempt、不丢失原身份 |
| Windows 停止后 controller 被杀 | 重启按持久决议恢复 | 无失控子进程、活动指针完整 |
| Linux 准备失败 | ABORT_RESUME 恢复原采集 | 同一 attempt、原 SHA、当前兼容 seal |
| Linux schema 途中失败 | 兼容则恢复；不兼容明确阻断 | 不能伪造旧 seal，不能旧版写新不兼容表 |
| COMMIT 提交，输出丢失 | 幂等完成新版本 | 不允许随后 ABORT 同一 attempt |
| ABORT 提交，Windows 暂离线 | 上线后按授权恢复 | 不需再发布新应用版本才能继续采集 |
| A 恢复迟到，B 已开始 | 拒绝 A 的陈旧写权限 | 跨 build 全局顺序，不能仅按 old SHA 读 grant |
| 两个 root 受控恢复并发 | 只有一个终态生效 | flock + 数据库锁 + 唯一 UID；无 TOCTOU |
| DB/网络不可用 | 保持围栏并及时告警 | 不能把 timeout 当恢复授权；恢复后收敛 |
| QMT 登录/CAPTCHA 失效 | 明确需要用户处理 | 不绕过；不虚报恢复成功 |
| 恢复成功后补数中断 | 再次运行续补 | 不重复入库、不覆盖完整证据、不漏交易日 |
| 权限攻击与回执篡改 | 一律拒绝写权限 | 用实际 runtime 用户验证无法写任一 v2 受保护行 |

最终验收标准是“失败后采集实际恢复且缺口被补齐”，不是 API 健康、任务启用、脚本退出 0、JSON schema 通过或单元测试数量。未完成此矩阵前，发布生命周期仍应列为明确上线阻断。

## 9. 联合回归的三个发布边界失败：不要误判，也不要机械改绿

前一轮只读审查没有改 `deploy/production_deploy.sh`，三个失败已在未修改的 `6c503a68ca1546b6705e17dd7b4f61986533b30c` 基线独立复现。以下记录那一轮测试维护，不应与第 0 节本轮新增的候选实现混淆；维护时保留原有全部身份、围栏与顺序断言，没有修改生产路径。

| 失败测试 | 源码实际情况 | 结论与正确测试方向 |
| --- | --- | --- |
| `test_deploy_starts_hardened_observer_only_after_success_without_waiting` | 测试硬找 `if ! start_release_data_readiness_observer`，实际新增 `RELEASE_DATA_VALIDATION_BLOCKING` 条件，且加入 preserved-no-receipt 恢复调用点。常量当前为 1；最终主路径依然在 DEPLOY_SUCCEEDED、journal 清理、trap teardown 后调用 observer | 静态匹配失效，不是 observer 被删除。应按主流程与三条完成路径分别验证条件和顺序，执行模拟 systemd-run 并验证硬化参数，而非只改计数。源码存在调用不等于线上 observer 已运行 |
| `test_exact_request_rerun_is_a_verified_read_only_noop` | 原内联检查已移入 `prepared_active_runtime_matches_current_request`；`prepared_request_is_already_active` 仍先验证 finalized receipt，再调用该 helper，失败立即返回 | 函数重构导致旧字符串断言失效。应验证调用链及每项 identity 漂移均拒绝。整个同 SHA 发布路径还会补 grant/写审计回执，不能把“只读判断函数”误说成整个发布完全不写 |
| `test_qmt_activation_grant_is_attempt_bound_and_last_before_success` | 测试用首次 `.index(finalize_activation_journal)` 命中 `finalize_preserved_no_receipt_request` helper，而非文件末尾正常发布主流程。helper 是已验证 runtime 的恢复流程，使用 grant-latest；正常发布仍向 grant 与返回校验同时传递本次 attempt ID | 选择了错误的代码窗口，不能据此声称正常发布丢失 attempt 绑定。应分别测试正常路径的显式 attempt 与恢复路径的最新已验证 hold |

`append_latest_release_activation_grant` 不是无绑定 grant：它读取指定 build 的最新 hold，生成带该 hold 的 attempt/hash 的 grant，insert 时再验证最新 hold/完整账本行；root/migrator 授权仍保留。但它仍然只有 v1 的按 build 范围和 COMMIT 语义，**不能被当作 v2 全局 ABORT_RESUME 机制**。本节不减轻前述实际生命周期上线阻断。

维护后的测试按实际 helper 调用链和主发布路径定位，另新增 12 个隔离执行场景：验证 receipt/runtime 检查顺序及失败短路；执行实际最终发布 shell 段，在 persist、finalize、publish、grant、grant 校验、journal 清理、observer 等环节注入故障，并验证 observer 开关。模拟 broker 同时严格核对 grant 调用及回执验证的 attempt ID，observer 只能在成功标记、journal 清理、trap teardown 后运行。三项维护测试加新增执行场景共 15 项定向通过；两个完整测试文件回归结果为 **138 passed、7 skipped（54.09 秒）**，跳过项不视作已经验证。

**这属于既有测试维护及正常发布顺序验证，不是 ABORT/授权恢复实现。** 不删除安全断言、不用 mock 测试数量冒充真实跨端恢复验收。
