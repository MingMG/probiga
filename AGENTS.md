# Codex branch and release workflow

For every new Codex conversation that changes this project:

1. Fetch the latest production `main` branch before editing.
2. Create a new, conversation-specific `codex/*` branch from that exact `main` revision.
3. Never make code changes directly on `main`, and never reuse a branch from another conversation.
4. Implement and test all changes on the conversation branch.
5. Commit and push the conversation branch, merge it into `main`, then deploy only the merged `main` revision.
6. Keep unrelated user changes out of the branch; use a separate worktree when the current tree is dirty.

Read-only investigation does not require a branch until a file change is needed.

# Deployment scope policy

For every task, scan the planned and actual change scope before implementation
and again before deployment. Classify the affected runtime boundary as Windows/QMT,
Linux/server, or cross-end.

1. Default to a single-end release limited to the affected runtime boundary.
2. Do not deploy or restart an unaffected endpoint.
3. Coordinate Windows and Linux only when an actual shared schema, QMT protocol,
   scheduler ownership contract, or cross-end configuration change requires both
   consumers to change. Reading or writing shared data alone is not sufficient.
4. Resolve uncertain scope by inspecting actual imports, consumers and changed
   contracts before deployment; do not assume every release is cross-end.
5. Before deployment, state the classification and the files or contracts that
   justify it.

# No transitional designs

1. Implement the final target architecture directly. Transitional designs are
   not allowed.
2. Do not introduce temporary compatibility layers, dual old/new execution paths,
   temporary feature flags, placeholder implementations, disposable adapters,
   migration shims, or workarounds intended to be removed later.
3. Do not accept plans framed as "ship this now and redesign/refactor it later."
4. Remove superseded paths as part of the same change once the final path is
   active, while preserving data and rollback safety.
5. If an external dependency or missing decision makes the final design impossible,
   report the specific blocker instead of implementing a transitional version;
   continue independent authorized work that can produce usable results.

# Production incident and recovery workflow

Use this workflow for production incidents, data gaps, stuck jobs, failed releases,
external-client failures, and any task whose first attempted fix did not work.

1. Before changing anything, state the user-visible objective in one sentence and
   define the machine-verifiable evidence that will prove completion.
2. Map the critical path from source to durable outcome. For data work, inspect at
   least source/client, adapter or bridge, scheduler/owner, transport, database
   write, completion receipt, and coverage/readback.
3. Probe those boundaries in source-to-sink order with read-only checks. Mark facts
   as observed or inferred; do not turn an inference into a root cause.
4. Identify the first failing boundary and fix its root cause. Include necessary
   changes across files or boundaries when causal evidence requires them; do not
   change downstream consumers to hide an unproven upstream failure.
5. Freeze the incident scope. Record unrelated defects for later work; do not put
   maintenance, refactoring, verifier improvements, or unrelated releases on the
   recovery critical path unless direct causal evidence makes them necessary.
6. Prefer an already-proven final-architecture path over repairing a broken
   secondary control path during the incident.
7. After the same failure occurs twice, or after 30 minutes without measurable
   forward progress, stop retrying. Re-establish the boundary facts, preserve the
   evidence, and choose a materially different approach before another mutation.
8. Never run duplicate deployers, collectors, backfills, migrations, or recovery
   jobs for the same scope. Prove that the prior run is terminal before starting
   another one.

# Completion and status semantics

Use precise states: `NOT_STARTED`, `RUNNING`, `BLOCKED`, `FAILED`, and `COMPLETE`.

1. A healthy process, fresh heartbeat, accepted request, populated queue, or
   successful source response proves only its own stage. None proves the requested
   business outcome.
2. Do not say that a backfill or acquisition has started until the business job
   itself has a run identity and has begun processing its target partitions.
3. Data acquisition is `COMPLETE` only when durable database writes, existing
   completion records, expected partition/date/security coverage (including
   evidence-backed legal no-data outcomes), and bounded readback agree. Reuse valid
   records and progress; neither recapture nor a new proof framework is required
   merely because a new run started. Row activity alone is not completion.
4. Deployment is `COMPLETE` only when the merged revision is active on every
   affected endpoint and the post-deploy acceptance check passes. A built artifact,
   accepted release request, or healthy old revision is not completion.
5. If only part of a request completed, report the exact completed and remaining
   partitions. Never convert partial progress into a global success statement.

# Bounded operations and progress

1. Every external call and every operational stage must have a timeout, a total
   deadline, bounded retries, and a terminal error that names the failed stage.
2. Long-running work must expose total units, completed units, current unit, last
   progress time, and a durable checkpoint. Retries must resume from the checkpoint
   rather than restart completed work.
3. If a bulk API exists, do not serialize it into one request per item without a
   measured safety reason. Use bounded batches with response-size and latency caps.
4. A watchdog must distinguish slow progress from no progress. CPU use, process
   existence, or an open socket alone is not progress.
5. Process and TCP liveness are insufficient for remote dependencies. Use a bounded
   application-level probe, such as an authenticated `SELECT 1`, and verify the
   complete end-to-end path.
6. Scheduler `running` state must be backed by a renewable owner lease. Reclaim a
   run only after the lease expires and the owning process or instance is proven
   absent; never leave phantom runs requiring manual database cleanup.

# External UI and runtime control

1. Identify the authoritative runtime control plane before automating an external
   application. An editor, configuration row, or source file is not necessarily the
   running instance.
2. Bind mutations to semantic identity: process, window, strategy/job name, row,
   instance ID, and expected current state. Hard-coded screen coordinates or DPI
   offsets must not be the sole selector for a production action.
3. Every start, stop, reload, or switch must have an independent postcondition from
   the runtime itself. For QMT this includes the target running-instance list,
   heartbeat state, model instance ID, build/source identity, and a bounded no-order
   capability request.
4. Once production evidence proves that a control path does not affect the runtime,
   record that fact as an invariant and add a regression test. Do not reintroduce
   the rejected path without new live evidence that invalidates the invariant.
5. Mocked or source-text tests are not sufficient for external UI control. Before
   declaring the fix complete, run one controlled live acceptance test with exact
   before/after evidence and rollback safety.

# Release and architecture decoupling

1. Version and compare deployable artifacts by their own content and contract
   identity. An unrelated repository commit must not force a QMT model reload,
   service restart, cache rebuild, or data recapture when that artifact is unchanged.
2. Expensive reference-data refreshes, catalog rebuilds, cache warming, and historical
   scans must be resumable business jobs. Do not make them a global release-readiness
   prerequisite unless every released consumer strictly requires fresh data.
3. Gate each workload only on the datasets and contracts it actually consumes. A
   failed sector catalog refresh, for example, must not block stock daily/minute
   ingestion that is valid against an existing verified catalog snapshot.
4. Reuse immutable verified snapshots by content hash and validity period. Do not
   reproduce identical reference data merely to bind it to a new repository SHA.
5. A runtime-only implementation change remains single-end unless a shared protocol,
   schema, ownership contract, or cross-end configuration actually changes. Path
   heuristics alone are not sufficient evidence for a coordinated release.

# Testing and incident communication

1. Validate the changed behavior with relevant deterministic checks, changed-boundary
   checks and live business acceptance. Reuse valid results for unaffected code;
   do not expand or repeat tests for ceremony. Tests do not replace usable data.
2. A regression test must reproduce the observed production failure, not merely
   assert the new implementation text or mocked call sequence.
3. During an incident, every update must state the current exact state, the next
   acceptance gate, measurable progress since the previous update, and the timeout
   or stop condition. Do not repeat "still running" without new evidence.
4. Correct an inaccurate status statement immediately. In particular, keep
   "client healthy", "request sent", "data written", and "dataset complete" as
   separate claims.

# 业务结果与自主执行

1. 对已授权任务持续执行，直到取得可使用的业务结果，或遇到无法自行消除的具体外部阻塞。调查、方案、规则、代码、测试、通知和异常清单均不能替代结果；更新规则和文档后必须继续实际工作。
2. 有工具和权限时，主动读取代码、配置、历史日志、响应和运行状态。暂时不能复现时继续查证，不凭空猜改，不把可以自行调查的问题交还用户。
3. 只有缺少必要业务决定、权限或无法取得的外部条件时才提出明确问题；其他工作继续。尊重生产授权，不绕过权限限制。
4. 已确认的常见问题必须固化为程序行为，不能依赖用户每天提醒。汇报使用普通语言，说明已取得的数据、未完成部分和当前措施，不用测试或评审数量代替业务进展，不承诺尚未发生的验证。

# 日常采集与合法无数据

1. 固定接口、参数和业务流程复用现有实现。缺失时自动区分停牌、未上市、日期错误、请求失败和源端未更新，不能仅凭无数据推断停牌。
2. 有源端状态、上市日期等依据的合法无行情属于正常处理结果，不使整批失败。不能用零值、旧数据或伪造结果填补缺失。
3. 正常交易而缺失的数据自动有界重试、补抓，或使用项目已有且口径兼容的来源。少数异常单独处理，其他有效数据继续保存使用；受影响股票暂不参与依赖缺失字段的计算。
4. 大面积缺失或整批日期错误先查共同原因，不能包装成正常完成。完整源清单与 A 股子集分别计数，成员按实际业务身份去重。
5. 合法空新闻窗口不要求必须有新闻；公告、分钟资金流和其他分片复用有效断点与已写数据，不重复全窗采集或无必要历史扫描。断点是否有效取决于数据要求，不能仅因早于本轮启动时间而拒绝。
6. 原有调度应无人提醒自动启动，缺失检查、补抓与恢复为自动步骤；第一批完成后继续剩余工作。普通进程重启后恢复原调度，有界结束，不无限重试或永久 RUNNING，不重复启动同范围有效任务。
7. 仅实际消费某项数据的功能等待它；无关任务继续。产品或已有查询入口直接使用结果，不默认增加消息通知或新渠道。

# 必要修改与直接部署

1. 优先沿用现有语言、框架、接口、调度和部署入口，围绕根因和结果进行必要跨文件修改，不做无关重构。
2. 不擅自新增门禁、审批、认证层、发布框架或无关流程。只有新机制确能解决已确认问题时才引入，并控制到必要规模。
3. 检查若错误阻止合法结果、断点恢复或正常部署，应修正条件；不能叠加补丁、删除正确的数据判断、吞异常或伪造成功。
4. 按实际依赖更新必要组件，不默认全量或跨端发布。保留现场任务启用状态、调度时间、执行参数和通知配置，除非本次需求明确修改它们。
5. 无关任务禁用或参数与默认值不同，不应阻止发布；部署不能用默认值覆盖现场设置。安全身份和实际依赖检查仍保留。
6. 新代码必须部署并实际生效，不能止于提交或合并。验证以问题消除、版本生效和数据可用为准，已有有效进度无需为生成证明而重复采集或全量读回，除非有具体正确性疑点。
7. 如实区分手动运行成功与无人干预的自然调度成功；后者只有实际发生后才能确认。
