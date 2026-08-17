# ProBigA 交易 V3 第四层生产激活 Runbook

> 2026-08-17 修订：加入 Horizon V2 数学协议、V3 artifact/完整候选账本注册治理、两条独立前向迁移、standalone 单一调度权威与 Layer 4 持久 writer fence。旧迁移及其 checksum 不得改写。

## 1. 当前只读结论

- 目标数据库为 Oracle MySQL `8.4.11`，schema 为 `probiga`。
- 三条前向迁移尚未写入生产迁移账本，当前不是半迁移状态：`20260804_000_shadow_intelligence_runtime`（46 statements，checksum `b09f22e6…91735`）、`20260817_000_horizon_protocol_v2_governance`（10 statements，checksum `9430f7bf…e118`）、`20260817_001_horizon_candidate_ledger_registration`（9 statements，checksum `88cbd1d4…c522`）。其余 21 条 V3 migration 已存在。
- 三条迁移已经在独立 Oracle MySQL 8.4.11 上通过串行、重放、三路并发和逐 DDL 断点恢复；这不等于已写入生产。
- 当前瞬时查询未发现 `last_run_status=running` 的 V3 task、未完成的 V3 task history、已占用的 V3 migration/Shadow/calibration named lock 或 `SHOW OPEN TABLES ... In_use > 0`。但本机 `standalone` scheduler daemon 进程树仍在运行，共享数据库还显示一个来自 `iZwz9byapurj1a3h4z617mZ` 的新鲜 `embedded` scheduler 心跳。此外，生产账号无 `PROCESS` 与 `performance_schema.metadata_locks` 读权限。因此维护窗口的 writer/metadata-lock 门禁明确未完成，当前不得 apply。
- 当前 `trading_v3_counterfactual_audit` 任务连续失败，直接原因是生产 scheduler 向空参数脚本追加了日期位置参数，脚本报 `unrecognized arguments: 2026-08-16`。工作树的参数策略已修复，但必须部署并更新任务参数后，以一次新的真实 `SUCCESS` 为准。
- 生产调度架构已经冻结为远端 `probiga-scheduler.service` standalone 单一权威：API 必须持久配置 `API_EMBEDDED_SCHEDULER_ENABLED=false` 且 embedded 线程不运行，standalone service 必须同时为 `enabled` 和 `active`。长训练不得绑定 FastAPI lifespan。本机 supervisor/scheduler 只能作为维护窗口外的非生产辅助，不能与远端生产权威同时连接共享生产库。
- 生产 verifier 的 scheduler 门禁现同时要求 `trading_v3_counterfactual_audit` 与 `trading_v3_continuous_calibration` 唯一存在，并逐项精确匹配部署定义中的 script、完整 argv、cron、`date_param`、interval 和 enabled；只出现参数名子串不再算通过。它会读取全部 scheduler heartbeat，严格以 `heartbeat_age_seconds <= 2 * poll_seconds` 识别新鲜 writer；两个同时新鲜或任一新鲜 embedded 实例均为 BLOCK。
- 三条当前 migration 的 ledger 必须分别具有同 checksum、同 statement count 且已推进到末尾的 progress 行；只有 ledger、没有 progress recovery proof 不再算完成。页面/API 验证只接受本机 IP loopback origin，跨 origin redirect、环境变量指向远端地址或非布尔订单权限字段均为 BLOCK。工作站不得直接传 `--local-runtime` 冒充生产验证；该参数仅供配置的生产 checkout 内部 SSH 调用。
- raw 日 K 可读范围为 2020-01-02 至 2026-08-14；完整的历史 QMT 行级 attestation 目前覆盖 2024-01-02 至 2026-07-24，之后只有零散单日。long-history OOS 和 forward Shadow outcome 是两类证据，禁止相互替代。
- 当前冻结协议为 `trading_v3.11.0-paper`，训练窗口由配置单一真值冻结为 `2023-01-01` 至 `2026-08-14`（`FROZEN_CONFIG_SIGNAL_START_INCLUSIVE_V1`）。当前全市场 suite `independent-horizons-v3-ledger-frozen-window-full-20260817-v1` 已完成并通过独立深验，但 T+1/T+5/T+20 的模型 Gate 仍全部为 `BLOCK`；没有 pin、没有注册、没有写数据库，也没有 Shadow contract、PAPER、生产或订单权限。

## 2. 迁移影响

`20260804_000_shadow_intelligence_runtime` 只扩展 V3/Shadow 研究账本：

1. 给 `st_decision_run_v3` 增加一个可空 `requested_as_of` 列和一个联合索引。
2. 新建六张表：horizon model artifact、horizon contract、horizon outcome、Shadow release、calibration gate、counterfactual learning run。
3. 建立三个外键，其中两个通过独立 `ALTER TABLE` 建立。
4. 建立十八个插入保护或不可变触发器；对应的 `DROP TRIGGER IF EXISTS` 与 `CREATE TRIGGER` 合计三十六条 statement。
5. 不修改 V2 账户、现金、Intent、订单、成交、Lot 或持仓账本；所有新增表的 `order_authority` 默认并被约束为 `0`。

`20260817_000_horizon_protocol_v2_governance` 在 artifact 表增加 artifact schema、模型协议和 selection policy 投影，并用四个 fail-closed 插入触发器把 V1 降为历史审计、把当时的 V2 约束为研究运行边界。

`20260817_001_horizon_candidate_ledger_registration` 是 Oracle MySQL 8.4-only 的前向迁移：

1. 增加五个完整候选账本/注册验证投影列。
2. 替换 named CHECK，使 V1/V2 仅可审计，V3 必须绑定 `CANDIDATE_EVALUATION_LEDGER_V1`。
3. 重建四个同名触发器；触发器总数不增加。V3 `PROCESS_VERIFIED` 必须有内容寻址账本和 registration verification，contract/outcome/release 只能引用当前 V3。
4. 该迁移依赖 MySQL 8.4 的 enforced CHECK 与 `DROP CHECK`；低于 8.4 必须直接 BLOCK，禁止生成另一套版本分支 DDL。

三条迁移完成后的隔离验收基线为 24 条 V3 migration、85 张表、83 个触发器；新增链仍不修改任何 V2 账户或执行事实表。

迁移使用 MySQL named lock、逐 statement progress、隐式 DDL commit 后的结构识别和最终 schema validator。进程中断后首选重新执行同一迁移，由恢复逻辑从已确认的 statement 继续；不能通过手工伪造 ledger 跳过校验。

## 3. 变更前门禁

只有统一维护批准后才能执行写操作。批准前只允许：

```powershell
.venv\Scripts\python.exe tools/migrate_trading_v3.py --dry-run
.venv\Scripts\python.exe tools/verify_trading_v3_production.py --local-runtime
```

正式窗口开始前必须：

1. 保存 `probiga` 的 schema-only 备份并验证可读。
2. 停止 V3 decision、counterfactual/Shadow worker 和手动 V3 任务入口，确认不存在持有 `st_decision_run_v3` metadata lock 的长事务。
3. 确认 MySQL 恰为允许的 8.4 版本、当前连接 schema 为 `probiga`，迁移账号具备 DDL/trigger/constraint 权限。
4. 再次 dry-run；只允许上述三条目标 migration 为 `would_apply`。若出现其他待执行项、checksum drift 或 progress 非零，终止窗口。

### 3.1 schema-only 备份

本机 PATH 中的 `mysqldump` 是 5.5，禁止用它备份 MySQL 8.4 schema。必须使用与服务器同版本的 8.4.11 客户端；下列命令会交互式请求密码，不把密码写入命令行或备份文件：

```powershell
$dumpExe = 'D:\MySQL84\software\mysql-8.4.11-winx64\bin\mysqldump.exe'
$backupDir = Join-Path (Resolve-Path '.') 'backups\schema'
New-Item -ItemType Directory -Path $backupDir -Force | Out-Null
$stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$dumpFile = Join-Path $backupDir "probiga_schema_before_shadow_$stamp.sql"
& $dumpExe --host=127.0.0.1 --port=3306 --protocol=tcp `
  --user=probiga_runtime --password --default-character-set=utf8mb4 `
  --no-data --routines --events --triggers --single-transaction `
  --skip-lock-tables --skip-column-statistics --set-gtid-purged=OFF `
  --result-file=$dumpFile probiga
if ($LASTEXITCODE -ne 0) { throw "mysqldump failed: exit=$LASTEXITCODE" }
$dumpInfo = Get-Item -LiteralPath $dumpFile
if ($dumpInfo.Length -le 0) { throw 'schema dump is empty' }
if (-not (Select-String -LiteralPath $dumpFile -SimpleMatch 'CREATE TABLE `st_decision_run_v3`' -Quiet)) {
  throw 'schema dump does not contain st_decision_run_v3'
}
if (-not (Select-String -LiteralPath $dumpFile -SimpleMatch 'CREATE TABLE `schema_migration_v3`' -Quiet)) {
  throw 'schema dump does not contain the V3 migration ledger'
}
$hash = Get-FileHash -Algorithm SHA256 -LiteralPath $dumpFile
$hash | Format-List
```

将 `$dumpFile`、文件大小、SHA-256 和生成时间写入维护记录。如需证明可恢复性，应由 DBA 将它恢复到一个新建、隔离、空的验证 schema，不得对 `probiga` 做覆盖恢复演练。

### 3.2 writer 和 metadata-lock 最终门禁

先通过现有运维入口优雅停止 `tools/run_scheduler_daemon.py`，等待其子任务退出；不得只改任务表的状态文字。然后用具有 `PROCESS` 以及 `performance_schema` 读权限的 DBA 连接执行下列只读 SQL：

本机 supervisor 会每五秒拉起 scheduler，所以必须先停 supervisor、再停 scheduler 整个进程树。下列命令只选择命令行和可执行路径都归属当前 ProBigA workspace 的进程，不会命中其他 workspace 同名脚本：

```powershell
$repo = (Resolve-Path '.').Path
$venvPython = (Resolve-Path '.\.venv\Scripts\python.exe').Path
$supervisorScript = (Resolve-Path '.\tools\run_local_live_supervisor.ps1').Path

$supervisors = @(Get-CimInstance Win32_Process | Where-Object {
  [string]$_.CommandLine -like "*$supervisorScript*"
})
$schedulers = @(Get-CimInstance Win32_Process | Where-Object {
  [string]$_.CommandLine -like "*$venvPython*" -and
  [string]$_.CommandLine -like '*tools/run_scheduler_daemon.py*'
})
$activeV3Children = @(Get-CimInstance Win32_Process | Where-Object {
  [string]$_.CommandLine -like "*$repo*" -and
  [string]$_.CommandLine -match 'tools[\\/]run_trading_v3_'
})
if ($activeV3Children.Count -ne 0) {
  throw "V3 child task is still running: $($activeV3Children.ProcessId -join ',')"
}
$supervisors | ForEach-Object { Stop-Process -Id $_.ProcessId -ErrorAction Stop }
$schedulers | Sort-Object ProcessId -Descending |
  ForEach-Object { Stop-Process -Id $_.ProcessId -ErrorAction SilentlyContinue }
Start-Sleep -Seconds 2
$remaining = @(Get-CimInstance Win32_Process | Where-Object {
  ([string]$_.CommandLine -like "*$supervisorScript*") -or
  (
    [string]$_.CommandLine -like "*$venvPython*" -and
    [string]$_.CommandLine -like '*tools/run_scheduler_daemon.py*'
  )
})
if ($remaining.Count -ne 0) {
  $remaining | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }
  Start-Sleep -Seconds 1
}
if (@(Get-CimInstance Win32_Process | Where-Object {
  ([string]$_.CommandLine -like "*$supervisorScript*") -or
  (
    [string]$_.CommandLine -like "*$venvPython*" -and
    [string]$_.CommandLine -like '*tools/run_scheduler_daemon.py*'
  )
}).Count -ne 0) { throw 'local scheduler writer fence failed' }
```

`data\scheduler.pid` 保存的 PID 是 venv launcher，数据库 heartbeat PID 是其 Python 子进程，两者不必相同。停止验收应以上述经 workspace 限定的整个进程树为空为准，不能只看 PID 文件。

远端 `iZwz9byapurj1a3h4z617mZ` 的旧 embedded scheduler 也是 writer。运维人员必须在该主机停止 API service 和 standalone scheduler，并把 `API_EMBEDDED_SCHEDULER_ENABLED=false` 作为 API 与 scheduler drop-in 的持久配置：

```bash
sudo systemctl stop probiga probiga-scheduler
systemctl is-active probiga probiga-scheduler
```

两个 service 在迁移窗口内都必须为 `inactive`。代码暗发布流程会在任何服务恢复前执行 `tools/add_trading_v3_tasks.py --writer-fence`，把 `trading_v3_counterfactual_audit` 与 `trading_v3_continuous_calibration` 持久化为 `enabled=0`；默认不再等价于激活。随后只恢复远端 API 与 standalone 权威：

```bash
sudo systemctl daemon-reload
sudo systemctl enable probiga-scheduler
sudo systemctl restart probiga probiga-scheduler
systemctl is-active probiga probiga-scheduler
systemctl is-enabled probiga-scheduler
```

必须得到 `probiga=active`、`probiga-scheduler=active/enabled`；`/api/health` 必须同时证明 `embedded_scheduler_enabled=false`、`embedded_scheduler_running=false`。恢复后还必须查询 `st_scheduler_runtime`，确认唯一新鲜权威为远端 `mode=standalone`，且 `heartbeat_age_seconds <= 2 * poll_seconds`。本机或 embedded 的任何第二条新鲜 heartbeat 都是双 writer BLOCK。

```sql
SELECT VERSION(), @@version_comment, DATABASE(), CURRENT_USER();
SHOW GRANTS FOR CURRENT_USER();
SELECT IS_USED_LOCK('probiga:trading_v3_schema');

SELECT instance_id, mode, host_name, pid, started_at, heartbeat_at,
       TIMESTAMPDIFF(SECOND, heartbeat_at, NOW()) AS heartbeat_age_seconds,
       poll_seconds
FROM st_scheduler_runtime
ORDER BY heartbeat_at DESC;

SELECT trx_mysql_thread_id, trx_state, trx_started, trx_tables_in_use,
       trx_tables_locked, trx_rows_locked, trx_rows_modified, trx_query
FROM information_schema.innodb_trx
ORDER BY trx_started;

SELECT ml.object_type, ml.object_schema, ml.object_name,
       ml.lock_type, ml.lock_duration, ml.lock_status,
       t.processlist_id, t.processlist_user, t.processlist_time,
       es.sql_text
FROM performance_schema.metadata_locks AS ml
LEFT JOIN performance_schema.threads AS t
  ON t.thread_id = ml.owner_thread_id
LEFT JOIN performance_schema.events_statements_current AS es
  ON es.thread_id = t.thread_id
WHERE ml.object_schema = 'probiga'
  AND ml.object_name IN (
    'st_decision_run_v3',
    'st_horizon_model_artifact_v3',
    'st_horizon_forecast_contract_v3',
    'st_horizon_outcome_v3',
    'st_shadow_release_v3',
    'st_calibration_gate_v3',
    'st_counterfactual_learning_run_v3'
  )
ORDER BY ml.lock_status, ml.object_name;
```

`innodb_trx` 必须为空，目标表不得有 `PENDING` metadata lock，且不得有不可解释的 transaction-duration `GRANTED` lock。运行 migration 的账号还必须在 `probiga.*` 上具有 `SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, REFERENCES, TRIGGER`；当前 `probiga_runtime` 的 `GRANT ALL PRIVILEGES ON probiga.*` 满足 DDL 集合，但它不能代替上述 DBA 的全局活跃事务/MDL 审计。

## 4. 经批准后的正向执行

```powershell
.venv\Scripts\python.exe tools/migrate_trading_v3.py
.venv\Scripts\python.exe tools/migrate_trading_v3.py --dry-run
```

第二条命令必须显示 24 条 migration 全部为 `exists`。随后先执行三套结构 validator；只有 ledger、progress 与 schema 全部通过后，才允许使用显式激活参数更新 Layer 4 任务：

```powershell
.venv\Scripts\python.exe tools/add_trading_v3_tasks.py --activate-layer4
```

不带 `--activate-layer4` 或显式使用 `--writer-fence` 时，两项 Layer 4 writer 必须保持 disabled。工具首先在单一事务中禁用这两个 task type 的所有现存行（包括意外重复行），然后才深验三条 migration；任一 ledger/checksum/statement-count/progress/schema 不通过即退出码 `2`，不执行定义 upsert或任何启用操作，已有 writer 仍保持关闭。通过后仍先把两项完整定义都落为 disabled，最后在同一事务中按精确两行 cardinality 一次性启用；任一 upsert、重复行或事务失败都必须保持双任务关闭。激活后两项任务必须各自唯一、enabled、完整定义无 drift；counterfactual 脚本必须包含显式 `--limit` 与 `--max-batches` 且不追加位置日期。不得手工把 `last_run_status` 改为成功；必须等待两项任务各自一次真实 `exit_code=0` 和成功 history。

结构验收必须同时确认：三条新增迁移的 checksum/statement count 与上述冻结值一致；progress 均为 `completed_statement_count=statement_count`；六张表、`requested_as_of`、artifact 协议/候选账本投影、联合索引、三个外键和最终触发器清单全部存在；Shadow、Horizon V2 governance 与 candidate-ledger 三个 schema validator 均无异常。不得仅以 migration 进程退出码为验收依据。

## 5. 模型与证据验收

三个模型 artifact 必须由配置显式 pin 到同一个 release，禁止扫描目录后猜测“最新”。当前 artifact/suite schema 为 V3，模型数学协议仍为 V2：

- T+1、T+5、T+20 的 model key、model version、artifact hash、feature protocol hash 必须各自独立。
- loader 必须重算 artifact 及所有嵌套证据 hash。
- 每个 artifact 必须引用确定性 gzip canonical JSONL 完整 OOS 候选账本；注册前从受控 release 目录流式重算压缩字节 hash、canonical record hash、fold prediction hash、每会话候选数、Top-12、selected ledger 和经济指标。缺文件、路径越界、重复样本、遗漏更高分候选、跨 suite 重放或任何 hash/计数不一致都必须 BLOCK。
- `prediction_kind` 必须为 `CALIBRATED_OOS`，gate 必须为 `PASS` 且 `contract_eligible=true`；模型自身仍须 `order_authority=false`。
- Walk-forward/OOS distinct session 不足必须单独 BLOCK，不得用六个 V3 forward decision day 代替 long-history OOS。
- artifact 的历史 dataset/OOS 标签来源必须陈述 QMT attestation 边界。未经 attestation 的 raw 历史只能作为明确标注的研究输入，不能被称作 execution-verified evidence。

2026-08-17 的旧 suite `independent-horizons-v3-ledger-diagnostic-full-20260817-v1` 使用了 `2020-01-02` 的非默认起点，与现行冻结的 `2023-01-01` 不一致。即使其内部曾经自洽且流式验证通过，在现行 V3 window/hash/code 契约下也必须标记为 `NON_DEFAULT_TRAINING_WINDOW` 历史诊断：不得 load 为 current，不得 pin、注册或发布 Shadow contract。旧 suite hash `43465232397716e3142f91c8a976f063d891f0f1201fd1aacc47b0140fee8ee2` 仅作审计索引，不属于当前模型证据。

现行冻结窗口的全市场 suite `independent-horizons-v3-ledger-frozen-window-full-20260817-v1` 独立执行了 `load_horizon_suite`、逐周期 `verify_horizon_artifact` 和三份完整候选账本的逐行流式重建，suite hash 为 `eb031261ab0d0780a37345eb214b4ed93db2acf95a9adfe5a98a5f180e892e4a`。其真实 Gate 结果仍为 `BLOCK`：

- T+1：OOS `1,178,021 / 219` 样本/会话，冻结筛选后无有效入选样本；Session IC `-0.017828`，Brier `0.247600`，PSI `0.149481`。
- T+5：OOS `1,170,370 / 218`，入选 `864 / 72`；成本后净期望 `-1.742925%`，profit factor `0.707481`，Session IC `-0.013732`，Brier `0.249166`，PSI `0.130266`。
- T+20：OOS `1,112,834 / 214`，入选 `852 / 71`；成本后净期望 `-4.487649%`，profit factor `0.549597`，Session IC `0.012980`，Brier `0.251932`，PSI `0.383498`。

三周期的 `contract_eligible/paper_eligible/production_eligible/order_authority` 均为 `false`。这些历史数据已被研究流程查看，后续调参回放只能继续标为历史研究；确认性证据必须来自协议冻结后的新 forward Shadow 成熟样本。

forward Shadow outcome 另行验收：最新 outcome 的每个冻结 bar 都必须在 `sm_stock_kline` 行级标记 `QMT_ATTESTED` 且有数据源。即使全部 attested，`execution_feasibility` 仍保持 `UNVERIFIED_RESEARCH`，因为涨跌停、停牌、容量和真实成交并未由日 K 证明。禁止将其改写为 `EXECUTABLE_VERIFIED`。

## 6. 页面与 API 真值

生产 verifier 会从正在运行的本机 API 并行 GET：

- `/api/v3/readiness`
- `/api/v3/research/governance`
- `/api/v3/research/horizons/latest`
- `/api/v3/research/learning/latest`
- `/api/v3/research/shadow/status`

HTTP、envelope、当前 config version/hash、`real_trading_enabled=false`、`order_authority=false` 和非 `UNAVAILABLE` 状态缺一即 BLOCK。`/api/v3/readiness` 还必须明确返回 `paper_ready=true`；数据库直查通过但页面 GET 不一致同样 BLOCK。

最终执行：

```powershell
.venv\Scripts\python.exe tools/verify_trading_v3_production.py --local-runtime
```

以 JSON 中 `fourth_layer.activation_status=PASS` 且总 `acceptance_status=PASS` 为完成依据。日志、任务状态或页面文字均不能替代该证据。

## 7. 中断与恢复

发生异常时首先停止 V3 writers，然后重新运行同版本 migration。恢复器会核对已提交的列、索引、外键、触发器和 progress；任何结构 drift 都会报错并停止。

暗发布一旦写入 Layer 4 writer fence，即使随后代码发布回滚，两项任务也必须继续保持 disabled；回滚流程不得凭“上一版本曾启用”自动恢复 writer。只有重新证明当前运行版本与三条 migration/schema 完全匹配后，才可再次执行 `tools/add_trading_v3_tasks.py --activate-layer4`。

前向恢复的唯一允许流程是：保留原始错误和当时 ledger/progress/结构快照；确认 writer fence 仍生效；不改代码中的 migration statement；再次执行 `tools/migrate_trading_v3.py`；随后执行 `--dry-run` 并要求 24 条全部 `exists`；最后执行全部结构 validator 和生产 verifier。若重试报 drift，终止窗口并由 DBA 将 schema-only 备份与 `SHOW CREATE` 现状进行对比，不得手工推进 progress 或伪造 ledger。

仓库没有自动 down migration。若六张 Shadow 表已经写入 artifact、合同、outcome、learning 或 release 数据，禁止原地回退，必须保留备份并采用前向修复。任何人工回退还必须逆序处理 candidate-ledger 与 V2 governance 的 CHECK、投影列和触发器，不能只按最初十八个 Shadow 触发器操作。只有确认六表全空、完成 schema 备份且获得单独破坏性操作批准后，才允许 DBA 根据三条迁移的精确逆序清单执行；该操作不可由本 Runbook 自动执行。

无论 Shadow 是否达到 evidence-ready，自动模型晋级和真实下单仍然关闭。外部签名证明与执行授权属于单独的发布批准，不得由 migration、artifact 或 calibration task 自动授予。
