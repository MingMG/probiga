# ProBigA MySQL 5.5.20 → 8.4.11 LTS 迁移升级实施手册

日期：2026-08-05（Asia/Shanghai）  
状态：实施中；生产 `localhost:3306` 尚未切换  
目标版本：Oracle MySQL Community Server 8.4.11 LTS

## 1. 已确定的架构决策

本次只替换数据库基础设施，不建立第二套账户、订单、持仓、成交、风控或迁移账本。V2 交易底座、V3 决策系统和 V4 增量能力继续使用同一个 `probiga` 业务库及既有权威账本；隔离验收库只用于升级验证，不承担生产读写。

MySQL 5.5.20 不能把物理数据目录直接交给 8.4.11。本次采用同机、旁路、逻辑迁移：旧 5.5 保持可回滚，8.4 使用全新数据目录，通过一致性逻辑备份恢复。禁止把 5.5 的 `ibdata1`、`.frm` 或系统库直接复制进 8.4 数据目录。

最终磁盘布局固定为：

| 用途 | 最终位置 | 约束 |
|---|---|---|
| MySQL 8.4.11 程序 | `D:\MySQL84\software\mysql-8.4.11-winx64` | 本机盘 |
| 正式配置 | `D:\MySQL84\config\my.ini` | 不得含 `F:` |
| 正式日志 | `D:\MySQL84\logs` | 不得含 `F:` |
| 正式 TLS 证书/私钥 | `D:\MySQL84\certs` | 本机盘；私钥仅 SYSTEM/Administrator |
| 正式数据目录 | `E:\MySQL84\Data` | 本机盘；`lower_case_table_names=1` 必须在初始化前确定 |
| 迁移备份与中转 | `F:\ProBigA-MySQL-Upgrade-20260805` | 可拔除；不得注册服务或成为运行时依赖 |

`F:` 是移动硬盘，只允许保存安装包、逻辑备份、旧库回滚副本、哈希和迁移日志。完成后服务二进制、配置、数据、证书、临时目录、日志、环境变量和计划任务均不得引用 `F:`。

## 2. 源库事实基线

- 服务：`MySQL`，Oracle MySQL 5.5.20，`LocalSystem`，监听 `3306`。
- 数据库：`biga`、`probiga`、`probiga_qmt_history`。
- 对象：181 张 InnoDB 表、2754 列、797 个索引、2 个触发器、1 个外键；无视图、存储过程和事件。
- 所有业务表均为 InnoDB，因此 `--single-transaction --quick` 可取得一致性快照；快照期间仍须禁止 DDL。
- 二进制日志关闭，无法依靠原生增量复制完成无停机追平。
- 物理布局：共享系统表空间 `E:\MySQL Datafiles\ibdata1`，约 133.46GiB，`innodb_file_per_table=0`。
- 估算业务数据与索引约 140GiB；E 盘当前空闲约 45GiB。只有在旧库物理回滚副本完整、哈希验证通过且旧服务已停止后，才能释放旧共享表空间，为 `E:\MySQL84\Data` 腾出空间。
- 源运行时区为 `SYSTEM`，实际 UTC+08:00；正式 8.4 必须保持 `+08:00` 业务语义。隔离验收实例使用 UTC，不代表正式配置。
- 源 SQL mode：`STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION`。

## 3. 已建立的隔离验收环境

- 程序：`D:\MySQL84\software\mysql-8.4.11-winx64`。
- 数据：`D:\MySQL84\test-data`，仅为隔离验收，不是正式数据目录。
- 配置：`D:\MySQL84\config\my-test.ini`。
- 地址：仅 `127.0.0.1:33084`，X Protocol 关闭。
- UUID：`810354d6-9061-11f1-84ae-74d4dd7f8500`。
- 认证：`caching_sha2_password`，验收管理员 `REQUIRE SSL`，凭据文件 ACL 仅 Administrator/SYSTEM。
- TLS 已实测，schema 审计连接使用 `TLS_AES_256_GCM_SHA384`。
- 严格 SQL mode 保持 `ONLY_FULL_GROUP_BY`、零日期检查、除零检查和严格事务模式；不得为了通过旧 SQL 而整体关闭。
- `default_collation_for_utf8mb4=utf8mb4_general_ci` 已通过 `SET PERSIST` 写入隔离数据目录，并经过重启验证为 `PERSISTED`。

安装包：

- `F:\ProBigA-MySQL-Upgrade-20260805\packages\mysql-8.4.11-winx64.zip`
- MD5：`2e833921898a9a030ea6bfe81bd811bc`
- SHA-256：`a492371d687d2bab088b0062581144a0044b8964baefdf4faa579292b423d25c`

## 4. 已确认并处理的兼容差异

### 4.1 逻辑备份 SQL mode

5.5 的 `mysqldump` 会在触发器前恢复已被 8.4 删除的 `NO_AUTO_CREATE_USER`，原始 schema 导入在该语句确定失败。新增 `tools/sanitize_mysql55_dump_for_mysql84.py`：

- 二进制逐行流式处理，内存占用与备份大小无关；
- 只匹配完整 `SET sql_mode='…'` 语句；
- 不做全局字符串替换，不会改 INSERT 数据；
- 源文件绝不原地覆盖；
- 输出源/目标 SHA-256、字节数和替换条数；
- 可用 `--expect-changed-statements` 阻止意外多改或少改。

schema 预演只修改 2 条语句：

- 原文件 SHA-256：`c02ffa788876c8c6f8eaef500dcf323115a976f59a43b5402566cd567d7998dd`
- 8.4 文件 SHA-256：`9780f1dcc3c4e5cea1f873a6ba97778c45ff842aed497693fbc01febba4bfb4f`

### 4.2 utf8mb4 默认排序规则

5.5 的 `SHOW CREATE` 会省略当时默认的 `utf8mb4_general_ci`；8.4 若直接恢复，会将省略项解释成 `utf8mb4_0900_ai_ci`，改变中文比较、排序和唯一键语义。目标采取两层控制：

1. `character-set-server=utf8mb4`、`collation-server=utf8mb4_general_ci`；
2. 初始化后执行并验证：

```sql
SET PERSIST default_collation_for_utf8mb4='utf8mb4_general_ci';
```

该变量在 8.4.11 中允许持久化但作为启动参数会导致服务器拒绝启动，因此不得写成 option-file 参数。正式初始化必须执行一次 `SET PERSIST`，重启后检查 `performance_schema.variables_info.VARIABLE_SOURCE='PERSISTED'`。

### 4.3 MySQL 8.4 保留字和 REGEXP

- 活动业务 SQL 中的 `rank` 列已统一安全反引号；绑定参数 `:rank`、Python 字段和 `RANK()` 窗口函数不变。
- V4 冻结迁移 001–007 的 statement 文本和 checksum 均未修改。
- 仅在 Oracle MySQL 8.x 执行边界，将不兼容的 `BINARY identifier [NOT] REGEXP` 渲染为显式 utf8mb4 二进制排序规则表达式；5.7.38 继续使用冻结原文。
- 真实 8.4.11 已验证 7 个迁移、32 个触发器、12 个 REGEXP 触发器体、负向 DML 和 partial recovery。

### 4.4 5.5 丢失的 CHECK

冻结 V2 migration 共声明 134 条 CHECK；5.5/5.7 不执行并在 dump 中丢失。新增 forward-only 物化器：

1. `ADD CONSTRAINT ... CHECK (...) NOT ENFORCED`；
2. 使用 MySQL 自身表达式审计所有历史行；
3. 只有 0 违规才 `ALTER CHECK ... ENFORCED`；
4. 有违规时保留 NOT ENFORCED 并阻断；
5. 只增不删，幂等恢复，不改冻结 migration/checksum。

当前恢复架构中只有 `st_trade_account_v2` 适用 3 条：`initial_cash>=0`、`cash_balance>=0`、`real_trading_enabled=0`。33084 实测均为 `ENFORCED=YES`，两项负金额写入被错误 3819 拒绝，实盘开关继续被既有触发器拒绝，重复应用新增 0 条。

正式 apply 必须同时提供独立核验的目标 schema、UUID、端口和“业务写入已离线”确认；任一不符时在 DDL 前拒绝。

### 4.5 精确版本门禁

V2/V3/V4 migration、schema gate、acceptance 和 runtime grant audit 共用精确 Oracle MySQL 白名单：

- `5.7.38`：保留既有验证基线；
- `8.4.11`：本轮隔离验收版本；
- 相邻 patch、未知发行版、MariaDB、Percona：fail-closed；
- `PRODUCTION_DATABASE_ACTIVATION_ALLOWED=False` 保持不变。

数据库升级成功不等于交易生产激活，不能借此绕过 V2/V3/V4 的业务审批和前向证据门槛。

### 4.6 binlog 与触发器最小权限

Oracle MySQL 8.4 默认开启 binary log，默认格式为 `ROW`。在
`log_bin_trust_function_creators=OFF` 时，只有 schema 级 `TRIGGER` 权限的
迁移账号创建触发器会确定性失败并返回错误 1419。处理方案不是给业务或迁移账号
全局 `SUPER`，也不是关闭 binlog，而是把信任开关限定在停写维护窗口：

1. 管理连接先核验 `8.4.11`、目标 UUID、端口、`log_bin=ON`、
   `binlog_format=ROW` 和业务已停写；
2. 记录当前值，执行 `SET GLOBAL log_bin_trust_function_creators=ON`；
3. 由 `REQUIRE SSL`、仅目标 schema 授权的迁移账号执行 V2/V3/V4；
4. 无论成功或失败都在 `finally` 路径恢复为 `OFF`；
5. 验证开关为 0，临时验收库/账号为 0 残留，并审计 binary log。

该变量在 8.4 已被标记为 deprecated，因此不写入正式持久配置；正式基线始终为
`OFF`。33084 真实验收已证明：V2 15 个迁移、V3 21 个迁移、V4 7 个迁移均可在
TLS 和 schema 级最小权限下首次执行及幂等重放，执行后信任开关、临时库和临时账号
全部恢复为 0。

### 4.7 外键元数据与严格日期默认值

MySQL 对 `NO ACTION` 与 `RESTRICT` 使用相同的外键行为，但不同版本的
`INFORMATION_SCHEMA` 可能返回不同标签。V2/schema 审计现在只把这一对标签规范为
`RESTRICT`；`CASCADE`、`SET NULL` 等真实语义差异仍会阻断。冻结迁移文本和 checksum
未修改。

源库有 8 个 `DATETIME` 列保留了 5.5 时代的零日期默认值：

- `jq_strategy_meta.created_at`、`jq_strategy_meta.updated_at`；
- `jq_strategy_picks.created_at`；
- `st_daily_review.etl_sync_at`；
- `st_portfolio_analysis_log.created_at`；
- `st_portfolio_trans_log.created_at`；
- `st_recommended_stocks.created_at`；
- `st_user_portfolio.etl_sync_at`。

在线源库逐列只读审计当前均为 0 个全零/部分零日期和 0 个 NULL，但这是运行中快照，
最终停写后必须重跑。恢复后仅在 8.4 目标执行 forward-only
`ALTER COLUMN ... SET DEFAULT (CURRENT_TIMESTAMP)`，不改 5.5 和冻结迁移。若最终审计
发现真实零日期，不允许用当前时间静默伪造历史；必须阻断切换并按业务时间确定性修复。
实现工具为 `tools/materialize_mysql84_datetime_defaults.py`；33084 独立临时库已验证
首次 8 列/7 条 DDL、重复执行 0 修改，并确认部分零日期会在任何 DDL 前阻断。

### 4.8 正式运行时 TLS

API、scheduler、worker、QMT 与批处理共用 `server/common/engine_factory.py`。正式
8.4 环境必须同时设置：

```text
MYSQL_TLS_REQUIRED=true
MYSQL_SSL_CA=D:\MySQL84\certs\ca.pem
```

CA 不进入数据库 URL；共享工厂只接受显式 `mysql+pymysql`，固定启用证书链校验，
连接建立后检查 `Ssl_cipher`，并拒绝 URL TLS 参数、调用方 TLS `connect_args`、
`creator/module` 绕过和任何无 TLS 的静默重连。5.5 回滚窗口只有在
`MYSQL_TLS_REQUIRED=false` 且没有 `MYSQL_SSL_CA` 时继续兼容。正式
`D:\MySQL84\config\my.ini` 同时设置 `require-secure-transport=ON`，所以服务端与
客户端形成双重门禁。33084 已按该项重启验证：`@@require_secure_transport=1`，
CA 验证连接协商 `TLS_AES_128_GCM_SHA256`，显式禁用 TLS 的客户端连接失败。

## 5. schema 语义验收证据

工具：`tools/audit_mysql55_to_mysql84_schema.py`  
证据：`F:\ProBigA-MySQL-Upgrade-20260805\manifests\schema-semantic-audit-preflight.json`

该工具使用受保护的 client option files，目标必须 TLS，且验证目标版本和 UUID。只规范化三类无业务语义差异：整数显示宽度、`DEFAULT_GENERATED` 元数据标记、MySQL 中等价的 `NO ACTION/RESTRICT` 外键标签。

预演结果：

| 对象 | 源数量 | 目标数量 | 语义差异 |
|---|---:|---:|---:|
| schemas | 3 | 3 | 0 |
| tables | 181 | 181 | 0 |
| columns | 2754 | 2754 | 0 |
| indexes | 797 | 797 | 0 |
| 非 CHECK constraints | 262 | 262 | 0 |
| referential constraints | 1 | 1 | 0 |
| triggers | 2 | 2 | 0 |

列默认值按原始字节 HEX 比较，排序规则不做宽松处理，触发器正文按字节比较；三条新增 CHECK 作为目标增强项单独列出。

## 6. 备份与恢复阶段

### 6.1 在线一致性底稿（当前阶段）

单次 `mysqldump` 同时导出三个库，以取得同一 InnoDB snapshot：

```text
--single-transaction --quick --skip-lock-tables
--routines --events --triggers --hex-blob
--default-character-set=utf8mb4 --max_allowed_packet=256M
--databases biga probiga probiga_qmt_history
```

输出：`F:\ProBigA-MySQL-Upgrade-20260805\backup\full-consistent-5.5.20-20260805T0834+0800.sql`。该快照不停止 3306，只用于完整恢复演练和回滚底稿；完成后必须检查退出码、0 字节 stderr、文件尾完成标记、源 schema 未漂移、SHA-256 和 sanitizer 精确替换数。

本次在线底稿为 96,613,265,697 字节，stderr 为 0，文件尾包含
`Dump completed on 2026-08-05 9:11:10`，结束后 schema 语义复审仍为 0 差异。
旧监控器只写出了空的 `DUMP_EXIT_CODE=`，没有形成可接受的数值退出码证据，因此该文件
可以用于恢复演练但不能直接充当最终切换备份。最终停写 dump 必须由新的编排器直接
创建子进程并 `wait()` 捕获真实退出码；不允许从“进程已消失”反推 0。实现为
`tools/run_mysql55_consistent_dump.py`：它还会验证精确 5.5.20/3306、三业务库、
181 张 InnoDB 表、空 legacy `test` schema、无其他客户端会话/事务（final-frozen
模式），并把退出码、footer、stderr 大小和 SHA-256 原子写入 manifest。

### 6.2 为什么仍需要最终停写窗口

源库未开启 binlog。在线 dump 开始后产生的 INSERT/UPDATE/DELETE 无法被可靠、完整地追平，尤其无法只靠时间列发现删除。因此不得把在线底稿直接宣称为最终一致数据。

最终切换必须：

1. 停止 API 写路径、scheduler、数据同步、QMT bridge/worker 和所有外部写入；
2. 以 `information_schema.processlist` 证明只剩维护连接且无业务事务；
3. 再生成最终单事务全量 dump，或采用另行验证过的 CDC 方案；本手册默认前者；
4. 写入保持冻结，直到目标恢复、验收、服务切换完成。

不能用“长短线系统仍可读”替代数据库一致性；维护窗口时长由最终 dump、转换、恢复和验收实测决定。

## 7. 正式切换步骤（全部前置门通过后执行）

### 7.1 写前和破坏性操作前门

- 在线底稿及最终 dump 均完整，stderr 为 0，SHA-256 已落盘；
- dump 转换替换数与 schema 预演一致；
- 恢复演练、V2/V3/V4、TLS、严格 SQL mode、保留字和 CHECK 验收全绿；
- 旧 `ibdata1`、C 盘 datadir 和 `my.ini` 已复制到 F 的独立回滚目录并逐文件哈希；
- 明确记录旧服务名、PathName、启动类型、端口、账户和防火墙规则；
- 确认 `E:\MySQL84\Data` 的预计容量与恢复临时空间足够；空间不足不得删除唯一旧副本。

### 7.2 释放 E 盘空间

仅在旧 `MySQL` 服务已正常停止、业务写入已冻结、物理回滚副本完整且哈希通过后，才允许把旧 `E:\MySQL Datafiles\ibdata1` 移出 E 盘。禁止在旧服务运行时复制后立即删除活动文件。

移动应使用单一 PowerShell 流程、显式绝对路径，并在操作前后再次解析目标路径。旧文件移到 F 后仍是回滚资产，不立即删除。

### 7.3 初始化正式 8.4 数据目录

1. 停止 33084 隔离实例，确认端口和 PID；
2. 创建空的 `E:\MySQL84\Data`，设置 LocalSystem 与管理员 ACL；
3. 使用正式 `D:\MySQL84\config\my.ini` 初始化；
4. 初始化模式只生成 RSA key，不生成正式 TLS CA/server 证书。首次启动必须使用不含
   显式 `ssl-ca/ssl-cert/ssl-key` 的本机 bootstrap 配置和临时端口 `33085`，让 8.4
   在新 datadir 自动生成证书；
5. 因为 `skip-name-resolve=ON` 且只开放 TCP，初始化产生的 `root@localhost` 不能从
   `127.0.0.1` 登录。首次启动必须通过 ACL 受保护的一次性 `init-file` 创建
   `caching_sha2_password` 的 `admin@127.0.0.1 REQUIRE SSL`、授予管理权限并锁定
   `root@localhost`；禁止通过 `--skip-grant-tables` 绕过。验证管理员登录后立即删除
   含明文临时凭据的 init-file；
6. 正常关闭 bootstrap 实例，把 `ca.pem`、`server-cert.pem`、
   `server-key.pem` 复制到 `D:\MySQL84\certs`，并把私钥 ACL 收紧到
   SYSTEM/Administrator；不得把 CA 私钥配置给服务端；
7. 使用正式 `D:\MySQL84\config\my.ini` 重启，验证 `require_secure_transport=ON`、
   CA 链和明文拒绝，再建立 `caching_sha2_password` 管理和业务账户；
8. 执行 `SET PERSIST default_collation_for_utf8mb4='utf8mb4_general_ci'` 并重启验证；
9. 恢复阶段使用临时非 3306 端口，正式配置最终仍为 3306；
10. 若恢复需要 F 上的临时目录，只允许使用单独的 rehearsal/restore 配置；正式
   `my.ini` 不得保留该项。

正式配置必须保留：

- `lower_case_table_names=1`；
- `character-set-server=utf8mb4`；
- `collation-server=utf8mb4_general_ci`；
- `default-time-zone=+08:00`；
- 严格 SQL mode（不关闭 `ONLY_FULL_GROUP_BY` 等）；
- `innodb_file_per_table=ON`；
- `local_infile=OFF`、`secure_file_priv=NULL`；
- `skip-name-resolve=ON`；
- 显式开启 `log-bin`、`binlog-format=ROW`、`sync-binlog=1`，并设置受容量监控的短期
  自动过期；`log_bin_trust_function_creators` 的常态值为 `OFF`；
- 显式本机数据、日志、PID 和证书路径。

### 7.4 恢复顺序

1. 恢复 sanitizer 输出，而不是原始 5.5 dump；
2. 导入客户端连接先确认目标版本、临时端口、UUID、datadir 和 TLS；
3. 导入退出码非 0 或 stderr 出现 ERROR 时立即阻断；
4. 执行 schema 语义审计；
5. 对全部表做精确行数、主键边界、分区/日期覆盖、关键金额聚合和抽样内容哈希；
6. 物化 CHECK，历史违规非 0 时阻断；
7. 最终零日期审计为 0 后，执行 8 个日期默认值的 forward-only 整改并复核；
8. 在受控临时信任窗口执行 V2/V3/V4 migration/acceptance，随后强制恢复
   `log_bin_trust_function_creators=OFF`；
9. 执行严格 SQL mode 业务回归；所有写入探针均在事务内回滚或使用唯一测试实体，
   确认残留 0。
10. 恢复库上的增量迁移只能通过
    `tools/run_mysql84_restored_migrations.py`：它固定 `probiga`，要求精确
    UUID/端口/datadir/TLS、业务停写确认和 trigger-maintenance wrapper，按
    V2→V3→V4 顺序执行并做一次幂等 replay；不能直接调用普通的
    `migrate_trading_v2.py`/`migrate_trading_v3.py`。
11. 使用只读账户或管理员只读事务执行
    `tools/mysql84_restored_business_smoke.py`，验证三库路由、UUID/端口/TLS、
    迁移 ledger 和 `real_trading_enabled=0`；该 smoke 不执行任何写入。

## 8. 最终验收门

以下任一失败都不能切换：

- 版本、发行版、端口、UUID、datadir 与预期不一致；
- TLS 为空或业务账户未 `REQUIRE SSL`；
- schema 语义哈希有差异；
- 关键表行数、主键范围、日期覆盖、金额聚合或抽样哈希不一致；
- 存在零日期/非法日期、CHECK 历史违规或字符集替换字节；
- `ONLY_FULL_GROUP_BY` 等严格模式下业务 SQL 报错；
- V2/V3/V4 migration checksum、trigger metadata 或 partial recovery 不通过；
- 账户、订单、持仓、成交或风控权威路径指向第二套账本；
- F 盘仍出现在服务、配置、环境变量、计划任务或运行时打开文件中。

切换后至少验证：登录、行情、选股、行业/事件因子、模拟盘决策、订单生命周期、持仓和现金一致性、风控拒绝、早报/晚评、scheduler、QMT 只读/写入边界、API 健康和数据库备份任务。

## 9. 服务切换与回滚

切换：

1. 保持业务写入冻结；
2. 正常停止旧 `MySQL`；
3. 将正式 8.4 服务注册到本机 D 盘程序和配置，启动后监听 3306；
4. 更新受保护的应用数据库 URL/凭据，重启应用进程；
5. 验证所有连接的 `@@version=8.4.11`、目标 UUID、TLS cipher 和 schema；
6. 通过 smoke/数据一致性后才恢复 scheduler 和写入；
7. 生产激活开关仍保持 false，数据库切换不改变交易授权。

回滚触发条件包括：数据不一致、关键 SQL/接口失败、认证/TLS 不兼容、严重性能回退、CHECK/触发器误拒绝或恢复后业务证据缺失。

回滚时停止新写入并保存 8.4 故障现场；停止 8.4，恢复旧物理目录和原配置，启动旧 `MySQL`，核对 5.5.20/3306/旧 datadir 后恢复应用。8.4 已接收的新写入不能自动回灌 5.5，因此切换初期必须缩短观察与决策时间，并留存所有新写入审计。

## 10. F 盘拔除验收

目标稳定后，先保持 F 插入但做运行时依赖审计：

- Windows 服务 PathName 不含 `F:`；
- MySQL `@@basedir`、`@@datadir`、日志、PID、证书、tmpdir 不含 `F:`；
- 项目 `.env`、正式配置、计划任务、启动脚本不含 `F:`；
- 没有 mysqld、Python、scheduler、QMT 进程打开 F 上文件；
- 最新本机备份策略已建立，不能把“移动硬盘仍插着”当成唯一持续备份。

完成上述检查并正常停止所有临时迁移进程后，才可安全弹出 F。F 上的旧库和逻辑备份在回滚保留期内不得删除。

## 11. 当前实施状态

| 项目 | 状态 |
|---|---|
| 5.5 源库与磁盘盘点 | 完成 |
| 8.4.11 ZIP 下载与双哈希 | 完成 |
| D 盘隔离实例、TLS、认证、严格模式 | 完成 |
| D 盘正式配置基线 | 完成并通过 `mysqld --validate-config`；不含 F 盘路径 |
| schema dump 转换和完整空架构恢复 | 完成 |
| schema 语义哈希比对 | 完成，差异 0 |
| `rank` / REGEXP / 精确版本策略 | 完成并已真实/单测验收 |
| CHECK forward-only 物化 | 完成并已真实验收 |
| 在线一致性全量底稿 | 完成 89.98 GiB；footer/stderr/schema 通过，旧监控 exit code 证据缺失，仅用于演练 |
| 全量 sanitizer 与双 SHA-256 | 进行中 |
| 恢复库 V2/V3/V4 统一增量迁移门禁 | CLI 已实现；待全量演练恢复后执行 |
| 恢复库只读业务 smoke | CLI 已实现；待全量演练恢复后执行 |
| REQUIRE SSL 正式 acceptance CLI | 完成；V2/V3/V4 真实 8.4 最小权限验收通过 |
| API/scheduler/worker/QMT 统一运行时 TLS | 完成；97 项测试及 33084 只读握手通过 |
| 8.4 binlog/触发器维护窗口控制 | 完成隔离验收；待正式切换执行 |
| 8.4 全量保留字审计 | 完成；`change`、`keys`、`rows` 活动 SQL 已整改 |
| 严格 SQL mode 与零日期审计 | 完成预演；82 项测试和 33084 探针通过，待恢复后动态 SQL/最终停写复核 |
| 全量数据恢复与逐表数据一致性 | 待在线底稿完成后执行 |

### 11.1 恢复演练异常与修复记录（2026-08-05）

第一次全量恢复在 `biga.a_share_daily_sina` 的约 1 MiB 多行 `INSERT`
解析阶段触发了 Oracle MySQL 8.4.11 Windows 进程访问冲突；错误回溯位于
`string2decimal()`，不是源数据 SQL 语法错误。恢复工具现已在两个层面处理：

1. `tools/sanitize_mysql55_dump_for_mysql84.py` 仍按 64 KiB 的值元组边界拆分
   超大 `INSERT`，原始 dump 永不原地修改；
2. `tools/run_mysql84_logical_restore.py` 的恢复默认边界收紧为 16 KiB，并在恢复
   stdin 管道中实时执行拆分，
   因此无需再制作第二份 96 GiB 文件；证据 JSON 会记录
   `stream_transform.split_insert_bytes`。

第二次演练发现 256 KiB 仍允许 257,677 字节的
`qmt_local_stock_kline` 语句进入 8.4.11 解析器并触发访问冲突；第三次演练
在 64 KiB 边界的 `sm_stock_minute` 语句再次复现访问冲突；第三次演练在
32 KiB 边界仍于 `sm_stock_minute` 的 `string2decimal()` 解析路径复现。已用同一
崩溃语句验证 16 KiB 边界可安全解析，并将恢复工具默认值收紧为 16 KiB。恢复同时支持
`--defer-secondary-indexes all`：先加载行数据，随后按原定义重建二级索引；该
变换只在演练恢复中启用并写入证据 JSON。每次失败演练的数据目录只能作为失败
证据保留，不得续用或作为生产数据目录。

第二次演练使用全新 `mysql84-data-6`、全新 UUID 和 33085 端口；该实例的临时
配置和数据目录仍在 F 盘，仅作为可拔除的迁移中转，不得注册为正式服务。生产
`3306`、旧 5.5 数据目录和应用连接未停止、未修改。只有恢复成功、schema/data
manifest、DATETIME/CHECK、V2/V3/V4 migration 和只读业务 smoke 全部通过后，才
能在另行批准的停写维护窗口执行最终切换。

为缩短仅用于容量/兼容性验证的演练恢复，运行中临时将该实例的
`innodb_redo_log_capacity` 调到 4 GiB、`innodb_buffer_pool_size` 调到 8 GiB、
`innodb_flush_log_at_trx_commit=0`、`sync_binlog=0`，并提高 `innodb_io_capacity`。
这些值没有写入正式 `D:\MySQL84\config\my.ini`，也不得带入生产切换；若演练
进程异常终止，只能丢弃该 F 盘演练目录并从头恢复。
| 最终停写快照、E 盘正式数据目录、3306 切换 | 未执行 |
| F 盘无依赖与拔除验收 | 未执行 |

在最后五项完成前，不得表述为“生产数据库升级完成”。
