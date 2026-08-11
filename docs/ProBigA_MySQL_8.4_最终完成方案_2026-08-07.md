# ProBigA MySQL 8.4 最终完成方案

日期：2026-08-07（Asia/Shanghai）  
状态：代码完成、升级专项测试通过；数据恢复与生产切换尚未完成  
目标：Oracle MySQL Community Server 8.4.11 LTS

## 一、最终结论

本次升级只替换数据库基础设施，不另建第二套交易系统。V2 账户、订单、持仓、成交、现金和风控账本，V3 决策数据，以及 V4 增量能力仍使用原 `probiga` 权威库。

旧 MySQL 5.5 不能直接把物理目录交给 8.4。本次采用以下唯一正式路径：

1. MySQL 5.5 在线一致性 dump，并记录快照 binlog 坐标；
2. 在隔离的 MySQL 8.4 目标恢复该快照；
3. 用 5.5 STATEMENT binlog 将恢复目标追平；
4. 停止业务写进程和计划任务；
5. 持续持有 MySQL 5.5 全局读锁，并以心跳证明锁未丢失；
6. 完成最后一次 binlog 追平、逐表数据比较、兼容性 DDL、V2/V3/V4 和 smoke；
7. 正常停止两个数据库实例；
8. 把旧 5.5 物理文件完整复制并校验到 F 盘后，才释放 E 盘的旧 `ibdata1`；
9. 把已验收的 8.4 冷数据完整复制并校验到 `E:\MySQL84\Data`；
10. 注册本机 MySQL 8.4 服务、原子切换 `.env`，在 3306 上复验 UUID、TLS、版本、数据目录和持久化参数。

任何一步失败都停止后续动作。停旧库之前失败时直接解锁并保留旧库在线；停旧库之后失败时，自动归档 E 盘的正式或中断 staging 现场到 F、恢复旧 `ibdata1` 和旧 `.env`、复验 5.5.20-log/3306 后再报告失败。旧库、F 盘恢复数据、逻辑备份和失败现场不会被当作临时垃圾删除。

## 二、确定的数据与运行布局

| 项目 | 正式位置 | 约束 |
|---|---|---|
| MySQL 8.4 程序 | `D:\MySQL84\software\mysql-8.4.11-winx64` | 本机盘 |
| 正式配置 | `D:\MySQL84\config\my.ini` | 不允许引用 F 盘 |
| 正式证书 | `D:\MySQL84\certs` | 私钥 ACL 仅 SYSTEM/Administrators |
| 正式日志 | `D:\MySQL84\logs` | 本机盘 |
| 正式数据 | `E:\MySQL84\Data` | 目标服务唯一数据目录 |
| 旧库物理回滚副本 | F 盘独立 rollback 目录 | 验收期内保留，不删除 |
| dump、sanitizer、迁移证据 | `F:\ProBigA-MySQL-Upgrade-20260806` | 只作迁移与回滚资产 |

F 盘拔掉后，正式服务、配置、证书、日志、数据目录、应用 `.env` 和 Windows 服务路径均不得引用 F 盘。

## 三、已经实现的执行组件

### 1. 快照与增量追平

- `tools/run_mysql55_consistent_dump.py`
  - 一致性 dump；
  - `--master-data=2` 快照坐标；
  - 校验精确源版本、端口、三业务库、stderr、footer、返回码和 SHA-256；
  - 原始 dump 不原地修改。
- `tools/run_mysql55_to_mysql84_binlog_catchup.py`
  - 只接受 5.5.20-log、3306、server-id 55、STATEMENT；
  - 只接受精确 UUID/端口/datadir 的 8.4.11 目标；
  - 分段提取、SQL 安全审计、逐段应用和 checkpoint；
  - 禁止系统库、管理命令和生产 3306 目标；
  - 最终模式要求真实停写且无活动业务事务。

### 2. 真实停写门禁

- `tools/freeze_probiga_business_writers.ps1`
  - 停止已知 API、scheduler、QMT、同步、抓取和回填写进程；
  - 禁用指向 ProBigA 的 Windows 计划任务；
  - 只保存 PID、进程名和命令哈希，不保存或重放完整命令行；
  - 业务恢复前需要单独的后切换确认。
- `tools/hold_mysql55_cutover_lock.py`
  - 获取命名锁和 `FLUSH TABLES WITH READ LOCK`；
  - 每 5 秒写锁心跳、源身份、binlog 坐标和阻塞写入计数；
  - 发现锁丢失或新的写入尝试立即阻断验收；
  - 只有显式 ABORT 才主动解锁；正常切换必须先写入“即将停止 5.5 服务”，再停止服务。

因此，最终一致性不再依赖“现在看起来没有业务”这一瞬时判断。

### 3. 数据验收策略

- `tools/build_mysql84_final_data_manifest_config.py`
  - 从在线源/目标事实生成配置，不使用主观写死的表清单；
  - 所有业务基础表做精确 `COUNT(*)`；
  - 所有主键表做主键边界；
  - 所有 DATE/DATETIME/TIMESTAMP 列做边界；
  - 小表和账户、现金、订单、持仓、交易、决策、执行、风控、组合等权威表做全行、有序 SHA-256；
  - 超大行情/分钟历史表做确定性主键窗口 SHA-256；
  - 无主键表明确列入覆盖缺口，不伪称全库密码学等价。
- `tools/mysql55_to_mysql84_data_manifest.py`
  - 分别生成冻结源和静止目标 manifest；
  - 校验配置、catalog、快照、恢复 artifact SHA 和 endpoint；
  - 只有全表精确行数、已配置哈希/边界、零日期风险全部一致时，`risk_based_cutover_checks_passed=true`。

### 4. MySQL 8.4 兼容性和交易系统验收

- `tools/materialize_mysql84_datetime_defaults.py`：清理 8 个遗留零日期默认值；真实零日期数据非零时阻断。
- `tools/materialize_mysql84_check_constraints.py`：先 NOT ENFORCED 审计，再在零违规条件下 ENFORCED。
- `tools/provision_mysql84_migration_account.py`：建立 TLS-only、仅 `probiga.*` 的迁移账号；无全局权限。
- `tools/run_mysql84_trigger_migration_window.py`：短暂开启 trigger creator trust，并在任何退出路径双连接证明恢复为 OFF。
- `tools/run_mysql84_restored_migrations.py`：统一执行并幂等重放 V2、V3、V4；不建立第二套业务账本。
- `tools/mysql84_restored_business_smoke.py`：只读验证三库路由、UUID、端口、TLS、ledger 和 `real_trading_enabled=0`。
- `tools/run_mysql84_final_acceptance.py`：把最后追平、迁移账号、schema 审计、数据 manifest、DATETIME、CHECK、V2/V3/V4 和 smoke 串成一个 checkpoint 化流程。

### 5. 冷数据搬迁、生产服务和回滚

- `tools/transition_mysql84_data_layout.ps1`
  - 要求所有 mysqld 已停止；
  - 旧 `ibdata1` 先复制到 F，校验长度和 SHA-256 后才从 E 释放；
  - 旧 C 盘 datadir、my.ini 和 binlog 一并复制并生成逐文件哈希；
  - 8.4 数据从 F 冷复制到 E，源/目标所有文件做长度和 SHA-256 比较；
  - F 上的已验收 8.4 源目录继续保留；
  - 回滚时识别 E 上唯一的正式 8.4 目录或中断的 `Data.staging-<UUID>`，完整归档并逐文件校验到 F 后，再恢复旧 `ibdata1`；
  - 如果旧 `ibdata1` 从未被释放，则验证布局仍完整并直接恢复旧服务，不执行多余搬迁。
- `tools/provision_mysql84_runtime.py`
  - 建立 `caching_sha2_password`、REQUIRE SSL 的运行账号；
  - 只授予三个业务 schema 权限，无全局权限；
  - 生成受 ACL 保护的 3306 client option 和 staged `.env`；
  - 不提前覆盖当前 `.env`。
- `tools/cutover_mysql84_production.py`
  - 校验正式配置不含 F 盘、datadir UUID、最终验收证据和 staged env 哈希；
  - 严格按 Windows 服务参数顺序注册 `ProBigA-MySQL84`；
  - 保留旧 `MySQL` 服务，仅禁用，不删除；
  - 先备份 `.env`，再原子替换；
  - 启动后通过运行账号验证 8.4.11、3306、UUID、TLS、ROW binlog、sync_binlog=1、flush-at-commit=1 和三个业务库；
  - 失败时停止/禁用新服务并恢复旧 `.env`；物理回滚由上层完整编排并单独出具证据。
- `tools/complete_mysql84_upgrade.ps1`
  - 将上述步骤按固定顺序一次执行；
  - 每阶段写状态和证据；
  - 停旧库后的任一步失败均自动执行数据布局和服务回退；回退也失败时保持所有 mysqld 停止并明确标记 `rollback-halted`，绝不继续带病切换；
  - 完成后业务 writer 与计划任务仍保持冻结，避免数据库刚切换就自动恢复交易写入；
  - 数据库升级不会改变实盘交易激活开关。

## 四、固定执行顺序

```text
seed dump/restore 完成
  -> 冻结业务进程和计划任务
  -> 5.5 全局读锁守护
  -> 构建事实化数据比较策略
  -> 最终 binlog 追平
  -> schema + 全表数据验收
  -> DATETIME/CHECK
  -> V2/V3/V4 + 幂等重放
  -> 只读业务 smoke
  -> 建立运行账号并生成 staged env
  -> 停止 33090
  -> 停止 5.5/3306
  -> 旧库物理副本到 F 并校验
  -> 8.4 冷复制到 E 并校验
  -> 注册并启动 8.4/3306
  -> 原子切换 env
  -> 生产身份/TLS/持久化参数复验
```

业务进程恢复是下一道门，不能和数据库切换混在同一个“成功”判断中。只有数据库证据、应用健康检查、QMT 边界和交易授权状态复核通过后，才能重新启用计划任务和 writer。

## 五、回滚路径

切换后如果 UUID、数据一致性、TLS、关键 SQL 或性能不满足要求：

1. 保持业务 writer 冻结；
2. 停止并禁用 MySQL 8.4 服务；
3. 用 `transition_mysql84_data_layout.ps1 -Mode PrepareRollback` 将 E 上的正式 8.4 或中断 staging 完整归档到 F；
4. 从 F 的已校验旧库副本恢复 `E:\MySQL Datafiles\ibdata1`；
5. 用 `cutover_mysql84_production.py --mode rollback` 恢复旧 `.env`，启动旧 `MySQL`；
6. 验证版本 5.5.20-log、3306 和旧 datadir；
7. 保留 8.4 故障现场，禁止删除。

8.4 开始接收新业务写入后，不能自动把数据倒灌回 5.5。因此初始观察期必须保持 writer 冻结，确认通过后再恢复业务。

## 六、当前事实状态

截至本文更新时：

- 旧 MySQL 5.5.20-log 仍在线监听 3306；
- source binlog 已启用，server-id=55、STATEMENT、sync_binlog=1；
- 5.5 -> 8.4 statement-binlog CRUD 探针已通过；
- data-11 全量恢复、V2/V3/V4、schema audit 和只读 smoke 已通过，用于恢复可行性证明；
- 最新在线一致性快照已封存成功：97,582,659,845 字节，包含 `mysql-bin.000001:6592943` 坐标、结束标记和 SHA-256；
- 新 data-12 已用独立 UUID 初始化，当前流水线正在执行 sanitizer，随后会自动启动 33090 全量恢复；
- 正式 TLS 文件已在 D 盘，正式 `my.ini` 不引用 F 盘；
- 当前 `.env` 尚未切换；
- 生产 3306 尚未切换；
- 最新升级/回退相关回归已通过；新增的停库后自动回退路径也已通过 PowerShell AST 和专项测试。

在 data-12 恢复、最终锁定验收、E 盘冷复制、3306 服务切换和生产复验全部通过前，不得表述为“升级完成”。
