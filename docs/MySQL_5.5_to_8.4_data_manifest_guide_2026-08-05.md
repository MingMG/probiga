# MySQL 5.5 → 8.4 数据清单与恢复比对

执行工具：`tools/mysql55_to_mysql84_data_manifest.py`

## 结论边界

这个工具只读数据库，并按配置生成两份密封 JSON：先生成源库 manifest，再生成恢复后目标库 manifest，最后离线 compare。

- 所有业务表可做精确 `COUNT(*)`，但这仍会扫描 InnoDB 表；约 4 亿行时应在最终停写窗口内以 1–2 个 worker 运行。
- PK/日期边界、数值聚合与 `COUNT(*)` 会合并成每张表的一条聚合 SQL，不把结果集装入内存。
- `deterministic_pk_windows_sha256` 通过整数主键等距锚点做有界索引窗口读取，适合超大表的前后恢复抽样。
- `deterministic_sample_sha256` 用 CRC32 选择样本、再由客户端做 SHA-256；CRC 条件通常仍需扫描索引，因此超大整数主键表优先使用 PK window。
- `full_ordered_sha256` 会按完整主键顺序流式读取配置列；它是可选的，适合小表或极关键表，不建议默认覆盖 140 GiB 全库。
- CRC、窗口抽样或抽样 SHA-256 都不能证明未读取的数据一致。即使配置了全表逻辑 SHA-256，它证明的也是指定快照下由连接器读取的逻辑值，不是物理备份文件校验。

## 强制门禁

捕获前会 fail-closed 校验：

1. 精确版本、端口、server UUID；MySQL 5.5 没有 `server_uuid` 时必须锁定 legacy identity SHA-256。
2. 目标连接必须提供 CA、协商出非空 TLS cipher，并精确匹配目标 UUID。
3. 配置中的 schema 必须全部存在；目标 table/column/order/PK 清单必须与源 manifest 完全一致。
4. 凭据只能从受保护的 MySQL `[client]` option file 读取，不进入 manifest 或控制台输出。
5. 最终 cutover 源 manifest 必须声明 DDL 与写入均已冻结，提供带时区的冻结时间，以及实际恢复 dump 的 SHA-256。
6. 最终 cutover 配置必须对全部 base table 做精确 COUNT，并保留已发现的 8 个零日期旧默认风险列。
7. 目标 capture 必须再次输入实际恢复 artifact SHA-256，并声明目标库静止。

## 快照与中断恢复

- `initial_consistent_snapshot`：严格使用一个连接、一个 repeatable-read consistent snapshot；用于演练，不具备最终切换资格。它不能和外部 `mysqldump` 共享同一个 MySQL 5.5 snapshot token，因此不能冒充最终恢复证明。
- `cutover_writes_frozen`：只在应用写入和 DDL 持续冻结时使用。此时可以有限并发，也可以通过 `--checkpoint` / `--resume` 续跑。
- checkpoint 每完成一张表就原子写入；中断时正在扫描的那张表会重跑，已经完整完成的表会跳过。恢复期间必须继续保持源写入冻结或目标静止。

## 1. 轻量身份探针

身份探针只读取系统变量，不扫描业务表。

```powershell
.venv\Scripts\python.exe tools\mysql55_to_mysql84_data_manifest.py probe-identity `
  --option-file D:\MySQL84\config\source-client.ini

.venv\Scripts\python.exe tools\mysql55_to_mysql84_data_manifest.py probe-identity `
  --option-file D:\MySQL84\config\target-client.ini `
  --ssl-ca D:\MySQL84\certs\ca.pem `
  --require-tls
```

把探针返回的精确 `version`、`port`、`server_uuid` 或 `legacy_identity_sha256` 写入配置。正式目标实例初始化后会有自己的 UUID，不能沿用 33084 测试实例 UUID。

下方骨架使用恢复阶段端口 `33085`；最终服务切到 `3306` 后若再次捕获 manifest，
必须生成一份端口为 `3306` 且 UUID 不变的新配置，不能把端口门禁改成宽松匹配。

## 2. 推荐配置骨架

以下配置优先采用“全表精确行数 + 全表 PK 边界 + 大表日期/数值聚合 + 大表 PK 窗口抽样”。尖括号内容必须替换成身份探针的真实值。

```json
{
  "format_version": 1,
  "schemas": ["biga", "probiga", "probiga_qmt_history"],
  "endpoints": {
    "source": {
      "version": "5.5.20",
      "port": 3306,
      "server_uuid": null,
      "legacy_identity_sha256": "<64位源库legacy identity SHA-256>",
      "require_tls": false
    },
    "target": {
      "version": "8.4.11",
      "port": 33085,
      "server_uuid": "<正式目标库UUID>",
      "legacy_identity_sha256": null,
      "require_tls": true
    }
  },
  "execution": {"max_workers": 2},
  "counts": {"mode": "all", "tables": []},
  "boundaries": {
    "primary_key_mode": "all",
    "primary_key_tables": [],
    "date_columns": {
      "probiga.sm_stock_minute": ["trade_date", "trade_time"],
      "probiga.sm_stock_kline": ["trade_date", "trade_time"],
      "probiga.qmt_local_stock_minute": ["trade_date", "trade_time"],
      "probiga.qmt_local_stock_kline": ["trade_date", "trade_time"],
      "probiga_qmt_history.qmt_local_stock_minute": ["trade_date", "trade_time"],
      "probiga_qmt_history.qmt_local_stock_kline": ["trade_date", "trade_time"]
    }
  },
  "aggregates": {
    "probiga.sm_stock_minute": [
      {"column": "volume", "functions": ["sum", "count_nonnull"], "absolute_tolerance": "0"},
      {"column": "amount", "functions": ["sum", "count_nonnull"], "absolute_tolerance": "0"}
    ],
    "probiga.sm_stock_kline": [
      {"column": "volume", "functions": ["sum", "count_nonnull"], "absolute_tolerance": "0"},
      {"column": "amount", "functions": ["sum", "count_nonnull"], "absolute_tolerance": "0"}
    ],
    "probiga_qmt_history.qmt_local_stock_minute": [
      {"column": "volume", "functions": ["sum", "count_nonnull"], "absolute_tolerance": "0"},
      {"column": "amount", "functions": ["sum", "count_nonnull"], "absolute_tolerance": "0"}
    ],
    "probiga_qmt_history.qmt_local_stock_kline": [
      {"column": "volume", "functions": ["sum", "count_nonnull"], "absolute_tolerance": "0"},
      {"column": "amount", "functions": ["sum", "count_nonnull"], "absolute_tolerance": "0"}
    ]
  },
  "hashes": {
    "probiga.sm_stock_minute": {
      "mode": "deterministic_pk_windows_sha256",
      "key_columns": ["id"],
      "columns": "*",
      "chunk_rows": 1000,
      "window_count": 256,
      "window_rows": 256
    },
    "probiga.sm_stock_kline": {
      "mode": "deterministic_pk_windows_sha256",
      "key_columns": ["id"],
      "columns": "*",
      "chunk_rows": 1000,
      "window_count": 256,
      "window_rows": 256
    },
    "probiga_qmt_history.qmt_local_stock_minute": {
      "mode": "deterministic_pk_windows_sha256",
      "key_columns": ["id"],
      "columns": "*",
      "chunk_rows": 1000,
      "window_count": 256,
      "window_rows": 256
    },
    "probiga_qmt_history.qmt_local_stock_kline": {
      "mode": "deterministic_pk_windows_sha256",
      "key_columns": ["id"],
      "columns": "*",
      "chunk_rows": 1000,
      "window_count": 256,
      "window_rows": 256
    }
  },
  "legacy_zero_date_columns": [
    "probiga.jq_strategy_meta.created_at",
    "probiga.jq_strategy_meta.updated_at",
    "probiga.jq_strategy_picks.created_at",
    "probiga.st_daily_review.etl_sync_at",
    "probiga.st_portfolio_analysis_log.created_at",
    "probiga.st_portfolio_trans_log.created_at",
    "probiga.st_recommended_stocks.created_at",
    "probiga.st_user_portfolio.etl_sync_at"
  ]
}
```

配置会在真正扫描前对表、列、日期类型、数值类型和主键顺序做校验。PK window 仅允许单列整数主键，并要求同时捕获该主键边界。

## 3. 最终源 manifest

顺序必须是：冻结应用写入与 DDL → 完成最终 dump → 计算 dump SHA-256 → 在保持冻结的情况下生成源 manifest。

```powershell
.venv\Scripts\python.exe tools\mysql55_to_mysql84_data_manifest.py capture-source `
  --config D:\MySQL84\config\data-manifest.json `
  --option-file D:\MySQL84\config\source-client.ini `
  --output F:\ProBigA-MySQL-Upgrade-20260805\manifests\source-data.json `
  --checkpoint F:\ProBigA-MySQL-Upgrade-20260805\manifests\source-data.checkpoint.json `
  --workers 2 `
  --snapshot-mode cutover_writes_frozen `
  --snapshot-id probiga-cutover-20260805 `
  --assert-ddl-frozen `
  --assert-writes-frozen `
  --writes-frozen-at 2026-08-05T20:00:00+08:00 `
  --restore-artifact-sha256 <最终dump的SHA-256>
```

中断后，在确认写入从未解冻的前提下，使用完全相同参数并增加 `--resume`。

## 4. 目标 manifest

恢复结束后保持目标库静止。目标运行目录、正式数据目录和配置不能引用移动硬盘；F 盘这里只承载可拔出的 dump、manifest 和 checkpoint。

```powershell
.venv\Scripts\python.exe tools\mysql55_to_mysql84_data_manifest.py capture-target `
  --config D:\MySQL84\config\data-manifest.json `
  --option-file D:\MySQL84\config\target-client.ini `
  --ssl-ca D:\MySQL84\certs\ca.pem `
  --source-manifest F:\ProBigA-MySQL-Upgrade-20260805\manifests\source-data.json `
  --restored-artifact-sha256 <实际恢复dump的SHA-256> `
  --output F:\ProBigA-MySQL-Upgrade-20260805\manifests\target-data.json `
  --checkpoint F:\ProBigA-MySQL-Upgrade-20260805\manifests\target-data.checkpoint.json `
  --workers 2 `
  --assert-target-quiescent
```

## 5. 离线比较

```powershell
.venv\Scripts\python.exe tools\mysql55_to_mysql84_data_manifest.py compare `
  --source-manifest F:\ProBigA-MySQL-Upgrade-20260805\manifests\source-data.json `
  --target-manifest F:\ProBigA-MySQL-Upgrade-20260805\manifests\target-data.json `
  --output F:\ProBigA-MySQL-Upgrade-20260805\manifests\data-compare.json
```

退出码：`0` 表示已配置检查一致；`1` 表示数据/目录/provenance 不一致；`2` 表示配置、身份、TLS、文件或数据库执行失败。

最终切换至少要求 `risk_based_cutover_checks_passed=true`。同时必须阅读 coverage：如果 `full_logical_sha256_coverage=false`，结果是“全表行数 + 关键边界/聚合 + 确定性样本”的风险分层验收，不应写成“密码学证明全库每个值完全相同”。
