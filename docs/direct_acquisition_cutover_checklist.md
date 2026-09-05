# 新薄采集入口：生产切换清单

## 当前结论

本分支的新 `acquisition/` 入口尚未在生产启用。本地假接口、SQLite 和协议测试通过，只说明已覆盖的代码路径成立；不代表实际 MySQL 业务表兼容、QMT 原生接口已验收，或“今日策略”页面已经改读新进度。

这里列的是本次切换必须处理的具体边界，不新增调度平台、通用迁移框架或交易授权。新采集继续使用现有 MySQL；不复用旧桥接、旧调度回执和策略发布证明。原有策略、PIT 和交易权限不能因采集状态为 `complete` 而放行。

## 1. 先确认实际业务表兼容

- [ ] 对明确选中的配置手动运行 `tools/prepare_direct_acquisition_schema.py --config <绝对路径> --check`，保存每个启用 dataset 的 `migration_required`。此处是待执行步骤，本轮没有连接生产数据库。
- [ ] `--apply` 只创建 `acquisition_partition_state`，不会修业务表、删除旧索引、补造旧证明字段或启用采集。业务表不兼容时仍应失败。
- [ ] 逐项确定实际表的业务唯一键、必需非空列、默认值及外键。只对确实不兼容的字段编写具体迁移；不能用虚假的批次 ID、覆盖证明、发布时间或通过标记满足旧约束。
- [ ] 股票/指数/ETF 的代码、日期或时间、周期及复权模式必须能表达新产品身份。较窄的旧唯一键不能把不同周期或复权产品相互覆盖。
- [ ] 财务显示表 `si_stock_finance` 必须有可空 `source_update_date VARCHAR(64)`，保留原生更新文本，防止历史补采覆盖较新显示版本。缺列时 schema 工具会明确报告 `missing_finance_source_update_date` 并给出一次性建议 DDL：``ALTER TABLE `si_stock_finance` ADD COLUMN `source_update_date` VARCHAR(64) NULL;``。工具不执行该 DDL，旧行也不补造版本时间；须经实际迁移后重查。
- [ ] 财务除 `si_stock_finance` 外，还需核对 `st_pit_finance_revision`。如果实际表要求旧父批次/覆盖外键或不允许未知发布时间为空，应先明确兼容迁移，不能把采集时间冒充公告发布时间。
- [ ] 龙虎榜当前新唯一键使用 `trade_id`；明细还区分 `operate_code` 与 `report_side`。仓库旧实现用 `TRADE_ID` 排序，但旧测试样例没有这一字段，不能据此证明真实响应必然含有它。上线前需已有真实原始响应或经授权采样确认其存在及稳定性，再确定具体唯一键迁移；禁止临时编排名或丢弃上榜原因/买卖身份。
- [ ] 使用隔离 MySQL 8 对实际迁移后的表完成一次写入、独立回读、重复写入和事务失败回滚。当前只发现本机 MySQL 5.5，未安装或启动其他服务，尚无 MySQL 8 实库验收。

`--check` 的可输出字段集合是兼容性候选，不保证每个真实响应都提供该字段；来源缺字段时仍由行级校验明确失败。

## 2. 切换时必须排除的旧写入入口

下表来自仓库任务定义，不表示它们目前都在生产启用。实际切换必须记录两端任务 ID、启用状态、计划任务及运行中进程，只停与本次已迁移数据集重叠的写入者，并等待旧写入结束。

| 数据范围 | 旧任务类型或入口 | 仓库依据/执行脚本 |
| --- | --- | --- |
| 股票实时 | `qmt_intraday_realtime` | `tools/qmt_host_ownership_contract.py:38` → `tools/sync_qmt_realtime.py` |
| 其他股票实时写入 | `portfolio_quote_refresh`、`intraday_realtime` | `tools/ensure_quality_gate.py:392`、`:408` → `tools/run_portfolio_quote_refresh.py`、`tools/crawl_realtime_batch.py --only snapshot` |
| 旧原生桥接消费 | 手工或服务启动的 `tools/run_big_qmt_bridge.py` | 与新 QMT 模型不是同一套通道；如仍运行且写同表，必须纳入停写清单 |
| 指数实时/日线/分钟 | `qmt_index_current`、`qmt_index_kline`、`qmt_index_minute` | `tools/qmt_host_ownership_contract.py:72`、`:89`、`:106` → `tools/sync_qmt_index_edge.py` |
| 股票日线/分钟 | `qmt_stock_daily_canonical`、`qmt_stock_minute_canonical` | `tools/qmt_host_ownership_contract.py:123`、`:141` → `tools/sync_qmt_stock_edge.py` |
| QMT 历史补数 | `qmt_canonical_history_gap_repair` 及手工历史任务 | `tools/qmt_host_ownership_contract.py:177` → `tools/repair_qmt_canonical_history_gaps.py`；同时核对 `tools/backfill_guojin_qmt_local_history.py`、`tools/run_guojin_qmt_full_market_history.py`、`tools/sync_qmt_primary.py` 的启动入口 |
| 日资金流 | `capital_flow_batch_fast` 及手工补数 | `tools/ensure_quality_gate.py:542` → `tools/crawl_realtime_batch.py --only flow`；另核对 `tools/backfill_capital_flow.py`、`tools/sync_capital_flow_push2delay.py` |
| Linux 跨产品补数 | `linux_recent_data_gap_repair` | `tools/qmt_host_ownership_contract.py:197` → `tools/repair_linux_recent_data_gaps.py`；其中直接修改日资金流表，不能只停常规采集而遗漏补数写入 |
| 龙虎榜日榜/明细 | `alist_daily`、`alist_info` | `tools/qmt_host_ownership_contract.py:219`、`:236` → `tools/sync_eastmoney_alist_exact.py` |
| 财务 | `stock_finance`、`stock_finance_historical_repair` | `tools/ensure_quality_gate.py:352`、`:373` → `biz/stock_finance/sync_finance.py`；后者仓库默认禁用，仍需查实际状态 |
| 公告 | `notice_eastmoney`、`notice_eastmoney_historical_repair` | `tools/ensure_quality_gate.py:331`、`:308` → `biz/notice/sync_notice_em.py` |
| ETF 日线 | `etf_forward_daily` | `tools/qmt_host_ownership_contract.py:355` → `tools/run_etf_forward_daily.py`；此任务还包含冻结策略前向记录，采集替换不等于该业务能力已迁移 |

- [ ] `tools/ensure_quality_gate.py:747` 的 `upsert_task` 会按定义更新任务字段，包括 `enabled`。必须同步限制已迁移任务的安装/ensure 选择，避免下次发布把手工停掉的旧任务重新启用。
- [ ] 跨产品补数入口只能关闭其已迁移写入范围，或明确暂停整项后保留未迁移产品的替代路径。不能为切换本清单产品而一刀切停止所有旧 QMT/公告/行业/涨停/分钟资金流任务。
- [ ] 只读质量观察任务不是写入者，不必因切换而删除；但其旧任务状态口径不能继续冒充新入口的采集状态。

## 3. 页面与正式消费尚需接入

新入口有自身进度和 CLI `status`，但本次检查时 `server/` 尚未导入新 `acquisition` 模块。仅把新数据写入表，不会自动让原页面和旧任务证明变为成功。

| 现有读取点 | 具体边界 |
| --- | --- |
| `server/static/js/app.js:5262` → `/api/v3/readiness` | “今日策略”仍请求旧 readiness；需明确区分新采集完成度与策略可用性 |
| `server/api/routers/trading_v3.py:1579` | `_load_readiness_snapshot` 调用 `tools/read_v3_readiness.py`，并非新进度表读取 |
| `server/api/routers/trading_v3.py:2306`、`server/common/release_data_readiness_contract.py` | 原调度健康与依赖仍绑定旧任务、build 和回执；不能给新采集伪造旧任务成功记录来通过它们 |
| `tools/data_quality_check.py`、`server/common/scheduler_validation.py` | 原质量/后验检查读取旧任务状态及部分旧批次证明；需明确其继续检查哪些实际业务数据，哪些旧调度判据不再适用于新入口 |
| `server/common/pit_facts.py:3906`、`:4112` | 财务正式 PIT 消费仍加载旧原子批次 seal；新财务行/修订记录不自动等价于该 seal。应适配真实源版本与知识时间，不补造签名或发布时间 |
| `server/common/qmt_daily_market_truth.py` | 原日线来源证明与旧原生 manifest 有关联；新行存在不等于旧证明已生成 |
| `server/api/routers/holding_strategy.py`、`server/api/routers/broad_etf_flow.py` | 需验证新行的实际字段、来源/时间、复权和质量语义可被现有持仓及 ETF 读取使用，保留策略截止时间和交易限制 |

- [ ] 页面新增/改接最小的采集状态读取：目标日、应有分区、完成/失败/待运行和最后错误。不要把“未采到”显示为 0 条业务结果，也不要把“采集完成”显示为允许交易。
- [ ] 正式消费兼容未完成前，允许清楚呈现“采集已完成，策略尚不可用”，不能把两者合成一个假绿灯。

## 4. 实际运行仍待验收的项目

- [ ] 新 QMT 模型安装、当前用户登录后的模型加载、代码目录/状态目录权限与实际原生 API 返回字段已确认。Windows 普通 Python 计划任务不能凭空创建 `ContextInfo`。
- [ ] `tools/register_direct_acquisition.ps1` 仅注册新入口的每日/实时任务，不会启动或登录 QMT，也不会停止上述旧写入者。重启后的 QMT 模型自动加载及任务正常恢复尚未完成实机验收。
- [ ] 原生长历史请求只在允许窗口执行；线程超时不能杀掉已经卡住的原生调用。任何自动恢复必须先确认受控 QMT 实例的进程身份和非实盘边界，未验证前不能承诺任意卡死可自动恢复，更不能停止用户其他交易进程。
- [ ] 已核对证券目录、上市/退市日期、市场交易日历、未来交易日覆盖、分钟时段与量额单位配置。不得以工作日推算交易日或把港股时段直接套用 A 股分钟网格。
- [ ] HTTP 真正的空事件集与网络/解析失败仍分开；分页完整才确认完成，历史资金流不能被今天的快照补成昨天的数据。
- [ ] 财务扫描目标日不是公告知识截止时间：合法的跨日补采必须保留真实公告/更新时间及实际 `known_at`，不能因修订晚于扫描目标日而永久拒绝，也不能反向回填知识时间。本项应随 normalizer 的定向修复一起验收。
- [ ] HTTP JSON 数字 token 已改用 `parse_float=str`，避免先经二进制 float 丢精度；离线真实 JSON 回归已覆盖。此项仍不代替目标 MySQL DECIMAL 精度回读。
- [ ] 巨潮适配器目前明确不可用；没有正式来源证据的财务（包括既有个别证券缺口）仍应失败，不以空值、旧版本或猜测披露日期代替。
- [ ] 重复领取、采集后崩溃、入库后进度更新前崩溃和超时迟到结果通过本地协议测试后，仍需在隔离 MySQL/QMT 环境验证；按任务启用后再观察实际跨交易日推进，不能提前声称已连续稳定运行。

## 5. 最小切换顺序与回退

1. 固定本次启用的数据集与配置，完成上述实际表兼容及正式读取边界确认；保存具体迁移和旧任务状态，未完成的数据集继续保持未启用。
2. 按数据集停止重叠旧写入入口并等待结束；限制 ensure 重建，保留未迁移产品原运行方式。
3. 在已批准的真实环境验证一小批来源到业务表的完整链路：正确目标、完整分页、准确数值、独立回读、重复执行不增重、失败不假成功。
4. 仅从合并后的 `main` 安装新入口、配置及计划任务，核对进度读取与实际写入一致。开启自动运行并记录跨日恢复结果；不把首次手工成功等同于日常自动运行已验收。
5. 如果需要回退，先停新入口并等待写入结束，再根据兼容迁移决定是否能恢复选定旧写入者；恢复原任务前复核表结构/来源语义，不让新旧写入者同时工作。

以上涉及生产任务、业务迁移、模型安装与读取端的动作，本文件均为待执行清单，不代表已经执行或已获生产验收。
