# 新薄采集入口：生产切换清单

## 当前结论

本分支的新 `acquisition/` 入口尚未在生产启用。本地假接口、SQLite 和协议测试通过；生产主库/历史库 schema 已完成只读核对并形成精确迁移，但迁移尚未应用、完整 QMT 内模型的真实 `transactioncount1d` 探针尚未执行，也不能宣称无人值守已验收。

这里列的是本次切换必须处理的具体边界，不新增调度平台、通用迁移框架或交易授权。新采集继续使用现有 MySQL；不复用旧桥接和旧采集写入。既有 `capital_flow_batch_fast` 任务身份仅作为 readiness 的只读验证入口保留，不再拉数或写表。当前联合发布明确不切换 ETF：`etf_forward_daily` 继续作为唯一 ETF 日线写入及前向验证链路，新入口的默认配置、CLI 和计划任务均拒绝 `etf_daily`。原有策略、PIT 和交易权限不能因采集状态为 `complete` 而放行。

## 1. 先确认实际业务表兼容

- [x] 已只读核对生产主库/历史库目标表：股票/指数日线与分钟、当前行情、资金流和公告等表可复用；两库均缺少 `acquisition_partition_state`。
- [ ] `--apply` 只创建 `acquisition_partition_state`，不会修业务表、删除旧索引、补造旧证明字段或启用采集。业务表不兼容时仍应失败。
- [x] 已逐项确定实际表的业务键、可用索引及本轮阻断字段；大型旧表继续复用现有键，不把全库查重做成日常门禁。
- [ ] 股票/指数/ETF 的代码、日期或时间、周期及复权模式必须能表达新产品身份。较窄的旧唯一键不能把不同周期或复权产品相互覆盖。
- [ ] 应用已生成的精确兼容迁移：`sm_etf_kline.validation_source/validation_status/validation_checked_at/quality_status/permission_status` 改为可空；`si_etf_code.validation_source/sync_status` 改为可空。新采集不填造权限、质量或验证结论。
- [ ] `si_stock_finance` 增加可空 `source_update_date VARCHAR(64)`；`st_a_list_daily` 增加 `trade_id VARCHAR(32)`；`st_a_list_info` 增加 `trade_id VARCHAR(32)` 和 `report_side VARCHAR(4)`。不新增用户、角色、授权表或触发器。
- [ ] 财务除 `si_stock_finance` 外，还需核对 `st_pit_finance_revision`。如果实际表要求旧父批次/覆盖外键或不允许未知发布时间为空，应先明确兼容迁移，不能把采集时间冒充公告发布时间。
- [ ] 龙虎榜真实接口样本已用于候选字段适配；迁移后仍须在目标库确认 `trade_id` 和 `report_side` 回读，不得临时编排名或丢弃上榜原因/买卖身份。
- [ ] 在实际迁移后的目标库完成一次写入、独立回读、重复写入和事务失败回滚；只读 schema 核对不能代替该验收。

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
| 日资金流 | 旧 `capital_flow`、手工补数及原 `capital_flow_batch_fast` 写入脚本 | 旧 `tools/crawl_realtime_batch.py --only flow`、`tools/backfill_capital_flow.py`、`tools/sync_capital_flow_push2delay.py` 必须退出写入；`capital_flow_batch_fast` 任务身份保留，但切为 `tools/verify_direct_capital_flow_daily.py` 只读验证 |
| Linux 跨产品补数 | `linux_recent_data_gap_repair` | `tools/qmt_host_ownership_contract.py:197` → `tools/repair_linux_recent_data_gaps.py`；其中直接修改日资金流表，不能只停常规采集而遗漏补数写入 |
| 龙虎榜日榜/明细 | `alist_daily`、`alist_info` | `tools/qmt_host_ownership_contract.py:219`、`:236` → `tools/sync_eastmoney_alist_exact.py` |
| 财务 | `stock_finance`、`stock_finance_historical_repair` | `tools/ensure_quality_gate.py:352`、`:373` → `biz/stock_finance/sync_finance.py`；后者仓库默认禁用，仍需查实际状态 |
| 公告 | `notice_eastmoney`、`notice_eastmoney_historical_repair` | `tools/ensure_quality_gate.py:331`、`:308` → `biz/notice/sync_notice_em.py` |
| ETF 日线 | `etf_forward_daily` | 本轮保留并保持为唯一写入者；`tools/qmt_host_ownership_contract.py:355` → `tools/run_etf_forward_daily.py` 同时承担冻结策略前向记录，当前 direct `etf_daily` 不得注册或运行 |

- [ ] 发布后确认 ensure 不会恢复旧资金流 writer：旧 `capital_flow` 保持退役；`capital_flow_batch_fast` 仍启用但脚本只读，不调用网络、不写业务表。
- [ ] 不停用 `etf_forward_daily`，并确认安装配置不含 `etf_daily`。只有在独立验证能产出与现有正式回测相同口径的 ETF 质量/权限/前向证据、读取端已切换且单写入者交接可回退后，才另行设计 ETF cutover。
- [ ] 跨产品补数入口只能关闭其已迁移写入范围，或明确暂停整项后保留未迁移产品的替代路径。不能为切换本清单产品而一刀切停止所有旧 QMT/公告/行业/涨停/分钟资金流任务。
- [ ] 只读质量观察任务不是写入者，不必因切换而删除；但其旧任务状态口径不能继续冒充新入口的采集状态。

## 3. 页面与正式消费尚需接入

候选分支已接入最小状态/来源读取，但尚未部署。仅把新数据写入表，不会自动让策略证明变为成功；readiness 对资金流只允许读取新进度和业务分区的只读验证结果。

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

- [ ] 将新 QMT 模型装入用户已登录的完整国金 QMT，在内模型中对一只股票、一个已闭市交易日执行 `get_market_data_ex(..., period='transactioncount1d', count=-1)`。必须实际返回 `bidMostAmount/offMostAmount`、`bidBigAmount/offBigAmount`、`bidMediumAmount/offMediumAmount`、`bidSmallAmount/offSmallAmount` 及正确日期；外部 MiniQMT 失败不代替这个探针。
- [ ] 用探针确认四档净流入均为同档 `bid - off`，且 `main_net_inflow = max_order_net_inflow + large_order_net_inflow`。八个原始金额有限且非负即可，派生净流入允许为负；全零结果不得冒充有效资金流。
- [ ] 资金流仅走完整 QMT `transactioncount1d`，无 HTTP fallback。逐证券结果先 staged 到现有进度行；预期证券全部齐全后才整日原子替换并独立回读。部分成功或中断不得修改旧完整业务分区。
- [ ] `tools/register_direct_acquisition.ps1` 仅注册新入口的每日/实时任务，不会启动或登录 QMT，也不会停止上述旧写入者。重启后的 QMT 模型自动加载及任务正常恢复尚未完成实机验收。
- [ ] 原生长历史请求只在允许窗口执行；线程超时不能杀掉已经卡住的原生调用。任何自动恢复必须先确认受控 QMT 实例的进程身份和非实盘边界，未验证前不能承诺任意卡死可自动恢复，更不能停止用户其他交易进程。
- [ ] 已核对证券目录、上市/退市日期、市场交易日历、未来交易日覆盖、分钟时段与量额单位配置。不得以工作日推算交易日或把港股时段直接套用 A 股分钟网格。
- [ ] HTTP 事件接口的真正空集合与网络/解析失败仍分开，分页完整才确认完成；HTTP 接口不参与资金流兜底。
- [ ] 财务扫描目标日不是公告知识截止时间：合法的跨日补采必须保留真实公告/更新时间及实际 `known_at`，不能因修订晚于扫描目标日而永久拒绝，也不能反向回填知识时间。本项应随 normalizer 的定向修复一起验收。
- [ ] HTTP JSON 数字 token 已改用 `parse_float=str`，避免先经二进制 float 丢精度；离线真实 JSON 回归已覆盖。此项仍不代替目标 MySQL DECIMAL 精度回读。
- [ ] 巨潮适配器目前明确不可用；没有正式来源证据的财务（包括既有个别证券缺口）仍应失败，不以空值、旧版本或猜测披露日期代替。
- [ ] 重复领取、采集后崩溃、入库后进度更新前崩溃和超时迟到结果通过本地协议测试后，仍需在隔离 MySQL/QMT 环境验证；按任务启用后再观察实际跨交易日推进，不能提前声称已连续稳定运行。

## 5. 最小切换顺序与回退

1. 固定本次启用的数据集与配置（不含 `etf_daily`），应用上述精确迁移并重跑 schema check；未完成的数据集继续保持未启用。
2. 在完整 QMT 内模型完成资金流单股单日只读探针；字段、日期或数据包能力任一不成立时，不启用该数据集，也不切 HTTP 兜底。
3. 按数据集停止重叠旧写入入口并等待结束；限制 ensure 重建，保留未迁移产品原运行方式。资金流 readiness 任务只读验证，不是第二个 writer。
4. 在已批准的真实环境验证一小批来源到业务表的完整链路：正确目标、准确数值、独立回读、重复执行不增重、整日原子替换及失败不破坏旧分区。
5. 仅从合并后的 `main` 安装新入口、配置及计划任务，核对进度读取与实际写入一致。开启自动运行并记录跨日恢复结果；不把首次手工成功等同于日常自动运行已验收。
6. 如果需要回退，先停新入口并等待写入结束，再根据兼容迁移决定是否能恢复选定旧写入者；恢复原任务前复核表结构/来源语义，不让新旧写入者同时工作。

以上涉及生产任务、业务迁移、模型安装与读取端的动作，本文件均为待执行清单，不代表已经执行或已获生产验收。
