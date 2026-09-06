# 新薄采集入口：生产切换清单

## 当前结论

本分支的新 `acquisition/` 入口尚未在生产启用。本地假接口、SQLite 和协议测试通过，进度表迁移已通过受控发布应用。完整 QMT 的真实 `transactioncount1d` 探针持续为空，**按用户 2026-09-06 最新决定，日资金流不再从国金 QMT 获取，保留现有东方财富采集**，不等待 QMT 资金流恢复。其他 QMT 产品及 Windows 无人值守恢复单独验收。

### 资金流来源决定与证据（2026-09-06）

- 当前完整国金 QMT 内模型对 `000001.SZ` / `2026-09-04` 经依赖修复、下载、订阅后仍返回 0 行；同股同日日线正常。只确认当前客户端/账户实测不可用，不推断所有 QMT 都不支持。
- 生产 Linux 对 `https://push2delay.eastmoney.com/api/qt/clist/get` 使用正常 TLS 校验的只读请求成功：HTTP 200、rc=0；5 条样本均有主力/超大/大/中/小净流入，且主力等于超大单加大单。接口返回 total=5654 只是来源报告的总量，不等于本轮逐股验收 5654 条。
- 生产日表 `2026-09-04` 有 5207 行、5207 代码，五档金额无空值、无重复业务键，与该日实际成交沪深股票集合精确一致，来源全部为 `push2hist`；9 月 3 日同样为 5209/5209。9 月 1—2 日来源为 `east_push2delay`。9 月 5 日 20:48:10 的补跑回执为 `verified_existing_exact`、PASS、5207/5207 并带分区指纹，证明完整分区被回读，不是本次新下载证据。
- 历史端点 `push2his` 本轮在本机三个股票、生产一个股票的只读请求均连接失败。历史表已有正确数据可以验证复用，但不能据此承诺当前漏日补数可用；缺口继续如实报告。
- 保留 `capital_flow` / `capital_flow_batch_fast` 的 `crawl_realtime_batch.py --only flow`，不启新 writer，不切成 QMT direct verifier。配置和 CLI 禁用候选 `capital_flow_daily`，数据源页归类改为东方财富；消费者与 readiness 继续使用原批量回执及精确分区回读，不新增服务、表或来源框架。
- 资金流请求本身不使用 QMT；完整性分母仍来自既有日线成交集合，当前上游日线使用 QMT。自选股盘中资金异动仍使用原盘中链路，日表完整不等于盘中提醒时效已验收。

本次来源调整的定向回归：配置/CLI/恢复/字段/页面来源/TLS 共 245 passed、1 skipped（主机不允许创建测试符号链接）；任务配置及 readiness 共 133 passed；资金流消费、盘中数据及自选股提示共 56 passed。分组有交叉，不相加成总数。另 Windows 已激活版本重启及发布回归 114 passed。测试均不代替生产部署或跨交易日验收。

2026-09-06 实机验收记录（时间均为本机北京时间）：

- 13:01 的单股单日现有缓存查询失败后，用户明确授权下载 `000001.SZ` / `2026-09-04` 的资金流缓存。13:35:03 已在独立内模型执行该有界请求，无下单、无业务库写入。`XtClient_datasource_20260906.log:24050-24054` 报告 `metaId=1808` 收到 252 条、覆盖至目标日并完成下载。但随后核对 `datadir/SZ/86400/000001_1808.DAT` 实际为 0 字节，14:28 复查仍为 0；该日志不能证明有效缓存已写入，也不能作为目标日验收通过的证据。
- 13:35:03 查询与 13:38:30 延迟复查中，`ContextInfo.get_market_data_ex` 均因缺少 `pandas` 报错；绕过 pandas 的 `get_market_data_ex_ori` 均返回 `null`。同一股票、同一日期的 `1d` 对照成功，原始日期为 `20260904`、收盘价为 `11.89`，因此不是所有行情接口都不可用，也不能把下载成功算成资金流读取成功。公式证据位于 `XtClient_FormulaOutput_20260906.log` 对应时间的 `PROBIGA_FLOW_PROBE` 记录。
- 已确认客户端为 QMT `2.1.19.0`，内置 Python `3.6.8` / 64 位。[迅投官方股票数据说明](https://dict.thinktrader.net/dictionary/stock.html) 将 `transactioncount1d` 列为 VIP 数据；本机接口文档的周期列表未列该项。当前日志没有明确的无权限或不支持错误，尚不能区分本版本 reader 支持和账户数据权限问题，不能武断认定无权。
- 已从官方 PyPI 安装与 QMT ABI 匹配的 `pandas 0.25.3`、`numpy 1.17.5` 及固定依赖到 QMT 自身 `bin.x64/Lib/site-packages`，校验官方 SHA256 与 wheel RECORD。随后用 python.org 官方 Python 3.6.8 x64 embeddable 包补齐该客户端缺失的标准库依赖，仅向原搜索路径中的 `Lib`、`DLLs` 新增 123 个不存在的文件，未覆盖 vendor 文件、Python DLL、原标准库 ZIP 或全局环境。安装清单及按哈希校验的精确回滚脚本位于本机项目 `.runtime/qmt-py36-site-repair-20260906/`。
- 14:25 与 14:28 已在仍运行的 QMT PID 60028 内分别完成 `init`、`handlebar` 阶段复测：numpy/pandas 导入和 DataFrame 运算成功，运行环境缺依赖已实际修复。但八字段/全部字段、十四位/八位日期的资金流 `get_market_data_ex` 仍返回空 DataFrame，`get_market_data_ex_ori` 仍为 `null`；日线对照仍正常。旧 `get_market_data` 对照也为空，且运行阶段出现 `transactioncount1d:invalid getMarketData function argument!`。该错误只证明旧接口这次调用被拒绝，不能据此断言新接口无权限。
- 三条实际数据库路由均指向同一物理 `probiga` 库。首批 `stock_daily`、`capital_flow_daily` 两张业务表及唯一键兼容，仅缺 `acquisition_partition_state`。已尝试专用工具创建进度表，但当前应用账号没有 `CREATE` 权限，复查确认表仍不存在，业务表未修改；不得改成给应用账号扩权。当前已部署版本的受控发布流程未包含该表的迁移；本轮补丁将共享 `STATE` 表定义接入既有受控 schema 工具的检查、迁移和应用账号回读阶段，仅支持既有 `probiga` 目标，不新增迁移入口或调整任务启用。该迁移随后已随 `c7926b3` 受控发布在生产应用，应用账号回读确认进度表存在、14 列、主键及 due 索引正确，当前 0 行；没有扩应用账号权限。

- 15:28:13 已在实际 QMT `after_init` 阶段按官方顺序完成有界下载、`subscribe_quote`、`get_market_data_ex`：订阅返回有效编号 `11`，但 `subscribe=True/False` 两次查询均为 0 行、0 列，读取时回调计数为 0。该结果排除了仅在 `init` 调用和缺少显式订阅这两个解释，不能证明账户无权限或客户端版本不支持。按用户最新决定终止该资金流路径排查；独立探针已停止，不启用新资金流 writer。
- 本轮实际 Windows PowerShell 5 `-File` 检查发现两个部署脚本缺陷：继承 PS7 模块路径会令 PS5 的 `Get-Acl` 失效；函数中的本地/脚本 `$LASTEXITCODE` 遮蔽原生命令更新的全局退出码，令真实 `READY` 发布许可误判失败。修复仅固定当前 `$PSHOME` 的 Security 模块并按同一全局作用域读写退出码；不改 ACL、grant 或激活条件。新增真实 PS5 进程回归覆盖成功、待授权和命令启动失败。

- 后续重载排查中，显式设置与原调度器一致的 `PROBIGA_DEPLOYMENT_MODE=production`、`PROBIGA_BUILD_COMMIT_SHA=c7926b3...` 后，原 Python `--check-activation` 实测 `READY`、`activation_granted=true`，原重载器 `-PreflightOnly` 也为 `READY`。未设置构建变量的独立诊断曾因封印未绑定当前构建失败；不能将该环境缺项归为授权失效。
- 原重载器全量运行停在 `QMT_USER_ACTION_REQUIRED`：无法唯一识别模型研究策略面板，回执明确 `old_model_was_stopped=false`、`new_model_was_started=false`，不算重载成功。现有唯一桥接脚本与发布清单已复制并校验到 `.runtime/qmt-manual-reload-c7926b3-20260906/`；原模型仍是 `b507dd13...`。为避免重载并发，尝试临时暂停现有 updater，但 Windows 返回 `E_ACCESSDENIED`，复查 `Enabled=true`；没有修改任务身份、动作或权限。
- `e618c95`、`8b3a441` 已合并 main，尚未部署。还发现旧 scheduler wrapper 强制已部署版本等于最新 `origin/main`，导致 main 前移后旧生产版本无法重启，而下一次发布又要求其真实运行身份。后续修复将启动条件改为“可信 main 祖先、当前有效发布许可、受保护控制记录仍选择该精确版本”；新目标已接管时旧版本仍被拒绝。不能通过退回远端 main、伪造运行身份或复制旧 receipt 解锁发布。

完整 QMT 已登录，原桥接模型心跳持续更新；独立探针已停止，原采集模型未替换。首批必要进度表迁移已验收，新入口未启用。QMT 资金流首分区和 writer 交接已取消，现有东方财富链路继续运行。Linux 与 Windows 生产 checkout 均已到 `c7926b3`，但加载中的 QMT 模型仍是旧版本，Windows edge scheduler 仍停止，不能把代码发布当作运行激活完成。

本轮 QMT 探针校验已复用入库规范化逻辑，拒绝错日、未来时间、全零/负数原始金额和重复日记录；候选实现保留但不启用。生产主库、历史库、分钟库实际共用同一物理 `probiga` 库，进度表已迁移；财务/龙虎榜必要字段仍未迁移，对应新数据集保持未启用。现有受控重载依赖发布 activation grant；不能把“磁盘预装了新模型”算作“新模型已加载”。其他迁移数据集首次完整分区与旧 writer 交接仍待真实验收。

这里列的是本次切换必须处理的具体边界，不新增调度平台、通用迁移框架或交易授权。新采集继续使用现有 MySQL。模型和 schema 发布与数据集 writer 交接分开：资金流保留现有东方财富 writer，不做 QMT 交接。当前联合发布也不切换 ETF：`etf_forward_daily` 继续作为唯一 ETF 日线写入及前向验证链路，新入口的默认配置、CLI 和计划任务均拒绝 `etf_daily`、`capital_flow_daily`。原有策略、PIT 和交易权限不能因采集状态为 `complete` 而放行。

## 1. 先确认实际业务表兼容

- [x] 已核对生产主库/历史库实际共用的物理目标：首批股票日线、日资金流业务表兼容；此前缺少的 `acquisition_partition_state` 已经受控 schema 发布创建并回读。
- [ ] `--apply` 只创建 `acquisition_partition_state`，不会修业务表、删除旧索引、补造旧证明字段或启用采集。业务表不兼容时仍应失败。
- [x] 已逐项确定实际表的业务键、可用索引及本轮阻断字段；大型旧表继续复用现有键，不把全库查重做成日常门禁。
- [ ] 股票/指数/ETF 的代码、日期或时间、周期及复权模式必须能表达新产品身份。较窄的旧唯一键不能把不同周期或复权产品相互覆盖。
- ETF 本轮不迁移、不切换：早期检查列出的 ETF 可空字段调整不属于当前必要动作，保留既有表结构及 `etf_forward_daily`。
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
| 日资金流（本轮保留） | `capital_flow`、`capital_flow_batch_fast` | 两任务继续调用 `tools/crawl_realtime_batch.py --only flow`；不启 direct QMT writer 或其他手工补数脚本，不切 `verify_direct_capital_flow_daily.py` |
| Linux 跨产品补数 | `linux_recent_data_gap_repair` | `tools/qmt_host_ownership_contract.py:197` → `tools/repair_linux_recent_data_gaps.py`；其中直接修改日资金流表，不能只停常规采集而遗漏补数写入 |
| 龙虎榜日榜/明细 | `alist_daily`、`alist_info` | `tools/qmt_host_ownership_contract.py:219`、`:236` → `tools/sync_eastmoney_alist_exact.py` |
| 财务 | `stock_finance`、`stock_finance_historical_repair` | `tools/ensure_quality_gate.py:352`、`:373` → `biz/stock_finance/sync_finance.py`；后者仓库默认禁用，仍需查实际状态 |
| 公告 | `notice_eastmoney`、`notice_eastmoney_historical_repair` | `tools/ensure_quality_gate.py:331`、`:308` → `biz/notice/sync_notice_em.py` |
| ETF 日线 | `etf_forward_daily` | 本轮保留并保持为唯一写入者；`tools/qmt_host_ownership_contract.py:355` → `tools/run_etf_forward_daily.py` 同时承担冻结策略前向记录，当前 direct `etf_daily` 不得注册或运行 |

- [ ] 发布后确认原东方财富资金流 writer 继续保留，未被转成 QMT verifier；本轮不新增资金流写入入口，既有命名锁与精确回读不变。
- [ ] 不停用 `etf_forward_daily`，并确认安装配置不含 `etf_daily`。只有在独立验证能产出与现有正式回测相同口径的 ETF 质量/权限/前向证据、读取端已切换且单写入者交接可回退后，才另行设计 ETF cutover。
- [ ] 跨产品补数入口只能关闭其已迁移写入范围，或明确暂停整项后保留未迁移产品的替代路径。不能为切换本清单产品而一刀切停止所有旧 QMT/公告/行业/涨停/分钟资金流任务。
- [ ] 只读质量观察任务不是写入者，不必因切换而删除；但其旧任务状态口径不能继续冒充新入口的采集状态。

## 3. 页面与正式消费尚需接入

最小状态/来源读取已随 `c7926b3` 部署；新采集入口未启用，页面不得据此显示采集成功。资金流继续展示东方财富任务的真实状态，readiness 沿用既有批量回执和业务分区独立回读，不要求未启用的 QMT 进度记录。仅把数据写入表，不会自动让策略证明变为成功。

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

- [x] 完整 QMT 单股单日资金流只读探针已执行，结果为空；该来源未通过，按用户决定不启用。
- [x] 东方财富批量接口已实测响应，生产日资金流分区的证券键、字段、来源和完整性已只读核验；不将复用已有分区说成新采成功。
- [ ] 当前历史补数接口连接失败，漏日保持明确缺失；未来替代历史源必须先验证日期、金额口径和覆盖范围，不能拿当前快照回填。
- [ ] `tools/register_direct_acquisition.ps1` 仅注册新入口的每日/实时任务，不会启动或登录 QMT，也不会停止上述旧写入者。重启后的 QMT 模型自动加载及任务正常恢复尚未完成实机验收。
- [ ] 原生长历史请求只在允许窗口执行；线程超时不能杀掉已经卡住的原生调用。任何自动恢复必须先确认受控 QMT 实例的进程身份和非实盘边界，未验证前不能承诺任意卡死可自动恢复，更不能停止用户其他交易进程。
- [ ] 已核对证券目录、上市/退市日期、市场交易日历、未来交易日覆盖、分钟时段与量额单位配置。不得以工作日推算交易日或把港股时段直接套用 A 股分钟网格。
- [ ] HTTP 事件接口的真正空集合与网络/解析失败仍分开，分页完整才确认完成。资金流独立复用现有东方财富批量入口，不新增跨来源自动兜底。
- [ ] 财务扫描目标日不是公告知识截止时间：合法的跨日补采必须保留真实公告/更新时间及实际 `known_at`，不能因修订晚于扫描目标日而永久拒绝，也不能反向回填知识时间。本项应随 normalizer 的定向修复一起验收。
- [ ] HTTP JSON 数字 token 已改用 `parse_float=str`，避免先经二进制 float 丢精度；离线真实 JSON 回归已覆盖。此项仍不代替目标 MySQL DECIMAL 精度回读。
- [ ] 巨潮适配器目前明确不可用；没有正式来源证据的财务（包括既有个别证券缺口）仍应失败，不以空值、旧版本或猜测披露日期代替。
- [ ] 重复领取、采集后崩溃、入库后进度更新前崩溃和超时迟到结果通过本地协议测试后，仍需在隔离 MySQL/QMT 环境验证；按任务启用后再观察实际跨交易日推进，不能提前声称已连续稳定运行。

## 5. 最小切换顺序与回退

1. 固定本次启用的数据集与配置（不含 `etf_daily`、`capital_flow_daily`），应用上述精确迁移并重跑 schema check；未完成的数据集继续保持未启用。
2. 资金流保留已验证的东方财富现有链路；对其他计划启用的 QMT 数据集分别完成只读真实样本验证。
3. 仅对实际迁移数据集停止重叠旧写入入口并等待结束；保留资金流和 ETF 原运行方式，不提前停写。
4. 在已批准的真实环境验证一小批来源到业务表的完整链路：正确目标、准确数值、独立回读、重复执行不增重、整日原子替换及失败不破坏旧分区。
5. 仅从合并后的 `main` 安装新入口、配置及计划任务，核对进度读取与实际写入一致。开启自动运行并记录跨日恢复结果；不把首次手工成功等同于日常自动运行已验收。
6. 如果需要回退，先停新入口并等待写入结束，再根据兼容迁移决定是否能恢复选定旧写入者；恢复原任务前复核表结构/来源语义，不让新旧写入者同时工作。

以上涉及生产任务、业务迁移、模型安装与读取端的动作，本文件均为待执行清单，不代表已经执行或已获生产验收。
