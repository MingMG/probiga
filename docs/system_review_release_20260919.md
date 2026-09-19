# 系统体验与判断改造：发布边界与验收记录

日期：2026-09-19。审查工作树：`E:/My Code/ProBigA-review-20260919`。本文记录本次差异及只读核验，不是生产已发布或采集已稳定的声明。行号以本次工作树为准，后续修改应重新核对。

## 最终运行边界

**Linux/server，涉及 Linux API、静态网页和 Linux 策略/股评任务；Windows/QMT 不在受影响端。**

完整差异扫描未发现 `deploy/`、`integrations/`、`biz/`、`tools/`、`server/common/` 或 `server/api/scheduler_runtime.py` 改动。未修改采集入口、QMT 协议、数据库 DDL/trigger/grant、跨端配置或任务 ownership 合同。静态页面只读刷新未新增采集、重算、下单或消息推送调用。原页面中的人工动作继续受原接口边界约束。

这不等于“全部只是只读展示”。策略判断函数会被 Linux 后台任务使用，并影响下一次正常任务的持久化结果；须同时纳入 Linux 调度服务发布验证，不能只替换静态文件。

| 受影响代码及证据 | 实际作用与写入边界 |
| --- | --- |
| `server/api/routers/hot_data.py:10459`，`monitor_data`；纯读辅助函数约 9816–10280 行 | 从现有行情、日线、板块读取数据并计算响应；改动没有 SQL 写入。修复缺失值、指数身份、样本口径、日期与过期状态，没有改采集或存储合同。 |
| `server/api/routers/sim_trade.py:292`、`:463`、`:644` | 平仓样本统计、归档和账户估值读模型；无新增模拟交易写入。现有 `portfolio_state` 作为单一核算来源，不修改成交/订单算法。 |
| `server/api/routers/holding_strategy.py:477`、`:728`；调用位于 `hot_data.py:8314`、`:8350` | 持仓建议的截止时点判断与页面投影，保留 T+1/可卖数量/过期证据；该调用链读取证据并构建响应，不执行卖出。 |
| `server/api/routers/commentary.py:264`、`:479` | 直接评估接口构建响应；既有 Linux 股评定时任务也复用该评估，仍会更新既有 profile 的运行状态；原任务若显式配置推送，内容会反映新判断。此次没有新增推送、任务或 ownership。 |
| `server/engine/strategy_center.py:769`、`:785`、`:2039`、`:4000` | 零质量、零胜率与未成熟历史证据处理会影响权重/候选。`build_strategy_center_snapshot` 在 4009 行读取指标、4031 行聚合信号；不是只给页面加标签。 |
| `server/engine/strategy_governance.py:27411`、`:28315`；`strategy_center.py:4161` | 既有 Linux 治理任务调用上述构建器，并将结果写入 `st_strategy_center_run`、`st_strategy_center_signal`、`st_strategy_center_conflict`、`st_market_state_daily`。本次未改表结构、写入协议或版本权限，但后续任务输出内容可变化。 |
| `server/trading_v2/decision_worker.py:241` | 既有 Linux 决策 worker 也使用上述 snapshot。此后台消费者必须随 Linux 发布验证，不能把修复称作只读读模型而忽略。 |
| `server/api/main.py:303`、`:429`；`server/static/` | 监控页只允许同源嵌入且保留鉴权；雷达统一入口；首页、交易页、问答与监控的交互/空值/解释修复。 |

### 为什么持久化结果没有使本次变成跨端发布

不能仅因两端连接同一个数据库，就把全部应用计算称为跨端共享合同。此次按消费者追踪：

- 全仓检索 `st_strategy_center_run/signal/conflict`、`st_market_state_daily`，实际业务读取集中于 Linux 的 `server/engine/strategy_center.py` 和 `server/api/routers/hot_data.py`；未发现 Windows/QMT 脚本消费这些结果。
- `server/api/scheduler_runtime.py:2664` 的 `scheduler_task_host_owner` 将普通策略治理、股评、V2 决策归属 `linux_standalone`；`:2732` 的 `_should_skip_task_for_host` 禁止 Windows 执行 Linux owner。只读调用该函数确认 `strategy_governance_daily`、`commentary_watch`、`trading_v2_close_decision` 均为 `linux_standalone`。
- `tools/qmt_host_ownership_contract.py:470`、`:503` 固定 Windows QMT 与非 QMT egress 任务清单。本次逐项核对该清单的 16 个 QMT task type 与 1 个雪球 egress task type，未发现调用本次修改的策略权重/评估函数。
- 特别核对 Windows 侧并非纯行情采集的两个入口：`tools/run_etf_forward_daily.py` 使用 `run_etf_forward_simulation` / ETF 独立算法；`tools/sync_upper_limit_snapshot.py` 的预选80使用 `biz.analysis.sync_analysis_fast`。这些路径和其算法本次未改，未消费修改后的 `strategy_center` 权重或输出表。
- Windows 调度器存在对 `hot_data._invalidate_recommended_stocks_cache` 的延迟导入（`scheduler_runtime.py:7387`），用于既有缓存失效；本次未改该函数。这不是 Windows 执行监控或策略判断的证据。

因此，不能为了使用现成部署工具而人为把本次升级为跨端发布。若之后加入 QMT 消费者、共享合同、数据库结构或任务归属改动，应重新分类。

## 现有可信部署路径及阻塞

`deploy/deploy.ps1` 明确拒绝旧的可变文件上传。当前仓库没有已跟踪的 `.github/workflows`；`tools/post_commit_deploy.ps1` 中“push 后 Actions 会部署”的文字不能当作发布已经触发的证据。

已安装入口为 `/usr/local/sbin/probiga-production-deploy`，由 `probiga-deploy` 通过受限 sudo 调用。源码参数解析在 `deploy/production_deploy_root.sh:252`：

| 入口参数 | 能力 | 是否适用于本次单端上线 |
| --- | --- | --- |
| `--capabilities` | 只读报告 broker 协议能力 | 可用于核验，不执行发布 |
| 一个完整的当前可信 `main` SHA | 准备/校验/切换完整应用版本，含既有 Windows hold/grant 协调 | **不适用作 Linux-only 发布** |
| `--recover-database-guard <SHA>` | 对已存在且匹配的受保护恢复状态执行恢复 | 不是任意发布、静态更新或 API-only 通道 |

没有 `ui-only`、`api-only`、`linux-only` 或其他单端参数。`production_deploy.sh:109` 的 `QMT_EDGE_DEPLOY_BLOCKING=0` 仅跳过部分等待，不取消 Windows 生命周期操作：`:14736` 开始请求 Windows quiescence，`:15524` 在最终成功时授予该次 Windows activation。

所以当前缺少与本次 **Linux/server** 范围一致的受信发布能力。本文没有修改 broker/engine、SCP 覆盖、切换线上符号链接、手工改 seal 或绕过既有门禁。提交/合并代码也不代表已经上线。

可复核的只读 broker 命令为：

```text
sudo -n /usr/local/sbin/probiga-production-deploy --capabilities
```

全量跨端发布语法确实是 `sudo -n /usr/local/sbin/probiga-production-deploy <merged-current-main-sha>`，但它不是本次可以误称为 Linux-only 的替代命令；此审查未执行。

## Windows 暂停/恢复与采集进度：只读结论

现有跨端流程有 writer 身份、保护 hold/grant、旧 checkout 保留及窄边界恢复，但不能据此承诺任意发布中断都无损。

- `tools/update_qmt_windows_edge.ps1:282` 的 `Stop-EdgeScheduler` 先写绑定 PID/instance/build 的 shutdown request；`:345` 等待60秒，未完成则在 `:378` 停计划任务，通过 Job Object 结束整个子进程树。
- `tools/run_scheduler_daemon.py:648` 调用 `wait_for_owned_scheduler_tasks(stop_owned=True)`；`scheduler_runtime.py:9030` 请求停止活跃工作进程，并要求拥有者提交匹配的终态审计。这是停止并核验，**不是等待所有采集业务自然跑完**。
- `tools/run_guojin_qmt_full_market_history_2024.py:800`、`:921` 的默认 resume 路径复核已保存 coverage，EXACT 的日期/证券跳过；缺口只补未认证代码。已有完成分区有续跑依据，当前未提交批次可能被中断并重做。该证据只覆盖该历史采集路径，不能泛化为所有原生行情缓存或所有任务都不损失进度。
- 本次只读观察 Windows 计划任务：Scheduler 为 Running、Updater 为 Ready；注册目录为 `E:/My Code/ProBigA-qmt-production`。runtime heartbeat 与 checkout 均报告 `5743c6c6701bdf50fcc0b2efc24035e61b49ca48`。观察瞬间未列出该 daemon PID 的直接子进程；它不等于全任务账本空闲，也不是稍后发布时仍空闲的保证。
- 未执行 hold、stop、restart、force、run、generate、grant 或 recovery，没有改变采集进度。当前不具备据此宣布“现有 broker 本次可无损停止并恢复所有采集”的端到端证据。

## 生产只读核验与不可验收项

通过已有 deploy 私钥及固定 known-hosts，未改变认证配置，核验到：

- Linux `/opt/ProBigA-current` 指向 `/opt/ProBigA-releases/5743c6c6701bdf50fcc0b2efc24035e61b49ca48`。
- `probiga` 与 `probiga-scheduler` 均为 active；API 运行于 `127.0.0.1:8000`，构建身份与该 SHA 一致。
- 已安装 broker 报告 `probiga-production-deploy-v4`、`probiga-database-guard-recovery-v2`、`probiga-trusted-artifacts-v2` 及现有 recovery/seal capabilities。
- deploy 账号的 sudo 范围只有该 broker；没有另一个已授权的 API-only 服务切换入口。
- 经 SSH 对 `http://127.0.0.1:8000/api/monitor/data?date=2026-09-18` 发只读请求，响应 **HTTP 401**。进程没有现成管理 token/会话，deploy 用户也不能读取受保护运行配置。因此没有取得 monitor、market-trend、V3 context/stock-pool 的已认证生产 payload，不把401误写为业务接口失败或成功。
- 主代理报告浏览器工具 provider 连接失败，`cua.getState` 返回空浏览器清单并伴随 fetch 失败。因此本次工具环境未完成在线浏览器视觉验收；此项来自主代理的观察，不是本子任务伪造的浏览器结果。

没有输出私钥、token、数据库凭据、私有持仓或完整受保护 API 内容。

## 已执行验证

以下数字是具体已完成命令的结果，不合并重复运行的用例数，也不代表全库/生产验收。

```powershell
& 'E:/My Code/ProBigA/.venv/Scripts/python.exe' -m pytest tests/test_monitor_accuracy.py tests/test_hot_data_detail.py -q
```

结果：**152 passed**。覆盖真实指数与样本拆分、缺失/零值、完整日期、午休/收盘点、旧快照时间、前日板块隔离、TMT完整分母与单位、异常脱敏、HTML转义、刷新失败保留状态、iframe卸载/消息来源校验、同源嵌入响应头与雷达唯一入口。

```powershell
& 'E:/My Code/ProBigA/.venv/Scripts/python.exe' -m pytest tests/test_account_auth.py tests/test_admin_auth.py tests/test_api_error_handling.py tests/test_api_generic_error_sanitization.py -q
```

结果：**31 passed**。该批验证鉴权与API异常边界。另一次开发中联合运行曾得到178项通过；随后新增5项监控边界用例，以以上最新152项监控结果为准，不累加成虚假的独立用例总数。

`node --check server/static/js/monitor.js` 与 `node --check server/static/js/app.js` 通过；针对本任务文件的 `git diff --check` 通过。判断逻辑、交易页、问答和首页的其他测试由对应审查记录及主任务最终验证记录负责。本次没有运行或声称完成 Linux CPython 3.14 wheelhouse/全部生产发布检查，也没有运行生产修改或恢复演练。

## 发布前扩大验证与失败归类

主任务在 CPython 3.14.3 的一次全库运行记录为 **40 failed、7812 passed、36 skipped、116 subtests passed**（515.21 秒）。随后在未修改的生产 main 基线工作树重跑这40个节点，得到 **32 failed、8 passed**。这些是开发中具体运行结果，不代表最终全部测试已通过；产品和导航回归由主任务继续处理。

本次只读归类的非产品失败如下。下表所指生产、采集、schema、QMT和脚本策略文件均未在本次差异中修改；不能为了通过断言而更改其发布合同。

| 失败组 | 可复核的失败原因与归类 |
| --- | --- |
| `test_production_deploy_recovery_state_machine.py:3846` | 合成 governance health 回执经现有 Bash parser 返回2，断言要求0；同一节点在未改基线仍失败。既存回执/解析契约不一致，本次未判断哪一方正确，也未修改发布解析器。 |
| `test_production_governance_contract_recovery.py:721` | 旧QMT快照恢复夹具期待6条变化，实际4条；未改基线相同失败。既存任务快照/断言不一致，不据此放宽恢复验证。 |
| `test_production_release_boundary.py:3046` | 测试期待 `CUTOVER_STEP=verify_no_scheduler_dropins` 旧标记，当前部署源码没有；未改基线相同失败。不是本次新增发布路径或服务参数变化。 |
| `test_qmt_myquant_runtime.py:255` | 从生产 PowerShell 提取的 equal-SHA 片段引用 `$ExpectedRoot`，单元夹具未设置，StrictMode报未定义；未改基线相同失败。既存测试隔离问题，不能把这个错误当作真实生产 SDK 验收。 |
| `test_layer4_maintenance_workflow.py` 两项 | `.github/workflows/layer4-maintenance.yml` 在当前与未改基线都不存在，均为FileNotFoundError。当前仓库并未提供该测试要求的手工受保护 workflow。 |
| `test_trading_v3_mysql_acceptance.py:394` | 静态表清单函数返回87张，测试冻结值为86；未改基线相同失败。这一节点没有执行生产MySQL验收，本次没有更改schema。 |
| `test_scheduler_script_policy.py` 四项 | 全库运行失败点为 `scheduler script HEAD differs from runtime build`，来自运行身份环境与临时/工作树HEAD不一致。基线节点单跑通过；本次用同一Python单跑整个文件为 **11 passed**（9.48秒）。不是Windows脚本字节或CRLF变化，没有修改脚本身份校验。 |
| `test_scan_tracked_secrets.py` | 初次有8条命中：6条在HEAD已有的测试夹具；1条为雷达HTML已删除但索引未stage的 `UNREADABLE_SOURCE`；1条为新增监控异常脱敏测试的合成凭据格式。最后一条已改成不含凭据格式的私有诊断文本，仍断言完整异常内容不泄漏，单项测试通过；未修改扫描规则或掩盖基线命中。雷达删除应随正式stage生效。 |

`test_hot_rank_api_safety.py` 的11项也在未改基线全部失败。它们覆盖 `hot_data.py:919–1714` 的外部热榜获取、严格双源完整批次、持久化融合批次绑定和抓取缓存：现有实现拒绝任一不完整来源，而部分旧测试要求保留单源；另有强制刷新仍接受1秒缓存、持久化结果条数/重建等断言不一致。本次该文件差异从9865行以后监控读模型开始，未触及以上函数或 `tools/merge_hot_rank.py`、`server/common/hot_rank_source_contract.py`、`ths_hot_contract.py`。这些是被排除的数据获取/来源完整性与融合合同边界，原样保留并记录，没有为绿测试降低来源要求。

## 发布前缓存与安全复核

本次修改的已有静态资源入口均已换版本：`app.js` 129、`style.css` 48、`ai-chat.js` 2、`ai-chat.css` 3、`login.js` 2、`trading-v2.js` 16及其CSS 8、`trading-v3.js` 43；新监控与首页资源也有版本参数。首页和AI入口继续返回no-store。没有新增鉴权豁免；监控iframe仅同源嵌入并继续验证消息origin/source。`.runtime/` 是本地验证产物，未被ignore，正式提交必须显式排除。

最终复核曾复现登录返回地址漏洞：同源绝对URL含双斜线路径，转回相对地址后会被浏览器解释为外站。现已在URL解析并校验origin后拒绝双斜线路径，同时保留问题文本原有编码。新增回归检查最终浏览器URL的origin；修复后同一CPython 3.14运行 `tests/test_account_auth.py tests/test_admin_auth.py tests/test_login_return_path.py -q` 为 **27 passed**（4.98秒）。

再次检索既有发布入口，旧上传脚本均已retired；遗留 `deploy/restart_probiga.sh` 只有直接systemctl重启，不安装受信发行版、没有既有deploy账号授权，也不构成API-only发布能力。当前可信broker的参数和跨端生命周期没有变化，仍没有Linux-only替代模式。

## 发布前结论

代码范围保持 Linux/server；其中策略判断改动影响 Linux 后台后续持久化结果，需要 API 与 Linux 任务共同验证。现有 broker 的跨端生命周期与该单端范围不匹配，不能借“使用同一数据库”或“只有一个部署命令”扩大到重启无关 Windows/QMT。认证生产 API 与视觉验收尚未完成，发布工具本次未调用；这些限制必须保留在交付说明中。

### 合并时追加事实

主任务合并前，远端 main 已加入另一任务的 `0325386`，修改 `biz/stock_market/sync_stock_market.py`、`server/common/qmt_minute_checkpoint.py` 及其文档/测试。本次通过无冲突合并保留这些修改，首次合并提交 `55ece5e38df56d9302752032e9950fd04caa66c6` 已推送 main；本次及该提交的关联回归合计496项通过。若从先前观测的5743c6c版本发布这个合并版本，累计发行范围应重新分类为 **cross-end**，需要与仍在运行的QMT任务统一协调。本任务没有执行该合并版本的发布，也没有自行暂停另一任务的采集。
