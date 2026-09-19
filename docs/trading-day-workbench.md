# 今日看盘

首页 `/?tab=trading-day` 以盘前、盘中、盘后三个阶段呈现市场判断、主线观察股、当前持仓风险、个人计划与复盘。原市场总览及其余导航页面继续保留。

主线股票来自同日盘前主题研究。盘前主题、独立竞价结果分别显示来源、时点和核验状态；不同策略批次不会互相升级确认状态。缺少确认或失效条件时由用户记录，不生成交易指令，不修改自选、持仓或 QMT 采集范围。历史盘中页面不以当前行情和持仓回填，盘后正文只接受所选日期已发布记录。

个人记录通过账户会话访问 `GET/PUT /api/trading-day/journal?trade_date=YYYY-MM-DD`。PUT 提交 `revision`、完整 `plans` 与 `review: {text}`。计划可写字段为 `stock_code`、`stock_name`、`theme`、`reason`、`trigger`、`invalidation`、`source_as_of`、`source_run_uid`、`status`、`note`。GET 附带的初始条件 `original`、创建及修改时间由服务器维护，不可回写。其他页面已修改同一记录时返回 409，页面重新读取后保留当前输入，供用户对照再保存。

新增观察计划需要当日交易时钟核验通过。历史计划保留股票集合及观察条件，只能补充状态、备注和复盘；未来日期不可写入。保存失败不会显示保存成功，自动刷新保留正在编辑的输入。未保存的输入仅存在当前页面，离开或重载页面前需保存。

服务端在既有 `PROBIGA_JOB_LOG_ROOT` 下永久保存每账户、日期的平铺 JSON 与锁文件，文件名形如 `trading-day-user-7-2026-09-18.json`。Linux 默认目录为 `/var/lib/probiga/jobs`，文件权限为 0600，服务账户拥有；存储独立于发布目录，原子替换并通过进程锁及 revision 防止覆盖。账户会话中的用户 ID 是唯一身份来源，传统共享 token 不支持个人记录。损坏或不安全的文件返回不可用，不重置为空记录。

## 验证

```text
python -m pytest tests/test_trading_day_model.py tests/test_trading_day_store.py tests/test_trading_day_api.py tests/test_market_workbench_ui.py tests/test_release_navigation_smoke.py tests/test_release_page_smoke.py tests/test_account_auth.py tests/test_admin_auth.py tests/test_api_generic_error_sanitization.py -q
node tests/trading_day_browser.cjs
```

浏览器检查需要 Playwright 和本机 Chrome；可将 Playwright 模块绝对路径作为参数，并用 `PLAYWRIGHT_CHANNEL` 选择其他已安装通道。检查使用隔离测试数据，覆盖真实首页入口、保留旧页、日期路由、阶段键盘导航、保存重载、失败与并发冲突、抽屉请求竞争、历史限制及 320–1280 像素布局，截图输出到系统临时目录。

## 发布范围

本功能只影响 Linux/server：静态页面、API 路由、服务器私有个人记录。不改变共享数据库、QMT 协议、调度归属或跨端配置。

当前已安装的 `probiga-production-deploy-v4` 无 Linux-only 入口；正式发布引擎仍会无条件触发 Windows hold 和 activation grant。依照单端发布规则，本功能不得使用该全端入口上线，也不得通过手工上传或切换服务绕过可信发布流程。上线前需要可验证的 Linux 单端发布能力，或用户明确授权扩大为协调发布。
