# ProBigA 工程任务编排

工程编排单独运行。已有行情、采集、策略和交易调度保持自己的运行归属。工程服务保存任务图、当前步骤、租约、外部执行身份和独立验收记录。后台入口：**调度、权限与部署 → 工程验收**。独立 Sites 控制台没有可用的授权同步通道；本站后台为当前证据入口。

## 登记一次，后台推进

在已受管的工程目录运行，`PROBIGA_WORKFLOW_DB` 和 `PROBIGA_WORKFLOW_SETTINGS` 由服务运维配置：

```sh
python -m server.project_workflow submit-engineering request.json
python -m server.project_workflow status TASK_ID
python -m server.project_workflow pause TASK_ID
python -m server.project_workflow resume TASK_ID
python -m server.project_workflow cancel TASK_ID
```

`request.json` 包含公开标题 `title`、内部目标 `goal`、验收列表 `acceptance`、Unix 秒 `expires` 和已绑定的 `test_profile`。可选 `id`、`priority`、`timeout`。入口从正式 origin/main 获取准确基线，在受管目录建立独立 codex/* 分支及工作树，登记开发和返工测试步骤。GitHub Issue 按 AGENTS.md 先查重登记；仓库不可写时记待同步。

数据、研究、合并、发布等具体任务通过 `submit` 传同一任务字段及 `steps`，每步包含 `id`、`kind`、`profile`、`inputs`、`depends` 和有界 `timeout`。用 `endpoint` 指定实际依赖端；`resource` 是按业务范围规范化的互斥键，由可信适配器匹配。候选用完整提交或 `$candidate` 引用最新工程检查点。需要发布时，把实际候选的合并、测试、分类及各端发布、读回纳入同一张任务图。需返工的测试显式设 `rework: true`。每个采集步骤应有依赖其真实业务回执的 coverage 步骤。详细阶段和回执见 [执行适配器](project_workflow_adapters.md)。

移交已有 Codex 工作树时先停止旧轮次或确认它已释放写入权。以 `submit` 登记 `session` 的 `worktree`、`branch`、`base`、可用的 `thread_id` 与 `turn_id`，并记录 `ownership_released: true`。服务核对工作树和活跃轮次。受管 App Server 读不到旧桌面线程时，不会声称接管了旧线程；应在确认写入移交后，用独立受管线程从同一 Git 检查点执行。暂停会阻止新阶段；已经提交的外部操作仍须对账。取消保留原有证据。

## 安装与权限

- 工程运行时仅部署 Linux。把经合并和测试的准确候选放到独立工程服务目录，按 `deploy/probiga-project-workflow.service` 由专用账号启动。该单元不接管原有业务调度。
- 先用独立 venv 安装 deploy/project_workflow_requirements.txt，再为工程服务准备隔离的受管工作树目录、可信配置文件，以及独立 Codex App Server 的 Unix socket。将 socket 放在工程服务与 App Server 共享且受文件权限保护的运行目录（如 /run 下的专用目录）；工程服务启用 PrivateTmp，不能使用另一个服务的 /tmp。监督实例使用独立只读沙箱。服务用户不使用桌面认证缓存。以实际模型能力请求核验服务身份；工作区写入和凭据不可读须通过探测。候选测试命令由运维绑定到隔离的执行环境。
- 生产发布、补采与回执查询使用原有受保护入口。仅工程控制面可登记授权，CLI 的 `authorize` 记录既有确认引用、精确候选、动作、资源、端点、环境和有效期。收到授权记录前，不派发相应生产变更。没有真实下单动作。
- 让现有 API 进程只读账本文件并可遍历父目录；工程服务继续独占写权限。生产 API 的启动命令清空继承的环境变量，在现有受保护的 `/opt/ProBigA/.env` 中设置 `PROBIGA_WORKFLOW_DB=/var/lib/probiga-workflow/tasks.sqlite3`。开发环境也可用同名环境变量。API 启动后读取配置；未配置或账本不可读时返回 503。私有执行证据和配置不授予 API 进程。
- API 沿用项目现有的访问边界。生产单操作员模式关闭应用 token 校验时，先核验站点入口的现有访问限制覆盖 `/api/project-workflow/tasks` 和任务详情。未授权请求须在入口被拒绝，授权操作员须能访问同源路径。没有可核对的访问限制时，保持账本路径未配置。

## 恢复与验收

通过同一数据库和配置重启编排服务。尚未运行的指令可派发；曾运行的写操作必须等租约过期并核验原执行器不在，再按相同 operation_id 对账。保留未知写操作及发布回执未读回的资源约束。看到 `external_outcome_unverified` 时查原业务系统的执行 ID 和正式回执，可执行 `python -m server.project_workflow reconcile OPERATION_ID --seconds 120` 重新开启只读对账窗口。失败或过期的生产确认应根据原规则更新范围和候选。回滚必须有原发布回执中的回滚点、失败读回以及独立有效授权，回滚仍须读回。

```sh
python -m pytest -q tests/test_project_workflow_store.py tests/test_project_workflow_adapters.py tests/test_project_workflow_codex_runner.py tests/test_project_workflow_console.py
python tests/acceptance/project_workflow_process.py
python -m server.project_workflow import-history docs/evidence/project-workflow-pr87.json
```

进程演练使用隔离端点和 SQLite，真实杀掉编排器和执行器后按原操作对账。它不代表实际 QMT 实例断线恢复。真实 Codex 返工演练使用 `tools/drill_project_workflow_codex.py` 与已认证的专用 App Server。真实工程服务验收还需在已批准的环境核对部署后版本、自动任务和后台读回。
