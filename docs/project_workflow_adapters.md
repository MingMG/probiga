# 项目编排执行适配器

工程服务从服务管理员控制的 settings JSON 读取 adapters。任务只选择 profile，并提供候选、环境和业务参数。命令、工作目录和环境变量名由服务管理员绑定，任务不能提交 shell、可执行文件或命令数组。应将设置文件、私有运行证据和远程身份放在受管工作树之外，且只允许工程服务账户读取。

## 原生只读入口

- adapters.repository 指向供现有 tools/classify_release_scope.py 读取的绝对 Git 仓库。分类返回共享合同摘要和候选改动范围。仅 Windows 私有实现改动可从现有分类器的 COORDINATED 结果收窄到 Windows。管理员必须先核对消费者，再用 windows_only_runtime_paths 列出这些文件。共享合同改变仍要求协调。
- adapters.linux_health_url 指向现有 /api/health。支持 HTTPS 或回环 HTTP。生产应用探测同时验证实际提交、组件清单及独立调度心跳。探测报告实际观察到的构建身份。发布验收另用业务读回。
- 在 Windows 执行器本机，adapters.windows_build 是预期的已运行 Windows edge 构建。探测组合 scheduler 身份、BigQMT spool 应用状态和当前模型的无委托 capability，核对 scheduler 构建、模型实例与策略制品身份。Linux 控制器通过已配置的 probe-windows profile 调用 Windows 只读入口，并取得同样的结构化结果。
- adapters.trusted_main_remote 指向项目正式 GitHub 远端，或发布运维维护的只读可信 main 镜像。提交生产发布前通过有界 git ls-remote 重新核对 refs/heads/main，候选不符时拒绝发起。服务环境需已有对应的非交互认证。现有 Linux root broker 仍独立执行自己的正式 GitHub main 检查。

## 绑定现有入口

adapters.bindings 按 profile 命名。每个绑定的 kind、endpoint、environment 和 resource 必须精确等于任务阶段。endpoint 和 resource 无值时写 null。argv[0] 必须是绝对可执行文件路径。只有完整参数占位符可以替换为已校验的单个值，例如 {candidate}、{external_id}。input_fields 指定允许传给该入口的业务字段。env_allowlist 指定本次运行另需从服务继承的环境变量名。托管测试用 managed_worktree 选用任务已登记的工作树，执行前后核对工作树干净且 HEAD 等于受测候选。

| protocol | 调用约定 |
| --- | --- |
| json | 子进程从 stdin 读单个 JSON 请求，输出单个 JSON 结果。结果必须回显 operation_id 和 fence，并给出 status、固定 code、阶段证据和可选 external_id、checkpoint。 |
| exit | 用于确定性测试命令。实际退出码、候选和环境落证据。失败用例标识及异常类型进入返工上下文。详细日志留在私有运行目录。 |
| submit | 调用接受命令行参数的既有提交工具。退出后用 reconcile_argv 查询权威回执。 |
| observe | 用于协调发布的第二个端点，仅读取已提交的共享发布回执。 |

对会修改状态的绑定，reconcile_argv 必须调用只读查询，输入输出沿用 JSON 约定。查询使用 operation_id 或已记录 external_id、候选和目标去核对原执行。提交进入未知状态后，服务保留资源归属并查询同一次执行。对账结果应区别正在进行、已完成、可证明终止、结果未知。应从运行系统的权威回执产生发布或采集证据。仅看到提交成功、进程活跃或旧版本健康无法证明本次交付。

| 阶段 | 现有系统入口与应读证据 |
| --- | --- |
| Linux 生产发布 | 已安装的 sudo -n /usr/local/sbin/probiga-production-deploy，使用准确合并 SHA。以受保护发布回执判定发布完成，随后读取 API 版本与目标业务结果。协调发布只能提交一次，第二端用 observe。 |
| Windows/QMT | 复用 Windows 发布 updater 的现有 request、hold、grant 和回执链。单端发布必须配置实际支持的单端发布入口和回执查询。 |
| 补采 | 复用已登记调度器或对应原有工作入口。要以任务业务 run ID 查结束回执，再按目标分片、数据范围和合法无数据凭证读正式结果。 |
| 研究及回放 | 复用 tools/trading_v4/run_research.py 或适用的已登记研究入口，保留数据快照、数据可用时间、策略和样本外结果。 |
| 模拟回归 | 仅绑定已证明隔离的环境和账户通道，绑定需写 isolated: true，回执必须报告 isolated: true 和 real_orders: 0。 |

安装编排代码不自动配发 GitHub、Linux 发布账户、Windows 远程执行器或生产回执权限。上线前需由现场运维为实际任务绑定具备原有授权的入口和查询身份。当前会话未配置这些生产绑定。缺少配置、身份或业务读回时，任务保留具体阻塞，独立步骤继续。

PR #87 样例位于 docs/evidence/project-workflow-pr87.json。证据来自 main 的 94d1b4c9 和原隔离验收分支 714b180c。HTTP 验收失败和单元回归通过同时保留。历史发布和生产生效缺少可核对的回执和读回。
