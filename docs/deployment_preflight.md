# 部署网络、权限与启动记录

本变更只调整现有 Linux broker/engine、Windows updater/注册器/恢复控制器的前置检查和诊断，不修改数据库合同、QMT 授权、票池或采集业务。

## 部署前检查

- Linux 在实际服务器的受控 Git SSH 环境验证 main，保留固定 deploy key、known-hosts、干净环境及缓存身份校验。main 探测上限 45 秒，fetch 上限 120 秒；SSH 单次连接 10 秒。错误包含 `environment`、`stage`、`reason` 和退出码，不回显可能包含凭据的原始 Git 输出。
- Linux engine 在任何服务修改前验证所需程序、systemd 服务账户、非交互 sudo 切换和部署目录权限。adata HTTPS 拉取上限 120 秒，失败停在 preparation，保留原服务。
- Windows 的每个 Git 子进程默认上限 45 秒，可通过 `-GitTimeoutSeconds` 调整到 5–300 秒；HTTP 低速 15 秒即失败。错误包含主机/阶段、退出码和脱敏原因，区分代理、网络、DNS、认证、TLS、仓库及权限。
- 恢复控制器先验证精确 merged main、生产仓库、真实权限、授权证明及生产 fetch，再禁用任务。updater 仅在实际前进版本时验证远端，保留同 SHA 恢复不依赖 GitHub 的既有行为。
- 使用真实 token 对任务安全描述符执行 AccessCheck，检查读、写、执行权限；查询成功不代表修改权限。注册器还在停任务前验证实际状态子目录的写入及设置 DACL 权限。预检不修改 ACL、任务 RunLevel 或用户权限。
- 快进前检查 Git 锁，报告 `REPOSITORY_BUSY` 并保留锁。随机写入探针使用 DeleteOnClose，不触碰已有文件。后续身份、数据库 seal、QMT、hold/grant 和任务绑定检查保留。

预检不能消除检查完成后发生的权限变化、断电或磁盘故障；原有部署恢复和 writer 围栏仍然有效。

## 临时代理

Windows 入口接受相同的 `-GitHubProxy` 参数：

| 参数 | 行为 |
| --- | --- |
| 不传或空字符串 | 使用现有 Git/环境配置，报告有效来源及 NO_PROXY 是否存在 |
| `direct` | 仅本次 Git 子进程明确直连 |
| `http://127.0.0.1:端口` 等代理 URL | 仅本次 Git 子进程覆盖仓库 URL 专用代理及 remote.origin.proxy |

代理 URL 支持 http、https、socks5、socks5h，不接受内嵌凭据、查询参数或片段。认证代理仍可使用既有受控配置。URL 专用代理比通用 http.proxy 更具体，remote.origin.proxy 又可能优先，因此两个位置均在子进程内覆盖。显式代理时清除该子进程的 NO_PROXY，避免看似已设代理却被绕过。

不写本地、全局或系统 Git 配置，也不改父进程环境；成功、失败、超时及 UAC 取消均无持久代理需要恢复。不会自动创建中继或关闭 TLS 校验。

## 标准 UAC 启动器

在干净、已合并的 exact-main 控制器目录使用 `tools/start_qmt_edge_recovery.ps1`，传入原恢复脚本的 `ProductionRoot`、`PriorBuildSha`、`TargetBuildSha` 和已有模式参数；可增加上述代理/超时参数。该入口仅用于已有恢复协议允许的恢复，不用于强制重启健康服务。不要复用旧的 `.runtime/deploy` 一次性脚本。

启动器通过 `Start-Process -Verb RunAs` 请求标准 Windows 确认，遵守机器执行策略。UAC 提升前后分别验证实际环境；不会自动重新请求确认或改变任务权限。

记录写在控制器目录已忽略的 `runtime/deploy/edge-recovery-<UUID>`：当前 JSON、追加事件 JSONL、控制器 stdout/stderr。状态包括：

- `WAITING_CONFIRMATION`：已提出标准管理员确认，尚未证明控制器运行。
- `CANCELLED`：Windows 返回取消码 1223。
- `STARTED`：提升宿主/控制器启动；用 `controller_started`、PID 和 stage 区分。
- `COMPLETED`：控制器实际退出，必须同时检查 `exit_code`；非零不代表成功。
- `FAILED`：预检、启动、宿主异常关闭、缺少完成记录或退出码不一致。

同一生产根目录以机器级互斥防重复弹窗和并行恢复；提升宿主用 kill-on-close job 管理本次控制器。宿主关闭时清理本次进程树，控制器原有 job 继续约束 daemon/bootstrap。仅清理自己创建的进程。

## 实际上线边界

2026-09-07 本任务只读核实 Linux active release 与 Windows checkout 都已前进到 `d29b43e2128c1ba95a68d5d2e97de3f6a192ab29`，旧的 `818cac25` 成功记录不能当作当前版本。Linux API/调度服务 active；Windows 调度器/更新器当时运行中。

当前工具进程虽为 Administrator 用户，但 token 未提升，任务 DACL 拒绝写/执行权限；没有通过修改 RunLevel、ACL 或绕过 UAC 消除此边界。Windows 直连 GitHub 的实际探测也出现低速超时，不能用此前另一条 fetch 成功替代验收。

Linux `production_deploy_root.sh` 是已安装的 root-owned broker；普通 main 发布只更新 engine。SSH 预检修复必须由具有既有 root 维护权限的操作员，通过 `deploy/install_production_deploy_broker.sh` 安装经过独立 SHA-256 核对的 root-owned staging 文件。普通 deploy 账户没有该安装权限。安装器不重启服务、不修改数据库；不扩大 sudo 规则。

因此在这些条件满足前，应在预检退出，保留运行中的服务。完成安装/标准 UAC 后，重新分别检查两个执行环境，再通过原 broker 仅发布已合并 main，并进行原数据库/QMT验收。

## 验证

新增测试使用真实 Bash/PowerShell 5.1、临时仓库、本机拒绝代理、模拟 Git 和自有 sleeper 进程；不操作生产任务。覆盖超时、代理优先级、脱敏、配置不变、DACL、Git 锁、UAC 取消、互斥、宿主退出和进程清理。

主要回归文件：`test_production_deploy_network_preflight.py`、`test_production_deploy_broker.py`、`test_windows_deploy_preflight.py`、`test_qmt_recovery_launcher.py`、`test_qmt_prior_edge_resume.py`，以及既有 QMT 交接、根目录绑定、自启动检查。

额外 Linux recovery state-machine 回归中，以下四项在未修改的基线 engine 上同样失败，本任务未调整其恢复/业务合同：`test_exact_live_request_noop_gate_is_strict_and_read_only`、`test_same_sha_request_identity_mismatch_fails_before_database_phase`、`test_activation_snapshot_only_recovery_grants_only_verified_new_runtime[new-runtime-preserved-no-receipt-True-False-True-False]`、`test_governance_recovery_parsers_bind_disposition_identity_and_fields`。

全库秘密扫描仍报告基线已有的三项测试 fixture `DB_URL_PASSWORD`：`tests/test_direct_acquisition_schema.py:283`、`tests/test_direct_store.py:498`、`tests/test_trading_v2_candidate_context_pit.py:92`。本任务新增和修改文件没有新增扫描发现；未输出匹配内容或借此修改无关测试。

权限依据：[Microsoft Task Scheduler security contexts](https://learn.microsoft.com/en-us/windows/win32/taskschd/security-contexts-for-running-tasks)。代理优先级依据：[Git config](https://git-scm.com/docs/git-config)。
