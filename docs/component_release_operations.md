# Linux 与 Windows/QMT 组件发布

生产入口继续使用安装在服务器上的 v4 root broker。发布前先合并到 `main`，再向 broker 传入完整的 40 位合并提交 SHA。默认是快速路径：

```bash
sudo -n /usr/local/sbin/probiga-production-deploy "$MERGED_MAIN_SHA"

# 显式执行数据库/任务/质量门/跨端协调事务
sudo -n /usr/local/sbin/probiga-production-deploy "$MERGED_MAIN_SHA" --full

# 仅用于分类器误报 COORDINATED、只读 schema 守卫仍能证明 DB 合同未缺项
sudo -n /usr/local/sbin/probiga-production-deploy "$MERGED_MAIN_SHA" \
  --fast --i-know-ddl-changed
```

发布范围由受信 Git 镜像中的完整“当前 Linux 版本 → 目标版本”差异决定。`fast/full` 只改变执行路径，不改变同一条 SHA 的组件 scope。`--fast --i-know-ddl-changed` 不能绕过只读 schema 守卫；默认 fast 遇到分类为 `COORDINATED` 的范围会在任何服务切换前退出。`--full` 始终执行协调事务，这既是真 DDL 的正常入口，也是快速路径失败后修复代码/schema 不一致的入口。

本版参数协议要求 root broker 与目标引擎同时支持 `probiga-release-mode-v1`。普通发布只替换目标引擎；高级参数和新增发布清单字段首次上线时，必须先从已合并的 `main` 用 `deploy/install_production_deploy_broker.sh` 完成 root 维护安装，再调用 broker。旧 broker 不支持参数透传，不能用它声称 `--full` 已生效。

## 三种版本身份

| 身份 | 含义 |
| --- | --- |
| Linux build（L） | 实际运行 API、独立调度器和已安装 AI worker 的代码版本 |
| Windows build（W） | 实际运行 Windows/QMT 调度器的代码版本 |
| Contract build（C） | 共享数据库与跨端运行合同的协调发布版本；W 必须等于 C |

每个 L 的密封清单位于 `/var/lib/probiga/release-artifacts/<L>/component-release.json`。所有 Linux 生产进程显式设置 `PROBIGA_COMPONENT_RELEASE_PATH` 指向这个固定文件；`PROBIGA_BUILD_COMMIT_SHA` 与 `PROBIGA_EXPECTED_GIT_SHA` 始终保留实际代码版本 L。

清单由 root 原子写入，并由独立发布工具向固定的 `st_component_release_manifest` 表写入 Ed25519 签名。表和存于 TABLE_COMMENT 的公钥仅在首次协调发布时创建；后续 Linux 发布只插入签名元数据，执行零 DDL。私钥固定保存在 `/etc/probiga/component-release-signing.key`，仅 root 可读。运行账户无法产生有效签名，修改数据库行会导致验签失败。API 健康检查要求文件清单与数据库签名证明完全一致，进程启动过程中不执行 DDL。

## 范围判定

`tools/classify_release_scope.py` 只返回 `LINUX` 或 `COORDINATED`：

- `LINUX`：普通静态文件、明确列出的私人 journal 叶模块，以及只导入和注册这个 router 的 `server/api/main.py` AST 变化；文档和测试不改变运行合同。
- `COORDINATED`：其他路径、删除、重命名、文件权限变化，以及任何共享运行合同摘要变化。依赖、部署工具、发布控制器、调度器、数据库、QMT 协议等均在此范围内。
- 软链、gitlink、路径逃逸、不完整 SHA、非祖先 base 等输入直接拒绝，不能通过协调发布来放行。

初次安装本架构改变部署和运行身份代码，必须协调发布，得到 `L = W = C = target`。以后合法的 Linux 单端发布只增加 L，继承上一份清单中的 W、C 和合同摘要。增添新的 Linux 私有模块必须审阅并修改代码中的明确策略，不能通过 CLI 扩大范围。

## Linux 单端切换

快速分支共用依赖锁、不可变源码、adata、venv、组件身份和回执准备。`prepare_release` 完成后先运行只读 schema 守卫，再由独立控制器切换 Linux；它不进入数据库业务 schema 迁移、任务变更、Windows request/hold/grant 或协调回滚流程。

`tools/check_schema_expected.py` 调用当前代码的运行时 schema validators，不执行 DDL、迁移、seed 或 recovery planner。任何一个 validator 要求的表/列/索引合同不满足时，发布在激活前失败并提示使用 `--full`。数据库边界不可达时日志明确标记跳过；激活后的健康检查仍会失败并回滚。

快速路径立即生成独立签名的 `component-release-lite.json`，其中包含 `schema_version: lite-1`、原组件清单、Ed25519 签名和公钥。`component-release.json` 保持不可变自封格式，不在副本上改字段。MySQL 账本在 60 秒后由 transient systemd unit 异步补写；发布启动时若发现 `deploy-guards/ledger-pending/` 中仍有状态文件，会明确告警。

API 健康检查优先验证 MySQL 账本签名。只有账本表中的该 SHA 行可证明为“不存在”时，才接受根目录保护的 lite 证明；账本存在但内容、元数据或签名错误时绝不回退到本地证明。

`tools/linux_release_activation.py` 将以下内容保存到 root 持久事务文件 `/var/lib/probiga/linux-activation/transaction.json`：

- API drop-in、scheduler unit、scheduler resource drop-in、已安装 AI worker drop-in 的原始字节和权限。
- Linux 服务及 AI timer 的原始启用、运行状态。
- `/opt/ProBigA-current` 原链接和本次 Linux 发布回执的原始状态。

控制器停止相应 Linux units，安装准备完成的配置，原子切换链接，重新加载 Nginx，再启动新运行时。健康检查核对实际进程环境、L/W/C 清单、当前 scheduler PID 对应的心跳，以及页面引用的静态资产字节。检查通过后才写入 `/var/lib/probiga/deploy-receipts/linux-<L>.json` 并提交事务。

AI worker 是单次任务：原来处于运行中的任务，重启后可以正常完成并回到 inactive，恢复 timer 后也可能正常触发新任务。控制器保留 timer 和启用策略，核验正在运行的 worker 的真实版本；停止状态下退出结果必须成功，不把任务瞬时 active/inactive 当成应被冻结的调度策略。

## 故障和重试

普通失败会立即恢复原 unit 字节、链接和回执，重新加载 Nginx，恢复原 unit 状态，再重新验证旧 API 和新启动的旧版 scheduler 实例。旧健康结果或旧 PID 回执不能代替此次验证。

断电、SIGKILL 或传输中断不依赖 shell 内存恢复。再次调用正常 broker 时，在识别当前 live units、分类新发布或进入协调恢复之前，先验证持久 journal 并运行其所属版本的密封控制器。未提交事务恢复旧版本；已提交事务核对新文件并归档。恢复失败时保留 journal，修复实际故障后重试相同正常入口，不要手工改链接、删 journal 或触发全端回滚。

已经激活的同 SHA 重试仍经过真实健康与静态资产核验，走 Linux 无变更路径，不为重试额外创建 Windows request/hold/grant。

从 `L != W` 状态再次协调发布时，旧 Linux 版本用于本机恢复，旧 Windows 版本用于 QMT 控制，旧 Contract 版本用于数据库合同校验。不能把这三个值替换成一个旧 SHA。清理器保留当前和回滚所引用的 W/C 源码、venv 和清单，避免连续发布多个 Linux UI 版本后丢失协调恢复依据。

## 验证依据

`tests/test_release_scope.py` 使用真实 Git 历史验证累计差异、AST 边界、环境隔离和危险树拒绝。`tests/test_linux_release_activation.py` 注入停止、安装、Nginx、启动、健康、回执及事务提交故障，验证独立恢复和同 SHA 重试。既有协调恢复状态机测试继续覆盖 Windows 与数据库路径。

QMT 未登录或行情数据尚未齐备仍由数据就绪检查单独报告，不改变实际组件版本，也不自动把已验证的代码发布报告成数据就绪。

## 健康等待、磁盘与回执

`production_release.env` 的 `HEARTBEAT_RETRIES`、`HEARTBEAT_INTERVAL`、`HEALTH_RETRIES`、`HEALTH_INTERVAL` 同时供引擎和 Linux 激活控制器使用，每项限制为 1–60。默认值分别为 `10 / 2 / 5 / 2`，即心跳最多 20 秒、API health 最多 10 秒。

`/tmp`、`/var/tmp`、`/opt`、venv 根、artifact 根和代码发布根都必须至少保留 500 MiB；低于 2 GiB 时告警，低于 500 MiB 时直接输出 `disk full on <path>; free space and retry`。

成功和失败回执都记录 `mode`、原始 `scope`、`execution_scope`、`flags`、目标 `sha`、`signed_manifest`、`lite_manifest` 和真实 `ledger` 状态。快速协调覆盖会保留 `fast-forced-over-coordinated`。引擎 stdout/stderr 每行带 `[MODE=fast]` 或 `[MODE=full]`；快速失败日志直接给出 `--full` 重试提示。

验收顺序是：确定性单元/合同测试；真实 Git 分类测试；schema guard 与 writer 两次零确认测试；在合并后的 `main` 上安装 root broker；最后分别验证快速、快速失败兜底和完整协调发布。只有目标 SHA 在 Linux 实际运行、health/identity 通过且回执与账本状态一致，部署才可标记为 `COMPLETE`。
