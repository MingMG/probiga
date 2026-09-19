# Linux 与 Windows/QMT 组件发布

生产入口继续使用安装在服务器上的 v4 root broker。发布前先合并到 `main`，再向 broker 传入完整的 40 位合并提交 SHA：

```bash
sudo -n /usr/local/sbin/probiga-production-deploy "$MERGED_MAIN_SHA"
```

发布范围由受信 Git 镜像中的完整“当前 Linux 版本 → 目标版本”差异决定，没有绕过参数或范围开关。检查依赖锁、不可变源码、adata、执行适配器密封与质量门的 `prepare_release` 流程保持共用。

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

单端分支在 `prepare_release` 完成后进入独立控制器，随后直接结束，不进入数据库业务 schema 迁移、任务变更、Windows request/hold/grant 或协调回滚流程。

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
