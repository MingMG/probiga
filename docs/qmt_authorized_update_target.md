# Windows 更新器：按授权版本推进

2026-09-05 实际发布发现：Linux 已给版本 11c0461 签发最终授权，但发布期间 main 又合入独立游戏目录，旧 Windows 更新器只检查最新 main，因新 tip 没有发布请求而滞留旧代码。另一个独立原因是生产 checkout 的本地空代理配置导致 GitHub 连接重置；已将该 checkout 的代理恢复为本机已验证可用的代理，不修改全局配置。

最小修复：

- 从现有受保护账本的全局最新 hold 选择更新目标，不新增数据库表或服务。
- 切换前确认目标属于 origin/main，并可从当前 checkout 快进；合并准确 SHA，保留 main 中的无关提交。
- 目标与当前 SHA 相同或已授权恢复旧版本时，不依赖 GitHub fetch；写权限仍须完整 schema seal、真实 activation grant 和 QMT/bootstrap 校验。
- 第一次兼容安装允许读取真实旧协议 hold/grant；未有最终 grant 时仅停止并等待，不覆盖旧 checkout。出现过受保护恢复 context 后禁止退回旧协议。

隔离测试覆盖真实 PowerShell 选择/切换片段和 SQLite 账本状态，包括 main 已前进、兼容等待、最终授权、ABORT 选择旧版、缺少 context 及错误主机/checkout。测试不等同于生产 MySQL 权限或 QMT 运行证明。

部署仍是首次兼容安装阶段（QMT_EDGE_RECOVERY_COMPATIBILITY_INSTALL=1），不得宣称窄范围 pre-cutover 自动恢复已经启用。必须先确认两端运行一致的新版本，再单独启用恢复 writer；不能在首次安装时伪造先前版本的恢复能力。
