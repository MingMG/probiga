# Windows 更新器：按授权版本推进

2026-09-05 实际发布发现：Linux 已给版本 11c0461 签发最终授权，但发布期间 main 又合入独立游戏目录，旧 Windows 更新器只检查最新 main，因新 tip 没有发布请求而滞留旧代码。另一个独立原因是生产 checkout 的本地空代理配置导致 GitHub 连接重置；已将该 checkout 的代理恢复为本机已验证可用的代理，不修改全局配置。

最小修复：

- 从现有受保护账本的全局最新 hold 选择更新目标，不新增数据库表或服务。
- 切换前确认目标属于 origin/main，并可从当前 checkout 快进；合并准确 SHA，保留 main 中的无关提交。
- 目标与当前 SHA 相同或已授权恢复旧版本时，不依赖 GitHub fetch；写权限仍须完整 schema seal、真实 activation grant 和 QMT/bootstrap 校验。
- 第一次兼容安装允许读取真实旧协议 hold/grant；未有最终 grant 时仅停止并等待，不覆盖旧 checkout。出现过受保护恢复 context 后禁止退回旧协议。

隔离测试覆盖真实 PowerShell 选择/切换片段和 SQLite 账本状态，包括 main 已前进、兼容等待、最终授权、ABORT 选择旧版、缺少 context 及错误主机/checkout。测试不等同于生产 MySQL 权限或 QMT 运行证明。

2026-09-05 15:33 两端已实际运行 d52a79b，Windows 完整回执 READY、两个执行器各只有一个新鲜心跳。下一版将 QMT_EDGE_RECOVERY_COMPATIBILITY_INSTALL 设为 0，使用此前已安装的可信 controller 记录窄范围 pre-cutover 恢复上下文。该机制不涵盖不兼容 schema 变更后的任意回滚。

同日生产 review 还确认 Linux 普通采集并发实际为 1，公告任务占用唯一槽位，补数排队。服务器约 3.5 GB 内存、1.9 GB 可用、swap 未使用。现有 Linux 调度器启动配置固定为 2 个普通采集槽位（原代码默认也是 2），只覆盖陈旧的单并发环境值；不增加服务或调度层，不改变各数据发布器的互斥锁、任务审计及既有专用通道。增加一个槽位会增加峰值内存与外部请求并发，部署后仍须观察资源和真实入库情况。

9 月 3 日日线已通过现有按日期 BigQMT 发布器补齐：5,548 条行情、8 只原生无交易记录，分区回读 PASS。资金流缺口及分钟级历史不能由这项结果冒充完成。GitHub 连接曾间歇性重置；系统 TLS 后端诊断并未证明可解决，已撤销该临时设置，未降低证书校验。新更新器的已安装版本重试不再依赖 GitHub。
