ProBigA 旧电脑离线安装与独立验收
硬件：i5-4210M / 16GB / 1TB机械盘；Windows 11 x64。

范围与真实状态
本工具准备完整业务库时点备份、QMT安装目录、MySQL8.4.11、Python3.13.14/3.14.3、
两套Windows离线依赖、Git、VC运行库、Chrome、Codex CLI，并安装独立验收入口。
Linux网站/API/计算服务仍在原服务器；当前电脑生产端不停止、不注销、不换授权。
安装成功 != QMT登录成功 != 行情新鲜 != AI历史完整 != 生产接管。
不会自动注册生产scheduler/updater/13306隧道，也不会领取生产AI队列。
采集验收工具永久可复用，不是替代正式调度架构。

一、当前电脑准备数据包
必须先把本次代码测试、提交、合并main；仅允许干净且与origin/main一致的main出包。
管理员PowerShell执行源码tools/secondary_edge/prepare_package.ps1。
默认输出 F:\ProBigA-OldPC-Package；请选择新的空目录，已有包不覆盖。
弹窗输入当前MySQL备份管理员账号密码，不是Windows密码，不要把密码发到聊天。
账号必须具备完整元数据可见权限和BACKUP_ADMIN；不自动扩权，不重置生产密码。
脚本执行完整非系统业务库单事务逻辑备份，包含views/triggers/routines/events。
不复制正在运行的MySQL Data；不停止数据库。备份期间不得做发布/DDL；备份锁
会让部分DDL等待。长期备份增加undo/磁盘压力，低峰进行并观察源机剩余空间。
数据库或任何软件/依赖失败均不写READY。没有READY的目录不是可安装迁移包。
数据库约156.6GiB；逻辑SQL可能更大，源输出盘要求450GiB剩余空间，NTFS。
第一次出包需要联网下载官方软件/完整wheel；目标安装不需要联网下载软件。
复制整个输出目录到移动硬盘。业务数据与QMT目录含隐私，请保护移动硬盘。
SHA256用于传输完整性检查，不等同于抵御恶意替换的数字签名。

二、旧电脑安装
必须在旧电脑运行；脚本拒绝在源电脑运行。不要拔移动硬盘或中断恢复。
用管理员PowerShell进入包目录，执行：
  powershell -NoProfile -ExecutionPolicy Bypass -File .\install_target.ps1 -InstallRoot C:\ProBigAEdge
安装位置可换到旧电脑较空的NTFS分区，需要450GiB空闲。不要选择盘根目录或现有资料目录。
软件安装提示需重启时，只重启旧电脑再执行同一命令。不会重启源机或Linux。
断电/导入失败保留现场，绝不自动清空数据库重新来；请把错误/报告交回审查。
恢复原存储函数仅在已校验的新候选实例、IMPORTING围栏内开启非持久化MySQL维护设置，
结束必须关闭并读回验证；失败不写ready。若进程崩溃，旧机mysqld重启会清除该非持久化
设置，但IMPORTING围栏仍阻止自动覆盖重导，必须管理员处理，不能借重启冒充恢复成功。
MySQL只监听旧电脑127.0.0.1:33085，TLS，4GiB缓冲池，保持刷盘/binlog安全配置。
生成新的本机数据库UUID及密码。旧授权、旧主机身份、生产账户哈希不冒充迁移。
QMT是完整安装目录复制，仍需要在旧机实测券商许可/系统组件；若不能启动，需国金
官方安装包，不能用掘金或MiniQMT替代。QMT既有自动恢复要求版本2.1.19.0和DPI96；
旧机分辨率/显示缩放建议100%，首次人工登录。Windows凭据须本机重新配置。
先确认国金允许双机同时登录：如果会挤掉源端，停止候选登录，不能用源端中断来试跑。

三、独立验收
Windows时区：中国标准时间。插电、有线网络优先；插电睡眠设置为“永不”，保持QMT
所在用户会话登录，不注销。锁屏不等于注销；AtLogOn任务并非无需登录的后台Windows服务。
人工打开 C:\ProBigAEdge\qmt\bin.x64\XtItClient.exe，登录，加载并运行安装的
probiga_big_qmt_bridge 原生只读策略。未调用交易、下单或撤单接口。
执行：
  powershell -NoProfile -ExecutionPolicy Bypass -File .\verify_target.ps1 -InstallRoot C:\ProBigAEdge -Observe
创建的是ProBigA Edge Acceptance验收任务，不是生产任务。可反复执行，文件锁阻止重复。
默认50个代码、最近5交易日日线和分钟历史、小批次串行；全行情覆盖单独验证。
READY只说明一次采集流程完成，盘中PASS和历史完成度另报；休市不视为盘中通过。
可在config.json把sample_size改成0进行全股票历史压力验收，机械盘先完成小样本。
观察至少2个完整交易日（含开盘/收盘），另做旧机重启、网络断开恢复、QMT重启测试。
CPU/内存/机械盘负载要在旧机测，不承诺此硬件满足最终吞吐。

四、AI也纳入范围，但不抢生产问答
登录Codex使用独立目录 C:\ProBigAEdge\ai\codex-home，执行：
  powershell -NoProfile -ExecutionPolicy Bypass -File .\verify_target.ps1 -InstallRoot C:\ProBigAEdge -LoginCodex
首次浏览器/设备授权必须本人完成。不会复制整个个人.codex或auth.json到移动硬盘。
执行verify_target.ps1加 -Ai，探针会用独立Chrome profile访问DeepSeek，本人登录/验证码。
配置config.json的ai.server_url为原生产网站地址。探针只GET健康信息，不claim真实问题。
Codex stock/general分别创建隔离测试会话，不给原生产两个线程写入测试问题。
原stock/general生产历史尚需受支持的导出/导入或fork后验证连续性；不能只复制JSONL
就声称原ID已经迁移。历史验收未完成时，ai-status.json会明确列为阻断接管项。
包内audit/codex-production-threads保留这两个原legacy rollout的只读归档；没有
复制个人其它任务、登录凭据或活跃SQLite/WAL，也不会自动激活归档作为生产线程。
新机127.0.0.1:7890代理不能直接复制成旧机网络方案；逐一确认Codex/DeepSeek/网站可达。
AI推荐池analysis_fast仍由Linux负责，不重新安装已退休的推荐worker。

五、正式切换（另一次有凭据与验收报告的协同操作）
先返回status.json、database-status.json、ai-status.json及旧机资源实测结果。
生产机制绑定原主机、数据库UUID和受保护授权；不得改hostname冒充、跳过检查、
手工清共享锁或直接启动daemon。Linux固定服务位置不变，但必要授权/config协同已获允许。
此安装包不含生产切换脚本：真实新机身份、AI历史、数据追平未验证前不能合法生成授权。
正式接管需要受保护的主机注册、数据库封印/完整性、最终数据追平、任务排空及单写者切换。
旧包是备份时点，不包含之后生产新增数据；不能把它直接顶到13306即算零丢失迁移。
通过后才停源Windows调度/桥接/隧道/AI启动项；保留源数据库与程序作为回退材料。
不会删除或移动Linux服务，也不会提前释放新机上的生产任务。

当前工具限制需明确
QMT官方安装包、券商双机登录策略、旧机实际运行表现、生产AI线程连续性未验证。
目标脚本测试使用mock/静态检查，不代表已经在那台旧电脑安装过。
包准备需要当前有效MySQL管理员认证；已查到的旧source-client.ini认证失败，不能
把普通runtime账号的部分可见导出当完整迁移备份。
