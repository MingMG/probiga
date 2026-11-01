# 自动研究生成与发布验收

正式入口是 `tools/preview_market_decision.py`。每份报告固定经过 DeepSeek 独立稿、Codex 独立稿、双向审查、DeepSeek 定稿和 Codex 最终复审。入口没有人工响应、人工改稿或 fixture 注入参数；最终正文由已验证的原始定稿确定性渲染。研究队列传输会写任务回执，默认不发布报告，也不会调用企业微信投递。

## 冻结真实输入

`freeze_bundle(facts, review_context, briefing_context, *, next_trade_date, previous_research=None, source_raw=None)` 创建完整输入包。`source_raw` 的三个键为 `facts`、`review_context`、`briefing_context`，值应为各原文件的 `read_bytes()`。原始字节以 base64 封存，并保留大小和 SHA256。仅清除旧研究、早报、策略正文后的量化事实进入模型；原始文件只用于审计。量额隔离在冻结前完成。

输入包保存行情日、下一交易日、原始行情截止、两份 context 的各自观察截止、前期自动研究、事实身份和完整包 hash。前期不存在新的自动研究时填 `None`，不得用旧人工稿填充。`validate_bundle` 会从原始字节重建事实并核对 context，重新计算 manifest hash 不能掩盖事实变更。

已有完整冻结包时运行：

```powershell
python -B tools/preview_market_decision.py --bundle E:\report-inputs\bundle.json --output-dir E:\report-runs
```

不指定 `--bundle` 时，入口读取指定日期已经通过校验的正式量化行、现有行情和新闻 context，并冻结输入。它不会执行新的行情采集或重扫61日量化历史。

## 检查点与恢复

每次新运行在输出根目录创建独立 UUID 子目录。`bundle.json`、`run_receipt.json`、两份检查点、请求前 messages/schema、六阶段原始响应、最终报告及正文均原子落盘。Codex 的原始 CLI JSONL 和进程回执另由 Windows 研究 worker 保存。

```powershell
python -B tools/preview_market_decision.py --resume E:\report-runs\UUID --output-dir E:\report-runs
```

恢复使用原始生成时间、完整事实包及同一规则版本。仅成功的阶段可复用；已经开始或失败的阶段不会重新发起模型请求。终态原因留在 receipt 和 checkpoint 中。输入、规则或发布范围发生变化时拒绝恢复。全流程上限1320秒；每份研究自身上限600秒，开始下一份研究前必须仍有完整阶段预算。

## 发布和真实回读

需要发布时，新运行显式指定：

```powershell
python -B tools/preview_market_decision.py --bundle E:\report-inputs\bundle.json --output-dir E:\report-runs --publish --api-base http://47.113.123.190
```

发布范围在运行开始时固定，恢复同一发布运行时保持相同 `--publish` 和 `--api-base`。发布前再次验证六阶段来源、原始定稿语义、双方审查和最终复审，不能仅凭正文 hash 接受报告。

`persist_report_pair` 在一笔事务中执行：锁住相应调度任务和目标量化行、检查仍有其他报告生产者运行、核对模型调用前留存的整行及事实身份、持久保存旧行备份、写入盘后研究，再依据父报告身份写入早报。后一步覆盖旧早报必须和前一步同事务，防止原量化保存函数保留旧早报而拼成不同版本的报告对。行情截止和量化生成时间不会被改写来绕过已有 UPSERT 保护。

提交后使用独立连接回读两份报告，并检查两份正文对应的企业微信回执为零。之后直接读取 `/api/hot-data/daily-review/quant?review_date=...`，要求精确日期、`fallback=false`、盘后和早报的完整结构及正文逐项一致。只有 API 验收通过才把发布运行标记为 `COMPLETE`；数据库写完但 API 未验收时保留 `DATABASE_COMPLETE`，不能声称系统展示已完成。

## Windows 研究 worker

Windows consumer 使用 `tools/run_codex_web_bridge.py --research-only`，只领取 `research` 队列，使用独立临时会话；不领取股票问答或通用问答，不恢复固定聊天，不使用 DeepSeek 网页兜底。领取代次和 worker 身份绑定租约，等待 CLI 期间每10秒续租。原始输入/输出、CLI session、实际返回的模型信息和内容身份进入回执；未返回的字段明确为 `UNKNOWN`。

研究进程使用专用 `probiga_openai_https` provider，以单次 CLI `-c` 指向官方 `https://chatgpt.com/backend-api/codex`，保持已有 ChatGPT 登录；`requires_openai_auth=true`、`supports_websockets=false`，HTTP和流重连的重试预算均为零，不更改模型选择或用户配置。当前本机 CLI 不允许覆盖保留的 `openai` provider，所以使用单独名称，而不是已经移除的 WebSocket feature flags。字段依据[官方 Codex provider 源码](https://github.com/openai/codex/blob/main/codex-rs/model-provider-info/src/lib.rs)，HTTPS/SSE选择依据[OpenAI Docs](https://developers.openai.com/siwc/token-sharing-open-source/preview-limitations)。CLI进程仍受任务自身总截止约束。

长期归属由 `tools/register_ai_research_worker.ps1` 注册独立隐藏的 `ProBigA AI Research Worker` 任务，并由 `tools/run_ai_research_worker.ps1` 前台托管进程。目标 checkout 必须 tracked clean 且 HEAD 精确等于已经 fetch 的 `origin/main`，可以使用 detached 专用生产目录。状态目录在代码树外，任务绑定 worker 自身内容 hash。注册不会启动任务，也不调整 QMT 调度、启动项或进程。

这条链路涉及 Linux 队列及 Windows consumer 的共同合同，属于跨端发布。代码合并后，必须分别核对两端实际内容身份，再完成受控真实请求、续租、原始响应持久化、完成回执和报告发布验收；仅进程存在或 HTTP 健康不能证明报告已生成。
