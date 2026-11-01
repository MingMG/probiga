# 统一复盘与早报

正式链路为：同日未复权行情与质量门禁 → 统一八角色研究 → 盘后报告 → 次日早报更新 → 下一次收盘审计。

## 生成与发布

- `biz/review/generate.py` 是现有 18:00 任务的唯一正式生成入口，写入既有 `st_quant_review_digest`。同日、同规则、同事实且全文哈希一致的产物可以直接复用；`--force` 明确要求重新生成。
- `biz/evening_review/generate.py` 是现有 20:00 晚报的发布入口，只读取指定交易日的正式产物，核对事实、规则和全文身份后推送。没有正式产物就报 `DATA_BLOCKED`。
- `biz/early_briefing/generate.py` 读取权威最近闭市日的盘后研究，结合有时间戳的新增新闻和已采集外围快照，生成下一开市日早报。
- 下一次盘后研究优先审计对应交易日的早报假设，没有早报时才使用前日盘后假设。没有历史假设时写 `UNKNOWN`。

调度任务名称、归属、执行时间与共享数据库结构不变。旧六步分析、旧 AI 提示词、旧 `st_daily_review` 写入路径和无 AI 备用模板已删除。历史数据仍可读取。

## 事实与证据

`quant_digest.py` 保留交易日、复权版本、全市场覆盖和 PIT 行业快照验证。新增事实包括涨跌家数、日收益 ±7% 极端样本、前一日收益前 10% 样本的次日反馈及匹配覆盖、收盘位置和行业内统计身份候选。极端收益不等于涨跌停，收益前排也不直接认定为龙头。

冻结因子只读取前一交易日及之前最近 61 个实际开市日，使用一次批量查询；服务器查询限时 45 秒。缺失历史日不会用更旧日期补成完整。

`market_decision_rules/` 保存 skill、决策模型、模板及结构化输出合同，按内容计算规则身份。八角色共用一次 DeepSeek 结构化请求；仅当输出校验失败且剩余预算足够时，允许附带原响应和具体错误纠正一次。两次请求共用 180 秒总截止，网络失败不自动重试，二次校验失败就终止，没有旧路径回退。UNKNOWN、元数据和旧假设不能冒充当前观察；数字校验使用完整数值和明确的百分比/金额单位换算。

新闻须同时满足发布时间及首次入库时间不晚于报告截止点。外围材料复用已采集快照；只有行情时点晚于前一 A 股收盘且不晚于报告截止的报价才能作为隔夜事实。陈旧或时间不明的快照只保留缺口说明，不向分析输入旧价格。新闻及公告正文作为不可信材料输入，不具有改变系统规则的权限。

## 存储与一致性

盘后研究位于 `factor_validation_json.market_decision`，其 `content` 与 `compact_review` 相同。早报位于同一行的 `factor_validation_json.early_briefing`，保存前日研究身份及完整父快照，不覆盖盘后正文。

早报写入锁定指定行，重新验证父报告身份及版本顺序。早晚报写入后读取数据库中的最终正文和哈希，再交给推送。晚报重跑保留已经生成的早报及其原假设。网页显示盘后正文，并提供下一交易日早报折叠面板；网页与推送使用同一份保存正文。

## 只读样稿

```powershell
python tools/preview_market_decision.py --output-dir artifacts/market-decision-preview
```

该命令调用真实研究服务，只写本地事实检查点、原始研究响应及报告，不写数据库、不推送。若研究阶段失败，可用同目录的 `verified_facts.json` 重放，避免重复扫描行情：

```powershell
python tools/preview_market_decision.py --date YYYY-MM-DD --facts artifacts/market-decision-preview/verified_facts.json --output-dir artifacts/market-decision-preview
```

重放仍会验证指定交易日、事实日期和质量状态，不会将旧日期包装成当日结果。

## 发布边界

本变更属于 **Linux/server**：修改业务报告模块、版本化研究规则、网页渲染及测试；没有数据库 DDL、QMT 协议或跨端调度所有权变化。只发布合并后的 `main` Linux 版本，不部署或重启 Windows/QMT。回滚使用原正式 broker 和旧 Linux 版本，保留已有报告行。
