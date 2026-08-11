# 开源项目借鉴记录

调研时间：2026-07-05

## 参考项目

- [microsoft/qlib](https://github.com/microsoft/qlib)：AI-oriented quant platform，强调从数据处理、建模、回测到执行的完整流水线；可借鉴的是把“数据质量/流程状态”放到研究工作流前面。
- [vnpy/vnpy](https://github.com/vnpy/vnpy)：Python 开源量化交易平台，偏交易运行时和策略执行；可借鉴的是运行状态、调度/交易环境健康度要在操作前可见。
- [tkfy920/qstock](https://github.com/tkfy920/qstock)：个人量化投研分析包，覆盖数据获取、可视化、选股、回测；可借鉴的是用规整化数据接口支撑选股和图表。
- [ZhuLinsen/daily_stock_analysis](https://github.com/ZhuLinsen/daily_stock_analysis)：LLM 驱动的多市场股票分析系统，强调决策仪表盘、风险警报、操作检查清单和自动推送；可借鉴的是“结论 + 风险 + 动作”的报告结构。
- [ArvinLovegood/go-stock](https://github.com/ArvinLovegood/go-stock)：AI 股票分析工具，支持行情、新闻、资金、财务、涨跌提醒和本地数据保存；可借鉴的是本地化数据和多源 AI 分析入口。
- [leaosunday/horacequant](https://github.com/leaosunday/horacequant)：面向 A 股的个人量化项目，提供行情入库、TDX 公式选股、FastAPI 前端展示；可借鉴的是轻量 FastAPI + 前端看板方式。
- [ling-0729/KHunter](https://github.com/ling-0729/KHunter)：A 股量化狩猎系统，强调候选池、策略命中、多维评分、风险过滤和交易计划；可借鉴的是把“发现机会”和“执行条件”放到同一个操作页。

## 对 ProBigA 的取舍

ProBigA 已经有 A 股数据入库、热榜、复盘、推荐、模拟交易、研报雷达和调度管理。继续照搬完整框架没有必要，最应该补的是“信号可信度前置”：

- 推荐结果之前先看关键数据任务是否正常。
- 盘中操作之前先看实时行情源是否可用。
- 自动化依赖调度，调度状态要在决策首页暴露。
- AI 推荐的日期、新鲜度、回退状态要明确显示。

## 本次落地

- 在智能决策页新增“系统可信度”面板。
- 聚合 `/api/datasource/required-health`、`/api/scheduler/tasks`、`/api/health/qmt-bridge` 和推荐数据 freshness。
- 用 100 分状态展示：高可信可进入决策；中等可信需复核；低可信先补数据。
- 所有新请求都有短超时和兜底，不会因为某个健康接口失败而拖垮智能首页。
- 新增“狩猎场”页面，借鉴 go-stock 的卡片化 AI 分析入口和 KHunter 的狩猎候选池：
  - 候选池来自现有 `/api/hot-data/recommended-stocks`。
  - 展示交易分、策略类型、买点状态、买区/止损/目标、五维评分、入场条件、退出规则、风险标签。
  - 支持按策略、状态、最低分筛选。
  - 支持直接打开 K 线、加入自选、跳转 AI 推荐详情。

## 许可证边界

本次只借鉴公开项目的产品和架构思路，没有复制外部项目代码、提示词、样式或数据。
