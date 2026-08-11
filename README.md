# ProBigA

ProBigA 是一个面向 A 股本地研究的综合数据分析平台。它把股票基础信息、行情、资金流、概念板块、龙虎榜、公告、舆情、财务、QMT/聚宽等数据统一落到 MySQL，再通过分析引擎、复盘脚本、推荐规则、模拟交易和前端看板，形成一套可追踪、可复盘、可验证的投研辅助流程。

本项目只用于数据整理、本地研究和策略验证，不构成投资建议。

## 核心能力

- 数据同步：同步 `si_*` 股票基础信息、`sm_*` 行情与资金流、`st_*` 舆情/龙虎榜/热榜等表。
- 实时链路：支持新浪快照、聚宽分钟线、国金 QMT 数据源和盘中实时轮询。
- 统一分析：按长线评分、短线评分、事件风险、推荐闸门输出结构化股票分析结果。
- AI 推荐：生成推荐池、观察池、风险警报和剔除池，并保留证据链、技术证据和失败归因。
- 市场复盘：生成早报、晚报、专业复盘、市场温度、情绪周期、板块主线和轮动分析。
- 自选股管理：维护自选列表、持仓成本、交易记录、实时盈亏和个股分析。
- 模拟交易：支持信号池、买卖规则、T+1、仓位预算、订单成交、前向模拟和历史回测。
- 平台前端：提供市场监控、热股排行、板块分析、资金流、主力行为、AI 推荐、自选股、模拟交易、数据源管理和调度管理页面。
- 运维部署：提供 FastAPI 服务、内置调度器、数据质量检查、企微推送和部署控制台。

## 目录结构

```text
biz/                 业务任务：数据同步、分析、复盘、早报、晚报、公告、舆情等
server/api/          FastAPI 后端入口和接口路由
server/engine/       统一股票分析引擎、评分、推荐闸门、模拟交易引擎
server/static/       Web 前端页面、样式和交互脚本
integrations/        外部数据源和服务集成，如 QMT、MyQuant、AkShare、企微
tools/               运维脚本、补数脚本、质量检查、调度和部署辅助工具
tests/               单元测试和回归测试
docs/                执行清单、复盘指南、策略规则和项目文档
adata/               本地数据采集库
deploy/              部署和远端运维脚本
```

## 快速启动

### 1. 安装依赖

```powershell
cd "E:\My Code\ProBigA"
pip install -e ./adata
pip install -r requirements-platform.txt
```

### 2. 配置环境变量

```powershell
copy .env.example .env
```

常用配置：

- `MYSQL_URL`：主 MySQL 连接串，数据同步任务和 API 共用。
- `WECOM_WEBHOOK_URL`：企微机器人推送地址。
- `DEEPSEEK_API_KEY`：早报、晚报或 AI 分析所需的大模型 Key。
- `DATA_SOURCE_*` / `SI_*` / `SM_*`：数据源和同步任务开关。
- `QMT_*`：国金 QMT 客户端和 SDK 路径配置。

### 3. 启动平台

```powershell
python -m uvicorn server.api.main:app --reload --host 0.0.0.0 --port 8000
```

打开：

- 平台首页：`http://127.0.0.1:8000/`
- API 文档：`http://127.0.0.1:8000/docs`
- 健康检查：`http://127.0.0.1:8000/api/health`

## 常用命令

### 同步股票基础信息

```powershell
python -m biz.stock_info.sync_stock_info
python -m biz.stock_info.sync_all_code_incremental
```

### 同步行情和资金流

```powershell
python -m biz.stock_market.sync_stock_market --only stock_current
python -m biz.stock_market.sync_stock_market --only stock_kline,index_kline --kline-today
python -m biz.stock_market.realtime_quotes --mysql
```

### 同步舆情、龙虎榜和公告

```powershell
python -m biz.sentiment.sync_sentiment --only hot_concept
python -m biz.sentiment.sync_sentiment --only a_list_daily,a_list_info
python -m biz.notice.sync_notice_em --from-si-all-code --limit 100 --max-pages 2
```

### 生成分析和推荐

```powershell
python -m biz.analysis.sync_analysis_result --top-n 80 --min-score 62
python -m biz.analysis.sync_analysis_fast --strict-prev-trade-day --auto-repair-missing-kline
python -m biz.analysis.sync_analysis_incremental
```

### 复盘和推送

```powershell
python -m biz.review.generate --today
python -m biz.early_briefing.generate --test
python -m biz.evening_review.generate --test
```

### 选股和候选池

```powershell
python tools\screen_stocks.py --list
python tools\screen_stocks.py --mode trend --top 40 --with-context
python tools\screen_stocks.py --mode low_start --top 40 --with-context
python tools\find_buy_candidates.py --top 20 --min-score 70
```

### 模拟交易

```powershell
python -m biz.analysis.sync_sim_trade --prepare-signals
python -m biz.analysis.sync_sim_trade --tick
python -m biz.analysis.sync_sim_trade --ensure-recommendations --tick
```

### 数据质量检查

```powershell
python tools\data_quality_check.py --json
python tools\data_quality_check.py --readiness --include-realtime
python tools\ensure_quality_gate.py
```

## 分析引擎

统一分析引擎位于 `server/engine/`，核心入口是 `StockAnalysisEngine`。它采用四层结构：

- `LongTermEngine`：长线投资价值，关注基本面、成长、估值和风险。
- `ShortTermEngine`：短线交易机会，关注资金、技术、情绪和事件催化。
- `EventRiskEngine`：公告、新闻、解禁、减持、扫雷等事件风险。
- `RecommendationGate`：最终推荐闸门，输出 `ALLOW`、`SUSPENDED` 或 `BLOCK`。

批量推荐的高性能实现主要在 `biz.analysis.sync_analysis_fast`，会整合行情、财务、资金、板块、宏观、技术形态、缠论结构、事件影响、市场宽度、失败样本和运行时阈值。

## 前端页面

启动 API 后，`/` 会加载 `server/static/index.html`。主要页面包括：

- 市场监控
- 热股排行
- 板块分析
- 强势股
- 概念 / 行业
- 龙虎榜
- 个股资金
- 主力行为
- 选股策略
- 聚宽策略
- AI 推荐
- 自选股
- 模拟交易
- 快讯
- 研报雷达
- 个股公告
- 股评监控
- 数据源管理
- 调度管理
- 全市场股票

## 运行约定

- 数据库默认使用 MySQL，库名通常为 `probiga`。
- 日级数据以交易日为锚点，盘前、盘中、盘后口径不要混用。
- 推荐结果必须经过风险闸门，不能只看分数排序。
- 选股结果是候选池，仍需要人工看图形、公告、流动性和业务逻辑。
- 盘中实时数据依赖数据源稳定性，QMT、聚宽、东财、同花顺等接口都可能限流或延迟。
- 新增脚本时建议同步更新 `docs/执行清单.md`。

## 测试

```powershell
python -m pytest -q
python -m compileall -q server biz integrations strategies tools tests
```

## 相关文档

- `docs/执行清单.md`：数据同步、启动和常用任务总索引。
- `docs/复盘与选股指南.md`：如何用库表做复盘和选股。
- `docs/stock_strategy_rules.md`：策略规则、推荐闸门和证据链落地说明。
- `server/engine/README.md`：统一股票分析引擎说明。
- `.env.example`：环境变量示例。

