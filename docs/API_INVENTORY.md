# ProBigA API Inventory

> 当前 FastAPI 端点清单。路由器均在 `server/api/main.py` 以 `/api` 前缀挂载；主页快捷路由除外。

## 认证与通用行为

- 管理/写接口由 `server/api/admin_auth.py` 统一保护。
- 可配置 `PROBIGA_ADMIN_AUTH_ENABLED`；凭证为 `X-ProBigA-Admin-Token` 或 `Authorization: Bearer <token>`。
- SQLAlchemy 异常返回 503；未处理异常返回 500；响应带 `X-ProBigA-Elapsed-Ms`。
- 读接口默认使用当前数据库/专用K线/分钟读库路由；部分热点接口使用进程内TTL缓存。

## 页面与静态入口

| 方法 | 路径 | 作用 |
|---|---|---|
| GET | `/` | 返回 `index.html` |
| GET | `/battle` | 盘中作战快捷入口，返回 `index.html` |
| GET | `/intraday-battle` | 盘中作战快捷入口 |
| GET | `/deploy` | 返回部署控制台 `deploy.html` |
| GET | `/static/*` | 静态资源 |

## `health.py`

| 方法 | 路径 | 参数 | 作用 |
|---|---|---|---|
| GET | `/api/health` | 无 | 基础健康 |
| GET | `/api/health/runtime` | 无 | API/调度/QMT运行时 |
| GET | `/api/health/schema` | 无 | 关键schema/迁移状态 |
| GET | `/api/health/security` | 无 | 管理鉴权和生产安全状态 |
| GET | `/api/health/intraday-readiness` | 无 | 交易时段、实时/分钟准备度 |
| GET | `/api/health/qmt-bridge` | 无 | QMT客户端、SDK、连接和网关 |
| GET | `/api/health/qmt-capabilities` | `force` | QMT能力台账 |
| GET | `/api/health/qmt-core-probe` | `force` | QMT核心数据探测 |

## `hot_data.py`：市场、热点、研究和组合

### 市场与热点

| 方法 | 路径 | 主要参数 |
|---|---|---|
| GET | `/api/hot-data/market-clock` | 无 |
| GET | `/api/hot-data/latest-trade-date` | 无 |
| GET | `/api/hot-data/available-dates` | 无 |
| GET | `/api/hot-data/fused` | `snapshot_date,top` |
| GET | `/api/hot-data/fused-live` | `top` |
| GET | `/api/hot-data/rank-ths` | `snapshot_date,top` |
| GET | `/api/hot-data/pop-rank-east` | `snapshot_date,top` |
| GET | `/api/hot-data/rank-sina` | `top` |
| GET | `/api/hot-data/rank-xq` | `snapshot_date,top` |
| GET | `/api/hot-data/concept-ths` | `snapshot_date` |
| GET | `/api/hot-data/concept-ths-live` | 无 |
| GET | `/api/hot-data/concept-stocks` | `concept_code,trade_date` |
| GET | `/api/hot-data/concept-multi-day` | `stat_date,days,plate_type` |
| GET | `/api/hot-data/multi-day` | `stat_date,days,top` |
| GET | `/api/hot-data/sector-rotation` | `trade_date,days` |
| GET | `/api/hot-data/sector-heat-matrix` | `end_date,days,raw` |
| POST | `/api/hot-data/sector-heat-matrix/sync-today` | `date` |
| POST | `/api/hot-data/sector-heat-upload` | JSON `payload` |
| GET | `/api/sector/movement` | `group_by` |
| GET | `/api/hot-data/capital-flow` | `trade_date,sort,top,stock_code` |
| GET | `/api/hot-data/capital-flow-realtime` | `stock_code` |
| GET | `/api/hot-data/mainforce-analysis` | `stock_code,trade_date` |
| GET | `/api/hot-data/mainforce-scan` | `trade_date,top` |
| GET | `/api/hot-data/market-sentiment` | `days,date,top,include_signal` |
| GET | `/api/hot-data/style-switch-signal` | `date,days` |
| GET | `/api/hot-data/tech-risk-signal` | `date,days` |
| GET | `/api/hot-data/decision-radar` | `date,days` |
| GET | `/api/hot-data/stock-list` | `keyword,price,page,page_size,sort,order` |
| GET | `/api/hot-data/stock-detail` | `stock_code` |
| GET | `/api/hot-data/stock-minute` | `stock_code,trade_date` |
| GET | `/api/hot-data/screen-stocks` | 多个筛选参数：`mode,trade_date,top,min_change,max_change,min_turnover,min_main_flow,min_boards,max_boards,vol_boost,max_from_low,low_lookback,min_chg_trend,limit_pct,trend_days,ma_slope_min,vol_ratio_min,vol_ratio_max,max_60d_gain,new_high_pct` |
| GET | `/api/hot-data/ai-screen` | `query,trade_date,top` |

### 推荐与复盘

| 方法 | 路径 | 主要参数/作用 |
|---|---|---|
| GET | `/api/hot-data/analysis-result` | `stock_code,status,min_short_score,min_long_score,sort_by,page,page_size` |
| GET | `/api/hot-data/recommended-stocks` | `trade_date,strategy,signal_status,start_date,end_date` |
| GET | `/api/hot-data/recommended-stocks/gate` | `execution_time,min_kline_coverage,target_trade_date,check_readiness` |
| GET | `/api/hot-data/recommended-stocks/progress` | 当前运行进度 |
| GET | `/api/hot-data/recommended-stocks/run-history` | `limit` |
| POST | `/api/hot-data/recommended-stocks/run` | `trade_date,min_score,top_n,strict_prev_trade_day,execution_time,min_kline_coverage,auto_repair_missing_kline,refresh_realtime,date_policy` |
| GET | `/api/hot-data/strategy-runtime-params` | `as_of_date` |
| GET | `/api/hot-data/daily-review` | `review_date` |
| GET | `/api/hot-data/daily-review/pro` | `review_date` |
| GET | `/api/hot-data/daily-review-dates` | 无 |
| GET | `/api/hot-data/daily-review/print` | `review_date`，返回可打印HTML |
| GET | `/api/hot-data/daily-review/export` | `review_date`，返回结构化导出 |
| POST | `/api/hot-data/daily-review/generate` | `review_date` |
| GET | `/api/hot-data/research-radar` | `trade_date` |

### 新闻、公告和实时刷新

| 方法 | 路径 | 主要参数 |
|---|---|---|
| GET | `/api/hot-data/news-flash` | `rn,pages,source` |
| GET | `/api/hot-data/news-important` | `pages` |
| GET | `/api/hot-data/news-history` | `source,limit` |
| GET | `/api/hot-data/stock-notices` | `stock_code,limit,include_future` |
| POST | `/api/realtime/refresh` | `only` |
| POST | `/api/monitor/sync-realtime` | 无 |
| GET | `/api/monitor/data` | `date` |
| GET | `/api/hot-data/command-monitor` | `date` |
| GET | `/api/hot-data/fallback-health` | 无 |

### 组合与聚宽策略

| 方法 | 路径 | 主要参数/作用 |
|---|---|---|
| GET | `/api/portfolio/list` | 自选/组合列表 |
| POST | `/api/portfolio/add` | JSON `body`：股票、成本、股数等 |
| DELETE | `/api/portfolio/remove/{stock_code}` | `stock_code` |
| POST | `/api/portfolio/reorder` | JSON `body`：代码顺序 |
| POST | `/api/portfolio/transact/{stock_code}` | `stock_code` + JSON交易体 |
| GET | `/api/portfolio/live` | `force` |
| POST | `/api/portfolio/refresh-prices` | 刷新组合价格 |
| GET | `/api/portfolio/analyze/{stock_code}` | 统一个股分析 |
| GET | `/api/portfolio/analysis-history/{stock_code}` | 分析历史 |
| GET | `/api/strategy/picks/list` | 聚宽策略列表 |
| GET | `/api/strategy/picks/data` | `strategy_name,date` |
| POST | `/api/strategy/picks/sync` | JSON策略同步体 |

### 策略中心

| 方法 | 路径 | 主要参数/作用 |
|---|---|---|
| GET | `/api/strategy-center/overview` | `trade_date,limit`；市场状态、策略卡片、候选池和冲突 |
| GET | `/api/strategy-center/market-state` | `trade_date`；四状态、证据、置信度和数据新鲜度 |
| GET | `/api/strategy-center/strategies` | `trade_date`；十套策略启停、有效权重和指标 |
| POST | `/api/strategy-center/strategies/{strategy_key}/toggle` | JSON `enabled,reason,operator`；需管理员鉴权 |
| GET | `/api/strategy-center/candidates` | `trade_date,strategy,category,market_state,signal_status,signal_direction,risk_level,limit` |
| GET | `/api/strategy-center/stock/{stock_code}` | `trade_date`；单股全部策略信号和裁决 |
| GET | `/api/strategy-center/compare` | `trade_date,strategies`；策略横向比较 |
| GET | `/api/strategy-center/conflicts` | `trade_date,limit`；相反信号和裁决记录 |
| POST | `/api/strategy-center/run` | JSON `trade_date,limit`；生成并保存策略中心运行批次，需管理员鉴权 |

## `sim_trade.py`

| 方法 | 路径 | 主要参数 |
|---|---|---|
| GET | `/api/sim-trade/runtime-config` | 无 |
| GET | `/api/sim-trade/automation-status` | 无 |
| GET | `/api/sim-trade/dashboard` | `trade_mode` |
| GET | `/api/sim-trade/candidates` | `signal_date,trade_mode,limit` |
| GET | `/api/sim-trade/recommendation-summary` | `signal_date,trade_mode,days` |
| GET | `/api/sim-trade/positions` | `strategy_type,trade_mode` |
| GET | `/api/sim-trade/history` | `strategy_type,status,trade_mode,limit` |
| GET | `/api/sim-trade/flow` | `source,strategy_type,stock_code,trade_mode,limit` |
| GET | `/api/sim-trade/stats` | `trade_mode` |
| GET | `/api/sim-trade/orders` | `trade_mode,status,side,limit` |
| GET | `/api/sim-trade/risk-budget` | `trade_mode,trade_date` |
| GET | `/api/sim-trade/events` | `trade_mode,limit` |
| POST | `/api/sim-trade/scan` | 运行一次扫描 |
| POST | `/api/sim-trade/close/{position_id}` | 手动关闭持仓 |
| POST | `/api/sim-trade/forward/start` | `signal_date,trade_date,end_date,reset` |
| POST | `/api/sim-trade/forward/scan` | 运行前向扫描 |
| POST | `/api/sim-trade/backtest` | `start_date,end_date,strategy_types,initial_capital` |
| GET | `/api/sim-trade/backtest/report` | `strategy_types,initial_capital` |

## `scheduler.py` 与 `datasource.py`

| 方法 | 路径 | 主要参数/作用 |
|---|---|---|
| GET | `/api/scheduler/tasks` | 任务列表、状态、下一次运行 |
| GET | `/api/scheduler/quality` | `trade_date,include_realtime,force,fast` |
| POST | `/api/scheduler/tasks/{task_id}/toggle` | 启停任务 |
| POST | `/api/scheduler/tasks/{task_id}/cron` | `task_id,cron_time` |
| POST | `/api/scheduler/tasks/{task_id}/date-param` | `task_id,date_param` |
| POST | `/api/scheduler/tasks/{task_id}/run` | 手动运行 |
| POST | `/api/scheduler/tasks/{task_id}/stop` | 停止运行 |
| GET | `/api/datasource/list` | 任务按提供商/业务类型分组 |
| GET | `/api/datasource/stats` | 数据源统计 |
| GET | `/api/datasource/required-health` | 必需数据健康，`force` |
| GET | `/api/datasource/{task_id}/history` | `task_id,limit` |
| GET | `/api/datasource/{task_id}/log` | `task_id` |
| POST | `/api/datasource/{task_id}/run` | 手动运行数据源任务 |
| POST | `/api/datasource/{task_id}/toggle` | 启停数据源任务 |

## `jq_minute.py`、`commentary.py`、`deploy.py`、`notify.py`

| 方法 | 路径 | 主要参数/作用 |
|---|---|---|
| GET | `/api/jq/minute/status` | `include_quota` |
| POST | `/api/jq/minute/table/ensure` | 确认分钟表 |
| POST | `/api/jq/minute/sync` | `universe,codes,limit,count,batch_size,include_now,include_paused,include_bj,skip_closed,min_coverage,dry_run` |
| POST | `/api/jq/minute/automation/enable` | `universe,codes,limit,count,batch_size,interval_minutes,cron_time,min_coverage,include_now,include_paused,include_bj` |
| POST | `/api/jq/minute/automation/disable` | 停用自动同步 |
| GET | `/api/commentary/profiles` | 股评画像列表 |
| POST | `/api/commentary/assess` | JSON评估请求 |
| POST | `/api/commentary/profiles` | JSON画像配置 |
| POST | `/api/commentary/profiles/{profile_id}/toggle` | 启停画像 |
| POST | `/api/commentary/profiles/{profile_id}/task/ensure` | 确保调度任务 |
| POST | `/api/commentary/profiles/{profile_id}/run` | `profile_id,push,as_of_date` |
| GET | `/api/deploy/status` | 部署状态 |
| POST | `/api/deploy/run` | 触发部署 |
| GET | `/api/deploy/runs/{run_id}` | 查看部署运行 |
| POST | `/api/notify/wecom-test` | 企业微信测试消息 |

## API 迁移注意事项

- `hot_data.py` 是核心聚合路由，迁移时应按页面/领域拆分，而不是按文件整体复制。
- API 查询普遍依赖 MySQL 既有表和字段；先迁移表/数据契约，再迁移路由。
- 管理接口涉及外部任务执行、部署和写操作，必须一起迁移鉴权、审计和超时策略。
- 推荐、复盘、模拟交易接口必须携带明确交易日/模式，不能用默认当前日期替代跨项目比较。
