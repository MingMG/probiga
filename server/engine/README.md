# 统一股票分析引擎

## 概述

统一分析引擎是整个股票分析系统的核心，采用四层引擎架构，保证同一只股票在AI推荐、自选股、全市场、个股详情四个页面看到的分析结果完全一致。

## 四层引擎架构

```
StockAnalysisEngine (统一入口)
├── LongTermEngine    (长线投资引擎) - 判断6个月~3年投资价值
├── ShortTermEngine   (短线交易引擎) - 判断3~20个交易日上涨机会
├── EventRiskEngine   (事件风险引擎) - 实时判断重大事件风险
└── RecommendationGate(推荐资格引擎) - 最终决定推不推荐
```

## 评分体系

### 长线评分 (0-100)

| 维度 | 权重 | 数据来源 |
|------|------|----------|
| 基本面 | 40% | ROE, ROA, 毛利率, 净利率, 资产负债率 |
| 成长性 | 30% | 营收同比, 净利润同比, 近4季度增长趋势 |
| 估值 | 20% | PE(TTM), PB, 历史PE/PB分位 |
| 风险 | 10% | 资产负债率, 解禁, 扫雷, 减持 |

### 短线评分 (0-100)

| 维度 | 权重 | 数据来源 |
|------|------|----------|
| 资金面 | 40% | 主力净流入, 超大单净流入, 5日/20日资金流, 龙虎榜 |
| 技术面 | 30% | MA, MACD, RSI, BOLL, KDJ, 支撑/压力位 |
| 情绪面 | 20% | 热门排名, 概念热度, 量比, 换手率 |
| 事件催化 | 10% | 公告, 新闻, 利好/利空关键词 |

### 推荐状态

| 状态 | 含义 | 触发条件 |
|------|------|----------|
| ALLOW | 允许推荐 | 无重大风险，评分达标 |
| SUSPENDED | 暂停推荐 | 有新公告/高风险事件，等待市场重新定价 |
| BLOCK | 禁止推荐 | 重大风险事件/评分过低 |

### 事件风险等级

| 等级 | 含义 | 示例 |
|------|------|------|
| LOW | 无风险 | - |
| MEDIUM | 中等风险 | 解禁, 高管变动, 业绩下滑 |
| HIGH | 高风险 | 减持, 质押, 诉讼, 处罚 |
| CRITICAL | 重大风险 | 立案, 退市, 暴雷, 造假 |

## 文件结构

```
server/engine/
├── __init__.py              # 包初始化
├── schemas.py               # 统一数据结构定义
├── data_loader.py           # 统一数据加载器
├── scoring.py               # 评分计算工具
├── stock_analysis_engine.py # 统一分析引擎入口
├── long_term_engine.py      # 长线投资引擎
├── short_term_engine.py     # 短线交易引擎
├── event_risk_engine.py     # 事件风险引擎
├── recommendation_gate.py   # 推荐资格引擎
└── README.md                # 本文件
```

## 使用方法

### 1. 单只股票分析

```python
from server.engine.stock_analysis_engine import StockAnalysisEngine

engine = StockAnalysisEngine()
result = engine.analyze('000001', full_data=True)

print(f"长线评分: {result.long_term_score}")
print(f"短线评分: {result.short_term_score}")
print(f"推荐状态: {result.recommend.status}")
print(f"事件风险: {result.event_risk.level}")
```

### 2. 批量分析

```python
codes = ['000001', '000002', '600519']
results = engine.analyze_batch(codes, full_data=True)

for r in results:
    if r.recommend.status == 'ALLOW':
        print(f"{r.stock_code} {r.stock_name}: 短线{r.short_term_score}")
```

### 3. 查询数据库中的分析结果

```sql
-- 查询所有允许推荐的股票，按短线评分排序
SELECT stock_code, stock_name, long_term_score, short_term_score,
       recommend_status, event_risk_level
FROM stock_analysis_result
WHERE analysis_date = CURDATE()
  AND recommend_status = 'ALLOW'
ORDER BY short_term_score DESC
LIMIT 20;
```

## 定时任务

| 任务 | 时间 | 说明 |
|------|------|------|
| 盘后全量分析 | 每个交易日18:30 | 对全市场股票运行四层引擎 |
| 盘中增量更新 | 交易时间内每30分钟 | 只更新自选股和推荐股票 |
| 周末事件风险检测 | 周六周日10:00 | 检测新公告是否导致推荐失效 |

### 手动运行

```bash
# 盘后全量分析
python -m biz.analysis.sync_analysis_result

# 只分析前100只
python -m biz.analysis.sync_analysis_result --limit 100

# 只分析单只股票
python -m biz.analysis.sync_analysis_result --code 000001

# 盘中增量更新
python -m biz.analysis.sync_analysis_incremental

# 周末事件风险检测
python -m biz.analysis.sync_event_risk_check
```

## 部署步骤

### 1. 创建数据库表

```bash
mysql -u root -p probiga < docs/sql/05_stock_analysis_result.sql
```

### 2. 配置定时任务

```bash
mysql -u root -p probiga < docs/sql/06_analysis_scheduled_tasks.sql
```

### 3. 首次运行全量分析

```bash
python -m biz.analysis.sync_analysis_result
```

### 4. 验证结果

```bash
# 运行单元测试
python -m tests.test_engines

# 查询数据库验证
mysql -u root -p probiga -e "SELECT COUNT(*) FROM stock_analysis_result;"
```

## API接口

### 查询统一分析结果

```
GET /api/hot-data/analysis-result
```

参数：
- `stock_code`: 股票代码或名称（可选）
- `status`: 推荐状态筛选（ALLOW/SUSPENDED/BLOCK）
- `min_short_score`: 最低短线评分
- `min_long_score`: 最低长线评分`
- `sort_by`: 排序字段（short_term_score/long_term_score/event_risk_score）
- `page`: 页码
- `page_size`: 每页数量

### 个股详情（已集成）

```
GET /api/hot-data/stock-detail?stock_code=000001
```

返回的 `ai_analysis` 字段包含完整的统一分析结果。

### 自选股分析（已集成）

```
GET /api/portfolio/analyze/000001
```

返回结构化的统一分析结果。

## 注意事项

1. **首次运行较慢**：全量分析5000只股票需要较长时间（约1-2小时），建议在收盘后运行
2. **盘中增量更新**：只更新自选股和推荐股票（约100只），速度较快
3. **事件风险检测**：周末运行可以检测盘后发布的公告，避免"周五推荐周末利空"的问题
4. **向后兼容**：所有API和前端都支持新旧两种格式，确保平滑过渡
