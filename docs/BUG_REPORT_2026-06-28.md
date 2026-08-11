# Bug 检查报告（2026-06-28）

## 检查范围

- 运行完整 pytest 测试集。
- 对主要源码目录执行 Python 编译检查。

## 已发现并修复的问题

### 1. 解禁风险测试使用系统当前日期，导致测试随日期漂移

- 位置：`tests/test_engines.py`
- 现象：`test_event_risk_engine_lifting` 失败，`event_risk_score` 为 `100`，未触发 7 天内解禁风险。
- 原因：测试数据的 `trade_date` 固定为 `2026-05-30`，但解禁日用 `datetime.now() + 3 days` 生成。引擎按 `trade_date` 作为分析锚点计算解禁天数，因此当前日期变化后，解禁日不再落在分析日后的 7 天内。
- 修复：改为基于测试数据的 `trade_date` 生成 `lift_date`，保证测试与引擎的日期锚点一致。

### 2. Pydantic v2 弃用警告

- 位置：`server/engine/schemas.py`
- 现象：测试通过但出现 3 个 `PydanticDeprecatedSince20` 警告。
- 原因：`StockAnalysisResult.to_dict()` 仍使用 Pydantic v1 风格的 `.dict()`。
- 修复：替换为 Pydantic v2 的 `.model_dump()`。

## 验证结果

```text
.venv\Scripts\python.exe -m pytest -q
225 passed in 1.98s
```

```text
.venv\Scripts\python.exe -m compileall -q server biz integrations strategies tools tests
通过，无语法编译错误
```

## 备注

- 本次未回滚或清理工作区中已有的其他未提交改动。
- 未发现新的测试失败项。
