"""Code-owned task surface for the quantitative workbench.

Only workflows registered in this module may be started from the workbench.
The execution layer remains the existing production scheduler; this contract
does not create a second executor or grant trading authority.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any


OPERATION_CONTRACT_SCHEMA = "probiga.quant-workbench-operations.v1"
DIRECT_EXECUTION = "DIRECT"
SCHEDULER_TARGET_EXECUTION = "SCHEDULER_TARGET"


_OPERATIONS = (
    {
        "operation_id": "strategy_pool",
        "task_type": "analysis_fast",
        "label": "生成正式策略票池",
        "category": "策略执行",
        "description": "按完整交易日冻结数据运行正式分析，生成并激活可读的策略票池。",
        "execution_mode": SCHEDULER_TARGET_EXECUTION,
        "requires_trade_date": True,
        "result_entry_tab": "recommended",
        "result_label": "查看策略票池",
        "priority": 10,
        "capability_codes": ("S3",),
    },
    {
        "operation_id": "qmt_strategy_simulation",
        "task_type": "qmt_strategy_simulation_daily",
        "label": "QMT 多策略模拟",
        "category": "策略执行",
        "description": "使用正式日频输入运行 10 策略、4 组合的隔离模拟并读取当日结果。",
        "execution_mode": DIRECT_EXECUTION,
        "requires_trade_date": False,
        "result_entry_tab": "qmt-strategy-results",
        "result_label": "查看模拟结果",
        "priority": 20,
        "capability_codes": ("T4",),
    },
    {
        "operation_id": "etf_forward_strategy",
        "task_type": "etf_forward_daily",
        "label": "ETF 前向策略",
        "category": "策略执行",
        "description": "更新双复权 ETF 日线并推进已冻结的前向策略记录。",
        "execution_mode": DIRECT_EXECUTION,
        "requires_trade_date": False,
        "result_entry_tab": "strategy-backtest",
        "result_label": "查看前向记录",
        "priority": 30,
        "capability_codes": ("S1",),
    },
    {
        "operation_id": "daily_review",
        "task_type": "daily_review",
        "label": "盘后量化复盘",
        "category": "交易复盘",
        "description": "重建量化三段式复盘、信号表现和失败样本。",
        "execution_mode": DIRECT_EXECUTION,
        "requires_trade_date": False,
        "result_entry_tab": "review",
        "result_label": "查看复盘",
        "priority": 40,
        "capability_codes": ("T2",),
    },
    {
        "operation_id": "event_risk_watch",
        "task_type": "exposure_event_risk_watch",
        "label": "持仓公告风险扫描",
        "category": "风险",
        "description": "扫描当前暴露股票的官方公告风险，并写回推荐与持仓门禁证据。",
        "execution_mode": DIRECT_EXECUTION,
        "requires_trade_date": False,
        "result_entry_tab": "notice",
        "result_label": "查看公告证据",
        "priority": 50,
        "capability_codes": ("R3", "K4"),
    },
    {
        "operation_id": "quality_post",
        "task_type": "quality_check_post",
        "label": "盘后数据质量审计",
        "category": "数据",
        "description": "核对盘后行情、资金流、覆盖、重复与新鲜度质量门禁。",
        "execution_mode": DIRECT_EXECUTION,
        "requires_trade_date": False,
        "result_entry_tab": "datasource",
        "result_label": "查看质量报告",
        "priority": 60,
        "capability_codes": ("D1",),
    },
    {
        "operation_id": "acquisition_quality",
        "task_type": "acquisition_quality_check",
        "label": "采集完整性巡检",
        "category": "数据",
        "description": "检查自动采集链路、分区覆盖和可重放数据证据。",
        "execution_mode": DIRECT_EXECUTION,
        "requires_trade_date": False,
        "result_entry_tab": "datasource",
        "result_label": "查看巡检证据",
        "priority": 70,
        "capability_codes": ("O2",),
    },
    {
        "operation_id": "v2_reconciliation",
        "task_type": "trading_v2_reconciliation",
        "label": "V2 日终账本对账",
        "category": "交易复盘",
        "description": "核对 V2 隔离模拟账户的信号、订单、成交与资金账本。",
        "execution_mode": DIRECT_EXECUTION,
        "requires_trade_date": False,
        "result_entry_tab": "trading",
        "result_label": "查看对账",
        "priority": 80,
        "capability_codes": ("T3",),
    },
)


def quant_workbench_operation_contract() -> dict[str, Any]:
    operations = [deepcopy(item) for item in _OPERATIONS]
    ids = [item["operation_id"] for item in operations]
    task_types = [item["task_type"] for item in operations]
    if len(ids) != len(set(ids)) or len(task_types) != len(set(task_types)):
        raise RuntimeError("quant workbench operation identity is invalid")
    return {
        "schema": OPERATION_CONTRACT_SCHEMA,
        "operations": operations,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def quant_workbench_operation(operation_id: str) -> dict[str, Any] | None:
    normalized = str(operation_id or "").strip()
    for item in _OPERATIONS:
        if item["operation_id"] == normalized:
            return deepcopy(item)
    return None


def direct_operation_task_types() -> frozenset[str]:
    return frozenset(
        item["task_type"]
        for item in _OPERATIONS
        if item["execution_mode"] == DIRECT_EXECUTION
    )


__all__ = [
    "DIRECT_EXECUTION",
    "OPERATION_CONTRACT_SCHEMA",
    "SCHEDULER_TARGET_EXECUTION",
    "direct_operation_task_types",
    "quant_workbench_operation",
    "quant_workbench_operation_contract",
]
