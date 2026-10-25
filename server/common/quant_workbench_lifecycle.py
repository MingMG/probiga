"""Integrated daily quantitative lifecycle projection.

The projection joins existing evidence domains without creating alternate
decision or execution authorities.  A green stage proves only the stated
condition; it never upgrades a research pool into an executable portfolio.
"""
from __future__ import annotations

from typing import Any, Mapping


LIFECYCLE_SCHEMA = "probiga.quant-workbench-lifecycle.v1"
_BLOCKING_STATES = frozenset(
    {"BLOCKED", "STALE", "WAITING", "UNAVAILABLE", "PARTIAL"}
)


def _status(value: Any) -> str:
    return str(value or "").strip().upper()


def _stage(
    code: str,
    label: str,
    state: str,
    summary: str,
    evidence: Mapping[str, Any],
    *,
    blocking_reason: str = "",
    operation_ids: tuple[str, ...] = (),
    entry_tab: str = "",
    entry_label: str = "",
) -> dict[str, Any]:
    return {
        "code": code,
        "label": label,
        "state": _status(state),
        "summary": str(summary or ""),
        "blocking_reason": str(blocking_reason or ""),
        "evidence": dict(evidence),
        "operation_ids": list(operation_ids),
        "entry_tab": entry_tab,
        "entry_label": entry_label,
    }


def build_quant_workbench_lifecycle(
    *,
    trade_date: str,
    target: Mapping[str, Any],
    outlook: Mapping[str, Any],
    pool: Mapping[str, Any],
    decision_gate: Mapping[str, Any],
    governance: Mapping[str, Any],
    simulation: Mapping[str, Any],
    review: Mapping[str, Any],
) -> dict[str, Any]:
    """Return one honest daily lifecycle and its first actionable blocker."""

    target_ready = target.get("ready") is True
    evidence = dict(target.get("evidence") or {})
    stages = [
        _stage(
            "DATA",
            "完整交易日数据",
            "READY" if target_ready else "BLOCKED",
            (
                f"日线 {evidence.get('kline', 0)} · 资金流 "
                f"{evidence.get('capital_flow', 0)} · 快照 "
                f"{evidence.get('snapshot', 0)}"
                if target_ready
                else "完整交易日输入尚未通过门禁"
            ),
            {
                "trade_date": trade_date,
                "ready": target_ready,
                "reason": str(target.get("reason") or ""),
                **evidence,
            },
            blocking_reason="" if target_ready else str(target.get("reason") or "DATA_NOT_READY"),
            operation_ids=("quality_post", "acquisition_quality"),
            entry_tab="datasource",
            entry_label="查看数据证据",
        ),
        _stage(
            "MARKET",
            "市场状态与情景",
            "READY" if _status(outlook.get("status")) == "READY" else "BLOCKED",
            (
                f"{outlook.get('market_mode_label') or outlook.get('market_mode') or '未知状态'}"
                f" · {outlook.get('tactical_style') or '等待战术模式'}"
                if _status(outlook.get("status")) == "READY"
                else str(outlook.get("reason") or "市场状态不可用")
            ),
            {
                "status": _status(outlook.get("status")),
                "market_mode": outlook.get("market_mode"),
                "market_mode_label": outlook.get("market_mode_label"),
                "tactical_style": outlook.get("tactical_style"),
                "next_trade_date": outlook.get("next_trade_date"),
                "stock_count": len(outlook.get("stocks") or []),
                "stocks": [
                    {
                        "stock_code": item.get("stock_code"),
                        "short_name": item.get("short_name"),
                        "role": item.get("role"),
                    }
                    for item in list(outlook.get("stocks") or [])[:6]
                    if isinstance(item, Mapping)
                ],
            },
            blocking_reason=(
                ""
                if _status(outlook.get("status")) == "READY"
                else str(outlook.get("reason") or "MARKET_STATE_UNAVAILABLE")
            ),
            entry_tab="market-outlook",
            entry_label="查看市场推演",
        ),
        _stage(
            "STRATEGY_POOL",
            "策略候选池",
            "READY" if _status(pool.get("status")) == "ACTIVE" else "BLOCKED",
            (
                f"{pool.get('active_count', 0)} 只 ACTIVE · 分析 {pool.get('analysis_count', 0)} 只"
                if _status(pool.get("status")) == "ACTIVE"
                else str(pool.get("reason") or "没有可证明有效的策略池")
            ),
            {
                "status": _status(pool.get("status")),
                "trade_date": pool.get("trade_date"),
                "run_uid": pool.get("run_uid"),
                "active_count": pool.get("active_count"),
                "analysis_count": pool.get("analysis_count"),
                "model_version": pool.get("model_version"),
            },
            blocking_reason=(
                ""
                if _status(pool.get("status")) == "ACTIVE"
                else str(pool.get("reason") or "STRATEGY_POOL_UNAVAILABLE")
            ),
            operation_ids=("strategy_pool",),
            entry_tab="recommended",
            entry_label="查看策略票池",
        ),
        _stage(
            "CANONICAL_DECISION",
            "规范治理决策",
            (
                "READY"
                if (
                    governance.get("trade_date") == trade_date
                    and _status(governance.get("status")) == "COMPLETED"
                    and governance.get("input_ready") is True
                )
                else "STALE"
                if governance.get("trade_date")
                else "BLOCKED"
            ),
            (
                f"运行 {governance.get('run_uid') or '—'} · "
                f"观察 {governance.get('observation_count', 0)} · "
                f"确认 {governance.get('confirmation_count', 0)} · "
                f"可交易 {governance.get('tradable_count', 0)}"
                if governance.get("trade_date")
                else "尚无规范治理运行"
            ),
            {
                key: governance.get(key)
                for key in (
                    "trade_date",
                    "run_uid",
                    "status",
                    "input_ready",
                    "market_state",
                    "observation_count",
                    "confirmation_count",
                    "tradable_count",
                    "created_at",
                    "finished_at",
                )
                if key in governance
            },
            blocking_reason=(
                ""
                if (
                    governance.get("trade_date") == trade_date
                    and _status(governance.get("status")) == "COMPLETED"
                    and governance.get("input_ready") is True
                )
                else str(
                    governance.get("blocking_reason")
                    or (
                        "CANONICAL_DECISION_TRADE_DATE_STALE"
                        if governance.get("trade_date")
                        else "CANONICAL_DECISION_NOT_RUN"
                    )
                )
            ),
            entry_tab="strategy-center",
            entry_label="打开策略治理",
        ),
        _stage(
            "EXECUTION_GATE",
            "候选执行门禁",
            "READY" if int(decision_gate.get("executable_count") or 0) > 0 else "BLOCKED",
            (
                f"{decision_gate.get('executable_count', 0)} / "
                f"{decision_gate.get('candidate_count', 0)} 只通过全部执行门禁"
            ),
            dict(decision_gate),
            blocking_reason=(
                ""
                if int(decision_gate.get("executable_count") or 0) > 0
                else str(
                    decision_gate.get("blocking_reason")
                    or "NO_CANDIDATE_PASSED_EXECUTION_GATES"
                )
            ),
            entry_tab="trading-v3-candidates",
            entry_label="查看候选决策",
        ),
        _stage(
            "SIMULATION",
            "QMT 策略模拟",
            (
                "READY"
                if _status(simulation.get("status")) in {"COMPLETED", "PARTIAL"}
                else "WAITING"
                if _status(simulation.get("status")) in {"PENDING", "QUEUED", "RUNNING"}
                else "BLOCKED"
            ),
            str(simulation.get("reason") or simulation.get("status") or "等待模拟输入"),
            {
                key: simulation.get(key)
                for key in (
                    "status",
                    "trade_date",
                    "run_uid",
                    "latest_status",
                    "selected_count",
                    "blocked_count",
                    "reason",
                    "owner",
                    "last_run_status",
                    "last_run_at",
                )
                if key in simulation
            },
            blocking_reason=(
                ""
                if _status(simulation.get("status")) in {"COMPLETED", "PARTIAL"}
                else str(simulation.get("reason") or "QMT_SIMULATION_NOT_AVAILABLE")
            ),
            operation_ids=("qmt_strategy_simulation",),
            entry_tab="qmt-strategy-results",
            entry_label="查看模拟结果",
        ),
        _stage(
            "REVIEW",
            "复盘与反馈",
            "READY" if review.get("publish_status") == "ready" else "BLOCKED",
            (
                f"复盘日 {review.get('review_date') or '—'} 已发布"
                if review.get("publish_status") == "ready"
                else f"复盘阻断：{review.get('blocking_reason') or '等待完整行情'}"
            ),
            dict(review),
            blocking_reason=(
                ""
                if review.get("publish_status") == "ready"
                else str(review.get("blocking_reason") or "REVIEW_NOT_READY")
            ),
            operation_ids=("daily_review", "v2_reconciliation"),
            entry_tab="review",
            entry_label="打开量化复盘",
        ),
    ]
    first_blocking = next(
        (item for item in stages if item["state"] in _BLOCKING_STATES),
        None,
    )
    ready_count = sum(1 for item in stages if item["state"] == "READY")
    if first_blocking is None:
        overall = "READY"
    elif any(item["state"] in {"BLOCKED", "STALE"} for item in stages[:5]):
        overall = "BLOCKED"
    else:
        overall = "PARTIAL"
    next_action = None
    if first_blocking is not None:
        next_action = {
            "stage_code": first_blocking["code"],
            "stage_label": first_blocking["label"],
            "reason": first_blocking["blocking_reason"],
            "operation_ids": first_blocking["operation_ids"],
            "entry_tab": first_blocking["entry_tab"],
            "entry_label": first_blocking["entry_label"],
        }
    return {
        "schema": LIFECYCLE_SCHEMA,
        "trade_date": trade_date,
        "overall_status": overall,
        "ready_stage_count": ready_count,
        "stage_count": len(stages),
        "first_blocking_stage": first_blocking["code"] if first_blocking else "",
        "next_action": next_action,
        "stages": stages,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


__all__ = ["LIFECYCLE_SCHEMA", "build_quant_workbench_lifecycle"]
