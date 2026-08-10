from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from typing import Any


@dataclass(frozen=True)
class PositionTransition:
    stock_code: str
    previous_state: str
    next_state: str
    action: str
    target_fraction: float
    reason_code: str
    reason: str
    sellable_quantity: int
    add_count: int

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def decide_position_transition(
    *,
    stock_code: str,
    previous_state: str,
    current_quantity: int,
    sellable_quantity: int,
    current_weight: float,
    target_weight: float,
    entry_date: date | None,
    trade_date: date,
    trend_valid: bool,
    hard_stop_triggered: bool,
    forecast_status: str,
    forecast_improving: bool,
    add_count: int,
    maximum_add_count: int = 1,
    signal_evaluation_valid: bool = True,
    explicit_exit_reason: str | None = None,
) -> PositionTransition:
    """Turn the latest evidence into a flexible, T+1-safe position action.

    Holding days are deliberately absent. A position is held while its thesis
    remains valid, reduced when evidence weakens, and exited immediately when
    the trend or hard stop fails. T+1 only delays execution; it never converts
    an invalid thesis back into a hold.
    """

    previous = previous_state or "NONE"
    quantity = max(0, int(current_quantity))
    sellable = max(0, min(quantity, int(sellable_quantity)))
    target = max(0.0, float(target_weight))
    current = max(0.0, float(current_weight))
    locked_t1 = (
        quantity > 0
        and sellable <= 0
        and entry_date is not None
        and entry_date >= trade_date
    )

    exit_required = hard_stop_triggered or not trend_valid
    if exit_required:
        reason_code = (
            "HARD_STOP_TRIGGERED"
            if hard_stop_triggered
            else "HYPOTHESIS_INVALIDATED"
            if explicit_exit_reason == "HYPOTHESIS_INVALIDATED"
            else "TREND_INVALIDATED"
        )
        reason = (
            "价格触发硬止损，退出优先于持有期限"
            if hard_stop_triggered
            else "原交易逻辑已经失效，不再机械持有"
        )
        if locked_t1:
            return PositionTransition(
                stock_code,
                previous,
                "EXIT_PENDING_T1",
                "WAIT_SELLABLE",
                0.0,
                reason_code,
                reason + "；受 T+1 限制，下一可卖时点立即退出",
                sellable,
                add_count,
            )
        return PositionTransition(
            stock_code,
            previous,
            "EXIT",
            "SELL_ALL",
            0.0,
            reason_code,
            reason,
            sellable,
            add_count,
        )

    validated = forecast_status == "VALIDATED_POSITIVE"
    paper_discovery = forecast_status == "PAPER_DISCOVERY_ACTIVE"
    if quantity <= 0:
        if (validated or paper_discovery) and target > 0:
            return PositionTransition(
                stock_code,
                previous,
                (
                    "PAPER_DISCOVERY"
                    if paper_discovery
                    else "PROBE"
                ),
                "BUY_PROBE",
                min(target, 0.5 * target + 0.02),
                (
                    "PAPER_DISCOVERY_PROBE"
                    if paper_discovery
                    else "NEW_POSITIVE_EXPECTANCY"
                ),
                (
                    "仅限模拟盘的小仓前向验证，不代表正期望已通过"
                    if paper_discovery
                    else "扣费后正期望通过，先用试仓验证成交与延续性"
                ),
                0,
                0,
            )
        return PositionTransition(
            stock_code,
            previous,
            "WATCH",
            "NO_TRADE",
            0.0,
            "NO_VALIDATED_EDGE",
            "尚无通过样本外闸门的净收益优势",
            0,
            add_count,
        )

    if paper_discovery and target > 0:
        return PositionTransition(
            stock_code,
            previous,
            "PAPER_DISCOVERY",
            "HOLD",
            min(current, target),
            "PAPER_DISCOVERY_STILL_ACTIVE",
            "模拟盘发现信号仍有效，维持小仓；不允许加仓或转实盘",
            sellable,
            add_count,
        )

    if previous == "PAPER_DISCOVERY" and not signal_evaluation_valid:
        return PositionTransition(
            stock_code,
            previous,
            "PAPER_DISCOVERY",
            "HOLD",
            current,
            "PAPER_DISCOVERY_SIGNAL_UNEVALUATED",
            "本批次数据不足，无法确认试错信号是否结束；保留已成交小仓且禁止加仓",
            sellable,
            add_count,
        )

    if previous == "PAPER_DISCOVERY" and not paper_discovery:
        if locked_t1:
            return PositionTransition(
                stock_code,
                previous,
                "EXIT_PENDING_T1",
                "WAIT_SELLABLE",
                0.0,
                "PAPER_DISCOVERY_SIGNAL_ENDED",
                "模拟试错信号已结束；受 T+1 限制，下一可卖时点退出",
                sellable,
                add_count,
            )
        return PositionTransition(
            stock_code,
            previous,
            "EXIT",
            "SELL_ALL",
            0.0,
            "PAPER_DISCOVERY_SIGNAL_ENDED",
            "模拟试错信号已结束，不把研究仓拖成长期持仓",
            sellable,
            add_count,
        )

    if not validated:
        return PositionTransition(
            stock_code,
            previous,
            "HOLD_TREND",
            "HOLD",
            current,
            "EDGE_UNCONFIRMED_TREND_VALID",
            "最新评分暂未通过开仓闸门，但原趋势未失效，继续持有且不加仓",
            sellable,
            add_count,
        )

    if (
        forecast_improving
        and target > current
        and add_count < maximum_add_count
    ):
        return PositionTransition(
            stock_code,
            previous,
            "ADD_ALLOWED",
            "BUY_TO_TARGET",
            target,
            "EDGE_CONFIRMED_AND_IMPROVING",
            "持仓后证据继续增强，且仍有一次受控加仓额度",
            sellable,
            add_count + 1,
        )

    next_state = "CONFIRMED" if previous == "PROBE" else "HOLD_TREND"
    return PositionTransition(
        stock_code,
        previous,
        next_state,
        "HOLD",
        min(current, target) if target > 0 else current,
        "THESIS_VALID",
        "交易逻辑仍有效，继续持有；不设置机械持有天数",
        sellable,
        add_count,
    )
