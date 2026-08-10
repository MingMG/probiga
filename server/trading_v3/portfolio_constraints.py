from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Iterable, Mapping


@dataclass(frozen=True)
class PortfolioAdmission:
    accepted: bool
    reason_code: str
    reason: str
    target_weight: float = 0.0
    target_value: float = 0.0
    target_quantity: int = 0
    delta_quantity: int = 0
    order_value: float = 0.0
    actual_delta_weight: float = 0.0
    stop_distance: float = 0.0
    open_risk_cny: float = 0.0
    estimated_roundtrip_cost_pct: float = 0.0


def estimate_roundtrip_cost_pct(
    order_value: float,
    *,
    commission_rate: float,
    minimum_commission: float,
    transfer_fee_rate: float,
    sell_stamp_duty_rate: float,
    slippage_rate: float,
) -> float:
    if order_value <= 0:
        return math.inf
    commission = max(
        float(minimum_commission),
        order_value * float(commission_rate),
    )
    buy_cost = commission + order_value * float(transfer_fee_rate)
    sell_cost = (
        commission
        + order_value * float(transfer_fee_rate)
        + order_value * float(sell_stamp_duty_rate)
    )
    slippage = order_value * float(slippage_rate) * 2.0
    return (buy_cost + sell_cost + slippage) / order_value * 100.0


class PortfolioConstraintState:
    """Shared, deterministic production/research portfolio admission state.

    The state owns only execution-independent portfolio constraints.  Forecast
    construction and fill simulation stay with their respective callers, but
    both production and historical replay must pass through this exact code.
    """

    def __init__(
        self,
        *,
        policy: Mapping[str, Any],
        equity: float,
        risk_asset_cap: float,
        current_theme_weights: Mapping[str, float] | None = None,
        current_position_weights: Mapping[str, float] | None = None,
        current_position_quantities: Mapping[str, int] | None = None,
        current_position_themes: Mapping[str, Iterable[str]] | None = None,
        current_open_risk_weight: float = 0.0,
    ) -> None:
        self.policy = dict(policy)
        self.equity = max(1.0, float(equity))
        self.position_weights = {
            str(code): max(0.0, float(weight))
            for code, weight in (current_position_weights or {}).items()
            if float(weight) > 0
        }
        self.position_quantities = {
            str(code): max(0, int(quantity))
            for code, quantity in (current_position_quantities or {}).items()
        }
        self.position_themes = {
            str(code): {
                str(theme) for theme in themes if str(theme)
            }
            for code, themes in (current_position_themes or {}).items()
        }
        self.theme_weights: defaultdict[str, float] = defaultdict(
            float,
            {
                str(theme): max(0.0, float(weight))
                for theme, weight in (current_theme_weights or {}).items()
            },
        )
        self.planned_weights = dict(self.position_weights)
        self.planned_themes = {
            code: set(themes) for code, themes in self.position_themes.items()
        }
        self.current_invested = min(1.0, sum(self.position_weights.values()))
        self.risk_asset_cap = max(0.0, min(1.0, float(risk_asset_cap)))
        self.remaining_weight = max(
            0.0,
            self.risk_asset_cap - self.current_invested,
        )
        self.remaining_turnover = max(
            0.0,
            float(self.policy["maximum_daily_turnover"]),
        )
        self.total_open_risk_cny = (
            max(0.0, float(current_open_risk_weight)) * self.equity
        )
        self.new_position_count = 0

    @staticmethod
    def _reject(code: str, reason: str) -> PortfolioAdmission:
        messages = {
            "ADDITIONAL_ENTRY_DISABLED": "当前冻结版本不允许加仓",
            "PORTFOLIO_POSITION_CAP": "组合可用持仓数量已满",
            "PRICE_MISSING": "缺少可执行价格",
            "THEME_OR_RISK_BUDGET_FULL": (
                "题材、相关题材、换手或组合风险额度已满"
            ),
            "TARGET_ALREADY_REACHED": "当前持仓已达到本轮风险目标",
            "ORDER_NOT_ECONOMIC": "计划订单低于最小经济订单",
            "NET_EDGE_BELOW_COST_BUFFER": "保守净收益不足往返成本缓冲",
            "PORTFOLIO_OPEN_RISK_CAP": "新增仓位会突破组合开放风险上限",
            "CASH_NOT_AVAILABLE": "现金不足以执行计划订单",
        }
        return PortfolioAdmission(
            accepted=False,
            reason_code=reason,
            reason=messages.get(reason, reason),
        )

    def admit(
        self,
        *,
        stock_code: str,
        price: float,
        initial_stop_pct: float,
        candidate_themes: Iterable[str] = (),
        conservative_return_pct: float | None = None,
        required_edge_pct: float | None = None,
        fees: Mapping[str, Any] | None = None,
        minimum_edge_to_cost_multiple: float | None = None,
        available_cash_cny: float | None = None,
    ) -> PortfolioAdmission:
        code = str(stock_code)
        current_weight = self.position_weights.get(code, 0.0)
        is_new_position = current_weight <= 0
        if (
            not is_new_position
            and int(self.policy.get("maximum_add_count", 0)) <= 0
        ):
            return self._reject(code, "ADDITIONAL_ENTRY_DISABLED")
        if (
            is_new_position
            and len(self.position_weights) + self.new_position_count
            >= int(self.policy["maximum_positions"])
        ):
            return self._reject(code, "PORTFOLIO_POSITION_CAP")
        price = float(price or 0.0)
        if not math.isfinite(price) or price <= 0:
            return self._reject(code, "PRICE_MISSING")

        stop_distance = max(0.02, abs(float(initial_stop_pct)) / 100.0)
        risk_weight_cap = (
            float(self.policy["standard_trade_risk"]) / stop_distance
        )
        maximum_target_weight = min(
            float(self.policy["normal_position_weight"]),
            float(self.policy["maximum_single_position_weight"]),
            risk_weight_cap,
        )
        if is_new_position:
            maximum_target_weight = min(
                maximum_target_weight,
                float(
                    self.policy.get(
                        "initial_probe_position_weight",
                        self.policy["normal_position_weight"],
                    )
                ),
            )
        desired_delta = min(
            max(0.0, maximum_target_weight - current_weight),
            self.remaining_weight,
            self.remaining_turnover,
        )
        themes = {str(theme) for theme in candidate_themes if str(theme)}
        for theme in themes:
            desired_delta = min(
                desired_delta,
                float(self.policy["maximum_theme_weight"])
                - self.theme_weights[theme],
            )
        if themes:
            correlated_weight = sum(
                weight
                for held_code, weight in self.planned_weights.items()
                if themes & self.planned_themes.get(held_code, set())
            )
            desired_delta = min(
                desired_delta,
                float(self.policy["maximum_correlated_theme_weight"])
                - correlated_weight,
            )
        if desired_delta <= 0:
            return self._reject(code, "THEME_OR_RISK_BUDGET_FULL")

        target_weight = current_weight + desired_delta
        target_quantity = math.floor(
            self.equity * target_weight / price / 100
        ) * 100
        current_quantity = self.position_quantities.get(code, 0)
        delta_quantity = max(0, target_quantity - current_quantity)
        order_value = delta_quantity * price
        target_value = target_quantity * price
        if delta_quantity <= 0:
            return self._reject(code, "TARGET_ALREADY_REACHED")
        if order_value < float(self.policy["minimum_economic_order_cny"]):
            return self._reject(code, "ORDER_NOT_ECONOMIC")
        estimated_cost_pct = 0.0
        if fees is not None:
            estimated_cost_pct = estimate_roundtrip_cost_pct(
                order_value,
                commission_rate=float(fees["commission_rate"]),
                minimum_commission=float(fees["minimum_commission_cny"]),
                transfer_fee_rate=float(fees["transfer_fee_rate"]),
                sell_stamp_duty_rate=float(fees["sell_stamp_duty_rate"]),
                slippage_rate=float(fees["default_slippage_rate"]),
            )
            if minimum_edge_to_cost_multiple is not None:
                required_edge_pct = estimated_cost_pct * float(
                    minimum_edge_to_cost_multiple
                )
        if (
            required_edge_pct is not None
            and conservative_return_pct is not None
            and float(conservative_return_pct) <= float(required_edge_pct)
        ):
            return self._reject(code, "NET_EDGE_BELOW_COST_BUFFER")
        if (
            available_cash_cny is not None
            and order_value > max(0.0, float(available_cash_cny))
        ):
            return self._reject(code, "CASH_NOT_AVAILABLE")

        actual_delta_weight = order_value / self.equity
        actual_target_weight = target_value / self.equity
        open_risk_cny = order_value * stop_distance
        if (
            self.total_open_risk_cny + open_risk_cny
            > self.equity * float(self.policy["maximum_open_risk"])
        ):
            return self._reject(code, "PORTFOLIO_OPEN_RISK_CAP")

        self.remaining_weight -= actual_delta_weight
        self.remaining_turnover -= actual_delta_weight
        self.total_open_risk_cny += open_risk_cny
        self.planned_weights[code] = actual_target_weight
        self.planned_themes[code] = themes
        if is_new_position:
            self.new_position_count += 1
        for theme in themes:
            self.theme_weights[theme] += actual_delta_weight
        return PortfolioAdmission(
            accepted=True,
            reason_code="ACCEPTED",
            reason="通过组合约束",
            target_weight=actual_target_weight,
            target_value=target_value,
            target_quantity=target_quantity,
            delta_quantity=delta_quantity,
            order_value=order_value,
            actual_delta_weight=actual_delta_weight,
            stop_distance=stop_distance,
            open_risk_cny=open_risk_cny,
            estimated_roundtrip_cost_pct=estimated_cost_pct,
        )

    @property
    def invested_weight(self) -> float:
        return min(1.0, sum(self.planned_weights.values()))

    @property
    def estimated_one_way_turnover_weight(self) -> float:
        return sum(
            max(
                0.0,
                self.planned_weights.get(code, 0.0)
                - self.position_weights.get(code, 0.0),
            )
            for code in self.planned_weights
        )
