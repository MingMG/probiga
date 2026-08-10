"""Portfolio risk adjudication for the 200,000 CNY V2 paper account."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from .config import load_frozen_json
from .domain import (
    AccountSnapshot,
    InstrumentRule,
    IntentAction,
    OrderSide,
    RiskDecision,
    RiskDecisionStatus,
    TradeIntent,
    decimal_value,
    money,
)


@dataclass(frozen=True)
class PortfolioPolicy:
    version: str
    config_hash: str
    initial_cash: Decimal
    maximum_positions: int
    single_initial_cap: Decimal
    single_absolute_cap: Decimal
    standard_trade_risk: Decimal
    hard_trade_risk_cap: Decimal
    portfolio_open_risk_cap: Decimal
    theme_position_count_cap: int
    theme_exposure_cap: Decimal
    opening_target_fraction: Decimal
    maximum_add_count: int
    quote_max_age_seconds: int
    visible_level1_participation: Decimal
    paper_snapshot_fallback: bool
    paper_snapshot_max_age_seconds: int
    paper_snapshot_slippage_rate: Decimal
    paper_snapshot_max_volume_participation: Decimal
    regime_caps: dict[str, Decimal]
    drawdown_gates: tuple[dict[str, Any], ...]
    fee_profile_version: str
    instrument_rule_version: str

    def drawdown_gate(self, drawdown: Decimal) -> dict[str, Any]:
        ordered = sorted(
            self.drawdown_gates,
            key=lambda item: decimal_value(item["drawdown_gte"]),
            reverse=True,
        )
        for gate in ordered:
            if drawdown >= decimal_value(gate["drawdown_gte"]):
                return gate
        raise RuntimeError("portfolio policy has no drawdown fallback gate")

    def effective_risk_asset_cap(self, regime: str, drawdown: Decimal) -> Decimal:
        regime_cap = self.regime_caps.get(regime, Decimal("0"))
        gate_cap = self.drawdown_gate(drawdown).get("risk_asset_cap")
        return regime_cap if gate_cap is None else min(regime_cap, decimal_value(gate_cap))


def load_portfolio_policy() -> PortfolioPolicy:
    payload, config_hash = load_frozen_json("strategies/portfolio_policy_v2.json")
    paper_execution = payload.get("paper_execution") or {}
    return PortfolioPolicy(
        version=str(payload["policy_version"]),
        config_hash=config_hash,
        initial_cash=decimal_value(payload["initial_cash_cny"]),
        maximum_positions=int(payload["maximum_positions"]),
        single_initial_cap=decimal_value(payload["single_position_initial_cap"]),
        single_absolute_cap=decimal_value(payload["single_position_absolute_cap"]),
        standard_trade_risk=decimal_value(payload["standard_trade_risk"]),
        hard_trade_risk_cap=decimal_value(payload["hard_trade_risk_cap"]),
        portfolio_open_risk_cap=decimal_value(payload["portfolio_open_risk_cap"]),
        theme_position_count_cap=int(payload["theme_position_count_cap"]),
        theme_exposure_cap=decimal_value(payload["theme_exposure_cap"]),
        opening_target_fraction=decimal_value(payload["opening_target_fraction"]),
        maximum_add_count=int(payload["maximum_add_count"]),
        quote_max_age_seconds=int(payload["quote_max_age_seconds"]),
        visible_level1_participation=decimal_value(payload["visible_level1_participation"]),
        paper_snapshot_fallback=bool(
            paper_execution.get("snapshot_fallback", False)
        ),
        paper_snapshot_max_age_seconds=int(
            paper_execution.get("snapshot_max_age_seconds") or 0
        ),
        paper_snapshot_slippage_rate=decimal_value(
            paper_execution.get("snapshot_slippage_rate") or 0
        ),
        paper_snapshot_max_volume_participation=decimal_value(
            paper_execution.get("snapshot_max_volume_participation") or 0
        ),
        regime_caps={
            key: decimal_value(value)
            for key, value in payload["regime_risk_asset_caps"].items()
        },
        drawdown_gates=tuple(payload["drawdown_gates"]),
        fee_profile_version=str(payload.get("fee_profile_version") or ""),
        instrument_rule_version=str(payload.get("instrument_rule_version") or ""),
    )


class RiskAdjudicator:
    def __init__(self, policy: PortfolioPolicy | None = None):
        self.policy = policy or load_portfolio_policy()

    @staticmethod
    def _check(checks: list[dict[str, Any]], code: str, passed: bool, **detail: Any) -> bool:
        checks.append({"code": code, "passed": bool(passed), **detail})
        return bool(passed)

    def adjudicate(
        self,
        intent: TradeIntent,
        account: AccountSnapshot,
        rule: InstrumentRule,
        *,
        market_regime: str,
        current_stock_market_value: Decimal = Decimal("0"),
        liquidity_quantity: int | None = None,
        estimated_fee: Decimal = Decimal("0"),
    ) -> RiskDecision:
        checks: list[dict[str, Any]] = []
        requested = intent.requested_quantity

        if intent.side == OrderSide.SELL:
            approved = min(requested, max(0, intent.current_quantity))
            self._check(checks, "SELL_QUANTITY_AVAILABLE", approved == requested)
            status = (
                RiskDecisionStatus.APPROVED
                if approved == requested
                else RiskDecisionStatus.REDUCED
                if approved > 0
                else RiskDecisionStatus.REJECTED
            )
            return RiskDecision(
                intent_id=intent.intent_id,
                status=status,
                requested_quantity=requested,
                approved_quantity=approved,
                trade_risk=Decimal("0"),
                post_single_weight=Decimal("0"),
                post_total_weight=(
                    max(Decimal("0"), account.current_market_value - current_stock_market_value)
                    / account.equity
                    if account.equity > 0
                    else Decimal("0")
                ),
                post_theme_weight=Decimal("0"),
                post_open_risk=max(Decimal("0"), account.current_open_risk),
                post_cash=account.available_cash,
                checks=tuple(checks),
                first_failure="" if approved else "SELL_QUANTITY_AVAILABLE",
            )

        def reject(code: str) -> RiskDecision:
            return RiskDecision(
                intent_id=intent.intent_id,
                status=RiskDecisionStatus.REJECTED,
                requested_quantity=requested,
                approved_quantity=0,
                trade_risk=Decimal("0"),
                post_single_weight=(
                    current_stock_market_value / account.equity
                    if account.equity > 0
                    else Decimal("0")
                ),
                post_total_weight=(
                    account.current_market_value / account.equity
                    if account.equity > 0
                    else Decimal("0")
                ),
                post_theme_weight=(
                    account.theme_market_values.get(intent.theme_code, Decimal("0"))
                    / account.equity
                    if account.equity > 0
                    else Decimal("0")
                ),
                post_open_risk=account.current_open_risk,
                post_cash=account.available_cash,
                checks=tuple(checks),
                first_failure=code,
            )

        if not self._check(checks, "ACCOUNT_ACTIVE", account.account_status == "ACTIVE"):
            return reject("ACCOUNT_ACTIVE")
        if not self._check(
            checks,
            "RECONCILIATION_PASS",
            account.reconciliation_status == "PASS",
        ):
            return reject("RECONCILIATION_PASS")
        rule_failure = rule.validate_for_buy()
        if not self._check(
            checks,
            "INSTRUMENT_RULE_PASS",
            rule_failure is None,
            failure=rule_failure or "",
        ):
            return reject(rule_failure or "INSTRUMENT_RULE_PASS")
        if not self._check(
            checks,
            "POLICY_EXTERNAL_CONFIG_CONFIRMED",
            bool(self.policy.fee_profile_version and self.policy.instrument_rule_version),
        ):
            return reject("POLICY_EXTERNAL_CONFIG_CONFIRMED")

        gate = self.policy.drawdown_gate(account.drawdown)
        action_allowed = bool(
            gate["allow_open"] if intent.action == IntentAction.OPEN else gate["allow_add"]
        )
        if not self._check(
            checks,
            "DRAWDOWN_GATE",
            action_allowed,
            drawdown=str(account.drawdown),
            gate=gate["account_action"],
        ):
            return reject("DRAWDOWN_GATE")
        if not self._check(checks, "MARKET_REGIME_ALLOWS_RISK", market_regime not in {"EXTREME", "DATA_BLOCKED"}):
            return reject("MARKET_REGIME_ALLOWS_RISK")
        if not self._check(checks, "EQUITY_POSITIVE", account.equity > 0):
            return reject("EQUITY_POSITIVE")
        risk_stop = (
            max(intent.initial_stop, intent.protective_stop)
            if intent.action == IntentAction.ADD
            else intent.initial_stop
        )
        if not self._check(
            checks,
            "STOP_VALID",
            risk_stop > 0 and risk_stop < intent.worst_price,
            risk_stop=str(risk_stop),
        ):
            return reject("STOP_VALID")

        risk_per_share = intent.worst_price - risk_stop
        standard_budget = account.equity * self.policy.standard_trade_risk
        risk_qty = rule.floor_buy_quantity(int(standard_budget / risk_per_share))
        position_cap = (
            self.policy.single_initial_cap
            if intent.action == IntentAction.OPEN
            else self.policy.single_absolute_cap
        )
        position_room = max(
            Decimal("0"),
            account.equity * position_cap - current_stock_market_value,
        )
        position_qty = rule.floor_buy_quantity(int(position_room / intent.worst_price))
        cash_room = max(Decimal("0"), account.available_cash - estimated_fee)
        cash_qty = rule.floor_buy_quantity(int(cash_room / intent.worst_price))
        candidates = [requested, risk_qty, position_qty, cash_qty]
        if liquidity_quantity is not None:
            candidates.append(rule.floor_buy_quantity(max(0, int(liquidity_quantity))))
        approved = rule.floor_buy_quantity(min(candidates))

        if not self._check(
            checks,
            "MINIMUM_BUY_QUANTITY",
            approved >= rule.first_buy_minimum,
            approved_quantity=approved,
        ):
            return reject("MINIMUM_BUY_QUANTITY")

        trade_value = intent.worst_price * approved
        trade_risk = risk_per_share * approved
        post_cash = money(account.available_cash - trade_value - estimated_fee)
        post_single_value = current_stock_market_value + trade_value
        post_total_value = account.current_market_value + trade_value
        theme_current = account.theme_market_values.get(intent.theme_code, Decimal("0"))
        post_theme_value = theme_current + trade_value
        post_single_weight = post_single_value / account.equity
        post_total_weight = post_total_value / account.equity
        post_theme_weight = post_theme_value / account.equity
        post_open_risk = account.current_open_risk + trade_risk
        position_count_after = account.position_count + (
            1 if intent.action == IntentAction.OPEN and intent.current_quantity == 0 else 0
        )
        theme_count_after = account.theme_position_counts.get(intent.theme_code, 0) + (
            1
            if intent.theme_code
            and intent.action == IntentAction.OPEN
            and intent.current_quantity == 0
            else 0
        )
        cap = self.policy.effective_risk_asset_cap(market_regime, account.drawdown)
        hard_checks = (
            ("TRADE_RISK_CAP", trade_risk <= account.equity * self.policy.hard_trade_risk_cap),
            ("CASH_NON_NEGATIVE", post_cash >= 0),
            ("POSITION_COUNT_CAP", position_count_after <= self.policy.maximum_positions),
            ("SINGLE_POSITION_CAP", post_single_weight <= self.policy.single_absolute_cap),
            ("RISK_ASSET_CAP", post_total_weight <= cap),
            ("PORTFOLIO_OPEN_RISK_CAP", post_open_risk <= account.equity * self.policy.portfolio_open_risk_cap),
            ("THEME_COUNT_CAP", not intent.theme_code or theme_count_after <= self.policy.theme_position_count_cap),
            ("THEME_EXPOSURE_CAP", not intent.theme_code or post_theme_weight <= self.policy.theme_exposure_cap),
        )
        for code, passed in hard_checks:
            if not self._check(checks, code, passed):
                return reject(code)

        return RiskDecision(
            intent_id=intent.intent_id,
            status=(
                RiskDecisionStatus.APPROVED
                if approved == requested
                else RiskDecisionStatus.REDUCED
            ),
            requested_quantity=requested,
            approved_quantity=approved,
            trade_risk=money(trade_risk),
            post_single_weight=post_single_weight,
            post_total_weight=post_total_weight,
            post_theme_weight=post_theme_weight,
            post_open_risk=money(post_open_risk),
            post_cash=post_cash,
            checks=tuple(checks),
        )
