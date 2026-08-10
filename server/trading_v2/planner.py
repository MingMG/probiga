"""Deterministic four-slot portfolio competition and trade-chain persistence."""
from __future__ import annotations

import json
import uuid
from dataclasses import replace
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from .config import canonical_json_hash
from .domain import (
    AccountSnapshot,
    InstrumentRule,
    IntentAction,
    OrderSide,
    TradeIntent,
    decimal_value,
)
from .execution import _fee_profile
from .execution_buy_gate import (
    append_buy_gate_binding,
    evaluate_buy_gate,
    load_current_buy_gate,
)
from .legacy_strategy_account_boundary import require_legacy_strategy_account
from .oms import order_idempotency_key
from .policy import RiskAdjudicator, load_portfolio_policy


def _candidate_competition_order_key(
    item: dict[str, Any],
) -> tuple[Any, ...]:
    """Rank proven expectancy first, then paper-trial model strength."""
    expected_lower = item.get("expected_return_lower_bound")
    expected_is_proven = expected_lower not in {None, ""}
    return (
        0 if expected_is_proven else 1,
        -decimal_value(expected_lower),
        -decimal_value(
            item.get("competition_score", item.get("raw_score"))
        ),
        -decimal_value(item.get("raw_score")),
        -decimal_value(item.get("risk_reward_ratio")),
        str(item.get("stock_code") or ""),
        str(item.get("strategy_version") or ""),
    )


def _next_trade_day(connection: Connection, trade_date: date) -> date:
    value = connection.execute(
        text(
            """
            SELECT MIN(trade_date) FROM si_trade_calendar
            WHERE trade_status = 1 AND trade_date > :trade_date
            """
        ),
        {"trade_date": trade_date},
    ).scalar()
    if value is None:
        raise RuntimeError("trade calendar has no next trading day")
    return (
        value
        if isinstance(value, date)
        else date.fromisoformat(str(value)[:10])
    )


def _instrument_rule(
    connection: Connection,
    *,
    stock_code: str,
    effective_date: date,
    rule_version: str = "",
) -> InstrumentRule | None:
    row = connection.execute(
        text(
            """
            SELECT * FROM st_instrument_rule_v2
            WHERE stock_code = :stock_code
              AND effective_from <= :effective_date
              AND (effective_to IS NULL OR effective_to >= :effective_date)
              AND (:rule_version = '' OR rule_version = :rule_version)
            ORDER BY effective_from DESC, rule_version DESC LIMIT 1
            """
        ),
        {
            "stock_code": stock_code,
            "effective_date": effective_date,
            "rule_version": rule_version,
        },
    ).mappings().first()
    if not row:
        return None
    return InstrumentRule(
        stock_code=str(row["stock_code"]),
        rule_version=str(row["rule_version"]),
        security_type=str(row["security_type"]),
        exchange=str(row["exchange_code"]),
        effective_from=row["effective_from"],
        effective_to=row["effective_to"],
        can_buy=bool(row["can_buy"]),
        first_buy_minimum=int(row["first_buy_minimum"]),
        buy_lot_size=int(row["buy_lot_size"]),
        sell_lot_size=int(row["sell_lot_size"]),
        settlement_days=int(row["settlement_days"]),
        tick_size=decimal_value(row["tick_size"]),
        limit_ratio=(
            decimal_value(row["limit_ratio"])
            if row["limit_ratio"] is not None
            else None
        ),
        is_suspended=bool(row["suspended"]),
        permission_required=str(row["permission_required"] or ""),
        permission_confirmed=bool(row["permission_confirmed"]),
        fee_profile_version=str(row["fee_profile_version"] or ""),
    )


def _initial_target_quantity(
    *,
    rule: InstrumentRule,
    raw_target: int,
    worst_price: Decimal,
    equity: Decimal,
    allow_minimum_board_lot: bool,
    minimum_board_lot_max_weight: Decimal,
) -> int:
    """Floor normally, but permit one board lot for a risk-capped retail probe."""
    quantity = rule.floor_buy_quantity(raw_target)
    if quantity > 0 or not allow_minimum_board_lot or equity <= 0:
        return quantity
    minimum_quantity = rule.floor_buy_quantity(rule.first_buy_minimum)
    if minimum_quantity <= 0:
        return 0
    minimum_weight = worst_price * minimum_quantity / equity
    if minimum_weight > minimum_board_lot_max_weight:
        return 0
    return minimum_quantity


def _reserve_pending_entry_exposure(
    account_state: AccountSnapshot,
    pending_rows: list[dict[str, Any]],
) -> tuple[AccountSnapshot, set[str]]:
    """Treat approved but unfilled buys as occupied slots and reserved cash."""
    pending_codes = {
        str(row["stock_code"]).zfill(6) for row in pending_rows
    }
    if not pending_rows:
        return account_state, pending_codes
    reserved_cash = sum(
        (
            decimal_value(row["limit_price"])
            * int(row["remaining_quantity"] or 0)
            for row in pending_rows
        ),
        Decimal("0"),
    )
    pending_open_risk = sum(
        (
            max(
                Decimal("0"),
                decimal_value(row["limit_price"])
                - decimal_value(row["initial_stop"]),
            )
            * int(row["remaining_quantity"] or 0)
            for row in pending_rows
        ),
        Decimal("0"),
    )
    theme_counts = dict(account_state.theme_position_counts)
    theme_values = dict(account_state.theme_market_values)
    for row in pending_rows:
        theme = str(row["theme_code"] or "")
        if not theme:
            continue
        value = (
            decimal_value(row["limit_price"])
            * int(row["remaining_quantity"] or 0)
        )
        theme_values[theme] = (
            theme_values.get(theme, Decimal("0")) + value
        )
    for theme in {
        str(row["theme_code"] or "")
        for row in pending_rows
        if str(row["theme_code"] or "")
    }:
        theme_counts[theme] = theme_counts.get(theme, 0) + len(
            {
                str(row["stock_code"]).zfill(6)
                for row in pending_rows
                if str(row["theme_code"] or "") == theme
            }
        )
    return (
        replace(
            account_state,
            available_cash=max(
                Decimal("0"),
                account_state.available_cash - reserved_cash,
            ),
            current_market_value=(
                account_state.current_market_value + reserved_cash
            ),
            current_open_risk=(
                account_state.current_open_risk + pending_open_risk
            ),
            position_count=(
                account_state.position_count + len(pending_codes)
            ),
            theme_position_counts=theme_counts,
            theme_market_values=theme_values,
        ),
        pending_codes,
    )


def _account_snapshot(
    connection: Connection,
    *,
    account: dict[str, Any],
    trade_date: date,
) -> AccountSnapshot:
    positions = connection.execute(
        text(
            """
            SELECT stock_code,
                   MAX(theme_code) AS theme_code,
                   SUM(
                       remaining_quantity
                       * COALESCE(
                           (
                               SELECT k.close
                               FROM sm_stock_kline k
                               WHERE BINARY k.stock_code =
                                   BINARY st_position_lot_v2.stock_code
                                 AND k.k_type = 1
                                 AND k.adjust_type = 0
                                 AND k.trade_date <= :trade_date
                               ORDER BY k.trade_date DESC
                               LIMIT 1
                           ),
                           cost_price
                       )
                   ) AS value,
                   SUM(GREATEST(
                       0,
                       COALESCE(
                           (
                               SELECT k.close
                               FROM sm_stock_kline k
                               WHERE BINARY k.stock_code =
                                   BINARY st_position_lot_v2.stock_code
                                 AND k.k_type = 1
                                 AND k.adjust_type = 0
                                 AND k.trade_date <= :trade_date
                               ORDER BY k.trade_date DESC
                               LIMIT 1
                           ),
                           cost_price
                       ) - protective_stop)
                       * remaining_quantity) AS open_risk
            FROM st_position_lot_v2
            WHERE account_id = :account_id AND remaining_quantity > 0
            GROUP BY stock_code
            """
        ),
        {
            "account_id": account["account_id"],
            "trade_date": trade_date,
        },
    ).mappings().all()
    market_value = sum(
        (decimal_value(row["value"]) for row in positions),
        Decimal("0"),
    )
    equity = decimal_value(account["cash_balance"]) + market_value
    theme_market_values: dict[str, Decimal] = {}
    theme_position_counts: dict[str, int] = {}
    for row in positions:
        theme_code = str(row["theme_code"] or "")
        if not theme_code:
            continue
        theme_market_values[theme_code] = (
            theme_market_values.get(theme_code, Decimal("0"))
            + decimal_value(row["value"])
        )
        theme_position_counts[theme_code] = (
            theme_position_counts.get(theme_code, 0) + 1
        )
    return AccountSnapshot(
        account_id=str(account["account_id"]),
        equity=equity,
        available_cash=decimal_value(account["cash_balance"]),
        peak_equity=decimal_value(account["peak_equity"]),
        current_market_value=market_value,
        current_open_risk=sum(
            (decimal_value(row["open_risk"]) for row in positions),
            Decimal("0"),
        ),
        position_count=len(positions),
        theme_position_counts=theme_position_counts,
        theme_market_values=theme_market_values,
        reconciliation_status=str(
            connection.execute(
                text(
                    """
                    SELECT status FROM st_reconciliation_v2
                    WHERE account_id = :account_id
                    ORDER BY trade_date DESC, version DESC LIMIT 1
                    """
                ),
                {"account_id": account["account_id"]},
            ).scalar()
            or "MISSING"
        ),
        account_status=str(account["status"]),
    )


def persist_portfolio_competition(
    connection: Connection,
    *,
    run_uid: str,
    trade_date: date,
    account: dict[str, Any],
    market_regime: str,
    candidates: list[dict[str, Any]],
    execution_at: datetime | None = None,
    execution_expires_at: datetime | None = None,
    reason_code: str = "FOUR_SLOT_COMPETITION_WINNER",
) -> dict[str, Any]:
    require_legacy_strategy_account(
        account["account_id"],
        entrypoint="trading_v2.persist_portfolio_competition",
    )
    policy = replace(
        load_portfolio_policy(),
        fee_profile_version=str(account.get("fee_profile_version") or ""),
        instrument_rule_version=str(
            account.get("instrument_rule_version") or ""
        ),
    )
    account_state = _account_snapshot(
        connection,
        account=account,
        trade_date=trade_date,
    )
    if execution_at is None:
        execution_date = _next_trade_day(connection, trade_date)
        earliest_at = datetime.combine(execution_date, time(9, 31))
        expires_at = datetime.combine(execution_date, time(15, 0))
    else:
        execution_date = execution_at.date()
        earliest_at = execution_at
        expires_at = execution_expires_at or datetime.combine(
            execution_date,
            time(14, 55),
        )
        if expires_at <= earliest_at:
            raise ValueError("execution_expires_at must be after execution_at")
    pending_rows = connection.execute(
        text(
            """
            SELECT o.stock_code, o.limit_price,
                   GREATEST(0, o.quantity - o.filled_quantity)
                       AS remaining_quantity,
                   i.theme_code, i.initial_stop
            FROM st_order_v2 o
            JOIN st_trade_intent_v2 i ON i.intent_id = o.intent_id
            WHERE o.account_id = :account_id
              AND o.status IN (
                  'RISK_APPROVED','QUEUED','PARTIALLY_FILLED'
              )
              AND o.expires_at >= :earliest_at
              AND o.quantity > o.filled_quantity
            """
        ),
        {
            "account_id": account["account_id"],
            "earliest_at": earliest_at,
        },
    ).mappings().all()
    account_state, pending_codes = _reserve_pending_entry_exposure(
        account_state,
        [dict(row) for row in pending_rows],
    )
    capacity = max(0, policy.maximum_positions - account_state.position_count)
    ordered = sorted(candidates, key=_candidate_competition_order_key)
    chosen_codes: set[str] = set()
    selected: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    intent_count = 0
    order_count = 0
    for candidate in ordered:
        code = str(candidate["stock_code"])
        if code in pending_codes:
            rejected.append(
                {
                    "stock_code": code,
                    "strategy_version": candidate["strategy_version"],
                    "rejection_code": "DUPLICATE_PENDING_ENTRY",
                }
            )
            continue
        if code in chosen_codes:
            rejected.append(
                {
                    "stock_code": code,
                    "strategy_version": candidate["strategy_version"],
                    "rejection_code": "LOWER_RANKED_SAME_STOCK_SIGNAL",
                }
            )
            continue
        if len(chosen_codes) >= capacity:
            rejected.append(
                {
                    "stock_code": code,
                    "strategy_version": candidate["strategy_version"],
                    "rejection_code": "FOUR_SLOT_COMPETITION_CAP",
                }
            )
            continue
        gate_load = load_current_buy_gate(
            connection,
            decision_run_uid=run_uid,
            strategy_version=str(candidate["strategy_version"]),
            stock_code=code,
            # Daily planning must not see a next-session recommendation that
            # was unavailable at the decision cutoff.  Intraday activation
            # passes its actual execution time and therefore remains current.
            as_of=(
                execution_at
                if execution_at is not None
                else datetime.combine(trade_date, time(23, 59, 59))
            ),
            lock=True,
        )
        gate_decision = evaluate_buy_gate(
            now=earliest_at,
            decision_run_uid=run_uid,
            strategy_version=str(candidate["strategy_version"]),
            stock_code=code,
            bound=gate_load.binding,
            current=gate_load.binding,
        )
        if not gate_decision.allowed:
            rejected.append(
                {
                    "stock_code": code,
                    "strategy_version": candidate["strategy_version"],
                    "rejection_code": (
                        gate_load.reason_code
                        or gate_decision.reason_code
                        or "BUY_GATE_DATA_BLOCKED"
                    ),
                }
            )
            continue
        assert gate_load.binding is not None
        gate_valid_until = datetime.fromisoformat(
            str(gate_load.binding["valid_until"])
        )
        candidate_expires_at = min(expires_at, gate_valid_until)
        if candidate_expires_at <= earliest_at:
            rejected.append(
                {
                    "stock_code": code,
                    "strategy_version": candidate["strategy_version"],
                    "rejection_code": "BUY_GATE_EXPIRED",
                }
            )
            continue
        rule = _instrument_rule(
            connection,
            stock_code=code,
            effective_date=execution_date,
            rule_version=str(
                account.get("instrument_rule_version") or ""
            ),
        )
        if rule is None:
            rejected.append(
                {
                    "stock_code": code,
                    "strategy_version": candidate["strategy_version"],
                    "rejection_code": "INSTRUMENT_RULE_BLOCKED",
                }
            )
            continue
        theme_code = str(candidate.get("theme_code") or "").strip()
        if not theme_code:
            rejected.append(
                {
                    "stock_code": code,
                    "strategy_version": candidate["strategy_version"],
                    "rejection_code": "THEME_CLASSIFICATION_MISSING",
                }
            )
            continue
        worst_price = decimal_value(candidate["entry_price"])
        initial_stop = decimal_value(candidate["initial_stop"])
        if worst_price <= 0 or initial_stop <= 0 or initial_stop >= worst_price:
            rejected.append(
                {
                    "stock_code": code,
                    "strategy_version": candidate["strategy_version"],
                    "rejection_code": "STOP_OR_ENTRY_INVALID",
                }
            )
            continue
        opening_fraction = decimal_value(
            candidate.get(
                "opening_target_fraction",
                policy.opening_target_fraction,
            )
        )
        opening_fraction = max(
            Decimal("0"),
            min(policy.opening_target_fraction, opening_fraction),
        )
        raw_target = int(
            account_state.equity
            * policy.single_initial_cap
            * opening_fraction
            / worst_price
        )
        allow_minimum_board_lot = bool(
            candidate.get("allow_minimum_board_lot")
        )
        target_quantity = _initial_target_quantity(
            rule=rule,
            raw_target=raw_target,
            worst_price=worst_price,
            equity=account_state.equity,
            allow_minimum_board_lot=allow_minimum_board_lot,
            minimum_board_lot_max_weight=decimal_value(
                candidate.get(
                    "minimum_board_lot_max_weight",
                    "0",
                )
            ),
        )
        if target_quantity <= 0:
            rejected.append(
                {
                    "stock_code": code,
                    "strategy_version": candidate["strategy_version"],
                    "rejection_code": (
                        "MINIMUM_BOARD_LOT_EXCEEDS_RADAR_CAP"
                        if allow_minimum_board_lot
                        else "TARGET_QUANTITY_BELOW_MINIMUM_LOT"
                    ),
                }
            )
            continue
        profile = _fee_profile(
            connection,
            version=str(account.get("fee_profile_version") or ""),
            security_type=rule.security_type,
            trade_date=execution_date,
        )
        if profile is None:
            rejected.append(
                {
                    "stock_code": code,
                    "strategy_version": candidate["strategy_version"],
                    "rejection_code": "FEE_PROFILE_UNCONFIRMED",
                }
            )
            continue
        intent_payload = {
            "account_id": account["account_id"],
            "decision_run_uid": run_uid,
            "strategy_version": candidate["strategy_version"],
            "stock_code": code,
            "action": "OPEN",
            "current_quantity": 0,
            "target_quantity": target_quantity,
            "entry_price": str(worst_price),
            "initial_stop": str(initial_stop),
            "execution_at": earliest_at.isoformat(),
            "execution_gate_hash": gate_load.binding["gate_hash"],
            "decision_context_hash": gate_load.binding["context_hash"],
            "execution_gate_valid_until": gate_load.binding["valid_until"],
            "reason_code": str(
                candidate.get("reason_code") or reason_code
            ),
            "intent_version": 1,
        }
        intent_key = canonical_json_hash(intent_payload)
        intent_id = intent_key[:32]
        intent = TradeIntent(
            intent_id=intent_id,
            account_id=str(account["account_id"]),
            decision_run_uid=run_uid,
            strategy_version=str(candidate["strategy_version"]),
            stock_code=code,
            action=IntentAction.OPEN,
            current_quantity=0,
            target_quantity=target_quantity,
            target_weight=(
                worst_price * target_quantity / account_state.equity
                if account_state.equity > 0
                else Decimal("0")
            ),
            earliest_at=earliest_at,
            expires_at=candidate_expires_at,
            limit_price=worst_price,
            worst_price=worst_price,
            initial_stop=initial_stop,
            protective_stop=initial_stop,
            invalidation_condition=str(
                candidate.get("invalidation_condition")
                or "frozen strategy invalidation condition"
            )[:1000],
            reason_code=str(
                candidate.get("reason_code") or reason_code
            )[:100],
            evidence=append_buy_gate_binding(
                tuple(candidate.get("evidence") or ()),
                gate_load.binding,
            ),
            idempotency_key=intent_key,
            theme_code=theme_code,
        )
        risk = RiskAdjudicator(policy).adjudicate(
            intent,
            account_state,
            rule,
            market_regime=market_regime,
            estimated_fee=profile.calculate(
                OrderSide.BUY,
                worst_price * target_quantity,
                quantity=target_quantity,
            ),
        )
        decision_hash = canonical_json_hash(risk.as_dict())
        connection.execute(
            text(
                """
            INSERT INTO st_trade_intent_v2
            (intent_id, account_id, decision_run_uid, strategy_version,
             stock_code, theme_code, action, current_quantity, target_quantity,
                 target_weight, earliest_at, expires_at, limit_price,
                 worst_price, initial_stop, protective_stop,
                 invalidation_condition, reason_code, evidence_json,
                 intent_version, idempotency_key, created_at)
                VALUES
            (:intent_id, :account_id, :run_uid, :strategy_version,
             :stock_code, :theme_code, 'OPEN', 0, :target_quantity,
             :target_weight,
                 :earliest_at, :expires_at, :limit_price, :worst_price,
                 :initial_stop, :protective_stop, :invalidation,
                 :reason_code, :evidence, 1, :idempotency_key, :created_at)
                """
            ),
            {
                "intent_id": intent_id,
                "account_id": account["account_id"],
                "run_uid": run_uid,
                "strategy_version": candidate["strategy_version"],
                "stock_code": code,
                "theme_code": intent.theme_code[:80],
                "target_quantity": target_quantity,
                "target_weight": intent.target_weight,
                "earliest_at": earliest_at,
                "expires_at": candidate_expires_at,
                "limit_price": worst_price,
                "worst_price": worst_price,
                "initial_stop": initial_stop,
                "protective_stop": initial_stop,
                "invalidation": intent.invalidation_condition,
                "reason_code": intent.reason_code,
                "evidence": json.dumps(
                    list(intent.evidence),
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                ),
                "idempotency_key": intent_key,
                "created_at": datetime.now(),
            },
        )
        intent_count += 1
        connection.execute(
            text(
                """
                INSERT INTO st_risk_decision_v2
                (intent_id, decision_status, requested_quantity,
                 approved_quantity, trade_risk, post_single_weight,
                 post_total_weight, post_theme_weight, post_open_risk,
                 post_cash, checks_json, first_failure,
                 decision_hash, created_at)
                VALUES
                (:intent_id, :status, :requested, :approved, :trade_risk,
                 :single_weight, :total_weight, :theme_weight,
                 :open_risk, :post_cash, :checks, :first_failure,
                 :decision_hash, :created_at)
                """
            ),
            {
                "intent_id": intent_id,
                "status": risk.status.value,
                "requested": risk.requested_quantity,
                "approved": risk.approved_quantity,
                "trade_risk": risk.trade_risk,
                "single_weight": risk.post_single_weight,
                "total_weight": risk.post_total_weight,
                "theme_weight": risk.post_theme_weight,
                "open_risk": risk.post_open_risk,
                "post_cash": risk.post_cash,
                "checks": json.dumps(
                    list(risk.checks),
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                ),
                "first_failure": risk.first_failure or None,
                "decision_hash": decision_hash,
                "created_at": datetime.now(),
            },
        )
        if risk.approved_quantity <= 0:
            rejected.append(
                {
                    "stock_code": code,
                    "strategy_version": candidate["strategy_version"],
                    "rejection_code": risk.first_failure
                    or "RISK_REJECTED",
                }
            )
            continue
        order_key = order_idempotency_key(
            account_id=str(account["account_id"]),
            decision_run_uid=run_uid,
            intent_id=intent_id,
            stock_code=code,
            side="BUY",
            target_quantity=risk.approved_quantity,
            intent_version=1,
        )
        order_id = order_key[:32]
        connection.execute(
            text(
                """
                INSERT INTO st_order_v2
                (order_id, account_id, intent_id, stock_code, side,
                 order_type, limit_price, quantity, filled_quantity,
                 status, earliest_at, expires_at, idempotency_key,
                 created_at, updated_at)
                VALUES
                (:order_id, :account_id, :intent_id, :stock_code, 'BUY',
                 'LIMIT', :limit_price, :quantity, 0, 'RISK_APPROVED',
                 :earliest_at, :expires_at, :idempotency_key,
                 :created_at, :created_at)
                """
            ),
            {
                "order_id": order_id,
                "account_id": account["account_id"],
                "intent_id": intent_id,
                "stock_code": code,
                "limit_price": worst_price,
                "quantity": risk.approved_quantity,
                "earliest_at": earliest_at,
                "expires_at": candidate_expires_at,
                "idempotency_key": order_key,
                "created_at": datetime.now(),
            },
        )
        order_count += 1
        chosen_codes.add(code)
        selected.append(
            {
                "stock_code": code,
                "strategy_version": candidate["strategy_version"],
                "theme_code": intent.theme_code,
                "target_quantity": risk.approved_quantity,
                "target_weight": str(risk.post_single_weight),
                "initial_stop": str(initial_stop),
                "trade_risk": str(risk.trade_risk),
                "intent_id": intent_id,
                "order_id": order_id,
            }
        )
        account_state = replace(
            account_state,
            available_cash=risk.post_cash,
            current_market_value=(
                risk.post_total_weight * account_state.equity
            ),
            current_open_risk=risk.post_open_risk,
            position_count=account_state.position_count + 1,
            theme_position_counts={
                **account_state.theme_position_counts,
                **(
                    {
                        intent.theme_code: (
                            account_state.theme_position_counts.get(
                                intent.theme_code, 0
                            )
                            + 1
                        )
                    }
                    if intent.theme_code
                    else {}
                ),
            },
            theme_market_values={
                **account_state.theme_market_values,
                **(
                    {
                        intent.theme_code: (
                            account_state.theme_market_values.get(
                                intent.theme_code, Decimal("0")
                            )
                            + worst_price * risk.approved_quantity
                        )
                    }
                    if intent.theme_code
                    else {}
                ),
            },
        )
    theme_exposure = {
        code: {
            "position_count": account_state.theme_position_counts.get(
                code, 0
            ),
            "market_value": str(value),
            "weight": str(
                value / account_state.equity
                if account_state.equity > 0
                else Decimal("0")
            ),
        }
        for code, value in sorted(
            account_state.theme_market_values.items()
        )
    }
    return {
        "selected": selected,
        "rejected": rejected,
        "intent_count": intent_count,
        "order_count": order_count,
        "target_cash": str(account_state.available_cash),
        "target_risk_asset_weight": str(
            account_state.current_market_value / account_state.equity
            if account_state.equity > 0
            else Decimal("0")
        ),
        "worst_case_loss": str(account_state.current_open_risk),
        "theme_exposure": theme_exposure,
    }
