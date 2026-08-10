"""Daily dynamic-position monitor that creates early risk exits."""
from __future__ import annotations

import json
from dataclasses import replace
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .config import canonical_json_hash
from .domain import (
    IntentAction,
    OrderSide,
    PositionFacts,
    PositionState,
    RiskDecisionStatus,
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
from .planner import _account_snapshot, _instrument_rule
from .policy import RiskAdjudicator, load_portfolio_policy
from .positions import evaluate_position


def _next_trade_day(connection, trade_date: date) -> date:
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
        raise RuntimeError("trade calendar has no next session")
    return (
        value
        if isinstance(value, date)
        else date.fromisoformat(str(value)[:10])
    )


def _market_facts(
    connection,
    *,
    stock_code: str,
    trade_date: date,
    strategy_version: str,
    current_price: Decimal | None = None,
) -> dict[str, Any] | None:
    rows = connection.execute(
        text(
            """
            SELECT trade_date, close, volume
            FROM sm_stock_kline
            WHERE stock_code = :stock_code
              AND k_type = 1 AND adjust_type = 0
              AND trade_date <= :trade_date
            ORDER BY trade_date DESC LIMIT 80
            """
        ),
        {"stock_code": stock_code, "trade_date": trade_date},
    ).mappings().all()
    if not rows:
        return None
    closes = [decimal_value(row["close"]) for row in reversed(rows)]
    last = rows[0]

    def average(period: int) -> Decimal | None:
        if len(closes) < period:
            return None
        return sum(closes[-period:], Decimal("0")) / Decimal(period)

    ma5, ma10, ma20, ma60 = (
        average(5),
        average(10),
        average(20),
        average(60),
    )
    close = (
        decimal_value(current_price)
        if current_price is not None and decimal_value(current_price) > 0
        else decimal_value(last["close"])
    )
    key = strategy_version.split(":")[-1]
    if key == "ultra_short":
        trend_valid = ma5 is not None and close >= ma5
        proposed = ma5 * Decimal("0.98") if ma5 else close
    elif key == "short_term":
        trend_valid = (
            ma10 is not None
            and ma20 is not None
            and (close >= ma10 or close >= ma20)
        )
        proposed = ma10 * Decimal("0.98") if ma10 else close
    elif key == "main_wave":
        trend_valid = ma20 is not None and close >= ma20
        proposed = ma20 * Decimal("0.97") if ma20 else close
    elif strategy_version.startswith("sector_preheat"):
        trend_valid = (
            ma5 is not None
            and ma10 is not None
            and (close >= ma5 or close >= ma10)
        )
        proposed = ma5 * Decimal("0.97") if ma5 else close
    else:
        trend_valid = ma60 is not None and close >= ma60
        proposed = ma20 * Decimal("0.95") if ma20 else close
    trend_strong = bool(
        ma5 is not None
        and ma10 is not None
        and ma20 is not None
        and close >= ma5 >= ma10 >= ma20
    )
    return {
        "last_price": close,
        "proposed_stop": proposed,
        "trend_valid": trend_valid,
        "trend_strong": trend_strong,
        "liquidity_available": decimal_value(last["volume"]) > 0,
        "ma5": ma5,
        "ma10": ma10,
        "ma20": ma20,
        "ma60": ma60,
    }


def _sector_position_facts(
    connection,
    *,
    theme_code: str,
    trade_date: date,
) -> dict[str, Any]:
    theme = str(theme_code or "").strip()
    if not theme:
        return {"available": False, "strong": False, "broken": False}
    try:
        row = connection.execute(
            text(
                """
                SELECT snapshot_at, direction, score, breadth_pct
                FROM sm_market_radar_sector
                WHERE sector_code = :theme_code
                LIMIT 1
                """
            ),
            {"theme_code": theme},
        ).mappings().first()
    except Exception:
        row = None
    if not row or not row.get("snapshot_at"):
        return {"available": False, "strong": False, "broken": False}
    snapshot_at = row["snapshot_at"]
    if isinstance(snapshot_at, str):
        try:
            snapshot_at = datetime.fromisoformat(snapshot_at)
        except ValueError:
            return {"available": False, "strong": False, "broken": False}
    if snapshot_at.date() != trade_date:
        return {"available": False, "strong": False, "broken": False}
    direction = str(row.get("direction") or "").upper()
    score = decimal_value(row.get("score"))
    breadth = decimal_value(row.get("breadth_pct"))
    return {
        "available": True,
        "strong": (
            direction == "UP"
            and score >= Decimal("20")
            and breadth >= Decimal("10")
        ),
        "broken": (
            direction == "DOWN"
            or score <= Decimal("-20")
            or breadth <= Decimal("-10")
        ),
        "direction": direction,
        "score": score,
        "breadth_pct": breadth,
    }


def _persist_exit_chain(
    connection,
    *,
    account_id: str,
    run_uid: str,
    strategy_version: str,
    stock_code: str,
    current_quantity: int,
    target_quantity: int,
    earliest_at: datetime,
    expires_at: datetime,
    limit_price: Decimal,
    initial_stop: Decimal,
    protective_stop: Decimal,
    invalidation: str,
    reason_code: str,
    now: datetime,
) -> dict[str, Any]:
    active_order = connection.execute(
        text(
            """
            SELECT order_id FROM st_order_v2
            WHERE account_id = :account_id
              AND stock_code = :stock_code
              AND side = 'SELL'
              AND status IN
                  ('RISK_APPROVED','QUEUED','PARTIALLY_FILLED')
            ORDER BY created_at, order_id LIMIT 1
            """
        ),
        {"account_id": account_id, "stock_code": stock_code},
    ).scalar()
    if active_order:
        return {
            "status": "idempotent_active_order",
            "order_id": str(active_order),
        }
    action = (
        IntentAction.EXIT
        if target_quantity == 0
        else IntentAction.REDUCE
    )
    payload = {
        "account_id": account_id,
        "run_uid": run_uid,
        "strategy_version": strategy_version,
        "stock_code": stock_code,
        "action": action.value,
        "current_quantity": current_quantity,
        "target_quantity": target_quantity,
        "protective_stop": str(protective_stop),
    }
    intent_key = canonical_json_hash(payload)
    intent_id = intent_key[:32]
    existing = connection.execute(
        text(
            """
            SELECT intent_id FROM st_trade_intent_v2
            WHERE idempotency_key = :key
            """
        ),
        {"key": intent_key},
    ).scalar()
    if existing:
        return {"status": "idempotent_hit", "intent_id": existing}
    target_weight = Decimal("0")
    connection.execute(
        text(
            """
            INSERT INTO st_trade_intent_v2
            (intent_id, account_id, decision_run_uid, strategy_version,
             stock_code, action, current_quantity, target_quantity,
             target_weight, earliest_at, expires_at, limit_price,
             worst_price, initial_stop, protective_stop,
             invalidation_condition, reason_code, evidence_json,
             intent_version, idempotency_key, created_at)
            VALUES
            (:intent_id, :account_id, :run_uid, :strategy_version,
             :stock_code, :action, :current_quantity, :target_quantity,
             :target_weight, :earliest_at, :expires_at, :limit_price,
             :limit_price, :initial_stop, :protective_stop,
             :invalidation, :reason_code, :evidence,
             1, :idempotency_key, :created_at)
            """
        ),
        {
            "intent_id": intent_id,
            "account_id": account_id,
            "run_uid": run_uid,
            "strategy_version": strategy_version,
            "stock_code": stock_code,
            "action": action.value,
            "current_quantity": current_quantity,
            "target_quantity": target_quantity,
            "target_weight": target_weight,
            "earliest_at": earliest_at,
            "expires_at": expires_at,
            "limit_price": limit_price,
            "initial_stop": initial_stop,
            "protective_stop": protective_stop,
            "invalidation": invalidation,
            "reason_code": reason_code,
            "evidence": json.dumps(
                [
                    {
                        "module": "position_monitor_v2",
                        "reason_code": reason_code,
                        "generated_at": now.isoformat(
                            timespec="seconds"
                        ),
                    }
                ],
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
            "idempotency_key": intent_key,
            "created_at": now,
        },
    )
    requested = current_quantity - target_quantity
    risk_payload = {
        "intent_id": intent_id,
        "status": "APPROVED",
        "requested": requested,
        "approved": requested,
        "reason": "sell-side position quantity check",
    }
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
            (:intent_id, :status, :requested, :approved,
             0, 0, 0, 0, 0, 0, :checks, NULL, :hash, :created_at)
            """
        ),
        {
            "intent_id": intent_id,
            "status": RiskDecisionStatus.APPROVED.value,
            "requested": requested,
            "approved": requested,
            "checks": json.dumps(
                [
                    {
                        "code": "SELL_TARGET_BELOW_CURRENT",
                        "passed": requested > 0,
                    }
                ],
                separators=(",", ":"),
            ),
            "hash": canonical_json_hash(risk_payload),
            "created_at": now,
        },
    )
    order_key = order_idempotency_key(
        account_id=account_id,
        decision_run_uid=run_uid,
        intent_id=intent_id,
        stock_code=stock_code,
        side="SELL",
        target_quantity=requested,
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
            (:order_id, :account_id, :intent_id, :stock_code, 'SELL',
             'LIMIT', :limit_price, :quantity, 0, 'RISK_APPROVED',
             :earliest_at, :expires_at, :idempotency_key,
             :created_at, :created_at)
            """
        ),
        {
            "order_id": order_id,
            "account_id": account_id,
            "intent_id": intent_id,
            "stock_code": stock_code,
            "limit_price": limit_price,
            "quantity": requested,
            "earliest_at": earliest_at,
            "expires_at": expires_at,
            "idempotency_key": order_key,
            "created_at": now,
        },
    )
    return {
        "status": "created",
        "intent_id": intent_id,
        "order_id": order_id,
        "action": action.value,
        "quantity": requested,
    }


def _persist_add_chain(
    connection,
    *,
    account: dict[str, Any],
    trade_date: date,
    run_uid: str,
    market_regime: str,
    strategy_version: str,
    stock_code: str,
    theme_code: str,
    current_quantity: int,
    target_quantity: int,
    last_price: Decimal,
    initial_stop: Decimal,
    protective_stop: Decimal,
    invalidation: str,
    signal: dict[str, Any] | None,
    now: datetime,
) -> dict[str, Any]:
    if signal is None:
        return {
            "status": "BLOCKED",
            "reason_code": "ADD_CURRENT_SIGNAL_MISSING",
        }
    lifecycle = str(signal.get("lifecycle_status") or "")
    if lifecycle not in {"PAPER_TRIAL", "PAPER_ACTIVE"}:
        return {
            "status": "BLOCKED",
            "reason_code": "ADD_STRATEGY_NOT_PAPER_ENABLED",
        }
    expected_lower = signal.get("expected_return_lower_bound")
    if (
        lifecycle == "PAPER_ACTIVE"
        and (
            expected_lower is None
            or decimal_value(expected_lower) <= 0
        )
    ):
        return {
            "status": "BLOCKED",
            "reason_code": "ADD_EXPECTED_RETURN_LOWER_BOUND_NOT_POSITIVE",
        }
    raw_features = json.loads(str(signal.get("raw_features_json") or "{}"))
    if not isinstance(raw_features, dict):
        raw_features = {}
    if (
        str(signal.get("action") or "").upper() != "BUY"
        or str(raw_features.get("signal_status") or "").upper() != "READY"
        or str(raw_features.get("gate_status") or "").upper() == "BLOCK"
    ):
        return {
            "status": "BLOCKED",
            "reason_code": "ADD_SIGNAL_NOT_CONFIRMED",
        }
    if str(raw_features.get("risk_level") or "").upper() in {
        "HIGH",
        "CRITICAL",
    }:
        return {
            "status": "BLOCKED",
            "reason_code": "ADD_SECURITY_EVENT_RISK",
        }
    gate_load = load_current_buy_gate(
        connection,
        decision_run_uid=run_uid,
        strategy_version=strategy_version,
        stock_code=stock_code,
        as_of=now,
        lock=True,
    )
    gate_decision = evaluate_buy_gate(
        now=now,
        decision_run_uid=run_uid,
        strategy_version=strategy_version,
        stock_code=stock_code,
        bound=gate_load.binding,
        current=gate_load.binding,
    )
    if not gate_decision.allowed:
        return {
            "status": "BLOCKED",
            "reason_code": (
                gate_load.reason_code
                or gate_decision.reason_code
                or "BUY_GATE_DATA_BLOCKED"
            ),
        }
    assert gate_load.binding is not None
    next_day = _next_trade_day(connection, trade_date)
    rule = _instrument_rule(
        connection,
        stock_code=stock_code,
        effective_date=next_day,
        rule_version=str(
            account.get("instrument_rule_version") or ""
        ),
    )
    if rule is None:
        return {
            "status": "BLOCKED",
            "reason_code": "INSTRUMENT_RULE_BLOCKED",
        }
    profile = _fee_profile(
        connection,
        version=str(account.get("fee_profile_version") or ""),
        security_type=rule.security_type,
        trade_date=next_day,
    )
    if profile is None:
        return {
            "status": "BLOCKED",
            "reason_code": "FEE_PROFILE_UNCONFIRMED",
        }
    requested_target = max(current_quantity, target_quantity)
    requested_add = rule.floor_buy_quantity(
        requested_target - current_quantity
    )
    if requested_add <= 0:
        return {
            "status": "BLOCKED",
            "reason_code": "ADD_LEGAL_QUANTITY_ZERO",
        }
    theme_code = str(theme_code or signal.get("theme_code") or "").strip()
    if not theme_code:
        return {
            "status": "BLOCKED",
            "reason_code": "THEME_CLASSIFICATION_MISSING",
        }
    active_order = connection.execute(
        text(
            """
            SELECT o.order_id
            FROM st_order_v2 o
            JOIN st_trade_intent_v2 i ON i.intent_id = o.intent_id
            WHERE o.account_id = :account_id
              AND o.stock_code = :stock_code
              AND o.side = 'BUY'
              AND i.action = 'ADD'
              AND o.status IN
                  ('RISK_APPROVED','QUEUED','PARTIALLY_FILLED')
            ORDER BY o.created_at, o.order_id LIMIT 1
            """
        ),
        {
            "account_id": account["account_id"],
            "stock_code": stock_code,
        },
    ).scalar()
    if active_order:
        return {
            "status": "idempotent_active_order",
            "order_id": str(active_order),
        }
    earliest_at = datetime.combine(next_day, time(9, 31))
    expires_at = min(
        datetime.combine(next_day, time(15, 0)),
        datetime.fromisoformat(str(gate_load.binding["valid_until"])),
    )
    if expires_at <= earliest_at:
        return {
            "status": "BLOCKED",
            "reason_code": "BUY_GATE_EXPIRED",
        }
    account_state = _account_snapshot(
        connection,
        account=account,
        trade_date=trade_date,
    )
    intent_payload = {
        "account_id": account["account_id"],
        "decision_run_uid": run_uid,
        "strategy_version": strategy_version,
        "stock_code": stock_code,
        "theme_code": theme_code,
        "action": "ADD",
        "current_quantity": current_quantity,
        "target_quantity": current_quantity + requested_add,
        "last_price": str(last_price),
        "initial_stop": str(initial_stop),
        "protective_stop": str(protective_stop),
        "execution_gate_hash": gate_load.binding["gate_hash"],
        "decision_context_hash": gate_load.binding["context_hash"],
        "execution_gate_valid_until": gate_load.binding["valid_until"],
        "intent_version": 1,
    }
    intent_key = canonical_json_hash(intent_payload)
    intent_id = intent_key[:32]
    intent = TradeIntent(
        intent_id=intent_id,
        account_id=str(account["account_id"]),
        decision_run_uid=run_uid,
        strategy_version=strategy_version,
        stock_code=stock_code,
        action=IntentAction.ADD,
        current_quantity=current_quantity,
        target_quantity=current_quantity + requested_add,
        target_weight=(
            last_price * (current_quantity + requested_add)
            / account_state.equity
            if account_state.equity > 0
            else Decimal("0")
        ),
        earliest_at=earliest_at,
        expires_at=expires_at,
        limit_price=last_price,
        worst_price=last_price,
        initial_stop=initial_stop,
        protective_stop=protective_stop,
        invalidation_condition=invalidation[:1000],
        reason_code="VALID_STRONG_ONE_TIME_ADD",
        evidence=append_buy_gate_binding(
            (
                {
                    "module": "position_monitor_v2",
                    "signal_result_hash": str(
                        signal.get("config_hash") or ""
                    ),
                    "expected_return_lower_bound": str(expected_lower),
                    "generated_at": now.isoformat(timespec="seconds"),
                },
            ),
            gate_load.binding,
        ),
        idempotency_key=intent_key,
        theme_code=theme_code,
    )
    policy = replace(
        load_portfolio_policy(),
        fee_profile_version=str(
            account.get("fee_profile_version") or ""
        ),
        instrument_rule_version=str(
            account.get("instrument_rule_version") or ""
        ),
    )
    estimated_fee = profile.calculate(
        OrderSide.BUY,
        last_price * requested_add,
        quantity=requested_add,
    )
    risk = RiskAdjudicator(policy).adjudicate(
        intent,
        account_state,
        rule,
        market_regime=market_regime,
        current_stock_market_value=last_price * current_quantity,
        estimated_fee=estimated_fee,
    )
    connection.execute(
        text(
            """
            INSERT INTO st_trade_intent_v2
            (intent_id, account_id, decision_run_uid, strategy_version,
             stock_code, theme_code, action, current_quantity,
             target_quantity, target_weight, earliest_at, expires_at,
             limit_price, worst_price, initial_stop, protective_stop,
             invalidation_condition, reason_code, evidence_json,
             intent_version, idempotency_key, created_at)
            VALUES
            (:intent_id, :account_id, :run_uid, :strategy_version,
             :stock_code, :theme_code, 'ADD', :current_quantity,
             :target_quantity, :target_weight, :earliest_at, :expires_at,
             :limit_price, :worst_price, :initial_stop, :protective_stop,
             :invalidation, :reason_code, :evidence, 1,
             :idempotency_key, :created_at)
            """
        ),
        {
            "intent_id": intent_id,
            "account_id": account["account_id"],
            "run_uid": run_uid,
            "strategy_version": strategy_version,
            "stock_code": stock_code,
            "theme_code": theme_code[:80],
            "current_quantity": current_quantity,
            "target_quantity": intent.target_quantity,
            "target_weight": intent.target_weight,
            "earliest_at": earliest_at,
            "expires_at": expires_at,
            "limit_price": last_price,
            "worst_price": last_price,
            "initial_stop": initial_stop,
            "protective_stop": protective_stop,
            "invalidation": invalidation[:1000],
            "reason_code": intent.reason_code,
            "evidence": json.dumps(
                list(intent.evidence),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ),
            "idempotency_key": intent_key,
            "created_at": now,
        },
    )
    connection.execute(
        text(
            """
            INSERT INTO st_risk_decision_v2
            (intent_id, decision_status, requested_quantity,
             approved_quantity, trade_risk, post_single_weight,
             post_total_weight, post_theme_weight, post_open_risk,
             post_cash, checks_json, first_failure, decision_hash,
             created_at)
            VALUES
            (:intent_id, :status, :requested, :approved, :trade_risk,
             :single_weight, :total_weight, :theme_weight, :open_risk,
             :post_cash, :checks, :first_failure, :decision_hash,
             :created_at)
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
            "decision_hash": canonical_json_hash(risk.as_dict()),
            "created_at": now,
        },
    )
    if risk.approved_quantity <= 0:
        return {
            "status": "RISK_REJECTED",
            "intent_id": intent_id,
            "reason_code": risk.first_failure or "RISK_REJECTED",
        }
    order_key = order_idempotency_key(
        account_id=str(account["account_id"]),
        decision_run_uid=run_uid,
        intent_id=intent_id,
        stock_code=stock_code,
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
            "stock_code": stock_code,
            "limit_price": last_price,
            "quantity": risk.approved_quantity,
            "earliest_at": earliest_at,
            "expires_at": expires_at,
            "idempotency_key": order_key,
            "created_at": now,
        },
    )
    return {
        "status": "created",
        "intent_id": intent_id,
        "order_id": order_id,
        "action": "ADD",
        "requested_quantity": requested_add,
        "approved_quantity": risk.approved_quantity,
        "risk_status": risk.status.value,
    }


def monitor_positions(
    engine: Engine,
    *,
    trade_date: date,
    run_uid: str,
    account_id: str = "paper-main-v2",
    as_of: datetime | None = None,
    market_price_overrides: dict[str, Decimal] | None = None,
) -> dict[str, Any]:
    require_legacy_strategy_account(
        account_id,
        entrypoint="trading_v2.monitor_positions",
    )
    now = as_of or datetime.now()
    market_price_overrides = market_price_overrides or {}
    actions: list[dict[str, Any]] = []
    with engine.begin() as connection:
        account = connection.execute(
            text(
                """
                SELECT * FROM st_trade_account_v2
                WHERE account_id = :account_id FOR UPDATE
                """
            ),
            {"account_id": account_id},
        ).mappings().first()
        if not account:
            raise ValueError("V2 account not found")
        account = dict(account)
        market_regime = str(
            connection.execute(
                text(
                    """
                    SELECT market_regime FROM st_decision_run_v2
                    WHERE run_uid = :run_uid
                    """
                ),
                {"run_uid": run_uid},
            ).scalar()
            or "DATA_BLOCKED"
        )
        positions = connection.execute(
            text(
                """
                SELECT stock_code, strategy_version,
                       MAX(theme_code) AS theme_code,
                       SUM(remaining_quantity) AS quantity,
                       SUM(remaining_quantity * cost_price)
                           / SUM(remaining_quantity) AS average_cost,
                       MAX(approved_target_quantity) AS approved_target,
                       MAX(add_count) AS add_count,
                       MAX(initial_stop) AS initial_stop,
                       MAX(protective_stop) AS protective_stop,
                       MAX(position_state) AS position_state,
                       MAX(invalidation_condition) AS invalidation
                FROM st_position_lot_v2
                WHERE account_id = :account_id
                  AND remaining_quantity > 0
                GROUP BY stock_code, strategy_version
                ORDER BY stock_code, strategy_version
                """
            ),
            {"account_id": account_id},
        ).mappings().all()
        next_day = _next_trade_day(connection, trade_date)
        for row in positions:
            signal = connection.execute(
                text(
                    """
                    SELECT * FROM st_strategy_signal_v2
                    WHERE run_uid = :run_uid
                      AND strategy_version = :strategy_version
                      AND stock_code = :stock_code
                    """
                ),
                {
                    "run_uid": run_uid,
                    "strategy_version": row["strategy_version"],
                    "stock_code": row["stock_code"],
                },
            ).mappings().first()
            signal = dict(signal) if signal else None
            raw_signal = {}
            if signal:
                try:
                    raw_signal = json.loads(
                        str(signal.get("raw_features_json") or "{}")
                    )
                except (TypeError, ValueError, json.JSONDecodeError):
                    raw_signal = {}
                if not isinstance(raw_signal, dict):
                    raw_signal = {}
            facts = _market_facts(
                connection,
                stock_code=str(row["stock_code"]),
                trade_date=trade_date,
                strategy_version=str(row["strategy_version"]),
                current_price=market_price_overrides.get(
                    str(row["stock_code"])
                ),
            )
            if facts is None:
                actions.append(
                    {
                        "stock_code": row["stock_code"],
                        "status": "BLOCKED",
                        "reason_code": "POSITION_MARKET_DATA_MISSING",
                    }
                )
                continue
            sector_facts = (
                _sector_position_facts(
                    connection,
                    theme_code=str(row["theme_code"] or ""),
                    trade_date=trade_date,
                )
                if str(row["strategy_version"]).startswith(
                    "sector_preheat"
                )
                else {
                    "available": False,
                    "strong": False,
                    "broken": False,
                }
            )
            quantity = int(row["quantity"])
            sellable = int(
                connection.execute(
                    text(
                        """
                        SELECT COALESCE(SUM(remaining_quantity), 0)
                        FROM st_position_lot_v2
                        WHERE account_id = :account_id
                          AND stock_code = :stock_code
                          AND strategy_version = :strategy_version
                          AND settlement_date <= :trade_date
                          AND remaining_quantity > 0
                        """
                    ),
                    {
                        "account_id": account_id,
                        "stock_code": row["stock_code"],
                        "strategy_version": row["strategy_version"],
                        "trade_date": trade_date,
                    },
                ).scalar()
                or 0
            )
            try:
                current_state = PositionState(str(row["position_state"]))
            except ValueError:
                current_state = PositionState.VALID
            decision = evaluate_position(
                PositionFacts(
                    current_state=current_state,
                    current_quantity=quantity,
                    approved_target_quantity=int(
                        row["approved_target"] or quantity
                    ),
                    add_count=int(row["add_count"] or 0),
                    average_cost=decimal_value(row["average_cost"]),
                    last_price=facts["last_price"],
                    current_protective_stop=decimal_value(
                        row["protective_stop"]
                    ),
                    proposed_protective_stop=facts["proposed_stop"],
                    hard_stop_breached=(
                        facts["last_price"]
                        <= decimal_value(row["protective_stop"])
                    ),
                    invalidated=(
                        not facts["trend_valid"]
                        or bool(sector_facts["broken"])
                        or str(raw_signal.get("signal_direction") or "")
                        .upper()
                        == "SELL"
                        or str(raw_signal.get("signal_status") or "")
                        .upper()
                        in {"SELL_ALERT", "BLOCKED"}
                    ),
                    trend_strong=(
                        facts["trend_strong"]
                        and (
                            not sector_facts["available"]
                            or bool(sector_facts["strong"])
                        )
                    ),
                    trend_valid=facts["trend_valid"],
                    risk_event=(
                        market_regime == "EXTREME"
                        or bool(sector_facts["broken"])
                        or str(raw_signal.get("risk_level") or "")
                        .upper()
                        in {"HIGH", "CRITICAL"}
                    ),
                    can_sell_today=sellable >= quantity,
                    liquidity_available=facts[
                        "liquidity_available"
                    ],
                )
            )
            connection.execute(
                text(
                    """
                    UPDATE st_position_lot_v2
                    SET position_state = :state,
                        protective_stop = :protective_stop,
                        version = version + 1
                    WHERE account_id = :account_id
                      AND stock_code = :stock_code
                      AND strategy_version = :strategy_version
                      AND remaining_quantity > 0
                    """
                ),
                {
                    "state": decision.next_state.value,
                    "protective_stop": decision.protective_stop,
                    "account_id": account_id,
                    "stock_code": row["stock_code"],
                    "strategy_version": row["strategy_version"],
                },
            )
            action = {
                "stock_code": row["stock_code"],
                "strategy_version": row["strategy_version"],
                "previous_state": decision.previous_state.value,
                "next_state": decision.next_state.value,
                "action": decision.action.value,
                "reason_code": decision.reason_code,
                "target_quantity": decision.target_quantity,
                "protective_stop": str(decision.protective_stop),
            }
            if decision.action in {
                IntentAction.EXIT,
                IntentAction.REDUCE,
            } and decision.target_quantity < quantity:
                # A protective exit may execute down to the effective daily
                # lower limit. The instrument rule is mandatory.
                hhmm = now.hour * 100 + now.minute
                can_execute_today = (
                    sellable >= quantity
                    and (
                        931 <= hhmm <= 1130
                        or 1301 <= hhmm <= 1455
                    )
                )
                execution_date = (
                    trade_date if can_execute_today else next_day
                )
                earliest_at = (
                    now
                    if can_execute_today
                    else datetime.combine(next_day, time(9, 31))
                )
                expires_at = (
                    datetime.combine(trade_date, time(14, 55))
                    if can_execute_today
                    else datetime.combine(next_day, time(15, 0))
                )
                rule = connection.execute(
                    text(
                        """
                        SELECT limit_ratio, tick_size
                        FROM st_instrument_rule_v2
                        WHERE stock_code = :stock_code
                          AND effective_from <= :execution_date
                          AND (effective_to IS NULL
                               OR effective_to >= :execution_date)
                        ORDER BY effective_from DESC, rule_version DESC
                        LIMIT 1
                        """
                    ),
                    {
                        "stock_code": row["stock_code"],
                        "execution_date": execution_date,
                    },
                ).mappings().first()
                if not rule or rule["limit_ratio"] is None:
                    action["order_status"] = "INSTRUMENT_RULE_BLOCKED"
                else:
                    limit_price = (
                        facts["last_price"]
                        * (
                            Decimal("1")
                            - decimal_value(rule["limit_ratio"])
                        )
                    )
                    action["order"] = _persist_exit_chain(
                        connection,
                        account_id=account_id,
                        run_uid=run_uid,
                        strategy_version=str(row["strategy_version"]),
                        stock_code=str(row["stock_code"]),
                        current_quantity=quantity,
                        target_quantity=decision.target_quantity,
                        earliest_at=earliest_at,
                        expires_at=expires_at,
                        limit_price=limit_price,
                        initial_stop=decimal_value(row["initial_stop"]),
                        protective_stop=decision.protective_stop,
                        invalidation=str(row["invalidation"]),
                        reason_code=decision.reason_code,
                        now=now,
                    )
            elif decision.action == IntentAction.ADD:
                action["order"] = _persist_add_chain(
                    connection,
                    account=account,
                    trade_date=trade_date,
                    run_uid=run_uid,
                    market_regime=market_regime,
                    strategy_version=str(row["strategy_version"]),
                    stock_code=str(row["stock_code"]),
                    theme_code=str(row["theme_code"] or ""),
                    current_quantity=quantity,
                    target_quantity=decision.target_quantity,
                    last_price=facts["last_price"],
                    initial_stop=decimal_value(row["initial_stop"]),
                    protective_stop=decision.protective_stop,
                    invalidation=str(row["invalidation"]),
                    signal=signal,
                    now=now,
                )
            actions.append(action)
    return {
        "status": "ok",
        "trade_date": trade_date.isoformat(),
        "position_count": len(actions),
        "actions": actions,
        "real_order_count": 0,
    }
