from __future__ import annotations

import hashlib
import json
import uuid
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from server.trading_v2.execution import _sync_v3_execution_plan_states
from server.trading_v2.execution_buy_gate import (
    GATE_MODULE,
    evaluate_buy_gate,
    load_current_buy_gate,
)
from server.trading_v2.legacy_strategy_account_boundary import (
    require_legacy_strategy_account,
)
from server.trading_v2.oms import order_idempotency_key
from server.trading_v2.position_monitor import _persist_exit_chain

from .config import load_v3_config


ACTIVE_ORDER_STATES = (
    "CREATED",
    "RISK_APPROVED",
    "QUEUED",
    "PARTIALLY_FILLED",
)


def _canonical_v2_buy_receipt(
    connection,
    *,
    decision_run_uid: str,
    strategy_version: str,
    stock_code: str,
    now: datetime,
) -> tuple[dict[str, Any] | None, str]:
    """Require one V2-executor-verifiable receipt before V3 can enqueue BUY."""

    loaded = load_current_buy_gate(
        connection,
        decision_run_uid=decision_run_uid,
        strategy_version=strategy_version,
        stock_code=stock_code,
        as_of=now,
        lock=True,
    )
    if loaded.binding is None:
        return None, loaded.reason_code or "BUY_GATE_DATA_BLOCKED"
    decision = evaluate_buy_gate(
        now=now,
        decision_run_uid=decision_run_uid,
        strategy_version=strategy_version,
        stock_code=stock_code,
        bound=loaded.binding,
        current=loaded.binding,
    )
    if not decision.allowed:
        return None, decision.reason_code or "BUY_GATE_DATA_BLOCKED"
    return loaded.binding, ""


def _cancel_superseded_v3_buys(
    connection,
    *,
    account_id: str,
    run_uid: str,
    now: datetime,
) -> dict[str, list[dict[str, Any]]]:
    """Cancel unfilled V3 buy work left by older portfolio decisions.

    The latest completed decision is canonical.  Keeping older discovery
    orders queued can make several mutually exclusive two-stock portfolios
    execute together on the next session.  A partially-filled order keeps its
    completed fills/lots, but its unfilled remainder is cancelled; the latest
    decision can then size a replacement from the reconciled position.
    """

    stale_orders = connection.execute(
        text(
            """
            SELECT o.order_id, o.stock_code, o.quantity,
                   o.filled_quantity, o.status,
                   i.decision_run_uid
            FROM st_order_v2 o
            JOIN st_trade_intent_v2 i ON i.intent_id = o.intent_id
            WHERE o.account_id = :account_id
              AND o.side = 'BUY'
              AND i.reason_code IN (
                  'V3_PAPER_DISCOVERY',
                  'V3_VALIDATED_POSITIVE'
              )
              AND i.decision_run_uid <> :run_uid
              AND o.status IN (
                  'CREATED', 'RISK_APPROVED', 'QUEUED',
                  'PARTIALLY_FILLED'
              )
            FOR UPDATE
            """
        ),
        {"account_id": account_id, "run_uid": run_uid},
    ).mappings().all()
    cancelled_orders: list[dict[str, Any]] = []
    cancelled_partial_orders: list[dict[str, Any]] = []
    for raw in stale_orders:
        order = dict(raw)
        is_partial = int(order.get("filled_quantity") or 0) > 0
        connection.execute(
            text(
                """
                UPDATE st_order_v2
                SET status = 'CANCELLED',
                    waiting_reason = :waiting_reason,
                    updated_at = :updated_at
                WHERE order_id = :order_id
                  AND status IN (
                      'CREATED', 'RISK_APPROVED', 'QUEUED',
                      'PARTIALLY_FILLED'
                  )
                """
            ),
            {
                "order_id": order["order_id"],
                "waiting_reason": (
                    "SUPERSEDED_PARTIAL_BY_V3"
                    if is_partial
                    else "SUPERSEDED_BY_V3_DECISION"
                ),
                "updated_at": now,
            },
        )
        cancelled_orders.append(order)
        if is_partial:
            cancelled_partial_orders.append(order)

    stale_plans = connection.execute(
        text(
            """
            SELECT execution_plan_id, run_uid, stock_code,
                   side, quantity, state
            FROM st_execution_plan_v3
            WHERE account_id = :account_id
              AND source IN (
                  'V3_PAPER_DISCOVERY',
                  'V3_PORTFOLIO'
              )
              AND side = 'BUY'
              AND state IN (
                  'PAPER_QUEUED', 'PAPER_PARTIALLY_FILLED'
              )
              AND run_uid <> :run_uid
            FOR UPDATE
            """
        ),
        {"account_id": account_id, "run_uid": run_uid},
    ).mappings().all()
    cancelled_plans: list[dict[str, Any]] = []
    cancelled_partial_plan_keys = {
        (
            str(item.get("decision_run_uid") or ""),
            str(item.get("stock_code") or ""),
        )
        for item in cancelled_partial_orders
    }
    for raw in stale_plans:
        plan = dict(raw)
        plan_key = (
            str(plan.get("run_uid") or ""),
            str(plan.get("stock_code") or ""),
        )
        next_state = (
            "PAPER_PARTIAL_CANCELLED"
            if plan_key in cancelled_partial_plan_keys
            else "CANCELLED"
        )
        connection.execute(
            text(
                """
                UPDATE st_execution_plan_v3
                SET state = :state,
                    updated_at = :updated_at
                WHERE execution_plan_id = :execution_plan_id
                  AND state IN (
                      'PAPER_QUEUED', 'PAPER_PARTIALLY_FILLED'
                  )
                """
            ),
            {
                "execution_plan_id": plan["execution_plan_id"],
                "state": next_state,
                "updated_at": now,
            },
        )
        cancelled_plans.append(plan)
    return {
        "cancelled_orders": cancelled_orders,
        "cancelled_execution_plans": cancelled_plans,
        "cancelled_partial_orders": cancelled_partial_orders,
    }


def _hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def _ownership_hash(
    run_uid: str,
    forecast_id: str,
    stock_code: str,
    strategy_key: str,
) -> str:
    return hashlib.sha256(
        (
            f"{run_uid}|{forecast_id}|{stock_code}|{strategy_key}"
        ).encode("utf-8")
    ).hexdigest()


def freeze_pending_v3_buys(
    engine: Engine,
    *,
    account_id: str = "paper-main-v2",
    now: datetime | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Fail closed before a new premarket decision starts computing."""

    require_legacy_strategy_account(
        account_id,
        entrypoint="trading_v3.freeze_pending_v3_buys",
    )
    frozen_at = (now or datetime.now()).replace(microsecond=0)
    with engine.begin() as connection:
        real_enabled = connection.execute(
            text(
                """
                SELECT real_trading_enabled
                FROM st_trade_account_v2
                WHERE account_id = :account_id
                FOR UPDATE
                """
            ),
            {"account_id": account_id},
        ).scalar()
        if real_enabled is None:
            raise RuntimeError("V3 paper account not found")
        if int(real_enabled or 0) != 0:
            raise RuntimeError(
                "V3 safety violation: real trading must remain disabled"
            )
        return _cancel_superseded_v3_buys(
            connection,
            account_id=account_id,
            run_uid=(
                "__V3_PREMARKET_FREEZE__"
                + frozen_at.date().isoformat()
            ),
            now=frozen_at,
        )


def _next_trade_date(connection, source_date: date) -> date:
    value = connection.execute(
        text(
            """
            SELECT MIN(trade_date)
            FROM si_trade_calendar
            WHERE trade_status = 1
              AND trade_date > :source_date
            """
        ),
        {"source_date": source_date},
    ).scalar()
    if value is None:
        return source_date + timedelta(days=1)
    return value if isinstance(value, date) else date.fromisoformat(str(value))


def materialize_internal_paper_orders(
    engine: Engine,
    *,
    run_uid: str,
    account_id: str = "paper-main-v2",
) -> dict[str, Any]:
    """Translate V3 portfolio deltas into the existing internal paper OMS.

    This adapter has no real-order path. It also refuses to run if the
    database real-trading switch is anything other than zero.
    """

    require_legacy_strategy_account(
        account_id,
        entrypoint="trading_v3.materialize_internal_paper_orders",
    )
    config = load_v3_config()
    created = []
    skipped = []
    now = datetime.now().replace(microsecond=0)
    _sync_v3_execution_plan_states(
        engine,
        account_id=account_id,
        now=now,
    )
    with engine.begin() as connection:
        account = connection.execute(
            text(
                """
                SELECT *
                FROM st_trade_account_v2
                WHERE account_id = :account_id
                FOR UPDATE
                """
            ),
            {"account_id": account_id},
        ).mappings().first()
        if not account:
            raise RuntimeError("ProBigA 模拟账户不存在")
        if int(account.get("real_trading_enabled") or 0) != 0:
            raise RuntimeError("V3 只允许内部模拟盘，真实交易开关必须为 0")
        run = connection.execute(
            text(
                """
                SELECT *
                FROM st_decision_run_v3
                WHERE run_uid = :run_uid
                FOR UPDATE
                """
            ),
            {"run_uid": run_uid},
        ).mappings().first()
        if not run or run["status"] != "COMPLETED":
            raise RuntimeError("V3 决策批次不存在或尚未完成")
        targets = connection.execute(
            text(
                """
                SELECT t.*,
                       (
                           SELECT MIN(f.initial_stop_pct)
                           FROM st_alpha_forecast_v3 f
                           WHERE f.run_uid = t.run_uid
                             AND f.stock_code = t.stock_code
                       ) AS initial_stop_pct
                FROM st_target_portfolio_v3 t
                WHERE t.run_uid = :run_uid
                ORDER BY t.rank_no
                """
            ),
            {"run_uid": run_uid},
        ).mappings().all()
        superseded = _cancel_superseded_v3_buys(
            connection,
            account_id=account_id,
            run_uid=run_uid,
            now=now,
        )
        maximum_live_positions = min(
            50,
            max(
                1,
                int(
                    config.get("paper_execution", {}).get(
                        "maximum_live_positions",
                        12,
                    )
                ),
            ),
        )
        live_position_codes = {
            str(row[0])
            for row in connection.execute(
                text(
                    """
                    SELECT stock_code
                    FROM st_position_lot_v2
                    WHERE account_id = :account_id
                      AND remaining_quantity > 0
                    GROUP BY stock_code
                    UNION
                    SELECT stock_code
                    FROM st_order_v2
                    WHERE account_id = :account_id
                      AND side = 'BUY'
                      AND status IN (
                          'CREATED', 'RISK_APPROVED', 'QUEUED',
                          'PARTIALLY_FILLED'
                      )
                    GROUP BY stock_code
                    """
                ),
                {"account_id": account_id},
            ).all()
        }
        source_date = run["trade_date"]
        execution_date = _next_trade_date(connection, source_date)
        earliest_at = datetime.combine(execution_date, time(9, 30))
        expires_at = datetime.combine(execution_date, time(14, 45))
        exit_states = connection.execute(
            text(
                """
                SELECT s.*, lots.actual_quantity
                FROM st_position_state_v3 s
                JOIN (
                    SELECT stock_code,
                           SUM(remaining_quantity) AS actual_quantity
                    FROM st_position_lot_v2
                    WHERE account_id = :account_id
                      AND remaining_quantity > 0
                    GROUP BY stock_code
                ) lots ON lots.stock_code = s.stock_code
                WHERE s.account_id = :account_id
                  AND lots.actual_quantity > 0
                  AND s.last_action IN (
                      'SELL_ALL',
                      'WAIT_SELLABLE'
                  )
                ORDER BY s.updated_at, s.stock_code
                """
            ),
            {"account_id": account_id},
        ).mappings().all()
        exit_codes: set[str] = set()
        for state in exit_states:
            code = str(state["stock_code"])
            exit_codes.add(code)
            try:
                invalidation = json.loads(
                    str(state.get("invalidation_json") or "{}")
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                invalidation = {}
            reference_price = float(
                invalidation.get("latest_price")
                or state.get("average_cost")
                or 0.0
            )
            if reference_price <= 0:
                skipped.append({
                    "stock_code": code,
                    "side": "SELL",
                    "reason": "EXIT_REFERENCE_PRICE_INVALID",
                })
                continue
            protective_stop = float(
                invalidation.get("protective_stop") or 0.0
            )
            limit_price = Decimal(
                str(round(reference_price * 0.995, 3))
            )
            exit_result = _persist_exit_chain(
                connection,
                account_id=account_id,
                run_uid=run_uid,
                strategy_version=config["strategy_version"],
                stock_code=code,
                current_quantity=int(state["actual_quantity"]),
                target_quantity=0,
                earliest_at=earliest_at,
                expires_at=expires_at,
                limit_price=limit_price,
                initial_stop=Decimal(
                    str(protective_stop or reference_price)
                ),
                protective_stop=Decimal(
                    str(protective_stop or reference_price)
                ),
                invalidation=str(
                    state.get("last_reason")
                    or "V3交易逻辑失效"
                ),
                reason_code=str(
                    state.get("last_reason_code")
                    or "V3_POSITION_EXIT"
                ),
                now=now,
            )
            if exit_result.get("status") == "created":
                quantity = int(exit_result["quantity"])
                plan_key = _hash([
                    run_uid,
                    account_id,
                    code,
                    "SELL",
                    quantity,
                ])
                connection.execute(
                    text(
                        """
                        INSERT IGNORE INTO st_execution_plan_v3 (
                            execution_plan_id, run_uid, account_id,
                            trade_date, stock_code, side, quantity,
                            limit_price, state, reason_code, source,
                            real_order_allowed, idempotency_key,
                            created_at, updated_at
                        ) VALUES (
                            :execution_plan_id, :run_uid, :account_id,
                            :trade_date, :stock_code, 'SELL', :quantity,
                            :limit_price, 'PAPER_QUEUED',
                            :reason_code, 'V3_POSITION_STATE',
                            0, :idempotency_key, :created_at, :updated_at
                        )
                        """
                    ),
                    {
                        "execution_plan_id": uuid.uuid4().hex,
                        "run_uid": run_uid,
                        "account_id": account_id,
                        "trade_date": execution_date,
                        "stock_code": code,
                        "quantity": quantity,
                        "limit_price": float(limit_price),
                        "reason_code": str(
                            state.get("last_reason_code")
                            or "V3_POSITION_EXIT"
                        ),
                        "idempotency_key": plan_key,
                        "created_at": now,
                        "updated_at": now,
                    },
                )
                created.append({
                    "stock_code": code,
                    "side": "SELL",
                    "quantity": quantity,
                    "order_id": exit_result["order_id"],
                    "execution_date": execution_date,
                })
            else:
                skipped.append({
                    "stock_code": code,
                    "side": "SELL",
                    "reason": str(exit_result.get("status")),
                })
        for target in targets:
            code = str(target["stock_code"])
            if code in exit_codes:
                skipped.append({
                    "stock_code": code,
                    "side": "BUY",
                    "reason": "V3_EXIT_HAS_PRIORITY",
                })
                continue
            if (
                code not in live_position_codes
                and len(live_position_codes) >= maximum_live_positions
            ):
                skipped.append({
                    "stock_code": code,
                    "side": "BUY",
                    "reason": "ACTUAL_PAPER_POSITION_CONFIGURED_CAP",
                })
                continue
            paper_discovery = str(
                target.get("reason") or ""
            ).startswith("PAPER_DISCOVERY")
            reason_code = (
                "V3_PAPER_DISCOVERY"
                if paper_discovery
                else "V3_VALIDATED_POSITIVE"
            )
            if paper_discovery:
                last_sell_at = connection.execute(
                    text(
                        """
                        SELECT MAX(updated_at)
                        FROM st_order_v2
                        WHERE account_id = :account_id
                          AND stock_code = :stock_code
                          AND side = 'SELL'
                          AND status = 'FILLED'
                        """
                    ),
                    {
                        "account_id": account_id,
                        "stock_code": code,
                    },
                ).scalar()
                if last_sell_at is not None:
                    elapsed_trade_days = int(
                        connection.execute(
                            text(
                                """
                                SELECT COUNT(*)
                                FROM si_trade_calendar
                                WHERE trade_status = 1
                                  AND trade_date > DATE(:last_sell_at)
                                  AND trade_date <= :execution_date
                                """
                            ),
                            {
                                "last_sell_at": last_sell_at,
                                "execution_date": execution_date,
                            },
                        ).scalar()
                        or 0
                    )
                    cooldown_days = int(
                        config.get("paper_discovery", {}).get(
                            "cooldown_trade_days_after_exit",
                            5,
                        )
                    )
                    if elapsed_trade_days <= cooldown_days:
                        skipped.append({
                            "stock_code": code,
                            "reason": (
                                "PAPER_DISCOVERY_COOLDOWN_"
                                f"{cooldown_days}D"
                            ),
                        })
                        continue
            active = connection.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM st_order_v2
                    WHERE account_id = :account_id
                      AND stock_code = :stock_code
                      AND side = 'BUY'
                      AND status IN (
                          'CREATED', 'RISK_APPROVED', 'QUEUED',
                          'PARTIALLY_FILLED'
                      )
                    """
                ),
                {"account_id": account_id, "stock_code": code},
            ).scalar()
            current_quantity = int(
                connection.execute(
                    text(
                        """
                        SELECT COALESCE(SUM(remaining_quantity), 0)
                        FROM st_position_lot_v2
                        WHERE account_id = :account_id
                          AND stock_code = :stock_code
                          AND remaining_quantity > 0
                        """
                    ),
                    {"account_id": account_id, "stock_code": code},
                ).scalar()
                or 0
            )
            target_quantity = int(target["target_quantity"])
            delta = target_quantity - current_quantity
            if active or delta <= 0:
                skipped.append({
                    "stock_code": code,
                    "reason": (
                        "ACTIVE_BUY_ORDER_EXISTS"
                        if active
                        else "TARGET_ALREADY_REACHED"
                    ),
                })
                continue
            reference_price = (
                float(target["target_value"]) / target_quantity
                if target_quantity > 0
                else 0.0
            )
            if reference_price <= 0:
                skipped.append({
                    "stock_code": code,
                    "reason": "REFERENCE_PRICE_INVALID",
                })
                continue
            if current_quantity == 0:
                probe_quantity = (
                    max(1, target_quantity // 200) * 100
                )
                probe_value = probe_quantity * reference_price
                if probe_value >= float(
                    config["portfolio"]["minimum_economic_order_cny"]
                ):
                    delta = min(delta, probe_quantity)
            execution_policy = dict(config.get("paper_execution") or {})
            maximum_entry_premium = float(
                execution_policy.get("maximum_entry_premium_pct", 0.5)
            ) / 100.0
            worst_price_premium = float(
                execution_policy.get("worst_price_premium_pct", 1.0)
            ) / 100.0
            limit_price = round(
                reference_price * (1.0 + maximum_entry_premium),
                3,
            )
            worst_price = round(
                reference_price * (1.0 + worst_price_premium),
                3,
            )
            stop_pct = float(target["initial_stop_pct"] or -5.0)
            initial_stop = round(reference_price * (1 + stop_pct / 100), 3)
            intent_id = uuid.uuid4().hex
            intent_payload = [
                account_id,
                run_uid,
                code,
                "BUY",
                target_quantity,
                config["strategy_version"],
            ]
            intent_key = _hash(intent_payload)
            existing_intent = connection.execute(
                text(
                    """
                    SELECT intent_id
                    FROM st_trade_intent_v2
                    WHERE idempotency_key = :idempotency_key
                    LIMIT 1
                    """
                ),
                {"idempotency_key": intent_key},
            ).scalar()
            if existing_intent:
                skipped.append({
                    "stock_code": code,
                    "reason": "V3_INTENT_ALREADY_MATERIALIZED",
                })
                continue
            try:
                target_strategy_keys = [
                    str(item)
                    for item in json.loads(
                        str(target.get("strategy_keys_json") or "[]")
                    )
                    if str(item) and str(item) != "paper_discovery"
                ]
            except (TypeError, ValueError, json.JSONDecodeError):
                target_strategy_keys = []
            target_strategy_keys = sorted(set(target_strategy_keys))
            primary_strategy_key = str(
                target.get("primary_strategy_key") or ""
            )
            if (
                not primary_strategy_key
                and len(target_strategy_keys) == 1
            ):
                primary_strategy_key = target_strategy_keys[0]
            if (
                not primary_strategy_key
                or primary_strategy_key not in target_strategy_keys
            ):
                skipped.append({
                    "stock_code": code,
                    "reason": "V3_SAMPLE_OWNER_AMBIGUOUS",
                })
                continue
            primary_forecast_id = str(
                target.get("primary_forecast_id") or ""
            )
            if not primary_forecast_id:
                primary_forecast_id = str(
                    connection.execute(
                        text(
                            """
                            SELECT forecast_id
                            FROM st_alpha_forecast_v3
                            WHERE run_uid = :run_uid
                              AND stock_code = :stock_code
                              AND strategy_key = :strategy_key
                            LIMIT 1
                            """
                        ),
                        {
                            "run_uid": run_uid,
                            "stock_code": code,
                            "strategy_key": primary_strategy_key,
                        },
                    ).scalar()
                    or ""
                )
            if not primary_forecast_id:
                skipped.append({
                    "stock_code": code,
                    "reason": "V3_SAMPLE_OWNER_FORECAST_MISSING",
                })
                continue
            ownership_hash = _ownership_hash(
                run_uid,
                primary_forecast_id,
                code,
                primary_strategy_key,
            )
            stored_ownership_hash = str(
                target.get("attribution_snapshot_hash") or ""
            )
            if (
                stored_ownership_hash
                and stored_ownership_hash != ownership_hash
            ):
                skipped.append({
                    "stock_code": code,
                    "reason": "V3_SAMPLE_OWNER_HASH_MISMATCH",
                })
                continue
            buy_gate_receipt, buy_gate_reason = _canonical_v2_buy_receipt(
                connection,
                decision_run_uid=run_uid,
                strategy_version=config["strategy_version"],
                stock_code=code,
                now=now,
            )
            if buy_gate_receipt is None:
                skipped.append({
                    "stock_code": code,
                    "side": "BUY",
                    "status": "RESEARCH_ONLY",
                    "reason": buy_gate_reason or "BUY_GATE_DATA_BLOCKED",
                })
                continue
            evidence = {
                "source": (
                    "V3_PAPER_DISCOVERY"
                    if paper_discovery
                    else "V3_POSITIVE_EXPECTANCY_PORTFOLIO"
                ),
                "run_uid": run_uid,
                "model_version": run["model_version"],
                "expected_return_net_pct": float(
                    target["expected_return_net_pct"]
                ),
                "conservative_return_pct": float(
                    target["conservative_return_pct"]
                ),
                "estimated_roundtrip_cost_pct": float(
                    target["estimated_roundtrip_cost_pct"]
                ),
                "real_trading_enabled": False,
                "positive_expectancy_validated": (
                    not paper_discovery
                ),
                "entry_stage": (
                    "PROBE" if current_quantity == 0 else "CONFIRM_ADD"
                ),
                "signal_strategy_keys": sorted(target_strategy_keys),
                "primary_strategy_key": primary_strategy_key,
                "primary_forecast_id": primary_forecast_id,
                "supporting_strategy_keys": target_strategy_keys,
                "sample_owner_role": "PRIMARY",
                "attribution_version": (
                    "V3_PRIMARY_FORECAST_SNAPSHOT_V1"
                ),
                "ownership_hash": ownership_hash,
                # Keep V3 attribution as a mapping while exposing the exact
                # canonical receipt shape consumed again by the V2 executor.
                GATE_MODULE: buy_gate_receipt,
            }
            connection.execute(
                text(
                    """
                    INSERT IGNORE INTO st_trade_intent_v2 (
                        intent_id, account_id, decision_run_uid,
                        strategy_version, stock_code, theme_code,
                        action, current_quantity, target_quantity,
                        target_weight, earliest_at, expires_at,
                        limit_price, worst_price, initial_stop,
                        protective_stop, invalidation_condition,
                        reason_code, evidence_json, intent_version,
                        idempotency_key, created_at
                    ) VALUES (
                        :intent_id, :account_id, :decision_run_uid,
                        :strategy_version, :stock_code, :theme_code,
                        'BUY', :current_quantity, :target_quantity,
                        :target_weight, :earliest_at, :expires_at,
                        :limit_price, :worst_price, :initial_stop,
                        :protective_stop, :invalidation_condition,
                        :reason_code, :evidence_json, 1,
                        :idempotency_key, :created_at
                    )
                    """
                ),
                {
                    "intent_id": intent_id,
                    "account_id": account_id,
                    "decision_run_uid": run_uid,
                    "strategy_version": config["strategy_version"],
                    "stock_code": code,
                    "theme_code": target["theme_code"] or "",
                    "current_quantity": current_quantity,
                    "target_quantity": target_quantity,
                    "target_weight": target["target_weight"],
                    "earliest_at": earliest_at,
                    "expires_at": expires_at,
                    "limit_price": limit_price,
                    "worst_price": worst_price,
                    "initial_stop": initial_stop,
                    "protective_stop": initial_stop,
                    "invalidation_condition": (
                        "趋势失效、硬止损或扣费后净期望不再为正时退出；"
                        "T+1 仅延迟卖出执行"
                    ),
                    "reason_code": reason_code,
                    "evidence_json": json.dumps(
                        evidence,
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    "idempotency_key": intent_key,
                    "created_at": now,
                },
            )
            risk_payload = {
                "approved_quantity": delta,
                "target_weight": float(target["target_weight"]),
                "expected_mae_pct": float(target["expected_mae_pct"]),
            }
            connection.execute(
                text(
                    """
                    INSERT IGNORE INTO st_risk_decision_v2 (
                        intent_id, decision_status, requested_quantity,
                        approved_quantity, trade_risk,
                        post_single_weight, post_total_weight,
                        post_theme_weight, post_open_risk, post_cash,
                        checks_json, first_failure, decision_hash,
                        created_at
                    ) VALUES (
                        :intent_id, 'APPROVED', :requested_quantity,
                        :approved_quantity, :trade_risk,
                        :post_single_weight, :post_total_weight,
                        :post_theme_weight, :post_open_risk, :post_cash,
                        :checks_json, NULL, :decision_hash, :created_at
                    )
                    """
                ),
                {
                    "intent_id": intent_id,
                    "requested_quantity": delta,
                    "approved_quantity": delta,
                    "trade_risk": abs(
                        delta * reference_price * stop_pct / 100.0
                    ),
                    "post_single_weight": target["target_weight"],
                    "post_total_weight": float(
                        json.loads(str(run["portfolio_json"])).get(
                            "target_risk_asset_weight",
                            0,
                        )
                    ),
                    "post_theme_weight": target["target_weight"],
                    "post_open_risk": float(
                        json.loads(str(run["portfolio_json"])).get(
                            "worst_case_loss_cny",
                            0,
                        )
                    ),
                    "post_cash": float(account["cash_balance"])
                    - delta * reference_price,
                    "checks_json": json.dumps(
                        {
                            "V3_OOS_PROFIT_GATE": not paper_discovery,
                            "V3_PAPER_DISCOVERY_ONLY": paper_discovery,
                            "V3_COST_BUFFER": not paper_discovery,
                            "V3_PORTFOLIO_RISK": True,
                            "REAL_TRADING_DISABLED": True,
                        },
                        sort_keys=True,
                    ),
                    "decision_hash": _hash(risk_payload),
                    "created_at": now,
                },
            )
            order_id = uuid.uuid4().hex
            order_key = order_idempotency_key(
                account_id=account_id,
                decision_run_uid=run_uid,
                intent_id=intent_id,
                stock_code=code,
                side="BUY",
                target_quantity=delta,
                intent_version=1,
            )
            connection.execute(
                text(
                    """
                    INSERT IGNORE INTO st_order_v2 (
                        order_id, account_id, intent_id, stock_code,
                        side, order_type, limit_price, quantity,
                        filled_quantity, status, waiting_reason,
                        earliest_at, expires_at, idempotency_key,
                        created_at, updated_at
                    ) VALUES (
                        :order_id, :account_id, :intent_id, :stock_code,
                        'BUY', 'LIMIT', :limit_price, :quantity,
                        0, 'QUEUED', 'V3_NEXT_SESSION',
                        :earliest_at, :expires_at, :idempotency_key,
                        :created_at, :updated_at
                    )
                    """
                ),
                {
                    "order_id": order_id,
                    "account_id": account_id,
                    "intent_id": intent_id,
                    "stock_code": code,
                    "limit_price": limit_price,
                    "quantity": delta,
                    "earliest_at": earliest_at,
                    "expires_at": expires_at,
                    "idempotency_key": order_key,
                    "created_at": now,
                    "updated_at": now,
                },
            )
            plan_key = _hash([
                run_uid,
                account_id,
                code,
                "BUY",
                delta,
            ])
            connection.execute(
                text(
                    """
                    INSERT IGNORE INTO st_execution_plan_v3 (
                        execution_plan_id, run_uid, account_id,
                        trade_date, stock_code, side, quantity,
                        limit_price, state, reason_code, source,
                        real_order_allowed, idempotency_key,
                        created_at, updated_at
                    ) VALUES (
                        :execution_plan_id, :run_uid, :account_id,
                        :trade_date, :stock_code, 'BUY', :quantity,
                        :limit_price, 'PAPER_QUEUED',
                        :reason_code, :source,
                        0, :idempotency_key, :created_at, :updated_at
                    )
                    """
                ),
                {
                    "execution_plan_id": uuid.uuid4().hex,
                    "run_uid": run_uid,
                    "account_id": account_id,
                    "trade_date": execution_date,
                    "stock_code": code,
                    "quantity": delta,
                    "limit_price": limit_price,
                    "idempotency_key": plan_key,
                    "reason_code": reason_code,
                    "source": (
                        "V3_PAPER_DISCOVERY"
                        if paper_discovery
                        else "V3_PORTFOLIO"
                    ),
                    "created_at": now,
                    "updated_at": now,
                },
            )
            created.append({
                "stock_code": code,
                "side": "BUY",
                "quantity": delta,
                "order_id": order_id,
                "execution_date": execution_date,
            })
            live_position_codes.add(code)
    return {
        "status": "ok",
        "created": created,
        "skipped": skipped,
        **superseded,
        "real_order_count": 0,
        "paper_order_count": len(created),
    }
