"""Transactional V2 execution for ProBigA's isolated paper account."""
from __future__ import annotations

import json
import uuid
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection, Engine

from server.integrations.v2_canonical_commit.prepared_adapter import (
    CanonicalExecutionCutover,
    CanonicalMechanicalMutation,
    CanonicalMechanicalTransition,
    commit_prepared_canonical_execution,
    preflight_prepared_commit,
)
from server.trading_v2.execution_evidence import OrderTransitionKind

from .calendar import is_trade_day
from .config import canonical_json_hash, load_frozen_json
from .domain import (
    OrderSide,
    OrderStatus,
    Quote,
    WaitingReason,
    decimal_value,
    money,
)
from .ledger import FeeProfile
from .legacy_execution_policy import LegacySectorPreheatExecutionPolicy
from .matcher import PaperMatcher, PaperSnapshotMatcher
from .oms import fill_idempotency_key
from .execution_buy_gate import (
    BuyGateDecision,
    bound_buy_gate,
    evaluate_buy_gate,
    load_current_buy_gate,
)
from .paper_configuration import is_internal_paper_configuration
from .policy import load_portfolio_policy
from .quotes import build_quote_event, latest_quote


ACTIVE_ORDER_STATUSES = (
    "RISK_APPROVED",
    "QUEUED",
    "PARTIALLY_FILLED",
)

SECTOR_CONFIRMATION_MAX_AGE_SECONDS = 180
_LEGACY_SECTOR_PREHEAT_POLICY = LegacySectorPreheatExecutionPolicy(
    confirmation_max_age_seconds=SECTOR_CONFIRMATION_MAX_AGE_SECONDS
)
_MARKET_TIMEZONE = ZoneInfo("Asia/Shanghai")


def _cutover_execution_clocks(value: datetime) -> tuple[datetime, datetime]:
    """Return DB-local naive time and evidence UTC time for one instant.

    Existing V2/MySQL ``DATETIME`` mechanics use naive Asia/Shanghai values.
    Canonical evidence contracts require timezone-aware timestamps.  Naive
    inputs therefore retain their historical Shanghai interpretation; aware
    inputs are converted to that same DB-local representation.
    """

    if type(value) is not datetime:
        raise TypeError("execution time must be exactly datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        market_aware = value.replace(tzinfo=_MARKET_TIMEZONE)
        mechanical = value
    else:
        market_aware = value.astimezone(_MARKET_TIMEZONE)
        mechanical = market_aware.replace(tzinfo=None)
    return mechanical, market_aware.astimezone(timezone.utc)


def _v3_plan_state(order_status: str, filled_quantity: int) -> str:
    status = str(order_status or "").upper()
    filled = max(0, int(filled_quantity or 0))
    if status == "FILLED":
        return "PAPER_FILLED"
    if status == "PARTIALLY_FILLED":
        return "PAPER_PARTIALLY_FILLED"
    if status == "CANCELLED":
        return "PAPER_PARTIAL_CANCELLED" if filled else "CANCELLED"
    if status == "EXPIRED":
        return "PAPER_PARTIAL_EXPIRED" if filled else "EXPIRED"
    if status == "REJECTED":
        return "REJECTED"
    return "PAPER_QUEUED"


def _sync_v3_execution_plan_states(
    engine: Engine,
    *,
    account_id: str,
    now: datetime,
) -> int:
    """Project OMS truth into the optional V3 execution-plan read model."""

    inspector = inspect(engine, raiseerr=False)
    if inspector is None or not inspector.has_table(
        "st_execution_plan_v3"
    ):
        return 0
    with engine.begin() as connection:
        rows = connection.execute(
            text(
                """
                SELECT p.execution_plan_id, p.state,
                       o.status AS order_status,
                       o.filled_quantity
                FROM st_execution_plan_v3 p
                JOIN st_trade_intent_v2 i
                  ON i.decision_run_uid = p.run_uid
                 AND i.stock_code = p.stock_code
                 AND i.action = p.side
                JOIN st_order_v2 o
                  ON o.intent_id = i.intent_id
                 AND o.side = p.side
                WHERE p.account_id = :account_id
                  AND p.state IN (
                      'PAPER_QUEUED', 'PAPER_PARTIALLY_FILLED'
                  )
                ORDER BY p.execution_plan_id, o.created_at DESC
                """
            ),
            {"account_id": account_id},
        ).mappings().all()
        updates = 0
        seen: set[str] = set()
        for raw in rows:
            row = dict(raw)
            plan_id = str(row["execution_plan_id"])
            if plan_id in seen:
                continue
            seen.add(plan_id)
            next_state = _v3_plan_state(
                str(row.get("order_status") or ""),
                int(row.get("filled_quantity") or 0),
            )
            if next_state == str(row.get("state") or ""):
                continue
            updates += int(
                connection.execute(
                    text(
                        """
                        UPDATE st_execution_plan_v3
                        SET state = :state, updated_at = :updated_at
                        WHERE execution_plan_id = :execution_plan_id
                          AND state IN (
                              'PAPER_QUEUED',
                              'PAPER_PARTIALLY_FILLED'
                          )
                        """
                    ),
                    {
                        "state": next_state,
                        "updated_at": now,
                        "execution_plan_id": plan_id,
                    },
                ).rowcount
                or 0
            )
        return updates


def _session_open(now: datetime) -> bool:
    hhmm = now.hour * 100 + now.minute
    return 931 <= hhmm <= 1130 or 1301 <= hhmm <= 1500


def _rule(
    connection: Connection,
    *,
    stock_code: str,
    trade_date: date,
    rule_version: str = "",
) -> dict[str, Any] | None:
    row = connection.execute(
        text(
            """
            SELECT * FROM st_instrument_rule_v2
            WHERE stock_code = :stock_code
              AND effective_from <= :trade_date
              AND (effective_to IS NULL OR effective_to >= :trade_date)
              AND (:rule_version = '' OR rule_version = :rule_version)
            ORDER BY effective_from DESC, rule_version DESC LIMIT 1
            """
        ),
        {
            "stock_code": stock_code,
            "trade_date": trade_date,
            "rule_version": rule_version,
        },
    ).mappings().first()
    return dict(row) if row else None


def _fee_profile(
    connection: Connection,
    *,
    version: str,
    security_type: str,
    trade_date: date,
) -> FeeProfile | None:
    row = connection.execute(
        text(
            """
            SELECT * FROM st_fee_profile_v2
            WHERE fee_profile_version = :version
              AND security_type = :security_type
              AND effective_from <= :trade_date
              AND (effective_to IS NULL OR effective_to >= :trade_date)
              AND confirmation_status IN
                  ('CONFIRMED','PAPER_ASSUMPTION')
            ORDER BY effective_from DESC LIMIT 1
            """
        ),
        {
            "version": version,
            "security_type": security_type,
            "trade_date": trade_date,
        },
    ).mappings().first()
    if not row:
        return None
    try:
        other = json.loads(str(row["other_fee_json"] or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(other, dict):
        return None
    return FeeProfile(
        version=str(row["fee_profile_version"]),
        buy_commission_rate=decimal_value(row["buy_commission_rate"]),
        sell_commission_rate=decimal_value(row["sell_commission_rate"]),
        minimum_commission=decimal_value(row["minimum_commission"]),
        stamp_tax_sell_rate=decimal_value(row["stamp_tax_sell_rate"]),
        transfer_fee_buy_rate=decimal_value(row["transfer_fee_buy_rate"]),
        transfer_fee_sell_rate=decimal_value(row["transfer_fee_sell_rate"]),
        other_buy_rate=decimal_value(other.get("buy_rate") or 0),
        other_sell_rate=decimal_value(other.get("sell_rate") or 0),
        other_buy_fixed=decimal_value(other.get("buy_fixed") or 0),
        other_sell_fixed=decimal_value(other.get("sell_fixed") or 0),
        other_buy_per_share=decimal_value(
            other.get("buy_per_share") or 0
        ),
        other_sell_per_share=decimal_value(
            other.get("sell_per_share") or 0
        ),
    )


def _sector_entry_wait_reason(
    connection: Connection,
    *,
    strategy_version: str,
    theme_code: str,
    side: OrderSide,
    now: datetime,
) -> str:
    """Compatibility facade for the legacy sector strategy policy."""

    return _LEGACY_SECTOR_PREHEAT_POLICY.sector_entry_wait_reason(
        connection,
        strategy_version=strategy_version,
        theme_code=theme_code,
        side=side,
        now=now,
    )


def _entry_trend_wait_reason(
    *,
    strategy_version: str,
    side: OrderSide,
    fill_price: Decimal,
    initial_stop: Decimal,
) -> str:
    """Compatibility facade for the legacy sector strategy policy."""

    return _LEGACY_SECTOR_PREHEAT_POLICY.entry_trend_wait_reason(
        strategy_version=strategy_version,
        side=side,
        fill_price=fill_price,
        initial_stop=initial_stop,
    )


def _paper_snapshot_quote(
    connection: Connection,
    *,
    stock_code: str,
    now: datetime,
    lot_size: int,
    already_filled_quantity: int,
) -> tuple[Any | None, int, str]:
    """Return QMT first, then an attested public quorum for paper matching."""
    policy = load_portfolio_policy()
    if not policy.paper_snapshot_fallback:
        return None, 0, ""

    def parse_quote_at(candidate: Any) -> datetime | None:
        value = (
            candidate.get("source_time")
            or candidate.get("snapshot_at")
        )
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(
                str(value).replace(" ", "T")[:26]
            )
        except (TypeError, ValueError):
            return None

    row = connection.execute(
        text(
            """
            SELECT stock_code, price, pre_close, volume, snapshot_at,
                   etl_sync_at, data_source, source_time, received_at,
                   batch_id
            FROM sm_stock_current
            WHERE BINARY stock_code = BINARY :stock_code
            LIMIT 1
            """
        ),
        {"stock_code": stock_code},
    ).mappings().first()
    source = str((row or {}).get("data_source") or "").strip()
    quote_at = parse_quote_at(row) if row else None
    qmt_age = (
        (now - quote_at).total_seconds()
        if quote_at is not None
        else float("inf")
    )
    qmt_usable = bool(
        row
        and "qmt" in source.lower()
        and quote_at is not None
        and quote_at.date() == now.date()
        and 0 <= qmt_age <= policy.paper_snapshot_max_age_seconds
    )
    if qmt_usable:
        provider = f"paper_qmt_snapshot:{source}"[:80]
    else:
        try:
            intraday_config, _ = load_frozen_json(
                "strategies/intraday_activation_v2.json"
            )
            failover = dict(
                intraday_config.get("public_quote_failover") or {}
            )
        except Exception:
            failover = {}
        if not bool(failover.get("enabled", False)):
            return None, 0, ""
        maximum_age = float(
            failover.get("maximum_snapshot_age_seconds") or 45
        )
        failover_provider = str(
            failover.get("source_provider")
            or "PUBLIC_QUOTE_QUORUM_V1"
        ).upper()
        row = connection.execute(
            text(
                """
                SELECT q.stock_code, q.price, q.pre_close, q.volume,
                       q.quote_at AS snapshot_at,
                       q.received_at AS etl_sync_at,
                       q.source_provider AS data_source,
                       q.quote_at AS source_time, q.received_at,
                       q.batch_id
                FROM st_public_quote_current_v2 q
                INNER JOIN st_public_quote_receipt_v2 r
                    ON r.batch_id = q.batch_id
                   AND r.quality_status = 'PASS'
                WHERE BINARY q.stock_code = BINARY :stock_code
                  AND q.quality_status = 'PASS'
                  AND q.source_provider = :source_provider
                  AND q.source_count >= 2
                  AND q.quote_at BETWEEN :cutoff AND :now
                ORDER BY q.quote_at DESC
                LIMIT 1
                """
            ),
            {
                "stock_code": stock_code,
                "source_provider": failover_provider,
                "cutoff": now - timedelta(seconds=maximum_age),
                "now": now,
            },
        ).mappings().first()
        if not row:
            return None, 0, ""
        quote_at = parse_quote_at(row)
        if quote_at is None:
            return None, 0, ""
        source = str(row["data_source"] or failover_provider)
        provider = f"paper_public_quorum_snapshot:{source}"[:80]

    event = build_quote_event(
        {
            **dict(row),
            "source_time": quote_at,
            "received_at": row["received_at"]
            or row["etl_sync_at"]
            or now,
            "data_source": provider,
        }
    )
    if event is None or event["last_price"] is None:
        return None, 0, ""
    connection.execute(
        text(
            """
            INSERT IGNORE INTO st_quote_event_v2
            (quote_event_id, stock_code, quote_at, received_at,
             bid1, bid1_volume, ask1, ask1_volume, last_price, pre_close,
             upper_limit, lower_limit, suspended, source_provider,
             source_batch_id, payload_hash, created_at)
            VALUES
            (:quote_event_id, :stock_code, :quote_at, :received_at,
             :bid1, :bid1_volume, :ask1, :ask1_volume, :last_price,
             :pre_close, :upper_limit, :lower_limit, :suspended,
             :source_provider, :source_batch_id, :payload_hash, :created_at)
            """
        ),
        event,
    )
    raw_volume = max(0, int(decimal_value(row["volume"])))
    legal_lot = max(1, int(lot_size))
    participation_cap = int(
        Decimal(raw_volume)
        * policy.paper_snapshot_max_volume_participation
    )
    participation_cap -= participation_cap % legal_lot
    liquidity_quantity = max(
        0,
        participation_cap - max(0, int(already_filled_quantity)),
    )
    quote = Quote(
        stock_code=str(event["stock_code"]),
        event_id=str(event["quote_event_id"]),
        quote_at=event["quote_at"],
        received_at=event["received_at"],
        bid1=None,
        bid1_volume=None,
        ask1=None,
        ask1_volume=None,
        last_price=event["last_price"],
        upper_limit=event["upper_limit"],
        lower_limit=event["lower_limit"],
        suspended=bool(event["suspended"]),
    )
    return quote, liquidity_quantity, provider


def _settlement_date(
    connection: Connection,
    *,
    trade_date: date,
    settlement_days: int,
) -> date:
    if settlement_days <= 0:
        return trade_date
    rows = connection.execute(
        text(
            """
            SELECT trade_date FROM si_trade_calendar
            WHERE trade_status = 1 AND trade_date > :trade_date
            ORDER BY trade_date LIMIT :limit
            """
        ),
        {"trade_date": trade_date, "limit": int(settlement_days)},
    ).fetchall()
    if len(rows) < settlement_days:
        raise RuntimeError("trade calendar cannot resolve settlement date")
    value = rows[-1][0]
    return (
        value
        if isinstance(value, date)
        else date.fromisoformat(str(value)[:10])
    )


def _available_sell_quantity(
    connection: Connection,
    *,
    account_id: str,
    stock_code: str,
    trade_date: date,
) -> int:
    return int(
        connection.execute(
            text(
                """
                SELECT COALESCE(SUM(remaining_quantity), 0)
                FROM st_position_lot_v2
                WHERE account_id = :account_id
                  AND stock_code = :stock_code
                  AND settlement_date <= :trade_date
                  AND remaining_quantity > 0
                """
            ),
            {
                "account_id": account_id,
                "stock_code": stock_code,
                "trade_date": trade_date,
            },
        ).scalar()
        or 0
    )


def _price_limit(
    pre_close: Decimal | None,
    limit_ratio: Decimal | None,
    tick_size: Decimal,
    *,
    upper: bool,
) -> Decimal | None:
    if pre_close is None or limit_ratio is None:
        return None
    factor = (
        Decimal("1") + limit_ratio
        if upper
        else Decimal("1") - limit_ratio
    )
    return (
        (pre_close * factor / tick_size)
        .quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        * tick_size
    )


def _record_event(
    connection: Connection,
    *,
    account_id: str,
    event_type: str,
    entity_type: str,
    entity_id: str,
    payload: dict[str, Any],
    occurred_at: datetime,
) -> None:
    payload_hash = canonical_json_hash(payload)
    connection.execute(
        text(
            """
            INSERT IGNORE INTO st_trade_event_v2
            (event_id, trace_id, account_id, event_type, entity_type,
             entity_id, event_payload_json, payload_hash,
             occurred_at, created_at)
            VALUES
            (:event_id, :trace_id, :account_id, :event_type, :entity_type,
             :entity_id, :payload, :payload_hash, :occurred_at, :created_at)
            """
        ),
        {
            "event_id": payload_hash[:32],
            "trace_id": payload_hash[32:64],
            "account_id": account_id,
            "event_type": event_type,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "payload": json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ),
            "payload_hash": payload_hash,
            "occurred_at": occurred_at,
            "created_at": datetime.now(),
        },
    )


def _wait_order(
    connection: Connection,
    *,
    order_id: str,
    reason: str,
    now: datetime,
) -> None:
    connection.execute(
        text(
            """
            UPDATE st_order_v2
            SET status = CASE
                    WHEN filled_quantity > 0 THEN 'PARTIALLY_FILLED'
                    ELSE 'QUEUED' END,
                waiting_reason = :reason,
                updated_at = :now
            WHERE order_id = :order_id
            """
        ),
        {"reason": reason, "now": now, "order_id": order_id},
    )


def _execution_buy_gate_decision(
    connection: Connection,
    *,
    order: dict[str, Any],
    now: datetime,
) -> BuyGateDecision:
    """Reconstruct current BUY evidence; SELL is deliberately exempt."""

    if str(order.get("side") or "").upper() != OrderSide.BUY.value:
        return BuyGateDecision(True)
    lock_clause = (
        " FOR UPDATE"
        if connection.dialect.name.lower() in {"mysql", "mariadb"}
        else ""
    )
    intent_row = connection.execute(
        text(
            "SELECT decision_run_uid, evidence_json "
            "FROM st_trade_intent_v2 "
            "WHERE intent_id = :intent_id" + lock_clause
        ),
        {"intent_id": str(order.get("intent_id") or "")},
    ).mappings().first()
    if not intent_row:
        return BuyGateDecision(
            False,
            "BUY_GATE_EVIDENCE_MISSING",
            "order intent is missing",
        )
    decision_run_uid = str(intent_row.get("decision_run_uid") or "")
    bound = bound_buy_gate(intent_row.get("evidence_json"))
    current_load = load_current_buy_gate(
        connection,
        decision_run_uid=decision_run_uid,
        strategy_version=str(order.get("strategy_version") or ""),
        stock_code=str(order.get("stock_code") or ""),
        as_of=now,
        lock=True,
    )
    if current_load.binding is None:
        return BuyGateDecision(
            False,
            current_load.reason_code or "BUY_GATE_EVIDENCE_MISSING",
            current_load.detail or "current BUY gate evidence is unavailable",
        )
    return evaluate_buy_gate(
        now=now,
        decision_run_uid=decision_run_uid,
        strategy_version=str(order.get("strategy_version") or ""),
        stock_code=str(order.get("stock_code") or ""),
        bound=bound,
        current=current_load.binding,
    )


def _consume_sell_lots(
    connection: Connection,
    *,
    account_id: str,
    stock_code: str,
    trade_date: date,
    quantity: int,
    now: datetime,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        text(
            """
            SELECT lot_id, remaining_quantity
            FROM st_position_lot_v2
            WHERE account_id = :account_id
              AND stock_code = :stock_code
              AND settlement_date <= :trade_date
              AND remaining_quantity > 0
            ORDER BY opened_trade_date, lot_id
            FOR UPDATE
            """
        ),
        {
            "account_id": account_id,
            "stock_code": stock_code,
            "trade_date": trade_date,
        },
    ).mappings().all()
    remaining = quantity
    allocations: list[dict[str, Any]] = []
    for row in rows:
        remaining_before = int(row["remaining_quantity"])
        consumed = min(remaining, remaining_before)
        next_quantity = remaining_before - consumed
        connection.execute(
            text(
                """
                UPDATE st_position_lot_v2
                SET remaining_quantity = :remaining,
                    version = version + 1,
                    position_state = CASE
                        WHEN :remaining = 0 THEN 'CLOSED'
                        ELSE position_state END,
                    closed_at = CASE
                        WHEN :remaining = 0 THEN :now
                        ELSE closed_at END
                WHERE lot_id = :lot_id
                """
            ),
            {
                "remaining": next_quantity,
                "now": now,
                "lot_id": row["lot_id"],
            },
        )
        allocations.append(
            {
                "lot_id": str(row["lot_id"]),
                "stock_code": stock_code,
                "consumed_quantity": consumed,
                "remaining_before": remaining_before,
                "remaining_after": next_quantity,
            }
        )
        remaining -= consumed
        if remaining == 0:
            return allocations
    raise RuntimeError("sell lot consumption invariant failed")


def _execute_one(
    engine: Engine,
    *,
    order_id: str,
    account_id: str,
    now: datetime,
    canonical_cutover: CanonicalExecutionCutover | None = None,
) -> dict[str, Any]:
    evidence_now: datetime | None = None
    if canonical_cutover is not None:
        now, evidence_now = _cutover_execution_clocks(now)
    with engine.begin() as connection:
        preflight = None
        if canonical_cutover is not None:
            # This must be the first SQL operation in the mutation transaction.
            # The returned context proves that the shared fence lock belongs to
            # this exact transaction when the prepared bundle is appended.
            preflight = preflight_prepared_commit(
                connection,
                cutover=canonical_cutover,
                now=evidence_now,
            )
        order_row = connection.execute(
            text(
                """
                SELECT o.*, i.strategy_version, i.action AS intent_action,
                       i.theme_code, i.initial_stop,
                       i.protective_stop, i.invalidation_condition,
                       i.intent_version, r.approved_quantity
                FROM st_order_v2 o
                JOIN st_trade_intent_v2 i ON i.intent_id = o.intent_id
                JOIN st_risk_decision_v2 r ON r.intent_id = i.intent_id
                WHERE o.order_id = :order_id
                FOR UPDATE
                """
            ),
            {"order_id": order_id},
        ).mappings().first()
        if not order_row or str(order_row["status"]) not in ACTIVE_ORDER_STATUSES:
            return {"order_id": order_id, "status": "skipped"}
        order = dict(order_row)
        if now < order["earliest_at"]:
            return {"order_id": order_id, "status": "NOT_YET_ACTIVE"}

        current_status = OrderStatus(str(order["status"]))
        current_filled_quantity = int(order["filled_quantity"] or 0)
        current_waiting_reason = (
            str(order["waiting_reason"])
            if order.get("waiting_reason") not in {None, ""}
            else None
        )
        mechanical_transitions: list[CanonicalMechanicalTransition] = []

        def record_mechanical_transition(
            *,
            to_status: OrderStatus,
            next_filled_quantity: int,
            next_waiting_reason: str | None,
            transition_kind: OrderTransitionKind,
            source_event_type: str,
            source_event_id: str | None = None,
            related_fill_id: str | None = None,
            source_payload: dict[str, Any] | None = None,
        ) -> None:
            nonlocal current_status
            nonlocal current_filled_quantity
            nonlocal current_waiting_reason
            if canonical_cutover is None:
                current_status = to_status
                current_filled_quantity = next_filled_quantity
                current_waiting_reason = next_waiting_reason
                return
            assert evidence_now is not None
            payload = source_payload or {
                "order_id": order_id,
                "account_id": account_id,
                "ordinal": len(mechanical_transitions),
                "from_status": current_status.value,
                "to_status": to_status.value,
                "previous_filled_quantity": current_filled_quantity,
                "next_filled_quantity": next_filled_quantity,
                "previous_waiting_reason": current_waiting_reason,
                "next_waiting_reason": next_waiting_reason,
                "transition_kind": transition_kind.value,
                "occurred_at": evidence_now,
                "related_fill_id": related_fill_id,
            }
            source_hash = canonical_json_hash(payload)
            mechanical_transitions.append(
                CanonicalMechanicalTransition(
                    order_id=order_id,
                    account_id=account_id,
                    from_status=current_status,
                    to_status=to_status,
                    previous_filled_quantity=current_filled_quantity,
                    next_filled_quantity=next_filled_quantity,
                    previous_waiting_reason=current_waiting_reason,
                    next_waiting_reason=next_waiting_reason,
                    transition_kind=transition_kind,
                    source_event_type=source_event_type,
                    source_event_id=source_event_id or source_hash,
                    source_event_hash=source_hash,
                    occurred_at=evidence_now,
                    related_fill_id=related_fill_id,
                )
            )
            current_status = to_status
            current_filled_quantity = next_filled_quantity
            current_waiting_reason = next_waiting_reason

        def apply_wait(reason: str) -> None:
            normalized_reason = str(reason or "").strip()
            if not normalized_reason:
                raise RuntimeError("waiting transition requires a reason")
            target_status = (
                OrderStatus.PARTIALLY_FILLED
                if current_filled_quantity > 0
                else OrderStatus.QUEUED
            )
            changed = (
                target_status is not current_status
                or normalized_reason != current_waiting_reason
            )
            # Under cutover, do not create an updated_at-only write that has
            # no corresponding canonical state transition to evidence.
            if canonical_cutover is None or changed:
                _wait_order(
                    connection,
                    order_id=order_id,
                    reason=normalized_reason,
                    now=now,
                )
            if changed:
                record_mechanical_transition(
                    to_status=target_status,
                    next_filled_quantity=current_filled_quantity,
                    next_waiting_reason=normalized_reason,
                    transition_kind=(
                        OrderTransitionKind.STATUS_CHANGE
                        if target_status is not current_status
                        else OrderTransitionKind.WAITING_REASON_CHANGED
                    ),
                    source_event_type="V2_ORDER_WAITING_TRANSITION",
                )

        def finish(result: dict[str, Any]) -> dict[str, Any]:
            if canonical_cutover is None or not mechanical_transitions:
                return result
            assert preflight is not None
            fill_transition = next(
                (
                    item
                    for item in mechanical_transitions
                    if item.transition_kind
                    is OrderTransitionKind.FILL_APPLIED
                ),
                None,
            )
            mutation = CanonicalMechanicalMutation(
                order_id=order_id,
                account_id=account_id,
                transitions=tuple(mechanical_transitions),
                result_status=current_status.value,
                recorded_at=evidence_now,
                fill_id=(
                    None
                    if fill_transition is None
                    else fill_transition.related_fill_id
                ),
            )
            commit_prepared_canonical_execution(
                connection,
                cutover=canonical_cutover,
                preflight=preflight,
                mutation=mutation,
            )
            committed = dict(result)
            committed["canonical_commit_status"] = "COMMITTED"
            committed["canonical_mutation_hash"] = mutation.mutation_hash
            return committed

        def cancel_buy_remainder(
            gate_decision: BuyGateDecision,
        ) -> dict[str, Any]:
            reason = str(
                gate_decision.reason_code or "BUY_GATE_DATA_BLOCKED"
            )[:40]
            cancellation_payload = {
                "order_id": order_id,
                "account_id": account_id,
                "stock_code": str(order.get("stock_code") or ""),
                "side": str(order.get("side") or ""),
                "filled_quantity": current_filled_quantity,
                "cancelled_quantity": max(
                    0,
                    int(order.get("quantity") or 0)
                    - current_filled_quantity,
                ),
                "reason_code": reason,
                "detail": gate_decision.detail,
                "real_order_count": 0,
            }
            connection.execute(
                text(
                    """
                    UPDATE st_order_v2
                    SET status = 'CANCELLED', waiting_reason = :reason,
                        updated_at = :now
                    WHERE order_id = :order_id
                      AND status IN (
                          'RISK_APPROVED','QUEUED','PARTIALLY_FILLED'
                      )
                    """
                ),
                {"reason": reason, "now": now, "order_id": order_id},
            )
            _record_event(
                connection,
                account_id=account_id,
                event_type="V2_BUY_GATE_CANCELLED",
                entity_type="ORDER",
                entity_id=order_id,
                payload=cancellation_payload,
                occurred_at=now,
            )
            record_mechanical_transition(
                to_status=OrderStatus.CANCELLED,
                next_filled_quantity=current_filled_quantity,
                next_waiting_reason=reason,
                transition_kind=OrderTransitionKind.STATUS_CHANGE,
                source_event_type="V2_BUY_GATE_CANCELLED",
                source_payload=cancellation_payload,
            )
            return finish(
                {
                    "order_id": order_id,
                    "status": OrderStatus.CANCELLED.value,
                    "waiting_reason": reason,
                    "blocks": [reason],
                    "cancelled_remainder": True,
                    "partial_cancelled": current_filled_quantity > 0,
                    "filled_quantity": current_filled_quantity,
                }
            )

        account = None
        if canonical_cutover is not None:
            account_row = connection.execute(
                text(
                    """
                    SELECT * FROM st_trade_account_v2
                    WHERE account_id = :account_id FOR UPDATE
                    """
                ),
                {"account_id": account_id},
            ).mappings().first()
            if not account_row:
                raise RuntimeError("V2 account not found")
            account = dict(account_row)
            if bool(account.get("real_trading_enabled")):
                raise RuntimeError(
                    "prepared canonical execution is paper-only"
                )
        if now >= order["expires_at"]:
            connection.execute(
                text(
                    """
                    UPDATE st_order_v2
                    SET status = 'EXPIRED', waiting_reason = NULL,
                        updated_at = :now
                    WHERE order_id = :order_id
                    """
                ),
                {"now": now, "order_id": order_id},
            )
            record_mechanical_transition(
                to_status=OrderStatus.EXPIRED,
                next_filled_quantity=current_filled_quantity,
                next_waiting_reason=None,
                transition_kind=OrderTransitionKind.STATUS_CHANGE,
                source_event_type="V2_ORDER_EXPIRED",
            )
            return finish({"order_id": order_id, "status": "EXPIRED"})

        if account is None:
            account_row = connection.execute(
                text(
                    """
                    SELECT * FROM st_trade_account_v2
                    WHERE account_id = :account_id FOR UPDATE
                    """
                ),
                {"account_id": account_id},
            ).mappings().first()
            if not account_row:
                raise RuntimeError("V2 account not found")
            account = dict(account_row)
        paper_blocks: list[str] = []
        if str(account["status"]) != "ACTIVE":
            paper_blocks.append(f"ACCOUNT_{account['status']}")
        if not account["fee_profile_version"]:
            paper_blocks.append("PAPER_FEE_PROFILE_MISSING")
        if not account["instrument_rule_version"]:
            paper_blocks.append("PAPER_INSTRUMENT_RULES_MISSING")
        if paper_blocks:
            apply_wait(paper_blocks[0])
            return finish(
                {
                    "order_id": order_id,
                    "status": "BLOCKED",
                    "blocks": paper_blocks,
                }
            )
        if str(order["status"]) == OrderStatus.RISK_APPROVED.value:
            connection.execute(
                text(
                    """
                    UPDATE st_order_v2
                    SET status = 'QUEUED', updated_at = :now
                    WHERE order_id = :order_id
                    """
                ),
                {"now": now, "order_id": order_id},
            )
            record_mechanical_transition(
                to_status=OrderStatus.QUEUED,
                next_filled_quantity=current_filled_quantity,
                next_waiting_reason=current_waiting_reason,
                transition_kind=OrderTransitionKind.STATUS_CHANGE,
                source_event_type="V2_ORDER_QUEUED",
            )

        trade_date = now.date()
        rule = _rule(
            connection,
            stock_code=str(order["stock_code"]),
            trade_date=trade_date,
            rule_version=str(
                account["instrument_rule_version"] or ""
            ),
        )
        if (
            not rule
            or not bool(rule["permission_confirmed"])
            or str(rule["fee_profile_version"] or "")
            != str(account["fee_profile_version"] or "")
        ):
            apply_wait("INSTRUMENT_RULE_BLOCKED")
            return finish(
                {
                    "order_id": order_id,
                    "status": "WAITING",
                    "waiting_reason": "INSTRUMENT_RULE_BLOCKED",
                }
            )
        profile = _fee_profile(
            connection,
            version=str(account["fee_profile_version"]),
            security_type=str(rule["security_type"]),
            trade_date=trade_date,
        )
        if profile is None:
            apply_wait("FEE_PROFILE_UNCONFIRMED")
            return finish(
                {
                    "order_id": order_id,
                    "status": "WAITING",
                    "waiting_reason": "FEE_PROFILE_UNCONFIRMED",
                }
            )

        side = OrderSide(str(order["side"]))
        gate_decision = _execution_buy_gate_decision(
            connection,
            order=order,
            now=now,
        )
        if not gate_decision.allowed:
            return cancel_buy_remainder(gate_decision)

        quote = latest_quote(
            connection,
            stock_code=str(order["stock_code"]),
        )
        tick_size = decimal_value(rule["tick_size"])
        if quote is not None:
            pre_close = connection.execute(
                text(
                    """
                    SELECT pre_close FROM st_quote_event_v2
                    WHERE quote_event_id = :event_id
                    """
                ),
                {"event_id": quote.event_id},
            ).scalar()
            limit_ratio = (
                decimal_value(rule["limit_ratio"])
                if rule["limit_ratio"] is not None
                else None
            )
            quote = replace(
                quote,
                upper_limit=quote.upper_limit
                or _price_limit(
                    _nullable_decimal(pre_close),
                    limit_ratio,
                    tick_size,
                    upper=True,
                ),
                lower_limit=quote.lower_limit
                or _price_limit(
                    _nullable_decimal(pre_close),
                    limit_ratio,
                    tick_size,
                    upper=False,
                ),
                suspended=quote.suspended or bool(rule["suspended"]),
            )

        sector_wait_reason = _sector_entry_wait_reason(
            connection,
            strategy_version=str(order["strategy_version"] or ""),
            theme_code=str(order["theme_code"] or ""),
            side=side,
            now=now,
        )
        if sector_wait_reason:
            apply_wait(sector_wait_reason)
            return finish(
                {
                    "order_id": order_id,
                    "status": "WAITING",
                    "waiting_reason": sector_wait_reason,
                }
            )
        remaining_quantity = int(order["quantity"]) - int(
            order["filled_quantity"]
        )
        approved_remaining = int(order["approved_quantity"]) - int(
            order["filled_quantity"]
        )
        if side == OrderSide.SELL:
            sellable = _available_sell_quantity(
                connection,
                account_id=account_id,
                stock_code=str(order["stock_code"]),
                trade_date=trade_date,
            )
            if sellable <= 0:
                apply_wait(WaitingReason.WAIT_T1.value)
                return finish(
                    {
                        "order_id": order_id,
                        "status": "WAITING",
                        "waiting_reason": WaitingReason.WAIT_T1.value,
                    }
                )
            approved_remaining = min(approved_remaining, sellable)

        match = PaperMatcher().match(
            side=side,
            remaining_quantity=remaining_quantity,
            approved_remaining_quantity=approved_remaining,
            limit_price=decimal_value(order["limit_price"]),
            quote=quote,
            now=now,
            tick_size=tick_size,
            liquidity_quantity=approved_remaining,
        )
        execution_price_source = "QMT_LEVEL1"
        matcher_version = "paper_level1_matcher_v2.0.0"
        if (
            match.status == "WAITING"
            and match.waiting_reason
            in {
                WaitingReason.WAIT_NO_QUOTE.value,
                WaitingReason.WAIT_STALE_QUOTE.value,
            }
            and is_internal_paper_configuration(account)
        ):
            snapshot_quote, snapshot_liquidity, snapshot_source = (
                _paper_snapshot_quote(
                    connection,
                    stock_code=str(order["stock_code"]),
                    now=now,
                    lot_size=(
                        int(rule["buy_lot_size"])
                        if side == OrderSide.BUY
                        else int(rule["sell_lot_size"])
                    ),
                    already_filled_quantity=int(
                        order["filled_quantity"]
                    ),
                )
            )
            if snapshot_quote is not None:
                snapshot_pre_close = _nullable_decimal(
                    connection.execute(
                        text(
                            """
                            SELECT pre_close
                            FROM st_quote_event_v2
                            WHERE quote_event_id = :event_id
                            """
                        ),
                        {"event_id": snapshot_quote.event_id},
                    ).scalar()
                )
                limit_ratio = (
                    decimal_value(rule["limit_ratio"])
                    if rule["limit_ratio"] is not None
                    else None
                )
                snapshot_quote = replace(
                    snapshot_quote,
                    upper_limit=snapshot_quote.upper_limit
                    or _price_limit(
                        snapshot_pre_close,
                        limit_ratio,
                        tick_size,
                        upper=True,
                    ),
                    lower_limit=snapshot_quote.lower_limit
                    or _price_limit(
                        snapshot_pre_close,
                        limit_ratio,
                        tick_size,
                        upper=False,
                    ),
                    suspended=(
                        snapshot_quote.suspended
                        or bool(rule["suspended"])
                    ),
                )
                match = PaperSnapshotMatcher().match(
                    side=side,
                    remaining_quantity=remaining_quantity,
                    approved_remaining_quantity=approved_remaining,
                    limit_price=decimal_value(order["limit_price"]),
                    quote=snapshot_quote,
                    now=now,
                    tick_size=tick_size,
                    liquidity_quantity=snapshot_liquidity,
                )
                execution_price_source = snapshot_source
                matcher_version = "paper_snapshot_matcher_v2.0.0"
        if match.status == "WAITING":
            apply_wait(match.waiting_reason)
            return finish(
                {
                    "order_id": order_id,
                    "status": "WAITING",
                    "waiting_reason": match.waiting_reason,
                    "execution_price_source": execution_price_source,
                }
            )
        assert match.fill_price is not None
        entry_trend_wait = _entry_trend_wait_reason(
            strategy_version=str(order["strategy_version"] or ""),
            side=side,
            fill_price=match.fill_price,
            initial_stop=decimal_value(order["initial_stop"]),
        )
        if entry_trend_wait:
            apply_wait(entry_trend_wait)
            return finish(
                {
                    "order_id": order_id,
                    "status": "WAITING",
                    "waiting_reason": entry_trend_wait,
                    "execution_price_source": execution_price_source,
                }
            )
        fill_quantity = int(match.fill_quantity)
        gross = money(match.fill_price * fill_quantity)
        prior_fill = connection.execute(
            text(
                """
                SELECT COALESCE(SUM(gross_amount), 0) AS gross_amount,
                       COALESCE(SUM(quantity), 0) AS quantity
                FROM st_fill_v2
                WHERE order_id = :order_id
                """
            ),
            {"order_id": order_id},
        ).mappings().first()
        fee = profile.calculate_incremental(
            side,
            previous_gross=decimal_value(
                (prior_fill or {}).get("gross_amount") or 0
            ),
            fill_gross=gross,
            previous_quantity=int(
                (prior_fill or {}).get("quantity") or 0
            ),
            fill_quantity=fill_quantity,
        )
        if side == OrderSide.BUY and money(gross + fee) > money(
            account["cash_balance"]
        ):
            apply_wait(WaitingReason.WAIT_LIQUIDITY.value)
            return finish(
                {
                    "order_id": order_id,
                    "status": "WAITING",
                    "waiting_reason": "CASH_CHANGED_AFTER_APPROVAL",
                }
            )

        # Reconstruct the gate again at the last safe point before a fill is
        # written.  The locking reads make revocation and fill mutually
        # exclusive within this transaction; SELL remains exempt.
        gate_decision = _execution_buy_gate_decision(
            connection,
            order=order,
            now=now,
        )
        if not gate_decision.allowed:
            return cancel_buy_remainder(gate_decision)

        match_event_id = canonical_json_hash(
            {
                "order_id": order_id,
                "quote_event_id": match.event_id,
                "matcher": matcher_version,
            }
        )
        idempotency_key = fill_idempotency_key(
            order_id=order_id,
            quote_event_id=match.event_id,
            match_event_id=match_event_id,
        )
        existing_fill = connection.execute(
            text(
                """
                SELECT fill_id FROM st_fill_v2
                WHERE idempotency_key = :key
                """
            ),
            {"key": idempotency_key},
        ).scalar()
        if existing_fill:
            return finish(
                {
                    "order_id": order_id,
                    "status": "idempotent_hit",
                    "fill_id": str(existing_fill),
                }
            )

        fill_id = uuid.uuid4().hex
        net_cash = money(
            -(gross + fee) if side == OrderSide.BUY else gross - fee
        )
        next_cash = money(decimal_value(account["cash_balance"]) + net_cash)
        if next_cash < 0:
            raise RuntimeError("negative cash invariant blocked fill")
        connection.execute(
            text(
                """
                INSERT INTO st_fill_v2
                (fill_id, order_id, account_id, stock_code, side,
                 quantity, price, gross_amount, fee_amount, net_cash_amount,
                 quote_event_id, match_event_id, idempotency_key,
                 filled_at, created_at)
                VALUES
                (:fill_id, :order_id, :account_id, :stock_code, :side,
                 :quantity, :price, :gross, :fee, :net_cash,
                 :quote_event_id, :match_event_id, :idempotency_key,
                 :filled_at, :created_at)
                """
            ),
            {
                "fill_id": fill_id,
                "order_id": order_id,
                "account_id": account_id,
                "stock_code": order["stock_code"],
                "side": side.value,
                "quantity": fill_quantity,
                "price": match.fill_price,
                "gross": gross,
                "fee": fee,
                "net_cash": net_cash,
                "quote_event_id": match.event_id,
                "match_event_id": match_event_id,
                "idempotency_key": idempotency_key,
                "filled_at": now,
                "created_at": datetime.now(),
            },
        )
        cash_event_key = f"FILL:{idempotency_key}"
        connection.execute(
            text(
                """
                INSERT INTO st_cash_ledger_v2
                (cash_event_id, account_id, business_event_key, event_type,
                 amount, balance_after, related_order_id, related_fill_id,
                 occurred_at, created_at)
                VALUES
                (:event_id, :account_id, :business_key, :event_type,
                 :amount, :balance_after, :order_id, :fill_id,
                 :occurred_at, :created_at)
                """
            ),
            {
                "event_id": canonical_json_hash(
                    {"business_event_key": cash_event_key}
                )[:32],
                "account_id": account_id,
                "business_key": cash_event_key,
                "event_type": (
                    "BUY_FILL" if side == OrderSide.BUY else "SELL_FILL"
                ),
                "amount": net_cash,
                "balance_after": next_cash,
                "order_id": order_id,
                "fill_id": fill_id,
                "occurred_at": now,
                "created_at": datetime.now(),
            },
        )
        connection.execute(
            text(
                """
                UPDATE st_trade_account_v2
                SET cash_balance = :cash, updated_at = :now
                WHERE account_id = :account_id
                """
            ),
            {"cash": next_cash, "now": now, "account_id": account_id},
        )

        lot_close_allocations: list[dict[str, Any]] = []
        if side == OrderSide.BUY:
            settlement_date = _settlement_date(
                connection,
                trade_date=trade_date,
                settlement_days=int(rule["settlement_days"]),
            )
            connection.execute(
                text(
                    """
                    INSERT INTO st_position_lot_v2
                    (lot_id, account_id, stock_code, theme_code,
                     strategy_version,
                     opened_fill_id, opened_trade_date, settlement_date,
                     original_quantity, remaining_quantity, cost_price,
                     allocated_buy_fee, position_state,
                     approved_target_quantity, add_count,
                     initial_stop, protective_stop,
                     invalidation_condition, version, created_at)
                    VALUES
                    (:lot_id, :account_id, :stock_code, :theme_code,
                     :strategy_version,
                     :fill_id, :trade_date, :settlement_date,
                     :quantity, :quantity, :price, :fee, 'OPENING',
                     :target_quantity, :add_count,
                     :initial_stop, :protective_stop,
                     :invalidation, 1, :created_at)
                    """
                ),
                {
                    "lot_id": f"LOT:{fill_id}",
                    "account_id": account_id,
                    "stock_code": order["stock_code"],
                    "theme_code": str(order["theme_code"] or "")[:80],
                    "strategy_version": order["strategy_version"],
                    "fill_id": fill_id,
                    "trade_date": trade_date,
                    "settlement_date": settlement_date,
                    "quantity": fill_quantity,
                    "price": match.fill_price,
                    "fee": fee,
                    "target_quantity": (
                        int(order["quantity"]) * 2
                        if str(order["intent_action"]) == "OPEN"
                        else int(order["quantity"])
                    ),
                    "add_count": (
                        1 if str(order["intent_action"]) == "ADD" else 0
                    ),
                    "initial_stop": order["initial_stop"],
                    "protective_stop": order["protective_stop"],
                    "invalidation": order["invalidation_condition"],
                    "created_at": datetime.now(),
                },
            )
        else:
            lot_close_allocations = _consume_sell_lots(
                connection,
                account_id=account_id,
                stock_code=str(order["stock_code"]),
                trade_date=trade_date,
                quantity=fill_quantity,
                now=now,
            )

        total_filled = int(order["filled_quantity"]) + fill_quantity
        next_status = (
            OrderStatus.FILLED.value
            if total_filled >= int(order["quantity"])
            else OrderStatus.PARTIALLY_FILLED.value
        )
        connection.execute(
            text(
                """
                UPDATE st_order_v2
                SET filled_quantity = :filled_quantity,
                    status = :status, waiting_reason = NULL,
                    updated_at = :now
                WHERE order_id = :order_id
                """
            ),
            {
                "filled_quantity": total_filled,
                "status": next_status,
                "now": now,
                "order_id": order_id,
            },
        )
        fill_event_payload = {
            "fill_id": fill_id,
            "order_id": order_id,
            "quote_event_id": match.event_id,
            "match_event_id": match_event_id,
            "side": side.value,
            "quantity": fill_quantity,
            "price": str(match.fill_price),
            "gross_amount": str(gross),
            "fee_amount": str(fee),
            "net_cash_amount": str(net_cash),
            "balance_after": str(next_cash),
            "execution_price_source": execution_price_source,
            "match_explanation": match.explanation,
            "lot_close_allocations": lot_close_allocations,
            "real_order_count": 0,
        }
        _record_event(
            connection,
            account_id=account_id,
            event_type="PAPER_FILL_APPLIED",
            entity_type="FILL",
            entity_id=fill_id,
            payload=fill_event_payload,
            occurred_at=now,
        )
        record_mechanical_transition(
            to_status=OrderStatus(next_status),
            next_filled_quantity=total_filled,
            next_waiting_reason=None,
            transition_kind=OrderTransitionKind.FILL_APPLIED,
            source_event_type="PAPER_FILL_APPLIED",
            source_event_id=fill_id,
            related_fill_id=fill_id,
            source_payload=fill_event_payload,
        )
        return finish(
            {
                "order_id": order_id,
                "status": next_status,
                "fill_id": fill_id,
                "fill_quantity": fill_quantity,
                "fill_price": str(match.fill_price),
                "fee_amount": str(fee),
                "balance_after": str(next_cash),
                "execution_price_source": execution_price_source,
                "match_explanation": match.explanation,
            }
        )


def _nullable_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    number = decimal_value(value)
    return number if number > 0 else None


def run_execution_tick(
    engine: Engine,
    *,
    now: datetime | None = None,
    account_id: str = "paper-main-v2",
    canonical_cutover: CanonicalExecutionCutover | None = None,
) -> dict[str, Any]:
    now = now or datetime.now()
    if canonical_cutover is not None:
        now, _ = _cutover_execution_clocks(now)
    with engine.begin() as connection:
        account = connection.execute(
            text(
                """
                SELECT real_trading_enabled FROM st_trade_account_v2
                WHERE account_id = :account_id
                """
            ),
            {"account_id": account_id},
        ).mappings().first()
        if not account:
            raise ValueError("V2 account not found")
        if bool(account["real_trading_enabled"]):
            raise RuntimeError(
                "V2 safety violation: real trading must remain disabled"
            )
        if canonical_cutover is None:
            expired_orders = int(
                connection.execute(
                    text(
                        """
                        UPDATE st_order_v2
                        SET status = 'EXPIRED',
                            waiting_reason = NULL,
                            updated_at = :now
                        WHERE account_id = :account_id
                          AND status IN (
                              'RISK_APPROVED', 'QUEUED',
                              'PARTIALLY_FILLED'
                          )
                          AND expires_at <= :now
                        """
                    ),
                    {"account_id": account_id, "now": now},
                ).rowcount
                or 0
            )
        else:
            # The cutover path may never expire orders in a transaction that
            # omits evidence/accounting/outbox.  Each expiry is delegated to
            # `_execute_one` below and shares its rollback boundary.
            expired_orders = 0
    market_open = is_trade_day(engine, now.date()) and _session_open(now)
    if canonical_cutover is None and not market_open:
        v3_plan_updates = _sync_v3_execution_plan_states(
            engine,
            account_id=account_id,
            now=now,
        )
        return {
            "status": "market_closed",
            "account_id": account_id,
            "processed_orders": 0,
            "expired_orders": expired_orders,
            "fill_count": 0,
            "real_orders": 0,
            "v3_execution_plan_updates": v3_plan_updates,
        }
    with engine.connect() as connection:
        if canonical_cutover is not None and not market_open:
            rows = connection.execute(
                text(
                    """
                    SELECT order_id FROM st_order_v2
                    WHERE account_id = :account_id
                      AND status IN
                          ('RISK_APPROVED','QUEUED','PARTIALLY_FILLED')
                      AND expires_at <= :now
                    ORDER BY created_at, order_id
                    """
                ),
                {"account_id": account_id, "now": now},
            ).fetchall()
        else:
            rows = connection.execute(
                text(
                    """
                    SELECT order_id FROM st_order_v2
                    WHERE account_id = :account_id
                      AND status IN
                          ('RISK_APPROVED','QUEUED','PARTIALLY_FILLED')
                    ORDER BY created_at, order_id
                    """
                ),
                {"account_id": account_id},
            ).fetchall()
    if canonical_cutover is None:
        results = [
            _execute_one(
                engine,
                order_id=str(order_id),
                account_id=account_id,
                now=now,
            )
            for (order_id,) in rows
        ]
    else:
        results = [
            _execute_one(
                engine,
                order_id=str(order_id),
                account_id=account_id,
                now=now,
                canonical_cutover=canonical_cutover,
            )
            for (order_id,) in rows
        ]
        expired_orders = sum(
            1 for item in results if item.get("status") == "EXPIRED"
        )
    fill_count = sum(
        1 for item in results if item.get("fill_id")
    )
    blocked = sorted(
        {
            block
            for item in results
            for block in item.get("blocks", [])
        }
    )
    if canonical_cutover is None:
        v3_plan_updates = _sync_v3_execution_plan_states(
            engine,
            account_id=account_id,
            now=now,
        )
    else:
        # The V3 outbox is now the sole projection path.  Calling the legacy
        # post-commit synchronizer here would reintroduce a second write path.
        v3_plan_updates = 0
    response = {
        "status": (
            "market_closed"
            if not market_open
            else "idle"
            if not rows
            else "blocked"
            if blocked
            else "completed"
        ),
        "account_id": account_id,
        "processed_orders": len(results),
        "expired_orders": expired_orders,
        "fill_count": fill_count,
        "real_orders": 0,
        "v3_execution_plan_updates": v3_plan_updates,
        "blocks": blocked,
        "orders": results,
    }
    if canonical_cutover is not None:
        response.update(
            {
                "canonical_cutover_active": True,
                "legacy_v3_direct_sync_suppressed": True,
                "production_activation_allowed": False,
            }
        )
    return response
