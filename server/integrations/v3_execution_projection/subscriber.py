"""Opt-in persistence boundary for verified V3 execution projections.

The subscriber accepts an existing SQLAlchemy ``Connection`` and never opens,
commits, or rolls back a transaction itself.  It reads the canonical V2
intent/order identity solely to verify a V3 read-model binding; all writes are
limited to the V3 projection binding, inbox, head, and execution-plan state.
No production worker imports this module.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from .projector import (
    V3ExecutionProjection,
    validate_v3_execution_projection,
)


class ProjectionApplyStatus(str, Enum):
    APPLIED = "APPLIED"
    IDEMPOTENT = "IDEMPOTENT"


@dataclass(frozen=True, slots=True)
class V3ProjectionApplyResult:
    status: ProjectionApplyStatus
    projection_id: str
    execution_plan_id: str
    source_sequence: int
    plan_state: str


class V3ProjectionSubscriberError(ValueError):
    """Raised when canonical binding, ordering, or idempotency fails closed."""


def _required_text(value: Any, field_name: str) -> str:
    if type(value) is not str:
        raise V3ProjectionSubscriberError(
            f"{field_name} must be exactly str"
        )
    normalized = value.strip()
    if not normalized:
        raise V3ProjectionSubscriberError(f"{field_name} is required")
    return normalized


def _integer(value: Any, field_name: str, *, minimum: int = 0) -> int:
    if type(value) is not int:
        raise V3ProjectionSubscriberError(
            f"{field_name} must be exactly int"
        )
    if value < minimum:
        raise V3ProjectionSubscriberError(
            f"{field_name} must be at least {minimum}"
        )
    return value


def _aware_utc(value: Any, field_name: str) -> datetime:
    if type(value) is not datetime:
        raise TypeError(f"{field_name} must be exactly datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise V3ProjectionSubscriberError(
            f"{field_name} must be timezone-aware"
        )
    return value.astimezone(timezone.utc)


def _db_datetime(value: Any, field_name: str) -> datetime:
    if type(value) is not datetime:
        raise V3ProjectionSubscriberError(
            f"stored {field_name} must be exactly datetime"
        )
    parsed = value
    if parsed.tzinfo is not None and parsed.utcoffset() is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _utc_naive(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _for_update(connection: Connection) -> str:
    dialect = str(
        getattr(getattr(connection, "dialect", None), "name", "") or ""
    ).lower()
    return " FOR UPDATE" if dialect in {"mysql", "mariadb"} else ""


def _first_mapping(result: Any) -> dict[str, Any] | None:
    row = result.mappings().first()
    return dict(row) if row is not None else None


def _side_matches_action(side: str, action: str) -> bool:
    normalized_side = side.strip().upper()
    normalized_action = action.strip().upper()
    if normalized_side == "BUY":
        return normalized_action in {"BUY", "OPEN", "ADD"}
    if normalized_side == "SELL":
        return normalized_action in {"SELL", "REDUCE", "EXIT"}
    return False


def _canonical_mapping(
    connection: Connection,
    projection: V3ExecutionProjection,
) -> dict[str, Any]:
    suffix = _for_update(connection)
    row = _first_mapping(
        connection.execute(
            text(
                """
                SELECT p.execution_plan_id,
                       p.account_id AS plan_account_id,
                       p.run_uid AS plan_run_uid,
                       p.stock_code AS plan_stock_code,
                       p.side AS plan_side,
                       p.quantity AS plan_quantity,
                       p.state AS plan_state,
                       p.real_order_allowed,
                       p.created_at AS plan_created_at,
                       i.intent_id,
                       i.account_id AS intent_account_id,
                       i.decision_run_uid AS intent_run_uid,
                       i.stock_code AS intent_stock_code,
                       i.action AS intent_action,
                       i.current_quantity AS intent_current_quantity,
                       i.target_quantity AS intent_target_quantity,
                       o.order_id,
                       o.intent_id AS order_intent_id,
                       o.account_id AS order_account_id,
                       o.stock_code AS order_stock_code,
                       o.side AS order_side,
                       o.quantity AS order_quantity,
                       o.created_at AS order_created_at,
                       o.updated_at AS order_updated_at
                FROM st_execution_plan_v3 p
                JOIN st_trade_intent_v2 i
                  ON i.intent_id = :source_intent_id
                JOIN st_order_v2 o
                  ON o.order_id = :source_order_id
                 AND o.intent_id = i.intent_id
                WHERE p.execution_plan_id = :execution_plan_id
                """
                + suffix
            ),
            {
                "execution_plan_id": projection.execution_plan_id,
                "source_intent_id": projection.source_intent_id,
                "source_order_id": projection.source_order_id,
            },
        )
    )
    if row is None:
        raise V3ProjectionSubscriberError(
            "canonical plan/intent/order binding was not found"
        )

    plan_account = _required_text(row["plan_account_id"], "plan_account_id")
    intent_account = _required_text(
        row["intent_account_id"],
        "intent_account_id",
    )
    order_account = _required_text(
        row["order_account_id"],
        "order_account_id",
    )
    if len({plan_account, intent_account, order_account}) != 1:
        raise V3ProjectionSubscriberError("canonical account binding differs")

    plan_run = _required_text(row["plan_run_uid"], "plan_run_uid")
    intent_run = _required_text(row["intent_run_uid"], "intent_run_uid")
    if plan_run != intent_run:
        raise V3ProjectionSubscriberError("canonical run binding differs")

    plan_stock = _required_text(row["plan_stock_code"], "plan_stock_code")
    intent_stock = _required_text(
        row["intent_stock_code"],
        "intent_stock_code",
    )
    order_stock = _required_text(row["order_stock_code"], "order_stock_code")
    if len({plan_stock, intent_stock, order_stock}) != 1:
        raise V3ProjectionSubscriberError("canonical stock binding differs")

    plan_side = _required_text(row["plan_side"], "plan_side").upper()
    order_side = _required_text(row["order_side"], "order_side").upper()
    intent_action = _required_text(row["intent_action"], "intent_action").upper()
    if plan_side != order_side or not _side_matches_action(
        plan_side,
        intent_action,
    ):
        raise V3ProjectionSubscriberError("canonical side binding differs")
    plan_quantity = _integer(
        row["plan_quantity"],
        "plan_quantity",
        minimum=1,
    )
    order_quantity = _integer(
        row["order_quantity"],
        "order_quantity",
        minimum=1,
    )
    if plan_quantity != order_quantity:
        raise V3ProjectionSubscriberError("canonical quantity binding differs")
    current_quantity = _integer(
        row["intent_current_quantity"],
        "intent_current_quantity",
    )
    target_quantity = _integer(
        row["intent_target_quantity"],
        "intent_target_quantity",
    )
    intended_delta = (
        target_quantity - current_quantity
        if plan_side == "BUY"
        else current_quantity - target_quantity
    )
    if intended_delta < order_quantity:
        raise V3ProjectionSubscriberError(
            "canonical order quantity exceeds the intent delta"
        )
    if _integer(row["real_order_allowed"], "real_order_allowed") != 0:
        raise V3ProjectionSubscriberError(
            "projection subscriber refuses a real-order-enabled V3 plan"
        )

    if _required_text(row["intent_id"], "intent_id") != (
        projection.source_intent_id
    ):
        raise V3ProjectionSubscriberError("canonical intent_id differs")
    if _required_text(row["order_id"], "order_id") != projection.source_order_id:
        raise V3ProjectionSubscriberError("canonical order_id differs")
    if _required_text(row["order_intent_id"], "order_intent_id") != (
        projection.source_intent_id
    ):
        raise V3ProjectionSubscriberError("canonical order intent_id differs")

    plan_created_at = _db_datetime(
        row["plan_created_at"],
        "plan_created_at",
    )
    order_created_at = _db_datetime(
        row["order_created_at"],
        "order_created_at",
    )
    binding_bound_at = _utc_naive(projection.binding_bound_at)
    projected_order_created_at = _utc_naive(
        projection.source_order_created_at
    )
    if projected_order_created_at != order_created_at:
        raise V3ProjectionSubscriberError(
            "canonical order creation time differs from projection"
        )
    if binding_bound_at != order_created_at:
        raise V3ProjectionSubscriberError(
            "execution plan binding must use canonical order creation time"
        )
    if plan_created_at > binding_bound_at:
        raise V3ProjectionSubscriberError(
            "execution plan was created after its canonical order binding"
        )
    if _db_datetime(row["order_updated_at"], "order_updated_at") < (
        order_created_at
    ):
        raise V3ProjectionSubscriberError(
            "canonical order timestamp precedes creation"
        )

    return {
        **row,
        "account_id": plan_account,
        "run_uid": plan_run,
        "stock_code": plan_stock,
        "side": plan_side,
        "quantity": plan_quantity,
        "plan_state": _required_text(row["plan_state"], "plan_state"),
    }


def _load_binding(
    connection: Connection,
    execution_plan_id: str,
) -> dict[str, Any] | None:
    return _first_mapping(
        connection.execute(
            text(
                """
                SELECT execution_plan_id, binding_id, binding_hash,
                       source_intent_id, source_order_id, account_id,
                       run_uid, stock_code, side, quantity, bound_at
                FROM st_execution_plan_binding_v3
                WHERE execution_plan_id = :execution_plan_id
                """
                + _for_update(connection)
            ),
            {"execution_plan_id": execution_plan_id},
        )
    )


def _load_order_binding(
    connection: Connection,
    source_order_id: str,
) -> dict[str, Any] | None:
    return _first_mapping(
        connection.execute(
            text(
                """
                SELECT execution_plan_id, source_order_id
                FROM st_execution_plan_binding_v3
                WHERE source_order_id = :source_order_id
                """
                + _for_update(connection)
            ),
            {"source_order_id": source_order_id},
        )
    )


def _load_head(
    connection: Connection,
    execution_plan_id: str,
) -> dict[str, Any] | None:
    return _first_mapping(
        connection.execute(
            text(
                """
                SELECT execution_plan_id, binding_id, binding_hash,
                       source_order_id, last_source_sequence,
                       last_projection_id, last_payload_hash,
                       last_plan_state, updated_at
                FROM st_execution_projection_head_v3
                WHERE execution_plan_id = :execution_plan_id
                """
                + _for_update(connection)
            ),
            {"execution_plan_id": execution_plan_id},
        )
    )


def _load_inbox(
    connection: Connection,
    projection: V3ExecutionProjection,
) -> dict[str, Any] | None:
    return _first_mapping(
        connection.execute(
            text(
                """
                SELECT projection_id, payload_hash, execution_plan_id,
                       source_order_id, source_event_id, source_sequence,
                       plan_state
                FROM st_execution_projection_inbox_v3
                WHERE source_order_id = :source_order_id
                  AND source_event_id = :source_event_id
                """
                + _for_update(connection)
            ),
            {
                "source_order_id": projection.source_order_id,
                "source_event_id": projection.source_event_id,
            },
        )
    )


def _verify_stored_binding(
    stored: dict[str, Any],
    projection: V3ExecutionProjection,
    canonical: dict[str, Any],
) -> None:
    expected_text = {
        "execution_plan_id": projection.execution_plan_id,
        "binding_id": projection.source_binding_id,
        "binding_hash": projection.source_binding_hash,
        "source_intent_id": projection.source_intent_id,
        "source_order_id": projection.source_order_id,
        "account_id": canonical["account_id"],
        "run_uid": canonical["run_uid"],
        "stock_code": canonical["stock_code"],
        "side": canonical["side"],
    }
    for field_name, expected in expected_text.items():
        actual = _required_text(stored[field_name], f"stored {field_name}")
        if field_name == "side":
            actual = actual.upper()
        if actual != expected:
            raise V3ProjectionSubscriberError(
                f"execution plan binding changed: {field_name}"
            )
    if _integer(stored["quantity"], "stored quantity", minimum=1) != (
        canonical["quantity"]
    ):
        raise V3ProjectionSubscriberError(
            "execution plan binding changed: quantity"
        )
    stored_bound_at = _db_datetime(stored["bound_at"], "bound_at")
    if stored_bound_at != _utc_naive(projection.binding_bound_at):
        raise V3ProjectionSubscriberError(
            "execution plan binding changed: bound_at"
        )


def _verify_head(
    head: dict[str, Any],
    projection: V3ExecutionProjection,
) -> int:
    for field_name, expected in (
        ("execution_plan_id", projection.execution_plan_id),
        ("binding_id", projection.source_binding_id),
        ("binding_hash", projection.source_binding_hash),
        ("source_order_id", projection.source_order_id),
    ):
        if _required_text(head[field_name], f"head {field_name}") != expected:
            raise V3ProjectionSubscriberError(
                f"projection head binding changed: {field_name}"
            )
    return _integer(
        head["last_source_sequence"],
        "last_source_sequence",
    )


def _insert_binding(
    connection: Connection,
    projection: V3ExecutionProjection,
    canonical: dict[str, Any],
    applied_at: datetime,
) -> None:
    connection.execute(
        text(
            """
            INSERT INTO st_execution_plan_binding_v3 (
                execution_plan_id, binding_id, binding_hash,
                source_intent_id, source_order_id, account_id,
                run_uid, stock_code, side, quantity, bound_at, created_at
            ) VALUES (
                :execution_plan_id, :binding_id, :binding_hash,
                :source_intent_id, :source_order_id, :account_id,
                :run_uid, :stock_code, :side, :quantity, :bound_at, :created_at
            )
            """
        ),
        {
            "execution_plan_id": projection.execution_plan_id,
            "binding_id": projection.source_binding_id,
            "binding_hash": projection.source_binding_hash,
            "source_intent_id": projection.source_intent_id,
            "source_order_id": projection.source_order_id,
            "account_id": canonical["account_id"],
            "run_uid": canonical["run_uid"],
            "stock_code": canonical["stock_code"],
            "side": canonical["side"],
            "quantity": canonical["quantity"],
            "bound_at": _utc_naive(projection.binding_bound_at),
            "created_at": _utc_naive(applied_at),
        },
    )


def _insert_inbox(
    connection: Connection,
    projection: V3ExecutionProjection,
    applied_at: datetime,
) -> None:
    connection.execute(
        text(
            """
            INSERT INTO st_execution_projection_inbox_v3 (
                projection_id, payload_hash, execution_plan_id,
                binding_id, binding_hash, binding_bound_at,
                source_intent_id, source_order_id,
                source_order_created_at, source_event_id,
                source_sequence, source_result_idempotency_key,
                source_result_fingerprint, source_transition_id,
                source_transition_payload_hash, source_order_state_hash,
                source_order_status, cumulative_filled_quantity,
                plan_state, occurred_at, applied_at
            ) VALUES (
                :projection_id, :payload_hash, :execution_plan_id,
                :binding_id, :binding_hash, :binding_bound_at,
                :source_intent_id, :source_order_id,
                :source_order_created_at, :source_event_id,
                :source_sequence, :source_result_idempotency_key,
                :source_result_fingerprint, :source_transition_id,
                :source_transition_payload_hash, :source_order_state_hash,
                :source_order_status, :cumulative_filled_quantity,
                :plan_state, :occurred_at, :applied_at
            )
            """
        ),
        {
            "projection_id": projection.projection_id,
            "payload_hash": projection.payload_hash,
            "execution_plan_id": projection.execution_plan_id,
            "binding_id": projection.source_binding_id,
            "binding_hash": projection.source_binding_hash,
            "binding_bound_at": _utc_naive(projection.binding_bound_at),
            "source_intent_id": projection.source_intent_id,
            "source_order_id": projection.source_order_id,
            "source_order_created_at": _utc_naive(
                projection.source_order_created_at
            ),
            "source_event_id": projection.source_event_id,
            "source_sequence": projection.source_sequence,
            "source_result_idempotency_key": (
                projection.source_result_idempotency_key
            ),
            "source_result_fingerprint": projection.source_result_fingerprint,
            "source_transition_id": projection.source_transition_id,
            "source_transition_payload_hash": (
                projection.source_transition_payload_hash
            ),
            "source_order_state_hash": projection.source_order_state_hash,
            "source_order_status": projection.source_order_status.value,
            "cumulative_filled_quantity": (
                projection.cumulative_filled_quantity
            ),
            "plan_state": projection.state.value,
            "occurred_at": _utc_naive(projection.occurred_at),
            "applied_at": _utc_naive(applied_at),
        },
    )


def apply_v3_execution_projection(
    connection: Connection,
    projection: V3ExecutionProjection,
    *,
    applied_at: datetime,
) -> V3ProjectionApplyResult:
    """Validate and atomically apply one projection on the caller's connection."""

    if not hasattr(connection, "execute"):
        raise TypeError("connection must provide Connection.execute")
    in_transaction = getattr(connection, "in_transaction", None)
    if callable(in_transaction) and not bool(in_transaction()):
        raise V3ProjectionSubscriberError(
            "subscriber requires a caller-owned active transaction"
        )
    if type(projection) is not V3ExecutionProjection:
        raise TypeError("projection must be exactly V3ExecutionProjection")
    if not validate_v3_execution_projection(projection):
        raise V3ProjectionSubscriberError("projection is invalid")
    normalized_applied_at = _aware_utc(applied_at, "applied_at")
    if normalized_applied_at < projection.occurred_at:
        raise V3ProjectionSubscriberError(
            "applied_at cannot precede projection occurred_at"
        )
    if len(projection.source_event_id) > 255:
        raise V3ProjectionSubscriberError(
            "source_event_id exceeds the V3 inbox storage contract"
        )

    canonical = _canonical_mapping(connection, projection)
    stored_binding = _load_binding(connection, projection.execution_plan_id)
    order_binding = _load_order_binding(connection, projection.source_order_id)
    if order_binding is not None and _required_text(
        order_binding["execution_plan_id"],
        "order binding execution_plan_id",
    ) != projection.execution_plan_id:
        raise V3ProjectionSubscriberError(
            "source order is already bound to another execution plan"
        )
    if stored_binding is not None:
        _verify_stored_binding(stored_binding, projection, canonical)

    head = _load_head(connection, projection.execution_plan_id)
    last_sequence = _verify_head(head, projection) if head is not None else 0
    if head is None:
        if canonical["plan_state"] != "PAPER_QUEUED":
            raise V3ProjectionSubscriberError(
                "projection head cannot adopt a non-queued execution plan"
            )
    elif canonical["plan_state"] != _required_text(
        head["last_plan_state"],
        "head last_plan_state",
    ):
        raise V3ProjectionSubscriberError(
            "execution plan state drifted from the projection head"
        )
    inbox = _load_inbox(connection, projection)
    if inbox is not None:
        stored_payload = _required_text(inbox["payload_hash"], "payload_hash")
        if stored_payload != projection.payload_hash:
            raise V3ProjectionSubscriberError(
                "source event was replayed with a different payload"
            )
        for field_name, expected in (
            ("projection_id", projection.projection_id),
            ("execution_plan_id", projection.execution_plan_id),
            ("source_order_id", projection.source_order_id),
            ("source_event_id", projection.source_event_id),
        ):
            if _required_text(inbox[field_name], f"inbox {field_name}") != expected:
                raise V3ProjectionSubscriberError(
                    f"stored source event identity differs: {field_name}"
                )
        stored_sequence = _integer(
            inbox["source_sequence"],
            "inbox source_sequence",
            minimum=1,
        )
        if stored_sequence != projection.source_sequence:
            raise V3ProjectionSubscriberError(
                "stored source event sequence differs"
            )
        if stored_binding is None or head is None or last_sequence < stored_sequence:
            raise V3ProjectionSubscriberError(
                "idempotent source event has incomplete binding/head state"
            )
        return V3ProjectionApplyResult(
            status=ProjectionApplyStatus.IDEMPOTENT,
            projection_id=projection.projection_id,
            execution_plan_id=projection.execution_plan_id,
            source_sequence=projection.source_sequence,
            plan_state=_required_text(inbox["plan_state"], "inbox plan_state"),
        )

    if head is not None and _utc_naive(normalized_applied_at) < _db_datetime(
        head["updated_at"],
        "head updated_at",
    ):
        raise V3ProjectionSubscriberError(
            "applied_at cannot move the projection head backwards"
        )
    expected_sequence = last_sequence + 1
    if projection.source_sequence < expected_sequence:
        raise V3ProjectionSubscriberError(
            "stale source event cannot regress the execution plan"
        )
    if projection.source_sequence > expected_sequence:
        raise V3ProjectionSubscriberError(
            "source event sequence contains a gap"
        )
    if stored_binding is None:
        _insert_binding(
            connection,
            projection,
            canonical,
            normalized_applied_at,
        )

    _insert_inbox(connection, projection, normalized_applied_at)
    head_parameters = {
        "execution_plan_id": projection.execution_plan_id,
        "binding_id": projection.source_binding_id,
        "binding_hash": projection.source_binding_hash,
        "source_order_id": projection.source_order_id,
        "source_sequence": projection.source_sequence,
        "projection_id": projection.projection_id,
        "payload_hash": projection.payload_hash,
        "plan_state": projection.state.value,
        "updated_at": _utc_naive(normalized_applied_at),
    }
    if head is None:
        connection.execute(
            text(
                """
                INSERT INTO st_execution_projection_head_v3 (
                    execution_plan_id, binding_id, binding_hash,
                    source_order_id, last_source_sequence,
                    last_projection_id, last_payload_hash,
                    last_plan_state, updated_at
                ) VALUES (
                    :execution_plan_id, :binding_id, :binding_hash,
                    :source_order_id, :source_sequence,
                    :projection_id, :payload_hash,
                    :plan_state, :updated_at
                )
                """
            ),
            head_parameters,
        )
    else:
        head_parameters["previous_sequence"] = last_sequence
        updated_head = connection.execute(
            text(
                """
                UPDATE st_execution_projection_head_v3
                SET last_source_sequence = :source_sequence,
                    last_projection_id = :projection_id,
                    last_payload_hash = :payload_hash,
                    last_plan_state = :plan_state,
                    updated_at = :updated_at
                WHERE execution_plan_id = :execution_plan_id
                  AND binding_id = :binding_id
                  AND binding_hash = :binding_hash
                  AND source_order_id = :source_order_id
                  AND last_source_sequence = :previous_sequence
                """
            ),
            head_parameters,
        )
        if int(updated_head.rowcount or 0) != 1:
            raise V3ProjectionSubscriberError("projection head CAS failed")

    updated_plan = connection.execute(
        text(
            """
            UPDATE st_execution_plan_v3
            SET state = :next_state,
                updated_at = CASE
                    WHEN updated_at > :updated_at THEN updated_at
                    ELSE :updated_at
                END
            WHERE execution_plan_id = :execution_plan_id
              AND state = :previous_state
              AND real_order_allowed = 0
            """
        ),
        {
            "execution_plan_id": projection.execution_plan_id,
            "previous_state": canonical["plan_state"],
            "next_state": projection.state.value,
            "updated_at": _utc_naive(normalized_applied_at),
        },
    )
    plan_rowcount = int(updated_plan.rowcount or 0)
    if plan_rowcount != 1 and canonical["plan_state"] != projection.state.value:
        raise V3ProjectionSubscriberError("execution plan state CAS failed")

    return V3ProjectionApplyResult(
        status=ProjectionApplyStatus.APPLIED,
        projection_id=projection.projection_id,
        execution_plan_id=projection.execution_plan_id,
        source_sequence=projection.source_sequence,
        plan_state=projection.state.value,
    )


__all__ = [
    "ProjectionApplyStatus",
    "V3ProjectionApplyResult",
    "V3ProjectionSubscriberError",
    "apply_v3_execution_projection",
]
