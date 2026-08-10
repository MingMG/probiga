"""Transactional append port for V2-to-V3 execution projections."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from server.integrations.v3_execution_projection import (
    V3ExecutionProjection,
    validate_v3_execution_projection,
)


class V3ProjectionOutboxError(RuntimeError):
    pass


class V3ProjectionOutboxConflictError(V3ProjectionOutboxError):
    pass


class V3ProjectionOutboxAppendStatus(str, Enum):
    INSERTED = "INSERTED"
    IDEMPOTENT = "IDEMPOTENT"


@dataclass(frozen=True, slots=True)
class V3ProjectionOutboxAppendResult:
    status: V3ProjectionOutboxAppendStatus
    outbox_id: str
    projection_id: str
    canonical_payload_hash: str


def _aware_utc_naive(value: object, field_name: str) -> datetime:
    if type(value) is not datetime:
        raise TypeError(f"{field_name} must be exactly datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _active_connection(connection: Any) -> Any:
    if connection is None or not callable(getattr(connection, "execute", None)):
        raise V3ProjectionOutboxError("outbox requires a caller-owned connection")
    probe = getattr(connection, "in_transaction", None)
    if not callable(probe) or probe() is not True:
        raise V3ProjectionOutboxError(
            "outbox append requires an active caller-owned V2 transaction"
        )
    return connection


def projection_to_payload(projection: V3ExecutionProjection) -> dict[str, Any]:
    if type(projection) is not V3ExecutionProjection:
        raise TypeError("projection must be exactly V3ExecutionProjection")
    if not validate_v3_execution_projection(projection):
        raise V3ProjectionOutboxError("projection validation failed")
    return {
        "projection_id": projection.projection_id,
        "payload_hash": projection.payload_hash,
        "execution_plan_id": projection.execution_plan_id,
        "source_binding_id": projection.source_binding_id,
        "source_binding_hash": projection.source_binding_hash,
        "binding_bound_at": projection.binding_bound_at.isoformat(
            timespec="microseconds"
        ),
        "source_intent_id": projection.source_intent_id,
        "source_order_id": projection.source_order_id,
        "source_order_created_at": projection.source_order_created_at.isoformat(
            timespec="microseconds"
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
        "cumulative_filled_quantity": projection.cumulative_filled_quantity,
        "state": projection.state.value,
        "occurred_at": projection.occurred_at.isoformat(timespec="microseconds"),
    }


def _canonical_payload(projection: V3ExecutionProjection) -> tuple[str, str]:
    payload_json = json.dumps(
        projection_to_payload(projection),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return payload_json, hashlib.sha256(payload_json.encode("utf-8")).hexdigest()


def projection_from_payload(payload_json: str) -> V3ExecutionProjection:
    if not isinstance(payload_json, str) or not payload_json:
        raise V3ProjectionOutboxError("outbox payload_json is required")
    try:
        payload = json.loads(payload_json)
    except (TypeError, ValueError) as exc:
        raise V3ProjectionOutboxError("outbox payload is not valid JSON") from exc
    if type(payload) is not dict:
        raise V3ProjectionOutboxError("outbox payload must be an object")
    for field_name in (
        "binding_bound_at",
        "source_order_created_at",
        "occurred_at",
    ):
        try:
            value = datetime.fromisoformat(str(payload[field_name]))
        except (KeyError, TypeError, ValueError) as exc:
            raise V3ProjectionOutboxError(
                f"outbox {field_name} is invalid"
            ) from exc
        if value.tzinfo is None or value.utcoffset() is None:
            raise V3ProjectionOutboxError(
                f"outbox {field_name} must be timezone-aware"
            )
        payload[field_name] = value
    try:
        projection = V3ExecutionProjection(**payload)
    except (TypeError, ValueError) as exc:
        raise V3ProjectionOutboxError("outbox projection is invalid") from exc
    if not validate_v3_execution_projection(projection):
        raise V3ProjectionOutboxError("outbox projection failed revalidation")
    expected_json, _ = _canonical_payload(projection)
    if expected_json != payload_json:
        raise V3ProjectionOutboxError("outbox projection JSON is not canonical")
    return projection


def append_v3_transition_outbox(
    connection: Any,
    projection: V3ExecutionProjection,
    *,
    created_at: datetime,
) -> V3ProjectionOutboxAppendResult:
    """Append an immutable projection inside the caller's V2 transaction."""

    active = _active_connection(connection)
    payload_json, canonical_hash = _canonical_payload(projection)
    normalized_created_at = _aware_utc_naive(created_at, "created_at")
    normalized_occurred_at = _aware_utc_naive(
        projection.occurred_at,
        "projection.occurred_at",
    )
    if normalized_created_at < normalized_occurred_at:
        raise V3ProjectionOutboxError(
            "outbox created_at cannot precede projection occurred_at"
        )
    outbox_id = projection.projection_id
    baseline = active.execute(
        text(
            "SELECT baseline_sequence, baseline_audit_hash "
            "FROM st_execution_projection_order_baseline_v3 "
            "WHERE source_order_id = :source_order_id FOR UPDATE"
        ),
        {"source_order_id": projection.source_order_id},
    ).mappings().first()
    baseline_sequence = 0
    if baseline is not None:
        baseline_sequence = int(baseline["baseline_sequence"])
        baseline_audit_hash = str(baseline["baseline_audit_hash"])
        if (
            baseline_sequence < 1
            or len(baseline_audit_hash) != 64
            or baseline_audit_hash != baseline_audit_hash.lower()
            or any(character not in "0123456789abcdef" for character in baseline_audit_hash)
        ):
            raise V3ProjectionOutboxError(
                "outbox order baseline is structurally invalid"
            )
        if projection.source_sequence <= baseline_sequence:
            raise V3ProjectionOutboxError(
                "outbox projection does not advance its audited baseline"
            )
    identity_parameters = {
        "outbox_id": outbox_id,
        "projection_id": projection.projection_id,
        "source_transition_id": projection.source_transition_id,
        "source_order_id": projection.source_order_id,
        "source_sequence": projection.source_sequence,
    }
    select_identity = text(
        "SELECT outbox_id, projection_id, projection_payload_hash, "
        "canonical_payload_hash, payload_json, source_order_id, "
        "source_transition_id, source_sequence "
        "FROM st_execution_projection_outbox_v2 "
        "WHERE outbox_id = :outbox_id OR projection_id = :projection_id "
        "OR source_transition_id = :source_transition_id "
        "OR (source_order_id = :source_order_id "
        "AND source_sequence = :source_sequence) FOR UPDATE"
    )
    stored_rows = tuple(
        active.execute(
            select_identity,
            identity_parameters,
        ).mappings()
    )
    if len(stored_rows) > 1:
        raise V3ProjectionOutboxConflictError(
            "V3 projection outbox identity resolves to multiple rows"
        )
    stored = stored_rows[0] if stored_rows else None
    expected = {
        "outbox_id": outbox_id,
        "projection_id": projection.projection_id,
        "projection_payload_hash": projection.payload_hash,
        "canonical_payload_hash": canonical_hash,
        "payload_json": payload_json,
        "source_order_id": projection.source_order_id,
        "source_transition_id": projection.source_transition_id,
        "source_sequence": projection.source_sequence,
    }
    if stored is not None:
        observed = {
            name: (
                int(stored[name])
                if name == "source_sequence"
                else str(stored[name])
            )
            for name in expected
        }
        if observed != expected:
            raise V3ProjectionOutboxConflictError(
                "V3 projection outbox identity already carries different content"
            )
        return V3ProjectionOutboxAppendResult(
            status=V3ProjectionOutboxAppendStatus.IDEMPOTENT,
            outbox_id=outbox_id,
            projection_id=projection.projection_id,
            canonical_payload_hash=canonical_hash,
        )
    tail = active.execute(
        text(
            "SELECT source_sequence FROM st_execution_projection_outbox_v2 "
            "WHERE source_order_id = :source_order_id "
            "ORDER BY source_sequence DESC LIMIT 1 FOR UPDATE"
        ),
        {"source_order_id": projection.source_order_id},
    ).mappings().first()
    previous_sequence = (
        baseline_sequence if tail is None else int(tail["source_sequence"])
    )
    if previous_sequence < baseline_sequence:
        raise V3ProjectionOutboxError(
            "outbox history precedes its audited baseline"
        )
    if projection.source_sequence != previous_sequence + 1:
        raise V3ProjectionOutboxError(
            "outbox append requires the next contiguous order sequence"
        )
    insert_statement = text(
        """
            INSERT INTO st_execution_projection_outbox_v2 (
                outbox_id, projection_id, projection_payload_hash,
                canonical_payload_hash, payload_json, source_order_id,
                source_transition_id, source_sequence, status, attempt_count,
                available_at, created_at, updated_at
            ) VALUES (
                :outbox_id, :projection_id, :projection_payload_hash,
                :canonical_payload_hash, :payload_json, :source_order_id,
                :source_transition_id, :source_sequence, 'PENDING', 0,
                :created_at, :created_at, :created_at
            )
        """
    )
    insert_parameters = {
        **expected,
        "outbox_id": outbox_id,
        "source_order_id": projection.source_order_id,
        "source_sequence": projection.source_sequence,
        "created_at": normalized_created_at,
    }
    try:
        insert_result = active.execute(insert_statement, insert_parameters)
    except IntegrityError as exc:
        error_code = getattr(getattr(exc, "orig", None), "args", (None,))[0]
        if error_code != 1062:
            raise
        # A concurrent canonical transaction may have won the natural-key
        # insert.  MySQL keeps the caller-owned transaction usable after a
        # duplicate statement error, so compare the locked winner exactly.
        winner_rows = tuple(
            active.execute(
                select_identity,
                identity_parameters,
            ).mappings()
        )
        if len(winner_rows) != 1:
            raise V3ProjectionOutboxConflictError(
                "concurrent V3 projection outbox identity is not unique"
            ) from exc
        winner = winner_rows[0]
        if {
            name: (
                int(winner[name])
                if name == "source_sequence"
                else str(winner[name])
            )
            for name in expected
        } != expected:
            raise V3ProjectionOutboxConflictError(
                "concurrent V3 projection outbox insert carried different content"
            ) from exc
        return V3ProjectionOutboxAppendResult(
            status=V3ProjectionOutboxAppendStatus.IDEMPOTENT,
            outbox_id=outbox_id,
            projection_id=projection.projection_id,
            canonical_payload_hash=canonical_hash,
        )
    if getattr(insert_result, "rowcount", None) != 1:
        raise V3ProjectionOutboxError(
            "V3 projection outbox insert was not durable"
        )
    inserted_rows = tuple(
        active.execute(
            select_identity,
            identity_parameters,
        ).mappings()
    )
    if len(inserted_rows) != 1:
        raise V3ProjectionOutboxError(
            "V3 projection outbox insert readback is not unique"
        )
    inserted = inserted_rows[0]
    if {
        name: (
            int(inserted[name])
            if name == "source_sequence"
            else str(inserted[name])
        )
        for name in expected
    } != expected:
        raise V3ProjectionOutboxError(
            "V3 projection outbox insert readback differs"
        )
    return V3ProjectionOutboxAppendResult(
        status=V3ProjectionOutboxAppendStatus.INSERTED,
        outbox_id=outbox_id,
        projection_id=projection.projection_id,
        canonical_payload_hash=canonical_hash,
    )


__all__ = [
    "V3ProjectionOutboxAppendResult",
    "V3ProjectionOutboxAppendStatus",
    "V3ProjectionOutboxConflictError",
    "V3ProjectionOutboxError",
    "append_v3_transition_outbox",
    "projection_from_payload",
    "projection_to_payload",
]
