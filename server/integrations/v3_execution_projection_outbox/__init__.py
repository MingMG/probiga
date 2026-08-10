"""Disabled-by-default V2 transition outbox and independent V3 worker."""

from .legacy_guard import (
    OUTBOX_RUNTIME_ENABLED,
    LegacyDirectSyncStatus,
    LegacyDirectSyncStillActiveError,
    inspect_legacy_direct_sync,
    require_outbox_replacement_safe,
)
from .outbox import (
    V3ProjectionOutboxAppendResult,
    V3ProjectionOutboxAppendStatus,
    V3ProjectionOutboxConflictError,
    V3ProjectionOutboxError,
    append_v3_transition_outbox,
    projection_from_payload,
    projection_to_payload,
)
from .schema import (
    V3ProjectionOutboxSchemaError,
    V3_PROJECTION_OUTBOX_DDL,
    validate_v3_projection_outbox_schema,
)
from .worker import (
    V3ProjectionBaselineResult,
    V3ProjectionBaselineStatus,
    V3ProjectionDeadLetterRequeueResult,
    V3ProjectionOutboxLease,
    V3ProjectionWorkerDisabledError,
    V3ProjectionWorkerError,
    V3ProjectionWorkerPorts,
    V3ProjectionWorkerResult,
    V3ProjectionWorkerStatus,
    lease_v3_projection_outbox,
    register_v3_projection_order_baseline,
    requeue_v3_projection_dead_letter,
    run_v3_projection_worker_once,
)

__all__ = [
    "OUTBOX_RUNTIME_ENABLED",
    "LegacyDirectSyncStatus",
    "LegacyDirectSyncStillActiveError",
    "V3ProjectionOutboxAppendResult",
    "V3ProjectionOutboxAppendStatus",
    "V3ProjectionOutboxConflictError",
    "V3ProjectionOutboxError",
    "V3ProjectionBaselineResult",
    "V3ProjectionBaselineStatus",
    "V3ProjectionDeadLetterRequeueResult",
    "V3ProjectionOutboxLease",
    "V3ProjectionOutboxSchemaError",
    "V3ProjectionWorkerDisabledError",
    "V3ProjectionWorkerError",
    "V3ProjectionWorkerPorts",
    "V3ProjectionWorkerResult",
    "V3ProjectionWorkerStatus",
    "V3_PROJECTION_OUTBOX_DDL",
    "append_v3_transition_outbox",
    "inspect_legacy_direct_sync",
    "lease_v3_projection_outbox",
    "projection_from_payload",
    "projection_to_payload",
    "register_v3_projection_order_baseline",
    "requeue_v3_projection_dead_letter",
    "require_outbox_replacement_safe",
    "run_v3_projection_worker_once",
    "validate_v3_projection_outbox_schema",
]
