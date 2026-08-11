"""Opt-in, caller-transaction boundary for canonical V2 execution commits."""

from .coordinator import (
    AcceptanceActivationToken,
    CanonicalCommitDisabledError,
    CanonicalCommitInvariantError,
    CanonicalCommitReceipt,
    SharedCapacityReservation,
    SharedCapacityReservationResult,
    SharedCapacityReservationStatus,
    coordinate_v2_canonical_commit,
)
from .prepared_adapter import (
    PREPARED_COMMIT_RUNTIME_ENABLED,
    PRODUCTION_ACTIVATION_ALLOWED,
    CanonicalExecutionCutover,
    CanonicalMechanicalMutation,
    CanonicalMechanicalTransition,
    PreparedCanonicalCommitBundle,
    PreparedCanonicalCommitReceipt,
    PreparedCommitTransactionContext,
    V3BaselineExternalAttestation,
    commit_prepared_canonical_execution,
    preflight_prepared_commit,
)

__all__ = [
    "AcceptanceActivationToken",
    "CanonicalCommitDisabledError",
    "CanonicalCommitInvariantError",
    "CanonicalCommitReceipt",
    "SharedCapacityReservation",
    "SharedCapacityReservationResult",
    "SharedCapacityReservationStatus",
    "coordinate_v2_canonical_commit",
    "PREPARED_COMMIT_RUNTIME_ENABLED",
    "PRODUCTION_ACTIVATION_ALLOWED",
    "CanonicalExecutionCutover",
    "CanonicalMechanicalMutation",
    "CanonicalMechanicalTransition",
    "PreparedCanonicalCommitBundle",
    "PreparedCanonicalCommitReceipt",
    "PreparedCommitTransactionContext",
    "V3BaselineExternalAttestation",
    "commit_prepared_canonical_execution",
    "preflight_prepared_commit",
]
