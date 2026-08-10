"""Fail-closed writer boundary for V2 execution evidence."""

from .writer import (
    EvidenceAppendConflictError,
    EvidenceAppendResult,
    EvidenceAppendStatus,
    EvidenceAuthorityUnsupportedError,
    EvidenceAuthorityConflictError,
    EvidenceAuthorityReplayError,
    EvidenceCanonicalRowError,
    EvidenceTransactionError,
    append_cash_event_binding,
    append_evidence,
    append_fill_execution_evidence,
    append_market_calendar_evidence,
    append_order_transition_evidence,
    append_quote_receipt_evidence,
)

__all__ = [
    "EvidenceAppendConflictError",
    "EvidenceAppendResult",
    "EvidenceAppendStatus",
    "EvidenceAuthorityUnsupportedError",
    "EvidenceAuthorityConflictError",
    "EvidenceAuthorityReplayError",
    "EvidenceCanonicalRowError",
    "EvidenceTransactionError",
    "append_cash_event_binding",
    "append_evidence",
    "append_fill_execution_evidence",
    "append_market_calendar_evidence",
    "append_order_transition_evidence",
    "append_quote_receipt_evidence",
]
