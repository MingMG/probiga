"""Account-namespace boundary for legacy V2/V3 strategy entry points.

The V4 control plane may reuse the canonical V2 execution ledger, but its
paper accounts must not be consumed by the older V2/V3 strategy planners.
Keeping this check in a small, dependency-free module lets outer entry points
fail before opening a transaction or issuing SQL.
"""
from __future__ import annotations


V4_PAPER_ACCOUNT_PREFIX = "paper-v4-"
V4_PAPER_ACCOUNT_ROOT = "paper-v4"


class LegacyStrategyAccountIsolationError(RuntimeError):
    """Raised when a V4 paper account enters a legacy strategy path."""


def require_legacy_strategy_account(
    account_id: object,
    *,
    entrypoint: str,
) -> None:
    """Reject the reserved V4 paper-account namespace.

    Existing account namespaces intentionally keep their prior behaviour.
    In particular, ``paper-main-v2`` and future ``paper-v3-*`` identifiers
    continue into the legacy implementation unchanged.
    """

    normalized = (
        account_id.strip().casefold()
        if isinstance(account_id, str)
        else ""
    )
    if normalized == V4_PAPER_ACCOUNT_ROOT or normalized.startswith(
        V4_PAPER_ACCOUNT_PREFIX
    ):
        raise LegacyStrategyAccountIsolationError(
            f"{entrypoint} rejects V4 paper account {account_id!r}: "
            "the paper-v4- namespace is isolated from legacy strategy paths"
        )
