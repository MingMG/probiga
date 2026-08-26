"""Strict runtime mode for the strategy-governance database boundary.

The default remains the fully required governance contract.  ``DEFERRED_DB``
is an explicit, temporary release mode: callers may expose a degraded,
cash-only view, but must never infer that the missing schema is usable.
"""

from __future__ import annotations

from enum import Enum
import os


STRATEGY_GOVERNANCE_MODE_ENV = "PROBIGA_STRATEGY_GOVERNANCE_MODE"
STRATEGY_GOVERNANCE_BASE_SCHEMA_READY_ENV = (
    "PROBIGA_STRATEGY_GOVERNANCE_BASE_SCHEMA_READY"
)


class StrategyGovernanceMode(str, Enum):
    REQUIRED = "REQUIRED"
    DEFERRED_DB = "DEFERRED_DB"


class StrategyGovernanceModeError(RuntimeError):
    """Raised when the configured mode is not one of the sealed values."""


def get_strategy_governance_mode() -> StrategyGovernanceMode:
    """Return the exact configured mode, failing closed on unknown values."""

    raw = os.environ.get(STRATEGY_GOVERNANCE_MODE_ENV, "")
    value = raw.strip()
    if not value:
        return StrategyGovernanceMode.REQUIRED
    try:
        return StrategyGovernanceMode(value)
    except ValueError as exc:
        raise StrategyGovernanceModeError(
            f"{STRATEGY_GOVERNANCE_MODE_ENV} must be REQUIRED or DEFERRED_DB"
        ) from exc


def strategy_governance_database_deferred() -> bool:
    """Return whether governance DB access is explicitly deferred."""

    return get_strategy_governance_mode() is StrategyGovernanceMode.DEFERRED_DB


def strategy_governance_base_schema_declared_ready() -> bool:
    """Read the root-deployment assertion written only after schema verify."""

    return os.environ.get(
        STRATEGY_GOVERNANCE_BASE_SCHEMA_READY_ENV, ""
    ).strip() == "true"


__all__ = [
    "STRATEGY_GOVERNANCE_MODE_ENV",
    "STRATEGY_GOVERNANCE_BASE_SCHEMA_READY_ENV",
    "StrategyGovernanceMode",
    "StrategyGovernanceModeError",
    "get_strategy_governance_mode",
    "strategy_governance_database_deferred",
    "strategy_governance_base_schema_declared_ready",
]
