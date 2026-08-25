"""Legacy strategy gates kept outside the strategy-neutral execution core.

These checks are investment-policy decisions used by the existing V2
``sector_preheat`` strategy.  They deliberately remain in the legacy V2
boundary: V4 and ``server.trading_core`` must never import this module.

The policy is read-only.  It does not create orders, apply fills, or mutate
the canonical V2 account/ledger.  Database failures fail closed for the old
strategy, preserving the behaviour that previously lived in
``server.trading_v2.execution``.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from sqlalchemy.engine import Connection

from .domain import OrderSide, WaitingReason, decimal_value


@dataclass(frozen=True)
class LegacySectorPreheatExecutionPolicy:
    """Read-only execution-time gates for the legacy sector strategy only."""

    confirmation_max_age_seconds: int = 180

    def __post_init__(self) -> None:
        if (
            type(self.confirmation_max_age_seconds) is not int
            or self.confirmation_max_age_seconds < 0
        ):
            raise ValueError(
                "confirmation_max_age_seconds must be a non-negative integer"
            )

    @staticmethod
    def applies(*, strategy_version: str, side: OrderSide) -> bool:
        return side == OrderSide.BUY and str(strategy_version).startswith(
            "sector_preheat"
        )

    def sector_entry_wait_reason(
        self,
        connection: Connection,
        *,
        strategy_version: str,
        theme_code: str,
        side: OrderSide,
        now: datetime,
    ) -> str:
        """Require a fresh intraday UP reading for legacy entries."""

        if not self.applies(strategy_version=strategy_version, side=side):
            return ""
        # ``sm_market_radar_sector`` is an overwrite-in-place advisory cache
        # whose concept memberships have no immutable received-time revision
        # or coverage receipt.  It cannot authorize a new funded entry.  Keep
        # the legacy strategy fail-closed until the radar publishes a PIT-bound
        # membership/evidence contract.
        return WaitingReason.WAIT_SECTOR_CONFIRMATION.value

    def entry_trend_wait_reason(
        self,
        *,
        strategy_version: str,
        side: OrderSide,
        fill_price: Decimal,
        initial_stop: Decimal,
    ) -> str:
        """Reject a legacy entry after price has already broken its stop."""

        if (
            self.applies(strategy_version=strategy_version, side=side)
            and initial_stop > 0
            and fill_price <= initial_stop
        ):
            return WaitingReason.WAIT_ENTRY_TREND_INVALID.value
        return ""


__all__ = ["LegacySectorPreheatExecutionPolicy"]
