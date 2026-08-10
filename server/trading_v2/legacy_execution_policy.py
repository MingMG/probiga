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

from sqlalchemy import text
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
        theme = str(theme_code or "").strip()
        if not theme:
            return WaitingReason.WAIT_SECTOR_CONFIRMATION.value
        try:
            row = connection.execute(
                text(
                    """
                    SELECT snapshot_at, direction, score, breadth_pct
                    FROM sm_market_radar_sector
                    WHERE sector_code = :theme_code
                    LIMIT 1
                    """
                ),
                {"theme_code": theme},
            ).mappings().first()
        except Exception:
            row = None
        if not row or not row.get("snapshot_at"):
            return WaitingReason.WAIT_SECTOR_CONFIRMATION.value
        snapshot_at = row["snapshot_at"]
        if isinstance(snapshot_at, str):
            try:
                snapshot_at = datetime.fromisoformat(snapshot_at)
            except ValueError:
                return WaitingReason.WAIT_SECTOR_CONFIRMATION.value
        try:
            age = (now - snapshot_at).total_seconds()
        except (TypeError, ValueError):
            return WaitingReason.WAIT_SECTOR_CONFIRMATION.value
        if age < 0 or age > self.confirmation_max_age_seconds:
            return WaitingReason.WAIT_SECTOR_CONFIRMATION.value
        if (
            str(row.get("direction") or "").upper() != "UP"
            or decimal_value(row.get("score")) < Decimal("20")
            or decimal_value(row.get("breadth_pct")) < Decimal("10")
        ):
            return WaitingReason.WAIT_SECTOR_CONFIRMATION.value
        return ""

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
