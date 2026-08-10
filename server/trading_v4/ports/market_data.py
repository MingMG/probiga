"""Market data boundary for the clean-room decision application."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable

from ..domain import DatasetResult


@runtime_checkable
class MarketDataPort(Protocol):
    def load_market_data(
        self,
        instruments: tuple[str, ...],
        *,
        knowledge_cutoff: datetime,
        fields: tuple[str, ...] = (),
    ) -> DatasetResult:
        """Return only records knowable at ``knowledge_cutoff``."""

        ...
