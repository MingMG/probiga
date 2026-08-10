"""Point-in-time fundamental data boundary."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable

from ..domain import DatasetResult


@runtime_checkable
class FundamentalDataPort(Protocol):
    def load_fundamentals(
        self,
        instruments: tuple[str, ...],
        *,
        knowledge_cutoff: datetime,
        fields: tuple[str, ...] = (),
    ) -> DatasetResult:
        ...
