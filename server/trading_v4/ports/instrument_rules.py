"""Effective-dated instrument rule boundary."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable

from ..domain import InstrumentRuleSnapshot


@runtime_checkable
class InstrumentRulesPort(Protocol):
    def load_instrument_rules(
        self,
        instruments: tuple[str, ...],
        *,
        effective_at: datetime,
    ) -> tuple[InstrumentRuleSnapshot, ...]:
        ...
