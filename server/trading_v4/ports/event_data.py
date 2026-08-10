"""Revision-aware event data boundary."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable

from ..domain import DatasetResult, ScopeRef


@runtime_checkable
class EventDataPort(Protocol):
    def load_events(
        self,
        scopes: tuple[ScopeRef, ...],
        *,
        knowledge_cutoff: datetime,
        since: datetime | None = None,
    ) -> DatasetResult:
        ...
