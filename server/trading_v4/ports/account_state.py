"""Factual account-state boundary; it contains no strategy opinions."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable

from ..domain import AccountSnapshot


@runtime_checkable
class AccountStatePort(Protocol):
    def load_account_snapshot(
        self,
        account_id: str,
        *,
        as_of: datetime,
    ) -> AccountSnapshot:
        ...
