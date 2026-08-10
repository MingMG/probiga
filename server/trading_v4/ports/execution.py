"""Neutral execution boundary for committed V4 intents."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..domain import CommittedExecutionIntent, ExecutionReceipt


@runtime_checkable
class ExecutionPort(Protocol):
    def submit(self, message: CommittedExecutionIntent) -> ExecutionReceipt:
        ...

    def get_receipt(self, idempotency_key: str) -> ExecutionReceipt | None:
        ...
