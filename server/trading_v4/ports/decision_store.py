"""Future atomic result-plane persistence boundary for V4 decisions.

``TradingV4Repository`` implements :class:`RunStorePort`, not this protocol.
The current control-plane schema has no bundle/forecast/action tables, so no
adapter may claim to implement ``DecisionStorePort`` until those forward-only
result migrations exist and pass their own acceptance gate.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..domain import DecisionBundle, DecisionCommitReceipt, DecisionInput


@runtime_checkable
class DecisionStorePort(Protocol):
    def commit(
        self,
        decision_input: DecisionInput,
        bundle: DecisionBundle,
    ) -> DecisionCommitReceipt:
        ...

    def get_bundle(self, decision_id: str) -> DecisionBundle | None:
        ...
