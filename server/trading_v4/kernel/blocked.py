"""Fail-closed deterministic kernel used before Stage 3 is authorized."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from ..domain import (
    DecisionBundle,
    DecisionBundleStatus,
    DecisionInput,
    derive_decision_id,
)


@runtime_checkable
class DecisionKernel(Protocol):
    """Pure evaluation boundary; implementations may not perform I/O."""

    @property
    def kernel_version(self) -> str:
        ...

    def evaluate(self, decision_input: DecisionInput) -> DecisionBundle:
        ...


@dataclass(frozen=True, slots=True)
class BlockedDecisionKernel:
    """Return a deterministic, non-actionable bundle for every valid input.

    This is the only concrete kernel permitted while the formal V3 baseline,
    real MySQL acceptance and PIT factor stage remain blocked.  It proves the
    application boundary without inventing forecasts or execution actions.
    """

    kernel_version: str = field(
        default="v4:kernel:blocked:v1",
        init=False,
    )
    reason_codes: tuple[str, ...] = field(
        default=(
            "ACTIONABLE_OUTPUT_DISABLED",
            "DATA_UNAVAILABLE",
            "SAFETY_INTERLOCK",
            "STAGE_3_NOT_AUTHORIZED",
        ),
        init=False,
    )

    def evaluate(self, decision_input: DecisionInput) -> DecisionBundle:
        if type(decision_input) is not DecisionInput:
            raise TypeError("decision_input must be exactly DecisionInput")
        decision_id = derive_decision_id(
            decision_input,
            self.kernel_version,
        )
        return DecisionBundle(
            decision_id=decision_id,
            decision_input=decision_input,
            kernel_version=self.kernel_version,
            status=DecisionBundleStatus.DATA_BLOCKED,
            forecasts=(),
            actions=(),
            execution_intents=(),
            diagnostics={
                "actionable_output_allowed": False,
                "kernel_mode": "BLOCKED_FAIL_CLOSED",
                "paper_buy_outbox_open": False,
                "production_activation_allowed": False,
                "reason_codes": self.reason_codes,
            },
        )


__all__ = ["BlockedDecisionKernel", "DecisionKernel"]
