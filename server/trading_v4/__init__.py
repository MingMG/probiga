"""Trading V4 clean-room decision contracts.

The package intentionally exposes contracts without importing any legacy
strategy runtime.  Infrastructure adapters live outside the decision kernel.
"""

from .domain import DecisionBundle, DecisionContext, DecisionInput, ExecutionIntent
from .kernel import (
    BlockedDecisionKernel,
    DecisionKernel,
    ResearchDecisionKernel,
    ResearchObservation,
)

__all__ = [
    "BlockedDecisionKernel",
    "DecisionBundle",
    "DecisionContext",
    "DecisionInput",
    "DecisionKernel",
    "ExecutionIntent",
    "ResearchDecisionKernel",
    "ResearchObservation",
]

__version__ = "4.1.0-research"
