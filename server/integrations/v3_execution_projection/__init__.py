"""Opt-in projection and persistence boundary for the legacy V3 read model."""

from .projector import (
    ProjectionState,
    V3ExecutionPlanBinding,
    V3ExecutionProjection,
    bind_v3_execution_plan,
    project_execution_result,
    validate_v3_execution_plan_binding,
    validate_v3_execution_projection,
)
from .subscriber import (
    ProjectionApplyStatus,
    V3ProjectionApplyResult,
    V3ProjectionSubscriberError,
    apply_v3_execution_projection,
)

__all__ = [
    "ProjectionState",
    "ProjectionApplyStatus",
    "V3ExecutionPlanBinding",
    "V3ExecutionProjection",
    "V3ProjectionApplyResult",
    "V3ProjectionSubscriberError",
    "apply_v3_execution_projection",
    "bind_v3_execution_plan",
    "project_execution_result",
    "validate_v3_execution_plan_binding",
    "validate_v3_execution_projection",
]
