"""Model artifact resolution boundary."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable

from ..domain import CalibrationArtifactRef, ModelArtifactRef


@runtime_checkable
class ModelRegistryPort(Protocol):
    def resolve_model(
        self,
        model_id: str,
        model_version: str,
        *,
        as_of: datetime,
    ) -> ModelArtifactRef:
        ...

    def resolve_calibration(
        self,
        calibration_id: str,
        calibration_version: str,
        *,
        as_of: datetime,
    ) -> CalibrationArtifactRef:
        ...
