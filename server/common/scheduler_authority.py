"""Frozen production scheduler authority and Layer-4 writer-fence contract.

Production deliberately keeps long-running scheduler work outside the FastAPI
lifespan.  These constants are shared by health, readiness and task deployment
so those surfaces cannot silently choose different scheduler owners.
"""
from __future__ import annotations


PRODUCTION_SCHEDULER_MODE = "standalone"
PRODUCTION_SCHEDULER_SERVICE = "probiga-scheduler.service"
PRODUCTION_EMBEDDED_SCHEDULER_ENABLED = False

LAYER4_WRITER_TASK_TYPES = (
    "trading_v3_counterfactual_audit",
    "trading_v3_continuous_calibration",
)


def scheduler_authority_contract() -> dict[str, object]:
    """Return the serializable production authority contract."""

    return {
        "mode": PRODUCTION_SCHEDULER_MODE,
        "service": PRODUCTION_SCHEDULER_SERVICE,
        "embedded_scheduler_enabled": PRODUCTION_EMBEDDED_SCHEDULER_ENABLED,
        "standalone_service_active": True,
        "standalone_service_enabled": True,
    }


__all__ = [
    "LAYER4_WRITER_TASK_TYPES",
    "PRODUCTION_EMBEDDED_SCHEDULER_ENABLED",
    "PRODUCTION_SCHEDULER_MODE",
    "PRODUCTION_SCHEDULER_SERVICE",
    "scheduler_authority_contract",
]
