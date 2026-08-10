"""Independent Trading V6 research package.

The package intentionally has no eager imports so the frozen evidence auditor can
run with the Python standard library only.  V6 is research-only and owns no
order, account, scheduler, API, or database integration.
"""

SYSTEM = "trading_v6"
SYSTEM_VERSION = "6.0.0-research"
RELEASE_ID = "trading_v6.0.0-research"
LIFECYCLE_STATUS = "RESEARCH_ONLY"
RELEASE_DECISION = "BLOCK"

__all__ = [
    "LIFECYCLE_STATUS",
    "RELEASE_DECISION",
    "RELEASE_ID",
    "SYSTEM",
    "SYSTEM_VERSION",
]
