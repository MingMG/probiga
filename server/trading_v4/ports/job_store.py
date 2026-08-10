"""Technology-neutral persistence boundary for V4 control-plane jobs.

This port owns only scheduling and lease state.  It does not start a worker,
emit an actionable decision, or own accounts, orders, fills, cash, positions,
or risk facts.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class JobStorePort(Protocol):
    def create_job(
        self,
        *,
        job_id: str,
        idempotency_key: str,
        job_type: str,
        scheduled_for: datetime,
        max_attempts: int,
        created_at: datetime,
        input_context_id: str = "",
        input_hash: str = "",
    ) -> Any:
        ...

    def get_job(self, job_id: str) -> Any | None:
        ...

    def claim_due_job(
        self,
        *,
        worker_id: str,
        lease_token: str,
        now: datetime,
        lease_until: datetime,
        job_type: str | None = None,
    ) -> Any | None:
        """Claim with a caller-generated globally fresh, never-reused token.

        A committed claim records the token in an append-only registry in the
        same transaction as the job CAS.  The same command may replay only
        while it still identifies that exact live lease; every historical or
        cross-command token reuse fails closed.
        """
        ...

    def heartbeat(
        self,
        job_id: str,
        *,
        worker_id: str,
        lease_token: str,
        attempt_count: int,
        observed_lease_until: datetime,
        now: datetime,
        lease_until: datetime,
    ) -> Any:
        ...

    def complete(
        self,
        job_id: str,
        *,
        worker_id: str,
        lease_token: str,
        attempt_count: int,
        observed_lease_until: datetime,
        run_uid: str,
        now: datetime,
    ) -> Any:
        ...

    def fail(
        self,
        job_id: str,
        *,
        worker_id: str,
        lease_token: str,
        attempt_count: int,
        observed_lease_until: datetime,
        now: datetime,
        retryable: bool,
        next_attempt_at: datetime | None,
        error_code: str,
        error_message: str = "",
    ) -> Any:
        ...

    def expire_exhausted_job(
        self,
        job_id: str,
        *,
        lease_owner: str,
        lease_token: str,
        attempt_count: int,
        observed_lease_until: datetime,
        now: datetime,
    ) -> Any:
        ...


__all__ = ["JobStorePort"]
