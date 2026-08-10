"""Control-plane persistence boundary for V4 decision runs.

This port deliberately stops at immutable contexts, run lifecycle state, and
committed channel heads.  It does not persist ``DecisionBundle`` values,
forecasts, actions, execution intents, positions, orders, fills, or account
ledgers.  Those facts require their own explicit stores and migrations.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Protocol, runtime_checkable

from ..domain import DecisionContext, QualityStatus


@runtime_checkable
class RunStorePort(Protocol):
    """Store only V4 context/run/head control-plane facts."""

    def create_or_get_context(
        self,
        context: DecisionContext,
        *,
        freshness_status: QualityStatus | str = QualityStatus.PASS,
        run_mode: str | None = None,
        is_realtime: bool | None = None,
        fallback_used: bool = False,
        feature_as_of: date | None = None,
        created_at: datetime | None = None,
    ) -> Any:
        ...

    def create_or_get_run(
        self,
        *,
        context_id: str,
        account_id: str = "",
        channel: str,
        run_type: str,
        trigger_type: str,
        trigger_ref_id: str = "",
        parent_run_uid: str = "",
        model_set_version: str,
        config_version: str,
        code_commit_sha: str,
        run_idempotency_key: str | None = None,
        run_uid: str | None = None,
        created_at: datetime | None = None,
    ) -> Any:
        ...

    def get_run(self, run_uid: str) -> dict[str, Any] | None:
        ...

    def transition_run(
        self,
        run_uid: str,
        *,
        next_status: str,
        expected_status: str | None = None,
        occurred_at: datetime | None = None,
        result_hash: str | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> dict[str, Any]:
        ...

    def get_head(
        self,
        channel: str,
        *,
        account_id: str = "",
    ) -> dict[str, Any] | None:
        ...

    def publish_committed_head(
        self,
        run_uid: str,
        *,
        published_by: str = "",
        published_at: datetime | None = None,
        expected_head_version: int | None = None,
    ) -> Any:
        ...

    def commit_and_publish_head(
        self,
        run_uid: str,
        *,
        result_hash: str,
        published_by: str = "",
        occurred_at: datetime | None = None,
        expected_head_version: int | None = None,
    ) -> Any:
        ...

