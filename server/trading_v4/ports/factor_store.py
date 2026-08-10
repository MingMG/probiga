"""Persistence port for immutable Stage-3 source and factor artifacts.

The records deliberately mirror the frozen ``005`` storage contract.  They
are not aliases for the richer domain contracts in :mod:`trading_v4.domain`:
the database vocabulary is independently frozen and must not be translated
implicitly.  Implementations use a caller-owned connection and transaction.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class SourceCertificationRecord:
    source_key: str
    certification_version: str
    source_table: str
    event_time_column: str
    knowledge_time_columns: tuple[str, ...]
    replay_eligibility: str
    certification_status: str
    availability_status: str
    research_status: str
    quality_status: str
    valid_from: datetime
    valid_to: datetime | None
    contract: Mapping[str, Any]
    evidence_hash: str
    certified_by: str
    certified_at: datetime
    created_at: datetime


@dataclass(frozen=True, slots=True)
class FactorDefinitionRecord:
    factor_key: str
    factor_version: str
    feature_set_version: str
    factor_role: str
    scope_type: str
    availability_status: str
    research_status: str
    quality_status: str
    missing_policy: str
    pit_eligible: bool
    max_age_seconds: int
    required_source_keys: tuple[str, ...]
    required_source_certifications: tuple[Mapping[str, Any], ...]
    formula: Mapping[str, Any]
    output_schema: Mapping[str, Any]
    definition_hash: str
    available_at: datetime
    created_at: datetime


@dataclass(frozen=True, slots=True)
class EntityFeatureSnapshotRecord:
    snapshot_id: str
    run_uid: str
    scope_type: str
    scope_id: str
    feature_set_version: str
    knowledge_cutoff_at: datetime
    computed_at: datetime
    available_at: datetime
    factor_count: int
    values: Mapping[str, Any]
    quality_status: str
    quality: Mapping[str, Any]
    source_certifications: tuple[Mapping[str, Any], ...]
    factor_definitions: tuple[Mapping[str, Any], ...]
    source_manifest_hash: str
    feature_hash: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class FactorStoreAppendResult:
    created: bool
    record: object


@runtime_checkable
class FactorStorePort(Protocol):
    """Append/read Stage-3 artifacts inside a caller-owned transaction.

    Implementations never commit or roll back the supplied transaction.  A
    MySQL lock timeout/deadlock therefore has to be handled by rolling back and
    replaying the complete unit of work in a fresh transaction; retrying one
    append statement inside the invalidated transaction is not supported.
    """

    def append_source_certification(
        self, connection: Any, record: SourceCertificationRecord
    ) -> FactorStoreAppendResult: ...

    def append_factor_definition(
        self, connection: Any, record: FactorDefinitionRecord
    ) -> FactorStoreAppendResult: ...

    def append_feature_snapshot(
        self, connection: Any, record: EntityFeatureSnapshotRecord
    ) -> FactorStoreAppendResult: ...

    def get_source_certification(
        self, connection: Any, source_key: str, certification_version: str
    ) -> SourceCertificationRecord | None: ...

    def get_factor_definition(
        self, connection: Any, factor_key: str, factor_version: str
    ) -> FactorDefinitionRecord | None: ...

    def get_feature_snapshot(
        self, connection: Any, snapshot_id: str
    ) -> EntityFeatureSnapshotRecord | None: ...


__all__ = [
    "EntityFeatureSnapshotRecord",
    "FactorDefinitionRecord",
    "FactorStoreAppendResult",
    "FactorStorePort",
    "SourceCertificationRecord",
]
