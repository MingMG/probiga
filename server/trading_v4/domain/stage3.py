"""Immutable Stage-3 point-in-time source and factor definitions.

These contracts describe evidence and computation specifications only.  They
own no clock, database, network client, model or order side effect.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .contracts import (
    _normalized_mapping_items,
    _required_text,
    _sha256,
    _v4_artifact_version,
    assert_clean_payload,
)
from .enums import (
    AvailabilityStatus,
    CertificationStatus,
    FactorRole,
    QualityStatus,
    ReplayEligibility,
    ResearchStatus,
    ScopeType,
)
from .hashes import ContractMixin, deterministic_hash, deterministic_id, freeze, require_aware


_MISSING_POLICIES = frozenset({"BLOCK", "PROPAGATE_NULL", "DISPLAY_ONLY"})
_PIT_SAFE_REVISION_POLICIES = frozenset(
    {
        "APPEND_ONLY_REVISION_CHAIN",
        "BITEMPORAL_REVISION_CHAIN",
        "IMMUTABLE_EVENT_LOG",
    }
)


def _normalized_text_tuple(values: Any, field_name: str) -> tuple[str, ...]:
    try:
        items = tuple(values)
    except TypeError as exc:
        raise TypeError(f"{field_name} must be iterable") from exc
    return tuple(
        sorted({_required_text(item, f"{field_name} item") for item in items})
    )


def _optional_aware(value: datetime | None, field_name: str) -> None:
    if value is not None:
        require_aware(value, field_name)


@dataclass(frozen=True)
class DataSourceCertification(ContractMixin):
    """Versioned evidence that a raw source is usable at a stated scope."""

    source_key: str
    source_version: str
    adapter_version: str
    certification_version: str
    replay_eligibility: ReplayEligibility
    certification_status: CertificationStatus
    availability_status: AvailabilityStatus
    research_status: ResearchStatus
    quality_status: QualityStatus
    knowledge_time_field: str
    ingested_at_field: str
    event_time_field: str
    revision_policy: str
    allowed_fields: tuple[str, ...]
    evidence_hashes: tuple[str, ...]
    available_at: datetime
    assessed_at: datetime
    certified_from: datetime | None = None
    valid_until: datetime | None = None
    reason_codes: tuple[str, ...] = ()
    details: Mapping[str, Any] = field(default_factory=dict)
    certification_hash: str = field(init=False)
    certification_id: str = field(init=False)

    def __post_init__(self) -> None:
        for name in (
            "source_key",
            "knowledge_time_field",
            "ingested_at_field",
            "event_time_field",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name), name))
        object.__setattr__(
            self,
            "revision_policy",
            _required_text(self.revision_policy, "revision_policy").upper(),
        )
        for name in (
            "source_version",
            "adapter_version",
            "certification_version",
        ):
            object.__setattr__(
                self,
                name,
                _v4_artifact_version(getattr(self, name), name),
            )
        object.__setattr__(
            self,
            "replay_eligibility",
            ReplayEligibility(self.replay_eligibility),
        )
        object.__setattr__(
            self,
            "certification_status",
            CertificationStatus(self.certification_status),
        )
        object.__setattr__(
            self,
            "availability_status",
            AvailabilityStatus(self.availability_status),
        )
        object.__setattr__(
            self,
            "research_status",
            ResearchStatus(self.research_status),
        )
        object.__setattr__(self, "quality_status", QualityStatus(self.quality_status))
        require_aware(self.available_at, "available_at")
        require_aware(self.assessed_at, "assessed_at")
        _optional_aware(self.certified_from, "certified_from")
        _optional_aware(self.valid_until, "valid_until")
        if self.available_at > self.assessed_at:
            raise ValueError("source certification cannot be assessed before availability")
        if self.valid_until is not None and self.valid_until < self.available_at:
            raise ValueError("source certification valid_until precedes availability")
        allowed_fields = _normalized_text_tuple(self.allowed_fields, "allowed_fields")
        if not allowed_fields:
            raise ValueError("allowed_fields must not be empty")
        assert_clean_payload({field_name: None for field_name in allowed_fields})
        evidence_hashes = tuple(
            sorted({_sha256(item, "evidence_hashes item") for item in self.evidence_hashes})
        )
        reason_codes = _normalized_text_tuple(self.reason_codes, "reason_codes")
        assert_clean_payload(self.details, path="source_certification.details")
        object.__setattr__(self, "allowed_fields", allowed_fields)
        object.__setattr__(self, "evidence_hashes", evidence_hashes)
        object.__setattr__(self, "reason_codes", reason_codes)
        object.__setattr__(self, "details", freeze(self.details))

        if self.replay_eligibility == ReplayEligibility.PIT_CERTIFIED:
            if (
                self.certification_status != CertificationStatus.PASSED
                or self.availability_status != AvailabilityStatus.ACTIVE
                or self.research_status != ResearchStatus.BACKTEST_READY
                or self.quality_status != QualityStatus.PASS
                or self.certified_from is None
                or not evidence_hashes
                or self.revision_policy not in _PIT_SAFE_REVISION_POLICIES
            ):
                raise ValueError(
                    "PIT_CERTIFIED requires passed evidence, an immutable revision "
                    "policy, certified_from and fully active PASS capability"
                )
        elif self.research_status == ResearchStatus.BACKTEST_READY:
            raise ValueError("BACKTEST_READY requires PIT_CERTIFIED eligibility")

        if self.certification_status != CertificationStatus.PASSED and (
            self.availability_status == AvailabilityStatus.ACTIVE
            or self.quality_status == QualityStatus.PASS
        ):
            raise ValueError("unpassed source certification cannot be ACTIVE/PASS")
        if self.quality_status != QualityStatus.PASS and not reason_codes:
            raise ValueError("non-PASS source certification requires reason_codes")

        content = {
            "source_key": self.source_key,
            "source_version": self.source_version,
            "adapter_version": self.adapter_version,
            "certification_version": self.certification_version,
            "replay_eligibility": self.replay_eligibility,
            "certification_status": self.certification_status,
            "availability_status": self.availability_status,
            "research_status": self.research_status,
            "quality_status": self.quality_status,
            "knowledge_time_field": self.knowledge_time_field,
            "ingested_at_field": self.ingested_at_field,
            "event_time_field": self.event_time_field,
            "revision_policy": self.revision_policy,
            "allowed_fields": self.allowed_fields,
            "evidence_hashes": self.evidence_hashes,
            "available_at": self.available_at,
            "assessed_at": self.assessed_at,
            "certified_from": self.certified_from,
            "valid_until": self.valid_until,
            "reason_codes": self.reason_codes,
            "details": self.details,
        }
        digest = deterministic_hash(content)
        object.__setattr__(self, "certification_hash", digest)
        object.__setattr__(
            self,
            "certification_id",
            deterministic_id("pitcert", content),
        )

    def is_available_as_of(self, as_of: datetime) -> bool:
        require_aware(as_of, "as_of")
        return bool(
            self.certification_status == CertificationStatus.PASSED
            and self.availability_status == AvailabilityStatus.ACTIVE
            and self.quality_status == QualityStatus.PASS
            and self.available_at <= as_of
            and (self.valid_until is None or as_of <= self.valid_until)
        )

    def is_backtest_ready_as_of(self, as_of: datetime) -> bool:
        return bool(
            self.is_available_as_of(as_of)
            and self.replay_eligibility == ReplayEligibility.PIT_CERTIFIED
            and self.research_status == ResearchStatus.BACKTEST_READY
            and self.certified_from is not None
            and self.certified_from <= as_of
        )


@dataclass(frozen=True)
class FactorDefinition(ContractMixin):
    """Immutable factor formula metadata; it cannot execute a formula."""

    factor_key: str
    factor_version: str
    role: FactorRole
    scope_type: ScopeType
    feature_set_version: str
    builder_version: str
    required_source_versions: Mapping[str, str]
    required_fields: Mapping[str, tuple[str, ...]]
    output_fields: tuple[str, ...]
    missing_policy: str
    availability_status: AvailabilityStatus
    research_status: ResearchStatus
    quality_status: QualityStatus
    available_at: datetime
    formula_hash: str
    max_age_seconds: int
    reason_codes: tuple[str, ...] = ()
    specification: Mapping[str, Any] = field(default_factory=dict)
    definition_hash: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "factor_key", _required_text(self.factor_key, "factor_key"))
        for name in ("factor_version", "feature_set_version", "builder_version"):
            object.__setattr__(
                self,
                name,
                _v4_artifact_version(getattr(self, name), name),
            )
        object.__setattr__(self, "role", FactorRole(self.role))
        object.__setattr__(self, "scope_type", ScopeType(self.scope_type))
        object.__setattr__(
            self,
            "availability_status",
            AvailabilityStatus(self.availability_status),
        )
        object.__setattr__(
            self,
            "research_status",
            ResearchStatus(self.research_status),
        )
        object.__setattr__(self, "quality_status", QualityStatus(self.quality_status))
        require_aware(self.available_at, "available_at")
        if not isinstance(self.max_age_seconds, int) or isinstance(
            self.max_age_seconds, bool
        ):
            raise TypeError("max_age_seconds must be an integer")
        if self.max_age_seconds < 1:
            raise ValueError("max_age_seconds must be positive")
        missing_policy = _required_text(self.missing_policy, "missing_policy").upper()
        if missing_policy not in _MISSING_POLICIES:
            raise ValueError("factor missing_policy must fail closed or propagate null")
        object.__setattr__(self, "missing_policy", missing_policy)
        object.__setattr__(
            self,
            "formula_hash",
            _sha256(self.formula_hash, "formula_hash"),
        )

        source_versions = {
            source_key: _v4_artifact_version(source_version, "source version")
            for source_key, source_version in _normalized_mapping_items(
                self.required_source_versions,
                "required_source_versions",
            )
        }
        if not source_versions:
            raise ValueError("factor definition requires at least one source")
        required_fields = {
            source_key: _normalized_text_tuple(fields, "required_fields value")
            for source_key, fields in _normalized_mapping_items(
                self.required_fields,
                "required_fields",
            )
        }
        if set(required_fields) != set(source_versions):
            raise ValueError("required fields and source versions must use identical keys")
        if any(not fields for fields in required_fields.values()):
            raise ValueError("every required source must declare fields")
        output_fields = _normalized_text_tuple(self.output_fields, "output_fields")
        if not output_fields:
            raise ValueError("factor definition requires output_fields")
        assert_clean_payload(
            {
                "required_fields": required_fields,
                "output_fields": output_fields,
                "specification": self.specification,
            },
            path="factor_definition",
        )
        reason_codes = _normalized_text_tuple(self.reason_codes, "reason_codes")
        if self.quality_status != QualityStatus.PASS and not reason_codes:
            raise ValueError("non-PASS factor definition requires reason_codes")
        if (
            self.availability_status == AvailabilityStatus.ACTIVE
            and self.quality_status == QualityStatus.FAIL
        ):
            raise ValueError("ACTIVE factor definition cannot have FAIL quality")
        object.__setattr__(self, "required_source_versions", freeze(source_versions))
        object.__setattr__(self, "required_fields", freeze(required_fields))
        object.__setattr__(self, "output_fields", output_fields)
        object.__setattr__(self, "reason_codes", reason_codes)
        object.__setattr__(self, "specification", freeze(self.specification))

        content = {
            "factor_key": self.factor_key,
            "factor_version": self.factor_version,
            "role": self.role,
            "scope_type": self.scope_type,
            "feature_set_version": self.feature_set_version,
            "builder_version": self.builder_version,
            "required_source_versions": self.required_source_versions,
            "required_fields": self.required_fields,
            "output_fields": self.output_fields,
            "missing_policy": self.missing_policy,
            "availability_status": self.availability_status,
            "research_status": self.research_status,
            "quality_status": self.quality_status,
            "available_at": self.available_at,
            "formula_hash": self.formula_hash,
            "max_age_seconds": self.max_age_seconds,
            "reason_codes": self.reason_codes,
            "specification": self.specification,
        }
        object.__setattr__(self, "definition_hash", deterministic_hash(content))

    def is_available_as_of(self, as_of: datetime) -> bool:
        require_aware(as_of, "as_of")
        return bool(
            self.available_at <= as_of
            and self.availability_status == AvailabilityStatus.ACTIVE
            and self.quality_status == QualityStatus.PASS
        )

    @property
    def actionable(self) -> bool:
        return bool(
            self.availability_status == AvailabilityStatus.ACTIVE
            and self.research_status == ResearchStatus.BACKTEST_READY
            and self.quality_status == QualityStatus.PASS
            and self.missing_policy != "DISPLAY_ONLY"
        )


__all__ = ["DataSourceCertification", "FactorDefinition"]
