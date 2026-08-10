"""Immutable clean-room contracts for the V4 decision kernel.

This module owns facts and messages only.  It performs no I/O, reads no
ambient clock, and has no dependency on a legacy trading implementation.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from .enums import (
    ActionType,
    AvailabilityStatus,
    CandidateStatus,
    CommitStatus,
    DecisionBundleStatus,
    DecisionClock,
    ExecutionReceiptStatus,
    ExecutionSide,
    LimitPolicy,
    ProbabilityKind,
    QualityStatus,
    ResearchStatus,
    ScopeType,
)
from .hashes import (
    ContractMixin,
    canonical_datetime,
    deterministic_hash,
    deterministic_id,
    freeze,
    require_aware,
)


FORBIDDEN_LEGACY_FIELDS = frozenset(
    {
        "legacy_score",
        "v3_forecast",
        "v3_regime_probability",
        "v3_theme_rank",
        "v3_candidate_rank",
        "v3_hypothesis",
        "v3_target_weight",
        "v3_exit_action",
        "v3_calibration_bucket",
    }
)

V4_ARTIFACT_NAMESPACE = "v4:"

_ALLOWED_EXECUTABLE_ACTIONS = {
    ActionType.NO_ACTION: frozenset({ActionType.NO_ACTION}),
    ActionType.WATCH: frozenset({ActionType.WATCH, ActionType.NO_ACTION}),
    ActionType.BUY: frozenset({ActionType.BUY, ActionType.NO_ACTION}),
    ActionType.ADD: frozenset({ActionType.ADD, ActionType.NO_ACTION}),
    ActionType.HOLD: frozenset({ActionType.HOLD, ActionType.NO_ACTION}),
    ActionType.REDUCE: frozenset({ActionType.REDUCE, ActionType.NO_ACTION}),
    # A desired full exit may degrade to selling only the currently sellable
    # quantity.  No other direction-changing execution is valid.
    ActionType.EXIT: frozenset(
        {ActionType.EXIT, ActionType.REDUCE, ActionType.NO_ACTION}
    ),
}


def _required_text(value: str, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _sha256(value: str, field_name: str) -> str:
    normalized = _required_text(value, field_name).lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError(f"{field_name} must be a 64-character SHA-256 digest")
    return normalized


def _hex_identifier(
    value: str,
    field_name: str,
    *,
    minimum_length: int,
    maximum_length: int,
) -> str:
    normalized = _required_text(value, field_name).lower()
    if not minimum_length <= len(normalized) <= maximum_length or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError(
            f"{field_name} must be a {minimum_length}-{maximum_length} "
            "character hexadecimal identifier"
        )
    return normalized


def _text_mapping(
    value: Mapping[str, str],
    field_name: str,
) -> Mapping[str, str]:
    return freeze(
        {
            key: _required_text(
                item,
                f"{field_name} value",
            )
            for key, item in _normalized_mapping_items(value, field_name)
        }
    )


def _normalized_mapping_items(
    value: Mapping[str, Any],
    field_name: str,
) -> tuple[tuple[str, Any], ...]:
    """Normalize mapping keys while rejecting trim-induced collisions."""

    if not isinstance(value, Mapping):
        raise TypeError(f"{field_name} must be a mapping")
    normalized: list[tuple[str, Any]] = []
    seen: set[str] = set()
    for raw_key, item in value.items():
        key = _required_text(raw_key, f"{field_name} key")
        if key in seen:
            raise ValueError(
                f"{field_name} contains duplicate keys after normalization"
            )
        seen.add(key)
        normalized.append((key, item))
    return tuple(normalized)


def _require_exact_type(
    value: Any,
    expected_type: type[Any],
    field_name: str,
) -> None:
    """Reject DTOs, duck types and subclasses at owned contract boundaries."""

    if type(value) is not expected_type:
        raise TypeError(
            f"{field_name} must be exactly {expected_type.__name__}"
        )


def _require_exact_items(
    values: Any,
    expected_type: type[Any],
    field_name: str,
) -> tuple[Any, ...]:
    try:
        items = tuple(values)
    except TypeError as exc:
        raise TypeError(f"{field_name} must be iterable") from exc
    for index, item in enumerate(items):
        _require_exact_type(
            item,
            expected_type,
            f"{field_name}[{index}]",
        )
    return items


def _v4_artifact_version(value: str, field_name: str) -> str:
    normalized = _required_text(value, field_name)
    if not normalized.startswith(V4_ARTIFACT_NAMESPACE) or not normalized[
        len(V4_ARTIFACT_NAMESPACE):
    ].strip():
        raise ValueError(
            f"{field_name} must use the {V4_ARTIFACT_NAMESPACE!r} namespace"
        )
    return normalized


def _v4_version_mapping(
    value: Mapping[str, str],
    field_name: str,
) -> Mapping[str, str]:
    return freeze(
        {
            key: _v4_artifact_version(
                item,
                f"{field_name} value",
            )
            for key, item in _normalized_mapping_items(value, field_name)
        }
    )


def _decimal(value: Decimal | int | float | str, field_name: str) -> Decimal:
    try:
        converted = value if isinstance(value, Decimal) else Decimal(str(value))
    except Exception as exc:  # Decimal raises several conversion exceptions.
        raise ValueError(f"{field_name} must be a decimal value") from exc
    if not converted.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return converted


def _integer(value: Any, field_name: str, *, minimum: int = 0) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field_name} must be an integer")
    if value < minimum:
        raise ValueError(f"{field_name} must be at least {minimum}")
    return value


def _bounded_probability(
    value: Decimal | int | float | str | None,
    field_name: str,
) -> Decimal | None:
    if value is None:
        return None
    converted = _decimal(value, field_name)
    if converted < 0 or converted > 1:
        raise ValueError(f"{field_name} must be between 0 and 1")
    return converted


def assert_clean_payload(value: Any, *, path: str = "payload") -> None:
    """Fail closed when a legacy opinion field crosses the V4 boundary."""

    if isinstance(value, Mapping):
        for key, child in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{path} requires string keys")
            if key.strip().casefold() in FORBIDDEN_LEGACY_FIELDS:
                raise ValueError(f"forbidden legacy field at {path}.{key}")
            assert_clean_payload(child, path=f"{path}.{key}")
    elif isinstance(value, (tuple, list)):
        for index, child in enumerate(value):
            assert_clean_payload(child, path=f"{path}[{index}]")


@dataclass(frozen=True)
class ScopeRef(ContractMixin):
    scope_type: ScopeType
    scope_id: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "scope_type", ScopeType(self.scope_type))
        object.__setattr__(self, "scope_id", _required_text(self.scope_id, "scope_id"))


@dataclass(frozen=True)
class SourceWatermark(ContractMixin):
    source: str
    knowledge_time: datetime
    record_count: int
    quality_status: QualityStatus
    snapshot_id: str
    source_event_at: datetime | None = None
    first_seen_at: datetime | None = None
    received_at: datetime | None = None
    available_at: datetime | None = None
    valid_until: datetime | None = None
    coverage: Decimal | None = None
    batch_id: str = ""
    schema_version: str = ""
    content_hash: str = ""
    reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "source", _required_text(self.source, "source"))
        object.__setattr__(
            self,
            "snapshot_id",
            _required_text(self.snapshot_id, "snapshot_id"),
        )
        require_aware(self.knowledge_time, "knowledge_time")
        _integer(self.record_count, "record_count")
        object.__setattr__(self, "quality_status", QualityStatus(self.quality_status))
        for name in (
            "source_event_at",
            "first_seen_at",
            "received_at",
            "available_at",
            "valid_until",
        ):
            value = getattr(self, name)
            if value is not None:
                require_aware(value, name)
        observable_times = {
            name: getattr(self, name)
            for name in (
                "source_event_at",
                "first_seen_at",
                "received_at",
                "available_at",
            )
            if getattr(self, name) is not None
        }
        if any(value > self.knowledge_time for value in observable_times.values()):
            raise ValueError(
                "source watermark knowledge_time cannot precede an "
                "observable source timestamp"
            )
        if self.valid_until is not None and self.valid_until < self.knowledge_time:
            raise ValueError("source watermark valid_until precedes knowledge_time")
        coverage = _bounded_probability(self.coverage, "coverage")
        if self.quality_status == QualityStatus.PASS:
            if self.record_count < 1:
                raise ValueError("PASS source watermark requires non-empty evidence")
            if coverage != Decimal("1"):
                raise ValueError("PASS source watermark requires complete coverage")
            if self.valid_until is None:
                raise ValueError("PASS source watermark requires valid_until")
            if not self.batch_id or not self.schema_version or not self.content_hash:
                raise ValueError(
                    "PASS source watermark requires batch, schema and content identity"
                )
        elif self.record_count == 0 and "EMPTY_UNIVERSE" not in self.reason_codes:
            raise ValueError(
                "empty source watermark requires explicit EMPTY_UNIVERSE reason"
            )
        object.__setattr__(self, "coverage", coverage)
        for name in ("batch_id", "schema_version"):
            value = getattr(self, name)
            if value:
                object.__setattr__(self, name, _required_text(value, name))
        if self.content_hash:
            object.__setattr__(
                self,
                "content_hash",
                _sha256(self.content_hash, "content_hash"),
            )
        object.__setattr__(
            self,
            "reason_codes",
            tuple(
                sorted(
                    {
                        _required_text(item, "reason_codes item")
                        for item in self.reason_codes
                    }
                )
            ),
        )


@dataclass(frozen=True)
class DataManifest(ContractMixin):
    """Immutable record-hash manifest bound into every decision context."""

    record_hashes: Mapping[str, str]
    manifest_version: str = "data-manifest-v1"
    manifest_hash: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "manifest_version",
            _required_text(self.manifest_version, "manifest_version"),
        )
        record_hashes = {
            record_id: _sha256(
                record_hash,
                "record_hashes value",
            )
            for record_id, record_hash in _normalized_mapping_items(
                self.record_hashes,
                "record_hashes",
            )
        }
        if not record_hashes:
            raise ValueError("data manifest must contain at least one record")
        object.__setattr__(self, "record_hashes", freeze(record_hashes))
        object.__setattr__(
            self,
            "manifest_hash",
            deterministic_hash(
                {
                    "manifest_version": self.manifest_version,
                    "record_hashes": self.record_hashes,
                }
            ),
        )

    def contains_exact_subset(self, candidate: Mapping[str, str]) -> bool:
        """Return whether every candidate id/hash pair is in this manifest."""

        return all(
            self.record_hashes.get(record_id) == record_hash
            for record_id, record_hash in candidate.items()
        )


@dataclass(frozen=True)
class CapabilityStatus(ContractMixin):
    name: str
    availability_status: AvailabilityStatus
    research_status: ResearchStatus
    quality_status: QualityStatus
    reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _required_text(self.name, "name"))
        object.__setattr__(
            self,
            "availability_status",
            AvailabilityStatus(self.availability_status),
        )
        object.__setattr__(self, "research_status", ResearchStatus(self.research_status))
        object.__setattr__(self, "quality_status", QualityStatus(self.quality_status))
        object.__setattr__(self, "reason_codes", tuple(sorted(set(self.reason_codes))))

    @property
    def actionable(self) -> bool:
        return (
            self.availability_status == AvailabilityStatus.ACTIVE
            and self.quality_status == QualityStatus.PASS
            and self.research_status == ResearchStatus.BACKTEST_READY
        )


@dataclass(frozen=True)
class AsOfRecord(ContractMixin):
    record_id: str
    source: str
    knowledge_time: datetime
    ingested_at: datetime
    payload: Mapping[str, Any]
    event_time: datetime | None = None
    source_published_at: datetime | None = None
    first_seen_at: datetime | None = None
    received_at: datetime | None = None
    revised_at: datetime | None = None
    revision_id: str = ""
    quality_status: QualityStatus = QualityStatus.PASS
    record_hash: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "record_id", _required_text(self.record_id, "record_id"))
        object.__setattr__(self, "source", _required_text(self.source, "source"))
        for name in (
            "knowledge_time",
            "ingested_at",
            "event_time",
            "source_published_at",
            "first_seen_at",
            "received_at",
            "revised_at",
        ):
            value = getattr(self, name)
            if value is not None:
                require_aware(value, name)
        acquisition_times = {
            name: getattr(self, name)
            for name in (
                "ingested_at",
                "source_published_at",
                "first_seen_at",
                "received_at",
                "revised_at",
            )
            if getattr(self, name) is not None
        }
        future_acquisitions = {
            name: value
            for name, value in acquisition_times.items()
            if value > self.knowledge_time
        }
        if future_acquisitions:
            raise ValueError(
                "knowledge_time cannot precede an acquisition timestamp: "
                f"{tuple(sorted(future_acquisitions))}"
            )
        assert_clean_payload(self.payload)
        object.__setattr__(self, "payload", freeze(self.payload))
        object.__setattr__(self, "quality_status", QualityStatus(self.quality_status))
        content = {
            "record_id": self.record_id,
            "source": self.source,
            "knowledge_time": self.knowledge_time,
            "ingested_at": self.ingested_at,
            "payload": self.payload,
            "event_time": self.event_time,
            "source_published_at": self.source_published_at,
            "first_seen_at": self.first_seen_at,
            "received_at": self.received_at,
            "revised_at": self.revised_at,
            "revision_id": self.revision_id,
            "quality_status": self.quality_status,
        }
        object.__setattr__(self, "record_hash", deterministic_hash(content))


@dataclass(frozen=True)
class AsOfDataset(ContractMixin):
    dataset_name: str
    as_of: datetime
    records: tuple[AsOfRecord, ...]
    quality_status: QualityStatus
    dataset_id: str = field(init=False)
    manifest_hash: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "dataset_name",
            _required_text(self.dataset_name, "dataset_name"),
        )
        require_aware(self.as_of, "as_of")
        records = _require_exact_items(
            self.records,
            AsOfRecord,
            "records",
        )
        ordered = tuple(
            sorted(
                records,
                key=lambda item: (
                    item.source,
                    item.record_id,
                    item.revision_id,
                    canonical_datetime(item.knowledge_time),
                ),
            )
        )
        keys = [(item.source, item.record_id, item.revision_id) for item in ordered]
        if len(keys) != len(set(keys)):
            raise ValueError("dataset contains duplicate source/record/revision keys")
        if any(item.knowledge_time > self.as_of for item in ordered):
            raise ValueError("dataset contains a record beyond its as_of boundary")
        object.__setattr__(self, "records", ordered)
        object.__setattr__(self, "quality_status", QualityStatus(self.quality_status))
        manifest = {
            "dataset_name": self.dataset_name,
            "as_of": self.as_of,
            "quality_status": self.quality_status,
            "record_hashes": [item.record_hash for item in ordered],
        }
        digest = deterministic_hash(manifest)
        object.__setattr__(self, "manifest_hash", digest)
        object.__setattr__(self, "dataset_id", f"dataset_{digest[:24]}")


@dataclass(frozen=True)
class DatasetResult(ContractMixin):
    dataset: AsOfDataset
    requested_cutoff: datetime
    requested_entities: tuple[str, ...]
    returned_entities: tuple[str, ...]
    requested_fields: tuple[str, ...]
    freshness_status: QualityStatus
    reason_codes: tuple[str, ...] = ()
    missing_entities: tuple[str, ...] = field(init=False)
    coverage: Decimal = field(init=False)

    def __post_init__(self) -> None:
        _require_exact_type(self.dataset, AsOfDataset, "dataset")
        require_aware(self.requested_cutoff, "requested_cutoff")
        if self.dataset.as_of > self.requested_cutoff:
            raise ValueError("dataset exceeds requested cutoff")
        requested = tuple(
            sorted(
                {
                    _required_text(item, "requested_entities item")
                    for item in self.requested_entities
                }
            )
        )
        if not requested:
            raise ValueError("requested_entities must not be empty")
        returned = tuple(
            sorted(
                {
                    _required_text(item, "returned_entities item")
                    for item in self.returned_entities
                }
            )
        )
        if not set(returned).issubset(requested):
            raise ValueError("returned_entities must be requested")
        if returned and not self.dataset.records:
            raise ValueError("returned_entities require dataset records")
        requested_fields = tuple(
            sorted(
                {
                    _required_text(item, "requested_fields item")
                    for item in self.requested_fields
                }
            )
        )
        missing = tuple(sorted(set(requested) - set(returned)))
        status = QualityStatus(self.freshness_status)
        if status == QualityStatus.PASS and (
            missing or self.dataset.quality_status != QualityStatus.PASS
        ):
            raise ValueError(
                "PASS dataset result requires complete, PASS coverage"
            )
        object.__setattr__(self, "requested_entities", requested)
        object.__setattr__(self, "returned_entities", returned)
        object.__setattr__(self, "requested_fields", requested_fields)
        object.__setattr__(self, "missing_entities", missing)
        object.__setattr__(self, "freshness_status", status)
        object.__setattr__(
            self,
            "reason_codes",
            tuple(
                sorted(
                    {
                        _required_text(item, "reason_codes item")
                        for item in self.reason_codes
                    }
                )
            ),
        )
        object.__setattr__(
            self,
            "coverage",
            Decimal(len(returned)) / Decimal(len(requested)),
        )


@dataclass(frozen=True)
class FeatureVector(ContractMixin):
    scope: ScopeRef
    feature_set_version: str
    feature_builder_version: str
    capability_name: str
    source_manifest_hash: str
    knowledge_time: datetime
    valid_until: datetime
    values: Mapping[str, Any]
    source_record_ids: tuple[str, ...] = ()
    source_record_hashes: Mapping[str, str] = field(default_factory=dict)
    quality_status: QualityStatus = QualityStatus.PASS
    missing_fields: tuple[str, ...] = ()
    reason_codes: tuple[str, ...] = ()
    feature_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_exact_type(self.scope, ScopeRef, "scope")
        object.__setattr__(
            self,
            "feature_set_version",
            _required_text(self.feature_set_version, "feature_set_version"),
        )
        for name in ("feature_builder_version", "capability_name"):
            object.__setattr__(
                self,
                name,
                _required_text(getattr(self, name), name),
            )
        object.__setattr__(
            self,
            "source_manifest_hash",
            _sha256(self.source_manifest_hash, "source_manifest_hash"),
        )
        require_aware(self.knowledge_time, "knowledge_time")
        require_aware(self.valid_until, "valid_until")
        if self.valid_until < self.knowledge_time:
            raise ValueError("feature valid_until cannot precede knowledge_time")
        assert_clean_payload(self.values, path="feature_values")
        object.__setattr__(self, "values", freeze(self.values))
        source_record_ids: list[str] = []
        seen_record_ids: set[str] = set()
        for raw_record_id in self.source_record_ids:
            record_id = _required_text(
                raw_record_id,
                "source_record_ids item",
            )
            if record_id in seen_record_ids:
                raise ValueError(
                    "source_record_ids contains duplicates after normalization"
                )
            seen_record_ids.add(record_id)
            source_record_ids.append(record_id)
        object.__setattr__(
            self,
            "source_record_ids",
            tuple(sorted(source_record_ids)),
        )
        hashes = {
            record_id: _sha256(
                record_hash,
                "source_record_hashes value",
            )
            for record_id, record_hash in _normalized_mapping_items(
                self.source_record_hashes,
                "source_record_hashes",
            )
        }
        if not self.source_record_ids or not hashes:
            raise ValueError(
                "feature vector requires source record ids and hashes"
            )
        if set(self.source_record_ids) != set(hashes):
            raise ValueError(
                "source record ids and hash keys must match"
            )
        object.__setattr__(
            self,
            "source_record_hashes",
            freeze(hashes),
        )
        object.__setattr__(
            self,
            "quality_status",
            QualityStatus(self.quality_status),
        )
        missing_fields = tuple(
            sorted(
                {
                    _required_text(item, "missing_fields item")
                    for item in self.missing_fields
                }
            )
        )
        reason_codes = tuple(
            sorted(
                {
                    _required_text(item, "reason_codes item")
                    for item in self.reason_codes
                }
            )
        )
        if self.quality_status == QualityStatus.PASS and missing_fields:
            raise ValueError("PASS feature vector cannot contain missing fields")
        if self.quality_status != QualityStatus.PASS and not reason_codes:
            raise ValueError("non-PASS feature vector requires reason_codes")
        object.__setattr__(self, "missing_fields", missing_fields)
        object.__setattr__(self, "reason_codes", reason_codes)
        content = {
            "scope": self.scope,
            "feature_set_version": self.feature_set_version,
            "feature_builder_version": self.feature_builder_version,
            "capability_name": self.capability_name,
            "source_manifest_hash": self.source_manifest_hash,
            "knowledge_time": self.knowledge_time,
            "valid_until": self.valid_until,
            "values": self.values,
            "source_record_ids": self.source_record_ids,
            "source_record_hashes": self.source_record_hashes,
            "quality_status": self.quality_status,
            "missing_fields": self.missing_fields,
            "reason_codes": self.reason_codes,
        }
        object.__setattr__(self, "feature_hash", deterministic_hash(content))


@dataclass(frozen=True)
class InstrumentRuleSnapshot(ContractMixin):
    instrument: str
    rule_version: str
    effective_at: datetime
    knowledge_time: datetime
    valid_until: datetime
    can_buy: bool
    can_sell: bool
    first_buy_minimum: int
    buy_lot_size: int
    sell_lot_size: int
    settlement_days: int
    tick_size: Decimal
    allow_odd_lot_liquidation: bool
    is_suspended: bool = False
    upper_limit: Decimal | None = None
    lower_limit: Decimal | None = None
    limit_up_locked: bool = False
    limit_down_locked: bool = False
    quality_status: QualityStatus = QualityStatus.PASS

    def __post_init__(self) -> None:
        object.__setattr__(self, "instrument", _required_text(self.instrument, "instrument"))
        object.__setattr__(
            self,
            "rule_version",
            _required_text(self.rule_version, "rule_version"),
        )
        require_aware(self.effective_at, "effective_at")
        require_aware(self.knowledge_time, "knowledge_time")
        require_aware(self.valid_until, "valid_until")
        if self.effective_at > self.knowledge_time:
            raise ValueError("rule effective_at cannot follow knowledge_time")
        if self.valid_until < self.effective_at:
            raise ValueError("rule valid_until cannot precede effective_at")
        for name in (
            "can_buy",
            "can_sell",
            "allow_odd_lot_liquidation",
            "is_suspended",
            "limit_up_locked",
            "limit_down_locked",
        ):
            if not isinstance(getattr(self, name), bool):
                raise TypeError(f"{name} must be a bool")
        _integer(self.first_buy_minimum, "first_buy_minimum")
        _integer(self.buy_lot_size, "buy_lot_size", minimum=1)
        _integer(self.sell_lot_size, "sell_lot_size", minimum=1)
        _integer(self.settlement_days, "settlement_days")
        tick_size = _decimal(self.tick_size, "tick_size")
        if tick_size <= 0:
            raise ValueError("tick_size must be positive")
        object.__setattr__(self, "tick_size", tick_size)
        for name in ("upper_limit", "lower_limit"):
            value = getattr(self, name)
            if value is not None:
                converted = _decimal(value, name)
                if converted <= 0:
                    raise ValueError(f"{name} must be positive")
                object.__setattr__(self, name, converted)
        if (self.upper_limit is None) != (self.lower_limit is None):
            raise ValueError(
                "upper_limit and lower_limit must be supplied together"
            )
        if (
            self.upper_limit is not None
            and self.lower_limit is not None
            and self.upper_limit < self.lower_limit
        ):
            raise ValueError("upper_limit must not be below lower_limit")
        object.__setattr__(
            self,
            "quality_status",
            QualityStatus(self.quality_status),
        )


@dataclass(frozen=True)
class PositionSnapshot(ContractMixin):
    instrument: str
    total_quantity: int
    sellable_quantity: int
    average_cost: Decimal
    last_price: Decimal
    origin_strategy: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "instrument", _required_text(self.instrument, "instrument"))
        _integer(self.total_quantity, "total_quantity")
        _integer(self.sellable_quantity, "sellable_quantity")
        if self.sellable_quantity > self.total_quantity:
            raise ValueError("sellable_quantity must be within total_quantity")
        for name in ("average_cost", "last_price"):
            converted = _decimal(getattr(self, name), name)
            if converted < 0:
                raise ValueError(f"{name} must be non-negative")
            object.__setattr__(self, name, converted)

    @property
    def market_value(self) -> Decimal:
        return self.last_price * self.total_quantity


@dataclass(frozen=True)
class AccountSnapshot(ContractMixin):
    account_snapshot_id: str
    account_id: str
    as_of: datetime
    available_cash: Decimal
    equity: Decimal
    positions: tuple[PositionSnapshot, ...] = ()
    state_hash: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "account_snapshot_id",
            _required_text(self.account_snapshot_id, "account_snapshot_id"),
        )
        object.__setattr__(self, "account_id", _required_text(self.account_id, "account_id"))
        require_aware(self.as_of, "as_of")
        for name in ("available_cash", "equity"):
            converted = _decimal(getattr(self, name), name)
            if converted < 0:
                raise ValueError(f"{name} must be non-negative")
            object.__setattr__(self, name, converted)
        positions = _require_exact_items(
            self.positions,
            PositionSnapshot,
            "positions",
        )
        ordered = tuple(sorted(positions, key=lambda item: item.instrument))
        instruments = [item.instrument for item in ordered]
        if len(instruments) != len(set(instruments)):
            raise ValueError("account snapshot contains duplicate instruments")
        object.__setattr__(self, "positions", ordered)
        content = {
            "account_snapshot_id": self.account_snapshot_id,
            "account_id": self.account_id,
            "as_of": self.as_of,
            "available_cash": self.available_cash,
            "equity": self.equity,
            "positions": self.positions,
        }
        object.__setattr__(self, "state_hash", deterministic_hash(content))


@dataclass(frozen=True)
class DecisionContext(ContractMixin):
    decision_time: datetime
    decision_clock: DecisionClock
    knowledge_cutoff: datetime
    trade_date: date
    universe_version: str
    data_manifest: DataManifest
    portfolio_policy_version: str
    execution_contract_version: str
    fee_schedule_version: str
    account_snapshot_id: str
    code_commit_sha: str
    config_hash: str
    random_seed: int
    source_watermarks: Mapping[str, SourceWatermark] = field(default_factory=dict)
    factor_spec_versions: Mapping[str, str] = field(default_factory=dict)
    forecast_contract_ids: tuple[str, ...] = ()
    model_versions: Mapping[str, str] = field(default_factory=dict)
    model_artifact_hashes: Mapping[str, str] = field(default_factory=dict)
    model_training_cutoffs: Mapping[str, datetime] = field(default_factory=dict)
    model_available_at: Mapping[str, datetime] = field(default_factory=dict)
    calibration_versions: Mapping[str, str] = field(default_factory=dict)
    calibration_artifact_hashes: Mapping[str, str] = field(default_factory=dict)
    calibration_training_cutoffs: Mapping[str, datetime] = field(
        default_factory=dict
    )
    calibration_available_at: Mapping[str, datetime] = field(
        default_factory=dict
    )
    capability_statuses: Mapping[str, CapabilityStatus] = field(default_factory=dict)
    raw_data_manifest_hash: str = field(init=False)
    context_id: str = field(init=False)
    context_hash: str = field(init=False)

    def __post_init__(self) -> None:
        require_aware(self.decision_time, "decision_time")
        require_aware(self.knowledge_cutoff, "knowledge_cutoff")
        if self.knowledge_cutoff > self.decision_time:
            raise ValueError("knowledge_cutoff must not exceed decision_time")
        if not isinstance(self.trade_date, date) or isinstance(self.trade_date, datetime):
            raise TypeError("trade_date must be a date")
        _require_exact_type(self.data_manifest, DataManifest, "data_manifest")
        object.__setattr__(
            self,
            "raw_data_manifest_hash",
            self.data_manifest.manifest_hash,
        )
        object.__setattr__(self, "decision_clock", DecisionClock(self.decision_clock))
        for name in (
            "universe_version",
            "portfolio_policy_version",
            "execution_contract_version",
            "fee_schedule_version",
            "account_snapshot_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name), name))
        object.__setattr__(
            self,
            "code_commit_sha",
            _hex_identifier(
                self.code_commit_sha,
                "code_commit_sha",
                minimum_length=7,
                maximum_length=64,
            ),
        )
        object.__setattr__(self, "config_hash", _sha256(self.config_hash, "config_hash"))
        _integer(self.random_seed, "random_seed")
        if self.random_seed > (2**64 - 1):
            raise ValueError("random_seed must fit an unsigned 64-bit integer")

        watermarks = dict(
            _normalized_mapping_items(
                self.source_watermarks,
                "source_watermarks",
            )
        )
        for source, watermark in watermarks.items():
            _require_exact_type(
                watermark,
                SourceWatermark,
                f"source_watermarks[{source!r}]",
            )
            if source != watermark.source:
                raise ValueError("source watermark key must match watermark.source")
            if watermark.knowledge_time > self.knowledge_cutoff:
                raise ValueError("source watermark exceeds knowledge_cutoff")
            if (
                watermark.valid_until is None
                or watermark.valid_until < self.decision_time
            ):
                raise ValueError("source watermark expired before decision_time")
        capabilities = dict(
            _normalized_mapping_items(
                self.capability_statuses,
                "capability_statuses",
            )
        )
        for name, capability in capabilities.items():
            _require_exact_type(
                capability,
                CapabilityStatus,
                f"capability_statuses[{name!r}]",
            )
            if name != capability.name:
                raise ValueError("capability key must match capability.name")

        object.__setattr__(self, "source_watermarks", freeze(watermarks))
        object.__setattr__(
            self,
            "factor_spec_versions",
            _text_mapping(
                self.factor_spec_versions,
                "factor_spec_versions",
            ),
        )
        object.__setattr__(
            self,
            "forecast_contract_ids",
            tuple(
                sorted(
                    {
                        _required_text(
                            item,
                            "forecast_contract_ids item",
                        )
                        for item in self.forecast_contract_ids
                    }
                )
            ),
        )
        object.__setattr__(
            self,
            "model_versions",
            _v4_version_mapping(self.model_versions, "model_versions"),
        )
        model_artifact_hashes = {
            key: _sha256(
                value,
                "model_artifact_hashes value",
            )
            for key, value in _normalized_mapping_items(
                self.model_artifact_hashes,
                "model_artifact_hashes",
            )
        }
        model_training_cutoffs = dict(
            _normalized_mapping_items(
                self.model_training_cutoffs,
                "model_training_cutoffs",
            )
        )
        for model_name, cutoff in model_training_cutoffs.items():
            require_aware(cutoff, "model_training_cutoffs value")
        model_available_at = dict(
            _normalized_mapping_items(
                self.model_available_at,
                "model_available_at",
            )
        )
        for model_name, available_at in model_available_at.items():
            require_aware(available_at, "model_available_at value")
        model_keys = set(self.model_versions)
        if set(model_artifact_hashes) != model_keys or set(
            model_training_cutoffs
        ) != model_keys or set(model_available_at) != model_keys:
            raise ValueError(
                "model versions, artifact hashes, training cutoffs and "
                "availability times "
                "must use identical keys"
            )
        for model_name in model_keys:
            training_cutoff = model_training_cutoffs[model_name]
            available_at = model_available_at[model_name]
            if training_cutoff > available_at:
                raise ValueError(
                    "model training cutoff exceeds model availability"
                )
            if available_at > self.knowledge_cutoff:
                raise ValueError(
                    "model availability exceeds knowledge_cutoff"
                )
        object.__setattr__(
            self,
            "model_artifact_hashes",
            freeze(model_artifact_hashes),
        )
        object.__setattr__(
            self,
            "model_training_cutoffs",
            freeze(model_training_cutoffs),
        )
        object.__setattr__(
            self,
            "model_available_at",
            freeze(model_available_at),
        )
        object.__setattr__(
            self,
            "calibration_versions",
            _v4_version_mapping(
                self.calibration_versions,
                "calibration_versions",
            ),
        )
        calibration_artifact_hashes = {
            key: _sha256(
                value,
                "calibration_artifact_hashes value",
            )
            for key, value in _normalized_mapping_items(
                self.calibration_artifact_hashes,
                "calibration_artifact_hashes",
            )
        }
        calibration_training_cutoffs = dict(
            _normalized_mapping_items(
                self.calibration_training_cutoffs,
                "calibration_training_cutoffs",
            )
        )
        calibration_available_at = dict(
            _normalized_mapping_items(
                self.calibration_available_at,
                "calibration_available_at",
            )
        )
        for field_name, timestamps in (
            (
                "calibration_training_cutoffs",
                calibration_training_cutoffs,
            ),
            ("calibration_available_at", calibration_available_at),
        ):
            for _calibration_name, timestamp in timestamps.items():
                require_aware(timestamp, f"{field_name} value")
        calibration_keys = set(self.calibration_versions)
        if (
            set(calibration_artifact_hashes) != calibration_keys
            or set(calibration_training_cutoffs) != calibration_keys
            or set(calibration_available_at) != calibration_keys
        ):
            raise ValueError(
                "calibration versions, artifact hashes, training cutoffs "
                "and availability times must use identical keys"
            )
        for calibration_name in calibration_keys:
            training_cutoff = calibration_training_cutoffs[
                calibration_name
            ]
            available_at = calibration_available_at[calibration_name]
            if training_cutoff > available_at:
                raise ValueError(
                    "calibration training cutoff exceeds availability"
                )
            if available_at > self.knowledge_cutoff:
                raise ValueError(
                    "calibration availability exceeds knowledge_cutoff"
                )
        object.__setattr__(
            self,
            "calibration_artifact_hashes",
            freeze(calibration_artifact_hashes),
        )
        object.__setattr__(
            self,
            "calibration_training_cutoffs",
            freeze(calibration_training_cutoffs),
        )
        object.__setattr__(
            self,
            "calibration_available_at",
            freeze(calibration_available_at),
        )
        object.__setattr__(self, "capability_statuses", freeze(capabilities))

        content = {
            "decision_time": self.decision_time,
            "decision_clock": self.decision_clock,
            "knowledge_cutoff": self.knowledge_cutoff,
            "trade_date": self.trade_date,
            "universe_version": self.universe_version,
            "data_manifest": self.data_manifest,
            "raw_data_manifest_hash": self.raw_data_manifest_hash,
            "source_watermarks": self.source_watermarks,
            "factor_spec_versions": self.factor_spec_versions,
            "forecast_contract_ids": self.forecast_contract_ids,
            "model_versions": self.model_versions,
            "model_artifact_hashes": self.model_artifact_hashes,
            "model_training_cutoffs": self.model_training_cutoffs,
            "model_available_at": self.model_available_at,
            "calibration_versions": self.calibration_versions,
            "calibration_artifact_hashes": self.calibration_artifact_hashes,
            "calibration_training_cutoffs": (
                self.calibration_training_cutoffs
            ),
            "calibration_available_at": self.calibration_available_at,
            "portfolio_policy_version": self.portfolio_policy_version,
            "execution_contract_version": self.execution_contract_version,
            "fee_schedule_version": self.fee_schedule_version,
            "account_snapshot_id": self.account_snapshot_id,
            "code_commit_sha": self.code_commit_sha,
            "config_hash": self.config_hash,
            "random_seed": self.random_seed,
            "capability_statuses": self.capability_statuses,
        }
        digest = deterministic_hash(content)
        object.__setattr__(self, "context_hash", digest)
        object.__setattr__(self, "context_id", f"ctx_{digest[:24]}")


@dataclass(frozen=True)
class DecisionInput(ContractMixin):
    context: DecisionContext
    account: AccountSnapshot
    scopes: tuple[ScopeRef, ...]
    feature_vectors: tuple[FeatureVector, ...]
    instrument_rules: tuple[InstrumentRuleSnapshot, ...]
    input_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_exact_type(self.context, DecisionContext, "context")
        _require_exact_type(self.account, AccountSnapshot, "account")
        scopes_input = _require_exact_items(
            self.scopes,
            ScopeRef,
            "scopes",
        )
        vectors_input = _require_exact_items(
            self.feature_vectors,
            FeatureVector,
            "feature_vectors",
        )
        rules_input = _require_exact_items(
            self.instrument_rules,
            InstrumentRuleSnapshot,
            "instrument_rules",
        )
        _require_exact_items(
            self.account.positions,
            PositionSnapshot,
            "account.positions",
        )
        if self.account.account_snapshot_id != self.context.account_snapshot_id:
            raise ValueError("account snapshot does not match decision context")
        if self.account.as_of > self.context.knowledge_cutoff:
            raise ValueError("account snapshot exceeds knowledge_cutoff")
        scopes = tuple(sorted(scopes_input, key=lambda item: (item.scope_type.value, item.scope_id)))
        scope_keys = [(item.scope_type.value, item.scope_id) for item in scopes]
        if len(scope_keys) != len(set(scope_keys)):
            raise ValueError("decision input contains duplicate scopes")
        vectors = tuple(
            sorted(
                vectors_input,
                key=lambda item: (
                    item.scope.scope_type.value,
                    item.scope.scope_id,
                    item.feature_set_version,
                ),
            )
        )
        vector_keys = [
            (item.scope.scope_type.value, item.scope.scope_id, item.feature_set_version)
            for item in vectors
        ]
        if len(vector_keys) != len(set(vector_keys)):
            raise ValueError("decision input contains duplicate feature vectors")
        if any(item.knowledge_time > self.context.knowledge_cutoff for item in vectors):
            raise ValueError("feature vector exceeds knowledge_cutoff")
        if any(item.valid_until < self.context.decision_time for item in vectors):
            raise ValueError("feature vector expired before decision_time")
        if any(
            item.source_manifest_hash
            != self.context.raw_data_manifest_hash
            for item in vectors
        ):
            raise ValueError(
                "feature vector source manifest does not match context"
            )
        if any(
            not self.context.data_manifest.contains_exact_subset(
                item.source_record_hashes
            )
            for item in vectors
        ):
            raise ValueError(
                "feature vector record hashes are absent from data manifest"
            )
        unknown_capabilities = {
            item.capability_name
            for item in vectors
            if item.capability_name not in self.context.capability_statuses
        }
        if unknown_capabilities:
            raise ValueError(
                "feature vectors reference undeclared capabilities: "
                f"{tuple(sorted(unknown_capabilities))}"
            )
        scope_set = set(scope_keys)
        if any(
            (item.scope.scope_type.value, item.scope.scope_id) not in scope_set
            for item in vectors
        ):
            raise ValueError("feature vector scope is absent from decision scopes")
        rules = tuple(
            sorted(rules_input, key=lambda item: item.instrument)
        )
        rule_instruments = [item.instrument for item in rules]
        if len(rule_instruments) != len(set(rule_instruments)):
            raise ValueError("decision input contains duplicate instrument rules")
        if any(
            item.knowledge_time > self.context.knowledge_cutoff
            for item in rules
        ):
            raise ValueError("instrument rule exceeds knowledge_cutoff")
        if any(
            item.effective_at > self.context.decision_time
            for item in rules
        ):
            raise ValueError("instrument rule is not yet effective")
        if any(
            item.valid_until < self.context.decision_time
            for item in rules
        ):
            raise ValueError("instrument rule expired before decision_time")
        instrument_scopes = {
            item.scope_id
            for item in scopes
            if item.scope_type == ScopeType.INSTRUMENT
        }
        position_instruments = {
            item.instrument for item in self.account.positions
        }
        missing_position_scopes = position_instruments - instrument_scopes
        if missing_position_scopes:
            raise ValueError(
                "account positions are absent from decision scopes: "
                f"{tuple(sorted(missing_position_scopes))}"
            )
        required_rules = instrument_scopes | position_instruments
        missing_rules = required_rules - set(rule_instruments)
        if missing_rules:
            raise ValueError(
                "instrument rules are missing from decision input: "
                f"{tuple(sorted(missing_rules))}"
            )
        object.__setattr__(self, "scopes", scopes)
        object.__setattr__(self, "feature_vectors", vectors)
        object.__setattr__(self, "instrument_rules", rules)
        content = {
            "context": self.context,
            "account": self.account,
            "scopes": self.scopes,
            "feature_vectors": self.feature_vectors,
            "instrument_rules": self.instrument_rules,
        }
        object.__setattr__(self, "input_hash", deterministic_hash(content))


@dataclass(frozen=True)
class ForecastResult(ContractMixin):
    scope: ScopeRef
    forecast_contract_id: str
    model_id: str
    model_version: str
    calibration_id: str
    calibration_version: str
    signal_at: datetime
    valid_until: datetime
    expected_return_net_pct: Decimal | None
    cvar95_loss_pct: Decimal | None
    probability_positive: Decimal | None
    confidence: Decimal
    probability_kind: ProbabilityKind
    status: CandidateStatus
    reason_codes: tuple[str, ...] = ()
    forecast_id: str = field(init=False)

    def __post_init__(self) -> None:
        _require_exact_type(self.scope, ScopeRef, "scope")
        for name in (
            "forecast_contract_id",
            "model_id",
            "calibration_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name), name))
        for name in ("model_version", "calibration_version"):
            object.__setattr__(
                self,
                name,
                _v4_artifact_version(getattr(self, name), name),
            )
        require_aware(self.signal_at, "signal_at")
        require_aware(self.valid_until, "valid_until")
        if self.valid_until < self.signal_at:
            raise ValueError("valid_until must not precede signal_at")
        for name in ("expected_return_net_pct", "cvar95_loss_pct"):
            value = getattr(self, name)
            if value is not None:
                converted = _decimal(value, name)
                if name == "cvar95_loss_pct" and converted < 0:
                    raise ValueError("cvar95_loss_pct must be non-negative")
                object.__setattr__(self, name, converted)
        object.__setattr__(
            self,
            "probability_positive",
            _bounded_probability(self.probability_positive, "probability_positive"),
        )
        confidence = _bounded_probability(self.confidence, "confidence")
        if confidence is None:
            raise ValueError("confidence is required")
        object.__setattr__(self, "confidence", confidence)
        object.__setattr__(self, "probability_kind", ProbabilityKind(self.probability_kind))
        object.__setattr__(self, "status", CandidateStatus(self.status))
        object.__setattr__(self, "reason_codes", tuple(sorted(set(self.reason_codes))))
        content = {
            "scope": self.scope,
            "forecast_contract_id": self.forecast_contract_id,
            "model_id": self.model_id,
            "model_version": self.model_version,
            "calibration_id": self.calibration_id,
            "calibration_version": self.calibration_version,
            "signal_at": self.signal_at,
            "valid_until": self.valid_until,
            "expected_return_net_pct": self.expected_return_net_pct,
            "cvar95_loss_pct": self.cvar95_loss_pct,
            "probability_positive": self.probability_positive,
            "confidence": self.confidence,
            "probability_kind": self.probability_kind,
            "status": self.status,
            "reason_codes": self.reason_codes,
        }
        object.__setattr__(self, "forecast_id", deterministic_id("forecast", content))


@dataclass(frozen=True)
class DecisionAction(ContractMixin):
    decision_id: str
    instrument: str
    desired_action: ActionType
    executable_action: ActionType
    current_quantity: int
    sellable_quantity: int
    target_quantity: int
    earliest_execution_time: datetime
    valid_until: datetime
    candidate_status: CandidateStatus
    blocked_reason: str = ""
    reason_codes: tuple[str, ...] = ()
    evidence_ids: tuple[str, ...] = ()
    forecast_ids: tuple[str, ...] = ()
    action_id: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "decision_id", _required_text(self.decision_id, "decision_id"))
        object.__setattr__(self, "instrument", _required_text(self.instrument, "instrument"))
        object.__setattr__(self, "desired_action", ActionType(self.desired_action))
        object.__setattr__(self, "executable_action", ActionType(self.executable_action))
        object.__setattr__(self, "candidate_status", CandidateStatus(self.candidate_status))
        _integer(self.current_quantity, "current_quantity")
        _integer(self.sellable_quantity, "sellable_quantity")
        _integer(self.target_quantity, "target_quantity")
        if self.sellable_quantity > self.current_quantity:
            raise ValueError("sellable_quantity must be within current_quantity")
        require_aware(self.earliest_execution_time, "earliest_execution_time")
        require_aware(self.valid_until, "valid_until")
        if self.valid_until < self.earliest_execution_time:
            raise ValueError("valid_until must not precede earliest_execution_time")
        trading_actions = {ActionType.BUY, ActionType.ADD, ActionType.REDUCE, ActionType.EXIT}
        allowed_executable = _ALLOWED_EXECUTABLE_ACTIONS[self.desired_action]
        if self.executable_action not in allowed_executable:
            raise ValueError(
                "executable_action is not allowed for desired_action"
            )
        if self.executable_action == ActionType.BUY:
            if self.current_quantity != 0 or self.target_quantity <= 0:
                raise ValueError("BUY requires a new positive position")
        elif self.executable_action == ActionType.ADD:
            if self.current_quantity <= 0 or self.target_quantity <= self.current_quantity:
                raise ValueError("ADD requires an increased existing position")
        elif self.executable_action == ActionType.REDUCE:
            reduction = self.current_quantity - self.target_quantity
            if reduction <= 0:
                raise ValueError("REDUCE requires a lower target quantity")
            if reduction > self.sellable_quantity:
                raise ValueError("REDUCE exceeds sellable_quantity")
        elif self.executable_action == ActionType.EXIT:
            if self.target_quantity != 0 or self.current_quantity <= 0:
                raise ValueError("EXIT requires a zero target from a position")
            if self.current_quantity > self.sellable_quantity:
                raise ValueError("EXIT exceeds sellable_quantity")
        elif self.target_quantity != self.current_quantity:
            raise ValueError(
                "non-trading executable action must keep current quantity"
            )
        if (
            self.executable_action in trading_actions
            and self.candidate_status != CandidateStatus.PAPER_ACTIONABLE
        ):
            raise ValueError(
                "an executable trade requires PAPER_ACTIONABLE status"
            )
        if (
            self.desired_action in trading_actions
            and self.executable_action == ActionType.NO_ACTION
            and not self.blocked_reason.strip()
        ):
            raise ValueError("a blocked trading action requires blocked_reason")
        if (
            self.executable_action != self.desired_action
            and self.executable_action != ActionType.NO_ACTION
            and not self.blocked_reason.strip()
        ):
            raise ValueError(
                "a degraded executable action requires blocked_reason"
            )
        for name in ("reason_codes", "evidence_ids", "forecast_ids"):
            object.__setattr__(self, name, tuple(sorted(set(getattr(self, name)))))
        content = {
            "decision_id": self.decision_id,
            "instrument": self.instrument,
            "desired_action": self.desired_action,
            "executable_action": self.executable_action,
            "current_quantity": self.current_quantity,
            "sellable_quantity": self.sellable_quantity,
            "target_quantity": self.target_quantity,
            "earliest_execution_time": self.earliest_execution_time,
            "valid_until": self.valid_until,
            "candidate_status": self.candidate_status,
            "blocked_reason": self.blocked_reason,
            "reason_codes": self.reason_codes,
            "evidence_ids": self.evidence_ids,
            "forecast_ids": self.forecast_ids,
        }
        object.__setattr__(self, "action_id", deterministic_id("action", content))


@dataclass(frozen=True)
class ExecutionIntent(ContractMixin):
    strategy_id: str
    decision_id: str
    action_id: str
    account_id: str
    instrument: str
    side: ExecutionSide
    desired_quantity: int
    target_quantity: int
    limit_policy: LimitPolicy
    earliest_at: datetime
    valid_until: datetime
    execution_contract_version: str
    limit_price: Decimal | None = None
    source_system: str = "V4"
    idempotency_key: str = ""

    def __post_init__(self) -> None:
        for name in (
            "strategy_id",
            "decision_id",
            "action_id",
            "account_id",
            "instrument",
            "execution_contract_version",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name), name))
        if self.source_system != "V4":
            raise ValueError("source_system must be V4")
        object.__setattr__(self, "side", ExecutionSide(self.side))
        object.__setattr__(self, "limit_policy", LimitPolicy(self.limit_policy))
        _integer(self.desired_quantity, "desired_quantity", minimum=1)
        _integer(self.target_quantity, "target_quantity")
        require_aware(self.earliest_at, "earliest_at")
        require_aware(self.valid_until, "valid_until")
        if self.valid_until < self.earliest_at:
            raise ValueError("valid_until must not precede earliest_at")
        if self.limit_price is not None:
            converted = _decimal(self.limit_price, "limit_price")
            if converted <= 0:
                raise ValueError("limit_price must be positive")
            object.__setattr__(self, "limit_price", converted)
        key_payload = {
            "source_system": self.source_system,
            "strategy_id": self.strategy_id,
            "decision_id": self.decision_id,
            "action_id": self.action_id,
            "account_id": self.account_id,
            "instrument": self.instrument,
            "side": self.side,
            "desired_quantity": self.desired_quantity,
            "target_quantity": self.target_quantity,
            "limit_policy": self.limit_policy,
            "limit_price": self.limit_price,
            "earliest_at": self.earliest_at,
            "valid_until": self.valid_until,
            "execution_contract_version": self.execution_contract_version,
        }
        expected_key = deterministic_hash(key_payload)
        if self.idempotency_key and self.idempotency_key != expected_key:
            raise ValueError("idempotency_key does not match intent content")
        object.__setattr__(self, "idempotency_key", expected_key)


def derive_decision_id(
    decision_input: DecisionInput,
    kernel_version: str,
) -> str:
    """Derive the only valid identity for one kernel/input evaluation."""

    _require_exact_type(decision_input, DecisionInput, "decision_input")
    version = _required_text(kernel_version, "kernel_version")
    return deterministic_id(
        "decision",
        {
            "input_hash": decision_input.input_hash,
            "kernel_version": version,
        },
    )


@dataclass(frozen=True)
class DecisionBundle(ContractMixin):
    decision_id: str
    decision_input: DecisionInput
    kernel_version: str
    status: DecisionBundleStatus
    forecasts: tuple[ForecastResult, ...] = ()
    actions: tuple[DecisionAction, ...] = ()
    execution_intents: tuple[ExecutionIntent, ...] = ()
    diagnostics: Mapping[str, Any] = field(default_factory=dict)
    context_id: str = field(init=False)
    input_hash: str = field(init=False)
    result_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_exact_type(
            self.decision_input,
            DecisionInput,
            "decision_input",
        )
        forecasts_input = _require_exact_items(
            self.forecasts,
            ForecastResult,
            "forecasts",
        )
        actions_input = _require_exact_items(
            self.actions,
            DecisionAction,
            "actions",
        )
        intents_input = _require_exact_items(
            self.execution_intents,
            ExecutionIntent,
            "execution_intents",
        )
        expected_decision_id = derive_decision_id(
            self.decision_input,
            self.kernel_version,
        )
        supplied_decision_id = _required_text(
            self.decision_id,
            "decision_id",
        )
        if supplied_decision_id != expected_decision_id:
            raise ValueError("decision_id does not match decision input")
        object.__setattr__(self, "decision_id", supplied_decision_id)
        object.__setattr__(
            self,
            "kernel_version",
            _required_text(self.kernel_version, "kernel_version"),
        )
        object.__setattr__(
            self,
            "context_id",
            self.decision_input.context.context_id,
        )
        object.__setattr__(self, "input_hash", self.decision_input.input_hash)
        object.__setattr__(self, "status", DecisionBundleStatus(self.status))
        forecasts = tuple(sorted(forecasts_input, key=lambda item: item.forecast_id))
        actions = tuple(sorted(actions_input, key=lambda item: item.action_id))
        intents = tuple(sorted(intents_input, key=lambda item: item.idempotency_key))
        for items, identifier, label in (
            (forecasts, "forecast_id", "forecast"),
            (actions, "action_id", "action"),
            (intents, "idempotency_key", "execution intent"),
        ):
            identifiers = [getattr(item, identifier) for item in items]
            if len(identifiers) != len(set(identifiers)):
                raise ValueError(f"bundle contains duplicate {label} identifiers")
        context = self.decision_input.context
        decision_scope_keys = {
            (item.scope_type, item.scope_id)
            for item in self.decision_input.scopes
        }
        instrument_scopes = {
            scope_id
            for scope_type, scope_id in decision_scope_keys
            if scope_type == ScopeType.INSTRUMENT
        }
        forecast_contract_ids = set(context.forecast_contract_ids)
        for forecast in forecasts:
            if forecast.signal_at > context.knowledge_cutoff:
                raise ValueError("forecast signal exceeds knowledge_cutoff")
            if forecast.valid_until < context.decision_time:
                raise ValueError("forecast expired before decision_time")
            if (
                forecast.scope.scope_type,
                forecast.scope.scope_id,
            ) not in decision_scope_keys:
                raise ValueError("forecast scope is absent from decision input")
            if forecast.forecast_contract_id not in forecast_contract_ids:
                raise ValueError("forecast contract is absent from context")
            if (
                context.model_versions.get(forecast.model_id)
                != forecast.model_version
            ):
                raise ValueError("forecast model is absent from context")
            if context.model_available_at[forecast.model_id] > forecast.signal_at:
                raise ValueError(
                    "forecast predates model availability"
                )
            if (
                context.calibration_versions.get(forecast.calibration_id)
                != forecast.calibration_version
            ):
                raise ValueError("forecast calibration is absent from context")
            if (
                context.calibration_available_at[forecast.calibration_id]
                > forecast.signal_at
            ):
                raise ValueError(
                    "forecast predates calibration availability"
                )
        if any(item.decision_id != self.decision_id for item in actions):
            raise ValueError("action decision_id does not match bundle")
        forecast_by_id = {
            item.forecast_id: item for item in forecasts
        }
        rule_by_instrument = {
            item.instrument: item
            for item in self.decision_input.instrument_rules
        }
        position_by_instrument = {
            item.instrument: item
            for item in self.decision_input.account.positions
        }
        for action in actions:
            if action.instrument not in instrument_scopes:
                raise ValueError("action instrument is absent from decision scopes")
            if action.earliest_execution_time < context.decision_time:
                raise ValueError("action starts before decision_time")
            missing_forecasts = set(action.forecast_ids) - set(
                forecast_by_id
            )
            if missing_forecasts:
                raise ValueError(
                    "action references forecasts absent from bundle: "
                    f"{tuple(sorted(missing_forecasts))}"
                )
            referenced_forecasts = [
                forecast_by_id[item] for item in action.forecast_ids
            ]
            if any(
                action.valid_until > forecast.valid_until
                for forecast in referenced_forecasts
            ):
                raise ValueError("action outlives a referenced forecast")
            position = position_by_instrument.get(action.instrument)
            expected_current = position.total_quantity if position else 0
            expected_sellable = position.sellable_quantity if position else 0
            if (
                action.current_quantity != expected_current
                or action.sellable_quantity != expected_sellable
            ):
                raise ValueError(
                    "action position quantities do not match decision input"
                )
            rule = rule_by_instrument.get(action.instrument)
            if rule is None:
                raise ValueError("action instrument has no rule snapshot")
            if action.valid_until > rule.valid_until:
                raise ValueError(
                    "action outlives its instrument rule snapshot"
                )
            if action.executable_action in {ActionType.BUY, ActionType.ADD}:
                if not action.forecast_ids:
                    raise ValueError("BUY/ADD requires a referenced forecast")
                primary_forecasts = [
                    forecast
                    for forecast in referenced_forecasts
                    if forecast.scope.scope_type == ScopeType.INSTRUMENT
                    and forecast.scope.scope_id == action.instrument
                ]
                if not primary_forecasts:
                    raise ValueError(
                        "BUY/ADD requires a same-instrument forecast"
                    )
                if any(
                    forecast.probability_kind
                    == ProbabilityKind.HEURISTIC_PRIOR
                    for forecast in referenced_forecasts
                ):
                    raise ValueError(
                        "heuristic forecasts cannot produce BUY/ADD actions"
                    )
                if any(
                    forecast.status != CandidateStatus.PAPER_ACTIONABLE
                    or forecast.expected_return_net_pct is None
                    or forecast.cvar95_loss_pct is None
                    or forecast.probability_positive is None
                    for forecast in referenced_forecasts
                ):
                    raise ValueError(
                        "BUY/ADD requires every referenced forecast to be "
                        "a complete PAPER_ACTIONABLE forecast"
                    )
                if not rule.can_buy or rule.is_suspended or rule.limit_up_locked:
                    raise ValueError("instrument rule blocks executable buy")
                buy_quantity = (
                    action.target_quantity
                    if action.executable_action == ActionType.BUY
                    else action.target_quantity - action.current_quantity
                )
                if (
                    action.executable_action == ActionType.BUY
                    and action.target_quantity < rule.first_buy_minimum
                ):
                    raise ValueError("BUY is below first_buy_minimum")
                if buy_quantity % rule.buy_lot_size != 0:
                    raise ValueError("buy quantity is not aligned to buy_lot_size")
            elif action.executable_action in {
                ActionType.REDUCE,
                ActionType.EXIT,
            }:
                if (
                    not rule.can_sell
                    or rule.is_suspended
                    or rule.limit_down_locked
                ):
                    raise ValueError("instrument rule blocks executable sell")
                sell_quantity = (
                    action.current_quantity - action.target_quantity
                )
                if sell_quantity % rule.sell_lot_size != 0:
                    odd_remainder = (
                        action.current_quantity % rule.sell_lot_size
                    )
                    if not (
                        rule.allow_odd_lot_liquidation
                        and odd_remainder > 0
                        and sell_quantity % rule.sell_lot_size
                        == odd_remainder
                    ):
                        raise ValueError(
                            "sell quantity is not aligned to sell_lot_size"
                        )
        action_ids = {item.action_id for item in actions}
        action_by_id = {item.action_id: item for item in actions}
        intent_action_ids: set[str] = set()
        if intents and self.status != DecisionBundleStatus.PAPER_ACTIONABLE:
            raise ValueError(
                "only a PAPER_ACTIONABLE bundle may contain execution intents"
            )
        for intent in intents:
            if intent.decision_id != self.decision_id:
                raise ValueError("execution intent decision_id does not match bundle")
            if intent.action_id not in action_ids:
                raise ValueError("execution intent does not reference a bundle action")
            if intent.action_id in intent_action_ids:
                raise ValueError("an action may produce only one execution intent")
            intent_action_ids.add(intent.action_id)
            action = action_by_id[intent.action_id]
            if action.candidate_status != CandidateStatus.PAPER_ACTIONABLE:
                raise ValueError(
                    "execution intent requires PAPER_ACTIONABLE action"
                )
            if intent.account_id != self.decision_input.account.account_id:
                raise ValueError(
                    "execution intent account does not match decision input"
                )
            if intent.instrument != action.instrument:
                raise ValueError(
                    "execution intent instrument does not match action"
                )
            if (
                intent.execution_contract_version
                != context.execution_contract_version
            ):
                raise ValueError(
                    "execution intent contract version does not match context"
                )
            expected_side = {
                ActionType.BUY: ExecutionSide.BUY,
                ActionType.ADD: ExecutionSide.BUY,
                ActionType.REDUCE: ExecutionSide.SELL,
                ActionType.EXIT: ExecutionSide.SELL,
            }.get(action.executable_action)
            if expected_side is None:
                raise ValueError(
                    "execution intent requires an executable trading action"
                )
            if intent.side != expected_side:
                raise ValueError("execution intent side does not match action")
            expected_quantity = abs(
                action.target_quantity - action.current_quantity
            )
            if intent.desired_quantity != expected_quantity:
                raise ValueError(
                    "execution intent quantity does not match action target"
                )
            if intent.target_quantity != action.target_quantity:
                raise ValueError(
                    "execution intent target does not match action target"
                )
            if intent.earliest_at < action.earliest_execution_time:
                raise ValueError("execution intent starts before its action")
            if intent.valid_until > action.valid_until:
                raise ValueError("execution intent outlives its action")
            rule = rule_by_instrument[intent.instrument]
            if (
                intent.limit_price is not None
                and rule.upper_limit is not None
                and intent.limit_price > rule.upper_limit
            ):
                raise ValueError("execution intent exceeds upper price limit")
            if (
                intent.limit_price is not None
                and rule.lower_limit is not None
                and intent.limit_price < rule.lower_limit
            ):
                raise ValueError("execution intent is below lower price limit")
            if (
                intent.limit_policy
                in {LimitPolicy.FIXED_LIMIT, LimitPolicy.PROTECTIVE_LIMIT}
                and intent.limit_price is None
            ):
                raise ValueError("limit policy requires limit_price")
            if (
                intent.limit_price is not None
                and intent.limit_price % rule.tick_size != 0
            ):
                raise ValueError("execution intent price is not tick aligned")
        executable_action_ids = {
            item.action_id
            for item in actions
            if item.executable_action
            in {ActionType.BUY, ActionType.ADD, ActionType.REDUCE, ActionType.EXIT}
        }
        if executable_action_ids != intent_action_ids:
            raise ValueError(
                "every executable trading action requires exactly one intent"
            )
        if intents:
            used_capabilities = {
                item.capability_name
                for item in self.decision_input.feature_vectors
            }
            blocked_capabilities = {
                name
                for name in used_capabilities
                if not context.capability_statuses[name].actionable
            }
            failed_features = {
                item.feature_set_version
                for item in self.decision_input.feature_vectors
                if item.quality_status != QualityStatus.PASS
            }
            failed_rules = {
                item.instrument
                for item in self.decision_input.instrument_rules
                if item.quality_status != QualityStatus.PASS
            }
            if blocked_capabilities or failed_features or failed_rules:
                raise ValueError(
                    "paper execution requires actionable, PASS inputs"
                )
        assert_clean_payload(self.diagnostics, path="diagnostics")
        object.__setattr__(self, "forecasts", forecasts)
        object.__setattr__(self, "actions", actions)
        object.__setattr__(self, "execution_intents", intents)
        object.__setattr__(self, "diagnostics", freeze(self.diagnostics))
        content = {
            "decision_id": self.decision_id,
            "context_id": self.context_id,
            "input_hash": self.input_hash,
            "kernel_version": self.kernel_version,
            "status": self.status,
            "forecasts": self.forecasts,
            "actions": self.actions,
            "execution_intents": self.execution_intents,
            "diagnostics": self.diagnostics,
        }
        object.__setattr__(self, "result_hash", deterministic_hash(content))


@dataclass(frozen=True)
class ExecutionReceipt(ContractMixin):
    idempotency_key: str
    status: ExecutionReceiptStatus
    recorded_at: datetime
    accepted_quantity: int = 0
    order_id: str = ""
    blocked_reason: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "idempotency_key",
            _required_text(self.idempotency_key, "idempotency_key"),
        )
        object.__setattr__(self, "status", ExecutionReceiptStatus(self.status))
        require_aware(self.recorded_at, "recorded_at")
        for name in ("order_id", "blocked_reason"):
            value = getattr(self, name)
            if not isinstance(value, str):
                raise TypeError(f"{name} must be a string")
        _integer(self.accepted_quantity, "accepted_quantity")
        if self.status == ExecutionReceiptStatus.BLOCKED:
            if not self.blocked_reason.strip():
                raise ValueError("blocked receipt requires blocked_reason")
            if self.accepted_quantity != 0 or self.order_id:
                raise ValueError(
                    "blocked receipt cannot accept quantity or carry order_id"
                )
        elif self.accepted_quantity <= 0 or not self.order_id.strip():
            raise ValueError(
                "accepted/duplicate receipt requires quantity and order_id"
            )


@dataclass(frozen=True)
class DecisionCommitReceipt(ContractMixin):
    decision_id: str
    result_hash: str
    status: CommitStatus
    committed_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "decision_id", _required_text(self.decision_id, "decision_id"))
        object.__setattr__(self, "result_hash", _sha256(self.result_hash, "result_hash"))
        object.__setattr__(self, "status", CommitStatus(self.status))
        require_aware(self.committed_at, "committed_at")


@dataclass(frozen=True)
class CommittedExecutionIntent(ContractMixin):
    """Outbox message proving an intent belongs to a committed decision."""

    bundle: DecisionBundle
    intent: ExecutionIntent
    commit_receipt: DecisionCommitReceipt
    outbox_id: str

    def __post_init__(self) -> None:
        _require_exact_type(self.bundle, DecisionBundle, "bundle")
        _require_exact_type(self.intent, ExecutionIntent, "intent")
        _require_exact_type(
            self.commit_receipt,
            DecisionCommitReceipt,
            "commit_receipt",
        )
        if not isinstance(self.outbox_id, str):
            raise TypeError("outbox_id must be a string")
        if self.intent.decision_id != self.commit_receipt.decision_id:
            raise ValueError("intent and commit receipt decision_id differ")
        if self.bundle.decision_id != self.commit_receipt.decision_id:
            raise ValueError("bundle and commit receipt decision_id differ")
        if self.bundle.result_hash != self.commit_receipt.result_hash:
            raise ValueError("commit receipt result_hash does not match bundle")
        if self.commit_receipt.status not in {
            CommitStatus.COMMITTED,
            CommitStatus.ALREADY_COMMITTED,
        }:
            raise ValueError("execution intent requires a committed decision")
        matching_intents = [
            item
            for item in self.bundle.execution_intents
            if item.idempotency_key == self.intent.idempotency_key
        ]
        if len(matching_intents) != 1 or matching_intents[0] != self.intent:
            raise ValueError("intent is not a member of committed bundle")
        expected_outbox_id = deterministic_id(
            "outbox",
            {
                "intent_idempotency_key": self.intent.idempotency_key,
                "result_hash": self.bundle.result_hash,
            },
        )
        if self.outbox_id and self.outbox_id != expected_outbox_id:
            raise ValueError("outbox_id does not match committed intent")
        object.__setattr__(self, "outbox_id", expected_outbox_id)


@dataclass(frozen=True)
class ModelArtifactRef(ContractMixin):
    model_id: str
    model_version: str
    artifact_hash: str
    training_cutoff: datetime
    feature_spec_version: str
    forecast_contract_id: str
    calibration_artifact_hash: str
    promoted_at: datetime
    status: str

    def __post_init__(self) -> None:
        for name in (
            "model_id",
            "feature_spec_version",
            "forecast_contract_id",
            "status",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name), name))
        object.__setattr__(
            self,
            "model_version",
            _v4_artifact_version(self.model_version, "model_version"),
        )
        object.__setattr__(
            self,
            "artifact_hash",
            _sha256(self.artifact_hash, "artifact_hash"),
        )
        object.__setattr__(
            self,
            "calibration_artifact_hash",
            _sha256(
                self.calibration_artifact_hash,
                "calibration_artifact_hash",
            ),
        )
        require_aware(self.training_cutoff, "training_cutoff")
        require_aware(self.promoted_at, "promoted_at")
        if self.training_cutoff > self.promoted_at:
            raise ValueError("training_cutoff cannot follow promoted_at")

    def is_available_as_of(self, as_of: datetime) -> bool:
        require_aware(as_of, "as_of")
        return self.status == "ACTIVE" and self.promoted_at <= as_of


@dataclass(frozen=True)
class CalibrationArtifactRef(ContractMixin):
    """Point-in-time calibration artifact bound to a V4 model contract."""

    calibration_id: str
    calibration_version: str
    artifact_hash: str
    training_cutoff: datetime
    model_id: str
    model_version: str
    forecast_contract_id: str
    promoted_at: datetime
    status: str

    def __post_init__(self) -> None:
        for name in (
            "calibration_id",
            "model_id",
            "forecast_contract_id",
            "status",
        ):
            object.__setattr__(
                self,
                name,
                _required_text(getattr(self, name), name),
            )
        for name in ("calibration_version", "model_version"):
            object.__setattr__(
                self,
                name,
                _v4_artifact_version(getattr(self, name), name),
            )
        object.__setattr__(
            self,
            "artifact_hash",
            _sha256(self.artifact_hash, "artifact_hash"),
        )
        require_aware(self.training_cutoff, "training_cutoff")
        require_aware(self.promoted_at, "promoted_at")
        if self.training_cutoff > self.promoted_at:
            raise ValueError("training_cutoff cannot follow promoted_at")

    def is_available_as_of(self, as_of: datetime) -> bool:
        require_aware(as_of, "as_of")
        return self.status == "ACTIVE" and self.promoted_at <= as_of
