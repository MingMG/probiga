"""Deterministic prefix/cutoff probes with no ambient I/O."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime

from ..domain import AsOfDataset, FeatureVector, deterministic_hash
from ..domain.hashes import ContractMixin, require_aware


FeatureBuilder = Callable[[AsOfDataset], tuple[FeatureVector, ...]]


@dataclass(frozen=True)
class PrefixInvarianceResult(ContractMixin):
    dataset_name: str
    cutoff: datetime
    baseline_manifest_hash: str
    extended_manifest_hash: str
    baseline_output_hash: str
    extended_output_hash: str
    prefix_invariant: bool
    cutoff_safe: bool
    reason_codes: tuple[str, ...]
    result_hash: str = field(init=False)

    def __post_init__(self) -> None:
        require_aware(self.cutoff, "cutoff")
        if not self.dataset_name.strip():
            raise ValueError("dataset_name must not be empty")
        for name in (
            "baseline_manifest_hash",
            "extended_manifest_hash",
            "baseline_output_hash",
            "extended_output_hash",
        ):
            value = getattr(self, name)
            if len(value) != 64 or any(ch not in "0123456789abcdef" for ch in value):
                raise ValueError(f"{name} must be SHA-256")
        reasons = tuple(sorted({item.strip() for item in self.reason_codes if item.strip()}))
        if (not self.prefix_invariant or not self.cutoff_safe) and not reasons:
            raise ValueError("failed PIT certification requires reason_codes")
        object.__setattr__(self, "reason_codes", reasons)
        object.__setattr__(
            self,
            "result_hash",
            deterministic_hash(
                {
                    "dataset_name": self.dataset_name,
                    "cutoff": self.cutoff,
                    "baseline_manifest_hash": self.baseline_manifest_hash,
                    "extended_manifest_hash": self.extended_manifest_hash,
                    "baseline_output_hash": self.baseline_output_hash,
                    "extended_output_hash": self.extended_output_hash,
                    "prefix_invariant": self.prefix_invariant,
                    "cutoff_safe": self.cutoff_safe,
                    "reason_codes": self.reason_codes,
                }
            ),
        )

    @property
    def passed(self) -> bool:
        return self.prefix_invariant and self.cutoff_safe


def dataset_prefix(dataset: AsOfDataset, cutoff: datetime) -> AsOfDataset:
    if type(dataset) is not AsOfDataset:
        raise TypeError("dataset must be exactly AsOfDataset")
    require_aware(cutoff, "cutoff")
    if cutoff > dataset.as_of:
        raise ValueError("cutoff exceeds dataset as_of")
    return AsOfDataset(
        dataset_name=dataset.dataset_name,
        as_of=cutoff,
        records=tuple(
            record for record in dataset.records if record.knowledge_time <= cutoff
        ),
        quality_status=dataset.quality_status,
    )


def _build(builder: FeatureBuilder, dataset: AsOfDataset) -> tuple[FeatureVector, ...]:
    vectors = tuple(builder(dataset))
    if any(type(vector) is not FeatureVector for vector in vectors):
        raise TypeError("feature builder must return exact FeatureVector values")
    keys = tuple(
        sorted(
            (
                vector.scope.scope_type.value,
                vector.scope.scope_id,
                vector.feature_set_version,
            )
            for vector in vectors
        )
    )
    if len(keys) != len(set(keys)):
        raise ValueError("feature builder emitted duplicate feature identities")
    return tuple(sorted(vectors, key=lambda vector: vector.feature_hash))


def certify_prefix_invariance(
    baseline: AsOfDataset,
    extended: AsOfDataset,
    *,
    cutoff: datetime,
    builder: FeatureBuilder,
) -> PrefixInvarianceResult:
    """Compare the same knowledge prefix before and after future appends.

    A later correction carrying a backdated ``knowledge_time`` changes the
    selected prefix and therefore fails.  Callers must never turn that failure
    into an empty or neutral feature set.
    """

    if type(baseline) is not AsOfDataset or type(extended) is not AsOfDataset:
        raise TypeError("baseline and extended must be exact AsOfDataset values")
    if baseline.dataset_name != extended.dataset_name:
        raise ValueError("datasets must have the same name")
    if baseline.as_of < cutoff or extended.as_of < cutoff:
        raise ValueError("both datasets must cover the certification cutoff")
    if not callable(builder):
        raise TypeError("builder must be callable")

    baseline_prefix = dataset_prefix(baseline, cutoff)
    extended_prefix = dataset_prefix(extended, cutoff)
    baseline_vectors = _build(builder, baseline_prefix)
    extended_vectors = _build(builder, extended_prefix)
    baseline_output_hash = deterministic_hash(
        tuple(vector.feature_hash for vector in baseline_vectors)
    )
    extended_output_hash = deterministic_hash(
        tuple(vector.feature_hash for vector in extended_vectors)
    )
    cutoff_safe = all(
        vector.knowledge_time <= cutoff
        for vector in (*baseline_vectors, *extended_vectors)
    )
    prefix_invariant = bool(
        baseline_prefix.manifest_hash == extended_prefix.manifest_hash
        and baseline_output_hash == extended_output_hash
    )
    reasons: list[str] = []
    if not prefix_invariant:
        reasons.append("PREFIX_CHANGED_AFTER_FUTURE_APPEND")
    if not cutoff_safe:
        reasons.append("FEATURE_KNOWLEDGE_EXCEEDS_CUTOFF")
    return PrefixInvarianceResult(
        dataset_name=baseline.dataset_name,
        cutoff=cutoff,
        baseline_manifest_hash=baseline_prefix.manifest_hash,
        extended_manifest_hash=extended_prefix.manifest_hash,
        baseline_output_hash=baseline_output_hash,
        extended_output_hash=extended_output_hash,
        prefix_invariant=prefix_invariant,
        cutoff_safe=cutoff_safe,
        reason_codes=tuple(reasons),
    )
