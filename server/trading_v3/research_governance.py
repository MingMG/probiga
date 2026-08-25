from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from typing import Any, Iterable, Mapping


SCHEMA_VERSION = "probiga.trading-v3-research-preregistration.v1"
RESULT_SCHEMA_VERSION = "probiga.trading-v3-governed-result.v1"
EXPLORATORY = "exploratory"
CONFIRMATORY = "confirmatory"
RESEARCH_CLASSIFICATIONS = frozenset({EXPLORATORY, CONFIRMATORY})
CALLER_DECLARED_UNVERIFIED = "CALLER_DECLARED_UNVERIFIED"
PROSPECTIVE = "PROSPECTIVE_BEFORE_FIRST_HOLDOUT"
RETROSPECTIVE = "RETROSPECTIVE_AFTER_HOLDOUT_OPENED"


class ResearchGovernanceError(ValueError):
    """Raised when research evidence violates its frozen contract."""


def _required_text(value: Any, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise ResearchGovernanceError(f"{field} must not be empty")
    return result


def _normalise_hash(value: Any, field: str) -> str:
    result = _required_text(value, field).lower()
    if len(result) != 64 or any(character not in "0123456789abcdef" for character in result):
        raise ResearchGovernanceError(
            f"{field} must be a 64-character SHA-256 hex digest"
        )
    return result


def _normalise_date(value: date | str, field: str) -> str:
    if isinstance(value, datetime):
        raise ResearchGovernanceError(f"{field} must be a date, not a datetime")
    try:
        parsed = value if isinstance(value, date) else date.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise ResearchGovernanceError(f"{field} must be an ISO-8601 date") from exc
    return parsed.isoformat()


def _normalise_datetime(value: datetime | str, field: str) -> str:
    try:
        if isinstance(value, datetime):
            parsed = value
        else:
            raw = str(value).strip()
            parsed = datetime.fromisoformat(raw[:-1] + "+00:00" if raw.endswith("Z") else raw)
    except (TypeError, ValueError) as exc:
        raise ResearchGovernanceError(
            f"{field} must be an ISO-8601 datetime with a timezone"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ResearchGovernanceError(f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def _normalise_classification(value: Any, field: str) -> str:
    result = _required_text(value, field).lower()
    if result not in RESEARCH_CLASSIFICATIONS:
        allowed = ", ".join(sorted(RESEARCH_CLASSIFICATIONS))
        raise ResearchGovernanceError(f"{field} must be one of: {allowed}")
    return result


def _canonical_json(value: Any, field: str = "value") -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise ResearchGovernanceError(f"{field} must be strictly JSON serializable") from exc


def _strict_json_copy(value: Any, field: str = "value") -> Any:
    return json.loads(_canonical_json(value, field))


@dataclass(frozen=True, slots=True)
class OuterFold:
    """A frozen walk-forward fold whose validation interval is the holdout."""

    name: str
    training_start: str
    training_end: str
    validation_start: str
    validation_end: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _required_text(self.name, "outer_fold.name"))
        for field in (
            "training_start",
            "training_end",
            "validation_start",
            "validation_end",
        ):
            object.__setattr__(
                self,
                field,
                _normalise_date(getattr(self, field), f"outer_fold.{field}"),
            )
        if self.training_start > self.training_end:
            raise ResearchGovernanceError("outer fold training_start is after training_end")
        if self.training_end >= self.validation_start:
            raise ResearchGovernanceError(
                "outer fold training period must end before its validation holdout"
            )
        if self.validation_start > self.validation_end:
            raise ResearchGovernanceError(
                "outer fold validation_start is after validation_end"
            )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> OuterFold:
        """Accept both validation_* and equivalent holdout_* field names."""

        return cls(
            name=value.get("name") or value.get("fold_id"),
            training_start=value.get("training_start"),
            training_end=value.get("training_end"),
            validation_start=value.get("validation_start")
            or value.get("holdout_start"),
            validation_end=value.get("validation_end") or value.get("holdout_end"),
        )

    @property
    def holdout_key(self) -> str:
        return f"{self.validation_start}/{self.validation_end}"

    def as_dict(self) -> dict[str, str]:
        return {
            "name": self.name,
            "training_start": self.training_start,
            "training_end": self.training_end,
            "validation_start": self.validation_start,
            "validation_end": self.validation_end,
        }


@dataclass(frozen=True, slots=True)
class CandidatePreregistration:
    """Immutable candidate contract frozen before a holdout is evaluated."""

    candidate_id: str
    family: str
    feature_protocol_hash: str
    calibration_protocol_hash: str
    portfolio_protocol_hash: str
    outer_folds: tuple[OuterFold, ...]
    data_cutoff: str
    created_at: str
    research_classification: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "candidate_id", _required_text(self.candidate_id, "candidate_id")
        )
        object.__setattr__(self, "family", _required_text(self.family, "family"))
        for field in (
            "feature_protocol_hash",
            "calibration_protocol_hash",
            "portfolio_protocol_hash",
        ):
            object.__setattr__(
                self,
                field,
                _normalise_hash(getattr(self, field), field),
            )
        folds = tuple(
            fold if isinstance(fold, OuterFold) else OuterFold.from_mapping(fold)
            for fold in self.outer_folds
        )
        if not folds:
            raise ResearchGovernanceError("outer_folds must not be empty")
        holdout_keys = [fold.holdout_key for fold in folds]
        if len(holdout_keys) != len(set(holdout_keys)):
            raise ResearchGovernanceError(
                "a preregistration cannot consume the same holdout more than once"
            )
        object.__setattr__(self, "outer_folds", folds)
        object.__setattr__(
            self, "data_cutoff", _normalise_date(self.data_cutoff, "data_cutoff")
        )
        if any(fold.validation_end > self.data_cutoff for fold in folds):
            raise ResearchGovernanceError(
                "outer fold validation_end cannot be after data_cutoff"
            )
        object.__setattr__(
            self, "created_at", _normalise_datetime(self.created_at, "created_at")
        )
        object.__setattr__(
            self,
            "research_classification",
            _normalise_classification(
                self.research_classification,
                "research_classification",
            ),
        )

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> CandidatePreregistration:
        return cls(
            candidate_id=value.get("candidate_id"),
            family=value.get("family"),
            feature_protocol_hash=value.get("feature_protocol_hash"),
            calibration_protocol_hash=value.get("calibration_protocol_hash"),
            portfolio_protocol_hash=value.get("portfolio_protocol_hash"),
            outer_folds=tuple(value.get("outer_folds") or ()),
            data_cutoff=value.get("data_cutoff"),
            created_at=value.get("created_at"),
            research_classification=value.get("research_classification"),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "candidate_id": self.candidate_id,
            "family": self.family,
            "feature_protocol_hash": self.feature_protocol_hash,
            "calibration_protocol_hash": self.calibration_protocol_hash,
            "portfolio_protocol_hash": self.portfolio_protocol_hash,
            "outer_folds": [fold.as_dict() for fold in self.outer_folds],
            "data_cutoff": self.data_cutoff,
            "created_at": self.created_at,
            "research_classification": self.research_classification,
        }

    @property
    def contract_hash(self) -> str:
        return hashlib.sha256(_canonical_json(self.as_dict()).encode("utf-8")).hexdigest()

    @property
    def timing_status(self) -> str:
        """Classify timing without trusting the caller-declared timestamp."""

        declared = datetime.fromisoformat(
            self.created_at.replace("Z", "+00:00")
        ).astimezone(timezone.utc)
        first_holdout = min(fold.validation_start for fold in self.outer_folds)
        holdout_open = datetime.combine(
            date.fromisoformat(first_holdout),
            time.min,
            tzinfo=timezone.utc,
        )
        return PROSPECTIVE if declared < holdout_open else RETROSPECTIVE


@dataclass(frozen=True, slots=True)
class HoldoutConsumption:
    """Familywise usage of one validation interval."""

    validation_start: str
    validation_end: str
    candidate_ids: tuple[str, ...]
    families: tuple[str, ...]
    contract_hashes: tuple[str, ...]

    @property
    def familywise_trial_count(self) -> int:
        return len(self.contract_hashes)

    @property
    def is_reused(self) -> bool:
        return self.familywise_trial_count > 1

    def as_dict(self) -> dict[str, Any]:
        return {
            "validation_start": self.validation_start,
            "validation_end": self.validation_end,
            "holdout_key": f"{self.validation_start}/{self.validation_end}",
            "familywise_trial_count": self.familywise_trial_count,
            "is_reused": self.is_reused,
            "candidate_ids": list(self.candidate_ids),
            "families": list(self.families),
            "contract_hashes": list(self.contract_hashes),
        }


def _unique_registrations(
    registrations: Iterable[CandidatePreregistration],
) -> tuple[CandidatePreregistration, ...]:
    by_hash: dict[str, CandidatePreregistration] = {}
    for registration in registrations:
        if not isinstance(registration, CandidatePreregistration):
            raise ResearchGovernanceError(
                "registrations must contain CandidatePreregistration objects"
            )
        by_hash.setdefault(registration.contract_hash, registration)
    return tuple(by_hash[key] for key in sorted(by_hash))


def holdout_consumption_report(
    registrations: Iterable[CandidatePreregistration],
) -> tuple[HoldoutConsumption, ...]:
    """Return deterministic per-holdout trial counts across frozen contracts."""

    consumers: dict[str, list[tuple[str, CandidatePreregistration]]] = {}
    for registration in _unique_registrations(registrations):
        for fold in registration.outer_folds:
            consumers.setdefault(fold.holdout_key, []).append(
                (registration.contract_hash, registration)
            )
    report: list[HoldoutConsumption] = []
    for holdout_key, entries in sorted(consumers.items()):
        validation_start, validation_end = holdout_key.split("/", maxsplit=1)
        ordered = sorted(entries, key=lambda item: item[0])
        report.append(
            HoldoutConsumption(
                validation_start=validation_start,
                validation_end=validation_end,
                candidate_ids=tuple(item.candidate_id for _, item in ordered),
                families=tuple(item.family for _, item in ordered),
                contract_hashes=tuple(contract_hash for contract_hash, _ in ordered),
            )
        )
    return tuple(report)


def detect_repeated_holdout_consumption(
    registrations: Iterable[CandidatePreregistration],
) -> tuple[HoldoutConsumption, ...]:
    """Identify validation intervals consumed by more than one unique contract."""

    return tuple(
        item for item in holdout_consumption_report(registrations) if item.is_reused
    )


def familywise_trial_counts(
    registrations: Iterable[CandidatePreregistration],
) -> dict[str, Any]:
    """Build a JSON-safe trial ledger without double-counting duplicate records."""

    unique = _unique_registrations(registrations)
    by_family: dict[str, int] = {}
    by_classification = {EXPLORATORY: 0, CONFIRMATORY: 0}
    for registration in unique:
        by_family[registration.family] = by_family.get(registration.family, 0) + 1
        by_classification[registration.research_classification] += 1
    holdouts = holdout_consumption_report(unique)
    return {
        "familywise_trial_count": len(unique),
        "by_family": dict(sorted(by_family.items())),
        "by_research_classification": by_classification,
        "by_holdout": {
            item.as_dict()["holdout_key"]: item.familywise_trial_count
            for item in holdouts
        },
        "reused_holdout_count": sum(item.is_reused for item in holdouts),
    }


def familywise_registration_manifest(
    registrations: Iterable[CandidatePreregistration],
    *,
    prior_registration_contract_hashes: Iterable[str] = (),
) -> dict[str, Any]:
    """Bind the entire candidate family; counts alone are never authority.

    Historical code accepted a caller-supplied prior trial count.  That count
    could be reduced after seeing results.  This contract instead requires
    every prior/current trial to be represented by its immutable contract
    hash, while duplicate candidate identifiers with different contracts are
    rejected.
    """

    unique = _unique_registrations(registrations)
    if not unique:
        raise ResearchGovernanceError(
            "familywise registration manifest must not be empty"
        )
    candidate_contracts: dict[str, str] = {}
    for registration in unique:
        previous = candidate_contracts.setdefault(
            registration.candidate_id,
            registration.contract_hash,
        )
        if previous != registration.contract_hash:
            raise ResearchGovernanceError(
                "one candidate_id cannot have multiple contracts in a family"
            )
    prior_hashes = tuple(sorted({
        _normalise_hash(value, "prior_registration_contract_hash")
        for value in prior_registration_contract_hashes
    }))
    current_hashes = tuple(sorted(item.contract_hash for item in unique))
    overlap = sorted(set(prior_hashes).intersection(current_hashes))
    if overlap:
        raise ResearchGovernanceError(
            "prior and current family registrations overlap"
        )
    current_rows = [
        {
            "candidate_id": item.candidate_id,
            "family": item.family,
            "contract_hash": item.contract_hash,
            "timing_status": item.timing_status,
        }
        for item in sorted(unique, key=lambda value: value.contract_hash)
    ]
    holdout_report = [
        item.as_dict() for item in holdout_consumption_report(unique)
    ]
    payload = {
        "schema_version": (
            "probiga.trading-v3-familywise-registration-manifest.v1"
        ),
        "current_registrations": current_rows,
        "current_candidate_ids": sorted(candidate_contracts),
        "current_trial_count": len(current_hashes),
        "prior_registration_contract_hashes": list(prior_hashes),
        "prior_trial_count": len(prior_hashes),
        "total_familywise_trial_count": len(prior_hashes) + len(current_hashes),
        "holdout_consumption": holdout_report,
        "caller_reported_count_accepted": False,
    }
    return {
        **payload,
        "manifest_hash": hashlib.sha256(
            _canonical_json(payload).encode("utf-8")
        ).hexdigest(),
    }


def label_research_result(
    preregistration: CandidatePreregistration,
    result: Mapping[str, Any],
    *,
    evaluated_at: datetime | str,
    claimed_classification: str | None = None,
    family_registrations: Iterable[CandidatePreregistration] | None = None,
    prior_registration_contract_hashes: Iterable[str] = (),
) -> dict[str, Any]:
    """Bind a result to its contract and enforce its maximum evidence class."""

    if not isinstance(preregistration, CandidatePreregistration):
        raise ResearchGovernanceError(
            "preregistration must be a CandidatePreregistration"
        )
    evidence_classification = _normalise_classification(
        claimed_classification or preregistration.research_classification,
        "claimed_classification",
    )
    if (
        evidence_classification == CONFIRMATORY
        and preregistration.research_classification != CONFIRMATORY
    ):
        raise ResearchGovernanceError(
            "exploratory preregistration cannot produce a confirmatory result"
        )
    # CandidatePreregistration is a value supplied by the research caller;
    # its created_at field is not a trusted clock receipt.  Until an
    # append-only server registry supplies such a receipt, it can describe
    # exploratory work only and can never authorize a confirmatory claim.
    if evidence_classification == CONFIRMATORY:
        raise ResearchGovernanceError(
            "confirmatory result requires a persisted authoritative "
            "preregistration receipt; caller declaration is unverified"
        )
    normalised_evaluated_at = _normalise_datetime(evaluated_at, "evaluated_at")
    if normalised_evaluated_at < preregistration.created_at:
        raise ResearchGovernanceError("evaluated_at cannot precede created_at")
    payload = _strict_json_copy(result, "result")
    family_scope_complete = family_registrations is not None
    family_members = tuple(
        family_registrations
        if family_registrations is not None
        else (preregistration,)
    )
    manifest = familywise_registration_manifest(
        family_members,
        prior_registration_contract_hashes=(
            prior_registration_contract_hashes
        ),
    )
    if preregistration.contract_hash not in {
        item["contract_hash"]
        for item in manifest["current_registrations"]
    }:
        raise ResearchGovernanceError(
            "result preregistration is missing from the family manifest"
        )
    return {
        "schema_version": RESULT_SCHEMA_VERSION,
        "candidate_id": preregistration.candidate_id,
        "family": preregistration.family,
        "preregistration_contract_hash": preregistration.contract_hash,
        "preregistered_classification": preregistration.research_classification,
        "evidence_classification": evidence_classification,
        "preregistration_timing_status": preregistration.timing_status,
        "registration_authority": CALLER_DECLARED_UNVERIFIED,
        "authoritative_preregistration_receipt_verified": False,
        "family_manifest_scope_complete": family_scope_complete,
        "familywise_registration_manifest": manifest,
        "confirmatory_claim_allowed": False,
        "activation_eligible": False,
        "evaluated_at": normalised_evaluated_at,
        "consumed_holdouts": [
            {
                "validation_start": fold.validation_start,
                "validation_end": fold.validation_end,
            }
            for fold in preregistration.outer_folds
        ],
        "result": payload,
    }


def assert_result_governance(
    result_envelope: Mapping[str, Any],
    registrations: Iterable[CandidatePreregistration],
    *,
    prior_registration_contract_hashes: Iterable[str] = (),
) -> None:
    """Reject an unbound, relabelled, or otherwise forged result envelope."""

    _canonical_json(result_envelope, "result_envelope")
    registration_by_hash = {
        item.contract_hash: item for item in _unique_registrations(registrations)
    }
    contract_hash = str(result_envelope.get("preregistration_contract_hash") or "")
    registration = registration_by_hash.get(contract_hash)
    if registration is None:
        raise ResearchGovernanceError("result references an unknown preregistration")
    expected = {
        "schema_version": RESULT_SCHEMA_VERSION,
        "candidate_id": registration.candidate_id,
        "family": registration.family,
        "preregistered_classification": registration.research_classification,
        "preregistration_timing_status": registration.timing_status,
        "registration_authority": CALLER_DECLARED_UNVERIFIED,
        "authoritative_preregistration_receipt_verified": False,
        "family_manifest_scope_complete": True,
        "confirmatory_claim_allowed": False,
        "activation_eligible": False,
    }
    for field, value in expected.items():
        if result_envelope.get(field) != value:
            raise ResearchGovernanceError(f"result {field} does not match its contract")
    evidence_classification = _normalise_classification(
        result_envelope.get("evidence_classification"),
        "evidence_classification",
    )
    if evidence_classification == CONFIRMATORY:
        raise ResearchGovernanceError(
            "caller-declared result cannot be relabelled as confirmatory"
        )
    manifest = familywise_registration_manifest(
        registration_by_hash.values(),
        prior_registration_contract_hashes=(
            prior_registration_contract_hashes
        ),
    )
    if result_envelope.get("familywise_registration_manifest") != manifest:
        raise ResearchGovernanceError(
            "result familywise manifest is incomplete or has shrunk"
        )
    evaluated_at = _normalise_datetime(
        result_envelope.get("evaluated_at"),
        "evaluated_at",
    )
    if result_envelope.get("evaluated_at") != evaluated_at:
        raise ResearchGovernanceError("result evaluated_at is not canonical UTC")
    if evaluated_at < registration.created_at:
        raise ResearchGovernanceError("evaluated_at cannot precede created_at")
    if "result" not in result_envelope:
        raise ResearchGovernanceError("result payload is missing")
    _canonical_json(result_envelope["result"], "result")
    expected_holdouts = [
        {
            "validation_start": fold.validation_start,
            "validation_end": fold.validation_end,
        }
        for fold in registration.outer_folds
    ]
    if result_envelope.get("consumed_holdouts") != expected_holdouts:
        raise ResearchGovernanceError("result holdouts do not match its contract")


def is_strictly_json_serializable(value: Any) -> bool:
    """Return whether a value is portable JSON (excluding NaN and infinity)."""

    try:
        _canonical_json(value)
    except ResearchGovernanceError:
        return False
    return not _contains_non_finite_number(value)


def _contains_non_finite_number(value: Any) -> bool:
    if isinstance(value, float):
        return not math.isfinite(value)
    if isinstance(value, Mapping):
        return any(
            _contains_non_finite_number(key) or _contains_non_finite_number(item)
            for key, item in value.items()
        )
    if isinstance(value, (list, tuple)):
        return any(_contains_non_finite_number(item) for item in value)
    return False
