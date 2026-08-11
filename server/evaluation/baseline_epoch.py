from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Any, Iterable, Mapping


BASELINE_SCHEMA_VERSION = "probiga.trading-v4-baseline-epoch.v1"
_HEX_DIGITS = frozenset("0123456789abcdef")
_HASH_FIELDS = (
    "code_hash",
    "config_hash",
    "model_hash",
    "calibration_hash",
    "feature_schema_hash",
    "raw_data_manifest_hash",
)


class BaselineEpochError(ValueError):
    """Raised when a supposedly frozen comparison epoch is incomplete."""


def _required_text(value: Any, field_name: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise BaselineEpochError(f"{field_name} must not be empty")
    return result


def _sha256(value: Any, field_name: str) -> str:
    result = _required_text(value, field_name).lower()
    if len(result) != 64 or any(char not in _HEX_DIGITS for char in result):
        raise BaselineEpochError(
            f"{field_name} must be a 64-character SHA-256 hex digest"
        )
    return result


def _utc_datetime(value: datetime | str) -> str:
    try:
        parsed = (
            value
            if isinstance(value, datetime)
            else datetime.fromisoformat(
                str(value).replace("Z", "+00:00")
            )
        )
    except (TypeError, ValueError) as exc:
        raise BaselineEpochError(
            "created_at must be an ISO-8601 datetime"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise BaselineEpochError("created_at must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat(
        timespec="seconds"
    ).replace("+00:00", "Z")


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise BaselineEpochError(
            "baseline payload must be strictly JSON serializable"
        ) from exc


def _freeze_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType(
            {
                str(key): _freeze_json(child)
                for key, child in sorted(
                    value.items(),
                    key=lambda item: str(item[0]),
                )
            }
        )
    if isinstance(value, list):
        return tuple(_freeze_json(child) for child in value)
    return value


def _thaw_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _thaw_json(child)
            for key, child in value.items()
        }
    if isinstance(value, tuple):
        return [_thaw_json(child) for child in value]
    return value


@dataclass(frozen=True, slots=True)
class BaselineEpoch:
    """Immutable evidence contract for a V3/V4 comparison epoch.

    Every material strategy or execution change creates a new instance.  The
    cumulative search count is deliberately part of the contract so a new
    namespace cannot silently reset prior model searches.
    """

    baseline_epoch_id: str
    source_system: str
    created_at: str
    code_hash: str
    config_hash: str
    model_hash: str
    calibration_hash: str
    feature_schema_hash: str
    universe_version: str
    forecast_contract_id: str
    exit_policy_id: str
    portfolio_policy_id: str
    execution_policy_id: str
    fee_schedule_version: str
    raw_data_manifest_hash: str
    decision_clocks: tuple[str, ...]
    cumulative_search_count: int
    evidence_status: str
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for field_name in (
            "baseline_epoch_id",
            "source_system",
            "universe_version",
            "forecast_contract_id",
            "exit_policy_id",
            "portfolio_policy_id",
            "execution_policy_id",
            "fee_schedule_version",
            "evidence_status",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field_name),
            )
        object.__setattr__(self, "created_at", _utc_datetime(self.created_at))
        for field_name in _HASH_FIELDS:
            object.__setattr__(
                self,
                field_name,
                _sha256(getattr(self, field_name), field_name),
            )
        clocks = tuple(
            _required_text(clock, "decision_clocks item")
            for clock in self.decision_clocks
        )
        if not clocks or len(clocks) != len(set(clocks)):
            raise BaselineEpochError(
                "decision_clocks must be non-empty and unique"
            )
        object.__setattr__(self, "decision_clocks", clocks)
        if not isinstance(self.cumulative_search_count, int) or isinstance(
            self.cumulative_search_count,
            bool,
        ):
            raise BaselineEpochError(
                "cumulative_search_count must be an integer"
            )
        count = self.cumulative_search_count
        if count < 0:
            raise BaselineEpochError(
                "cumulative_search_count must be non-negative"
            )
        object.__setattr__(self, "cumulative_search_count", count)
        object.__setattr__(
            self,
            "metadata",
            _freeze_json(
                json.loads(_canonical_json(dict(self.metadata)))
            ),
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> BaselineEpoch:
        payload = dict(value)
        schema_version = payload.pop("schema_version", BASELINE_SCHEMA_VERSION)
        if schema_version != BASELINE_SCHEMA_VERSION:
            raise BaselineEpochError(
                f"unsupported baseline schema: {schema_version}"
            )
        payload.pop("contract_hash", None)
        payload["decision_clocks"] = tuple(payload.get("decision_clocks") or ())
        return cls(**payload)

    def as_dict(self) -> dict[str, Any]:
        payload = {
            item.name: getattr(self, item.name)
            for item in fields(self)
            if item.name != "metadata"
        }
        payload["metadata"] = _thaw_json(self.metadata)
        payload["decision_clocks"] = list(self.decision_clocks)
        return {
            "schema_version": BASELINE_SCHEMA_VERSION,
            **payload,
            "contract_hash": self.contract_hash,
        }

    @property
    def contract_hash(self) -> str:
        payload = {
            item.name: getattr(self, item.name)
            for item in fields(self)
            if item.name != "metadata"
        }
        payload["metadata"] = _thaw_json(self.metadata)
        payload["decision_clocks"] = list(self.decision_clocks)
        return hashlib.sha256(
            _canonical_json(payload).encode("utf-8")
        ).hexdigest()


def hash_artifact_paths(
    project_root: str | Path,
    relative_paths: Iterable[str | Path],
) -> str:
    """Hash an explicit artifact set without importing the legacy runtime."""

    root = Path(project_root).resolve()
    files: list[Path] = []
    requested = tuple(relative_paths)
    if not requested:
        raise BaselineEpochError("relative_paths must not be empty")
    for relative in requested:
        candidate = (root / relative).resolve()
        try:
            candidate.relative_to(root)
        except ValueError as exc:
            raise BaselineEpochError(
                f"artifact path escapes project root: {relative}"
            ) from exc
        if not candidate.exists():
            raise BaselineEpochError(f"artifact path does not exist: {relative}")
        if candidate.is_dir():
            files.extend(
                child
                for child in candidate.rglob("*")
                if child.is_file()
                and "__pycache__" not in child.parts
                and child.suffix not in {".pyc", ".pyo"}
            )
        else:
            files.append(candidate)
    digest = hashlib.sha256()
    unique_files = sorted(
        set(files),
        key=lambda path: path.relative_to(root).as_posix(),
    )
    if not unique_files:
        raise BaselineEpochError("artifact set contains no files")
    for path in unique_files:
        relative = path.relative_to(root).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()
