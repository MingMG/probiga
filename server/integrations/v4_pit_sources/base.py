"""Shared fail-closed mechanics for static V4 PIT source adapters."""

from __future__ import annotations

import json
import math
import re
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.elements import TextClause

from server.trading_v4.domain import AsOfDataset, AsOfRecord, DatasetResult
from server.trading_v4.domain.enums import QualityStatus, ReplayEligibility

from .contracts import PitSourceContract


CHINA_TIMEZONE = ZoneInfo("Asia/Shanghai")
SESSION_CLOSE = time(15, 0)
_STOCK_CODE = re.compile(r"^(?P<code>[0-9]{1,6})(?:\.(?:SH|SZ|BJ))?$", re.I)


class PitSourceError(RuntimeError):
    """Base error for a source read that cannot safely produce a dataset."""


class PitSourceReadError(PitSourceError):
    """The exact contracted source could not be read."""


class PitSourceRowLimitExceeded(PitSourceError):
    """A bounded source read returned more rows than it may safely process."""


class PitSourceDataError(PitSourceError):
    """A source row cannot establish its point-in-time chronology."""


def require_aware(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


def local_naive(value: datetime) -> datetime:
    return require_aware(value, "datetime").astimezone(CHINA_TIMEZONE).replace(
        tzinfo=None
    )


def aware_datetime(
    value: Any,
    field_name: str,
    *,
    date_time: time = time(0, 0),
    required: bool = False,
) -> datetime | None:
    if value is None or value == "":
        if required:
            raise PitSourceDataError(f"{field_name} is required for PIT chronology")
        return None
    parsed: datetime
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, date_time)
    elif isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            if required:
                raise PitSourceDataError(
                    f"{field_name} is required for PIT chronology"
                )
            return None
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        try:
            candidate = datetime.fromisoformat(normalized)
        except ValueError as exc:
            try:
                candidate_date = date.fromisoformat(normalized[:10])
            except ValueError:
                raise PitSourceDataError(
                    f"{field_name} is not an ISO date/time"
                ) from exc
            parsed = datetime.combine(candidate_date, date_time)
        else:
            parsed = candidate
            if len(normalized) == 10:
                parsed = datetime.combine(parsed.date(), date_time)
    else:
        raise PitSourceDataError(f"{field_name} is not a date/time value")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=CHINA_TIMEZONE)
    return parsed.astimezone(CHINA_TIMEZONE)


def session_close(value: Any, field_name: str) -> datetime:
    parsed = aware_datetime(
        value,
        field_name,
        date_time=SESSION_CLOSE,
        required=True,
    )
    assert parsed is not None
    return datetime.combine(parsed.date(), SESSION_CLOSE, tzinfo=CHINA_TIMEZONE)


def maximum_time(*values: datetime | None) -> datetime:
    present = tuple(value for value in values if value is not None)
    if not present:
        raise PitSourceDataError("PIT chronology has no observable timestamp")
    return max(present)


def canonical_entity(value: Any) -> str:
    if not isinstance(value, str):
        value = str(value or "")
    normalized = value.strip().upper()
    if not normalized:
        raise ValueError("entity must not be empty")
    match = _STOCK_CODE.fullmatch(normalized)
    if match:
        return match.group("code").zfill(6)
    return normalized


def canonical_entities(values: Iterable[str]) -> tuple[str, ...]:
    entities = tuple(sorted({canonical_entity(value) for value in values}))
    if not entities:
        raise ValueError("at least one entity is required")
    return entities


def requested_fields(
    values: Iterable[str],
    *,
    contract: PitSourceContract,
) -> tuple[str, ...]:
    requested = tuple(
        sorted(
            {
                str(value).strip()
                for value in values
                if isinstance(value, str) and value.strip()
            }
        )
    )
    resolved = requested or tuple(sorted(contract.default_fields))
    unknown = set(resolved) - set(contract.default_fields)
    if unknown:
        raise ValueError(
            f"unsupported fields for {contract.source_name}: {tuple(sorted(unknown))}"
        )
    return resolved


def normalized_payload(value: Any) -> Any:
    """Convert driver-specific values to deterministic contract-safe values."""

    if value is None or isinstance(value, (str, bool, int, Decimal)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise PitSourceDataError("source payload contains a non-finite number")
        return value
    if isinstance(value, datetime):
        parsed = aware_datetime(value, "payload datetime", required=True)
        assert parsed is not None
        return parsed.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if isinstance(value, Mapping):
        return {
            str(key): normalized_payload(child)
            for key, child in value.items()
        }
    if isinstance(value, (tuple, list)):
        return [normalized_payload(child) for child in value]
    return str(value)


def decoded_json(value: Any, field_name: str) -> Any:
    if value is None or value == "":
        return []
    if isinstance(value, (list, tuple, dict)):
        return normalized_payload(value)
    if not isinstance(value, str):
        raise PitSourceDataError(f"{field_name} is not JSON")
    try:
        return normalized_payload(json.loads(value))
    except (TypeError, ValueError) as exc:
        raise PitSourceDataError(f"{field_name} is not valid JSON") from exc


def linked_stock_codes(value: Any) -> frozenset[str]:
    payload = decoded_json(value, "stocks")
    raw_codes: list[Any] = []
    if isinstance(payload, Mapping):
        payload = [payload]
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, Mapping):
                raw_codes.append(
                    item.get("code")
                    or item.get("stock_code")
                    or item.get("security_code")
                )
            elif isinstance(item, str):
                raw_codes.append(item)
    codes: set[str] = set()
    for value in raw_codes:
        if value not in (None, ""):
            codes.add(canonical_entity(value))
    return frozenset(codes)


def chronology_is_visible(knowledge_time: datetime, cutoff: datetime) -> bool:
    require_aware(knowledge_time, "knowledge_time")
    require_aware(cutoff, "knowledge_cutoff")
    return knowledge_time <= cutoff


def _status_and_reasons(
    *,
    contract: PitSourceContract,
    requested: tuple[str, ...],
    returned: tuple[str, ...],
) -> tuple[QualityStatus, tuple[str, ...]]:
    reasons: set[str] = set()
    if not returned:
        status = QualityStatus.FAIL
        reasons.update(("NO_RECORDS_BEFORE_CUTOFF", "PARTIAL_COVERAGE"))
    elif set(returned) != set(requested):
        status = QualityStatus.WARN
        reasons.add("PARTIAL_COVERAGE")
    else:
        status = QualityStatus.PASS
    eligibility = contract.default_replay_eligibility
    if eligibility != ReplayEligibility.PIT_CERTIFIED:
        status = QualityStatus.WARN if returned else QualityStatus.FAIL
        reasons.add(f"SOURCE_{eligibility.value}")
    return status, tuple(sorted(reasons))


def dataset_result(
    *,
    contract: PitSourceContract,
    cutoff: datetime,
    requested: tuple[str, ...],
    returned: Iterable[str],
    fields: tuple[str, ...],
    records: Iterable[AsOfRecord],
) -> DatasetResult:
    returned_entities = tuple(sorted(set(returned)))
    status, reasons = _status_and_reasons(
        contract=contract,
        requested=requested,
        returned=returned_entities,
    )
    dataset = AsOfDataset(
        dataset_name=contract.dataset_name,
        as_of=cutoff,
        records=tuple(records),
        quality_status=status,
    )
    return DatasetResult(
        dataset=dataset,
        requested_cutoff=cutoff,
        requested_entities=requested,
        returned_entities=returned_entities,
        requested_fields=fields,
        freshness_status=status,
        reason_codes=reasons,
    )


@dataclass(frozen=True)
class StaticPitAdapter:
    """Base for one static SQL adapter with an enforced raw-row ceiling."""

    engine: Any
    max_rows: int = 100_000

    def __post_init__(self) -> None:
        if self.engine is None:
            raise ValueError("engine is required")
        if not isinstance(self.max_rows, int) or isinstance(self.max_rows, bool):
            raise TypeError("max_rows must be an integer")
        if self.max_rows < 1:
            raise ValueError("max_rows must be positive")

    def _read_bounded(
        self,
        *,
        contract: PitSourceContract,
        statement: TextClause,
        params: Mapping[str, Any],
    ) -> tuple[dict[str, Any], ...]:
        query_params = dict(params)
        query_params["fetch_limit"] = self.max_rows + 1
        connection_context = (
            self.engine.connect()
            if hasattr(self.engine, "connect")
            else nullcontext(self.engine)
        )
        try:
            with connection_context as connection:
                rows = connection.execute(statement, query_params).mappings().all()
        except SQLAlchemyError as exc:
            raise PitSourceReadError(
                f"failed to read contracted source {contract.table_name}"
            ) from exc
        if len(rows) > self.max_rows:
            raise PitSourceRowLimitExceeded(
                f"{contract.table_name} exceeded max_rows={self.max_rows}"
            )
        return tuple(dict(row) for row in rows)


def default_start(cutoff: datetime, days: int) -> datetime:
    require_aware(cutoff, "knowledge_cutoff")
    return cutoff - timedelta(days=days)
