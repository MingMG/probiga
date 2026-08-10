from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time
from enum import Enum
from typing import Any, TypeAlias
from zoneinfo import ZoneInfo


MARKET_TIMEZONE = ZoneInfo("Asia/Shanghai")


class EvidenceStatus(str, Enum):
    AVAILABLE = "AVAILABLE"
    NO_INPUT = "NO_INPUT"
    FUTURE_ONLY = "FUTURE_ONLY"
    INVALID_INPUT = "INVALID_INPUT"
    STALE_SNAPSHOT = "STALE_SNAPSHOT"


class CombinedEvidenceStatus(str, Enum):
    COMPLETE = "COMPLETE"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True)
class ThemeMembershipRecord:
    stock_code: str
    theme_code: str
    theme_name: str
    source: str
    effective_from: date
    effective_to: date | None


@dataclass(frozen=True)
class ThemeMembershipSnapshot:
    snapshot_id: str
    as_of: date
    recorded_at: datetime
    is_complete: bool
    records: tuple[ThemeMembershipRecord, ...]


@dataclass(frozen=True)
class ThemeNewsNoveltyRecord:
    theme_code: str
    theme_name: str
    novelty_score: float
    evidence_count: int
    evidence_ids: tuple[str, ...]
    source: str
    effective_from: date
    effective_to: date | None


@dataclass(frozen=True)
class ThemeNewsSnapshot:
    snapshot_id: str
    as_of: date
    recorded_at: datetime
    lookback_start: date
    lookback_end: date
    is_complete: bool
    records: tuple[ThemeNewsNoveltyRecord, ...]


@dataclass(frozen=True)
class SnapshotValidation:
    is_valid: bool
    status: EvidenceStatus
    errors: tuple[str, ...]


Membership: TypeAlias = tuple[str, str, str]


@dataclass(frozen=True)
class ThemeHistoryView:
    signal_date: date
    signal_cutoff: datetime
    memberships: dict[str, list[Membership]]
    news_novelty: dict[str, float]
    membership_evidence_status: EvidenceStatus
    news_evidence_status: EvidenceStatus
    evidence_status: CombinedEvidenceStatus
    membership_snapshot_id: str | None
    membership_as_of: date | None
    membership_age_days: int | None
    news_snapshot_id: str | None
    news_as_of: date | None
    news_age_days: int | None
    errors: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "signal_date": self.signal_date.isoformat(),
            "signal_cutoff": self.signal_cutoff.isoformat(),
            "memberships": self.memberships,
            "news_novelty": self.news_novelty,
            "membership_evidence_status": self.membership_evidence_status.value,
            "news_evidence_status": self.news_evidence_status.value,
            "evidence_status": self.evidence_status.value,
            "membership_snapshot_id": self.membership_snapshot_id,
            "membership_as_of": (
                self.membership_as_of.isoformat()
                if self.membership_as_of is not None
                else None
            ),
            "membership_age_days": self.membership_age_days,
            "news_snapshot_id": self.news_snapshot_id,
            "news_as_of": (
                self.news_as_of.isoformat()
                if self.news_as_of is not None
                else None
            ),
            "news_age_days": self.news_age_days,
            "errors": list(self.errors),
        }


_MISSING = object()


def _mapping_value(raw: Mapping[str, Any], key: str) -> Any:
    return raw[key] if key in raw else _MISSING


def _required_value(
    raw: Any,
    key: str,
    *,
    path: str,
    errors: list[str],
) -> Any:
    if isinstance(raw, Mapping):
        value = _mapping_value(raw, key)
    else:
        value = getattr(raw, key, _MISSING)
    if value is _MISSING:
        errors.append(f"{path}.{key}: missing required field")
    return value


def _strict_date(value: Any, *, path: str, errors: list[str]) -> date | None:
    if not isinstance(value, date) or isinstance(value, datetime):
        errors.append(f"{path}: expected date")
        return None
    return value


def _aware_datetime(
    value: Any,
    *,
    path: str,
    errors: list[str],
) -> datetime | None:
    if not isinstance(value, datetime):
        errors.append(f"{path}: expected timezone-aware datetime")
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        errors.append(f"{path}: timezone is required")
        return None
    return value


def _non_empty_text(
    value: Any,
    *,
    path: str,
    errors: list[str],
) -> str | None:
    if not isinstance(value, str) or not value.strip():
        errors.append(f"{path}: expected non-empty string")
        return None
    return value.strip()


def _records_value(
    value: Any,
    *,
    path: str,
    errors: list[str],
) -> tuple[Any, ...] | None:
    if not isinstance(value, (list, tuple)):
        errors.append(f"{path}: expected list or tuple")
        return None
    return tuple(value)


def _validate_period(
    effective_from: date | None,
    effective_to: date | None,
    *,
    path: str,
    errors: list[str],
) -> None:
    if (
        effective_from is not None
        and effective_to is not None
        and effective_to <= effective_from
    ):
        errors.append(f"{path}: effective_to must be after effective_from")


def _parse_membership_record(
    raw: Any,
    *,
    path: str,
    errors: list[str],
) -> ThemeMembershipRecord | None:
    if not isinstance(raw, (ThemeMembershipRecord, Mapping)):
        errors.append(f"{path}: expected membership record")
        return None

    stock_code = _non_empty_text(
        _required_value(raw, "stock_code", path=path, errors=errors),
        path=f"{path}.stock_code",
        errors=errors,
    )
    theme_code = _non_empty_text(
        _required_value(raw, "theme_code", path=path, errors=errors),
        path=f"{path}.theme_code",
        errors=errors,
    )
    theme_name = _non_empty_text(
        _required_value(raw, "theme_name", path=path, errors=errors),
        path=f"{path}.theme_name",
        errors=errors,
    )
    source = _non_empty_text(
        _required_value(raw, "source", path=path, errors=errors),
        path=f"{path}.source",
        errors=errors,
    )
    effective_from = _strict_date(
        _required_value(raw, "effective_from", path=path, errors=errors),
        path=f"{path}.effective_from",
        errors=errors,
    )
    effective_to_raw = _required_value(
        raw,
        "effective_to",
        path=path,
        errors=errors,
    )
    effective_to = (
        None
        if effective_to_raw is None
        else _strict_date(
            effective_to_raw,
            path=f"{path}.effective_to",
            errors=errors,
        )
    )
    _validate_period(
        effective_from,
        effective_to,
        path=path,
        errors=errors,
    )
    if None in (stock_code, theme_code, theme_name, source, effective_from):
        return None
    return ThemeMembershipRecord(
        stock_code=stock_code,
        theme_code=theme_code,
        theme_name=theme_name,
        source=source,
        effective_from=effective_from,
        effective_to=effective_to,
    )


def _periods_overlap(
    left_from: date,
    left_to: date | None,
    right_from: date,
    right_to: date | None,
) -> bool:
    left_ends_after_right_starts = left_to is None or right_from < left_to
    right_ends_after_left_starts = right_to is None or left_from < right_to
    return left_ends_after_right_starts and right_ends_after_left_starts


def _validate_membership_overlaps(
    records: tuple[ThemeMembershipRecord, ...],
    *,
    errors: list[str],
) -> None:
    grouped: dict[tuple[str, str, str], list[ThemeMembershipRecord]] = {}
    for record in records:
        key = (record.stock_code, record.theme_code, record.source)
        grouped.setdefault(key, []).append(record)
    for key, values in grouped.items():
        ordered = sorted(values, key=lambda item: item.effective_from)
        for left, right in zip(ordered, ordered[1:]):
            if _periods_overlap(
                left.effective_from,
                left.effective_to,
                right.effective_from,
                right.effective_to,
            ):
                errors.append(
                    "records: overlapping membership periods for "
                    f"{key[0]}/{key[1]}/{key[2]}"
                )


def _parse_membership_snapshot(
    raw: Any,
    *,
    path: str,
) -> tuple[ThemeMembershipSnapshot | None, tuple[str, ...]]:
    errors: list[str] = []
    if not isinstance(raw, (ThemeMembershipSnapshot, Mapping)):
        return None, (f"{path}: expected membership snapshot",)

    snapshot_id = _non_empty_text(
        _required_value(raw, "snapshot_id", path=path, errors=errors),
        path=f"{path}.snapshot_id",
        errors=errors,
    )
    as_of = _strict_date(
        _required_value(raw, "as_of", path=path, errors=errors),
        path=f"{path}.as_of",
        errors=errors,
    )
    recorded_at = _aware_datetime(
        _required_value(raw, "recorded_at", path=path, errors=errors),
        path=f"{path}.recorded_at",
        errors=errors,
    )
    is_complete = _required_value(
        raw,
        "is_complete",
        path=path,
        errors=errors,
    )
    if is_complete is not True:
        errors.append(f"{path}.is_complete: complete snapshot is required")
    records_raw = _records_value(
        _required_value(raw, "records", path=path, errors=errors),
        path=f"{path}.records",
        errors=errors,
    )
    records = tuple(
        record
        for index, value in enumerate(records_raw or ())
        if (
            record := _parse_membership_record(
                value,
                path=f"{path}.records[{index}]",
                errors=errors,
            )
        )
        is not None
    )
    _validate_membership_overlaps(records, errors=errors)
    if as_of is not None and recorded_at is not None:
        recorded_date = recorded_at.astimezone(MARKET_TIMEZONE).date()
        if recorded_date < as_of:
            errors.append(f"{path}.recorded_at: cannot precede as_of")
    if errors:
        return None, tuple(errors)
    return (
        ThemeMembershipSnapshot(
            snapshot_id=snapshot_id,
            as_of=as_of,
            recorded_at=recorded_at,
            is_complete=True,
            records=records,
        ),
        (),
    )


def _parse_news_record(
    raw: Any,
    *,
    path: str,
    errors: list[str],
) -> ThemeNewsNoveltyRecord | None:
    if not isinstance(raw, (ThemeNewsNoveltyRecord, Mapping)):
        errors.append(f"{path}: expected news novelty record")
        return None

    theme_code = _non_empty_text(
        _required_value(raw, "theme_code", path=path, errors=errors),
        path=f"{path}.theme_code",
        errors=errors,
    )
    theme_name = _non_empty_text(
        _required_value(raw, "theme_name", path=path, errors=errors),
        path=f"{path}.theme_name",
        errors=errors,
    )
    source = _non_empty_text(
        _required_value(raw, "source", path=path, errors=errors),
        path=f"{path}.source",
        errors=errors,
    )
    score_raw = _required_value(
        raw,
        "novelty_score",
        path=path,
        errors=errors,
    )
    score: float | None = None
    if isinstance(score_raw, bool) or not isinstance(score_raw, (int, float)):
        errors.append(f"{path}.novelty_score: expected finite number in [0, 1]")
    else:
        score = float(score_raw)
        if not math.isfinite(score) or not 0.0 <= score <= 1.0:
            errors.append(
                f"{path}.novelty_score: expected finite number in [0, 1]"
            )
    count_raw = _required_value(
        raw,
        "evidence_count",
        path=path,
        errors=errors,
    )
    evidence_count: int | None = None
    if isinstance(count_raw, bool) or not isinstance(count_raw, int) or count_raw < 0:
        errors.append(f"{path}.evidence_count: expected non-negative integer")
    else:
        evidence_count = count_raw
    ids_raw = _records_value(
        _required_value(raw, "evidence_ids", path=path, errors=errors),
        path=f"{path}.evidence_ids",
        errors=errors,
    )
    evidence_ids: tuple[str, ...] = ()
    if ids_raw is not None:
        parsed_ids: list[str] = []
        for index, value in enumerate(ids_raw):
            item = _non_empty_text(
                value,
                path=f"{path}.evidence_ids[{index}]",
                errors=errors,
            )
            if item is not None:
                parsed_ids.append(item)
        evidence_ids = tuple(parsed_ids)
        if len(set(evidence_ids)) != len(evidence_ids):
            errors.append(f"{path}.evidence_ids: duplicate evidence id")
    if evidence_count is not None and evidence_count < len(evidence_ids):
        errors.append(f"{path}.evidence_count: smaller than retained evidence ids")
    effective_from = _strict_date(
        _required_value(raw, "effective_from", path=path, errors=errors),
        path=f"{path}.effective_from",
        errors=errors,
    )
    effective_to_raw = _required_value(
        raw,
        "effective_to",
        path=path,
        errors=errors,
    )
    effective_to = (
        None
        if effective_to_raw is None
        else _strict_date(
            effective_to_raw,
            path=f"{path}.effective_to",
            errors=errors,
        )
    )
    _validate_period(
        effective_from,
        effective_to,
        path=path,
        errors=errors,
    )
    if None in (
        theme_code,
        theme_name,
        source,
        score,
        evidence_count,
        effective_from,
    ):
        return None
    return ThemeNewsNoveltyRecord(
        theme_code=theme_code,
        theme_name=theme_name,
        novelty_score=score,
        evidence_count=evidence_count,
        evidence_ids=evidence_ids,
        source=source,
        effective_from=effective_from,
        effective_to=effective_to,
    )


def _validate_news_overlaps(
    records: tuple[ThemeNewsNoveltyRecord, ...],
    *,
    errors: list[str],
) -> None:
    grouped: dict[str, list[ThemeNewsNoveltyRecord]] = {}
    for record in records:
        grouped.setdefault(record.theme_code, []).append(record)
    for theme_code, values in grouped.items():
        ordered = sorted(values, key=lambda item: item.effective_from)
        for left, right in zip(ordered, ordered[1:]):
            if _periods_overlap(
                left.effective_from,
                left.effective_to,
                right.effective_from,
                right.effective_to,
            ):
                errors.append(
                    "records: overlapping news novelty periods for "
                    f"{theme_code}"
                )


def _parse_news_snapshot(
    raw: Any,
    *,
    path: str,
) -> tuple[ThemeNewsSnapshot | None, tuple[str, ...]]:
    errors: list[str] = []
    if not isinstance(raw, (ThemeNewsSnapshot, Mapping)):
        return None, (f"{path}: expected news snapshot",)

    snapshot_id = _non_empty_text(
        _required_value(raw, "snapshot_id", path=path, errors=errors),
        path=f"{path}.snapshot_id",
        errors=errors,
    )
    as_of = _strict_date(
        _required_value(raw, "as_of", path=path, errors=errors),
        path=f"{path}.as_of",
        errors=errors,
    )
    recorded_at = _aware_datetime(
        _required_value(raw, "recorded_at", path=path, errors=errors),
        path=f"{path}.recorded_at",
        errors=errors,
    )
    lookback_start = _strict_date(
        _required_value(raw, "lookback_start", path=path, errors=errors),
        path=f"{path}.lookback_start",
        errors=errors,
    )
    lookback_end = _strict_date(
        _required_value(raw, "lookback_end", path=path, errors=errors),
        path=f"{path}.lookback_end",
        errors=errors,
    )
    is_complete = _required_value(
        raw,
        "is_complete",
        path=path,
        errors=errors,
    )
    if is_complete is not True:
        errors.append(f"{path}.is_complete: complete snapshot is required")
    records_raw = _records_value(
        _required_value(raw, "records", path=path, errors=errors),
        path=f"{path}.records",
        errors=errors,
    )
    records = tuple(
        record
        for index, value in enumerate(records_raw or ())
        if (
            record := _parse_news_record(
                value,
                path=f"{path}.records[{index}]",
                errors=errors,
            )
        )
        is not None
    )
    _validate_news_overlaps(records, errors=errors)
    if (
        lookback_start is not None
        and lookback_end is not None
        and lookback_start > lookback_end
    ):
        errors.append(f"{path}: lookback_start must not follow lookback_end")
    if as_of is not None and lookback_end is not None and lookback_end > as_of:
        errors.append(f"{path}.lookback_end: cannot follow as_of")
    if as_of is not None and recorded_at is not None:
        recorded_date = recorded_at.astimezone(MARKET_TIMEZONE).date()
        if recorded_date < as_of:
            errors.append(f"{path}.recorded_at: cannot precede as_of")
    if errors:
        return None, tuple(errors)
    return (
        ThemeNewsSnapshot(
            snapshot_id=snapshot_id,
            as_of=as_of,
            recorded_at=recorded_at,
            lookback_start=lookback_start,
            lookback_end=lookback_end,
            is_complete=True,
            records=records,
        ),
        (),
    )


def validate_membership_snapshot(raw: Any) -> SnapshotValidation:
    _, errors = _parse_membership_snapshot(raw, path="membership_snapshot")
    return SnapshotValidation(
        is_valid=not errors,
        status=(
            EvidenceStatus.AVAILABLE
            if not errors
            else EvidenceStatus.INVALID_INPUT
        ),
        errors=errors,
    )


def validate_news_snapshot(raw: Any) -> SnapshotValidation:
    _, errors = _parse_news_snapshot(raw, path="news_snapshot")
    return SnapshotValidation(
        is_valid=not errors,
        status=(
            EvidenceStatus.AVAILABLE
            if not errors
            else EvidenceStatus.INVALID_INPUT
        ),
        errors=errors,
    )


def _signal_cutoff(value: date | datetime) -> tuple[date, datetime]:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("signal_date datetime must be timezone-aware")
        cutoff = value
    elif isinstance(value, date):
        cutoff = datetime.combine(value, time.max, tzinfo=MARKET_TIMEZONE)
    else:
        raise TypeError("signal_date must be date or timezone-aware datetime")
    signal_day = cutoff.astimezone(MARKET_TIMEZONE).date()
    return signal_day, cutoff


def _is_active(
    effective_from: date,
    effective_to: date | None,
    *,
    signal_date: date,
) -> bool:
    return effective_from <= signal_date and (
        effective_to is None or signal_date < effective_to
    )


def _parse_membership_collection(
    snapshots: Iterable[ThemeMembershipSnapshot | Mapping[str, Any]],
) -> tuple[list[ThemeMembershipSnapshot], tuple[str, ...]]:
    parsed: list[ThemeMembershipSnapshot] = []
    errors: list[str] = []
    for index, raw in enumerate(snapshots):
        snapshot, item_errors = _parse_membership_snapshot(
            raw,
            path=f"membership_snapshots[{index}]",
        )
        errors.extend(item_errors)
        if snapshot is not None:
            parsed.append(snapshot)
    return parsed, tuple(errors)


def _parse_news_collection(
    snapshots: Iterable[ThemeNewsSnapshot | Mapping[str, Any]],
) -> tuple[list[ThemeNewsSnapshot], tuple[str, ...]]:
    parsed: list[ThemeNewsSnapshot] = []
    errors: list[str] = []
    for index, raw in enumerate(snapshots):
        snapshot, item_errors = _parse_news_snapshot(
            raw,
            path=f"news_snapshots[{index}]",
        )
        errors.extend(item_errors)
        if snapshot is not None:
            parsed.append(snapshot)
    return parsed, tuple(errors)


def _visible_snapshot(
    snapshots: Iterable[ThemeMembershipSnapshot | ThemeNewsSnapshot],
    *,
    signal_date: date,
    signal_cutoff: datetime,
) -> ThemeMembershipSnapshot | ThemeNewsSnapshot | None:
    visible = [
        snapshot
        for snapshot in snapshots
        if snapshot.as_of <= signal_date
        and snapshot.recorded_at <= signal_cutoff
    ]
    if not visible:
        return None
    return max(
        visible,
        key=lambda snapshot: (
            snapshot.as_of,
            snapshot.recorded_at,
            snapshot.snapshot_id,
        ),
    )


def _age_status(
    *,
    age_days: int,
    max_age_days: int | None,
) -> EvidenceStatus:
    if max_age_days is not None and age_days > max_age_days:
        return EvidenceStatus.STALE_SNAPSHOT
    return EvidenceStatus.AVAILABLE


def resolve_theme_history(
    *,
    signal_date: date | datetime,
    membership_snapshots: Iterable[
        ThemeMembershipSnapshot | Mapping[str, Any]
    ] = (),
    news_snapshots: Iterable[ThemeNewsSnapshot | Mapping[str, Any]] = (),
    membership_max_age_days: int | None = None,
    news_max_age_days: int | None = 0,
) -> ThemeHistoryView:
    """Resolve only evidence that was genuinely visible at signal time.

    ``recorded_at`` is the knowledge-time boundary and ``effective_*`` is the
    business-time boundary.  Both must pass.  A current snapshot therefore
    cannot be relabelled with an old ``as_of`` date to backfill history.
    Malformed input fails closed for its whole evidence class.
    """

    signal_day, cutoff = _signal_cutoff(signal_date)
    for field_name, value in (
        ("membership_max_age_days", membership_max_age_days),
        ("news_max_age_days", news_max_age_days),
    ):
        if value is not None and (
            isinstance(value, bool) or not isinstance(value, int) or value < 0
        ):
            raise ValueError(f"{field_name} must be a non-negative integer or None")

    membership_inputs = tuple(membership_snapshots)
    news_inputs = tuple(news_snapshots)
    parsed_memberships, membership_errors = _parse_membership_collection(
        membership_inputs
    )
    parsed_news, news_errors = _parse_news_collection(news_inputs)

    memberships: dict[str, list[Membership]] = {}
    membership_snapshot: ThemeMembershipSnapshot | None = None
    membership_age: int | None = None
    if membership_errors:
        membership_status = EvidenceStatus.INVALID_INPUT
    elif not membership_inputs:
        membership_status = EvidenceStatus.NO_INPUT
    else:
        selected = _visible_snapshot(
            parsed_memberships,
            signal_date=signal_day,
            signal_cutoff=cutoff,
        )
        membership_snapshot = (
            selected if isinstance(selected, ThemeMembershipSnapshot) else None
        )
        if membership_snapshot is None:
            membership_status = EvidenceStatus.FUTURE_ONLY
        else:
            membership_age = (signal_day - membership_snapshot.as_of).days
            membership_status = _age_status(
                age_days=membership_age,
                max_age_days=membership_max_age_days,
            )
            if membership_status is EvidenceStatus.AVAILABLE:
                for record in membership_snapshot.records:
                    if not _is_active(
                        record.effective_from,
                        record.effective_to,
                        signal_date=signal_day,
                    ):
                        continue
                    memberships.setdefault(record.stock_code, []).append(
                        (record.theme_code, record.theme_name, record.source)
                    )
                memberships = {
                    stock_code: sorted(set(values))
                    for stock_code, values in sorted(memberships.items())
                }

    news_novelty: dict[str, float] = {}
    news_snapshot: ThemeNewsSnapshot | None = None
    news_age: int | None = None
    if news_errors:
        news_status = EvidenceStatus.INVALID_INPUT
    elif not news_inputs:
        news_status = EvidenceStatus.NO_INPUT
    else:
        selected = _visible_snapshot(
            parsed_news,
            signal_date=signal_day,
            signal_cutoff=cutoff,
        )
        news_snapshot = selected if isinstance(selected, ThemeNewsSnapshot) else None
        if news_snapshot is None:
            news_status = EvidenceStatus.FUTURE_ONLY
        else:
            news_age = (signal_day - news_snapshot.as_of).days
            news_status = _age_status(
                age_days=news_age,
                max_age_days=news_max_age_days,
            )
            if news_status is EvidenceStatus.AVAILABLE:
                news_novelty = {
                    record.theme_code: record.novelty_score
                    for record in news_snapshot.records
                    if _is_active(
                        record.effective_from,
                        record.effective_to,
                        signal_date=signal_day,
                    )
                }

    complete = (
        membership_status is EvidenceStatus.AVAILABLE
        and news_status is EvidenceStatus.AVAILABLE
    )
    return ThemeHistoryView(
        signal_date=signal_day,
        signal_cutoff=cutoff,
        memberships=memberships,
        news_novelty=news_novelty,
        membership_evidence_status=membership_status,
        news_evidence_status=news_status,
        evidence_status=(
            CombinedEvidenceStatus.COMPLETE
            if complete
            else CombinedEvidenceStatus.BLOCKED
        ),
        membership_snapshot_id=(
            membership_snapshot.snapshot_id
            if membership_snapshot is not None
            else None
        ),
        membership_as_of=(
            membership_snapshot.as_of
            if membership_snapshot is not None
            else None
        ),
        membership_age_days=membership_age,
        news_snapshot_id=(
            news_snapshot.snapshot_id if news_snapshot is not None else None
        ),
        news_as_of=news_snapshot.as_of if news_snapshot is not None else None,
        news_age_days=news_age,
        errors=tuple((*membership_errors, *news_errors)),
    )


__all__ = [
    "CombinedEvidenceStatus",
    "EvidenceStatus",
    "MARKET_TIMEZONE",
    "SnapshotValidation",
    "ThemeHistoryView",
    "ThemeMembershipRecord",
    "ThemeMembershipSnapshot",
    "ThemeNewsNoveltyRecord",
    "ThemeNewsSnapshot",
    "resolve_theme_history",
    "validate_membership_snapshot",
    "validate_news_snapshot",
]
