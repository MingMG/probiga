"""Explicit exchange-session windows for neutral LIMIT+DAY execution.

There is deliberately no weekday or holiday fallback.  A caller must provide
an immutable calendar slice containing the trade date, local sessions and its
point-in-time availability.  Content hashes detect mutation but do not prove
that a calendar came from an authoritative V2 registry; production callers
must use an externally verified receipt reference.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date, datetime, time, timezone
from enum import Enum
import hashlib
import json
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


class SessionCalendarEvidenceKind(str, Enum):
    EXTERNAL_RECEIPT_REFERENCE = "EXTERNAL_RECEIPT_REFERENCE"
    CONTENT_CHECKSUM_ONLY = "CONTENT_CHECKSUM_ONLY"


class SessionWindowState(str, Enum):
    NOT_OBSERVABLE = "NOT_OBSERVABLE"
    PRE_OPEN = "PRE_OPEN"
    ACTIVE = "ACTIVE"
    BREAK = "BREAK"
    CLOSED = "CLOSED"


def _text(value: object, field_name: str) -> str:
    if type(value) is not str:
        raise TypeError(f"{field_name} must be exactly str")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    return normalized


def _sha256(value: object, field_name: str) -> str:
    normalized = _text(value, field_name).lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError(f"{field_name} must be a SHA-256 digest")
    return normalized


def _date(value: object, field_name: str) -> date:
    if type(value) is not date:
        raise TypeError(f"{field_name} must be exactly date")
    return value


def _time(value: object, field_name: str) -> time:
    if type(value) is not time:
        raise TypeError(f"{field_name} must be exactly time")
    if value.tzinfo is not None:
        raise ValueError(f"{field_name} must be a local wall time without tzinfo")
    return value


def _aware(value: object, field_name: str) -> datetime:
    if type(value) is not datetime:
        raise TypeError(f"{field_name} must be exactly datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


def _zone(value: object) -> ZoneInfo:
    name = _text(value, "market_timezone")
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError("market_timezone is unknown") from exc


def _canonical(value: Any) -> Any:
    if type(value) is datetime:
        return value.astimezone(timezone.utc).isoformat(timespec="microseconds")
    if type(value) is date:
        return value.isoformat()
    if type(value) is time:
        return value.isoformat(timespec="microseconds")
    if isinstance(value, Enum):
        return value.value
    if type(value) in {tuple, list}:
        return [_canonical(item) for item in value]
    if type(value) is dict:
        return {
            str(key): _canonical(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if value is None or type(value) in {str, int, bool}:
        return value
    raise TypeError(f"unsupported session hash value: {type(value).__name__}")


def _digest(namespace: str, payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        {"namespace": namespace, "payload": _canonical(payload)},
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _require_reconstructable(
    value: Any,
    expected_type: type,
    field_name: str,
) -> Any:
    if type(value) is not expected_type:
        raise TypeError(
            f"{field_name} must be exactly {expected_type.__name__}"
        )
    try:
        reconstructed = replace(value)
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} cannot be reconstructed") from exc
    if reconstructed != value:
        raise ValueError(
            f"{field_name} differs from its canonical reconstructed value"
        )
    return value


@dataclass(frozen=True, slots=True)
class LocalTradingSession:
    session_id: str
    opens_at: time
    closes_at: time

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "session_id",
            _text(self.session_id, "session_id"),
        )
        opens_at = _time(self.opens_at, "opens_at")
        closes_at = _time(self.closes_at, "closes_at")
        if opens_at >= closes_at:
            raise ValueError("session opens_at must precede closes_at")


@dataclass(frozen=True, slots=True)
class TradingSessionCalendarEvidence:
    evidence_kind: SessionCalendarEvidenceKind
    calendar_version: str
    market_timezone: str
    trade_date: date
    trading_days: tuple[date, ...]
    sessions: tuple[LocalTradingSession, ...]
    available_at: datetime
    source_provider: str
    source_payload_hash: str
    source_receipt_hash: str | None = None
    quality_status: str = "NOT_ASSESSED"
    calendar_hash: str = field(init=False)
    evidence_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.evidence_kind) is not SessionCalendarEvidenceKind:
            raise TypeError(
                "evidence_kind must be exactly SessionCalendarEvidenceKind"
            )
        for field_name in (
            "calendar_version",
            "market_timezone",
            "source_provider",
            "quality_status",
        ):
            object.__setattr__(
                self,
                field_name,
                _text(getattr(self, field_name), field_name),
            )
        zone = _zone(self.market_timezone)
        trade_date = _date(self.trade_date, "trade_date")
        if type(self.trading_days) is not tuple:
            raise TypeError("trading_days must be exactly tuple")
        trading_days = tuple(
            _date(item, "trading_day") for item in self.trading_days
        )
        if (
            not trading_days
            or trading_days != tuple(sorted(set(trading_days)))
        ):
            raise ValueError(
                "trading_days must be non-empty, unique, and increasing"
            )
        if trade_date not in trading_days:
            raise ValueError("trade_date is absent from explicit trading_days")
        if type(self.sessions) is not tuple or not self.sessions:
            raise TypeError("sessions must be a non-empty exact tuple")
        for item in self.sessions:
            _require_reconstructable(item, LocalTradingSession, "session")
        sessions = tuple(
            sorted(self.sessions, key=lambda item: (item.opens_at, item.session_id))
        )
        identifiers = tuple(item.session_id for item in sessions)
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("session_id values must be unique")
        for previous, current in zip(sessions, sessions[1:]):
            if previous.closes_at > current.opens_at:
                raise ValueError("trading sessions cannot overlap")
        available_at = _aware(self.available_at, "available_at")
        object.__setattr__(
            self,
            "source_payload_hash",
            _sha256(self.source_payload_hash, "source_payload_hash"),
        )
        receipt_hash: str | None
        if (
            self.evidence_kind
            == SessionCalendarEvidenceKind.EXTERNAL_RECEIPT_REFERENCE
        ):
            if self.source_receipt_hash is None:
                raise ValueError(
                    "external calendar evidence requires source_receipt_hash"
                )
            receipt_hash = _sha256(
                self.source_receipt_hash,
                "source_receipt_hash",
            )
            if self.quality_status != "PASS":
                raise ValueError("external calendar evidence quality must be PASS")
        else:
            if self.source_receipt_hash is not None:
                raise ValueError(
                    "checksum-only calendar evidence cannot claim a receipt"
                )
            if self.quality_status != "NOT_ASSESSED":
                raise ValueError(
                    "checksum-only calendar evidence must be NOT_ASSESSED"
                )
            receipt_hash = None
        first_open = datetime.combine(
            trade_date,
            sessions[0].opens_at,
            tzinfo=zone,
        )
        last_close = datetime.combine(
            trade_date,
            sessions[-1].closes_at,
            tzinfo=zone,
        )
        calendar_hash = _digest(
            "trading-core.trading-session-calendar.v1",
            {
                "calendar_version": self.calendar_version,
                "market_timezone": self.market_timezone,
                "trading_days": trading_days,
                "trade_date": trade_date,
                "sessions": tuple(
                    {
                        "session_id": item.session_id,
                        "opens_at": item.opens_at,
                        "closes_at": item.closes_at,
                    }
                    for item in sessions
                ),
            },
        )
        evidence_hash = _digest(
            "trading-core.trading-session-calendar-evidence.v1",
            {
                "evidence_kind": self.evidence_kind,
                "calendar_hash": calendar_hash,
                "available_at": available_at,
                "source_provider": self.source_provider,
                "source_payload_hash": self.source_payload_hash,
                "source_receipt_hash": receipt_hash,
                "quality_status": self.quality_status,
                "first_open": first_open,
                "last_close": last_close,
            },
        )
        object.__setattr__(self, "trading_days", trading_days)
        object.__setattr__(self, "sessions", sessions)
        object.__setattr__(self, "source_receipt_hash", receipt_hash)
        object.__setattr__(self, "calendar_hash", calendar_hash)
        object.__setattr__(self, "evidence_hash", evidence_hash)

    @property
    def timezone(self) -> ZoneInfo:
        return _zone(self.market_timezone)

    @property
    def first_open(self) -> datetime:
        return datetime.combine(
            self.trade_date,
            self.sessions[0].opens_at,
            tzinfo=self.timezone,
        )

    @property
    def last_close(self) -> datetime:
        return datetime.combine(
            self.trade_date,
            self.sessions[-1].closes_at,
            tzinfo=self.timezone,
        )


@dataclass(frozen=True, slots=True)
class SessionWindowAssessment:
    state: SessionWindowState
    evaluated_at: datetime
    trade_date: date
    session_id: str | None
    evidence_hash: str
    assessment_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.state) is not SessionWindowState:
            raise TypeError("state must be exactly SessionWindowState")
        _aware(self.evaluated_at, "evaluated_at")
        _date(self.trade_date, "trade_date")
        if self.session_id is not None:
            object.__setattr__(
                self,
                "session_id",
                _text(self.session_id, "session_id"),
            )
        if (self.state == SessionWindowState.ACTIVE) != (
            self.session_id is not None
        ):
            raise ValueError("only ACTIVE assessments identify a session")
        object.__setattr__(
            self,
            "evidence_hash",
            _sha256(self.evidence_hash, "evidence_hash"),
        )
        object.__setattr__(
            self,
            "assessment_hash",
            _digest(
                "trading-core.session-window-assessment.v1",
                {
                    "state": self.state,
                    "evaluated_at": self.evaluated_at,
                    "trade_date": self.trade_date,
                    "session_id": self.session_id,
                    "evidence_hash": self.evidence_hash,
                },
            ),
        )


@dataclass(frozen=True, slots=True)
class LimitDayExecutionWindow:
    trade_date: date
    earliest_at: datetime
    expires_at: datetime
    session_boundaries: tuple[tuple[str, datetime, datetime], ...]
    calendar_evidence_hash: str
    window_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _date(self.trade_date, "trade_date")
        earliest_at = _aware(self.earliest_at, "earliest_at")
        expires_at = _aware(self.expires_at, "expires_at")
        if earliest_at >= expires_at:
            raise ValueError("earliest_at must precede expires_at")
        if type(self.session_boundaries) is not tuple or not self.session_boundaries:
            raise TypeError("session_boundaries must be a non-empty exact tuple")
        normalized: list[tuple[str, datetime, datetime]] = []
        for item in self.session_boundaries:
            if type(item) is not tuple or len(item) != 3:
                raise TypeError(
                    "session boundaries must be exact (id, open, close) tuples"
                )
            session_id, opens_at, closes_at = item
            session_id = _text(session_id, "session_id")
            opens_at = _aware(opens_at, "session opens_at")
            closes_at = _aware(closes_at, "session closes_at")
            if opens_at >= closes_at:
                raise ValueError("session boundary open must precede close")
            normalized.append((session_id, opens_at, closes_at))
        boundaries = tuple(normalized)
        if boundaries != tuple(sorted(boundaries, key=lambda item: item[1])):
            raise ValueError("session boundaries must be in chronological order")
        if earliest_at < boundaries[0][1] or expires_at != boundaries[-1][2]:
            raise ValueError("LIMIT+DAY window must be bounded by calendar sessions")
        object.__setattr__(
            self,
            "calendar_evidence_hash",
            _sha256(self.calendar_evidence_hash, "calendar_evidence_hash"),
        )
        object.__setattr__(
            self,
            "window_hash",
            _digest(
                "trading-core.limit-day-execution-window.v1",
                {
                    "trade_date": self.trade_date,
                    "earliest_at": earliest_at,
                    "expires_at": expires_at,
                    "session_boundaries": boundaries,
                    "calendar_evidence_hash": self.calendar_evidence_hash,
                },
            ),
        )


def _require_external_if_requested(
    evidence: TradingSessionCalendarEvidence,
    require_external_receipt: bool,
) -> None:
    if type(require_external_receipt) is not bool:
        raise TypeError("require_external_receipt must be exactly bool")
    if require_external_receipt and (
        evidence.evidence_kind
        != SessionCalendarEvidenceKind.EXTERNAL_RECEIPT_REFERENCE
    ):
        raise ValueError(
            "production session evaluation requires external calendar receipt"
        )


def assess_session_window(
    evidence: TradingSessionCalendarEvidence,
    *,
    evaluated_at: datetime,
    require_external_receipt: bool = True,
) -> SessionWindowAssessment:
    """Classify one point in time against explicit local exchange sessions."""

    _require_reconstructable(
        evidence,
        TradingSessionCalendarEvidence,
        "calendar evidence",
    )
    _require_external_if_requested(evidence, require_external_receipt)
    evaluated_at = _aware(evaluated_at, "evaluated_at")
    local = evaluated_at.astimezone(evidence.timezone)
    if evaluated_at < evidence.available_at:
        state = SessionWindowState.NOT_OBSERVABLE
        session_id = None
    elif local.date() < evidence.trade_date:
        state = SessionWindowState.PRE_OPEN
        session_id = None
    elif local.date() > evidence.trade_date:
        state = SessionWindowState.CLOSED
        session_id = None
    else:
        session_id = next(
            (
                item.session_id
                for item in evidence.sessions
                if item.opens_at <= local.time().replace(tzinfo=None) < item.closes_at
            ),
            None,
        )
        if session_id is not None:
            state = SessionWindowState.ACTIVE
        elif local.time().replace(tzinfo=None) < evidence.sessions[0].opens_at:
            state = SessionWindowState.PRE_OPEN
        elif local.time().replace(tzinfo=None) >= evidence.sessions[-1].closes_at:
            state = SessionWindowState.CLOSED
        else:
            state = SessionWindowState.BREAK
    return SessionWindowAssessment(
        state=state,
        evaluated_at=evaluated_at,
        trade_date=evidence.trade_date,
        session_id=session_id,
        evidence_hash=evidence.evidence_hash,
    )


def derive_limit_day_execution_window(
    evidence: TradingSessionCalendarEvidence,
    *,
    order_created_at: datetime,
    require_external_receipt: bool = True,
) -> LimitDayExecutionWindow:
    """Derive a DAY order's first executable instant and exclusive expiry."""

    _require_reconstructable(
        evidence,
        TradingSessionCalendarEvidence,
        "calendar evidence",
    )
    _require_external_if_requested(evidence, require_external_receipt)
    created_at = _aware(order_created_at, "order_created_at")
    if created_at < evidence.available_at:
        raise ValueError("calendar evidence was not observable at order creation")
    local_created = created_at.astimezone(evidence.timezone)
    if local_created.date() > evidence.trade_date:
        raise ValueError("order was created after the calendar trade date")
    boundaries = tuple(
        (
            item.session_id,
            datetime.combine(
                evidence.trade_date,
                item.opens_at,
                tzinfo=evidence.timezone,
            ),
            datetime.combine(
                evidence.trade_date,
                item.closes_at,
                tzinfo=evidence.timezone,
            ),
        )
        for item in evidence.sessions
    )
    if local_created.date() < evidence.trade_date:
        earliest = boundaries[0][1]
    else:
        eligible = next(
            (
                (opens_at, closes_at)
                for _, opens_at, closes_at in boundaries
                if local_created < closes_at
            ),
            None,
        )
        if eligible is None:
            raise ValueError("order was created at or after final session close")
        opens_at, _ = eligible
        earliest = max(local_created, opens_at)
    return LimitDayExecutionWindow(
        trade_date=evidence.trade_date,
        earliest_at=earliest,
        expires_at=boundaries[-1][2],
        session_boundaries=boundaries,
        calendar_evidence_hash=evidence.evidence_hash,
    )


__all__ = [
    "LimitDayExecutionWindow",
    "LocalTradingSession",
    "SessionCalendarEvidenceKind",
    "SessionWindowAssessment",
    "SessionWindowState",
    "TradingSessionCalendarEvidence",
    "assess_session_window",
    "derive_limit_day_execution_window",
]
