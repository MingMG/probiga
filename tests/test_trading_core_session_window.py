from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import date, datetime, time, timezone
from zoneinfo import ZoneInfo

import pytest

from server.trading_core.execution import (
    LocalTradingSession,
    SessionCalendarEvidenceKind,
    SessionWindowState,
    TradingSessionCalendarEvidence,
    assess_session_window,
    derive_limit_day_execution_window,
)


SHANGHAI = ZoneInfo("Asia/Shanghai")
TRADE_DATE = date(2026, 8, 3)
TRADING_DAYS = (
    date(2026, 7, 31),
    TRADE_DATE,
    date(2026, 8, 4),
)
AVAILABLE_AT = datetime(2026, 8, 2, 16, 0, tzinfo=SHANGHAI)


def _sessions() -> tuple[LocalTradingSession, ...]:
    return (
        LocalTradingSession("MORNING", time(9, 30), time(11, 30)),
        LocalTradingSession("AFTERNOON", time(13, 0), time(15, 0)),
    )


def _evidence(**overrides: object) -> TradingSessionCalendarEvidence:
    values: dict[str, object] = {
        "evidence_kind": (
            SessionCalendarEvidenceKind.EXTERNAL_RECEIPT_REFERENCE
        ),
        "calendar_version": "v2-calendar-20260803",
        "market_timezone": "Asia/Shanghai",
        "trade_date": TRADE_DATE,
        "trading_days": TRADING_DAYS,
        "sessions": _sessions(),
        "available_at": AVAILABLE_AT,
        "source_provider": "canonical-v2-calendar-boundary",
        "source_payload_hash": "a" * 64,
        "source_receipt_hash": "b" * 64,
        "quality_status": "PASS",
    }
    values.update(overrides)
    return TradingSessionCalendarEvidence(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("evaluated_at", "expected_state", "session_id"),
    (
        (
            datetime(2026, 8, 2, 15, 59, tzinfo=SHANGHAI),
            SessionWindowState.NOT_OBSERVABLE,
            None,
        ),
        (
            datetime(2026, 8, 2, 16, 1, tzinfo=SHANGHAI),
            SessionWindowState.PRE_OPEN,
            None,
        ),
        (
            datetime(2026, 8, 3, 9, 29, 59, tzinfo=SHANGHAI),
            SessionWindowState.PRE_OPEN,
            None,
        ),
        (
            datetime(2026, 8, 3, 9, 30, tzinfo=SHANGHAI),
            SessionWindowState.ACTIVE,
            "MORNING",
        ),
        (
            datetime(2026, 8, 3, 11, 30, tzinfo=SHANGHAI),
            SessionWindowState.BREAK,
            None,
        ),
        (
            datetime(2026, 8, 3, 13, 0, tzinfo=SHANGHAI),
            SessionWindowState.ACTIVE,
            "AFTERNOON",
        ),
        (
            datetime(2026, 8, 3, 15, 0, tzinfo=SHANGHAI),
            SessionWindowState.CLOSED,
            None,
        ),
        (
            datetime(2026, 8, 4, 9, 30, tzinfo=SHANGHAI),
            SessionWindowState.CLOSED,
            None,
        ),
    ),
)
def test_session_boundaries_are_explicit_and_close_is_exclusive(
    evaluated_at: datetime,
    expected_state: SessionWindowState,
    session_id: str | None,
) -> None:
    assessment = assess_session_window(
        _evidence(),
        evaluated_at=evaluated_at,
    )
    assert assessment.state is expected_state
    assert assessment.session_id == session_id
    assert len(assessment.assessment_hash) == 64


def test_limit_day_window_moves_lunch_creation_to_next_session() -> None:
    window = derive_limit_day_execution_window(
        _evidence(),
        order_created_at=datetime(
            2026,
            8,
            3,
            12,
            0,
            tzinfo=SHANGHAI,
        ),
    )
    assert window.earliest_at == datetime(
        2026,
        8,
        3,
        13,
        0,
        tzinfo=SHANGHAI,
    )
    assert window.expires_at == datetime(
        2026,
        8,
        3,
        15,
        0,
        tzinfo=SHANGHAI,
    )
    assert tuple(item[0] for item in window.session_boundaries) == (
        "MORNING",
        "AFTERNOON",
    )
    assert len(window.window_hash) == 64


def test_limit_day_window_uses_creation_inside_session_and_rejects_after_close() -> None:
    created_at = datetime(2026, 8, 3, 10, 7, tzinfo=SHANGHAI)
    window = derive_limit_day_execution_window(
        _evidence(),
        order_created_at=created_at,
    )
    assert window.earliest_at == created_at

    with pytest.raises(ValueError, match="final session close"):
        derive_limit_day_execution_window(
            _evidence(),
            order_created_at=datetime(
                2026,
                8,
                3,
                15,
                0,
                tzinfo=SHANGHAI,
            ),
        )


def test_prior_day_order_starts_at_first_explicit_session() -> None:
    window = derive_limit_day_execution_window(
        _evidence(),
        order_created_at=datetime(
            2026,
            8,
            2,
            16,
            30,
            tzinfo=SHANGHAI,
        ),
    )
    assert window.earliest_at == datetime(
        2026,
        8,
        3,
        9,
        30,
        tzinfo=SHANGHAI,
    )


def test_checksum_only_calendar_is_research_only_by_default() -> None:
    evidence = _evidence(
        evidence_kind=SessionCalendarEvidenceKind.CONTENT_CHECKSUM_ONLY,
        source_receipt_hash=None,
        quality_status="NOT_ASSESSED",
    )
    with pytest.raises(ValueError, match="external calendar receipt"):
        assess_session_window(
            evidence,
            evaluated_at=datetime(
                2026,
                8,
                3,
                10,
                0,
                tzinfo=SHANGHAI,
            ),
        )
    assert assess_session_window(
        evidence,
        evaluated_at=datetime(
            2026,
            8,
            3,
            10,
            0,
            tzinfo=SHANGHAI,
        ),
        require_external_receipt=False,
    ).state is SessionWindowState.ACTIVE


def test_calendar_uses_only_explicit_dates_without_weekday_guessing() -> None:
    with pytest.raises(ValueError, match="absent"):
        _evidence(trading_days=(date(2026, 7, 31), date(2026, 8, 4)))

    explicit_weekend = _evidence(
        trade_date=date(2026, 8, 1),
        trading_days=(date(2026, 7, 31), date(2026, 8, 1)),
        available_at=datetime(2026, 7, 31, 16, 0, tzinfo=SHANGHAI),
    )
    assert assess_session_window(
        explicit_weekend,
        evaluated_at=datetime(2026, 8, 1, 10, 0, tzinfo=SHANGHAI),
    ).state is SessionWindowState.ACTIVE


def test_late_calendar_evidence_is_allowed_but_never_retroactive() -> None:
    evidence = _evidence(
        available_at=datetime(2026, 8, 3, 15, 30, tzinfo=SHANGHAI)
    )
    assert assess_session_window(
        evidence,
        evaluated_at=datetime(2026, 8, 3, 15, 0, tzinfo=SHANGHAI),
    ).state is SessionWindowState.NOT_OBSERVABLE
    assert assess_session_window(
        evidence,
        evaluated_at=datetime(2026, 8, 3, 15, 30, tzinfo=SHANGHAI),
    ).state is SessionWindowState.CLOSED
    with pytest.raises(ValueError, match="not observable"):
        derive_limit_day_execution_window(
            evidence,
            order_created_at=datetime(
                2026,
                8,
                3,
                14,
                0,
                tzinfo=SHANGHAI,
            ),
        )
    with pytest.raises(ValueError, match="final session close"):
        derive_limit_day_execution_window(
            evidence,
            order_created_at=datetime(
                2026,
                8,
                3,
                15,
                30,
                tzinfo=SHANGHAI,
            ),
        )


def test_sessions_must_be_same_day_wall_times_and_non_overlapping() -> None:
    with pytest.raises(ValueError, match="overlap"):
        _evidence(
            sessions=(
                LocalTradingSession("A", time(9), time(12)),
                LocalTradingSession("B", time(11), time(15)),
            )
        )
    with pytest.raises(ValueError, match="without tzinfo"):
        LocalTradingSession(
            "BAD",
            time(9, tzinfo=timezone.utc),
            time(10, tzinfo=timezone.utc),
        )


def test_calendar_hash_is_deterministic_across_session_input_order() -> None:
    first = _evidence()
    second = _evidence(sessions=tuple(reversed(_sessions())))
    assert first.sessions == second.sessions
    assert first.calendar_hash == second.calendar_hash
    assert first.evidence_hash == second.evidence_hash


class _DateSubclass(date):
    pass


class _DatetimeSubclass(datetime):
    pass


class _TimeSubclass(time):
    pass


@pytest.mark.parametrize(
    "factory",
    (
        lambda: _evidence(
            trade_date=_DateSubclass(2026, 8, 3),
        ),
        lambda: _evidence(
            available_at=_DatetimeSubclass(
                2026,
                8,
                2,
                16,
                tzinfo=SHANGHAI,
            ),
        ),
        lambda: LocalTradingSession(
            "BAD",
            _TimeSubclass(9, 30),
            time(10),
        ),
        lambda: assess_session_window(
            _evidence(),
            evaluated_at=datetime(2026, 8, 3, 10, 0),
        ),
    ),
)
def test_session_contract_rejects_naive_and_base_type_subclasses(factory) -> None:
    with pytest.raises((TypeError, ValueError)):
        factory()


def test_low_level_frozen_evidence_mutation_is_revalidated() -> None:
    forged = replace(_evidence())
    object.__setattr__(forged, "evidence_hash", "c" * 64)
    with pytest.raises(ValueError, match="canonical reconstructed"):
        assess_session_window(
            forged,
            evaluated_at=datetime(
                2026,
                8,
                3,
                10,
                0,
                tzinfo=SHANGHAI,
            ),
        )


def test_outputs_are_immutable() -> None:
    assessment = assess_session_window(
        _evidence(),
        evaluated_at=datetime(
            2026,
            8,
            3,
            10,
            0,
            tzinfo=SHANGHAI,
        ),
    )
    with pytest.raises(FrozenInstanceError):
        assessment.session_id = "AFTERNOON"  # type: ignore[misc]
