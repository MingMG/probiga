from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from server.trading_v3.theme_history import (
    CombinedEvidenceStatus,
    EvidenceStatus,
    ThemeMembershipRecord,
    ThemeMembershipSnapshot,
    ThemeNewsNoveltyRecord,
    ThemeNewsSnapshot,
    resolve_theme_history,
    validate_membership_snapshot,
    validate_news_snapshot,
)


CN = ZoneInfo("Asia/Shanghai")


def _recorded(value: str) -> datetime:
    return datetime.fromisoformat(value).replace(tzinfo=CN)


def _membership_snapshot(
    *,
    snapshot_id: str,
    as_of: date,
    recorded_at: datetime,
    stock_code: str,
    theme_code: str,
    effective_from: date,
    effective_to: date | None = None,
) -> ThemeMembershipSnapshot:
    return ThemeMembershipSnapshot(
        snapshot_id=snapshot_id,
        as_of=as_of,
        recorded_at=recorded_at,
        is_complete=True,
        records=(
            ThemeMembershipRecord(
                stock_code=stock_code,
                theme_code=theme_code,
                theme_name=theme_code,
                source="qmt_concept",
                effective_from=effective_from,
                effective_to=effective_to,
            ),
        ),
    )


def _news_snapshot(
    *,
    snapshot_id: str,
    as_of: date,
    recorded_at: datetime,
    theme_code: str,
    score: float,
    effective_from: date,
    effective_to: date | None = None,
) -> ThemeNewsSnapshot:
    return ThemeNewsSnapshot(
        snapshot_id=snapshot_id,
        as_of=as_of,
        recorded_at=recorded_at,
        lookback_start=as_of.replace(day=max(1, as_of.day - 6)),
        lookback_end=as_of,
        is_complete=True,
        records=(
            ThemeNewsNoveltyRecord(
                theme_code=theme_code,
                theme_name=theme_code,
                novelty_score=score,
                evidence_count=1,
                evidence_ids=(f"news-{snapshot_id}",),
                source="licensed_news",
                effective_from=effective_from,
                effective_to=effective_to,
            ),
        ),
    )


def test_resolves_only_membership_and_news_visible_on_signal_date():
    old_membership = _membership_snapshot(
        snapshot_id="membership-2025-01-10",
        as_of=date(2025, 1, 10),
        recorded_at=_recorded("2025-01-10T16:00:00"),
        stock_code="000001",
        theme_code="OLD_THEME",
        effective_from=date(2025, 1, 1),
    )
    current_membership = _membership_snapshot(
        snapshot_id="membership-2026-08-01",
        as_of=date(2026, 8, 1),
        recorded_at=_recorded("2026-08-01T16:00:00"),
        stock_code="000001",
        theme_code="CURRENT_THEME",
        effective_from=date(2025, 1, 1),
    )
    news = _news_snapshot(
        snapshot_id="news-2025-01-10",
        as_of=date(2025, 1, 10),
        recorded_at=_recorded("2025-01-10T16:01:00"),
        theme_code="OLD_THEME",
        score=0.72,
        effective_from=date(2025, 1, 10),
        effective_to=date(2025, 1, 11),
    )

    view = resolve_theme_history(
        signal_date=date(2025, 1, 10),
        membership_snapshots=(old_membership, current_membership),
        news_snapshots=(news,),
    )

    assert view.memberships == {
        "000001": [("OLD_THEME", "OLD_THEME", "qmt_concept")]
    }
    assert view.news_novelty == {"OLD_THEME": 0.72}
    assert view.membership_snapshot_id == "membership-2025-01-10"
    assert view.evidence_status is CombinedEvidenceStatus.COMPLETE


def test_current_membership_snapshot_never_backfills_historical_signal():
    current = _membership_snapshot(
        snapshot_id="current",
        as_of=date(2026, 8, 1),
        recorded_at=_recorded("2026-08-01T16:00:00"),
        stock_code="000001",
        theme_code="AI_APPLICATION",
        effective_from=date(2020, 1, 1),
    )

    view = resolve_theme_history(
        signal_date=date(2025, 1, 10),
        membership_snapshots=(current,),
    )

    assert view.memberships == {}
    assert view.membership_evidence_status is EvidenceStatus.FUTURE_ONLY
    assert view.evidence_status is CombinedEvidenceStatus.BLOCKED


def test_late_recorded_snapshot_cannot_disguise_current_data_as_old_as_of():
    disguised_backfill = _membership_snapshot(
        snapshot_id="late-backfill",
        as_of=date(2025, 1, 10),
        recorded_at=_recorded("2026-08-01T16:00:00"),
        stock_code="000001",
        theme_code="ROBOTICS",
        effective_from=date(2025, 1, 1),
    )

    view = resolve_theme_history(
        signal_date=date(2025, 1, 10),
        membership_snapshots=(disguised_backfill,),
    )

    assert view.memberships == {}
    assert view.membership_evidence_status is EvidenceStatus.FUTURE_ONLY


@pytest.mark.parametrize("missing_field", ["as_of", "recorded_at"])
def test_missing_membership_snapshot_time_fails_closed(missing_field: str):
    raw = {
        "snapshot_id": "broken",
        "as_of": date(2025, 1, 10),
        "recorded_at": _recorded("2025-01-10T16:00:00"),
        "is_complete": True,
        "records": [],
    }
    raw.pop(missing_field)

    validation = validate_membership_snapshot(raw)
    view = resolve_theme_history(
        signal_date=date(2025, 1, 10),
        membership_snapshots=(raw,),
    )

    assert not validation.is_valid
    assert validation.status is EvidenceStatus.INVALID_INPUT
    assert view.memberships == {}
    assert view.membership_evidence_status is EvidenceStatus.INVALID_INPUT


@pytest.mark.parametrize("missing_field", ["effective_from", "effective_to"])
def test_missing_membership_effective_date_fails_closed(missing_field: str):
    record = {
        "stock_code": "000001",
        "theme_code": "AI_APPLICATION",
        "theme_name": "AI Application",
        "source": "qmt_concept",
        "effective_from": date(2025, 1, 1),
        "effective_to": None,
    }
    record.pop(missing_field)
    raw = {
        "snapshot_id": "broken-record",
        "as_of": date(2025, 1, 10),
        "recorded_at": _recorded("2025-01-10T16:00:00"),
        "is_complete": True,
        "records": [record],
    }

    view = resolve_theme_history(
        signal_date=date(2025, 1, 10),
        membership_snapshots=(raw,),
    )

    assert view.memberships == {}
    assert view.membership_evidence_status is EvidenceStatus.INVALID_INPUT
    assert any(missing_field in error for error in view.errors)


def test_one_invalid_snapshot_blocks_valid_rows_in_same_evidence_class():
    valid = _membership_snapshot(
        snapshot_id="valid",
        as_of=date(2025, 1, 10),
        recorded_at=_recorded("2025-01-10T16:00:00"),
        stock_code="000001",
        theme_code="VALID_THEME",
        effective_from=date(2025, 1, 1),
    )
    invalid = {
        "snapshot_id": "unknown-time",
        "recorded_at": _recorded("2025-01-10T16:00:00"),
        "is_complete": True,
        "records": [],
    }

    view = resolve_theme_history(
        signal_date=date(2025, 1, 10),
        membership_snapshots=(valid, invalid),
    )

    assert view.memberships == {}
    assert view.membership_evidence_status is EvidenceStatus.INVALID_INPUT


def test_effective_intervals_are_half_open_at_signal_date():
    membership = _membership_snapshot(
        snapshot_id="membership-ended",
        as_of=date(2025, 1, 10),
        recorded_at=_recorded("2025-01-10T15:00:00"),
        stock_code="000001",
        theme_code="ENDED_THEME",
        effective_from=date(2025, 1, 1),
        effective_to=date(2025, 1, 10),
    )
    news = _news_snapshot(
        snapshot_id="news-ended",
        as_of=date(2025, 1, 10),
        recorded_at=_recorded("2025-01-10T15:01:00"),
        theme_code="ENDED_THEME",
        score=0.9,
        effective_from=date(2025, 1, 9),
        effective_to=date(2025, 1, 10),
    )

    view = resolve_theme_history(
        signal_date=date(2025, 1, 10),
        membership_snapshots=(membership,),
        news_snapshots=(news,),
    )

    assert view.memberships == {}
    assert view.news_novelty == {}
    assert view.evidence_status is CombinedEvidenceStatus.COMPLETE


def test_stale_news_is_not_reused_for_a_later_signal_date():
    news = _news_snapshot(
        snapshot_id="news-yesterday",
        as_of=date(2025, 1, 9),
        recorded_at=_recorded("2025-01-09T16:00:00"),
        theme_code="AI_APPLICATION",
        score=0.8,
        effective_from=date(2025, 1, 9),
        effective_to=date(2025, 1, 11),
    )

    view = resolve_theme_history(
        signal_date=date(2025, 1, 10),
        news_snapshots=(news,),
    )

    assert view.news_novelty == {}
    assert view.news_evidence_status is EvidenceStatus.STALE_SNAPSHOT
    assert view.news_age_days == 1


def test_missing_news_time_or_effective_date_fails_closed():
    raw = {
        "snapshot_id": "broken-news",
        "recorded_at": _recorded("2025-01-10T16:00:00"),
        "lookback_start": date(2025, 1, 4),
        "lookback_end": date(2025, 1, 10),
        "is_complete": True,
        "records": [
            {
                "theme_code": "AI_APPLICATION",
                "theme_name": "AI Application",
                "novelty_score": 0.8,
                "evidence_count": 1,
                "evidence_ids": ["news-1"],
                "source": "licensed_news",
                "effective_to": date(2025, 1, 11),
            }
        ],
    }

    validation = validate_news_snapshot(raw)
    view = resolve_theme_history(
        signal_date=date(2025, 1, 10),
        news_snapshots=(raw,),
    )

    assert not validation.is_valid
    assert view.news_novelty == {}
    assert view.news_evidence_status is EvidenceStatus.INVALID_INPUT
    assert any("as_of" in error for error in view.errors)
    assert any("effective_from" in error for error in view.errors)


def test_validation_rejects_naive_recording_time_and_invalid_novelty():
    raw = {
        "snapshot_id": "bad-news",
        "as_of": date(2025, 1, 10),
        "recorded_at": datetime(2025, 1, 10, 16, 0),
        "lookback_start": date(2025, 1, 4),
        "lookback_end": date(2025, 1, 10),
        "is_complete": True,
        "records": [
            {
                "theme_code": "AI_APPLICATION",
                "theme_name": "AI Application",
                "novelty_score": 1.01,
                "evidence_count": 0,
                "evidence_ids": [],
                "source": "licensed_news",
                "effective_from": date(2025, 1, 10),
                "effective_to": date(2025, 1, 11),
            }
        ],
    }

    validation = validate_news_snapshot(raw)

    assert not validation.is_valid
    assert any("timezone" in error for error in validation.errors)
    assert any("novelty_score" in error for error in validation.errors)


def test_incomplete_snapshot_is_never_accepted_as_point_in_time_evidence():
    raw = {
        "snapshot_id": "partial-membership",
        "as_of": date(2025, 1, 10),
        "recorded_at": _recorded("2025-01-10T16:00:00"),
        "is_complete": False,
        "records": [],
    }

    validation = validate_membership_snapshot(raw)

    assert not validation.is_valid
    assert validation.status is EvidenceStatus.INVALID_INPUT
    assert any("complete snapshot" in error for error in validation.errors)
