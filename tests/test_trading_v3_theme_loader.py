from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import create_engine, text

from server.trading_v3.daily_features import _load_theme_memberships


def _engine():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE qmt_membership_snapshot_run (
                snapshot_date DATE NOT NULL,
                source TEXT NOT NULL,
                quality_status TEXT NOT NULL,
                concept_relation_count INTEGER NOT NULL,
                captured_at DATETIME NOT NULL
            )
        """))
        connection.execute(text("""
            CREATE TABLE qmt_concept_member_snapshot (
                snapshot_date DATE NOT NULL,
                source TEXT NOT NULL,
                concept_code TEXT NOT NULL,
                concept_name TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                quality_status TEXT NOT NULL,
                captured_at DATETIME NOT NULL
            )
        """))
    return engine


def _insert_snapshot(
    engine,
    *,
    snapshot_date: date,
    captured_at: datetime,
    expected_relations: int = 1,
) -> None:
    with engine.begin() as connection:
        connection.execute(
            text("""
                INSERT INTO qmt_membership_snapshot_run
                    (snapshot_date, source, quality_status,
                     concept_relation_count, captured_at)
                VALUES
                    (:snapshot_date, 'QMT_LOCAL', 'QMT_VALIDATED',
                     :expected_relations, :captured_at)
            """),
            {
                "snapshot_date": snapshot_date,
                "expected_relations": expected_relations,
                "captured_at": captured_at,
            },
        )
        connection.execute(
            text("""
                INSERT INTO qmt_concept_member_snapshot
                    (snapshot_date, source, concept_code, concept_name,
                     stock_code, quality_status, captured_at)
                VALUES
                    (:snapshot_date, 'QMT_LOCAL', 'AI_APP', 'AI application',
                     '000001', 'QMT_VALIDATED', :captured_at)
            """),
            {"snapshot_date": snapshot_date, "captured_at": captured_at},
        )


def test_loader_accepts_complete_snapshot_known_by_signal_date():
    engine = _engine()
    _insert_snapshot(
        engine,
        snapshot_date=date(2026, 7, 30),
        captured_at=datetime(2026, 7, 30, 18, 0),
    )

    memberships, snapshot_date = _load_theme_memberships(
        engine,
        as_of=date(2026, 7, 30),
        codes=["000001"],
        industries={"000001": ("BANK", "Bank")},
    )

    assert snapshot_date == date(2026, 7, 30)
    assert ("BANK", "Bank", "industry") in memberships["000001"]
    assert ("AI_APP", "AI application", "concept") in memberships["000001"]


def test_loader_rejects_snapshot_recorded_after_historical_signal_date():
    engine = _engine()
    _insert_snapshot(
        engine,
        snapshot_date=date(2026, 7, 30),
        captured_at=datetime(2026, 8, 2, 12, 0),
    )

    memberships, snapshot_date = _load_theme_memberships(
        engine,
        as_of=date(2026, 7, 30),
        codes=["000001"],
        industries={"000001": ("BANK", "Bank")},
    )

    assert snapshot_date is None
    assert memberships == {"000001": [("BANK", "Bank", "industry")]}


def test_loader_rejects_incomplete_snapshot_batch():
    engine = _engine()
    _insert_snapshot(
        engine,
        snapshot_date=date(2026, 7, 30),
        captured_at=datetime(2026, 7, 30, 18, 0),
        expected_relations=2,
    )

    memberships, snapshot_date = _load_theme_memberships(
        engine,
        as_of=date(2026, 7, 30),
        codes=["000001"],
        industries={"000001": ("BANK", "Bank")},
    )

    assert snapshot_date is None
    assert memberships == {"000001": [("BANK", "Bank", "industry")]}
