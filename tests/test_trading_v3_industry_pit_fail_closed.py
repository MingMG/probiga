from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import create_engine, text

from server.common.pit_facts import PIT_AVAILABLE, PIT_DATA_BLOCKED
from server.trading_v3.daily_features import _load_industries


def _engine():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE qmt_membership_snapshot_run (
                snapshot_date DATE NOT NULL,
                source TEXT NOT NULL,
                quality_status TEXT NOT NULL,
                industry_relation_count INTEGER NOT NULL,
                captured_at DATETIME NOT NULL
            )
        """))
        connection.execute(text("""
            CREATE TABLE qmt_industry_member_snapshot (
                snapshot_date DATE NOT NULL,
                source TEXT NOT NULL,
                industry_code TEXT NOT NULL,
                industry_name TEXT NOT NULL,
                industry_type TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                quality_status TEXT NOT NULL,
                captured_at DATETIME NOT NULL
            )
        """))
        # If the loader ever reintroduces the mutable fallback, these values
        # make the failure visible instead of accidentally returning no row.
        connection.execute(text("""
            CREATE TABLE si_industry_sw (
                stock_code TEXT NOT NULL,
                sw_code TEXT NOT NULL,
                industry_name TEXT NOT NULL,
                industry_type TEXT NOT NULL,
                etl_sync_at DATETIME NOT NULL,
                id INTEGER NOT NULL
            )
        """))
        connection.execute(text("""
            INSERT INTO si_industry_sw VALUES
                ('000001', 'CURRENT', 'Current mutable industry',
                 '申万一级', '2026-08-24 09:00:00', 1)
        """))
    return engine


def _insert_snapshot(
    engine,
    *,
    snapshot_date: str,
    captured_at: str,
    industry_code: str,
    industry_name: str,
    expected_count: int = 1,
):
    with engine.begin() as connection:
        connection.execute(
            text("""
                INSERT INTO qmt_membership_snapshot_run
                    (snapshot_date, source, quality_status,
                     industry_relation_count, captured_at)
                VALUES (:snapshot_date, 'QMT_LOCAL', 'QMT_VALIDATED',
                        :expected_count, :captured_at)
            """),
            {
                "snapshot_date": snapshot_date,
                "expected_count": expected_count,
                "captured_at": captured_at,
            },
        )
        connection.execute(
            text("""
                INSERT INTO qmt_industry_member_snapshot
                    (snapshot_date, source, industry_code, industry_name,
                     industry_type, stock_code, quality_status, captured_at)
                VALUES (:snapshot_date, 'QMT_LOCAL', :industry_code,
                        :industry_name, '申万一级', '000001',
                        'QMT_VALIDATED', :captured_at)
            """),
            {
                "snapshot_date": snapshot_date,
                "industry_code": industry_code,
                "industry_name": industry_name,
                "captured_at": captured_at,
            },
        )


def test_industry_migration_uses_each_exact_date_snapshot():
    engine = _engine()
    _insert_snapshot(
        engine,
        snapshot_date="2026-08-11",
        captured_at="2026-08-11 18:00:00",
        industry_code="801780",
        industry_name="银行",
    )
    _insert_snapshot(
        engine,
        snapshot_date="2026-08-12",
        captured_at="2026-08-12 18:00:00",
        industry_code="801080",
        industry_name="电子",
    )
    before = _load_industries(
        engine,
        ["000001"],
        as_of=date(2026, 8, 11),
        decision_at=datetime(2026, 8, 11, 19),
    )
    after = _load_industries(
        engine,
        ["000001"],
        as_of=date(2026, 8, 12),
        decision_at=datetime(2026, 8, 12, 19),
    )
    assert before == {"000001": ("801780", "银行")}
    assert after == {"000001": ("801080", "电子")}


def test_missing_exact_date_never_uses_stale_or_mutable_current_industry():
    engine = _engine()
    _insert_snapshot(
        engine,
        snapshot_date="2026-08-11",
        captured_at="2026-08-11 18:00:00",
        industry_code="801780",
        industry_name="银行",
    )
    result, evidence = _load_industries(
        engine,
        ["000001"],
        as_of=date(2026, 8, 12),
        decision_at=datetime(2026, 8, 12, 19),
        include_evidence=True,
    )
    assert result == {}
    assert evidence["status"] == PIT_DATA_BLOCKED
    assert evidence["status_by_code"]["000001"] == PIT_DATA_BLOCKED
    assert evidence["reason"] == "PIT_INDUSTRY_EXACT_DATE_SNAPSHOT_MISSING"
    assert evidence["snapshot_hash"]


def test_snapshot_captured_after_decision_is_not_known_yet():
    engine = _engine()
    _insert_snapshot(
        engine,
        snapshot_date="2026-08-11",
        captured_at="2026-08-11 18:00:00",
        industry_code="801780",
        industry_name="银行",
    )
    blocked, evidence = _load_industries(
        engine,
        ["000001"],
        as_of=date(2026, 8, 11),
        decision_at=datetime(2026, 8, 11, 15),
        include_evidence=True,
    )
    assert blocked == {}
    assert evidence["status"] == PIT_DATA_BLOCKED
    assert evidence["reason_by_code"]["000001"]

    available, available_evidence = _load_industries(
        engine,
        ["000001"],
        as_of=date(2026, 8, 11),
        decision_at=datetime(2026, 8, 11, 19),
        include_evidence=True,
    )
    assert available == {"000001": ("801780", "银行")}
    assert available_evidence["status"] == PIT_AVAILABLE


def test_incomplete_or_bad_snapshot_fails_closed_without_current_fallback():
    engine = _engine()
    _insert_snapshot(
        engine,
        snapshot_date="2026-08-11",
        captured_at="2026-08-11 18:00:00",
        industry_code="801780",
        industry_name="银行",
        expected_count=2,
    )
    result, evidence = _load_industries(
        engine,
        ["000001"],
        as_of=date(2026, 8, 11),
        decision_at=datetime(2026, 8, 11, 19),
        include_evidence=True,
    )
    assert result == {}
    assert evidence["reason"] == "PIT_INDUSTRY_SNAPSHOT_INCOMPLETE"

    broken = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with broken.begin() as connection:
        connection.execute(text("""
            CREATE TABLE si_industry_sw (
                stock_code TEXT, sw_code TEXT, industry_name TEXT,
                industry_type TEXT, etl_sync_at DATETIME, id INTEGER
            )
        """))
        connection.execute(text("""
            INSERT INTO si_industry_sw VALUES
                ('000001', 'CURRENT', 'Current mutable industry',
                 '申万一级', '2026-08-11 10:00:00', 1)
        """))
    broken_result, broken_evidence = _load_industries(
        broken,
        ["000001"],
        as_of=date(2026, 8, 11),
        decision_at=datetime(2026, 8, 11, 19),
        include_evidence=True,
    )
    assert broken_result == {}
    assert broken_evidence["reason"].startswith(
        "PIT_INDUSTRY_SCHEMA_OR_CHAIN_INVALID"
    )
