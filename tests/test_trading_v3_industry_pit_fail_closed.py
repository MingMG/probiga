from __future__ import annotations

from datetime import date, datetime
import sqlite3

import pytest

from sqlalchemy import create_engine, text

from integrations.bigqmt.membership_snapshot import _canonical_hash as _membership_hash
from integrations.bigqmt.reference import PROVIDER_ID
from server.common.pit_facts import PIT_AVAILABLE, PIT_DATA_BLOCKED
from server.engine.strategy_industry_history import (
    QMT_PREVIOUS_OPEN_SESSION_REASON,
    _canonical_qmt_industry_hash,
)
from server.trading_v3.daily_features import _load_industries, _load_theme_memberships


def _engine():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE qmt_membership_snapshot_run (
                snapshot_date DATE NOT NULL,
                source TEXT NOT NULL,
                quality_status TEXT NOT NULL,
                capture_mode TEXT NOT NULL,
                industry_count INTEGER NOT NULL,
                industry_relation_count INTEGER NOT NULL,
                industry_hash TEXT NOT NULL,
                concept_relation_count INTEGER NOT NULL DEFAULT 0,
                concept_hash TEXT NOT NULL DEFAULT '',
                captured_at DATETIME NOT NULL
            )
        """))
        connection.execute(text("""
            CREATE TABLE si_trade_calendar (
                trade_date DATE NOT NULL, trade_status INTEGER NOT NULL
            )
        """))
        connection.execute(text("""
            INSERT INTO si_trade_calendar VALUES
                ('2026-08-11', 1), ('2026-08-12', 1), ('2026-08-13', 1),
                ('2026-08-27', 1), ('2026-08-28', 1)
        """))
        connection.execute(text("""
            CREATE TABLE qmt_industry_member_snapshot (
                snapshot_date DATE NOT NULL,
                source TEXT NOT NULL,
                industry_code TEXT NOT NULL,
                industry_name TEXT NOT NULL,
                industry_type TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                short_name TEXT NOT NULL,
                quality_status TEXT NOT NULL,
                captured_at DATETIME NOT NULL
            )
        """))
        connection.execute(text("""
            CREATE TABLE qmt_concept_member_snapshot (
                id INTEGER PRIMARY KEY,
                snapshot_date DATE NOT NULL, source TEXT NOT NULL,
                stock_code TEXT NOT NULL, concept_code TEXT NOT NULL,
                concept_name TEXT NOT NULL, short_name TEXT NOT NULL,
                quality_status TEXT NOT NULL, captured_at DATETIME NOT NULL
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
        connection.execute(text("""
            CREATE TABLE st_strategy_industry_history (
                trade_date DATE NOT NULL,
                stock_code TEXT NOT NULL,
                industry_name TEXT NOT NULL
            )
        """))
        connection.execute(text("""
            INSERT INTO st_strategy_industry_history VALUES
                ('2026-08-24', '000001', 'LEGACY_POISON'),
                ('2026-08-25', '000001', 'LEGACY_POISON')
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
    expected_industry_count: int = 1,
    source: str = PROVIDER_ID,
    published_hash: str | None = None,
    member_captured_at: str | None = None,
    member_quality_status: str = "QMT_VALIDATED",
):
    member = {
        "industry_code": industry_code,
        "industry_name": industry_name,
        "industry_type": "申万一级",
        "stock_code": "000001",
        "short_name": "平安银行",
    }
    industry_hash = published_hash or _canonical_qmt_industry_hash([member])
    with engine.begin() as connection:
        connection.execute(
            text("""
                INSERT INTO qmt_membership_snapshot_run
                    (snapshot_date, source, quality_status,
                     capture_mode, industry_count, industry_relation_count,
                     industry_hash, captured_at)
                VALUES (:snapshot_date, :source, 'QMT_VALIDATED',
                        'qmt_close_full_refresh', :expected_industry_count,
                        :expected_count, :industry_hash, :captured_at)
            """),
            {
                "snapshot_date": snapshot_date,
                "source": source,
                "expected_industry_count": expected_industry_count,
                "expected_count": expected_count,
                "industry_hash": industry_hash,
                "captured_at": captured_at,
            },
        )
        connection.execute(
            text("""
                INSERT INTO qmt_industry_member_snapshot
                    (snapshot_date, source, industry_code, industry_name,
                     industry_type, stock_code, short_name, quality_status,
                     captured_at)
                VALUES (:snapshot_date, :source, :industry_code,
                        :industry_name, '申万一级', '000001',
                        '平安银行', :member_quality_status,
                        :member_captured_at)
            """),
            {
                "snapshot_date": snapshot_date,
                "source": source,
                "industry_code": industry_code,
                "industry_name": industry_name,
                "member_quality_status": member_quality_status,
                "member_captured_at": member_captured_at or captured_at,
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


def test_missing_exact_date_uses_only_the_previous_open_session():
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
    assert result == {"000001": ("801780", "银行")}
    assert evidence["status"] == PIT_AVAILABLE
    assert evidence["target_snapshot_date"] == "2026-08-12"
    assert evidence["source_snapshot_date"] == "2026-08-11"
    assert evidence["captured_at"] == "2026-08-11T18:00:00"
    assert evidence["fallback_reason"] == QMT_PREVIOUS_OPEN_SESSION_REASON
    assert evidence["retrospective_last_known"] is False

    stale, stale_evidence = _load_industries(
        engine,
        ["000001"],
        as_of=date(2026, 8, 13),
        decision_at=datetime(2026, 8, 13, 19),
        include_evidence=True,
    )
    assert stale == {}
    assert stale_evidence["reason"] == "PIT_INDUSTRY_EXACT_DATE_SNAPSHOT_MISSING"


def test_late_valid_target_does_not_hide_previous_open_session_at_the_cutoff():
    engine = _engine()
    for snapshot_date, hour, code, name in (
        ("2026-08-11", 18, "801780", "银行"),
        ("2026-08-12", 20, "801080", "电子"),
    ):
        _insert_snapshot(
            engine, snapshot_date=snapshot_date,
            captured_at=f"{snapshot_date} {hour}:00:00",
            industry_code=code, industry_name=name,
        )

    prior, evidence = _load_industries(
        engine, ["000001"], as_of=date(2026, 8, 12),
        decision_at=datetime(2026, 8, 12, 19), include_evidence=True,
    )
    exact = _load_industries(
        engine, ["000001"], as_of=date(2026, 8, 12),
        decision_at=datetime(2026, 8, 12, 21),
    )
    assert prior == {"000001": ("801780", "银行")}
    assert evidence["source_snapshot_date"] == "2026-08-11"
    assert exact == {"000001": ("801080", "电子")}


def test_formal_concepts_bind_cutoff_and_prior_date_while_research_may_use_older():
    engine = _engine()
    _insert_snapshot(
        engine, snapshot_date="2026-08-11", captured_at="2026-08-11 18:00:00",
        industry_code="801780", industry_name="银行",
    )
    _insert_snapshot(
        engine, snapshot_date="2026-08-12", captured_at="2026-08-12 20:00:00",
        industry_code="801780", industry_name="银行",
    )
    with engine.begin() as connection:
        connection.execute(text("""
            UPDATE qmt_membership_snapshot_run
            SET concept_relation_count = 1, concept_hash = :concept_hash
            WHERE snapshot_date = '2026-08-11'
        """), {"concept_hash": _membership_hash([("A", "人工智能", "000001", "平安银行")])})
        connection.execute(text("""
            INSERT INTO qmt_concept_member_snapshot
                (snapshot_date,source,stock_code,concept_code,concept_name,short_name,quality_status,captured_at)
            VALUES ('2026-08-11', :source, '000001', 'A', '人工智能',
                    '平安银行', 'QMT_VALIDATED', '2026-08-11 18:00:00')
        """), {"source": PROVIDER_ID})
    kwargs = {"codes": ["000001"], "industries": {"000001": ("801780", "银行")}}
    prior, source_date = _load_theme_memberships(
        engine, as_of=date(2026, 8, 12),
        decision_at=datetime(2026, 8, 12, 19), **kwargs,
    )
    assert prior["000001"] == [("801780", "银行", "industry"), ("A", "人工智能", "concept")]
    assert source_date == date(2026, 8, 11)

    formal, source_date = _load_theme_memberships(
        engine, as_of=date(2026, 8, 13),
        decision_at=datetime(2026, 8, 13, 21), **kwargs,
    )
    assert formal["000001"] == [("801780", "银行", "industry")]
    assert source_date is None
    with engine.begin() as connection:
        connection.execute(text("DELETE FROM qmt_membership_snapshot_run WHERE snapshot_date = '2026-08-12'"))

    research, source_date = _load_theme_memberships(
        engine, as_of=date(2026, 8, 13),
        decision_at=datetime(2026, 8, 13, 21),
        allow_research_last_known=True, **kwargs,
    )
    assert research == prior
    assert source_date == date(2026, 8, 11)

    with engine.begin() as connection:
        connection.execute(text("UPDATE qmt_concept_member_snapshot SET concept_name = 'tampered'"))
    tampered, source_date = _load_theme_memberships(
        engine, as_of=date(2026, 8, 12),
        decision_at=datetime(2026, 8, 12, 19), **kwargs,
    )
    assert tampered["000001"] == [("801780", "银行", "industry")]
    assert source_date is None


def _concept_reader_engine():
    engine=_engine()
    _insert_snapshot(engine,snapshot_date='2026-08-11',captured_at='2026-08-11 18:00:00',
                     industry_code='801780',industry_name='银行')
    with engine.begin() as c:
        c.execute(text('''UPDATE qmt_membership_snapshot_run SET concept_relation_count=1,
            concept_hash=:digest'''),{'digest':_membership_hash([('A','人工智能','000001','平安银行')])})
        c.execute(text('''INSERT INTO qmt_concept_member_snapshot
            (snapshot_date,source,stock_code,concept_code,concept_name,short_name,quality_status,captured_at)
            VALUES ('2026-08-11',:source,'000001','A','人工智能','平安银行',
                    'QMT_VALIDATED','2026-08-11 18:00:00')'''),{'source':PROVIDER_ID})
    return engine


def test_concept_full_market_read_does_not_expand_thousands_of_stock_parameters():
    engine=_concept_reader_engine()
    with engine.connect() as c:
        c.connection.driver_connection.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER,999)
    rows,snapshot=_load_theme_memberships(engine,as_of=date(2026,8,11),
        decision_at=datetime(2026,8,11,20),codes=[f'{i:06d}' for i in range(1,4756)],industries={})
    assert snapshot==date(2026,8,11)
    assert rows['000001']==[('A','人工智能','concept')]
    engine.dispose()


def test_concept_member_read_failure_is_not_reported_as_missing_snapshot():
    engine=_concept_reader_engine()
    with engine.connect() as c:
        dbapi=c.connection.driver_connection
    def authorize(action,table,column,*_):
        return (sqlite3.SQLITE_DENY if action==sqlite3.SQLITE_READ
                and table=='qmt_concept_member_snapshot' and column=='concept_name' else sqlite3.SQLITE_OK)
    dbapi.set_authorizer(authorize)
    try:
        with pytest.raises(RuntimeError,match='QMT_CONCEPT_SNAPSHOT_READ_FAILED:MEMBERS'):
            _load_theme_memberships(engine,as_of=date(2026,8,11),decision_at=datetime(2026,8,11,20),
                codes=['000001'],industries={})
    finally:
        dbapi.set_authorizer(None)
        engine.dispose()


def test_retrospective_research_may_use_verified_last_known_snapshot():
    engine = _engine()
    _insert_snapshot(
        engine,
        snapshot_date="2026-09-02",
        captured_at="2026-09-02 18:00:00",
        industry_code="801780",
        industry_name="银行",
    )

    result, evidence = _load_industries(
        engine,
        ["000001"],
        as_of=date(2026, 9, 3),
        decision_at=datetime(2026, 9, 6, 21),
        include_evidence=True,
        allow_research_last_known=True,
    )

    assert result == {"000001": ("801780", "银行")}
    assert evidence["status"] == PIT_AVAILABLE
    assert evidence["target_snapshot_date"] == "2026-09-03"
    assert evidence["source_snapshot_date"] == "2026-09-02"
    assert evidence["snapshot_age_days"] == 1
    assert evidence["retrospective_last_known"] is True
    assert evidence["fallback_reason"] == (
        "RETROSPECTIVE_RESEARCH_LAST_KNOWN_QMT_SNAPSHOT"
    )


def test_retrospective_last_known_requires_snapshot_captured_by_target_day_end():
    engine = _engine()
    _insert_snapshot(
        engine,
        snapshot_date="2026-09-02",
        captured_at="2026-09-04 00:00:00",
        industry_code="801780",
        industry_name="银行",
    )

    result, evidence = _load_industries(
        engine,
        ["000001"],
        as_of=date(2026, 9, 3),
        decision_at=datetime(2026, 9, 6, 21),
        include_evidence=True,
        allow_research_last_known=True,
    )

    assert result == {}
    assert evidence["status"] == PIT_DATA_BLOCKED
    assert evidence["reason"] == "PIT_INDUSTRY_EXACT_DATE_SNAPSHOT_MISSING"


def test_retrospective_exact_snapshot_must_also_exist_by_target_day_end():
    engine = _engine()
    _insert_snapshot(
        engine,
        snapshot_date="2026-09-03",
        captured_at="2026-09-04 00:00:00",
        industry_code="801780",
        industry_name="银行",
    )

    result, evidence = _load_industries(
        engine,
        ["000001"],
        as_of=date(2026, 9, 3),
        decision_at=datetime(2026, 9, 6, 21),
        include_evidence=True,
        allow_research_last_known=True,
    )

    assert result == {}
    assert evidence["status"] == PIT_DATA_BLOCKED
    assert evidence["reason"] == "PIT_INDUSTRY_SNAPSHOT_PROVENANCE_INVALID"


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


def test_authorized_previous_session_fallback_is_explicit_and_bounded():
    engine = _engine()
    _insert_snapshot(
        engine,
        snapshot_date="2026-08-27",
        captured_at="2026-08-27 18:00:00",
        industry_code="801780",
        industry_name="银行",
    )

    result, evidence = _load_industries(
        engine,
        ["000001"],
        as_of=date(2026, 8, 28),
        decision_at=datetime(2026, 8, 28, 19),
        include_evidence=True,
    )

    assert result == {"000001": ("801780", "银行")}
    assert evidence["status"] == PIT_AVAILABLE
    assert evidence["target_snapshot_date"] == "2026-08-28"
    assert evidence["source_snapshot_date"] == "2026-08-27"
    assert evidence["capture_mode"] == "qmt_close_full_refresh"
    assert evidence["fallback_reason"] == (
        "QMT_HISTORICAL_SECTOR_API_UNAVAILABLE"
    )
    assert evidence["previous_session_fallback"] is True
    assert "LEGACY_POISON" not in str(result)
    assert "LEGACY_POISON" not in str(evidence)


def test_late_target_run_blocks_previous_session_fallback():
    engine = _engine()
    _insert_snapshot(
        engine,
        snapshot_date="2026-08-27",
        captured_at="2026-08-27 18:00:00",
        industry_code="801780",
        industry_name="银行",
    )
    _insert_snapshot(
        engine,
        snapshot_date="2026-08-28",
        captured_at="2026-08-29 00:01:00",
        industry_code="801080",
        industry_name="电子",
    )

    result, evidence = _load_industries(
        engine,
        ["000001"],
        as_of=date(2026, 8, 28),
        decision_at=datetime(2026, 8, 28, 19),
        include_evidence=True,
    )

    assert result == {}
    assert evidence["status"] == PIT_DATA_BLOCKED
    assert evidence["reason"] == "PIT_INDUSTRY_SNAPSHOT_PROVENANCE_INVALID"
    assert evidence.get("previous_session_fallback") is not True


def test_exact_snapshot_requires_provider_counts_hash_and_member_contract():
    corruptions = (
        {"source": "OTHER_PROVIDER"},
        {"expected_industry_count": 2},
        {"published_hash": "a" * 64},
        {"member_captured_at": "2026-08-11 18:00:01"},
        {"member_quality_status": "PARTIAL"},
    )
    for corruption in corruptions:
        engine = _engine()
        _insert_snapshot(
            engine,
            snapshot_date="2026-08-11",
            captured_at="2026-08-11 18:00:00",
            industry_code="801780",
            industry_name="银行",
            **corruption,
        )

        result, evidence = _load_industries(
            engine,
            ["000001"],
            as_of=date(2026, 8, 11),
            decision_at=datetime(2026, 8, 11, 19),
            include_evidence=True,
        )

        assert result == {}, corruption
        assert evidence["status"] == PIT_DATA_BLOCKED, corruption
        assert evidence["reason"] in {
            "PIT_INDUSTRY_SNAPSHOT_INCOMPLETE",
            "PIT_INDUSTRY_SNAPSHOT_PROVENANCE_INVALID",
        }, corruption


def test_authorized_fallback_rejects_corrupt_previous_session_hash():
    engine = _engine()
    _insert_snapshot(
        engine,
        snapshot_date="2026-08-27",
        captured_at="2026-08-27 18:00:00",
        industry_code="801780",
        industry_name="银行",
        published_hash="b" * 64,
    )

    result, evidence = _load_industries(
        engine,
        ["000001"],
        as_of=date(2026, 8, 28),
        decision_at=datetime(2026, 8, 28, 19),
        include_evidence=True,
    )

    assert result == {}
    assert evidence["status"] == PIT_DATA_BLOCKED
    assert evidence["reason"] == "PIT_INDUSTRY_SNAPSHOT_PROVENANCE_INVALID"
    assert evidence.get("previous_session_fallback") is not True
