from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import create_engine, text

from integrations.bigqmt.reference import PROVIDER_ID
from server.common.pit_facts import PIT_AVAILABLE, PIT_DATA_BLOCKED
from server.engine.strategy_industry_history import _canonical_qmt_industry_hash
from server.trading_v3.daily_features import _load_industries


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
                ('2026-08-11', 1), ('2026-08-12', 1),
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
