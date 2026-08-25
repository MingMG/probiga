from __future__ import annotations

from sqlalchemy import create_engine, text
import pytest

from integrations.bigqmt.reference import PROVIDER_ID
from server.engine.strategy_industry_history import (
    IndustrySnapshotIntegrityError,
    IndustrySnapshotNotReady,
    _canonical_qmt_industry_hash,
    _digest,
    build_history_rows,
    capture_industry_history,
)


TARGET = "2026-08-21"
CAPTURED_AT = "2026-08-21 15:12:00"


def _engine():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE qmt_membership_snapshot_run (
                snapshot_date TEXT NOT NULL,
                source TEXT NOT NULL,
                quality_status TEXT NOT NULL,
                capture_mode TEXT NOT NULL,
                industry_count INTEGER NOT NULL,
                industry_relation_count INTEGER NOT NULL,
                industry_hash TEXT NOT NULL,
                captured_at TEXT NOT NULL
            )
        """))
        connection.execute(text("""
            CREATE TABLE qmt_industry_member_snapshot (
                snapshot_date TEXT NOT NULL,
                source TEXT NOT NULL,
                industry_code TEXT NOT NULL,
                industry_name TEXT NOT NULL,
                industry_type TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                short_name TEXT NOT NULL,
                quality_status TEXT NOT NULL,
                captured_at TEXT NOT NULL
            )
        """))
        # Deliberately present a mutable current table.  The production capture
        # path must never query it, including when recovering an older date.
        connection.execute(text("""
            CREATE TABLE si_industry_sw (
                stock_code TEXT, industry_name TEXT, industry_type TEXT
            )
        """))
        connection.execute(text("""
            INSERT INTO si_industry_sw VALUES
            ('000001', '今天的覆盖值', 'L1')
        """))
        connection.execute(text("""
            CREATE TABLE st_strategy_industry_history (
                snapshot_id TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                as_of_exclusive TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                industry_name TEXT NOT NULL,
                industry_type TEXT NOT NULL,
                source_system TEXT NOT NULL,
                source_fact_id TEXT NOT NULL UNIQUE,
                source_effective_at TEXT NOT NULL,
                source_etl_sync_at TEXT NOT NULL,
                row_hash TEXT NOT NULL UNIQUE,
                PRIMARY KEY (snapshot_id, stock_code)
            )
        """))
    return engine


def _row(
    code: str,
    industry_code: str,
    name: str,
    *,
    industry_type: str = "L1",
    trade_date: str = TARGET,
) -> dict:
    return {
        "snapshot_date": trade_date,
        "source": PROVIDER_ID,
        "industry_code": industry_code,
        "industry_name": name,
        "industry_type": industry_type,
        "stock_code": code,
        "short_name": f"股票{code}",
        "quality_status": "QMT_VALIDATED",
        "captured_at": CAPTURED_AT,
    }


def _publish(engine, rows: list[dict], *, trade_date: str = TARGET) -> str:
    industry_hash = _canonical_qmt_industry_hash(rows)
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO qmt_membership_snapshot_run
            (snapshot_date, source, quality_status, capture_mode,
             industry_count, industry_relation_count, industry_hash,
             captured_at)
            VALUES
            (:snapshot_date, :source, 'QMT_VALIDATED',
             'qmt_close_full_refresh', :industry_count, :relation_count,
             :industry_hash, :captured_at)
        """), {
            "snapshot_date": trade_date,
            "source": PROVIDER_ID,
            "industry_count": len({row["industry_code"] for row in rows}),
            "relation_count": len(rows),
            "industry_hash": industry_hash,
            "captured_at": CAPTURED_AT,
        })
        for row in rows:
            connection.execute(text("""
                INSERT INTO qmt_industry_member_snapshot
                (snapshot_date, source, industry_code, industry_name,
                 industry_type, stock_code, short_name, quality_status,
                 captured_at)
                VALUES
                (:snapshot_date, :source, :industry_code, :industry_name,
                 :industry_type, :stock_code, :short_name, :quality_status,
                 :captured_at)
            """), row)
    return industry_hash


def test_history_rows_are_hash_bound_to_verified_qmt_snapshot():
    source = [
        _row("000001", "801780", "银行"),
        _row("600036", "801780", "银行"),
        _row("000001", "801001", "申万市场", industry_type="L0"),
    ]
    industry_hash = _canonical_qmt_industry_hash(source)

    snapshot_id, rows = build_history_rows(
        source,
        trade_date=TARGET,
        source=PROVIDER_ID,
        industry_hash=industry_hash,
        captured_at=CAPTURED_AT,
    )

    assert len(snapshot_id) == 64
    assert [row["stock_code"] for row in rows] == ["000001", "600036"]
    assert all(row["source_system"] == PROVIDER_ID for row in rows)
    assert all(industry_hash in row["source_fact_id"] for row in rows)
    assert all(len(row["source_fact_id"]) <= 160 for row in rows)
    for row in rows:
        assert _digest({
            key: value for key, value in row.items() if key != "row_hash"
        }) == row["row_hash"]


def test_mislabeled_or_preclose_snapshot_time_is_integrity_error():
    source = [_row("000001", "801780", "银行")]
    with pytest.raises(
        IndustrySnapshotIntegrityError, match="收盘后的精确日期"
    ):
        build_history_rows(
            source,
            trade_date=TARGET,
            source=PROVIDER_ID,
            industry_hash=_canonical_qmt_industry_hash(source),
            captured_at="2026-08-20 15:12:00",
        )


def test_exact_date_snapshot_supports_safe_missed_day_recovery_and_idempotency():
    engine = _engine()
    industry_hash = _publish(engine, [
        _row("000001", "801780", "银行"),
        _row("600036", "801780", "银行"),
    ])

    first = capture_industry_history(engine, trade_date=TARGET)
    second = capture_industry_history(engine, trade_date=TARGET)

    assert first["status"] == "COMPLETED"
    assert first["idempotent_replay"] is False
    assert first["source_snapshot_hash"] == industry_hash
    assert first["historical_recovery_source"] == "IMMUTABLE_QMT_EXACT_DATE"
    assert first["mutable_current_table_backfill_allowed"] is False
    assert first["immutable_exact_date_recovery_allowed"] is True
    assert second["idempotent_replay"] is True
    with engine.connect() as connection:
        copied = connection.execute(text(
            "SELECT stock_code, industry_name "
            "FROM st_strategy_industry_history ORDER BY stock_code"
        )).all()
    assert copied == [("000001", "银行"), ("600036", "银行")]


def test_mutable_current_table_cannot_fill_a_missing_historical_snapshot():
    engine = _engine()

    with pytest.raises(IndustrySnapshotNotReady, match="精确日期.*尚未发布"):
        capture_industry_history(engine, trade_date=TARGET)

    with engine.connect() as connection:
        assert connection.execute(text(
            "SELECT COUNT(*) FROM st_strategy_industry_history"
        )).scalar() == 0


def test_nearest_older_qmt_snapshot_cannot_substitute_for_exact_date():
    engine = _engine()
    older = "2026-08-20"
    old_row = _row(
        "000001", "801780", "银行", trade_date=older,
    )
    old_row["captured_at"] = "2026-08-20 15:12:00"
    # Use a local insertion because the helper's capture timestamp is frozen to
    # TARGET for the normal recovery tests.
    industry_hash = _canonical_qmt_industry_hash([old_row])
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO qmt_membership_snapshot_run VALUES
            (:snapshot_date, :source, 'QMT_VALIDATED',
             'qmt_close_full_refresh', 1, 1, :industry_hash, :captured_at)
        """), {
            "snapshot_date": older,
            "source": PROVIDER_ID,
            "industry_hash": industry_hash,
            "captured_at": old_row["captured_at"],
        })
        connection.execute(text("""
            INSERT INTO qmt_industry_member_snapshot VALUES
            (:snapshot_date, :source, :industry_code, :industry_name,
             :industry_type, :stock_code, :short_name, :quality_status,
             :captured_at)
        """), old_row)

    with pytest.raises(IndustrySnapshotNotReady):
        capture_industry_history(engine, trade_date=TARGET)


def test_qmt_row_hash_drift_is_integrity_error_not_retryable_not_ready():
    engine = _engine()
    _publish(engine, [_row("000001", "801780", "银行")])
    with engine.begin() as connection:
        connection.execute(text("""
            UPDATE qmt_industry_member_snapshot
            SET industry_name='被篡改'
            WHERE snapshot_date=:trade_date
        """), {"trade_date": TARGET})

    with pytest.raises(
        IndustrySnapshotIntegrityError, match="canonical hash校验失败"
    ):
        capture_industry_history(engine, trade_date=TARGET)


def test_duplicate_l1_membership_for_one_stock_is_integrity_error():
    engine = _engine()
    _publish(engine, [
        _row("000001", "801780", "银行"),
        _row("000001", "801790", "非银金融"),
    ])

    with pytest.raises(
        IndustrySnapshotIntegrityError, match="重复或不完整"
    ):
        capture_industry_history(engine, trade_date=TARGET)
