from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine, text

from server.trading_v3.daily_features import _load_industries


def test_industry_loader_uses_complete_snapshot_known_by_signal_date():
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
        connection.execute(text("""
            INSERT INTO qmt_membership_snapshot_run
                (snapshot_date, source, quality_status,
                 industry_relation_count, captured_at)
            VALUES
                ('2026-08-11', 'QMT_LOCAL', 'QMT_VALIDATED', 1,
                 '2026-08-11 18:00:00')
        """))
        connection.execute(text("""
            INSERT INTO qmt_industry_member_snapshot
                (snapshot_date, source, industry_code, industry_name,
                 industry_type, stock_code, quality_status, captured_at)
            VALUES
                ('2026-08-11', 'QMT_LOCAL', '801010', 'Bank',
                 '申万一级', '000001', 'QMT_VALIDATED',
                 '2026-08-11 18:00:00')
        """))

    result = _load_industries(engine, ["000001"], as_of=date(2026, 8, 11))

    assert result == {"000001": ("801010", "Bank")}
