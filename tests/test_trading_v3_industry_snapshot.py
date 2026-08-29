from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine, text

from integrations.bigqmt.reference import PROVIDER_ID
from server.engine.strategy_industry_history import _canonical_qmt_industry_hash
from server.trading_v3.daily_features import _load_industries


def test_industry_loader_uses_complete_snapshot_known_by_signal_date():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    industry_hash = _canonical_qmt_industry_hash([{
        "industry_code": "801010",
        "industry_name": "Bank",
        "industry_type": "申万一级",
        "stock_code": "000001",
        "short_name": "Ping An Bank",
    }])
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
            INSERT INTO qmt_membership_snapshot_run
                (snapshot_date, source, quality_status,
                 capture_mode, industry_count, industry_relation_count,
                 industry_hash, captured_at)
            VALUES
                ('2026-08-11', :source, 'QMT_VALIDATED',
                 'qmt_close_full_refresh', 1, 1, :industry_hash,
                 '2026-08-11 18:00:00')
        """), {"source": PROVIDER_ID, "industry_hash": industry_hash})
        connection.execute(text("""
            INSERT INTO qmt_industry_member_snapshot
                (snapshot_date, source, industry_code, industry_name,
                 industry_type, stock_code, short_name, quality_status,
                 captured_at)
            VALUES
                ('2026-08-11', :source, '801010', 'Bank',
                 '申万一级', '000001', 'Ping An Bank', 'QMT_VALIDATED',
                 '2026-08-11 18:00:00')
        """), {"source": PROVIDER_ID})

    result = _load_industries(engine, ["000001"], as_of=date(2026, 8, 11))

    assert result == {"000001": ("801010", "Bank")}
