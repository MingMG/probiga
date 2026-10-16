"""The radar's market overview must not scan the whole daily history.

Observed in production as latency, not as a wrong answer.  idx_date_ktype leads
with trade_date and carries k_type second, so `MAX(trade_date) WHERE k_type = 1`
cannot seek: MySQL widens the range to k_type <= 1 and walks the index forward
from the oldest day - 3.6M index rows, measured 3.2-4.8s per request on an
endpoint the command page gives 4s (app.js) .  The reverse-scan form returns the
same value in 0.03-0.12s because it stops at the first match.

The same statement shape feeds the candidate list, where
`COALESCE(r.change_pct, s.change_pct)` named a column st_recommended_stocks does
not have; it raised 1054 on every request and the handler silently fell through
to a reduced field set (no final_trade_score, industry_name, concept_tag,
pop_tag).
"""
import logging
import os
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "server/common/tech_risk.py"

from server.common import tech_risk  # noqa: E402


def test_market_overview_does_not_scan_the_daily_history():
    source = SOURCE.read_text(encoding="utf-8")
    for table in ("sm_stock_kline", "sm_index_kline"):
        assert not re.search(
            r"MAX\(\s*trade_date\s*\)\s*\n?\s*FROM\s*" + table, source, re.I
        ), f"{table} overview is back to the full-range MAX() form"


def _sqlite_kline_query():
    """A query callable over a seeded daily-bar table, so the SQL really runs."""
    from sqlalchemy import create_engine, text

    from server.common.sql_reader import read_sql_rows

    engine = create_engine("sqlite://")
    bars = [
        # (stock_code, trade_date, k_type, change_pct, amount)
        ("600000", "2026-10-08", 1, 1.0, 100.0),
        ("600001", "2026-10-08", 1, -2.0, 200.0),
        ("600000", "2026-10-09", 1, 2.0, 300.0),
        ("600000", "2026-10-10", 1, 3.0, 400.0),
        ("600001", "2026-10-10", 1, -0.5, 500.0),
        ("600000", "2026-10-20", 1, 9.0, 900.0),
        # A weekly bar must never be mistaken for the newest daily one.
        ("600000", "2026-10-25", 8, 50.0, 5000.0),
    ]
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE sm_stock_kline (
                id INTEGER PRIMARY KEY, stock_code TEXT, trade_date TEXT,
                k_type INTEGER, adjust_type INTEGER,
                change_pct REAL, amount REAL)
        """))
        for code, day, k_type, change, amount in bars:
            connection.execute(
                text("""
                    INSERT INTO sm_stock_kline
                        (stock_code, trade_date, k_type, adjust_type, change_pct, amount)
                    VALUES (:code, :day, :k_type, 0, :change, :amount)
                """),
                {"code": code, "day": day, "k_type": k_type,
                 "change": change, "amount": amount},
            )
    return lambda sql, params=None: read_sql_rows(engine, sql, params)


def _overview(query, trade_date):
    rows = tech_risk._load_market_rows(query, trade_date)
    return next((row for row in rows if row.get("_kind") == "overview"), None)


def test_market_overview_takes_the_newest_daily_bar_at_or_before_the_date():
    query = _sqlite_kline_query()
    # 2026-10-15 has no bar and 2026-10-20 is after the bound: 10-10 wins.
    row = _overview(query, "2026-10-15")
    assert row is not None, "the overview vanished instead of falling back a day"
    assert str(row["trade_date"])[:10] == "2026-10-10"
    assert row["total"] == 2
    assert row["up_count"] == 1
    assert row["down_count"] == 1
    assert row["avg_change_pct"] == pytest.approx(1.25)
    assert row["amount"] == pytest.approx(900.0)


def test_market_overview_respects_the_upper_bound_and_the_k_type():
    query = _sqlite_kline_query()
    # The weekly bar (k_type=8) on 10-25 must not become the market's latest day
    # until the daily bar arrives.
    assert str(_overview(query, "2026-10-24")["trade_date"])[:10] == "2026-10-20"
    assert str(_overview(query, "2026-10-17")["trade_date"])[:10] == "2026-10-10"


def test_market_overview_is_absent_before_the_first_bar():
    query = _sqlite_kline_query()
    assert _overview(query, "2026-10-01") is None


def test_a_failing_candidate_query_is_reported_not_swallowed(caplog):
    """The old code hid a permanent 1054 behind a silent fallback."""
    raised = []

    def query(sql, params=None):
        if "final_trade_score" in sql:
            raised.append(sql)
            raise RuntimeError("Unknown column 'r.change_pct' in 'field list'")
        return [{"stock_code": "600000", "ai_score": 1}]

    with caplog.at_level(logging.WARNING):
        rows = tech_risk._load_candidate_rows(query, "2026-10-17")

    assert raised, "the primary candidate query was never attempted"
    assert any("reduced fields" in record.getMessage() for record in caplog.records), \
        [record.getMessage() for record in caplog.records]
    assert rows, "the reduced fallback must still serve a candidate list"


def _live_engine():
    if not (os.environ.get("MYSQL_URL") or os.environ.get("DATABASE_URL")):
        pytest.skip("no MySQL configured for the live query-plan check")
    from sqlalchemy import text

    from server.common.batch_db import create_batch_engine

    try:
        engine = create_batch_engine(future=True)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1")).fetchall()
    except Exception as exc:
        pytest.skip(f"live MySQL unavailable: {exc}")
    return engine


def test_live_overview_plan_stops_at_the_first_matching_bar():
    """Reproduces the production latency: the shipped plan read 8.01e+6 rows.

    EXPLAIN's estimated row count cannot tell the two forms apart - a LIMIT is
    not folded into the estimate - so this reads the actual rows from
    EXPLAIN ANALYZE.  Measured: MAX() read 8.01e+6 index rows in 3917ms, the
    reverse scan reads 1 in 0.106ms.
    """
    from sqlalchemy import text

    engine = _live_engine()
    statement = text("""
        SELECT trade_date FROM sm_stock_kline
        WHERE k_type = 1 AND trade_date <= :trade_date
        ORDER BY trade_date DESC LIMIT 1
    """)
    with engine.connect() as connection:
        plan = connection.execute(
            text("EXPLAIN ANALYZE " + str(statement)), {"trade_date": "2026-10-17"}
        ).fetchall()
    assert plan
    detail = "\n".join(str(row[0]) for row in plan)
    assert "(reverse)" in detail, detail
    actual_rows = re.findall(r"rows=([0-9.e+]+) loops", detail)
    assert actual_rows, detail
    assert all(value == "1" for value in actual_rows), detail


def test_live_recommended_candidates_keep_the_full_field_set(caplog):
    """Reproduces the reduced-fields defect: the primary query used to 1054."""
    from server.common.sql_reader import read_sql_rows

    engine = _live_engine()
    query = lambda sql, params=None: read_sql_rows(engine, sql, params)  # noqa: E731
    with caplog.at_level(logging.WARNING):
        rows = tech_risk._load_candidate_rows(query, None)
    recommended = [row for row in rows if row.get("_source") == "recommended"]
    if not recommended:
        pytest.skip("no published recommendation batch to check")
    assert not any("reduced fields" in record.getMessage() for record in caplog.records), \
        "the candidate query failed and only the reduced fields were served"
    assert {"final_trade_score", "industry_name", "concept_tag", "pop_tag"} <= set(
        recommended[0]
    ), sorted(recommended[0])
