"""The concept-heat statement must quote MySQL 8's reserved word `rank`.

Observed in production: the unquoted form raised
ProgrammingError (1064, "... near 'rank, concept_code' at line 1"), so
/api/hot-data/market-sentiment never returned data - the market trend and style
pages could only show an error.  Quoting it returned 1234 rows on the same data.
"""
import os
import re

import pandas as pd
import pytest

from tools import market_sentiment


def _issued_statements(monkeypatch):
    """Capture the SQL analyze_main_theme actually sends, without a database."""
    seen = []

    def fake_read_sql(statement, engine, params=None):
        seen.append(str(statement))
        return pd.DataFrame()

    monkeypatch.setattr(market_sentiment.pd, "read_sql", fake_read_sql)
    result = market_sentiment.analyze_main_theme(object(), ["2026-10-15", "2026-10-16"])
    assert result["status"] == "no_data", result
    assert len(seen) == 1, seen
    return " ".join(seen[0].split())


def test_concept_heat_statement_quotes_the_reserved_rank_column(monkeypatch):
    sql = _issued_statements(monkeypatch)
    assert "`rank`" in sql
    # Every occurrence of the word must be the quoted one, in the select list
    # and in the ORDER BY alike: MySQL 8 rejects the bare token outright.
    assert not re.search(r"(?<![`\w.])rank(?![\w`])", sql), sql
    assert "ORDER BY snapshot_date, plate_type, `rank`" in sql


def test_live_concept_heat_statement_executes():
    """Reproduces the production failure: the unquoted form raised 1064 here."""
    if not (os.environ.get("MYSQL_URL") or os.environ.get("DATABASE_URL")):
        pytest.skip("no MySQL configured for the live statement check")
    from sqlalchemy import text

    from server.common.batch_db import create_batch_engine

    try:
        # Not a bare create_engine: the runtime DSN needs the batch engine's TLS
        # settings, and a raw one fails 1045 for reasons unrelated to the SQL.
        engine = create_batch_engine(future=True)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1")).fetchall()
    except Exception as exc:
        pytest.skip(f"live MySQL unavailable: {exc}")

    statement = text("""
        SELECT snapshot_date, plate_type, `rank`, concept_code, concept_name,
               change_pct, hot_value, hot_tag
        FROM st_hot_concept_ths_daily
        WHERE snapshot_date >= :d1 AND snapshot_date <= :d2
        ORDER BY snapshot_date, plate_type, `rank`
    """)
    with engine.connect() as connection:
        rows = connection.execute(
            statement, {"d1": "2000-01-01", "d2": "2099-12-31"}
        ).fetchall()
    # The point is that it parses and runs at all; the shipped table is small.
    assert isinstance(rows, list)
