"""The style buckets must be computed server-side, and must not change.

Observed in production as latency on the market-trend and style pages: the
window's whole daily history (83,374 rows for a 16-day window) was pulled over
the tunnel and regrouped in pandas, which cost 10-13s per request on a link that
streams bulk rows at roughly 7,700 rows/s - the fetch time tracked the row count
almost exactly.  The same numbers come back as one row per day from a single
aggregate.

Two things must hold together, so both are asserted against the live table:
the statement returns exactly one row per trade day (no bulk transfer), and its
buckets equal what the pandas split it replaced produced, field by field.
"""
import os
import re

import pandas as pd
import pytest

from tools.market_sentiment import _STYLE_AGGREGATE_SQL, _to_date_str

WINDOW = ("2026-09-17", "2026-10-17")


def _live_engine():
    if not (os.environ.get("MYSQL_URL") or os.environ.get("DATABASE_URL")):
        pytest.skip("no MySQL configured for the live aggregate check")
    from sqlalchemy import text

    from server.common.batch_db import create_batch_engine

    try:
        engine = create_batch_engine(future=True)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1")).fetchall()
    except Exception as exc:
        pytest.skip(f"live MySQL unavailable: {exc}")
    return engine


def test_statement_is_a_per_day_aggregate_not_a_bulk_pull():
    """The row count is bounded by the window, not by the market's row count."""
    normalized = " ".join(_STYLE_AGGREGATE_SQL.split()).lower()
    assert "group by" in normalized
    assert "row_number() over" in normalized
    assert " cast(change_pct as double) " in normalized
    # A DISTINCT window function is a MySQL syntax error (8.4 raises 1235), so
    # the breadth count has to come from its own grouped CTE.
    assert not re.search(r"distinct[\s\S]{0,40}over\s*\(", normalized)
    assert "count(distinct stock_code)" in normalized


def test_live_aggregate_matches_the_pandas_split_it_replaced():
    from sqlalchemy import text

    engine = _live_engine()
    d1, d2 = WINDOW

    trade_dates = [
        _to_date_str(row[0])
        for row in engine.connect()
        .execute(
            text(
                """
                SELECT DISTINCT trade_date FROM sm_stock_kline
                WHERE k_type = 1 AND trade_date >= :d1 AND trade_date <= :d2
                ORDER BY trade_date
                """
            ),
            {"d1": d1, "d2": d2},
        )
        .fetchall()
    ]
    if not trade_dates:
        pytest.skip("no daily bars in the window")

    # --- what the code does now: one aggregate, one row per day
    style_rows = pd.read_sql(
        text(_STYLE_AGGREGATE_SQL), engine, params={"d1": d1, "d2": d2}
    )
    assert len(style_rows) <= len(trade_dates), (
        f"the aggregate returned {len(style_rows)} rows for {len(trade_dates)} days"
    )

    # --- what it did before: every bar, cut in pandas
    import numpy as np

    raw_df = pd.read_sql(
        text(
            """
            SELECT stock_code, trade_date, change_pct, amount
            FROM sm_stock_kline
            WHERE trade_date >= :d1 AND trade_date <= :d2 AND k_type = 1
              AND adjust_type = 0 AND amount > 0
              AND stock_code REGEXP '^(0|3|6)[0-9]{5}$'
            ORDER BY trade_date, amount
            """
        ),
        engine,
        params={"d1": d1, "d2": d2},
    )
    if raw_df.empty:
        pytest.skip("the window has no usable bars")
    raw_df = raw_df.copy()
    raw_df["change_pct"] = pd.to_numeric(raw_df["change_pct"], errors="coerce")
    raw_df["amount"] = pd.to_numeric(raw_df["amount"], errors="coerce")
    raw_df = raw_df[
        np.isfinite(raw_df["change_pct"])
        & np.isfinite(raw_df["amount"])
        & (raw_df["amount"] > 0)
    ]
    assert len(raw_df) > len(style_rows) * 10, (
        "the old path was supposed to be the bulk one; the comparison is vacuous"
    )

    expected = {}
    for trade_date, grp in raw_df.groupby("trade_date"):
        n = len(grp)
        ordered = grp.sort_values("amount", ascending=False)
        large = ordered.iloc[: int(n * 0.33)]
        small = ordered.iloc[int(n * 0.67) + 1:]
        if large.empty or small.empty:
            continue
        expected[_to_date_str(trade_date)] = {
            "large_chg": round(float(large["change_pct"].mean() or 0), 4),
            "large_cnt": int(len(large)),
            "small_chg": round(float(small["change_pct"].mean() or 0), 4),
            "small_cnt": int(len(small)),
        }

    actual = {
        _to_date_str(row["trade_date"]): {
            "large_chg": round(float(row["large_chg"] or 0), 4),
            "large_cnt": int(row["large_cnt"] or 0),
            "small_chg": round(float(row["small_chg"] or 0), 4),
            "small_cnt": int(row["small_cnt"] or 0),
        }
        for row in style_rows.to_dict("records")
        if int(row["large_cnt"] or 0) and int(row["small_cnt"] or 0)
    }

    assert actual, "the aggregate produced no comparable day at all"
    assert set(actual) == set(expected), (
        f"days differ: only-new={sorted(set(actual) - set(expected))} "
        f"only-old={sorted(set(expected) - set(actual))}"
    )
    for day in sorted(expected):
        assert actual[day] == expected[day], f"{day}: {actual[day]} != {expected[day]}"

    # The window statistics must be equal too, not merely close: they feed the
    # breadth and trend cards straight through.
    for column in ("avg_chg", "up_ratio"):
        by_day = {
            _to_date_str(row["trade_date"]): float(row[column] or 0)
            for row in style_rows.to_dict("records")
        }
        for trade_date, grp in raw_df.groupby("trade_date"):
            day = _to_date_str(trade_date)
            if day not in by_day:
                continue
            if column == "avg_chg":
                want = float(grp["change_pct"].mean())
            else:
                want = float((grp["change_pct"] > 0).mean())
            assert by_day[day] == pytest.approx(want, abs=1e-9), (
                f"{column} on {day}: {by_day[day]} != {want}"
            )
