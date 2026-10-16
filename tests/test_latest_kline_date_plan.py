"""No request-path query may ask for the latest kline date with MAX(trade_date).

Observed in production as latency on many pages at once.  idx_date_ktype leads
with trade_date and carries k_type second, and every index on sm_stock_kline
that mentions k_type also leads with stock_code, so `WHERE k_type = 1` alone can
neither seek nor use the MIN/MAX optimisation: MySQL answers MAX(trade_date) by
scanning the whole index - measured 8.01e+6 rows examined, 2.4-3.2s per call, on
a 7.1M-row / 2.7GB table that the concurrent QMT backfill is streaming through
the same buffer pool.

The reverse scan is the same question asked so it can stop early:
`ORDER BY trade_date DESC LIMIT 1` reads 1 row in 0.04-0.10s, with an identical
answer for the unbounded, `<= :d` and `< :d` shapes alike.

Scope: the directories that serve a browser request.  The ETL/backfill modules
(biz/stock_market/sync_*, biz/analysis/sync_*, tools/sync_*, tools/crawl_*)
still carry the MAX() form; each costs the same ~3s but sits in a batch job
where it is not the user-visible symptom, so they are recorded here as
known-remaining rather than silently swept.
"""
import os
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

REQUEST_PATH_GLOBS = (
    "server/api/routers/*.py",
    "server/engine/*.py",
    "server/common/scheduler_validation.py",
    "server/trading_v3/*.py",
    "biz/review/*.py",
    "biz/research_radar/*.py",
    "tools/data_quality_check.py",
)

# MAX(<column>) over a kline table, with a k_type predicate in the same statement.
MAX_DATE_ON_KLINE = re.compile(
    r"MAX\(\s*trade_date\s*\)[\s\S]{0,300}?FROM\s+sm_\w*kline",
    re.I,
)
K_TYPE = re.compile(r"\bk_type\b", re.I)


def _offenders():
    found = []
    for pattern in REQUEST_PATH_GLOBS:
        for path in sorted(ROOT.glob(pattern)):
            text = path.read_text(encoding="utf-8", errors="replace")
            for match in MAX_DATE_ON_KLINE.finditer(text):
                window = text[match.start():match.end()]
                if not K_TYPE.search(window):
                    continue
                # A per-security MAX() over a GROUP BY is a different question
                # and cannot be answered by a single reverse scan.
                if re.search(r"GROUP\s+BY\s+stock_code", window, re.I):
                    continue
                line = text[:match.start()].count("\n") + 1
                found.append(f"{path.relative_to(ROOT)}:{line}")
    return found


def test_no_request_path_query_scans_the_index_for_the_latest_date():
    offenders = _offenders()
    assert not offenders, (
        "these ask for the latest trade_date with MAX() and pay a full index "
        "scan (~3s) per request: " + ", ".join(offenders)
    )


def _live_engine():
    if not (os.environ.get("MYSQL_URL") or os.environ.get("DATABASE_URL")):
        pytest.skip("no MySQL configured for the live plan check")
    from sqlalchemy import text

    from server.common.batch_db import create_batch_engine

    try:
        engine = create_batch_engine(future=True)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1")).fetchall()
    except Exception as exc:
        pytest.skip(f"live MySQL unavailable: {exc}")
    return engine


@pytest.mark.parametrize(
    "predicate",
    ["", " AND trade_date <= :d", " AND trade_date < :d"],
    ids=["unbounded", "bounded-inclusive", "bounded-exclusive"],
)
def test_live_reverse_scan_matches_max_and_reads_one_row(predicate):
    """Reproduces the production cost, and proves the answer did not change."""
    from sqlalchemy import text

    engine = _live_engine()
    params = {"d": "2026-10-17"}
    old = f"SELECT MAX(trade_date) FROM sm_stock_kline WHERE k_type = 1{predicate}"
    new = (
        f"SELECT trade_date FROM sm_stock_kline WHERE k_type = 1{predicate} "
        f"ORDER BY trade_date DESC LIMIT 1"
    )

    with engine.connect() as connection:
        expected = connection.execute(text(old), params).scalar()
        plan = connection.execute(
            text("EXPLAIN ANALYZE " + new), params
        ).fetchall()
        actual = connection.execute(text(new), params).scalar()

    assert actual == expected, f"the rewrite changed the answer: {actual} != {expected}"
    assert actual is not None, "no daily bar at all - the assertion above is vacuous"

    detail = "\n".join(str(row[0]) for row in plan)
    assert "(reverse)" in detail, detail
    rows_read = re.findall(r"rows=([0-9.e+]+) loops", detail)
    assert rows_read, detail
    assert all(value == "1" for value in rows_read), detail
