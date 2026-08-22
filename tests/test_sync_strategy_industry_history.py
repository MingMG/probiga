from __future__ import annotations

from datetime import date, timedelta

import pytest

from tools.sync_strategy_industry_history import _digest, build_history_rows


def _source(code: str, source_id: int) -> dict:
    return {
        "id": source_id,
        "stock_code": code,
        "industry_name": "银行",
        "industry_type": "L1",
        "source": "qmt",
        "etl_sync_at": date.today().isoformat() + "T15:00:00",
    }


def test_build_history_rows_is_complete_append_only_and_hash_bound():
    target = date.today().isoformat()
    snapshot_id, rows = build_history_rows(
        [_source("000001", 1), _source("600036", 2)],
        trade_date=target,
    )
    assert len(snapshot_id) == 64
    assert [row["stock_code"] for row in rows] == ["000001", "600036"]
    assert all(row["snapshot_id"] == snapshot_id for row in rows)
    for row in rows:
        row_hash = row["row_hash"]
        assert _digest({key: value for key, value in row.items() if key != "row_hash"}) == row_hash


def test_industry_history_refuses_retroactive_current_table_backfill():
    with pytest.raises(ValueError, match="禁止.*回填历史"):
        build_history_rows(
            [_source("000001", 1)],
            trade_date=(date.today() - timedelta(days=1)).isoformat(),
        )


def test_industry_history_rejects_duplicate_or_incomplete_source_facts():
    with pytest.raises(ValueError, match="重复或不完整"):
        build_history_rows(
            [_source("000001", 1), _source("000001", 2)],
            trade_date=date.today().isoformat(),
        )
