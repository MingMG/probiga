from __future__ import annotations

from datetime import datetime

from integrations.qmt.local_history import (
    LOCAL_KLINE_TABLE,
    LOCAL_MINUTE_TABLE,
    _data_version,
    _normalize_date,
    _same_database,
)
from tools.backfill_guojin_qmt_local_history import _resolve_limits


def test_local_history_tables_are_dedicated_qmt_tables():
    assert LOCAL_KLINE_TABLE == "qmt_local_stock_kline"
    assert LOCAL_MINUTE_TABLE == "qmt_local_stock_minute"


def test_same_database_blocks_localhost_equivalent_production_url():
    prod = "mysql+pymysql://root:pass@127.0.0.1:3306/probiga?charset=utf8mb4"
    local = "mysql+pymysql://root:pass@localhost:3306/probiga?charset=utf8mb4"

    assert _same_database(local, prod) is True


def test_same_database_allows_different_local_history_database():
    prod = "mysql+pymysql://root:pass@127.0.0.1:3306/probiga?charset=utf8mb4"
    local = "mysql+pymysql://root:pass@127.0.0.1:3306/probiga_qmt_history?charset=utf8mb4"

    assert _same_database(local, prod) is False


def test_normalize_date_accepts_compact_and_datetime_values():
    assert _normalize_date("20260626") == "2026-06-26"
    assert _normalize_date(datetime(2026, 6, 26, 15, 0)) == "2026-06-26"


def test_data_version_ignores_batch_runtime_fields():
    row_a = {"stock_code": "000001", "close": 10.2, "batch_id": "a", "received_at": "x"}
    row_b = {"stock_code": "000001", "close": 10.2, "batch_id": "b", "received_at": "y"}

    assert _data_version(row_a) == _data_version(row_b)


def test_from_gaps_limit_does_not_limit_stock_universe_by_default():
    limits = _resolve_limits("from-gaps", limit=50, stock_limit=None, gap_limit=None)

    assert limits.gap_limit == 50
    assert limits.stock_limit == 0


def test_daily_limit_still_limits_stock_universe():
    limits = _resolve_limits("daily", limit=50, stock_limit=None, gap_limit=None)

    assert limits.stock_limit == 50
    assert limits.gap_limit == 20
