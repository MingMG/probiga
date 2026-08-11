from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch

from integrations.qmt.local_history import (
    LOCAL_KLINE_TABLE,
    LOCAL_MINUTE_TABLE,
    _data_version,
    _normalize_date,
    _same_database,
)
from tools import (
    backfill_guojin_qmt_local_history,
    repair_guojin_qmt_gaps,
    replay_guojin_qmt_pending_writes,
    run_guojin_qmt_full_market_history_2024,
)
from tools.backfill_guojin_qmt_local_history import GAP_ORDER_SQL, _resolve_limits


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


def test_gap_backfill_prioritizes_recent_minute_gaps():
    assert "sm_stock_minute.1m" in GAP_ORDER_SQL
    assert "gap_start DESC" in GAP_ORDER_SQL
    assert GAP_ORDER_SQL.index("sm_stock_minute.1m") < GAP_ORDER_SQL.index("gap_start DESC")


def test_qmt_backfill_source_engine_uses_batch_engine():
    engine = object()

    with patch(
        "tools.backfill_guojin_qmt_local_history.create_batch_engine",
        return_value=engine,
    ) as create_batch_engine:
        assert backfill_guojin_qmt_local_history._source_engine() is engine

    create_batch_engine.assert_called_once_with(future=True)


def test_qmt_full_history_source_engine_uses_batch_engine():
    engine = object()

    with patch(
        "tools.run_guojin_qmt_full_market_history_2024.create_batch_engine",
        return_value=engine,
    ) as create_batch_engine:
        assert run_guojin_qmt_full_market_history_2024._source_engine() is engine

    create_batch_engine.assert_called_once_with(future=True)


def test_qmt_gap_repair_main_uses_batch_engine():
    engine = object()
    plan = SimpleNamespace(mode="dry_run", selected=0, locked=0, items=[])

    with patch.object(repair_guojin_qmt_gaps.sys, "argv", ["repair_guojin_qmt_gaps.py", "--json"]), \
         patch("tools.repair_guojin_qmt_gaps.create_batch_engine", return_value=engine) as create_batch_engine, \
         patch("tools.repair_guojin_qmt_gaps.plan_gap_repairs", return_value=plan) as plan_gap_repairs, \
         patch("tools.repair_guojin_qmt_gaps.result_dict", return_value={"selected": 0}):
        assert repair_guojin_qmt_gaps.main() == 0

    create_batch_engine.assert_called_once_with(future=True)
    plan_gap_repairs.assert_called_once_with(engine, limit=20, apply=False)


def test_qmt_pending_replay_main_uses_batch_engine():
    engine = object()
    result = SimpleNamespace(failed=0)

    with patch.object(
        replay_guojin_qmt_pending_writes.sys,
        "argv",
        ["replay_guojin_qmt_pending_writes.py", "--limit", "3", "--pending-root", "pending"],
    ), patch(
        "tools.replay_guojin_qmt_pending_writes.create_batch_engine",
        return_value=engine,
    ) as create_batch_engine, patch(
        "tools.replay_guojin_qmt_pending_writes.replay_pending_writes",
        return_value=result,
    ) as replay_pending_writes, patch(
        "tools.replay_guojin_qmt_pending_writes.result_dict",
        return_value={"failed": 0},
    ):
        assert replay_guojin_qmt_pending_writes.main() == 0

    create_batch_engine.assert_called_once_with(future=True)
    replay_pending_writes.assert_called_once_with(engine, pending_root="pending", limit=3)
