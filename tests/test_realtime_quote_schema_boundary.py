from __future__ import annotations

import inspect
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from biz.stock_market import realtime_quotes
from tools import crawl_realtime_batch


def test_runtime_snapshot_guard_performs_only_read_only_validation():
    engine = Mock()
    with patch.object(realtime_quotes, "validate_runtime_tables") as validator:
        realtime_quotes._ensure_rt_snapshot_table(engine)

    validator.assert_called_once_with(
        engine,
        realtime_quotes._RT_SNAPSHOT_CONTRACT,
        context="realtime quote snapshot",
    )
    source = inspect.getsource(realtime_quotes._ensure_rt_snapshot_table).upper()
    assert "CREATE TABLE" not in source
    assert "ALTER TABLE" not in source


def test_snapshot_contract_matches_the_frozen_nine_column_production_shape():
    contract = realtime_quotes._RT_SNAPSHOT_CONTRACT["sm_rt_quote_snapshot"]
    assert tuple(contract.columns) == (
        "id",
        "stock_code",
        "short_name",
        "price",
        "change",
        "change_pct",
        "volume",
        "amount",
        "snapshot_at",
    )
    assert contract.columns["id"].auto_increment is True
    assert contract.columns["price"].numeric_precision == 50
    assert contract.columns["price"].numeric_scale == 6
    assert {index.columns for index in contract.indexes} == {
        ("id",),
        ("stock_code",),
        ("snapshot_at",),
    }


def test_realtime_batch_routes_all_live_replacements_through_atomic_helper():
    source = inspect.getsource(crawl_realtime_batch).upper()
    assert "TRUNCATE TABLE" not in source
    snapshot_source = inspect.getsource(
        crawl_realtime_batch.refresh_snapshot
    ).upper()
    publication_source = inspect.getsource(
        crawl_realtime_batch._publish_snapshot_and_archive
    ).upper()
    assert "_PUBLISH_SNAPSHOT_AND_ARCHIVE" in snapshot_source
    assert "WITH ENGINE.BEGIN()" in publication_source
    assert "WRITE_FRAME" in publication_source
    assert "TO_SQL" not in publication_source
    for function in (
        crawl_realtime_batch.refresh_flow,
        crawl_realtime_batch.refresh_concept_east,
        crawl_realtime_batch.refresh_index,
    ):
        function_source = inspect.getsource(function).upper()
        assert "REPLACE_TABLE_ROWS" in function_source
        assert "DELETE FROM" not in function_source


def test_realtime_batch_passes_complete_frames_to_atomic_replacement(monkeypatch):
    item = {
        "f2": 10.5,
        "f3": 1.2,
        "f4": 0.12,
        "f5": 1000,
        "f6": 2000,
        "f7": 2.0,
        "f8": 3.0,
        "f12": "600000",
        "f14": "浦发银行",
        "f15": 10.8,
        "f16": 10.1,
        "f17": 10.2,
        "f18": 10.38,
        "f62": 100,
        "f66": 40,
        "f72": 30,
        "f78": 20,
        "f84": 10,
    }
    monkeypatch.setattr(crawl_realtime_batch, "fetch_batch", lambda *_args, **_kwargs: [item])
    monkeypatch.setattr(crawl_realtime_batch, "_latest_stock_universe_count", lambda _engine: 1)
    monkeypatch.setattr(crawl_realtime_batch, "_latest_open_trade_date", lambda _engine: "2026-08-25")
    monkeypatch.setattr(
        crawl_realtime_batch,
        "_capital_flow_target_kind",
        lambda *_args: "current",
    )
    monkeypatch.setattr(
        crawl_realtime_batch,
        "_read_target_traded_flow_codes",
        lambda _engine, _day: {"600000"},
    )

    calls: list[tuple[str, pd.DataFrame, object, dict]] = []

    def _publish_snapshot(engine, frame, *, archive_snapshot):
        calls.append(("sm_stock_current", frame.copy(), engine, {
            "archive_snapshot": archive_snapshot,
        }))
        return len(frame)

    def _replace(frame, table_name, engine, **kwargs):
        calls.append((table_name, frame.copy(), engine, dict(kwargs)))
        return len(frame)

    monkeypatch.setattr(crawl_realtime_batch, "replace_table_rows", _replace)

    def _replace_flow(engine, frame, *, trade_date, expected_codes):
        calls.append((
            "sm_stock_capital_flow_daily",
            frame.copy(),
            engine,
            {
                "trade_date": trade_date,
                "expected_codes": set(expected_codes),
            },
        ))
        return len(frame)

    monkeypatch.setattr(
        crawl_realtime_batch,
        "_replace_table_rows_flow_partition_exact",
        _replace_flow,
    )
    monkeypatch.setattr(
        crawl_realtime_batch,
        "_publish_snapshot_and_archive",
        _publish_snapshot,
    )
    monkeypatch.setattr(
        pd.DataFrame,
        "to_sql",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("live table write bypassed replace_table_rows")
        ),
    )

    engine = object()
    assert crawl_realtime_batch.refresh_snapshot(engine) == 1
    assert crawl_realtime_batch.refresh_flow(engine) == 1
    assert crawl_realtime_batch.refresh_concept_east(engine) == 1
    assert crawl_realtime_batch.refresh_index(engine) == 1

    assert [call[0] for call in calls] == [
        "sm_stock_current",
        "sm_stock_capital_flow_daily",
        "sm_concept_east_current",
        "sm_index_current",
    ]
    assert all(call[2] is engine for call in calls)
    assert all("etl_sync_at" in call[1].columns for call in calls)
    assert calls[0][3] == {"archive_snapshot": False}
    assert calls[1][3] == {
        "trade_date": "2026-08-25",
        "expected_codes": {"600000"},
    }
    assert calls[2][3] == {"chunksize": 500, "method": "multi"}
    assert calls[3][3] == {"chunksize": 500, "method": "multi"}


def test_snapshot_and_archive_share_transaction_and_archive_failure_rolls_back(monkeypatch):
    frame = pd.DataFrame([
        {
            "stock_code": "600000",
            "short_name": "浦发银行",
            "price": 10.5,
            "change": 0.1,
            "change_pct": 1.0,
            "volume": 100,
            "amount": 1000,
            "snapshot_at": pd.Timestamp("2026-08-25 10:00:00"),
            "etl_sync_at": pd.Timestamp("2026-08-25 10:00:00"),
        }
    ])
    transaction = SimpleNamespace(saw_error=False)
    connection = SimpleNamespace(execute=Mock())

    class _Transaction:
        def __enter__(self):
            return connection

        def __exit__(self, exc_type, _exc, _tb):
            transaction.saw_error = exc_type is not None
            return False

    engine = SimpleNamespace(begin=lambda: _Transaction())
    writes = []

    def _write(_frame, table_name, bind, **_kwargs):
        writes.append((table_name, bind))
        if table_name == "sm_rt_quote_snapshot":
            raise RuntimeError("archive write failed")
        return 1

    monkeypatch.setattr(crawl_realtime_batch, "_ensure_rt_snapshot_table", lambda _engine: None)
    monkeypatch.setattr(crawl_realtime_batch, "write_frame", _write)

    with pytest.raises(RuntimeError, match="archive write failed"):
        crawl_realtime_batch._publish_snapshot_and_archive(
            engine,
            frame,
            archive_snapshot=True,
        )

    assert transaction.saw_error is True
    assert writes == [
        ("sm_stock_current", connection),
        ("sm_rt_quote_snapshot", connection),
    ]
