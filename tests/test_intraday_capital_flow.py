from __future__ import annotations

import importlib
import sys
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest


def _crawler():
    return importlib.import_module("tools.crawl_minute_kline")


def test_flow_main_uses_minute_engine_and_accepts_scheduler_tuning(monkeypatch):
    crawler = _crawler()
    primary_engine = object()
    minute_engine = object()
    kline_engine = object()
    codes = [("000001", 0), ("600000", 1)]
    summary = {
        "table": crawler.FLOW_TABLE,
        "total": 2,
        "ok": 2,
        "coverage": 1.0,
    }

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "crawl_minute_kline.py",
            "--type", "flow",
            "--min-coverage", "0.98",
            "--request-delay", "0.03",
            "--request-jitter", "0.02",
            "--batch-every", "0",
            "--fetch-attempts", "2",
            "--skip-closed",
        ],
    )
    monkeypatch.setattr(crawler, "DELAY", 0.5)
    monkeypatch.setattr(crawler, "JITTER", 0.3)
    monkeypatch.setattr(crawler, "BATCH_EVERY", 100)
    monkeypatch.setattr(crawler, "FETCH_ATTEMPTS", 3)
    monkeypatch.setattr(crawler, "create_batch_engine", lambda: primary_engine)
    monkeypatch.setattr(crawler, "get_minute_engine", lambda: minute_engine)
    monkeypatch.setattr(crawler, "get_kline_engine", lambda: kline_engine)
    monkeypatch.setattr(crawler, "is_trading_time", lambda engine: True)
    universe = MagicMock(return_value=codes)
    crawl_flow = MagicMock(return_value=summary)
    monkeypatch.setattr(crawler, "get_latest_kline_stock_codes", universe)
    monkeypatch.setattr(crawler, "crawl_flow", crawl_flow)

    assert crawler.main() == 0
    assert crawler.DELAY == 0.03
    assert crawler.JITTER == 0.02
    assert crawler.BATCH_EVERY == 0
    assert crawler.FETCH_ATTEMPTS == 2
    universe.assert_called_once_with(kline_engine, fallback_engine=primary_engine)
    assert crawl_flow.call_args.args == (minute_engine, codes, 0, 0.98)
    assert len(crawl_flow.call_args.kwargs["trade_date"]) == 10


def test_flow_coverage_failure_does_not_publish_or_count_old_bars(monkeypatch):
    crawler = _crawler()
    engine = object()
    appended = []
    publish = MagicMock(return_value=0)
    drop = MagicMock()

    def fetch(_fetcher, code, _market):
        day = "2026-08-11" if code == "000001" else "2026-08-10"
        return [f"{day} 09:31,1,2,3,4,5"]

    def append(_engine, _stage, rows):
        appended.extend(rows)
        return len(rows)

    monkeypatch.setattr(crawler, "fetch_with_retries", fetch)
    monkeypatch.setattr(crawler, "_create_flow_stage", lambda _engine: "flow_stage")
    monkeypatch.setattr(crawler, "_append_flow_stage", append)
    monkeypatch.setattr(crawler, "_publish_flow_stage", publish)
    monkeypatch.setattr(crawler, "_drop_flow_stage", drop)
    monkeypatch.setattr(crawler.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(crawler.random, "uniform", lambda _a, _b: 0.0)

    result = crawler.crawl_flow(
        engine,
        [("000001", 0), ("600000", 1)],
        0,
        0.75,
        trade_date="2026-08-11",
    )

    assert result["coverage"] == 0.5
    assert result["rows"] == 0
    assert [row["stock_code"] for row in appended] == ["000001"]
    publish.assert_not_called()
    drop.assert_called_once_with(engine, "flow_stage")


def test_flow_coverage_success_publishes_staged_day(monkeypatch):
    crawler = _crawler()
    engine = object()
    publish = MagicMock(return_value=2)
    drop = MagicMock()

    monkeypatch.setattr(
        crawler,
        "fetch_with_retries",
        lambda _fetcher, code, _market: [f"2026-08-11 09:31,{code[-1]},2,3,4,5"],
    )
    monkeypatch.setattr(crawler, "_create_flow_stage", lambda _engine: "flow_stage")
    monkeypatch.setattr(crawler, "_append_flow_stage", lambda _engine, _stage, rows: len(rows))
    monkeypatch.setattr(crawler, "_publish_flow_stage", publish)
    monkeypatch.setattr(crawler, "_drop_flow_stage", drop)
    monkeypatch.setattr(crawler.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(crawler.random, "uniform", lambda _a, _b: 0.0)

    result = crawler.crawl_flow(
        engine,
        [("000001", 0), ("600000", 1)],
        0,
        0.98,
        trade_date="2026-08-11",
    )

    assert result["coverage"] == 1.0
    assert result["rows"] == 2
    publish.assert_called_once_with(engine, "flow_stage", "2026-08-11")
    drop.assert_called_once_with(engine, "flow_stage")


def test_flow_cleanup_failure_does_not_mask_collection_failure(monkeypatch):
    crawler = _crawler()
    engine = object()

    def fail_fetch(*_args):
        raise RuntimeError("source failed")

    def fail_cleanup(*_args):
        raise RuntimeError("cleanup failed")

    monkeypatch.setattr(crawler, "_create_flow_stage", lambda _engine: "flow_stage")
    monkeypatch.setattr(crawler, "fetch_with_retries", fail_fetch)
    monkeypatch.setattr(crawler, "_drop_flow_stage", fail_cleanup)

    with pytest.raises(RuntimeError, match="source failed"):
        crawler.crawl_flow(
            engine,
            [("000001", 0)],
            0,
            0.98,
            trade_date="2026-08-11",
        )


def test_publish_flow_stage_replaces_day_in_one_transaction(monkeypatch):
    crawler = _crawler()
    statements = []

    class _Connection:
        def execute(self, statement, params=None):
            statements.append((str(statement), params))
            return SimpleNamespace(rowcount=7)

    connection = _Connection()
    engine = SimpleNamespace(begin=lambda: nullcontext(connection))
    monkeypatch.setattr(crawler, "mysql_named_lock", lambda *args, **kwargs: nullcontext())

    assert crawler._publish_flow_stage(engine, "flow_stage", "2026-08-11") == 7
    assert len(statements) == 2
    assert statements[0][0].lstrip().upper().startswith("DELETE FROM")
    assert "TRADE_TIME >=" in statements[0][0].upper()
    assert statements[1][0].lstrip().upper().startswith("INSERT INTO")
    assert "SELECT" in statements[1][0].upper()


def test_fetch_with_retries_recovers_transient_empty_result(monkeypatch):
    crawler = _crawler()
    fetcher = MagicMock(side_effect=[None, ["row"]])
    sleep = MagicMock()
    monkeypatch.setattr(crawler, "FETCH_ATTEMPTS", 2)
    monkeypatch.setattr(crawler, "RETRY_DELAY", 0.25)
    monkeypatch.setattr(crawler.time, "sleep", sleep)

    assert crawler.fetch_with_retries(fetcher, "000001", 0) == ["row"]
    assert fetcher.call_count == 2
    sleep.assert_called_once_with(0.25)


def test_batch_router_and_scheduler_metadata_use_minute_engine(monkeypatch):
    from server.common import batch_db, minute_data, scheduler_validation

    primary_engine = SimpleNamespace(connect=MagicMock())

    class _Result:
        def mappings(self):
            return self

        def all(self):
            return [{"COLUMN_NAME": "stock_code"}, {"COLUMN_NAME": "trade_time"}]

    minute_connection = SimpleNamespace(execute=lambda statement, params: _Result())
    minute_engine = SimpleNamespace(connect=lambda: nullcontext(minute_connection))
    monkeypatch.setattr(batch_db, "should_use_kline_engine", lambda _sql: False)
    monkeypatch.setattr(minute_data, "get_minute_engine", lambda: minute_engine)

    sql = "SELECT * FROM sm_stock_capital_flow_min"
    assert batch_db.routed_read_engine(sql, primary_engine) is minute_engine
    assert scheduler_validation._table_columns(primary_engine, "sm_stock_capital_flow_min") == {
        "stock_code", "trade_time",
    }
    primary_engine.connect.assert_not_called()


def test_intraday_flow_validation_accepts_first_sparse_bar_per_stock():
    from server.common.scheduler_validation import TASK_OUTPUT_REQUIREMENTS

    requirement = TASK_OUTPUT_REQUIREMENTS["intraday_minute_flow"][0]
    assert requirement.min_rows == 5000
    assert requirement.min_distinct == 5000
