from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from biz.stock_market import sync_stock_market


def test_qmt_minute_capture_marks_only_live_arrival_forward_eligible():
    live = sync_stock_market._classify_qmt_minute_capture(
        trade_date="2026-07-30",
        last_trade_time=datetime(2026, 7, 30, 10, 0),
        captured_at=datetime(2026, 7, 30, 10, 0, 30),
    )
    after_close = sync_stock_market._classify_qmt_minute_capture(
        trade_date="2026-07-30",
        last_trade_time=datetime(2026, 7, 30, 15, 0),
        captured_at=datetime(2026, 7, 30, 15, 30),
    )
    historical = sync_stock_market._classify_qmt_minute_capture(
        trade_date="2026-07-29",
        last_trade_time=datetime(2026, 7, 29, 15, 0),
        captured_at=datetime(2026, 7, 30, 10, 0),
    )

    assert live[:2] == ("LIVE_FORWARD", True)
    assert after_close[:2] == ("AFTER_CLOSE_BACKFILL", False)
    assert historical[:2] == ("AFTER_CLOSE_BACKFILL", False)


def _daily_frame(codes: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "stock_code": code,
                "trade_time": "2026-07-17 15:00:00",
                "trade_date": "2026-07-17",
                "k_type": 1,
                "adjust_type": 0,
                "open": 10,
                "close": 10.1,
                "high": 10.2,
                "low": 9.9,
                "volume": 1000,
                "amount": 10100,
            }
            for code in codes
        ]
    )


def _minute_frame(codes: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "stock_code": code,
                "trade_time": "2026-07-17 09:31:00",
                "trade_date": "2026-07-17",
                "price": 10.1,
                "avg_price": 10.1,
                "change": 0.1,
                "change_pct": 1,
                "volume": 100,
                "amount": 1010,
            }
            for code in codes
        ]
    )


def test_qmt_daily_full_snapshot_prunes_only_after_coverage_passes(monkeypatch):
    codes = ["000001", "600519"]
    pruned: list[dict] = []
    write_engines: list[object] = []
    history_engine = object()
    backend = SimpleNamespace(fetch_kline=lambda batch, *_args, **_kwargs: _daily_frame(batch))
    monkeypatch.setattr(sync_stock_market, "get_kline_engine", lambda: history_engine)
    monkeypatch.setenv("QMT_PRODUCTION_KLINE_BATCH_SIZE", "20")
    monkeypatch.setenv("QMT_KLINE_MIN_COVERAGE", "1")
    monkeypatch.setattr(
        sync_stock_market,
        "_upsert_qmt_kline_frame",
        lambda target_engine, frame: write_engines.append(target_engine) or len(frame),
    )
    monkeypatch.setattr(sync_stock_market, "_is_complete_stock_universe", lambda *_args: True)
    monkeypatch.setattr(
        sync_stock_market,
        "_prune_snapshot_codes",
        lambda _engine, **kwargs: pruned.append(kwargs) or 0,
    )

    sync_stock_market._step_stock_kline_qmt(
        object(), backend, codes, "2026-07-17", "2026-07-17", {}
    )

    assert len(pruned) == 1
    assert pruned[0]["table_name"] == "sm_stock_kline"
    assert pruned[0]["target_date"] == "2026-07-17"
    assert pruned[0]["keep_codes"] == set(codes)
    assert write_engines == [history_engine]


def test_bigqmt_daily_capture_is_persisted_before_canonical_publish(
    monkeypatch,
):
    codes = ["000001", "600519"]
    events: list[str] = []
    backend = SimpleNamespace(
        name="bigqmt",
        fetch_kline=lambda batch, *_args, **_kwargs: _daily_frame(batch),
    )
    monkeypatch.setattr(
        sync_stock_market,
        "get_kline_engine",
        lambda: object(),
    )
    monkeypatch.setenv("QMT_PRODUCTION_KLINE_BATCH_SIZE", "20")
    monkeypatch.setenv("QMT_KLINE_MIN_COVERAGE", "1")
    monkeypatch.setattr(
        "integrations.qmt.local_history.persist_daily_kline_capture",
        lambda frame, **_kwargs: events.append("raw") or len(frame),
    )
    monkeypatch.setattr(
        sync_stock_market,
        "_upsert_qmt_kline_frame",
        lambda _engine, frame: events.append("canonical") or len(frame),
    )
    monkeypatch.setattr(
        sync_stock_market,
        "_is_complete_stock_universe",
        lambda *_args: False,
    )

    sync_stock_market._step_stock_kline_qmt(
        object(), backend, codes, "2026-07-17", "2026-07-17", {}
    )

    assert events == ["raw", "canonical"]


def test_qmt_full_day_minute_prunes_stale_codes(monkeypatch):
    codes = ["000001", "600519"]
    pruned: list[dict] = []
    write_engines: list[object] = []
    history_engine = object()
    backend = SimpleNamespace(fetch_minute=lambda batch, *_args, **_kwargs: _minute_frame(batch))
    monkeypatch.setattr(sync_stock_market, "get_kline_engine", lambda: history_engine)
    monkeypatch.setenv("QMT_MINUTE_COUNT", "0")
    monkeypatch.setenv("QMT_MINUTE_MIN_COVERAGE", "1")
    monkeypatch.setattr(sync_stock_market, "_default_myquant_minute_date", lambda _engine: "2026-07-17")
    monkeypatch.setattr(sync_stock_market, "_create_qmt_minute_stage", lambda *_args: None)
    monkeypatch.setattr(
        sync_stock_market,
        "_append_qmt_minute_stage",
        lambda target_engine, _stage, frame: write_engines.append(target_engine) or len(frame),
    )
    monkeypatch.setattr(sync_stock_market, "_commit_qmt_minute_stage", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(sync_stock_market, "_record_qmt_minute_receipt", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(sync_stock_market, "_drop_qmt_minute_stage", lambda *_args: None)
    monkeypatch.setattr(sync_stock_market, "_is_complete_stock_universe", lambda *_args: True)
    monkeypatch.setattr(
        sync_stock_market,
        "_prune_snapshot_codes",
        lambda _engine, **kwargs: pruned.append(kwargs) or 0,
    )
    monkeypatch.setattr(sync_stock_market, "_prune_snapshot_time_bounds", lambda *_args, **_kwargs: 0)

    sync_stock_market._step_stock_minute_qmt(object(), backend, codes)

    assert len(pruned) == 1
    assert pruned[0]["table_name"] == "sm_stock_minute"
    assert pruned[0]["keep_codes"] == set(codes)
    assert write_engines == [history_engine]


def test_qmt_intraday_minute_never_prunes_the_full_day(monkeypatch):
    codes = ["000001", "600519"]
    backend = SimpleNamespace(fetch_minute=lambda batch, *_args, **_kwargs: _minute_frame(batch))
    monkeypatch.setattr(sync_stock_market, "get_kline_engine", lambda: object())
    monkeypatch.setenv("QMT_MINUTE_COUNT", "20")
    monkeypatch.setenv("QMT_MINUTE_MIN_COVERAGE", "1")
    monkeypatch.setattr(sync_stock_market, "_default_myquant_minute_date", lambda _engine: "2026-07-17")
    monkeypatch.setattr(sync_stock_market, "_create_qmt_minute_stage", lambda *_args: None)
    monkeypatch.setattr(sync_stock_market, "_append_qmt_minute_stage", lambda _engine, _stage, frame: len(frame))
    monkeypatch.setattr(sync_stock_market, "_commit_qmt_minute_stage", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(sync_stock_market, "_record_qmt_minute_receipt", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(sync_stock_market, "_drop_qmt_minute_stage", lambda *_args: None)
    monkeypatch.setattr(sync_stock_market, "_is_complete_stock_universe", lambda *_args: True)
    monkeypatch.setattr(
        sync_stock_market,
        "_prune_snapshot_codes",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("intraday must not prune")),
    )

    sync_stock_market._step_stock_minute_qmt(object(), backend, codes)


def test_qmt_minute_coverage_failure_does_not_commit_staged_rows(monkeypatch):
    codes = ["000001", "600519"]
    backend = SimpleNamespace(fetch_minute=lambda batch, *_args, **_kwargs: _minute_frame(batch[:1]))
    appended: list[int] = []
    committed: list[bool] = []

    monkeypatch.setattr(sync_stock_market, "get_kline_engine", lambda: object())
    monkeypatch.setenv("QMT_MINUTE_COUNT", "20")
    monkeypatch.setenv("QMT_MINUTE_MIN_COVERAGE", "1")
    monkeypatch.setattr(sync_stock_market, "_default_myquant_minute_date", lambda _engine: "2026-07-17")
    monkeypatch.setattr(sync_stock_market, "_create_qmt_minute_stage", lambda *_args: None)
    monkeypatch.setattr(
        sync_stock_market,
        "_append_qmt_minute_stage",
        lambda _engine, _stage, frame: appended.append(len(frame)) or len(frame),
    )
    monkeypatch.setattr(sync_stock_market, "_commit_qmt_minute_stage", lambda *_args, **_kwargs: committed.append(True))
    monkeypatch.setattr(sync_stock_market, "_drop_qmt_minute_stage", lambda *_args: None)

    with pytest.raises(RuntimeError, match="coverage below threshold"):
        sync_stock_market._step_stock_minute_qmt(object(), backend, codes)

    assert appended
    assert committed == []


def test_qmt_minute_receipt_failure_keeps_job_failed(monkeypatch):
    codes = ["000001", "600519"]
    backend = SimpleNamespace(
        name="bigqmt",
        fetch_minute=lambda batch, *_args, **_kwargs: _minute_frame(batch),
    )

    monkeypatch.setattr(sync_stock_market, "get_kline_engine", lambda: object())
    monkeypatch.setenv("QMT_MINUTE_COUNT", "20")
    monkeypatch.setenv("QMT_MINUTE_MIN_COVERAGE", "1")
    monkeypatch.setattr(
        sync_stock_market,
        "_default_myquant_minute_date",
        lambda _engine: "2026-07-17",
    )
    monkeypatch.setattr(
        sync_stock_market,
        "_create_qmt_minute_stage",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        sync_stock_market,
        "_append_qmt_minute_stage",
        lambda _engine, _stage, frame: len(frame),
    )
    monkeypatch.setattr(
        sync_stock_market,
        "_commit_qmt_minute_stage",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        sync_stock_market,
        "_drop_qmt_minute_stage",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        sync_stock_market,
        "_record_qmt_minute_receipt",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("receipt write failed")
        ),
    )

    with pytest.raises(RuntimeError, match="receipt write failed"):
        sync_stock_market._step_stock_minute_qmt(object(), backend, codes)
