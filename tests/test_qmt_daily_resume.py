from contextlib import nullcontext
from types import SimpleNamespace

import pandas as pd
import pytest

from integrations.bigqmt.spool import BigQmtResourceBlocked
from biz.stock_market import sync_stock_market as market
from server.common import qmt_stock_catalog, qmt_trade_calendar


@pytest.mark.parametrize("failure", [TimeoutError, BigQmtResourceBlocked])
def test_restart_preserves_verified_batches_and_global_pressure_stops_immediately(monkeypatch, tmp_path, failure):
    monkeypatch.setenv("PROBIGA_JOB_LOG_ROOT", str(tmp_path))
    monkeypatch.setenv("QMT_PRODUCTION_KLINE_BATCH_SIZE", "20")
    codes = [str(index).zfill(6) for index in range(1, 61)]
    catalog = SimpleNamespace(
        manifest_hash="a" * 64, batch_id="catalog",
        members=[{"stock_code": code, "list_date": "2020-01-01"} for code in codes],
    )
    calendar = SimpleNamespace(manifest_hash="b" * 64,
                               sessions_between=lambda *_: ["2026-09-10"])
    engine = SimpleNamespace(connect=lambda: nullcontext(None))
    monkeypatch.setattr(qmt_stock_catalog, "load_stock_catalog", lambda *a, **kw: catalog)
    monkeypatch.setattr(qmt_trade_calendar, "load_trade_calendar_receipt", lambda *a, **kw: calendar)
    monkeypatch.setattr(market, "get_kline_engine", lambda: engine)
    monkeypatch.setattr(market, "_create_temporary_stage",
                        lambda *a, **kw: (SimpleNamespace(close=lambda: None), "stage"))
    staged = []
    published = []
    def append(*args, **kwargs):
        staged.extend(args[2].stock_code.tolist())
        return len(args[2])
    monkeypatch.setattr(market, "_append_temporary_stage", append)
    monkeypatch.setattr(market, "_publish_temporary_stage", lambda *a, **kw: published.append(True) or 60)
    calls = []
    fail = True
    def fetch(batch, *args, **kwargs):
        calls.append(batch[0])
        if fail and batch[0] == "000021":
            raise failure("request blocked")
        return pd.DataFrame([{
            "stock_code": code, "trade_date": pd.Timestamp("2026-09-10"),
            "k_type": 1, "adjust_type": 0, "open": 10.123456789012345,
            "close": 10, "high": 11, "low": 9, "volume": 100, "amount": 1000,
        } for code in batch])
    backend = SimpleNamespace(name="qmt", fetch_kline=fetch)
    expected_error = BigQmtResourceBlocked if failure is BigQmtResourceBlocked else RuntimeError
    with pytest.raises(expected_error):
        market._step_stock_kline_qmt(engine, backend, codes, "2026-09-10", "2026-09-10", {})
    initial_calls = ["000001", "000021"] if failure is BigQmtResourceBlocked else ["000001", "000021", "000041"]
    assert calls == initial_calls
    assert len(staged) == (20 if failure is BigQmtResourceBlocked else 40)
    assert not published
    fail = False
    staged.clear()
    market._step_stock_kline_qmt(engine, backend, codes, "2026-09-10", "2026-09-10", {})
    resumed_calls = ["000021", "000041"] if failure is BigQmtResourceBlocked else ["000021"]
    assert calls == initial_calls + resumed_calls
    assert staged == codes
    assert published == [True]
