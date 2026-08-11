from __future__ import annotations

from unittest.mock import patch

import pandas as pd

from tools import sync_qmt_realtime


def test_sync_qmt_realtime_uses_batch_engine_when_engine_not_provided():
    engine = object()

    with patch("tools.sync_qmt_realtime.create_batch_engine", return_value=engine) as create_batch_engine, \
         patch("tools.sync_qmt_realtime.is_trading_time", return_value=False) as is_trading_time:
        result = sync_qmt_realtime.sync_qmt_realtime(skip_closed=True)

    create_batch_engine.assert_called_once_with(future=True)
    is_trading_time.assert_called_once_with(engine)
    assert result["status"] == "skipped"
    assert result["reason"] == "market_closed"


def test_sync_qmt_realtime_reuses_provided_engine():
    engine = object()

    with patch("tools.sync_qmt_realtime.create_batch_engine") as create_batch_engine, \
         patch("tools.sync_qmt_realtime.is_trading_time", return_value=False) as is_trading_time:
        result = sync_qmt_realtime.sync_qmt_realtime(engine=engine, skip_closed=True)

    create_batch_engine.assert_not_called()
    is_trading_time.assert_called_once_with(engine)
    assert result["status"] == "skipped"


def test_sync_qmt_realtime_passes_engine_to_snapshot_archive():
    engine = object()
    frame = pd.DataFrame(
        [
            {
                "stock_code": "1",
                "short_name": "Ping An",
                "price": 10.0,
                "change": 0.1,
                "change_pct": 1.0,
                "volume": 100,
                "amount": 1000,
            }
        ]
    )

    class Backend:
        def fetch_current(self, codes, *, short_name_map):
            return frame

    with patch("tools.sync_qmt_realtime._load_short_name_map", return_value={"000001": "Ping An"}), \
         patch("tools.sync_qmt_realtime.QmtBackend", return_value=Backend()), \
         patch("tools.sync_qmt_realtime._write_current_table", return_value=1), \
         patch("tools.sync_qmt_realtime.save_to_mysql", return_value=1) as save_to_mysql:
        result = sync_qmt_realtime.sync_qmt_realtime(
            engine=engine,
            codes=["1"],
            skip_closed=False,
            archive_snapshot=True,
            run_rt_ddl=False,
        )

    assert result["status"] == "success"
    save_to_mysql.assert_called_once()
    assert save_to_mysql.call_args.kwargs["engine"] is engine
    assert save_to_mysql.call_args.kwargs["run_ddl"] is False
