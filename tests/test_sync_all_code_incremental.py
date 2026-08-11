# -*- coding: utf-8 -*-
from unittest.mock import patch

import pandas as pd

from biz.stock_info import sync_all_code_incremental


def test_sync_all_code_incremental_main_uses_batch_engine():
    engine = object()
    frame = pd.DataFrame([{"stock_code": "1", "short_name": "Ping An", "exchange": "SZ", "list_date": None}])

    with patch(
        "biz.stock_info.sync_all_code_incremental._fetch_all_code_df",
        return_value=frame,
    ), patch(
        "biz.stock_info.sync_all_code_incremental.create_batch_engine",
        return_value=engine,
    ) as create_batch_engine, patch(
        "biz.stock_info.sync_all_code_incremental.upsert_si_all_code",
        return_value=1,
    ) as upsert:
        sync_all_code_incremental.main()

    create_batch_engine.assert_called_once_with(future=True)
    upsert.assert_called_once_with(engine, frame)


def test_sync_all_code_incremental_exits_when_source_empty():
    with patch(
        "biz.stock_info.sync_all_code_incremental._fetch_all_code_df",
        return_value=pd.DataFrame(),
    ), patch("biz.stock_info.sync_all_code_incremental.create_batch_engine") as create_batch_engine:
        try:
            sync_all_code_incremental.main()
        except SystemExit as exc:
            assert exc.code == 1
        else:
            raise AssertionError("empty source should exit")

    create_batch_engine.assert_not_called()
