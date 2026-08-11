# -*- coding: utf-8 -*-
from unittest.mock import patch

import pandas as pd

from biz.stock_finance import sync_finance


def test_stock_finance_get_engine_uses_batch_engine():
    engine = object()

    with patch("biz.stock_finance.sync_finance.create_batch_engine", return_value=engine) as create_batch_engine:
        assert sync_finance.get_engine() is engine

    create_batch_engine.assert_called_once_with(pool_size=5, max_overflow=10)


def test_stock_finance_stock_codes_use_batch_reader():
    engine = object()
    frame = pd.DataFrame({"stock_code": ["000001", "600000"]})

    with patch("biz.stock_finance.sync_finance.read_frame", return_value=frame) as read_frame:
        assert sync_finance.get_all_stock_codes(engine) == ["000001", "600000"]

    read_frame.assert_called_once()
    assert read_frame.call_args.args[1] is engine
    assert "si_all_code" in str(read_frame.call_args.args[0])
