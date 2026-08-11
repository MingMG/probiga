# -*- coding: utf-8 -*-
from unittest.mock import patch

import pandas as pd

from biz.stock_market import realtime_quotes


def _sample_quotes() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "stock_code": "000001",
                "short_name": "Ping An",
                "price": 10.0,
                "change": 0.1,
                "change_pct": 1.0,
                "volume": 100,
                "amount": 1000,
            }
        ]
    )


def test_save_to_mysql_uses_batch_engine_when_engine_not_provided():
    engine = object()

    with patch("biz.stock_market.realtime_quotes.create_batch_engine", return_value=engine) as create_batch_engine, \
         patch("biz.stock_market.realtime_quotes._ensure_rt_snapshot_table") as ensure_table, \
         patch.object(pd.DataFrame, "to_sql") as to_sql:
        assert realtime_quotes.save_to_mysql(_sample_quotes(), run_ddl=True) == 1

    create_batch_engine.assert_called_once_with(future=True)
    ensure_table.assert_called_once_with(engine)
    assert to_sql.call_args.args[0] == "sm_rt_quote_snapshot"
    assert to_sql.call_args.args[1] is engine


def test_save_to_mysql_reuses_provided_engine():
    engine = object()

    with patch("biz.stock_market.realtime_quotes.create_batch_engine") as create_batch_engine, \
         patch("biz.stock_market.realtime_quotes._ensure_rt_snapshot_table") as ensure_table, \
         patch.object(pd.DataFrame, "to_sql") as to_sql:
        assert realtime_quotes.save_to_mysql(_sample_quotes(), run_ddl=False, engine=engine) == 1

    create_batch_engine.assert_not_called()
    ensure_table.assert_not_called()
    assert to_sql.call_args.args[1] is engine
