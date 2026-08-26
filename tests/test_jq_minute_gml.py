# -*- coding: utf-8 -*-
from datetime import datetime

import pandas as pd

from server.api.routers.jq_minute import _build_script_args
from tools.sync_jq_minute_gml import _frame_to_rows, _stock_code_to_jq, sync_jq_minute_gml


def test_stock_code_to_jq_filters_to_sh_sz_by_default():
    assert _stock_code_to_jq("600000") == "600000.XSHG"
    assert _stock_code_to_jq("000001") == "000001.XSHE"
    assert _stock_code_to_jq("300750") == "300750.XSHE"
    assert _stock_code_to_jq("833171") == ""


def test_frame_to_rows_parses_multi_index_bars():
    idx = pd.MultiIndex.from_tuples(
        [
            ("000001.XSHE", pd.Timestamp("2026-06-15 09:31:00")),
            ("000001.XSHE", pd.Timestamp("2026-06-15 09:32:00")),
        ],
        names=["code", "date"],
    )
    df = pd.DataFrame(
        {
            "open": [10.0, 10.1],
            "high": [10.2, 10.3],
            "low": [9.9, 10.0],
            "close": [10.1, 10.2],
            "volume": [1000, 1200],
            "money": [10100.0, 12240.0],
        },
        index=idx,
    )

    rows = _frame_to_rows(
        df,
        ["000001.XSHE"],
        include_now=True,
        synced_at=datetime(2026, 6, 15, 9, 32),
    )

    assert len(rows) == 2
    assert rows[0]["stock_code"] == "000001"
    assert rows[0]["is_current_bar"] == 0
    assert rows[1]["is_current_bar"] == 1
    assert rows[1]["amount"] == 12240.0


def test_build_script_args_for_scheduler():
    args = _build_script_args(
        universe="latest-kline",
        codes="000001;600000",
        limit=2,
        count=3,
        batch_size=200,
        min_coverage=0.0,
        include_now=True,
        include_paused=False,
        include_bj=False,
    )

    assert "--skip-closed" in args
    assert "--json" in args
    assert "--codes 000001,600000" in args
    assert "--limit 2" in args


def test_sync_raises_when_jq_returns_no_rows(monkeypatch):
    class FakeJQ:
        @staticmethod
        def get_query_count():
            return {"total": 1, "spare": 1}

        @staticmethod
        def get_bars(*args, **kwargs):
            return pd.DataFrame()

    monkeypatch.setattr("tools.sync_jq_minute_gml.jq_auth", lambda: None)
    monkeypatch.setattr("tools.sync_jq_minute_gml.jq", FakeJQ)
    monkeypatch.setattr("tools.sync_jq_minute_gml._read_codes", lambda *args, **kwargs: ["000001.XSHE"])
    schema_validations: list[object] = []
    monkeypatch.setattr(
        "tools.sync_jq_minute_gml._run_ddl",
        lambda engine: schema_validations.append(engine),
    )
    engine = object()

    try:
        sync_jq_minute_gml(
            engine,
            codes="000001",
            dry_run=True,
            skip_ddl=True,
            skip_closed=False,
        )
    except RuntimeError as exc:
        assert "returned no rows" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")
    assert schema_validations == [engine]
