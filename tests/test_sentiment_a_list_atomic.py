from __future__ import annotations

import pandas as pd

from biz.sentiment import sync_sentiment


def test_replace_a_list_dates_scopes_delete_to_fetched_dates(monkeypatch):
    captured = {}

    def fake_replace(frame, table_name, engine, **kwargs):
        captured.update(
            frame=frame,
            table_name=table_name,
            engine=engine,
            kwargs=kwargs,
        )

    monkeypatch.setattr(sync_sentiment, "replace_table_rows", fake_replace)
    monkeypatch.setattr(
        sync_sentiment,
        "_with_etl",
        lambda frame: frame.assign(etl_sync_at="now"),
    )
    frame = pd.DataFrame(
        {
            "trade_date": ["2026-08-07", "2026-08-07", "2026-08-10"],
            "stock_code": ["000001", "000002", "600519"],
        }
    )
    engine = object()

    sync_sentiment._replace_a_list_dates(
        engine,
        "st_a_list_daily",
        frame,
    )

    assert captured["table_name"] == "st_a_list_daily"
    assert captured["engine"] is engine
    assert captured["kwargs"]["where_sql"] == (
        "trade_date IN (:trade_date_0, :trade_date_1)"
    )
    assert captured["kwargs"]["params"] == {
        "trade_date_0": "2026-08-07",
        "trade_date_1": "2026-08-10",
    }
    assert "etl_sync_at" in captured["frame"].columns


def test_replace_a_list_dates_rejects_unscoped_frame():
    frame = pd.DataFrame({"stock_code": ["000001"]})

    try:
        sync_sentiment._replace_a_list_dates(
            object(),
            "st_a_list_daily",
            frame,
        )
    except ValueError as exc:
        assert "trade_date" in str(exc)
    else:
        raise AssertionError("unscoped replacement must be rejected")
