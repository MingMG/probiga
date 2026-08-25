from __future__ import annotations

import pandas as pd
from types import SimpleNamespace

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


def test_a_list_info_empty_code_keeps_old_partition(monkeypatch):
    captured = {}

    def get_info(*, stock_code, report_date):
        if stock_code == "000002":
            return pd.DataFrame()
        return pd.DataFrame(
            [{"trade_date": report_date, "stock_code": stock_code, "reason": "ok"}]
        )

    provider = SimpleNamespace(hot=SimpleNamespace(get_a_list_info=get_info))
    monkeypatch.setenv("SE_A_LIST_INFO", "1")
    monkeypatch.setenv("SE_A_LIST_INFO_MAX", "0")
    monkeypatch.delenv("SE_A_LIST_DATE", raising=False)
    monkeypatch.setattr(sync_sentiment, "_sleep", lambda: None)
    monkeypatch.setattr(
        sync_sentiment,
        "retry_remote",
        lambda function, *args, **kwargs: function(*args, **kwargs),
    )
    monkeypatch.setattr(
        sync_sentiment,
        "replace_table_rows",
        lambda frame, table, engine, **kwargs: captured.update(
            frame=frame,
            table=table,
            engine=engine,
            kwargs=kwargs,
        ),
    )
    daily = pd.DataFrame(
        {
            "trade_date": ["2026-08-25", "2026-08-25"],
            "stock_code": ["000001", "000002"],
        }
    )

    sync_sentiment.step_a_list_info(object(), provider, daily)

    assert captured["table"] == "st_a_list_info"
    assert captured["frame"]["stock_code"].unique().tolist() == ["000001"]
    assert "stock_code = :stock_code_0" in captured["kwargs"]["where_sql"]
    assert captured["kwargs"]["params"] == {
        "trade_date_0": "2026-08-25",
        "stock_code_0": "000001",
    }


def test_a_list_date_response_for_wrong_date_is_not_published(monkeypatch):
    writes: list[object] = []
    provider = SimpleNamespace(
        hot=SimpleNamespace(
            list_a_list_daily=lambda report_date: pd.DataFrame(
                [{"trade_date": "2026-08-24", "stock_code": "000001"}]
            )
        )
    )
    monkeypatch.setattr(sync_sentiment, "_a_list_report_dates", lambda _engine: ["2026-08-25"])
    monkeypatch.setattr(sync_sentiment, "_sleep", lambda: None)
    monkeypatch.setattr(
        sync_sentiment,
        "retry_remote",
        lambda function, *args, **kwargs: function(*args, **kwargs),
    )
    monkeypatch.setattr(
        sync_sentiment,
        "replace_table_rows",
        lambda *_args, **_kwargs: writes.append(True),
    )

    try:
        sync_sentiment.step_a_list_daily(object(), provider)
    except RuntimeError as exc:
        assert "no a-list daily rows" in str(exc)
    else:
        raise AssertionError("wrong-date response must not replace the requested date")

    assert writes == []
