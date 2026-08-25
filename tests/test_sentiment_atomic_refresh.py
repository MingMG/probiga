from __future__ import annotations

import inspect
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from biz.sentiment import sync_sentiment


def test_generic_full_table_writer_fails_closed_for_nonempty_frame():
    frame = pd.DataFrame([{"stock_code": "000001", "value": 1}])
    with pytest.raises(RuntimeError, match="通用全表替换已禁用"):
        sync_sentiment.df_to_table(object(), frame, "st_example")


def test_hot_concept_routes_both_views_to_one_atomic_publisher(monkeypatch):
    columns = {
        "rank": list(range(1, 21)),
        "concept_code": [f"C{index:02d}" for index in range(1, 21)],
        "concept_name": [f"测试{index}" for index in range(1, 21)],
        "change_pct": [1.0] * 20,
        "hot_value": [10.0] * 20,
        "hot_tag": ["热"] * 20,
    }
    provider = SimpleNamespace(
        hot=SimpleNamespace(
            hot_concept_20_ths=lambda plate_type: pd.DataFrame(columns)
        )
    )
    publishes: list[dict] = []
    monkeypatch.setattr(sync_sentiment, "_sleep", lambda: None)
    monkeypatch.setattr(sync_sentiment, "_request_date", lambda: "2026-08-25")
    monkeypatch.setattr(
        sync_sentiment,
        "_publish_hot_concept_snapshots",
        lambda engine, **kwargs: publishes.append({"engine": engine, **kwargs}),
    )

    engine = object()
    sync_sentiment.step_hot_concept(engine, provider)

    assert len(publishes) == 1
    assert publishes[0]["engine"] is engine
    assert publishes[0]["snapshot_date"] == "2026-08-25"
    assert len(publishes[0]["realtime"]) == 40
    assert len(publishes[0]["daily"]) == 40


def test_hot_concept_empty_source_shard_preserves_both_snapshots(monkeypatch):
    provider = SimpleNamespace(
        hot=SimpleNamespace(
            hot_concept_20_ths=lambda plate_type: (
                pd.DataFrame({"concept_code": ["C1"]})
                if plate_type == 1
                else pd.DataFrame()
            )
        )
    )
    writes: list[str] = []
    monkeypatch.setattr(sync_sentiment, "_sleep", lambda: None)
    monkeypatch.setattr(
        sync_sentiment,
        "_publish_hot_concept_snapshots",
        lambda *_args, **_kwargs: writes.append("published"),
    )

    with pytest.raises(RuntimeError, match="snapshot is incomplete"):
        sync_sentiment.step_hot_concept(object(), provider)

    assert writes == []


def test_truncated_nonempty_margin_refresh_preserves_missing_history(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE st_securities_margin ("
                "trade_date TEXT PRIMARY KEY, rzye INTEGER, etl_sync_at TEXT)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO st_securities_margin(trade_date,rzye,etl_sync_at) VALUES "
                "('2026-08-22', 7, 'old'), ('2026-08-25', 8, 'old')"
            )
        )
    provider = SimpleNamespace(
        securities_margin=lambda start_date: pd.DataFrame(
            [{"trade_date": "2026-08-25", "rzye": 99}]
        )
    )
    monkeypatch.setenv("SE_MARGIN_START", "2026-08-01")
    monkeypatch.setattr(sync_sentiment, "_request_date", lambda: "2026-08-25")
    monkeypatch.setattr(sync_sentiment, "_sleep", lambda: None)

    sync_sentiment.step_margin(engine, provider)

    with engine.connect() as connection:
        rows = connection.execute(
            text(
                "SELECT trade_date,rzye FROM st_securities_margin "
                "ORDER BY trade_date"
            )
        ).fetchall()
    assert [tuple(row) for row in rows] == [
        ("2026-08-22", 7),
        ("2026-08-25", 99),
    ]


def test_wrong_date_history_result_is_not_published(monkeypatch):
    writes: list[object] = []
    provider = SimpleNamespace(
        securities_margin=lambda start_date: pd.DataFrame(
            [{"trade_date": "2026-07-31", "rzye": 99}]
        )
    )
    monkeypatch.setenv("SE_MARGIN_START", "2026-08-01")
    monkeypatch.setattr(sync_sentiment, "_request_date", lambda: "2026-08-25")
    monkeypatch.setattr(
        sync_sentiment,
        "replace_table_rows",
        lambda *_args, **_kwargs: writes.append(True),
    )

    with pytest.raises(RuntimeError, match="date coverage mismatch"):
        sync_sentiment.step_margin(object(), provider)
    assert writes == []


def test_interrupted_hot_rank_page_is_not_published(monkeypatch):
    writes: list[object] = []
    provider = SimpleNamespace(
        hot=SimpleNamespace(
            hot_rank_100_ths=lambda: pd.DataFrame(
                {
                    "rank": list(range(1, 100)),
                    "stock_code": [f"{index:06d}" for index in range(1, 100)],
                }
            )
        )
    )
    monkeypatch.setattr(sync_sentiment, "_request_date", lambda: "2026-08-25")
    monkeypatch.setattr(
        sync_sentiment,
        "replace_table_rows",
        lambda *_args, **_kwargs: writes.append(True),
    )

    with pytest.raises(RuntimeError, match="incomplete Top-100 snapshot"):
        sync_sentiment.step_hot_ths(object(), provider)
    assert writes == []


def test_hot_concept_second_table_failure_rolls_back_both_views():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE st_hot_concept_ths_rt ("
                "concept_code TEXT PRIMARY KEY, rank INTEGER)"
            )
        )
        connection.execute(
            text(
                "CREATE TABLE st_hot_concept_ths_daily ("
                "snapshot_date TEXT, plate_type INTEGER, "
                "concept_code TEXT, rank INTEGER)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO st_hot_concept_ths_rt(concept_code,rank) "
                "VALUES ('OLD', 1)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO st_hot_concept_ths_daily("
                "snapshot_date,plate_type,concept_code,rank) "
                "VALUES ('2026-08-25', 1, 'OLD', 1)"
            )
        )

    realtime = pd.DataFrame([{"concept_code": "NEW", "rank": 1}])
    bad_daily = pd.DataFrame(
        [
            {
                "snapshot_date": "2026-08-25",
                "plate_type": 1,
                "concept_code": "NEW",
                "rank": 1,
                "unexpected_column": "force second insert failure",
            }
        ]
    )
    with pytest.raises(Exception):
        sync_sentiment._publish_hot_concept_snapshots(
            engine,
            realtime=realtime,
            daily=bad_daily,
            snapshot_date="2026-08-25",
        )

    with engine.connect() as connection:
        rt = connection.execute(
            text("SELECT concept_code,rank FROM st_hot_concept_ths_rt")
        ).one()
        daily = connection.execute(
            text(
                "SELECT snapshot_date,plate_type,concept_code,rank "
                "FROM st_hot_concept_ths_daily"
            )
        ).one()
    assert tuple(rt) == ("OLD", 1)
    assert tuple(daily) == ("2026-08-25", 1, "OLD", 1)


def test_sentiment_runtime_has_no_preclear_path():
    source = inspect.getsource(sync_sentiment)
    main_source = inspect.getsource(sync_sentiment.main)
    assert "truncate_all_sentiment(engine)" not in main_source
    for step in (
        sync_sentiment.step_lifting,
        sync_sentiment.step_margin,
        sync_sentiment.step_north_daily,
        sync_sentiment.step_north_min,
        sync_sentiment.step_north_current,
        sync_sentiment.step_hot_east,
        sync_sentiment.step_hot_ths,
        sync_sentiment.step_hot_concept,
        sync_sentiment.step_mine,
    ):
        step_source = inspect.getsource(step)
        assert "truncate_only(" not in step_source
        assert "df_to_table(" not in step_source
    assert "TRUNCATE TABLE" not in source.upper()
