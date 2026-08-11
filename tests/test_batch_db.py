# -*- coding: utf-8 -*-
from unittest.mock import patch

import pandas as pd
from sqlalchemy import create_engine, text

import pytest

from server.common.batch_db import (
    create_batch_engine,
    qualified_table_name,
    quote_identifier,
    read_frame,
    read_frame_direct,
    read_records,
    records_from_frame,
    write_frame,
)


def test_create_batch_engine_uses_configured_mysql_url():
    fake_engine = object()
    with patch("server.common.batch_db.get_mysql_url", return_value="mysql://primary"), \
         patch("server.common.batch_db.create_pooled_engine", return_value=fake_engine) as create_engine_mock:
        assert create_batch_engine(future=True) is fake_engine

    create_engine_mock.assert_called_once_with("mysql://primary", future=True)


def test_read_frame_reads_sqlalchemy_text_against_engine():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE sample (id INTEGER, name TEXT)"))
        conn.execute(text("INSERT INTO sample (id, name) VALUES (1, 'alpha')"))

    frame = read_frame(text("SELECT id, name FROM sample WHERE id = :id"), engine, params={"id": 1})

    assert frame.to_dict(orient="records") == [{"id": 1, "name": "alpha"}]


def test_records_from_frame_normalizes_missing_values():
    frame = pd.DataFrame([{"a": 1.0, "b": float("nan"), "c": pd.NaT}])

    assert records_from_frame(frame) == [{"a": 1.0, "b": None, "c": None}]


def test_read_records_can_ignore_query_errors():
    engine = create_engine("sqlite:///:memory:")

    assert read_records("SELECT * FROM missing_table", engine, ignore_errors=True) == []


def test_read_frame_routes_pure_kline_query_to_kline_engine():
    primary_engine = object()
    kline_engine = object()
    expected = pd.DataFrame([{"d": "2026-07-01"}])
    with patch("server.common.batch_db.should_use_kline_engine", return_value=True), \
         patch("server.common.batch_db.get_kline_engine", return_value=kline_engine), \
         patch("server.common.batch_db.pd.read_sql", return_value=expected) as read_sql:
        out = read_frame("SELECT MAX(trade_date) AS d FROM sm_stock_kline", primary_engine)

    assert out is expected
    assert read_sql.call_args.args[1] is kline_engine


def test_read_frame_direct_uses_explicit_engine_without_routing():
    explicit_engine = object()
    expected = pd.DataFrame([{"d": "2026-07-01"}])
    with patch(
        "server.common.batch_db.get_kline_engine",
        side_effect=AssertionError("direct reads must not route"),
    ), patch("server.common.batch_db.pd.read_sql", return_value=expected) as read_sql:
        out = read_frame_direct("SELECT MAX(trade_date) AS d FROM sm_stock_kline", explicit_engine)

    assert out is expected
    assert read_sql.call_args.args[1] is explicit_engine


def test_quote_identifier_rejects_unsafe_names():
    assert quote_identifier("si_all_code") == "`si_all_code`"
    assert qualified_table_name("probiga", "sm_stock_kline") == "`probiga`.`sm_stock_kline`"

    with pytest.raises(ValueError):
        quote_identifier("si_all_code;DROP")
    with pytest.raises(ValueError):
        quote_identifier("probiga.sm_stock_kline")


def test_write_frame_writes_valid_table_and_returns_row_count():
    engine = create_engine("sqlite:///:memory:")
    frame = pd.DataFrame([{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}])

    assert write_frame(frame, "sample", engine, if_exists="replace") == 2

    out = read_frame(text("SELECT id, name FROM sample ORDER BY id"), engine)
    assert out.to_dict(orient="records") == [
        {"id": 1, "name": "alpha"},
        {"id": 2, "name": "beta"},
    ]


def test_write_frame_skips_empty_frames_and_validates_table_name():
    engine = create_engine("sqlite:///:memory:")

    assert write_frame(pd.DataFrame(), "sample", engine) == 0

    with pytest.raises(ValueError):
        write_frame(pd.DataFrame([{"id": 1}]), "sample;DROP", engine)
