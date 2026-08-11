# -*- coding: utf-8 -*-
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import patch

from sqlalchemy import create_engine, text

from server.common.sql_reader import normalize_sql_value, read_sql_rows, sql_preview


def test_read_sql_rows_returns_mapping_rows():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE sample (id INTEGER, name TEXT)"))
        conn.execute(text("INSERT INTO sample (id, name) VALUES (1, 'alpha')"))

    rows = read_sql_rows(engine, "SELECT id, name FROM sample WHERE id = :id", {"id": 1}, context="test")

    assert rows == [{"id": 1, "name": "alpha"}]


def test_normalize_sql_value_handles_decimal_and_nan():
    assert normalize_sql_value(Decimal("12.34")) == 12.34
    assert normalize_sql_value(float("nan")) is None


def test_normalize_sql_value_can_stringify_dates():
    assert normalize_sql_value(date(2026, 7, 5), stringify_datetime=True) == "2026-07-05"
    assert (
        normalize_sql_value(datetime(2026, 7, 5, 9, 30, 1), stringify_datetime=True)
        == "2026-07-05 09:30:01"
    )


def test_sql_preview_compacts_and_truncates():
    preview = sql_preview("SELECT\n  *\nFROM table_name WHERE name = :name", limit=24)

    assert "\n" not in preview
    assert preview.endswith("...")
    assert ":name" in sql_preview("SELECT * FROM t WHERE name = :name")


def test_read_sql_rows_logs_slow_sql_without_params():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE sample (id INTEGER, secret TEXT)"))
        conn.execute(text("INSERT INTO sample (id, secret) VALUES (1, 'hidden')"))

    with patch("server.common.sql_reader.get_api_observability_config", return_value={"slow_sql_ms": 1}), \
         patch("server.common.sql_reader.time.perf_counter", side_effect=[0.0, 1.0]):
        with patch("server.common.sql_reader.logger.warning") as warning_mock:
            rows = read_sql_rows(
                engine,
                "SELECT id FROM sample WHERE secret = :secret",
                {"secret": "hidden"},
                context="unit",
            )

    assert rows == [{"id": 1}]
    message = " ".join(str(part) for part in warning_mock.call_args.args)
    assert "Slow SQL" in message
    assert "hidden" not in message
