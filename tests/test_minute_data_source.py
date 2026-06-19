# -*- coding: utf-8 -*-
from types import SimpleNamespace

from server.common import minute_data


def test_minute_table_defaults_to_legacy(monkeypatch):
    monkeypatch.setattr(
        minute_data,
        "get_settings",
        lambda: SimpleNamespace(minute_data_source="legacy", minute_stock_table="", minute_mysql_url=""),
    )

    assert minute_data.get_minute_stock_table() == "sm_stock_minute"
    assert minute_data.minute_source_info()["kind"] == "legacy"


def test_minute_table_uses_gm_source(monkeypatch):
    monkeypatch.setattr(
        minute_data,
        "get_settings",
        lambda: SimpleNamespace(minute_data_source="gm", minute_stock_table="", minute_mysql_url="mysql://local"),
    )

    info = minute_data.minute_source_info()

    assert info["table"] == "sm_stock_minute_gm"
    assert info["kind"] == "ohlc"
    assert info["external"] is True


def test_minute_table_override_is_validated(monkeypatch):
    monkeypatch.setattr(
        minute_data,
        "get_settings",
        lambda: SimpleNamespace(minute_data_source="gm", minute_stock_table="bad;drop"),
    )

    try:
        minute_data.get_minute_stock_table()
    except ValueError as exc:
        assert "Invalid minute table name" in str(exc)
    else:
        raise AssertionError("expected invalid table name to fail")
