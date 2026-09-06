from __future__ import annotations

from datetime import datetime

import pytest

from server.trading_v2 import candidate_context


def test_general_optional_column_probe_keeps_existing_empty_fallback(monkeypatch):
    class BrokenInspector:
        def get_columns(self, _table):
            raise RuntimeError("optional schema unavailable")

    monkeypatch.setattr(candidate_context, "inspect", lambda _engine: BrokenInspector())

    assert candidate_context._columns(object(), "another_optional_table") == set()


def test_capital_flow_without_etl_sync_at_is_data_blocked(monkeypatch):
    flow_engine = object()
    monkeypatch.setattr(candidate_context, "get_minute_engine", lambda: flow_engine)
    monkeypatch.setattr(
        candidate_context,
        "_flow_columns",
        lambda engine: {
            "stock_code",
            "trade_date",
            "main_net_inflow",
        } if engine is flow_engine else set(),
    )

    def forbidden_rows(*_args, **_kwargs):
        raise AssertionError("unversioned capital flow must not be queried")

    monkeypatch.setattr(candidate_context, "_rows", forbidden_rows)

    facts, source = candidate_context._load_flows(
        object(),
        ["000001"],
        "2026-08-24",
        datetime(2026, 8, 24, 15, 5),
    )

    assert facts == {}
    assert source["status"] == "DATA_BLOCKED"
    assert source["pit_reason"] == "CAPITAL_FLOW_ETL_SYNC_AT_UNAVAILABLE"


def test_capital_flow_query_requires_non_null_etl_before_decision(monkeypatch):
    flow_engine = object()
    monkeypatch.setattr(candidate_context, "get_minute_engine", lambda: flow_engine)
    monkeypatch.setattr(
        candidate_context,
        "_flow_columns",
        lambda engine: {
            "stock_code",
            "trade_date",
            "main_net_inflow",
            "etl_sync_at",
        } if engine is flow_engine else set(),
    )
    observed: dict[str, object] = {}

    def rows(engine, sql, params):
        observed["engine"] = engine
        observed["sql"] = " ".join(sql.split())
        observed["params"] = params
        return []

    monkeypatch.setattr(candidate_context, "_rows", rows)
    decision_at = datetime(2026, 8, 24, 15, 5)

    facts, source = candidate_context._load_flows(
        object(),
        ["000001"],
        "2026-08-24",
        decision_at,
    )

    assert facts == {}
    assert source["status"] == "NO_ROWS"
    assert observed["engine"] is flow_engine
    assert "etl_sync_at IS NOT NULL" in str(observed["sql"])
    assert "etl_sync_at <= :decision_at" in str(observed["sql"])
    assert observed["params"]["decision_at"] == decision_at


@pytest.mark.parametrize("failure_at", ["engine", "columns", "rows"])
def test_capital_flow_source_failure_is_safely_unavailable(monkeypatch, failure_at):
    flow_engine = object()
    secret = "mysql://reader:SECRET_PASSWORD@private.example/market"

    def engine():
        if failure_at == "engine":
            raise RuntimeError(secret)
        return flow_engine

    def columns(engine_value):
        assert engine_value is flow_engine
        if failure_at == "columns":
            raise RuntimeError(secret)
        return {"stock_code", "trade_date", "main_net_inflow", "etl_sync_at"}

    def rows(engine_value, _sql, _params):
        assert engine_value is flow_engine
        if failure_at == "rows":
            raise RuntimeError(secret)
        return []

    monkeypatch.setattr(candidate_context, "get_minute_engine", engine)
    monkeypatch.setattr(candidate_context, "_flow_columns", columns)
    monkeypatch.setattr(candidate_context, "_rows", rows)

    facts, source = candidate_context._load_flows(
        object(),
        ["000001"],
        "2026-08-24",
        datetime(2026, 8, 24, 15, 5),
    )

    assert facts == {}
    assert source == {
        "status": "UNAVAILABLE",
        "row_count": 0,
        "latest_at": "",
        "note": "个股日资金流数据源暂不可用",
        "reason": "CAPITAL_FLOW_SOURCE_UNAVAILABLE",
    }
    assert "SECRET_PASSWORD" not in str(source)
    assert "private.example" not in str(source)
