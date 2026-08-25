from __future__ import annotations

from datetime import datetime

from server.trading_v2 import candidate_context


def test_capital_flow_without_etl_sync_at_is_data_blocked(monkeypatch):
    monkeypatch.setattr(
        candidate_context,
        "_columns",
        lambda _engine, _table: {
            "stock_code",
            "trade_date",
            "main_net_inflow",
        },
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
    monkeypatch.setattr(
        candidate_context,
        "_columns",
        lambda _engine, _table: {
            "stock_code",
            "trade_date",
            "main_net_inflow",
            "etl_sync_at",
        },
    )
    observed: dict[str, object] = {}

    def rows(_engine, sql, params):
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
    assert "etl_sync_at IS NOT NULL" in str(observed["sql"])
    assert "etl_sync_at <= :decision_at" in str(observed["sql"])
    assert observed["params"]["decision_at"] == decision_at
