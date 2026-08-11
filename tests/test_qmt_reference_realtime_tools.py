from __future__ import annotations

import os
from datetime import datetime
from unittest.mock import patch

import pandas as pd

from tools import simulate_qmt_intraday_realtime, sync_guojin_qmt_reference_data


class _QueryResult:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows

    def mappings(self):
        return self


class _ConnectionContext:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc, tb):
        return False


class _SchemaAwareConnection:
    def __init__(self, columns, rows):
        self.columns = columns
        self.rows = rows
        self.statements = []

    def execute(self, statement, params=None):
        sql = str(statement)
        self.statements.append((sql, params))
        if "information_schema.COLUMNS" in sql:
            return _QueryResult([(column,) for column in self.columns])
        return _QueryResult(self.rows)


class _SchemaAwareEngine:
    def __init__(self, connection):
        self.connection = connection

    def begin(self):
        return _ConnectionContext(self.connection)


def test_sync_reference_data_uses_batch_engine_for_dry_run():
    engine = object()
    empty_details = pd.DataFrame(columns=["qmt_code", "stock_code", "short_name", "exchange", "list_date"])

    with patch(
        "tools.sync_guojin_qmt_reference_data.create_batch_engine",
        return_value=engine,
    ) as create_batch_engine, patch(
        "tools.sync_guojin_qmt_reference_data.ensure_reference_tables",
    ) as ensure_reference_tables, patch(
        "tools.sync_guojin_qmt_reference_data.bridge.sector_list",
        return_value=pd.DataFrame(columns=["sector_name"]),
    ), patch(
        "tools.sync_guojin_qmt_reference_data.bridge.sector_members_many",
        return_value=pd.DataFrame(),
    ), patch(
        "tools.sync_guojin_qmt_reference_data.fetch_sector_datasets",
        return_value={},
    ), patch(
        "tools.sync_guojin_qmt_reference_data._read_stock_qmt_codes",
        return_value=[],
    ) as read_stock_qmt_codes, patch(
        "tools.sync_guojin_qmt_reference_data._read_index_qmt_codes",
        return_value=[],
    ) as read_index_qmt_codes, patch(
        "tools.sync_guojin_qmt_reference_data._fetch_instrument_details",
        side_effect=[empty_details.copy(), empty_details.copy()],
    ), patch(
        "tools.sync_guojin_qmt_reference_data.bridge.index_weight_many",
        return_value=pd.DataFrame(columns=["index_code", "stock_code"]),
    ):
        result = sync_guojin_qmt_reference_data.sync_reference_data(
            start_year=2026,
            end_year=2026,
            iscomplete=False,
            refresh_timeout=1,
            skip_refresh=True,
            dry_run=True,
        )

    create_batch_engine.assert_called_once_with(future=True)
    ensure_reference_tables.assert_called_once_with(engine)
    read_stock_qmt_codes.assert_called_once_with(engine)
    assert read_index_qmt_codes.call_args_list[0].args[0] is engine
    assert result["status"] == "dry_run"


def test_run_simulation_uses_batch_engine_when_engine_not_provided():
    engine = object()

    with patch(
        "tools.simulate_qmt_intraday_realtime.create_batch_engine",
        return_value=engine,
    ) as create_batch_engine, patch(
        "tools.simulate_qmt_intraday_realtime.sync_qmt_realtime",
        return_value={"received": 0, "coverage": 1.0},
    ) as sync_qmt_realtime, patch(
        "tools.simulate_qmt_intraday_realtime._current_rows",
        return_value=[],
    ) as current_rows:
        result = simulate_qmt_intraday_realtime.run_simulation(
            cycles=1,
            interval_seconds=0,
            codes=["000001"],
            use_gateway=True,
        )

    create_batch_engine.assert_called_once_with(future=True)
    assert sync_qmt_realtime.call_args.kwargs["engine"] is engine
    current_rows.assert_called_once_with(engine, ["000001"])
    assert result["cycles"] == 1


def test_current_rows_adapts_to_canonical_production_schema_without_optional_qmt_columns():
    sync_at = datetime.now()
    connection = _SchemaAwareConnection(
        columns={
            "stock_code",
            "short_name",
            "price",
            "change",
            "change_pct",
            "volume",
            "amount",
            "snapshot_at",
            "etl_sync_at",
        },
        rows=[
            {
                "stock_code": "000001",
                "short_name": "平安银行",
                "price": 10.0,
                "change": 0.1,
                "change_pct": 1.0,
                "volume": 100,
                "amount": 1000,
                "snapshot_at": sync_at,
                "etl_sync_at": sync_at,
            }
        ],
    )

    rows = simulate_qmt_intraday_realtime._current_rows(
        _SchemaAwareEngine(connection),
        ["000001"],
    )

    data_query = connection.statements[1][0]
    assert "`etl_sync_at`" in data_query
    assert "`received_at`" not in data_query
    assert "`data_source`" not in data_query
    assert rows[0]["received_latency_seconds"] is not None
    assert rows[0]["page_live_quote_eligible"] is True


def test_run_simulation_reuses_provided_engine():
    engine = object()

    with patch("tools.simulate_qmt_intraday_realtime.create_batch_engine") as create_batch_engine, patch(
        "tools.simulate_qmt_intraday_realtime.sync_qmt_realtime",
        return_value={"received": 0, "coverage": 1.0},
    ) as sync_qmt_realtime, patch(
        "tools.simulate_qmt_intraday_realtime._current_rows",
        return_value=[],
    ):
        simulate_qmt_intraday_realtime.run_simulation(
            cycles=1,
            interval_seconds=0,
            codes=["000001"],
            use_gateway=True,
            engine=engine,
        )

    create_batch_engine.assert_not_called()
    assert sync_qmt_realtime.call_args.kwargs["engine"] is engine


def test_run_simulation_uses_production_bigqmt_spool_by_default(monkeypatch):
    engine = object()
    monkeypatch.delenv("QMT_GATEWAY_ENABLED", raising=False)

    def fake_sync_big_qmt_realtime(**kwargs):
        assert os.environ["QMT_GATEWAY_ENABLED"] == "0"
        return {"requested": 1, "received": 1, "tracked_rows": 1}

    with patch(
        "tools.simulate_qmt_intraday_realtime.sync_big_qmt_realtime",
        side_effect=fake_sync_big_qmt_realtime,
    ) as sync_big_qmt_realtime, patch(
        "tools.simulate_qmt_intraday_realtime._current_rows",
        return_value=[],
    ):
        simulate_qmt_intraday_realtime.run_simulation(
            cycles=1,
            interval_seconds=0,
            codes=["000001"],
            use_gateway=False,
            engine=engine,
        )

    assert sync_big_qmt_realtime.called
    assert "QMT_GATEWAY_ENABLED" not in os.environ


def test_simulation_main_reuses_engine_for_resolve_and_run():
    engine = object()
    result = {"status": "success", "cycles": 1, "results": []}

    with patch.object(
        simulate_qmt_intraday_realtime.sys,
        "argv",
        [
            "simulate_qmt_intraday_realtime.py",
            "--codes",
            "1,2",
            "--limit",
            "2",
            "--cycles",
            "1",
            "--interval-seconds",
            "0",
            "--json",
        ],
    ), patch(
        "tools.simulate_qmt_intraday_realtime.create_batch_engine",
        return_value=engine,
    ) as create_batch_engine, patch(
        "tools.simulate_qmt_intraday_realtime._resolve_codes",
        return_value=["000001", "000002"],
    ) as resolve_codes, patch(
        "tools.simulate_qmt_intraday_realtime.run_simulation",
        return_value=result,
    ) as run_simulation:
        assert simulate_qmt_intraday_realtime.main() == 0

    create_batch_engine.assert_called_once_with(future=True)
    resolve_codes.assert_called_once_with(engine, "1,2", limit=2)
    assert run_simulation.call_args.kwargs["engine"] is engine
