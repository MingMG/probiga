from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from biz.stock_market import sync_stock_snapshot
from server.common.scheduler_validation import (
    TASK_OUTPUT_REQUIREMENTS,
    _resolve_target_date,
)
from server.common.daily_stock_universe import DailyStockUniverse
from tools import refresh_market_overview_daily


def _kline_frame(trade_date: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "stock_code": "000001",
                "short_name": "sample",
                "trade_date": trade_date,
                "open": 10.0,
                "close": 10.5,
                "high": 11.0,
                "low": 9.5,
                "pre_close": 10.0,
                "change": 0.5,
                "change_pct": 5.0,
                "volume": 100,
                "amount": 1000,
                "turnover_ratio": 1.0,
            }
        ]
    )


def _universe(target: str = "2026-08-26") -> DailyStockUniverse:
    return DailyStockUniverse(
        target_date=target,
        catalog_batch_id="catalog-1",
        catalog_manifest_hash="a" * 64,
        catalog_member_set_hash="b" * 64,
        expected_codes=("000001",),
        expected_code_set_hash="c" * 64,
    )


def test_stock_snapshot_reads_quotes_and_flow_only_from_target_date(monkeypatch):
    target = "2026-08-26"
    observations: list[tuple[str, dict]] = []

    def fake_read_sql(statement, engine, params=None):
        sql = " ".join(str(statement).split())
        observations.append((sql, dict(params or {})))
        if "FROM sm_stock_kline k" in sql:
            return _kline_frame(target)
        if "FROM sm_stock_current" in sql:
            return pd.DataFrame(
                [{"stock_code": "000001", "cur_price": 10.8, "cur_change_pct": 8.0}]
            )
        if "FROM sm_stock_capital_flow_daily" in sql:
            return pd.DataFrame(
                [
                    {
                        "stock_code": "000001",
                        "main_net_inflow": 1,
                        "max_net_inflow": 2,
                        "lg_net_inflow": 3,
                        "mid_net_inflow": 4,
                        "sm_net_inflow": 5,
                    }
                ]
            )
        if "close AS close_n" in sql:
            return pd.DataFrame([{"stock_code": "000001", "close_n": 10.0}])
        if "FROM si_stock_shares" in sql:
            return pd.DataFrame([{"stock_code": "000001", "total_shares": 1000}])
        if "FROM si_industry_sw" in sql:
            return pd.DataFrame([{"stock_code": "000001", "industry_name": "bank"}])
        raise AssertionError(sql)

    monkeypatch.setattr(sync_stock_snapshot.pd, "read_sql", fake_read_sql)
    monkeypatch.setattr(
        sync_stock_snapshot,
        "load_daily_stock_universe",
        lambda engine, trade_date: _universe(trade_date),
    )
    monkeypatch.setattr(
        sync_stock_snapshot,
        "get_nth_trade_date",
        lambda engine, trade_date, offset: target,
    )

    result = sync_stock_snapshot.fetch_snapshot(object(), target)

    current_sql, current_params = next(
        item for item in observations if "FROM sm_stock_current" in item[0]
    )
    flow_sql, flow_params = next(
        item
        for item in observations
        if "FROM sm_stock_capital_flow_daily" in item[0]
    )
    assert "DATE(snapshot_at) = :d" in current_sql
    assert current_params == {"d": target}
    assert "WHERE trade_date = :d" in flow_sql
    assert "MAX(trade_date)" not in flow_sql
    assert flow_params == {"d": target}
    assert result.iloc[0]["price"] == 10.8
    assert result.iloc[0]["main_net_inflow"] == 1


def test_stock_snapshot_blocks_before_other_sources_when_target_kline_is_empty(
    monkeypatch,
):
    calls: list[str] = []

    def fake_read_sql(statement, engine, params=None):
        calls.append(str(statement))
        return _kline_frame("2026-08-26").iloc[0:0]

    monkeypatch.setattr(sync_stock_snapshot.pd, "read_sql", fake_read_sql)
    monkeypatch.setattr(
        sync_stock_snapshot,
        "load_daily_stock_universe",
        lambda engine, trade_date: _universe(trade_date),
    )

    with pytest.raises(RuntimeError, match="daily K-line set differs"):
        sync_stock_snapshot.fetch_snapshot(object(), "2026-08-26")

    assert len(calls) == 1


def test_stock_snapshot_cli_returns_nonzero_without_target_kline(monkeypatch):
    class Engine:
        disposed = False

        def dispose(self):
            self.disposed = True

    engine = Engine()
    writes: list[pd.DataFrame] = []
    monkeypatch.setattr(sync_stock_snapshot, "get_engine", lambda: engine)
    monkeypatch.setattr(
        sync_stock_snapshot,
        "fetch_snapshot",
        lambda engine, trade_date: (_ for _ in ()).throw(
            RuntimeError("DATA_BLOCKED: target-date daily K-line is missing")
        ),
    )
    monkeypatch.setattr(
        sync_stock_snapshot,
        "write_snapshot",
        lambda engine, frame: writes.append(frame),
    )

    status = sync_stock_snapshot.main(["--date", "2026-08-26"])

    assert status == 2
    assert writes == []
    assert engine.disposed is True


class _MappingResult:
    def __init__(self, row):
        self.row = row

    def mappings(self):
        return self

    def first(self):
        return self.row

    def all(self):
        return self.row


def test_market_overview_blocks_before_write_when_target_kline_is_incomplete():
    class KlineConnection:
        def execute(self, statement, params):
            sql = " ".join(str(statement).split())
            if "SELECT stock_code, volume, amount" in sql:
                return _MappingResult([])
            raise AssertionError("aggregate must not run after coverage failure")

    class OutputConnection:
        def execute(self, statement, params):
            raise AssertionError("incomplete input must not be published")

    with pytest.raises(RuntimeError, match="daily K-line set differs"):
        refresh_market_overview_daily.refresh_one(
            OutputConnection(),
            KlineConnection(),
            "2026-08-26",
            universe=_universe(),
        )


class _Context:
    def __init__(self, value):
        self.value = value

    def __enter__(self):
        return self.value

    def __exit__(self, exc_type, exc, traceback):
        return False


class _FakeEngine:
    def __init__(self):
        self.disposed = False

    def begin(self):
        return _Context(object())

    def connect(self):
        return _Context(object())

    def dispose(self):
        self.disposed = True


@pytest.mark.parametrize(
    ("result", "expected_status", "expected_marker"),
    [
        (
            {
                "date": "2026-08-26",
                "status": "DATA_BLOCKED",
                "reason": "target_date_kline_missing_or_incomplete",
            },
            2,
            False,
        ),
        (
            {"date": "2026-08-26", "status": "ok", "total": 5200},
            0,
            True,
        ),
    ],
)
def test_market_overview_cli_status_and_date_marker(
    monkeypatch,
    capsys,
    result,
    expected_status,
    expected_marker,
):
    output_engine = _FakeEngine()
    kline_engine = _FakeEngine()
    monkeypatch.setattr(refresh_market_overview_daily, "load_project_env", lambda: None)
    monkeypatch.setattr(
        refresh_market_overview_daily,
        "create_batch_engine",
        lambda **kwargs: output_engine,
    )
    monkeypatch.setattr(
        refresh_market_overview_daily,
        "get_kline_engine",
        lambda: kline_engine,
    )
    monkeypatch.setattr(
        refresh_market_overview_daily,
        "validate_market_overview_daily_runtime_schema",
        lambda engine: None,
    )
    monkeypatch.setattr(
        refresh_market_overview_daily,
        "resolve_dates",
        lambda connection, **kwargs: ["2026-08-26"],
    )
    monkeypatch.setattr(
        refresh_market_overview_daily,
        "load_daily_stock_universe",
        lambda engine, trade_date: _universe(trade_date),
    )
    monkeypatch.setattr(
        refresh_market_overview_daily,
        "refresh_one",
        lambda connection, kline_connection, trade_date, **kwargs: result,
    )

    status = refresh_market_overview_daily.main(["2026-08-26"])
    output = capsys.readouterr()

    assert status == expected_status
    assert ("DATE=2026-08-26" in output.out) is expected_marker
    assert output_engine.disposed is True
    assert kline_engine.disposed is True


@pytest.mark.parametrize(
    "task_type",
    ["stock_snapshot_daily", "market_overview_daily"],
)
def test_daily_derived_validator_uses_emitted_output_date(task_type):
    requirement = TASK_OUTPUT_REQUIREMENTS[task_type][0]

    assert requirement.target == "output_date"
    assert _resolve_target_date(
        object(),
        requirement,
        started_at=datetime(2026, 8, 26, 18, 20),
        now=datetime(2026, 8, 26, 18, 30),
        output="completed\nDATE=2026-08-26",
    ).isoformat() == "2026-08-26"
