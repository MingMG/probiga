from datetime import datetime
from unittest.mock import MagicMock

import pandas as pd
import pytest
from sqlalchemy import create_engine, event, text

from biz.analysis import sync_analysis_fast as analysis
from server.common.strategy_daily_input_window import daily_input_snapshot
from server.trading_v3 import daily_features


def test_mysql_snapshot_sets_repeatable_read_before_starting_transaction():
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value
    connection.dialect.name = "mysql"
    connection.execution_options.return_value = connection
    with daily_input_snapshot(engine) as snapshot:
        assert snapshot is connection
        connection.execution_options.assert_called_once_with(isolation_level="REPEATABLE READ")
        connection.begin.assert_called_once_with()
    assert [call[0] for call in connection.method_calls][:2] == ["execution_options", "begin"]


@pytest.mark.parametrize("consumer", ["analysis", "v3"])
def test_concurrent_daily_replacement_cannot_change_already_verified_consumer_view(
    tmp_path, monkeypatch, consumer,
):
    engine = create_engine(f"sqlite+pysqlite:///{(tmp_path / 'daily.db').as_posix()}")

    @event.listens_for(engine, "connect")
    def explicit_transactions(dbapi_connection, _record):
        dbapi_connection.isolation_level = None
        dbapi_connection.execute("PRAGMA journal_mode=WAL")

    @event.listens_for(engine, "begin")
    def begin(connection):
        connection.exec_driver_sql("BEGIN")

    days = [day.date().isoformat() for day in pd.bdate_range(end="2026-09-18", periods=60)]
    rows = pd.DataFrame([
        {"stock_code": "000001", "short_name": "Example", "trade_date": day,
         "open": 10, "high": 11, "low": 9, "close": 10,
         "pre_close": 10, "volume": 1000, "amount": 10000,
         "change_pct": 0, "turnover_ratio": 2, "k_type": 1, "adjust_type": 0,
         "data_source": "gj_big_qmt_inner", "batch_id": "original", "data_version": "v1",
         "quality_status": "QMT_ATTESTED", "permission_status": "SUPPORTED",
         "received_at": f"{day} 17:00:00"}
        for day in days
    ])
    rows.to_sql("sm_stock_kline", engine, index=False)
    proof = {
        "sessions": days, "decision_known_at": "2026-09-18 18:00:00",
        "catalog_batches_by_session": {day: "catalog" for day in days},
        "daily_partition_roots": {day: "a" * 64 for day in days},
    }
    validated = []

    def validate(connection, **_kwargs):
        # This read stands for the native per-row truth validation. A writer
        # then replaces the rows before the consumer's first chunk SELECT.
        assert connection.in_transaction()
        assert connection.execute(text("SELECT COUNT(*) FROM sm_stock_kline")).scalar_one() == 60
        validated.append(connection)
        with engine.begin() as writer:
            writer.execute(text(
                "UPDATE sm_stock_kline SET close=999, received_at='2026-09-18 19:00:00'"
            ))
        return proof

    monkeypatch.setattr(analysis, "load_daily_input_window", validate)
    monkeypatch.setattr(daily_features, "validate_daily_input_sessions", validate)
    monkeypatch.setattr(analysis, "daily_input_catalog_join", lambda _window: ("", {}))
    monkeypatch.setattr(daily_features, "daily_input_catalog_join", lambda _window: ("", {}))
    monkeypatch.setattr(analysis, "_kline_rolling_state_path", lambda: None)
    monkeypatch.setattr(analysis, "_attach_canonical_chase_risk_evidence", lambda frame, *_a, **_k: frame)
    cutoff = datetime(2026, 9, 18, 18)
    if consumer == "analysis":
        result = analysis.load_kline_features(engine, days[-1], decision_known_at=cutoff)
        assert len(result) == 1
        assert result.iloc[0]["close"] == 10
    else:
        result = daily_features._load_bars(
            engine, dates=[datetime.fromisoformat(day).date() for day in days],
            decision_known_at=cutoff,
        )
        assert len(result) == 60
        assert set(result["raw_close"]) == {10}
    assert len(validated) == 1
    assert validated[0].closed
    with engine.connect() as connection:
        assert connection.execute(text("SELECT MIN(close) FROM sm_stock_kline")).scalar_one() == 999
