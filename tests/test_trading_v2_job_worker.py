from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from server.trading_v2 import job_worker


def _request() -> dict[str, object]:
    return {
        "strategy_version": "etf_trend_risk_v2.0.0",
        "start_date": "2021-01-04",
        "end_date": "2026-07-24",
        "random_seed": 20260726,
    }


def test_backtest_failure_is_compensated_before_error_propagates():
    engine = MagicMock()
    error = ModuleNotFoundError("missing backtest adapter")
    with patch.object(
        job_worker,
        "_run_backtest_job_impl",
        side_effect=error,
    ), patch.object(
        job_worker,
        "_mark_latest_matching_backtest_failed",
        return_value=1,
    ) as mark_failed:
        with pytest.raises(ModuleNotFoundError, match="missing"):
            job_worker._run_backtest_job(engine, _request())

    mark_failed.assert_called_once()
    assert mark_failed.call_args.args[0] is engine
    assert mark_failed.call_args.args[1] == _request()
    assert mark_failed.call_args.args[2] is error


def test_failure_compensation_only_updates_matching_running_row():
    engine = MagicMock()
    connection = engine.begin.return_value.__enter__.return_value
    connection.execute.return_value.rowcount = 1
    error = RuntimeError("calculation failed")

    changed = job_worker._mark_latest_matching_backtest_failed(
        engine,
        _request(),
        error,
    )

    assert changed == 1
    statement, params = connection.execute.call_args.args
    sql = str(statement)
    assert "status = 'RUNNING'" in sql
    assert "ORDER BY started_at DESC" in sql
    assert params["strategy_version"] == "etf_trend_risk_v2.0.0"
    assert params["error_code"] == "RUNTIMEERROR"
    assert params["error_message"] == "calculation failed"
