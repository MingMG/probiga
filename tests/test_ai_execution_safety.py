from __future__ import annotations

import inspect
import os
import sys
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

from biz.analysis import sync_analysis_fast, sync_sim_trade
from server.api.routers import hot_data
from server.api import scheduler_runtime
from tools import (
    ensure_quality_gate,
    run_ai_recommendation_premarket as premarket,
    run_ai_recommendation_worker as retired_worker,
)


UID = "a" * 32
BUILD_SHA = "b" * 40


def test_analysis_wall_clock_is_shanghai_even_when_host_clock_is_utc():
    utc_now = datetime(2026, 8, 27, 10, 50, tzinfo=timezone.utc)
    expected = datetime(2026, 8, 27, 18, 50)

    assert sync_analysis_fast._now_shanghai_naive(utc_now) == expected
    assert premarket._now_shanghai_naive(utc_now) == expected


def _stats():
    publication_receipt = {
        "schema": "probiga.analysis-strategy-pool-publication.v1",
        "canonical_pool_sha256": "c" * 64,
        "executable_count": 3,
    }
    return SimpleNamespace(
        trade_date="2026-08-24",
        analysis_count=5100,
        recommendation_count=30,
        market_mood_score=55.0,
        flow_date="2026-08-24",
        hot_date="2026-08-24",
        executable_count=3,
        canonical_pool_sha256="c" * 64,
        publication_receipt=publication_receipt,
    )


def _production_env():
    return {
        "PROBIGA_DEPLOYMENT_MODE": "production",
        "PROBIGA_SCHEDULER_HISTORY_RUN_UID": UID,
        "PROBIGA_SCHEDULER_BUILD_SHA": BUILD_SHA,
        "PROBIGA_BUILD_COMMIT_SHA": BUILD_SHA,
        "PROBIGA_SCHEDULER_TASK_TYPE": "analysis_fast",
    }


def test_scheduled_recommendation_uses_scheduler_uid_for_both_ledgers() -> None:
    engine = object()
    argv = [
        "run_ai_recommendation_premarket.py",
        "--date", "2026-08-24",
        "--strict-prev-trade-day",
        "--json",
    ]
    with patch.dict("os.environ", _production_env(), clear=False), patch.object(
        sys, "argv", argv
    ), patch.object(
        premarket, "create_batch_engine", return_value=engine
    ), patch.object(
        premarket, "_wait_for_db"
    ), patch.object(
        premarket, "_resolve_target_trade_date", return_value="2026-08-24"
    ), patch.object(
        premarket, "_recommended_run_history_start", return_value=UID
    ) as start, patch.object(
        premarket, "_recommended_run_history_update", return_value=True
    ), patch.object(
        premarket, "_recommended_run_history_finish", return_value={"status": "done"}
    ) as finish, patch.object(
        premarket, "run_batch", return_value=_stats()
    ):
        assert premarket.main() == 0

    assert start.call_args.kwargs["run_uid"] == UID
    assert start.call_args.kwargs["scheduler_job_id"] == UID
    assert start.call_args.kwargs["trigger_source"] == "scheduled"
    assert finish.call_args.kwargs["status"] == "done"


def test_manual_recommendation_requires_matching_prebound_scheduler_uid() -> None:
    engine = object()
    argv = [
        "run_ai_recommendation_premarket.py",
        "--date", "2026-08-24",
        "--strict-prev-trade-day",
        "--run-uid", UID,
        "--json",
    ]
    with patch.dict("os.environ", _production_env(), clear=False), patch.object(
        sys, "argv", argv
    ), patch.object(
        premarket, "create_batch_engine", return_value=engine
    ), patch.object(
        premarket, "_wait_for_db"
    ), patch.object(
        premarket, "_resolve_target_trade_date", return_value="2026-08-24"
    ), patch.object(
        premarket, "_assert_prebound_recommendation_history"
    ) as prebound, patch.object(
        premarket, "_recommended_run_history_start"
    ) as start, patch.object(
        premarket, "_recommended_run_history_update", return_value=True
    ) as update, patch.object(
        premarket, "_recommended_run_history_finish", return_value={"status": "done"}
    ), patch.object(
        premarket, "run_batch", return_value=_stats()
    ):
        assert premarket.main() == 0

    prebound.assert_called_once_with(engine, UID)
    start.assert_not_called()
    assert update.call_args_list[0].args[0] == UID


def test_scheduler_injects_exact_audit_identity_into_recommendation_child() -> None:
    row = {
        "id": 91,
        "task_name": "scheduled recommendation",
        "task_type": "analysis_premarket_external",
        "script_path": "tools/run_ai_recommendation_premarket.py",
        "script_args": "--strict-prev-trade-day --json",
        "date_param": "",
        "interval_minutes": 0,
    }
    process = MagicMock()
    process.communicate.return_value = ("ok", "")
    process.returncode = 0
    with patch.object(
        scheduler_runtime,
        "resolve_scheduler_script",
        return_value=Path("E:/fake/run_ai_recommendation_premarket.py"),
    ), patch.object(Path, "exists", return_value=True), patch.object(
        scheduler_runtime, "build_child_env", return_value={}
    ), patch.object(
        scheduler_runtime, "_build_task_args", return_value=[]
    ), patch.object(
        scheduler_runtime, "_scheduler_build_commit_sha", return_value=BUILD_SHA
    ), patch.object(
        scheduler_runtime.subprocess, "Popen", return_value=process
    ) as popen, patch.object(
        scheduler_runtime, "update_scheduler_task"
    ), patch.object(
        scheduler_runtime, "_task_history_finish"
    ), patch.object(
        scheduler_runtime,
        "validate_scheduler_task_result",
        return_value=SimpleNamespace(checked=False, ok=True, message=""),
    ):
        scheduler_runtime._run_task_impl(
            row, Path("E:/fake"), object(), history_run_uid=UID
        )

    child_env = popen.call_args.kwargs["env"]
    assert child_env["PROBIGA_SCHEDULER_HISTORY_RUN_UID"] == UID
    assert child_env["PROBIGA_SCHEDULER_TASK_ID"] == "91"
    assert child_env["PROBIGA_SCHEDULER_TASK_TYPE"] == (
        "analysis_premarket_external"
    )
    assert child_env["PROBIGA_SCHEDULER_BUILD_SHA"] == BUILD_SHA


def test_recommendation_identity_rejects_missing_or_mismatched_scheduler_uid() -> None:
    with patch.dict(
        "os.environ",
        {"PROBIGA_DEPLOYMENT_MODE": "production",
         "PROBIGA_SCHEDULER_HISTORY_RUN_UID": ""},
        clear=False,
    ), pytest.raises(RuntimeError, match="scheduler audit identity"):
        premarket._scheduler_recommendation_identity("")

    with patch.dict("os.environ", _production_env(), clear=False), pytest.raises(
        RuntimeError, match="differs"
    ):
        premarket._scheduler_recommendation_identity("c" * 32)


@pytest.mark.parametrize(
    "unsafe_flag",
    [
        "--auto-repair-missing-kline",
        "--auto-repair-missing-data",
        "--refresh-realtime",
        "--use-intraday-current",
    ],
)
def test_production_recommendation_rejects_local_write_or_intraday_flags(
    unsafe_flag: str,
) -> None:
    with patch.dict("os.environ", _production_env(), clear=False), patch.object(
        sys,
        "argv",
        ["run_ai_recommendation_premarket.py", unsafe_flag],
    ), patch.object(premarket, "create_batch_engine") as create_engine:
        with pytest.raises(RuntimeError):
            premarket.main()
    create_engine.assert_not_called()


def test_terminal_audit_failure_prevents_recommendation_success() -> None:
    engine = object()
    argv = ["run_ai_recommendation_premarket.py", "--date", "2026-08-24"]
    with patch.dict("os.environ", _production_env(), clear=False), patch.object(
        sys, "argv", argv
    ), patch.object(
        premarket, "create_batch_engine", return_value=engine
    ), patch.object(
        premarket, "_wait_for_db"
    ), patch.object(
        premarket, "_resolve_target_trade_date", return_value="2026-08-24"
    ), patch.object(
        premarket, "_recommended_run_history_start", return_value=UID
    ), patch.object(
        premarket, "_recommended_run_history_update", return_value=True
    ), patch.object(
        premarket,
        "_recommended_run_history_finish",
        side_effect=[RuntimeError("terminal update affected 0 rows"), {"status": "error"}],
    ), patch.object(
        premarket, "run_batch", return_value=_stats()
    ), pytest.raises(RuntimeError, match="affected 0 rows"):
        premarket.main()


def test_history_progress_update_returns_false_on_zero_row_update() -> None:
    with patch.object(
        hot_data, "_ensure_recommended_run_history_table"
    ), patch.object(hot_data, "_exec_sql", return_value=0):
        assert hot_data._recommended_run_history_update(UID) is False


def test_recommendation_history_rejects_scheduler_environment_uid_drift() -> None:
    with patch.dict(
        "os.environ",
        {
            "PROBIGA_DEPLOYMENT_MODE": "production",
            "PROBIGA_SCHEDULER_HISTORY_RUN_UID": "c" * 32,
            "PROBIGA_SCHEDULER_BUILD_SHA": BUILD_SHA,
        },
        clear=False,
    ), patch.object(
        hot_data, "_ensure_recommended_run_history_table"
    ), patch.object(hot_data, "_exec_sql") as execute:
        with pytest.raises(RuntimeError, match="environment identity differs"):
            hot_data._recommended_run_history_start(
                trade_date="2026-08-24",
                min_score=62,
                top_n=80,
                strict_prev_trade_day=True,
                execution_time="2026-08-25 09:00:00",
                run_uid=UID,
                scheduler_job_id=UID,
            )
    execute.assert_not_called()


def test_manual_dynamic_cutoff_publisher_is_retired_fail_closed() -> None:
    with patch.dict(
        os.environ, {"PROBIGA_DEPLOYMENT_MODE": "production"}, clear=False
    ):
        result = hot_data._submit_manual_recommended_stocks(
            trade_date="2026-08-24",
            min_score=62,
            top_n=80,
            execution_time="2026-08-25T09:00:00",
            min_kline_coverage=0.8,
            auto_repair_missing_kline=False,
            strict_prev_trade_day=True,
            refresh_realtime=False,
            date_policy="previous_complete",
        )

    assert result["accepted"] is False
    assert result["status"] == "canonical_pool_managed_by_eod_pipeline"
    assert result["next_refresh_time"] == "22:20 Asia/Shanghai"


def _mysql_lock_engine(*, get_lock=1, used_by=91, release=1):
    engine = MagicMock()
    engine.dialect.name = "mysql"
    connection = engine.connect.return_value

    def execute(statement, _params=None):
        sql = str(statement).upper()
        result = MagicMock()
        if "SELECT DATABASE" in sql:
            result.scalar.return_value = "probiga"
        elif "CONNECTION_ID" in sql:
            result.scalar.return_value = 91
        elif "GET_LOCK" in sql:
            result.scalar.return_value = get_lock
        elif "IS_USED_LOCK" in sql:
            result.scalar.return_value = used_by
        elif "RELEASE_LOCK" in sql:
            result.scalar.return_value = release
        else:
            raise AssertionError(sql)
        return result

    connection.execute.side_effect = execute
    return engine, connection


def test_analysis_advisory_lock_claim_verify_and_release() -> None:
    engine, connection = _mysql_lock_engine()
    with sync_analysis_fast._analysis_execution_lock(
        engine, "2026-08-24"
    ) as verify:
        verify()

    statements = [str(item.args[0]).upper() for item in connection.execute.call_args_list]
    assert any("GET_LOCK" in sql for sql in statements)
    assert any("IS_USED_LOCK" in sql for sql in statements)
    assert any("RELEASE_LOCK" in sql for sql in statements)
    connection.close.assert_called_once()


@pytest.mark.parametrize(
    ("get_lock", "message"),
    [(0, "already active"), (None, "returned NULL")],
)
def test_analysis_advisory_lock_fails_closed_when_claim_unavailable(
    get_lock, message,
) -> None:
    engine, _connection = _mysql_lock_engine(get_lock=get_lock)
    with pytest.raises(RuntimeError, match=message):
        with sync_analysis_fast._analysis_execution_lock(
            engine, "2026-08-24"
        ):
            pytest.fail("unowned writer entered critical section")


def test_analysis_advisory_lock_detects_lost_ownership_before_write() -> None:
    engine, _connection = _mysql_lock_engine(used_by=92)
    with pytest.raises(RuntimeError, match="ownership was lost"):
        with sync_analysis_fast._analysis_execution_lock(
            engine, "2026-08-24"
        ) as verify:
            verify()


def test_production_non_mysql_analysis_is_rejected() -> None:
    engine = MagicMock()
    engine.dialect.name = "sqlite"
    with patch.dict(
        "os.environ", {"PROBIGA_DEPLOYMENT_MODE": "production"}, clear=False
    ), pytest.raises(RuntimeError, match="requires MySQL"):
        with sync_analysis_fast._analysis_execution_lock(
            engine, "2026-08-24"
        ):
            pass


def test_all_analysis_write_entrypoints_verify_lock_before_save() -> None:
    events: list[str] = []

    @contextmanager
    def fake_lock(_engine, _trade_date):
        def verify():
            events.append("verify")
        yield verify

    prepared = ([], [], 50.0, "2026-08-24", "2026-08-24")
    with patch.object(
        sync_analysis_fast, "_analysis_execution_lock", side_effect=fake_lock
    ), patch.object(
        sync_analysis_fast, "_prepare_batch_outputs", return_value=prepared
    ), patch.object(
        sync_analysis_fast,
        "save_outputs",
        side_effect=lambda *_args, **_kwargs: events.append("save"),
    ):
        sync_analysis_fast.run_batch(object(), trade_date="2026-08-24")
        sync_analysis_fast.run_batch_for_codes(
            object(), ["000001"], trade_date="2026-08-24"
        )

    assert events == ["verify", "save", "verify", "save"]


def test_production_scoped_analysis_is_rejected_before_pool_write() -> None:
    with patch.dict(
        "os.environ",
        {"PROBIGA_DEPLOYMENT_MODE": "production"},
        clear=False,
    ), patch.object(sync_analysis_fast, "save_outputs") as save:
        with pytest.raises(RuntimeError, match="scoped analysis"):
            sync_analysis_fast.run_batch_for_codes(
                object(),
                ["000001"],
                trade_date="2026-08-27",
            )
    save.assert_not_called()


def test_sim_trade_missing_recommendation_never_generates_or_prepares() -> None:
    with patch.object(
        sync_sim_trade, "_previous_trade_date", return_value="2026-08-24"
    ), patch.object(
        sync_sim_trade, "_recommendation_count", return_value=0
    ), patch.object(sync_sim_trade, "SimTradeEngine") as sim_engine:
        result = sync_sim_trade.prepare_signals(trade_date="2026-08-25")

    assert result["status"] == "error"
    assert result["recommendation_prerequisite"]["read_only"] is True
    sim_engine.assert_not_called()
    source = inspect.getsource(sync_sim_trade.ensure_recommendations_for_signal_date)
    assert "run_batch" not in source


def test_registered_sim_prepare_and_morning_analysis_have_no_repair_flags() -> None:
    tasks = {item["task_type"]: item for item in ensure_quality_gate.TASKS}
    assert tasks["sim_trade_signal_prepare"]["script_args"] == (
        "--prepare-signals --reset --json"
    )
    assert "--ensure-recommendations" not in tasks["sim_trade_signal_prepare"]["script_args"]
    assert "--auto-repair" not in tasks["analysis_morning_strict"]["script_args"]
    assert "--auto-repair" not in tasks["analysis_premarket_external"]["script_args"]
    assert tasks["analysis_fast"]["script_path"] == (
        "tools/run_ai_recommendation_premarket.py"
    )
    assert tasks["analysis_morning_strict"]["script_path"] == (
        "tools/run_ai_recommendation_premarket.py"
    )
    assert "--json" in tasks["analysis_fast"]["script_args"]
    assert "--json" in tasks["analysis_morning_strict"]["script_args"]


def test_retired_worker_has_no_database_or_subprocess_execution(monkeypatch) -> None:
    monkeypatch.setattr(sys, "argv", ["run_ai_recommendation_worker.py", "--json"])
    assert retired_worker.main() == retired_worker.RETIRED_EXIT_CODE
    source = inspect.getsource(retired_worker)
    assert "import subprocess" not in source
    assert "subprocess.Popen" not in source
    assert "create_batch_engine" not in source
    assert "st_recommended_run_history" not in source
