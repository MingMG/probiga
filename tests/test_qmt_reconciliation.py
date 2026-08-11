from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

from integrations.qmt.reconciliation import (
    MAIN_A_SHARE_CODE_PATTERN,
    _coverage_status,
    _price_consistency_status,
)
from tools import nightly_guojin_qmt_reconciliation, run_ai_recommendation_premarket, run_ai_recommendation_worker
from tools.ensure_quality_gate import TASKS


def test_coverage_status_thresholds():
    assert _coverage_status(0.96, warn_threshold=0.80, pass_threshold=0.95) == "PASS"
    assert _coverage_status(0.85, warn_threshold=0.80, pass_threshold=0.95) == "WARN"
    assert _coverage_status(0.50, warn_threshold=0.80, pass_threshold=0.95) == "FAIL"


def test_reconciliation_uses_main_a_share_universe():
    assert MAIN_A_SHARE_CODE_PATTERN == "^(0|3|6)"


def test_price_consistency_status_thresholds():
    assert _price_consistency_status(checked_rows=100, failed_rows=0) == "PASS"
    assert _price_consistency_status(checked_rows=100, failed_rows=1) == "FAIL"
    assert _price_consistency_status(checked_rows=0, failed_rows=0) == "WARN"


def test_qmt_nightly_reconciliation_task_is_registered_but_safely_disabled():
    task = next(item for item in TASKS if item["task_type"] == "qmt_nightly_reconciliation")

    assert task["cron_time"] == "01:30"
    assert task["script_path"] == "tools/nightly_guojin_qmt_reconciliation.py"
    assert "--scan-days 20" in task["script_args"]
    assert task["enabled"] == 0


def test_qmt_intraday_realtime_task_is_independent_channel():
    task = next(item for item in TASKS if item["task_type"] == "qmt_intraday_realtime")

    assert task["group_name"] == "国金QMT"
    assert task["script_path"] == "tools/sync_qmt_realtime.py"
    assert "--no-archive-snapshot" in task["script_args"]


def test_qmt_gap_repair_plan_task_is_registered_as_dry_run():
    task = next(item for item in TASKS if item["task_type"] == "qmt_gap_repair_plan")

    assert task["cron_time"] == "02:00"
    assert task["script_path"] == "tools/repair_guojin_qmt_gaps.py"
    assert "--apply" not in task["script_args"]


def test_qmt_catalog_refresh_task_is_registered_but_safely_disabled():
    task = next(item for item in TASKS if item["task_type"] == "qmt_catalog_capability_refresh")

    assert task["cron_time"] == "01:10"
    assert task["script_path"] == "tools/setup_guojin_qmt_catalog.py"
    assert task["enabled"] == 0


def test_qmt_local_gap_repair_execute_task_is_registered():
    task = next(item for item in TASKS if item["task_type"] == "qmt_local_gap_repair_execute")

    assert task["cron_time"] == "07:05"
    assert task["script_path"] == "tools/backfill_guojin_qmt_local_history.py"
    assert "from-gaps" in task["script_args"]
    assert "--apply" in task["script_args"]


def test_ai_morning_recommendation_task_writes_run_history():
    task = next(item for item in TASKS if item["task_type"] == "analysis_morning_strict")

    assert task["cron_time"] == "08:30"
    assert task["script_path"] == "tools/run_ai_recommendation_premarket.py"
    assert "--strict-prev-trade-day" in task["script_args"]
    assert "--json" in task["script_args"]


def test_sim_trade_intraday_tick_task_is_registered():
    task = next(item for item in TASKS if item["task_type"] == "sim_trade")

    assert task["cron_time"] == "09:31"
    assert task["interval_minutes"] == 1
    assert task["script_path"] == "biz/analysis/sync_sim_trade.py"
    assert "--tick" in task["script_args"]
    assert "--skip-outside-intraday" in task["script_args"]
    assert task["enabled"] == 1


def test_sim_trade_signal_prepare_task_is_registered():
    task = next(item for item in TASKS if item["task_type"] == "sim_trade_signal_prepare")

    assert task["cron_time"] == "09:20"
    assert task["interval_minutes"] == 0
    assert task["script_path"] == "biz/analysis/sync_sim_trade.py"
    assert "--prepare-signals" in task["script_args"]
    assert "--ensure-recommendations" in task["script_args"]
    assert task["enabled"] == 1


def test_date_import_keeps_pytest_collection_stable():
    assert date(2026, 6, 27).isoformat() == "2026-06-27"


def test_nightly_reconciliation_main_uses_batch_engine():
    engine = object()
    result = SimpleNamespace(
        status="SUCCESS",
        run_id=7,
        target_trade_date="2026-07-01",
        coverage=[],
        quality=[],
        gaps_created_or_open=0,
    )

    with patch.object(
        nightly_guojin_qmt_reconciliation.sys,
        "argv",
        ["nightly_guojin_qmt_reconciliation.py", "--scan-days", "5", "--json"],
    ), patch(
        "tools.nightly_guojin_qmt_reconciliation.create_batch_engine",
        return_value=engine,
    ) as create_batch_engine, patch(
        "tools.nightly_guojin_qmt_reconciliation.run_nightly_reconciliation",
        return_value=result,
    ) as run_nightly_reconciliation, patch(
        "tools.nightly_guojin_qmt_reconciliation.result_dict",
        return_value={"status": "SUCCESS"},
    ):
        assert nightly_guojin_qmt_reconciliation.main() == 0

    create_batch_engine.assert_called_once_with(future=True)
    run_nightly_reconciliation.assert_called_once_with(engine, scan_days=5)


def test_premarket_recommendation_main_uses_batch_engine():
    engine = object()
    stats = SimpleNamespace(
        trade_date="2026-07-01",
        analysis_count=80,
        recommendation_count=12,
        market_mood_score=66.5,
        flow_date="2026-07-01",
        hot_date="2026-07-01",
    )

    with patch.object(
        run_ai_recommendation_premarket.sys,
        "argv",
        [
            "run_ai_recommendation_premarket.py",
            "--date",
            "2026-07-01",
            "--top-n",
            "20",
            "--min-score",
            "70",
            "--execution-time",
            "2026-07-02 08:30:00",
            "--json",
        ],
    ), patch(
        "tools.run_ai_recommendation_premarket.create_batch_engine",
        return_value=engine,
    ) as create_batch_engine, patch(
        "tools.run_ai_recommendation_premarket._recommended_run_history_start",
        return_value="run-1",
    ) as history_start, patch(
        "tools.run_ai_recommendation_premarket.run_batch",
        return_value=stats,
    ) as run_batch, patch(
        "tools.run_ai_recommendation_premarket._recommended_run_history_finish",
    ) as history_finish:
        assert run_ai_recommendation_premarket.main() == 0

    create_batch_engine.assert_called_once_with()
    history_start.assert_called_once()
    assert history_start.call_args.kwargs["trade_date"] == "2026-07-01"
    run_batch.assert_called_once()
    assert run_batch.call_args.kwargs["engine"] is engine
    assert run_batch.call_args.kwargs["trade_date"] == "2026-07-01"
    assert run_batch.call_args.kwargs["use_intraday_current"] is False
    history_finish.assert_called_once()
    assert history_finish.call_args.args[0] == "run-1"


def test_premarket_external_snapshot_retries_until_complete():
    partial = {"available_count": 18, "expected_count": 21, "source_warnings": []}
    complete = {"available_count": 21, "expected_count": 21, "source_warnings": []}

    with patch(
        "tools.run_ai_recommendation_premarket.fetch_external_market_snapshot",
        side_effect=[partial, complete],
    ) as fetch, patch("tools.run_ai_recommendation_premarket.time.sleep") as sleep:
        result = run_ai_recommendation_premarket._fetch_external_market_snapshot_with_retries(
            attempts=3,
            retry_delay_seconds=0.1,
        )

    assert result is complete
    assert fetch.call_count == 2
    sleep.assert_called_once_with(0.1)


def test_premarket_recommendation_main_passes_intraday_current_flag():
    engine = object()
    stats = SimpleNamespace(
        trade_date="2026-07-08",
        analysis_count=80,
        recommendation_count=12,
        market_mood_score=66.5,
        flow_date="2026-07-08",
        hot_date="2026-07-08",
    )

    with patch.object(
        run_ai_recommendation_premarket.sys,
        "argv",
        [
            "run_ai_recommendation_premarket.py",
            "--date",
            "2026-07-08",
            "--execution-time",
            "2026-07-08 10:20:00",
            "--use-intraday-current",
            "--json",
        ],
    ), patch(
        "tools.run_ai_recommendation_premarket.create_batch_engine",
        return_value=engine,
    ), patch(
        "tools.run_ai_recommendation_premarket._recommended_run_history_start",
        return_value="run-1",
    ), patch(
        "tools.run_ai_recommendation_premarket.run_batch",
        return_value=stats,
    ) as run_batch, patch(
        "tools.run_ai_recommendation_premarket._recommended_run_history_finish",
    ):
        assert run_ai_recommendation_premarket.main() == 0

    assert run_batch.call_args.kwargs["trade_date"] == "2026-07-08"
    assert run_batch.call_args.kwargs["execution_time"] == "2026-07-08 10:20:00"
    assert run_batch.call_args.kwargs["use_intraday_current"] is True


def test_recommendation_worker_detects_intraday_current_job():
    job = {"trade_date": "2026-07-08", "execution_time": "2026-07-08 12:10:00"}

    assert run_ai_recommendation_worker._job_uses_intraday_current(job, refresh_realtime=True) is True
    assert run_ai_recommendation_worker._job_uses_intraday_current(job, refresh_realtime=False) is False
    assert run_ai_recommendation_worker._job_uses_intraday_current(
        {"trade_date": "2026-07-07", "execution_time": "2026-07-08 12:10:00"},
        refresh_realtime=True,
    ) is False
