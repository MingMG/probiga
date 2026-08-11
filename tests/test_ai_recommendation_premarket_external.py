from __future__ import annotations

import sys
from types import SimpleNamespace
from unittest.mock import patch

from tools import run_ai_recommendation_premarket


def test_external_market_snapshot_retries_until_complete() -> None:
    partial = {
        "available_count": 18,
        "expected_count": 21,
        "source_warnings": [],
    }
    complete = {
        "available_count": 21,
        "expected_count": 21,
        "source_warnings": [],
    }

    with (
        patch(
            "tools.run_ai_recommendation_premarket.fetch_external_market_snapshot",
            side_effect=[partial, complete],
        ) as fetch,
        patch("tools.run_ai_recommendation_premarket.time.sleep") as sleep,
    ):
        snapshot = run_ai_recommendation_premarket._fetch_external_market_snapshot_with_retries(
            attempts=3,
            retry_delay_seconds=0.1,
        )

    assert snapshot is complete
    assert fetch.call_count == 2
    sleep.assert_called_once_with(0.1)


def test_main_accepts_external_market_and_runs_batch() -> None:
    engine = object()
    stats = SimpleNamespace(
        trade_date="2026-07-01",
        analysis_count=123,
        recommendation_count=7,
        market_mood_score=66.5,
        flow_date="2026-07-01",
        hot_date="2026-07-01",
    )
    snapshot = {
        "available_count": 21,
        "expected_count": 21,
        "source_warnings": [],
        "external_market_status": "SUPPORT",
        "external_market_score": 62.0,
    }
    external_report = {
        "external_market_data_quality": "COMPLETE",
        "external_market_status": "SUPPORT",
    }

    with (
        patch.object(sys, "argv", [
            "run_ai_recommendation_premarket.py",
            "--date",
            "2026-07-01",
            "--external-market",
            "--json",
        ]),
        patch("tools.run_ai_recommendation_premarket.create_batch_engine", return_value=engine),
        patch("tools.run_ai_recommendation_premarket._wait_for_db"),
        patch("tools.run_ai_recommendation_premarket._recommended_run_history_start", return_value="run-1"),
        patch("tools.run_ai_recommendation_premarket._recommended_run_history_update"),
        patch("tools.run_ai_recommendation_premarket._recommended_run_history_finish") as finish,
        patch(
            "tools.run_ai_recommendation_premarket._fetch_external_market_snapshot_with_retries",
            return_value=snapshot,
        ),
        patch(
            "tools.run_ai_recommendation_premarket.store_external_market_snapshot",
            return_value=external_report,
        ) as store,
        patch("tools.run_ai_recommendation_premarket.run_batch", return_value=stats) as run_batch,
    ):
        result = run_ai_recommendation_premarket.main()

    assert result == 0
    store.assert_called_once_with(engine, snapshot)
    assert run_batch.call_args.kwargs["use_intraday_current"] is False
    finish.assert_called_once()
    assert finish.call_args.kwargs["status"] == "done"
