# -*- coding: utf-8 -*-

from datetime import date, datetime
import json

from server.common import scheduler_validation
from server.common.scheduler_validation import (
    TASK_OUTPUT_REQUIREMENTS,
    _extract_output_date,
    _resolve_target_date,
)
from tools import crawl_realtime_batch


def test_realtime_quote_threshold_matches_valid_market_universe():
    assert TASK_OUTPUT_REQUIREMENTS["stock_current"][0].min_distinct == 5000
    assert TASK_OUTPUT_REQUIREMENTS["intraday_realtime"][0].min_distinct == 5000


def test_sector_heat_validation_uses_provider_data_date_marker():
    requirement = TASK_OUTPUT_REQUIREMENTS["sector_heat_east"][0]

    assert requirement.target == "output_date"
    assert _resolve_target_date(
        object(),
        requirement,
        started_at=datetime(2026, 8, 16, 17, 8),
        now=datetime(2026, 8, 16, 17, 9),
        output="写入完成\nDATE=2026-08-14\nSYNCED=275",
    ).isoformat() == "2026-08-14"


def test_provider_data_date_marker_is_strict_and_unambiguous():
    assert _extract_output_date("DATE=2026-08-14\nDATE=2026-08-14") is not None
    assert _extract_output_date("DATE=2026-08-14\nDATE=2026-08-15") is None
    assert _extract_output_date("DATE=2026-02-30") is None
    assert _extract_output_date("requested DATE=2026-08-14") is None


def test_release_analysis_validation_uses_hash_bound_scheduler_target(monkeypatch):
    observed = {}

    def validate_requirement(
        _engine,
        _requirement,
        *,
        target_date_override=None,
        **_kwargs,
    ):
        observed["target"] = target_date_override
        return True, "exact release target verified"

    monkeypatch.setattr(
        scheduler_validation,
        "_validate_requirement",
        validate_requirement,
    )
    def validate_daily_coverage(
        _engine,
        *,
        task_type,
        target_date,
        decision_known_at,
    ):
        observed["coverage_task"] = task_type
        observed["coverage_target"] = target_date
        observed["coverage_known_at"] = decision_known_at
        return True, "exact capital-flow coverage verified"

    monkeypatch.setattr(
        scheduler_validation,
        "_validate_daily_universe_coverage",
        validate_daily_coverage,
    )
    result = scheduler_validation.validate_scheduler_task_result(
        {
            "task_type": "analysis_fast",
            "_trigger_source": "release_catchup",
            "_release_target_date": "2026-08-26",
        },
        engine=object(),
        started_at=datetime(2026, 8, 27, 3, 5),
        now=datetime(2026, 8, 27, 3, 6),
    )

    assert result.ok
    assert observed["target"] == date(2026, 8, 26)
    assert observed["coverage_task"] == "analysis_fast"
    assert observed["coverage_target"] == date(2026, 8, 26)


def test_capital_flow_batch_validation_fails_when_full_universe_proof_fails(
    monkeypatch,
):
    monkeypatch.setattr(
        scheduler_validation,
        "_validate_requirement",
        lambda *_args, **_kwargs: (True, "table threshold passed"),
    )
    monkeypatch.setattr(
        scheduler_validation,
        "_resolve_target_date",
        lambda *_args, **_kwargs: date(2026, 8, 26),
    )
    observed = {}

    def reject_partial(
        _engine,
        *,
        task_type,
        target_date,
        decision_known_at,
    ):
        observed.update(task_type=task_type, target_date=target_date)
        return False, "capital flow full-universe proof failed"

    monkeypatch.setattr(
        scheduler_validation,
        "_validate_daily_universe_coverage",
        reject_partial,
    )

    receipt = crawl_realtime_batch._signed_receipt({
        "schema": crawl_realtime_batch.CAPITAL_FLOW_RESULT_SCHEMA,
        "status": "PASS",
        "task_type": crawl_realtime_batch.CAPITAL_FLOW_TASK_TYPE,
        "dataset": crawl_realtime_batch.CAPITAL_FLOW_DATASET,
        "trade_date": "2026-08-26",
        "source_trade_date": "2026-08-26",
        "source_timestamp_required": True,
        "row_count": 5000,
        "elapsed_seconds": 1.0,
        "generated_at": "2026-08-26T15:21:00",
    })
    result = scheduler_validation.validate_scheduler_task_result(
        {"task_type": "capital_flow_batch_fast"},
        engine=object(),
        started_at=datetime(2026, 8, 26, 15, 20),
        now=datetime(2026, 8, 26, 15, 25),
        output=json.dumps(receipt),
    )

    assert result.checked
    assert not result.ok
    assert result.message == "capital flow full-universe proof failed"
    assert observed == {
        "task_type": "capital_flow_batch_fast",
        "target_date": date(2026, 8, 26),
    }


def test_release_validation_target_rejects_non_catchup_injection():
    result = scheduler_validation.validate_scheduler_task_result(
        {
            "task_type": "analysis_fast",
            "_trigger_source": "scheduled",
            "_release_target_date": "2026-08-26",
        },
        engine=object(),
        started_at=datetime(2026, 8, 27, 18, 50),
    )

    assert result.checked
    assert not result.ok
    assert "no catch-up authority" in result.message
