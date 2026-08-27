# -*- coding: utf-8 -*-

from datetime import date, datetime
import json
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, text

from biz.analysis import sync_analysis_fast
from server.api import scheduler_runtime
from server.common import scheduler_validation
from server.common.analysis_pool_receipt import (
    CANONICAL_ANALYSIS_COLUMNS,
    CANONICAL_RECOMMENDATION_COLUMNS,
    TURNOVER_DIRECT_FORMULA,
    build_publication_receipt,
    build_turnover_evidence,
    build_upper_limit_evidence,
    canonical_sha256,
    read_persisted_pool_manifest,
    validate_turnover_evidence,
)
from server.common.scheduler_validation import (
    TASK_OUTPUT_REQUIREMENTS,
    _extract_output_date,
    _resolve_target_date,
)
from tools import crawl_realtime_batch, sync_bigqmt_reference


def test_realtime_quote_threshold_matches_valid_market_universe():
    assert TASK_OUTPUT_REQUIREMENTS["stock_current"][0].min_distinct == 5000
    assert TASK_OUTPUT_REQUIREMENTS["intraday_realtime"][0].min_distinct == 5000


def test_turnover_scheduler_receipt_requires_exact_iso_cutoff_and_readback() -> None:
    target = "2026-08-27"
    cutoff = datetime(2026, 8, 27, 23, 55)
    build_sha = "a" * 40
    run_id = "b" * 32
    semantic_sha = "c" * 64
    task = {
        "_scheduler_pipeline_target_date": target,
        "_scheduler_pipeline_decision_at": cutoff.isoformat(timespec="seconds"),
        "_scheduler_expected_build_sha": build_sha,
    }
    receipt = {
        "schema": "probiga.market-field-capture.v1",
        "status": "COMPLETED",
        "run_id": run_id,
        "target_date": target,
        "decision_at": cutoff.isoformat(timespec="seconds"),
        "collector_build_sha": build_sha,
        "expected_count": 1,
        "promoted_count": 1,
        "semantic_sha256": semantic_sha,
    }
    proof = {
        "snapshot_run_id": run_id,
        "collector_build_sha": build_sha,
        "snapshot_semantic_sha256": semantic_sha,
    }
    with patch(
        "server.common.turnover_snapshot.MIN_TURNOVER_UNIVERSE_COUNT", 1
    ), patch(
        "server.common.turnover_snapshot.load_verified_turnover_evidence",
        return_value={"000001": {"turnover_evidence_json": "{}"}},
    ), patch(
        "server.common.analysis_pool_receipt.validate_turnover_evidence",
        return_value=proof,
    ):
        ok, _message = (
            scheduler_validation._validate_target_turnover_scheduler_receipt(
                task,
                engine=object(),
                output=json.dumps(receipt),
            )
        )
        assert ok
        receipt["decision_at"] = "2026-08-27 23:55:00.000000"
        ok, message = (
            scheduler_validation._validate_target_turnover_scheduler_receipt(
                task,
                engine=object(),
                output=json.dumps(receipt),
            )
        )
        assert not ok
        assert "identity differs" in message


def test_upper_scheduler_receipt_accepts_recovered_exact_subject_readback() -> None:
    target = "2026-08-27"
    cutoff = datetime(2026, 8, 27, 23, 55)
    build_sha = "a" * 40
    run_id = "b" * 32
    preview_sha = "c" * 64
    codes = [f"{number:06d}" for number in range(1, 81)]
    task = {
        "_scheduler_pipeline_target_date": target,
        "_scheduler_pipeline_decision_at": cutoff.isoformat(timespec="seconds"),
        "_scheduler_expected_build_sha": build_sha,
    }
    receipt = {
        "schema": "probiga.market-field-capture.v1",
        "status": "COMPLETED",
        "run_id": run_id,
        "target_date": target,
        "decision_at": cutoff.isoformat(timespec="seconds"),
        "collector_build_sha": build_sha,
        "preliminary_receipt_sha256": preview_sha,
        "expected_stock_count": 80,
        "expected_date_count": 21,
        "recovered": True,
    }
    preliminary = {
        "receipt_sha256": preview_sha,
        "ordered_stock_codes": codes,
    }
    evidence = {
        code: {"upper_limit_evidence_json": "{}"} for code in codes
    }
    with patch(
        "biz.analysis.sync_analysis_fast."
        "prepare_preliminary_upper_subject_receipt",
        return_value=preliminary,
    ), patch(
        "server.common.upper_limit_snapshot."
        "load_latest_verified_upper_limit_evidence",
        return_value=evidence,
    ), patch(
        "server.common.analysis_pool_receipt.validate_upper_limit_evidence",
        return_value={"snapshot_run_id": run_id},
    ):
        ok, message = (
            scheduler_validation._validate_upper_evidence_scheduler_receipt(
                task,
                engine=object(),
                output=json.dumps(receipt),
            )
        )
    assert ok
    assert "exact immutable snapshot verified" in message


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
    monkeypatch.setattr(
        scheduler_validation,
        "_validate_analysis_strategy_pool",
        lambda *_args, **_kwargs: (
            True,
            "non-empty actionable strategy pool verified",
        ),
    )
    membership_targets = []
    monkeypatch.setattr(
        "integrations.bigqmt.membership_snapshot."
        "verify_existing_membership_snapshot",
        lambda _engine, *, snapshot_date, decision_known_at: (
            membership_targets.append((snapshot_date, decision_known_at))
            or _membership_proof(snapshot_date.isoformat())
        ),
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
    assert membership_targets == [
        (date(2026, 8, 26), datetime(2026, 8, 27, 3, 6))
    ]


def test_analysis_membership_proof_tracks_downstream_target_across_cutoffs(
    monkeypatch,
):
    monkeypatch.setattr(
        scheduler_validation,
        "_validate_requirement",
        lambda *_args, **_kwargs: (True, "exact analysis partition verified"),
    )
    monkeypatch.setattr(
        scheduler_validation,
        "_validate_daily_universe_coverage",
        lambda *_args, **_kwargs: (True, "exact market universe verified"),
    )
    monkeypatch.setattr(
        scheduler_validation,
        "_validate_analysis_strategy_pool",
        lambda *_args, **_kwargs: (True, "current-run pool verified"),
    )
    observed = []
    monkeypatch.setattr(
        "integrations.bigqmt.membership_snapshot."
        "verify_existing_membership_snapshot",
        lambda _engine, *, snapshot_date, decision_known_at: (
            observed.append((snapshot_date, decision_known_at))
            or _membership_proof(snapshot_date.isoformat())
        ),
    )
    cases = (
        ("analysis_fast", datetime(2026, 8, 27, 15, 9), "2026-08-26"),
        ("analysis_fast", datetime(2026, 8, 27, 15, 10), "2026-08-26"),
        ("analysis_fast", datetime(2026, 8, 27, 18, 0), "2026-08-27"),
        (
            "analysis_morning_strict",
            datetime(2026, 8, 27, 15, 9),
            "2026-08-26",
        ),
        (
            "analysis_morning_strict",
            datetime(2026, 8, 27, 15, 10),
            "2026-08-26",
        ),
        (
            "analysis_morning_strict",
            datetime(2026, 8, 27, 18, 0),
            "2026-08-26",
        ),
    )
    for task_type, decision_time, target in cases:
        result = scheduler_validation.validate_scheduler_task_result(
            {
                "task_type": task_type,
                "_trigger_source": "release_catchup",
                "_release_target_date": target,
                "_scheduler_history_run_uid": "1" * 32,
                "_scheduler_expected_build_sha": "2" * 40,
            },
            engine=object(),
            started_at=decision_time,
            now=decision_time,
        )
        assert result.checked and result.ok

    assert observed == [
        (date.fromisoformat(target), decision_time)
        for _task_type, decision_time, target in cases
    ]


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


def _membership_proof(snapshot_date: str = "2026-08-26") -> dict:
    return {
        "snapshot_date": snapshot_date,
        "source": "gj_big_qmt_inner",
        "quality_status": "QMT_VALIDATED",
        "capture_mode": "qmt_close_full_refresh",
        "captured_at": f"{snapshot_date} 15:12:00",
        "concept_count": 500,
        "concept_relation_count": 30_000,
        "concept_stock_count": 3_000,
        "concept_hash": "a" * 64,
        "industry_count": 20,
        "industry_relation_count": 5_000,
        "industry_stock_count": 4_500,
        "industry_hash": "b" * 64,
    }


def test_release_membership_receipt_is_exact_fresh_and_db_reverified(
    monkeypatch,
):
    proof = _membership_proof()
    receipt = sync_bigqmt_reference._membership_verification_receipt(
        status="PASS",
        snapshot_date="2026-08-26",
        verified_at=datetime(2026, 8, 27, 3, 6),
        proof=proof,
    )
    observed = {}

    def verify(_engine, *, snapshot_date, decision_known_at):
        observed.update(
            snapshot_date=snapshot_date,
            decision_known_at=decision_known_at,
        )
        return proof

    monkeypatch.setattr(
        "integrations.bigqmt.membership_snapshot."
        "verify_existing_membership_snapshot",
        verify,
    )
    task = {
        "task_type": "qmt_membership_snapshot",
        "_trigger_source": "release_catchup",
        "_release_target_date": "2026-08-26",
    }
    output = json.dumps(receipt)

    assert scheduler_validation.scheduler_output_status(
        task,
        output,
        return_code=0,
    ) == "success"
    result = scheduler_validation.validate_scheduler_task_result(
        task,
        engine=object(),
        started_at=datetime(2026, 8, 27, 3, 5),
        now=datetime(2026, 8, 27, 3, 7),
        output=output,
    )

    assert result.checked and result.ok
    assert "date=2026-08-26" in result.message
    assert observed == {
        "snapshot_date": date(2026, 8, 26),
        "decision_known_at": datetime(2026, 8, 27, 3, 7),
    }


def test_release_membership_rejects_target_staleness_and_db_proof_drift(
    monkeypatch,
):
    proof = _membership_proof()
    task = {
        "task_type": "qmt_membership_snapshot",
        "_trigger_source": "release_catchup",
        "_release_target_date": "2026-08-26",
    }

    wrong_target = sync_bigqmt_reference._membership_verification_receipt(
        status="PASS",
        snapshot_date="2026-08-25",
        verified_at=datetime(2026, 8, 27, 3, 6),
        proof=_membership_proof("2026-08-25"),
    )
    result = scheduler_validation.validate_scheduler_task_result(
        task,
        engine=object(),
        started_at=datetime(2026, 8, 27, 3, 5),
        now=datetime(2026, 8, 27, 3, 7),
        output=json.dumps(wrong_target),
    )
    assert not result.ok
    assert "differs from release target" in result.message

    stale = sync_bigqmt_reference._membership_verification_receipt(
        status="PASS",
        snapshot_date="2026-08-26",
        verified_at=datetime(2026, 8, 27, 2, 0),
        proof=proof,
    )
    result = scheduler_validation.validate_scheduler_task_result(
        task,
        engine=object(),
        started_at=datetime(2026, 8, 27, 3, 5),
        now=datetime(2026, 8, 27, 3, 7),
        output=json.dumps(stale),
    )
    assert not result.ok
    assert "not fresh" in result.message

    monkeypatch.setattr(
        "integrations.bigqmt.membership_snapshot."
        "verify_existing_membership_snapshot",
        lambda *_args, **_kwargs: {**proof, "industry_hash": "c" * 64},
    )
    fresh = sync_bigqmt_reference._membership_verification_receipt(
        status="PASS",
        snapshot_date="2026-08-26",
        verified_at=datetime(2026, 8, 27, 3, 6),
        proof=proof,
    )
    result = scheduler_validation.validate_scheduler_task_result(
        task,
        engine=object(),
        started_at=datetime(2026, 8, 27, 3, 5),
        now=datetime(2026, 8, 27, 3, 7),
        output=json.dumps(fresh),
    )
    assert not result.ok
    assert "proof differs" in result.message


def test_membership_blocked_receipt_retries_and_ordinary_requires_receipt():
    blocked = sync_bigqmt_reference._membership_verification_receipt(
        status="DATA_BLOCKED",
        snapshot_date="2026-08-26",
        verified_at=datetime(2026, 8, 27, 3, 6),
        reason="DATA_BLOCKED: exact snapshot is unavailable",
    )
    assert scheduler_validation.scheduler_output_status(
        {
            "task_type": "qmt_membership_snapshot",
            "_trigger_source": "release_catchup",
        },
        json.dumps(blocked),
        return_code=2,
    ) == "blocked"
    assert scheduler_validation.scheduler_output_status(
        {"task_type": "qmt_membership_snapshot"},
        '{"status":"success","applied":true}',
        return_code=0,
    ) == "failed"
    ordinary = scheduler_validation.validate_scheduler_task_result(
        {"task_type": "qmt_membership_snapshot"},
        engine=object(),
    )
    assert ordinary.checked and not ordinary.ok


def test_ordinary_membership_publication_receipt_is_db_reverified(monkeypatch):
    proof = _membership_proof()
    receipt = sync_bigqmt_reference._membership_publication_receipt(
        snapshot_date="2026-08-26",
        published_at=datetime(2026, 8, 26, 15, 12),
        publish_status="created",
        proof=proof,
    )
    output = json.dumps({"membership_publication_receipt": receipt})
    monkeypatch.setattr(
        scheduler_validation,
        "authoritative_closed_trade_date",
        lambda *_args, **_kwargs: "2026-08-26",
    )
    monkeypatch.setattr(
        "integrations.bigqmt.membership_snapshot."
        "verify_existing_membership_snapshot",
        lambda *_args, **_kwargs: proof,
    )
    task = {"task_type": "qmt_membership_snapshot"}

    assert scheduler_validation.scheduler_output_status(
        task, output, return_code=0
    ) == "success"
    result = scheduler_validation.validate_scheduler_task_result(
        task,
        engine=object(),
        started_at=datetime(2026, 8, 26, 15, 11),
        now=datetime(2026, 8, 26, 15, 13),
        output=output,
    )
    assert result.checked and result.ok
    assert "date=2026-08-26" in result.message


_STRATEGY_RUN_UID = "1" * 32
_STRATEGY_BUILD_SHA = "2" * 40


def _turnover_evidence(stock_code: str) -> str:
    return build_turnover_evidence({
        "status": "PASS",
        "stock_code": stock_code,
        "trade_date": "2026-08-26",
        "decision_known_at": "2026-08-26 18:50:00",
        "source_table": "st_market_field_capture_row",
        "formula": TURNOVER_DIRECT_FORMULA,
        "volume": "1000",
        "turnover_ratio": "2.00",
        "provider": "eastmoney.push2his.kline",
        "transport_contract": "HTTPS_TLS_VERIFIED_PINNED_RESOLVE_V1",
        "resolved_endpoint": "push2his.eastmoney.com:443:61.129.129.48",
        "source_field": "f61",
        "unit": "PERCENT",
        "source_trade_date": "2026-08-26",
        "captured_at": "2026-08-26 18:49:00",
        "provider_http_date": "Wed, 26 Aug 2026 10:49:00 GMT",
        "snapshot_run_id": "3" * 32,
        "collector_build_sha": "a" * 40,
        "collector_binary_sha256": "b" * 64,
        "authority_proof_kind": "QMT_DAILY_MARKET_TRUTH",
        "authority_proof_identity": "qmt-daily-truth-run-1",
        "authority_proof_sha256": "c" * 64,
        "authority_set_sha256": "d" * 64,
        "raw_payload_sha256": "4" * 64,
        "snapshot_row_sha256": "5" * 64,
        "snapshot_semantic_sha256": "6" * 64,
        "source_open": "10.00",
        "source_high": "10.50",
        "source_low": "9.80",
        "source_close": "10.20",
        "source_volume_shares": "1000",
        "qmt_open": "10.00",
        "qmt_high": "10.50",
        "qmt_low": "9.80",
        "qmt_close": "10.20",
        "qmt_volume_shares": "1000",
        "qmt_received_at": "2026-08-26 18:40:00",
        "qmt_data_source": "gj_big_qmt_inner",
        "qmt_batch_id": "batch-1",
        "qmt_data_version": "version-1",
        "qmt_quality_status": "QMT_ATTESTED",
        "qmt_permission_status": "SUPPORTED",
    })


def _upper_limit_evidence(stock_code: str) -> str:
    return build_upper_limit_evidence({
        "status": "PASS",
        "stock_code": stock_code,
        "trade_date": "2026-08-26",
        "window_start_date": "2026-07-29",
        "window_end_date": "2026-08-26",
        "decision_known_at": "2026-08-27 03:05:00",
        "captured_at": "2026-08-27 03:04:00",
        "source_table": "st_market_field_capture_row",
        "capture_kind": "DAILY_UPPER_LIMIT_HISTORY",
        "provider": "myquant.gm.get_history_instruments",
        "source_field": "upper_limit",
        "unit": "PRICE_CNY",
        "transport_contract": "MYQUANT_GM_SDK_FIXED_ACTION_V1",
        "entitlement_status": "SUPPORTED",
        "timezone": "Asia/Shanghai",
        "expected_stock_count": 1,
        "expected_date_count": 21,
        "snapshot_run_id": "7" * 32,
        "subject_identity": "2026-08-26:fixture",
        "subject_sha256": "e" * 64,
        "code_set_sha256": canonical_sha256({
            "schema": "probiga.upper-limit-code-set.v1",
            "target_date": "2026-08-26",
            "stock_codes": [stock_code],
        }),
        "trade_dates_sha256": "f" * 64,
        "calendar_batch_id": "calendar-batch-1",
        "calendar_manifest_sha256": "1" * 64,
        "calendar_session_set_sha256": "2" * 64,
        "expected_keyset_sha256": "8" * 64,
        "snapshot_semantic_sha256": "9" * 64,
        "stock_rows_sha256": "a" * 64,
        "provider_response_sha256": "b" * 64,
        "canonical_request_sha256": "c" * 64,
        "worker_sha256": "d" * 64,
        "sdk_version": "3.0.114",
        "python_version": "3.6.8",
    })


def test_turnover_pass_requires_exact_target_direct_snapshot_proof():
    proof = validate_turnover_evidence(_turnover_evidence("600001"))
    assert proof["unit"] == "PERCENT"

    for changed in (
        {"source_table": "sm_stock_kline"},
        {"transport_contract": "HTTPS_INSECURE"},
        {"captured_at": "2026-08-26 18:51:00"},
        {"qmt_close": "10.21"},
        {"source_trade_date": "2026-08-25"},
    ):
        invalid = {**proof, **changed}
        invalid.pop("proof_sha256", None)
        with pytest.raises(ValueError, match="direct turnover snapshot proof"):
            validate_turnover_evidence(build_turnover_evidence(invalid))


def _strategy_pool_engine(
    *,
    actionable: bool,
    history_passed: int | None = None,
    history_status: str = "done",
    history_build_sha: str = _STRATEGY_BUILD_SHA,
    history_trade_date: str = "2026-08-26",
):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(text(
            "CREATE TABLE stock_analysis_result ("
            + ", ".join(
                f"`{column}` TEXT" for column in CANONICAL_ANALYSIS_COLUMNS
            )
            + ")"
        ))
        connection.execute(text(
            "CREATE TABLE st_recommended_stocks ("
            + ", ".join(
                f"`{column}` TEXT"
                for column in (
                    *CANONICAL_RECOMMENDATION_COLUMNS,
                    "recommend_status",
                    "ordinary_buy_eligible",
                    "publication_status",
                )
            )
            + ")"
        ))
        connection.execute(text("""
            CREATE TABLE st_recommended_run_history (
                id INTEGER NOT NULL,
                run_uid TEXT NOT NULL,
                scheduler_job_id TEXT NOT NULL,
                trade_date DATE NOT NULL,
                status TEXT NOT NULL,
                build_sha TEXT NOT NULL,
                total INTEGER,
                passed INTEGER,
                started_at DATETIME,
                finished_at DATETIME,
                publisher_task_type TEXT,
                canonical_pool_sha256 TEXT,
                published_at DATETIME,
                executable_count INTEGER,
                membership_snapshot_date DATE,
                membership_snapshot_source TEXT,
                membership_proof_sha256 TEXT
            )
        """))
        analysis_row = dict.fromkeys(CANONICAL_ANALYSIS_COLUMNS)
        analysis_row.update({
            "stock_code": "600001",
            "stock_name": "策略一",
            "analysis_date": "2026-08-26",
            "recommend_status": "ALLOW" if actionable else "SUSPENDED",
            "model_version": "test-v1",
        })
        _insert_pool_row(
            connection,
            "stock_analysis_result",
            analysis_row,
        )
        if actionable:
            recommendation_row = dict.fromkeys(
                CANONICAL_RECOMMENDATION_COLUMNS
            )
            recommendation_row.update({
                "stock_code": "600001",
                "short_name": "策略一",
                "pick_date": "2026-08-26",
                "recommend_status": "PENDING",
                "candidate_recommend_status": "ALLOW",
                "signal_status": "BUY_READY",
                "chase_risk_status": "ALLOW",
                "ordinary_buy_eligible": 0,
                "candidate_ordinary_buy_eligible": 1,
                "publisher_run_uid": _STRATEGY_RUN_UID,
                "publication_status": "PENDING",
                "membership_snapshot_date": "2026-08-26",
                "membership_snapshot_source": "gj_big_qmt_inner",
                "membership_proof_sha256": canonical_sha256(
                    _membership_proof()
                ),
                "turnover_evidence_json": _turnover_evidence("600001"),
                "upper_limit_evidence_json": _upper_limit_evidence("600001"),
                "model_version": "test-v1",
            })
            _insert_pool_row(
                connection,
                "st_recommended_stocks",
                recommendation_row,
            )
        manifest = read_persisted_pool_manifest(connection, "2026-08-26")
        published_at = datetime(2026, 8, 27, 3, 6)
        receipt = build_publication_receipt(
            manifest=manifest,
            run_uid=_STRATEGY_RUN_UID,
            publisher_task_type="analysis_fast",
            build_sha=history_build_sha,
            published_at=published_at,
        )
        connection.execute(text("""
            INSERT INTO st_recommended_run_history
            VALUES (1, :run_uid, :run_uid, :trade_date, :status, :build_sha,
                    1, :passed, '2026-08-27 03:05:05',
                    '2026-08-27 03:06:30', 'analysis_fast', :pool_hash,
                    :published_at, :executable_count, '2026-08-26',
                    'gj_big_qmt_inner', :membership_hash)
        """), {
            "run_uid": _STRATEGY_RUN_UID,
            "trade_date": history_trade_date,
            "status": history_status,
            "build_sha": history_build_sha,
            "passed": (
                int(manifest["recommendation_count"])
                if history_passed is None
                else history_passed
            ),
            "pool_hash": manifest["canonical_pool_sha256"],
            "published_at": published_at,
            "executable_count": manifest["executable_count"],
            "membership_hash": canonical_sha256(_membership_proof()),
        })
    return engine, receipt


def _insert_pool_row(connection, table: str, row: dict) -> None:
    columns = tuple(row)
    connection.execute(
        text(
            f"INSERT INTO {table} ("
            + ", ".join(f"`{column}`" for column in columns)
            + ") VALUES ("
            + ", ".join(f":{column}" for column in columns)
            + ")"
        ),
        row,
    )


def _validate_strategy_pool(engine, receipt, *, run_uid=_STRATEGY_RUN_UID):
    with patch(
        "integrations.bigqmt.membership_snapshot."
        "verify_existing_membership_snapshot",
        return_value=_membership_proof(),
    ):
        return scheduler_validation._validate_analysis_strategy_pool(
            engine,
            target_date=date(2026, 8, 26),
            started_at=datetime(2026, 8, 27, 3, 5),
            now=datetime(2026, 8, 27, 3, 7),
            scheduler_run_uid=run_uid,
            expected_build_sha=_STRATEGY_BUILD_SHA,
            output=json.dumps(receipt, ensure_ascii=False),
        )


def _add_running_scheduler_audit(engine):
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_scheduled_task_history (
                run_uid TEXT NOT NULL,
                task_type TEXT NOT NULL,
                status TEXT NOT NULL,
                finished_at DATETIME,
                duration INTEGER,
                exit_code INTEGER,
                output TEXT
            )
        """))
        connection.execute(text("""
            INSERT INTO st_scheduled_task_history
                (run_uid, task_type, status)
            VALUES (:run_uid, 'analysis_fast', 'running')
        """), {"run_uid": _STRATEGY_RUN_UID})
    with engine.connect() as connection:
        connection.connection.driver_connection.create_function(
            "NOW", 0, lambda: "2026-08-27 03:07:00"
        )


def _activate_strategy_pool(engine, *, proof=None):
    with patch(
        "integrations.bigqmt.membership_snapshot."
        "verify_existing_membership_snapshot",
        return_value=proof or _membership_proof(),
    ):
        with engine.begin() as connection:
            scheduler_runtime._activate_analysis_strategy_pool(
                connection,
                run_uid=_STRATEGY_RUN_UID,
                task_type="analysis_fast",
            )


def test_analysis_strategy_pool_requires_allow_pick_and_actionable_counts():
    engine, receipt = _strategy_pool_engine(actionable=False)
    ok, message = _validate_strategy_pool(engine, receipt)
    assert not ok
    assert "current scheduler run" in message
    assert "executable=0" in message

    engine, receipt = _strategy_pool_engine(actionable=True)
    ok, message = _validate_strategy_pool(engine, receipt)
    assert ok
    assert f"producer_run_uid={_STRATEGY_RUN_UID}" in message
    assert "picks=1" in message
    assert "executable=1" in message


def test_analysis_strategy_pool_rejects_other_run_rows_when_current_run_has_zero():
    engine, receipt = _strategy_pool_engine(
        actionable=True, history_passed=0
    )
    ok, message = _validate_strategy_pool(engine, receipt)

    assert not ok
    assert "current scheduler run" in message
    assert "passed=0" in message


def test_analysis_strategy_pool_rejects_ambiguous_current_run_history():
    engine, receipt = _strategy_pool_engine(actionable=True)
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO st_recommended_run_history
            SELECT 2, run_uid, scheduler_job_id, trade_date, status, build_sha,
                   total, passed, started_at, finished_at,
                   publisher_task_type, canonical_pool_sha256, published_at,
                   executable_count, membership_snapshot_date,
                   membership_snapshot_source, membership_proof_sha256
            FROM st_recommended_run_history WHERE id=1
        """), {
        })

    ok, message = _validate_strategy_pool(engine, receipt)

    assert not ok
    assert "unavailable or ambiguous" in message


def test_analysis_strategy_pool_rejects_history_build_date_and_status_drift():
    for engine, receipt in (
        _strategy_pool_engine(actionable=True, history_status="running"),
        _strategy_pool_engine(actionable=True, history_build_sha="3" * 40),
        _strategy_pool_engine(
            actionable=True,
            history_trade_date="2026-08-25",
        ),
    ):
        ok, message = _validate_strategy_pool(engine, receipt)
        assert not ok
        assert "current scheduler run" in message


def _overwrite_strategy_partition(
    engine,
    *,
    claimed_hash: str | None = None,
):
    second_uid = "3" * 32
    with engine.begin() as connection:
        connection.execute(text("DELETE FROM stock_analysis_result"))
        connection.execute(text("DELETE FROM st_recommended_stocks"))
        analysis_row = dict.fromkeys(CANONICAL_ANALYSIS_COLUMNS)
        analysis_row.update({
            "stock_code": "600002",
            "stock_name": "策略二",
            "analysis_date": "2026-08-26",
            "recommend_status": "ALLOW",
            "model_version": "test-v2",
        })
        recommendation_row = dict.fromkeys(CANONICAL_RECOMMENDATION_COLUMNS)
        recommendation_row.update({
            "stock_code": "600002",
            "short_name": "策略二",
            "pick_date": "2026-08-26",
            "recommend_status": "PENDING",
            "candidate_recommend_status": "ALLOW",
            "signal_status": "CONFIRM",
            "chase_risk_status": "ALLOW",
            "ordinary_buy_eligible": 0,
            "candidate_ordinary_buy_eligible": 1,
            "publisher_run_uid": second_uid,
            "publication_status": "PENDING",
            "membership_snapshot_date": "2026-08-26",
            "membership_snapshot_source": "gj_big_qmt_inner",
            "membership_proof_sha256": canonical_sha256(
                _membership_proof()
            ),
            "turnover_evidence_json": _turnover_evidence("600002"),
            "upper_limit_evidence_json": _upper_limit_evidence("600002"),
            "model_version": "test-v2",
        })
        _insert_pool_row(connection, "stock_analysis_result", analysis_row)
        _insert_pool_row(connection, "st_recommended_stocks", recommendation_row)
        manifest = read_persisted_pool_manifest(connection, "2026-08-26")
        connection.execute(text("""
            INSERT INTO st_recommended_run_history
            VALUES (2, :run_uid, :run_uid, '2026-08-26', 'done', :build_sha,
                    1, 1, '2026-08-27 03:06:31',
                    '2026-08-27 03:06:50', 'analysis_morning_strict',
                    :pool_hash, '2026-08-27 03:06:40', 1,
                    '2026-08-26', 'gj_big_qmt_inner', :membership_hash)
        """), {
            "run_uid": second_uid,
            "build_sha": _STRATEGY_BUILD_SHA,
            "pool_hash": claimed_hash or manifest["canonical_pool_sha256"],
            "membership_hash": canonical_sha256(_membership_proof()),
        })
    return second_uid, manifest


def test_analysis_strategy_pool_replay_converges_after_newer_writer_overwrite():
    engine, first_receipt = _strategy_pool_engine(actionable=True)
    second_uid, _manifest = _overwrite_strategy_partition(engine)

    ok, message = _validate_strategy_pool(engine, first_receipt)

    assert ok
    assert f"validated_run_uid={_STRATEGY_RUN_UID}" in message
    assert f"producer_run_uid={second_uid}" in message


def test_analysis_strategy_pool_rejects_two_run_hash_impersonation():
    engine, first_receipt = _strategy_pool_engine(actionable=True)
    _overwrite_strategy_partition(
        engine,
        claimed_hash=first_receipt["canonical_pool_sha256"],
    )

    ok, message = _validate_strategy_pool(engine, first_receipt)

    assert not ok
    assert "mutable partition differs" in message


def test_analysis_strategy_pool_activation_keeps_stable_manifest_hash():
    engine, _receipt = _strategy_pool_engine(actionable=True)
    _add_running_scheduler_audit(engine)
    with engine.connect() as connection:
        pending = read_persisted_pool_manifest(connection, "2026-08-26")

    _activate_strategy_pool(engine)

    with engine.connect() as connection:
        active = read_persisted_pool_manifest(connection, "2026-08-26")
    assert pending["publication_statuses"] == ["PENDING"]
    assert active["publication_statuses"] == ["ACTIVE"]
    assert pending["canonical_pool_sha256"] == active["canonical_pool_sha256"]
    assert active["live_gate_alignment"] is True
    assert active["executable_count"] == 1


def test_analysis_strategy_pool_activation_rechecks_hash_after_validation():
    engine, receipt = _strategy_pool_engine(actionable=True)
    _add_running_scheduler_audit(engine)
    ok, _message = _validate_strategy_pool(engine, receipt)
    assert ok
    with engine.begin() as connection:
        connection.execute(text("""
            UPDATE st_recommended_stocks
            SET signal_status='WATCH'
            WHERE pick_date='2026-08-26'
        """))

    try:
        _activate_strategy_pool(engine)
    except RuntimeError as exc:
        assert "manifest differs" in str(exc)
    else:  # pragma: no cover - explicit fail-closed assertion
        raise AssertionError("hash-bound tamper unexpectedly activated")

    with engine.connect() as connection:
        row = connection.execute(text("""
            SELECT publication_status, recommend_status,
                   ordinary_buy_eligible
            FROM st_recommended_stocks
        """)).mappings().one()
    assert row == {
        "publication_status": "PENDING",
        "recommend_status": "PENDING",
        "ordinary_buy_eligible": "0",
    }


def test_analysis_strategy_pool_activation_rejects_membership_proof_drift():
    engine, _receipt = _strategy_pool_engine(actionable=True)
    _add_running_scheduler_audit(engine)
    drifted = {**_membership_proof(), "industry_hash": "f" * 64}

    try:
        _activate_strategy_pool(engine, proof=drifted)
    except RuntimeError as exc:
        assert "membership proof differs" in str(exc)
    else:  # pragma: no cover - explicit fail-closed assertion
        raise AssertionError("membership drift unexpectedly activated")

    with engine.connect() as connection:
        status = connection.execute(text(
            "SELECT publication_status FROM st_recommended_stocks"
        )).scalar_one()
    assert status == "PENDING"


def test_analysis_activation_failure_cannot_leave_successful_terminal_audit():
    engine, _receipt = _strategy_pool_engine(actionable=True)
    _add_running_scheduler_audit(engine)
    with engine.begin() as connection:
        connection.execute(text("""
            UPDATE st_recommended_stocks
            SET signal_status='WATCH'
            WHERE pick_date='2026-08-26'
        """))
    with patch(
        "integrations.bigqmt.membership_snapshot."
        "verify_existing_membership_snapshot",
        return_value=_membership_proof(),
    ):
        try:
            scheduler_runtime._task_history_finish(
                engine,
                _STRATEGY_RUN_UID,
                status="success",
                duration=1,
                exit_code=0,
                output="validated",
                task_type="analysis_fast",
            )
        except RuntimeError as exc:
            assert "activation/terminal audit failed" in str(exc)
        else:  # pragma: no cover - explicit fail-closed assertion
            raise AssertionError("activation failure was swallowed")
    scheduler_runtime._task_history_finish(
        engine,
        _STRATEGY_RUN_UID,
        status="failed",
        duration=1,
        exit_code=1,
        output="activation failed",
        task_type="analysis_fast",
    )
    with engine.connect() as connection:
        audit_status = connection.execute(text(
            "SELECT status FROM st_scheduled_task_history"
        )).scalar_one()
        publication_status = connection.execute(text(
            "SELECT publication_status FROM st_recommended_stocks"
        )).scalar_one()
    assert audit_status == "failed"
    assert publication_status == "PENDING"


def test_direct_analysis_history_prebind_and_terminal_finish(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_recommended_run_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_uid TEXT NOT NULL UNIQUE,
                scheduler_job_id TEXT,
                trade_date DATE,
                status TEXT NOT NULL,
                min_score REAL,
                top_n INTEGER,
                strict_prev_trade_day INTEGER,
                execution_time DATETIME,
                started_at DATETIME,
                finished_at DATETIME,
                progress_percent INTEGER,
                done_count INTEGER,
                total INTEGER,
                message TEXT,
                error TEXT,
                trigger_source TEXT,
                build_sha TEXT,
                publisher_task_type TEXT,
                canonical_pool_sha256 TEXT,
                published_at DATETIME,
                executable_count INTEGER
            )
        """))
    monkeypatch.setattr(
        sync_analysis_fast,
        "validate_recommended_run_history_schema",
        lambda _engine: {},
    )
    sync_analysis_fast._prebind_direct_publication_history(
        engine,
        run_uid=_STRATEGY_RUN_UID,
        task_type="analysis_fast",
        build_sha=_STRATEGY_BUILD_SHA,
        trade_date="2026-08-26",
        min_score=62,
        top_n=80,
        strict_prev_trade_day=False,
        execution_time="2026-08-26 18:50:00",
    )
    with engine.begin() as connection:
        running = connection.execute(text("""
            SELECT status, scheduler_job_id, trade_date, build_sha
            FROM st_recommended_run_history
        """)).mappings().one()
        connection.execute(text("""
            UPDATE st_recommended_run_history
            SET canonical_pool_sha256=:pool_hash,
                published_at=CURRENT_TIMESTAMP,
                executable_count=1, total=5000
        """), {"pool_hash": "a" * 64})
    assert running == {
        "status": "running",
        "scheduler_job_id": _STRATEGY_RUN_UID,
        "trade_date": "2026-08-26",
        "build_sha": _STRATEGY_BUILD_SHA,
    }

    sync_analysis_fast._finish_direct_publication_history(
        engine,
        run_uid=_STRATEGY_RUN_UID,
        success=True,
    )

    with engine.connect() as connection:
        terminal = connection.execute(text("""
            SELECT status, finished_at, progress_percent, done_count
            FROM st_recommended_run_history
        """)).mappings().one()
    assert terminal["status"] == "done"
    assert terminal["finished_at"] is not None
    assert terminal["progress_percent"] == 100
    assert terminal["done_count"] == 5000


def test_direct_analysis_main_without_json_emits_validator_receipt(
    monkeypatch,
    capsys,
):
    engine, receipt = _strategy_pool_engine(actionable=True)
    child_engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    monkeypatch.setenv(
        "PROBIGA_SCHEDULER_HISTORY_RUN_UID", _STRATEGY_RUN_UID
    )
    monkeypatch.setenv("PROBIGA_SCHEDULER_TASK_TYPE", "analysis_fast")
    monkeypatch.setenv("PROBIGA_SCHEDULER_BUILD_SHA", _STRATEGY_BUILD_SHA)
    monkeypatch.setattr(
        sync_analysis_fast.sys,
        "argv",
        ["sync_analysis_fast.py", "--date", "2026-08-26"],
    )
    monkeypatch.setattr(
        sync_analysis_fast, "create_batch_engine", lambda: child_engine
    )
    monkeypatch.setattr(
        sync_analysis_fast,
        "_prebind_direct_publication_history",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        sync_analysis_fast,
        "_finish_direct_publication_history",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        sync_analysis_fast,
        "run_batch",
        lambda **_kwargs: sync_analysis_fast.BatchStats(
            trade_date="2026-08-26",
            analysis_count=1,
            recommendation_count=1,
            market_mood_score=50.0,
            flow_date="2026-08-26",
            hot_date="2026-08-26",
            executable_count=1,
            canonical_pool_sha256=receipt["canonical_pool_sha256"],
            publication_receipt=receipt,
        ),
    )

    assert sync_analysis_fast.main() == 0
    output = capsys.readouterr().out.strip()
    assert len(output.splitlines()) == 1
    assert json.loads(output)["publication_receipt"] == receipt
    with patch(
        "integrations.bigqmt.membership_snapshot."
        "verify_existing_membership_snapshot",
        return_value=_membership_proof(),
    ):
        ok, message = scheduler_validation._validate_analysis_strategy_pool(
            engine,
            target_date=date(2026, 8, 26),
            started_at=datetime(2026, 8, 27, 3, 5),
            now=datetime(2026, 8, 27, 3, 7),
            scheduler_run_uid=_STRATEGY_RUN_UID,
            expected_build_sha=_STRATEGY_BUILD_SHA,
            output=output,
        )
    assert ok, message


def test_release_analysis_postvalidation_fails_closed_on_empty_strategy_pool(
    monkeypatch,
):
    monkeypatch.setattr(
        scheduler_validation,
        "_validate_requirement",
        lambda *_args, **_kwargs: (True, "full analysis coverage verified"),
    )
    monkeypatch.setattr(
        scheduler_validation,
        "_validate_daily_universe_coverage",
        lambda *_args, **_kwargs: (
            True,
            "exact market universe coverage verified",
        ),
    )
    monkeypatch.setattr(
        "integrations.bigqmt.membership_snapshot."
        "verify_existing_membership_snapshot",
        lambda *_args, **_kwargs: _membership_proof(),
    )
    engine, receipt = _strategy_pool_engine(actionable=False)
    result = scheduler_validation.validate_scheduler_task_result(
        {
            "task_type": "analysis_fast",
            "_trigger_source": "release_catchup",
            "_release_target_date": "2026-08-26",
            "_scheduler_history_run_uid": _STRATEGY_RUN_UID,
            "_scheduler_expected_build_sha": _STRATEGY_BUILD_SHA,
        },
        engine=engine,
        started_at=datetime(2026, 8, 27, 3, 5),
        now=datetime(2026, 8, 27, 3, 7),
        output=json.dumps(receipt, ensure_ascii=False),
    )

    assert result.checked and not result.ok
    assert "current scheduler run" in result.message
    assert "executable=0" in result.message
