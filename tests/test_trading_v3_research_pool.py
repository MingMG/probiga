from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from pathlib import Path

import pytest

from server.trading_v3.research_pool import (
    ResearchPoolValidationError,
    load_research_payload_file,
    publish_research_pool,
    read_research_pool,
)
from tools import publish_trading_v3_research_pool as publish_cli


BUILD_SHA = "8" * 40


def _artifact_hash(artifact: dict) -> str:
    unhashed = dict(artifact)
    unhashed.pop("artifact_sha256", None)
    return hashlib.sha256(json.dumps(
        unhashed,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")).hexdigest()


def _payload(*, known_at: str = "2026-09-07 00:10:00") -> dict:
    forecasts = [
        {
            "stock_code": "000001",
            "stock_name": "平安银行",
            "strategy_key": "paper_strategy",
            "status": "PAPER_DISCOVERY_CANDIDATE",
            "reasons": ["纸面观察"],
            "raw_score": 0.72,
            "confidence": 0.42,
            "initial_stop_pct": -6.0,
            "valid_until": "2026-09-18 15:00:00",
            "features": {"price": 12.34},
        },
        {
            "stock_code": "000001",
            "stock_name": "平安银行",
            "strategy_key": "left_strategy",
            "status": "LEFT_SIDE_PREPARE",
            "reasons": ["左侧准备"],
            "raw_score": 0.61,
            "confidence": 0.31,
            "features": {"price": 12.34},
        },
        {
            "stock_code": "000001",
            "stock_name": "平安银行",
            "strategy_key": "blocked_strategy",
            "status": "DATA_QUALITY_BLOCKED",
            "reasons": ["不得混入观察理由"],
            "raw_score": 0.99,
            "confidence": 0.99,
            "features": {"price": 99.99},
        },
        {
            "stock_code": "000002",
            "stock_name": "万科A",
            "strategy_key": "uncalibrated_strategy",
            "status": "RESEARCH_ONLY_UNCALIBRATED",
            "reasons": ["尚未校准"],
            "raw_score": 0.55,
            "confidence": 0.0,
            "features": {"price": 8.76},
        },
        {
            "stock_code": "000003",
            "stock_name": "仅覆盖",
            "strategy_key": "setup_strategy",
            "status": "SETUP_NOT_READY",
            "reasons": ["条件未形成"],
            "raw_score": 0.8,
            "confidence": 0.0,
            "features": {"price": 5.0},
        },
    ]
    artifact = {
        "schema": "probiga.trading-v3-retrospective-research.v1",
        "research_run_uid": "research-0904",
        "requested_as_of": "2026-09-04",
        "trade_date": "2026-09-04",
        "historical_fact_cutoff_at": "2026-09-04 23:59:59.999999",
        "research_known_at": known_at,
        "interpretation": "CURRENT_CODE_AND_MODEL_APPLIED_TO_HISTORICAL_FACTS",
        "historical_production_decision": False,
        "canonical_eligible": False,
        "competition_eligible": False,
        "order_authority": False,
        "notification_eligible": False,
        "persisted": False,
        "model_evaluation": {
            "strategy_version": "v3-test",
            "historical_model_identity_proven": False,
        },
        "research_assumptions": {
            "account_snapshot_consumed": False,
            "position_state_consumed": False,
            "open_order_state_consumed": False,
            "paper_learning_consumed": False,
            "portfolio_allocation_computed": False,
        },
        "data_snapshot_hash": "d" * 64,
        "pit_evidence": {
            "fact_cutoff_at": "2026-09-04T23:59:59.999999",
            "decision_known_at": known_at.replace(" ", "T"),
        },
        "regime": {},
        "strategy_weights": {},
        "consensus": [],
        "forecasts": forecasts,
    }
    artifact["artifact_sha256"] = _artifact_hash(artifact)
    return {
        "schema": "probiga.trading-v3-retrospective-research.v1",
        "status": "ok",
        "run_status": "COMPLETED",
        "actionable_status": "REPLAY_ONLY",
        "result_scope": "RETROSPECTIVE_RESEARCH",
        "persisted": False,
        "canonical_eligible": False,
        "competition_eligible": False,
        "order_authority": False,
        "notification_eligible": False,
        "execution_enabled": False,
        "real_trading_enabled": False,
        "real_order_count": 0,
        "target_count": 0,
        "portfolio_status": "RESEARCH_ONLY",
        "paper_order_count": 0,
        "position_state_updates": 0,
        "paper_orders": [],
        "superseded_paper_orders": [],
        "superseded_partial_paper_orders": [],
        "superseded_execution_plans": [],
        "premarket_frozen_paper_orders": [],
        "premarket_frozen_execution_plans": [],
        "notification": {
            "status": "suppressed",
            "reason": "RETROSPECTIVE_RESEARCH",
        },
        "trade_date": "2026-09-04",
        "decision_at": "2026-09-04 23:59:59.999999",
        "research_run_uid": "research-0904",
        "forecast_count": len(forecasts),
        "validated_count": 0,
        "research_artifact": artifact,
    }


def _publish(
    payload: dict,
    store_root: Path,
    *,
    minute: int = 11,
    require_observations: bool = False,
) -> dict:
    source = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    return publish_research_pool(
        payload,
        publisher_build_sha=BUILD_SHA,
        store_root=store_root,
        published_at=datetime(2026, 9, 7, 0, minute),
        source_bytes=source,
        require_observations=require_observations,
    )


def _same_day_payload(*, cutoff_at: str, known_at: str) -> dict:
    payload = _payload(known_at=known_at)
    artifact = payload["research_artifact"]
    payload["trade_date"] = "2026-09-07"
    payload["decision_at"] = cutoff_at
    artifact["requested_as_of"] = "2026-09-07"
    artifact["trade_date"] = "2026-09-07"
    artifact["historical_fact_cutoff_at"] = cutoff_at
    artifact["pit_evidence"]["fact_cutoff_at"] = cutoff_at.replace(" ", "T")
    artifact["artifact_sha256"] = _artifact_hash(artifact)
    return payload


def test_publish_and_read_projects_only_observation_forecasts(tmp_path: Path):
    store_root = tmp_path / "jobs"
    receipt = _publish(_payload(), store_root, require_observations=True)

    entries = sorted(path.name for path in store_root.iterdir())
    assert len(entries) == 2
    assert all((store_root / name).is_file() for name in entries)
    assert any(name.startswith("research-pool-object-") for name in entries)
    assert any(name.startswith("research-pool-manifest-") for name in entries)

    result = read_research_pool(
        date(2026, 9, 4),
        store_root=store_root,
        now=datetime(2026, 9, 7, 0, 12),
    )

    assert receipt["publication_status"] == "PASS"
    assert receipt["database_writes"] is False
    assert receipt["notifications_sent"] is False
    assert result["status"] == "READY"
    assert result["pool_readable"] is True
    assert result["trade_date"] == result["data_date"] == "2026-09-04"
    assert result["summary"] == {
        "observation_stock_count": 2,
        "matching_forecast_count": 3,
        "total_forecast_count": 5,
        "excluded_forecast_count": 2,
        "excluded_stock_count": 1,
        "status_forecast_counts": {
            "DATA_QUALITY_BLOCKED": 1,
            "LEFT_SIDE_PREPARE": 1,
            "PAPER_DISCOVERY_CANDIDATE": 1,
            "RESEARCH_ONLY_UNCALIBRATED": 1,
            "SETUP_NOT_READY": 1,
        },
    }
    first = result["items"][0]
    assert first["stock_code"] == "000001"
    assert first["strategy_keys"] == ["left_strategy", "paper_strategy"]
    assert first["statuses"] == [
        "PAPER_DISCOVERY_CANDIDATE",
        "LEFT_SIDE_PREPARE",
    ]
    assert first["reasons"] == ["纸面观察", "左侧准备"]
    assert first["reference_price"] == 12.34
    assert first["display_action"] == "WATCH"
    assert first["decision_scope"] == "RESEARCH_ONLY"
    assert first["new_buy_eligible"] is False
    assert first["order_eligible"] is False
    for forbidden in ("initial_stop_pct", "valid_until", "entry_price", "target"):
        assert forbidden not in first


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("canonical_eligible", True),
        ("order_authority", True),
        ("persisted", True),
        ("execution_enabled", True),
    ],
)
def test_publish_rejects_any_outer_authority(field: str, value: bool, tmp_path: Path):
    payload = _payload()
    payload[field] = value
    with pytest.raises(ResearchPoolValidationError, match=field):
        _publish(payload, tmp_path / "store")


def test_publish_rejects_tampered_forecast_hash(tmp_path: Path):
    payload = _payload()
    payload["research_artifact"]["forecasts"][0]["reasons"] = ["篡改"]
    with pytest.raises(ResearchPoolValidationError, match="artifact hash differs"):
        _publish(payload, tmp_path / "store")


def test_read_never_falls_back_to_another_date(tmp_path: Path):
    store_root = tmp_path / "store"
    _publish(_payload(), store_root)
    result = read_research_pool(
        date(2026, 9, 3),
        store_root=store_root,
        now=datetime(2026, 9, 7, 0, 12),
    )
    assert result["status"] == "UNAVAILABLE"
    assert result["reason_codes"] == ["NO_EXACT_RESEARCH_POOL"]
    assert result["items"] == []


def test_publish_accepts_same_day_observation_only_after_close(tmp_path: Path):
    payload = _same_day_payload(
        cutoff_at="2026-09-07 18:00:00",
        known_at="2026-09-07 22:10:00",
    )
    receipt = publish_research_pool(
        payload,
        publisher_build_sha=BUILD_SHA,
        store_root=tmp_path / "store",
        published_at=datetime(2026, 9, 7, 22, 11),
    )
    result = read_research_pool(
        date(2026, 9, 7),
        store_root=tmp_path / "store",
        now=datetime(2026, 9, 7, 22, 12),
    )
    assert receipt["publication_status"] == "PASS"
    assert result["status"] == "READY"
    assert result["historical_fact_cutoff_at"] == "2026-09-07 18:00:00"
    assert result["research_known_at"] == "2026-09-07 22:10:00"


def test_publish_rejects_same_day_observation_before_close(tmp_path: Path):
    payload = _same_day_payload(
        cutoff_at="2026-09-07 17:59:59",
        known_at="2026-09-07 22:10:00",
    )
    with pytest.raises(ResearchPoolValidationError, match="closed session"):
        publish_research_pool(
            payload,
            publisher_build_sha=BUILD_SHA,
            store_root=tmp_path / "store",
            published_at=datetime(2026, 9, 7, 22, 11),
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("real_order_count", 1),
        ("target_count", 1),
        ("portfolio_status", "READY"),
    ],
)
def test_publish_rejects_outer_execution_projection(
    field: str,
    value: int | str,
    tmp_path: Path,
):
    payload = _payload()
    payload[field] = value
    with pytest.raises(ResearchPoolValidationError):
        _publish(payload, tmp_path / "store")


def test_publish_rejects_matching_forecast_without_strategy(tmp_path: Path):
    payload = _payload()
    payload["research_artifact"]["forecasts"][0]["strategy_key"] = ""
    payload["research_artifact"]["artifact_sha256"] = _artifact_hash(
        payload["research_artifact"]
    )
    with pytest.raises(ResearchPoolValidationError, match="strategy key"):
        _publish(payload, tmp_path / "store")


def test_valid_pool_with_no_observation_status_is_empty(tmp_path: Path):
    payload = _payload()
    for forecast in payload["research_artifact"]["forecasts"]:
        forecast["status"] = "SETUP_NOT_READY"
    payload["research_artifact"]["artifact_sha256"] = _artifact_hash(
        payload["research_artifact"]
    )
    _publish(payload, tmp_path / "store")

    result = read_research_pool(
        date(2026, 9, 4),
        store_root=tmp_path / "store",
        now=datetime(2026, 9, 7, 0, 12),
    )
    assert result["status"] == "EMPTY"
    assert result["pool_readable"] is True
    assert result["reason_codes"] == ["NO_MATCHING_RESEARCH_OBSERVATIONS"]
    assert result["items"] == []


def test_required_observations_reject_empty_before_writing_and_keep_latest_ready(
    tmp_path: Path,
):
    store_root = tmp_path / "store"
    ready_receipt = _publish(_payload(), store_root)
    paths_before = sorted(path.name for path in store_root.iterdir())

    empty_payload = _payload(known_at="2026-09-07 00:20:00")
    for forecast in empty_payload["research_artifact"]["forecasts"]:
        forecast["status"] = "SETUP_NOT_READY"
    empty_payload["research_artifact"]["artifact_sha256"] = _artifact_hash(
        empty_payload["research_artifact"]
    )

    with pytest.raises(
        ResearchPoolValidationError,
        match="NO_RESEARCH_OBSERVATION_CANDIDATES",
    ):
        _publish(
            empty_payload,
            store_root,
            minute=21,
            require_observations=True,
        )

    assert sorted(path.name for path in store_root.iterdir()) == paths_before
    result = read_research_pool(
        date(2026, 9, 4),
        store_root=store_root,
        now=datetime(2026, 9, 7, 0, 22),
    )
    assert result["status"] == "READY"
    assert result["artifact_sha256"] == ready_receipt["artifact_sha256"]
    assert result["payload_file_sha256"] == ready_receipt["payload_file_sha256"]


@pytest.mark.parametrize(
    "status",
    [
        "VALIDATED_POSITIVE",
        "PAPER_DISCOVERY_CANDIDATE",
        "LEFT_SIDE_PREPARE",
        "RESEARCH_ONLY_UNCALIBRATED",
    ],
)
def test_required_observations_use_shared_allowed_status_projection(
    status: str,
    tmp_path: Path,
):
    payload = _payload()
    forecasts = payload["research_artifact"]["forecasts"]
    for forecast in forecasts:
        forecast["status"] = "SETUP_NOT_READY"
    forecasts[0]["status"] = status
    payload["validated_count"] = int(status == "VALIDATED_POSITIVE")
    payload["research_artifact"]["artifact_sha256"] = _artifact_hash(
        payload["research_artifact"]
    )

    _publish(payload, tmp_path / "store", require_observations=True)
    result = read_research_pool(
        date(2026, 9, 4),
        store_root=tmp_path / "store",
        now=datetime(2026, 9, 7, 0, 12),
    )
    assert result["status"] == "READY"
    assert result["summary"]["observation_stock_count"] == 1


def test_required_observations_reject_invalid_matching_code_before_writing(
    tmp_path: Path,
):
    payload = _payload()
    payload["research_artifact"]["forecasts"][0]["stock_code"] = "INVALID"
    payload["research_artifact"]["artifact_sha256"] = _artifact_hash(
        payload["research_artifact"]
    )
    store_root = tmp_path / "store"

    with pytest.raises(ResearchPoolValidationError, match="stock code"):
        _publish(payload, store_root, require_observations=True)

    assert not store_root.exists()


def test_reader_selects_latest_valid_artifact_and_skips_corruption(tmp_path: Path):
    store_root = tmp_path / "store"
    old_receipt = _publish(_payload(known_at="2026-09-07 00:10:00"), store_root)
    new_payload = _payload(known_at="2026-09-07 00:20:00")
    new_payload["research_artifact"]["forecasts"][0]["raw_score"] = 0.88
    new_payload["research_artifact"]["artifact_sha256"] = _artifact_hash(
        new_payload["research_artifact"]
    )
    new_receipt = publish_research_pool(
        new_payload,
        publisher_build_sha=BUILD_SHA,
        store_root=store_root,
        published_at=datetime(2026, 9, 7, 0, 21),
    )
    new_object = (
        store_root
        / f"research-pool-object-{new_receipt['payload_file_sha256']}.json"
    )
    new_object.write_text("{}", encoding="utf-8")

    result = read_research_pool(
        date(2026, 9, 4),
        store_root=store_root,
        now=datetime(2026, 9, 7, 0, 22),
    )
    assert result["status"] == "READY"
    assert result["payload_file_sha256"] == old_receipt["payload_file_sha256"]
    assert result["research_known_at"] == "2026-09-07 00:10:00"


def test_input_loader_rejects_path_outside_allowed_root(tmp_path: Path):
    allowed = tmp_path / "allowed"
    outside = tmp_path / "outside.json"
    allowed.mkdir()
    outside.write_text("{}", encoding="utf-8")
    with pytest.raises(ResearchPoolValidationError, match="outside allowed roots"):
        load_research_payload_file(outside, allowed_roots=(allowed,))


def test_cli_accepts_artifact_from_git_workspace_and_publishes_without_db(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    input_path = workspace / "research.json"
    input_path.write_text(
        json.dumps(_payload(), ensure_ascii=False),
        encoding="utf-8",
    )
    store_root = tmp_path / "jobs"
    monkeypatch.setattr(publish_cli, "load_project_env", lambda: None)
    monkeypatch.setattr(
        publish_cli,
        "_assert_release_checkout",
        lambda expected: BUILD_SHA if expected == BUILD_SHA else pytest.fail("SHA changed"),
    )
    monkeypatch.setattr(publish_cli, "_workspace_root", lambda: workspace)

    code = publish_cli.main([
        "--input",
        str(input_path),
        "--expected-build-sha",
        BUILD_SHA,
        "--store-root",
        str(store_root),
    ])

    receipt = json.loads(capsys.readouterr().out)
    assert code == 0
    assert receipt["publication_status"] == "PASS"
    assert receipt["database_writes"] is False
    assert receipt["notifications_sent"] is False
    assert read_research_pool(
        date(2026, 9, 4),
        store_root=store_root,
    )["status"] == "READY"
