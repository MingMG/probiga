from __future__ import annotations

import json
import sys
import types
from pathlib import Path

from tools import trading_v3_fourth_layer_readiness as readiness
from tools import verify_trading_v3_production as production_verifier


def test_workstation_verifier_executes_only_the_active_release_command(
    monkeypatch,
):
    events = []

    class _Stream:
        def __init__(self, payload=b"", status=2):
            self._payload = payload
            self.channel = types.SimpleNamespace(
                recv_exit_status=lambda: status
            )

        def read(self):
            return self._payload

    class _Client:
        def connect(self, **kwargs):
            events.append(("connect", kwargs))

        def exec_command(self, command, timeout):
            events.append(("exec", command, timeout))
            return None, _Stream(b'{"acceptance_status":"BLOCKED"}\n'), _Stream()

        def close(self):
            events.append(("close",))

    monkeypatch.setattr(production_verifier, "load_project_env", lambda: None)
    monkeypatch.setattr(
        production_verifier,
        "production_release_command",
        lambda entrypoint, arguments: (
            events.append(("build", entrypoint, arguments))
            or "PINNED_RELEASE_COMMAND"
        ),
    )
    monkeypatch.setattr(
        production_verifier,
        "production_ssh_connect_kwargs",
        lambda **kwargs: {"username": "deploy", **kwargs},
    )
    monkeypatch.setattr(
        production_verifier,
        "production_ssh_client",
        lambda _module: _Client(),
    )
    monkeypatch.setitem(sys.modules, "paramiko", types.SimpleNamespace())

    assert production_verifier._run_on_production_host() == 2
    assert events == [
        (
            "build",
            "tools/verify_trading_v3_production.py",
            ("--local-runtime",),
        ),
        ("connect", {"username": "deploy", "timeout": 30}),
        ("exec", "PINNED_RELEASE_COMMAND", 240),
        ("close",),
    ]


def test_local_verifier_rejects_path_only_production_claim(monkeypatch):
    monkeypatch.setattr(
        production_verifier, "_is_production_runtime", lambda: True
    )
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "development")

    ready, reason = production_verifier._local_production_runtime_identity()

    assert ready is False
    assert "not production" in reason


def test_shadow_migration_missing_and_partial_are_distinct_blockers():
    missing = readiness.evaluate_migration_state(
        {}, {}, schema_verified=False
    )
    assert missing["ready"] is False
    assert "SHADOW_MIGRATION_LEDGER_MISSING" in missing["reason_codes"]

    expected = missing["expected"]
    partial = readiness.evaluate_migration_state(
        {},
        {
            **expected,
            "completed_statement_count": expected["statement_count"] - 1,
        },
        schema_verified=False,
    )
    assert partial["ready"] is False
    assert "SHADOW_MIGRATION_PARTIALLY_APPLIED" in partial["reason_codes"]


def test_shadow_migration_requires_exact_ledger_and_structural_validation():
    expected = readiness.evaluate_migration_state(
        {}, {}, schema_verified=False
    )["expected"]
    result = readiness.evaluate_migration_state(
        expected,
        {**expected, "completed_statement_count": expected["statement_count"]},
        schema_verified=True,
    )
    assert result["ready"] is True
    drifted = readiness.evaluate_migration_state(
        {**expected, "checksum": "0" * 64},
        {},
        schema_verified=True,
    )
    assert "SHADOW_MIGRATION_LEDGER_DRIFT" in drifted["reason_codes"]


def test_shadow_migration_rejects_ledger_without_progress_proof():
    expected = readiness.evaluate_migration_state(
        {}, {}, schema_verified=False
    )["expected"]
    result = readiness.evaluate_migration_state(
        expected,
        {},
        schema_verified=True,
    )
    assert result["ready"] is False
    assert result["progress_consistent"] is False
    assert "SHADOW_MIGRATION_PROGRESS_MISSING" in result["reason_codes"]


def _scheduler_task(**overrides):
    task = {
        "id": 78,
        "task_type": "trading_v3_counterfactual_audit",
        "enabled": 1,
        "script_path": "tools/run_trading_v3_counterfactual.py",
        "script_args": "--limit 10000 --max-batches 10",
        "date_param": "",
        "cron_time": "16:30",
        "interval_minutes": 0,
        "last_run_status": "success",
    }
    task.update(overrides)
    return task


def _continuous_task(**overrides):
    task = {
        "id": 79,
        "task_type": "trading_v3_continuous_calibration",
        "enabled": 1,
        "script_path": "tools/run_trading_v3_continuous_calibration.py",
        "script_args": (
            "--lock-timeout-seconds 0 "
            "--training-timeout-seconds 19800"
        ),
        "date_param": "",
        "cron_time": "17:10",
        "interval_minutes": 0,
        "last_run_status": "success",
    }
    task.update(overrides)
    return task


def _heartbeat(**overrides):
    value = {
        "mode": "standalone",
        "heartbeat_age_seconds": 5,
        "poll_seconds": 60,
    }
    value.update(overrides)
    return value


def test_shadow_scheduler_requires_bounded_args_success_and_standalone():
    assert readiness.evaluate_scheduler_state(
        [_scheduler_task(), _continuous_task()], _heartbeat()
    )["ready"] is True

    failed = readiness.evaluate_scheduler_state(
        [
            _scheduler_task(script_args="", last_run_status="failed"),
            _continuous_task(),
        ],
        _heartbeat(mode="embedded"),
    )
    assert failed["ready"] is False
    assert "SHADOW_SCHEDULER_BOUNDS_MISSING" in failed["reason_codes"]
    assert "SHADOW_SCHEDULER_LAST_RUN_NOT_SUCCESS" in failed["reason_codes"]
    assert "SCHEDULER_NOT_STANDALONE" in failed["reason_codes"]


def test_shadow_scheduler_rejects_unregistered_alias():
    result = readiness.evaluate_scheduler_state(
        [
            _scheduler_task(
                task_type="unregistered_shadow_alias",
                script_args="",
            ),
            _continuous_task(),
        ],
        _heartbeat(),
    )
    assert "SHADOW_SCHEDULER_TASK_CARDINALITY_INVALID" in result["reason_codes"]


def test_shadow_scheduler_requires_both_exact_layer4_task_definitions():
    missing = readiness.evaluate_scheduler_state(
        [_scheduler_task()],
        _heartbeat(),
    )
    assert missing["ready"] is False
    assert "SHADOW_SCHEDULER_TASK_CARDINALITY_INVALID" in missing["reason_codes"]

    drifted = readiness.evaluate_scheduler_state(
        [
            _scheduler_task(script_args="--limit 1 --max-batches 10"),
            _continuous_task(cron_time="18:10"),
        ],
        _heartbeat(),
    )
    assert drifted["ready"] is False
    assert "SHADOW_SCHEDULER_TASK_DEFINITION_DRIFT" in drifted["reason_codes"]


def test_shadow_scheduler_blocks_two_simultaneously_fresh_writers():
    standalone = _heartbeat(instance_id="local", mode="standalone")
    embedded = _heartbeat(instance_id="remote", mode="embedded")
    result = readiness.evaluate_scheduler_state(
        [_scheduler_task(), _continuous_task()],
        standalone,
        heartbeats=[standalone, embedded],
    )
    assert result["ready"] is False
    assert "SCHEDULER_MULTIPLE_LIVE_WRITERS" in result["reason_codes"]
    assert "SCHEDULER_NOT_STANDALONE" in result["reason_codes"]


def test_shadow_scheduler_uses_two_poll_freshness_boundary():
    stale = _heartbeat(heartbeat_age_seconds=121, poll_seconds=60)
    result = readiness.evaluate_scheduler_state(
        [_scheduler_task(), _continuous_task()],
        stale,
    )
    assert result["ready"] is False
    assert "SCHEDULER_HEARTBEAT_STALE" in result["reason_codes"]


def test_shadow_scheduler_rejects_invalid_heartbeat_contract():
    invalid = _heartbeat(poll_seconds=0)
    result = readiness.evaluate_scheduler_state(
        [_scheduler_task(), _continuous_task()],
        invalid,
    )
    assert result["ready"] is False
    assert "SCHEDULER_HEARTBEAT_CONTRACT_INVALID" in result["reason_codes"]


def _endpoint_payload(config_hash: str, *, status: str = "ok", data=None):
    return {
        "http_status": 200,
        "payload": {
            "status": status,
            "config_version": "v-test",
            "config_hash": config_hash,
            "real_trading_enabled": False,
            "data": data or {"order_authority": False},
        },
    }


def test_page_get_truth_blocks_when_readiness_is_truthfully_not_ready():
    digest = "a" * 64
    responses = {
        name: _endpoint_payload(digest)
        for name in readiness.PAGE_GET_PATHS
    }
    responses["readiness"] = _endpoint_payload(
        digest,
        status="blocked",
        data={
            "paper_ready": False,
            "order_authority": False,
            "blocks": ["V3_SCHEMA_INCOMPLETE"],
        },
    )
    result = readiness.evaluate_page_get_truth(
        responses,
        config_version="v-test",
        config_hash=digest,
    )
    assert result["ready"] is False
    assert "PAGE_GET_READINESS_NOT_READY" in result["reason_codes"]


def test_page_get_truth_rejects_stale_config_and_order_authority():
    digest = "a" * 64
    responses = {
        name: _endpoint_payload(digest)
        for name in readiness.PAGE_GET_PATHS
    }
    responses["readiness"] = _endpoint_payload(
        digest,
        data={"paper_ready": True, "order_authority": False},
    )
    responses["shadow"] = _endpoint_payload(
        "b" * 64,
        data={"order_authority": True},
    )
    result = readiness.evaluate_page_get_truth(
        responses,
        config_version="v-test",
        config_hash=digest,
    )
    assert "PAGE_GET_SHADOW_TRUTH_INVALID" in result["reason_codes"]


def test_page_get_truth_rejects_non_boolean_order_authority():
    digest = "a" * 64
    responses = {
        name: _endpoint_payload(digest)
        for name in readiness.PAGE_GET_PATHS
    }
    responses["readiness"] = _endpoint_payload(
        digest,
        data={"paper_ready": True, "order_authority": False},
    )
    responses["shadow"] = _endpoint_payload(
        digest,
        data={"order_authority": "false"},
    )
    result = readiness.evaluate_page_get_truth(
        responses,
        config_version="v-test",
        config_hash=digest,
    )
    assert result["ready"] is False
    assert "PAGE_GET_SHADOW_TRUTH_INVALID" in result["reason_codes"]


def test_page_get_collection_rejects_non_loopback_base_without_fetch(monkeypatch):
    monkeypatch.setenv("PROBIGA_LOCAL_API_BASE_URL", "https://example.com")
    monkeypatch.setattr(
        readiness,
        "_fetch_one",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("must not fetch a non-loopback verifier URL")
        ),
    )
    result = readiness.collect_page_get_truth(
        config_version="v-test",
        config_hash="a" * 64,
    )
    assert result["ready"] is False
    assert result["reason_codes"] == ["PAGE_GET_BASE_URL_NOT_LOOPBACK"]


def test_real_trading_guard_truth_requires_exact_signal_bodies():
    rows = []
    for name, (timing, event, table_name, column) in (
        production_verifier._EXPECTED_REAL_TRADING_GUARDS.items()
    ):
        rows.append({
            "TRIGGER_NAME": name,
            "ACTION_TIMING": timing,
            "EVENT_MANIPULATION": event,
            "EVENT_OBJECT_TABLE": table_name,
            "ACTION_STATEMENT": (
                f"BEGIN IF COALESCE(NEW.{column}, 0) <> 0 THEN "
                "SIGNAL SQLSTATE '45000'; END IF; END"
            ),
        })
    assert production_verifier._real_trading_guard_rows_valid(rows) is True

    rows[0]["ACTION_STATEMENT"] = "BEGIN SET NEW.real_trading_enabled = 0; END"
    assert production_verifier._real_trading_guard_rows_valid(rows) is False


def test_real_trading_guard_truth_rejects_query_error():
    assert production_verifier._real_trading_guard_rows_valid([
        {"query_error": "permission denied"}
    ]) is False


def test_latest_shadow_outcome_requires_every_bar_to_be_qmt_attested():
    outcome = {"outcome_id": "o1", "execution_feasibility": "UNVERIFIED_RESEARCH"}
    rows = [
        {"quality_status": "QMT_ATTESTED", "data_source": "gj_big_qmt_inner"},
        {"quality_status": "RAW", "data_source": "gj_big_qmt_inner"},
    ]
    assert readiness.evaluate_latest_outcome_attestation(
        outcome, rows, expected_bar_count=2
    ) is False
    rows[1]["quality_status"] = "QMT_ATTESTED"
    assert readiness.evaluate_latest_outcome_attestation(
        outcome, rows, expected_bar_count=2
    ) is True
    assert outcome["execution_feasibility"] == "UNVERIFIED_RESEARCH"


def _artifact(horizon: int, config_hash: str, *, sessions: int = 120):
    marker = {1: "1", 5: "5", 20: "9"}[horizon]
    feature_marker = {1: "2", 5: "6", 20: "0"}[horizon]
    return {
        "schema_version": readiness.CURRENT_HORIZON_ARTIFACT_SCHEMA,
        "model_protocol": readiness.CURRENT_HORIZON_MODEL_PROTOCOL,
        "release_id": "release-a",
        "model_key": f"model-t{horizon}",
        "model_version": "1.0-oos",
        "horizon_days": horizon,
        "prediction_kind": "CALIBRATED_OOS",
        "artifact_hash": marker * 64,
        "feature_protocol_hash": feature_marker * 64,
        "dataset_manifest_hash": str((horizon + 2) % 10) * 64,
        "config_hash": config_hash,
        "order_authority": False,
        "candidate_evaluation_ledger": {
            "schema_version": readiness.CANDIDATE_EVALUATION_LEDGER_SCHEMA,
            "content_sha256": str((horizon + 3) % 10) * 64,
            "row_count": 120,
            "registration_verification_required": True,
        },
        "selection_policy": {
            "selection_policy_hash": (
                readiness.CURRENT_HORIZON_SELECTION_POLICY_HASH
            ),
        },
        "walk_forward": {"distinct_oos_session_count": sessions},
        "oos_evidence": {
            "model_protocol": readiness.CURRENT_HORIZON_MODEL_PROTOCOL,
            "distinct_oos_sessions": sessions,
            "economic_metrics_use_frozen_selection_ledger": True,
            "selection_evidence": {
                "protocol": readiness.CURRENT_HORIZON_SELECTION_PROTOCOL,
                "selection_policy_hash": (
                    readiness.CURRENT_HORIZON_SELECTION_POLICY_HASH
                ),
                "economic_evaluation_scope": (
                    "FULL_UNIVERSE_TOP12_RESEARCH_DIAGNOSTIC"
                ),
                "selected_oos_sample_count": 120,
                "selected_oos_session_count": sessions,
                "deployment_candidate_domain_verified": False,
                "order_authority": False,
            },
            "execution_evidence_scope": "LONG_HISTORY_OOS_RESEARCH_ONLY",
            "label_attestation_required_for_execution": True,
            "executable_verified": False,
            "qmt_attested_label_count": 100,
        },
        "gate": {"status": "PASS", "contract_eligible": True},
    }


def _artifact_config(config_hash: str):
    return {
        "strategy_version": "v-test",
        "multi_horizon_forecasts": {
            "artifact_release_id": "release-a",
            "trainable_models": {
                f"T+{horizon}": {
                    "model_key": f"model-t{horizon}",
                    "model_version": "1.0-oos",
                }
                for horizon in (1, 5, 20)
            },
            "training_policy": {
                "minimum_oos_sessions": {"1": 100, "5": 100, "20": 80}
            },
        },
        "continuous_calibration": {
            "minimum_oos_samples": {"1": 100, "5": 100, "20": 80},
            "minimum_oos_sessions": {"1": 100, "5": 100, "20": 80},
        },
        "config_hash": config_hash,
    }


def _install_fake_loader(monkeypatch):
    module = types.ModuleType("server.trading_v3.horizon_models")
    module.load_horizon_artifact = lambda path: json.loads(
        Path(path).read_text(encoding="utf-8")
    )
    monkeypatch.setitem(sys.modules, module.__name__, module)


def test_artifact_readiness_reports_missing_artifact_separately(
    tmp_path, monkeypatch
):
    _install_fake_loader(monkeypatch)
    monkeypatch.setattr(readiness, "ROOT", tmp_path)
    config_hash = "f" * 64
    base = tmp_path / "artifacts/trading_v3/horizon_models/release-a"
    base.mkdir(parents=True)
    for horizon in (1, 20):
        (base / f"T{horizon}.json").write_text(
            json.dumps(_artifact(horizon, config_hash)), encoding="utf-8"
        )
    result = readiness.collect_artifact_readiness(
        _artifact_config(config_hash), current_config_hash=config_hash
    )
    assert result["ready"] is False
    assert "HORIZON_ARTIFACT_MISSING_T5" in result["reason_codes"]


def test_artifact_readiness_reports_insufficient_oos_sessions_separately(
    tmp_path, monkeypatch
):
    _install_fake_loader(monkeypatch)
    monkeypatch.setattr(readiness, "ROOT", tmp_path)
    config_hash = "f" * 64
    base = tmp_path / "artifacts/trading_v3/horizon_models/release-a"
    base.mkdir(parents=True)
    for horizon in (1, 5, 20):
        sessions = 2 if horizon == 1 else 120
        (base / f"T{horizon}.json").write_text(
            json.dumps(_artifact(horizon, config_hash, sessions=sessions)),
            encoding="utf-8",
        )
    result = readiness.collect_artifact_readiness(
        _artifact_config(config_hash), current_config_hash=config_hash
    )
    assert result["ready"] is False
    assert "HORIZON_OOS_SESSIONS_INSUFFICIENT_T1" in result["reason_codes"]


def test_artifact_readiness_accepts_only_three_pinned_independent_oos_models(
    tmp_path, monkeypatch
):
    _install_fake_loader(monkeypatch)
    monkeypatch.setattr(readiness, "ROOT", tmp_path)
    config_hash = "f" * 64
    base = tmp_path / "artifacts/trading_v3/horizon_models/release-a"
    base.mkdir(parents=True)
    for horizon in (1, 5, 20):
        (base / f"T{horizon}.json").write_text(
            json.dumps(_artifact(horizon, config_hash)), encoding="utf-8"
        )
    result = readiness.collect_artifact_readiness(
        _artifact_config(config_hash), current_config_hash=config_hash
    )
    assert result["shadow_research_ready"] is True
    assert result["long_history_oos_ready"] is True
    assert result["paper_deployment_ready"] is False
    assert result["ready"] is False
    assert "SELECTION_DOMAIN_NOT_DEPLOYMENT_VERIFIED" in result["reason_codes"]


def test_artifact_readiness_reports_v1_as_audit_only(tmp_path, monkeypatch):
    _install_fake_loader(monkeypatch)
    monkeypatch.setattr(readiness, "ROOT", tmp_path)
    config_hash = "f" * 64
    base = tmp_path / "artifacts/trading_v3/horizon_models/release-a"
    base.mkdir(parents=True)
    for horizon in (1, 5, 20):
        artifact = _artifact(horizon, config_hash)
        artifact["schema_version"] = (
            readiness.HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V1
        )
        (base / f"T{horizon}.json").write_text(
            json.dumps(artifact), encoding="utf-8"
        )
    result = readiness.collect_artifact_readiness(
        _artifact_config(config_hash), current_config_hash=config_hash
    )
    assert result["ready"] is False
    assert result["historical_v1_runtime_eligible"] is False
    assert all(
        item["protocol_status"] == "HISTORICAL_AUDIT_ONLY"
        for item in result["artifacts"].values()
    )
