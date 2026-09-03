from __future__ import annotations

import json
from pathlib import Path
import pytest
from sqlalchemy import create_engine, text

from server.common.scheduler_authority import (
    LAYER4_WRITER_TASK_TYPES,
    PRODUCTION_EMBEDDED_SCHEDULER_ENABLED,
    PRODUCTION_SCHEDULER_MODE,
    PRODUCTION_SCHEDULER_SERVICE,
)
from server.common.scheduler_tasks import evaluate_fresh_scheduler_writers
from tools import add_trading_v3_tasks as task_deployment
from tools import trading_v3_fourth_layer_readiness as readiness


ROOT = Path(__file__).resolve().parents[1]


class _Engine:
    def __init__(self) -> None:
        self.disposed = False

    def dispose(self) -> None:
        self.disposed = True


def test_scheduler_authority_is_one_shared_standalone_contract() -> None:
    assert PRODUCTION_SCHEDULER_MODE == "standalone"
    assert PRODUCTION_SCHEDULER_SERVICE == "probiga-scheduler.service"
    assert PRODUCTION_EMBEDDED_SCHEDULER_ENABLED is False
    assert readiness.LAYER4_SCHEDULER_TASK_TYPES == LAYER4_WRITER_TASK_TYPES

    evaluated = readiness.evaluate_scheduler_state(
        [],
        {
            "mode": "standalone",
            "heartbeat_age_seconds": 1,
            "poll_seconds": 60,
        },
    )
    assert evaluated["authority_contract"] == {
        "mode": "standalone",
        "service": "probiga-scheduler.service",
        "embedded_scheduler_enabled": False,
        "standalone_service_active": True,
        "standalone_service_enabled": True,
    }


def test_local_live_qmt_launcher_never_owns_shared_database_scheduler() -> None:
    source = (ROOT / "tools" / "start_local_live_services.ps1").read_text(
        encoding="utf-8"
    )

    assert "run_scheduler_daemon.py" not in source
    assert "scheduler_daemon.out.log" not in source
    assert "scheduler_daemon.err.log" not in source
    assert "run_guojin_qmt_gateway.py" in source
    assert "run_qmt_live_runtime.py" in source
    assert "run_remote_mysql_tunnel.py" in source


def test_readiness_exposes_the_durable_layer4_writer_fence() -> None:
    tasks = []
    for definition in task_deployment.deployment_tasks(
        activate_layer4=False
    ):
        if definition["task_type"] not in LAYER4_WRITER_TASK_TYPES:
            continue
        tasks.append({**definition, "last_run_status": "success"})
    result = readiness.evaluate_scheduler_state(
        tasks,
        {
            "mode": "standalone",
            "heartbeat_age_seconds": 1,
            "poll_seconds": 60,
        },
    )
    assert result["writer_fence_active"] is True
    assert set(result["fenced_task_types"]) == set(LAYER4_WRITER_TASK_TYPES)
    assert "LAYER4_WRITER_FENCE_ACTIVE" in result["reason_codes"]
    assert result["ready"] is False


def test_deploy_engine_fences_layer4_before_starting_standalone() -> None:
    deploy_engine = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    assert (
        "Environment=API_EMBEDDED_SCHEDULER_ENABLED=true"
        not in deploy_engine
    )
    assert deploy_engine.count(
        "Environment=API_EMBEDDED_SCHEDULER_ENABLED=false"
    ) >= 2
    fence = deploy_engine.index("tools/add_trading_v3_tasks.py --fence-only")
    schema_cutover = deploy_engine.index(
        "--phase cutover --writers-fenced", fence
    )
    stage = deploy_engine.index(
        '"$PREPARED_CODE_ROOT/tools/add_trading_v3_tasks.py"',
        schema_cutover,
    )
    assert "--writer-fence" in deploy_engine[stage:stage + 200]
    enable = deploy_engine.index(
        "sudo systemctl enable probiga-scheduler", fence
    )
    restart = deploy_engine.index(
        "sudo systemctl restart probiga-scheduler", enable
    )
    health = deploy_engine.index("http://127.0.0.1/api/health", restart)
    assert fence < schema_cutover < stage < enable < restart < health
    assert (
        "systemctl is-active --quiet probiga-scheduler"
        in deploy_engine[restart:]
    )
    assert (
        "systemctl is-enabled --quiet probiga-scheduler"
        in deploy_engine[restart:]
    )


def test_deploy_engine_blocks_external_writer_before_service_restart() -> None:
    deploy_engine = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    normalized = " ".join(
        line.strip() for line in deploy_engine.splitlines() if line.strip()
    )
    runtime = normalized.index("trap 'rollback \"$?\" \"$LINENO\"' ERR")
    stop_scheduler_marker = normalized.index(
        "CUTOVER_STEP=stop_linux_scheduler_before_writer_quiescence",
        runtime,
    )
    stop_scheduler = normalized.index(
        "sudo systemctl stop probiga-scheduler",
        stop_scheduler_marker,
    )
    writer_quiescence = normalized.index(
        "CUTOVER_STEP=verify_cross_host_writer_quiescence_before_api_stop",
        stop_scheduler,
    )
    guard = normalized.index(
        "--require-no-live-scheduler-writers",
        writer_quiescence,
    )
    stop_api_marker = normalized.index(
        "CUTOVER_STEP=stop_api",
        guard,
    )
    stop_api = normalized.index(
        'sudo systemctl stop "$MAIN_SERVICE"',
        stop_api_marker,
    )
    start_api = normalized.index('sudo systemctl start "$MAIN_SERVICE"', guard)
    start_scheduler = normalized.index(
        "sudo systemctl restart probiga-scheduler",
        start_api,
    )
    pending = normalized.index(
        "persist_deployed_receipt_pending",
        start_scheduler,
    )
    finalized = normalized.index(
        'controlled_guard_finalize_successful_activation "$EXPECTED_SHA"',
        pending,
    )
    deployed = normalized.index(
        'publish_deployed_receipt_pending "$EXPECTED_SHA"',
        finalized,
    )
    journal_removed = normalized.index(
        "activation_snapshot_remove_finalized_before_deploy",
        deployed,
    )

    assert (
        stop_scheduler_marker
        < stop_scheduler
        < writer_quiescence
        < guard
        < stop_api_marker
        < stop_api
        < start_api
        < start_scheduler
        < pending
        < finalized
        < deployed
        < journal_removed
    )
    assert "--writer-drain-timeout-seconds 0" in deploy_engine
    assert 'if [ "$WRITER_FENCE_STATUS" -eq 3 ]; then' in deploy_engine
    assert "EXTERNAL_WRITER_BLOCKED=1" in deploy_engine
    assert 'write_receipt "BLOCKED_EXTERNAL_WRITER"' in deploy_engine
    assert (
        'if [ "$EXTERNAL_WRITER_BLOCKED" -eq 1 ] || '
        "\\\n      [ \"$DATABASE_GUARD_MIGRATION_UNVERIFIED\" -eq 1 ]; then"
        in deploy_engine
    )
    assert "keep probiga stopped after database writer block" in deploy_engine
    assert "keep scheduler stopped after database writer block" in deploy_engine
    assert (
        "probiga-scheduler restarted after database writer block"
        in deploy_engine
    )
    rollback_start = deploy_engine.index("rollback() {")
    main_stop_message = deploy_engine.index(
        'rollback_failure "keep probiga stopped after database writer block"',
        rollback_start,
    )
    main_block_start = deploy_engine.rindex(
        'if [ "$restoration_ready" -eq 1 ] &&',
        rollback_start,
        main_stop_message,
    )
    main_block_end = deploy_engine.index(
        'elif [ "$restoration_ready" -eq 1 ]; then',
        main_stop_message,
    )
    main_block = deploy_engine[main_block_start:main_block_end]
    assert '"$EXTERNAL_WRITER_BLOCKED" -eq 1' in main_block
    assert '"$DATABASE_GUARD_MIGRATION_UNVERIFIED" -eq 1' in main_block
    assert 'sudo systemctl disable "$MAIN_SERVICE"' in main_block
    assert 'sudo systemctl stop "$MAIN_SERVICE"' in main_block

    scheduler_stop_message = deploy_engine.index(
        'rollback_failure "keep scheduler stopped after database writer block"',
        main_block_end,
    )
    scheduler_block_start = deploy_engine.rindex(
        'if [ "$EXTERNAL_WRITER_BLOCKED" -eq 1 ] ||',
        main_block_end,
        scheduler_stop_message,
    )
    scheduler_block_end = deploy_engine.index(
        "    else", scheduler_stop_message
    )
    scheduler_block = deploy_engine[
        scheduler_block_start:scheduler_block_end
    ]
    assert '"$DATABASE_GUARD_MIGRATION_UNVERIFIED" -eq 1' in scheduler_block
    assert "sudo systemctl disable probiga-scheduler" in scheduler_block
    assert "sudo systemctl stop probiga-scheduler" in scheduler_block


def test_fresh_scheduler_writer_evaluation_is_exact_and_fail_closed() -> None:
    fresh = {
        "instance_id": "local-1",
        "mode": "standalone",
        "heartbeat_age_seconds": 120,
        "poll_seconds": 60,
    }
    future = {
        "instance_id": "future-clock",
        "mode": "embedded",
        "heartbeat_age_seconds": -5,
        "poll_seconds": 60,
    }
    stale = {
        "instance_id": "stale-1",
        "mode": "standalone",
        "heartbeat_age_seconds": 121,
        "poll_seconds": 60,
    }

    assert evaluate_fresh_scheduler_writers([fresh, future, stale]) == (
        fresh,
        future,
    )
    with pytest.raises(RuntimeError, match="poll interval must be positive"):
        evaluate_fresh_scheduler_writers([
            {**fresh, "poll_seconds": 0},
        ])
    with pytest.raises(RuntimeError, match="must be integers"):
        evaluate_fresh_scheduler_writers([
            {**fresh, "heartbeat_age_seconds": None},
        ])


def test_default_task_deployment_persists_layer4_writer_fence(
    monkeypatch,
    capsys,
) -> None:
    engine = _Engine()
    observed: list[dict] = []
    monkeypatch.setattr(task_deployment, "load_project_env", lambda: None)
    monkeypatch.setattr(task_deployment, "create_tool_engine", lambda: engine)
    monkeypatch.setattr(
        task_deployment,
        "upsert_scheduler_task",
        lambda _engine, task, **_kwargs: observed.append(dict(task))
        or {"action": "updated"},
    )
    monkeypatch.setattr(
        task_deployment,
        "enforce_layer4_writer_fence_atomically",
        lambda _engine: 2,
    )

    assert task_deployment.main([]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "writer-fence"
    assert payload["writer_fence_active"] is True
    assert engine.disposed is True
    assert {
        task["task_type"]: task["enabled"]
        for task in observed
        if task["task_type"] in LAYER4_WRITER_TASK_TYPES
    } == {task_type: 0 for task_type in LAYER4_WRITER_TASK_TYPES}
    assert all(
        task["enabled"] == 1
        for task in observed
        if task["task_type"] not in LAYER4_WRITER_TASK_TYPES
    )


def test_writer_quiescence_block_keeps_fence_and_skips_task_upserts(
    monkeypatch,
    capsys,
) -> None:
    engine = _Engine()
    observed: list[dict] = []
    live_writer = {
        "instance_id": "external-7",
        "mode": "standalone",
        "host_name": "other-host",
        "heartbeat_age_seconds": 3,
        "poll_seconds": 60,
    }
    monkeypatch.setattr(task_deployment, "load_project_env", lambda: None)
    monkeypatch.setattr(task_deployment, "create_tool_engine", lambda: engine)
    monkeypatch.setattr(
        task_deployment,
        "enforce_layer4_writer_fence_atomically",
        lambda _engine: 2,
    )
    monkeypatch.setattr(
        task_deployment,
        "wait_for_scheduler_writer_quiescence",
        lambda _engine, **_kwargs: (live_writer,),
    )
    monkeypatch.setattr(
        task_deployment,
        "upsert_scheduler_task",
        lambda _engine, task, **_kwargs: observed.append(dict(task)),
    )

    assert task_deployment.main([
        "--writer-fence",
        "--require-no-live-scheduler-writers",
        "--writer-drain-timeout-seconds",
        "0",
    ]) == task_deployment.WRITER_QUIESCENCE_BLOCK_EXIT_CODE
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "blocked"
    assert payload["writer_fence_active"] is True
    assert payload["writer_quiescence"]["ready"] is False
    assert payload["writer_quiescence"]["reason_codes"] == [
        "SCHEDULER_LIVE_WRITERS_REMAIN"
    ]
    assert payload["writer_quiescence"]["live_writers"] == [live_writer]
    assert observed == []
    assert engine.disposed is True


def test_explicit_layer4_activation_fails_before_any_write_when_schema_blocks(
    monkeypatch,
    capsys,
) -> None:
    engine = _Engine()
    observed: list[dict] = []
    monkeypatch.setattr(task_deployment, "load_project_env", lambda: None)
    monkeypatch.setattr(task_deployment, "create_tool_engine", lambda: engine)
    monkeypatch.setattr(
        task_deployment,
        "layer4_activation_preconditions",
        lambda _engine: {
            "ready": False,
            "reason_codes": ["SHADOW_SCHEMA_UNVERIFIED"],
        },
    )
    monkeypatch.setattr(
        task_deployment,
        "upsert_scheduler_task",
        lambda _engine, task, **_kwargs: observed.append(dict(task)),
    )
    monkeypatch.setattr(
        task_deployment,
        "enforce_layer4_writer_fence_atomically",
        lambda _engine: 2,
    )

    assert task_deployment.main(["--activate-layer4"]) == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "blocked"
    assert payload["writer_fence_active"] is True
    assert observed == []
    assert engine.disposed is True


def test_explicit_layer4_activation_enables_writers_after_deep_migration_gate(
    monkeypatch,
    capsys,
) -> None:
    engine = _Engine()
    observed: list[dict] = []
    activation_calls = []
    monkeypatch.setattr(task_deployment, "load_project_env", lambda: None)
    monkeypatch.setattr(task_deployment, "create_tool_engine", lambda: engine)
    monkeypatch.setattr(
        task_deployment,
        "layer4_activation_preconditions",
        lambda _engine: {
            "ready": True,
            "migrations": {
                "shadow": {"ready": True},
                "horizon_protocol_v2": {"ready": True},
                "horizon_candidate_ledger_v3": {"ready": True},
            },
        },
    )
    monkeypatch.setattr(
        task_deployment,
        "upsert_scheduler_task",
        lambda _engine, task, **_kwargs: observed.append(dict(task))
        or {"action": "updated"},
    )
    monkeypatch.setattr(
        task_deployment,
        "enforce_layer4_writer_fence_atomically",
        lambda _engine: 2,
    )
    monkeypatch.setattr(
        task_deployment,
        "activate_layer4_writers_atomically",
        lambda _engine: activation_calls.append(True) or 2,
    )

    assert task_deployment.main(["--activate-layer4"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["writer_fence_active"] is False
    assert payload["layer4_writers_enabled"] is True
    assert all(
        task["enabled"] == 0
        for task in observed
        if task["task_type"] in LAYER4_WRITER_TASK_TYPES
    )
    assert activation_calls == [True]
    assert engine.disposed is True


def test_layer4_writer_activation_is_atomic_and_rejects_duplicate_rows() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text(
            "CREATE TABLE st_scheduled_tasks (task_type TEXT, enabled INT)"
        ))
        connection.execute(
            text(
                "INSERT INTO st_scheduled_tasks (task_type, enabled) VALUES "
                "(:counterfactual, 0), (:continuous, 0)"
            ),
            {
                "counterfactual": LAYER4_WRITER_TASK_TYPES[0],
                "continuous": LAYER4_WRITER_TASK_TYPES[1],
            },
        )
    assert task_deployment.enforce_layer4_writer_fence_atomically(engine) == 2
    assert task_deployment.activate_layer4_writers_atomically(engine) == 2
    with engine.connect() as connection:
        assert connection.execute(text(
            "SELECT COUNT(*) FROM st_scheduled_tasks WHERE enabled=1"
        )).scalar_one() == 2

    with engine.begin() as connection:
        connection.execute(text(
            "UPDATE st_scheduled_tasks SET enabled=0"
        ))
        connection.execute(
            text(
                "INSERT INTO st_scheduled_tasks (task_type, enabled) "
                "VALUES (:counterfactual, 0)"
            ),
            {"counterfactual": LAYER4_WRITER_TASK_TYPES[0]},
        )
    with pytest.raises(RuntimeError, match="cardinality mismatch"):
        task_deployment.activate_layer4_writers_atomically(engine)
    with engine.connect() as connection:
        assert connection.execute(text(
            "SELECT COUNT(*) FROM st_scheduled_tasks WHERE enabled=1"
        )).scalar_one() == 0
    engine.dispose()
