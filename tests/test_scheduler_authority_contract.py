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


def test_deploy_workflow_fences_layer4_before_starting_standalone() -> None:
    workflow_source = (ROOT / ".github/workflows/deploy.yml").read_text(
        encoding="utf-8"
    )
    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    workflow = workflow_source + "\n" + deploy_script
    assert "Environment=API_EMBEDDED_SCHEDULER_ENABLED=true" not in workflow
    assert deploy_script.count(
        "Environment=API_EMBEDDED_SCHEDULER_ENABLED=false"
    ) >= 2
    fence = workflow.index("tools/add_trading_v3_tasks.py --writer-fence")
    enable = workflow.index("sudo systemctl enable probiga-scheduler", fence)
    restart = workflow.index("sudo systemctl restart probiga-scheduler", enable)
    health = workflow.index("http://127.0.0.1/api/health", restart)
    assert fence < enable < restart < health
    assert "systemctl is-active --quiet probiga-scheduler" in workflow[restart:]
    assert "systemctl is-enabled --quiet probiga-scheduler" in workflow[restart:]


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
