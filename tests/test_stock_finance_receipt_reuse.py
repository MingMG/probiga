from datetime import datetime, timedelta
import json

import pytest
from sqlalchemy import create_engine, text

from server.api import scheduler_runtime as runtime
from server.common import component_release_attestation, daily_delivery_control as control
from server.common import pit_facts
from tests.test_finance_scheduler_manifest import _receipt


PRIOR_BUILD = "b" * 40
CURRENT_BUILD = "a" * 40
CONTRACT = "c" * 40
RUN_UID = "f" * 32


def install_finance_completion(
    engine, monkeypatch, *, target, started, task_id=123, task_name="stock_finance",
):
    completed = started + timedelta(seconds=30, microseconds=500)
    finished = started + timedelta(minutes=1)
    control.privileged_migrate_daily_delivery_schema(engine)
    pit_facts.ensure_pit_fact_schema(engine)
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", CURRENT_BUILD)
    monkeypatch.setattr(runtime, "runtime_contract_build_sha", lambda **_: CONTRACT)
    monkeypatch.setattr(control, "_control_now", lambda: started)

    def component(_connection, build):
        return {
            "linux_build_sha": build,
            "windows_build_sha": CONTRACT,
            "contract_build_sha": CONTRACT,
            "contract_sha256": "d" * 64,
            "scope": "LINUX",
        }

    monkeypatch.setattr(control, "load_component_attestation", component)
    monkeypatch.setattr(component_release_attestation, "load_component_attestation", component)
    control.start_daily_stage_attempt(
        engine, trade_date=target, release_id=CONTRACT, strategy_release_id="a" * 64,
        scheduler_run_uid=RUN_UID, stage_name="stock_finance", lease_owner="linux-prior",
        lease_seconds=300,
    )
    receipt = _receipt(excluded=True)
    seal = receipt["atomic_batch"]
    seal_row = pit_facts.append_source_coverage(
        engine, fact_kind="finance", stock_code=pit_facts.FINANCE_ATOMIC_BATCH_CODE,
        window_start="1900-01-01", window_end=target,
        known_at=completed, received_at=completed, covered_through_at=completed,
        watermark_kind="CAPTURED_AT", watermark_evidence={"batch_root_sha256": seal["batch_root_sha256"]},
        source_rows=[], fact_bindings=[], source=pit_facts.FINANCE_ATOMIC_BATCH_SOURCE,
        batch_id=f"finance-atomic-{seal['batch_root_sha256'][:32]}",
    )
    exclusions = [pit_facts.build_finance_data_exclusion(
        stock_code="000002", target_date=target, observed_at=started,
        source="eastmoney.finance.mainfinadata.direct",
        reason_code="FINANCE_FETCH_FAILED", error_detail="Timeout: source unavailable",
    )]
    receipt.update(as_of=target, data_exclusions=exclusions)
    seal.update(
        as_of_date=target, completed_known_at=completed.isoformat(),
        seal_coverage_id=seal_row.coverage_id, data_exclusions=exclusions,
    )
    with monkeypatch.context() as original:
        original.setenv("PROBIGA_BUILD_COMMIT_SHA", PRIOR_BUILD)
        output = runtime._build_history_validation_evidence(
            {
                "id": task_id, "task_name": task_name, "task_type": "stock_finance",
                "_scheduler_target_trade_date": target,
            },
            run_uid=RUN_UID, machine_output=json.dumps(receipt), status="degraded",
            exit_code=0, started_at=started, validation_message="exact finance coverage verified",
        )
    checkpoint = json.loads(output)
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO st_scheduled_task_history
                (run_uid, task_id, task_name, task_type, run_at, finished_at, status,
                 exit_code, output, build_sha, trigger_source)
            VALUES (:uid, :task_id, :task_name, 'stock_finance', :started, :finished, 'degraded',
                    0, :output, :build, 'scheduled')
        """), {
            "uid": RUN_UID, "task_id": task_id, "task_name": task_name, "started": started,
            "finished": finished, "output": output, "build": PRIOR_BUILD,
        })
        control.finish_daily_stage_attempt(
            connection, scheduler_run_uid=RUN_UID, status="degraded", now=finished,
            input_root_sha256=checkpoint["input_receipt_root_sha256"], checkpoint=checkpoint,
        )
    return checkpoint, finished


@pytest.fixture
def finance_case(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_scheduled_task_history (
                id INTEGER PRIMARY KEY, run_uid TEXT UNIQUE, task_id INTEGER, task_name TEXT,
                task_type TEXT, run_at DATETIME, finished_at DATETIME, status TEXT,
                exit_code INTEGER, output TEXT, build_sha TEXT, trigger_source TEXT
            )
        """))
    target = "2026-09-07"
    started = datetime(2026, 9, 7, 22, 0)
    checkpoint, finished = install_finance_completion(
        engine, monkeypatch, target=target, started=started,
    )
    return engine, target, started, finished, checkpoint


def test_code_only_release_reuses_degraded_finance_without_restarting(finance_case, monkeypatch):
    engine, target, started, finished, checkpoint = finance_case
    monkeypatch.setattr(runtime, "_release_catchup_disabled_for_deferred_database", lambda: False)
    row = {
        "id": 123, "task_type": "stock_finance",
        "_release_history_available": True, "_release_catchup_authorized": True,
        "_release_expected_target_required": True, "_release_expected_target_available": True,
        "_release_expected_target_date": target, "_scheduler_target_trade_date": target,
        "_release_terminal_build_sha": PRIOR_BUILD, "_release_terminal_status": "degraded",
        "_release_terminal_output": json.dumps(checkpoint),
    }
    next_release = finished + timedelta(hours=4)
    assert runtime._release_build_catchup_allowed(row, now=next_release)
    runtime._attach_completed_stock_finance_target(engine, [row], now=next_release)
    assert row["_completed_stock_finance_target"] == target
    assert not runtime._release_build_catchup_allowed(row, now=next_release)
    assert not runtime._release_build_catchup_pending(row)

    # A failed attempt on current code never replaces the original completion.
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO st_scheduled_task_history
                (run_uid, task_id, task_type, build_sha, status, run_at, finished_at, exit_code)
            VALUES ('new-failed', 123, 'stock_finance', :build, 'failed', :now, :now, 1)
        """), {"build": CURRENT_BUILD, "now": next_release})
        observed = runtime._load_completed_stock_finance_history(
            connection, target=target, current_linux_build_sha=CURRENT_BUILD, decision_at=next_release,
        )
    assert observed["run_uid"] == RUN_UID
    assert observed["build_sha"] == PRIOR_BUILD
    assert observed["status"] == "degraded"
    assert runtime._coerce_datetime(observed["run_at"]) == started
    assert runtime._coerce_datetime(observed["finished_at"]) == finished
    assert runtime._history_validation_evidence(observed["output"]) == checkpoint


def test_automatic_analysis_readiness_accepts_finance_only_through_original_completion(finance_case, monkeypatch):
    from tests.test_scheduler_validation_thresholds import _scheduler_evidence

    engine, target, _, finished, _ = finance_case
    now = finished + timedelta(hours=4)
    dependencies = runtime.RELEASE_DATA_CATCHUP_DEPENDENCIES["analysis_fast"]
    release_rows = [{
        "id": 123, "task_type": "stock_finance", "_release_expected_target_date": target,
        "_release_expected_target_available": True, "_release_expected_target_required": True,
    }]
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_scheduled_tasks (
                id INTEGER PRIMARY KEY, task_type TEXT, enabled INTEGER, last_triggered_at DATETIME,
                last_run_status TEXT, last_run_output TEXT
            )
        """))
        connection.execute(text("INSERT INTO st_scheduled_tasks VALUES (123, 'stock_finance', 1, NULL, NULL, NULL)"))
        for index, task_type in enumerate(dependencies, 1):
            if task_type == "stock_finance":
                continue
            uid = f"{index:032x}"
            checkpoint = json.loads(_scheduler_evidence(
                run_uid=uid, task_id=index, task_type=task_type,
                build_sha=CURRENT_BUILD, target=target, replay_output="{}",
            ))
            checkpoint.pop("evidence_sha256")
            checkpoint["release_target_date"] = target
            checkpoint["evidence_sha256"] = runtime.canonical_sha256(checkpoint)
            evidence = json.dumps(checkpoint)
            connection.execute(text("""
                INSERT INTO st_scheduled_tasks VALUES (:id, :task, 1, NULL, NULL, NULL)
            """), {"id": index, "task": task_type})
            connection.execute(text("""
                INSERT INTO st_scheduled_task_history
                    (task_id, run_uid, task_type, build_sha, status, run_at, finished_at, exit_code, output)
                VALUES (:id, :uid, :task, :build, 'success', :started, :finished, 0, :output)
            """), {
                "id": index, "uid": uid, "task": task_type, "build": CURRENT_BUILD,
                "started": finished, "finished": finished, "output": evidence,
            })
            release_rows.append({
                "id": index, "task_type": task_type, "_release_expected_target_date": target,
                "_release_expected_target_available": True, "_release_expected_target_required": True,
            })
    monkeypatch.setattr(
        runtime, "_dependency_component_build_shas",
        lambda *_args, **_kwargs: {"linux": CURRENT_BUILD, "windows": CURRENT_BUILD},
    )
    monkeypatch.setattr(runtime, "load_qmt_daily_market_truth", lambda *_args, **_kwargs: None)
    downstream = {
        "task_type": "analysis_fast",
        "_release_expected_target_date": target, "_scheduler_target_trade_date": target,
    }
    assert runtime._attach_release_catchup_history(engine, release_rows)
    assert runtime._release_catchup_dependencies_ready(
        downstream, release_rows, engine=engine, now=now,
    ) == (True, "ready")
    assert runtime._strategy_pipeline_dependencies_ready(downstream, engine, now=now) == (True, "ready")

    # No generic old-build relaxation was introduced for a different producer.
    with engine.begin() as connection:
        connection.execute(text("""
            UPDATE st_scheduled_task_history SET build_sha=:prior WHERE task_type='qmt_announcement_pit'
        """), {"prior": PRIOR_BUILD})
    assert runtime._strategy_pipeline_dependencies_ready(downstream, engine, now=now) == (
        False, "qmt_announcement_pit:history_identity_mismatch",
    )


@pytest.mark.parametrize("failure", ["future", "other_date", "incompatible", "audit_drift", "seal_absent"])
def test_finance_reuse_requires_original_verified_target_and_seal(finance_case, monkeypatch, failure):
    engine, target, _, finished, _ = finance_case
    decision_at = finished + timedelta(seconds=1)
    with engine.begin() as connection:
        if failure == "future":
            decision_at = finished - timedelta(seconds=1)
        elif failure == "other_date":
            target = "2026-09-08"
        elif failure == "incompatible":
            original = component_release_attestation.load_component_attestation

            def incompatible(conn, build):
                identity = original(conn, build)
                if build == CURRENT_BUILD:
                    identity["contract_sha256"] = "e" * 64
                return identity

            monkeypatch.setattr(component_release_attestation, "load_component_attestation", incompatible)
        elif failure == "audit_drift":
            connection.execute(text("UPDATE st_scheduled_task_history SET output='{}'"))
        elif failure == "seal_absent":
            connection.execute(text("DROP TABLE st_pit_source_coverage"))
            connection.execute(text(pit_facts._sqlite_table_ddl(pit_facts.SOURCE_COVERAGE_TABLE)))
        assert runtime._load_completed_stock_finance_history(
            connection, target=target, current_linux_build_sha=CURRENT_BUILD, decision_at=decision_at,
        ) is None
