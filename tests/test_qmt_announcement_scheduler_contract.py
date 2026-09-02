from __future__ import annotations

from datetime import datetime, timedelta
import json
import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

import integrations.qmt.runtime as qmt_runtime
import tools.env_config as env_config
import tools.sync_qmt_announcement_pit as announcement_tool
from tools.check_strategy_governance_health import (
    _qmt_announcement_scheduler_checks,
)

from server.api.scheduler_runtime import (
    WINDOWS_QMT_BRIDGE_TASK_TYPES,
    _should_skip_task_for_host,
    evaluate_strategy_pipeline_dependencies,
)
from server.common.scheduler_args import build_scheduler_task_args
from server.common.scheduler_validation import scheduler_output_status
from server.common.qmt_announcement_pit import (
    ANNOUNCEMENT_FALLBACK_REASON_CODES as CORE_FALLBACK_REASON_CODES,
    QMT_ANNOUNCEMENT_TASK_SCHEMA,
    validate_task_result,
)
from tools.ensure_quality_gate import TASKS
from tools.add_qmt_announcement_task import (
    _require_unique_operation_tasks,
    _require_unique_task,
    _restore_snapshot,
    _verify_snapshot,
    _write_snapshot,
    install,
)
from tools.qmt_announcement_task_contract import (
    ANALYSIS_FAST_CRON,
    ANALYSIS_UPPER_EVIDENCE_CRON,
    MIN_ANALYSIS_GOVERNANCE_GAP_MINUTES,
    QMT_ANNOUNCEMENT_CHECKPOINT_DIR,
    QMT_ANNOUNCEMENT_FALLBACK_EGRESS_CONTRACT,
    QMT_ANNOUNCEMENT_FALLBACK_PROVIDER,
    QMT_ANNOUNCEMENT_FALLBACK_REASON_CODES,
    QMT_ANNOUNCEMENT_FALLBACK_SOURCE,
    QMT_ANNOUNCEMENT_PRIMARY_SOURCE,
    STRATEGY_GOVERNANCE_CRON,
    TASK,
    validate_pipeline_order,
)
from tools.qmt_operations_task_contract import TASKS as QMT_OPERATIONS_TASKS
from tools.sync_qmt_announcement_pit import (
    BigQmtAnnouncementAdapter,
    _announcement_capture_options,
    _announcement_data_adapter,
    _checkpoint_root,
)
from tools.strategy_governance_task_contract import TASK as GOVERNANCE_TASK


def _result(status="COMPLETE"):
    complete = status == "COMPLETE"
    return {
        "schema": QMT_ANNOUNCEMENT_TASK_SCHEMA,
        "status": status,
        "reason_code": (
            "QMT_ANNOUNCEMENT_FULL_MARKET_COMPLETE"
            if complete else "QMT_ANNOUNCEMENT_NO_PERMISSION_OR_QUERY_FAILED"
        ),
        "detail": "",
        "batch_id": "qmt-ann-20260825T182000-contract" if complete else "",
        "batch_root_hash": "a" * 64 if complete else "",
        "catalog_batch_id": "catalog",
        "catalog_manifest_hash": "b" * 64,
        "catalog_member_set_hash": "c" * 64,
        "stock_count": 5500 if complete else 0,
        "coverage_count": 5500 if complete else 0,
        "event_count": 100,
        "empty_stock_count": 5400,
        "fact_cutoff_at": "2026-08-25T18:20:00.000000",
        "decision_at": "2026-08-25T18:25:00.000000",
        "received_at": "2026-08-25T18:25:00.000000",
        "capture_seconds": 300,
        "window_start": "2026-07-26",
        "window_end": "2026-08-25",
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def _dependency(task_type, at, status="success"):
    return {
        "task_type": task_type,
        "enabled": 1,
        "last_triggered_at": at,
        "last_run_status": status,
    }


def test_frozen_cross_host_pipeline_order_is_qmt_then_analysis_then_governance():
    order = validate_pipeline_order(governance_cron=GOVERNANCE_TASK["cron_time"])
    assert order == {
        "qmt_announcement_minutes": 18 * 60 + 20,
        "upper_evidence_minutes": 22 * 60 + 10,
        "analysis_minutes": 22 * 60 + 20,
        "governance_minutes": 22 * 60 + 35,
    }
    assert order["governance_minutes"] - order["analysis_minutes"] >= (
        MIN_ANALYSIS_GOVERNANCE_GAP_MINUTES
    )
    # The collector's 30-minute bound limits its own capture duration; it does
    # not force analysis to start before later finance/notice evidence exists.
    assert order["analysis_minutes"] - order["qmt_announcement_minutes"] > 30
    with pytest.raises(ValueError, match="order is invalid"):
        validate_pipeline_order(upper_evidence_cron="22:21")
    with pytest.raises(ValueError, match="at least 10 minutes"):
        validate_pipeline_order(governance_cron="22:29")


def test_task_is_installed_by_quality_gate_and_owned_only_by_windows_qmt_host():
    installed = [item for item in TASKS if item["task_type"] == TASK["task_type"]]
    assert installed == [TASK]
    assert TASK["task_type"] in WINDOWS_QMT_BRIDGE_TASK_TYPES
    row = {"task_type": TASK["task_type"], "script_path": TASK["script_path"]}
    assert _should_skip_task_for_host(row, platform_name="nt") is False
    assert _should_skip_task_for_host(row, platform_name="posix") is True
    assert build_scheduler_task_args(
        TASK, TASK["script_path"], "2026-08-25"
    ) == [
        "--window-days",
        "30",
        "--overlap-days",
        "3",
        "--batch-size",
        "100",
        "--fallback-provider",
        QMT_ANNOUNCEMENT_FALLBACK_PROVIDER,
        "--checkpoint-dir",
        QMT_ANNOUNCEMENT_CHECKPOINT_DIR,
    ]


def test_qmt_first_cninfo_fallback_egress_identity_is_frozen():
    assert QMT_ANNOUNCEMENT_FALLBACK_EGRESS_CONTRACT == {
        "schema": "probiga.qmt-announcement-fallback-egress.v1",
        "owner": "qmt_windows_edge",
        "primary_source": QMT_ANNOUNCEMENT_PRIMARY_SOURCE,
        "fallback_provider": QMT_ANNOUNCEMENT_FALLBACK_PROVIDER,
        "fallback_source": QMT_ANNOUNCEMENT_FALLBACK_SOURCE,
        "activation": "frozen-primary-unavailability-only",
        "eligible_reason_codes": tuple(
            sorted(QMT_ANNOUNCEMENT_FALLBACK_REASON_CODES)
        ),
    }
    assert QMT_ANNOUNCEMENT_PRIMARY_SOURCE == "qmt.announcement"
    assert QMT_ANNOUNCEMENT_FALLBACK_PROVIDER == "cninfo"
    assert QMT_ANNOUNCEMENT_FALLBACK_SOURCE == "cninfo.announcement"
    assert (
        "QMT_ANNOUNCEMENT_TERMINAL_DEPENDENCY_UNAVAILABLE"
        in QMT_ANNOUNCEMENT_FALLBACK_REASON_CODES
    )
    assert QMT_ANNOUNCEMENT_FALLBACK_REASON_CODES == CORE_FALLBACK_REASON_CODES
    assert "ModuleNotFoundError" not in QMT_ANNOUNCEMENT_FALLBACK_REASON_CODES
    assert "--fallback-provider cninfo" in TASK["script_args"]
    assert "仅在QMT返回冻结的不可用理由后" in TASK["description"]


def test_five_frozen_qmt_operations_tasks_are_installed_and_host_owned():
    installed = {
        item["task_type"]: item
        for item in TASKS
        if item["task_type"] in {
            task["task_type"] for task in QMT_OPERATIONS_TASKS
        }
    }

    assert installed == {
        task["task_type"]: task for task in QMT_OPERATIONS_TASKS
    }
    for task_type in (
        "qmt_local_gap_repair_execute",
        "qmt_local_history_2024",
        "qmt_reference_incremental",
    ):
        assert task_type in WINDOWS_QMT_BRIDGE_TASK_TYPES
        assert _should_skip_task_for_host(
            {"task_type": task_type}, platform_name="posix"
        ) is True
        assert _should_skip_task_for_host(
            {"task_type": task_type}, platform_name="nt"
        ) is False


def test_production_deploy_prepares_state_roots_and_upserts_before_health():
    deploy = (
        Path(__file__).resolve().parents[1] / "deploy" / "production_deploy.sh"
    ).read_text(encoding="utf-8")

    assert (
        "QMT_FULL_MARKET_HISTORY_STATE_ROOT="
        "/var/lib/probiga/qmt-full-market-history"
    ) in deploy
    assert (
        "QMT_LOCAL_GAP_REPAIR_STATE_ROOT="
        "/var/lib/probiga/qmt-local-gap-repair"
    ) in deploy
    assert "prepare_qmt_full_market_history_state_root" in deploy
    assert "prepare_qmt_local_gap_repair_state_root" in deploy
    disabled = deploy.index("CUTOVER_STEP=install_qmt_operations_tasks_disabled")
    normalize = deploy.index(
        "CUTOVER_STEP=normalize_daily_strategy_pipeline_schedule"
    )
    enabled = deploy.index("CUTOVER_STEP=enable_qmt_operations_tasks")
    new_snapshot = deploy.index(
        "CUTOVER_STEP=capture_qmt_announcement_task_after_enable"
    )
    strict_health = deploy.index("CUTOVER_STEP=verify_strategy_governance_before_start")
    assert disabled < normalize < enabled < new_snapshot < strict_health
    assert "--task-type analysis_upper_evidence_prepare" in deploy[
        normalize:enabled
    ]
    assert "--task-type analysis_fast" in deploy[normalize:enabled]
    assert (
        '"$PREPARED_CODE_ROOT/tools/add_qmt_operations_tasks.py" --disabled'
        in deploy[disabled:enabled]
    )
    assert (
        '"script_args": "--window-days 30 --overlap-days 3 --batch-size 100 '
        '--fallback-provider cninfo --checkpoint-dir '
        '/var/lib/probiga/qmt-announcement-checkpoints"'
        in deploy
    )
    assert 'qmt_announcement_source in authoritative_announcement_sources' in deploy
    assert 'qmt_announcement_source_valid' in deploy
    assert 'qmt_announcement_detail.get("primary_source")' in deploy
    assert 'qmt_announcement_detail.get("fallback_reason")' in deploy
    assert (
        '"QMT_ANNOUNCEMENT_TERMINAL_DEPENDENCY_UNAVAILABLE"' in deploy
    )


def test_qmt_task_install_stages_old_schedule_disabled_then_requires_normalized_dag(
    monkeypatch,
):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_scheduled_tasks (
                id INTEGER PRIMARY KEY,
                task_type TEXT NOT NULL,
                script_path TEXT NOT NULL DEFAULT '',
                cron_time TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1
            )
        """))
        for identifier, task_type, cron_time in (
            (1, "analysis_upper_evidence_prepare", "23:40"),
            (2, "analysis_fast", "23:56"),
            (3, "strategy_governance_daily", STRATEGY_GOVERNANCE_CRON),
        ):
            connection.execute(
                text(
                    "INSERT INTO st_scheduled_tasks "
                    "(id, task_type, cron_time) "
                    "VALUES (:id, :task_type, :cron_time)"
                ),
                {
                    "id": identifier,
                    "task_type": task_type,
                    "cron_time": cron_time,
                },
            )
    monkeypatch.setattr(
        "tools.add_qmt_announcement_task.upsert_scheduler_task",
        lambda *_args, **_kwargs: {"action": "stubbed"},
    )

    staged = install(engine, disabled=True)
    assert staged["pipeline_schedule"]["validated"] is False
    with pytest.raises(RuntimeError, match="scheduler contract differs"):
        install(engine)

    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE st_scheduled_tasks SET cron_time=:cron_time "
                "WHERE task_type='analysis_upper_evidence_prepare'"
            ),
            {"cron_time": ANALYSIS_UPPER_EVIDENCE_CRON},
        )
        connection.execute(
            text(
                "UPDATE st_scheduled_tasks SET cron_time=:cron_time "
                "WHERE task_type='analysis_fast'"
            ),
            {"cron_time": ANALYSIS_FAST_CRON},
        )
    installed = install(engine)
    assert installed["pipeline_schedule"]["validated"] is True
    assert installed["pipeline_schedule"]["observed_before_install"] == {
        "analysis_fast": ANALYSIS_FAST_CRON,
        "analysis_upper_evidence_prepare": ANALYSIS_UPPER_EVIDENCE_CRON,
        "strategy_governance_daily": STRATEGY_GOVERNANCE_CRON,
    }


def test_governance_health_reads_and_rejects_real_pipeline_cron_drift():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_scheduled_tasks (
                id INTEGER PRIMARY KEY,
                task_name TEXT NOT NULL DEFAULT '',
                task_type TEXT NOT NULL,
                group_name TEXT NOT NULL DEFAULT '',
                script_path TEXT NOT NULL DEFAULT '',
                script_args TEXT NOT NULL DEFAULT '',
                cron_time TEXT NOT NULL,
                interval_minutes INTEGER NOT NULL DEFAULT 0,
                date_param TEXT NOT NULL DEFAULT '',
                enabled INTEGER NOT NULL DEFAULT 1
            )
        """))
        connection.execute(
            text("""
                INSERT INTO st_scheduled_tasks
                (id, task_name, task_type, group_name, script_path,
                 script_args, cron_time, interval_minutes, date_param, enabled)
                VALUES
                (1, :task_name, :task_type, :group_name, :script_path,
                 :script_args, :cron_time, :interval_minutes, :date_param,
                 :enabled)
            """),
            TASK,
        )
        for identifier, task_type, cron_time in (
            (2, "analysis_upper_evidence_prepare", ANALYSIS_UPPER_EVIDENCE_CRON),
            (3, "analysis_fast", ANALYSIS_FAST_CRON),
            (4, "strategy_governance_daily", STRATEGY_GOVERNANCE_CRON),
        ):
            connection.execute(
                text(
                    "INSERT INTO st_scheduled_tasks "
                    "(id, task_type, cron_time) "
                    "VALUES (:id, :task_type, :cron_time)"
                ),
                {
                    "id": identifier,
                    "task_type": task_type,
                    "cron_time": cron_time,
                },
            )

    def check() -> tuple[bool, dict]:
        observed = {}
        with engine.connect() as connection:
            passed = _qmt_announcement_scheduler_checks(
                connection,
                {"st_scheduled_tasks"},
                lambda name, ok, detail, **_kwargs: observed.update(
                    {name: {"passed": ok, "detail": detail}}
                ),
            )
        return passed, observed

    passed, observed = check()
    assert passed
    assert observed["qmt_announcement_scheduler_task_contract"]["passed"]

    with engine.begin() as connection:
        connection.execute(text(
            "UPDATE st_scheduled_tasks SET cron_time='23:56' "
            "WHERE task_type='analysis_fast'"
        ))
    passed, observed = check()
    assert not passed
    assert not observed["qmt_announcement_scheduler_task_contract"]["passed"]


def test_checkpoint_root_rejects_relative_and_descendant_symlink(
    monkeypatch, tmp_path
):
    monkeypatch.delenv("PROBIGA_DEPLOYMENT_MODE", raising=False)
    with pytest.raises(Exception, match="CHECKPOINT_ROOT_INVALID"):
        _checkpoint_root("relative/checkpoints")

    root = tmp_path / "checkpoints"
    root.mkdir(mode=0o700)
    target = tmp_path / "outside.json"
    target.write_text("{}", encoding="utf-8")
    link = root / "escaped.json"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("symlink creation is unavailable")
    with pytest.raises(Exception, match="CHECKPOINT_ROOT_INVALID"):
        _checkpoint_root(str(root))


def test_production_checkpoint_root_is_frozen_and_must_preexist(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    with pytest.raises(Exception, match="CHECKPOINT_ROOT_INVALID"):
        _checkpoint_root(str(tmp_path / "mutable-other-root"))


def test_windows_default_checkpoint_root_uses_authorized_scheduler_child(
    monkeypatch, tmp_path
):
    if os.name != "nt":
        pytest.skip("Windows ProgramData mapping is Windows-only")
    monkeypatch.delenv("PROBIGA_DEPLOYMENT_MODE", raising=False)
    monkeypatch.delenv("QMT_ANNOUNCEMENT_CHECKPOINT_DIR", raising=False)
    monkeypatch.setenv("PROGRAMDATA", str(tmp_path))

    root = _checkpoint_root(QMT_ANNOUNCEMENT_CHECKPOINT_DIR)

    assert root == (
        tmp_path
        / "ProBigA"
        / "scheduler"
        / "qmt-announcement-checkpoints"
    )
    assert root.is_dir()


def test_read_only_cli_does_not_touch_checkpoint_or_import_xtdata(
    monkeypatch, capsys
):
    payload = {
        **_result(),
        "mode": "validate-existing-complete-batch",
        "trade_date": "2026-08-25",
        "source": "qmt.announcement",
        "funding_eligible": True,
        "calendar_batch_id": "calendar-20260825",
        "calendar_manifest_hash": "d" * 64,
        "database_writes": False,
    }
    observed = {}

    class Engine:
        def dispose(self):
            observed["disposed"] = True

    monkeypatch.setattr(env_config, "load_project_env", lambda: None)
    monkeypatch.setattr(env_config, "create_tool_engine", Engine)
    monkeypatch.setattr(
        announcement_tool,
        "_checkpoint_root",
        lambda *_args, **_kwargs: pytest.fail(
            "read-only validation touched checkpoint state"
        ),
    )
    monkeypatch.setattr(
        qmt_runtime,
        "import_xtdata",
        lambda: pytest.fail("read-only validation imported xtdata"),
    )

    def validate(_engine, **kwargs):
        observed["kwargs"] = kwargs
        return payload

    monkeypatch.setattr(
        announcement_tool,
        "validate_existing_complete_qmt_announcement_batch",
        validate,
    )

    exit_code = announcement_tool.main([
        "--validate-existing-complete-batch",
        "--window-days", "30",
        "--expected-trade-date", "2026-08-25",
    ])

    assert exit_code == 0
    assert observed == {
        "kwargs": {
            "window_days": 30,
            "expected_trade_date": "2026-08-25",
        },
        "disposed": True,
    }
    assert json.loads(capsys.readouterr().out) == payload


def _bigqmt_capabilities():
    payload = {
        "status": "ok",
        "source": "gj_big_qmt_inner",
        "bridge_version": "bigqmt_inner_v2",
        "actions": ["announcement"],
        "strategy_identity_frozen": True,
        "strategy_identity_status": "BOUND",
    }
    for field in announcement_tool._BIGQMT_IDENTITY_FIELDS:
        payload.setdefault(field, f"proof-{field}")
    payload["strategy_identity_frozen"] = True
    payload["strategy_identity_status"] = "BOUND"
    return payload


def test_windows_auto_announcement_adapter_selects_bigqmt(monkeypatch):
    sentinel = object()
    monkeypatch.setattr(
        announcement_tool, "BigQmtAnnouncementAdapter", lambda: sentinel
    )
    assert _announcement_data_adapter("auto", platform_name="nt") is sentinel
    assert _announcement_data_adapter("bigqmt", platform_name="posix") is sentinel


def test_bigqmt_capture_forces_fresh_checkpoint_and_closed_session(monkeypatch):
    monkeypatch.setattr(
        announcement_tool,
        "authoritative_closed_trade_date",
        lambda _engine: "2026-08-28",
    )
    adapter = type("Adapter", (), {"force_fresh_capture": True})()

    assert _announcement_capture_options(
        adapter, engine=object(), no_resume=False
    ) == {
        "resume": False,
        "coverage_target_date": "2026-08-28",
    }


def test_bigqmt_announcement_adapter_preserves_xtdata_full_scope_contract(
    monkeypatch,
):
    capabilities = _bigqmt_capabilities()
    release_proof = {
        field: capabilities[field]
        for field in announcement_tool._BIGQMT_IDENTITY_FIELDS
    }

    class Bridge:
        calls = []

        @staticmethod
        def capabilities(*, timeout):
            Bridge.calls.append(("capabilities", timeout))
            return capabilities

        @staticmethod
        def announcement_capture(codes, **kwargs):
            Bridge.calls.append(("announcement", codes, kwargs))
            capture = {
                **capabilities,
                "action": "announcement",
                "source_method": "ContextInfo.get_market_data_ex_ori",
                "period": "announcement",
                "count": -1,
                "dividend_type": "none",
                "fill_data": False,
                "subscribe": False,
                "download_history": True,
                "requested_start_time": "20260801000000",
                "requested_end_time": "20260828210000",
                "requested_stock_count": 2,
                "requested_stock_set_sha256": (
                    announcement_tool._announcement_stock_set_sha256(codes)
                ),
                "observed_stock_count": 2,
                "observed_stock_set_sha256": (
                    announcement_tool._announcement_stock_set_sha256(codes)
                ),
                "observed_row_count": 0,
                "estimated_uncompressed_bytes": 0,
                "frames": {},
            }
            receipt = {
                field: capture.get(field)
                for field in announcement_tool._ANNOUNCEMENT_RECEIPT_FIELDS
            }
            capture["capture_receipt_sha256"] = __import__("hashlib").sha256(
                json.dumps(
                    receipt,
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode("utf-8")
            ).hexdigest()
            return capture

        @staticmethod
        def announcement_frames(_capture):
            return {"000001.SZ": object(), "600000.SH": object()}

    adapter = BigQmtAnnouncementAdapter(
        bridge=Bridge,
        timeout=23,
        expected_build_sha="a" * 40,
        release_validator=lambda *_args, **_kwargs: release_proof,
    )
    adapter.bind_capture_deadline(
        fact_cutoff_at=datetime.now(announcement_tool.PRODUCTION_TIMEZONE),
        max_capture_delay=timedelta(minutes=30),
    )
    adapter.connect(port=58610, remember_if_success=False)
    for code in ("000001.SZ", "600000.SH"):
        adapter.download_history_data(
            code,
            period="announcement",
            start_time="20260801000000",
            end_time="20260828210000",
        )
    frames = adapter.get_market_data_ex(
        field_list=[],
        stock_list=["000001.SZ", "600000.SH"],
        period="announcement",
        start_time="20260801000000",
        end_time="20260828210000",
        count=-1,
        dividend_type="none",
        fill_data=False,
    )

    assert set(frames) == {"000001.SZ", "600000.SH"}
    assert Bridge.calls == [
        ("capabilities", 23),
        (
            "announcement",
            ["000001.SZ", "600000.SH"],
            {
                "start_date": "20260801000000",
                "end_date": "20260828210000",
                "download_history": True,
                "timeout": 23,
            },
        ),
    ]

    monkeypatch.setattr(
        Bridge,
        "announcement_frames",
        staticmethod(lambda _capture: {"000001.SZ": object()}),
    )
    partial = BigQmtAnnouncementAdapter(
        bridge=Bridge,
        timeout=23,
        expected_build_sha="a" * 40,
        release_validator=lambda *_args, **_kwargs: release_proof,
    )
    partial.bind_capture_deadline(
        fact_cutoff_at=datetime.now(announcement_tool.PRODUCTION_TIMEZONE),
        max_capture_delay=timedelta(minutes=30),
    )
    partial.connect(port=58610, remember_if_success=False)
    for code in ("000001.SZ", "600000.SH"):
        partial.download_history_data(
            code,
            period="announcement",
            start_time="20260801000000",
            end_time="20260828210000",
        )
    with pytest.raises(RuntimeError, match="stock scope differs"):
        partial.get_market_data_ex(
            field_list=[],
            stock_list=["000001.SZ", "600000.SH"],
            period="announcement",
            start_time="20260801000000",
            end_time="20260828210000",
            count=-1,
            dividend_type="none",
            fill_data=False,
        )


def test_analysis_requires_exact_successful_capital_flow_and_terminal_qmt_task():
    now = datetime(2026, 8, 25, 18, 50)
    ready, reason = evaluate_strategy_pipeline_dependencies(
        "analysis_fast", [], now=now
    )
    assert ready is False
    assert reason == "qmt_announcement_pit:missing_or_duplicate"

    ready, reason = evaluate_strategy_pipeline_dependencies(
        "analysis_fast",
        [
            _dependency("qmt_announcement_pit", datetime(2026, 8, 25, 18, 20), "blocked"),
            _dependency("capital_flow_batch_fast", datetime(2026, 8, 25, 15, 20)),
        ],
        now=now,
    )
    assert ready is True
    assert reason == "ready"

    ready, reason = evaluate_strategy_pipeline_dependencies(
        "analysis_fast",
        [
            _dependency("qmt_announcement_pit", datetime(2026, 8, 25, 18, 20)),
            _dependency(
                "capital_flow_batch_fast",
                datetime(2026, 8, 25, 15, 20),
                "blocked",
            ),
        ],
        now=now,
    )
    assert ready is False
    assert reason == "capital_flow_batch_fast:not_success_today"


def test_governance_requires_analysis_to_have_run_after_qmt_terminal():
    now = datetime(2026, 8, 25, 22, 35)
    rows = [
        _dependency("qmt_announcement_pit", datetime(2026, 8, 25, 18, 20)),
        _dependency("capital_flow_batch_fast", datetime(2026, 8, 25, 15, 20)),
        _dependency("analysis_fast", datetime(2026, 8, 25, 18, 50)),
    ]
    assert evaluate_strategy_pipeline_dependencies(
        "strategy_governance_daily", rows, now=now
    ) == (True, "ready")
    rows[2]["last_triggered_at"] = datetime(2026, 8, 25, 18, 10)
    assert evaluate_strategy_pipeline_dependencies(
        "strategy_governance_daily", rows, now=now
    )[0] is False


@pytest.mark.parametrize("status", ("blocked", "failed", "timeout", "stopped"))
def test_governance_requires_successful_analysis_not_only_terminal(status):
    now = datetime(2026, 8, 25, 22, 35)
    rows = [
        _dependency("qmt_announcement_pit", datetime(2026, 8, 25, 18, 20)),
        _dependency("capital_flow_batch_fast", datetime(2026, 8, 25, 15, 20)),
        _dependency(
            "analysis_fast",
            datetime(2026, 8, 25, 22, 20),
            status,
        ),
    ]

    assert evaluate_strategy_pipeline_dependencies(
        "strategy_governance_daily", rows, now=now
    ) == (False, "analysis_fast:not_success_today")


def test_scheduler_maps_machine_complete_and_data_blocked_without_false_success():
    complete = json.dumps(_result(), ensure_ascii=False)
    blocked = json.dumps(_result("DATA_BLOCKED"), ensure_ascii=False)
    assert validate_task_result(json.loads(complete), 0) == "complete"
    assert validate_task_result(json.loads(blocked), 2) == "data_blocked"
    assert scheduler_output_status(TASK, complete, return_code=0) == "success"
    assert scheduler_output_status(TASK, blocked, return_code=2) == "blocked"
    assert scheduler_output_status(
        TASK, complete + "\n" + complete, return_code=0
    ) == "failed"
    assert scheduler_output_status(TASK, complete, return_code=2) == "failed"


def test_scheduler_task_snapshot_restores_exact_predeploy_row(
    monkeypatch, tmp_path
):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_scheduled_tasks (
                id INTEGER PRIMARY KEY,
                task_type TEXT NOT NULL,
                script_path TEXT NOT NULL,
                cron_time TEXT NOT NULL,
                enabled INTEGER NOT NULL
            )
        """))
        connection.execute(
            text("""
                INSERT INTO st_scheduled_tasks
                (id, task_type, script_path, cron_time, enabled)
                VALUES (7, :task_type, :script_path, '18:10', 0)
            """),
            {
                "task_type": TASK["task_type"],
                "script_path": TASK["script_path"],
            },
        )
    snapshot = tmp_path / "qmt-task-before.json"
    _write_snapshot(snapshot, _require_unique_task(engine))
    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE st_scheduled_tasks SET cron_time='18:20', enabled=1 "
                "WHERE id=7"
            )
        )
    with pytest.raises(RuntimeError, match="differs from sealed snapshot"):
        _verify_snapshot(engine, snapshot)
    monkeypatch.setattr(
        "tools.add_qmt_announcement_task.table_columns",
        lambda _engine: {"id", "task_type", "script_path", "cron_time", "enabled"},
    )
    assert _restore_snapshot(engine, snapshot)["action"] == (
        "restored_existing_task"
    )
    assert _verify_snapshot(engine, snapshot) == {
        "verified": True,
        "row_count": 1,
        "operation_row_count": 0,
    }


def test_qmt_cutover_snapshot_atomically_covers_all_five_operations_tasks(
    monkeypatch,
    tmp_path,
):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_scheduled_tasks (
                id INTEGER PRIMARY KEY,
                task_type TEXT NOT NULL,
                script_path TEXT NOT NULL,
                cron_time TEXT NOT NULL,
                enabled INTEGER NOT NULL
            )
        """))
        connection.execute(
            text(
                "INSERT INTO st_scheduled_tasks "
                "(id, task_type, script_path, cron_time, enabled) "
                "VALUES (7, :task_type, :script_path, '18:10', 0)"
            ),
            {
                "task_type": TASK["task_type"],
                "script_path": TASK["script_path"],
            },
        )
        for index, task in enumerate(QMT_OPERATIONS_TASKS, 20):
            connection.execute(
                text(
                    "INSERT INTO st_scheduled_tasks "
                    "(id, task_type, script_path, cron_time, enabled) "
                    "VALUES (:id, :task_type, :script_path, :cron_time, 0)"
                ),
                {
                    "id": index,
                    "task_type": task["task_type"],
                    "script_path": task["script_path"],
                    "cron_time": task["cron_time"],
                },
            )
    snapshot = tmp_path / "all-qmt-tasks-before.json"
    _write_snapshot(
        snapshot,
        _require_unique_task(engine),
        _require_unique_operation_tasks(engine),
    )
    with engine.begin() as connection:
        connection.execute(
            text("UPDATE st_scheduled_tasks SET enabled=1, cron_time='23:59'")
        )
    monkeypatch.setattr(
        "tools.add_qmt_announcement_task.table_columns",
        lambda _engine: {
            "id", "task_type", "script_path", "cron_time", "enabled",
        },
    )

    restored = _restore_snapshot(engine, snapshot)

    assert restored["operation_row_count"] == 5
    assert set(restored["actions"]) == {
        TASK["task_type"],
        *(task["task_type"] for task in QMT_OPERATIONS_TASKS),
    }
    assert _verify_snapshot(engine, snapshot) == {
        "verified": True,
        "row_count": 1,
        "operation_row_count": 5,
    }
