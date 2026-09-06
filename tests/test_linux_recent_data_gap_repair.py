from __future__ import annotations

from datetime import datetime
from contextlib import contextmanager
from decimal import Decimal
import json
import re
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import create_engine, event, text

from server.api import scheduler_runtime
from server.common import scheduler_validation
from server.common.daily_stock_universe import DailyStockUniverse
from tools import repair_linux_recent_data_gaps as repair
from tools import sync_eastmoney_alist_exact as alist
from tools.qmt_host_ownership_contract import LINUX_PROVIDER_TASKS_BY_TYPE


SHANGHAI = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 8, 27, 1, 30, tzinfo=SHANGHAI)
BUILD_SHA = "a" * 40


def _window(*sessions: str) -> repair.AuthorityWindow:
    return repair.AuthorityWindow(
        sessions=tuple(sessions),
        batch_id="qmt_rel_" + "b" * 40,
        manifest_hash="c" * 64,
        source_session_set_hash="d" * 64,
    )


def _proof(partition: repair.PartitionRef) -> dict[str, object]:
    authority = {
        "calendar": "immutable",
        "partition_id": partition.partition_id,
    }
    return {
        "dataset": partition.dataset,
        "trade_date": partition.trade_date,
        "row_count": 1,
        "row_hash": repair._digest([partition.partition_id, "canonical"]),
        "authority": authority,
        "authority_sha256": repair._digest(authority),
    }


class _State:
    def __init__(self, exact: set[str]) -> None:
        self.exact = set(exact)
        self.published: list[str] = []

    def inspect(self, partition: repair.PartitionRef) -> dict[str, object]:
        if partition.partition_id not in self.exact:
            raise repair.LinuxGapRepairBlocked("partition missing")
        return _proof(partition)

    def publish(self, partition: repair.PartitionRef) -> dict[str, object]:
        self.published.append(partition.partition_id)
        self.exact.add(partition.partition_id)
        return {
            "source_schema": "test.exact-publisher.v1",
            "source_status": "PASS",
            "source_receipt_sha256": repair._digest(partition.partition_id),
            "automatic_order_submission": False,
        }


def _run(
    state: _State,
    *,
    datasets=("stock_daily_flow", "market_overview"),
    sessions=("2026-08-25", "2026-08-26"),
    budget=20,
    apply=True,
    publisher=None,
) -> dict[str, object]:
    return repair.repair_recent_partitions(
        expected_build_sha=BUILD_SHA,
        datasets=datasets,
        lookback_sessions=len(sessions),
        max_repairs_per_run=budget,
        apply=apply,
        now=NOW,
        window=_window(*sessions),
        inspect_partition=state.inspect,
        publish_partition=publisher or state.publish,
    )


def test_known_august_gap_window_and_latest_snapshot_plan_are_exact() -> None:
    sessions = (
        "2026-08-20",
        "2026-08-21",
        "2026-08-24",
        "2026-08-25",
        "2026-08-26",
    )
    plan = repair.build_plan(
        _window(*sessions),
        ("stock_daily_flow", "market_overview", "stock_snapshot"),
    )

    assert [item.partition_id for item in plan] == [
        value
        for day in sessions
        for value in (
            f"stock_daily_flow:{day}",
            f"market_overview:{day}",
        )
    ] + ["stock_snapshot:2026-08-26"]


def test_concept_kline_repairs_only_latest_closed_directory_partition() -> None:
    sessions = (
        "2026-08-20",
        "2026-08-21",
        "2026-08-24",
        "2026-08-25",
        "2026-08-26",
    )

    plan = repair.build_plan(_window(*sessions), ("concept_kline",))

    assert [item.partition_id for item in plan] == [
        "concept_kline:2026-08-26"
    ]
    assert repair.CAPABILITY_POLICY["concept_kline"]["mode"] == (
        "LATEST_CLOSED_ONLY_WITH_EXACT_DIRECTORY"
    )


def test_default_release_scope_excludes_unrecoverable_point_in_time_replays() -> None:
    assert "analysis_recommendations" in repair.DATASET_ORDER
    assert "trading_v3_replay" in repair.DATASET_ORDER
    assert "analysis_recommendations" not in repair.DEFAULT_DATASET_ORDER
    assert "trading_v3_replay" not in repair.DEFAULT_DATASET_ORDER
    assert repair.build_plan(
        _window("2026-08-25", "2026-08-26"),
        repair.DEFAULT_DATASET_ORDER,
    ) == [
        repair.PartitionRef(day, dataset)
        for day in ("2026-08-25", "2026-08-26")
        for dataset in (
            "stock_daily_flow",
            "sector_heat",
            "alist_daily",
            "alist_info",
            "market_overview",
        )
    ] + [
        repair.PartitionRef("2026-08-26", "concept_kline"),
        repair.PartitionRef("2026-08-26", "stock_snapshot"),
    ]


def test_full_five_session_receipt_stays_below_scheduler_history_limit() -> None:
    sessions = (
        "2026-08-20",
        "2026-08-21",
        "2026-08-24",
        "2026-08-25",
        "2026-08-26",
    )
    window = _window(*sessions)
    plan = repair.build_plan(window, repair.DATASET_ORDER)
    large_directory = [
        {
            "code": f"BK{index:04d}",
            "name": f"provider-directory-sector-{index:04d}",
        }
        for index in range(300)
    ]

    def inspect(partition: repair.PartitionRef) -> dict[str, object]:
        authority = {
            "partition_id": partition.partition_id,
            "upstream_directory_receipt": large_directory,
        }
        return {
            "dataset": partition.dataset,
            "trade_date": partition.trade_date,
            "row_count": 5_500,
            "row_hash": repair._digest([partition.partition_id, "rows"]),
            "authority": authority,
            "authority_sha256": repair._digest(authority),
        }

    full_proofs = {item.partition_id: inspect(item) for item in plan}
    assert len(plan) == 37
    assert (
        len(repair._canonical_json(full_proofs).encode("utf-8"))
        > repair.SCHEDULER_REPLAY_RECEIPT_LIMIT_BYTES
    )

    result = repair.repair_recent_partitions(
        expected_build_sha=BUILD_SHA,
        datasets=repair.DATASET_ORDER,
        lookback_sessions=len(sessions),
        max_repairs_per_run=20,
        apply=True,
        now=NOW,
        window=window,
        inspect_partition=inspect,
        publish_partition=lambda _partition: pytest.fail(
            "an exact partition must not be republished"
        ),
    )

    proof_hashes = result["exact_partition_proof_hashes"]
    assert "exact_partition_proofs" not in result
    assert set(proof_hashes) == {item.partition_id for item in plan}
    assert all(repair.SHA64.fullmatch(value) for value in proof_hashes.values())
    assert (
        len(repair._canonical_json(result).encode("utf-8"))
        < repair.SCHEDULER_REPLAY_RECEIPT_LIMIT_BYTES
    )
    assert repair.validate_task_result(result, 0) == "complete"


def test_exact_partitions_are_never_republished_and_second_run_is_idempotent() -> None:
    state = _State(
        {
            "stock_daily_flow:2026-08-25",
            "market_overview:2026-08-26",
        }
    )

    first = _run(state)
    assert first["status"] == "COMPLETE"
    assert state.published == [
        "stock_daily_flow:2026-08-26",
        "market_overview:2026-08-25",
    ]
    assert first["repaired_count"] == 2
    assert repair.validate_task_result(first, 0) == "complete"

    state.published.clear()
    second = _run(state)
    assert second["status"] == "COMPLETE"
    assert second["publish_attempt_count"] == 0
    assert state.published == []


def test_default_market_repair_persists_newest_date_before_oldest_gap() -> None:
    state = _State(set())
    persisted: list[str] = []
    persisted_before_oldest_failure: list[str] = []
    sessions = (
        "2026-08-31",
        "2026-09-01",
        "2026-09-02",
        "2026-09-03",
        "2026-09-04",
    )

    def publish(partition: repair.PartitionRef) -> dict[str, object]:
        if partition.partition_id == "sector_heat:2026-08-31":
            persisted_before_oldest_failure.extend(persisted)
            raise repair.LinuxGapRepairBlocked("oldest sector provider stalled")
        return state.publish(partition)

    result = repair.repair_recent_partitions(
        expected_build_sha=BUILD_SHA,
        datasets=("sector_heat", "market_overview"),
        lookback_sessions=5,
        max_repairs_per_run=20,
        apply=True,
        now=NOW,
        window=_window(*sessions),
        inspect_partition=state.inspect,
        publish_partition=publish,
        persist_repaired_proof=lambda partition, _proof: persisted.append(
            partition.partition_id
        ),
    )

    expected_recent = [
        f"{dataset}:{trade_date}"
        for trade_date in reversed(sessions[1:])
        for dataset in ("sector_heat", "market_overview")
    ]
    assert result["plan_partition_count"] == 10
    assert result["remaining_partition_ids"] == ["sector_heat:2026-08-31"]
    assert persisted_before_oldest_failure == expected_recent
    assert persisted[: len(expected_recent)] == expected_recent


def test_explicit_replay_keeps_strict_oldest_to_newest_dependency_order() -> None:
    state = _State(set())

    result = _run(
        state,
        datasets=("trading_v3_replay",),
        sessions=("2026-08-31", "2026-09-01"),
    )

    assert result["status"] == "COMPLETE"
    assert state.published == [
        f"{dataset}:{trade_date}"
        for trade_date in ("2026-08-31", "2026-09-01")
        for dataset in (
            "stock_daily_flow",
            "sector_heat",
            "analysis_recommendations",
            "trading_v3_replay",
        )
    ]


def test_latest_materialization_stays_after_every_historical_partition() -> None:
    state = _State(set())

    result = _run(
        state,
        datasets=("market_overview", "stock_snapshot"),
        sessions=("2026-08-31", "2026-09-01"),
    )

    assert result["status"] == "COMPLETE"
    assert state.published == [
        "stock_daily_flow:2026-09-01",
        "market_overview:2026-09-01",
        "stock_daily_flow:2026-08-31",
        "market_overview:2026-08-31",
        "stock_snapshot:2026-09-01",
    ]


def test_dependency_order_prevents_analysis_and_v3_from_running_on_missing_inputs() -> None:
    datasets = (
        "stock_daily_flow",
        "sector_heat",
        "analysis_recommendations",
        "trading_v3_replay",
    )
    state = _State(set())

    def publish(partition: repair.PartitionRef):
        if partition.dataset == "stock_daily_flow":
            raise repair.LinuxGapRepairBlocked("native minute flow pending")
        return state.publish(partition)

    result = _run(
        state,
        datasets=datasets,
        sessions=("2026-08-26",),
        publisher=publish,
    )

    assert state.published == ["sector_heat:2026-08-26"]
    assert [item["status"] for item in result["attempts"]] == [
        "DATA_BLOCKED",
        "REPAIRED",
        "DEPENDENCY_BLOCKED",
        "DEPENDENCY_BLOCKED",
    ]
    assert result["remaining_partition_ids"] == [
        "analysis_recommendations:2026-08-26",
        "stock_daily_flow:2026-08-26",
        "trading_v3_replay:2026-08-26",
    ]
    assert result["retryable"] is True
    assert repair.validate_task_result(result, 2) == "blocked"


def test_pit_terminal_block_is_not_misrepresented_as_retryable() -> None:
    state = _State(
        {
            "stock_daily_flow:2026-08-26",
            "sector_heat:2026-08-26",
        }
    )

    def blocked(_partition: repair.PartitionRef):
        raise repair.LinuxGapRepairBlocked(
            "official historical QMT announcement PIT unavailable",
            retryable=False,
        )

    result = _run(
        state,
        datasets=("analysis_recommendations",),
        sessions=("2026-08-26",),
        publisher=blocked,
    )

    assert result["status"] == "DATA_BLOCKED"
    assert result["retryable"] is False
    assert result["blocked_reason"] == "historical_reconstruction_not_provable"
    assert repair.scheduler_output_status(
        repair._canonical_json(result), return_code=2
    ) == "blocked"


def test_budget_is_bounded_and_next_run_resumes_only_remaining_partitions() -> None:
    state = _State(set())
    first = _run(
        state,
        datasets=("stock_daily_flow", "market_overview"),
        sessions=("2026-08-26",),
        budget=1,
    )

    assert first["status"] == "DATA_BLOCKED"
    assert first["publish_attempt_count"] == 1
    assert first["blocked_reason"] == "repair_budget_exhausted"
    assert state.published == ["stock_daily_flow:2026-08-26"]

    state.published.clear()
    second = _run(
        state,
        datasets=("stock_daily_flow", "market_overview"),
        sessions=("2026-08-26",),
        budget=1,
    )
    assert second["status"] == "COMPLETE"
    assert state.published == ["market_overview:2026-08-26"]


def test_dry_run_is_read_only_and_reports_every_gap() -> None:
    state = _State(set())

    def forbidden(_partition):
        raise AssertionError("dry-run called publisher")

    result = _run(
        state,
        sessions=("2026-08-26",),
        apply=False,
        publisher=forbidden,
    )

    assert result["status"] == "DATA_BLOCKED"
    assert result["blocked_reason"] == "dry_run_missing_partitions"
    assert result["publish_attempt_count"] == 0
    assert state.exact == set()


def test_receipt_tampering_duplicate_machine_output_and_wrong_exit_are_rejected() -> None:
    state = _State(
        {
            "stock_daily_flow:2026-08-26",
            "market_overview:2026-08-26",
        }
    )
    result = _run(state, sessions=("2026-08-26",))
    tampered = dict(result)
    tampered["exact_after_count"] = 1

    with pytest.raises(ValueError, match="hash differs"):
        repair.validate_task_result(tampered, 0)
    with pytest.raises(ValueError, match="COMPLETE"):
        repair.validate_task_result(result, 2)

    output = "provider noise\n" + repair._canonical_json(result) + "\n"
    assert repair.parse_result(output) == result
    assert repair.scheduler_output_status(output, return_code=0) == "success"
    with pytest.raises(ValueError, match="exactly one"):
        repair.parse_result(output + repair._canonical_json(result))


def test_retryable_precondition_failure_is_a_valid_failed_scheduler_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PROBIGA_SCHEDULER_BUILD_SHA", raising=False)
    monkeypatch.delenv("PROBIGA_BUILD_COMMIT_SHA", raising=False)
    result = repair._failure_result(
        datasets=("stock_daily_flow",),
        lookback_sessions=5,
        max_repairs_per_run=20,
        apply=True,
        build_sha=BUILD_SHA,
        started_at=datetime.now(SHANGHAI),
        error=RuntimeError("provider unavailable"),
    )

    assert result["remaining_count"] == 0
    assert result["retryable"] is True
    assert repair.validate_task_result(result, 2) == "blocked"
    assert repair.scheduler_output_status(
        repair._canonical_json(result), return_code=2
    ) == "failed"


def test_terminal_precondition_failure_is_strictly_validated_and_blocked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PROBIGA_SCHEDULER_BUILD_SHA", raising=False)
    monkeypatch.delenv("PROBIGA_BUILD_COMMIT_SHA", raising=False)
    result = repair._failure_result(
        datasets=("analysis_recommendations",),
        lookback_sessions=5,
        max_repairs_per_run=20,
        apply=False,
        build_sha="",
        started_at=datetime.now(SHANGHAI),
        error=repair.LinuxGapRepairBlocked(
            "executor ownership differs",
            retryable=False,
        ),
    )

    assert result["build_sha"] is None
    assert result["retryable"] is False
    assert repair.validate_task_result(result, 2) == "blocked"
    assert repair.scheduler_output_status(
        repair._canonical_json(result), return_code=2
    ) == "blocked"

    malformed = dict(result)
    malformed.pop("result_sha256")
    malformed["remaining_count"] = 1
    malformed = repair._signed(malformed)
    with pytest.raises(ValueError, match="precondition result contract differs"):
        repair.validate_task_result(malformed, 2)


def test_capability_policy_forbids_observation_time_historical_relabelling() -> None:
    assert repair.CAPABILITY_POLICY["stock_daily_flow"]["mode"] == "EXACT_HISTORICAL"
    assert repair.CAPABILITY_POLICY["sector_heat"]["mode"] == "EXACT_HISTORICAL"
    assert repair.CAPABILITY_POLICY["alist_daily"]["mode"] == "EXACT_HISTORICAL"
    assert repair.CAPABILITY_POLICY["analysis_recommendations"]["mode"] == (
        "CANONICAL_ANALYSIS_PUBLISHER_ONLY"
    )
    assert repair.CAPABILITY_POLICY["analysis_recommendations"]["safe"] is False
    for dataset in (
        "concept_current",
        "concept_minute",
        "hot_ths",
        "news",
        "finance",
        "dividend",
        "sim_signal_prepare",
    ):
        assert repair.CAPABILITY_POLICY[dataset]["safe"] is False


def test_native_minute_close_to_daily_flow_requires_exact_identity_and_accounting() -> None:
    rows = [
        {
            "stock_code": "000001",
            "trade_time": "2026-08-26 15:00:00",
            "main_net_inflow": Decimal("30"),
            "max_net_inflow": Decimal("10"),
            "lg_net_inflow": Decimal("20"),
            "mid_net_inflow": Decimal("-15"),
            "sm_net_inflow": Decimal("-15"),
            "data_source": "gj_qmt_transactioncount1m",
            "quality_status": "QMT_NATIVE_EXACT",
            "permission_status": "SUPPORTED",
        },
        {
            "stock_code": "000002",
            "trade_time": "2026-08-26 15:00:00",
            "main_net_inflow": Decimal("-12"),
            "max_net_inflow": Decimal("-7"),
            "lg_net_inflow": Decimal("-5"),
            "mid_net_inflow": Decimal("8"),
            "sm_net_inflow": Decimal("4"),
            "data_source": "gj_qmt_transactioncount1m",
            "quality_status": "QMT_NATIVE_EXACT",
            "permission_status": "SUPPORTED",
        },
    ]

    result = repair._daily_flow_rows_from_minute_close(
        rows,
        trade_date="2026-08-26",
        traded_codes=("000001", "000002"),
        etl_sync_at=NOW,
    )
    assert [row["stock_code"] for row in result] == ["000001", "000002"]
    assert {row["data_source"] for row in result} == {
        "gj_qmt_transactioncount1m_close"
    }

    bad = [dict(rows[0], main_net_inflow=999), rows[1]]
    with pytest.raises(repair.LinuxGapRepairBlocked, match="accounting differs"):
        repair._daily_flow_rows_from_minute_close(
            bad,
            trade_date="2026-08-26",
            traded_codes=("000001", "000002"),
            etl_sync_at=NOW,
        )

    with pytest.raises(repair.LinuxGapRepairBlocked, match="close set differs"):
        repair._daily_flow_rows_from_minute_close(
            rows[:1],
            trade_date="2026-08-26",
            traded_codes=("000001", "000002"),
            etl_sync_at=NOW,
        )

    unsupported = [dict(rows[0], permission_status="UNSUPPORTED"), rows[1]]
    with pytest.raises(repair.LinuxGapRepairBlocked, match="provenance"):
        repair._daily_flow_rows_from_minute_close(
            unsupported,
            trade_date="2026-08-26",
            traded_codes=("000001", "000002"),
            etl_sync_at=NOW,
        )


def test_executor_is_strictly_linux_provider_owned() -> None:
    repair.validate_executor(
        platform_name="posix",
        executor_role=repair.EXECUTOR_OWNER,
        task_type=repair.TASK_TYPE,
    )
    with pytest.raises(repair.LinuxGapRepairBlocked):
        repair.validate_executor(
            platform_name="nt",
            executor_role=repair.EXECUTOR_OWNER,
            task_type=repair.TASK_TYPE,
        )
    with pytest.raises(repair.LinuxGapRepairBlocked):
        repair.validate_executor(
            platform_name="posix",
            executor_role="qmt_windows_edge",
            task_type=repair.TASK_TYPE,
        )


def test_formal_task_is_linux_owned_long_running_and_retryable_all_day() -> None:
    task = dict(LINUX_PROVIDER_TASKS_BY_TYPE[repair.TASK_TYPE])

    assert task["script_path"] == "tools/repair_linux_recent_data_gaps.py"
    assert "--lookback-sessions 5" in task["script_args"]
    assert "--max-repairs-per-run 20" in task["script_args"]
    assert scheduler_runtime.scheduler_task_host_owner(task) == "linux_standalone"
    assert repair.TASK_TYPE in scheduler_runtime.CRITICAL_CRON_CATCHUP_TASK_TYPES
    assert (
        scheduler_runtime.CRITICAL_CRON_CATCHUP_WINDOWS_SECONDS[repair.TASK_TYPE]
        == 24 * 60 * 60
    )
    assert repair.TASK_TYPE in scheduler_runtime.LONG_RUNNING_TASK_TYPES
    assert scheduler_runtime._task_timeout_minutes(task) == 20
    assert repair.TASK_TYPE not in scheduler_runtime.NON_TRADING_DAY_SKIP_TYPES


def test_scheduler_replays_single_build_bound_receipt_and_postvalidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _State(
        {
            "stock_daily_flow:2026-08-25",
            "market_overview:2026-08-25",
            "stock_daily_flow:2026-08-26",
            "market_overview:2026-08-26",
        }
    )
    result = _run(state)
    output = repair._canonical_json(result)
    task = dict(LINUX_PROVIDER_TASKS_BY_TYPE[repair.TASK_TYPE])
    started_at = datetime.fromisoformat(str(result["started_at"])).replace(
        tzinfo=None
    )
    finished_at = datetime.fromisoformat(str(result["finished_at"])).replace(
        tzinfo=None
    )
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", BUILD_SHA)

    assert scheduler_validation.scheduler_output_status(
        task,
        output,
        return_code=0,
    ) == "success"
    assert scheduler_validation.scheduler_output_status(
        task,
        output + "\n" + output,
        return_code=0,
    ) == "failed"

    monkeypatch.setattr(
        repair,
        "validate_persisted_result",
        lambda _engine, _payload, **_kwargs: {
            "sessions": ["2026-08-25", "2026-08-26"],
            "partition_count": 4,
        },
    )
    validation = scheduler_validation.validate_scheduler_task_result(
        task,
        engine=object(),
        started_at=started_at,
        now=finished_at,
        output=output,
    )
    assert validation.checked is True
    assert validation.ok is True
    assert "partitions=4" in validation.message


def test_proof_ledger_is_atomic_hash_bound_and_detects_tampering(tmp_path) -> None:
    path = tmp_path / "repair-ledger.json"
    ledger = repair.ProofLedger(path)
    partition = repair.PartitionRef("2026-08-26", "concept_kline")
    proof = _proof(partition)

    assert ledger.load() == {}
    ledger.record(partition, proof, now=NOW)
    assert repair.ProofLedger(path).load() == {partition.partition_id: proof}

    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["entries"][partition.partition_id]["row_count"] = 2
    path.write_text(json.dumps(payload), encoding="utf-8")
    if repair.os.name != "nt":
        path.chmod(0o600)
    with pytest.raises(repair.LinuxGapRepairBlocked, match="hash/counters differ"):
        repair.ProofLedger(path).load()


def _authoritative_empty_alist_receipt(*, dataset: str = "daily") -> dict:
    daily = alist.ReportEvidence(
        report=alist.DAILY_REPORT,
        trade_date="2026-08-26",
        rows=(),
        declared_count=0,
        declared_pages=0,
        fetched_pages=1,
        authoritative_empty=True,
        response_hash="1" * 64,
    )
    details = tuple(
        alist.ReportEvidence(
            report=report,
            trade_date="2026-08-26",
            rows=(),
            declared_count=0,
            declared_pages=0,
            fetched_pages=1,
            authoritative_empty=True,
            response_hash=str(index) * 64,
        )
        for index, report in enumerate(alist.DETAIL_REPORTS, start=2)
    )
    rows: list[dict] = []
    return alist._signed(
        {
            "schema": alist.RESULT_SCHEMA,
            "status": "PASS",
            "dataset": dataset,
            "task_type": alist.TASK_TYPES[dataset],
            "executor_owner": alist.EXECUTOR_OWNER,
            "provider": alist.PROVIDER_ID,
            "trade_date": "2026-08-26",
            "build_sha": BUILD_SHA,
            "started_at": "2026-08-26T17:40:00+08:00",
            "finished_at": "2026-08-26T17:40:00+08:00",
            "catalog": {
                "batch_id": "catalog",
                "manifest_hash": "4" * 64,
                "member_set_hash": "5" * 64,
                "captured_at": "2026-08-26 15:30:00",
                "history_complete_from": "2026-01-01",
                "eligible_code_count": 1,
                "eligible_code_set_hash": alist.code_set_hash(["000001"]),
            },
            "collection": alist._source_receipt(
                daily_report=daily,
                daily_rows=rows,
                detail_reports=details if dataset == "info" else (),
                detail_rows=rows,
            ),
            "database": alist.database_proof(rows, dataset=dataset),
        }
    )


def _install_empty_alist_replay(monkeypatch: pytest.MonkeyPatch) -> None:
    catalog = type(
        "Catalog",
        (),
        {
            "batch_id": "catalog",
            "manifest_hash": "4" * 64,
            "member_set_hash": "5" * 64,
            "captured_at": "2026-08-26 15:30:00",
            "history_complete_from": "2026-01-01",
        },
    )()
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", BUILD_SHA)
    monkeypatch.delenv("PROBIGA_SCHEDULER_BUILD_SHA", raising=False)
    monkeypatch.setattr(alist, "_git_head", lambda: BUILD_SHA)
    monkeypatch.setattr(alist, "validate_runtime_schema", lambda _engine: {})
    monkeypatch.setattr(
        alist,
        "load_target_stock_catalog",
        lambda *_args, **_kwargs: (catalog, ["000001"]),
    )
    monkeypatch.setattr(alist, "_read_partition", lambda *_args, **_kwargs: [])


@pytest.mark.parametrize(
    ("partition_dataset", "receipt_dataset"),
    (("alist_daily", "daily"), ("alist_info", "info")),
)
def test_authoritative_empty_alist_receipt_converges_and_replays_from_ledger(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    partition_dataset: str,
    receipt_dataset: str,
) -> None:
    _install_empty_alist_replay(monkeypatch)
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    partition = repair.PartitionRef("2026-08-26", partition_dataset)
    receipts = {
        name: _authoritative_empty_alist_receipt(dataset=name)
        for name in ("daily", "info")
    }
    receipt = receipts[receipt_dataset]
    assert alist.validate_task_result(receipt, 0) == "complete"
    ledger = repair.ProofLedger(tmp_path / "repair-ledger.json")
    inspector = repair.ProductionPartitionInspector(
        engine,
        engine,
        decision_time=NOW,
        expected_build_sha=BUILD_SHA,
        prior_proofs=ledger.load(),
    )
    publisher = repair.ProductionPartitionPublisher(
        engine,
        engine,
        object(),
        expected_build_sha=BUILD_SHA,
        now=NOW,
        alist_receipt_sink=inspector.record_alist_receipt,
    )
    monkeypatch.setattr(
        alist,
        "run_sync",
        lambda *_args, **kwargs: receipts[str(kwargs["dataset"])],
    )

    result = repair.repair_recent_partitions(
        expected_build_sha=BUILD_SHA,
        datasets=(partition_dataset,),
        lookback_sessions=1,
        max_repairs_per_run=2,
        apply=True,
        now=NOW,
        window=_window("2026-08-26"),
        inspect_partition=inspector,
        publish_partition=publisher,
        persist_repaired_proof=lambda item, proof: ledger.record(
            item, proof, now=NOW
        ),
    )

    assert result["status"] == "COMPLETE"
    assert result["repaired_count"] == (1 if receipt_dataset == "daily" else 2)
    stored = repair.ProofLedger(ledger.path).load()
    proof = stored[partition.partition_id]
    assert proof["authority"]["source_receipt_sha256"] == receipt["receipt_id"]
    assert proof["authority"]["authoritative_empty_receipt"] == receipt
    assert repair._validate_partition_proof(partition, proof) == proof

    replay = repair.ProductionPartitionInspector(
        engine,
        engine,
        decision_time=NOW,
        expected_build_sha=BUILD_SHA,
        prior_proofs=stored,
    )
    second = repair.repair_recent_partitions(
        expected_build_sha=BUILD_SHA,
        datasets=(partition_dataset,),
        lookback_sessions=1,
        max_repairs_per_run=2,
        apply=True,
        now=NOW,
        window=_window("2026-08-26"),
        inspect_partition=replay,
        publish_partition=lambda _partition: pytest.fail(
            "a persisted authoritative-empty receipt must avoid republishing"
        ),
    )
    assert second["status"] == "COMPLETE"
    assert second["candidate_before_count"] == 0


def test_empty_alist_receipt_uses_validation_time_after_slow_collection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_empty_alist_replay(monkeypatch)
    receipt = _authoritative_empty_alist_receipt()
    partition = repair.PartitionRef("2026-08-26", "alist_daily")
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    inspector = repair.ProductionPartitionInspector(
        engine,
        engine,
        decision_time=datetime(2026, 8, 26, 17, 30, tzinfo=SHANGHAI),
        expected_build_sha=BUILD_SHA,
        prior_proofs={},
    )
    inspector.record_alist_receipt(partition, receipt)

    proof = inspector(partition)

    assert proof["authority"]["source_receipt_sha256"] == receipt["receipt_id"]


@pytest.mark.parametrize("partition_dataset", ("alist_daily", "alist_info"))
def test_empty_alist_without_exact_receipt_remains_blocked(
    monkeypatch: pytest.MonkeyPatch,
    partition_dataset: str,
) -> None:
    _install_empty_alist_replay(monkeypatch)
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    inspector = repair.ProductionPartitionInspector(
        engine,
        engine,
        decision_time=NOW,
        expected_build_sha=BUILD_SHA,
        prior_proofs={},
    )
    with pytest.raises(repair.LinuxGapRepairBlocked, match="authoritative-empty"):
        inspector(repair.PartitionRef("2026-08-26", partition_dataset))


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("dataset", "info"),
        ("trade_date", "2026-08-25"),
        ("build_sha", "b" * 40),
        ("provider", "untrusted"),
    ),
)
def test_empty_alist_rejects_mismatched_signed_receipt(
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: str,
) -> None:
    _install_empty_alist_replay(monkeypatch)
    receipt = _authoritative_empty_alist_receipt()
    receipt.pop("receipt_id")
    receipt[field] = value
    if field == "dataset":
        receipt["task_type"] = alist.TASK_TYPES[value]
        receipt["collection"] = _authoritative_empty_alist_receipt(
            dataset=value
        )["collection"]
        receipt["database"] = alist.database_proof([], dataset=value)
    receipt = alist._signed(receipt)
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    partition = repair.PartitionRef("2026-08-26", "alist_daily")
    inspector = repair.ProductionPartitionInspector(
        engine,
        engine,
        decision_time=NOW,
        expected_build_sha=BUILD_SHA,
        prior_proofs={},
    )
    inspector.record_alist_receipt(partition, receipt)
    with pytest.raises(repair.LinuxGapRepairBlocked, match="authoritative-empty"):
        inspector(partition)


def test_nonempty_alist_proof_hashes_its_complete_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    monkeypatch.setattr(alist, "_read_partition", lambda *_args, **_kwargs: [{}])
    database_proof = {
        "row_count": 1,
        "row_hash": "6" * 64,
        "code_count": 1,
        "code_set_hash": "7" * 64,
        "authoritative_empty": False,
    }
    monkeypatch.setattr(
        alist,
        "partition_proof",
        lambda *_args, **_kwargs: dict(database_proof),
    )
    partition = repair.PartitionRef("2026-08-26", "alist_daily")
    inspector = repair.ProductionPartitionInspector(
        engine,
        engine,
        decision_time=NOW,
        expected_build_sha=BUILD_SHA,
    )
    proof = inspector(partition)
    assert proof["authority"]["database_proof"] == database_proof
    assert proof["authority_sha256"] == repair._digest(proof["authority"])
    assert repair._validate_partition_proof(partition, proof) == proof


def test_complete_receipt_is_replayed_against_persisted_partition_hashes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _State(
        {
            "stock_daily_flow:2026-08-25",
            "market_overview:2026-08-25",
            "stock_daily_flow:2026-08-26",
            "market_overview:2026-08-26",
        }
    )
    result = _run(state)
    checked_at = datetime.fromisoformat(str(result["finished_at"]))
    monkeypatch.delenv("PROBIGA_SCHEDULER_BUILD_SHA", raising=False)
    monkeypatch.delenv("PROBIGA_BUILD_COMMIT_SHA", raising=False)

    proof = repair.validate_persisted_result(
        object(),
        result,
        now=checked_at,
        window_loader=lambda _engine, **_kwargs: _window(
            "2026-08-25", "2026-08-26"
        ),
        inspect_partition=state.inspect,
    )

    assert proof["status"] == "COMPLETE"
    assert proof["partition_count"] == 4
    assert proof["exact_partition_root_sha256"] == result[
        "exact_partition_root_sha256"
    ]

    state.exact.remove("market_overview:2026-08-26")
    with pytest.raises(
        repair.LinuxGapRepairBlocked,
        match="persisted window differs",
    ):
        repair.validate_persisted_result(
            object(),
            result,
            now=checked_at,
            window_loader=lambda _engine, **_kwargs: _window(
                "2026-08-25", "2026-08-26"
            ),
            inspect_partition=state.inspect,
        )


def test_trading_v3_publisher_is_always_replay_only(monkeypatch: pytest.MonkeyPatch) -> None:
    observed: dict[str, object] = {}

    def fake_run(_engine, **kwargs):
        observed.update(kwargs)
        return {
            "schema": "probiga.trading-v3-decision-result.v1",
            "status": "ok",
            "run_status": "COMPLETED",
            "actionable_status": "REPLAY_ONLY",
            "paper_order_count": 0,
            "real_trading_enabled": False,
        }

    import server.trading_v3.decision_worker as worker

    monkeypatch.setattr(worker, "run_daily_decision_v3", fake_run)
    publisher = repair.ProductionPartitionPublisher(
        object(),
        object(),
        object(),
        expected_build_sha=BUILD_SHA,
        now=NOW,
    )
    result = publisher(repair.PartitionRef("2026-08-26", "trading_v3_replay"))

    assert observed["execution_enabled"] is False
    assert observed["as_of"].isoformat() == "2026-08-26"
    assert result["automatic_order_submission"] is False
    assert len(result["source_receipt_sha256"]) == 64


def _historical_flow_fixture(monkeypatch, tmp_path, *, rows=(), missing_bars=False):
    from tools import backfill_screener_history_inputs as backfill

    engine = create_engine("sqlite:///:memory:")

    # Execute the production writer against a real transactional DB while
    # translating only MySQL's upsert syntax, not its filtering or readback.
    @event.listens_for(engine, "before_cursor_execute", retval=True)
    def sqlite_upsert(_connection, _cursor, statement, parameters, _context, _many):
        if "ON DUPLICATE KEY UPDATE" in statement:
            statement = statement.replace(
                "ON DUPLICATE KEY UPDATE", "ON CONFLICT(stock_code,trade_date) DO UPDATE SET",
            )
            statement = re.sub(r"VALUES\((\w+)\)", r"excluded.\1", statement)
        return statement, parameters

    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE si_trade_calendar (trade_date TEXT, trade_status INTEGER)"))
        connection.execute(text("INSERT INTO si_trade_calendar VALUES ('2026-09-03', 1)"))
        connection.execute(text("""CREATE TABLE sm_stock_kline (
            stock_code TEXT, trade_date TEXT, k_type INTEGER,
            adjust_type INTEGER, volume REAL, amount REAL
        )"""))
        if not missing_bars:
            for code in ("000001", "600000", "920001"):
                connection.execute(text("INSERT INTO sm_stock_kline VALUES (:code, '2026-09-03', 1, 0, 100, 1000)"), {"code": code})
        connection.execute(text("""CREATE TABLE sm_stock_capital_flow_daily (
            stock_code TEXT, trade_date TEXT, main_net_inflow REAL,
            max_net_inflow REAL, lg_net_inflow REAL, mid_net_inflow REAL,
            sm_net_inflow REAL, data_source TEXT, etl_sync_at TIMESTAMP,
            PRIMARY KEY(stock_code,trade_date)
        )"""))
        for row in rows:
            connection.execute(text("""INSERT INTO sm_stock_capital_flow_daily
                (stock_code,trade_date,main_net_inflow,max_net_inflow,lg_net_inflow,
                 mid_net_inflow,sm_net_inflow,data_source)
                VALUES (:stock_code,:trade_date,:main_net_inflow,:max_net_inflow,
                        :lg_net_inflow,:mid_net_inflow,:sm_net_inflow,:data_source)
            """), row)
    universe = DailyStockUniverse(
        target_date="2026-09-03", catalog_batch_id="catalog", catalog_manifest_hash="1" * 64,
        catalog_member_set_hash="2" * 64, expected_codes=("000001", "600000", "920001"),
        expected_code_set_hash="3" * 64,
    )
    monkeypatch.setattr(repair, "load_daily_stock_universe", lambda *_args, **_kwargs: universe)

    @contextmanager
    def local_lock(*_args, **_kwargs):
        yield

    monkeypatch.setattr(backfill, "mysql_named_lock", local_lock)
    monkeypatch.setattr(repair, "publish_daily_flow_from_exact_minute", lambda *_args, **_kwargs: pytest.fail("must not replace Eastmoney with QMT minute semantics"))
    publisher = repair.ProductionPartitionPublisher(
        engine, engine, object(), expected_build_sha=BUILD_SHA,
        now=datetime(2026, 9, 5, 12, tzinfo=SHANGHAI), flow_evidence_root=tmp_path,
    )
    return engine, publisher, backfill


def _historical_flow_row(code="000001", *, day="2026-09-03", source="push2hist"):
    return {
        "stock_code": code, "trade_date": day,
        "main_net_inflow": 30, "max_net_inflow": 10, "lg_net_inflow": 20,
        "mid_net_inflow": -15, "sm_net_inflow": -15, "data_source": source,
    }


def _read_historical_flow(engine):
    with engine.connect() as connection:
        return repair._daily_flow_database_rows(connection, "2026-09-03")


def test_scheduled_repair_waits_for_exact_eastmoney_without_calling_unmanaged_provider(monkeypatch, tmp_path):
    engine, publisher, backfill = _historical_flow_fixture(monkeypatch, tmp_path)
    monkeypatch.setattr(backfill, "_fetch_flow_code", lambda *_args: pytest.fail("unmanaged provider must not run"))
    monkeypatch.setattr(backfill, "backfill_flow", lambda *_args, **_kwargs: pytest.fail("unmanaged writer must not run"))
    with pytest.raises(repair.LinuxGapRepairBlocked, match="exact Eastmoney"):
        publisher(repair.PartitionRef("2026-09-03", "stock_daily_flow"))
    assert _read_historical_flow(engine) == []
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("source", ["east_push2delay", "push2hist", "baidu"])
def test_complete_historical_flow_reuses_without_network_and_keeps_beijing_rows(monkeypatch, tmp_path, source):
    rows = [_historical_flow_row(code, source=source) for code in ("000001", "600000", "920001")]
    engine, publisher, backfill = _historical_flow_fixture(monkeypatch, tmp_path, rows=rows)
    monkeypatch.setattr(backfill, "_fetch_flow_code", lambda *_args: pytest.fail("complete partition must remain offline"))
    receipt = publisher(repair.PartitionRef("2026-09-03", "stock_daily_flow"))
    assert receipt["reused_existing"] is True
    assert len(_read_historical_flow(engine)) == 3
    assert list(tmp_path.iterdir()) == []


def test_partial_other_provider_waits_for_exact_eastmoney_without_mixing(monkeypatch, tmp_path):
    rows = [_historical_flow_row(source="baidu")]
    engine, publisher, backfill = _historical_flow_fixture(monkeypatch, tmp_path, rows=rows)
    monkeypatch.setattr(backfill, "_fetch_flow_code", lambda *_args: pytest.fail("provider selection requires explicit policy"))
    with pytest.raises(repair.LinuxGapRepairBlocked, match="exact Eastmoney"):
        publisher(repair.PartitionRef("2026-09-03", "stock_daily_flow"))
    assert _read_historical_flow(engine) == rows


def test_unknown_historical_flow_source_remains_blocked(monkeypatch, tmp_path):
    rows = [
        _historical_flow_row(code, source="unknown")
        for code in ("000001", "600000", "920001")
    ]
    engine, publisher, backfill = _historical_flow_fixture(
        monkeypatch, tmp_path, rows=rows
    )
    monkeypatch.setattr(
        backfill,
        "_fetch_flow_code",
        lambda *_args: pytest.fail("unknown persisted source must remain offline"),
    )
    with pytest.raises(repair.LinuxGapRepairBlocked, match="exact Eastmoney"):
        publisher(repair.PartitionRef("2026-09-03", "stock_daily_flow"))
    assert _read_historical_flow(engine) == rows


def test_east_push2delay_bucket_mismatch_remains_blocked(monkeypatch, tmp_path):
    rows = [
        _historical_flow_row(code, source="east_push2delay")
        for code in ("000001", "600000", "920001")
    ]
    rows[0]["main_net_inflow"] = 2_000_000
    engine, publisher, backfill = _historical_flow_fixture(
        monkeypatch, tmp_path, rows=rows
    )
    monkeypatch.setattr(
        backfill,
        "_fetch_flow_code",
        lambda *_args: pytest.fail("invalid persisted buckets must remain offline"),
    )
    with pytest.raises(repair.LinuxGapRepairBlocked, match="exact Eastmoney"):
        publisher(repair.PartitionRef("2026-09-03", "stock_daily_flow"))
    assert _read_historical_flow(engine) == rows


def test_mixed_historical_flow_sources_remain_blocked(monkeypatch, tmp_path):
    rows = [
        _historical_flow_row("000001", source="east_push2delay"),
        _historical_flow_row("600000", source="push2hist"),
        _historical_flow_row("920001", source="east_push2delay"),
    ]
    engine, publisher, backfill = _historical_flow_fixture(
        monkeypatch, tmp_path, rows=rows
    )
    monkeypatch.setattr(
        backfill,
        "_fetch_flow_code",
        lambda *_args: pytest.fail("mixed persisted sources must remain offline"),
    )
    with pytest.raises(repair.LinuxGapRepairBlocked, match="exact Eastmoney"):
        publisher(repair.PartitionRef("2026-09-03", "stock_daily_flow"))
    assert _read_historical_flow(engine) == rows


@pytest.mark.parametrize("wrong_date", [False, True])
def test_provider_failure_or_wrong_date_never_overwrites_good_rows(monkeypatch, tmp_path, wrong_date):
    rows = [_historical_flow_row()]
    engine, publisher, backfill = _historical_flow_fixture(monkeypatch, tmp_path, rows=rows)
    monkeypatch.setattr(backfill, "_fetch_flow_code", lambda code, dates: (
        code, ([_historical_flow_row(code, day="2026-09-04")] if wrong_date else []), "push2delay",
    ))
    with pytest.raises(repair.LinuxGapRepairBlocked):
        publisher(repair.PartitionRef("2026-09-03", "stock_daily_flow"))
    assert _read_historical_flow(engine) == rows
    with engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM sm_stock_capital_flow_daily")).scalar() == 1


def test_missing_canonical_daily_universe_cannot_produce_zero_row_success(monkeypatch, tmp_path):
    engine, publisher, backfill = _historical_flow_fixture(monkeypatch, tmp_path, missing_bars=True)
    monkeypatch.setattr(backfill, "_fetch_flow_code", lambda *_args: pytest.fail("no independent daily prerequisite"))
    with pytest.raises(RuntimeError, match="DATA_BLOCKED"):
        publisher(repair.PartitionRef("2026-09-03", "stock_daily_flow"))
    assert _read_historical_flow(engine) == []
