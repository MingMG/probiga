from __future__ import annotations

from datetime import datetime
from decimal import Decimal
import json
from zoneinfo import ZoneInfo

import pytest

from server.api import scheduler_runtime
from server.common import scheduler_validation
from tools import repair_linux_recent_data_gaps as repair
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
        "market_overview:2026-08-25",
        "stock_daily_flow:2026-08-26",
    ]
    assert first["repaired_count"] == 2
    assert repair.validate_task_result(first, 0) == "complete"

    state.published.clear()
    second = _run(state)
    assert second["status"] == "COMPLETE"
    assert second["publish_attempt_count"] == 0
    assert state.published == []


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
