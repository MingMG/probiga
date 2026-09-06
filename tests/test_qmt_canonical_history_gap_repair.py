from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from tools import repair_qmt_canonical_history_gaps as repair


SHANGHAI = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 8, 27, 1, 15, tzinfo=SHANGHAI)
BUILD_SHA = "a" * 40


def _window(*sessions: str) -> repair.CalendarWindow:
    return repair.CalendarWindow(
        sessions=tuple(sessions),
        batch_id="qmt_rel_" + "b" * 40,
        manifest_hash="c" * 64,
        source_session_set_hash="d" * 64,
    )


def _proof(partition: repair.PartitionRef) -> dict[str, object]:
    return {
        "dataset": partition.dataset,
        "trade_date": partition.trade_date,
        "row_count": 1,
        "row_hash": repair._digest(
            [partition.dataset, partition.trade_date, "canonical"]
        ),
    }


class _State:
    def __init__(self, exact: set[str]) -> None:
        self.exact = set(exact)
        self.inspected: list[str] = []

    def inspect(self, partition: repair.PartitionRef) -> dict[str, object]:
        self.inspected.append(partition.partition_id)
        if partition.partition_id not in self.exact:
            raise RuntimeError("partition incomplete")
        return _proof(partition)

    def publish(self, partition: repair.PartitionRef) -> dict[str, object]:
        self.exact.add(partition.partition_id)
        return {
            "source_schema": "test.publisher.v1",
            "source_status": "PASS",
            "source_receipt_sha256": repair._digest(partition.partition_id),
            "forward_observation_created": False,
        }


def _run(
    state: _State,
    *,
    datasets=("stock_daily", "stock_minute"),
    sessions=("2026-08-25", "2026-08-26"),
    budget=5,
    apply=True,
    publisher=None,
) -> dict[str, object]:
    window = _window(*sessions)
    return repair.repair_recent_partitions(
        expected_build_sha=BUILD_SHA,
        datasets=datasets,
        lookback_sessions=len(sessions),
        max_repairs_per_run=budget,
        apply=apply,
        now=NOW,
        window=window,
        inspect_partition=state.inspect,
        publish_partition=publisher or state.publish,
    )


def test_complete_exact_window_never_calls_publisher() -> None:
    exact = {
        "stock_daily:2026-08-25",
        "stock_minute:2026-08-25",
        "stock_daily:2026-08-26",
        "stock_minute:2026-08-26",
    }
    state = _State(exact)

    def forbidden(_partition):
        raise AssertionError("exact canonical partition was fetched again")

    result = _run(state, publisher=forbidden)

    assert result["status"] == "COMPLETE"
    assert result["candidate_before_count"] == 0
    assert result["attempted_count"] == 0
    assert result["remaining_count"] == 0
    assert repair.validate_task_result(result, 0) == "complete"


def test_repairs_only_missing_partitions_and_second_run_is_idempotent() -> None:
    state = _State(
        {
            "stock_daily:2026-08-25",
            "stock_minute:2026-08-26",
        }
    )
    published: list[str] = []

    def publish(partition: repair.PartitionRef):
        published.append(partition.partition_id)
        return state.publish(partition)

    first = _run(state, publisher=publish)
    assert first["status"] == "COMPLETE"
    assert published == [
        "stock_minute:2026-08-25",
        "stock_daily:2026-08-26",
    ]
    assert first["repaired_count"] == 2
    assert repair.validate_task_result(first, 0) == "complete"

    published.clear()
    second = _run(state, publisher=publish)
    assert second["status"] == "COMPLETE"
    assert second["attempted_count"] == 0
    assert published == []


def test_failed_partition_resumes_after_prior_partition_was_committed() -> None:
    state = _State(set())
    failed_id = "stock_minute:2026-08-25"

    def fail_second(partition: repair.PartitionRef):
        if partition.partition_id == failed_id:
            raise RuntimeError("provider temporarily unavailable")
        return state.publish(partition)

    first = _run(
        state,
        datasets=("stock_daily", "stock_minute", "index_kline"),
        sessions=("2026-08-25",),
        publisher=fail_second,
    )
    assert first["status"] == "DATA_BLOCKED"
    assert first["repaired_count"] == 2
    assert first["remaining_partition_ids"] == [failed_id]
    assert state.exact == {
        "stock_daily:2026-08-25",
        "index_kline:2026-08-25",
    }
    assert repair.validate_task_result(first, 2) == "blocked"

    second = _run(
        state,
        datasets=("stock_daily", "stock_minute", "index_kline"),
        sessions=("2026-08-25",),
    )
    assert second["status"] == "COMPLETE"
    assert second["candidate_before_ids"] == [failed_id]
    assert second["repaired_count"] == 1


def test_repair_budget_is_bounded_and_retryable() -> None:
    state = _State(set())
    result = _run(
        state,
        datasets=("stock_daily", "stock_minute", "index_kline"),
        sessions=("2026-08-26",),
        budget=1,
    )
    assert result["status"] == "DATA_BLOCKED"
    assert result["blocked_reason"] == "repair_budget_exhausted"
    assert result["attempted_count"] == 1
    assert result["remaining_count"] == 2
    assert result["retryable"] is True


def test_dry_run_never_mutates_and_reports_missing_partitions() -> None:
    state = _State(set())

    def forbidden(_partition):
        raise AssertionError("dry run called a publisher")

    result = _run(
        state,
        sessions=("2026-08-26",),
        apply=False,
        publisher=forbidden,
    )
    assert result["status"] == "DATA_BLOCKED"
    assert result["blocked_reason"] == "dry_run_missing_partitions"
    assert result["attempted_count"] == 0
    assert state.exact == set()


def test_receipt_tampering_and_wrong_exit_codes_are_rejected() -> None:
    state = _State(
        {"stock_daily:2026-08-26", "stock_minute:2026-08-26"}
    )
    result = _run(state, sessions=("2026-08-26",))
    tampered = dict(result)
    tampered["exact_after_count"] = 1
    with pytest.raises(ValueError, match="hash differs"):
        repair.validate_task_result(tampered, 0)
    with pytest.raises(ValueError, match="COMPLETE"):
        repair.validate_task_result(result, 2)


def test_machine_output_parser_and_scheduler_disposition() -> None:
    state = _State(
        {"stock_daily:2026-08-26", "stock_minute:2026-08-26"}
    )
    result = _run(state, sessions=("2026-08-26",))
    output = "provider noise\n" + repair._canonical_json(result) + "\n"
    assert repair.parse_result(output) == result
    assert repair.scheduler_output_status(output, return_code=0) == "success"
    assert repair.scheduler_output_status(output, return_code=2) == "failed"
    duplicate = output + repair._canonical_json(result) + "\n"
    with pytest.raises(ValueError, match="exactly one"):
        repair.parse_result(duplicate)
    assert repair.scheduler_output_status(duplicate, return_code=0) == "failed"


def test_persisted_validator_replays_same_calendar_and_partition_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PROBIGA_SCHEDULER_BUILD_SHA", raising=False)
    monkeypatch.delenv("PROBIGA_BUILD_COMMIT_SHA", raising=False)
    sessions = ("2026-08-25", "2026-08-26")
    exact = {
        f"{dataset}:{trade_date}"
        for trade_date in sessions
        for dataset in ("stock_daily", "index_minute", "etf_daily")
    }
    state = _State(exact)
    result = _run(
        state,
        datasets=("stock_daily", "index_minute", "etf_daily"),
        sessions=sessions,
    )
    window = _window(*sessions)

    def load_window(_engine, **kwargs):
        assert kwargs["lookback_sessions"] == 2
        assert kwargs["batch_id"] == window.batch_id
        return window

    verified = repair.validate_persisted_result(
        object(),
        result,
        history_engine=object(),
        now=repair._parse_timestamp(result["finished_at"], field="finished")
        + timedelta(minutes=1),
        window_loader=load_window,
        inspect_partition=state.inspect,
    )
    assert verified["status"] == "COMPLETE"
    assert verified["partition_count"] == 6
    assert (
        verified["exact_partition_root_sha256"]
        == result["exact_partition_root_sha256"]
    )


def test_executor_contract_is_strictly_windows_edge_and_outer_task() -> None:
    repair.validate_executor(
        platform_name="nt",
        executor_role=repair.EDGE_ROLE,
        task_type=repair.TASK_TYPE,
    )
    with pytest.raises(repair.CanonicalGapRepairBlocked):
        repair.validate_executor(
            platform_name="posix",
            executor_role=repair.EDGE_ROLE,
            task_type=repair.TASK_TYPE,
        )
    with pytest.raises(repair.CanonicalGapRepairBlocked):
        repair.validate_executor(
            platform_name="nt",
            executor_role="linux_scheduler",
            task_type=repair.TASK_TYPE,
        )
    with pytest.raises(repair.CanonicalGapRepairBlocked):
        repair.validate_executor(
            platform_name="nt",
            executor_role=repair.EDGE_ROLE,
            task_type="qmt_index_current",
        )


def test_heavy_repair_clock_is_confined_to_the_overnight_qmt_window() -> None:
    repair.validate_repair_clock(
        datetime(2026, 8, 27, 1, 15, tzinfo=SHANGHAI)
    )
    repair.validate_repair_clock(
        datetime(2026, 8, 27, 20, 30, tzinfo=SHANGHAI)
    )
    with pytest.raises(repair.CanonicalGapRepairBlocked, match="20:30-08:00"):
        repair.validate_repair_clock(
            datetime(2026, 8, 27, 9, 0, tzinfo=SHANGHAI)
        )


def test_five_closed_sessions_cover_august_20_through_26(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    class _Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class _Engine:
        def connect(self):
            return _Connection()

    class _Receipt:
        batch_id = "qmt_rel_" + "b" * 40
        manifest_hash = "c" * 64
        session_set_hash = "d" * 64

        @staticmethod
        def sessions_between(_start: str, _end: str):
            return [
                "2026-08-20",
                "2026-08-21",
                "2026-08-24",
                "2026-08-25",
                "2026-08-26",
            ]

    monkeypatch.setattr(
        repair, "validate_trade_calendar_runtime_schema", lambda _engine: None
    )

    def load_receipt(_connection, **kwargs):
        observed.update(kwargs)
        return _Receipt()

    monkeypatch.setattr(repair, "load_trade_calendar_receipt", load_receipt)
    window = repair.load_recent_closed_window(
        _Engine(), now=NOW, lookback_sessions=5
    )

    assert window.sessions == (
        "2026-08-20",
        "2026-08-21",
        "2026-08-24",
        "2026-08-25",
        "2026-08-26",
    )
    assert observed["end_date"] == "2026-08-26"
    assert observed["decision_known_at"] == datetime(2026, 8, 27, 1, 15)


def test_current_datasets_are_not_in_scope() -> None:
    assert "stock_current" not in repair.DATASET_ORDER
    assert "index_current" not in repair.DATASET_ORDER
    assert "stock_minute_flow" in repair.DATASET_ORDER
    with pytest.raises(SystemExit):
        repair._parse_args(["--dataset", "index_current"])


def test_completed_stock_partition_with_native_no_trade_is_not_refetched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tools import sync_qmt_stock_edge as stock

    engine = object()
    proof = {
        "row_count": 5546,
        "row_hash": "1" * 64,
        "code_count": 5546,
        "code_set_hash": "2" * 64,
        "native_no_trade_rows": 1,
        "native_no_trade_codes": ["002731"],
    }

    def reusable(actual_engine, *, trade_date, decision_known_at):
        assert actual_engine is engine
        assert trade_date == "2026-08-26"
        assert decision_known_at == NOW.replace(tzinfo=None)
        return proof

    monkeypatch.setattr(stock, "_reusable_daily_partition", reusable)
    inspector = repair.CanonicalPartitionInspector(
        object(), engine, object(),
        window=_window("2026-08-26"), decision_time=NOW,
    )

    def forbidden(_partition):
        raise AssertionError("validated NO_TRADE partition was fetched again")

    result = repair.repair_recent_partitions(
        expected_build_sha=BUILD_SHA, datasets=("stock_daily",),
        lookback_sessions=1, max_repairs_per_run=1, apply=True, now=NOW,
        window=_window("2026-08-26"), inspect_partition=inspector,
        publish_partition=forbidden,
    )
    assert result["status"] == "COMPLETE"
    assert result["attempted_count"] == 0


@pytest.mark.parametrize("corrupt", [False, True])
def test_stock_inspection_rejects_missing_or_corrupt_attestation(
    monkeypatch: pytest.MonkeyPatch, corrupt: bool,
) -> None:
    from tools import sync_qmt_stock_edge as stock

    def reusable(*_args, **_kwargs):
        if corrupt:
            raise stock.StockDataBlocked("persisted daily attestation is invalid")
        return None

    monkeypatch.setattr(stock, "_reusable_daily_partition", reusable)
    inspector = repair.CanonicalPartitionInspector(
        object(), object(), object(),
        window=_window("2026-08-26"), decision_time=NOW,
    )
    with pytest.raises(
        (repair.CanonicalGapRepairBlocked, stock.StockDataBlocked),
        match="attestation",
    ):
        inspector(repair.PartitionRef("stock_daily", "2026-08-26"))


def test_index_catalog_uses_its_own_authority_not_calendar_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tools import sync_qmt_index_edge as index

    primary = object()
    catalog = [object()]
    calls = []

    def load(engine, *, expected_batch_id=None):
        assert engine is primary
        assert expected_batch_id is None
        calls.append(engine)
        return catalog

    monkeypatch.setattr(index, "_load_index_catalog", load)
    inspector = repair.CanonicalPartitionInspector(
        primary, object(), object(),
        window=_window("2026-08-26"), decision_time=NOW,
    )
    assert inspector._load_index_catalog() is catalog
    assert inspector._load_index_catalog() is catalog
    assert calls == [primary]


def test_index_catalog_invalid_authority_still_blocks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tools import sync_qmt_index_edge as index

    def invalid(*_args, **_kwargs):
        raise index.IndexDataBlocked("QMT index catalog authority invalid")

    monkeypatch.setattr(index, "_load_index_catalog", invalid)
    inspector = repair.CanonicalPartitionInspector(
        object(), object(), object(),
        window=_window("2026-08-26"), decision_time=NOW,
    )
    with pytest.raises(index.IndexDataBlocked, match="authority invalid"):
        inspector._load_index_catalog()


def test_default_five_session_plan_contains_every_native_minute_flow_date() -> None:
    sessions = (
        "2026-08-20",
        "2026-08-21",
        "2026-08-24",
        "2026-08-25",
        "2026-08-26",
    )
    plan = repair._plan(_window(*sessions), repair.DATASET_ORDER)

    assert len(plan) == len(sessions) * 6
    assert [
        item.partition_id for item in plan if item.dataset == "stock_minute_flow"
    ] == [f"stock_minute_flow:{trade_date}" for trade_date in sessions]


def test_minute_flow_inspector_replays_catalog_grid_and_native_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tools import sync_qmt_minute_flow_exact as exact

    proof = {
        "row_count": 2 * len(exact.GRID),
        "row_hash": "1" * 64,
        "code_count": 2,
        "code_set_hash": "2" * 64,
        "minute_grid_profile": exact.QMT_MINUTE_GRID_PROFILE,
        "minute_grid_count": len(exact.GRID),
        "minute_grid_hash": exact.GRID_HASH,
        "nonzero_code_ratio": "0.500000",
    }

    class _MinuteConnection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class _MinuteEngine:
        def connect(self):
            return _MinuteConnection()

    class _Universe:
        traded_stock_count = 2
        traded_stock_set_hash = "2" * 64
        catalog = {"manifest_hash": "3" * 64}
        daily_truth = {"truth_hash": "4" * 64}

    observed: dict[str, object] = {}
    monkeypatch.setattr(
        exact,
        "validate_runtime_schema",
        lambda primary, minute: observed.update(primary=primary, minute=minute),
    )
    monkeypatch.setattr(exact, "load_flow_universe", lambda *_args, **_kwargs: _Universe())
    monkeypatch.setattr(exact, "_stream_table_proof", lambda *_args, **_kwargs: proof)
    primary = object()
    minute = _MinuteEngine()
    inspector = repair.CanonicalPartitionInspector(
        primary,
        object(),
        minute,
        window=_window("2026-08-26"),
        decision_time=NOW,
    )

    result = inspector(repair.PartitionRef("stock_minute_flow", "2026-08-26"))

    assert result == {
        "dataset": "stock_minute_flow",
        "trade_date": "2026-08-26",
        "row_count": 482,
        "row_hash": "1" * 64,
        "code_count": 2,
        "code_set_hash": "2" * 64,
        "minute_grid_count": len(exact.GRID),
        "minute_grid_hash": exact.GRID_HASH,
        "nonzero_code_ratio": "0.500000",
        "catalog_manifest_hash": "3" * 64,
        "daily_truth_hash": "4" * 64,
    }
    assert observed == {"primary": primary, "minute": minute}


def test_minute_flow_repair_adapter_uses_exact_date_bound_publisher(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tools import sync_qmt_minute_flow_exact as exact

    calls: list[dict[str, object]] = []
    minute = object()

    def run_sync(primary, minute_engine, **kwargs):
        calls.append(
            {
                "primary": primary,
                "minute": minute_engine,
                "task_type": __import__("os").environ[
                    "PROBIGA_SCHEDULER_TASK_TYPE"
                ],
                **kwargs,
            }
        )
        return {
            "schema": exact.RESULT_SCHEMA,
            "status": "PASS",
            "trade_date": "2026-08-26",
            "build_sha": BUILD_SHA,
            "receipt_id": "5" * 64,
        }

    monkeypatch.setattr(exact, "run_sync", run_sync)
    monkeypatch.setattr(exact, "validate_task_result", lambda *_args: "complete")
    monkeypatch.setenv("PROBIGA_SCHEDULER_TASK_TYPE", repair.TASK_TYPE)
    primary = object()
    publisher = repair.ExactPartitionPublisher(
        primary,
        expected_build_sha=BUILD_SHA,
        now=NOW,
        minute_engine=minute,
    )

    result = publisher(
        repair.PartitionRef("stock_minute_flow", "2026-08-26")
    )

    assert result["source_receipt_sha256"] == "5" * 64
    assert result["forward_observation_created"] is False
    assert calls == [
        {
            "primary": primary,
            "minute": minute,
            "task_type": exact.TASK_TYPE,
            "trade_date": "2026-08-26",
            "apply": True,
            "expected_build_sha": BUILD_SHA,
            "now": NOW,
        }
    ]
    assert __import__("os").environ["PROBIGA_SCHEDULER_TASK_TYPE"] == repair.TASK_TYPE


def test_etf_historical_adapter_requires_market_only_forward_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tools import run_etf_forward_daily as forward
    from tools import sync_etf_bigqmt_daily as market

    market_summary = {
        "status": "PASS",
        "receipt_id": "e" * 64,
        "trade_date": "2026-08-26",
        "database": {"row_count": 28, "row_hash": "f" * 64},
    }
    result = forward._receipt(
        {
            "schema": forward.RECEIPT_SCHEMA,
            "status": "PASS",
            "trade_date": "2026-08-26",
            "provider": market.PROVIDER_ID,
            "executor_owner": repair.EDGE_ROLE,
            "market_data": market_summary,
            "forward_ledger": {
                "status": "NOT_RUN_HISTORICAL_BACKFILL_PROHIBITED",
                "data_date": "2026-08-26",
            },
            "automatic_order_submission": False,
        }
    )
    monkeypatch.setattr(forward, "run_daily", lambda *_args, **_kwargs: result)
    monkeypatch.setattr(
        market,
        "run_sync",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("historical ETF used same-day market-only branch")
        ),
    )
    publisher = repair.ExactPartitionPublisher(
        object(), expected_build_sha=BUILD_SHA, now=NOW
    )
    summary = publisher(repair.PartitionRef("etf_daily", "2026-08-26"))
    assert summary["forward_observation_created"] is False
    assert summary["source_receipt_sha256"] == result["receipt_id"]


def test_etf_same_day_adapter_calls_only_market_phase(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tools import run_etf_forward_daily as forward
    from tools import sync_etf_bigqmt_daily as market

    current = datetime(2026, 8, 27, 16, 0, tzinfo=SHANGHAI)
    market_result = market._receipt(
        {
            "schema": market.RECEIPT_SCHEMA,
            "status": "PASS",
            "trade_date": "2026-08-27",
            "provider": market.PROVIDER_ID,
            "executor_owner": repair.EDGE_ROLE,
            "database": {"row_count": 28, "row_hash": "f" * 64},
            "automatic_order_submission": False,
        }
    )
    monkeypatch.setattr(market, "run_sync", lambda *_args, **_kwargs: market_result)
    monkeypatch.setattr(
        forward,
        "run_daily",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("same-day gap repair created a forward observation")
        ),
    )
    publisher = repair.ExactPartitionPublisher(
        object(), expected_build_sha=BUILD_SHA, now=current
    )
    summary = publisher(repair.PartitionRef("etf_daily", "2026-08-27"))
    assert summary["forward_observation_created"] is False
    assert summary["source_receipt_sha256"] == market_result["receipt_id"]


def test_tool_never_uses_local_history_tables() -> None:
    source = Path(repair.__file__).read_text(encoding="utf-8")
    assert "qmt_local_" not in source


def test_scheduler_maps_complete_to_success_and_progress_to_retryable_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from server.common.scheduler_validation import scheduler_output_status

    monkeypatch.setenv("PROBIGA_SCHEDULER_BUILD_SHA", BUILD_SHA)
    complete_state = _State(
        {"stock_daily:2026-08-26", "stock_minute:2026-08-26"}
    )
    complete = _run(complete_state, sessions=("2026-08-26",))
    task = {"task_type": repair.TASK_TYPE}
    output = repair._canonical_json(complete)
    assert scheduler_output_status(task, output, return_code=0) == "success"
    assert scheduler_output_status(task, output + "\n" + output, return_code=0) == "failed"

    blocked = _run(
        _State(set()),
        sessions=("2026-08-26",),
        budget=1,
    )
    assert blocked["retryable"] is True
    assert (
        scheduler_output_status(
            task,
            repair._canonical_json(blocked),
            return_code=2,
        )
        == "failed"
    )


def test_scheduler_postvalidator_replays_complete_canonical_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from server.common import scheduler_validation

    monkeypatch.setenv("PROBIGA_SCHEDULER_BUILD_SHA", BUILD_SHA)
    state = _State(
        {"stock_daily:2026-08-26", "stock_minute:2026-08-26"}
    )
    payload = _run(state, sessions=("2026-08-26",))
    receipt_started = repair._parse_timestamp(
        payload["started_at"], field="started_at"
    ).replace(tzinfo=None)
    receipt_finished = repair._parse_timestamp(
        payload["finished_at"], field="finished_at"
    ).replace(tzinfo=None)
    observed: dict[str, object] = {}

    def validate_persisted(engine, candidate, *, now):
        observed.update(engine=engine, payload=candidate, now=now)
        return {
            "sessions": ["2026-08-26"],
            "partition_count": 2,
        }

    monkeypatch.setattr(repair, "validate_persisted_result", validate_persisted)
    engine = object()
    result = scheduler_validation.validate_scheduler_task_result(
        {"task_type": repair.TASK_TYPE},
        engine=engine,
        started_at=receipt_started,
        now=receipt_finished + timedelta(minutes=1),
        output=repair._canonical_json(payload),
    )

    assert result.checked is True
    assert result.ok is True
    assert "partitions=2" in result.message
    assert observed["engine"] is engine
    assert observed["payload"] == payload


def test_scheduler_gives_daily_gap_repair_an_overnight_retry_window() -> None:
    from server.api import scheduler_runtime
    from tools.qmt_host_ownership_contract import (
        QMT_CANONICAL_HISTORY_GAP_REPAIR_TASK,
    )

    task = dict(QMT_CANONICAL_HISTORY_GAP_REPAIR_TASK)
    assert task["cron_time"] == "00:15"
    assert task["interval_minutes"] == 0
    assert (
        scheduler_runtime._task_timeout_minutes(task)
        == scheduler_runtime.LONG_TASK_TIMEOUT_MINUTES
    )
    task.update(
        last_triggered_at="2026-08-26 00:15:00",
        last_run_status="success",
    )
    assert scheduler_runtime._critical_cron_catchup_allowed(
        task,
        now=datetime(2026, 8, 27, 23, 0),
        cron_time="00:15",
    )
