from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, timedelta
import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from server.api import scheduler_runtime
from server.common import release_data_readiness_contract as readiness
from server.common import scheduler_validation
from server.common.scheduler_args import build_scheduler_task_args
from tools import crawl_realtime_batch as flow


TARGET = "2026-08-26"
LATEST = "2026-08-27"
BUILD_SHA = "a" * 40


def test_daily_flow_universe_excludes_unsupported_bse_without_claiming_coverage():
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value.mappings.return_value.all.return_value = [
        {"stock_code": code, "volume": 100, "amount": 1000}
        for code in ("000001", "600000", "830799", "920071")
    ]
    assert flow._read_target_traded_flow_codes(engine, TARGET) == {"000001", "600000"}


def test_existing_backfill_source_is_reused_without_relabeling():
    frame = _flow_frame("600000", source="push2hist")
    verified, missing = flow._inspect_reusable_flow_partition(
        frame, trade_date=TARGET, target_codes={"600000"})
    assert not missing
    assert verified["data_source"].tolist() == ["push2hist"]


def test_bounded_raw_gap_repair_gets_release_slot_before_long_provider_jobs(monkeypatch):
    monkeypatch.setattr(scheduler_runtime, "_release_build_catchup_allowed", lambda *_a, **_k: True)
    now = datetime(2026, 9, 5, 19)
    repair = {"id": 116, "task_type": "linux_recent_data_gap_repair"}
    other = {"id": 45, "task_type": "capital_flow_batch_fast"}
    assert scheduler_runtime._scheduler_task_sort_key(repair, now=now) < scheduler_runtime._scheduler_task_sort_key(other, now=now)


def _timestamp(day: str, hour: int = 15) -> int:
    parsed = datetime.fromisoformat(f"{day}T{hour:02d}:00:00").replace(
        tzinfo=flow.SHANGHAI
    )
    return int(parsed.timestamp())


def _item(day: str, code: str = "600000") -> dict:
    return {
        "f12": code,
        "f14": "test",
        "f62": 100,
        "f66": 40,
        "f72": 30,
        "f78": 20,
        "f84": 10,
        "f124": _timestamp(day),
    }


def _flow_frame(*codes: str, day: str = TARGET, source: str = "east_push2delay"):
    return pd.DataFrame([
        {
            "stock_code": code,
            "trade_date": day,
            "main_net_inflow": 100,
            "max_net_inflow": 40,
            "lg_net_inflow": 30,
            "mid_net_inflow": 20,
            "sm_net_inflow": 10,
            "data_source": source,
        }
        for code in codes
    ])


def _receipt(day: str = TARGET, *, generated_at: str | None = None) -> dict:
    captured_at = generated_at or f"{day}T15:21:00"
    partition_sha256 = "b" * 64
    source_counts = {"east_push2delay": 5000}
    execution = {
        "mode": flow.CAPITAL_FLOW_EXECUTION_VERIFIED_EXISTING,
        "target_kind": "historical",
        "captured_at": captured_at,
        "reuse_verified_existing": True,
        "existing_row_count": 5000,
        "missing_before_count": 0,
        "rows_written": 0,
        "live_source_called": False,
        "historical_fallback_called": False,
        "network_accessed": False,
        "target_code_count": 5000,
        "live_primary_row_count": 0,
        "fallback_requested_count": 0,
        "fallback_returned_count": 0,
        "partition_replaced": False,
        "partition_verified": True,
        "partition_sha256": partition_sha256,
        "source_counts": source_counts,
    }
    return flow._signed_receipt(
        {
            "schema": flow.CAPITAL_FLOW_RESULT_SCHEMA,
            "status": "PASS",
            "task_type": flow.CAPITAL_FLOW_TASK_TYPE,
            "dataset": flow.CAPITAL_FLOW_DATASET,
            "build_sha": BUILD_SHA,
            "trade_date": day,
            "source_trade_date": day,
            "source_timestamp_required": True,
            "row_count": 5000,
            "execution_mode": execution["mode"],
            "captured_at": captured_at,
            "partition_sha256": partition_sha256,
            "source_counts": source_counts,
            "execution": execution,
            "elapsed_seconds": 1.0,
            "generated_at": captured_at,
        }
    )


def _current_receipt(
    day: str = LATEST,
    *,
    generated_at: str | None = None,
) -> dict:
    captured_at = generated_at or f"{day}T15:21:00"
    partition_sha256 = "c" * 64
    source_counts = {"east_push2delay": 5000}
    execution = {
        "mode": flow.CAPITAL_FLOW_EXECUTION_CURRENT_LIVE,
        "target_kind": "current",
        "captured_at": captured_at,
        "reuse_verified_existing": True,
        "existing_row_count": 0,
        "missing_before_count": 5000,
        "rows_written": 5000,
        "live_source_called": True,
        "historical_fallback_called": False,
        "network_accessed": True,
        "target_code_count": 5000,
        "live_primary_row_count": 5000,
        "fallback_requested_count": 0,
        "fallback_returned_count": 0,
        "partition_replaced": True,
        "partition_verified": True,
        "partition_sha256": partition_sha256,
        "source_counts": source_counts,
    }
    return flow._signed_receipt({
        "schema": flow.CAPITAL_FLOW_RESULT_SCHEMA,
        "status": "PASS",
        "task_type": flow.CAPITAL_FLOW_TASK_TYPE,
        "dataset": flow.CAPITAL_FLOW_DATASET,
        "build_sha": BUILD_SHA,
        "trade_date": day,
        "source_trade_date": day,
        "source_timestamp_required": True,
        "row_count": 5000,
        "execution_mode": execution["mode"],
        "captured_at": captured_at,
        "partition_sha256": partition_sha256,
        "source_counts": source_counts,
        "execution": execution,
        "elapsed_seconds": 1.0,
        "generated_at": captured_at,
    })


def test_release_capital_flow_is_closed_target_and_analysis_prerequisite():
    assert (
        "capital_flow_batch_fast"
        in readiness.RELEASE_CATCHUP_CLOSED_TARGET_TASK_TYPES
    )
    for task_type in ("analysis_fast", "analysis_morning_strict"):
        assert (
            "capital_flow_batch_fast"
            in readiness.RELEASE_DATA_CATCHUP_DEPENDENCIES[task_type]
        )


def test_release_capital_flow_args_bind_exact_closed_date_without_changing_cron():
    row = {
        "task_type": "capital_flow_batch_fast",
        "script_args": "--only flow --min-coverage 0.70 --json",
        "date_param": "",
    }
    assert build_scheduler_task_args(
        row,
        "tools/crawl_realtime_batch.py",
        "2026-08-27",
    ) == ["--only", "flow", "--min-coverage", "0.70", "--json"]
    release_args = build_scheduler_task_args(
        {**row, "_trigger_source": "release_catchup"},
        "tools/crawl_realtime_batch.py",
        TARGET,
    )
    assert release_args[-3:] == [
        "--trade-date",
        TARGET,
        "--reuse-verified-existing",
    ]
    assert release_args.count("--reuse-verified-existing") == 1

    explicit = build_scheduler_task_args(
        {
            **row,
            "script_args": row["script_args"] + " --reuse-verified-existing",
            "_trigger_source": "release_catchup",
        },
        "tools/crawl_realtime_batch.py",
        TARGET,
    )
    assert explicit.count("--reuse-verified-existing") == 1

    with pytest.raises(ValueError, match="duplicated"):
        build_scheduler_task_args(
            {
                **row,
                "script_args": (
                    row["script_args"]
                    + " --reuse-verified-existing --reuse-verified-existing"
                ),
                "_trigger_source": "release_catchup",
            },
            "tools/crawl_realtime_batch.py",
            TARGET,
        )

    with pytest.raises(ValueError, match="conflicts"):
        build_scheduler_task_args(
            {
                **row,
                "script_args": row["script_args"] + " --reuse-verified-existing=false",
                "_trigger_source": "release_catchup",
            },
            "tools/crawl_realtime_batch.py",
            TARGET,
        )

    with pytest.raises(ValueError, match="authoritative target"):
        build_scheduler_task_args(
            {
                **row,
                "script_args": (
                    "--only flow --min-coverage 0.70 --json "
                    "--trade-date 2026-08-25"
                ),
                "_trigger_source": "release_catchup",
            },
            "tools/crawl_realtime_batch.py",
            TARGET,
        )


def test_release_capital_flow_target_rolls_over_at_1800():
    row = {"task_type": "capital_flow_batch_fast"}
    with patch(
        "server.api.scheduler_runtime._release_catchup_closed_target_date",
        return_value=TARGET,
    ):
        assert scheduler_runtime._attach_release_catchup_expected_targets(
            object(),
            [row],
            now=datetime(2026, 8, 27, 17, 59),
        )
    assert row["_release_expected_target_date"] == TARGET

    with patch(
        "server.api.scheduler_runtime._release_catchup_closed_target_date",
        return_value="2026-08-27",
    ):
        assert scheduler_runtime._attach_release_catchup_expected_targets(
            object(),
            [row],
            now=datetime(2026, 8, 27, 18, 0),
        )
    assert row["_release_expected_target_date"] == "2026-08-27"


def test_ordinary_1520_then_release_retry_never_backlabels_and_converges(
    monkeypatch,
):
    build_sha = "c" * 40
    row = {
        "id": 884,
        "task_type": "capital_flow_batch_fast",
        "_release_history_available": True,
        "_release_catchup_authorized": True,
        "_release_expected_target_required": True,
        "_release_expected_target_available": True,
        "_release_expected_target_date": TARGET,
        # A successful ordinary 15:20 run deliberately has no hash-bound
        # release target and therefore cannot satisfy release readiness.
        "_release_terminal_status": "success",
        "_release_terminal_build_sha": build_sha,
        "_release_terminal_exit_code": 0,
        "_release_terminal_output": "ordinary cron success",
    }
    monkeypatch.setattr(
        scheduler_runtime,
        "_scheduler_build_commit_sha",
        lambda: build_sha,
    )
    assert scheduler_runtime._release_build_catchup_allowed(
        row,
        now=datetime(2026, 8, 27, 15, 21),
    )

    monkeypatch.setattr(flow, "_require_open_trade_date", lambda *_args: None)
    monkeypatch.setattr(flow, "_latest_open_trade_date", lambda *_args: LATEST)
    live = MagicMock(return_value=[_item(LATEST)])
    monkeypatch.setattr(
        flow,
        "fetch_batch",
        live,
    )
    with pytest.raises(RuntimeError, match="requires explicit"):
        flow.refresh_flow(
            object(),
            trade_date=TARGET,
            min_coverage=0.7,
            require_source_date=True,
        )
    live.assert_not_called()

    # A failed pre-rollover attempt retains bounded retry backoff.  Once that
    # backoff expires after the 18:00 target rollover, today's exact snapshot
    # is eligible and the same source-date guard admits it.
    row.update(
        {
            "_release_expected_target_date": "2026-08-27",
            "_release_terminal_status": "failed",
            "_release_terminal_finished_at": datetime(2026, 8, 27, 17, 59),
            "_release_terminal_exit_code": 1,
        }
    )
    assert not scheduler_runtime._release_build_catchup_allowed(
        row,
        now=datetime(2026, 8, 27, 18, 0),
    )
    retry_at = datetime(2026, 8, 27, 17, 59) + timedelta(
        minutes=scheduler_runtime.RELEASE_CATCHUP_RETRY_INTERVAL_MINUTES
    )
    assert scheduler_runtime._release_build_catchup_allowed(row, now=retry_at)
    assert flow._exact_flow_source_items(
        [_item(LATEST)],
        trade_date=LATEST,
    ) == [_item(LATEST)]


def test_historical_complete_partition_reuses_without_network_or_write(monkeypatch):
    target_codes = {"600000", "920001"}
    evidence = {}
    monkeypatch.setattr(flow, "_require_open_trade_date", lambda *_args: None)
    monkeypatch.setattr(flow, "_latest_open_trade_date", lambda *_args: LATEST)
    monkeypatch.setattr(
        flow,
        "_read_target_traded_flow_codes",
        lambda *_args: target_codes,
    )
    monkeypatch.setattr(
        flow,
        "_read_existing_flow_partition",
        lambda *_args: _flow_frame("600000", "920001"),
    )
    for name in (
        "fetch_batch",
        "_fetch_missing_flow_rows",
        "_replace_table_rows_flow_partition_exact",
        "_upsert_flow_partition_delta_exact",
    ):
        monkeypatch.setattr(
            flow,
            name,
            lambda *_args, _name=name, **_kwargs: pytest.fail(
                f"historical complete fast-path called {_name}"
            ),
        )

    result = flow.refresh_flow(
        object(),
        trade_date=TARGET,
        require_source_date=True,
        reuse_verified_existing=True,
        execution_evidence=evidence,
    )

    assert type(result) is int
    assert result == 2
    assert evidence["mode"] == flow.CAPITAL_FLOW_EXECUTION_VERIFIED_EXISTING
    assert evidence["target_kind"] == "historical"
    assert evidence["network_accessed"] is False
    assert evidence["live_source_called"] is False
    assert evidence["rows_written"] == 0
    assert evidence["partition_replaced"] is False
    assert evidence["partition_verified"] is True
    assert len(evidence["partition_sha256"]) == 64
    assert evidence["source_counts"] == {"east_push2delay": 2}


def test_historical_partial_fetches_only_exact_missing_codes_and_never_live(
    monkeypatch,
):
    target_codes = {"600000", "920001"}
    evidence = {}
    observed = {}
    monkeypatch.setattr(flow, "_require_open_trade_date", lambda *_args: None)
    monkeypatch.setattr(flow, "_latest_open_trade_date", lambda *_args: LATEST)
    monkeypatch.setattr(
        flow,
        "_read_target_traded_flow_codes",
        lambda *_args: target_codes,
    )
    monkeypatch.setattr(
        flow,
        "_read_existing_flow_partition",
        lambda *_args: _flow_frame("600000"),
    )
    monkeypatch.setattr(
        flow,
        "fetch_batch",
        lambda *_args, **_kwargs: pytest.fail("historical target called live source"),
    )

    def fetch_missing(codes, *, trade_date):
        observed["fallback"] = (set(codes), trade_date)
        return _flow_frame("920001", day=trade_date, source="push2his")

    def write_delta(engine, frame, *, trade_date, expected_codes):
        observed["write"] = (
            engine,
            frame.copy(),
            trade_date,
            set(expected_codes),
        )
        return _flow_frame(
            "600000",
            "920001",
            day=trade_date,
        ).assign(
            data_source=["east_push2delay", "push2his"]
        ), 1

    monkeypatch.setattr(flow, "_fetch_missing_flow_rows", fetch_missing)
    monkeypatch.setattr(flow, "_upsert_flow_partition_delta_exact", write_delta)
    monkeypatch.setattr(
        flow,
        "_replace_table_rows_flow_partition_exact",
        lambda *_args, **_kwargs: pytest.fail("historical gap replaced partition"),
    )
    engine = object()

    result = flow.refresh_flow(
        engine,
        trade_date=TARGET,
        require_source_date=True,
        reuse_verified_existing=True,
        execution_evidence=evidence,
    )

    assert type(result) is int
    assert result == 2
    assert observed["fallback"] == ({"920001"}, TARGET)
    assert observed["write"][0] is engine
    assert observed["write"][1]["stock_code"].tolist() == ["920001"]
    assert observed["write"][2:] == (TARGET, target_codes)
    assert evidence["mode"] == flow.CAPITAL_FLOW_EXECUTION_HISTORICAL_REPAIR
    assert evidence["live_source_called"] is False
    assert evidence["historical_fallback_called"] is True
    assert evidence["missing_before_count"] == evidence["rows_written"] == 1
    assert evidence["partition_replaced"] is False
    assert evidence["source_counts"] == {
        "east_push2delay": 1,
        "push2his": 1,
    }


def test_historical_invalid_row_is_repaired_without_refetching_verified_row(
    monkeypatch,
):
    target_codes = {"600000", "920001"}
    existing = _flow_frame("600000", "920001")
    existing.loc[existing["stock_code"] == "920001", "main_net_inflow"] = float("nan")
    evidence = {}
    observed = {}
    monkeypatch.setattr(flow, "_require_open_trade_date", lambda *_args: None)
    monkeypatch.setattr(flow, "_latest_open_trade_date", lambda *_args: LATEST)
    monkeypatch.setattr(
        flow,
        "_read_target_traded_flow_codes",
        lambda *_args: target_codes,
    )
    monkeypatch.setattr(
        flow,
        "_read_existing_flow_partition",
        lambda *_args: existing.copy(),
    )
    monkeypatch.setattr(
        flow,
        "fetch_batch",
        lambda *_args, **_kwargs: pytest.fail("historical target called live source"),
    )

    def fetch_missing(codes, *, trade_date):
        observed["fallback"] = (set(codes), trade_date)
        return _flow_frame("920001", day=trade_date, source="push2his")

    def write_delta(engine, frame, *, trade_date, expected_codes):
        observed["write_codes"] = set(frame["stock_code"])
        good = existing[existing["stock_code"] == "600000"].copy()
        repaired = pd.concat([good, frame], ignore_index=True)
        return repaired, 1

    monkeypatch.setattr(flow, "_fetch_missing_flow_rows", fetch_missing)
    monkeypatch.setattr(flow, "_upsert_flow_partition_delta_exact", write_delta)
    monkeypatch.setattr(
        flow,
        "_replace_table_rows_flow_partition_exact",
        lambda *_args, **_kwargs: pytest.fail("historical repair replaced partition"),
    )

    assert flow.refresh_flow(
        object(),
        trade_date=TARGET,
        require_source_date=True,
        reuse_verified_existing=True,
        execution_evidence=evidence,
    ) == 2

    assert observed["fallback"] == ({"920001"}, TARGET)
    assert observed["write_codes"] == {"920001"}
    assert evidence["existing_row_count"] == 1
    assert evidence["missing_before_count"] == 1
    assert evidence["repair"]["row_count"] == 1
    assert evidence["repair"]["source_counts"] == {"push2his": 1}
    assert evidence["repair"]["captured_at"] == evidence["captured_at"]
    assert len(evidence["repair"]["row_sha256"]) == 64
    assert len(evidence["verified_existing_sha256"]) == 64


def test_historical_invalid_row_targeted_upsert_preserves_verified_row(
    monkeypatch,
):
    good = _flow_frame("600000").iloc[0].to_dict()
    good.update({
        "main_net_inflow": 601,
        "max_net_inflow": 602,
        "lg_net_inflow": 603,
        "mid_net_inflow": 604,
        "sm_net_inflow": 605,
    })
    bad = _flow_frame("920001").iloc[0].to_dict()
    bad["data_source"] = "unverified_legacy"
    store = {row["stock_code"]: dict(row) for row in (good, bad)}

    class Result:
        def __init__(self, rows=()):
            self.rows = [dict(row) for row in rows]

        def mappings(self):
            return self

        def all(self):
            return [dict(row) for row in self.rows]

    class Connection:
        def __init__(self):
            self.written_codes = []
            self.statements = []

        def in_transaction(self):
            return False

        def commit(self):
            pytest.fail("clean test connection should not need a pre-commit")

        def begin(self):
            return nullcontext()

        def execute(self, statement, params=None):
            sql = str(statement)
            self.statements.append(sql)
            if sql.lstrip().upper().startswith("SELECT"):
                return Result(store.values())
            assert "ON DUPLICATE KEY UPDATE" in sql
            for row in params:
                code = row["stock_code"]
                self.written_codes.append(code)
                store[code] = {
                    key: row[key]
                    for key in (
                        "stock_code",
                        "trade_date",
                        *flow.CAPITAL_FLOW_FIELDS,
                        "data_source",
                    )
                }
                store[code]["etl_sync_at"] = row["etl_sync_at"]
            return Result()

    connection = Connection()
    monkeypatch.setattr(
        flow,
        "mysql_named_lock",
        lambda *_args, **_kwargs: nullcontext(connection),
    )
    original_good = dict(store["600000"])
    replacement = _flow_frame("920001", source="push2his")

    stored, written = flow._upsert_flow_partition_delta_exact(
        object(),
        replacement,
        trade_date=TARGET,
        expected_codes={"600000", "920001"},
    )

    assert written == 1
    assert connection.written_codes == ["920001"]
    assert store["600000"] == original_good
    assert store["920001"]["data_source"] == "push2his"
    assert isinstance(store["920001"]["etl_sync_at"], datetime)
    assert set(stored["stock_code"]) == {"600000", "920001"}


def test_historical_invalid_row_fallback_failure_is_data_blocked(monkeypatch):
    target_codes = {"600000", "920001"}
    existing = _flow_frame("600000", "920001")
    existing.loc[existing["stock_code"] == "920001", "data_source"] = "unknown"
    writer = MagicMock()
    monkeypatch.setattr(flow, "_require_open_trade_date", lambda *_args: None)
    monkeypatch.setattr(flow, "_latest_open_trade_date", lambda *_args: LATEST)
    monkeypatch.setattr(
        flow,
        "_read_target_traded_flow_codes",
        lambda *_args: target_codes,
    )
    monkeypatch.setattr(
        flow,
        "_read_existing_flow_partition",
        lambda *_args: existing.copy(),
    )
    monkeypatch.setattr(
        flow,
        "_fetch_missing_flow_rows",
        lambda codes, **_kwargs: pd.DataFrame(),
    )
    monkeypatch.setattr(flow, "_upsert_flow_partition_delta_exact", writer)

    with pytest.raises(RuntimeError, match="DATA_BLOCKED:.*fallback"):
        flow.refresh_flow(
            object(),
            trade_date=TARGET,
            require_source_date=True,
            reuse_verified_existing=True,
        )
    writer.assert_not_called()


def test_historical_fallback_provider_exception_names_exact_blocked_identity(
    monkeypatch,
):
    def fail_provider(stock_code, trade_date):
        raise OSError(f"provider unavailable for {stock_code} on {trade_date}")

    monkeypatch.setattr(flow, "_fetch_exact_push2his_flow_row", fail_provider)

    with pytest.raises(
        RuntimeError,
        match=(
            "DATA_BLOCKED: exact historical capital-flow fallback failed: "
            f"stock_code=920001 trade_date={TARGET} error_type=OSError"
        ),
    ):
        flow._fetch_missing_flow_rows({"920001"}, trade_date=TARGET)


def test_current_target_still_live_refreshes_when_reuse_flag_is_forced(monkeypatch):
    target_codes = {"600000", "920001"}
    evidence = {}
    published = []
    monkeypatch.setattr(flow, "_require_open_trade_date", lambda *_args: None)
    monkeypatch.setattr(flow, "_latest_open_trade_date", lambda *_args: LATEST)
    monkeypatch.setattr(flow, "_capital_flow_target_kind", lambda *_args: "current")
    monkeypatch.setattr(
        flow,
        "_read_target_traded_flow_codes",
        lambda *_args: target_codes,
    )
    monkeypatch.setattr(
        flow,
        "_read_existing_flow_partition",
        lambda *_args, **_kwargs: pytest.fail("current target reused persisted rows"),
    )
    monkeypatch.setattr(
        flow,
        "fetch_batch",
        lambda *_args, **_kwargs: [
            _item(LATEST, "600000"),
            _item(LATEST, "920001"),
        ],
    )
    monkeypatch.setattr(
        flow,
        "_fetch_missing_flow_rows",
        lambda codes, **_kwargs: (
            pd.DataFrame()
            if not codes
            else pytest.fail(f"unexpected current fallback: {codes}")
        ),
    )
    monkeypatch.setattr(
        flow,
        "_upsert_flow_partition_delta_exact",
        lambda *_args, **_kwargs: pytest.fail("current target used gap writer"),
    )
    monkeypatch.setattr(
        flow,
        "_replace_table_rows_flow_partition_exact",
        lambda engine, frame, **kwargs: published.append(
            (engine, frame.copy(), kwargs)
        ) or len(frame),
    )
    engine = object()

    assert flow.refresh_flow(
        engine,
        trade_date=LATEST,
        require_source_date=True,
        reuse_verified_existing=True,
        execution_evidence=evidence,
    ) == 2

    assert published[0][0] is engine
    assert set(published[0][1]["stock_code"]) == target_codes
    assert evidence["mode"] == flow.CAPITAL_FLOW_EXECUTION_CURRENT_LIVE
    assert evidence["target_kind"] == "current"
    assert evidence["live_source_called"] is True
    assert evidence["partition_replaced"] is True
    assert evidence["rows_written"] == 2


def test_exact_source_date_guard_runs_before_any_capital_flow_write(monkeypatch):
    replace = MagicMock()
    monkeypatch.setattr(flow, "_replace_table_rows_flow_partition_exact", replace)
    monkeypatch.setattr(flow, "_require_open_trade_date", lambda *_args: None)
    monkeypatch.setattr(flow, "_latest_open_trade_date", lambda *_args: TARGET)
    monkeypatch.setattr(flow, "_capital_flow_target_kind", lambda *_args: "current")
    monkeypatch.setattr(flow, "_latest_stock_universe_count", lambda _engine: 1)
    monkeypatch.setattr(
        flow,
        "_read_target_traded_flow_codes",
        lambda *_args: {"600000"},
    )
    monkeypatch.setattr(
        flow,
        "fetch_batch",
        lambda *_args, **_kwargs: [_item("2026-08-27")],
    )

    with pytest.raises(RuntimeError, match="newer than target"):
        flow.refresh_flow(
            object(),
            trade_date=TARGET,
            min_coverage=0.7,
            require_source_date=True,
        )
    replace.assert_not_called()


def test_exact_source_date_guard_drops_old_rows_and_publishes_only_target(
    monkeypatch,
):
    published = []

    def replace(engine, frame, *, trade_date, expected_codes):
        published.append((engine, frame.copy(), trade_date, expected_codes))
        return len(frame)

    monkeypatch.setattr(flow, "_replace_table_rows_flow_partition_exact", replace)
    monkeypatch.setattr(flow, "_require_open_trade_date", lambda *_args: None)
    monkeypatch.setattr(flow, "_latest_open_trade_date", lambda *_args: TARGET)
    monkeypatch.setattr(flow, "_capital_flow_target_kind", lambda *_args: "current")
    monkeypatch.setattr(
        flow,
        "_read_target_traded_flow_codes",
        lambda _engine, _day: {"600000"},
    )
    monkeypatch.setattr(
        flow,
        "fetch_batch",
        lambda *_args, **_kwargs: [
            _item("2026-08-25", "000001"),
            _item(TARGET),
        ],
    )

    assert flow.refresh_flow(
        object(),
        trade_date=TARGET,
        min_coverage=0.7,
        require_source_date=True,
    ) == 1
    frame = published[0][1]
    assert frame["stock_code"].tolist() == ["600000"]
    assert frame["trade_date"].tolist() == [TARGET]
    assert frame["data_source"].tolist() == ["east_push2delay"]
    assert published[0][2:] == (TARGET, {"600000"})


def test_exact_flow_includes_new_bse_selector_and_keeps_real_provider_sources(
    monkeypatch,
):
    assert "m:0+t:81+s:2048" in flow.CAPITAL_FLOW_MARKETS
    captured = []
    monkeypatch.setattr(flow, "_require_open_trade_date", lambda *_args: None)
    monkeypatch.setattr(flow, "_latest_open_trade_date", lambda *_args: TARGET)
    monkeypatch.setattr(flow, "_capital_flow_target_kind", lambda *_args: "current")
    monkeypatch.setattr(
        flow,
        "fetch_batch",
        lambda fs, fields, **kwargs: (
            captured.append((fs, fields, kwargs))
            or [_item(TARGET, "600000")]
        ),
    )
    monkeypatch.setattr(
        flow,
        "_read_target_traded_flow_codes",
        lambda *_args: {"600000", "920001"},
    )
    monkeypatch.setattr(
        flow,
        "_fetch_missing_flow_rows",
        lambda codes, *, trade_date: pd.DataFrame(
            [
                {
                    "stock_code": "920001",
                    "trade_date": trade_date,
                    "main_net_inflow": 1,
                    "max_net_inflow": 2,
                    "lg_net_inflow": 3,
                    "mid_net_inflow": 4,
                    "sm_net_inflow": 5,
                    "data_source": "push2his",
                }
            ]
        )
        if codes == {"920001"}
        else pytest.fail(f"unexpected fallback codes: {codes}"),
    )
    published = []
    monkeypatch.setattr(
        flow,
        "_replace_table_rows_flow_partition_exact",
        lambda engine, frame, **kwargs: published.append(frame.copy()) or len(frame),
    )

    assert flow.refresh_flow(
        object(),
        trade_date=TARGET,
        require_source_date=True,
    ) == 2
    assert captured[0][2] == {"fid": "f12", "po": "0"}
    frame = published[0].set_index("stock_code")
    assert frame.loc["600000", "data_source"] == "east_push2delay"
    assert frame.loc["920001", "data_source"] == "push2his"


def test_unresolved_target_code_blocks_before_any_flow_publication(monkeypatch):
    monkeypatch.setattr(flow, "_require_open_trade_date", lambda *_args: None)
    monkeypatch.setattr(flow, "_latest_open_trade_date", lambda *_args: TARGET)
    monkeypatch.setattr(flow, "_capital_flow_target_kind", lambda *_args: "current")
    monkeypatch.setattr(flow, "fetch_batch", lambda *_args, **_kwargs: [_item(TARGET)])
    monkeypatch.setattr(
        flow,
        "_read_target_traded_flow_codes",
        lambda *_args: {"600000", "920001"},
    )
    monkeypatch.setattr(
        flow,
        "_fetch_missing_flow_rows",
        lambda *_args, **_kwargs: pd.DataFrame(),
    )
    publish = MagicMock()
    monkeypatch.setattr(
        flow,
        "_replace_table_rows_flow_partition_exact",
        publish,
    )

    with pytest.raises(RuntimeError, match="missing_count=1"):
        flow.refresh_flow(
            object(),
            trade_date=TARGET,
            require_source_date=True,
        )
    publish.assert_not_called()


def test_fallback_row_rejects_unproven_ths_date_and_missing_components():
    base = {
        "stock_code": "920001",
        "trade_date": TARGET,
        "main_net_inflow": 1,
        "max_net_inflow": 2,
        "lg_net_inflow": 3,
        "mid_net_inflow": 4,
        "sm_net_inflow": 5,
        "data_source": "push2his",
    }
    assert flow._validated_fallback_row(
        pd.DataFrame([base]),
        stock_code="920001",
        trade_date=TARGET,
    )["data_source"] == "push2his"
    assert flow._validated_fallback_row(
        pd.DataFrame([{**base, "data_source": "baidu"}]),
        stock_code="920001",
        trade_date=TARGET,
    ) is None
    assert flow._validated_fallback_row(
        pd.DataFrame([{**base, "data_source": "ths"}]),
        stock_code="920001",
        trade_date=TARGET,
    ) is None
    assert flow._validated_fallback_row(
        pd.DataFrame([{**base, "trade_date": "2026-08-25"}]),
        stock_code="920001",
        trade_date=TARGET,
    ) is None
    assert flow._validated_fallback_row(
        pd.DataFrame([{**base, "max_net_inflow": "--"}]),
        stock_code="920001",
        trade_date=TARGET,
    ) is None


def test_missing_code_fallback_uses_only_strict_source_identity_parser(
    monkeypatch,
):
    calls = []

    def fetch_one(code, trade_date):
        calls.append((code, trade_date))
        return {
            "stock_code": code,
            "trade_date": trade_date,
            "main_net_inflow": 1,
            "max_net_inflow": 2,
            "lg_net_inflow": 3,
            "mid_net_inflow": 4,
            "sm_net_inflow": 5,
            "data_source": "push2his",
        }

    monkeypatch.setattr(flow, "_fetch_exact_push2his_flow_row", fetch_one)
    result = flow._fetch_missing_flow_rows(
        {"430001", "830001", "920001"},
        trade_date=TARGET,
    )

    assert set(calls) == {
        ("430001", TARGET),
        ("830001", TARGET),
        ("920001", TARGET),
    }
    assert set(result["stock_code"]) == {"430001", "830001", "920001"}
    assert set(result["trade_date"]) == {TARGET}
    assert set(result["data_source"]) == {"push2his"}


class _FlowResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class _FlowClient:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return _FlowResponse(self.payload)


def _push2his_payload(
    *,
    code: str = "920001",
    market: int = 0,
    line: str = f"{TARGET},1,2,3,4,5,6,7,8,9,10",
):
    return {
        "rc": 0,
        "data": {
            "code": code,
            "market": market,
            "klines": [line],
        },
    }


def test_strict_push2his_fallback_binds_source_code_market_date_and_fields():
    client = _FlowClient(_push2his_payload())

    row = flow._fetch_exact_push2his_flow_row(
        "920001", TARGET, client=client
    )

    assert row == {
        "stock_code": "920001",
        "trade_date": TARGET,
        "main_net_inflow": 1.0,
        "sm_net_inflow": 2.0,
        "mid_net_inflow": 3.0,
        "lg_net_inflow": 4.0,
        "max_net_inflow": 5.0,
        "data_source": "push2his",
    }
    assert client.calls[0][1]["params"]["secid"] == "0.920001"


@pytest.mark.parametrize(
    "payload",
    (
        _push2his_payload(code="920002"),
        _push2his_payload(market=1),
    ),
)
def test_strict_push2his_fallback_rejects_request_identity_backfill(payload):
    with pytest.raises(RuntimeError, match="response identity differs"):
        flow._fetch_exact_push2his_flow_row(
            "920001", TARGET, client=_FlowClient(payload)
        )


@pytest.mark.parametrize(
    "line",
    (
        f"{TARGET},1,--,3,4,5,6,7,8,9,10",
        f"{TARGET},1,2,3,4",
        f"{TARGET},1,2,NaN,4,5,6,7,8,9,10",
    ),
)
def test_strict_push2his_fallback_never_coerces_missing_components_to_zero(line):
    with pytest.raises((RuntimeError, ValueError)):
        flow._fetch_exact_push2his_flow_row(
            "920001",
            TARGET,
            client=_FlowClient(_push2his_payload(line=line)),
        )


def test_capital_flow_machine_receipt_and_release_target_are_strict(monkeypatch):
    freshness_modes = []
    output = json.dumps(
        _receipt(TARGET, generated_at="2026-08-27T03:06:00"),
        sort_keys=True,
    )
    monkeypatch.setattr(
        scheduler_validation,
        "_validate_requirement",
        lambda _engine, requirement, **_kwargs: (
            freshness_modes.append(requirement.require_fresh)
            or (True, "target table verified")
        ),
    )
    monkeypatch.setattr(
        scheduler_validation,
        "_validate_daily_universe_coverage",
        lambda *_args, **_kwargs: (True, "full universe verified"),
    )
    monkeypatch.setattr(
        scheduler_validation,
        "_validate_capital_flow_persisted_receipt",
        lambda *_args, **_kwargs: (True, "partition hash verified"),
    )
    monkeypatch.setattr(
        scheduler_validation,
        "_resolve_target_date",
        lambda *_args, **_kwargs: datetime.fromisoformat(LATEST).date(),
    )
    task = {
        "task_type": "capital_flow_batch_fast",
        "_trigger_source": "release_catchup",
        "_release_target_date": TARGET,
        "_scheduler_expected_build_sha": BUILD_SHA,
    }

    assert scheduler_validation.scheduler_output_status(
        task,
        output,
        return_code=0,
    ) == "success"
    validated = scheduler_validation.validate_scheduler_task_result(
        task,
        engine=object(),
        output=output,
        started_at=datetime(2026, 8, 27, 3, 5),
        now=datetime(2026, 8, 27, 3, 7),
    )
    assert validated.checked and validated.ok
    assert freshness_modes == [False]

    mismatch = scheduler_validation.validate_scheduler_task_result(
        {**task, "_release_target_date": "2026-08-27"},
        engine=object(),
        output=output,
        started_at=datetime(2026, 8, 27, 18, 0),
        now=datetime(2026, 8, 27, 18, 1),
    )
    assert mismatch.checked and not mismatch.ok
    assert "receipt date differs" in mismatch.message

    tampered = json.loads(output)
    tampered["trade_date"] = "2026-08-27"
    assert scheduler_validation.scheduler_output_status(
        task,
        json.dumps(tampered),
        return_code=0,
    ) == "failed"


def test_current_flow_receipt_keeps_db_freshness_and_requires_live_mode(monkeypatch):
    freshness_modes = []
    output = json.dumps(
        _current_receipt(generated_at=f"{LATEST}T15:21:00"),
        sort_keys=True,
    )
    monkeypatch.setattr(
        scheduler_validation,
        "_validate_requirement",
        lambda _engine, requirement, **_kwargs: (
            freshness_modes.append(requirement.require_fresh)
            or (True, "target table verified")
        ),
    )
    monkeypatch.setattr(
        scheduler_validation,
        "_validate_daily_universe_coverage",
        lambda *_args, **_kwargs: (True, "full universe verified"),
    )
    monkeypatch.setattr(
        scheduler_validation,
        "_validate_capital_flow_persisted_receipt",
        lambda *_args, **_kwargs: (True, "partition hash verified"),
    )
    monkeypatch.setattr(
        scheduler_validation,
        "_resolve_target_date",
        lambda *_args, **_kwargs: datetime.fromisoformat(LATEST).date(),
    )
    task = {
        "task_type": "capital_flow_batch_fast",
        "_scheduler_expected_build_sha": BUILD_SHA,
    }

    result = scheduler_validation.validate_scheduler_task_result(
        task,
        engine=object(),
        output=output,
        started_at=datetime.fromisoformat(f"{LATEST}T15:20:00"),
        now=datetime.fromisoformat(f"{LATEST}T15:22:00"),
    )

    assert result.checked and result.ok
    assert freshness_modes == [True]

    disguised = _receipt(LATEST, generated_at=f"{LATEST}T15:21:00")
    disguised_result = scheduler_validation.validate_scheduler_task_result(
        task,
        engine=object(),
        output=json.dumps(disguised),
        started_at=datetime.fromisoformat(f"{LATEST}T15:20:00"),
        now=datetime.fromisoformat(f"{LATEST}T15:22:00"),
    )
    assert disguised_result.checked and not disguised_result.ok
    assert "latest target requires current live refresh" in disguised_result.message


def test_flow_receipt_rejects_build_and_execution_counter_contradictions():
    task = {
        "task_type": "capital_flow_batch_fast",
        "_scheduler_expected_build_sha": BUILD_SHA,
    }
    wrong_build = _receipt()
    wrong_build.pop("receipt_id")
    wrong_build["build_sha"] = "d" * 40
    wrong_build = flow._signed_receipt(wrong_build)
    assert scheduler_validation.scheduler_output_status(
        task,
        json.dumps(wrong_build),
        return_code=0,
    ) == "failed"

    contradictory = _receipt()
    contradictory.pop("receipt_id")
    contradictory["execution"]["network_accessed"] = True
    contradictory = flow._signed_receipt(contradictory)
    assert scheduler_validation.scheduler_output_status(
        task,
        json.dumps(contradictory),
        return_code=0,
    ) == "failed"


def test_flow_only_cli_emits_signed_target_bound_receipt(monkeypatch, capsys):
    calls = []
    monkeypatch.setattr(flow, "create_batch_engine", lambda: object())
    monkeypatch.setattr(flow, "_latest_open_trade_date", lambda _engine: TARGET)
    monkeypatch.setenv("PROBIGA_SCHEDULER_BUILD_SHA", BUILD_SHA)

    def fake_refresh(*_args, **kwargs):
        evidence = kwargs["execution_evidence"]
        evidence.update({
            "mode": flow.CAPITAL_FLOW_EXECUTION_CURRENT_LIVE,
            "target_kind": "current",
            "captured_at": f"{TARGET}T15:21:00",
            "reuse_verified_existing": False,
            "existing_row_count": 0,
            "missing_before_count": 5000,
            "rows_written": 5000,
            "live_source_called": True,
            "historical_fallback_called": False,
            "network_accessed": True,
            "target_code_count": 5000,
            "live_primary_row_count": 5000,
            "fallback_requested_count": 0,
            "fallback_returned_count": 0,
            "partition_replaced": True,
            "partition_verified": True,
            "partition_sha256": "b" * 64,
            "source_counts": {"east_push2delay": 5000},
        })
        calls.append({key: value for key, value in kwargs.items() if key != "execution_evidence"})
        return 5000

    monkeypatch.setattr(
        flow,
        "refresh_flow",
        fake_refresh,
    )
    monkeypatch.setattr(
        flow.sys,
        "argv",
        ["crawl_realtime_batch.py", "--only", "flow", "--json"],
    )

    assert flow.main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == flow.CAPITAL_FLOW_RESULT_SCHEMA
    assert payload["trade_date"] == payload["source_trade_date"] == TARGET
    assert payload["source_timestamp_required"] is True
    assert payload["build_sha"] == BUILD_SHA
    assert payload["execution_mode"] == flow.CAPITAL_FLOW_EXECUTION_CURRENT_LIVE
    assert payload["captured_at"] == payload["execution"]["captured_at"]
    assert payload["partition_sha256"] == payload["execution"]["partition_sha256"]
    assert payload["source_counts"] == {"east_push2delay": 5000}
    assert scheduler_validation.scheduler_output_status(
        {
            "task_type": "capital_flow_batch_fast",
            "_scheduler_expected_build_sha": BUILD_SHA,
        },
        json.dumps(payload),
        return_code=0,
    ) == "success"
    assert calls == [
        {
            "trade_date": TARGET,
            "min_coverage": 0.0,
            "require_source_date": True,
            "reuse_verified_existing": False,
        }
    ]
