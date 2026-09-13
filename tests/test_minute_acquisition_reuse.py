import json
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from server.common import minute_acquisition_reuse as reuse
from server.common import scheduler_validation as scheduler

DAY = "2026-09-11"
NOW = datetime(2026, 9, 13, 19)
BUILD = "a" * 40


@pytest.fixture(autouse=True)
def clock(monkeypatch):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW.replace(tzinfo=tz)

    monkeypatch.setattr(reuse, "datetime", Clock)
    monkeypatch.setenv("PROBIGA_SCHEDULER_HISTORY_RUN_UID", "b" * 32)


def inventory_row(code="000001", *, kind="stock", native=False):
    source, size = list(reuse.SOURCES[kind].items())[int(native)]
    return dict(stock_code=code, data_source=source, row_count=size,
                time_count=size, source_count=1, invalid_count=0, has_nonzero=1,
                grid_hash=reuse.GRID_HASHES[size], row_hash="c" * 64, hashed_bytes=size * 64)


def partition(kind="stock", day=DAY, native=False):
    proof = reuse.validate_inventory([inventory_row(kind=kind, native=native)],
                                    kind=kind, expected_codes=["000001"], no_trade_codes=[])
    return dict(dataset=kind, trade_date=day, table=reuse.TABLES[kind],
                decision_known_at=NOW.isoformat(sep=" "), catalog_batch_id="frozen-catalog",
                catalog_manifest_hash="d" * 64, expected_stock_count=1,
                expected_stock_set_hash=reuse.canonical_digest(["000001"]),
                native_no_trade_evidence=None, **proof)


def receipt(kind="stock", task_type=None):
    return reuse.result_for([partition(kind)], task_type=task_type or (
        "qmt_stock_minute_canonical" if kind == "stock" else "qmt_stock_minute_flow_canonical"),
        build_sha=BUILD, started_at=NOW)


@pytest.mark.parametrize("kind", ["stock", "flow"])
@pytest.mark.parametrize("native", [False, True])
def test_complete_provider_grid_is_reusable_with_real_source(kind, native):
    row = inventory_row(kind=kind, native=native)
    proof = reuse.validate_inventory([row], kind=kind, expected_codes=["000001", "000002"],
                                    no_trade_codes=["000002"])
    assert proof["row_count"] == row["row_count"]
    assert proof["source_rows"] == {row["data_source"]: row["row_count"]}
    assert proof["no_trade_count"] == 1


@pytest.mark.parametrize("change", [
    {"row_count": 239}, {"time_count": 239}, {"source_count": 2}, {"invalid_count": 1},
    {"grid_hash": "0" * 64}, {"hashed_bytes": 1024}, {"row_hash": None},
    {"stock_code": "000002"}, {"data_source": "unknown"},
])
def test_incomplete_duplicate_wrong_time_or_invalid_values_require_collection(change):
    row = {**inventory_row(), **change}
    assert reuse.validate_inventory([row], kind="stock", expected_codes=["000001"], no_trade_codes=[]) is None


def test_missing_stock_and_uncertified_suspension_are_not_complete():
    row = inventory_row()
    assert reuse.validate_inventory([row], kind="stock", expected_codes=["000001", "000002"], no_trade_codes=[]) is None
    assert reuse.validate_inventory([row, row], kind="stock", expected_codes=["000001"], no_trade_codes=[]) is None
    assert reuse.validate_inventory([row], kind="stock", expected_codes=["000001"], no_trade_codes=["000002"]) is None


def test_full_zero_flow_grid_cannot_mask_missing_vip_permission():
    rows = [{**inventory_row(kind="flow", native=True), "has_nonzero": 0}]
    assert reuse.validate_inventory(rows, kind="flow", expected_codes=["000001"], no_trade_codes=[]) is None


@pytest.mark.parametrize("change", [
    {"publication_authority": True}, {"network_accessed": True}, {"database_writes": True},
    {"captured_sessions": ["2026-09-10"]}, {"build_sha": "0" * 40},
    {"finished_at": "2026-09-13 18:00:00"},
])
def test_resealed_false_reuse_claims_are_rejected(change):
    result = {**receipt(), **change}
    result.pop("receipt_sha256")
    result["receipt_sha256"] = reuse.canonical_digest(result)
    with pytest.raises(ValueError):
        reuse.validate_result(result, task_type=result["task_type"])


def test_replay_checks_current_values_with_original_catalog_and_decision(monkeypatch):
    from server.common import kline_data
    result = receipt()
    calls = []
    monkeypatch.setattr(kline_data, "get_kline_engine", lambda: "history")

    def inspect(primary, history, **kwargs):
        calls.append((primary, history, kwargs))
        return result["partitions"][0]

    monkeypatch.setattr(reuse, "inspect_complete_partition", inspect)
    reuse.replay_result(result, "primary", now=NOW + timedelta(minutes=1))
    assert calls[0][2]["now"] == NOW
    assert calls[0][2]["catalog_batch_id"] == "frozen-catalog"
    monkeypatch.setattr(reuse, "inspect_complete_partition", lambda *_a, **_k: None)
    with pytest.raises(ValueError, match="changed or is incomplete"):
        reuse.replay_result(result, "primary", now=NOW)


def stock_setup(monkeypatch, sessions):
    from tools import sync_qmt_stock_edge as stock
    monkeypatch.setattr(stock, "_validate_executor", lambda *_a: None)
    monkeypatch.setattr(stock, "_build_sha", lambda *_a: BUILD)
    monkeypatch.setattr(stock, "create_batch_engine", lambda **_k: object())
    monkeypatch.setattr(stock, "get_kline_engine", lambda: object())
    monkeypatch.setattr(stock, "_now", lambda: NOW)
    calendar = SimpleNamespace(batch_id="calendar", manifest_hash="c" * 64, session_set_hash="d" * 64)
    monkeypatch.setattr(stock, "_sessions", lambda *_a, **_k: (calendar, sessions))
    return stock


def test_qmt_stock_complete_public_data_never_connects_or_captures(monkeypatch):
    stock = stock_setup(monkeypatch, [DAY])
    monkeypatch.setattr(reuse, "inspect_complete_partition", lambda *_a, **_k: partition())
    monkeypatch.setattr(stock, "_release", lambda *_a: pytest.fail("QMT connection attempted"))
    monkeypatch.setattr(stock, "run_dataset", lambda *_a, **_k: pytest.fail("source fetch attempted"))
    result = stock.run(dataset="minute", latest_session=True, start_date="", end_date="",
                       expected_build_sha=BUILD, apply=True, now=NOW)
    assert result["status"] == "REUSED"
    assert result["network_accessed"] is result["database_writes"] is False
    assert result["partitions"][0]["source_rows"] == {"east_push2delay": 240}


def test_qmt_stock_mixed_range_only_fetches_missing_session(monkeypatch):
    old = "2026-09-10"
    stock = stock_setup(monkeypatch, [old, DAY])
    fetched = []

    def inspect(*_a, **kw):
        day = kw["trade_date"]
        return partition(day=day, native=day == DAY) if day == old or fetched else None

    def fetch(*_a, **kw):
        fetched.append(kw["date_str"])
        return {"status": "success", "source_policy": "bigqmt_primary"}

    monkeypatch.setattr(reuse, "inspect_complete_partition", inspect)
    monkeypatch.setattr(stock, "_release", lambda *_a: {"identity": "unchanged"})
    monkeypatch.setattr(stock, "_release_identity", lambda value: value)
    monkeypatch.setattr(stock, "run_dataset", fetch)
    monkeypatch.setattr(stock, "_minute_receipt", lambda *_a: {})
    monkeypatch.setattr(stock, "_validate_minute_partition", lambda *_a, **_k: {})
    result = stock.run(dataset="minute", latest_session=False, start_date=old, end_date=DAY,
                       expected_build_sha=BUILD, apply=True, now=NOW)
    assert fetched == [DAY]
    assert result["captured_sessions"] == [DAY]
    assert result["network_accessed"] is result["database_writes"] is True
    assert result["publication_authority"] is False
    assert [p["source_rows"] for p in result["partitions"]] == [
        {"east_push2delay": 240}, {"gj_big_qmt_inner": 241}]


def test_flow_complete_public_data_never_opens_native_worker(monkeypatch):
    from tools import sync_qmt_minute_flow_exact as flow
    monkeypatch.setattr(flow, "resolve_build_sha", lambda *_a: BUILD)
    monkeypatch.setattr(flow, "_validate_executor", lambda: None)
    monkeypatch.setattr(reuse, "inspect_complete_partition", lambda *_a, **_k: partition("flow"))
    monkeypatch.setattr(flow, "validate_runtime_schema", lambda *_a: pytest.fail("capture path entered"))
    result = flow.run_sync(object(), object(), trade_date=DAY, apply=True,
                           expected_build_sha=BUILD, now=NOW)
    assert result["status"] == "REUSED"
    assert result["network_accessed"] is False


@pytest.mark.parametrize("kind", ["stock", "flow"])
@pytest.mark.parametrize("limit", ["0", "1"])
def test_public_cli_complete_partition_skips_all_upstream_work(monkeypatch, capsys, kind, limit):
    from tools import crawl_minute_kline as public
    import sys
    monkeypatch.setenv("PROBIGA_SCHEDULER_BUILD_SHA", BUILD)
    monkeypatch.delenv("PROBIGA_SCHEDULER_TASK_TYPE", raising=False)
    monkeypatch.setattr(sys, "argv", ["crawl_minute_kline", "--type", kind, "--trade-date", DAY, "--limit", limit])
    monkeypatch.setattr(public, "_now", lambda: NOW)
    monkeypatch.setattr(public, "create_batch_engine", lambda: object())
    monkeypatch.setattr(public, "get_kline_engine", lambda: object())
    monkeypatch.setattr(public, "get_minute_engine", lambda: object())
    monkeypatch.setattr(public, "_is_trade_day", lambda *_a: True)
    monkeypatch.setattr(reuse, "inspect_complete_partition", lambda *_a, **_k: partition(kind))
    monkeypatch.setattr(public, "load_target_stock_catalog", lambda *_a, **_k: pytest.fail("source path entered"))
    assert public.main() == 0
    result = json.loads(capsys.readouterr().out)
    assert result["schema"] == reuse.SCHEMA
    assert result["network_accessed"] is result["database_writes"] is False


def task():
    return dict(task_type="qmt_stock_minute_canonical", script_path="tools/sync_qmt_stock_edge.py",
                script_args="--dataset minute --latest-session --apply --json",
                _scheduler_expected_build_sha=BUILD, _scheduler_history_run_uid="b" * 32)


def test_scheduler_requires_current_calendar_and_database_replay(monkeypatch):
    from tools import sync_qmt_stock_edge as stock
    calls = []
    monkeypatch.setattr(stock, "_sessions", lambda *_a, **_k: (None, [DAY]))
    monkeypatch.setattr(reuse, "replay_result", lambda *_a, **_k: calls.append("replay"))
    output = json.dumps(receipt())
    assert scheduler.scheduler_output_status(task(), output, return_code=0) == "success"
    assert scheduler.validate_scheduler_task_result(task(), engine=object(), started_at=NOW, now=NOW, output=output).ok
    assert calls == ["replay"]
    monkeypatch.setattr(stock, "_sessions", lambda *_a, **_k: (None, ["2026-09-10"]))
    assert not scheduler.validate_scheduler_task_result(task(), engine=object(), started_at=NOW, now=NOW, output=output).ok
    assert calls == ["replay"]


@pytest.mark.parametrize("change", [
    {"_scheduler_expected_build_sha": "f" * 40}, {"_scheduler_history_run_uid": "e" * 32},
    {"_scheduler_target_trade_date": "2026-09-10"}, {"script_path": "unrelated.py"},
    {"script_args": "--dataset daily --latest-session --apply"},
    {"script_args": "--dataset minute --latest-session"},
    {"script_args": "--dataset minute --start-date 2026-09-10 --end-date 2026-09-10 --apply"},
])
def test_scheduler_rejects_wrong_execution_or_request(change):
    assert scheduler.scheduler_output_status({**task(), **change}, json.dumps(receipt()), return_code=0) == "failed"


def test_inspector_counts_existing_public_data_without_qmt_attestation(monkeypatch):
    from tools import repair_qmt_canonical_history_gaps as repair
    monkeypatch.setattr(reuse, "inspect_complete_partition", lambda *_a, **_k: partition())
    inspector = repair.CanonicalPartitionInspector(object(), object(), object(),
        window=SimpleNamespace(sessions=[DAY]), decision_time=NOW)
    result = inspector(repair.PartitionRef("stock_minute", DAY))
    assert result["source_rows"] == {"east_push2delay": 240}
    assert result["publication_authority"] is False


def test_history_publisher_accepts_data_completed_after_initial_scan(monkeypatch):
    from tools import repair_qmt_canonical_history_gaps as repair, sync_qmt_stock_edge as stock
    monkeypatch.setattr(stock, "run", lambda **_k: receipt())
    calls = []
    monkeypatch.setattr(reuse, "replay_result", lambda *_a, **_k: calls.append("replay"))
    publisher = repair.ExactPartitionPublisher(object(), expected_build_sha=BUILD, now=NOW)
    result = publisher(repair.PartitionRef("stock_minute", DAY))
    assert result["source_status"] == "REUSED"
    assert result["source_schema"] == reuse.SCHEMA
    assert result["forward_observation_created"] is False
    assert calls == ["replay"]


def test_repair_root_does_not_change_with_inspection_time(monkeypatch):
    from tools import repair_qmt_canonical_history_gaps as repair

    def inspect(*_a, **kw):
        return {**partition(), "decision_known_at": kw["now"].isoformat(),
                "native_no_trade_evidence": {"truth_hash": kw["now"].isoformat(), "proof_sha256": "c" * 64}}

    monkeypatch.setattr(reuse, "inspect_complete_partition", inspect)
    roots = []
    for decision in (NOW, NOW + timedelta(minutes=3)):
        inspector = repair.CanonicalPartitionInspector(object(), object(), object(),
            window=SimpleNamespace(sessions=[DAY]), decision_time=decision)
        roots.append(inspector(repair.PartitionRef("stock_minute", DAY)))
    assert roots[0] == roots[1]


def test_database_probe_uses_second_precision_and_full_catalog_batches(monkeypatch):
    from server.common import qmt_stock_catalog as catalog
    from tools import crawl_minute_kline as public
    expected = [f"{i:06d}" for i in range(1, 202)]
    calls = []
    no_trade = expected[-1:]
    monkeypatch.setattr(public, "_is_trade_day", lambda *_a: True)
    monkeypatch.setattr(catalog, "load_target_stock_catalog", lambda *_a, **_k: (
        SimpleNamespace(batch_id="catalog", manifest_hash="d" * 64), expected))

    def exemptions(*_a, **kw):
        assert kw["decision_known_at"].microsecond == 0
        return set(no_trade), {"proof_sha256": "e" * 64}

    monkeypatch.setattr(public, "verified_no_trade_codes", exemptions)

    class Connection:
        def __enter__(self): return self
        def __exit__(self, *_a): pass
        def exec_driver_sql(self, sql): return SimpleNamespace(scalar_one=lambda: 1024)
        def execute(self, sql, params):
            calls.append(params["codes"])
            rows = [inventory_row(code) for code in params["codes"] if code not in no_trade]
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: rows))

    data = SimpleNamespace(connect=lambda: Connection())
    result = reuse.inspect_complete_partition(object(), data, kind="stock", trade_date=DAY,
                                              now=NOW.replace(microsecond=654321))
    assert list(map(len, calls)) == [100, 100, 1]
    assert sum(calls, []) == expected
    assert result["row_count"] == 200 * 240
    assert result["no_trade_count"] == 1
