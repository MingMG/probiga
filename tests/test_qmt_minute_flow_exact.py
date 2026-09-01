from __future__ import annotations

import ast
from contextlib import nullcontext
from datetime import datetime
import importlib.util
import json
import sys
from types import ModuleType, SimpleNamespace

import pytest
from sqlalchemy import create_engine, text

from server.common.scheduler_validation import (
    scheduler_output_status,
    validate_scheduler_task_result,
)
from tools import sync_qmt_minute_flow_exact as exact


BUILD_SHA = "a" * 40
TRADE_DATE = "2026-08-26"


def _runtime_identity(**overrides):
    identity = {
        "connection_port": 58611,
        "sdk_module": "C:/QMT/xtquant/xtdata.py",
        "sdk_version": "1.0",
        "download_method": "download_history_data2",
        "count": -1,
        "fill_data": True,
        "fields": list(exact.NATIVE_FIELDS),
    }
    identity.update(overrides)
    return identity


def _response(codes=("000001.SZ",), *, times=None, runtime=None, nonzero=True):
    minute_times = list(exact.GRID if times is None else times)
    rows = []
    for qmt_code in codes:
        for minute in minute_times:
            maximum = 1 if nonzero else 0
            large = 2 if nonzero else 0
            rows.append(
                {
                    "qmt_code": qmt_code,
                    "stock_code": qmt_code[:6],
                    "trade_time": f"{TRADE_DATE} {minute}",
                    "netInflowMostAmount": maximum,
                    "netInflowBigAmount": large,
                    "netInflowMediumAmount": 3 if nonzero else 0,
                    "netInflowSmallAmount": 4 if nonzero else 0,
                }
            )
    return {
        "ok": True,
        "provider": exact.QMT_PROVIDER_ID,
        "period": exact.PERIOD,
        "trade_date": TRADE_DATE,
        "requested_qmt_code_count": len(codes),
        "requested_qmt_code_set_hash": exact._qmt_code_set_hash(codes),
        "row_count": len(rows),
        "rows": rows,
        "source_identity": runtime or _runtime_identity(),
    }


def _normalized(codes=("000001.SZ",), **response_kwargs):
    response = _response(codes, **response_kwargs)
    return exact.normalize_flow_batch(
        response,
        expected_qmt_codes=codes,
        qmt_to_stock={code: code[:6] for code in codes},
        trade_date=TRADE_DATE,
        observed_at=datetime(2026, 8, 26, 16, 20),
        batch_id="b" * 64,
        build_sha=BUILD_SHA,
    )


def test_flow_universe_uses_attested_catalog_and_honors_proven_no_row(monkeypatch):
    truth = SimpleNamespace(
        catalog_batch_id="attested-catalog",
        catalog_manifest_hash="a" * 64,
        catalog_member_set_hash="b" * 64,
        attested_row_count=1,
        requested_sessions=(TRADE_DATE,),
        no_row_exception_proof_sha256="c" * 64,
        run_id="run-1",
        run_finished_at="2026-08-26 16:00:00",
        calendar_batch_id="calendar-1",
        calendar_manifest_hash="d" * 64,
        truth_hash="e" * 64,
    )
    catalog = SimpleNamespace(
        batch_id="attested-catalog",
        manifest_hash="a" * 64,
        member_set_hash="b" * 64,
        captured_at="2026-08-26 15:30:00",
        history_complete_from="2026-01-01",
        members=(
            {
                "stock_code": "000001",
                "qmt_code": "000001.SZ",
                "list_date": "1991-04-03",
                "expire_date": None,
            },
            {
                "stock_code": "000002",
                "qmt_code": "000002.SZ",
                "list_date": "1991-01-29",
                "expire_date": None,
            },
        ),
    )
    daily_rows = [
        {
            "stock_code": "000001",
            "volume": 1,
            "amount": 1,
            "data_source": exact.QMT_DAILY_PROVIDER,
            "quality_status": "QMT_ATTESTED",
            "permission_status": "SUPPORTED",
        }
    ]

    class Result:
        def mappings(self):
            return self

        def all(self):
            return daily_rows

    connection = SimpleNamespace(execute=lambda *_args, **_kwargs: Result())
    engine = SimpleNamespace(connect=lambda: nullcontext(connection))
    selected = {}

    def load_catalog(_engine, **kwargs):
        selected.update(kwargs)
        return catalog, ["000001", "000002"]

    monkeypatch.setattr(exact, "validate_stock_catalog_runtime_schema", lambda _engine: None)
    monkeypatch.setattr(exact, "load_qmt_daily_market_truth", lambda *_args, **_kwargs: truth)
    monkeypatch.setattr(exact, "load_target_stock_catalog", load_catalog)

    universe = exact.load_flow_universe(
        engine,
        trade_date=TRADE_DATE,
        now=datetime(2026, 8, 26, 16, 20),
    )

    assert selected["batch_id"] == "attested-catalog"
    assert universe.all_stock_count == 1
    assert universe.qmt_by_stock == {"000001": "000001.SZ"}

    truth.no_row_exception_proof_sha256 = None
    with pytest.raises(exact.MinuteFlowDataBlocked, match="daily partition differs"):
        exact.load_flow_universe(
            engine,
            trade_date=TRADE_DATE,
            now=datetime(2026, 8, 26, 16, 20),
        )


def test_normalize_requires_every_native_minute_and_builds_cumulative_main_flow():
    rows, identity = _normalized(("000001.SZ", "600000.SH"))

    assert len(rows) == 2 * len(exact.GRID) == 482
    assert rows[0]["trade_time"].strftime("%H:%M:%S") == "09:30:00"
    assert rows[-1]["trade_time"].strftime("%H:%M:%S") == "15:00:00"
    assert rows[0]["main_net_inflow"] == rows[0]["max_net_inflow"] + rows[0]["lg_net_inflow"]
    assert identity == _runtime_identity()
    proof = exact.proof_from_rows(rows)
    assert proof["row_count"] == 482
    assert proof["code_count"] == 2
    assert proof["minute_grid_count"] == 241
    assert proof["minute_grid_hash"] == exact.GRID_HASH
    assert proof["nonzero_code_ratio"] == 1.0


def test_normalize_fails_closed_on_missing_grid_or_unproven_runtime_contract():
    with pytest.raises(exact.MinuteFlowDataBlocked, match="grid differs"):
        _normalized(times=exact.GRID[:-1])

    with pytest.raises(exact.MinuteFlowDataBlocked, match="runtime/source contract"):
        _normalized(runtime=_runtime_identity(count=0))

    response = _response()
    response["requested_qmt_code_set_hash"] = "0" * 64
    with pytest.raises(exact.MinuteFlowDataBlocked, match="identity differs"):
        exact.normalize_flow_batch(
            response,
            expected_qmt_codes=("000001.SZ",),
            qmt_to_stock={"000001.SZ": "000001"},
            trade_date=TRADE_DATE,
            observed_at=datetime(2026, 8, 26, 16, 20),
            batch_id="b" * 64,
            build_sha=BUILD_SHA,
        )


def test_streaming_proof_rejects_duplicate_order_and_bad_main_accounting():
    rows, _ = _normalized()
    accumulator = exact.FlowProofAccumulator()
    accumulator.add(rows[0])
    with pytest.raises(exact.MinuteFlowDataBlocked, match="duplicated"):
        accumulator.add(rows[0])

    broken = dict(rows[0])
    broken["main_net_inflow"] = 999
    accumulator = exact.FlowProofAccumulator()
    with pytest.raises(exact.MinuteFlowDataBlocked, match=r"max\+large"):
        accumulator.add(broken)


def test_signed_scheduler_result_binds_universe_source_grid_and_runtime(monkeypatch):
    rows, runtime = _normalized()
    proof = exact.proof_from_rows(rows)
    universe_object = exact.FlowUniverse(
        trade_date=TRADE_DATE,
        qmt_by_stock={"000001": "000001.SZ"},
        catalog={
            "batch_id": "catalog",
            "manifest_hash": "d" * 64,
            "member_set_hash": "e" * 64,
            "captured_at": "2026-08-26 15:30:00",
            "history_complete_from": "2026-01-01",
        },
        daily_truth={
            "run_id": "run",
            "run_finished_at": "2026-08-26 16:00:00",
            "calendar_batch_id": "calendar",
            "calendar_manifest_hash": "f" * 64,
            "truth_hash": "1" * 64,
        },
        all_stock_count=1,
        traded_stock_count=1,
        traded_stock_set_hash=exact._code_set_hash(["000001"]),
    )
    payload = exact._signed(
        {
            "schema": exact.RESULT_SCHEMA,
            "status": "PASS",
            "task_type": exact.TASK_TYPE,
            "dataset": "stock_minute_capital_flow",
            "executor_owner": exact.EXECUTOR_OWNER,
            "provider": exact.PROVIDER_ID,
            "trade_date": TRADE_DATE,
            "build_sha": BUILD_SHA,
            "finished_at": "2026-08-26T16:20:00+08:00",
            "universe": universe_object.receipt(),
            "source_identity": {
                "build_sha": BUILD_SHA,
                "worker_sha256": "c" * 64,
                "period": exact.PERIOD,
                "count": -1,
                "fill_data": True,
                "qmt_runtime": runtime,
            },
            "collection": proof,
            "database": dict(proof),
        }
    )
    assert exact.validate_task_result(payload, 0) == "complete"

    fake_minute_engine = SimpleNamespace(
        connect=lambda: nullcontext(object()),
        dispose=lambda: None,
    )
    monkeypatch.setattr(exact, "_git_head", lambda: BUILD_SHA)
    monkeypatch.setattr(exact, "load_flow_universe", lambda *_args, **_kwargs: universe_object)
    monkeypatch.setattr(exact, "validate_runtime_schema", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(exact, "_stream_table_proof", lambda *_args, **_kwargs: proof)
    monkeypatch.setattr(exact, "get_minute_engine", lambda: fake_minute_engine)
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", BUILD_SHA)
    rendered = json.dumps(payload, default=str)
    assert scheduler_output_status(
        {"task_type": exact.TASK_TYPE},
        rendered,
        return_code=0,
    ) == "success"
    scheduler_proof = validate_scheduler_task_result(
        {
            "task_type": exact.TASK_TYPE,
            "_release_target_date": TRADE_DATE,
            "_trigger_source": "release_catchup",
        },
        engine=object(),
        output=rendered,
        started_at=datetime(2026, 8, 26, 16, 19),
        now=datetime(2026, 8, 26, 16, 25),
    )
    assert scheduler_proof.checked and scheduler_proof.ok
    mismatched_scheduler_proof = validate_scheduler_task_result(
        {
            "task_type": exact.TASK_TYPE,
            "_release_target_date": "2026-08-27",
            "_trigger_source": "release_catchup",
        },
        engine=object(),
        output=rendered,
        started_at=datetime(2026, 8, 26, 16, 19),
        now=datetime(2026, 8, 26, 16, 25),
    )
    assert mismatched_scheduler_proof.checked
    assert not mismatched_scheduler_proof.ok
    assert "receipt session differs" in mismatched_scheduler_proof.message
    persisted = exact.validate_persisted_result(
        object(),
        payload,
        minute_engine=fake_minute_engine,
        now=datetime(2026, 8, 27, 17, 59, tzinfo=exact.SHANGHAI),
        expected_session=TRADE_DATE,
    )
    assert persisted["row_hash"] == proof["row_hash"]

    with pytest.raises(
        exact.MinuteFlowDataBlocked,
        match="stale QMT minute-flow session",
    ):
        exact.validate_persisted_result(
            object(),
            payload,
            minute_engine=fake_minute_engine,
            now=datetime(2026, 8, 27, 18, 0, tzinfo=exact.SHANGHAI),
            expected_session="2026-08-27",
        )

    invalid = dict(payload)
    invalid["source_identity"] = {
        **payload["source_identity"],
        "qmt_runtime": _runtime_identity(fill_data=False),
    }
    invalid.pop("receipt_id")
    invalid = exact._signed(invalid)
    assert exact.validate_task_result(invalid, 0) == "failed"


def test_latest_session_resolution_excludes_an_unclosed_current_day():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text("CREATE TABLE si_trade_calendar (trade_date DATE, trade_status INTEGER)")
        )
        connection.execute(
            text(
                "INSERT INTO si_trade_calendar VALUES "
                "('2026-08-25',1),('2026-08-26',1)"
            )
        )

    assert exact.resolve_requested_trade_date(
        engine,
        trade_date="",
        latest_session=True,
        now=datetime(2026, 8, 26, 15, 0, tzinfo=exact.SHANGHAI),
    ) == "2026-08-25"
    assert exact.resolve_requested_trade_date(
        engine,
        trade_date="",
        latest_session=True,
        now=datetime(2026, 8, 26, 15, 20, tzinfo=exact.SHANGHAI),
    ) == "2026-08-26"


def test_latest_minute_flow_session_uses_close_cutoff_and_calendar_weekends():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text("CREATE TABLE si_trade_calendar (trade_date DATE, trade_status INTEGER)")
        )
        connection.execute(
            text(
                "INSERT INTO si_trade_calendar VALUES "
                "('2026-08-26',1),('2026-08-27',1),('2026-08-28',1)"
            )
        )

    cases = (
        (datetime(2026, 8, 27, 0, 0, tzinfo=exact.SHANGHAI), "2026-08-26"),
        (datetime(2026, 8, 27, 8, 0, tzinfo=exact.SHANGHAI), "2026-08-26"),
        (datetime(2026, 8, 27, 15, 9, tzinfo=exact.SHANGHAI), "2026-08-26"),
        (datetime(2026, 8, 27, 15, 10, tzinfo=exact.SHANGHAI), "2026-08-27"),
        (datetime(2026, 8, 29, 8, 0, tzinfo=exact.SHANGHAI), "2026-08-28"),
    )
    for now, expected in cases:
        assert exact.resolve_requested_trade_date(
            engine,
            trade_date="",
            latest_session=True,
            now=now,
        ) == expected


def test_worker_download_and_query_are_strict_and_use_full_history_count():
    source = exact.WORKER.read_text(encoding="utf-8")
    module = ast.parse(source)
    dispatch = next(
        node
        for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name == "dispatch"
    )
    rendered = ast.unparse(dispatch)

    assert "count=-1" in rendered
    assert "fill_data=True" in rendered
    assert "transactioncount1m" not in rendered  # dispatch must use the pinned constant
    assert "eastmoney" not in source.lower()
    assert not any(
        isinstance(node, ast.ExceptHandler) and len(node.body) == 1
        and isinstance(node.body[0], ast.Pass)
        for node in ast.walk(dispatch)
    )


def test_worker_timestamp_accepts_qmt_compact_and_epoch_labels(monkeypatch):
    fake_xtquant = ModuleType("xtquant")
    fake_xtquant.__version__ = "test"
    fake_xtquant.xtdata = SimpleNamespace()
    monkeypatch.setitem(sys.modules, "xtquant", fake_xtquant)
    monkeypatch.setitem(sys.modules, "xtquant.xtdata", fake_xtquant.xtdata)
    spec = importlib.util.spec_from_file_location(
        "_qmt_minute_flow_exact_worker_test", exact.WORKER
    )
    assert spec is not None and spec.loader is not None
    worker_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(worker_module)

    assert worker_module._timestamp(20260826093000).strftime(
        "%Y-%m-%d %H:%M:%S"
    ) == "2026-08-26 09:30:00"
    epoch_ms = int(
        datetime(2026, 8, 26, 9, 30, tzinfo=exact.SHANGHAI).timestamp() * 1000
    )
    assert worker_module._timestamp(epoch_ms).strftime(
        "%Y-%m-%d %H:%M:%S"
    ) == "2026-08-26 09:30:00"


def test_exact_worker_request_is_date_and_code_bound():
    calls = []

    def runner(*args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(
            returncode=0,
            stdout=json.dumps({"ok": True, "rows": []}),
            stderr="",
        )

    worker = exact.ExactQmtFlowWorker(
        expected_build_sha=BUILD_SHA,
        python_path=exact.Path(__import__("sys").executable),
        runner=runner,
    )
    result = worker.fetch(["600000.SH", "000001.SZ"], trade_date=TRADE_DATE)

    assert result["ok"] is True
    request = json.loads(calls[0][1]["input"])
    assert request == {
        "action": "flow_min_exact",
        "history_wait_seconds": 1.0,
        "qmt_codes": ["000001.SZ", "600000.SH"],
        "trade_date": TRADE_DATE,
    }
    assert calls[0][1]["timeout"] == exact.WORKER_TIMEOUT_SECONDS


def test_transient_qmt_unavailability_retries_but_entitlement_gap_blocks():
    transient = exact._failure(
        trade_date=TRADE_DATE,
        error=exact.MinuteFlowDataBlocked(
            "DATA_BLOCKED: QMT minute-flow source unavailable: terminal disconnected"
        ),
    )
    terminal = exact._failure(
        trade_date=TRADE_DATE,
        error=exact.MinuteFlowDataBlocked(
            "DATA_BLOCKED: QMT transactioncount1m lacks nonzero VIP field evidence"
        ),
    )

    assert transient["retryable"] is True
    assert exact.validate_task_result(transient, 2) == "failed"
    assert terminal["retryable"] is False
    assert exact.validate_task_result(terminal, 2) == "blocked"
