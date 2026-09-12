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
        "source": exact.QMT_PROVIDER_ID,
        "source_method": "ContextInfo.get_market_data_ex_ori",
        "download_method": "download_history_data2",
        "count": -1,
        "fill_data": False,
        "subscribe": False,
        "native_fields": list(exact.NATIVE_FIELDS),
        "strategy_release_protocol": "probiga.bigqmt-strategy-release.v2",
        "strategy_identity_protocol": "probiga.bigqmt-loaded-strategy-identity.v1",
        "strategy_identity_frozen": True,
        "strategy_identity_status": "BOUND",
        "strategy_build_sha": BUILD_SHA,
        "strategy_git_blob": "b" * 40,
        "strategy_source_sha256": "c" * 64,
        "strategy_artifact_sha256": "d" * 64,
        "strategy_loaded_identity_sha256": "e" * 64,
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
        **(runtime or _runtime_identity()),
        "schema": "probiga.bigqmt-minute-flow-capture.v1",
        "action": "minute_flow_exact",
        "status": "ok",
        "request_id": "request-" + codes[0],
        "model_instance_id": "model-1",
        "period": exact.PERIOD,
        "trade_date": TRADE_DATE,
        "requested_qmt_code_count": len(codes),
        "requested_qmt_code_set_hash": exact._qmt_code_set_hash(codes),
        "row_count": len(rows),
        "rows": rows,
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
                "frozen_model": dict(runtime),
                "release_proof": {"test_release": BUILD_SHA},
                "period": exact.PERIOD,
                "count": -1,
                "fill_data": False,
                "qmt_runtime": runtime,
            },
            "collection": proof,
            "database": dict(proof),
        }
    )
    raw = _response()
    response_proof = exact.SourceResponseProof(["000001.SZ"])
    response_proof.add(raw, requested_qmt_codes=["000001.SZ"], runtime_identity=runtime, trade_date=TRADE_DATE)
    payload.pop("receipt_id")
    payload.update(source_response_proof=response_proof.finish())
    payload = exact._signed(payload)
    assert exact.validate_task_result(payload, 0) == "complete"
    tampered = json.loads(json.dumps(payload, default=str))
    tampered.pop("receipt_id")
    tampered["source_response_proof"]["qmt_code_set_hash"] = exact._qmt_code_set_hash(["000001.SH"])
    assert exact.validate_task_result(exact._signed(tampered), 0) == "failed"

    fake_minute_engine = SimpleNamespace(
        connect=lambda: nullcontext(object()),
        dispose=lambda: None,
    )
    monkeypatch.setattr(exact, "_git_head", lambda: BUILD_SHA)
    monkeypatch.setattr(exact, "validate_bigqmt_strategy_release", lambda *_a, **_k: {"test_release": BUILD_SHA})
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

    # Even a self-consistently re-signed market suffix claim must be checked
    # against the immutable catalog again on the persisted/Linux read path.
    wrong_market = json.loads(exact._canonical_json(payload))
    wrong_market.pop("receipt_id")
    wrong_hash = exact._qmt_code_set_hash(["000001.SH"])
    wrong_market["universe"]["qmt_code_set_hash"] = wrong_hash
    wrong_market["source_response_proof"]["qmt_code_set_hash"] = wrong_hash
    with pytest.raises(exact.MinuteFlowDataBlocked, match="prerequisite universe differs"):
        exact.validate_persisted_result(object(), exact._signed(wrong_market),
            minute_engine=fake_minute_engine,
            now=datetime(2026, 8, 27, 17, 59, tzinfo=exact.SHANGHAI),
            expected_session=TRADE_DATE)

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
        "qmt_runtime": _runtime_identity(fill_data=True),
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


def _flow_recovery_run(monkeypatch, *, failing_calls, recovered=True, drift=False,
                       failing_identity_calls=(), nonzero=True, identity_extra=None):
    # Three stocks across two real normalizer batches prove that retry does
    # not discard/re-fetch already staged market rows.
    monkeypatch.setattr(exact, "CODE_BATCH_SIZE", 2)
    symbols = ("000001.SZ", "000002.SZ", "000003.SZ")
    universe = SimpleNamespace(
        qmt_codes=symbols,
        qmt_by_stock={code[:6]: code for code in symbols},
        catalog={"manifest_hash": "c" * 64},
        daily_truth={"truth_hash": "d" * 64},
        traded_stock_count=3,
        traded_stock_set_hash=exact._code_set_hash([code[:6] for code in symbols]),
        receipt=lambda: {},
    )
    events = []
    calls = []
    recovery_calls = []

    class Worker:
        identity_calls = 0

        def identity(self):
            self.identity_calls += 1
            if self.identity_calls in failing_identity_calls:
                raise exact._MinuteFlowConnectionUnavailable("model proof transport unavailable")
            return {"worker_sha256": "changed" if drift and recovery_calls else "fixed", **(identity_extra or {})}

        def fetch(self, codes, *, trade_date):
            calls.append((tuple(codes), trade_date))
            events.append("fetch")
            if len(calls) in failing_calls:
                raise exact._MinuteFlowConnectionUnavailable("native connection failed")
            return _response(codes, nonzero=nonzero)

    def recover():
        recovery_calls.append(True)
        events.append("recover")
        return recovered

    connection = SimpleNamespace(close=lambda: events.append("close"))
    monkeypatch.setattr(exact, "resolve_build_sha", lambda _v: BUILD_SHA)
    monkeypatch.setattr(exact, "_validate_executor", lambda: None)
    monkeypatch.setattr(exact, "validate_runtime_schema", lambda *_a: {"schema_hash": "s"})
    monkeypatch.setattr(exact, "load_flow_universe", lambda *_a, **_k: universe)
    monkeypatch.setattr(exact, "_recover_qmt_session_after_failure", recover)
    monkeypatch.setattr(exact, "_create_stage", lambda _c: "stage")
    monkeypatch.setattr(exact, "_append_stage", lambda _c, **k: events.append(("append", len(k["rows"]))))

    def publish(*_a, **k):
        events.append(("publish", k["expected"]["row_count"]))
        return k["expected"]

    monkeypatch.setattr(exact, "_publish_stage", publish)
    invoke = lambda: exact.run_sync(
        object(), SimpleNamespace(connect=lambda: connection), trade_date=TRADE_DATE,
        apply=True, expected_build_sha=BUILD_SHA, provider=Worker(),
        now=datetime(2026, 8, 26, 16, 20), batch_size=2,
    )
    return invoke, calls, recovery_calls, events


def test_connection_recovery_retries_only_failed_batch_preserving_stage(monkeypatch):
    invoke, calls, recovery, events = _flow_recovery_run(monkeypatch, failing_calls={2})
    result = invoke()
    assert result["status"] == "PASS"
    assert calls == [(("000001.SZ", "000002.SZ"), TRADE_DATE), (("000003.SZ",), TRADE_DATE), (("000003.SZ",), TRADE_DATE)]
    assert recovery == [True]
    assert events == ["fetch", ("append", 482), "fetch", "recover", "fetch", ("append", 241), ("publish", 723), "close"]


@pytest.mark.parametrize("fail_identity", [{1}, {2}])
def test_initial_or_final_proof_recovery_does_not_refetch_completed_batches(monkeypatch, fail_identity):
    invoke, calls, recovery, events = _flow_recovery_run(
        monkeypatch, failing_calls=set(), failing_identity_calls=fail_identity,
    )
    assert invoke()["status"] == "PASS"
    assert len(calls) == 2 and recovery == [True]
    assert events[-2:] == [("publish", 723), "close"]


def test_zero_vip_values_do_not_publish_or_trigger_login(monkeypatch):
    invoke, calls, recovery, events = _flow_recovery_run(monkeypatch, failing_calls=set(), nonzero=False)
    with pytest.raises(exact.MinuteFlowDataBlocked, match="lacks nonzero VIP"):
        invoke()
    assert len(calls) == 2 and recovery == []
    assert not any(isinstance(event, tuple) and event[0] == "publish" for event in events)


def test_proof_and_collection_share_one_recovery_budget(monkeypatch):
    invoke, calls, recovery, events = _flow_recovery_run(
        monkeypatch, failing_calls={2}, failing_identity_calls={1},
    )
    with pytest.raises(exact._MinuteFlowConnectionUnavailable):
        invoke()
    assert len(calls) == 2 and recovery == [True]
    assert events[-1] == "close"


@pytest.mark.parametrize("failing_calls,recovered,drift,fetch_count", [
    ({2}, False, False, 2),
    ({2, 3}, True, False, 3),
    ({1, 3}, True, False, 3),
    ({2}, True, True, 2),
])
def test_connection_recovery_fails_closed_without_repeated_login_or_publish(
    monkeypatch, failing_calls, recovered, drift, fetch_count,
):
    invoke, calls, recovery, events = _flow_recovery_run(
        monkeypatch, failing_calls=failing_calls, recovered=recovered, drift=drift,
    )
    with pytest.raises(exact.MinuteFlowDataBlocked):
        invoke()
    assert len(calls) == fetch_count
    assert recovery == [True]
    assert not any(isinstance(event, tuple) and event[0] == "publish" for event in events)
    assert events[-1] == "close"


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


@pytest.fixture(scope="module")
def full_market_receipt():
    """Real production cardinality and native-shaped batches, without live calls."""
    codes = tuple(f"{number:06d}.SZ" for number in range(1, 5550))
    runtime = _runtime_identity()
    builder = exact.SourceResponseProof(codes)
    for batch_index, batch in enumerate(exact._chunks(codes, exact.CODE_BATCH_SIZE)):
        response = _response(tuple(batch))
        response["request_id"] = f"1789250123456789012_53860_{batch_index:010d}"
        response["model_instance_id"] = "01234567-89ab-cdef-0123-456789abcdef" if batch_index < 70 else "fedcba98-7654-3210-fedc-ba9876543210"
        builder.add(response, requested_qmt_codes=batch, runtime_identity=runtime, trade_date=TRADE_DATE)
    universe = exact.FlowUniverse(
        trade_date=TRADE_DATE, qmt_by_stock={code[:6]: code for code in codes},
        catalog={"batch_id": "qmt_reference_" + "1" * 40, "manifest_hash": "2" * 64,
                 "member_set_hash": "3" * 64, "captured_at": "2026-08-26 15:30:00",
                 "history_complete_from": "1991-01-01"},
        daily_truth={"run_id": "4" * 36, "run_finished_at": "2026-08-26 16:00:00",
                     "calendar_batch_id": "calendar_" + "5" * 40,
                     "calendar_manifest_hash": "6" * 64, "truth_hash": "7" * 64},
        all_stock_count=5562, traded_stock_count=len(codes),
        traded_stock_set_hash=exact._code_set_hash(code[:6] for code in codes),
    )
    collection = {"row_count": len(codes) * len(exact.GRID), "row_hash": "8" * 64,
                  "code_count": len(codes), "code_set_hash": universe.traded_stock_set_hash,
                  "minute_grid_profile": exact.QMT_MINUTE_GRID_PROFILE,
                  "minute_grid_count": len(exact.GRID), "minute_grid_hash": exact.GRID_HASH,
                  "nonzero_code_count": len(codes), "nonzero_code_ratio": 1.0}
    frozen = {key: runtime.get(key) for key in exact.FROZEN_CAPABILITY_FIELDS}
    frozen.update(status="ok", source=exact.QMT_PROVIDER_ID, bridge_version="bigqmt_inner_v2",
                  read_only=True, simulation_only=True, automatic_real_order_submission=False,
                  real_order_authority=False, actions=["minute_flow_exact", "trading_calendar"],
                  native_capabilities=[{"capability": name, "action": name, "available": True,
                                        "source_method": "ContextInfo.get_trading_dates" if name == "trading_calendar" else "ContextInfo.get_weight_in_index"}
                                       for name in ("index_weight", "trading_calendar")])
    release = {key: frozen[key] for key in exact.FROZEN_IDENTITY_FIELDS}
    release.update(schema="probiga.bigqmt-strategy-release-proof.v2",
                   compatible_app_build_sha=BUILD_SHA, strategy_compatibility_status="EXACT_BUILD",
                   read_only=True, simulation_only=True, automatic_real_order_submission=False,
                   real_order_authority=False,
                   trading_calendar=frozen["native_capabilities"][1], index_weight=frozen["native_capabilities"][0])
    return exact._bounded_signed_receipt({
        "schema": exact.RESULT_SCHEMA, "status": "PASS", "task_type": exact.TASK_TYPE,
        "dataset": "stock_minute_capital_flow", "executor_owner": exact.EXECUTOR_OWNER,
        "provider": exact.PROVIDER_ID, "trade_date": TRADE_DATE, "build_sha": BUILD_SHA,
        "started_at": "2026-08-26T16:20:00+08:00", "finished_at": "2026-08-26T16:59:59+08:00",
        "batch_id": "9" * 64, "runtime_schema_hash": "a" * 64,
        "universe": universe.receipt(), "collection": collection, "database": dict(collection),
        "source_identity": {"build_sha": BUILD_SHA, "period": exact.PERIOD, "count": -1,
                            "fill_data": False, "frozen_model": frozen, "release_proof": release,
                            "qmt_runtime": runtime},
        "source_response_proof": builder.finish(),
    })


def test_full_5549_stock_receipt_fits_unmodified_scheduler_history(full_market_receipt):
    from server.api.scheduler_runtime import _history_validation_replay_output, _HISTORY_REPLAY_OUTPUT_LIMIT
    receipt = full_market_receipt
    assert len(receipt["source_response_proof"]["batches"]) == 139
    assert receipt["collection"]["row_count"] == 5549 * 241
    wire = exact._canonical_json(receipt)
    assert len(wire.encode("utf-8")) <= exact.RECEIPT_MAX_BYTES == _HISTORY_REPLAY_OUTPUT_LIMIT == 24_000
    assert json.loads(_history_validation_replay_output(wire)) == receipt
    assert exact.validate_task_result(receipt, 0) == "complete"


@pytest.mark.parametrize("change", ["duplicate_request", "missing_batch", "extra_batch", "negative_model",
    "bool_model", "unknown_model", "model_reversal", "duplicate_model", "unused_model", "third_model",
    "wrong_market_hash", "wrong_count", "wrong_batch_size", "wrong_columns", "bad_row_hash", "unknown_key"])
def test_compact_proof_rejects_contradictory_resigned_metadata(full_market_receipt, change):
    payload = json.loads(exact._canonical_json(full_market_receipt))
    payload.pop("receipt_id")
    proof = payload["source_response_proof"]
    batches = proof["batches"]
    if change == "duplicate_request": batches[1][0] = batches[0][0]
    elif change == "missing_batch": batches.pop()
    elif change == "extra_batch": batches.append(list(batches[-1]))
    elif change == "negative_model": batches[0][1] = -1
    elif change == "bool_model": batches[0][1] = False
    elif change == "unknown_model": batches[0][1] = 2
    elif change == "model_reversal": batches[-1][1] = 0
    elif change == "duplicate_model": proof["model_instances"][1] = proof["model_instances"][0]
    elif change == "unused_model":
        for batch in batches: batch[1] = 0
    elif change == "third_model": proof["model_instances"].append("third-model")
    elif change == "wrong_market_hash": proof["qmt_code_set_hash"] = exact._qmt_code_set_hash(["000001.SH"])
    elif change == "wrong_count": proof["requested_qmt_code_count"] -= 1
    elif change == "wrong_batch_size": proof["batch_size"] = 41
    elif change == "wrong_columns": proof["columns"] = list(reversed(proof["columns"]))
    elif change == "bad_row_hash": batches[0][2] = "z" * 64
    elif change == "unknown_key": proof["unvalidated"] = True
    assert exact.validate_task_result(exact._signed(payload), 0) == "failed"


def test_row_hash_tamper_invalidates_signed_receipt(full_market_receipt):
    payload = json.loads(exact._canonical_json(full_market_receipt))
    payload["source_response_proof"]["batches"][0][2] = "0" * 64
    assert exact.validate_task_result(payload, 0) == "failed"


@pytest.mark.parametrize("change", ["market", "row_count", "code_count", "frozen", "request", "model", "batch_order"])
def test_compact_builder_requires_exact_full_native_header_and_catalog_slice(change):
    codes = ("000001.SZ", "600000.SH")
    response = _response(codes)
    requested = codes
    if change == "market": response["requested_qmt_code_set_hash"] = exact._qmt_code_set_hash(["000001.SH", "600000.SH"])
    elif change == "row_count": response["row_count"] -= 1
    elif change == "code_count": response["requested_qmt_code_count"] = "2"
    elif change == "frozen": response["strategy_source_sha256"] = "0" * 64
    elif change == "request": response["request_id"] = ""
    elif change == "model": response["model_instance_id"] = False
    elif change == "batch_order": requested = tuple(reversed(codes))
    builder = exact.SourceResponseProof(codes)
    with pytest.raises(exact.MinuteFlowDataBlocked, match="compact response evidence differs"):
        builder.add(response, requested_qmt_codes=requested, runtime_identity=_runtime_identity(), trade_date=TRADE_DATE)


def test_oversized_source_metadata_fails_before_publication(monkeypatch):
    invoke, _calls, recovery, events = _flow_recovery_run(monkeypatch, failing_calls=set(),
        identity_extra={"oversized_source_metadata": "原" * 9_000})
    with pytest.raises(exact.MinuteFlowDataBlocked, match="receipt exceeds 24000-byte"):
        invoke()
    assert not recovery
    assert not any(isinstance(event, tuple) and event[0] == "publish" for event in events)
    assert events[-1] == "close"
