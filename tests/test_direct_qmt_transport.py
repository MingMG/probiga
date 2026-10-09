import ast
import datetime as dt
import json
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from acquisition import qmt_model as model
from acquisition import qmt_transport as transport_module
from acquisition.qmt_transport import QmtTransport


AFTER_CLOSE = dt.datetime(2026, 9, 4, 16, 0, tzinfo=model.SHANGHAI)
SYMBOL = "000001.SZ"


def request(request_id="batch_1", dataset="stock_daily", **changes):
    value = {"request_id": request_id, "dataset": dataset, "source": "guojin_qmt",
             "codes": [SYMBOL], "start_date": "2026-09-04", "end_date": "2026-09-04",
             "period": ("1m" if dataset.endswith("minute") else "tick" if dataset.endswith("current")
                        else "transactioncount1d" if dataset == "capital_flow_daily" else "1d"),
             "adjustment": "none", "requested_at": "2026-09-04T15:59:59+08:00",
             "deadline_at": "2026-09-04T16:03:00+08:00"}
    return dict(value, **changes)


def ready(plan, outcomes=None):
    return {"request": plan, "received_at": AFTER_CLOSE.isoformat(),
            "source_method": "ContextInfo.get_market_data_ex",
            "outcomes": outcomes or {code: {"status": "data", "rows": [{"qmt_code": code, "close": 12.5}]}
                                     for code in plan["codes"]}}


def publish_result(root, value):
    model.publish_json(str(root / (value["request"]["request_id"] + ".ready.json")), value,
                       model.MAX_RESULT_BYTES, immutable=True)


def test_model_heartbeat_carries_the_exact_installed_source_identity(tmp_path):
    source_hash = "a" * 64
    instance = model.Model(
        tmp_path,
        clock=lambda: AFTER_CLOSE,
        source_sha256=source_hash,
    )

    instance.heartbeat("idle")

    heartbeat = json.loads((tmp_path / "heartbeat.json").read_text())
    assert heartbeat["status"] == instance.last_status == "idle"
    assert heartbeat["pid"] == os.getpid()
    assert heartbeat["model_source_sha256"] == source_hash
    with pytest.raises(ValueError, match="source sha256"):
        model.Model(tmp_path, source_sha256="not-a-hash")


def test_prepare_activate_result_and_archive_are_idempotent(tmp_path):
    transport = QmtTransport(tmp_path)
    plan = request()
    transport.prepare(plan)
    transport.prepare(plan)
    assert transport.recover()["active"] is None
    transport.activate("batch_1")
    transport.activate("batch_1")
    assert transport.recover()["prepared"] == ["batch_1"]
    assert transport.read_result("batch_1") is None
    value = ready(plan)
    publish_result(tmp_path, value)
    assert transport.read_result("batch_1") == value
    transport.archive("batch_1")  # Simulated caller has committed all outcomes.
    transport.archive("batch_1")
    state = transport.recover()
    assert state["active"] is None and state["prepared"] == state["ready"] == []
    assert state["processed"] == ["batch_1"]
    assert transport.read_result("batch_1") == value
    with pytest.raises(ValueError, match="archived"):
        transport.prepare(plan)


def test_read_request_is_validated_read_only_and_survives_archive(tmp_path):
    transport = QmtTransport(tmp_path)
    plan = request()
    transport.prepare(plan)
    before = transport.recover()
    assert transport.read_request("batch_1") == plan
    assert transport.read_request("missing") is None
    assert transport.recover() == before
    transport.activate("batch_1")
    publish_result(tmp_path, ready(plan))
    transport.archive("batch_1")
    assert transport.read_request("batch_1") == plan
    assert transport.recover()["active"] is None


def test_read_request_rejects_mismatched_immutable_identity(tmp_path):
    transport = QmtTransport(tmp_path)
    model.publish_json(str(tmp_path / "batch_1.prepared.json"), request("batch_2"),
                       model.MAX_REQUEST_BYTES, immutable=True)
    with pytest.raises(ValueError, match="request_id differs"):
        transport.read_request("batch_1")
    assert transport.recover()["active"] is None


def test_immutable_request_and_single_active_even_with_two_callers(tmp_path):
    transport = QmtTransport(tmp_path)
    transport.prepare(request())
    with pytest.raises(ValueError, match="immutable"):
        transport.prepare(request(codes=["000002.SZ"]))
    transport.prepare(request("batch_2"))
    def activate(request_id):
        try:
            transport.activate(request_id)
            return request_id
        except RuntimeError:
            return None
    with ThreadPoolExecutor(max_workers=2) as pool:
        winners = list(pool.map(activate, ["batch_1", "batch_2"]))
    assert sum(value is not None for value in winners) == 1
    assert transport.recover()["active"]["request_id"] in winners


def test_wait_timeout_and_recovery_do_not_cancel_or_destroy_work(tmp_path):
    transport = QmtTransport(tmp_path)
    transport.prepare(request())
    transport.activate("batch_1")
    with pytest.raises(TimeoutError, match="not cancelled"):
        transport.wait_result("batch_1", timeout=0)
    assert transport.recover()["active"]["request_id"] == "batch_1"
    assert not (tmp_path / "cancelled").exists()
    publish_result(tmp_path, ready(request()))
    assert transport.wait_result("batch_1", timeout=0)["request"] == request()


@pytest.mark.parametrize("request_id", ["../x", "a/b", "a\\b", ".", "", "x" * 65])
def test_request_ids_cannot_escape_private_directory(tmp_path, request_id):
    with pytest.raises(ValueError, match="request_id"):
        QmtTransport(tmp_path).prepare(request(request_id))


@pytest.mark.parametrize("changes", [
    {"adjustment": "follow"}, {"end_date": "2026-09-05"},
    {"codes": [SYMBOL, SYMBOL]}, {"codes": ["000001"]},
    {"source": "mini_qmt"}, {"dataset": "order"},
    {"requested_at": "2026-09-04T16:00:00"},
])
def test_fixed_request_contract_rejects_ambiguous_inputs(tmp_path, changes):
    with pytest.raises(ValueError):
        QmtTransport(tmp_path).prepare(request(**changes))


def test_result_requires_original_request_and_all_outcomes(tmp_path):
    transport = QmtTransport(tmp_path)
    transport.prepare(request(codes=[SYMBOL, "000002.SZ"]))
    bad = ready(request())
    publish_result(tmp_path, bad)
    with pytest.raises(ValueError, match="immutable"):
        transport.read_result("batch_1")


def test_result_with_missing_outcome_is_not_complete(tmp_path):
    transport = QmtTransport(tmp_path)
    plan = request(codes=[SYMBOL, "000002.SZ"])
    transport.prepare(plan)
    bad = ready(plan)
    del bad["outcomes"][SYMBOL]
    publish_result(tmp_path, bad)
    with pytest.raises(ValueError, match="exactly one outcome"):
        transport.read_result("batch_1")


def test_errors_and_legal_empty_can_be_archived_after_caller_commits(tmp_path):
    transport = QmtTransport(tmp_path)
    plan = request(codes=[SYMBOL, "000002.SZ"])
    transport.prepare(plan)
    transport.activate(plan["request_id"])
    outcomes = {SYMBOL: {"status": "error", "rows": [], "reason": "provider unavailable"},
                "000002.SZ": {"status": "no_data", "rows": [], "reason": "explicit native no-data proof"}}
    publish_result(tmp_path, ready(plan, outcomes))
    transport.archive(plan["request_id"])
    assert transport.recover()["active"] is None


class Native:
    def __init__(self, rows=None):
        self.calls = []
        self.rows = {SYMBOL: [{"time": "20260904150000", "close": 12.5}]} if rows is None else rows

    def get_market_data_ex(self, fields, codes, **kwargs):
        self.calls.append(("history", codes, kwargs))
        return self.rows

    def get_full_tick(self, codes):
        self.calls.append(("current", codes))
        return self.rows


def test_history_calls_native_reader_without_fill_and_preserves_invalid_raw():
    native = Native({SYMBOL: [{"time": None, "volume": -3, "close": float("nan"), "amount": None}]})
    downloads = []
    result = model.execute_request(native, request(), clock=lambda: AFTER_CLOSE,
                                   native_globals={"download_history_data": lambda *args: downloads.append(args)})
    assert len(downloads) == 1
    assert native.calls[0][2]["fill_data"] is False
    assert native.calls[0][2]["dividend_type"] == "none"
    assert result["source_method"] == "ContextInfo.get_market_data_ex"
    raw = result["outcomes"][SYMBOL]["rows"][0]
    assert raw["time"] is None and raw["volume"] == -3 and raw["amount"] is None
    assert raw["close"] == "nan"
    json.dumps(result, allow_nan=False)


def test_history_prefers_native_batch_download_and_reads_documented_rows():
    calls = []
    native = Native({SYMBOL: [{"time": "20260904150000", "close": 12.5}]})
    result = model.execute_request(native, request(), clock=lambda: AFTER_CLOSE,
                                   native_globals={
                                       "download_history_data2": lambda *args, **kwargs: calls.append((args, kwargs)),
                                       "download_history_data": lambda *args: pytest.fail("single downloader should not run"),
                                   })
    assert calls == [((), {"stock_list": [SYMBOL], "period": "1d",
                            "start_time": "20260904000000", "end_time": "20260904235959"})]
    assert result["source_method"] == "ContextInfo.get_market_data_ex"
    assert result["outcomes"][SYMBOL]["status"] == "data"


def test_history_prefers_dependency_free_ori_and_expands_columnar_rows():
    class NativeOri(Native):
        def get_market_data_ex_ori(self, fields, codes, **kwargs):
            self.calls.append(("history_ori", codes, kwargs))
            return {SYMBOL: {
                "stime": ["20260904145900", "20260904150000"],
                "open": [12.3, 12.4],
                "close": [12.4, 12.5],
                "volume": [10, 20],
            }}

        def get_market_data_ex(self, *args, **kwargs):
            pytest.fail("the pandas-backed reader should not be selected")

    native = NativeOri()
    result = model.execute_request(
        native,
        request(),
        clock=lambda: AFTER_CLOSE,
        native_globals={"download_history_data": lambda *args: None},
    )

    assert result["source_method"] == "ContextInfo.get_market_data_ex_ori"
    rows = result["outcomes"][SYMBOL]["rows"]
    assert [row["close"] for row in rows] == [12.4, 12.5]
    assert [row["native_index"] for row in rows] == ["20260904145900", "20260904150000"]


def test_history_rejects_misaligned_ori_columns():
    class NativeOri(Native):
        def get_market_data_ex_ori(self, fields, codes, **kwargs):
            return {SYMBOL: {"time": [1, 2], "close": [12.5]}}

    result = model.execute_request(
        NativeOri(),
        request(),
        clock=lambda: AFTER_CLOSE,
        native_globals={"download_history_data": lambda *args: None},
    )

    assert result["outcomes"][SYMBOL]["error_code"] == "INVALID_NATIVE_ROWS"


def test_capital_flow_uses_documented_fields_count_and_period():
    class FlowNative(Native):
        def get_market_data_ex(self, fields, codes, **kwargs):
            self.calls.append((fields, codes, kwargs))
            return {SYMBOL: [{"native_index": "20260904", "bidMostAmount": 1}]}

    native = FlowNative()
    result = model.execute_request(
        native,
        request(dataset="capital_flow_daily"),
        clock=lambda: AFTER_CLOSE,
        native_globals={"download_history_data": lambda *args: None},
    )
    fields, codes, kwargs = native.calls[0]
    assert fields == list(model.FLOW_NATIVE_FIELDS)
    assert codes == [SYMBOL]
    assert kwargs["period"] == "transactioncount1d" and kwargs["count"] == -1
    assert result["source_method"] == "ContextInfo.get_market_data_ex"


def test_history_stops_between_single_downloads_after_deadline():
    values = iter((AFTER_CLOSE, AFTER_CLOSE, AFTER_CLOSE + dt.timedelta(minutes=4)))
    clock = lambda: next(values, AFTER_CLOSE + dt.timedelta(minutes=4))
    downloads = []
    native = Native({SYMBOL: [], "000002.SZ": []})
    result = model.execute_request(
        native,
        request(codes=[SYMBOL, "000002.SZ"]),
        clock=clock,
        native_globals={"download_history_data": lambda *args: downloads.append(args)},
    )
    assert len(downloads) == 1
    assert native.calls == []
    assert all(item["error_code"] == "NATIVE_CALL_FAILED" for item in result["outcomes"].values())


def test_missing_native_security_is_error_not_suspension():
    result = model.execute_request(Native({}), request(), clock=lambda: AFTER_CLOSE,
                                   native_globals={"download_history_data": lambda *args: None})
    assert result["outcomes"][SYMBOL]["status"] == "error"
    assert result["outcomes"][SYMBOL]["error_code"] == "MISSING_SOURCE_RESULT"


def test_empty_stock_minute_uses_exact_native_daily_suspension_proof():
    calls = []
    downloads = []

    class Suspended:
        def get_market_data_ex_ori(self, fields, codes, **kwargs):
            calls.append((fields, codes, kwargs))
            if kwargs["period"] == "1m":
                return {SYMBOL: []}
            return {SYMBOL: {
                "stime": ["20260904"],
                "suspendFlag": [1],
                "volume": [0],
                "amount": [0],
            }}

    result = model.execute_request(
        Suspended(),
        request(dataset="stock_minute"),
        clock=lambda: AFTER_CLOSE,
        native_globals={"download_history_data2":
                        lambda *args, **kwargs: downloads.append((args, kwargs))},
    )

    assert [call[2]["period"] for call in calls] == ["1m", "1d"]
    assert [call[1] for call in calls] == [[SYMBOL], [SYMBOL]]
    assert [item[1]["period"] for item in downloads] == ["1m", "1d"]
    assert all(call[2]["fill_data"] is False and call[2]["subscribe"] is False
               for call in calls)
    assert result["source_method"] == "ContextInfo.get_market_data_ex_ori"
    assert result["outcomes"][SYMBOL] == {
        "status": "no_data",
        "rows": [],
        "reason": "suspended",
        "evidence": {
            "source_method": "ContextInfo.get_market_data_ex_ori",
            "period": "1d",
            "target_date": "2026-09-04",
            "suspendFlag": 1,
        },
    }


@pytest.mark.parametrize("daily_rows", [
    {"stime": ["20260904"], "suspendFlag": [0]},
    {"stime": ["20260903"], "suspendFlag": [1]},
    {"stime": ["20260904"]},
    {"stime": ["20260904"], "suspendFlag": [True]},
    {"stime": ["20260904"], "suspendFlag": ["1"]},
    {"stime": ["20260904", "20260904"], "suspendFlag": [1, 1]},
])
def test_empty_stock_minute_rejects_ambiguous_suspension_evidence(daily_rows):
    class Ambiguous:
        def get_market_data_ex_ori(self, fields, codes, **kwargs):
            return {SYMBOL: [] if kwargs["period"] == "1m" else daily_rows}

    result = model.execute_request(
        Ambiguous(),
        request(dataset="stock_minute"),
        clock=lambda: AFTER_CLOSE,
        native_globals={"download_history_data": lambda *args: None},
    )

    assert result["outcomes"][SYMBOL]["status"] == "error"
    assert result["outcomes"][SYMBOL]["error_code"] == "EMPTY_NATIVE_RESULT"


def test_suspension_proof_is_bounded_to_empty_stock_minute_codes():
    other = "000002.SZ"
    calls = []

    class Mixed:
        def get_market_data_ex_ori(self, fields, codes, **kwargs):
            calls.append((codes, kwargs["period"]))
            if kwargs["period"] == "1m":
                return {
                    SYMBOL: [{"time": "20260904150000", "close": 12.5}],
                    other: [],
                }
            return {other: {"stime": ["20260904"], "suspendFlag": [1]}}

    result = model.execute_request(
        Mixed(),
        request(dataset="stock_minute", codes=[SYMBOL, other]),
        clock=lambda: AFTER_CLOSE,
        native_globals={"download_history_data": lambda *args: None},
    )

    assert calls == [([SYMBOL, other], "1m"), ([other], "1d")]
    assert result["outcomes"][SYMBOL]["status"] == "data"
    assert result["outcomes"][other]["status"] == "no_data"


def test_failed_suspension_probe_preserves_primary_outcomes():
    other = "000002.SZ"

    class DailyFailure:
        def get_market_data_ex_ori(self, fields, codes, **kwargs):
            if kwargs["period"] == "1d":
                raise RuntimeError("daily cache unavailable")
            return {
                SYMBOL: [{"time": "20260904150000", "close": 12.5}],
                other: [],
            }

    result = model.execute_request(
        DailyFailure(),
        request(dataset="stock_minute", codes=[SYMBOL, other]),
        clock=lambda: AFTER_CLOSE,
        native_globals={"download_history_data": lambda *args: None},
    )

    assert result["outcomes"][SYMBOL]["status"] == "data"
    assert result["outcomes"][other]["error_code"] == "EMPTY_NATIVE_RESULT"


def test_empty_non_stock_minute_does_not_infer_suspension():
    native = Native({SYMBOL: []})
    result = model.execute_request(
        native,
        request(dataset="index_minute"),
        clock=lambda: AFTER_CLOSE,
        native_globals={"download_history_data": lambda *args: None},
    )
    assert len(native.calls) == 1
    assert result["outcomes"][SYMBOL]["error_code"] == "EMPTY_NATIVE_RESULT"


def test_bad_security_container_does_not_discard_other_raw_results():
    native = Native({SYMBOL: [{"time": "20260904150000", "close": 12.5}], "000002.SZ": 42})
    result = model.execute_request(native, request(codes=[SYMBOL, "000002.SZ"]), clock=lambda: AFTER_CLOSE,
                                   native_globals={"download_history_data": lambda *args: None})
    assert result["outcomes"][SYMBOL]["status"] == "data"
    assert result["outcomes"]["000002.SZ"]["error_code"] == "INVALID_NATIVE_ROWS"


@pytest.mark.parametrize("product", ["instrument", "calendar", "sector"])
def test_reference_uses_only_fixed_native_methods(product):
    class Reference:
        def get_instrument_detail(self, code):
            return {"InstrumentName": "sample", "OpenDate": 20200101}
        def get_trading_dates(self, code, start, end, count, period):
            return ["20260904"]
        def get_stock_list_in_sector(self, sector):
            return [SYMBOL]
    codes = ["沪深A股"] if product == "sector" else [SYMBOL]
    result = model.execute_request(Reference(), request(dataset="reference", period=product, codes=codes),
                                   clock=lambda: AFTER_CLOSE)
    assert result["outcomes"][codes[0]]["status"] == "data"
    assert result["source_method"].startswith("ContextInfo.")


def test_reference_can_persist_asset_class_without_opening_other_request_fields():
    model.validate_request(request(dataset="reference", period="instrument", asset_class="stock"))
    model.validate_request(request(dataset="reference", period="instrument"))
    with pytest.raises(ValueError, match="asset class"):
        model.validate_request(request(dataset="reference", period="instrument", asset_class="options"))
    with pytest.raises(ValueError, match="fields differ"):
        model.validate_request(request(asset_class="stock"))


def test_symlinked_file_is_not_accepted(tmp_path):
    transport = QmtTransport(tmp_path)
    outside = tmp_path / "ordinary.json"
    model.publish_json(str(outside), request(), 4096)
    linked = tmp_path / "batch_1.prepared.json"
    try:
        linked.symlink_to(outside)
    except OSError:
        pytest.skip("host does not permit test symlink creation")
    with pytest.raises(ValueError, match="links"):
        transport.prepare(request())


@pytest.mark.parametrize("dataset", ["stock_daily", "stock_minute", "index_daily", "index_minute"])
@pytest.mark.parametrize("hour", [0, 8, 9, 10, 12, 15, 23])
def test_closed_history_request_runs_at_any_hour_without_a_window_override(dataset, hour):
    current = dt.datetime(2026, 10, 9, hour, tzinfo=model.SHANGHAI)
    plan = request(dataset=dataset, requested_at=current.isoformat(),
                   deadline_at=(current + dt.timedelta(minutes=3)).isoformat())
    downloads = []
    native = Native()
    result = model.execute_request(native, plan, clock=lambda: current,
                                   native_globals={"download_history_data":
                                                   lambda *args: downloads.append(args)})
    assert len(downloads) == len(native.calls) == 1
    assert result["request"] == plan
    assert result["outcomes"][SYMBOL]["status"] == "data"


def test_expired_plan_still_makes_no_history_call():
    native = Native()
    downloads = []
    result = model.execute_request(native, request(), clock=lambda: AFTER_CLOSE.replace(hour=17),
                                   native_globals={"download_history_data":
                                                   lambda *args: downloads.append(args)})
    assert result["outcomes"][SYMBOL]["error_code"] == "REQUEST_EXPIRED"
    assert downloads == native.calls == []


def test_crossing_former_morning_boundary_does_not_end_a_valid_request():
    started = dt.datetime(2026, 10, 9, 8, 29, 59, tzinfo=model.SHANGHAI)
    later = started + dt.timedelta(minutes=2)
    values = iter((started, started, later))
    native = Native()
    downloads = []
    plan = request(requested_at=started.isoformat(),
                   deadline_at=(started + dt.timedelta(minutes=3)).isoformat())
    result = model.execute_request(native, plan, clock=lambda: next(values, later),
                                   native_globals={"download_history_data":
                                                   lambda *args: downloads.append(args)})
    assert len(downloads) == len(native.calls) == 1
    assert result["outcomes"][SYMBOL]["status"] == "data"


def test_batch_download_returning_after_deadline_cannot_start_a_history_reader():
    current = [AFTER_CLOSE]
    downloads = []
    native = Native()
    def download(**_kwargs):
        downloads.append(True)
        current[0] += dt.timedelta(minutes=4)
    result = model.execute_request(native, request(), clock=lambda: current[0],
                                   native_globals={"download_history_data2": download})
    assert downloads == [True]
    assert native.calls == []
    assert result["outcomes"][SYMBOL]["error_code"] == "NATIVE_CALL_FAILED"


def test_model_restart_with_ready_does_not_redownload(tmp_path, monkeypatch):
    transport = QmtTransport(tmp_path)
    transport.prepare(request())
    transport.activate("batch_1")
    native = Native()
    monkeypatch.setattr(model, "download_history_data", lambda *args: None, raising=False)
    model.Model(tmp_path, clock=lambda: AFTER_CLOSE).poll(native)
    assert transport.read_result("batch_1")["outcomes"][SYMBOL]["status"] == "data"
    model.Model(tmp_path, clock=lambda: AFTER_CLOSE).poll(native)
    assert len(native.calls) == 1


def test_partial_archive_is_recoverable_and_model_does_not_repeat(tmp_path, monkeypatch):
    transport = QmtTransport(tmp_path)
    transport.prepare(request())
    transport.activate("batch_1")
    value = ready(request())
    publish_result(tmp_path, value)
    original_unlink = os.unlink
    def crash_after_files(path, *args, **kwargs):
        if os.fspath(path) == str(tmp_path / "active.json"):
            raise OSError("simulated crash before releasing active")
        return original_unlink(path, *args, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(os, "unlink", crash_after_files)
        with pytest.raises(OSError):
            transport.archive("batch_1")
    assert transport.read_result("batch_1") == value
    native = Native()
    model.Model(tmp_path, clock=lambda: AFTER_CLOSE).poll(native)
    assert native.calls == []
    transport.archive("batch_1")
    assert transport.recover()["active"] is None


def windows_file_error(code, path):
    error = PermissionError(13, "simulated Windows file contention", str(path))
    error.winerror = code
    return error


def windows_retries(monkeypatch):
    delays = []
    monkeypatch.setattr(transport_module, "_WINDOWS_FILESYSTEM", True)
    monkeypatch.setattr(transport_module.time, "sleep", delays.append)
    return delays


@pytest.mark.parametrize("error_code", [5, 32, 33])
@pytest.mark.parametrize("operation", ["read", "activate_link", "archive_link", "unlink"])
def test_windows_file_contention_retries_exact_operation(tmp_path, monkeypatch, error_code, operation):
    transport = QmtTransport(tmp_path)
    plan = request()
    transport.prepare(plan)
    if operation != "activate_link":
        transport.activate("batch_1")
    value = ready(plan)
    publish_result(tmp_path, value)
    delays = windows_retries(monkeypatch)
    attempts = []
    if operation == "read":
        original = transport_module.read_json
        def read(path, limit):
            if path == str(tmp_path / "batch_1.ready.json"):
                attempts.append(path)
                if len(attempts) <= 2:
                    raise windows_file_error(error_code, path)
            return original(path, limit)
        monkeypatch.setattr(transport_module, "read_json", read)
        assert transport.read_result("batch_1") == value
    elif operation in {"activate_link", "archive_link"}:
        original = os.link
        wanted = (tmp_path / "active.json" if operation == "activate_link"
                  else tmp_path / "processed" / "batch_1" / "batch_1.ready.json")
        def link(source, target):
            if target == str(wanted):
                attempts.append(target)
                if len(attempts) <= 2:
                    raise windows_file_error(error_code, target)
            return original(source, target)
        monkeypatch.setattr(os, "link", link)
        if operation == "activate_link":
            transport.activate("batch_1")
            assert transport.recover()["active"] == plan
        else:
            transport.archive("batch_1")
            assert transport.read_result("batch_1") == value
            assert transport.recover()["active"] is None
    else:
        original = os.unlink
        def unlink(path, *args, **kwargs):
            if os.fspath(path) == str(tmp_path / "batch_1.ready.json"):
                attempts.append(path)
                if len(attempts) <= 2:
                    raise windows_file_error(error_code, path)
            return original(path, *args, **kwargs)
        monkeypatch.setattr(os, "unlink", unlink)
        transport.archive("batch_1")
        assert transport.read_result("batch_1") == value
        assert transport.recover()["active"] is None
    assert len(attempts) == 3
    assert delays == list(transport_module._WINDOWS_RETRY_DELAYS[:2])


def test_prepare_retries_atomic_publish_without_overwriting_request(tmp_path, monkeypatch):
    transport = QmtTransport(tmp_path)
    plan = request()
    delays = windows_retries(monkeypatch)
    original = transport_module.publish_json
    attempts = []
    def publish(path, payload, limit, immutable=False):
        attempts.append(path)
        original(path, payload, limit, immutable=immutable)
        if len(attempts) == 1:
            # The atomic link succeeded but temporary cleanup lost an OS race.
            raise windows_file_error(5, path)
    monkeypatch.setattr(transport_module, "publish_json", publish)
    transport.prepare(plan)
    assert len(attempts) == 2
    assert delays == [transport_module._WINDOWS_RETRY_DELAYS[0]]
    assert model.read_json(str(tmp_path / "batch_1.prepared.json"), model.MAX_REQUEST_BYTES) == plan
    with pytest.raises(ValueError, match="immutable"):
        transport.prepare(request(codes=["000002.SZ"]))


@pytest.mark.skipif(os.name != "nt", reason="requires native Windows file-sharing semantics")
def test_archive_recovers_from_real_windows_reader_handle(tmp_path, monkeypatch):
    transport = QmtTransport(tmp_path)
    transport.prepare(request())
    transport.activate("batch_1")
    value = ready(request())
    publish_result(tmp_path, value)
    reader = open(tmp_path / "batch_1.ready.json", "rb")
    errors, delays = [], []
    original_link, original_unlink = os.link, os.unlink
    def observed(operation):
        def invoke(*args, **kwargs):
            try:
                return operation(*args, **kwargs)
            except OSError as exc:
                errors.append(getattr(exc, "winerror", None))
                raise
        return invoke
    def release_reader(delay):
        delays.append(delay)
        reader.close()
    try:
        monkeypatch.setattr(os, "link", observed(original_link))
        monkeypatch.setattr(os, "unlink", observed(original_unlink))
        monkeypatch.setattr(transport_module.time, "sleep", release_reader)
        transport.archive("batch_1")
    finally:
        reader.close()
    assert errors and all(code in (5, 32, 33) for code in errors)
    assert delays == [transport_module._WINDOWS_RETRY_DELAYS[0]]
    assert transport.recover()["active"] is None
    assert transport.read_result("batch_1") == value


@pytest.mark.parametrize("suffix", [".ready.json", "active.json"])
@pytest.mark.parametrize("error_code", [5, 32, 33])
def test_persistent_windows_denial_retains_complete_result_and_active(tmp_path, monkeypatch, suffix, error_code):
    transport = QmtTransport(tmp_path)
    plan = request()
    transport.prepare(plan)
    transport.activate("batch_1")
    value = ready(plan)
    publish_result(tmp_path, value)
    wanted = tmp_path / ("batch_1" + suffix if suffix.startswith(".") else suffix)
    error = windows_file_error(error_code, wanted)
    original = os.unlink
    attempts = []
    delays = windows_retries(monkeypatch)
    def unlink(path, *args, **kwargs):
        if os.fspath(path) == str(wanted):
            attempts.append(path)
            raise error
        return original(path, *args, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(os, "unlink", unlink)
        with pytest.raises(PermissionError) as captured:
            transport.archive("batch_1")
    assert captured.value is error
    assert len(attempts) == len(transport_module._WINDOWS_RETRY_DELAYS) + 1
    assert delays == list(transport_module._WINDOWS_RETRY_DELAYS)
    retained = tmp_path / "processed" / "batch_1"
    assert model.read_json(str(retained / "batch_1.prepared.json"), model.MAX_REQUEST_BYTES) == plan
    assert model.read_json(str(retained / "batch_1.ready.json"), model.MAX_RESULT_BYTES) == value
    assert transport.recover()["active"] == plan
    assert transport.read_result("batch_1") == value
    native = Native()
    model.Model(tmp_path, clock=lambda: AFTER_CLOSE).poll(native)
    assert native.calls == []
    transport.archive("batch_1")
    transport.archive("batch_1")
    assert transport.recover()["active"] is None
    assert transport.read_result("batch_1") == value


@pytest.mark.parametrize("windows,error_code", [(False, 32), (False, 5), (True, None), (True, 112)])
def test_unclassified_or_non_windows_denial_is_not_blindly_retried(tmp_path, monkeypatch, windows, error_code):
    transport = QmtTransport(tmp_path)
    transport.prepare(request())
    transport.activate("batch_1")
    publish_result(tmp_path, ready(request()))
    wanted = str(tmp_path / "batch_1.ready.json")
    error = PermissionError(13, "real permission denial", wanted)
    if error_code is not None:
        error.winerror = error_code
    attempts = []
    delays = windows_retries(monkeypatch)
    monkeypatch.setattr(transport_module, "_WINDOWS_FILESYSTEM", windows)
    original = os.unlink
    def unlink(path, *args, **kwargs):
        if os.fspath(path) == wanted:
            attempts.append(path)
            raise error
        return original(path, *args, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(os, "unlink", unlink)
        with pytest.raises(PermissionError) as captured:
            transport.archive("batch_1")
    assert captured.value is error
    assert len(attempts) == 1 and delays == []
    assert transport.recover()["active"] == request()
    assert transport.read_result("batch_1") == ready(request())


def test_archive_retry_never_releases_a_replacement_active_request(tmp_path, monkeypatch):
    transport = QmtTransport(tmp_path)
    transport.prepare(request())
    transport.prepare(request("batch_2"))
    transport.activate("batch_1")
    publish_result(tmp_path, ready(request()))
    delays = windows_retries(monkeypatch)
    original = os.unlink
    active_path = str(tmp_path / "active.json")
    attempts = []
    def unlink(path, *args, **kwargs):
        if os.fspath(path) == active_path:
            attempts.append(path)
            original(path, *args, **kwargs)
            os.link(str(tmp_path / "batch_2.prepared.json"), active_path)
            raise windows_file_error(32, path)
        return original(path, *args, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(os, "unlink", unlink)
        with pytest.raises(RuntimeError, match="changed during archive"):
            transport.archive("batch_1")
    assert len(attempts) == 1
    assert delays == [transport_module._WINDOWS_RETRY_DELAYS[0]]
    assert transport.recover()["active"] == request("batch_2")
    assert transport.read_result("batch_1") == ready(request())
    with pytest.raises(RuntimeError, match="another active request"):
        transport.archive("batch_1")


def test_archive_revalidates_ordinary_path_after_each_file_retry(tmp_path, monkeypatch):
    transport = QmtTransport(tmp_path)
    transport.prepare(request())
    transport.activate("batch_1")
    publish_result(tmp_path, ready(request()))
    windows_retries(monkeypatch)
    original_unlink, original_ordinary = os.unlink, transport_module._ordinary
    wanted = str(tmp_path / "batch_1.ready.json")
    denied = []
    def unlink(path, *args, **kwargs):
        if os.fspath(path) == wanted:
            denied.append(path)
            raise windows_file_error(32, path)
        return original_unlink(path, *args, **kwargs)
    def ordinary(path):
        if path == wanted and denied:
            raise ValueError("acquisition paths cannot be links or reparse points")
        return original_ordinary(path)
    with monkeypatch.context() as patch:
        patch.setattr(os, "unlink", unlink)
        patch.setattr(transport_module, "_ordinary", ordinary)
        with pytest.raises(ValueError, match="reparse"):
            transport.archive("batch_1")
    assert len(denied) == 1
    assert transport.recover()["active"] == request()
    assert transport.read_result("batch_1") == ready(request())


def test_low_disk_does_not_delete_results_or_start_native(tmp_path, monkeypatch):
    transport = QmtTransport(tmp_path)
    transport.prepare(request())
    transport.activate("batch_1")
    monkeypatch.setattr(model.shutil, "disk_usage", lambda root: type("Disk", (), {"free": 0})())
    native = Native()
    model.Model(tmp_path, clock=lambda: AFTER_CLOSE).poll(native)
    assert native.calls == []
    assert transport.read_result("batch_1")["outcomes"][SYMBOL]["error_code"] == "DISK_SPACE_LOW"


def test_result_size_limit_and_no_ready_overwrite(tmp_path):
    path = str(tmp_path / "batch_1.ready.json")
    with pytest.raises(ValueError, match="size"):
        model.publish_json(path, {"oversize": "x" * 30}, 20, immutable=True)
    model.publish_json(path, {"ok": 1}, 100, immutable=True)
    with pytest.raises(FileExistsError):
        model.publish_json(path, {"ok": 2}, 100, immutable=True)
    assert model.read_json(path, 100) == {"ok": 1}


def test_live_snapshot_keeps_native_time_and_never_moves_backwards(tmp_path):
    now = AFTER_CLOSE.replace(hour=10)
    model.publish_json(str(tmp_path / "live_plan.json"), {"stock_current": [SYMBOL]}, 4096)
    native = Native({SYMBOL: {"time": "20260904100000", "lastPrice": 12}})
    instance = model.Model(tmp_path, clock=lambda: now)
    instance.live(native)
    path = str(tmp_path / "stock_current.snapshot.json")
    assert model.read_json(path, 4096)["outcomes"][SYMBOL]["rows"][0]["lastPrice"] == 12
    now += dt.timedelta(seconds=16)
    native.rows = {SYMBOL: {"time": "20260904095900", "lastPrice": 11}}
    instance.live(native)
    assert model.read_json(path, 4096)["outcomes"][SYMBOL]["rows"][0]["lastPrice"] == 12
    now += dt.timedelta(seconds=16)
    native.rows = {SYMBOL: {"lastPrice": 13}}
    instance.live(native)
    outcome = model.read_json(path, 4096)["outcomes"][SYMBOL]
    assert outcome["status"] == "error" and not outcome["rows"]


def test_full_market_live_plan_keeps_bounded_native_calls(tmp_path):
    codes = [f"{index:06d}.SZ" for index in range(5558)]
    model.publish_json(str(tmp_path / "live_plan.json"), {"stock_current": codes}, model.MAX_REQUEST_BYTES)
    calls = []
    class FullMarket:
        def get_full_tick(self, batch):
            calls.append(list(batch))
            return {code: {"time": "20260904100000", "lastPrice": 12} for code in batch}
    model.Model(tmp_path, clock=lambda: AFTER_CLOSE.replace(hour=10)).live(FullMarket())
    snapshot = model.read_json(str(tmp_path / "stock_current.snapshot.json"), model.MAX_RESULT_BYTES)
    assert len(snapshot["outcomes"]) == 5558
    assert max(map(len, calls)) <= model.MAX_LIVE_BATCH
    assert len(calls) == 7
    with pytest.raises(ValueError, match="bounded"):
        model.validate_request(request(codes=codes[:41]))


@pytest.mark.parametrize("hour", [8, 12, 16, 23])
def test_history_policy_does_not_open_the_separate_live_quote_window(tmp_path, hour):
    model.publish_json(str(tmp_path / "live_plan.json"), {"stock_current": [SYMBOL]}, 4096)
    native = Native()
    model.Model(tmp_path, clock=lambda: AFTER_CLOSE.replace(hour=hour)).live(native)
    assert native.calls == []
    assert not (tmp_path / "stock_current.snapshot.json").exists()


@pytest.mark.parametrize("dataset", ["stock_daily", "stock_minute", "index_daily", "index_minute"])
def test_runner_normal_path_can_acquire_closed_history_during_market_hours(tmp_path, monkeypatch, dataset):
    from types import SimpleNamespace
    from acquisition.config import Config
    from acquisition.runner import Runner

    now = dt.datetime(2026, 10, 9, 10, 0, tzinfo=model.SHANGHAI)
    config = Config({"state_dir": str(tmp_path / "state"), "write_enabled": True,
                     "start_date": "2026-09-04", "datasets": [dataset]}, tmp_path / "config.json")
    runner = Runner(config, clock=lambda: now)
    store = SimpleNamespace(
        catalog=lambda asset: {SYMBOL: {"qmt_code": SYMBOL, "asset_class": asset}},
        calendar=lambda start, end: {"2026-09-04": 1},
        states=lambda name: [], retrying_sources=lambda current: [],
    )
    runner._stores.update(primary=store, history=store)
    monkeypatch.setattr(runner, "recover_http", lambda: None)
    monkeypatch.setattr(runner, "recover_qmt", lambda: True)
    monkeypatch.setattr(runner, "status", lambda names, target: {"status": "partial"})
    acquired = []

    def acquire(units, timeout):
        acquired.extend(units)
        assert timeout > 0
        return {"complete": len(units)}

    monkeypatch.setattr(runner, "acquire", acquire)
    result = runner.run([dataset], start="2026-09-04", end="2026-09-04")
    assert result["errors"] == []
    assert result["status"] == "partial"  # A dispatch model is not a coverage proof.
    assert result["runs"][dataset]["completed_units"] == 1
    assert [(unit.dataset, unit.target_date, unit.code) for unit in acquired] == [
        (dataset, "2026-09-04", SYMBOL)]


@pytest.mark.parametrize("dataset", ["stock_daily", "stock_minute", "index_daily", "index_minute"])
@pytest.mark.parametrize("target,reason", [
    ("2026-10-09", "target is not ready"),
    ("2026-10-10", "future target is not allowed"),
])
def test_capture_time_policy_does_not_open_unclosed_or_future_targets(tmp_path, dataset, target, reason):
    from types import SimpleNamespace
    from acquisition.config import Config
    from acquisition.datasets import get_spec
    from acquisition.runner import Runner

    now = dt.datetime(2026, 10, 9, 10, 0, tzinfo=model.SHANGHAI)
    config = Config({"state_dir": str(tmp_path / "state")}, tmp_path / "config.json")
    runner = Runner(config, clock=lambda: now)
    runner._stores["primary"] = SimpleNamespace(calendar=lambda start, end: {"2026-10-09": 1})
    with pytest.raises(ValueError, match=reason):
        runner._target(get_spec(dataset), target)


def test_standalone_model_imports_only_standard_library():
    source = Path(model.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add(node.module.split(".")[0])
    assert imported <= {"datetime", "json", "math", "os", "re", "shutil", "stat", "threading", "time", "uuid"}
