import ast
import datetime as dt
import json
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from acquisition import qmt_model as model
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


@pytest.mark.parametrize("hour,error_code", [(10, "HISTORY_WINDOW_CLOSED"), (17, "REQUEST_EXPIRED")])
def test_live_window_or_expired_plan_makes_no_history_call(hour, error_code):
    native = Native()
    result = model.execute_request(native, request(), clock=lambda: AFTER_CLOSE.replace(hour=hour))
    assert result["outcomes"][SYMBOL]["error_code"] == error_code
    assert native.calls == []


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
