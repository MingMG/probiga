from __future__ import annotations

import pytest

from integrations.bigqmt import diagnostics as d


def _release(**updates):
    return {
        "model_instance_id": "model-A", "strategy_build_sha": "a" * 40,
        "strategy_identity_frozen": True, "strategy_identity_status": "BOUND",
        "strategy_git_blob": "b" * 40, "strategy_source_sha256": "c" * 64,
        "strategy_artifact_sha256": "d" * 64, "strategy_loaded_identity_sha256": "e" * 64,
        "strategy_release_protocol": "probiga.bigqmt-strategy-release.v2",
        "strategy_identity_protocol": "probiga.bigqmt-loaded-strategy-identity.v1",
        "actions": ["kline", "minute", "minute_flow_exact", "announcement"], **updates,
    }


def _bar(**updates):
    return {
        "qmt_code": "000001.SZ", "trade_date": "2026-09-11",
        "open": 10, "high": 12, "low": 9, "close": 11,
        "volume": 10, "amount": 100, **updates,
    }


def _base(monkeypatch, plan):
    monkeypatch.setattr(d, "authoritative_elapsed_trade_date", lambda engine: "2026-09-11")
    monkeypatch.setattr(d, "_release", lambda timeout: _release())
    normalized = {name: (action, lambda timeout, fetch=fetch, action=action: {
        **_envelope(action), **fetch(timeout),
    }, validator) for name, (action, fetch, validator) in plan.items()}
    monkeypatch.setattr(d, "_probe_plan", lambda target: normalized)


def _envelope(action, **updates):
    return {**_release(), "source": "gj_big_qmt_inner", "status": "ok", "action": action,
            "request_id": "request-1", **updates}


def _row(result, name):
    return next(row for row in result["rows"] if row["probe_name"] == name)


def test_declared_action_does_not_prove_sample_permission(monkeypatch):
    _base(monkeypatch, {
        "stock_daily_bar": ("kline", lambda timeout: {"rows": []}, lambda rows: True),
    })
    result = d.core_probe(engine=object())
    assert _row(result, "stock_daily_bar")["status"] == "NO_DATA"
    assert _row(result, "stock_minute_bar")["status"] == "UNSUPPORTED_CLIENT"
    assert not any(row["status"] == "SUPPORTED" for row in result["rows"])


def test_real_native_sample_is_required_for_supported(monkeypatch):
    calls = []
    _base(monkeypatch, {
        "stock_daily_bar": (
            "kline", lambda timeout: calls.append(timeout) or {"rows": [_bar()]},
            lambda rows: d._valid_bars(rows, "000001.SZ", "2026-09-11"),
        ),
    })
    result = d.core_probe(engine=object())
    row = _row(result, "stock_daily_bar")
    assert len(calls) == 1 and 0 < calls[0] <= 240
    assert row["status"] == "SUPPORTED" and row["row_count"] == 1
    assert result["sdk_module"] == "BigQMT.ContextInfo"
    assert result["connection_port"] is None


@pytest.mark.parametrize("changes", [
    {"qmt_code": "600000.SH"}, {"trade_date": "2026-09-10"},
    {"open": None}, {"high": float("inf")}, {"low": 11.5},
    {"close": 0}, {"volume": -1}, {"amount": float("nan")},
])
def test_wrong_identity_date_or_native_values_fail(changes):
    assert not d._valid_bars([_bar(**changes)], "000001.SZ", "2026-09-11")


@pytest.mark.parametrize("value,status", [(0, "NO_DATA"), (2, "SUPPORTED"), (None, "FAILED")])
def test_zero_or_missing_vip_fields_do_not_prove_permission(monkeypatch, value, status):
    flow = {"qmt_code": "000001.SZ", "trade_time": "2026-09-11 09:31:00",
            **dict.fromkeys(d.FLOW_FIELDS, value)}
    _base(monkeypatch, {
        "stock_flow_min": (
            "minute_flow_exact", lambda timeout: {"rows": [flow]},
            lambda rows: d._valid_flow(rows, "000001.SZ", "2026-09-11"),
        ),
    })
    assert _row(d.core_probe(engine=object()), "stock_flow_min")["status"] == status


def test_actual_request_failure_is_not_supported_or_permission_guess(monkeypatch):
    def fail(timeout):
        raise RuntimeError("native RPC error: sensitive details must not propagate")
    _base(monkeypatch, {"stock_daily_bar": ("kline", fail, lambda rows: True)})
    result = d.core_probe(engine=object())
    assert result["status"] == "error"
    assert _row(result, "stock_daily_bar")["error"] == "RuntimeError"


def test_model_restart_during_probe_invalidates_entire_result(monkeypatch):
    _base(monkeypatch, {})
    releases = iter([_release(), _release(model_instance_id="model-B")])
    monkeypatch.setattr(d, "_release", lambda timeout: next(releases))
    with pytest.raises(RuntimeError, match="identity changed"):
        d.core_probe(engine=object())


def test_capabilities_metadata_alone_produces_no_supported_rows(monkeypatch):
    monkeypatch.setattr(d, "_release", lambda timeout: _release())
    result = d.capabilities(force=True)
    assert result["rows"] == []
    assert result["source"] == "gj_big_qmt_inner"


def test_fixed_plan_requests_actual_target_without_legacy_xtquant(monkeypatch):
    calls = []
    monkeypatch.setattr(d.bridge, "kline_capture", lambda codes, **kw: calls.append((codes, kw)) or {"rows": []})
    action, fetch, validate = d._probe_plan("2026-09-11")["stock_daily_bar"]
    fetch(12)
    assert action == "kline"
    assert calls == [(["000001.SZ"], {"start_date": "2026-09-11", "end_date": "2026-09-11", "dividend_type": "none", "download_history": True, "timeout": 12})]


@pytest.mark.parametrize("key,value", [
    ("model_instance_id", "different-model"), ("strategy_artifact_sha256", "0" * 64),
    ("strategy_identity_frozen", False), ("request_id", ""),
])
def test_each_native_response_is_bound_to_observed_model(monkeypatch, key, value):
    _base(monkeypatch, {"stock_daily_bar": (
        "kline", lambda timeout: {"rows": [_bar()], key: value}, lambda rows: True,
    )})
    assert _row(d.core_probe(engine=object()), "stock_daily_bar")["status"] == "FAILED"


def test_minute_aggregate_checks_every_original_batch_receipt(monkeypatch):
    _base(monkeypatch, {"stock_minute_bar": (
        "minute", lambda timeout: {"rows": [_bar()], "batch_receipts": [
            _envelope("minute"), _envelope("minute", model_instance_id="old-model"),
        ]}, lambda rows: True,
    )})
    assert _row(d.core_probe(engine=object()), "stock_minute_bar")["status"] == "FAILED"


@pytest.mark.parametrize("native_rows,status", [([], "NO_DATA"),
    ([{"index": 0, "row": {"publish_time": "2026-09-11 10:00:00", "title": "Sample announcement", "stock_code": "000001"}}], "SUPPORTED"),
    ([{"index": 0, "row": {"time": 20260911100000, "close": 10}}], "FAILED"),
])
def test_announcement_uses_real_native_frames_and_pit_parser(monkeypatch, native_rows, status):
    frame = {"frames": {"000001.SZ": {"index_name": None, "rows": native_rows}}}
    _base(monkeypatch, {"announcement": ("announcement", lambda timeout: frame, lambda rows: bool(rows))})
    assert _row(d.core_probe(engine=object()), "announcement")["status"] == status


@pytest.mark.parametrize("phase", ["initial", "sample"])
def test_transport_recovery_restarts_the_whole_unwritten_probe_set_once(monkeypatch, phase):
    calls = []
    recovered = []
    def meta(**kwargs):
        calls.append("metadata")
        if phase == "initial" and not recovered:
            raise d.QmtCapabilityTransportUnavailable("unavailable")
        return {"model_instance_id": "new" if recovered else "old"}
    def core(**kwargs):
        calls.append("samples")
        if not recovered:
            raise d.QmtCapabilityTransportUnavailable("unavailable")
        return {"model_instance_id": "new", "rows": [{"probe_name": "fresh"}]}
    monkeypatch.setattr(d, "capabilities", meta)
    monkeypatch.setattr(d, "core_probe", core)
    metadata, samples = d.probe_capabilities(engine=object(), recover_session=lambda: recovered.append(True) or True)
    assert recovered == [True] and metadata["model_instance_id"] == samples["model_instance_id"] == "new"
    assert calls == (["metadata", "metadata", "samples"] if phase == "initial" else ["metadata", "samples", "metadata", "samples"])


def test_repeated_transport_failure_does_not_loop_or_write(monkeypatch):
    recovered = []
    def unavailable(**kwargs):
        raise d.QmtCapabilityTransportUnavailable("unavailable")
    monkeypatch.setattr(d, "capabilities", unavailable)
    with pytest.raises(d.QmtCapabilityTransportUnavailable):
        d.probe_capabilities(engine=object(), recover_session=lambda: recovered.append(True) or True)
    assert recovered == [True]


@pytest.mark.parametrize("status", ["FAILED", "NO_DATA", "UNSUPPORTED_CLIENT"])
def test_data_or_permission_results_never_trigger_login(monkeypatch, status):
    monkeypatch.setattr(d, "capabilities", lambda **k: {"model_instance_id": "same"})
    monkeypatch.setattr(d, "core_probe", lambda **k: {"model_instance_id": "same", "rows": [{"status": status}]})
    d.probe_capabilities(engine=object(), recover_session=lambda: pytest.fail("unexpected login"))


def test_total_probe_deadline_is_not_a_session_failure(monkeypatch):
    _base(monkeypatch, {"stock_daily_bar": (
        "kline", lambda timeout: pytest.fail("expired request dispatched"), lambda rows: True,
    )})
    ticks = iter([0, 0, 241])
    monkeypatch.setattr(d.time, "monotonic", lambda: next(ticks))
    with pytest.raises(TimeoutError, match="total deadline expired"):
        d.core_probe(engine=object(), timeout=240)


def test_metadata_exhaustion_does_not_start_samples_or_login(monkeypatch):
    ticks = iter([0, 0, 241])
    monkeypatch.setattr(d.time, "monotonic", lambda: next(ticks))
    monkeypatch.setattr(d, "capabilities", lambda **kwargs: {})
    monkeypatch.setattr(d, "core_probe", lambda **kwargs: pytest.fail("expired samples dispatched"))
    with pytest.raises(TimeoutError, match="total deadline expired"):
        d.probe_capabilities(engine=object(), timeout=240, recover_session=lambda: pytest.fail("unexpected login"))
