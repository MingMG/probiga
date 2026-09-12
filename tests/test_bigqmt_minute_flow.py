from datetime import datetime
from types import SimpleNamespace

import pytest

from integrations.bigqmt import bridge
from integrations.bigqmt.qmt_strategy import probiga_big_qmt_bridge as strategy
from tools import sync_qmt_minute_flow_exact as exact


DAY = "2026-09-11"
CODE = "000001.SZ"


def _native_rows():
    return [{"time": int((DAY + " " + clock).replace("-", "").replace(" ", "").replace(":", "")),
             **dict(zip(strategy.MINUTE_FLOW_NATIVE_FIELDS, (1, -2, 3, -4)))}
            for clock in exact.GRID]


def _envelope(result):
    return {**result, "action": "minute_flow_exact", "status": "ok",
            "source": exact.QMT_PROVIDER_ID, "request_id": "request-1", "model_instance_id": "model-1",
            "strategy_release_protocol": "probiga.bigqmt-strategy-release.v2",
            "strategy_identity_protocol": "probiga.bigqmt-loaded-strategy-identity.v1",
            "strategy_identity_frozen": True, "strategy_identity_status": "BOUND",
            "strategy_build_sha": "a" * 40, "strategy_git_blob": "b" * 40,
            "strategy_source_sha256": "c" * 64, "strategy_artifact_sha256": "d" * 64,
            "strategy_loaded_identity_sha256": "e" * 64}


def test_native_strategy_preserves_241_unfilled_cumulative_rows_through_consumer(monkeypatch):
    calls = []
    monkeypatch.setattr(strategy, "_download_history", lambda *a: calls.append(("download", a)))

    def read(fields, symbols, **kwargs):
        calls.append(("read", kwargs))
        assert symbols == [CODE] and fields == []
        assert kwargs == dict(period="transactioncount1m", start_time="20260911000000", end_time="20260911235959",
                              count=-1, dividend_type="none", fill_data=False, subscribe=False)
        return {CODE: _native_rows()}

    response = _envelope(strategy._execute_request(
        SimpleNamespace(get_market_data_ex_ori=read), "minute_flow_exact", {"stock_codes": [CODE], "trade_date": DAY},
    ))
    assert calls[0] == ("download", ([CODE], "transactioncount1m", "20260911000000", "20260911235959"))
    assert response["row_count"] == 241
    normalized, _ = exact.normalize_flow_batch(response, expected_qmt_codes=[CODE], qmt_to_stock={CODE: CODE[:6]},
        trade_date=DAY, observed_at=datetime(2026, 9, 12), batch_id="f" * 64, build_sha="a" * 40)
    assert len(normalized) == 241
    assert normalized[0]["main_net_inflow"] == -1
    assert normalized[-1]["sm_net_inflow"] == -4


@pytest.mark.parametrize("shape", ["records", "columns", "time_map"])
def test_native_supported_frame_shapes_preserve_field_values(monkeypatch, shape):
    rows = _native_rows()
    data = rows if shape == "records" else ({key: [row[key] for row in rows] for key in rows[0]} if shape == "columns"
                                           else {str(row["time"]): row for row in rows})
    monkeypatch.setattr(strategy, "_download_history", lambda *a: None)
    response = strategy._minute_flow_capture(SimpleNamespace(get_market_data_ex_ori=lambda *a, **k: {CODE: data}),
                                            {"stock_codes": [CODE], "trade_date": DAY})
    assert response["row_count"] == 241
    assert response["rows"][0]["netInflowBigAmount"] == -2


@pytest.mark.parametrize("params", [
    {"stock_codes": [CODE], "trade_date": DAY, "period": "1m"},
    {"stock_codes": [CODE, CODE], "trade_date": DAY},
    {"stock_codes": [], "trade_date": DAY},
    {"stock_codes": [f"{n:06}.SZ" for n in range(41)], "trade_date": DAY},
])
def test_native_scope_rejected_before_download(monkeypatch, params):
    monkeypatch.setattr(strategy, "_download_history", lambda *a: pytest.fail("unexpected provider request"))
    with pytest.raises(ValueError):
        strategy._minute_flow_capture(object(), params)


@pytest.mark.parametrize("frame,reason", [
    ([{"time": 20260911093000, "close": 10}], "NATIVE_FIELDS_MISSING"),
    ([{**_native_rows()[0], "time": 20260910093000}], "TIMESTAMP_INVALID"),
    (_native_rows() + [_native_rows()[0]], "GRID_OVERSIZED"),
])
def test_native_ohlc_wrong_date_or_excess_rows_are_not_flow(monkeypatch, frame, reason):
    monkeypatch.setattr(strategy, "_download_history", lambda *a: None)
    with pytest.raises(RuntimeError, match=reason):
        strategy._minute_flow_capture(SimpleNamespace(get_market_data_ex_ori=lambda *a, **k: {CODE: frame}),
                                      {"stock_codes": [CODE], "trade_date": DAY})


def test_bridge_submits_exact_single_batch_without_period_override(monkeypatch):
    calls = []
    response = {"native": "response"}
    monkeypatch.setattr(bridge, "_call", lambda action, **kw: calls.append((action, kw)) or response)
    assert bridge.minute_flow_capture([CODE], trade_date=DAY) is response
    assert calls == [("minute_flow_exact", {"stock_codes": [CODE], "trade_date": DAY, "timeout": 180})]
    with pytest.raises(ValueError):
        bridge.minute_flow_capture([CODE] * 41, trade_date=DAY)


def test_source_checks_frozen_identity_on_every_capture(monkeypatch):
    raw = _envelope({"rows": []})
    capability = {**raw, "actions": ["minute_flow_exact"], "generated_ts": 1}
    monkeypatch.setattr(exact.bigqmt_bridge, "capabilities", lambda **k: capability)
    monkeypatch.setattr(exact, "validate_bigqmt_strategy_release", lambda *a, **k: {"validated": True})
    monkeypatch.setattr(exact.bigqmt_bridge, "minute_flow_capture", lambda *a, **k: raw)
    source = exact.BigQmtFlowSource(expected_build_sha="a" * 40)
    first = source.identity()
    capability["model_instance_id"] = "recovered-model"
    capability["generated_ts"] = 2
    assert source.identity() == first
    assert source.fetch([CODE], trade_date=DAY) is raw
    raw["strategy_artifact_sha256"] = "0" * 64
    with pytest.raises(exact.MinuteFlowDataBlocked, match="frozen identity differs"):
        source.fetch([CODE], trade_date=DAY)


@pytest.mark.parametrize("cached_first", [True, False])
def test_cached_and_live_capability_envelopes_bind_identical_frozen_source(monkeypatch, cached_first):
    baseline = {**_envelope({}), "read_only": True, "simulation_only": True,
                "automatic_real_order_submission": False, "real_order_authority": False,
                "bridge_version": "bigqmt_inner_v2", "actions": ["minute", "minute_flow_exact", "trading_calendar"],
                "native_capabilities": [{"capability": "trading_calendar", "action": "trading_calendar",
                                         "available": True, "source_method": "ContextInfo.get_trading_dates"},
                                        {"capability": "index_weight", "action": "index_members_many", "available": False,
                                         "source_method": "membership_only_no_native_weight"}]}
    cached = {**baseline, "capability_transport": "cached_control_plane", "generated_at": "2026-09-12 09:00:00"}
    live = {**baseline, "schema_version": 3, "request_id": "live-2", "model_instance_id": "recovered-model",
            "generated_at": "2026-09-12 09:10:00", "cursor": 0, "attempt": 1, "run_id": "", "build_id": ""}
    payloads = iter([cached, live] if cached_first else [live, cached])
    captured = []
    monkeypatch.setattr(exact.bigqmt_bridge, "capabilities", lambda **kw: next(payloads))
    def validate(payload, **kwargs):
        captured.append(payload)
        assert not ({"capability_transport", "model_instance_id", "generated_at", "request_id"} & set(payload))
        return {"verified_frozen": payload}
    monkeypatch.setattr(exact, "validate_bigqmt_strategy_release", validate)
    source = exact.BigQmtFlowSource(expected_build_sha="a" * 40)
    assert source.identity() == source.identity()
    assert captured[0] == captured[1]
    assert set(captured[0]) == set(exact.FROZEN_CAPABILITY_FIELDS) | {"actions", "native_capabilities"}


def test_source_does_not_classify_native_data_error_as_login_failure(monkeypatch):
    source = exact.BigQmtFlowSource(expected_build_sha="a" * 40)
    source._bound_identity = {"frozen_model": {}}
    def rejected(*a, **k):
        raise RuntimeError("QMT_MINUTE_FLOW_NATIVE_FIELDS_MISSING")
    monkeypatch.setattr(exact.bigqmt_bridge, "minute_flow_capture", rejected)
    with pytest.raises(RuntimeError, match="NATIVE_FIELDS_MISSING") as caught:
        source.fetch([CODE], trade_date=DAY)
    assert not isinstance(caught.value, exact._MinuteFlowConnectionUnavailable)
