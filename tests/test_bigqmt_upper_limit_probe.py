import json
from pathlib import Path
import runpy

import pytest

from server.common.upper_limit_snapshot import (
    UpperLimitSnapshotBlocked,
    build_upper_limit_capture_run,
    build_upper_limit_subject,
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests/fixtures/qmt_native_upper_history_probe_20260905.json"
PROBE = ROOT / "tools/probes/qmt_native_upper_history_probe.py"


def _fixture():
    return json.loads(FIXTURE.read_text(encoding="utf-8"))["probe"]


def test_schema_only_full_qmt_fallback_has_no_historical_stop_price_fields():
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert fixture["synthetic_only"] is True
    assert fixture["fixture_scope"] == "SCHEMA_ONLY_EMPTY_ARRAYS"
    payload = _fixture()
    assert payload["source_method"] == "ContextInfo.get_market_data_ex_ori"
    assert payload["evidence_status"] == "UNVERIFIED"
    stop = payload["period_results"]["stoppricedata"]["raw"]
    assert stop == payload["period_results"]["1d"]["raw"]
    assert set(stop) == {"000001.SZ", "600000.SH"}
    for native in stop.values():
        assert all(values == [] for values in native.values())
        assert "preClose" in native and "suspendFlag" in native
        assert not set(native).intersection({
            "upper_limit", "lower_limit", "UpStopPrice", "DownStopPrice",
            "涨停价", "跌停价",
        })


def test_ordinary_daily_payload_cannot_enter_formal_upper_limit_contract():
    subject = build_upper_limit_subject(
        target_date="2026-09-04", stock_codes=["000001", "600000"],
        trade_dates=["2026-09-03", "2026-09-04"],
        expected_stock_count=2, expected_date_count=2,
    )
    with pytest.raises(UpperLimitSnapshotBlocked, match="fixed-action response contract"):
        build_upper_limit_capture_run(
            subject=subject, bridge_result=_fixture(),
            decision_at="2026-09-05T15:00:00", collector_build_sha="a" * 40,
        )


def test_standalone_probe_preserves_columnar_native_schema_without_invention(capsys):
    PROBE.read_bytes().decode("ascii")
    namespace = runpy.run_path(str(PROBE))
    payload = _fixture()
    calls = []

    class Context:
        def get_market_data_ex_ori(self, fields, codes, **kwargs):
            calls.append((fields, codes, kwargs))
            return payload["period_results"][kwargs["period"]]["raw"]

        def get_market_data_ex(self, *args, **kwargs):
            pytest.fail("must avoid the pandas-dependent wrapper when _ori exists")

    namespace["init"](Context())
    namespace["handlebar"](Context())
    output = capsys.readouterr().out
    assert output.startswith("PROBIGA_QMT_NATIVE_UPPER_HISTORY_PROBE ")
    assert json.loads(output.split(" ", 1)[1]) == payload
    assert len(calls) == 2
    assert [call[2]["period"] for call in calls] == ["stoppricedata", "1d"]
    for fields, codes, options in calls:
        assert fields == []
        assert codes == ["000001.SZ", "600000.SH"]
        assert options == {
            "period": options["period"], "start_time": "20260903",
            "end_time": "20260904", "count": -1, "dividend_type": "none",
            "fill_data": False, "subscribe": False,
        }


def test_missing_wrapper_dependency_is_not_reported_as_missing_entitlement(capsys):
    namespace = runpy.run_path(str(PROBE))

    class Context:
        def get_market_data_ex(self, *args, **kwargs):
            raise ModuleNotFoundError("No module named 'pandas'")

    namespace["init"](Context())
    result = json.loads(capsys.readouterr().out.split(" ", 1)[1])
    assert result["evidence_status"] == "UNVERIFIED"
    assert all(item["error_type"] == "ModuleNotFoundError"
               for item in result["period_results"].values())
    assert "entitlement_status" not in result


def test_native_probe_bounds_output_without_reinterpreting_fields(capsys):
    namespace = runpy.run_path(str(PROBE))

    class Context:
        def get_market_data_ex_ori(self, *args, **kwargs):
            return {"000001.SZ": {"unknown_native_field": "x" * 65536}}

    namespace["init"](Context())
    result = json.loads(capsys.readouterr().out.split(" ", 1)[1])
    assert all(item["status"] == "ERROR"
               for item in result["period_results"].values())
    assert all("64 KiB" in item["error"]
               for item in result["period_results"].values())
