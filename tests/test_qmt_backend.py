from __future__ import annotations

import sys
import types
import json
from pathlib import Path

import pandas as pd

from integrations.qmt import bridge
from integrations.qmt.backend import QmtBackend, from_qmt_symbol, to_qmt_symbol
from integrations.qmt import info as qmt_info
from integrations.qmt.runtime import ensure_xtquant_on_path


class _GatewayResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


def test_qmt_symbol_mapping_roundtrip():
    assert to_qmt_symbol("600519") == "600519.SH"
    assert to_qmt_symbol("000001") == "000001.SZ"
    assert to_qmt_symbol("510300") == "510300.SH"
    assert to_qmt_symbol("159915") == "159915.SZ"
    assert to_qmt_symbol("830799") == "830799.BJ"
    assert to_qmt_symbol("920808") == "920808.BJ"
    assert to_qmt_symbol("810011") is None
    assert to_qmt_symbol("810011.BJ") is None
    assert to_qmt_symbol("899050") is None
    assert to_qmt_symbol("899050.BJ") is None
    assert to_qmt_symbol("900901") is None
    assert from_qmt_symbol("600519.SH") == "600519"


def test_bridge_accepts_canonical_six_digit_stock_codes(monkeypatch):
    payloads = []

    def fake_run(payload, *, timeout=None):
        payloads.append(payload)
        return {"ok": True, "rows": []}

    monkeypatch.setattr(bridge, "_run", fake_run)
    bridge.kline(
        ["000001", "600519", "920808", "000001.SH"],
        start_date="20260717",
        end_date="20260717",
    )

    assert payloads[0]["stock_codes"] == [
        "000001.SZ",
        "600519.SH",
        "920808.BJ",
        "000001.SH",
    ]


def test_gateway_retries_transient_worker_decode_error(monkeypatch):
    responses = iter(
        [
            _GatewayResponse({"ok": False, "error": "UnicodeDecodeError: invalid continuation byte"}),
            _GatewayResponse({"ok": True, "rows": [{"stock_code": "000001"}]}),
        ]
    )
    calls = []

    def urlopen(_request, *, timeout):
        calls.append(timeout)
        return next(responses)

    monkeypatch.setenv("QMT_GATEWAY_ENABLED", "1")
    monkeypatch.setenv("QMT_GATEWAY_REQUIRED", "1")
    monkeypatch.setenv("QMT_GATEWAY_ATTEMPTS", "2")
    monkeypatch.setenv("QMT_GATEWAY_RETRY_DELAY", "0")
    monkeypatch.setattr(bridge.urllib_request, "urlopen", urlopen)

    result = bridge._run_gateway({"action": "minute"}, timeout=30)

    assert result["ok"] is True
    assert len(calls) == 2


def test_gateway_retries_qmt_wrapped_winsock_network_error(monkeypatch):
    responses = iter(
        [
            _GatewayResponse(
                {
                    "ok": False,
                    "error": (
                        'RuntimeError: func:getFullTick, error:{ "error id" : 10054, '
                        '"error" : "远程主机强迫关闭了一个现有的连接。", "isNetError" : true }'
                    ),
                }
            ),
            _GatewayResponse({"ok": True, "rows": [{"stock_code": "000001"}]}),
        ]
    )
    calls = []

    def urlopen(_request, *, timeout):
        calls.append(timeout)
        return next(responses)

    monkeypatch.setenv("QMT_GATEWAY_ENABLED", "1")
    monkeypatch.setenv("QMT_GATEWAY_REQUIRED", "1")
    monkeypatch.setenv("QMT_GATEWAY_ATTEMPTS", "2")
    monkeypatch.setenv("QMT_GATEWAY_RETRY_DELAY", "0")
    monkeypatch.setattr(bridge.urllib_request, "urlopen", urlopen)

    result = bridge._run_gateway({"action": "current"}, timeout=30)

    assert result["ok"] is True
    assert len(calls) == 2


def test_qmt_stock_pool_filters_non_equity_sector_members(monkeypatch):
    requested = []
    members = pd.DataFrame(
        {
            "qmt_code": [
                "000001.SZ",
                "600519.SH",
                "810011.BJ",
                "899050.BJ",
                "920808.BJ",
                "910000.BJ",
            ]
        }
    )

    monkeypatch.setattr(qmt_info.bridge, "sector_members", lambda *_args, **_kwargs: members)

    def fake_details(codes, **_kwargs):
        requested.extend(codes)
        return pd.DataFrame(
            [
                {
                    "stock_code": code.split(".", 1)[0],
                    "short_name": code,
                    "exchange": code.split(".", 1)[1],
                    "list_date": "20260717",
                }
                for code in codes
            ]
        )

    monkeypatch.setattr(qmt_info.bridge, "instrument_details", fake_details)
    result = qmt_info.fetch_all_stock_codes()

    assert requested == ["000001.SZ", "600519.SH", "920808.BJ"]
    assert result["stock_code"].tolist() == ["000001", "600519", "920808"]


def test_transform_kline_filters_range_and_computes_change():
    backend = QmtBackend()
    frame = pd.DataFrame(
        {
            "open": [10.0, 10.5, 10.8],
            "high": [10.6, 10.9, 11.2],
            "low": [9.9, 10.2, 10.7],
            "close": [10.2, 10.8, 11.0],
            "volume": [100, 120, 140],
            "amount": [1000, 1200, 1400],
            "turnover": [1.1, 1.2, 1.3],
        },
        index=pd.to_datetime(["2026-06-10", "2026-06-11", "2026-06-12"]),
    )

    result = backend._transform_kline(  # pylint: disable=protected-access
        {"600519.SH": frame},
        short_name_map={"600519": "Kweichow Moutai"},
        start_date="2026-06-11",
        end_date="2026-06-12",
    )

    assert list(result["trade_date"]) == ["2026-06-11", "2026-06-12"]
    assert list(result["stock_code"]) == ["600519", "600519"]
    assert result.iloc[0]["pre_close"] == 10.2
    assert round(float(result.iloc[0]["change"]), 2) == 0.6
    assert round(float(result.iloc[1]["change_pct"]), 4) == 1.8519


def test_runtime_can_resolve_qmt_python_layout(monkeypatch, tmp_path: Path):
    site_packages = tmp_path / "Lib" / "site-packages"
    (site_packages / "xtquant").mkdir(parents=True)
    monkeypatch.setenv("QMT_PYTHON", str(tmp_path / "python.exe"))

    resolved = ensure_xtquant_on_path()

    assert resolved == str(site_packages)


def test_current_transform_accepts_fractional_qmt_timetag(monkeypatch):
    fake_xtquant = types.ModuleType("xtquant")
    fake_xtdata = types.ModuleType("xtdata")
    fake_xtquant.xtdata = fake_xtdata
    monkeypatch.setitem(sys.modules, "xtquant", fake_xtquant)
    monkeypatch.setitem(sys.modules, "xtquant.xtdata", fake_xtdata)

    from integrations.qmt.worker import _transform_current

    rows = _transform_current(
        {
            "000001.SZ": {
                "lastPrice": 10.2,
                "lastClose": 10.0,
                "volume": 100,
                "amount": 102000,
                "timetag": "20260626 15:00:00.1",
            }
        }
    )

    assert rows[0]["snapshot_at"] == "2026-06-26 15:00:00"
    assert rows[0]["stock_code"] == "000001"


def test_minute_transform_merges_opening_auction_and_converts_lots(monkeypatch):
    fake_xtquant = types.ModuleType("xtquant")
    fake_xtdata = types.ModuleType("xtdata")
    fake_xtquant.xtdata = fake_xtdata
    monkeypatch.setitem(sys.modules, "xtquant", fake_xtquant)
    monkeypatch.setitem(sys.modules, "xtquant.xtdata", fake_xtdata)

    from integrations.qmt.worker import _transform_minute

    frame = pd.DataFrame(
        {
            "close": [10.75, 10.82],
            "preClose": [10.77, 10.75],
            "volume": [2374, 24626],
            "amount": [2552050, 26555597],
        },
        index=pd.to_datetime(["2026-07-17 09:30:00", "2026-07-17 09:31:00"]),
    )
    rows = _transform_minute({"000001.SZ": frame}, trade_date="20260717")

    assert len(rows) == 1
    assert rows[0]["trade_time"] == "2026-07-17 09:31:00"
    assert rows[0]["trade_date"] == "2026-07-17"
    assert rows[0]["volume"] == 2_700_000
    assert rows[0]["amount"] == 29_107_647
    assert round(rows[0]["change"], 2) == 0.05
