from __future__ import annotations

import sys
import types
from pathlib import Path

import pandas as pd

from integrations.qmt.backend import QmtBackend, from_qmt_symbol, to_qmt_symbol
from integrations.qmt.runtime import (
    ensure_xtquant_on_path,
    qmt_connection_port_candidates,
)


def test_qmt_symbol_mapping_roundtrip():
    assert to_qmt_symbol("600519") == "600519.SH"
    assert to_qmt_symbol("000001") == "000001.SZ"
    assert to_qmt_symbol("830799") == "830799.BJ"
    assert to_qmt_symbol("920001") == "920001.BJ"
    assert to_qmt_symbol("900901") == "900901.SH"
    assert from_qmt_symbol("600519.SH") == "600519"


def test_transform_kline_filters_range_and_computes_change():
    backend = QmtBackend()
    frame = pd.DataFrame(
        {
            "open": [10.0, 10.5, 10.8],
            "high": [10.6, 10.9, 11.2],
            "low": [9.9, 10.2, 10.7],
            "close": [10.2, 10.8, 11.0],
            "preClose": [9.8, 10.1, 10.8],
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
    assert result.iloc[0]["pre_close"] == 10.1
    assert round(float(result.iloc[0]["change"]), 2) == 0.7
    assert round(float(result.iloc[1]["change_pct"]), 4) == 1.8519


def test_transform_kline_never_invents_pre_close_from_previous_raw_close():
    backend = QmtBackend()
    frame = pd.DataFrame(
        {
            "open": [10.0, 8.0],
            "high": [10.2, 8.2],
            "low": [9.8, 7.8],
            "close": [10.0, 8.0],
            "volume": [100, 100],
            "amount": [1000, 800],
        },
        index=pd.to_datetime(["2026-06-10", "2026-06-11"]),
    )

    result = backend._transform_kline(
        {"600519.SH": frame},
        short_name_map={},
        start_date="2026-06-10",
        end_date="2026-06-11",
    )

    assert result["pre_close"].isna().all()
    assert result["change"].isna().all()


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


def test_worker_connect_never_uses_bigqmt_58600_as_default_fallback(
    monkeypatch,
):
    fake_xtquant = types.ModuleType("xtquant")
    fake_xtdata = types.ModuleType("xtdata")
    fake_xtquant.xtdata = fake_xtdata
    monkeypatch.setitem(sys.modules, "xtquant", fake_xtquant)
    monkeypatch.setitem(sys.modules, "xtquant.xtdata", fake_xtdata)

    from integrations.qmt import worker

    attempted: list[int] = []

    def connect(*, port, remember_if_success):
        assert remember_if_success is False
        attempted.append(port)
        if port != 58610:
            raise RuntimeError("not this QMT desktop port")

    monkeypatch.setenv("QMT_PORT", "59999")
    monkeypatch.setattr(worker.xtdata, "connect", connect, raising=False)

    assert worker._connect() == 58610
    assert attempted == [59999, 58610]
    assert 58600 not in qmt_connection_port_candidates("")
