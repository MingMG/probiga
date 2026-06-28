from __future__ import annotations

import sys
import types
from pathlib import Path

import pandas as pd

from integrations.qmt.backend import QmtBackend, from_qmt_symbol, to_qmt_symbol
from integrations.qmt.runtime import ensure_xtquant_on_path


def test_qmt_symbol_mapping_roundtrip():
    assert to_qmt_symbol("600519") == "600519.SH"
    assert to_qmt_symbol("000001") == "000001.SZ"
    assert to_qmt_symbol("830799") == "830799.BJ"
    assert from_qmt_symbol("600519.SH") == "600519"


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
