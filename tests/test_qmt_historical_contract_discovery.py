from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pytest

from server.common.qmt_stock_catalog import canonical_catalog_members


def _load_worker(monkeypatch, xtdata):
    fake_xtquant = types.ModuleType("xtquant")
    fake_xtquant.xtdata = xtdata
    monkeypatch.setitem(sys.modules, "xtquant", fake_xtquant)
    module_name = "_probiga_qmt_worker_contract_test"
    monkeypatch.delitem(sys.modules, module_name, raising=False)
    path = Path(__file__).resolve().parents[1] / "integrations" / "qmt" / "worker.py"
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


class _HistoricalXtData:
    def __init__(self):
        self.download_count = 0
        self.members = {
            "沪市过期证券": ["600001.SH", "000001.SH"],
            "深市过期证券": ["000002.SZ"],
        }

    def download_history_contracts(self):
        self.download_count += 1

    def get_sector_list(self):
        return ["上证A股", *self.members]

    def get_stock_list_in_sector(self, sector_name, real_timetag=-1):
        assert real_timetag == -1
        return self.members.get(sector_name)


def test_native_history_download_binds_all_expired_sector_names_and_members(
    monkeypatch,
):
    xtdata = _HistoricalXtData()
    worker = _load_worker(monkeypatch, xtdata)

    payload = worker._historical_contract_catalog({})

    assert xtdata.download_count == 1
    assert payload["expired_sectors"] == ["沪市过期证券", "深市过期证券"]
    assert {(row["sector_name"], row["qmt_code"]) for row in payload["rows"]} == {
        ("沪市过期证券", "000001.SH"),
        ("沪市过期证券", "600001.SH"),
        ("深市过期证券", "000002.SZ"),
    }


def test_native_history_discovery_fails_if_any_expired_sector_is_unreadable(
    monkeypatch,
):
    xtdata = _HistoricalXtData()
    xtdata.members["深市过期证券"] = None
    worker = _load_worker(monkeypatch, xtdata)

    with pytest.raises(RuntimeError, match="membership is unavailable"):
        worker._historical_contract_catalog({})


def test_expired_candidate_without_equity_product_type_cannot_enter_catalog(
    monkeypatch,
):
    worker = _load_worker(monkeypatch, _HistoricalXtData())
    index_detail = worker._instrument_detail_row(
        "000001.SH",
        {
            "ExchangeID": "SH",
            "InstrumentName": "上证指数",
            "OpenDate": "19910715",
            "ExpireDate": "20260824",
            "ProductType": "INDEX",
        },
    )
    missing_detail = worker._instrument_detail_row("600001.SH", None)

    with pytest.raises(ValueError, match="not proven as equity"):
        canonical_catalog_members([{
            "qmt_code": index_detail["qmt_code"],
            "stock_code": index_detail["stock_code"],
            "list_date": "1991-07-15",
            "expire_date": "2026-08-24",
            "instrument_batch_id": "f" * 64,
            "instrument_type": index_detail["product_type"],
        }])
    with pytest.raises(ValueError, match="not proven as equity"):
        canonical_catalog_members([{
            "qmt_code": missing_detail["qmt_code"],
            "stock_code": missing_detail["stock_code"],
            "list_date": "1990-12-19",
            "expire_date": "2026-08-24",
            "instrument_batch_id": "e" * 64,
            "instrument_type": missing_detail["product_type"],
        }])
