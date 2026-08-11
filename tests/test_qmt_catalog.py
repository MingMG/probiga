from __future__ import annotations

from unittest.mock import patch

from integrations.qmt.catalog import CORE_PROBE_TO_REGISTRY_KEYS, PROVIDER_ID, _dedupe_capability_rows, api_definitions
from tools import setup_guojin_qmt_catalog


def test_catalog_definitions_have_unique_capability_keys():
    definitions = api_definitions()
    keys = [item.capability_key for item in definitions]

    assert len(keys) == len(set(keys))


def test_catalog_covers_required_business_data():
    definitions = api_definitions()
    names = {item.api_name for item in definitions}
    periods = {item.period for item in definitions if item.api_name == "get_market_data_ex"}

    assert PROVIDER_ID == "gj_qmt"
    assert {"get_instrument_detail", "get_sector_list", "get_index_weight", "get_financial_data"} <= names
    assert {"1d", "1m", "tick", "transactioncount1d", "orderflow1m", "announcement", "interactiveqa"} <= periods


def test_catalog_marks_embedded_only_apis_explicitly():
    embedded = {item.api_name for item in api_definitions() if item.execution_mode == "embedded_only"}

    assert {"get_longhubang", "get_hkt_details", "get_market_time"} <= embedded


def test_core_probe_maps_market_periods_to_registry_keys():
    assert "native:get_market_data_ex:1m" in CORE_PROBE_TO_REGISTRY_KEYS["stock_minute_bar"]
    assert "native:get_market_data_ex:5m" in CORE_PROBE_TO_REGISTRY_KEYS["stock_5m_bar"]
    assert "native:get_market_data_ex:transactioncount1m" in CORE_PROBE_TO_REGISTRY_KEYS["stock_flow_min"]
    assert "native:get_market_data_ex:l2quote" in CORE_PROBE_TO_REGISTRY_KEYS["stock_l2_quote"]
    assert "native:get_market_data_ex:announcement" in CORE_PROBE_TO_REGISTRY_KEYS["announcement"]


def test_dedupe_capability_rows_keeps_supported_result():
    rows = [
        {"provider": PROVIDER_ID, "capability_key": "native:get_market_data_ex:1d", "capability_status": "NO_DATA", "available": 0},
        {"provider": PROVIDER_ID, "capability_key": "native:get_market_data_ex:1d", "capability_status": "SUPPORTED", "available": 1},
    ]

    deduped = _dedupe_capability_rows(rows)

    assert len(deduped) == 1
    assert deduped[0]["capability_status"] == "SUPPORTED"


def test_setup_catalog_main_uses_batch_engine():
    engine = object()

    with patch("tools.setup_guojin_qmt_catalog.create_batch_engine", return_value=engine) as create_batch_engine, \
         patch("tools.setup_guojin_qmt_catalog.ensure_catalog_tables") as ensure_catalog_tables, \
         patch("tools.setup_guojin_qmt_catalog.seed_registry", return_value=10) as seed_registry, \
         patch("tools.setup_guojin_qmt_catalog.capabilities", return_value={"status": "ok"}) as capabilities, \
         patch("tools.setup_guojin_qmt_catalog.core_probe", return_value={"status": "ok"}) as core_probe, \
         patch("tools.setup_guojin_qmt_catalog.save_capabilities", return_value=8) as save_capabilities, \
         patch("tools.setup_guojin_qmt_catalog.complete_capability_ledger", return_value=2) as complete_ledger:
        assert setup_guojin_qmt_catalog.main() == 0

    create_batch_engine.assert_called_once_with(future=True)
    ensure_catalog_tables.assert_called_once_with(engine)
    seed_registry.assert_called_once_with(engine)
    capabilities.assert_called_once_with(timeout=30, force=True)
    core_probe.assert_called_once_with(timeout=45, force=True)
    save_capabilities.assert_called_once_with(engine, {"status": "ok"}, {"status": "ok"})
    complete_ledger.assert_called_once_with(engine)
