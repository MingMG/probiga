from __future__ import annotations

from integrations.qmt.backend import dividend_type_to_adjust_type
from integrations.qmt.info import CORE_INDEXES, to_qmt_index_symbol


def test_to_qmt_index_symbol_maps_sh_and_sz_indexes():
    assert to_qmt_index_symbol("000300") == "000300.SH"
    assert to_qmt_index_symbol("399001") == "399001.SZ"
    assert to_qmt_index_symbol("000001.SH") == "000001.SH"


def test_dividend_type_to_adjust_type_defaults_to_none():
    assert dividend_type_to_adjust_type("none") == 0
    assert dividend_type_to_adjust_type("front") == 1
    assert dividend_type_to_adjust_type("hfq") == 2


def test_core_indexes_include_major_benchmarks():
    assert CORE_INDEXES["000300.SH"] == "沪深300"
    assert CORE_INDEXES["000905.SH"] == "中证500"
