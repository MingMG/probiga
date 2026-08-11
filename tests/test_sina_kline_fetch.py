# -*- coding: utf-8 -*-
from biz.stock_market.sina_kline_fetch import _eval_factor_data


def test_eval_factor_data_parses_json_payload():
    payload = 'var hfq={"data":[["2026-07-01","1.23"]]};\n'

    assert _eval_factor_data(payload) == {"data": [["2026-07-01", "1.23"]]}


def test_eval_factor_data_parses_legacy_literal_payload():
    payload = "var qfq={'data': [['2026-07-01', '0.98']]};\n"

    assert _eval_factor_data(payload) == {"data": [["2026-07-01", "0.98"]]}
