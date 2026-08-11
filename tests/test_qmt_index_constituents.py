from __future__ import annotations

import pandas as pd

from integrations.qmt.info import fetch_index_constituents


def test_index_constituents_reject_non_stock_qmt_symbols(monkeypatch):
    monkeypatch.setattr(
        "integrations.qmt.info.bridge.index_weight_many",
        lambda _symbols, timeout: pd.DataFrame(
            [
                {"index_code": "000300", "stock_code": "000001", "qmt_code": "000001.SZ"},
                {"index_code": "000300", "stock_code": "110044", "qmt_code": "110044.SH"},
                {"index_code": "000300", "stock_code": "900901", "qmt_code": "900901.SH"},
            ]
        ),
    )
    seen: list[str] = []

    def details(symbols, *, batch_size, timeout):
        seen.extend(symbols)
        return pd.DataFrame([{"stock_code": "000001", "short_name": "平安银行"}])

    monkeypatch.setattr("integrations.qmt.info.bridge.instrument_details", details)

    result = fetch_index_constituents(["000300"])

    assert seen == ["000001.SZ"]
    assert result[["index_code", "stock_code"]].to_dict("records") == [
        {"index_code": "000300", "stock_code": "000001"}
    ]
