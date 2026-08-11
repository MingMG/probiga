# -*- coding: utf-8 -*-

import pandas as pd

from biz.stock_info import sync_stock_info


class _Info:
    @staticmethod
    def all_concept_code_east():
        return pd.DataFrame(
            [
                {"concept_code": "BK001", "name": "one"},
                {"concept_code": "BK002", "name": "two"},
            ]
        )

    @staticmethod
    def concept_constituent_east(*, concept_code: str):
        return pd.DataFrame([{"stock_code": "000001" if concept_code == "BK001" else "600000"}])


def test_external_concept_reference_is_fetched_before_atomic_replace(monkeypatch):
    monkeypatch.setattr(sync_stock_info, "load_info", lambda: _Info())
    monkeypatch.setattr(sync_stock_info, "_sleep", lambda: None)

    tables = sync_stock_info._fetch_external_concept_reference()

    assert tables["concept_catalog"]["concept_code"].tolist() == ["BK001", "BK002"]
    assert tables["concept_constituents"][["concept_code", "stock_code"]].to_dict("records") == [
        {"concept_code": "BK001", "stock_code": "000001"},
        {"concept_code": "BK002", "stock_code": "600000"},
    ]
