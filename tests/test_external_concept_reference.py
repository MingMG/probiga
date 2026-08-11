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


def test_external_concept_reference_fails_fast_without_touching_tables(monkeypatch):
    class BrokenInfo(_Info):
        @staticmethod
        def all_concept_code_east():
            return pd.DataFrame(
                [{"concept_code": f"BK{i:03d}", "name": str(i)} for i in range(8)]
            )

        @staticmethod
        def concept_constituent_east(*, concept_code: str):
            raise ValueError(f"non-json response for {concept_code}")

    monkeypatch.setattr(sync_stock_info, "load_info", lambda: BrokenInfo())
    monkeypatch.setattr(sync_stock_info, "_sleep", lambda: None)
    monkeypatch.setenv("EXTERNAL_CONCEPT_PROBE_LIMIT", "3")

    try:
        sync_stock_info._fetch_external_concept_reference()
    except sync_stock_info.ExternalConceptSourceUnavailable as exc:
        assert "attempted=3" in str(exc)
        assert "preserving previous snapshots" in str(exc)
    else:
        raise AssertionError("systemic concept outage must stop before a destructive replace")
