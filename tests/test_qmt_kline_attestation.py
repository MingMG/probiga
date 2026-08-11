# -*- coding: utf-8 -*-
from __future__ import annotations

import inspect

import pandas as pd

from integrations.qmt import local_history
from tools.attest_qmt_daily_kline import attest_range, values_match
from tools.run_qmt_kline_attestation_monthly import month_ranges


def test_attestation_match_uses_small_price_and_relative_flow_tolerances():
    source = {
        "open": 10.0,
        "close": 10.5,
        "high": 10.8,
        "low": 9.9,
        "volume": 1_000_000,
        "amount": 10_200_000,
    }
    target = {
        **source,
        "close": 10.50005,
        "volume": 1_000_050,
        "amount": 10_205_000,
    }
    assert values_match(target, source) is True
    assert values_match({**source, "volume": source["volume"] + 100}, source) is True
    assert values_match({**source, "volume": source["volume"] + 101}, source) is False
    assert values_match({**target, "close": 10.51}, source) is False
    assert values_match({**target, "volume": 1_010_000}, source) is False


def test_bigqmt_daily_backfill_defaults_to_raw_unadjusted(monkeypatch):
    signature = inspect.signature(local_history.backfill_daily_kline_local)
    assert signature.parameters["backend"].default == "bigqmt"
    assert signature.parameters["dividend_type"].default == "none"

    monkeypatch.setattr(local_history, "_short_name_map", lambda *_args, **_kwargs: {})
    rows = local_history._prepare_kline_rows(
        pd.DataFrame(
            [
                {
                    "stock_code": "000001",
                    "trade_time": "2026-07-24 15:00:00",
                    "trade_date": "2026-07-24",
                    "adjust_type": 0,
                    "open": 10,
                    "close": 11,
                    "high": 11,
                    "low": 10,
                    "volume": 1000,
                    "amount": 10500,
                    "data_source": "gj_big_qmt_inner",
                }
            ]
        ),
        source_engine=object(),
        period="1d",
        batch_id="batch",
        provider="gj_big_qmt_inner",
    )
    assert rows[0]["adjust_type"] == 0
    assert rows[0]["provider"] == "gj_big_qmt_inner"


def test_monthly_attestation_ranges_keep_requested_boundaries():
    assert list(month_ranges("2024-01-02", "2024-03-15")) == [
        ("2024-01-02", "2024-01-31"),
        ("2024-02-01", "2024-02-29"),
        ("2024-03-01", "2024-03-15"),
    ]


def test_attestation_apply_only_updates_newly_attested_rows():
    source = inspect.getsource(attest_range)
    assert "COALESCE((" in source
    assert "AND NOT COALESCE(q.provenance_already, 0)" in source
