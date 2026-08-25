from __future__ import annotations

import inspect

import pandas as pd
import pytest

from server.trading_v2 import job_worker
from tools import backtest_etf_ensemble as ensemble


def _snapshot_row(**overrides):
    row = {
        "etf_code": "510300",
        "trade_date": "2026-08-24",
        "adjust_type": 0,
        "data_version": "a" * 64,
        "received_at": "2026-08-24 16:00:00",
        "open": 4.0,
        "close": 4.1,
        "pre_close": 4.05,
        "amount": 100_000_000,
        "validation_status": "passed",
        "quality_status": "validated",
        "asset_class": "A股宽基",
        "classification_updated_at": "2026-08-24 16:30:00",
    }
    row.update(overrides)
    return row


def test_etf_truth_quarantines_current_classification_and_missing_revision_ledger():
    truth = job_worker._etf_research_truth_contract([_snapshot_row()])

    assert truth["native_unadjusted_prices_only"] is True
    assert truth["adjusted_history_rows_consumed"] is False
    assert truth["historical_classification_verified"] is False
    assert truth["current_classification_can_authorize_promotion"] is False
    assert truth["activation_eligible"] is False
    assert set(truth["promotion_blockers"]) == set(
        job_worker.ETF_MUTABLE_INPUT_BLOCKERS
    )
    assert len(truth["contract_hash"]) == 64


@pytest.mark.parametrize(
    "replacement",
    [
        {"adjust_type": 1},
        {"data_version": ""},
        {"received_at": None},
        {"pre_close": 0},
        {"classification_updated_at": None},
    ],
)
def test_etf_truth_rejects_non_native_or_unversioned_rows(replacement):
    with pytest.raises(RuntimeError):
        job_worker._etf_research_truth_contract([
            _snapshot_row(**replacement)
        ])


def test_etf_loader_derives_continuous_prices_without_adjusted_history(monkeypatch):
    rows = [
        {
            "etf_code": "510300",
            "short_name": "300ETF",
            "trade_date": "2026-08-21",
            "open": 9.8,
            "close": 10.0,
            "pre_close": 9.5,
            "amount": 10_000_000,
            "asset_class": "A股宽基",
        },
        {
            "etf_code": "510300",
            "short_name": "300ETF",
            "trade_date": "2026-08-24",
            "open": 10.5,
            "close": 11.0,
            "pre_close": 10.0,
            "amount": 12_000_000,
            "asset_class": "A股宽基",
        },
    ]
    observed = {}

    def reader(_engine, sql, **_kwargs):
        observed["sql"] = " ".join(sql.split())
        return rows

    monkeypatch.setattr(ensemble, "read_sql_rows", reader)

    data = ensemble.load_market_data(
        object(), "2026-08-21", "2026-08-24"
    )

    assert "k.adjust_type = 0" in observed["sql"]
    assert "k.adjust_type = 1" not in observed["sql"]
    assert data.close["510300"].tolist() == pytest.approx([10.0, 11.0])
    assert data.open["510300"].tolist() == pytest.approx([9.8, 10.5])


def test_etf_job_snapshot_never_selects_adjusted_history():
    source = inspect.getsource(job_worker._etf_backtest)

    assert "WHERE k.adjust_type = 0" in source
    assert "WHERE k.adjust_type = 1" not in source
    assert "ETF_MUTABLE_INPUT_BLOCKERS" in source
