from __future__ import annotations

import pytest

from server.common.qmt_daily_market_truth import (
    QmtDailyMarketNotFinal,
    _qmt_datadir_receipt_finished_at,
)


def test_qmt_daily_receipt_accepts_next_day_completion_of_prior_session():
    receipt = {
        "started_at": "2026-10-30 16:25:40",
        "finished_at": "2026-11-01 13:51:24",
    }

    assert _qmt_datadir_receipt_finished_at(
        receipt,
        trade_date="2026-10-30",
    ) == "2026-11-01 13:51:24"


def test_qmt_daily_receipt_rejects_same_day_preclose_start():
    receipt = {
        "started_at": "2026-10-30 15:29:59",
        "finished_at": "2026-10-30 16:00:00",
    }

    with pytest.raises(
        QmtDailyMarketNotFinal,
        match="predates the market-data cutoff",
    ):
        _qmt_datadir_receipt_finished_at(
            receipt,
            trade_date="2026-10-30",
        )


def test_qmt_daily_receipt_accepts_same_day_postclose_start():
    receipt = {
        "started_at": "2026-10-30 15:30:00",
        "finished_at": "2026-10-30 16:00:00",
    }

    assert _qmt_datadir_receipt_finished_at(
        receipt,
        trade_date="2026-10-30",
    ) == "2026-10-30 16:00:00"


def test_qmt_daily_receipt_rejects_impossible_interval():
    receipt = {
        "started_at": "2026-10-30 16:25:00",
        "finished_at": "2026-10-30 16:24:59",
    }

    with pytest.raises(RuntimeError, match="ends before it starts"):
        _qmt_datadir_receipt_finished_at(
            receipt,
            trade_date="2026-10-30",
        )
