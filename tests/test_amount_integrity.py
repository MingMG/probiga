from copy import deepcopy

import pandas as pd
import pytest

from biz.review.amount_integrity import assess_amount_integrity, apply_amount_integrity


DAY = "2026-10-30"
PREVIOUS = "2026-10-29"


def _row(**changes):
    # Observed 10/30 price/volume sample; the reported monetary field was bad.
    row = dict(stock_code="300502", trade_date=DAY, adjust_type=0,
               low=194, high=214.4, amount=132_768_394, volume=106_030_100)
    row.update(changes)
    return row


def test_observed_bad_monetary_sample_is_unknown_without_guessing_a_multiplier():
    row = _row()
    result = assess_amount_integrity([row], trade_date=DAY)
    assert result["status"] == "UNKNOWN"
    assert result["inconsistent_count"] == 1
    assert result["samples"][0]["reason"] == "implied_price_outside_daily_range"
    assert result["samples"][0]["amount"] == 132_768_394
    assert result["samples"][0]["implied_price"] < 2
    assert row["amount"] == 132_768_394


def test_price_volume_consistency_is_computable_for_independently_reported_amount():
    result = assess_amount_integrity([_row(amount=21_608_000_000)], trade_date=DAY)
    assert result["status"] == "OBSERVED"
    assert result["checked_count"] == result["sample_count"] == 1


@pytest.mark.parametrize("changes,reason", [
    ({"volume": None}, "missing_amount_or_volume"),
    ({"amount": None}, "missing_amount_or_volume"),
    ({"amount": float("inf")}, "missing_amount_or_volume"),
    ({"volume": 0}, "amount_volume_zero_mismatch"),
    ({"amount": 0}, "amount_volume_zero_mismatch"),
    ({"amount": -1}, "negative_amount_or_volume"),
    ({"adjust_type": 1}, "unadjusted_prices_required"),
    ({"low": None}, "invalid_or_missing_price_range"),
    ({"trade_date": PREVIOUS}, "wrong_or_missing_trade_date"),
])
def test_missing_or_incomparable_inputs_fail_closed(changes, reason):
    result = assess_amount_integrity([_row(**changes)], trade_date=DAY)
    assert result["status"] == "UNKNOWN"
    assert result["samples"][0]["reason"] == reason


def test_suspension_is_zero_without_vwap_and_one_price_trade_can_be_valid():
    result = assess_amount_integrity([
        _row(amount=0, volume=0, low=None, high=None),
        _row(stock_code="603131", amount=17_550, volume=1_000, low=17.55, high=17.55),
    ])
    assert result["status"] == "OBSERVED"
    assert result["suspended_count"] == 1
    assert result["checked_count"] == 2


def test_samples_are_bounded_but_every_row_is_counted_and_duplicates_are_unknown():
    result = assess_amount_integrity([_row(stock_code=str(index)) for index in range(25)], max_samples=3)
    assert result["inconsistent_count"] == 25
    assert len(result["samples"]) == 3
    duplicated = assess_amount_integrity([_row(amount=21_608_000_000)] * 2)
    assert duplicated["status"] == "UNKNOWN"
    assert duplicated["samples"][0]["reason"] == "duplicate_stock_code"


def _digest():
    return {
        "review_date": DAY, "compact_review": "old turnover prose",
        "quality_json": {"status": "pass", "source_dates": {"previous_bars": PREVIOUS}},
        "market_structure_json": {
            "sample_count": 1, "previous_sample_count": 1,
            "total_amount": 1, "previous_total_amount": 1, "amount_change_pct": 0,
            "amount_weighted_return_pct": -1, "return_median_pct": -14.1856,
            "decision_facts": {"breadth": {"up_count": 0}, "stock_roles": {"candidates": [
                {"identity": "相对强度前排候选", "evidence": {"return_pct": -14.1856, "amount": 1, "industry_amount_share": 1}},
                {"identity": "成交额容量核心候选", "evidence": {"amount": 1}},
            ]}},
        },
        "industry_rotation_json": {"status": "available", "amount_concentration_top5": 1,
            "industries": [{"industry_name": "通信", "median_return_pct": -5, "amount": 1, "amount_share": 1, "rank": 1}],
            "previous_comparison_available": True, "rotation_state": "延续"},
    }


def test_frozen_digest_copy_cannot_preserve_bad_amount_prose_or_amount_based_identity():
    digest = _digest()
    original = deepcopy(digest)
    previous = assess_amount_integrity([_row(trade_date=PREVIOUS, amount=21_608_000_000)])
    result = apply_amount_integrity(digest, target_quality=assess_amount_integrity([_row()]), previous_quality=previous)
    market = result["market_structure_json"]
    assert digest == original
    assert market["total_amount"] is None
    assert market["previous_total_amount"] == 1
    assert market["amount_change_pct"] is None
    assert market["amount_weighted_return_pct"] is None
    assert market["return_median_pct"] == -14.1856
    assert market["decision_facts"]["breadth"] == {"up_count": 0}
    candidates = market["decision_facts"]["stock_roles"]["candidates"]
    assert len(candidates) == 1
    assert candidates[0]["evidence"]["return_pct"] == -14.1856
    assert candidates[0]["evidence"]["amount"] is None
    assert result["industry_rotation_json"]["industries"][0]["rank"] is None
    assert result["industry_rotation_json"]["industries"][0]["median_return_pct"] == -5
    assert result["compact_review"] == ""
    assert result["quality_json"]["status"] == "warn"


def test_partial_or_wrong_date_receipt_cannot_validate_a_whole_frozen_market():
    receipt = assess_amount_integrity([_row(amount=21_608_000_000)])
    digest = _digest()
    digest["market_structure_json"]["sample_count"] = 5_571
    result = apply_amount_integrity(digest, target_quality=receipt)
    assert result["market_structure_json"]["total_amount"] is None
    assert result["quality_json"]["amount_integrity"]["current"]["reason"] == "receipt_sample_count_mismatch"
    receipt["trade_date"] = PREVIOUS
    result = apply_amount_integrity(_digest(), target_quality=receipt)
    assert result["quality_json"]["amount_integrity"]["current"]["reason"] == "receipt_date_mismatch"


def test_frozen_previous_amount_tie_break_cannot_survive_bad_previous_receipt():
    digest = _digest()
    industry = digest["industry_rotation_json"]
    industry.update(rank_correlation=0.9, top3_overlap_count=3, top3_overlap=["通信"])
    industry["industries"][0].update(previous_rank=2, rank_change=1)
    current = assess_amount_integrity([_row(amount=21_608_000_000)])
    previous = assess_amount_integrity([_row(trade_date=PREVIOUS)])
    result = apply_amount_integrity(digest, target_quality=current, previous_quality=previous)
    updated = result["industry_rotation_json"]
    assert updated["industries"][0]["rank"] == 1
    assert updated["industries"][0]["amount"] == 1
    assert updated["industries"][0]["previous_rank"] is None
    assert updated["industries"][0]["rank_change"] is None
    assert updated["previous_comparison_available"] is False
    assert updated["rank_correlation"] is None
    assert updated["top3_overlap_count"] is None
    assert updated["top3_overlap"] == []
    assert updated["rotation_state"] == "数据不足"
    assert digest["industry_rotation_json"]["industries"][0]["previous_rank"] == 2
