from copy import deepcopy
from datetime import date, timedelta
import json
import math

import pytest

from biz.review.technical_facts import compute_technical_facts


def bars(count=121, *, trend=False, flat_range=False):
    days, current = [], date(2026, 1, 1)
    while len(days) < count:
        if current.weekday() < 5:
            days.append(current.isoformat())
        current += timedelta(days=1)
    result = []
    for index, day in enumerate(days):
        close = 10 + index if trend else 10
        previous = result[-1]["close"] if result else close
        result.append({"stock_code": "000001", "trade_date": day, "adjust_type": 0, "k_type": 1,
                       "open": close, "high": close if flat_range else close + 1,
                       "low": close if flat_range else close - 1, "close": close,
                       "pre_close": previous, "volume": 100})
    return result


def quality(rows):
    return {"status": "VERIFIED", "unit": "shares", "amount_price_status": "VERIFIED", "sample_count": len(rows)}


def compute(rows, **kwargs):
    return compute_technical_facts(rows, trade_date=rows[-1]["trade_date"], **kwargs)


def test_step_after_120_flat_sessions_has_hand_calculated_ma_macd_kdj_and_crosses():
    rows = bars()
    rows[-1].update(open=10, high=12, low=9, close=11, volume=200)
    value = compute(rows, volume_quality=quality(rows))
    assert value["ma"]["5"]["last"] == pytest.approx(10.2)
    assert value["ma"]["5"]["previous"] == 10
    assert value["ma"]["5"]["last_deviation_pct"] == pytest.approx((11 / 10.2 - 1) * 100)
    assert value["ma"]["5"]["cross"] == "BULLISH"
    dif = 2 / 13 - 2 / 27
    assert value["macd"]["last"]["dif"] == pytest.approx(dif)
    assert value["macd"]["last"]["dea"] == pytest.approx(dif * 2 / 10)
    assert value["macd"]["last"]["hist"] == pytest.approx(2 * (dif - dif * 2 / 10))
    assert value["macd"]["previous"]["dif"] == pytest.approx(0)
    assert value["macd"]["cross"] == "BULLISH"
    rsv = 100 * 2 / 3
    k, d = (100 + rsv) / 3, (100 + (100 + rsv) / 3) / 3
    assert value["kdj"]["last"]["k"] == pytest.approx(k)
    assert value["kdj"]["last"]["d"] == pytest.approx(d)
    assert value["kdj"]["last"]["j"] == pytest.approx(3 * k - 2 * d)
    assert value["kdj"]["previous"]["k"] == pytest.approx(50)
    assert value["kdj"]["cross"] == "BULLISH"
    assert value["volume"]["ratio5"] == 2
    assert value["volume"]["ratio20"] == 2  # Current 200 is excluded from both means.
    assert value["volume"]["previous_ratio20"] == 1


def test_linear_trend_macd_matches_closed_form_ema_adjust_false():
    rows = bars(trend=True)
    value = compute(rows)
    a, b, r = 11 / 13, 25 / 27, 8 / 10
    def expected(t):
        dif = 7 - 12.5 * b ** t + 5.5 * a ** t
        dea = 7 * (1 - r ** t) - 12.5 * (1-r) * b * (b ** t-r ** t)/(b-r) + 5.5 * (1-r) * a * (a ** t-r ** t)/(a-r)
        return dif, dea
    for key, t in (("last", 120), ("previous", 119)):
        dif, dea = expected(t)
        assert value["macd"][key]["dif"] == pytest.approx(dif, abs=1e-11)
        assert value["macd"][key]["dea"] == pytest.approx(dea, abs=1e-11)
        assert value["macd"][key]["hist"] == pytest.approx(2 * (dif-dea), abs=1e-11)
    assert value["ma"]["60"]["last"] == pytest.approx(sum(row["close"] for row in rows[-60:]) / 60)
    assert value["metadata"]["macd"]["ema_adjust"] is False


def test_zero_range_kdj_is_explicit_neutral_and_never_nan_or_a_cross():
    value = compute(bars(flat_range=True))
    assert value["kdj"]["last"] == {"k": 50, "d": 50, "j": 50, "rsv": 50, "range_status": "NEUTRAL_FLAT_RANGE"}
    assert value["kdj"]["cross"] == "UNKNOWN"
    assert value["kdj"]["neutral_range_windows"] == 113
    assert value["macd"]["cross"] == "NONE"
    json.dumps(value, allow_nan=False)


@pytest.mark.parametrize("count", [1, 4, 5, 20, 59, 60, 119, 120, 121])
def test_each_window_and_both_warmups_are_gated_independently(count):
    value = compute(bars(count))
    for period in (5, 10, 20, 60):
        ma = value["ma"][str(period)]
        assert (ma["last"] is not None) == (count >= period)
        assert (ma["previous"] is not None) == (count > period)
    for name in ("macd", "kdj"):
        assert (value[name]["last"] is not None) == (count >= 120)
        assert (value[name]["previous"] is not None) == (count >= 121)
        if count < 121:
            assert value[name]["cross"] == "UNKNOWN"


@pytest.mark.parametrize("mutation,code", [
    (lambda rows: rows[-1].update(high=1), "invalid_ohlc_or_preclose"),
    (lambda rows: rows[10].update(pre_close=9), "previous_close_discontinuity_pending_verification"),
    (lambda rows: rows[10].update(adjust_type=1), "mixed_missing_or_nonzero_adjust_type"),
    (lambda rows: rows[-1].update(close=float("nan")), "invalid_ohlc_or_preclose"),
    (lambda rows: rows[-1].update(high=float("inf")), "invalid_ohlc_or_preclose"),
    (lambda rows: rows.reverse(), "duplicate_or_unordered_dates"),
    (lambda rows: rows[10].update(trade_date=rows[9]["trade_date"]), "duplicate_or_unordered_dates"),
    (lambda rows: rows[10].update(stock_code="000002"), "multiple_instruments"),
])
def test_unverified_price_series_never_gets_full_indicators(mutation, code):
    rows = bars()
    target = rows[-1]["trade_date"]
    mutation(rows)
    value = compute_technical_facts(rows, trade_date=target)
    assert value["status"] == "UNKNOWN"
    assert code in {item["code"] for item in value["issues"]}
    assert all(item["last"] is None for item in value["ma"].values())
    assert value["macd"]["last"] is None and value["kdj"]["last"] is None
    json.dumps(value, allow_nan=False)


def test_future_bar_is_rejected_instead_of_silently_used_or_backfilled():
    rows = bars()
    value = compute_technical_facts(rows, trade_date=rows[-2]["trade_date"])
    assert value["status"] == "UNKNOWN"
    assert "future_bar_after_cutoff" in {item["code"] for item in value["issues"]}


def test_missing_early_dates_allow_short_ma_but_not_macd_warmup():
    all_rows = bars(trend=True)
    expected = [row["trade_date"] for row in all_rows]
    value = compute(all_rows[7:], expected_dates=expected)
    assert value["coverage"]["contiguous_tail_rows"] == 114
    assert value["coverage"]["missing_dates_count"] == 7
    assert value["ma"]["60"]["last"] is not None
    assert value["macd"]["last"] is None and value["kdj"]["last"] is None


def test_known_middle_gap_is_not_misdiagnosed_as_a_price_adjustment():
    rows = bars(trend=True)
    expected = [row["trade_date"] for row in rows]
    incomplete = rows[:100] + rows[101:]
    value = compute(incomplete, expected_dates=expected)
    assert value["coverage"]["contiguous_tail_rows"] == 20
    assert value["ma"]["20"]["last"] is not None
    assert value["ma"]["20"]["previous"] is None
    assert value["ma"]["60"]["last"] is None
    assert "previous_close_discontinuity_pending_verification" not in {item["code"] for item in value["issues"]}


@pytest.mark.parametrize("change", [None, {"status": "SOURCE_VERIFIED"}, {"unit": ""},
    {"amount_price_status": "UNKNOWN"}, {"sample_count": 1}, {"trade_date": "2026-01-01"}])
def test_volume_needs_explicit_unit_and_amount_price_quality_for_this_input(change):
    rows = bars()
    receipt = quality(rows)
    if change is not None:
        receipt.update(change)
    value = compute(rows, volume_quality=None if change is None else receipt)
    assert value["ma"]["60"]["last"] is not None
    assert value["volume"]["status"] == "UNKNOWN"
    assert value["volume"]["ratio5"] is None and value["volume"]["ratio20"] is None
    assert value["volume"]["last_volume"] is None


def test_missing_volume_is_not_converted_to_zero():
    rows = bars()
    rows[-1]["volume"] = None
    value = compute(rows, volume_quality=quality(rows))
    assert value["coverage"]["volume_missing_or_invalid_rows"] == 1
    assert value["volume"]["ratio5"] is None
    assert value["volume"]["last_volume"] is None


def test_verified_actual_zero_volume_and_zero_denominator_have_distinct_meanings():
    rows = bars()
    rows[-1]["volume"] = 0
    value = compute(rows, volume_quality=quality(rows))
    assert value["volume"]["ratio5"] == 0
    for row in rows:
        row["volume"] = 0
    value = compute(rows, volume_quality=quality(rows))
    assert value["volume"]["ratio5"] is None and value["volume"]["ratio20"] is None
    assert value["volume"]["last_volume"] == 0
    json.dumps(value, allow_nan=False)


def test_overflowing_volume_ratio_is_unknown_instead_of_infinity():
    rows = bars()
    for row in rows:
        row["volume"] = 1e-300
    rows[-1]["volume"] = 1e300
    value = compute(rows, volume_quality=quality(rows))
    assert value["volume"]["ratio5"] is None
    json.dumps(value, allow_nan=False)


def test_index_rows_use_the_same_algorithm_without_stock_loader_or_mutation():
    rows = bars()
    for row in rows:
        row["index_code"] = row.pop("stock_code")
    before = deepcopy(rows)
    value = compute(rows)
    assert value["ma"]["60"]["last"] == pytest.approx(10)
    assert rows == before


def test_finite_extreme_prices_and_invalid_quality_metadata_never_emit_nan_or_infinity():
    rows = bars(flat_range=True)
    for row in rows:
        for field in ("open", "high", "low", "close", "pre_close"):
            row[field] = 1e308
    receipt = quality(rows)
    receipt["sample_count"] = float("nan")
    value = compute(rows, volume_quality=receipt)
    json.dumps(value, allow_nan=False)
    assert value["ma"]["60"]["last"] == pytest.approx(1e308)
    assert value["volume"]["ratio5"] is None
