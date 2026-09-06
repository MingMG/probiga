from datetime import datetime, timezone
from decimal import Decimal

import pytest

from acquisition.datasets import DATASETS, get_spec
from acquisition.models import WorkUnit
from acquisition.normalize import NormalizationError, normalize_batch


DATE = "2026-09-04"
RECEIVED = "2026-09-04T19:00:00+08:00"


def _batch(dataset="stock_daily", code="000001.SZ", *, rows=None, outcome=None, **request_changes):
    spec = get_spec(dataset)
    request = dict(request_id="request-1", dataset=dataset, source=spec.source,
                   codes=[code], start_date=DATE, end_date=DATE, period=spec.period,
                   adjustment="none", requested_at="2026-09-04T15:00:00+08:00", deadline_at=RECEIVED)
    request.update(request_changes)
    return {"request": request, "received_at": RECEIVED, "source_method": "ContextInfo.get_market_data_ex" if dataset == "capital_flow_daily" else "get_market_data_ex",
            "outcomes": {code: outcome or {"status": "data", "rows": rows if rows is not None else [_bar()]}}}


def _bar(**changes):
    return {"trade_time": "2026-09-04T15:00:00+08:00", "trade_date": DATE,
            "open": "101", "high": "102", "low": "99", "close": "100",
            "pre_close": "101", "volume": "12.3456789", "amount": "1234.5678912", **changes}


def _normalize(batch, **kwargs):
    spec = get_spec(batch["request"]["dataset"])
    factors = {(batch["source_method"], spec.period, spec.asset_class): {"volume": 1, "amount": 1}}
    return normalize_batch(spec, batch, volume_factors=kwargs.pop("volume_factors", factors), **kwargs)


def test_fixed_products_and_internal_exchange_identity():
    assert len(DATASETS) == 13
    assert WorkUnit("stock_daily", "guojin_qmt", DATE, "000001.SZ").partition_key == "000001.SZ:1d:none"
    assert WorkUnit("index_daily", "guojin_qmt", DATE, "000001.SH").partition_key != WorkUnit("index_daily", "guojin_qmt", DATE, "000001.SZ").partition_key
    assert get_spec("stock_daily").database == "history"
    assert get_spec("etf_daily").database == "primary"
    assert get_spec("capital_flow_daily").database == "minute"
    with pytest.raises(ValueError):
        get_spec("unreviewed-product")


def test_daily_decimal_negative_change_and_configured_units():
    batch = _batch()
    result = _normalize(batch, volume_factors={("get_market_data_ex", "1d", "stock"): {"volume": 100, "amount": 1}})
    row = result.units[0].rows[0]
    assert result.units[0].status == "complete"
    assert row["volume"] == Decimal("1234.567890")
    assert row["change"] == Decimal("-1.000000")
    assert row["change_pct"] == Decimal("-0.990099")
    assert row["trade_time"] == datetime(2026, 9, 4, 15)
    assert result.received_at.utcoffset().total_seconds() == 28800


@pytest.mark.parametrize("field,value", [("volume", None), ("volume", -1), ("amount", "nan"), ("close", "Infinity"), ("close", 0)])
def test_invalid_numbers_are_errors_not_zero(field, value):
    result = _normalize(_batch(rows=[_bar(**{field: value})]))
    assert result.units[0].status == "error"
    assert result.units[0].rows == []


def test_absent_native_reference_price_stays_missing():
    row = _normalize(_batch(rows=[_bar(pre_close=None)])).units[0].rows[0]
    assert row["pre_close"] is None
    assert row["change"] is None
    assert row["change_pct"] is None


def test_missing_unit_contract_is_explicit_unsupported():
    result = _normalize(_batch(), volume_factors={})
    assert result.units[0].error_code == "UNSUPPORTED_UNITS"


def test_missing_and_bad_time_are_not_filled_from_request():
    result = _normalize(_batch(rows=[_bar(trade_time=None)]))
    assert result.units[0].error_code == "MISSING_SOURCE_TIME"
    result = _normalize(_batch(rows=[_bar(trade_time="2026-09-03T15:00:00+08:00")]))
    assert result.units[0].error_code == "WRONG_DATE"


def test_delayed_native_result_compares_with_receipt_not_request_start():
    batch = _batch("stock_current", rows=[_bar(price=100, trade_time="2026-09-04T07:01:00Z")])
    result = _normalize(batch)
    assert result.units[0].status == "complete"
    assert result.units[0].rows[0]["trade_time"] == datetime(2026, 9, 4, 15, 1)


def test_missing_outcome_does_not_discard_successful_security():
    batch = _batch()
    batch["request"]["codes"].append("000002.SZ")
    result = _normalize(batch)
    assert [item.status for item in result.units] == ["complete", "error"]
    assert result.units[1].error_code == "MISSING_OUTCOME"


@pytest.mark.parametrize("change", [{"source": "mini_qmt"}, {"adjustment": "follow"}, {"end_date": "2026-09-05"}, {"codes": ["000001"]}])
def test_wrong_request_contract_never_defaults(change):
    with pytest.raises(NormalizationError):
        _normalize(_batch(**change))


def test_wrong_exchange_and_duplicate_business_key_rejected():
    assert _normalize(_batch(rows=[_bar(qmt_code="000001.SH")])).units[0].error_code == "WRONG_CODE"
    assert _normalize(_batch(rows=[_bar(), _bar()])).units[0].error_code == "DUPLICATE_OR_MISSING_KEY"


def test_legitimate_no_data_is_distinct_from_empty_response():
    assert _normalize(_batch(outcome={"status": "no_data", "reason": "suspended", "rows": []})).units[0].status == "no_data"
    assert _normalize(_batch(outcome={"status": "data", "rows": []})).units[0].status == "error"
    assert _normalize(_batch(outcome={"status": "no_data", "reason": "provider_empty", "rows": []})).units[0].error_code == "UNPROVEN_NO_DATA"


def test_etf_existing_decimal_scales_without_fabricated_validation_or_permission():
    result = _normalize(_batch("etf_daily", "510300.SH", rows=[_bar(short_name="ETF")]))
    row = result.units[0].rows[0]
    assert row["volume"] == Decimal("12.3457")
    assert row["change_pct"] == Decimal("-0.99009901")
    assert "validation_source" not in row and "validation_status" not in row
    assert "quality_status" not in row and "permission_status" not in row
    assert "validation_price_max_delta" not in row and "validation_volume_delta_pct" not in row
    assert "etl_sync_at" not in row
    assert len(row["data_version"]) == 64


def test_etf_native_preclose_and_storage_precision_are_required():
    batch = _batch("etf_daily", "510300.SH", rows=[_bar(short_name="ETF", pre_close=None)])
    assert _normalize(batch).units[0].error_code == "MISSING_FIELD"
    batch = _batch("etf_daily", "510300.SH", rows=[_bar(short_name="ETF", close="1000000000000")])
    assert _normalize(batch).units[0].error_code == "INVALID_NUMBER"


def test_optional_security_minute_grid_filters_without_rejecting_halts():
    rows = [_bar(trade_time=f"{DATE} {minute}") for minute in ("09:30:00", "09:31:00", "16:00:00")]
    batch = _batch("index_minute", "980001.SZ", rows=rows)
    grid = {("index", "980001.SZ"): ["09:30:00", "09:31:00"]}
    result = _normalize(batch, minute_grids=grid)
    assert result.units[0].status == "complete"
    assert len(result.units[0].rows) == 2
    assert result.units[0].detail["out_of_scope_rows"] == 1
    batch["outcomes"]["980001.SZ"]["rows"].pop(0)
    result = _normalize(batch, minute_grids=grid)
    assert result.units[0].status == "complete"
    assert result.units[0].detail["missing_expected_rows"] == 1


def test_minute_without_grid_accepts_native_minute_aligned_rows():
    assert _normalize(_batch("stock_minute")).units[0].status == "complete"
    batch = _batch("stock_minute", rows=[_bar(trade_time=f"{DATE} 15:00:01")])
    assert _normalize(batch).units[0].error_code == "WRONG_TIME_GRID"


def test_flow_preserves_signed_yuan_and_rejects_unsupported_market():
    row = dict(native_index="20260904", bidMostAmount="5", offMostAmount="9",
               bidBigAmount="10", offBigAmount="17", bidMediumAmount="5",
               offMediumAmount="3", bidSmallAmount="2", offSmallAmount="1")
    result = _normalize(_batch("capital_flow_daily", rows=[row]))
    saved = result.units[0].rows[0]
    assert saved["main_net_inflow"] == Decimal("-11.000000")
    assert saved["max_net_inflow"] == Decimal("-4.000000")
    assert saved["sm_net_inflow"] == Decimal("1.000000")
    assert saved["data_source"] == "gj_big_qmt_inner"
    assert _normalize(_batch("capital_flow_daily", "920001.BJ", rows=[row])).units[0].error_code == "UNSUPPORTED_MARKET"


@pytest.mark.parametrize("field,value,error", [
    ("bidMostAmount", "-1", "INVALID_NUMBER"),
    ("offSmallAmount", "nan", "INVALID_NUMBER"),
])
def test_flow_rejects_invalid_native_bid_off(field, value, error):
    row = dict(native_index="20260904", bidMostAmount="5", offMostAmount="9",
               bidBigAmount="10", offBigAmount="17", bidMediumAmount="5",
               offMediumAmount="3", bidSmallAmount="2", offSmallAmount="1")
    row[field] = value
    assert _normalize(_batch("capital_flow_daily", rows=[row])).units[0].error_code == error


def test_flow_rejects_all_zero_native_bid_off():
    row = {"native_index": "20260904"}
    for fields in (("bidMostAmount", "offMostAmount"), ("bidBigAmount", "offBigAmount"),
                   ("bidMediumAmount", "offMediumAmount"), ("bidSmallAmount", "offSmallAmount")):
        row.update({field: 0 for field in fields})
    assert _normalize(_batch("capital_flow_daily", rows=[row])).units[0].error_code == "EMPTY_FLOW_VALUES"


def test_finance_preserves_nullable_publication_and_rejects_future_period():
    raw = {"stock_code": "000001", "report_date": "2026-06-30", "report_type": "half_year", "notice_date": None, "source_update_date": "2026-09-04", "basic_eps": "0.30"}
    result = _normalize(_batch("finance", rows=[raw]))
    assert result.units[0].rows[0]["notice_date"] is None
    assert result.units[0].rows[0]["basic_eps"] == Decimal("0.30")
    assert result.units[0].detail["revision_rows"] == [raw]
    raw["report_date"] = "2026-09-30"
    assert _normalize(_batch("finance", rows=[raw])).units[0].error_code == "INVALID_REPORT_PERIOD"


def test_notice_security_association_is_not_assumed_from_request():
    row = dict(stock_code="000001", art_code="AN123", notice_date=DATE, title="Notice", source_security_codes=["000002"])
    assert _normalize(_batch("notices", rows=[row])).units[0].error_code == "UNPROVEN_ASSOCIATION"
    row["source_security_codes"] = ["000001"]
    assert _normalize(_batch("notices", rows=[row])).units[0].status == "complete"


def test_billboard_keeps_buy_sell_and_multiple_reason_identity():
    row = dict(stock_code="000001", trade_date=DATE, operate_code="ORG1", operate_name="department", trade_id="T1", report_side="BUY", a_buy_amount="12")
    batch = _batch("alist_detail", rows=[row, {**row, "report_side": "SELL"}, {**row, "trade_id": "T2"}])
    assert len(_normalize(batch).units[0].rows) == 3


def test_reference_is_not_silently_published_as_market_data():
    batch = _batch("reference", rows=[{}])
    assert _normalize(batch).units[0].status == "error"


def test_native_dataframe_index_and_tick_stime_are_real_time_inputs():
    bar = _bar()
    bar.pop("trade_time")
    bar["native_index"] = "20260904150000"
    assert _normalize(_batch(rows=[bar])).units[0].status == "complete"
    bar.pop("native_index")
    bar["stime"] = "20260904150000"
    assert _normalize(_batch("stock_current", rows=[bar])).units[0].status == "complete"


def test_finance_nan_is_not_a_successful_zero_or_null_metric():
    raw = {"stock_code": "000001", "report_date": "2026-06-30", "report_type": "half_year", "basic_eps": "NaN"}
    assert _normalize(_batch("finance", rows=[raw])).units[0].error_code == "INVALID_NUMBER"
    assert _normalize(_batch("finance", outcome={"status": "no_data", "reason": "empty_event_set", "rows": []})).units[0].status == "no_data"


@pytest.mark.parametrize("volume,amount,traded", [(0, 0, False), (1, 100, True)])
def test_stock_daily_activity_is_explicit_for_flow_dependency(volume, amount, traded):
    result = _normalize(_batch(rows=[_bar(volume=volume, amount=amount)]))
    assert result.units[0].status == "complete"
    assert result.units[0].detail["traded"] is traded


@pytest.mark.parametrize("volume,amount", [(0, 100), (1, 0)])
def test_stock_daily_conflicting_activity_does_not_shrink_flow_universe(volume, amount):
    result = _normalize(_batch(rows=[_bar(volume=volume, amount=amount)]))
    assert result.units[0].error_code == "INCONSISTENT_ACTIVITY"
    assert "traded" not in result.units[0].detail


def test_notice_date_is_displayed_without_invented_midnight_source_time():
    raw = dict(stock_code="000001", art_code="AN123", notice_date=DATE, title="Notice", source_security_codes=["000001"], display_time=DATE, source_time=DATE)
    result = _normalize(_batch("notices", rows=[raw]))
    assert result.units[0].rows[0]["display_time"] == DATE
    assert result.units[0].rows[0]["source_time"] is None
    raw.update(display_time=DATE + " 18:25:37", source_time=DATE + " 18:25:37")
    result = _normalize(_batch("notices", rows=[raw]))
    assert result.units[0].rows[0]["source_time"] == datetime(2026, 9, 4, 18, 25, 37)


@pytest.mark.parametrize("updated", [DATE, DATE + " 18:25", DATE + " 18:25:37.123456+08:00"])
def test_finance_keeps_available_update_precision_without_fabricating_seconds(updated):
    raw = dict(stock_code="000001", report_date="2026-06-30", report_type="half_year", source_update_date=updated, basic_eps="0.30")
    result = _normalize(_batch("finance", rows=[raw]))
    assert result.units[0].rows[0]["source_update_date"] == updated


def test_finance_late_revision_uses_actual_receipt_not_scan_date_as_knowledge_limit():
    raw = dict(stock_code="000001", report_date="2026-06-30", report_type="half_year", notice_date="2026-09-05", source_update_date="2026-09-05 08:30:00", basic_eps="0.31")
    batch = _batch("finance", rows=[raw])
    batch["received_at"] = "2026-09-05T09:00:00+08:00"
    result = _normalize(batch)
    assert result.units[0].status == "complete"
    assert result.units[0].unit.target_date == "2026-09-04"
    assert result.units[0].rows[0]["source_update_date"] == "2026-09-05 08:30:00"
    raw["source_update_date"] = "2026-09-05 10:00:00"
    assert _normalize(batch).units[0].error_code == "FUTURE_SOURCE_DATE"
