from datetime import date

import pytest

from acquisition.normalize import NormalizationError
from acquisition.reference import extract_sector_codes, merge_calendar_rows, normalize_reference


DAY = "2026-09-04"


def batch(period="instrument", codes=None, rows=None):
    codes = codes or ["000001.SZ"]
    return {"request": {"request_id": "ref-1", "dataset": "reference", "source": "guojin_qmt",
            "period": period, "adjustment": "none", "codes": codes, "start_date": DAY, "end_date": DAY},
            "received_at": "2026-09-05T08:00:00+08:00",
            "source_method": {"instrument": "ContextInfo.get_instrument_detail", "sector": "ContextInfo.get_stock_list_in_sector", "calendar": "ContextInfo.get_trading_dates"}[period],
            "outcomes": {code: {"status": "data", "rows": rows if rows is not None else [instrument(code)]} for code in codes}}


def instrument(code="000001.SZ", **changes):
    return dict(InstrumentID=code[:6], ExchangeID=code[-2:], InstrumentName="native name", OpenDate="19910403", ExpireDate=0, qmt_code=code, **changes)


@pytest.mark.parametrize("asset,table,column", [("stock", "si_all_code", "stock_code"), ("index", "si_all_index_code", "index_code"), ("etf", "si_etf_code", "etf_code")])
def test_directory_spec_and_native_fields(asset, table, column):
    spec, result = normalize_reference(batch(), asset)
    assert spec.name == "reference" and spec.period == "instrument"
    assert spec.table == table and spec.database == "primary" and spec.key_columns == (column,)
    unit = result.units[0]
    assert unit.status == "complete" and unit.unit.partition_key == "000001.SZ:instrument:none"
    assert unit.rows[0][column] == "000001"
    assert unit.rows[0]["qmt_code"] == "000001.SZ"
    assert unit.rows[0]["list_date"] == "1991-04-03"
    assert unit.detail["instrument_raw"]["ExpireDate"] == 0
    if asset == "etf":
        assert "asset_class" not in unit.rows[0]
        assert unit.detail["requires_asset_class"] is True


@pytest.mark.parametrize("missing", [None, 0, "", "00000000"])
def test_missing_dates_remain_null(missing):
    raw = instrument()
    raw.update(OpenDate=missing, ExpireDate=missing)
    _, result = normalize_reference(batch(rows=[raw]), "stock")
    assert result.units[0].rows[0]["list_date"] is None
    assert result.units[0].rows[0]["expire_date"] is None


def test_instrument_failure_isolated_from_other_security():
    raw = batch(codes=["000001.SZ", "000002.SZ"])
    raw["outcomes"]["000002.SZ"]["rows"][0]["ExchangeID"] = "SH"
    _, result = normalize_reference(raw, "stock")
    assert [unit.status for unit in result.units] == ["complete", "error"]
    assert result.units[1].error_code == "WRONG_INSTRUMENT_ID"


def test_missing_instrument_outcome_and_invalid_dates_are_not_success():
    raw = batch()
    raw["outcomes"].clear()
    assert normalize_reference(raw, "stock")[1].units[0].error_code == "MISSING_OUTCOME"
    raw = instrument()
    raw["OpenDate"] = "20261340"
    assert normalize_reference(batch(rows=[raw]), "stock")[1].units[0].error_code == "INVALID_REFERENCE_DATE"


def test_statistical_index_is_not_published_as_price_index():
    raw = batch(codes=["395001.SZ"])
    assert normalize_reference(raw, "index")[1].units[0].error_code == "UNSUPPORTED_INSTRUMENT"


def test_sector_uses_explicit_names_and_preserves_qualified_identity():
    raw = batch("sector", ["configured-a", "configured-b"], [])
    raw["outcomes"]["configured-a"]["rows"] = [dict(qmt_code="000001.SZ", sector="configured-a")]
    raw["outcomes"]["configured-b"]["rows"] = [dict(qmt_code="000001.SZ", sector="configured-b"), dict(qmt_code="000001.SH", sector="configured-b")]
    assert extract_sector_codes(raw) == ["000001.SH", "000001.SZ"]
    raw["outcomes"]["configured-b"]["rows"][0]["sector"] = "other"
    with pytest.raises(NormalizationError, match="sector membership"):
        extract_sector_codes(raw)


def test_partial_sector_result_does_not_shrink_catalog():
    raw = batch("sector", ["configured-a", "configured-b"], [])
    raw["outcomes"].pop("configured-a")
    with pytest.raises(NormalizationError):
        extract_sector_codes(raw)


def calendar_batch(native_time="20260904"):
    return batch("calendar", rows=[dict(native_time=native_time, qmt_code="000001.SZ")])


def test_calendar_adds_only_proved_past_open_and_retains_future_authority():
    existing = [{"trade_date": "2026-09-03", "trade_status": 1}, {"trade_date": "2026-09-07", "trade_status": 1, "source": "official"}]
    result = merge_calendar_rows(calendar_batch(), existing)
    assert [row["trade_date"] for row in result] == ["2026-09-03", DAY, "2026-09-07"]
    assert result[-1]["source"] == "official"
    assert not any(row["trade_status"] == 0 for row in result)
    assert len(existing) == 2


def test_calendar_conflict_rejected_without_changing_existing():
    with pytest.raises(NormalizationError, match="contradicts"):
        merge_calendar_rows(calendar_batch(), {date(2026, 9, 4): False})


def test_native_future_dates_and_other_requested_dates_are_not_authority():
    with pytest.raises(NormalizationError):
        merge_calendar_rows(calendar_batch("20260907"), {})
    raw = calendar_batch("20260907")
    raw["request"].update(start_date="2026-09-07", end_date="2026-09-07")
    with pytest.raises(NormalizationError):
        merge_calendar_rows(raw, {})


def test_wrong_source_or_period_cannot_be_used_for_reference():
    raw = batch()
    raw["request"]["source"] = "mini_qmt"
    with pytest.raises(NormalizationError):
        normalize_reference(raw, "stock")
