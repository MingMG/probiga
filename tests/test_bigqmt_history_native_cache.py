"""Closed-session collection reuses only complete, valid native QMT bars."""
from copy import deepcopy
from datetime import datetime, time, timedelta
import importlib.util
from pathlib import Path

import pytest

from server.common.qmt_history_coverage import minute_time_grid
from tools.sync_qmt_stock_edge import STOCK_HISTORY_READY_TIMES


DAY = "2026-09-18"
CODES = ["000001.SZ", "600000.SH"]


@pytest.fixture
def producer(monkeypatch):
    source = Path(__file__).resolve().parents[1] / "integrations/bigqmt/qmt_strategy/probiga_big_qmt_bridge.py"
    spec = importlib.util.spec_from_file_location("native_cache_producer", source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    freeze_clock(monkeypatch, module, datetime(2026, 9, 21, 16))
    return module


def freeze_clock(monkeypatch, module, shanghai_now):
    class Clock(datetime):
        @classmethod
        def utcnow(cls):
            return shanghai_now - timedelta(hours=8)
    monkeypatch.setattr(module.datetime, "datetime", Clock)


def bar(clock="15:00:00", day=DAY):
    return dict(time=day + " " + clock, open=10, high=11, low=9, close=10.5,
                volume=100, amount=1050, preClose=9.9)


def bars(period="1m", day=DAY):
    return [bar(clock, day) for clock in minute_time_grid()] if period == "1m" else [bar(day=day)]


def params(**overrides):
    return dict(stock_codes=CODES, start_date=DAY, end_date=DAY,
                download_history=True, **overrides)


class Context:
    def __init__(self, *responses, daily=None):
        self.responses = list(responses)
        self.calls = []
        self.daily_calls = []
        self.daily = daily

    def get_market_data_ex_ori(self, fields, symbols, **kwargs):
        if kwargs["period"] == "1d" and self.calls:
            self.daily_calls.append((list(symbols), kwargs))
            return {code: bars("1d") for code in symbols} if self.daily is None else self.daily
        self.calls.append((list(symbols), kwargs))
        result = self.responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def test_complete_native_cache_does_not_download(producer, monkeypatch):
    native = {code: bars() for code in CODES}
    context = Context(native)
    monkeypatch.setattr(producer, "_download_history", lambda *args: pytest.fail("complete cache downloaded"))

    result = producer._market_rows(context, params(), "1m")

    assert len(result) == 482
    assert len(context.calls) == 1
    assert context.calls[0][0] == CODES
    assert context.calls[0][1]["fill_data"] is False
    assert context.calls[0][1]["subscribe"] is False
    assert context.calls[0][1]["count"] == -1


def test_only_incomplete_code_downloads_then_entire_native_response_is_reread(producer, monkeypatch):
    before = {CODES[0]: bars(), CODES[1]: []}
    after = {code: bars() for code in CODES}
    after[CODES[0]][0]["close"] = 10.7
    context = Context(before, after)
    downloads = []
    monkeypatch.setattr(producer, "_download_history", lambda *args: downloads.append(args))

    result = producer._market_rows(context, params(), "1m")

    assert downloads == [([CODES[1]], "1m", "20260918", "20260918")]
    assert [call[0] for call in context.calls] == [CODES, CODES]
    assert result[0]["close"] == 10.7  # Never splice the initial cached rows.


@pytest.mark.parametrize("corruption", [
    "missing", "duplicate", "cross_day", "invalid_date", "off_grid", "afternoon_1300",
    "extra_invalid_time", "missing_volume", "negative_volume", "nan_amount",
    "infinite_close", "negative_low", "ohlc_range", "boolean_volume", "invalid_average",
])
def test_invalid_minute_cache_triggers_download(producer, monkeypatch, corruption):
    values = bars()
    if corruption == "missing":
        values.pop()
    elif corruption == "duplicate":
        values[-1] = deepcopy(values[0])
    elif corruption == "cross_day":
        values[-1]["time"] = "2026-09-17 15:00:00"
    elif corruption == "invalid_date":
        values[-1]["time"] = "2026-09-32 15:00:00"
    elif corruption == "off_grid":
        values[-1]["time"] = DAY + " 15:00:01"
    elif corruption == "afternoon_1300":
        values[121]["time"] = DAY + " 13:00:00"
    elif corruption == "extra_invalid_time":
        values.append(dict(bar(), time="invalid"))
    elif corruption == "missing_volume":
        values[0].pop("volume")
    elif corruption == "negative_volume":
        values[0]["volume"] = -1
    elif corruption == "nan_amount":
        values[0]["amount"] = float("nan")
    elif corruption == "infinite_close":
        values[0]["close"] = float("inf")
    elif corruption == "negative_low":
        values[0]["low"] = -1
    elif corruption == "ohlc_range":
        values[0]["high"] = 8
    elif corruption == "boolean_volume":
        values[0]["volume"] = False
    elif corruption == "invalid_average":
        values[0]["avgPrice"] = 0
    context = Context({CODES[0]: bars(), CODES[1]: values}, {code: bars() for code in CODES})
    downloads = []
    monkeypatch.setattr(producer, "_download_history", lambda *args: downloads.append(args[0]))

    assert len(producer._market_rows(context, params(), "1m")) == 482
    assert downloads == [[CODES[1]]]


def test_complete_daily_bar_still_downloads_because_cache_cannot_prove_finality(producer, monkeypatch):
    context = Context({code: bars("1d") for code in CODES})
    downloads = []
    monkeypatch.setattr(producer, "_download_history", lambda *args: downloads.append(args[0]))

    assert len(producer._market_rows(context, params(), "1d")) == 2
    assert downloads == [CODES]
    assert len(context.calls) == 1


@pytest.mark.parametrize("shape", ["records", "time_mapping", "columns", "frame"])
def test_supported_native_shapes_keep_the_same_cache_criterion(producer, shape):
    values = bars()
    if shape == "time_mapping":
        values = {row["time"]: {key: value for key, value in row.items() if key != "time"} for row in values}
    elif shape == "columns":
        values = {field: [row[field] for row in values] for field in values[0]}
    elif shape == "frame":
        import pandas as pd
        values = pd.DataFrame(values).set_index("time")
    assert producer._native_cache_complete(values, DAY)


@pytest.mark.parametrize("stage,error", [
    ("cache", RuntimeError("IDENTITY_MISMATCH")),
    ("cache", OSError("transport unavailable")),
    ("download", RuntimeError("QMT_HISTORY_RESOURCE_PRESSURE")),
    ("reread", RuntimeError("QMT_HISTORY_RESOURCE_PRESSURE")),
])
def test_native_errors_propagate_without_another_download(producer, monkeypatch, stage, error):
    context = Context(error if stage == "cache" else {}, error)
    downloads = []
    def download(*args):
        downloads.append(args)
        if stage == "download":
            raise error
    monkeypatch.setattr(producer, "_download_history", download)

    with pytest.raises(type(error), match=str(error)):
        producer._market_rows(context, params(), "1m")
    assert len(downloads) == (0 if stage == "cache" else 1)
    assert len(context.calls) == (2 if stage == "reread" else 1)


def test_cache_read_obeys_real_native_budget_before_access(producer, monkeypatch):
    monkeypatch.setattr(producer, "_native_resource_snapshot", lambda: dict(
        total_physical=32*1024**3, available_physical=8*1024**3,
        available_commit=12*1024**3, private_bytes=producer._NATIVE_HISTORY_PRIVATE_ROTATE,
        working_set_bytes=1024**3, handles=4000))
    context = Context({code: bars() for code in CODES})
    monkeypatch.setattr(producer, "_download_history", lambda *args: pytest.fail("pressure became a cache miss"))
    with pytest.raises(producer._NativeHistoryResourceBlocked, match="QMT_HISTORY_RESOURCE_PRESSURE"):
        producer._market_rows(producer._QuoteCacheContext(context), params(), "1m")
    assert not context.calls


def test_unexpected_symbol_fails_response_integrity_instead_of_download(producer, monkeypatch):
    context = Context({"600001.SH": bars()})
    monkeypatch.setattr(producer, "_download_history", lambda *args: pytest.fail("identity became a cache miss"))
    with pytest.raises(RuntimeError, match="response differs"):
        producer._market_rows(context, params(), "1m")


@pytest.mark.parametrize("payload", [None, [], "invalid", {CODES[0]: "invalid frame"}])
def test_unsupported_response_is_not_silently_retried_as_cache_miss(producer, monkeypatch, payload):
    context = Context(payload)
    monkeypatch.setattr(producer, "_download_history", lambda *args: pytest.fail("invalid response downloaded"))
    with pytest.raises(RuntimeError, match="QMT_HISTORY_CACHE_.*INVALID"):
        producer._market_rows(context, params(), "1m")


@pytest.mark.parametrize("timestamp", [
    "20260918093000", 20260918093000, "20260918093000000", 20260918093000000,
    "20260918093000.000", 20260918093000.0,
])
def test_native_compact_stime_shapes_agree_with_exported_time(producer, timestamp):
    values = bars()
    values[0].pop("time")
    values[0]["stime"] = timestamp
    assert producer._native_cache_complete(values, DAY)
    assert producer._bar_rows({CODES[0]: values}, "1m")[0]["trade_time"] == DAY + " 09:30:00"


@pytest.mark.parametrize("timestamp", ["20260918093000123", "20260918093000.123", "20261318093000"])
def test_compact_stime_must_be_valid_and_minute_aligned(producer, timestamp):
    values = bars()
    values[0]["stime"] = timestamp
    assert not producer._native_cache_complete(values, DAY)


@pytest.mark.parametrize("clock,cached", [
    (time(15, 34, 59), False), (time(15, 35), True), (time(10), False),
])
def test_same_day_cache_starts_only_after_canonical_finalization(producer, monkeypatch, clock, cached):
    freeze_clock(monkeypatch, producer, datetime.combine(datetime.fromisoformat(DAY).date(), clock))
    context = Context({code: bars() for code in CODES})
    downloads = []
    monkeypatch.setattr(producer, "_download_history", lambda *args: downloads.append(args))
    producer._market_rows(context, params(), "1m")
    assert bool(downloads) is not cached


@pytest.mark.parametrize("overrides", [
    {"end_date": "2026-09-19"}, {"count": 30}, {"dividend_type": "front"},
    {"start_date": DAY + " 10:00:00"}, {"end_date": DAY + " 14:59:00"},
    {"start_date": "2026-09-22", "end_date": "2026-09-22"},
])
def test_ranges_partial_windows_and_future_requests_still_refresh(producer, monkeypatch, overrides):
    request = params()
    request.update(overrides)
    context = Context({code: bars() for code in CODES})
    downloads = []
    monkeypatch.setattr(producer, "_download_history", lambda *args: downloads.append(args))
    producer._market_rows(context, request, "1m")
    assert len(downloads) == 1
    assert downloads[0][0] == CODES
    assert len(context.calls) == 1


def test_native_empty_after_download_is_not_filled_or_classified_as_suspended(producer, monkeypatch):
    context = Context({}, {})
    monkeypatch.setattr(producer, "_download_history", lambda *args: None)
    assert producer._market_rows(context, params(), "1m") == []


def test_read_only_request_never_initiates_download(producer, monkeypatch):
    context = Context({})
    request = params()
    request["download_history"] = False
    monkeypatch.setattr(producer, "_download_history", lambda *args: pytest.fail("read-only request downloaded"))
    assert producer._market_rows(context, request, "1m") == []
    assert len(context.calls) == 1


def test_embedded_finalization_constants_match_canonical_schedule(producer):
    assert producer._MINUTE_CACHE_READY_TIME == STOCK_HISTORY_READY_TIMES["minute"]


@pytest.mark.parametrize("daily", [
    {}, {CODES[1]: [dict(bar(), close=11)]},
    {CODES[1]: [dict(bar(), close=float("nan"))]},
    {CODES[1]: [bar(day="2026-09-17")]},
])
def test_daily_anchor_missing_or_mismatched_refreshes_only_affected_symbol(producer, monkeypatch, daily):
    daily = {CODES[0]: bars("1d"), **daily}
    context = Context({code: bars() for code in CODES}, {code: bars() for code in CODES}, daily=daily)
    downloads = []
    monkeypatch.setattr(producer, "_download_history", lambda *args: downloads.append(args[0]))
    assert len(producer._market_rows(context, params(), "1m")) == 482
    assert downloads == [[CODES[1]]]
    assert context.daily_calls[0][1]["dividend_type"] == "none"
    assert context.daily_calls[0][1]["fill_data"] is False


@pytest.mark.parametrize("field,value", [
    ("volume", -123), ("amount", "broken"), ("volume", None),
    ("amount", float("nan")), ("close", float("inf")), ("volume", False),
])
def test_downloaded_invalid_numbers_cannot_be_exported_as_valid_zero(producer, monkeypatch, field, value):
    malformed = [dict(bar(), **{field: value})]
    context = Context({}, {CODES[0]: malformed})
    monkeypatch.setattr(producer, "_download_history", lambda *args: None)
    with pytest.raises(RuntimeError, match="QMT_HISTORY_NATIVE_NUMBER_INVALID"):
        producer._market_rows(context, params(), "1m")


def test_invalid_extra_native_timestamp_cannot_be_silently_dropped(producer):
    with pytest.raises(RuntimeError, match="QMT_HISTORY_NATIVE_TIMESTAMP_INVALID"):
        producer._bar_rows({CODES[0]: bars() + [dict(bar(), time="20260918150000.123")]}, "1m")


@pytest.mark.parametrize("timestamp", ["20260918", 20260918, 20260918.0])
def test_compact_daily_calendar_date_is_not_interpreted_as_epoch(producer, timestamp):
    value = dict(bar(), time=timestamp)
    assert producer._native_daily_close([value], DAY) == 10.5
    row = producer._bar_rows({CODES[0]: [value]}, "1d")[0]
    assert row["trade_time"] == DAY + " 15:00:00"


@pytest.mark.parametrize("value", [True, False, -1, float("inf"), "bad"])
def test_invalid_native_pre_close_cannot_gain_native_origin(producer, value):
    with pytest.raises(RuntimeError, match="NUMBER_INVALID: preClose"):
        producer._bar_rows({CODES[0]: [dict(bar(), preClose=value)]}, "1d")


@pytest.mark.parametrize("value", [None, 0, False, ""])
def test_explicit_invalid_stime_cannot_be_hidden_by_time_fallback(producer, value):
    with pytest.raises(RuntimeError, match="TIMESTAMP_INVALID"):
        producer._bar_rows({CODES[0]: [dict(bar(), stime=value)]}, "1m")


@pytest.mark.parametrize("changes", [
    {"open": 0}, {"high": 1}, {"low": 11},
    {"open": 0, "high": 0, "low": 0, "close": 0},
])
def test_invalid_ohlc_is_rejected_before_backend_discards_price_fields(producer, changes):
    with pytest.raises(RuntimeError, match="OHLC_INVALID"):
        producer._bar_rows({CODES[0]: [dict(bar(), **changes)]}, "1m")


def test_native_all_zero_no_trade_row_is_retained_without_classifying_suspension(producer):
    native = dict(bar(), open=0, high=0, low=0, close=0, volume=0, amount=0)
    result = producer._bar_rows({CODES[0]: [native]}, "1d")
    assert len(result) == 1 and result[0]["close"] == 0
    assert "suspended" not in result[0]


@pytest.mark.parametrize("field,value", [("preClose", True), ("avgPrice", False)])
def test_invalid_cached_optional_number_triggers_refresh(producer, monkeypatch, field, value):
    before = {code: bars() for code in CODES}
    before[CODES[1]][0][field] = value
    context = Context(before, {code: bars() for code in CODES})
    downloads = []
    monkeypatch.setattr(producer, "_download_history", lambda *args: downloads.append(args[0]))
    assert len(producer._market_rows(context, params(), "1m")) == 482
    assert downloads == [[CODES[1]]]


@pytest.mark.parametrize("response", [
    {**{code: bars() for code in CODES}, "BAD": None},
    {CODES[0]: "invalid frame"}, None,
])
def test_fresh_native_response_integrity_is_checked_after_download(producer, monkeypatch, response):
    context = Context({}, response)
    monkeypatch.setattr(producer, "_download_history", lambda *args: None)
    with pytest.raises(RuntimeError, match="response differs|CACHE_.*INVALID"):
        producer._market_rows(context, params(), "1m")
