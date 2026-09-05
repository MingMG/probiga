"""Offline provider contracts: no real sockets, credentials or databases."""

from copy import deepcopy
from datetime import datetime, timedelta
from email.utils import format_datetime
from zoneinfo import ZoneInfo

import pytest
import requests

from acquisition.providers import CninfoProvider, EastmoneyProvider
from acquisition.providers import eastmoney as em

TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 9, 5, 18, 0, tzinfo=TZ)


class Clock:
    def __init__(self):
        self.now = NOW
        self.sleeps = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += timedelta(seconds=seconds)


class Response:
    def __init__(self, payload=None, status=200, headers=None):
        self.payload = payload
        self.status_code = status
        self.headers = headers or {}
        self.closed = False

    def json(self, **kwargs):
        if isinstance(self.payload, Exception):
            raise self.payload
        return deepcopy(self.payload)

    def close(self):
        self.closed = True


class Client:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        assert self.responses, "Unexpected extra network attempt"
        result = self.responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result() if callable(result) else result


def request(dataset="finance", codes=None, day="2026-09-04", budget=300):
    return {"request_id": "batch-001", "dataset": dataset, "source": "eastmoney",
            "codes": codes or ["000001.SZ"], "start_date": day, "end_date": day,
            "period": "1d", "adjustment": "none", "requested_at": NOW.isoformat(),
            "deadline_at": (NOW + timedelta(seconds=budget)).isoformat()}


def fetch(req, *responses, clock=None):
    timer = clock or Clock()
    client = Client(*responses)
    result = EastmoneyProvider(client, timer, timer.sleep).fetch_batch(req["dataset"], req)
    return result, client, timer


def page(rows, *, pages=1, count=None):
    return Response({"success": True, "code": 0,
                     "result": {"pages": pages, "count": len(rows) if count is None else count, "data": rows}})


def finance_row(**overrides):
    return {"SECURITY_CODE": "000001", "SECUCODE": "000001.SZ",
            "REPORT_DATE": "2026-06-30 00:00:00", "REPORT_TYPE": "中报",
            "NOTICE_DATE": "2026-08-20 00:00:00", "UPDATE_DATE": "2026-09-03 12:34:56",
            "TOTALOPERATEREVE": "100001.2300", "EPSJB": "1.03", **overrides}


def notice(art="AN001", **overrides):
    return {"art_code": art, "title": "半年报", "notice_date": "2026-09-04 00:00:00",
            "codes": [{"stock_code": "000001"}, {"stock_code": "000002"}],
            "columns": [{"column_name": "定期报告"}], "display_time": "2026-09-04 19:02:03", **overrides}


def notice_page(rows, *, total=None, number=1):
    return Response({"success": 1, "data": {"page_index": number, "page_size": em.PAGE_SIZE,
        "total_hits": len(rows) if total is None else total, "list": rows}})


def alist_row(**overrides):
    return {"SECURITY_CODE": "000001", "TRADE_DATE": "2026-09-04 00:00:00", "TRADE_ID": "106",
            "EXPLANATION": "连续三个交易日偏离值累计达到20%", "OPERATEDEPT_CODE": "D001",
            "OPERATEDEPT_NAME": "证券营业部", "BUY": "20.12", "SELL": "10.11", "NET": "10.01", **overrides}


def test_finance_preserves_identity_dates_and_raw_precision_once():
    req = request()
    original = deepcopy(req)
    raw = finance_row()
    result, client, _ = fetch(req, page([raw]))
    row = result["outcomes"]["000001.SZ"]["rows"][0]
    assert row["report_date"] == "2026-06-30"
    assert row["notice_date"] == raw["NOTICE_DATE"]
    assert row["source_update_date"] == raw["UPDATE_DATE"]
    assert row["total_rev"] == "100001.2300"
    assert row["raw"] == raw and "source_time" not in row
    assert req == original and result["request"] == original
    assert len(client.calls) == 1
    assert client.calls[0][1]["params"]["type"] == "RPT_F10_FINANCE_MAINFINADATA"
    assert client.calls[0][1]["timeout"] == (5.0, 30.0)
    assert client.calls[0][1]["allow_redirects"] is False


def test_native_json_decimal_token_is_not_rounded_through_binary_float():
    response = requests.Response()
    response.status_code = 200
    response._content = (
        b'{"success":true,"code":0,"result":{"pages":1,"count":1,"data":['
        b'{"SECURITY_CODE":"000001","SECUCODE":"000001.SZ",'
        b'"REPORT_DATE":"2026-06-30","REPORT_TYPE":"half-year",'
        b'"NOTICE_DATE":"2026-08-20","TOTALOPERATEREVE":123456789012345.678901}]}}'
    )
    result, _, _ = fetch(request(), response)
    row = result["outcomes"]["000001.SZ"]["rows"][0]
    assert row["total_rev"] == "123456789012345.678901"
    assert row["raw"]["TOTALOPERATEREVE"] == "123456789012345.678901"


@pytest.mark.parametrize("change,code", [
    ({"SECURITY_CODE": "000002"}, "SECURITY_MISMATCH"),
    ({"SECUCODE": "000001.SH"}, "SECURITY_MISMATCH"),
    ({"REPORT_DATE": "2026-02-31"}, "INVALID_DATE"),
    ({"UPDATE_DATE": "yesterday"}, "INVALID_DATE"),
    ({"REPORT_TYPE": None}, "INVALID_RESPONSE"),
])
def test_finance_rejects_wrong_issuer_or_date(change, code):
    result, _, _ = fetch(request(), page([finance_row(**change)]))
    assert result["outcomes"]["000001.SZ"]["error_code"] == code


def test_finance_empty_is_not_a_nonfiling_exemption():
    result, _, _ = fetch(request(), page([]))
    assert result["outcomes"]["000001.SZ"]["status"] == "error"


def test_finance_eps_diluted_and_nonrecurring_metrics_are_not_swapped():
    result, _, _ = fetch(request(), page([finance_row(EPSXS="1.01", EPSKCJB="0.92")]))
    row = result["outcomes"]["000001.SZ"]["rows"][0]
    assert row["diluted_eps"] == "1.01" and row["non_gaap_eps"] == "0.92"


def test_finance_reads_every_page_and_checks_total(monkeypatch):
    monkeypatch.setattr(em, "PAGE_SIZE", 1)
    result, client, _ = fetch(request(), page([finance_row()], pages=2, count=2),
        page([finance_row(REPORT_DATE="2026-03-31", REPORT_TYPE="一季报")], pages=2, count=2))
    assert len(result["outcomes"]["000001.SZ"]["rows"]) == 2
    assert [call[1]["params"]["p"] for call in client.calls] == [1, 2]


@pytest.mark.parametrize("second,error", [
    (lambda: page([], pages=2, count=2), "PAGINATION_INCOMPLETE"),
    (lambda: page([finance_row()], pages=2, count=2), "PAGINATION_DUPLICATE"),
    (lambda: page([finance_row(REPORT_DATE="2026-03-31")], pages=2, count=3), "PAGINATION_CHANGED"),
])
def test_partial_or_changed_report_never_returns_data(second, error):
    result, _, _ = fetch(request(), page([finance_row()], pages=2, count=2), second())
    assert result["outcomes"]["000001.SZ"] == {"status": "error", "rows": [],
        "error_code": error, "reason": result["outcomes"]["000001.SZ"]["reason"]}


def test_page_limit_is_error_not_success(monkeypatch):
    monkeypatch.setattr(em, "MAX_PAGES", 1)
    result, client, _ = fetch(request(), page([finance_row()], pages=2, count=2))
    assert result["outcomes"]["000001.SZ"]["error_code"] == "PAGINATION_LIMIT"
    assert len(client.calls) == 1


def test_history_flow_keeps_native_date_and_signed_amounts():
    result, client, _ = fetch(request("capital_flow_daily"), Response({"rc": 0, "data": {
        "code": "000001", "klines": ["2026-09-03,99,8,7,6,5", "2026-09-04,-1.05,2,3,4,-5.05"]}}))
    row = result["outcomes"]["000001.SZ"]["rows"][0]
    assert row["trade_date"] == "2026-09-04"
    assert row["main_net_inflow"] == "-1.05" and row["max_net_inflow"] == "-5.05"
    assert client.calls[0][1]["params"]["secid"] == "0.000001"


def test_current_endpoint_never_fills_historical_missing_day():
    result, client, _ = fetch(request("capital_flow_daily"), Response({"rc": 0, "data": {"code": "000001", "klines": []}}))
    assert result["outcomes"]["000001.SZ"]["error_code"] == "TARGET_DATE_MISSING"
    assert len(client.calls) == 1


@pytest.mark.parametrize("source_time,error", [
    (datetime(2026, 9, 5, 15, tzinfo=TZ).timestamp(), None),
    (datetime(2026, 9, 4, 15, tzinfo=TZ).timestamp(), "SOURCE_DATE_MISMATCH"),
    (datetime(2026, 9, 5, 14, 59, tzinfo=TZ).timestamp(), "SOURCE_DATE_MISMATCH"),
    (datetime(2026, 9, 5, 19, tzinfo=TZ).timestamp(), "SOURCE_DATE_MISMATCH"),
    (None, "SOURCE_TIME_MISSING"),
    (float("nan"), "SOURCE_TIME_MISSING"),
])
def test_current_fallback_requires_native_closed_target_timestamp(source_time, error):
    result, _, _ = fetch(request("capital_flow_daily", day="2026-09-05"),
        Response({"rc": 0, "data": {"code": "000001", "klines": []}}),
        Response({"rc": 0, "data": {"total": 1, "diff": [{"f12": "000001", "f13": 0,
            "f124": source_time, "f62": "1", "f66": "2", "f72": "3", "f78": "4", "f84": "5"}]}}))
    outcome = result["outcomes"]["000001.SZ"]
    assert outcome.get("error_code") == error
    if error is None:
        assert outcome["rows"][0]["trade_date"] == "2026-09-05"
        assert outcome["rows"][0]["source_method"] == "eastmoney.clist.fflow.current"


def test_unsupported_flow_market_does_not_contact_source():
    result, client, _ = fetch(request("capital_flow_daily", ["920001.BJ"]))
    assert result["outcomes"]["920001.BJ"]["error_code"] == "UNSUPPORTED_MARKET"
    assert client.calls == []


def test_billboard_detail_keeps_both_sides_reasons_and_trade_identity():
    result, client, _ = fetch(request("alist_detail"), page([alist_row()]), page([alist_row()]))
    rows = result["outcomes"]["000001.SZ"]["rows"]
    assert [row["report_side"] for row in rows] == ["BUY", "SELL"]
    assert all(row["reason"] == alist_row()["EXPLANATION"] and row["trade_id"] == "106" for row in rows)
    assert all("rank" not in row for row in rows)
    assert client.calls[1][1]["params"]["reportName"].endswith("SELL")


def test_billboard_second_side_failure_discards_partial_first_side():
    result, _, _ = fetch(request("alist_detail"), page([alist_row()]), Response(ValueError()))
    outcome = result["outcomes"]["000001.SZ"]
    assert outcome["status"] == "error" and outcome["rows"] == []


def test_event_source_explicit_empty_response_is_legal():
    result, _, _ = fetch(request("alist_daily"), Response({"success": False, "code": 9201,
        "message": "返回数据为空", "result": None}))
    assert result["outcomes"]["000001.SZ"] == {"status": "no_data", "rows": [], "reason": "empty_event_set"}


def test_event_network_empty_is_not_legal_empty():
    result, client, _ = fetch(request("alist_daily"), Response(ValueError("empty body")))
    assert result["outcomes"]["000001.SZ"]["error_code"] == "INVALID_JSON"
    assert len(client.calls) == 1


def test_notices_require_all_pages_and_preserve_issuer_associations(monkeypatch):
    monkeypatch.setattr(em, "PAGE_SIZE", 1)
    result, client, _ = fetch(request("notices"), notice_page([notice("AN001")], total=2),
        notice_page([notice("AN002")], total=2, number=2))
    rows = result["outcomes"]["000001.SZ"]["rows"]
    assert len(rows) == 2 and rows[0]["source_security_codes"] == ["000001", "000002"]
    assert rows[0]["source_time"] == "2026-09-04T19:02:03+08:00"
    assert client.calls[1][1]["params"]["page_index"] == 2


@pytest.mark.parametrize("overrides,code", [
    ({"codes": [{"stock_code": "000002"}]}, "SECURITY_MISMATCH"),
    ({"notice_date": "2026-09-03"}, "SOURCE_DATE_MISMATCH"),
    ({"display_time": "2026-09-06 00:00:00"}, "INVALID_SOURCE_TIME"),
    ({"art_code": ""}, "DUPLICATE_IDENTITY"),
])
def test_notice_identity_and_publication_date_failures(overrides, code):
    result, _, _ = fetch(request("notices"), notice_page([notice(**overrides)]))
    assert result["outcomes"]["000001.SZ"]["error_code"] == code


def test_no_native_notice_time_is_not_replaced_with_receive_time():
    result, _, _ = fetch(request("notices"), notice_page([notice(display_time=None)]))
    assert "source_time" not in result["outcomes"]["000001.SZ"]["rows"][0]


def test_notice_date_only_is_not_fabricated_into_midnight_timestamp():
    result, _, _ = fetch(request("notices"), notice_page([notice(display_time="2026-09-04")]))
    row = result["outcomes"]["000001.SZ"]["rows"][0]
    assert row["display_time"] == "2026-09-04" and "source_time" not in row


def test_malformed_notice_association_is_an_error_outcome():
    result, _, _ = fetch(request("notices"), notice_page([notice(codes=[{"stock_code": ["000001"]}])]))
    assert result["outcomes"]["000001.SZ"]["error_code"] == "INVALID_RESPONSE"


def test_notice_empty_and_incomplete_are_distinguished():
    complete, _, _ = fetch(request("notices"), notice_page([]))
    incomplete, _, _ = fetch(request("notices"), notice_page([], total=1))
    assert complete["outcomes"]["000001.SZ"]["status"] == "no_data"
    assert incomplete["outcomes"]["000001.SZ"]["error_code"] == "PAGINATION_INCOMPLETE"


def test_transport_retries_only_three_times_then_stops_source_batch():
    result, client, clock = fetch(request(codes=["000001.SZ", "000002.SZ"]),
        requests.Timeout(), requests.ConnectionError(), requests.Timeout())
    assert len(client.calls) == 3 and clock.sleeps == [2, 10]
    assert all(item["error_code"] == "SOURCE_UNAVAILABLE" for item in result["outcomes"].values())


@pytest.mark.parametrize("retry_after", ["17", format_datetime(NOW + timedelta(seconds=17))])
def test_retry_after_is_respected(retry_after):
    result, client, clock = fetch(request(), Response(status=429, headers={"Retry-After": retry_after}), page([finance_row()]))
    assert result["outcomes"]["000001.SZ"]["status"] == "data"
    assert clock.sleeps == [17] and len(client.calls) == 2


def test_retry_after_beyond_whole_deadline_does_not_sleep_or_retry():
    result, client, clock = fetch(request(budget=10), Response(status=429, headers={"Retry-After": "60"}))
    assert result["outcomes"]["000001.SZ"]["error_code"] == "DEADLINE_EXCEEDED"
    assert len(client.calls) == 1 and clock.sleeps == []


def test_late_response_is_rejected_and_no_next_page_is_requested():
    clock = Clock()
    def late_page():
        clock.now += timedelta(seconds=11)
        return page([finance_row()], pages=2, count=2)
    result, client, _ = fetch(request(budget=10), late_page, clock=clock)
    assert result["outcomes"]["000001.SZ"]["error_code"] == "DEADLINE_EXCEEDED"
    assert len(client.calls) == 1


def test_access_denied_is_not_retried_for_every_security():
    result, client, _ = fetch(request(codes=["000001.SZ", "000002.SZ"]), Response(status=403))
    assert len(client.calls) == 1
    assert {v["error_code"] for v in result["outcomes"].values()} == {"SOURCE_ACCESS_DENIED"}


@pytest.mark.parametrize("change", [
    {"start_date": "2026-9-4"}, {"end_date": "2026-09-06"},
    {"period": "1m"}, {"adjustment": "front"}, {"source": "other"},
    {"requested_at": "2026-09-05T18:00:00"},
])
def test_invalid_request_does_not_open_network(change):
    req = request()
    req.update(change)
    result, client, _ = fetch(req)
    assert result["outcomes"]["000001.SZ"]["status"] == "error" and not client.calls


def test_cninfo_does_not_invent_dates_or_contact_an_unverified_endpoint():
    client = Client()
    result = CninfoProvider(client, Clock()).fetch_batch("finance", request())
    assert not client.calls
    assert result["outcomes"]["000001.SZ"]["error_code"] == "UNSUPPORTED_DISCLOSURE_PROTOCOL"
