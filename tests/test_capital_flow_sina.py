from copy import deepcopy
from urllib.parse import urlencode
from unittest.mock import MagicMock

import pytest
import requests

from biz.stock_market import capital_flow_sina as sina
from tools import crawl_realtime_batch as flow


def source_row(day="2026-09-10"):
    return {"opendate": day, "r0": "300", "r1": "100", "r2": "200", "r3": "300",
            "r0_net": "100.25", "r1_net": "-10.25", "r2_net": "-20", "r3_net": "-30",
            "netamount": "40"}


def response_url(code="920066", page=1):
    return sina.ENDPOINT + "?" + urlencode({"daima": sina.sina_symbol(code), "page": page,
        "num": sina.PAGE_SIZE, "sort": "opendate", "asc": 0})


def parse(payload, url=None):
    return sina.parse_history(payload, stock_code="920066", response_url=url or response_url(), page=1)


def test_native_cny_size_buckets_and_common_main_are_not_total_net():
    row = parse([source_row()])[0]
    assert row == {"stock_code": "920066", "trade_date": "2026-09-10", "data_source": "sina_l1",
                   "main_net_inflow": 90, "max_net_inflow": 100.25, "lg_net_inflow": -10.25,
                   "mid_net_inflow": -20, "sm_net_inflow": -30}


@pytest.mark.parametrize("code,expected", [("920066", "bj920066"), ("830799", "bj830799"),
    ("430047", "bj430047"), ("600000", "sh600000"), ("000001", "sz000001"), ("300059", "sz300059")])
def test_exact_market_symbol(code, expected):
    assert sina.sina_symbol(code) == expected


@pytest.mark.parametrize("url", [response_url("600000"), response_url(page=2),
    response_url().replace("https:", "http:"), response_url().replace("MoneyFlow.ssl_qsfx_lscjfb", "other"),
    response_url().replace("sina.com.cn", "example.com"), response_url()+"&daima=bj920000"])
def test_history_requires_actual_request_identity(url):
    with pytest.raises(ValueError, match="identity differs"):
        parse([source_row()], url)


@pytest.mark.parametrize("field,value", [("r2_net", None), ("r0_net", "--"), ("r3_net", "NaN"),
    ("r1_net", True), ("r0", "-1"), ("r0_net", "999999"), ("netamount", "0"),
    ("symbol", "sh600000"), ("opendate", "2026/09/10")])
def test_incomplete_wrong_or_inconsistent_native_rows_never_become_zero(field, value):
    raw = source_row()
    raw[field] = value
    with pytest.raises(ValueError):
        parse([raw])


def test_duplicate_dates_and_unsorted_pages_fail():
    with pytest.raises(ValueError, match="duplicated"):
        parse([source_row(), source_row()])
    with pytest.raises(ValueError, match="ordering"):
        parse([source_row("2026-09-09"), source_row("2026-09-10")])


def test_source_dates_are_filtered_exactly_and_page_loop_is_bounded(monkeypatch):
    rows = parse([source_row()])
    monkeypatch.setattr(sina, "_history_page", lambda *_a: tuple(deepcopy(rows)))
    assert sina.fetch_sina_flow_row("920066", "2026-09-10")["trade_date"] == "2026-09-10"
    assert sina.fetch_sina_flow_row("920066", "2026-09-09") is None


def test_broken_primary_switches_provider_once_per_batch(monkeypatch):
    calls = []
    def primary(code, day):
        calls.append(code)
        raise flow.CapitalFlowSourceUnavailable("transport failed")
    def fallback(code, day):
        return {**parse([source_row()])[0], "stock_code": code, "trade_date": day}
    monkeypatch.setattr(flow, "_fetch_exact_eastmoney_flow_row", primary)
    monkeypatch.setattr(flow, "fetch_sina_flow_row", fallback)
    monkeypatch.setenv("FLOW_FALLBACK_WORKERS", "1")
    frame = flow._fetch_missing_flow_rows({"920066", "920000", "600000"}, trade_date="2026-09-10")
    assert len(calls) == 1
    assert len(frame) == 3 and set(frame.data_source) == {"sina_l1"}


def test_primary_identity_failure_does_not_authorize_another_provider(monkeypatch):
    def invalid(*_args):
        raise ValueError("response identity differs")
    monkeypatch.setattr(flow, "_fetch_exact_eastmoney_flow_row", invalid)
    monkeypatch.setattr(flow, "fetch_sina_flow_row", lambda *_a: pytest.fail("integrity error must propagate"))
    with pytest.raises(RuntimeError, match="fallback failed"):
        flow._fetch_missing_flow_rows({"920066"}, trade_date="2026-09-10")


def test_valid_primary_without_target_date_uses_dated_alternate(monkeypatch):
    monkeypatch.setattr(flow, "_fetch_exact_eastmoney_flow_row", lambda *_a: None)
    monkeypatch.setattr(flow, "fetch_sina_flow_row", lambda *_a: parse([source_row()])[0])
    frame = flow._fetch_missing_flow_rows({"920066"}, trade_date="2026-09-10")
    assert frame.iloc[0].data_source == "sina_l1"


def test_transient_gateway_failure_retries_same_dated_request(monkeypatch):
    session = MagicMock()
    session.__enter__.return_value = session
    response = MagicMock(status_code=200, url=response_url())
    response.json.return_value = [source_row()]
    failure = requests.HTTPError(response=MagicMock(status_code=502))
    session.get.side_effect = [failure, response]
    monkeypatch.setattr(sina.requests, "Session", lambda: session)
    monkeypatch.setattr(sina, "_request_slot", lambda: None)
    monkeypatch.setattr(sina.time, "sleep", lambda *_a: None)
    rows = sina._history_page.__wrapped__("920066", 1, "2026-09-13")
    assert len(rows) == 1 and session.get.call_count == 2
    assert session.get.call_args.kwargs["allow_redirects"] is False


def test_access_challenge_is_not_retried_or_parsed_as_data(monkeypatch):
    session = MagicMock()
    session.__enter__.return_value = session
    session.get.side_effect = requests.HTTPError(response=MagicMock(status_code=403))
    monkeypatch.setattr(sina.requests, "Session", lambda: session)
    monkeypatch.setattr(sina, "_request_slot", lambda: None)
    with pytest.raises(requests.HTTPError):
        sina._history_page.__wrapped__("920066", 1, "2026-09-13")
    assert session.get.call_count == 1
