from datetime import date

import pandas as pd

from biz.sentiment.sync_sentiment import _finalize_a_list_info_df
from tools import backfill_screener_history_inputs as backfill
from tools import fetch_sm_stock_capital_flow_daily as daily_flow
from tools.backfill_screener_history_inputs import (
    flow_components_valid,
    normalize_flow_rows,
    normalize_lhb_daily,
    _json_default,
    _info_exact_key,
)


def test_baidu_fetch_batches_multiple_dates_for_one_stock(monkeypatch):
    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "Result": {
                    "content": [
                        {
                            "date": "2026/08/19",
                            "extMainIn": "30",
                            "littleNetIn": "-15",
                            "mediumNetIn": "-15",
                            "largeNetIn": "20",
                            "superNetIn": "10",
                        },
                        {
                            "date": "2026/08/18",
                            "extMainIn": "40",
                            "littleNetIn": "-20",
                            "mediumNetIn": "-20",
                            "largeNetIn": "25",
                            "superNetIn": "15",
                        },
                    ]
                }
            }

    urls = []

    def fake_get(url, **_kwargs):
        urls.append(url)
        return Response()

    monkeypatch.setattr(daily_flow._SESSION, "get", fake_get)

    frame = daily_flow._fetch_baidu_dates(
        "600000", {"2026-08-18", "2026-08-19"}
    )

    assert frame is not None
    assert sorted(frame["trade_date"].tolist()) == ["2026-08-18", "2026-08-19"]
    assert len(urls) == 1
    assert "rn=20" in urls[0]


def test_baidu_backfill_normalizes_all_requested_dates_in_one_call(monkeypatch):
    frame = pd.DataFrame([
        {
            "stock_code": "600000",
            "trade_date": "2026-08-18",
            "main_net_inflow": 40,
            "max_net_inflow": 15,
            "lg_net_inflow": 25,
            "mid_net_inflow": -20,
            "sm_net_inflow": -20,
        },
        {
            "stock_code": "600000",
            "trade_date": "2026-08-19",
            "main_net_inflow": 30,
            "max_net_inflow": 10,
            "lg_net_inflow": 20,
            "mid_net_inflow": -15,
            "sm_net_inflow": -15,
        },
    ])
    calls = []

    def fake_fetch(code, dates):
        calls.append((code, set(dates)))
        return frame

    monkeypatch.setattr(backfill, "_fetch_baidu_dates", fake_fetch)

    code, rows, error = backfill._fetch_flow_code_baidu(
        "600000", {"2026-08-18", "2026-08-19"}
    )

    assert code == "600000"
    assert len(rows) == 2
    assert error == ""
    assert calls == [("600000", {"2026-08-18", "2026-08-19"})]
    assert {row["_data_source"] for row in rows} == {"baidu"}


def test_flow_component_validation_detects_rotated_buckets():
    correct = {
        "main_net_inflow": -64_265_948,
        "max_net_inflow": -51_160_151,
        "lg_net_inflow": -13_105_797,
        "mid_net_inflow": -4_737_456,
        "sm_net_inflow": 69_003_403,
    }
    rotated = dict(correct)
    rotated.update({
        "max_net_inflow": 69_003_403,
        "lg_net_inflow": -4_737_456,
        "mid_net_inflow": -13_105_797,
        "sm_net_inflow": -51_160_151,
    })

    assert flow_components_valid(correct) is True
    assert flow_components_valid(rotated) is False


def test_normalize_flow_rows_keeps_only_requested_valid_pairs():
    rows = [{
        "stock_code": "1",
        "trade_date": "2026-06-09",
        "main_net_inflow": 30,
        "max_net_inflow": 10,
        "lg_net_inflow": 20,
        "mid_net_inflow": -15,
        "sm_net_inflow": -15,
    }, {
        "stock_code": "1",
        "trade_date": "2026-06-10",
        "main_net_inflow": 30,
        "max_net_inflow": 10,
        "lg_net_inflow": 20,
        "mid_net_inflow": -15,
        "sm_net_inflow": -15,
    }]

    result = normalize_flow_rows(rows, {"000001": {"2026-06-09"}})

    assert len(result) == 1
    assert result[0]["stock_code"] == "000001"
    assert result[0]["trade_date"] == "2026-06-09"


def test_lhb_daily_normalization_filters_scope_and_deduplicates_stock_date():
    frame = pd.DataFrame([
        {"trade_date": "2026-08-07", "stock_code": "000001", "reason": "first"},
        {"trade_date": "2026-08-07", "stock_code": "000001", "reason": "last"},
        {"trade_date": "2026-08-07", "stock_code": "900915", "reason": "B share"},
    ])

    result = normalize_lhb_daily(frame)

    assert result[["stock_code", "reason"]].to_dict("records") == [
        {"stock_code": "000001", "reason": "last"},
    ]


def test_lhb_info_finalizer_removes_buy_sell_report_overlap():
    row = {
        "trade_date": "2026-08-07",
        "stock_code": "000603",
        "operate_code": "10634757",
        "operate_name": "深股通专用",
        "a_buy_amount": 100,
        "a_sell_amount": 80,
        "a_net_amount": 20,
        "a_buy_amount_rate": 1,
        "a_sell_amount_rate": 0.8,
        "reason": "日涨幅偏离值",
    }

    result = _finalize_a_list_info_df(pd.DataFrame([row, row]))

    assert len(result) == 1


def test_lhb_info_finalizer_deduplicates_at_database_precision():
    base = {
        "trade_date": "2026-08-07",
        "stock_code": "000603",
        "operate_code": "10634757",
        "operate_name": "深股通专用",
        "a_buy_amount": 100,
        "a_sell_amount": 80,
        "a_net_amount": 20,
        "a_sell_amount_rate": 0.8,
        "reason": "日涨幅偏离值",
    }
    first = dict(base, a_buy_amount_rate=1.00000001)
    second = dict(base, a_buy_amount_rate=1.00000002)

    result = _finalize_a_list_info_df(pd.DataFrame([first, second]))

    assert len(result) == 1
    assert result.iloc[0]["a_buy_amount_rate"] == 1.0


def test_evidence_serializer_handles_database_dates():
    assert _json_default(date(2026, 7, 16)) == "2026-07-16"


def test_info_identity_does_not_conflate_null_and_zero():
    assert _info_exact_key({"a_buy_amount": None}) != _info_exact_key(
        {"a_buy_amount": 0}
    )
