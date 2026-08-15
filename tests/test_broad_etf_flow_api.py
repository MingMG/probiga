# -*- coding: utf-8 -*-
from __future__ import annotations

import importlib
import sys
from typing import Any

import pytest
import requests


MODULE_NAME = "server.api.routers.broad_etf_flow"


def _router_module():
    return importlib.import_module(MODULE_NAME)


def _share_row(value: float) -> dict[str, Any]:
    return {"fund_share": value, "source": "test exchange fixture"}


def _price_row(*, pre_close: float, close: float = 4.30) -> dict[str, Any]:
    return {
        "pre_close": pre_close,
        "close": close,
        "change_pct": -0.8,
        "amount": 123_000_000.0,
        "quality_status": "test",
    }


def test_router_import_has_no_network_or_database_side_effect(monkeypatch):
    """Defining the route must not fetch data or initialise a DB connection."""
    engine_module = importlib.import_module("server.api.routers._engine")
    sql_reader_module = importlib.import_module("server.common.sql_reader")
    routers_package = importlib.import_module("server.api.routers")
    calls: list[str] = []

    def forbidden_network(*_args, **_kwargs):
        calls.append("network")
        raise AssertionError("router import attempted a network request")

    def forbidden_database(*_args, **_kwargs):
        calls.append("database")
        raise AssertionError("router import attempted database access")

    monkeypatch.setattr(requests, "get", forbidden_network)
    monkeypatch.setattr(requests.sessions.Session, "request", forbidden_network)
    monkeypatch.setattr(engine_module, "get_engine", forbidden_database)
    monkeypatch.setattr(sql_reader_module, "read_sql_rows", forbidden_database)

    original_module = sys.modules.pop(MODULE_NAME, None)
    missing = object()
    original_attribute = getattr(routers_package, "broad_etf_flow", missing)
    try:
        imported = importlib.import_module(MODULE_NAME)
        assert imported.router is not None
        assert calls == []
    finally:
        sys.modules.pop(MODULE_NAME, None)
        if original_module is not None:
            sys.modules[MODULE_NAME] = original_module
        if original_attribute is missing:
            try:
                delattr(routers_package, "broad_etf_flow")
            except AttributeError:
                pass
        else:
            setattr(routers_package, "broad_etf_flow", original_attribute)


def test_build_flow_rows_uses_share_delta_times_previous_close():
    module = _router_module()
    code = module.CORE_ETFS[0].code
    shares = {
        "2026-08-11": {code: _share_row(100_000_000.0)},
        "2026-08-12": {code: _share_row(112_000_000.0)},
    }
    prices = {
        # Deliberately differs from the next day's pre_close so the assertion
        # proves which field is used by the documented calculation.
        "2026-08-11": {code: {"close": 4.00}},
        "2026-08-12": {code: _price_row(pre_close=4.25)},
    }

    rows = module._build_flow_rows(
        ["2026-08-11", "2026-08-12"],
        shares,
        prices,
    )

    assert len(rows) == 1
    assert rows[0]["share_change"] == pytest.approx(12_000_000.0)
    assert rows[0]["net_amount"] == pytest.approx(12_000_000.0 * 4.25)
    assert rows[0]["flow_method"] == "份额变化 × 前一交易日收盘价"


def test_large_share_jump_is_excluded_as_suspected_corporate_action():
    module = _router_module()
    code = module.CORE_ETFS[0].code
    rows = module._build_flow_rows(
        ["2026-08-11", "2026-08-12"],
        {
            "2026-08-11": {code: _share_row(100_000_000.0)},
            "2026-08-12": {code: _share_row(150_000_000.0)},
        },
        {"2026-08-12": {code: _price_row(pre_close=4.25)}},
    )

    assert rows[0]["share_change_pct"] == pytest.approx(50.0)
    assert rows[0]["net_amount"] is None
    assert rows[0]["quality_status"] == "corporate_action_suspected"


def test_api_returns_insufficient_when_core_pool_coverage_is_below_gate(monkeypatch):
    module = _router_module()
    previous_date = "2026-08-11"
    current_date = "2026-08-12"
    # Two of four funds is 50%, below the router's 65% minimum gate.
    covered = module.CORE_ETFS[:2]
    shares = {
        previous_date: {
            item.code: _share_row(100_000_000.0) for item in covered
        },
        current_date: {
            item.code: _share_row(101_000_000.0) for item in covered
        },
    }
    prices = {
        current_date: {
            item.code: _price_row(pre_close=4.00) for item in covered
        }
    }

    monkeypatch.setattr(
        module,
        "_trading_dates",
        lambda _requested_date, _limit: [current_date, previous_date],
    )
    monkeypatch.setattr(
        module,
        "_collect_inputs",
        lambda _trade_dates: (shares, prices, []),
    )

    payload = module.broad_etf_flow(
        trade_date=current_date,
        days=5,
        refresh=True,
    )

    assert payload["status"] == "insufficient"
    assert payload["summary"]["signal"] == "数据不足，暂不判断"
    assert payload["summary"]["signal_tone"] == "unknown"
    assert payload["summary"]["confidence_label"] == "不可用"
    assert payload["summary"]["coverage_pct"] == pytest.approx(50.0)
    assert payload["coverage"] == {
        "expected": len(module.CORE_ETFS),
        "available": len(covered),
        "missing": payload["coverage"]["missing"],
        "excluded": [],
    }


def test_low_coverage_history_is_not_silently_counted_as_zero():
    module = _router_module()
    selected_rows = [
        {"benchmark": item.benchmark, "net_amount": 2_000_000_000.0, "change_pct": -0.4}
        for item in module.CORE_ETFS
    ]
    history = [
        {"trade_date": "2026-08-08", "net_amount": 6_000_000_000.0, "coverage_pct": 100.0},
        {"trade_date": "2026-08-11", "net_amount": 1_000_000_000.0, "coverage_pct": 50.0},
        {"trade_date": "2026-08-12", "net_amount": 8_000_000_000.0, "coverage_pct": 100.0},
    ]

    summary, evidence = module._signal(history, selected_rows, expected=4)

    assert summary["net_1d"] == pytest.approx(8_000_000_000.0)
    assert summary["net_3d"] is None
    assert summary["net_5d"] is None
    assert summary["net_20d"] is None
    warning = next(item for item in evidence if item["title"] == "近5日累计暂不计算")
    assert "缺失 ETF 不会按0元" in warning["detail"]


def test_window_sum_requires_the_full_number_of_trading_days():
    module = _router_module()
    history = [
        {"trade_date": "2026-08-11", "net_amount": 1.0, "coverage_pct": 100.0},
        {"trade_date": "2026-08-12", "net_amount": 2.0, "coverage_pct": 100.0},
    ]

    assert module._window_sum(history, 2) == pytest.approx(3.0)
    assert module._window_sum(history, 3) is None
    assert module._window_sum(history, 5) is None


def test_corporate_action_row_remains_visible_but_is_excluded_from_totals(monkeypatch):
    module = _router_module()
    previous_date = "2026-08-11"
    current_date = "2026-08-12"
    shares = {previous_date: {}, current_date: {}}
    prices = {current_date: {}}
    for index, item in enumerate(module.CORE_ETFS):
        shares[previous_date][item.code] = _share_row(100_000_000.0)
        shares[current_date][item.code] = _share_row(
            150_000_000.0 if index == 0 else 101_000_000.0
        )
        prices[current_date][item.code] = _price_row(pre_close=4.00)

    monkeypatch.setattr(
        module,
        "_trading_dates",
        lambda _requested_date, _limit: [current_date, previous_date],
    )
    monkeypatch.setattr(
        module,
        "_collect_inputs",
        lambda _trade_dates: (shares, prices, []),
    )

    payload = module.broad_etf_flow(trade_date=current_date, days=5, refresh=True)

    assert payload["status"] == "degraded"
    assert len(payload["etfs"]) == len(module.CORE_ETFS)
    suspected = next(
        row for row in payload["etfs"] if row["quality_status"] == "corporate_action_suspected"
    )
    assert suspected["net_amount"] is None
    assert suspected["share_change"] == pytest.approx(50_000_000.0)
    assert payload["coverage"]["available"] == len(module.CORE_ETFS) - 1
    assert payload["coverage"]["missing"] == []
    assert payload["coverage"]["excluded"][0]["etf_code"] == suspected["etf_code"]


@pytest.mark.parametrize(
    ("daily_flow", "expected_tone", "expected_signal"),
    [
        (24_000_000_000.0, "inflow", "较强护盘型资金线索"),
        (-24_000_000_000.0, "outflow", "宽基强净赎回压力"),
    ],
)
def test_positive_and_negative_signal_copy_does_not_claim_national_team_identity(
    daily_flow: float,
    expected_tone: str,
    expected_signal: str,
):
    module = _router_module()
    benchmarks = ("上证50", "沪深300", "中证500", "中证1000")
    selected_rows = [
        {
            "benchmark": benchmark,
            "net_amount": daily_flow / len(benchmarks),
            "change_pct": -1.0,
        }
        for benchmark in benchmarks
    ]
    history = [
        {
            "trade_date": f"2026-08-{day:02d}",
            "net_amount": 1_000_000_000.0 if day % 2 else -1_000_000_000.0,
            "coverage_pct": 100.0,
        }
        for day in range(1, 9)
    ]
    history.append({
        "trade_date": "2026-08-09",
        "net_amount": daily_flow,
        "coverage_pct": 100.0,
    })

    summary, evidence = module._signal(
        history,
        selected_rows,
        expected=len(selected_rows),
    )

    assert summary["signal_tone"] == expected_tone
    assert summary["signal"] == expected_signal
    assert "国家队" not in summary["signal"]
    assert "出货" not in summary["signal"]
    assert summary["identity_status"] == "未确认"

    identity_warning = next(
        item for item in evidence if item["title"] == "国家队身份未确认"
    )
    assert identity_warning["kind"] == "warning"
    assert "不披露申购、赎回主体" in identity_warning["detail"]
    assert "不能据此断言国家队正在出货" in identity_warning["detail"]
