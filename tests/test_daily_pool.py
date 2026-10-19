from __future__ import annotations

from tools.run_daily_pool import limit_pct_for, select_daily_pool


def _row(code: str, name: str, *, change: float, amount: float,
         close: float = 10.0, high: float = 10.2, low: float = 9.6):
    return {
        "stock_code": code,
        "stock_name": name,
        "close": close,
        "high": high,
        "low": low,
        "change_pct": change,
        "amount": amount,
    }


def test_selects_liquid_strong_close_and_excludes_st():
    rows = [
        _row("600001", "强势股份", change=4.0, amount=800_000_000),
        _row("600002", "ST风险", change=4.0, amount=900_000_000),
        _row("600003", "弱收盘", change=4.0, amount=900_000_000,
             close=9.7),
        _row("600004", "低成交", change=4.0, amount=100_000_000),
        _row("600005", "下跌", change=-1.0, amount=900_000_000),
    ]
    pool = select_daily_pool(rows, {}, top=10)
    assert [item["stock_code"] for item in pool] == ["600001"]


def test_board_specific_limit_allows_growth_board_but_not_main_board():
    rows = [
        _row("300001", "创业板强势", change=12.0, amount=800_000_000),
        _row("600001", "主板过热", change=12.0, amount=900_000_000),
    ]
    pool = select_daily_pool(rows, {}, top=10)
    assert [item["stock_code"] for item in pool] == ["300001"]
    assert limit_pct_for("300001") == 19.5
    assert limit_pct_for("600001") == 9.5


def test_negative_main_flow_is_excluded_and_positive_flow_ranks_first():
    rows = [
        _row("600001", "流入", change=4.0, amount=800_000_000),
        _row("600002", "流出", change=4.0, amount=900_000_000),
    ]
    pool = select_daily_pool(
        rows, {"600001": 300_000_000.0, "600002": -1.0}, top=10
    )
    assert [item["stock_code"] for item in pool] == ["600001"]


def test_ranking_is_deterministic_for_equal_scores():
    rows = [
        _row("600002", "乙", change=4.0, amount=800_000_000),
        _row("600001", "甲", change=4.0, amount=800_000_000),
    ]
    pool = select_daily_pool(rows, {}, top=10)
    assert [item["stock_code"] for item in pool] == ["600001", "600002"]
