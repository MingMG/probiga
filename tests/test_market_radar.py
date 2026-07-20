# -*- coding: utf-8 -*-
from datetime import datetime

from biz.market_radar.core import MarketRadarEngine, _robust_z, market_phase


def test_robust_z_is_cross_sectional_and_zero_for_flat_values():
    assert _robust_z([1, 1, 1]) == [0.0, 0.0, 0.0]
    values = _robust_z([1, 2, 3])
    assert values[0] < 0 < values[-1]


def test_market_phase_includes_call_auction():
    assert market_phase(datetime(2026, 7, 20, 9, 20)) == "call_auction"
    assert market_phase(datetime(2026, 7, 20, 10, 0)) == "morning"
    assert market_phase(datetime(2026, 7, 20, 16, 0)) == "closed"


def test_sector_roles_include_dragons_core_and_followers():
    engine = MarketRadarEngine(None, {"min_sector_members": 3, "sector_limit": 50})
    engine._sectors = {
        "CONCEPT:C1": {
            "sector_code": "CONCEPT:C1",
            "sector_name": "测试题材",
            "sector_type": "concept",
            "members": {"000001", "000002", "000003", "000004"},
        }
    }
    quotes = []
    for index, score in enumerate([85, 75, 65, 45], start=1):
        quotes.append(
            {
                "stock_code": f"00000{index}",
                "short_name": f"测试{index}",
                "score": score,
                "direction": "UP",
                "change_pct": score / 10,
                "amount_delta": 100 - index,
                "five_pressure": 20,
            }
        )
    sectors = engine._build_sectors(quotes, datetime(2026, 7, 20, 10, 0))
    assert sectors[0]["direction"] == "UP"
    assert [row["role"] for row in sectors[0]["dragons"]] == ["龙1", "龙2", "龙3"]
    assert sectors[0]["core"]["role"] == "板块中军"
    assert sectors[0]["followers"][0]["role"] == "跟涨"
