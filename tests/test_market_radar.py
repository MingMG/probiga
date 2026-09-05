# -*- coding: utf-8 -*-
from datetime import datetime

from biz.market_radar.core import (
    MarketRadarEngine,
    _robust_z,
    annotate_radar_relations,
    build_radar_relation_index,
    market_phase,
)


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


def test_radar_relations_use_watchlist_holding_and_latest_candidate_rows():
    index = build_radar_relation_index(
        [
            {"stock_code": "000001", "shares": 0},
            {"stock_code": "000002", "shares": 300},
        ],
        [{"stock_code": "000003"}],
        candidate_date="2026-09-04",
    )
    rows = annotate_radar_relations(
        [
            {
                "sector_name": "测试板块",
                "dragon_json": [{"stock_code": "000001"}, {"stock_code": "000003"}],
                "core_json": {"stock_code": "000002"},
            }
        ],
        index,
    )
    assert rows[0]["relations"] == {"watchlist": 2, "holding": 1, "strategy_candidate": 1}
    assert rows[0]["relation_codes"]["holding"] == ["000002"]
    assert index["candidate_date"] == "2026-09-04"


def test_radar_relation_scope_filters_without_changing_scores():
    index = build_radar_relation_index(
        [{"stock_code": "000001", "shares": 100}],
        [],
    )
    source = [
        {"stock_code": "000001", "score": 88.0},
        {"stock_code": "000002", "score": 77.0},
    ]
    rows = annotate_radar_relations(source, index, scope="holding")
    assert [row["stock_code"] for row in rows] == ["000001"]
    assert rows[0]["score"] == 88.0


def test_unavailable_relation_source_is_not_reported_as_zero():
    index = build_radar_relation_index(
        [],
        [],
        portfolio_status="unavailable",
        candidate_status="unavailable",
    )
    row = annotate_radar_relations([{"stock_code": "000001"}], index)[0]
    assert row["relations"]["watchlist"] is None
    assert row["relations"]["holding"] is None
    assert row["relations"]["strategy_candidate"] is None
