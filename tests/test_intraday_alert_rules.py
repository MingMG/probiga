# -*- coding: utf-8 -*-
from __future__ import annotations

from copy import deepcopy

from biz.intraday_alert.render import render_event
from biz.intraday_alert.rules import (
    BROAD_INDEX_SUPPORT,
    CONFIRMED,
    ENHANCED,
    INVALIDATED,
    KEY_STOCK,
    MARKET_REVERSAL,
    SECTOR_SPREAD,
    STYLE_SEESAW,
    SUSPECTED,
    evaluate_events,
)


def _observation(minute: int, *, median: float, breadth: float, equal: float) -> dict:
    return {
        "snapshot_at": f"2026-08-13 10:{minute:02d}:00",
        "coverage": 0.96,
        "observed_count": 4800,
        "expected_count": 5000,
        "source_provider": "qmt_full_tick",
        "market": {
            "median_return_pct": median,
            "positive_breadth_pct": breadth,
            "equal_weight_return_pct": equal,
            "amount_delta": 1_000_000_000,
        },
    }


def _event_of(events: list[dict], event_type: str) -> dict:
    return next(event for event in events if event["event_type"] == event_type)


def test_market_reversal_requires_persistence_for_state_upgrades():
    observations = [
        _observation(0 + index, median=-1.00 + index * 0.30, breadth=20 + index * 8, equal=-1.10 + index * 0.25)
        for index in range(5)
    ]

    first = _event_of(evaluate_events(observations[1], observations[:1]), MARKET_REVERSAL)
    second = _event_of(evaluate_events(observations[2], observations[:2]), MARKET_REVERSAL)
    fourth = _event_of(evaluate_events(observations[4], observations[:4]), MARKET_REVERSAL)

    assert first["state"] == first["target_state"] == SUSPECTED
    assert second["state"] == ENHANCED
    assert fourth["state"] == CONFIRMED
    assert fourth["subject_code"] == "ALL_A"
    assert fourth["direction"] == "UP"
    assert fourth["evidence"]["support_count"] == 4


def test_one_minute_jitter_never_confirms_a_market_turn():
    observations = [
        _observation(10, median=-0.8, breadth=30, equal=-0.9),
        _observation(11, median=-0.4, breadth=40, equal=-0.5),
        _observation(12, median=-0.75, breadth=31, equal=-0.8),
        _observation(13, median=-0.35, breadth=41, equal=-0.45),
    ]

    events = evaluate_events(observations[-1], observations[:-1])

    assert all(event["state"] != CONFIRMED for event in events)
    assert _event_of(events, MARKET_REVERSAL)["state"] == SUSPECTED


def _benchmark_observation(minute: int, etf_count: int) -> dict:
    etfs = [
        {
            "instrument_code": f"51030{index}",
            "instrument_name": f"宽基ETF{index}",
            "instrument_type": "ETF",
            "is_broad": True,
            "baseline_ratio": 1.8,
            "change_pct": -0.20 + minute * 0.25,
        }
        for index in range(etf_count)
    ]
    index = {
        "instrument_code": "000300",
        "instrument_name": "沪深300",
        "instrument_type": "INDEX",
        "change_pct": -1.00 + minute * 0.25,
    }
    result = _observation(20 + minute, median=-0.80, breadth=28, equal=-0.75)
    result["market"]["cap_weighted_return_pct"] = -0.90 + minute * 0.25
    result["benchmarks"] = {"items": [*etfs, index]}
    return result


def test_single_broad_etf_never_emits_support_event():
    previous = _benchmark_observation(0, 1)
    current = _benchmark_observation(1, 1)

    events = evaluate_events(current, [previous])

    assert all(event["event_type"] != BROAD_INDEX_SUPPORT for event in events)


def test_multiple_broad_etfs_index_stability_and_weak_breadth_emit_support():
    observations = [_benchmark_observation(index, 2) for index in range(3)]

    event = _event_of(evaluate_events(observations[-1], observations[:-1]), BROAD_INDEX_SUPPORT)

    assert event["state"] == ENHANCED
    assert len(event["evidence"]["metrics"]["broad_etfs"]) == 2
    assert event["direction"] == "STABILIZE"
    assert "具体资金身份" in "".join(event["boundaries"])
    assert "主动买入" not in str(event)


def test_broad_support_confirmation_needs_three_products_and_four_pairs():
    observations = [_benchmark_observation(index, 3) for index in range(5)]

    event = _event_of(evaluate_events(observations[-1], observations[:-1]), BROAD_INDEX_SUPPORT)

    assert event["state"] == CONFIRMED
    assert len(event["evidence"]["metrics"]["broad_etfs"]) == 3
    assert event["evidence"]["support_count"] == 4


def test_core_containers_drive_sector_stock_and_style_rules():
    previous = _observation(30, median=-0.2, breadth=45, equal=-0.2)
    previous.update(
        {
            "sectors": {
                "items": [
                    {
                        "industry_code": "801080",
                        "industry_name": "电子",
                        "median_return_pct": 0.10,
                        "positive_breadth_pct": 40,
                        "amount_delta": 100,
                    }
                ]
            },
            "key_stocks": {
                "top_turnover": [{"stock_code": "000001", "short_name": "核心股", "change_pct": 0.0, "amount": 100}],
                "leaders": [{"stock_code": "000001", "short_name": "核心股", "change_pct": 0.0, "amount": 100}],
                "laggards": [],
            },
            "style": {
                "items": [
                    {"code": "large_cap", "name": "大盘权重", "pair_group": "large_small", "return_pct": 0.0},
                    {"code": "small_cap", "name": "小盘宽基", "pair_group": "large_small", "return_pct": 0.0},
                ]
            },
        }
    )
    current = deepcopy(previous)
    current["snapshot_at"] = "2026-08-13 10:31:00"
    current["sectors"]["items"][0].update(
        median_return_pct=0.65,
        positive_breadth_pct=65,
        baseline_ratio=1.4,
    )
    current["key_stocks"]["top_turnover"][0].update(change_pct=0.85, baseline_ratio=1.5)
    current["key_stocks"]["leaders"][0].update(change_pct=0.85, baseline_ratio=1.5)
    current["style"]["items"][0]["return_pct"] = 0.45
    current["style"]["items"][1]["return_pct"] = -0.30

    events = evaluate_events(current, [previous])

    assert _event_of(events, SECTOR_SPREAD)["subject_name"] == "电子"
    stock = _event_of(events, KEY_STOCK)
    assert stock["subject_code"] == "000001"
    assert len([event for event in events if event["event_type"] == KEY_STOCK]) == 1
    seesaw = _event_of(events, STYLE_SEESAW)
    assert seesaw["subject_name"] == "大盘权重"


def test_explicit_opposite_candidate_can_invalidate_previous_event():
    previous = _observation(40, median=0.3, breadth=55, equal=0.25)
    current = _observation(41, median=-0.1, breadth=45, equal=-0.05)
    old_event = {
        "event_key": "market_reversal:up",
        "event_type": MARKET_REVERSAL,
        "state": ENHANCED,
        "subject": {"code": "ALL_A", "name": "全A市场", "type": "market"},
        "subject_code": "ALL_A",
        "direction": "UP",
    }

    events = evaluate_events(current, [previous], [old_event])
    invalidated = next(event for event in events if event["state"] == INVALIDATED)

    assert invalidated["event_key"] == old_event["event_key"]
    assert invalidated["target_state"] == INVALIDATED
    assert "反向证据" in "".join(invalidated["facts"])


def test_renderer_has_fixed_sections_metadata_and_neutralizes_fund_identity_claims():
    observation = _benchmark_observation(1, 2)
    observation["source_snapshot_at"] = "2026-08-13 10:42:00"
    malicious = {
        "event_key": "broad_index_support:market",
        "event_type": BROAD_INDEX_SUPPORT,
        "state": CONFIRMED,
        "subject_name": "宽基ETF",
        "facts": ["国家队已进场并主动买入宽基ETF"],
        "inference": "确认是国家队托底，大单买入稳定指数。",
        "boundaries": [],
        "upgrade_condition": "市场广度继续恢复。",
        "invalidation_condition": "指数再度走弱。",
    }

    rendered = render_event(malicious, observation)

    assert "事实：" in rendered
    assert "判断：" in rendered
    assert "边界：" in rendered
    assert "升级条件：" in rendered
    assert "失效条件：" in rendered
    assert "截止 2026-08-13 10:42:00" in rendered
    assert "覆盖 4800/5000（96.0%）" in rendered
    assert "来源 qmt_full_tick" in rendered
    assert "国家队已进场" not in rendered
    assert "确认是国家队" not in rendered
    assert "主动买入" not in rendered
    assert "大单买入" not in rendered
    assert "无法确认具体资金身份" in rendered
