from __future__ import annotations

from copy import deepcopy
from datetime import datetime

import pytest

from biz.premarket.theme_forecast import (
    PREMARKET_STAGE,
    _flow_dataset_quality,
    _flow_history_by_code,
    _neutralize_flow_profiles,
    _stock_candidate_score,
    _stock_flow_factor_details,
    _stock_flow_factor_score,
    build_auction_confirmation_from_records,
    build_theme_forecast_from_records,
    classify_catalyst,
    family_for_label,
    format_forecast_markdown,
    score_external_theme,
)


def _external(symbol: str, change: float, name: str | None = None) -> dict:
    return {
        "symbol": symbol,
        "display_name": name or symbol,
        "change_pct": change,
        "price": 100.0,
        "availability": "available",
    }


def _forecast(*, news_rows=None):
    memberships = [
        {"stock_code": "300999", "concept_name": "固态电池"},
        {"stock_code": "000936", "concept_name": "宁德时代概念"},
        {"stock_code": "300333", "concept_name": "国产软件"},
        {"stock_code": "300777", "concept_name": "量子科技"},
    ]
    analysis = [
        {"stock_code": "300999", "stock_name": "测试电池", "short_term_score": 84, "technical_score": 82, "capital_score": 78, "event_score": 88, "event_risk_level": "LOW", "recommend_status": "ALLOW"},
        {"stock_code": "000936", "stock_name": "华西股份", "short_term_score": 72, "technical_score": 80, "capital_score": 62, "event_score": 70, "event_risk_level": "LOW", "recommend_status": "ALLOW"},
        {"stock_code": "300333", "stock_name": "兆日科技", "short_term_score": 75, "technical_score": 86, "capital_score": 66, "event_score": 65, "event_risk_level": "LOW", "recommend_status": "ALLOW"},
        {"stock_code": "300777", "stock_name": "动态样本", "short_term_score": 70, "technical_score": 72, "capital_score": 60, "event_score": 55, "event_risk_level": "LOW", "recommend_status": "ALLOW"},
    ]
    klines = [
        {"stock_code": row["stock_code"], "short_name": row["stock_name"], "change_pct": 1.5}
        for row in analysis
    ]
    default_news = [
        {
            "title": "韩国三星SDI全固态电池进入工程验证",
            "content": "日本松下同步测试，新一代动力电池需求增长",
            "publish_time": "2026-08-13 08:18:00",
        },
        {
            "title": "某手机服务价格上涨",
            "content": "与资源品和电池无关",
            "publish_time": "2026-08-13 08:20:00",
        },
    ]
    return build_theme_forecast_from_records(
        session_date="2026-08-13",
        source_trade_date="2026-08-12",
        cutoff_at="2026-08-13 09:07:59",
        hot_rows=[
            {"snapshot_date": "2026-08-12", "concept_name": "固态电池", "rank": 6, "change_pct": 0.8},
            {"snapshot_date": "2026-08-12", "concept_name": "国产软件", "rank": 3, "change_pct": 1.1},
            {"snapshot_date": "2026-08-12", "concept_name": "量子科技", "rank": 8, "change_pct": 0.5},
        ],
        news_rows=default_news if news_rows is None else news_rows,
        membership_rows=memberships,
        analysis_rows=analysis,
        kline_rows=klines,
        stock_flow_rows=[
            {"stock_code": "300999", "main_net_inflow": 20_000_000},
            {"stock_code": "000936", "main_net_inflow": 5_000_000},
            {"stock_code": "300333", "main_net_inflow": -2_000_000},
            {"stock_code": "300777", "main_net_inflow": 1_000_000},
        ],
        external_items=[
            _external("kospi", 2.2, "韩国KOSPI"),
            _external("nikkei", 1.1, "日本日经225"),
            _external("nasdaq", 0.5, "美国纳斯达克"),
            _external("copper", 1.8, "铜"),
            _external("sp500", 0.4, "美国标普500"),
        ],
        external_summary={"external_market_score": 56.0},
        theme_limit=10,
        stocks_per_theme=5,
    )


def test_subject_aware_news_does_not_turn_generic_price_story_into_resources():
    generic = classify_catalyst("某手机服务价格上涨", "软件会员调价")
    battery = classify_catalyst("韩国三星SDI全固态电池进入工程验证")

    assert "resources" not in generic["family_keys"]
    assert battery["qualified"] is True
    assert "battery_lithium" in battery["family_keys"]
    assert "korea" in battery["regions"]


def test_short_ascii_theme_tokens_do_not_match_inside_unrelated_words():
    assert family_for_label("MicroLED") is None
    assert family_for_label("CRO概念").key == "biomedicine"
    assert family_for_label("AI应用").key == "ai_compute"


def test_ambiguous_chinese_compounds_go_to_the_real_industry():
    assert family_for_label("电子").key == "semiconductor"
    assert family_for_label("消费电子").key == "semiconductor"
    assert family_for_label("电子书") is None
    assert family_for_label("生物质能").key == "power_grid"
    chip_news = classify_catalyst(
        "中芯国际AI配套芯片需求旺盛",
        "消费电子收入增加",
    )
    assert "consumer" not in chip_news["family_keys"]
    assert "semiconductor" in chip_news["family_keys"]


def test_overseas_resonance_uses_korea_japan_us_and_commodities():
    supportive, evidence = score_external_theme(
        "battery_lithium",
        [
            _external("kospi", 2.0, "韩国KOSPI"),
            _external("nikkei", 1.0, "日本日经225"),
            _external("nasdaq", 0.8, "美国纳斯达克"),
            _external("copper", 1.5, "铜"),
            _external("sp500", 0.4, "美国标普500"),
        ],
        catalyst_regions=["korea", "japan", "us"],
    )
    adverse, _ = score_external_theme(
        "battery_lithium",
        [
            _external("kospi", -2.0),
            _external("nikkei", -1.0),
            _external("nasdaq", -0.8),
            _external("copper", -1.5),
            _external("sp500", -0.4),
        ],
    )

    assert supportive > 60
    assert supportive > adverse
    assert any("韩国" in item for item in evidence)
    assert any("日本" in item for item in evidence)
    assert any("美国" in item for item in evidence)


def test_overseas_sector_proxies_drive_theme_specific_resonance():
    supportive, evidence = score_external_theme(
        "semiconductor",
        [
            _external("us_semiconductor", 2.0, "美股半导体ETF"),
            _external("kr_semiconductor", 1.5, "韩国三星电子"),
            _external("jp_semiconductor", 1.0, "日本东京电子"),
            _external("taiwan_semiconductor", 2.5, "中国台湾台积电"),
        ],
    )
    adverse, _ = score_external_theme(
        "semiconductor",
        [
            _external("us_semiconductor", -2.0),
            _external("kr_semiconductor", -1.5),
            _external("jp_semiconductor", -1.0),
            _external("taiwan_semiconductor", -2.5),
        ],
    )
    assert supportive > 60
    assert supportive > adverse
    assert any("韩国" in item for item in evidence)
    assert any("日本" in item for item in evidence)


def test_stock_flow_factor_rewards_normalized_persistent_confirmed_inflow():
    strong_history = [
        {
            "trade_date": f"2026-08-{day:02d}",
            "main_net_inflow": 5_000_000,
            "max_net_inflow": 3_000_000,
            "lg_net_inflow": 2_000_000,
        }
        for day in (12, 11, 10, 9, 8)
    ]
    weak_history = [
        {
            "trade_date": f"2026-08-{day:02d}",
            "main_net_inflow": value,
            "max_net_inflow": -3_000_000,
            "lg_net_inflow": -2_000_000,
        }
        for day, value in zip((12, 11, 10, 9, 8), (5_000_000, -4_000_000, -3_000_000, -2_000_000, -1_000_000))
    ]

    strong_score, evidence = _stock_flow_factor_score(
        strong_history,
        {"amount": 100_000_000},
        theme_flow_score=72.0,
    )
    weak_score, _ = _stock_flow_factor_score(
        weak_history,
        {"amount": 100_000_000},
        theme_flow_score=72.0,
    )

    assert strong_score is not None and weak_score is not None
    assert strong_score > weak_score + 15
    assert any("主力净流入强度+5.00%" in item for item in evidence)
    assert any("近5日净流入5日" in item for item in evidence)
    assert any("大单与超大单方向一致" in item for item in evidence)
    assert any("板块个股资金共振" in item for item in evidence)


def test_stock_flow_factor_uses_turnover_rate_instead_of_absolute_amount():
    small_cap_history = [{
        "trade_date": "2026-08-12",
        "main_net_inflow": 5_000_000,
        "max_net_inflow": 3_000_000,
        "lg_net_inflow": 2_000_000,
    }]
    large_cap_history = [{
        "trade_date": "2026-08-12",
        "main_net_inflow": 50_000_000,
        "max_net_inflow": 30_000_000,
        "lg_net_inflow": 20_000_000,
    }]

    small_score, _ = _stock_flow_factor_score(
        small_cap_history,
        {"amount": 100_000_000},
        theme_flow_score=65.0,
    )
    large_score, _ = _stock_flow_factor_score(
        large_cap_history,
        {"amount": 1_000_000_000},
        theme_flow_score=65.0,
    )

    assert small_score == large_score


def test_flow_history_rejects_future_sessions_and_keeps_latest_five():
    rows = [
        {"stock_code": "300999", "trade_date": f"2026-08-{day:02d}", "main_net_inflow": day}
        for day in range(7, 14)
    ]
    history = _flow_history_by_code(
        rows,
        source_trade_date="2026-08-12",
        limit=5,
    )["300999"]

    assert [row["trade_date"] for row in history] == [
        "2026-08-12", "2026-08-11", "2026-08-10", "2026-08-09", "2026-08-08",
    ]


def test_flow_profile_normalizes_every_day_by_that_days_turnover():
    history = [
        {
            "trade_date": f"2026-08-{day:02d}",
            "main_net_inflow": turnover * 0.05,
            "max_net_inflow": turnover * 0.03,
            "lg_net_inflow": turnover * 0.02,
            "turnover_amount": turnover,
        }
        for day, turnover in zip((12, 11, 10, 9, 8), (100, 200, 300, 400, 500))
    ]
    profile = _stock_flow_factor_details(history, {})

    assert profile["quality_status"] == "PASS"
    assert profile["normalized_days"] == 5
    assert profile["five_day_rate"] == 5.0


def test_flow_profiles_are_neutralized_within_industry_and_cap_peers():
    profiles = {
        f"30000{index}": {"raw_score": score, "score": score, "quality_status": "PASS"}
        for index, score in enumerate((30, 40, 50, 60, 70), start=1)
    }
    klines = {
        code: {"industry_name": "测试行业", "market_cap": (index + 1) * 1_000_000}
        for index, code in enumerate(profiles)
    }
    result = _neutralize_flow_profiles(profiles, klines)

    ordered = sorted(result, key=lambda code: result[code]["raw_score"])
    assert result[ordered[0]]["industry_percentile"] == 0.0
    assert result[ordered[-1]]["industry_percentile"] == 100.0
    assert result[ordered[-1]]["score"] > result[ordered[0]]["score"]


def test_flow_quality_gate_blocks_missing_coverage_and_passes_complete_data():
    klines = {f"30000{index}": {} for index in range(1, 6)}
    profiles = {
        code: {"score": 60, "normalized_days": 5}
        for code in klines
    }
    rows = [
        {"stock_code": code, "trade_date": "2026-08-12", "main_net_inflow": 1, "turnover_amount": 100}
        for code in klines
    ]

    assert _flow_dataset_quality(profiles, klines, rows)["status"] == "PASS"
    assert _flow_dataset_quality({}, klines, [])["status"] == "BLOCK"


def test_candidate_does_not_double_count_legacy_capital_score_when_flow_exists():
    profile = {
        "score": 72.0,
        "raw_score": 72.0,
        "quality_status": "PASS",
        "latest_direction": 0.5,
        "history_days": 5,
        "normalized_days": 5,
        "positive_days": 4,
    }
    common = dict(
        code="300999", theme_score=75, theme_match_count=2, theme_v3_score=70,
        alpha={"v3_alpha_score": 72}, recommendation={},
        kline={"change_pct": 1.0}, flow={}, flow_profile=profile,
        theme_flow_score=70, flow_dataset_status="PASS",
    )
    low = _stock_candidate_score(analysis={"capital_score": 0}, **common)
    high = _stock_candidate_score(analysis={"capital_score": 100}, **common)

    assert low[0] == high[0]


def test_0920_confirmation_is_independent_and_rejects_future_quotes():
    forecast = {
        "session_date": "2026-08-13",
        "stage": "PREMARKET_0908",
        "model_version": "frozen",
        "run_uid": "run-0908",
        "stock_candidates": [{
            "global_rank": 1,
            "stock_code": "300999",
            "stock_name": "测试电池",
            "score": 78.0,
        }],
    }
    frozen = deepcopy(forecast)
    result = build_auction_confirmation_from_records(
        forecast,
        [
            {"stock_code": "300999", "price": 20, "change_pct": 2.5, "snapshot_at": "2026-08-13 09:19:59"},
            {"stock_code": "300999", "price": 22, "change_pct": 9.8, "snapshot_at": "2026-08-13 09:21:00"},
        ],
        cutoff_at="2026-08-13 09:20:59",
    )

    assert forecast == frozen
    assert result["source_run_uid"] == "run-0908"
    assert result["confirmations"][0]["confirmation_status"] == "CONFIRMED"
    assert result["confirmations"][0]["quote_change_pct"] == 2.5


def test_forecast_is_theme_first_dynamic_and_uses_database_memberships():
    forecast = _forecast()
    theme_map = {item["family_key"]: item for item in forecast["themes"]}
    dynamic_names = {item["theme_name"] for item in forecast["themes"]}

    assert forecast["stage"] == PREMARKET_STAGE
    assert forecast["source_trade_date"] == "2026-08-12"
    assert theme_map["battery_lithium"]["score"] > theme_map["software_security"]["score"]
    assert theme_map["battery_lithium"]["external_score"] > 50
    assert "量子科技" in dynamic_names
    battery_codes = {item["stock_code"] for item in theme_map["battery_lithium"]["stock_candidates"]}
    assert "300999" in battery_codes


def test_news_after_0908_cutoff_cannot_change_the_result():
    base = _forecast(news_rows=[])
    future = _forecast(news_rows=[{
        "title": "韩国固态电池获得超级订单并全面量产",
        "content": "重大催化",
        "publish_time": "2026-08-13 09:08:01",
    }])
    base_score = next(item["score"] for item in base["themes"] if item["family_key"] == "battery_lithium")
    future_score = next(item["score"] for item in future["themes"] if item["family_key"] == "battery_lithium")
    assert future_score == base_score


def test_source_trade_date_must_be_before_session_date():
    with pytest.raises(ValueError, match="source_trade_date"):
        build_theme_forecast_from_records(
            session_date="2026-08-13",
            source_trade_date="2026-08-13",
            cutoff_at=datetime(2026, 8, 13, 9, 7, 59),
        )


def test_markdown_states_frozen_cutoff_and_candidates():
    markdown = format_forecast_markdown(_forecast())
    assert "09:08盘前主线预判" in markdown
    assert "数据截止 2026-08-13 09:07:59" in markdown
    assert "结果已冻结" in markdown
    assert "测试电池(300999)" in markdown
