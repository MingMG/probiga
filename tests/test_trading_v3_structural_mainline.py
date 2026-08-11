from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
import pytest

from server.trading_v3.engine import TradingV3Engine
from server.trading_v3.sleeves import SLEEVE_BUILDERS
from server.trading_v3.structural_mainline import (
    STRATEGY_KEY,
    weak_market_structural_mainline,
)
from server.trading_v3.theme_features import (
    THEME_COMPOSITE_WEIGHTS,
    calculate_theme_composite_score,
    calculate_theme_statistics,
    cluster_theme_labels,
    cluster_themes_by_component_overlap,
)


NOW = datetime(2026, 8, 1, 15, 0)


def _structural_features() -> dict[str, float | str | list[str]]:
    return {
        "market_return_20d_pct": -3.2,
        "sector_breadth_pct": 68.0,
        "sector_breadth_acceleration_pct": 8.0,
        "sector_amount_acceleration_pct": 42.0,
        "sector_relative_return_pct": 4.5,
        "sector_leadership_depth": 0.8,
        "theme_news_novelty_score": 0.75,
        "theme_topk_member_score_median": 0.81,
        "theme_composite_score": 0.82,
        "theme_score_news_novelty_available": 1.0,
        "stock_leadership_score": 0.78,
        "relative_strength_20d_pct": 14.0,
        "return_5d_pct": 7.0,
        "amount_ratio_5_20": 1.4,
        "latest_change_pct": 2.3,
        "distance_ma20_pct": 4.0,
        "atr_14d_pct": 2.8,
        "sector_crowding": 0.35,
        "entry_eligible": 1.0,
        "latest_tradable": 1.0,
        "theme_code": "AI_APP",
        "theme_name": "AI应用",
        "theme_cluster_keys": ["AI_APPLICATION"],
    }


def test_sector_composite_score_exposes_all_five_weighted_components():
    result = calculate_theme_composite_score(
        advance_breadth_pct=70.0,
        breadth_acceleration_pct=12.0,
        capital_acceleration_pct=40.0,
        relative_strength_pct=4.0,
        news_novelty=0.80,
        topk_member_score_median=0.75,
    )

    component_keys = {
        "theme_score_advance_breadth",
        "theme_score_capital_acceleration",
        "theme_score_relative_strength",
        "theme_score_news_novelty",
        "theme_score_topk_member_median",
    }
    assert component_keys <= result.keys()
    expected = (
        THEME_COMPOSITE_WEIGHTS["advance_breadth"]
        * result["theme_score_advance_breadth"]
        + THEME_COMPOSITE_WEIGHTS["capital_acceleration"]
        * result["theme_score_capital_acceleration"]
        + THEME_COMPOSITE_WEIGHTS["relative_strength"]
        * result["theme_score_relative_strength"]
        + THEME_COMPOSITE_WEIGHTS["news_novelty"]
        * result["theme_score_news_novelty"]
        + THEME_COMPOSITE_WEIGHTS["topk_member_median"]
        * result["theme_score_topk_member_median"]
    )
    assert result["theme_composite_score"] == pytest.approx(expected)


def test_theme_statistics_use_news_and_topk_member_score_median():
    dates = pd.date_range("2026-07-27", periods=6, freq="D")
    members = [f"00000{index}" for index in range(1, 6)]
    rows = []
    for day_index, trade_date in enumerate(dates):
        for stock_index, code in enumerate(members):
            rows.append({
                "stock_code": code,
                "trade_date": trade_date,
                "change_pct": 0.5 + stock_index * 0.2,
                "amount": 100_000_000 * (1 + day_index * 0.15),
            })
        for stock_index in range(6, 16):
            rows.append({
                "stock_code": f"0000{stock_index:02d}",
                "trade_date": trade_date,
                "change_pct": -0.3,
                "amount": 500_000_000,
            })
    memberships = {
        code: [("AI_APP", "人工智能应用概念", "concept")]
        for code in members
    }
    member_scores = dict(
        zip(members, (0.95, 0.85, 0.75, 0.45, 0.20), strict=True)
    )

    stats = calculate_theme_statistics(
        pd.DataFrame(rows),
        as_of=dates[-1],
        memberships=memberships,
        member_scores=member_scores,
        theme_news_novelty={"AI_APPLICATION": 0.80},
        top_k=3,
    )["AI_APP"]

    assert stats["theme_news_novelty_score"] == pytest.approx(0.80)
    assert stats["theme_topk_member_score_median"] == pytest.approx(0.85)
    assert stats["theme_topk_member_count"] == 3
    assert stats["theme_opportunity_score"] == stats["theme_composite_score"]


def test_theme_label_clustering_deduplicates_aliases_without_merging_families():
    clusters = cluster_theme_labels([
        "AI 应用概念",
        "人工智能应用",
        "AIGC板块",
        "机器人概念",
        "人形机器人",
        "具身智能主题",
    ])
    by_key = {item["cluster_key"]: item for item in clusters}

    assert set(by_key) == {"AI_APPLICATION", "ROBOTICS"}
    assert by_key["AI_APPLICATION"]["canonical_label"] == "AI应用"
    assert by_key["ROBOTICS"]["canonical_label"] == "机器人"
    assert len(by_key["AI_APPLICATION"]["labels"]) == 3
    assert len(by_key["ROBOTICS"]["labels"]) == 3


def test_theme_label_clustering_is_generic_not_ai_robot_allowlist():
    clusters = cluster_theme_labels([
        "云办公概念",
        "云服务板块",
        "国产软件",
        "创新药概念",
        "未知新主题",
    ])
    keys = {item["cluster_key"] for item in clusters}

    assert "CLOUD_COMPUTING" in keys
    assert "DOMESTIC_SOFTWARE" in keys
    assert "INNOVATIVE_DRUG" in keys
    assert "LABEL:未知新" in keys


def test_component_overlap_clustering_is_deterministic_and_complete_linked():
    components = {
        "量子算力": {"000001", "000002", "000003", "000004"},
        "量子计算": {
            "000001",
            "000002",
            "000003",
            "000004",
            "000005",
        },
        "量子传感": {"000001", "000006", "000007", "000008"},
        "AI应用": {"300001", "300002", "300003"},
        "机器人": {"300001", "300002", "300003"},
    }

    clusters = cluster_themes_by_component_overlap(components)
    reversed_clusters = cluster_themes_by_component_overlap(
        dict(reversed(tuple(components.items())))
    )

    assert clusters == reversed_clusters
    grouped_ids = {frozenset(item["theme_ids"]) for item in clusters}
    assert frozenset({"量子算力", "量子计算"}) in grouped_ids
    assert frozenset({"量子传感"}) in grouped_ids
    assert frozenset({"AI应用"}) in grouped_ids
    assert frozenset({"机器人"}) in grouped_ids
    quantum = next(
        item
        for item in clusters
        if set(item["theme_ids"]) == {"量子算力", "量子计算"}
    )
    assert quantum["minimum_pairwise_overlap"] == pytest.approx(0.8)
    assert quantum["semantic_cluster_keys"][0].startswith("LABEL:")


def test_weak_market_internal_strength_can_trigger_independent_signal():
    signal = weak_market_structural_mainline(
        "300001",
        "样本AI公司",
        _structural_features(),
        NOW,
        NOW + timedelta(days=20),
    )

    assert signal.status == "SCORED"
    assert signal.strategy_key == "weak_market_structural_mainline"
    assert signal.strategy_key != "right_side_trend"
    assert signal.score >= 0.64
    assert signal.features["structural_sector_internal_strength"] >= 0.64


def test_weak_market_signal_rejects_weak_sector_internals():
    features = _structural_features()
    features.update({
        "sector_breadth_pct": 46.0,
        "sector_amount_acceleration_pct": 4.0,
        "sector_relative_return_pct": 0.3,
        "theme_composite_score": 0.35,
        "theme_topk_member_score_median": 0.40,
    })

    signal = weak_market_structural_mainline(
        "300001",
        "样本AI公司",
        features,
        NOW,
        NOW + timedelta(days=10),
    )

    assert signal.status == "SETUP_NOT_READY"


def test_high_breadth_uses_soft_exhaustion_penalty_instead_of_hard_block():
    features = _structural_features()
    features.update({
        "sector_breadth_pct": 97.0,
        "sector_breadth_acceleration_pct": 12.0,
        "sector_leadership_depth": 1.0,
        "sector_crowding": 0.75,
        "theme_composite_score": 0.90,
        "theme_topk_member_score_median": 0.90,
        "stock_leadership_score": 0.90,
    })

    signal = weak_market_structural_mainline(
        "300001",
        "高宽度结构主线样本",
        features,
        NOW,
        NOW + timedelta(days=10),
    )

    assert signal.status == "SCORED"
    assert signal.features["structural_exhaustion_penalty"] > 0


def test_structural_signal_cannot_bypass_data_quality_gate():
    features = _structural_features()
    features["qmt_attestation_current"] = False

    signal = weak_market_structural_mainline(
        "300001",
        "样本AI公司",
        features,
        NOW,
        NOW + timedelta(days=10),
    )

    assert signal.status == "DATA_QUALITY_BLOCKED"
    assert "QMT_DAILY_KLINE_ATTESTATION_NOT_CURRENT" in signal.reasons[-1]


def test_structural_signal_requires_complete_five_factor_sector_features():
    features = _structural_features()
    del features["theme_news_novelty_score"]

    signal = weak_market_structural_mainline(
        "300001",
        "样本AI公司",
        features,
        NOW,
        NOW + timedelta(days=10),
    )

    assert signal.status == "INSUFFICIENT_DATA"
    assert "theme_news_novelty_score" in signal.reasons[0]


def test_structural_model_does_not_take_right_side_identity_in_strong_market():
    features = _structural_features()
    features["market_return_20d_pct"] = 2.0

    signal = weak_market_structural_mainline(
        "300001",
        "样本AI公司",
        features,
        NOW,
        NOW + timedelta(days=10),
    )

    assert signal.status == "MARKET_REGIME_NOT_APPLICABLE"
    assert signal.strategy_key == STRATEGY_KEY


def test_new_strategy_has_its_own_oos_gate_and_sleeve_registry_key():
    signal = weak_market_structural_mainline(
        "300001",
        "样本AI公司",
        _structural_features(),
        NOW,
        NOW + timedelta(days=10),
    )
    forecast = TradingV3Engine().forecast(signal)

    assert SLEEVE_BUILDERS[STRATEGY_KEY] is weak_market_structural_mainline
    assert forecast.strategy_key == STRATEGY_KEY
    assert forecast.status == "RESEARCH_ONLY_UNCALIBRATED"
    assert forecast.expected_return_net_pct is None


def test_engine_scores_every_theme_then_selects_theme_specific_winner():
    features = _structural_features()
    features["theme_signal_candidates"] = [
        {
            "theme_feature_key": "cloud-key",
            "theme_code": "CLOUD",
            "theme_name": "云办公",
            "theme_cluster_keys": ["CLOUD_COMPUTING"],
            "theme_composite_score": 0.70,
            "theme_opportunity_score": 0.70,
            "stock_leadership_score": 0.60,
        },
        {
            "theme_feature_key": "drug-key",
            "theme_code": "DRUG",
            "theme_name": "创新药",
            "theme_cluster_keys": ["INNOVATIVE_DRUG"],
            "theme_composite_score": 0.91,
            "theme_opportunity_score": 0.91,
            "stock_leadership_score": 0.90,
        },
    ]

    forecasts, theme_signals = (
        TradingV3Engine().evaluate_stock_with_theme_signals(
            "300001",
            "多题材样本",
            features,
            NOW,
            NOW + timedelta(days=10),
        )
    )
    weak = next(
        item
        for item in forecasts
        if item.strategy_key == "weak_market_structural_mainline"
    )
    weak_rows = [
        item
        for item in theme_signals
        if item["strategy_key"] == "weak_market_structural_mainline"
    ]

    assert weak.theme_code == "创新药"
    assert {item["theme_feature_key"] for item in weak_rows} == {
        "cloud-key",
        "drug-key",
    }
    assert sum(item["selected_as_primary"] for item in weak_rows) == 1
