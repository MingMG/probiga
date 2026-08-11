from __future__ import annotations

from sqlalchemy import create_engine, text

from biz.analysis.sync_analysis_fast import build_research_theme_features
from biz.early_briefing.generate import build_user_prompt
from biz.research_radar.radar import (
    build_research_radar,
    classify_news_catalysts,
    format_radar_markdown,
)


def _radar_engine():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE st_news_flash (
                    id INTEGER PRIMARY KEY,
                    source TEXT,
                    title TEXT,
                    content TEXT,
                    publish_time DATETIME,
                    level TEXT,
                    is_top INTEGER,
                    jpush INTEGER
                )
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TABLE st_hot_concept_ths_daily (
                    concept_name TEXT,
                    change_pct REAL,
                    hot_value REAL,
                    plate_type TEXT,
                    rank INTEGER,
                    snapshot_date DATE
                )
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TABLE st_hot_rank_fused (
                    stock_code TEXT,
                    short_name TEXT,
                    change_pct REAL,
                    fused_rank INTEGER,
                    snapshot_date DATE
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO st_news_flash
                    (id, source, title, content, publish_time, level, is_top, jpush)
                VALUES
                    (1, '财联社', '日本熊本地震致多家半导体工厂停产',
                     '市场担忧材料供应中断', '2026-07-30 08:00:00', 'A', 1, 1),
                    (2, '财联社', '三星电机宣布MLCC产品价格上调30%',
                     'AI服务器需求增长', '2026-07-30 07:50:00', 'A', 1, 1),
                    (3, '央视', '安徽卫视首次播出AI电视剧',
                     'AI内容生产进入卫视播出场景', '2026-07-30 07:40:00', 'A', 1, 1),
                    (4, '财联社', '海外云厂商资本开支下调',
                     '光模块长协价无法上涨且需求不及预期', '2026-07-30 07:30:00', 'A', 1, 1),
                    (5, '财联社', '某新行业签署重大订单',
                     '合同金额创历史新高', '2026-07-30 07:20:00', 'A', 1, 1)
                """
            )
        )
    return engine


def test_classification_covers_user_examples():
    earthquake = classify_news_catalysts("日本熊本地震致多家半导体工厂停产")
    mlcc = classify_news_catalysts("三星电机宣布MLCC产品价格上调30%")
    ai_drama = classify_news_catalysts("安徽卫视首次播出AI电视剧")

    assert "semi_materials_japan" in earthquake["theme_ids"]
    assert "SUPPLY_DISRUPTION" in earthquake["trigger_types"]
    assert "mlcc_passive" in mlcc["theme_ids"]
    assert "PRICE_INCREASE" in mlcc["trigger_types"]
    assert "ai_apps" in ai_drama["theme_ids"]
    assert "TECH_PRODUCT" in ai_drama["trigger_types"]


def test_radar_keeps_all_themes_and_unclassified_catalysts():
    radar = build_research_radar(_radar_engine(), "2026-07-30")
    theme_map = {theme["id"]: theme for theme in radar["themes"]}

    assert radar["coverage_summary"]["scanned_theme_count"] == len(radar["themes"])
    assert len(radar["themes"]) >= 15
    assert theme_map["semi_materials_japan"]["active"] is True
    assert theme_map["mlcc_passive"]["active"] is True
    assert theme_map["ai_apps"]["active"] is True
    assert theme_map["ai_compute"]["status"] == "逻辑转弱"
    assert any("某新行业签署重大订单" in item["title"] for item in radar["unclassified_catalysts"])

    markdown = format_radar_markdown(radar)
    assert "对日半导体材料替代" in markdown
    assert "MLCC与被动元件涨价" in markdown
    assert "AI应用、软件与内容" in markdown
    assert "高股息与防御资产" in markdown


def test_early_briefing_requires_full_market_pool_before_top_lines():
    prompt = build_user_prompt({}, [])

    assert "全市场催化候选池（不得省略）" in prompt
    assert "不得因为排名靠后而删除" in prompt
    assert "暂无合格标的" in prompt
    assert "再选 2-3 条最高优先级主线" in prompt
    assert "今天最核心 2-3 条主线" not in prompt


def test_weakening_theme_is_visible_without_positive_stock_bonus():
    features = build_research_theme_features(
        [
            {
                "id": "ai_compute",
                "name": "AI海外链",
                "score": 92,
                "status": "逻辑转弱",
                "stocks": [
                    {
                        "code": "300308",
                        "name": "中际旭创",
                        "role": "光模块",
                        "tier": "核心验证",
                    }
                ],
            }
        ]
    )

    assert len(features) == 1
    assert float(features.iloc[0]["research_theme_score"]) <= 50
