from __future__ import annotations

from datetime import date, datetime, timedelta

from sqlalchemy import create_engine, text

from server.trading_v3.context import (
    load_asof_context,
    theme_context_score,
)
from server.trading_v3.domain import (
    AlphaForecast,
    RegimeProbabilities,
)
from server.trading_v3.hypotheses import (
    apply_evidence,
    build_stock_hypotheses,
    strategy_weights_for_regime,
)
from server.trading_v3.order_flow import calculate_order_flow


NOW = datetime(2026, 7, 30, 9, 45)


def _forecast(
    *,
    status: str = "RESEARCH_ONLY_UNCALIBRATED",
    probability: float | None = None,
    score: float = 0.82,
    strategy_key: str = "right_side_trend",
) -> AlphaForecast:
    return AlphaForecast(
        stock_code="002326",
        stock_name="永太科技",
        strategy_key=strategy_key,
        horizon_days=10,
        expected_return_net_pct=2.0 if probability else None,
        return_q10_pct=-2.0 if probability else None,
        return_q50_pct=2.0 if probability else None,
        return_q90_pct=6.0 if probability else None,
        probability_positive=probability,
        expected_mae_pct=-2.0 if probability else None,
        expected_mfe_pct=5.0 if probability else None,
        profit_factor=1.6 if probability else None,
        payoff_ratio=1.5 if probability else None,
        sample_count=120 if probability else 0,
        confidence=0.7 if probability else 0.0,
        status=status,
        feature_time=NOW,
        valid_until=NOW + timedelta(days=10),
        initial_stop_pct=-5.0,
        theme_code="氟化工",
        raw_score=score,
        reasons=("趋势启动",),
        model_version="right_side_trend.v3.0.2-test",
        dataset_hash="dataset",
        features={
            "sector_breadth_pct": 68.0,
            "sector_breadth_acceleration_pct": 12.0,
            "sector_relative_return_pct": 3.0,
            "sector_amount_acceleration_pct": 28.0,
            "relative_strength_20d_pct": 8.0,
            "amount_ratio_1_20": 1.8,
            "distance_ma20_pct": 5.0,
            "stock_leadership_score": 0.82,
        },
    )


def _regime(state: str) -> RegimeProbabilities:
    return RegimeProbabilities(
        probabilities={
            "TREND_UP": 1.0 if state == "TREND_UP" else 0.0,
            "THEME_ROTATION": (
                1.0 if state == "THEME_ROTATION" else 0.0
            ),
            "RANGE": 1.0 if state == "RANGE" else 0.0,
            "PANIC_RECOVERY": (
                1.0 if state == "PANIC_RECOVERY" else 0.0
            ),
            "RISK_OFF": 1.0 if state == "RISK_OFF" else 0.0,
        },
        risk_asset_cap=0.5,
        confidence=1.0,
        quality_status="PASS",
        evidence=("测试市场状态",),
    )


def test_structured_prior_is_not_presented_as_calibrated_win_rate():
    hypothesis = build_stock_hypotheses(
        [_forecast()],
        run_uid="run",
        trade_date=date(2026, 7, 30),
        decision_at=NOW,
        regime=_regime("TREND_UP"),
    )[0]
    assert hypothesis.probability_kind == "STRUCTURED_RESEARCH_PRIOR"
    assert hypothesis.probability <= 0.69
    assert hypothesis.max_position_weight == 0.0


def test_calibrated_forecast_keeps_explicit_probability_kind():
    hypothesis = build_stock_hypotheses(
        [
            _forecast(
                status="VALIDATED_POSITIVE",
                probability=0.68,
            )
        ],
        run_uid="run",
        trade_date=date(2026, 7, 30),
        decision_at=NOW,
        regime=_regime("TREND_UP"),
    )[0]
    assert hypothesis.probability_kind == "OOS_CALIBRATED"
    assert hypothesis.max_position_weight == 0.08


def test_hypothesis_lists_only_sleeves_that_actually_support_signal():
    hypothesis = build_stock_hypotheses(
        [
            _forecast(
                status="PAPER_DISCOVERY_CANDIDATE",
                strategy_key="oversold_reversal",
            ),
            _forecast(
                status="SETUP_NOT_READY",
                score=0.91,
                strategy_key="theme_diffusion",
            ),
            _forecast(
                status="MARKET_REGIME_BLOCKED",
                score=0.88,
                strategy_key="right_side_trend",
            ),
            _forecast(
                status="INSUFFICIENT_DATA",
                score=0.86,
                strategy_key="intraday_surprise",
            ),
        ],
        run_uid="run",
        trade_date=date(2026, 7, 30),
        decision_at=NOW,
        regime=_regime("THEME_ROTATION"),
    )[0]
    assert hypothesis.strategy_keys == ("oversold_reversal",)
    assert "超跌修复" in hypothesis.thesis
    assert "板块扩散" not in hypothesis.thesis
    assert any(
        "右侧主升：市场状态不匹配" == item
        for item in hypothesis.opposing_evidence
    )


def test_positive_intraday_evidence_does_not_trade_research_only_prior():
    hypothesis = build_stock_hypotheses(
        [_forecast()],
        run_uid="run",
        trade_date=date(2026, 7, 30),
        decision_at=NOW,
        regime=_regime("TREND_UP"),
    )[0]
    updated, event = apply_evidence(
        hypothesis,
        observed_at=NOW + timedelta(minutes=15),
        evidence_type="QMT_CONFIRM",
        source="QMT",
        summary="放量并跑赢市场",
        strength=1.2,
        polarity="POSITIVE",
        trigger_confirmed=True,
    )
    assert updated.state == "ACTIVE"
    assert updated.proposed_action == "ALERT_ONLY"
    assert event.probability_after > event.probability_before


def test_strategy_weights_change_with_market_regime():
    trend = strategy_weights_for_regime(_regime("TREND_UP"))
    recovery = strategy_weights_for_regime(
        _regime("PANIC_RECOVERY")
    )
    assert trend["right_side_trend"] > recovery["right_side_trend"]
    assert (
        recovery["oversold_reversal"]
        > trend["oversold_reversal"]
    )


def test_best_bid_ask_order_flow_detects_buying_pressure():
    events = [
        {
            "quote_event_id": "1",
            "quote_at": NOW,
            "bid1": 10.00,
            "bid1_volume": 1000,
            "ask1": 10.01,
            "ask1_volume": 1200,
            "last_price": 10.00,
            "source_provider": "QMT",
        },
        {
            "quote_event_id": "2",
            "quote_at": NOW + timedelta(seconds=5),
            "bid1": 10.01,
            "bid1_volume": 2600,
            "ask1": 10.02,
            "ask1_volume": 800,
            "last_price": 10.02,
            "source_provider": "QMT",
        },
    ]
    result = calculate_order_flow(events)
    assert result["quality_status"] == "PASS"
    assert result["ofi_normalized"] > 0
    assert result["queue_imbalance"] > 0


def test_context_deduplicates_and_structures_policy_theme_and_overseas():
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
                    first_seen_at DATETIME,
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
                INSERT INTO st_news_flash
                    (id, source, title, content, publish_time,
                     first_seen_at, level, is_top, jpush)
                VALUES
                    (1, '国家发改委',
                     '支持特高压电网建设',
                     '加快设备更新和电网投资',
                     '2026-07-30 08:00:00', '2026-07-30 08:00:10', 'A', 1, 1),
                    (2, '国家发改委',
                     '支持特高压电网建设',
                     '加快设备更新和电网投资',
                     '2026-07-30 08:01:00', '2026-07-30 08:01:10', 'A', 1, 1),
                    (3, '财联社',
                     '美股暴跌',
                     '纳指下跌，市场出现流动性风险',
                     '2026-07-30 07:30:00', '2026-07-30 07:30:10', 'A', 1, 1),
                    (4, '未来消息',
                     '支持人工智能',
                     '该消息在决策之后发布',
                     '2026-07-30 10:30:00', '2026-07-30 10:30:10', 'A', 1, 1),
                    (5, 'late_ingest',
                     'historically backfilled headline',
                     'published before the decision but only ingested later',
                     '2026-07-30 08:30:00', '2026-07-30 10:30:00', 'A', 1, 1)
                """
            )
        )
    result = load_asof_context(
        engine,
        as_of=date(2026, 7, 30),
        cutoff_at=datetime(2026, 7, 30, 9, 15),
    )
    assert result["context_news_count"] == 3
    assert result["context_unique_event_count"] == 2
    assert result["policy_support_score"] > 0
    assert result["news_risk_score"] > 0
    assert result["overseas_risk_score"] > 0
    assert result["context_theme_scores"]["电力与电网"] > 0
    assert (
        theme_context_score(
            "特高压",
            result["context_theme_scores"],
        )
        > 0
    )
    assert len(result["context_hash"]) == 64
    assert result["context_evidence_status"] == "POINT_IN_TIME_VERIFIED"
    assert result["context_knowledge_time_column"] == "first_seen_at"


def test_ai_and_robot_news_context_are_independent():
    scores = {
        "人工智能": 0.8,
        "机器人": -0.6,
    }

    assert theme_context_score("AI应用 大模型", scores) == 0.8
    assert theme_context_score("机器人 具身智能", scores) == -0.6
