from __future__ import annotations

import copy
import json
from dataclasses import replace
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from server.trading_v3.backtest import _dynamic_signal_outcome
from server.trading_v3.calibration import fit_calibration
from server.trading_v3.config import load_v3_config
from server.trading_v3.daily_features import (
    _load_finance,
    _theme_context_label,
)
from server.trading_v3.domain import (
    AlphaForecast,
    ConsensusForecast,
    PortfolioDecision,
    RegimeProbabilities,
)
from server.trading_v3.exit_policy import daily_exit_reason
from server.trading_v3.engine import TradingV3Engine
from server.trading_v3.portfolio import (
    add_paper_discovery_targets,
    optimize_retail_portfolio,
)
from server.trading_v3 import paper_execution
from server.trading_v3.repository import (
    TradingV3Repository,
    _select_theme_signal_evidence,
)
from server.trading_v3.right_side_policy import (
    right_side_model_contract_hash,
)
from server.trading_v3.sleeves import _feature_snapshot
from server.trading_v3.theme_features import (
    attach_best_theme,
    paper_research_groups,
)
from server.trading_v3.versioning import code_version
from tools.register_trading_v3_artifact import _verify_profit_gate


def _theme_evidence_row(
    stock_code: str,
    strategy_key: str,
    theme_code: str,
    score: float,
    *,
    selected: bool = False,
) -> dict[str, object]:
    return {
        "stock_code": stock_code,
        "strategy_key": strategy_key,
        "theme_code": theme_code,
        "theme_name": theme_code,
        "theme_feature_key": f"{stock_code}-{strategy_key}-{theme_code}",
        "raw_score": score,
        "selected_as_primary": int(selected),
    }


def test_theme_evidence_selection_covers_every_exact_theme_strategy() -> None:
    rows = [
        _theme_evidence_row(code, strategy, theme, score)
        for theme in ("cloud", "defense", "medicine")
        for strategy in (
            "theme_diffusion",
            "weak_market_structural_mainline",
        )
        for code, score in (("000001", 0.9), ("000002", 0.8))
    ]
    rows.append(
        _theme_evidence_row(
            "000003",
            "theme_diffusion",
            "cloud",
            0.1,
            selected=True,
        )
    )
    forecast_keys = {
        (str(item["stock_code"]), str(item["strategy_key"]))
        for item in rows
    }

    selected, summary = _select_theme_signal_evidence(
        rows,
        forecast_keys=forecast_keys,
        maximum_rows=20,
        top_k_per_theme_strategy=1,
    )

    covered = {
        (str(item["strategy_key"]), str(item["theme_code"]))
        for item in selected
    }
    assert covered == {
        (strategy, theme)
        for strategy in (
            "theme_diffusion",
            "weak_market_structural_mainline",
        )
        for theme in ("cloud", "defense", "medicine")
    }
    assert any(
        item["stock_code"] == "000003"
        and item["selected_as_primary"] == 1
        for item in selected
    )
    assert summary["evaluated_count"] == len(rows)
    assert summary["exact_theme_strategy_group_count"] == 6


def test_theme_evidence_selection_fails_if_cap_would_hide_theme() -> None:
    rows = [
        _theme_evidence_row(
            "000001",
            "theme_diffusion",
            theme,
            0.5,
        )
        for theme in ("cloud", "defense", "medicine")
    ]
    with pytest.raises(RuntimeError, match="cannot cover every exact theme"):
        _select_theme_signal_evidence(
            rows,
            forecast_keys={("000001", "theme_diffusion")},
            maximum_rows=2,
            top_k_per_theme_strategy=1,
        )


def _consensus(
    code: str,
    *,
    theme: str = "主主题",
    related: tuple[str, ...] = ("相关簇",),
) -> ConsensusForecast:
    return ConsensusForecast(
        stock_code=code,
        stock_name=code,
        expected_return_net_pct=4.0,
        conservative_return_pct=3.0,
        probability_positive=0.65,
        expected_mae_pct=3.0,
        profit_factor=1.6,
        payoff_ratio=1.5,
        confidence=0.8,
        selection_score=0.8,
        strategy_keys=("right_side_trend",),
        primary_strategy_key="right_side_trend",
        theme_code=theme,
        initial_stop_pct=-5.0,
        evidence=("测试正期望",),
        theme_codes=related,
    )


def _regime(cap: float = 0.75) -> RegimeProbabilities:
    return RegimeProbabilities(
        probabilities={"TREND_UP": 1.0},
        risk_asset_cap=cap,
        confidence=1.0,
        quality_status="PASS",
        evidence=(),
    )


def _paper_forecast(
    code: str,
    *,
    score: float,
    theme_codes: tuple[str, ...],
    research_groups: tuple[str, ...] = (),
) -> AlphaForecast:
    now = datetime(2026, 7, 31, 15, 0)
    return AlphaForecast(
        stock_code=code,
        stock_name=code,
        strategy_key="oversold_reversal",
        horizon_days=5,
        expected_return_net_pct=None,
        return_q10_pct=None,
        return_q50_pct=None,
        return_q90_pct=None,
        probability_positive=None,
        expected_mae_pct=None,
        expected_mfe_pct=None,
        profit_factor=None,
        payoff_ratio=None,
        sample_count=0,
        confidence=0.0,
        status="PAPER_DISCOVERY_CANDIDATE",
        feature_time=now,
        valid_until=now + timedelta(days=5),
        initial_stop_pct=-6.0,
        theme_code=theme_codes[0],
        raw_score=score,
        reasons=(),
        features={
            "theme_codes": list(theme_codes),
            "paper_research_groups": list(research_groups),
        },
    )


def test_data_block_keeps_full_market_theme_radar_visible():
    forecast = _paper_forecast(
        "300609",
        score=0.91,
        theme_codes=("AI_THEME",),
        research_groups=("AI_APP",),
    )
    result = TradingV3Engine().decide(
        [forecast],
        market_features={
            "market_return_20d_pct": 1.0,
            "market_breadth_pct": 50.0,
            "breadth_change_5d_pct": 0.0,
            "realized_volatility_20d_pct": 2.0,
            "limit_down_ratio_pct": 0.0,
            "sector_concentration_pct": 20.0,
            "qmt_attestation_current": False,
            "qmt_attestation_status": "MISSING",
        },
        prices={"300609": 20.0},
        equity=200_000.0,
        opportunity_audit_forecasts=[forecast],
    )

    portfolio = result["portfolio"]
    audit = portfolio["opportunity_audit"]
    assert portfolio["status"] == "DATA_BLOCKED"
    assert audit["forecast_count"] == 1
    assert audit["dynamic_theme_radar"][0]["theme"] == "AI_THEME"
    groups = {item["group"] for item in audit["research_groups"]}
    assert "AI_APP" in groups
    assert "DATA_QUALITY_BLOCKED" in audit["warnings"]
    assert audit["selection_blocked_by_data_quality"] == [
        "QMT_DAILY_KLINE_ATTESTATION_MISSING"
    ]


def test_theme_groups_keep_ai_and_robot_labels_independent():
    assert paper_research_groups([
        "人工智能",
        "机器人概念",
        "计算机",
    ]) == ["AI应用", "机器人"]


def test_news_context_uses_secondary_theme_labels():
    label = _theme_context_label({
        "theme_code": "计算机",
        "theme_name": "计算机",
        "theme_names": ["计算机", "人工智能", "AI应用"],
    })

    assert "人工智能" in label
    assert "AI应用" in label


def test_theme_labels_survive_when_new_theme_has_no_statistics_yet():
    base = {"300001": {}}
    attach_best_theme(
        base,
        memberships={
            "300001": [("NEW_AI", "人工智能新概念", "concept")],
        },
        statistics={},
    )

    assert base["300001"]["theme_codes"] == ["NEW_AI"]
    assert base["300001"]["theme_names"] == ["人工智能新概念"]
    assert base["300001"]["paper_research_groups"] == ["AI应用"]


def test_signal_snapshot_preserves_paper_research_groups():
    snapshot = _feature_snapshot({
        "theme_names": ["人工智能", "机器人概念"],
        "paper_research_groups": ["AI应用", "机器人"],
    })

    assert snapshot["paper_research_groups"] == ["AI应用", "机器人"]


def test_paper_discovery_lot_adaptation_keeps_top_ranked_research():
    forecasts = [
        _paper_forecast(
            "002380",
            score=0.926,
            theme_codes=("国产软件", "机器人概念"),
            research_groups=("机器人",),
        ),
        _paper_forecast(
            "600589",
            score=0.921,
            theme_codes=("国产软件", "云计算"),
        ),
        _paper_forecast(
            "300609",
            score=0.889,
            theme_codes=("人工智能",),
            research_groups=("AI应用",),
        ),
    ]
    empty = PortfolioDecision(
        targets=(),
        rejected=(),
        target_cash=200_000.0,
        target_risk_asset_weight=0.0,
        expected_portfolio_return_pct=0.0,
        worst_case_loss_cny=0.0,
        status="CASH_OR_ETF_PREFERRED",
    )
    result = add_paper_discovery_targets(
        empty,
        forecasts,
        prices={
            "002380": 27.78,
            "600589": 7.38,
            "300609": 24.58,
        },
        equity=200_000.0,
        current_theme_weights={},
        regime=_regime(),
    )
    assert [item.stock_code for item in result.targets] == [
        "002380",
        "600589",
        "300609",
    ]
    assert result.targets[0].target_quantity == 100
    assert result.targets[0].target_value == 2_778.0
    assert "robotics_research" not in result.targets[0].strategy_keys
    assert "oversold_reversal" in result.targets[0].strategy_keys
    assert result.targets[2].target_quantity == 200
    assert result.targets[2].target_value == 4_916.0
    audit = result.opportunity_audit
    assert audit["unexplained_unselected_count"] == 0
    assert "TARGET_THEME_CONCENTRATION" not in audit["warnings"]
    groups = {item["group"]: item for item in audit["research_groups"]}
    assert groups["机器人"]["status"] == "COVERED"
    assert groups["AI应用"]["status"] == "COVERED"
    assert audit["scope"] == "ALL_DECISION_FORECASTS"
    assert audit["universe_stock_count"] == 3
    assert audit["forecast_count"] == 3
    assert groups["AI应用"]["universe_stock_count"] == 1
    assert groups["AI应用"]["forecast_count"] == 1
    assert groups["AI应用"]["top_signal"]["stock_code"] == "300609"


def test_theme_and_trend_joint_signal_can_enter_paper_research():
    base = _paper_forecast(
        "300609",
        score=0.91,
        theme_codes=("AI_THEME",),
        research_groups=("AI应用",),
    )
    forecasts = [
        replace(
            base,
            strategy_key="theme_diffusion",
            status="RESEARCH_ONLY_UNCALIBRATED",
            raw_score=0.91,
        ),
        replace(
            base,
            strategy_key="right_side_trend",
            status="RESEARCH_ONLY_CALIBRATION_DIRECTION_FAILED",
            raw_score=0.86,
        ),
    ]
    empty = PortfolioDecision(
        targets=(),
        rejected=(),
        target_cash=200_000.0,
        target_risk_asset_weight=0.0,
        expected_portfolio_return_pct=0.0,
        worst_case_loss_cny=0.0,
        status="CASH_OR_ETF_PREFERRED",
    )
    result = add_paper_discovery_targets(
        empty,
        forecasts,
        prices={"300609": 20.0},
        equity=200_000.0,
        current_theme_weights={},
        regime=_regime(),
    )
    assert len(result.targets) == 1
    assert set(result.targets[0].strategy_keys) == {
        "paper_discovery",
        "right_side_trend",
        "theme_diffusion",
    }


def test_high_score_theme_radar_warns_even_without_paper_candidate():
    forecast = replace(
        _paper_forecast(
            "300609",
            score=0.91,
            theme_codes=("AI_THEME",),
            research_groups=("AI应用",),
        ),
        status="SETUP_NOT_READY",
    )
    empty = PortfolioDecision(
        targets=(),
        rejected=(),
        target_cash=200_000.0,
        target_risk_asset_weight=0.0,
        expected_portfolio_return_pct=0.0,
        worst_case_loss_cny=0.0,
        status="CASH_OR_ETF_PREFERRED",
    )
    result = add_paper_discovery_targets(
        empty,
        [forecast],
        prices={"300609": 20.0},
        equity=200_000.0,
        current_theme_weights={},
        regime=_regime(),
    )
    audit = result.opportunity_audit
    ai_group = next(
        item for item in audit["research_groups"]
        if item["group"] == "AI应用"
    )
    assert ai_group["candidate_count"] == 0
    assert ai_group["status"] == "HIGH_SCORE_UNSELECTED"
    assert "HIGH_SCORE_RESEARCH_GROUP_UNSELECTED:AI应用" in (
        audit["warnings"]
    )


def test_dynamic_theme_alerts_cover_all_themes_and_define_candidates():
    forecasts = [
        replace(
            _paper_forecast(
                f"{300000 + index:06d}",
                score=0.95 - index * 0.001,
                theme_codes=(f"未知题材{index:02d}",),
            ),
            status="SETUP_NOT_READY",
        )
        for index in range(25)
    ]
    empty = PortfolioDecision(
        targets=(),
        rejected=(),
        target_cash=200_000.0,
        target_risk_asset_weight=0.0,
        expected_portfolio_return_pct=0.0,
        worst_case_loss_cny=0.0,
        status="CASH_OR_ETF_PREFERRED",
    )

    result = add_paper_discovery_targets(
        empty,
        forecasts,
        prices={item.stock_code: 20.0 for item in forecasts},
        equity=200_000.0,
        current_theme_weights={},
        regime=_regime(),
    )

    audit = result.opportunity_audit
    detail = audit["warning_details"][
        "high_score_dynamic_theme_unselected"
    ]
    assert audit["candidate_count"] == 0
    assert audit["candidate_definition"] == (
        "UNIQUE_STOCKS_PASSING_PAPER_DISCOVERY_SIGNAL_GATES_"
        "BEFORE_PORTFOLIO_CONSTRAINTS"
    )
    assert detail["count"] == 25
    assert detail["emitted_warning_count"] == 20
    assert detail["truncated_warning_count"] == 5
    assert len(detail["items"]) == 25
    assert {item["theme"] for item in detail["items"]} == {
        f"未知题材{index:02d}" for index in range(25)
    }
    assert all(item["candidate_count"] == 0 for item in detail["items"])
    assert all(
        item["candidate_count"] == 0
        and item["top_candidate"] is None
        for item in audit["dynamic_theme_radar"]
    )
    assert "HIGH_SCORE_DYNAMIC_THEME_UNSELECTED_COUNT:25" in (
        audit["warnings"]
    )
    assert "HIGH_SCORE_DYNAMIC_THEME_WARNING_ROWS_TRUNCATED:5" in (
        audit["warnings"]
    )
    assert "HIGH_SCORE_DYNAMIC_THEME_UNSELECTED:未知题材19" in (
        audit["warnings"]
    )
    assert "HIGH_SCORE_DYNAMIC_THEME_UNSELECTED:未知题材24" not in (
        audit["warnings"]
    )


def test_existing_paper_position_consumes_count_and_weight_capacity():
    forecasts = [
        _paper_forecast(
            "002380",
            score=0.926,
            theme_codes=("国产软件", "机器人概念"),
        ),
        _paper_forecast(
            "600589",
            score=0.921,
            theme_codes=("云计算",),
        ),
        _paper_forecast(
            "300609",
            score=0.889,
            theme_codes=("人工智能",),
        ),
    ]
    portfolio = PortfolioDecision(
        targets=(),
        rejected=(),
        target_cash=195_000.0,
        target_risk_asset_weight=0.025,
        expected_portfolio_return_pct=0.0,
        worst_case_loss_cny=300.0,
        status="CASH_OR_ETF_PREFERRED",
    )
    result = add_paper_discovery_targets(
        portfolio,
        forecasts,
        prices={
            "002380": 25.0,
            "600589": 10.0,
            "300609": 25.0,
        },
        equity=200_000.0,
        current_theme_weights={"人工智能": 0.025},
        current_position_weights={"300609": 0.025},
        current_position_quantities={"300609": 200},
        current_position_themes={"300609": ("人工智能",)},
        current_paper_discovery_codes={"300609"},
        regime=_regime(),
    )

    assert {item.stock_code for item in result.targets} == {
        "002380",
        "600589",
        "300609",
    }
    assert len(result.targets) == 3
    retained = next(
        item for item in result.targets if item.stock_code == "300609"
    )
    assert retained.target_quantity == 200
    assert sum(item.target_weight for item in result.targets) <= 0.20
    assert result.estimated_one_way_turnover_weight == 0.05


def test_forecast_numeric_search_does_not_match_json_decimals():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_decision_run_v3 (
                run_uid TEXT PRIMARY KEY,
                trade_date TEXT,
                decision_at TEXT,
                created_at TEXT,
                regime_json TEXT,
                portfolio_json TEXT
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_alpha_forecast_v3 (
                run_uid TEXT,
                rank_no INTEGER,
                stock_code TEXT,
                short_name TEXT,
                strategy_key TEXT,
                forecast_status TEXT,
                theme_code TEXT,
                reasons_json TEXT,
                features_json TEXT
            )
        """))
        connection.execute(text("""
            INSERT INTO st_decision_run_v3 VALUES (
                'run-1', '2026-07-31', '2026-08-01 12:00:00',
                '2026-08-01 12:00:00', '{}', '{}'
            )
        """))
        connection.execute(
            text("""
                INSERT INTO st_alpha_forecast_v3 VALUES
                ('run-1', 1, '300609', '汇纳科技', 'oversold_reversal',
                 'PAPER_DISCOVERY_CANDIDATE', '计算机', '[]', :ai),
                ('run-1', 2, '000001', '平安银行', 'oversold_reversal',
                 'SETUP_NOT_READY', '银行', '[]', :decimal)
            """),
            {
                "ai": json.dumps(
                    {"paper_research_groups": ["AI应用"]},
                    ensure_ascii=False,
                ),
                "decimal": json.dumps({"noise": 0.300609}),
            },
        )
    repository = TradingV3Repository(engine)

    numeric = repository.latest_forecasts(query="300609")
    thematic = repository.latest_forecasts(query="AI应用")

    assert [item["stock_code"] for item in numeric] == ["300609"]
    assert [item["stock_code"] for item in thematic] == ["300609"]


def test_candidate_frontend_keeps_theme_search_results_visible():
    script = (
        Path(__file__).resolve().parents[1]
        / "server"
        / "static"
        / "js"
        / "trading-v3.js"
    ).read_text(encoding="utf-8")

    assert "themeText(x).toLowerCase().indexOf(q)>=0" in script


def test_calibration_rejects_extrapolation_but_interpolates_inside_support():
    table = fit_calibration(
        "right_side_trend",
        [
            {
                "score": 0.70 + bucket * 0.05,
                "net_return_pct": 2.0,
                "mae_pct": -1.0,
                "mfe_pct": 3.0,
            }
            for bucket in range(5)
            for _ in range(100)
        ],
        model_version="right_side_trend.v3.4.1-test",
        bucket_count=5,
    )
    assert table.bucket_for(0.775) is not None
    assert table.bucket_for(0.69) is None
    assert table.bucket_for(0.91) is None


def test_right_side_model_contract_binds_daily_top_n():
    config = load_v3_config()
    changed = copy.deepcopy(config)
    changed["calibration"]["top_per_day"] += 1

    assert right_side_model_contract_hash(config) != (
        right_side_model_contract_hash(changed)
    )


def test_portfolio_gate_rejects_signal_pass_with_weak_portfolio_pf():
    payload = {
        "gate_status": "PASS",
        "block_reasons": [],
        "validation_metrics": {
            "sample_count": 840,
            "net_expectancy_pct": 1.8,
            "profit_factor": 1.57,
            "payoff_ratio": 1.63,
        },
        "portfolio_metrics": {
            "trade_count": 120,
            "net_expectancy_pct": 0.8,
            "profit_factor": 1.18,
            "payoff_ratio": 1.5,
            "maximum_drawdown_pct": 10.0,
            "net_profit_cny": 5_000,
        },
    }
    with pytest.raises(
        RuntimeError,
        match="PORTFOLIO_PROFIT_FACTOR_TOO_LOW",
    ):
        _verify_profit_gate(payload, load_v3_config())


def test_optimizer_subtracts_existing_risk_asset_exposure():
    result = optimize_retail_portfolio(
        [_consensus("000001")],
        prices={"000001": 10.0},
        equity=200_000.0,
        current_theme_weights={},
        current_position_weights={"600000": 0.74},
        current_position_quantities={"600000": 14_800},
        current_position_themes={"600000": ("银行",)},
        regime=_regime(0.75),
    )
    assert not result.targets
    assert result.target_risk_asset_weight == pytest.approx(0.74)


def test_optimizer_enforces_correlated_theme_cap():
    result = optimize_retail_portfolio(
        [_consensus("000001", theme="新主题")],
        prices={"000001": 10.0},
        equity=200_000.0,
        current_theme_weights={"旧主题": 0.34},
        current_position_weights={"600000": 0.34},
        current_position_quantities={"600000": 6_800},
        current_position_themes={"600000": ("相关簇",)},
        regime=_regime(),
    )
    assert not result.targets
    assert any(
        item["reason_code"] == "ORDER_NOT_ECONOMIC"
        for item in result.rejected
    )


def test_optimizer_caps_every_theme_membership_not_only_primary():
    result = optimize_retail_portfolio(
        [
            _consensus(
                "000001",
                theme="primary-theme",
                related=("secondary-theme",),
            )
        ],
        prices={"000001": 10.0},
        equity=200_000.0,
        current_theme_weights={"secondary-theme": 0.25},
        regime=_regime(),
    )
    assert not result.targets
    assert result.rejected[0]["reason_code"] == (
        "THEME_OR_RISK_BUDGET_FULL"
    )


def test_optimizer_never_exceeds_daily_turnover_cap():
    result = optimize_retail_portfolio(
        [_consensus(f"{index:06d}") for index in range(1, 7)],
        prices={f"{index:06d}": 10.0 for index in range(1, 7)},
        equity=200_000.0,
        current_theme_weights={},
        regime=_regime(),
    )
    assert result.targets
    assert result.estimated_one_way_turnover_weight <= 0.30


def test_dynamic_label_uses_production_stop_and_real_order_cost():
    dates = pd.date_range("2026-01-05", periods=4, freq="B")
    group = pd.DataFrame([
        {
            "trade_date": dates[0],
            "open": 10.0,
            "high": 10.2,
            "low": 9.9,
            "close": 10.0,
            "amount": 100_000_000,
            "ma20": 9.0,
            "close_above_ma20": 1.0,
            "ma20_above_ma60": 1.0,
            "score": 0.8,
            "initial_stop_pct": -5.0,
        },
        {
            "trade_date": dates[1],
            "open": 10.0,
            "high": 10.1,
            "low": 9.8,
            "close": 10.0,
            "amount": 100_000_000,
            "ma20": 9.0,
            "close_above_ma20": 1.0,
            "ma20_above_ma60": 1.0,
            "score": 0.8,
            "initial_stop_pct": -5.0,
        },
        {
            "trade_date": dates[2],
            "open": 10.0,
            "high": 10.0,
            "low": 9.0,
            "close": 9.4,
            "amount": 100_000_000,
            "ma20": 9.0,
            "close_above_ma20": 1.0,
            "ma20_above_ma60": 1.0,
            "score": 0.8,
            "initial_stop_pct": -5.0,
        },
        {
            "trade_date": dates[3],
            "open": 9.3,
            "high": 9.5,
            "low": 9.1,
            "close": 9.2,
            "amount": 100_000_000,
            "ma20": 9.0,
            "close_above_ma20": 1.0,
            "ma20_above_ma60": 1.0,
            "score": 0.8,
            "initial_stop_pct": -5.0,
        },
    ])
    outcome = _dynamic_signal_outcome(
        group,
        signal_index=0,
        config=load_v3_config(),
    )
    assert outcome is not None
    assert outcome["exit_reason"] == "HARD_STOP"
    assert outcome["exit_date"] == dates[3]
    assert outcome["net_return_pct"] < -5.0
    assert outcome["label_order_value_cny"] == 10_000.0


def test_right_censored_signal_remains_a_portfolio_candidate():
    dates = pd.date_range("2026-01-05", periods=4, freq="B")
    group = pd.DataFrame([
        {
            "trade_date": trade_date,
            "open": 10.0,
            "high": 10.2,
            "low": 9.9,
            "close": 10.1,
            "amount": 100_000_000,
            "close_above_ma20": 1.0,
            "ma20_above_ma60": 1.0,
            "score": 0.8,
            "initial_stop_pct": -5.0,
        }
        for trade_date in dates
    ])

    outcome = _dynamic_signal_outcome(
        group,
        signal_index=0,
        config=load_v3_config(),
        include_censored=True,
    )

    assert outcome is not None
    assert outcome["label_mature"] is False
    assert outcome["exit_reason"] == "RIGHT_CENSORED"
    assert pd.isna(outcome["net_return_pct"])


def test_dynamic_label_matches_single_session_production_order_expiry():
    dates = pd.date_range("2026-01-05", periods=6, freq="B")
    rows = [
        {
            "trade_date": dates[0], "open": 10.0, "high": 10.1,
            "low": 9.9, "close": 10.0, "amount": 1e8,
            "close_above_ma20": 1.0, "ma20_above_ma60": 1.0,
            "initial_stop_pct": -5.0,
        },
        {
            "trade_date": dates[1], "open": 11.0, "high": 11.0,
            "low": 11.0, "close": 11.0, "amount": 1e8,
            "close_above_ma20": 1.0, "ma20_above_ma60": 1.0,
            "initial_stop_pct": -5.0,
        },
        {
            "trade_date": dates[2], "open": 10.8, "high": 10.9,
            "low": 10.7, "close": 10.8, "amount": 1e8,
            "close_above_ma20": 1.0, "ma20_above_ma60": 1.0,
            "initial_stop_pct": -5.0,
        },
        {
            "trade_date": dates[3], "open": 10.7, "high": 10.8,
            "low": 10.0, "close": 10.1, "amount": 1e8,
            "close_above_ma20": 1.0, "ma20_above_ma60": 1.0,
            "initial_stop_pct": -5.0,
        },
        {
            "trade_date": dates[4], "open": 9.09, "high": 9.09,
            "low": 9.09, "close": 9.09, "amount": 1e8,
            "close_above_ma20": 0.0, "ma20_above_ma60": 1.0,
            "initial_stop_pct": -5.0,
        },
        {
            "trade_date": dates[5], "open": 9.2, "high": 9.3,
            "low": 9.1, "close": 9.2, "amount": 1e8,
            "close_above_ma20": 0.0, "ma20_above_ma60": 1.0,
            "initial_stop_pct": -5.0,
        },
    ]
    outcome = _dynamic_signal_outcome(
        pd.DataFrame(rows),
        signal_index=0,
        config=load_v3_config(),
    )
    assert outcome is None


def test_canonical_daily_exit_policy_does_not_assume_intraday_fill():
    reason = daily_exit_reason(
        protective_stop=9.5,
        session_low=9.0,
        close_above_ma20=1.0,
        ma20_above_ma60=1.0,
    )
    assert reason == "HARD_STOP"


def test_left_side_discovery_does_not_require_right_side_ma_alignment():
    reason = daily_exit_reason(
        protective_stop=8.0,
        session_low=9.0,
        close_above_ma20=0.0,
        ma20_above_ma60=0.0,
        require_trend_alignment=False,
    )
    assert reason is None


def test_v3_code_provenance_never_falls_back_to_worktree(monkeypatch):
    monkeypatch.delenv("PROBIGA_BUILD_COMMIT_SHA", raising=False)
    version, source = code_version()
    assert len(version) == 64
    assert source == "source_artifact_sha256"


def test_finance_loader_keeps_missing_values_missing():
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value
    connection.execute.return_value.mappings.return_value.all.return_value = [{
        "stock_code": "000001",
        "net_asset_ps": 10.0,
        "oper_cf_ps": None,
        "total_rev_yoy_gr": 5.0,
        "net_profit_yoy_gr": 4.0,
        "roe_wtd": 8.0,
        "gross_margin": 20.0,
        "net_margin": 10.0,
        "cash_flow_ratio": None,
        "asset_liab_ratio": 60.0,
    }]
    result = _load_finance(
        engine,
        as_of=date(2026, 7, 30),
        codes=["000001"],
    )
    assert result["000001"]["oper_cf_ps"] is None
    assert result["000001"]["cash_flow_ratio"] is None
    statement = str(connection.execute.call_args.args[0])
    assert "report_date <= :as_of" in statement
    assert "notice_date >= report_date" in statement
    assert "f.report_date <= :as_of" in statement
    assert "f.notice_date >= f.report_date" in statement


def test_theme_attachment_preserves_every_asof_membership():
    base = {
        "000001": {
            "return_5d_pct": 8.0,
            "amount_ratio_5_20": 1.5,
            "breakout_20d_proximity": 0.95,
            "latest_change_pct": 2.0,
        }
    }
    memberships = {
        "000001": [
            ("主题A", "主题A", "concept"),
            ("主题B", "主题B", "concept"),
        ]
    }
    common = {
        "theme_source": "concept",
        "member_count": 20,
        "sector_return_5d_pct": 3.0,
        "theme_opportunity_score": 0.8,
        "sector_breadth_pct": 60.0,
        "sector_breadth_prior_pct": 50.0,
        "sector_breadth_3d_prior_pct": 45.0,
        "sector_breadth_acceleration_pct": 10.0,
        "sector_relative_return_pct": 2.0,
        "sector_amount_acceleration_pct": 20.0,
        "sector_leadership_depth": 0.7,
        "sector_crowding": 0.2,
    }
    statistics = {
        "主题A": {
            **common,
            "theme_code": "主题A",
            "theme_name": "主题A",
        },
        "主题B": {
            **common,
            "theme_code": "主题B",
            "theme_name": "主题B",
            "theme_opportunity_score": 0.7,
        },
    }
    attach_best_theme(
        base,
        memberships=memberships,
        statistics=statistics,
    )
    assert base["000001"]["theme_code"] == "主题A"
    assert base["000001"]["theme_codes"] == ["主题A", "主题B"]


def test_runtime_rejects_active_model_from_another_config():
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        connection.execute(text(
            """
            CREATE TABLE st_model_registry_v3 (
                strategy_key TEXT,
                model_version TEXT,
                lifecycle_status TEXT,
                dataset_hash TEXT,
                feature_schema_hash TEXT,
                calibration_json TEXT,
                metrics_json TEXT,
                config_json TEXT,
                activated_at TEXT,
                created_at TEXT
            )
            """
        ))
    table = fit_calibration(
        "right_side_trend",
        [
            {
                "score": 0.70 + bucket * 0.05,
                "net_return_pct": 2.0,
                "mae_pct": -1.0,
                "mfe_pct": 3.0,
            }
            for bucket in range(5)
            for _ in range(100)
        ],
        model_version="right_side_trend.v3.4.1-test",
        bucket_count=5,
    )
    wrong_config = dict(load_v3_config())
    wrong_config["strategy_version"] = "trading_v3.2.0-paper"
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO st_model_registry_v3 (
                    strategy_key, model_version, lifecycle_status,
                    dataset_hash, feature_schema_hash,
                    calibration_json, metrics_json, config_json,
                    activated_at, created_at
                ) VALUES (
                    :strategy_key, :model_version, 'PAPER_ACTIVE',
                    :dataset_hash, :feature_schema_hash,
                    :calibration_json, :metrics_json, :config_json,
                    '2026-07-30 15:00:00',
                    '2026-07-30 15:00:00'
                )
                """
            ),
            {
                "strategy_key": table.strategy_key,
                "model_version": table.model_version,
                "dataset_hash": table.dataset_hash,
                "feature_schema_hash": right_side_model_contract_hash(
                    load_v3_config()
                ),
                "calibration_json": json.dumps(table.as_dict()),
                "metrics_json": json.dumps({
                    "validation": {
                        "sample_count": 100,
                        "net_expectancy_pct": 1.0,
                        "profit_factor": 1.5,
                        "payoff_ratio": 1.2,
                    },
                    "portfolio": {
                        "trade_count": 100,
                        "net_expectancy_pct": 1.0,
                        "profit_factor": 1.5,
                        "payoff_ratio": 1.2,
                        "maximum_drawdown_pct": 5.0,
                        "net_profit_cny": 1_000.0,
                    },
                }),
                "config_json": json.dumps(wrong_config),
            },
        )
    status = TradingV3Repository(
        engine
    ).active_calibration_status()
    assert not status["calibrations"]
    assert status["rejections"]["right_side_trend"] == [
        "MODEL_CONFIG_HASH_MISMATCH"
    ]


def test_superseded_discovery_orders_cancel_unfilled_remainders():
    class Result:
        def __init__(self, rows=None):
            self.rows = rows or []

        def mappings(self):
            return self

        def all(self):
            return self.rows

    class Connection:
        def __init__(self):
            self.updates = []

        def execute(self, statement, params=None):
            sql = str(statement)
            if "SELECT o.order_id" in sql:
                assert "V3_VALIDATED_POSITIVE" in sql
                return Result([
                    {
                        "order_id": "old-unfilled",
                        "stock_code": "300609",
                        "quantity": 200,
                        "filled_quantity": 0,
                        "status": "QUEUED",
                        "decision_run_uid": "old-run",
                    },
                    {
                        "order_id": "old-partial",
                        "stock_code": "603158",
                        "quantity": 500,
                        "filled_quantity": 100,
                        "status": "PARTIALLY_FILLED",
                        "decision_run_uid": "old-run",
                    },
                ])
            if "SELECT execution_plan_id" in sql:
                assert "V3_PORTFOLIO" in sql
                return Result([
                    {
                        "execution_plan_id": "old-plan",
                        "run_uid": "old-run",
                        "stock_code": "300609",
                        "side": "BUY",
                        "quantity": 200,
                        "state": "PAPER_QUEUED",
                    },
                    {
                        "execution_plan_id": "partial-plan",
                        "run_uid": "old-run",
                        "stock_code": "603158",
                        "side": "BUY",
                        "quantity": 500,
                        "state": "PAPER_QUEUED",
                    },
                ])
            if sql.lstrip().startswith("UPDATE"):
                self.updates.append((sql, params))
            return Result()

    connection = Connection()
    result = paper_execution._cancel_superseded_v3_buys(
        connection,
        account_id="paper-main-v2",
        run_uid="new-run",
        now=datetime(2026, 8, 1, 18, 30),
    )

    assert [item["order_id"] for item in result["cancelled_orders"]] == [
        "old-unfilled",
        "old-partial",
    ]
    assert [
        item["order_id"] for item in result["cancelled_partial_orders"]
    ] == ["old-partial"]
    assert [
        item["execution_plan_id"]
        for item in result["cancelled_execution_plans"]
    ] == ["old-plan", "partial-plan"]
    assert len(connection.updates) == 4
    assert connection.updates[0][1]["waiting_reason"] == (
        "SUPERSEDED_BY_V3_DECISION"
    )
    assert connection.updates[1][1]["waiting_reason"] == (
        "SUPERSEDED_PARTIAL_BY_V3"
    )
    assert all(
        len(update[1]["waiting_reason"]) <= 40
        for update in connection.updates[:2]
    )
    assert connection.updates[2][1]["state"] == "CANCELLED"
    assert connection.updates[3][1]["state"] == (
        "PAPER_PARTIAL_CANCELLED"
    )


def test_v3_buy_target_without_canonical_v2_receipt_is_not_enqueued(
    monkeypatch,
):
    class Result:
        def __init__(self, *, first=None, all_rows=None, scalar=None):
            self._first = first
            self._all = all_rows or []
            self._scalar = scalar

        def mappings(self):
            return self

        def first(self):
            return self._first

        def all(self):
            return self._all

        def scalar(self):
            return self._scalar

    class Connection:
        def __init__(self):
            self.inserts = []

        def execute(self, statement, params=None):
            sql = str(statement)
            if sql.lstrip().startswith("INSERT"):
                self.inserts.append((sql, params))
                return Result()
            if "SELECT *" in sql and "FROM st_trade_account_v2" in sql:
                return Result(first={
                    "account_id": "paper-main-v2",
                    "cash_balance": 200_000.0,
                    "real_trading_enabled": 0,
                })
            if "FROM st_decision_run_v3" in sql:
                return Result(first={
                    "run_uid": "run-buy-blocked",
                    "trade_date": date(2026, 8, 5),
                    "status": "COMPLETED",
                    "model_version": "trading_v3.3.0-paper",
                    "portfolio_json": "{}",
                })
            if "FROM st_target_portfolio_v3" in sql:
                return Result(all_rows=[{
                    "stock_code": "000001",
                    "reason": "VALIDATED_POSITIVE",
                    "target_quantity": 1_000,
                    "target_value": 10_000.0,
                    "initial_stop_pct": -5.0,
                    "strategy_keys_json": '["right_side_trend"]',
                    "primary_strategy_key": "right_side_trend",
                    "primary_forecast_id": "forecast-1",
                    "attribution_snapshot_hash": "",
                }])
            if "SELECT MIN(trade_date)" in sql:
                return Result(scalar=date(2026, 8, 6))
            if "FROM st_position_state_v3" in sql:
                return Result(all_rows=[])
            if "SELECT o.order_id" in sql or "SELECT execution_plan_id" in sql:
                return Result(all_rows=[])
            if "SELECT stock_code" in sql and "UNION" in sql:
                return Result(all_rows=[])
            if "SELECT COUNT(*)" in sql:
                return Result(scalar=0)
            if "SELECT COALESCE(SUM(remaining_quantity)" in sql:
                return Result(scalar=0)
            if "SELECT intent_id" in sql:
                return Result(scalar=None)
            return Result()

    connection = Connection()

    class Context:
        def __enter__(self):
            return connection

        def __exit__(self, *args):
            return False

    class Engine:
        def begin(self):
            return Context()

    monkeypatch.setattr(
        paper_execution,
        "_canonical_v2_buy_receipt",
        lambda *_args, **_kwargs: (None, "BUY_GATE_DATA_BLOCKED"),
    )

    result = paper_execution.materialize_internal_paper_orders(
        Engine(),
        run_uid="run-buy-blocked",
    )

    assert result["created"] == []
    assert result["paper_order_count"] == 0
    assert result["skipped"][-1] == {
        "stock_code": "000001",
        "side": "BUY",
        "status": "RESEARCH_ONLY",
        "reason": "BUY_GATE_DATA_BLOCKED",
    }
    assert connection.inserts == []


def test_v3_exit_state_is_materialized_even_without_buy_targets(
    monkeypatch,
):
    class Result:
        def __init__(self, *, first=None, all_rows=None, scalar=None):
            self._first = first
            self._all = all_rows or []
            self._scalar = scalar

        def mappings(self):
            return self

        def first(self):
            return self._first

        def all(self):
            return self._all

        def scalar(self):
            return self._scalar

    class Connection:
        def execute(self, statement, params=None):
            sql = str(statement)
            if "FROM st_trade_account_v2" in sql:
                return Result(first={
                    "account_id": "paper-main-v2",
                    "cash_balance": 200_000.0,
                    "real_trading_enabled": 0,
                })
            if "FROM st_decision_run_v3" in sql:
                return Result(first={
                    "run_uid": "run-1",
                    "trade_date": date(2026, 7, 30),
                    "status": "COMPLETED",
                    "model_version": "trading_v3.3.0-paper",
                    "portfolio_json": "{}",
                })
            if "FROM st_target_portfolio_v3" in sql:
                return Result(all_rows=[])
            if "SELECT MIN(trade_date)" in sql:
                return Result(scalar=date(2026, 7, 31))
            if "FROM st_position_state_v3" in sql:
                return Result(all_rows=[{
                    "stock_code": "000001",
                    "quantity": 1_000,
                    "actual_quantity": 1_000,
                    "average_cost": 10.0,
                    "last_action": "SELL_ALL",
                    "last_reason_code": "TREND_INVALIDATED",
                    "last_reason": "趋势失效",
                    "invalidation_json": json.dumps({
                        "latest_price": 9.8,
                        "protective_stop": 9.5,
                    }),
                }])
            return Result()

    class Context:
        def __init__(self):
            self.connection = Connection()

        def __enter__(self):
            return self.connection

        def __exit__(self, *args):
            return False

    class Engine:
        def begin(self):
            return Context()

    captured = {}

    def fake_exit(connection, **kwargs):
        captured.update(kwargs)
        return {
            "status": "created",
            "quantity": kwargs["current_quantity"],
            "order_id": "sell-order",
        }

    monkeypatch.setattr(
        paper_execution,
        "_persist_exit_chain",
        fake_exit,
    )
    result = paper_execution.materialize_internal_paper_orders(
        Engine(),
        run_uid="run-1",
    )
    assert result["paper_order_count"] == 1
    assert result["created"][0]["side"] == "SELL"
    assert captured["target_quantity"] == 0
