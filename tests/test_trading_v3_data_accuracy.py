import pandas as pd
import pytest
from decimal import Decimal
from pathlib import Path
from sqlalchemy import create_engine, text

from server.trading_v3 import backtest
from server.trading_v3.backtest import (
    _build_features,
    _walk_forward_validation,
)
from server.trading_v3.config import load_v3_config
from server.trading_v3 import daily_features
from server.trading_v3.theme_features import (
    attach_best_theme,
    calculate_theme_statistics,
)
from tools import backtest_trading_v3
from tools import verify_trading_v3_production
from tools.register_trading_v3_artifact import _verify_profit_gate


def test_unadjusted_price_split_does_not_create_fake_negative_momentum():
    dates = pd.date_range("2026-01-01", periods=70, freq="B")
    rows = []
    raw_close = 20.0
    for index, trade_date in enumerate(dates):
        if index == 40:
            raw_close = 10.0
        pre_close = raw_close
        raw_close *= 1.001
        rows.append({
            "stock_code": "600001",
            "short_name": "测试股票",
            "trade_date": trade_date,
            "open": raw_close,
            "close": raw_close,
            "high": raw_close * 1.01,
            "low": raw_close * 0.99,
            "pre_close": pre_close,
            "amount": 100_000_000,
            "change_pct": 0.1,
        })
    features = _build_features(pd.DataFrame(rows))
    latest = features.iloc[-1]
    assert 1.5 < latest["return_20d_pct"] < 2.5
    assert latest["relative_strength_20d_pct"] == 0


def test_decimal_zero_source_values_do_not_break_feature_arithmetic():
    dates = pd.date_range("2026-01-01", periods=70, freq="B")
    rows = []
    for index, trade_date in enumerate(dates):
        close = Decimal("10") + Decimal(index) / Decimal("100")
        rows.append({
            "stock_code": "600002",
            "short_name": "decimal-source",
            "trade_date": trade_date,
            "open": close,
            "close": close,
            "high": close + Decimal("0.1"),
            "low": close - Decimal("0.1"),
            "pre_close": Decimal("0") if index == 0 else close - Decimal("0.01"),
            "amount": Decimal("0") if index == 0 else Decimal("100000000"),
            "change_pct": Decimal("0.1"),
        })

    features = _build_features(pd.DataFrame(rows))

    assert len(features) == len(rows)
    assert pd.notna(features.iloc[-1]["score"])


def _passing_artifact():
    return {
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
            "profit_factor": 1.45,
            "payoff_ratio": 1.6,
            "maximum_drawdown_pct": 11.7,
            "net_profit_cny": 6_500,
        },
    }


def test_verified_artifact_rechecks_current_production_gate():
    _verify_profit_gate(_passing_artifact(), load_v3_config())


def test_verified_artifact_rejects_tampered_profit_factor():
    artifact = _passing_artifact()
    artifact["validation_metrics"]["profit_factor"] = 0.9
    with pytest.raises(
        RuntimeError,
        match="OOS_PROFIT_FACTOR_TOO_LOW",
    ):
        _verify_profit_gate(artifact, load_v3_config())


def test_unbounded_backtest_is_refused_on_online_host(
    monkeypatch,
):
    monkeypatch.setattr(
        backtest_trading_v3,
        "ROOT",
        backtest_trading_v3.Path("/opt/ProBigA"),
    )
    monkeypatch.delenv("INVOCATION_ID", raising=False)
    with pytest.raises(RuntimeError, match="production backtests"):
        backtest_trading_v3._assert_resource_bounded_production()


def test_production_verifier_uses_local_runtime_on_server(monkeypatch):
    monkeypatch.setattr(
        verify_trading_v3_production,
        "ROOT",
        Path("/opt/ProBigA"),
    )
    assert verify_trading_v3_production._is_production_runtime()


def test_walk_forward_never_uses_inverted_calibration(monkeypatch):
    class InvertedCalibration:
        def has_valid_score_direction(self):
            return False

        def bucket_for(self, _score):
            raise AssertionError(
                "an inverted calibration must never select signals"
            )

    monkeypatch.setattr(
        backtest,
        "fit_calibration",
        lambda *_args, **_kwargs: InvertedCalibration(),
    )
    samples = pd.DataFrame({
        "trade_date": pd.to_datetime([
            "2025-01-10",
            "2025-07-10",
        ]),
        "exit_date": pd.to_datetime([
            "2025-01-20",
            "2025-07-20",
        ]),
        "score": [0.7, 0.9],
        "net_return_pct": [2.0, -2.0],
        "mae_pct": [-1.0, -3.0],
        "mfe_pct": [3.0, 1.0],
    })
    _calibration, training, validation, candidates = (
        _walk_forward_validation(
            samples,
            training_start=pd.Timestamp("2025-01-01").date(),
            validation_start=pd.Timestamp("2025-07-01").date(),
            validation_end=pd.Timestamp("2025-07-31").date(),
            model_version="right_side_trend.v3.4.1-test",
        )
    )
    assert training.empty
    assert validation.empty
    assert candidates.empty


def test_daily_decision_refuses_stale_qmt_kline(monkeypatch):
    monkeypatch.setattr(
        daily_features,
        "_expected_trade_date",
        lambda *_args, **_kwargs: pd.Timestamp(
            "2026-07-28"
        ).date(),
    )
    monkeypatch.setattr(
        daily_features,
        "_latest_trade_dates",
        lambda *_args, **_kwargs: [
            pd.Timestamp("2026-07-27").date()
        ],
    )

    with pytest.raises(
        RuntimeError,
        match="QMT_DAILY_KLINE_NOT_READY",
    ):
        daily_features.load_daily_feature_universe(
            object(),
            object(),
            as_of=pd.Timestamp("2026-07-28").date(),
        )


def test_daily_source_label_never_claims_unattested_data_is_qmt():
    blocked = daily_features._daily_source_label({
        "qmt_attestation_current": False,
    })
    attested = daily_features._daily_source_label({
        "qmt_attestation_current": True,
    })

    assert blocked.startswith("UNATTESTED_DAILY_KLINE_DATA_BLOCKED")
    assert attested.startswith("QMT_ATTESTED_DAILY_KLINE")


def test_daily_loader_excludes_suspended_zero_amount_bars():
    source = (
        Path(__file__).resolve().parents[1]
        / "server"
        / "trading_v3"
        / "daily_features.py"
    ).read_text(encoding="utf-8")
    assert "amount.iloc[-1] <= 0" in source


def test_daily_bar_loader_preserves_raw_low_for_stop_monitoring():
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        connection.execute(text(
            """
            CREATE TABLE sm_stock_kline (
                stock_code TEXT,
                short_name TEXT,
                trade_date DATE,
                open REAL,
                close REAL,
                high REAL,
                low REAL,
                pre_close REAL,
                amount REAL,
                change_pct REAL,
                k_type INTEGER
            )
            """
        ))
        connection.execute(
            text(
                """
                INSERT INTO sm_stock_kline VALUES
                ('000001', '', '2026-07-29', 10, 10, 10.2, 9.8,
                 9.9, 100000000, 1.01, 1),
                ('000001', 'sample', '2026-07-30', 10, 10.1, 10.3,
                 9.5, 10, 100000000, 1.0, 1)
                """
            )
        )
    bars = daily_features._load_bars(
        engine,
        dates=[
            pd.Timestamp("2026-07-29").date(),
            pd.Timestamp("2026-07-30").date(),
        ],
    )
    assert "raw_low" in bars
    assert float(bars.iloc[-1]["raw_low"]) == 9.5


def test_theme_breadth_acceleration_uses_prior_session_not_five_day_proxy():
    dates = pd.to_datetime(["2026-07-27", "2026-07-28"])
    rows = []
    prior_changes = [-1.0, 1.0, -0.5]
    latest_changes = [2.0, 3.0, 1.0]
    for stock_index, code in enumerate(("000001", "000002", "000003")):
        for day_index, trade_date in enumerate(dates):
            rows.append({
                "stock_code": code,
                "trade_date": trade_date,
                "change_pct": (
                    prior_changes[stock_index]
                    if day_index == 0
                    else latest_changes[stock_index]
                ),
                "amount": 100_000_000 * (day_index + 1),
            })
    memberships = {
        code: [("创新药", "创新药", "concept")]
        for code in ("000001", "000002", "000003")
    }
    stats = calculate_theme_statistics(
        pd.DataFrame(rows),
        as_of=dates[-1],
        memberships=memberships,
    )["创新药"]
    assert stats["sector_breadth_prior_pct"] == pytest.approx(
        100 / 3
    )
    assert stats["sector_breadth_pct"] == 100.0
    assert stats["sector_breadth_acceleration_pct"] > 0


def test_theme_members_receive_stock_specific_leadership_scores():
    base = {
        "000001": {
            "return_5d_pct": 12.0,
            "amount_ratio_5_20": 2.2,
            "breakout_20d_proximity": 0.99,
            "latest_change_pct": 5.0,
        },
        "000002": {
            "return_5d_pct": 1.0,
            "amount_ratio_5_20": 0.9,
            "breakout_20d_proximity": 0.92,
            "latest_change_pct": 0.2,
        },
    }
    memberships = {
        code: [("电力改革", "电力改革", "concept")]
        for code in base
    }
    statistics = {
        "电力改革": {
            "theme_code": "电力改革",
            "theme_name": "电力改革",
            "theme_source": "concept",
            "member_count": 44,
            "sector_return_5d_pct": 3.0,
            "theme_opportunity_score": 0.8,
            "sector_breadth_pct": 65.0,
            "sector_breadth_prior_pct": 45.0,
            "sector_breadth_3d_prior_pct": 42.0,
            "sector_breadth_acceleration_pct": 18.0,
            "sector_relative_return_pct": 2.0,
            "sector_amount_acceleration_pct": 25.0,
            "sector_leadership_depth": 0.8,
            "sector_crowding": 0.2,
        }
    }
    attach_best_theme(
        base,
        memberships=memberships,
        statistics=statistics,
    )
    assert (
        base["000001"]["stock_leadership_score"]
        > base["000002"]["stock_leadership_score"]
    )
