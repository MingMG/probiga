from datetime import date, datetime, timedelta
from pathlib import Path

from sqlalchemy import create_engine, text

from server.trading_v2.config import load_frozen_json
from server.trading_v2.intraday_activation import (
    MarketPoint,
    _discover_market_wide_momentum_alerts,
    _expected_session_minutes,
    _expected_universe_count,
    _load_watch_candidates,
    _watch_quote_change_metrics,
    assess_market,
    assess_reversal_candidate,
    select_reversal_activations,
    select_theme_activations,
)


def test_watch_candidate_loader_excludes_rejected_daily_buy_signals():
    statements = []

    class Result:
        def __init__(self, *, first=None, rows=None):
            self._first = first
            self._rows = rows or []

        def mappings(self):
            return self

        def first(self):
            return self._first

        def all(self):
            return self._rows

    class Connection:
        def execute(self, statement, _params=None):
            statements.append(str(statement))
            if len(statements) == 1:
                return Result(
                    first={"run_uid": "run-1", "market_regime": "TREND_UP"}
                )
            return Result(rows=[])

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class Engine:
        def connect(self):
            return Connection()

    run_uid, regime, rows = _load_watch_candidates(
        Engine(),
        now=datetime(2026, 8, 4, 9, 35),
        source_versions=["sector_preheat_v1.1.0"],
    )

    assert (run_uid, regime, rows) == ("run-1", "TREND_UP", [])
    assert "competition_status IN" in statements[1]
    assert "PAPER_TRIAL_ELIGIBLE" in statements[1]
    assert "rejection_code IS NULL" in statements[1]


def _config():
    return load_frozen_json(
        "strategies/intraday_activation_v2.json"
    )[0]


def _points(*, coverage=0.90):
    base = datetime(2026, 7, 27, 9, 32, 20)
    return [
        MarketPoint(
            observed_at=base + timedelta(minutes=index),
            observed_count=5000,
            expected_count=5527,
            coverage=coverage,
            positive_breadth_pct=breadth,
            equal_weight_return_pct=average_return,
            median_return_pct=average_return,
            source="GJ_BIG_QMT_INNER",
        )
        for index, (breadth, average_return) in enumerate(
            [(65.0, 0.30), (69.0, 0.45), (75.2, 0.68)]
        )
    ]


def _candidate(code, role, rank, *, score=80.0):
    return {
        "stock_code": code,
        "short_name": "测试股票",
        "theme_code": "CONCEPT:POWER",
        "strategy_version": "sector_preheat_v1.1.0",
        "raw_score": score,
        "initial_stop": 9.70,
        "raw_features_json": {
            "stock_name": "测试股票",
            "theme_name": "电力",
            "sector_role": role,
            "sector_rank": rank,
            "db_close": 10.0,
            "stop_loss": 9.70,
            "take_profit_2": 12.0,
        },
    }


def _reversal_candidate(code="603629"):
    pre_close = 108.61
    return {
        "stock_code": code,
        "short_name": "利通电子",
        "theme_code": "INDUSTRY:SW2消费电子",
        "strategy_version": "intraday_dynamic_activation_v2.4.1",
        "raw_score": 88.0,
        "initial_stop": pre_close * 0.985,
        "raw_features_json": {
            "stock_name": "利通电子",
            "theme_name": "消费电子",
            "session_low_return_pct": -7.0,
            "rebound_from_low_pct": 8.85,
            "momentum_10m_pct": 3.14,
            "momentum_5m_pct": 2.2,
            "intraday_amount_ratio": 1.36,
            "minute_history_coverage": 0.99,
            "minute_history_age_seconds": 60,
            "take_profit_2": pre_close * 1.08,
        },
    }


def test_0934_multi_point_recovery_can_confirm_previous_extreme():
    result = assess_market(
        _points(),
        previous_regime="EXTREME",
        now=datetime(2026, 7, 27, 9, 34, 30),
        config=_config(),
    )

    assert result.state == "PANIC_RECOVERY_CONFIRMED"
    assert result.execution_regime == "PANIC_RECOVERY"
    assert result.actionable is True
    assert result.confirming_points == 3


def test_partial_qmt_sample_is_watch_only_even_during_visible_rally():
    result = assess_market(
        _points(coverage=0.72),
        previous_regime="EXTREME",
        now=datetime(2026, 7, 27, 9, 34, 30),
        config=_config(),
    )

    assert result.state == "DATA_BLOCKED"
    assert result.actionable is False
    assert any("覆盖率不足" in item for item in result.evidence)
    assert any("只观察" in item for item in result.evidence)


def test_strong_watch_candidate_can_be_promoted_to_small_paper_probe():
    market = assess_market(
        _points(),
        previous_regime="RANGE",
        now=datetime(2026, 7, 27, 9, 34, 30),
        config=_config(),
    )
    candidate = _candidate("600001", "龙头", 1)
    decisions = select_theme_activations(
        [candidate],
        market=market,
        market_return_pct=0.68,
        quotes={
            "600001": {
                "price": 10.20,
                "pre_close": 10.0,
                "return_pct": 2.0,
                "near_limit_up": False,
            }
        },
        amount_ratios={"600001": 1.50},
        theme_metrics={
            "CONCEPT:POWER": {
                "observed_count": 20,
                "positive_breadth_pct": 75.0,
                "average_return_pct": 1.2,
            }
        },
        config=_config(),
    )

    assert decisions[0].action == "ACTIVATE_PROBE"
    assert decisions[0].opening_target_fraction == 0.25
    assert decisions[0].risk_reward_ratio >= 2.0


def test_locked_leader_can_select_tradeable_core_at_smaller_size():
    market = assess_market(
        _points(),
        previous_regime="EXTREME",
        now=datetime(2026, 7, 27, 9, 34, 30),
        config=_config(),
    )
    leader = _candidate("600001", "龙头", 1, score=87)
    core = _candidate("600002", "中军", 2, score=82)
    decisions = select_theme_activations(
        [leader, core],
        market=market,
        market_return_pct=0.68,
        quotes={
            "600001": {
                "price": 10.98,
                "pre_close": 10.0,
                "return_pct": 9.8,
                "near_limit_up": True,
            },
            "600002": {
                "price": 10.20,
                "pre_close": 10.0,
                "return_pct": 2.0,
                "near_limit_up": False,
            },
        },
        amount_ratios={"600001": 3.0, "600002": 1.6},
        theme_metrics={
            "CONCEPT:POWER": {
                "observed_count": 20,
                "positive_breadth_pct": 75.0,
                "average_return_pct": 1.2,
            }
        },
        config=_config(),
    )
    by_code = {item.stock_code: item for item in decisions}

    assert by_code["600001"].action not in {
        "ACTIVATE_PROBE",
        "ACTIVATE_SUBSTITUTE",
    }
    assert by_code["600002"].action == "ACTIVATE_SUBSTITUTE"
    assert by_code["600002"].opening_target_fraction == 0.15


def test_locked_leader_can_select_low_position_substitute():
    market = assess_market(
        _points(),
        previous_regime="EXTREME",
        now=datetime(2026, 7, 27, 9, 34, 30),
        config=_config(),
    )
    leader = _candidate("600001", "龙头", 1, score=87)
    substitute = _candidate("600002", "低位替补", 2, score=79)
    decisions = select_theme_activations(
        [leader, substitute],
        market=market,
        market_return_pct=0.68,
        quotes={
            "600001": {
                "price": 10.98,
                "pre_close": 10.0,
                "return_pct": 9.8,
                "near_limit_up": True,
            },
            "600002": {
                "price": 10.22,
                "pre_close": 10.0,
                "return_pct": 2.2,
                "near_limit_up": False,
            },
        },
        amount_ratios={"600001": 3.0, "600002": 1.6},
        theme_metrics={
            "CONCEPT:POWER": {
                "observed_count": 20,
                "positive_breadth_pct": 75.0,
                "average_return_pct": 1.2,
            }
        },
        config=_config(),
    )
    by_code = {item.stock_code: item for item in decisions}

    assert by_code["600002"].action == "ACTIVATE_SUBSTITUTE"
    assert by_code["600002"].opening_target_fraction == 0.15


def test_locked_leader_does_not_promote_unapproved_observation_role():
    market = assess_market(
        _points(),
        previous_regime="EXTREME",
        now=datetime(2026, 7, 27, 9, 34, 30),
        config=_config(),
    )
    leader = _candidate("600001", "龙头", 1, score=87)
    observation = _candidate("600002", "观察龙头", 2, score=82)
    decisions = select_theme_activations(
        [leader, observation],
        market=market,
        market_return_pct=0.68,
        quotes={
            "600001": {
                "price": 10.98,
                "pre_close": 10.0,
                "return_pct": 9.8,
                "near_limit_up": True,
            },
            "600002": {
                "price": 10.22,
                "pre_close": 10.0,
                "return_pct": 2.2,
                "near_limit_up": False,
            },
        },
        amount_ratios={"600001": 3.0, "600002": 1.6},
        theme_metrics={
            "CONCEPT:POWER": {
                "observed_count": 20,
                "positive_breadth_pct": 75.0,
                "average_return_pct": 1.2,
            }
        },
        config=_config(),
    )
    by_code = {item.stock_code: item for item in decisions}

    assert by_code["600002"].action == "WATCH"
    assert by_code["600002"].reason_code == "SUBSTITUTE_ROLE_NOT_ALLOWED"


def test_locked_leader_rejects_weak_substitute_even_if_role_is_allowed():
    market = assess_market(
        _points(),
        previous_regime="EXTREME",
        now=datetime(2026, 7, 27, 9, 34, 30),
        config=_config(),
    )
    leader = _candidate("600001", "龙头", 1, score=87)
    weak_core = _candidate("600002", "中军", 2, score=60)
    decisions = select_theme_activations(
        [leader, weak_core],
        market=market,
        market_return_pct=0.68,
        quotes={
            "600001": {
                "price": 10.98,
                "pre_close": 10.0,
                "return_pct": 9.8,
                "near_limit_up": True,
            },
            "600002": {
                "price": 10.22,
                "pre_close": 10.0,
                "return_pct": 2.2,
                "near_limit_up": False,
            },
        },
        amount_ratios={"600001": 3.0, "600002": 1.6},
        theme_metrics={
            "CONCEPT:POWER": {
                "observed_count": 20,
                "positive_breadth_pct": 75.0,
                "average_return_pct": 1.2,
            }
        },
        config=_config(),
    )
    by_code = {item.stock_code: item for item in decisions}

    assert by_code["600002"].action == "WATCH"
    assert by_code["600002"].reason_code == "SUBSTITUTE_SCORE_GAP_TOO_LARGE"


def test_candidate_is_not_activated_when_theme_breadth_breaks():
    market = assess_market(
        _points(),
        previous_regime="RANGE",
        now=datetime(2026, 7, 27, 9, 34, 30),
        config=_config(),
    )
    decisions = select_theme_activations(
        [_candidate("600001", "龙头", 1)],
        market=market,
        market_return_pct=0.68,
        quotes={
            "600001": {
                "price": 10.20,
                "pre_close": 10.0,
                "return_pct": 2.0,
                "near_limit_up": False,
            }
        },
        amount_ratios={"600001": 1.5},
        theme_metrics={
            "CONCEPT:POWER": {
                "observed_count": 20,
                "positive_breadth_pct": 45.0,
                "average_return_pct": 0.2,
            }
        },
        config=_config(),
    )

    assert decisions[0].action == "WATCH"
    assert decisions[0].reason_code == "THEME_BREADTH_NOT_CONFIRMED"


def test_litong_like_deep_water_reversal_can_trigger_small_paper_probe():
    market = assess_market(
        _points(),
        previous_regime="RANGE",
        now=datetime(2026, 7, 27, 9, 34, 30),
        config=_config(),
    )
    candidate = _reversal_candidate()
    result = assess_reversal_candidate(
        candidate,
        market=market,
        market_return_pct=0.68,
        market_breadth_pct=75.2,
        quote={
            "price": 109.95,
            "pre_close": 108.61,
            "return_pct": 1.23,
            "near_limit_up": False,
        },
        theme_metrics={
            "observed_count": 85,
            "positive_breadth_pct": 98.82,
            "average_return_pct": 3.80,
        },
        config=_config(),
    )

    assert result.action == "ACTIVATE_REVERSAL_PROBE"
    assert result.reason_code == "MARKET_WIDE_DEEP_REVERSAL_CONFIRMED"
    assert result.opening_target_fraction == 0.10
    assert result.risk_reward_ratio >= 2.0
    assert any("日内最低-7.00%" in item for item in result.evidence)


def test_litong_late_vertical_extension_is_alert_only_not_a_chase():
    market = assess_market(
        _points(),
        previous_regime="RANGE",
        now=datetime(2026, 7, 27, 9, 34, 30),
        config=_config(),
    )
    candidate = _reversal_candidate()
    candidate["raw_features_json"]["momentum_5m_pct"] = 5.4
    result = assess_reversal_candidate(
        candidate,
        market=market,
        market_return_pct=0.68,
        market_breadth_pct=75.2,
        quote={
            "price": 117.00,
            "pre_close": 108.61,
            "return_pct": 7.72,
            "near_limit_up": False,
        },
        theme_metrics={
            "observed_count": 85,
            "positive_breadth_pct": 98.82,
            "average_return_pct": 3.80,
        },
        config=_config(),
    )

    assert result.action == "WATCH"
    assert result.reason_code == "REVERSAL_TOO_EXTENDED_TO_CHASE"
    assert result.risk_reward_ratio < 1.0


def test_regular_underwater_recovery_is_not_limited_to_deep_water():
    market = assess_market(
        _points(),
        previous_regime="RANGE",
        now=datetime(2026, 7, 27, 9, 34, 30),
        config=_config(),
    )
    candidate = _reversal_candidate("600003")
    candidate["initial_stop"] = 98.5
    candidate["raw_features_json"].update(
        {
            "session_low_return_pct": -1.5,
            "rebound_from_low_pct": 2.5,
            "momentum_10m_pct": 1.2,
            "momentum_5m_pct": 0.8,
            "intraday_amount_ratio": 1.8,
            "take_profit_2": 108.0,
        }
    )
    result = assess_reversal_candidate(
        candidate,
        market=market,
        market_return_pct=0.68,
        market_breadth_pct=75.2,
        quote={
            "price": 100.96,
            "pre_close": 100.0,
            "return_pct": 0.96,
            "near_limit_up": False,
        },
        theme_metrics={
            "observed_count": 40,
            "positive_breadth_pct": 82.0,
            "average_return_pct": 2.0,
        },
        config=_config(),
    )

    assert result.action == "ACTIVATE_REVERSAL_PROBE"
    assert result.reason_code == "MARKET_WIDE_WATERLINE_RECOVERY_CONFIRMED"
    assert result.role == "盘中水下修复"
    assert result.opening_target_fraction == 0.08


def test_one_minute_limit_attack_alert_does_not_require_full_history():
    market = assess_market(
        _points(),
        previous_regime="RANGE",
        now=datetime(2026, 7, 27, 9, 34, 30),
        config=_config(),
    )
    candidate = _reversal_candidate("600004")
    candidate["raw_features_json"].update(
        {
            "discovery_lane": "MARKET_WIDE_MOMENTUM_ALERT",
            "minute_history_coverage": 0.0,
            "reference_interval_return_pct": 9.5,
            "reference_interval_minutes": 1.0,
        }
    )
    result = assess_reversal_candidate(
        candidate,
        market=market,
        market_return_pct=0.68,
        market_breadth_pct=75.2,
        quote={
            "price": 109.8,
            "pre_close": 100.0,
            "return_pct": 9.8,
            "near_limit_up": True,
        },
        theme_metrics={
            "observed_count": 40,
            "positive_breadth_pct": 82.0,
            "average_return_pct": 2.0,
        },
        config=_config(),
    )

    assert result.action == "WATCH"
    assert result.reason_code == "MARKET_WIDE_LIMIT_ATTACK_ALERT"
    assert not any("分钟数据不完整" in item for item in result.evidence)


def test_low_turnover_limit_attack_is_still_discovered(monkeypatch):
    monkeypatch.setattr(
        "server.trading_v2.intraday_activation._load_primary_industries",
        lambda *_args, **_kwargs: {
            "603459": {
                "theme_code": "INDUSTRY:SW2电子",
                "theme_name": "电子",
                "short_name": "红板科技",
            }
        },
    )
    result = _discover_market_wide_momentum_alerts(
        trade_date=date(2026, 7, 27),
        now=datetime(2026, 7, 27, 14, 41),
        quotes={
            "603459": {
                "price": 55.0,
                "pre_close": 50.0,
                "return_pct": 10.0,
                "amount": 6_000_000,
                "short_name": "红板科技",
                "near_limit_up": True,
            }
        },
        reference_quotes={},
        excluded_codes=set(),
        config=_config(),
    )
    assert [item["stock_code"] for item in result] == ["603459"]


def test_positive_volume_burst_can_be_a_separate_small_paper_lane():
    market = assess_market(
        _points(),
        previous_regime="RANGE",
        now=datetime(2026, 7, 27, 9, 34, 30),
        config=_config(),
    )
    candidate = _reversal_candidate("600005")
    candidate["initial_stop"] = 99.0
    candidate["raw_features_json"].update(
        {
            "discovery_lane": "MARKET_WIDE_VOLUME_BURST",
            "reference_interval_return_pct": 0.8,
            "reference_interval_seconds": 60,
            "interval_amount_delta": 10000000,
            "live_snapshot_count": 3,
            "intraday_amount_ratio": 2.5,
            "take_profit_2": 108.0,
        }
    )
    result = assess_reversal_candidate(
        candidate,
        market=market,
        market_return_pct=0.68,
        market_breadth_pct=75.2,
        quote={
            "price": 101.5,
            "pre_close": 100.0,
            "return_pct": 1.5,
            "near_limit_up": False,
        },
        theme_metrics={
            "observed_count": 40,
            "positive_breadth_pct": 82.0,
            "average_return_pct": 2.0,
        },
        config=_config(),
    )

    assert result.action == "ACTIVATE_VOLUME_PROBE"
    assert result.reason_code == "MARKET_WIDE_VOLUME_BURST_CONFIRMED"
    assert result.opening_target_fraction == 0.08


def test_locked_leader_can_activate_independent_dragon_two_probe():
    market = assess_market(
        _points(),
        previous_regime="RANGE",
        now=datetime(2026, 7, 27, 9, 34, 30),
        config=_config(),
    )
    candidate = {
        "stock_code": "600002",
        "short_name": "龙二测试",
        "theme_code": "INDUSTRY:SW2电子",
        "strategy_version": "intraday_dynamic_activation_v2.4.1",
        "raw_score": 82.0,
        "initial_stop": 99.5,
        "raw_features_json": {
            "stock_name": "龙二测试",
            "theme_name": "电子",
            "sector_role": "龙二",
            "discovery_lane": "MARKET_WIDE_LEADER_SUBSTITUTE",
            "leader_code": "600001",
            "leader_name": "一字龙头",
            "stop_loss": 99.5,
            "take_profit_2": 109.5,
        },
    }
    result = assess_reversal_candidate(
        candidate,
        market=market,
        market_return_pct=0.68,
        market_breadth_pct=75.2,
        quote={
            "price": 101.5,
            "pre_close": 100.0,
            "return_pct": 1.5,
            "near_limit_up": False,
        },
        theme_metrics={
            "observed_count": 30,
            "positive_breadth_pct": 80.0,
            "average_return_pct": 2.0,
        },
        config=_config(),
    )
    assert result.action == "ACTIVATE_SUBSTITUTE"
    assert result.reason_code == "LOCKED_LEADER_FOLLOWER_CONFIRMED"
    assert result.leader_code == "600001"
    assert result.opening_target_fraction == 0.08


def test_reversal_radar_activates_at_most_one_candidate_per_tick():
    market = assess_market(
        _points(),
        previous_regime="RANGE",
        now=datetime(2026, 7, 27, 9, 34, 30),
        config=_config(),
    )
    candidates = [_reversal_candidate("603629"), _reversal_candidate("600002")]
    candidates[1]["short_name"] = "次优股票"
    candidates[1]["raw_score"] = 80.0
    quotes = {
        code: {
            "price": 109.95,
            "pre_close": 108.61,
            "return_pct": 1.23,
            "near_limit_up": False,
        }
        for code in ("603629", "600002")
    }
    decisions = select_reversal_activations(
        candidates,
        market=market,
        market_return_pct=0.68,
        market_breadth_pct=75.2,
        quotes=quotes,
        theme_metrics={
            "INDUSTRY:SW2消费电子": {
                "observed_count": 85,
                "positive_breadth_pct": 98.82,
                "average_return_pct": 3.80,
            }
        },
        config=_config(),
    )

    assert sum(
        item.action == "ACTIVATE_REVERSAL_PROBE" for item in decisions
    ) == 1
    assert any(
        item.reason_code == "LOWER_RANKED_REVERSAL_CANDIDATE"
        for item in decisions
    )


def test_expected_session_minutes_excludes_lunch_break():
    assert _expected_session_minutes(datetime(2026, 7, 27, 9, 50)) == 21
    assert _expected_session_minutes(datetime(2026, 7, 27, 14, 42)) == 224


def test_live_quote_metrics_detect_positive_amount_burst():
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE st_intraday_watch_quote_v2 (
                    stock_code TEXT, trade_date DATE,
                    observed_at DATETIME, price REAL, amount REAL
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO st_intraday_watch_quote_v2
                (stock_code, trade_date, observed_at, price, amount)
                VALUES
                ('600001','2026-07-27','2026-07-27 14:40:00',100.0,100.0),
                ('600001','2026-07-27','2026-07-27 14:41:00',100.5,200.0),
                ('600001','2026-07-27','2026-07-27 14:42:00',101.5,500.0)
                """
            )
        )

    result = _watch_quote_change_metrics(
        engine,
        trade_date=date(2026, 7, 27),
        candidate_codes={"600001"},
        now=datetime(2026, 7, 27, 14, 42),
    )["600001"]

    assert result["amount_ratio"] == 3.0
    assert result["amount_delta"] == 300.0
    assert result["price_return_pct"] > 0.9
    assert result["snapshot_count"] == 3.0


def test_current_quote_loader_rejects_future_rows():
    source = (
        Path(__file__).resolve().parents[1]
        / "server"
        / "trading_v2"
        / "intraday_activation.py"
    ).read_text(encoding="utf-8")

    assert "COALESCE(source_time, snapshot_at)" in source
    assert "<= :now" in source
    assert "`change` AS price_change" in source


def test_expected_universe_uses_latest_tradable_kline_pool(monkeypatch):
    class Result:
        @staticmethod
        def scalar():
            return 5536

    class Connection:
        @staticmethod
        def execute(_statement):
            return Result()

    class Context:
        @staticmethod
        def __enter__():
            return Connection()

        @staticmethod
        def __exit__(_exc_type, _exc, _traceback):
            return False

    class Engine:
        @staticmethod
        def connect():
            return Context()

    monkeypatch.setattr(
        "server.trading_v2.intraday_activation.get_kline_engine",
        lambda: Engine(),
    )

    assert _expected_universe_count(object()) == 5536
