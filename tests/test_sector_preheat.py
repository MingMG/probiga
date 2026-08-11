from __future__ import annotations

from datetime import date, datetime, timedelta

from server.trading_v2.bootstrap import _strategy_manifests
from server.trading_v2.domain import OrderSide
from server.trading_v2.execution import _sector_entry_wait_reason
from server.trading_v2.execution import _entry_trend_wait_reason
from server.trading_v2.position_monitor import _sector_position_facts
from server.trading_v2.planner import _candidate_competition_order_key
from server.trading_v2.sector_preheat import (
    _candidate_signal,
    _is_orderly_right_side_startup,
    _sector_sources_are_fresh,
    _stock_features,
    load_sector_preheat_config,
    merge_sector_preheat_candidates,
    score_sector_preheat,
)
from server.trading_v2.decision_worker import (
    _canonical_new_buy_rejection,
    _data_quality,
)


def _bars(*, near_limit_leader: bool = False) -> list[dict]:
    start = date(2026, 7, 13)
    rows: list[dict] = []
    for stock_index in range(20):
        code = f"0000{stock_index + 1:02d}"
        close = 10.0
        for day_index in range(8):
            if day_index < 7 and stock_index < 3:
                change_pct = 0.3
            elif day_index < 7 and stock_index < 15:
                change_pct = 0.1
            elif stock_index < 5:
                change_pct = (
                    10.0
                    if near_limit_leader
                    and stock_index == 0
                    and day_index == 7
                    else 2.5
                )
            elif stock_index < 15:
                change_pct = 0.9
            else:
                change_pct = -0.4
            pre_close = close
            close = pre_close * (1 + change_pct / 100)
            amount = 120_000_000 * (
                1.6 if day_index == 7 else 1.0
            )
            rows.append(
                {
                    "stock_code": code,
                    "short_name": f"测试{stock_index + 1}",
                    "trade_date": (
                        start + timedelta(days=day_index)
                    ).isoformat(),
                    "open": pre_close,
                    "close": close,
                    "high": close,
                    "low": pre_close,
                    "pre_close": pre_close,
                    "change_pct": change_pct,
                    "amount": amount,
                }
            )
    for stock_index in range(20):
        code = f"0021{stock_index:02d}"
        close = 10.0
        for day_index in range(8):
            change_pct = -0.2
            pre_close = close
            close = pre_close * (1 + change_pct / 100)
            rows.append(
                {
                    "stock_code": code,
                    "short_name": f"市场样本{stock_index + 1}",
                    "trade_date": (
                        start + timedelta(days=day_index)
                    ).isoformat(),
                    "open": pre_close,
                    "close": close,
                    "high": pre_close,
                    "low": close,
                    "pre_close": pre_close,
                    "change_pct": change_pct,
                    "amount": 150_000_000,
                }
            )
    return rows


def _confirmed_bars(*, near_limit_leader: bool = False) -> list[dict]:
    rows = _bars()
    for row in rows:
        if (
            row["stock_code"].startswith("0000")
            and row["trade_date"] == "2026-07-20"
        ):
            change_pct = (
                10.0
                if near_limit_leader
                and row["stock_code"] == "000001"
                else (2.8 if near_limit_leader else 3.0)
            )
            row["change_pct"] = change_pct
            row["close"] = row["pre_close"] * (
                1.0 + change_pct / 100.0
            )
            row["high"] = row["close"]
    return rows


def _memberships() -> list[dict]:
    rows = []
    for stock_index in range(20):
        code = f"0000{stock_index + 1:02d}"
        rows.extend(
            [
                {
                    "sector_type": "industry",
                    "sector_code": "SW2测试",
                    "sector_name": "测试行业",
                    "stock_code": code,
                },
                {
                    "sector_type": "concept",
                    "sector_code": "TGN测试",
                    "sector_name": "测试概念",
                    "stock_code": code,
                },
            ]
        )
    return rows


def test_sector_preheat_detects_breadth_and_produces_ready_leaders():
    result = score_sector_preheat(
        memberships=_memberships(),
        bars=_confirmed_bars(),
        trade_date="2026-07-20",
        market_regime="THEME_ROTATION",
    )
    assert result["hot_sector_count"] == 1
    assert result["ready_count"] >= 1
    assert result["candidate_count"] == (
        result["execution_candidate_count"]
        + result["discovery_candidate_count"]
    )
    assert result["ready_count"] == sum(
        item["signal_status"] == "READY"
        for item in result["execution_candidates"]
    )
    assert all(
        item["signal_status"] != "READY"
        and item["signal_direction"] == "HOLD"
        for item in result["discovery_candidates"]
    )
    assert {
        item["sector_stage"] for item in result["candidates"]
    } <= {"PREHEAT", "CONFIRMED"}
    assert all(item["theme_code"] for item in result["candidates"])
    assert all(
        item["risk_reward_ratio"] >= 3.0
        for item in result["candidates"]
    )
    assert all(
        item["theme_name"] in item["trigger_conditions"][-1]
        for item in result["candidates"]
    )


def test_sector_preheat_never_floors_actual_risk_reward_to_threshold():
    feature = _stock_features(
        [
            row
            for row in _confirmed_bars()
            if row["stock_code"] == "000001"
        ]
    )
    assert feature is not None
    config = load_sector_preheat_config()
    config["candidate_thresholds"] = {
        **config["candidate_thresholds"],
        "take_profit_2_pct": 8.0,
        "minimum_risk_reward": 3.0,
    }
    signal = _candidate_signal(
        feature,
        sector={
            "sector_code": "TGN测试",
            "sector_name": "测试概念",
            "sector_type": "concept",
            "stage": "CONFIRMED",
            "stage_reason": "STRICT_CONFIRMATION",
            "score": 90.0,
            "positive_breadth_pct": 80.0,
            "above_ma5_pct": 80.0,
            "ignition_evidence_count": 8,
            "ignition_evidence": {},
            "first_ignition_spike": False,
        },
        raw_score=90.0,
        role="龙头",
        rank=1,
        market_regime="TREND_UP",
        config=config,
    )
    assert signal["risk_reward_ratio"] < 3.0
    assert signal["signal_status"] == "WATCH"
    assert "实际入场" in signal["gate_reason"]


def test_sector_source_freshness_requires_same_day_membership_and_kline():
    common = {
        "target_date": "2026-07-27",
        "industry_snapshot_date": "2026-07-27",
        "concept_snapshot_date": "2026-07-27",
        "kline_snapshot_date": "2026-07-27",
        "membership_row_count": 100,
        "kline_row_count": 100,
    }
    assert _sector_sources_are_fresh(**common) is True
    assert _sector_sources_are_fresh(
        **{**common, "kline_snapshot_date": "2026-07-24"}
    ) is False
    assert _sector_sources_are_fresh(
        **{**common, "concept_snapshot_date": ""}
    ) is False


def test_decision_quality_rejects_attestation_that_ends_before_trade_day():
    status, blocks = _data_quality(
        {"source_status": "fresh"},
        {
            "runs": [
                {
                    "start_date": "2024-01-02",
                    "end_date": "2026-07-24",
                    "status": "COMPLETED",
                    "coverage_pct": 100,
                }
            ]
        },
        "sha",
        date(2026, 7, 27),
    )
    assert status == "BLOCK"
    assert "QMT_DAILY_KLINE_NOT_ATTESTED" in blocks


def test_decision_quality_accepts_complete_attestation_covering_trade_day():
    status, blocks = _data_quality(
        {"source_status": "fresh"},
        {
            "runs": [
                {
                    "start_date": "2026-07-27",
                    "end_date": "2026-07-27",
                    "status": "COMPLETED",
                    "coverage_pct": 100,
                }
            ]
        },
        "sha",
        date(2026, 7, 27),
    )
    assert status == "PASS"
    assert blocks == []


def test_limit_up_leader_is_visible_but_never_ready_to_chase():
    result = score_sector_preheat(
        memberships=_memberships(),
        bars=_confirmed_bars(near_limit_leader=True),
        trade_date="2026-07-20",
        market_regime="THEME_ROTATION",
    )
    leader = next(
        item
        for item in result["candidates"]
        if item["stock_code"] == "000001"
    )
    assert leader["signal_status"] == "WATCH"
    assert leader["gate_status"] == "REDUCE"
    assert "禁止次日盲目追板" in leader["gate_reason"]
    assert any(
        item["signal_status"] == "READY"
        and item["stock_code"] != leader["stock_code"]
        for item in result["candidates"]
    )


def test_first_broad_ignition_is_not_mislabeled_as_overheated():
    bars = _bars()
    for row in bars:
        if (
            row["stock_code"].startswith("0000")
            and row["trade_date"] == "2026-07-20"
        ):
            row["change_pct"] = 4.0
            row["close"] = row["pre_close"] * 1.04
            row["high"] = row["close"]
    result = score_sector_preheat(
        memberships=_memberships(),
        bars=bars,
        trade_date="2026-07-20",
        market_regime="THEME_ROTATION",
    )
    sector = next(
        item
        for item in result["sectors"]
        if item["sector_name"] == "测试行业"
    )
    assert sector["first_ignition_spike"] is True
    assert sector["stage"] == "PREHEAT"
    assert sector["stage_reason"] == "FIRST_IGNITION_OBSERVATION"
    assert result["candidates"]
    assert all(
        item["signal_lane"] == "DISCOVERY_SHADOW"
        and item["signal_status"] != "READY"
        and item["signal_direction"] == "HOLD"
        for item in result["candidates"]
    )
    assert result["execution_candidate_count"] == 0
    assert result["discovery_candidate_count"] == result["candidate_count"]


def test_weak_to_strong_repair_can_preheat_before_five_day_recovery():
    bars = _bars()
    for stock_index in range(20):
        code = f"0000{stock_index + 1:02d}"
        close = 10.0
        for row in sorted(
            (item for item in bars if item["stock_code"] == code),
            key=lambda item: item["trade_date"],
        ):
            change_pct = (
                -1.0 if row["trade_date"] < "2026-07-20" else 2.0
            )
            row["pre_close"] = close
            row["open"] = close
            row["change_pct"] = change_pct
            close *= 1.0 + change_pct / 100.0
            row["close"] = close
            row["high"] = max(row["open"], close)
            row["low"] = min(row["open"], close)
    result = score_sector_preheat(
        memberships=_memberships(),
        bars=bars,
        trade_date="2026-07-20",
        market_regime="THEME_ROTATION",
    )
    sector = next(
        item
        for item in result["sectors"]
        if item["sector_name"] == "测试行业"
    )
    assert sector["average_return_5d_pct"] < -1.5
    assert sector["stage"] == "PREHEAT"
    assert sector["stage_reason"] == "WEAK_TO_STRONG_IGNITION"


def test_panic_recovery_tracks_shadow_candidate_without_auto_order():
    result = score_sector_preheat(
        memberships=_memberships(),
        bars=_confirmed_bars(),
        trade_date="2026-07-20",
        market_regime="PANIC_RECOVERY",
    )
    candidate = next(
        item
        for item in result["candidates"]
        if item["sector_role"] in {"龙头", "中军", "低位核心"}
    )
    assert candidate["signal_status"] == "WATCH"
    assert candidate["gate_status"] == "REDUCE"
    assert candidate["effective_weight"] == 0.25
    assert candidate["weight_detail"]["state_multiplier"] == 0.25
    assert "影子跟踪" in candidate["gate_reason"]


def test_orderly_right_side_startup_can_use_small_paper_lane_in_recovery():
    rows = []
    close = 10.0
    changes = [0.2] * 9 + [3.0, 4.0, 8.0]
    start = date(2026, 7, 1)
    for index, change_pct in enumerate(changes):
        pre_close = close
        close *= 1.0 + change_pct / 100.0
        rows.append(
            {
                "stock_code": "002326",
                "short_name": "永太科技",
                "trade_date": (start + timedelta(days=index)).isoformat(),
                "open": pre_close,
                "close": close,
                "high": close * 1.005,
                "low": pre_close * 0.995,
                "pre_close": pre_close,
                "change_pct": change_pct,
                "amount": (
                    240_000_000 if index == len(changes) - 1
                    else 100_000_000
                ),
            }
        )
    config = load_sector_preheat_config()
    feature = _stock_features(rows)
    assert feature is not None
    feature["orderly_right_side_startup"] = (
        _is_orderly_right_side_startup(feature, config=config)
    )
    assert feature["orderly_right_side_startup"] is True
    signal = _candidate_signal(
        feature,
        sector={
            "sector_code": "CONCEPT:NEW_MATERIAL",
            "sector_name": "新材料",
            "sector_type": "concept",
            "stage": "PREHEAT",
            "stage_reason": "FIRST_IGNITION_OBSERVATION",
            "score": 84.0,
            "positive_breadth_pct": 72.0,
            "above_ma5_pct": 68.0,
            "first_ignition_spike": True,
        },
        raw_score=84.5,
        role="龙头",
        rank=1,
        market_regime="PANIC_RECOVERY",
        config=config,
    )
    assert signal["signal_status"] == "READY"
    assert signal["effective_weight"] == 0.25
    assert signal["orderly_right_side_startup"] is True


def test_daily_market_discovery_does_not_require_sector_membership():
    rows = []
    close = 10.0
    start = date(2026, 7, 1)
    for index in range(10):
        change_pct = 8.0 if index == 9 else 0.1
        pre_close = close
        close *= 1.0 + change_pct / 100.0
        rows.append(
            {
                "stock_code": "603459",
                "short_name": "红板科技",
                "trade_date": (start + timedelta(days=index)).isoformat(),
                "open": pre_close,
                "close": close,
                "high": close,
                "low": pre_close,
                "pre_close": pre_close,
                "change_pct": change_pct,
                "amount": 200_000_000,
            }
        )
    result = score_sector_preheat(
        memberships=[],
        bars=rows,
        trade_date=rows[-1]["trade_date"],
        market_regime="THEME_ROTATION",
    )
    candidate = result["candidates"][0]
    assert candidate["stock_code"] == "603459"
    assert candidate["theme_name"] == "全市场强势异动"
    assert candidate["signal_lane"] == "DAILY_MARKET_DISCOVERY"
    assert candidate["signal_status"] == "WATCH"


def test_overlapping_hot_themes_are_retained_as_candidate_aliases():
    memberships = _memberships()
    for stock_index in range(20):
        memberships.append(
            {
                "sector_type": "concept",
                "sector_code": "TGN_ALIAS",
                "sector_name": "测试概念别名",
                "stock_code": f"0000{stock_index + 1:02d}",
            }
        )
    result = score_sector_preheat(
        memberships=memberships,
        bars=_bars(),
        trade_date="2026-07-20",
        market_regime="THEME_ROTATION",
    )
    candidate = result["candidates"][0]
    names = {
        item["theme_name"] for item in candidate["theme_matches"]
    }
    assert "测试概念" in names
    assert "测试概念别名" in names
    assert candidate["theme_count"] >= 2


def test_sector_candidates_merge_without_removing_legacy_signals():
    sector = score_sector_preheat(
        memberships=_memberships(),
        bars=_bars(),
        trade_date="2026-07-20",
        market_regime="THEME_ROTATION",
    )
    legacy = {
        "candidates": [
            {
                "stock_code": "000001",
                "stock_name": "测试1",
                "final_status": "WATCH",
                "strategy_signals": [{
                    "strategy_key": "short_term",
                    "chase_risk_status": "ALLOW",
                    "ordinary_buy_eligible": True,
                }],
            }
        ],
        "summary": {"candidate_count": 1},
        "data_sources": ["st_recommended_stocks"],
    }
    merged = merge_sector_preheat_candidates(legacy, sector)
    first = next(
        item
        for item in merged["candidates"]
        if item["stock_code"] == "000001"
    )
    assert {item["strategy_key"] for item in first["strategy_signals"]} == {
        "short_term",
        "sector_preheat",
    }
    assert "qmt_point_in_time_sector_preheat" in merged["data_sources"]


def _sector_ready_snapshot(stock_code: str = "000001") -> dict:
    return {
        "candidate_count": 1,
        "ready_count": 1,
        "candidates": [
            {
                "stock_code": stock_code,
                "stock_name": "测试股",
                "strategy_key": "sector_preheat",
                "signal_direction": "BUY",
                "signal_status": "READY",
                "gate_status": "PASS",
                "raw_score": 95,
                "evidence_chain": [],
            }
        ],
    }


def test_sector_ready_is_blocked_when_canonical_chase_gate_is_watch():
    legacy = {
        "candidates": [
            {
                "stock_code": "000001",
                "final_direction": "HOLD",
                "final_status": "WATCH",
                "strategy_signals": [
                    {
                        "strategy_key": "short_term",
                        "source_recommend_status": "ALLOW",
                        "source_signal_status": "BUY_READY",
                        "chase_risk_status": "WATCH",
                        "ordinary_buy_eligible": False,
                    }
                ],
            }
        ],
        "summary": {},
        "data_sources": [],
    }

    merged = merge_sector_preheat_candidates(
        legacy,
        _sector_ready_snapshot(),
    )
    sector_signal = next(
        item
        for item in merged["candidates"][0]["strategy_signals"]
        if item.get("strategy_key") == "sector_preheat"
    )

    assert sector_signal["signal_direction"] == "HOLD"
    assert sector_signal["signal_status"] == "BLOCKED"
    assert sector_signal["canonical_new_buy_rejection_code"] == (
        "CANONICAL_CHASE_GATE_NOT_ALLOWED"
    )
    assert merged["summary"]["sector_preheat_ready_count"] == 0


def test_sector_ready_is_blocked_when_same_batch_has_exit_signal():
    legacy_exit = {
        "candidates": [
            {
                "stock_code": "000001",
                "final_direction": "SELL",
                "final_status": "SELL_ALERT",
                "strategy_signals": [
                    {
                        "strategy_key": "short_term",
                        "signal_direction": "SELL",
                        "signal_status": "SELL_ALERT",
                    }
                ],
            }
        ],
        "summary": {},
        "data_sources": [],
    }

    merged = merge_sector_preheat_candidates(
        legacy_exit,
        _sector_ready_snapshot(),
    )
    candidate = merged["candidates"][0]
    sector_signal = next(
        item
        for item in candidate["strategy_signals"]
        if item.get("strategy_key") == "sector_preheat"
    )

    assert candidate["final_direction"] == "SELL"
    assert sector_signal["signal_direction"] == "HOLD"
    assert sector_signal["canonical_new_buy_rejection_code"] == (
        "CONFLICTING_EXIT_SIGNAL"
    )
    assert _canonical_new_buy_rejection(
        candidate["strategy_signals"][0],
        candidate_has_exit=True,
    ) is None


def test_v2_final_route_requires_explicit_canonical_chase_gate():
    assert _canonical_new_buy_rejection(
        {
            "signal_direction": "BUY",
            "source_recommend_status": "ALLOW",
            "source_signal_status": "BUY_READY",
            "chase_risk_status": "ALLOW",
            "ordinary_buy_eligible": True,
        }
    ) is None
    assert _canonical_new_buy_rejection(
        {"signal_direction": "BUY"}
    ) == "CANONICAL_RECOMMEND_GATE_NOT_ALLOWED"
    assert _canonical_new_buy_rejection(
        {
            "signal_direction": "BUY",
            "source_recommend_status": "ALLOW",
            "chase_risk_status": "ALLOW",
            "ordinary_buy_eligible": True,
        }
    ) == "CANONICAL_SIGNAL_NOT_CONFIRMED"
    assert _canonical_new_buy_rejection(
        {
            "signal_direction": "BUY",
            "source_recommend_status": "ALLOW",
            "source_signal_status": "BUY_READY",
            "chase_risk_status": "ALLOW",
            "ordinary_buy_eligible": True,
        },
        candidate_has_exit=True,
    ) == "CONFLICTING_EXIT_SIGNAL"


class _Rows:
    def __init__(self, row):
        self.row = row

    def mappings(self):
        return self

    def first(self):
        return self.row


class _Connection:
    def __init__(self, row):
        self.row = row

    def execute(self, *_args, **_kwargs):
        return _Rows(self.row)


def test_sector_paper_order_waits_for_fresh_intraday_confirmation():
    now = datetime(2026, 7, 27, 9, 31)
    good = {
        "snapshot_at": now,
        "direction": "UP",
        "score": 35,
        "breadth_pct": 25,
    }
    stale = {**good, "snapshot_at": now - timedelta(seconds=181)}
    kwargs = {
        "strategy_version": "sector_preheat_v1.0.0",
        "theme_code": "INDUSTRY:SW2电力",
        "side": OrderSide.BUY,
        "now": now,
    }
    assert _sector_entry_wait_reason(_Connection(good), **kwargs) == ""
    assert (
        _sector_entry_wait_reason(_Connection(stale), **kwargs)
        == "WAIT_SECTOR_CONFIRMATION"
    )


def test_sector_entry_does_not_fill_below_initial_stop():
    assert (
        _entry_trend_wait_reason(
            strategy_version="sector_preheat_v1.0.0",
            side=OrderSide.BUY,
            fill_price=9.5,
            initial_stop=9.6,
        )
        == "WAIT_ENTRY_TREND_INVALID"
    )
    assert (
        _entry_trend_wait_reason(
            strategy_version="sector_preheat_v1.0.0",
            side=OrderSide.BUY,
            fill_price=9.7,
            initial_stop=9.6,
        )
        == ""
    )


def test_paper_trial_competition_uses_model_score_not_stock_code():
    candidates = [
        {
            "stock_code": "000001",
            "strategy_version": "sector_preheat_v1.0.0",
            "expected_return_lower_bound": None,
            "raw_score": 72,
            "risk_reward_ratio": 3,
        },
        {
            "stock_code": "600001",
            "strategy_version": "sector_preheat_v1.0.0",
            "expected_return_lower_bound": None,
            "raw_score": 88,
            "risk_reward_ratio": 3,
        },
    ]
    ordered = sorted(candidates, key=_candidate_competition_order_key)
    assert ordered[0]["stock_code"] == "600001"


def test_sector_position_turns_down_into_dynamic_exit_fact():
    row = {
        "snapshot_at": datetime(2026, 7, 27, 14, 58),
        "direction": "DOWN",
        "score": -35,
        "breadth_pct": -25,
    }
    result = _sector_position_facts(
        _Connection(row),
        theme_code="CONCEPT:创新药",
        trade_date=date(2026, 7, 27),
    )
    assert result["available"] is True
    assert result["broken"] is True
    assert result["strong"] is False


def test_sector_preheat_is_registered_as_research_strategy_manifest():
    config = load_sector_preheat_config()
    manifest = next(
        item
        for item in _strategy_manifests()
        if item["strategy_id"] == "sector_preheat"
    )
    assert manifest["strategy_version"] == config["strategy_version"]
    assert manifest["validation_protocol"]["status"] == "RESEARCH"
