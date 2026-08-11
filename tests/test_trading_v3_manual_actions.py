from datetime import date, datetime
from unittest.mock import MagicMock

import pytest

from server.api import scheduler_runtime
from server.api.routers import trading_v3
from server.trading_v3 import decision_worker
from tools.add_trading_v3_tasks import TASKS as TRADING_V3_TASKS


def _task_engine(task_type: str):
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value
    result = connection.execute.return_value
    result.mappings.return_value.first.return_value = {
        "id": 72,
        "task_name": "manual test",
        "task_type": task_type,
        "script_path": "tools/test.py",
        "script_args": "--mode close",
        "enabled": 1,
    }
    return engine


def test_live_calibrated_universe_matches_backtest_top_n():
    items = []
    for index in range(12):
        item = MagicMock()
        item.raw_score = index / 10
        item.stock_code = f"{index:06d}"
        item.status = "VALIDATED_POSITIVE"
        items.append(item)

    blocked = MagicMock()
    blocked.raw_score = 99.0
    blocked.stock_code = "999999"
    blocked.status = "SETUP_NOT_READY"
    items.append(blocked)

    selected = decision_worker._calibrated_universe(
        "right_side_trend",
        items,
        compatible_calibrations={"right_side_trend": object()},
        config={"calibration": {"top_per_day": 10}},
    )

    assert len(selected) == 10
    assert [item.raw_score for item in selected] == [
        index / 10 for index in range(11, 1, -1)
    ]


@pytest.mark.parametrize(
    ("action_key", "task_type", "expected_args"),
    [
        (
            "daily",
            "trading_v3_close_decision",
            "--mode manual --universe-limit 5000 --per-sleeve-limit 5000",
        ),
    ],
)
def test_manual_actions_are_allow_listed_and_async(
    monkeypatch,
    action_key,
    task_type,
    expected_args,
):
    engine = _task_engine(task_type)
    captured = {}

    def fake_launch(row, **kwargs):
        captured.update(row)
        return {
            "accepted": True,
            "status": "running",
            "task_id": row["id"],
            "task_name": row["task_name"],
        }

    monkeypatch.setattr(trading_v3, "get_engine", lambda: engine)
    monkeypatch.setattr(
        trading_v3,
        "launch_scheduler_task",
        fake_launch,
    )
    result = trading_v3.run_manual_action(action_key)

    assert result["status"] == "running"
    assert result["data"]["real_trading_enabled"] is False
    assert captured["script_args"] == expected_args
    query_params = (
        engine.connect.return_value.__enter__
        .return_value.execute.call_args.args[1]
    )
    assert query_params["task_type"] == task_type


def test_v3_intraday_manual_action_fails_closed():
    with pytest.raises(
        trading_v3.HTTPException,
        match="V3_ONLY_ROUTE",
    ) as raised:
        trading_v3.run_manual_action("intraday")
    assert raised.value.status_code == 409


def test_scheduled_v3_decisions_use_full_liquid_universe():
    decisions = [
        task for task in TRADING_V3_TASKS
        if task["task_type"] in {
            "trading_v3_close_decision",
            "trading_v3_premarket_review",
        }
    ]

    assert len(decisions) == 2
    for task in decisions:
        assert "--universe-limit 5000" in task["script_args"]
        assert "--per-sleeve-limit 5000" in task["script_args"]


def test_async_launcher_uses_database_claim_and_thread(monkeypatch):
    row = {
        "id": 901,
        "task_name": "test task",
        "task_type": "trading_v3_close_decision",
        "enabled": 1,
    }
    started = []

    class FakeThread:
        def __init__(self, **kwargs):
            started.append(kwargs)

        def start(self):
            started.append("started")

    monkeypatch.setattr(
        scheduler_runtime,
        "_claim_task_run",
        lambda task, engine: True,
    )
    monkeypatch.setattr(
        scheduler_runtime.threading,
        "Thread",
        FakeThread,
    )
    scheduler_runtime._running_task_ids.discard(901)
    scheduler_runtime._fast_lane_running_task_ids.discard(901)
    try:
        result = scheduler_runtime.launch_scheduler_task(
            row,
            engine=object(),
        )
        assert result["accepted"] is True
        assert result["status"] == "running"
        assert started[-1] == "started"
    finally:
        scheduler_runtime._running_task_ids.discard(901)
        scheduler_runtime._fast_lane_running_task_ids.discard(901)


@pytest.mark.parametrize(
    ("mode", "resolved_date"),
    [
        ("premarket", date(2026, 7, 28)),
        ("manual", date(2026, 7, 29)),
        ("close", date(2026, 7, 29)),
    ],
)
def test_decision_mode_uses_only_completed_daily_data(
    monkeypatch,
    mode,
    resolved_date,
):
    captured = {}

    class FakeRepository:
        def __init__(self, engine):
            pass

        def active_calibrations(self):
            return {}

        def save_decision(self, **kwargs):
            return {
                "forecast_count": 0,
                "validated_count": 0,
                "target_count": 0,
                "result_hash": "result",
            }

    class FakeDecisionEngine:
        def __init__(self, calibrations):
            pass

        def decide(self, *args, **kwargs):
            return {
                "regime": {
                    "dominant_state": "RANGE",
                    "risk_asset_cap": 0.2,
                },
                "portfolio": {
                    "status": "CASH_OR_ETF_PREFERRED",
                    "targets": [],
                },
            }

    def fake_load(primary, market, *, as_of, limit):
        captured["as_of"] = as_of
        return {
            "feature_time": datetime(2026, 7, 29, 15, 1),
            "trade_date": as_of,
            "stocks": [],
            "market_features": {},
            "data_snapshot_hash": "snapshot",
            "source": "QMT",
        }

    monkeypatch.setattr(
        decision_worker,
        "_previous_trade_date",
        lambda engine, as_of: date(2026, 7, 28),
    )
    monkeypatch.setattr(
        decision_worker,
        "_latest_completed_kline_date",
        lambda engine, as_of: date(2026, 7, 29),
    )
    monkeypatch.setattr(
        decision_worker,
        "load_daily_feature_universe",
        fake_load,
    )
    monkeypatch.setattr(
        decision_worker,
        "TradingV3Repository",
        FakeRepository,
    )
    monkeypatch.setattr(
        decision_worker,
        "TradingV3Engine",
        FakeDecisionEngine,
    )
    monkeypatch.setattr(
        decision_worker,
        "_account_equity",
        lambda engine: 200_000.0,
    )
    monkeypatch.setattr(
        decision_worker,
        "_current_portfolio_state",
        lambda engine, **kwargs: {
            "position_weights": {},
            "position_quantities": {},
            "position_themes": {},
            "theme_weights": {},
            "open_risk_weight": 0.0,
        },
    )
    monkeypatch.setattr(
        decision_worker,
        "sync_position_states",
        lambda *args, **kwargs: {"updated_count": 0},
    )
    monkeypatch.setattr(
        decision_worker,
        "materialize_internal_paper_orders",
        lambda *args, **kwargs: {
            "paper_order_count": 0,
            "real_order_count": 0,
            "created": [],
            "skipped": [],
        },
    )
    monkeypatch.setattr(
        decision_worker,
        "freeze_pending_v3_buys",
        lambda *args, **kwargs: {
            "cancelled_orders": [],
            "cancelled_execution_plans": [],
            "cancelled_partial_orders": [],
        },
    )

    result = decision_worker.run_daily_decision_v3(
        object(),
        as_of=date(2026, 7, 29),
        decision_at=datetime(2026, 7, 29, 9, 15),
        mode=mode,
        kline_engine=object(),
    )

    assert captured["as_of"] == resolved_date
    assert result["trade_date"] == resolved_date.isoformat()
