from __future__ import annotations

import json
from datetime import date

from sqlalchemy import create_engine, text

from server.api.routers import trading_v3
from server.trading_v3.repository import TradingV3Repository


def _pre_layer4_repository() -> TradingV3Repository:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE st_decision_run_v3 (
                    run_uid TEXT PRIMARY KEY,
                    trade_date TEXT NOT NULL,
                    decision_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    regime_json TEXT NOT NULL,
                    portfolio_json TEXT NOT NULL
                )
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TABLE st_alpha_forecast_v3 (
                    forecast_id TEXT,
                    run_uid TEXT,
                    rank_no INTEGER,
                    stock_code TEXT,
                    short_name TEXT,
                    strategy_key TEXT,
                    raw_score REAL,
                    expected_return_net_pct REAL,
                    probability_positive REAL,
                    confidence REAL,
                    forecast_status TEXT,
                    theme_code TEXT,
                    valid_until TEXT,
                    reasons_json TEXT
                )
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TABLE st_target_portfolio_v3 (
                    run_uid TEXT,
                    rank_no INTEGER,
                    stock_code TEXT
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO st_decision_run_v3 (
                    run_uid, trade_date, decision_at, created_at, status,
                    regime_json, portfolio_json
                ) VALUES (
                    :run_uid, :trade_date, :decision_at, :created_at, :status,
                    :regime_json, :portfolio_json
                )
                """
            ),
            {
                "run_uid": "pre-layer4-run",
                "trade_date": "2026-08-18",
                "decision_at": "2026-08-20 09:00:00",
                "created_at": "2026-08-20 09:00:00",
                "status": "COMPLETED",
                "regime_json": "{}",
                "portfolio_json": json.dumps(
                    {
                        "decision_snapshot": {
                            "requested_as_of": "2026-08-19",
                        },
                    }
                ),
            },
        )
    return TradingV3Repository(engine)


def test_historical_v3_pages_read_the_pre_layer4_schema(monkeypatch):
    repository = _pre_layer4_repository()
    requested = date(2026, 8, 19)
    monkeypatch.setattr(trading_v3, "_repo", lambda: repository)

    context = trading_v3.decision_context(trade_date=requested)
    overview = trading_v3.overview(compact=True, trade_date=requested)
    targets = trading_v3.latest_portfolio(trade_date=requested)

    assert context["data"]["run_uid"] == "pre-layer4-run"
    assert context["data"]["decision_session_date"] == "2026-08-19"
    assert overview["data"]["run"]["run_uid"] == "pre-layer4-run"
    assert targets["data"] == []


def test_historical_run_uses_the_layer4_column_when_present():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE st_decision_run_v3 (
                    run_uid TEXT PRIMARY KEY,
                    trade_date TEXT NOT NULL,
                    requested_as_of TEXT,
                    decision_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    regime_json TEXT NOT NULL,
                    portfolio_json TEXT NOT NULL
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO st_decision_run_v3 (
                    run_uid, trade_date, requested_as_of, decision_at,
                    created_at, regime_json, portfolio_json
                ) VALUES (
                    'layer4-run', '2026-08-18', '2026-08-19',
                    '2026-08-20 09:00:00', '2026-08-20 09:00:00', '{}', '{}'
                )
                """
            )
        )

    run = TradingV3Repository(engine).latest_run_metadata(date(2026, 8, 19))

    assert run is not None
    assert run["run_uid"] == "layer4-run"
    assert str(run["requested_as_of"]) == "2026-08-19"
