from __future__ import annotations

import json
from datetime import date

import pytest
from sqlalchemy import create_engine, text

from server.api.routers import trading_v3
from server.trading_v3.decision_truth import (
    DECISION_INTEGRITY_SCHEMA_VERSION,
    canonical_forecast_ledger,
    canonical_hash,
    canonical_target_ledger,
    decision_result_hash,
)
from server.trading_v3.repository import (
    TradingV3Repository,
    _stock_pool_action_plan,
)


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


def test_unverified_legacy_run_cannot_masquerade_as_a_readable_stock_pool():
    repository = _pre_layer4_repository()
    with repository.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO st_alpha_forecast_v3 (
                    forecast_id, run_uid, rank_no, stock_code, short_name,
                    strategy_key, raw_score, forecast_status, reasons_json
                ) VALUES
                    ('f-target', 'pre-layer4-run', 2, '000002', '目标二号',
                     'right_side_trend', 80, 'VALIDATED_POSITIVE', '[]'),
                    ('f-watch', 'pre-layer4-run', 1, '000001', '观察一号',
                     'right_side_trend', 82, 'RESEARCH_SAMPLE', '[]')
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO st_target_portfolio_v3 (run_uid, rank_no, stock_code)
                VALUES ('pre-layer4-run', 1, '000002')
                """
            )
        )

    pool = repository.stock_pool(trade_date=date(2026, 8, 19))

    assert pool["run_uid"] is None
    assert pool["pool_status"] == "UNAVAILABLE"
    assert pool["pool_readable"] is False
    assert pool["decision_integrity_verified"] is False
    assert pool["items"] == []
    assert pool["reason_codes"] == [
        "NO_VERIFIED_COMPLETED_DECISION_RUN",
    ]


def _verified_pool_repository() -> TradingV3Repository:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_decision_run_v3 (
                run_uid TEXT PRIMARY KEY,
                trade_date TEXT NOT NULL,
                requested_as_of TEXT,
                decision_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                completed_at TEXT,
                status TEXT NOT NULL,
                regime_json TEXT NOT NULL,
                portfolio_json TEXT NOT NULL,
                forecast_count INTEGER NOT NULL,
                target_count INTEGER NOT NULL,
                result_hash TEXT NOT NULL
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_alpha_forecast_v3 (
                forecast_id TEXT,
                run_uid TEXT,
                trade_date TEXT,
                rank_no INTEGER,
                stock_code TEXT,
                short_name TEXT,
                strategy_key TEXT,
                horizon_days INTEGER,
                raw_score REAL,
                expected_return_net_pct REAL,
                return_q10_pct REAL,
                return_q50_pct REAL,
                return_q90_pct REAL,
                probability_positive REAL,
                expected_mae_pct REAL,
                expected_mfe_pct REAL,
                profit_factor REAL,
                payoff_ratio REAL,
                sample_count INTEGER,
                confidence REAL,
                forecast_status TEXT,
                theme_code TEXT,
                model_version TEXT,
                dataset_hash TEXT,
                feature_time TEXT,
                valid_until TEXT,
                initial_stop_pct REAL,
                reasons_json TEXT,
                features_json TEXT,
                created_at TEXT
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_target_portfolio_v3 (
                run_uid TEXT, rank_no INTEGER, stock_code TEXT
            )
        """))
        connection.execute(text(
            "CREATE TABLE st_theme_signal_v3 (run_uid TEXT)"
        ))
        connection.execute(text(
            "CREATE TABLE st_trade_hypothesis_v3 (run_uid TEXT)"
        ))
    return TradingV3Repository(engine)


def _insert_pool_run(
    repository: TradingV3Repository,
    *,
    run_uid: str,
    requested_as_of: date,
    status: str,
    forecasts: list[tuple[int, str]],
    forecast_status: str = "PAPER_DISCOVERY_CANDIDATE",
    corrupt_result_hash: bool = False,
) -> None:
    source_date = requested_as_of
    decision_at = f"{requested_as_of.isoformat()} 18:00:00"
    forecast_rows = [
        {
            "forecast_id": f"{run_uid}-{stock_code}",
            "run_uid": run_uid,
            "trade_date": source_date,
            "rank_no": rank_no,
            "stock_code": stock_code,
            "short_name": f"股票{stock_code}",
            "strategy_key": "right_side_trend",
            "horizon_days": 5,
            "raw_score": 80,
            "expected_return_net_pct": None,
            "return_q10_pct": None,
            "return_q50_pct": None,
            "return_q90_pct": None,
            "probability_positive": None,
            "expected_mae_pct": None,
            "expected_mfe_pct": None,
            "profit_factor": None,
            "payoff_ratio": None,
            "sample_count": 0,
            "confidence": 0.5,
            "forecast_status": forecast_status,
            "theme_code": "",
            "model_version": "test-v2",
            "dataset_hash": "a" * 64,
            "feature_time": f"{source_date.isoformat()} 15:00:00",
            "valid_until": f"{source_date.isoformat()} 23:59:59",
            "initial_stop_pct": -5,
            "reasons_json": "[]",
            "features_json": "{}",
            "created_at": decision_at,
        }
        for rank_no, stock_code in forecasts
    ]
    integrity = {
        "schema_version": DECISION_INTEGRITY_SCHEMA_VERSION,
        "forecast_count": len(forecasts),
        "forecast_ledger_hash": canonical_hash(
            canonical_forecast_ledger(forecast_rows)
        ),
        "raw_theme_signal_count": 0,
        "persisted_theme_signal_count": 0,
        "hypothesis_count": 0,
        "target_count": 0,
        "target_ledger_hash": canonical_hash(canonical_target_ledger(
            [],
            run_uid=run_uid,
            trade_date=source_date,
            persisted=True,
        )),
    }
    portfolio = {
        "decision_snapshot": {
            "requested_as_of": requested_as_of.isoformat(),
        },
        "decision_integrity": integrity,
        "targets": [],
    }
    result_hash = decision_result_hash(
        regime={},
        portfolio=portfolio,
        forecast_count=len(forecasts),
        theme_signal_count=0,
        hypothesis_count=0,
    )
    if corrupt_result_hash:
        result_hash = "0" * 64
    with repository.engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO st_decision_run_v3 (
                run_uid, trade_date, requested_as_of, decision_at, created_at,
                completed_at, status, regime_json, portfolio_json,
                forecast_count, target_count, result_hash
            ) VALUES (
                :run_uid, :trade_date, :requested_as_of, :decision_at,
                :decision_at, :decision_at, :status, :regime_json,
                :portfolio_json, :forecast_count, 0, :result_hash
            )
        """), {
            "run_uid": run_uid,
            "trade_date": source_date.isoformat(),
            "requested_as_of": requested_as_of,
            "decision_at": decision_at,
            "status": status,
            "regime_json": json.dumps({}),
            "portfolio_json": json.dumps(portfolio),
            "forecast_count": len(forecasts),
            "result_hash": result_hash,
        })
        for row in forecast_rows:
            connection.execute(text("""
                INSERT INTO st_alpha_forecast_v3 (
                    forecast_id, run_uid, trade_date, rank_no, stock_code,
                    short_name, strategy_key, horizon_days, raw_score,
                    expected_return_net_pct, return_q10_pct, return_q50_pct,
                    return_q90_pct, probability_positive, expected_mae_pct,
                    expected_mfe_pct, profit_factor, payoff_ratio,
                    sample_count, confidence, forecast_status, theme_code,
                    model_version, dataset_hash, feature_time, valid_until,
                    initial_stop_pct, reasons_json, features_json, created_at
                ) VALUES (
                    :forecast_id, :run_uid, :trade_date, :rank_no, :stock_code,
                    :short_name, :strategy_key, :horizon_days, :raw_score,
                    :expected_return_net_pct, :return_q10_pct, :return_q50_pct,
                    :return_q90_pct, :probability_positive, :expected_mae_pct,
                    :expected_mfe_pct, :profit_factor, :payoff_ratio,
                    :sample_count, :confidence, :forecast_status, :theme_code,
                    :model_version, :dataset_hash, :feature_time, :valid_until,
                    :initial_stop_pct, :reasons_json, :features_json, :created_at
                )
            """), row)


def test_stock_pool_skips_partial_and_corrupt_runs_for_latest_readable_batch(
    monkeypatch,
):
    repository = _verified_pool_repository()
    _insert_pool_run(
        repository,
        run_uid="verified-older",
        requested_as_of=date(2026, 8, 25),
        status="COMPLETED",
        forecasts=[(2, "000002"), (1, "000001")],
    )
    _insert_pool_run(
        repository,
        run_uid="partial-newer",
        requested_as_of=date(2026, 8, 26),
        status="PROCESSING",
        forecasts=[(1, "000003")],
    )
    _insert_pool_run(
        repository,
        run_uid="corrupt-newest",
        requested_as_of=date(2026, 8, 27),
        status="COMPLETED",
        forecasts=[(1, "000004")],
        corrupt_result_hash=True,
    )

    exact_partial = repository.stock_pool(trade_date=date(2026, 8, 26))
    latest = repository.stock_pool()

    assert exact_partial["run_uid"] is None
    assert exact_partial["pool_status"] == "UNAVAILABLE"
    assert latest["run_uid"] == "verified-older"
    assert latest["run_status"] == "COMPLETED"
    assert latest["decision_integrity_verified"] is True
    assert latest["pool_readable"] is True
    assert latest["pool_status"] == "READY"
    assert [row["rank_no"] for row in latest["items"]] == [1, 2]
    assert [row["stock_code"] for row in latest["items"]] == [
        "000001",
        "000002",
    ]

    monkeypatch.setattr(trading_v3, "_repo", lambda: repository)
    api_payload = trading_v3.stock_pool(trade_date=date(2026, 8, 26))
    assert api_payload["data"]["pool_status"] == "UNAVAILABLE"
    assert api_payload["data"]["items"] == []


def test_verified_completed_zero_candidate_pool_is_explicit_empty():
    repository = _verified_pool_repository()
    _insert_pool_run(
        repository,
        run_uid="verified-empty",
        requested_as_of=date(2026, 8, 28),
        status="COMPLETED",
        forecasts=[],
    )

    pool = repository.stock_pool(trade_date=date(2026, 8, 28))

    assert pool["run_uid"] == "verified-empty"
    assert pool["pool_readable"] is True
    assert pool["decision_integrity_verified"] is True
    assert pool["pool_status"] == "EMPTY"
    assert pool["summary"]["strategy_candidate_count"] == 0
    assert pool["items"] == []


def test_monitoring_only_insufficient_forecast_is_never_a_pool_candidate():
    repository = _verified_pool_repository()
    _insert_pool_run(
        repository,
        run_uid="verified-monitoring-only",
        requested_as_of=date(2026, 8, 28),
        status="COMPLETED",
        forecasts=[(1, "000003")],
        forecast_status="INSUFFICIENT_DATA",
    )

    pool = repository.stock_pool(trade_date=date(2026, 8, 28))

    assert pool["pool_readable"] is True
    assert pool["pool_status"] == "EMPTY"
    assert pool["summary"]["strategy_candidate_count"] == 0
    assert pool["items"][0]["stock_code"] == "000003"
    assert pool["items"][0]["is_strategy_candidate"] is False
    assert pool["items"][0]["actionability"] == "RESEARCH_ONLY"


def test_stock_pool_before_session_date_selects_latest_strictly_older_run():
    repository = _verified_pool_repository()
    _insert_pool_run(
        repository,
        run_uid="verified-older",
        requested_as_of=date(2026, 8, 25),
        status="COMPLETED",
        forecasts=[(1, "000001")],
    )
    _insert_pool_run(
        repository,
        run_uid="verified-later",
        requested_as_of=date(2026, 8, 27),
        status="COMPLETED",
        forecasts=[(1, "000002")],
    )

    pool = repository.stock_pool(
        before_session_date=date(2026, 8, 26)
    )

    assert pool["run_uid"] == "verified-older"
    assert pool["decision_session_date"] == "2026-08-25"
    assert pool["requested_trade_date"] == "2026-08-26"
    assert pool["before_session_date"] == "2026-08-26"
    assert pool["is_historical_fallback"] is True
    assert pool["historical_read_only"] is True
    assert pool["historical_fallback_status"] == "HISTORICAL_READ_ONLY"
    assert pool["historical_fallback_session_date"] == "2026-08-25"


def test_stock_pool_exposes_daily_changes_and_strategy_execution() -> None:
    repository = _verified_pool_repository()
    _insert_pool_run(
        repository,
        run_uid="verified-previous",
        requested_as_of=date(2026, 8, 27),
        status="COMPLETED",
        forecasts=[(1, "000001"), (2, "000002")],
    )
    _insert_pool_run(
        repository,
        run_uid="verified-current",
        requested_as_of=date(2026, 8, 28),
        status="COMPLETED",
        forecasts=[(1, "000002"), (2, "000003")],
    )

    pool = repository.stock_pool(trade_date=date(2026, 8, 28))
    by_code = {row["stock_code"]: row for row in pool["items"]}

    assert by_code["000002"]["daily_change"] == "UPGRADED"
    assert by_code["000003"]["daily_change"] == "NEW"
    assert pool["daily_change"]["removed_stock_codes"] == ["000001"]
    assert pool["daily_change"]["previous_run_uid"] == "verified-previous"
    assert pool["summary"]["daily_new_count"] == 1
    assert pool["summary"]["daily_removed_count"] == 1
    assert pool["strategy_execution"]["strategy_count"] == 1
    assert pool["strategy_execution"]["strategies"][0]["status"] == (
        "COMPLETED_WITH_CANDIDATES"
    )


def test_stock_pool_rejects_mutually_exclusive_date_filters():
    repository = _verified_pool_repository()

    with pytest.raises(ValueError, match="mutually exclusive"):
        repository.stock_pool(
            trade_date=date(2026, 8, 26),
            before_session_date=date(2026, 8, 26),
        )


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("raw_score", -999),
        ("strategy_key", "tampered_strategy"),
        ("reasons_json", '["tampered"]'),
        ("features_json", '{"tampered":true}'),
    ],
)
def test_stock_pool_fails_closed_after_forecast_content_mutation(
    column,
    value,
):
    repository = _verified_pool_repository()
    _insert_pool_run(
        repository,
        run_uid="verified-content",
        requested_as_of=date(2026, 8, 28),
        status="COMPLETED",
        forecasts=[(1, "000001")],
    )
    with repository.engine.begin() as connection:
        connection.execute(
            text(
                f"""
                UPDATE st_alpha_forecast_v3
                SET {column} = :value
                WHERE run_uid = 'verified-content'
                """
            ),
            {"value": value},
        )

    pool = repository.stock_pool(trade_date=date(2026, 8, 28))

    assert pool["run_uid"] is None
    assert pool["pool_status"] == "UNAVAILABLE"
    assert pool["decision_integrity_verified"] is False


def test_decision_integrity_v1_run_is_never_verified_ready():
    repository = _verified_pool_repository()
    _insert_pool_run(
        repository,
        run_uid="legacy-v1",
        requested_as_of=date(2026, 8, 28),
        status="COMPLETED",
        forecasts=[(1, "000001")],
    )
    with repository.engine.begin() as connection:
        row = connection.execute(
            text(
                """
                SELECT regime_json, portfolio_json
                FROM st_decision_run_v3
                WHERE run_uid = 'legacy-v1'
                """
            )
        ).mappings().first()
        portfolio = json.loads(row["portfolio_json"])
        portfolio["decision_integrity"]["schema_version"] = (
            "probiga.trading-v3.decision-integrity.v1"
        )
        portfolio["decision_integrity"].pop("forecast_ledger_hash", None)
        result_hash = decision_result_hash(
            regime=json.loads(row["regime_json"]),
            portfolio=portfolio,
            forecast_count=1,
            theme_signal_count=0,
            hypothesis_count=0,
        )
        connection.execute(
            text(
                """
                UPDATE st_decision_run_v3
                SET portfolio_json=:portfolio_json, result_hash=:result_hash
                WHERE run_uid='legacy-v1'
                """
            ),
            {
                "portfolio_json": json.dumps(portfolio),
                "result_hash": result_hash,
            },
        )

    metadata = repository.latest_run_metadata(date(2026, 8, 28))
    pool = repository.stock_pool(trade_date=date(2026, 8, 28))

    assert metadata["decision_integrity_verified"] is False
    assert metadata["decision_integrity_reason"] == (
        "DECISION_INTEGRITY_V2_REQUIRED"
    )
    assert pool["run_uid"] is None
    assert pool["pool_status"] == "UNAVAILABLE"


def test_stock_pool_paper_target_never_receives_invented_buy_or_sell_ranges():
    plan = _stock_pool_action_plan(
        run_uid="immutable-run",
        forecasts=[{
            "rank_no": 1,
            "strategy_key": "quality_momentum",
            "forecast_status": "PAPER_DISCOVERY_CANDIDATE",
            "raw_score": 0.86,
            "sample_count": 0,
            "initial_stop_pct": -5,
            "features": {"price": 16.86},
            "feature_time": "2026-08-19 15:00:00",
        }],
        target={
            "primary_strategy_key": "quality_momentum",
            "reason": "PAPER_DISCOVERY: uncalibrated forward trial",
        },
        rejection=None,
    )

    assert plan["source"] == "V3_IMMUTABLE_RUN"
    assert plan["actionability"] == "PAPER_ONLY"
    assert plan["buy_range"] is None
    assert plan["sell_range"] is None
    assert plan["protective_stop"] == 16.017
    assert plan["range_status"] == "NOT_GENERATED"


def test_stock_pool_calibrated_target_uses_only_run_native_price_and_returns():
    plan = _stock_pool_action_plan(
        run_uid="immutable-run",
        forecasts=[{
            "rank_no": 2,
            "strategy_key": "right_side_trend",
            "forecast_status": "VALIDATED_POSITIVE",
            "expected_return_net_pct": 3.0,
            "return_q50_pct": 3.0,
            "return_q90_pct": 6.0,
            "sample_count": 40,
            "initial_stop_pct": -4.0,
            "features": {"price": 100.0},
            "feature_time": "2026-08-19 15:00:00",
        }],
        target={
            "primary_strategy_key": "right_side_trend",
            "reason": "validated target",
        },
        rejection=None,
    )

    assert plan["actionability"] == "BUY_ZONE"
    assert plan["buy_range"] == {"low": 99.5, "high": 100.5}
    assert plan["sell_range"] == {"low": 103.0, "high": 106.0}
    assert plan["protective_stop"] == 96.0
    assert plan["range_status"] == "READY"
