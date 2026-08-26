from __future__ import annotations

import hashlib
import json
from datetime import date, datetime

import pytest
from sqlalchemy import create_engine, text

from server.trading_v3.decision_truth import (
    DECISION_INTEGRITY_SCHEMA_VERSION,
    DecisionTruthBlocked,
    canonical_forecast_ledger,
    canonical_hash,
    canonical_target_ledger,
    decision_result_hash,
    load_decision_snapshot,
)
from server.trading_v3.decision_worker import _decision_truth_status
from server.trading_v3.paper_execution import (
    _evaluate_cumulative_buy_risk,
)
from server.trading_v3 import paper_execution
from server.trading_v3.repository import TradingV3Repository
from tools.run_trading_v3_decision import (
    execution_enabled_for_request,
    resolve_decision_at,
)


def _snapshot_engine(*, equity: float = 200_000.0, reconciliation="PASS"):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_trade_account_v2 (
                account_id TEXT PRIMARY KEY, status TEXT, cash_balance REAL,
                peak_equity REAL, policy_version TEXT, policy_hash TEXT,
                fee_profile_version TEXT, instrument_rule_version TEXT,
                real_trading_enabled INTEGER, created_at TEXT, updated_at TEXT
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_equity_daily_v2 (
                account_id TEXT, trade_date TEXT, cash_balance REAL,
                market_value REAL, receivables REAL, payables REAL,
                total_equity REAL, peak_equity REAL, drawdown REAL,
                price_snapshot_hash TEXT, created_at TEXT
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_reconciliation_v2 (
                account_id TEXT, trade_date TEXT, version INTEGER,
                status TEXT, cash_difference REAL, equity_difference REAL,
                position_difference INTEGER, order_difference INTEGER,
                fill_difference INTEGER, checks_json TEXT,
                reconciliation_hash TEXT, created_at TEXT
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_position_lot_v2 (
                lot_id TEXT, account_id TEXT, stock_code TEXT,
                theme_code TEXT, remaining_quantity INTEGER,
                cost_price REAL, protective_stop REAL,
                opened_fill_id TEXT, opened_trade_date TEXT,
                settlement_date TEXT, created_at TEXT, closed_at TEXT
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_fill_v2 (fill_id TEXT, order_id TEXT)
        """))
        connection.execute(text("""
            CREATE TABLE st_order_v2 (
                order_id TEXT, intent_id TEXT, account_id TEXT,
                stock_code TEXT, side TEXT, quantity INTEGER,
                filled_quantity INTEGER, status TEXT, limit_price REAL,
                earliest_at TEXT, expires_at TEXT,
                created_at TEXT, updated_at TEXT
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_trade_intent_v2 (
                intent_id TEXT, decision_run_uid TEXT, stock_code TEXT,
                theme_code TEXT, worst_price REAL, protective_stop REAL,
                reason_code TEXT
            )
        """))
        connection.execute(text("""
            INSERT INTO st_trade_account_v2 VALUES (
                'paper-main-v2', 'ACTIVE', 200000, 200000,
                'policy-v1', 'hash', 'fee-v1', 'rule-v1', 0,
                '2026-08-01 09:00:00', '2026-08-05 15:10:00'
            )
        """))
        connection.execute(
            text("""
                INSERT INTO st_equity_daily_v2 VALUES (
                    'paper-main-v2', '2026-08-05', 200000, 0, 0, 0,
                    :equity, 200000, 0, 'prices',
                    '2026-08-05 15:10:00'
                )
            """),
            {"equity": equity},
        )
        connection.execute(
            text("""
                INSERT INTO st_reconciliation_v2 VALUES (
                    'paper-main-v2', '2026-08-05', 1, :status,
                    0, 0, 0, 0, 0, '{}', 'recon-hash',
                    '2026-08-05 15:10:00'
                )
            """),
            {"status": reconciliation},
        )
    return engine


def _load_snapshot(engine):
    return load_decision_snapshot(
        engine,
        requested_as_of=date(2026, 8, 5),
        trade_date=date(2026, 8, 5),
        decision_at=datetime(2026, 8, 5, 16, 5),
        feature_time=datetime(2026, 8, 5, 15, 1),
        data_snapshot_hash="a" * 64,
        data_source="QMT",
        stocks=[],
    )


def _persisted_forecast_row(
    *,
    run_uid: str = "run-risk-reject",
    trade_date: date = date(2026, 8, 5),
) -> dict:
    return {
        "forecast_id": f"{run_uid}-forecast-1",
        "run_uid": run_uid,
        "trade_date": trade_date,
        "rank_no": 1,
        "stock_code": "000001",
        "short_name": "平安银行",
        "strategy_key": "right_side_trend",
        "horizon_days": 5,
        "raw_score": 0.8,
        "expected_return_net_pct": 1.0,
        "return_q10_pct": -2.0,
        "return_q50_pct": 1.0,
        "return_q90_pct": 4.0,
        "probability_positive": 0.6,
        "expected_mae_pct": -2.0,
        "expected_mfe_pct": 3.0,
        "profit_factor": 1.4,
        "payoff_ratio": 1.2,
        "sample_count": 50,
        "confidence": 0.7,
        "forecast_status": "VALIDATED_POSITIVE",
        "theme_code": "BANK",
        "model_version": "dynamic-test-version",
        "dataset_hash": "a" * 64,
        "feature_time": f"{trade_date.isoformat()} 15:00:00",
        "valid_until": f"{trade_date.isoformat()} 23:59:59",
        "initial_stop_pct": -5.0,
        "reasons_json": "[]",
        "features_json": "{}",
        "created_at": f"{trade_date.isoformat()} 16:05:00",
    }


def _verified_portfolio_json(
    *,
    trade_date: date = date(2026, 8, 5),
    equity: float = 200_000.0,
    valuation_prices: dict[str, float] | None = None,
    run_uid: str = "run-risk-reject",
    target_rows: list[dict] | None = None,
) -> tuple[str, str]:
    manifest = {
        "schema_version": "probiga.trading-v3.decision-snapshot.v1",
        "requested_as_of": trade_date.isoformat(),
        "trade_date": trade_date.isoformat(),
        "decision_at": f"{trade_date.isoformat()} 16:05:00",
        "knowledge_cutoff_at": f"{trade_date.isoformat()} 16:05:00",
        "feature_time": f"{trade_date.isoformat()} 15:01:00",
        "account": {"account_id": "paper-main-v2"},
        "equity": {"total_equity": equity},
        "reconciliation": {
            "status": "PASS",
            "reconciliation_hash": "recon-1",
        },
        "positions": [],
        "open_orders": [],
        "valuation_prices": dict(valuation_prices or {}),
        "section_hashes": {},
    }
    manifest["manifest_hash"] = canonical_hash(manifest)
    target_rows = list(target_rows or [])
    forecast_rows = [
        _persisted_forecast_row(run_uid=run_uid, trade_date=trade_date)
    ]
    targets = [
        {
            "stock_code": row["stock_code"],
            "stock_name": row.get("short_name") or "",
            "target_weight": row["target_weight"],
            "target_value": row["target_value"],
            "target_quantity": row["target_quantity"],
            "estimated_roundtrip_cost_pct": row[
                "estimated_roundtrip_cost_pct"
            ],
            "expected_return_net_pct": row["expected_return_net_pct"],
            "conservative_return_pct": row["conservative_return_pct"],
            "expected_mae_pct": row["expected_mae_pct"],
            "theme_code": row.get("theme_code") or "",
            "theme_codes": json.loads(row.get("theme_codes_json") or "[]"),
            "strategy_keys": json.loads(
                row.get("strategy_keys_json") or "[]"
            ),
            "primary_strategy_key": row.get("primary_strategy_key") or "",
            "reason": row.get("reason") or "",
        }
        for row in target_rows
    ]
    portfolio = {
        "targets": targets,
        "decision_snapshot": manifest,
        "decision_truth": {
            "schema_version": "probiga.trading-v3.decision-truth.v1",
            "run_status": "COMPLETED",
            "actionable_status": "PAPER_ACTIONABLE",
            "decision_scope": "INTERNAL_PAPER_TRIAL",
            "paper_order_authority": "V2_GATED",
            "execution_authority": "V2_CANONICAL_LEDGER",
            "order_authority": False,
            "real_order_allowed": False,
        },
        "decision_integrity": {
            "schema_version": DECISION_INTEGRITY_SCHEMA_VERSION,
            "forecast_count": 1,
            "forecast_ledger_hash": canonical_hash(
                canonical_forecast_ledger(forecast_rows)
            ),
            "raw_theme_signal_count": 0,
            "persisted_theme_signal_count": 0,
            "hypothesis_count": 0,
            "target_count": len(target_rows),
            "target_ledger_hash": canonical_hash(
                canonical_target_ledger(
                    target_rows,
                    run_uid=run_uid,
                    trade_date=trade_date,
                    persisted=True,
                )
            ),
        },
    }
    regime = {"dominant_state": "RISK_ON"}
    return json.dumps(portfolio), decision_result_hash(
        regime=regime,
        portfolio=portfolio,
        forecast_count=1,
        theme_signal_count=0,
        hypothesis_count=0,
    )


def _persisted_target_row() -> dict:
    return {
        "run_uid": "run-risk-reject",
        "trade_date": date(2026, 8, 5),
        "rank_no": 1,
        "stock_code": "000001",
        "short_name": "平安银行",
        "reason": "VALIDATED_POSITIVE",
        "status": "PLANNED",
        "target_quantity": 1_000,
        "target_value": 10_000.0,
        "target_weight": 0.05,
        "initial_stop_pct": -5.0,
        "expected_return_net_pct": 1.0,
        "conservative_return_pct": 0.5,
        "estimated_roundtrip_cost_pct": 0.2,
        "expected_mae_pct": -2.0,
        "theme_code": "BANK",
        "theme_codes_json": '["BANK"]',
        "strategy_keys_json": '["right_side_trend"]',
        "primary_strategy_key": "right_side_trend",
        "primary_forecast_id": "forecast-1",
        "attribution_snapshot_hash": hashlib.sha256(
            (
                "run-risk-reject|forecast-1|000001|"
                "right_side_trend|dynamic-test-version:right_side_trend"
            ).encode("utf-8")
        ).hexdigest(),
    }


def test_decision_snapshot_is_deterministic_and_binds_account_truth():
    engine = _snapshot_engine()

    first = _load_snapshot(engine)
    second = _load_snapshot(engine)

    assert first["equity"] == 200_000.0
    assert first["manifest"] == second["manifest"]
    assert len(first["manifest"]["manifest_hash"]) == 64
    assert first["manifest"]["reconciliation"]["status"] == "PASS"
    assert first["manifest"]["decision_at"] == "2026-08-05 16:05:00"


@pytest.mark.parametrize(
    ("equity", "reconciliation", "error_code"),
    [
        (0.0, "PASS", "EQUITY_NOT_POSITIVE"),
        (200_000.0, "RECONCILIATION_BLOCKED", "RECONCILIATION_BLOCKED"),
    ],
)
def test_decision_snapshot_fails_closed_without_valid_capital_truth(
    equity,
    reconciliation,
    error_code,
):
    engine = _snapshot_engine(
        equity=equity,
        reconciliation=reconciliation,
    )

    with pytest.raises(DecisionTruthBlocked, match=error_code):
        _load_snapshot(engine)


def test_historical_decision_clock_is_reproducible():
    now = datetime(2026, 8, 16, 12, 34, 56)

    assert resolve_decision_at(
        as_of=date(2026, 8, 5),
        mode="close",
        as_of_was_explicit=True,
        now=now,
    ) == datetime(2026, 8, 5, 16, 5)
    assert resolve_decision_at(
        as_of=date(2026, 8, 5),
        mode="premarket",
        as_of_was_explicit=True,
        now=now,
    ) == datetime(2026, 8, 5, 9, 15)


def test_decision_clock_rejects_cross_date_override_and_historical_execution():
    with pytest.raises(ValueError, match="must equal the requested as_of"):
        resolve_decision_at(
            as_of=date(2026, 8, 5),
            mode="close",
            as_of_was_explicit=True,
            explicit="2026-08-16T16:05:00+08:00",
        )

    assert execution_enabled_for_request(
        as_of=date(2026, 8, 5),
        decision_at=datetime(2026, 8, 5, 16, 5),
        today=date(2026, 8, 16),
    ) is False
    assert execution_enabled_for_request(
        as_of=date(2026, 8, 16),
        decision_at=datetime(2026, 8, 16, 16, 5),
        today=date(2026, 8, 16),
    ) is True


def test_run_truth_does_not_report_data_block_as_completed_or_actionable():
    assert _decision_truth_status(
        {
            "regime": {"quality_status": "BLOCKED"},
            "portfolio": {"status": "DATA_BLOCKED", "targets": []},
        },
        execution_enabled=True,
        forecast_count=1,
    ) == ("BLOCKED", "DATA_BLOCKED")
    assert _decision_truth_status(
        {
            "regime": {"quality_status": "PASS"},
            "portfolio": {"status": "READY", "targets": [{"id": 1}]},
        },
        execution_enabled=False,
        forecast_count=1,
    ) == ("COMPLETED", "REPLAY_ONLY")


def test_empty_forecast_ledger_blocks_but_nonempty_zero_target_is_valid():
    no_target_result = {
        "regime": {"quality_status": "PASS"},
        "portfolio": {"status": "READY", "targets": []},
    }

    assert _decision_truth_status(
        no_target_result,
        execution_enabled=True,
        forecast_count=0,
    ) == ("BLOCKED", "DATA_BLOCKED")
    assert _decision_truth_status(
        no_target_result,
        execution_enabled=True,
        forecast_count=12,
    ) == ("COMPLETED", "NO_ACTION")


def test_cumulative_risk_reserves_cash_and_rejects_the_next_order():
    first = _evaluate_cumulative_buy_risk(
        requested_quantity=1_000,
        worst_price=10.0,
        initial_stop=9.5,
        current_code_value=0,
        current_total_value=0,
        current_theme_value=0,
        current_open_risk_cny=0,
        available_cash_cny=15_000,
        current_turnover_cny=0,
        equity_cny=100_000,
        creates_new_position=True,
        live_position_count=0,
        maximum_live_positions=12,
        maximum_single_weight=0.12,
        maximum_total_weight=0.50,
        maximum_theme_weight=0.25,
        maximum_open_risk_weight=0.02,
        maximum_daily_turnover_weight=0.30,
    )
    second = _evaluate_cumulative_buy_risk(
        requested_quantity=1_000,
        worst_price=10.0,
        initial_stop=9.5,
        current_code_value=0,
        current_total_value=first["reserved_notional"],
        current_theme_value=0,
        current_open_risk_cny=first["post_open_risk_cny"],
        available_cash_cny=first["post_cash"],
        current_turnover_cny=first["reserved_notional"],
        equity_cny=100_000,
        creates_new_position=True,
        live_position_count=1,
        maximum_live_positions=12,
        maximum_single_weight=0.12,
        maximum_total_weight=0.50,
        maximum_theme_weight=0.25,
        maximum_open_risk_weight=0.02,
        maximum_daily_turnover_weight=0.30,
    )

    assert first["decision_status"] == "APPROVED"
    assert first["post_cash"] == 5_000
    assert second["decision_status"] == "REJECTED"
    assert second["approved_quantity"] == 0
    assert second["first_failure"] == "CASH_AVAILABLE"


@pytest.mark.parametrize(
    "tamper",
    ["target", "forecast", "result_hash", "schema_v1"],
)
def test_persisted_decision_integrity_rejects_relational_drift(tamper):
    target = _persisted_target_row()
    portfolio_json, result_hash = _verified_portfolio_json(
        target_rows=[target],
    )
    persisted_target = dict(target)
    persisted_forecast = _persisted_forecast_row()
    if tamper == "target":
        persisted_target["target_quantity"] = 2_000
    if tamper == "forecast":
        persisted_forecast["raw_score"] = -999
    if tamper == "result_hash":
        result_hash = "0" * 64
    if tamper == "schema_v1":
        portfolio = json.loads(portfolio_json)
        portfolio["decision_integrity"]["schema_version"] = (
            "probiga.trading-v3.decision-integrity.v1"
        )
        portfolio["decision_integrity"].pop("forecast_ledger_hash", None)
        portfolio_json = json.dumps(portfolio)
        result_hash = decision_result_hash(
            regime={"dominant_state": "RISK_ON"},
            portfolio=portfolio,
            forecast_count=1,
            theme_signal_count=0,
            hypothesis_count=0,
        )

    class Result:
        def __init__(self, row):
            self.row = row

        def mappings(self):
            return self

        def first(self):
            return self.row

        def all(self):
            return list(self.row or []) if isinstance(self.row, list) else []

    class Connection:
        def execute(self, statement, params=None):
            sql = str(statement)
            if "FROM st_equity_daily_v2" in sql:
                return Result({"total_equity": 200_000.0})
            if "FROM st_reconciliation_v2" in sql:
                return Result({
                    "status": "PASS",
                    "reconciliation_hash": "recon-1",
                    "created_at": datetime(2026, 8, 5, 15, 10),
                })
            if "AS forecast_count" in sql:
                return Result({
                    "forecast_count": 1,
                    "theme_signal_count": 0,
                    "hypothesis_count": 0,
                })
            if "FROM st_alpha_forecast_v3" in sql:
                return Result([persisted_forecast])
            raise AssertionError(sql)

    run = {
        "run_uid": "run-risk-reject",
        "trade_date": date(2026, 8, 5),
        "status": "COMPLETED",
        "lifecycle_status": "PAPER_TRIAL",
        "forecast_count": 1,
        "target_count": 1,
        "regime_json": json.dumps({"dominant_state": "RISK_ON"}),
        "portfolio_json": portfolio_json,
        "result_hash": result_hash,
    }
    _portfolio, _equity, verified, reason = (
        paper_execution._verify_persisted_decision_truth(
            Connection(),
            account={
                "status": "ACTIVE",
                "cash_balance": 200_000.0,
                "real_trading_enabled": 0,
                "updated_at": datetime(2026, 8, 5, 15, 10),
            },
            run=run,
            targets=[persisted_target],
            account_id="paper-main-v2",
            now=datetime(2026, 8, 5, 16, 5),
        )
    )

    assert verified is False
    assert reason == (
        "V3_DECISION_INTEGRITY_V2_REQUIRED"
        if tamper == "schema_v1"
        else "V3_DECISION_RESULT_OR_TARGET_LEDGER_UNVERIFIED"
    )


def test_next_trade_session_never_falls_back_to_calendar_day():
    class Result:
        def scalar(self):
            return None

    class Connection:
        def execute(self, statement, params=None):
            return Result()

    with pytest.raises(
        RuntimeError,
        match="V3_NEXT_TRADE_SESSION_UNAVAILABLE",
    ):
        paper_execution._next_trade_date(
            Connection(),
            date(2026, 8, 7),
        )


@pytest.mark.parametrize(
    "tamper_target",
    [False, True],
    ids=["risk-rejected", "target-ledger-tampered"],
)
def test_materializer_persists_rejected_risk_without_creating_order(
    monkeypatch,
    tamper_target,
):
    target_row = _persisted_target_row()
    portfolio_json, result_hash = _verified_portfolio_json(
        valuation_prices={"600000": 20.0},
        target_rows=[target_row],
    )
    persisted_target = dict(target_row)
    persisted_forecast = _persisted_forecast_row()
    if tamper_target:
        persisted_target["target_quantity"] = 2_000

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
                self.inserts.append((sql, params or {}))
                return Result()
            if "SELECT *" in sql and "FROM st_trade_account_v2" in sql:
                return Result(first={
                    "account_id": "paper-main-v2",
                    "status": "ACTIVE",
                    "cash_balance": 1_000.0,
                    "real_trading_enabled": 0,
                    "updated_at": datetime(2026, 8, 5, 15, 10),
                })
            if "FROM st_decision_run_v3" in sql:
                return Result(first={
                    "run_uid": "run-risk-reject",
                    "trade_date": date(2026, 8, 5),
                    "status": "COMPLETED",
                    "lifecycle_status": "PAPER_TRIAL",
                    "model_version": "dynamic-test-version",
                    "risk_asset_cap": 0.50,
                    "regime_json": json.dumps({
                        "dominant_state": "RISK_ON"
                    }),
                    "forecast_count": 1,
                    "target_count": 1,
                    "result_hash": result_hash,
                    "portfolio_json": portfolio_json,
                })
            if "FROM st_equity_daily_v2" in sql:
                return Result(first={
                    "total_equity": 200_000.0,
                    "cash_balance": 1_000.0,
                })
            if "FROM st_reconciliation_v2" in sql:
                return Result(first={
                    "status": "PASS",
                    "reconciliation_hash": "recon-1",
                    "created_at": datetime(2026, 8, 5, 15, 10),
                })
            if "FROM st_target_portfolio_v3" in sql:
                return Result(all_rows=[persisted_target])
            if "AS forecast_count" in sql:
                return Result(first={
                    "forecast_count": 1,
                    "theme_signal_count": 0,
                    "hypothesis_count": 0,
                })
            if "FROM st_alpha_forecast_v3" in sql:
                return Result(all_rows=[persisted_forecast])
            if "SUM(ABS(gross_amount))" in sql:
                return Result(scalar=1_500.0)
            if (
                "SELECT stock_code, theme_code, remaining_quantity" in sql
                and "FROM st_position_lot_v2" in sql
            ):
                return Result(all_rows=[{
                    "stock_code": "600000",
                    "theme_code": "INDUSTRIAL",
                    "remaining_quantity": 100,
                    "cost_price": 1.0,
                    "protective_stop": 18.0,
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
            return Result(all_rows=[])

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
        "_sync_v3_execution_plan_states",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        paper_execution,
        "_canonical_v2_buy_receipt",
        lambda *args, **kwargs: ({"receipt": "ok"}, ""),
    )
    monkeypatch.setattr(
        paper_execution,
        "_canonical_governance_buy_receipt",
        lambda *args, **kwargs: ({
            "strategy_key": "right_side_trend",
            "strategy_version": "dynamic-test-version:right_side_trend",
            "target_bp": 500,
            "new_buy_allowed": True,
            "real_order_authority": False,
        }, ""),
    )
    real_risk_evaluator = paper_execution._evaluate_cumulative_buy_risk
    captured_risk_inputs = {}

    def capture_risk_inputs(**kwargs):
        captured_risk_inputs.update(kwargs)
        return real_risk_evaluator(**kwargs)

    monkeypatch.setattr(
        paper_execution,
        "_evaluate_cumulative_buy_risk",
        capture_risk_inputs,
    )

    result = paper_execution.materialize_internal_paper_orders(
        Engine(),
        run_uid="run-risk-reject",
    )

    if tamper_target:
        assert result["created"] == []
        assert result["decision_integrity_verified"] is False
        assert result["skipped"][-1]["reason"] == (
            "DECISION_RESULT_OR_TARGET_LEDGER_UNVERIFIED"
        )
        assert not any(
            "INSERT IGNORE INTO st_trade_intent_v2" in sql
            or "INSERT IGNORE INTO st_risk_decision_v2" in sql
            or "INSERT IGNORE INTO st_order_v2" in sql
            for sql, _params in connection.inserts
        )
        return

    risk_params = next(
        params
        for sql, params in connection.inserts
        if "INSERT IGNORE INTO st_risk_decision_v2" in sql
    )
    intent_params = next(
        params
        for sql, params in connection.inserts
        if "INSERT IGNORE INTO st_trade_intent_v2" in sql
    )
    intent_evidence = json.loads(intent_params["evidence_json"])
    assert intent_evidence["model_version"] == "dynamic-test-version"
    assert intent_evidence["primary_strategy_key"] == "right_side_trend"
    assert intent_evidence["primary_strategy_version"] == (
        "dynamic-test-version:right_side_trend"
    )
    assert risk_params["decision_status"] == "REJECTED"
    assert risk_params["approved_quantity"] == 0
    assert risk_params["first_failure"] == "CASH_AVAILABLE"
    assert captured_risk_inputs["current_total_value"] == 2_000.0
    assert captured_risk_inputs["current_turnover_cny"] == 1_500.0
    assert result["created"] == []
    assert result["skipped"][-1]["status"] == "RISK_REJECTED"
    assert not any(
        "INSERT IGNORE INTO st_order_v2" in sql
        for sql, _params in connection.inserts
    )


@pytest.mark.parametrize("has_requested_as_of", [True, False])
def test_repository_persists_run_and_actionable_status_separately(
    has_requested_as_of,
):
    class Result:
        rowcount = 1

        def scalar(self):
            return 1 if has_requested_as_of else None

        def mappings(self):
            return self

        def all(self):
            return []

    class Connection:
        def __init__(self):
            self.calls = []

        def execute(self, statement, params=None):
            self.calls.append((str(statement), params or {}))
            return Result()

    connection = Connection()

    class Context:
        def __enter__(self):
            return connection

        def __exit__(self, *args):
            return False

    class Engine:
        dialect = type("Dialect", (), {"name": "mysql"})()

        def begin(self):
            return Context()

    repository = TradingV3Repository(Engine())
    saved = repository.save_decision(
        run_uid="run-blocked",
        trade_date=date(2026, 8, 5),
        requested_as_of=date(2026, 8, 5),
        decision_at=datetime(2026, 8, 5, 16, 5),
        mode="close",
        model_version="test",
        lifecycle_status="PAPER_TRIAL",
        regime={"dominant_state": "DATA_BLOCKED", "risk_asset_cap": 0},
        portfolio={"status": "DATA_BLOCKED", "targets": []},
        forecasts=[],
        data_snapshot_hash="a" * 64,
        run_status="BLOCKED",
        actionable_status="DATA_BLOCKED",
        snapshot_manifest={"manifest_hash": "b" * 64},
        defer_completion=True,
    )

    insert_sql, insert = next(
        (sql, params)
        for sql, params in connection.calls
        if "INSERT INTO st_decision_run_v3" in sql
    )
    portfolio = json.loads(insert["portfolio_json"])
    assert insert["status"] == "PROCESSING"
    assert insert["requested_as_of"] == date(2026, 8, 5)
    assert ("run_uid, trade_date, requested_as_of," in insert_sql) is (
        has_requested_as_of
    )
    assert (
        ":run_uid, :trade_date, :requested_as_of," in insert_sql
    ) is has_requested_as_of
    assert portfolio["decision_snapshot"]["requested_as_of"] == "2026-08-05"
    assert portfolio["decision_truth"]["run_status"] == "BLOCKED"
    assert portfolio["decision_truth"]["actionable_status"] == "DATA_BLOCKED"
    assert portfolio["decision_truth"]["order_authority"] is False
    assert portfolio["decision_truth"]["paper_order_authority"] == "NONE"
    assert portfolio["decision_truth"]["execution_authority"] == (
        "V2_CANONICAL_LEDGER"
    )
    integrity_sql, integrity_params = next(
        (sql, params)
        for sql, params in connection.calls
        if "SET portfolio_json = :portfolio_json" in sql
    )
    assert "WHERE run_uid = :run_uid" in integrity_sql
    persisted_portfolio = json.loads(integrity_params["portfolio_json"])
    persisted_integrity = persisted_portfolio["decision_integrity"]
    assert persisted_integrity["schema_version"] == (
        DECISION_INTEGRITY_SCHEMA_VERSION
    )
    assert persisted_integrity["forecast_count"] == 0
    assert persisted_integrity["forecast_ledger_hash"] == canonical_hash([])
    assert saved["result_hash"] == integrity_params["result_hash"]
    assert saved["run_status"] == "BLOCKED"
    assert saved["persisted_run_status"] == "PROCESSING"
    repository.finalize_run("run-blocked", status="BLOCKED")
    finalize_sql, finalize_params = connection.calls[-1]
    assert "AND status = 'PROCESSING'" in finalize_sql
    assert finalize_params["status"] == "BLOCKED"
    repository.mark_run_failed(
        "run-blocked",
        stage="ORDER_MATERIALIZATION",
        error="boom",
    )
    update_sql, update_params = connection.calls[-1]
    assert "SET status = 'FAILED'" in update_sql
    assert update_params["run_uid"] == "run-blocked"
    assert update_params["error_message"].startswith(
        "ORDER_MATERIALIZATION: boom"
    )
