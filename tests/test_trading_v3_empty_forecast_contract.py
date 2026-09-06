from __future__ import annotations

import json
from datetime import date, datetime

from server.common import scheduler_validation
from server.api import scheduler_runtime
from server.trading_v3 import decision_worker
from server.trading_v3.decision_truth import (
    DECISION_INTEGRITY_SCHEMA_VERSION,
    canonical_forecast_ledger,
    canonical_hash,
    canonical_target_ledger,
    decision_result_hash,
)
from tools import run_trading_v3_decision


SCHEMA = "probiga.trading-v3-decision-result.v1"
RUN_UID = "a" * 32


def _forecast_rows(count: int) -> list[dict]:
    return [
        {
            "forecast_id": f"forecast-{index}",
            "run_uid": RUN_UID,
            "trade_date": date(2026, 8, 26),
            "rank_no": index,
            "stock_code": f"{index:06d}",
            "short_name": f"股票{index}",
            "strategy_key": "right_side_trend",
            "horizon_days": 5,
            "raw_score": float(index),
            "expected_return_net_pct": 1.0,
            "return_q10_pct": -1.0,
            "return_q50_pct": 1.0,
            "return_q90_pct": 3.0,
            "probability_positive": 0.6,
            "expected_mae_pct": -2.0,
            "expected_mfe_pct": 3.0,
            "profit_factor": 1.2,
            "payoff_ratio": 1.1,
            "sample_count": 50,
            "confidence": 0.7,
            "forecast_status": "VALIDATED_POSITIVE",
            "theme_code": "TEST",
            "model_version": "test-v2",
            "dataset_hash": "d" * 64,
            "feature_time": "2026-08-26 15:00:00",
            "valid_until": "2026-08-26 23:59:59",
            "initial_stop_pct": -5.0,
            "reasons_json": "[]",
            "features_json": "{}",
            "created_at": "2026-08-26 22:05:00",
        }
        for index in range(1, count + 1)
    ]


def _result_payload(**overrides) -> dict:
    payload = {
        "schema": SCHEMA,
        "status": "ok",
        "run_uid": RUN_UID,
        "run_status": "COMPLETED",
        "actionable_status": "NO_ACTION",
        "trade_date": "2026-08-26",
        "forecast_count": 12,
        "target_count": 0,
    }
    payload.update(overrides)
    payload.setdefault("retryable", payload["run_status"] == "BLOCKED")
    return payload


def _persisted_run_row(
    *,
    forecast_count: int = 12,
    target_rows: list[dict] | None = None,
    target_ledger_hash: str = "",
    completed_at: datetime | None = None,
) -> dict:
    targets = list(target_rows or [])
    forecasts = _forecast_rows(forecast_count)
    integrity = {
        "schema_version": DECISION_INTEGRITY_SCHEMA_VERSION,
        "forecast_count": forecast_count,
        "forecast_ledger_hash": canonical_hash(
            canonical_forecast_ledger(forecasts)
        ),
        "raw_theme_signal_count": 0,
        "persisted_theme_signal_count": 0,
        "hypothesis_count": 0,
        "target_count": len(targets),
        "target_ledger_hash": target_ledger_hash or canonical_hash(
            canonical_target_ledger(
                targets,
                run_uid=RUN_UID,
                trade_date=date(2026, 8, 26),
                persisted=True,
            )
        ),
    }
    regime = {"dominant_state": "RISK_ON"}
    portfolio = {
        "status": "READY",
        "targets": [],
        "decision_integrity": integrity,
    }
    return {
        "run_uid": RUN_UID,
        "trade_date": date(2026, 8, 26),
        "status": "COMPLETED" if forecast_count else "BLOCKED",
        "forecast_count": forecast_count,
        "target_count": len(targets),
        "actual_forecast_count": forecast_count,
        "actual_target_count": len(targets),
        "actual_theme_signal_count": 0,
        "actual_hypothesis_count": 0,
        "regime_json": json.dumps(regime),
        "portfolio_json": json.dumps(portfolio),
        "result_hash": decision_result_hash(
            regime=regime,
            portfolio=portfolio,
            forecast_count=forecast_count,
            theme_signal_count=0,
            hypothesis_count=0,
        ),
        "completed_at": completed_at
        or datetime(2026, 8, 26, 22, 5, 30),
        "_forecast_rows": forecasts,
    }


def _v3_db_reader(run_row: dict, target_rows: list[dict] | None = None):
    def read(_engine, sql, params=None):
        assert params == {"run_uid": RUN_UID}
        if "FROM st_decision_run_v3" in sql:
            return [run_row]
        if "FROM st_target_portfolio_v3" in sql:
            return list(target_rows or [])
        if "FROM st_alpha_forecast_v3" in sql:
            return list(run_row.get("_forecast_rows") or [])
        raise AssertionError(sql)

    return read


def test_scheduler_accepts_nonempty_forecast_zero_target_decision() -> None:
    output = "provider log\n" + json.dumps(
        _result_payload(), ensure_ascii=False, indent=2
    )

    assert scheduler_validation.scheduler_output_status(
        {"task_type": "trading_v3_close_decision"},
        output,
        return_code=0,
    ) == "success"


def test_scheduler_retries_exact_empty_forecast_receipt() -> None:
    payload = _result_payload(
        status="blocked",
        run_status="BLOCKED",
        actionable_status="DATA_BLOCKED",
        forecast_count=0,
    )

    assert scheduler_validation.scheduler_output_status(
        {"task_type": "trading_v3_close_decision"},
        json.dumps(payload),
        return_code=2,
    ) == "failed"


def test_scheduler_rejects_empty_forecast_success_or_ambiguous_identity() -> None:
    forged = _result_payload(forecast_count=0)
    assert scheduler_validation.scheduler_output_status(
        {"task_type": "trading_v3_close_decision"},
        json.dumps(forged),
        return_code=0,
    ) == "failed"

    for key, value in (
        ("run_uid", "not-a-run"),
        ("trade_date", "2026-02-30"),
        ("forecast_count", "12"),
        ("target_count", 13),
    ):
        invalid = _result_payload(**{key: value})
        assert scheduler_validation.scheduler_output_status(
            {"task_type": "trading_v3_premarket_review"},
            json.dumps(invalid),
            return_code=0,
        ) == "failed"


def test_scheduler_binds_v3_receipt_to_exact_persisted_run(monkeypatch) -> None:
    completed_at = datetime(2026, 8, 26, 22, 5, 30)
    output = json.dumps(_result_payload())

    monkeypatch.setattr(
        scheduler_validation,
        "_read_all",
        _v3_db_reader(_persisted_run_row(completed_at=completed_at)),
    )
    result = scheduler_validation.validate_scheduler_task_result(
        {"task_type": "trading_v3_close_decision"},
        engine=object(),
        started_at=datetime(2026, 8, 26, 22, 5),
        output=output,
    )

    assert result.checked is True
    assert result.ok is True
    assert RUN_UID in result.message
    assert "forecast_count=12" in result.message


def test_scheduler_release_v3_receipt_requires_exact_replay_only_safety(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        scheduler_validation,
        "_read_all",
        _v3_db_reader(
            _persisted_run_row(
                completed_at=datetime(2026, 8, 27, 3, 5, 30)
            )
        ),
    )
    safe_payload = _result_payload(
        actionable_status="REPLAY_ONLY",
        decision_at="2026-08-26 16:05:00",
        mode="close",
        execution_enabled=False,
        paper_order_count=0,
        paper_orders=[],
        real_order_count=0,
        position_state_updates=0,
    )
    task = {
        "task_type": "trading_v3_close_decision",
        "_trigger_source": "release_catchup",
        "_release_target_date": "2026-08-26",
    }

    result = scheduler_validation.validate_scheduler_task_result(
        task,
        engine=object(),
        started_at=datetime(2026, 8, 27, 3, 5),
        output=json.dumps(safe_payload),
    )
    assert result.checked is True
    assert result.ok is True

    unsafe_payload = {**safe_payload, "execution_enabled": True}
    result = scheduler_validation.validate_scheduler_task_result(
        task,
        engine=object(),
        started_at=datetime(2026, 8, 27, 3, 5),
        output=json.dumps(unsafe_payload),
    )
    assert result.checked is True
    assert result.ok is False
    assert "release replay date/safety contract differs" in result.message


def test_scheduler_rejects_v3_receipt_count_mismatch(monkeypatch) -> None:
    monkeypatch.setattr(
        scheduler_validation,
        "_read_all",
        _v3_db_reader(_persisted_run_row(forecast_count=0)),
    )
    result = scheduler_validation.validate_scheduler_task_result(
        {"task_type": "trading_v3_close_decision"},
        engine=object(),
        started_at=datetime(2026, 8, 26, 22, 5),
        output=json.dumps(_result_payload()),
    )

    assert result.checked is True
    assert result.ok is False
    assert "db_forecasts=0" in result.message


def test_scheduler_rejects_v3_target_ledger_hash_mismatch(
    monkeypatch,
) -> None:
    targets = [{
        "rank_no": 1,
        "stock_code": "300059",
        "short_name": "东方财富",
        "target_quantity": 100,
        "strategy_keys_json": '["momentum"]',
        "theme_codes_json": "[]",
    }]
    run_row = _persisted_run_row(
        target_rows=targets,
        target_ledger_hash=canonical_hash([]),
    )
    monkeypatch.setattr(
        scheduler_validation,
        "_read_all",
        _v3_db_reader(run_row, targets),
    )
    result = scheduler_validation.validate_scheduler_task_result(
        {"task_type": "trading_v3_close_decision"},
        engine=object(),
        started_at=datetime(2026, 8, 26, 22, 5),
        output=json.dumps(_result_payload(target_count=1)),
    )

    assert result.checked is True
    assert result.ok is False
    assert "target ledger differs" in result.message


def test_v3_cli_returns_nonzero_for_empty_forecast_block(
    monkeypatch,
    capsys,
) -> None:
    class Disposable:
        def dispose(self):
            pass

    monkeypatch.setattr(
        "sys.argv",
        ["run_trading_v3_decision.py", "--mode", "close"],
    )
    monkeypatch.setattr(run_trading_v3_decision, "load_project_env", lambda: None)
    monkeypatch.setattr(
        run_trading_v3_decision, "create_tool_engine", Disposable
    )
    monkeypatch.setattr(run_trading_v3_decision, "get_kline_engine", Disposable)
    monkeypatch.setattr(
        run_trading_v3_decision,
        "run_daily_decision_v3",
        lambda *_args, **_kwargs: _result_payload(
            status="blocked",
            run_status="BLOCKED",
            actionable_status="DATA_BLOCKED",
            forecast_count=0,
        ),
    )
    monkeypatch.setattr(
        "biz.analysis.trading_wecom.notify_v3_decision_result",
        lambda _result: {"status": "skipped"},
    )

    assert run_trading_v3_decision.main() == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == SCHEMA
    assert payload["forecast_count"] == 0
    assert payload["status"] == "blocked"


def test_v3_release_replay_flag_disables_execution_even_for_current_date(
    monkeypatch,
    capsys,
) -> None:
    class Disposable:
        def dispose(self):
            pass

    observed = {}

    def run(*_args, **kwargs):
        observed.update(kwargs)
        return _result_payload(
            actionable_status="REPLAY_ONLY",
            decision_at="2026-08-27 16:05:00",
            mode="close",
            execution_enabled=False,
            paper_order_count=0,
            paper_orders=[],
            real_order_count=0,
            position_state_updates=0,
        )

    monkeypatch.setattr(
        "sys.argv",
        [
            "run_trading_v3_decision.py",
            "--mode",
            "close",
            "--as-of",
            "2026-08-27",
            "--decision-at",
            "2026-08-27T16:05:00",
            "--replay-only",
        ],
    )
    monkeypatch.setattr(run_trading_v3_decision, "load_project_env", lambda: None)
    monkeypatch.setattr(
        run_trading_v3_decision,
        "create_tool_engine",
        Disposable,
    )
    monkeypatch.setattr(run_trading_v3_decision, "get_kline_engine", Disposable)
    monkeypatch.setattr(
        run_trading_v3_decision,
        "execution_enabled_for_request",
        lambda **_kwargs: True,
    )
    monkeypatch.setattr(run_trading_v3_decision, "run_daily_decision_v3", run)
    monkeypatch.setattr(
        "biz.analysis.trading_wecom.notify_v3_decision_result",
        lambda _result: {"status": "skipped"},
    )

    assert run_trading_v3_decision.main() == 0
    assert observed["as_of"] == date(2026, 8, 27)
    assert observed["decision_at"] == datetime(2026, 8, 27, 16, 5)
    assert observed["execution_enabled"] is False
    payload = json.loads(capsys.readouterr().out)
    assert payload["paper_order_count"] == 0


def test_v3_cli_retrospective_research_uses_split_clocks_and_never_notifies(
    monkeypatch,
    capsys,
) -> None:
    class Disposable:
        def __init__(self):
            self.disposed = False

        def dispose(self):
            self.disposed = True

    primary = Disposable()
    kline = Disposable()
    observed = {}

    def run(*_args, **kwargs):
        observed.update(kwargs)
        return {
            "schema": "probiga.trading-v3-retrospective-research.v1",
            "status": "ok",
            "run_status": "COMPLETED",
            "actionable_status": "REPLAY_ONLY",
            "persisted": False,
        }

    monkeypatch.setattr(
        "sys.argv",
        [
            "run_trading_v3_decision.py",
            "--mode",
            "close",
            "--as-of",
            "2026-09-03",
            "--retrospective-research",
        ],
    )
    monkeypatch.setattr(run_trading_v3_decision, "load_project_env", lambda: None)
    monkeypatch.setattr(
        run_trading_v3_decision, "create_tool_engine", lambda: primary
    )
    monkeypatch.setattr(
        run_trading_v3_decision, "get_kline_engine", lambda: kline
    )
    monkeypatch.setattr(
        run_trading_v3_decision,
        "current_shanghai_time",
        lambda: datetime(2026, 9, 6, 21, 30),
    )
    monkeypatch.setattr(
        run_trading_v3_decision,
        "run_retrospective_research_v3",
        run,
    )
    monkeypatch.setattr(
        "biz.analysis.trading_wecom.notify_v3_decision_result",
        lambda _result: (_ for _ in ()).throw(
            AssertionError("retrospective research must not notify")
        ),
    )

    assert run_trading_v3_decision.main() == 0
    assert observed["as_of"] == date(2026, 9, 3)
    assert observed["decision_at"] == datetime(
        2026, 9, 3, 23, 59, 59, 999999
    )
    assert observed["research_known_at"] == datetime(2026, 9, 6, 21, 30)
    assert primary.disposed is True
    assert kline.disposed is True
    payload = json.loads(capsys.readouterr().out)
    assert payload["persisted"] is False
    assert payload["notification"] == {
        "status": "suppressed",
        "reason": "RETROSPECTIVE_RESEARCH",
    }


def test_scheduler_records_a_blocked_v3_receipt_as_retryable_failure(
    monkeypatch,
    tmp_path,
) -> None:
    payload = _result_payload(
        status="blocked",
        run_status="BLOCKED",
        actionable_status="DATA_BLOCKED",
        forecast_count=0,
    )

    class Process:
        returncode = 2

        def communicate(self, timeout=None):
            return json.dumps(payload), ""

        def poll(self):
            return self.returncode

    updates = []
    validations = []
    script = tmp_path / "decision.py"
    script.write_text("pass", encoding="utf-8")
    monkeypatch.setattr(
        scheduler_runtime, "resolve_scheduler_script", lambda *_args: script
    )
    monkeypatch.setattr(scheduler_runtime, "_build_task_args", lambda *_args: [])
    monkeypatch.setattr(scheduler_runtime, "build_child_env", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(scheduler_runtime.subprocess, "Popen", lambda *_args, **_kwargs: Process())
    monkeypatch.setattr(
        scheduler_runtime,
        "update_scheduler_task",
        lambda _engine, _task_id, values, **_kwargs: updates.append(values),
    )
    monkeypatch.setattr(scheduler_runtime, "_task_history_finish", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        scheduler_runtime,
        "validate_scheduler_task_result",
        lambda *args, **kwargs: (
            validations.append((args, kwargs))
            or scheduler_validation.SchedulerValidationResult(
                checked=True,
                ok=True,
                message="exact blocked receipt",
            )
        ),
    )

    scheduler_runtime._run_task_impl(
        {
            "id": 130,
            "task_name": "V3收盘正期望决策",
            "task_type": "trading_v3_close_decision",
            "script_path": "tools/run_trading_v3_decision.py",
            "script_args": "--mode close",
        },
        tmp_path,
        object(),
        history_run_uid="f" * 32,
    )

    assert validations == []
    assert updates[-1]["last_run_status"] == "failed"


def test_decision_worker_persists_empty_forecast_as_blocked(monkeypatch) -> None:
    class PrimaryEngine:
        dialect = type("Dialect", (), {"name": "sqlite"})()

    class FakeRepository:
        instance = None

        def __init__(self, _engine):
            self.saved = None
            self.finalized = None
            FakeRepository.instance = self

        def active_calibrations(self):
            return {}

        def strategy_learning_summary(self, _strategy_key):
            return {}

        def save_decision(self, **kwargs):
            self.saved = kwargs
            return {
                "run_uid": kwargs["run_uid"],
                "forecast_count": len(kwargs["forecasts"]),
                "validated_count": 0,
                "target_count": len(kwargs["portfolio"]["targets"]),
                "theme_signal_count": 0,
                "hypothesis_count": 0,
            }

        def finalize_run(self, run_uid, *, status):
            self.finalized = (run_uid, status)

    class FakeDecisionEngine:
        def __init__(self, _calibrations):
            pass

        def evaluate_stock_with_theme_signals(self, *_args):
            return (), ()

        def decide(self, *_args, **_kwargs):
            return {
                "regime": {
                    "quality_status": "PASS",
                    "dominant_state": "NORMAL",
                    "risk_asset_cap": 0.5,
                },
                "strategy_weights": {},
                "portfolio": {
                    "status": "READY",
                    "targets": [],
                    "opportunity_audit": {},
                },
            }

    class Regime:
        quality_status = "BLOCKED"

    dataset = {
        "trade_date": date(2026, 8, 26),
        "feature_time": datetime(2026, 8, 26, 15, 0),
        "data_snapshot_hash": "d" * 64,
        "source": "QMT",
        "stocks": [{
            "stock_code": "000001",
            "stock_name": "平安银行",
            "price": 10.0,
        }],
        "market_features": {},
    }
    monkeypatch.setattr(decision_worker, "TradingV3Repository", FakeRepository)
    monkeypatch.setattr(decision_worker, "TradingV3Engine", FakeDecisionEngine)
    monkeypatch.setattr(
        decision_worker,
        "load_v3_config",
        lambda: {
            "strategy_version": "test-v3",
            "lifecycle_status": "PAPER_TRIAL",
            "paper_discovery": {"enabled": False},
        },
    )
    monkeypatch.setattr(
        decision_worker,
        "load_daily_feature_universe",
        lambda _primary, _market, *, as_of, limit: dataset,
    )
    monkeypatch.setattr(
        decision_worker,
        "load_decision_snapshot",
        lambda *_args, **_kwargs: {
            "equity": 200_000.0,
            "portfolio_state": {
                "theme_weights": {},
                "position_weights": {},
                "position_quantities": {},
                "position_themes": {},
                "paper_discovery_codes": set(),
                "open_risk_weight": 0.0,
            },
            "manifest": {"manifest_hash": "m" * 64},
        },
    )
    monkeypatch.setattr(
        decision_worker,
        "classify_regime_probabilities",
        lambda _features: Regime(),
    )
    monkeypatch.setattr(
        decision_worker.ShadowIntelligenceRepository,
        "release_audit",
        lambda _self: {"status": "PASS"},
    )
    monkeypatch.setattr(
        decision_worker,
        "sync_position_states",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("empty forecasts must not rewrite position state")
        ),
    )
    monkeypatch.setattr(
        decision_worker,
        "materialize_internal_paper_orders",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("empty forecasts must not materialize orders")
        ),
    )

    result = decision_worker.run_daily_decision_v3(
        PrimaryEngine(),
        as_of=date(2026, 8, 26),
        decision_at=datetime(2026, 8, 26, 22, 5),
        mode="close",
        kline_engine=object(),
        execution_enabled=True,
    )

    repository = FakeRepository.instance
    assert result["schema"] == SCHEMA
    assert result["status"] == "blocked"
    assert result["run_status"] == "BLOCKED"
    assert result["actionable_status"] == "DATA_BLOCKED"
    assert result["retryable"] is True
    assert result["forecast_count"] == 0
    assert result["portfolio_status"] == "DATA_BLOCKED"
    assert repository.saved["run_status"] == "BLOCKED"
    assert repository.saved["portfolio"]["opportunity_audit"][
        "forecast_ledger"
    ]["reason"] == "FORECAST_LEDGER_EMPTY"
    assert repository.finalized == (result["run_uid"], "BLOCKED")


def test_retrospective_research_exports_candidates_without_any_writer(
    monkeypatch,
) -> None:
    class PrimaryEngine:
        dialect = type("Dialect", (), {"name": "mysql"})()

    class FakeRepository:
        def __init__(self, _engine):
            pass

        def active_calibrations(self):
            return {}

        def strategy_learning_summary(self, _strategy_key):
            return {}

        def save_decision(self, **_kwargs):
            raise AssertionError("research must not save a decision")

        def finalize_run(self, *_args, **_kwargs):
            raise AssertionError("research must not finalize a decision")

        def mark_run_failed(self, *_args, **_kwargs):
            raise AssertionError("research must not mutate a decision run")

    class Forecast:
        stock_code = "000001"
        stock_name = "平安银行"
        strategy_key = "oversold_reversal"
        status = "VALIDATED_POSITIVE"
        expected_return_net_pct = 1.2
        raw_score = 0.8
        feature_time = datetime(2026, 9, 3, 15)
        valid_until = datetime(2026, 10, 3, 15)

        def as_dict(self):
            return {
                "stock_code": self.stock_code,
                "stock_name": self.stock_name,
                "strategy_key": self.strategy_key,
                "status": self.status,
                "expected_return_net_pct": self.expected_return_net_pct,
                "raw_score": self.raw_score,
                "reasons": ["历史日终事实通过当前模型研究评估"],
            }

    class FakeDecisionEngine:
        def __init__(self, _calibrations):
            pass

        def evaluate_stock_with_theme_signals(self, *_args):
            return (Forecast(),), ()

        def decide(self, *_args, **_kwargs):
            raise AssertionError("research must not load portfolio/account state")

    class Regime:
        quality_status = "PASS"
        risk_asset_cap = 0.5

        def as_dict(self):
            return {
                "quality_status": "PASS",
                "dominant_state": "NORMAL",
                "risk_asset_cap": self.risk_asset_cap,
            }

    class Consensus:
        def as_dict(self):
            return {
                "stock_code": "000001",
                "stock_name": "平安银行",
                "strategy_keys": ["oversold_reversal"],
                "reasons": ["当前模型对历史日终事实的研究候选"],
            }

    observed = {}

    def load_features(
        _primary,
        _market,
        *,
        as_of,
        context_cutoff_at,
        limit,
        required_codes=(),
        allow_research_industry_last_known=False,
    ):
        observed.update({
            "as_of": as_of,
            "context_cutoff_at": context_cutoff_at,
            "limit": limit,
            "required_codes": required_codes,
            "allow_research_industry_last_known": (
                allow_research_industry_last_known
            ),
        })
        return {
            "trade_date": date(2026, 9, 3),
            "feature_time": datetime(2026, 9, 3, 15),
            "data_snapshot_hash": "d" * 64,
            "source": "QMT",
            "concept_snapshot_date": "2026-09-02",
            "industry_pit": {
                "target_snapshot_date": "2026-09-03",
                "source_snapshot_date": "2026-09-02",
                "retrospective_last_known": True,
            },
            "stocks": [{
                "stock_code": "000001",
                "stock_name": "平安银行",
                "price": 10.0,
            }],
            "market_features": {
                "pit_fact_cutoff_at": "2026-09-03 23:59:59.999999",
                "pit_decision_at": "2026-09-06 21:30:00",
                "pit_common_receipt_root_hash": "r" * 64,
                "pit_reconstruction_mode": "HISTORICAL_RECONSTRUCTION",
                "pit_reconstruction_sha256": "q" * 64,
                "pit_reconstructed_at": "2026-09-06 21:20:00",
                "pit_reconstruction_provenance": {"provider": "QMT"},
                "concept_snapshot_age_days": 1.0,
            },
            "theme_count": 1,
        }

    monkeypatch.setattr(decision_worker, "TradingV3Repository", FakeRepository)
    monkeypatch.setattr(decision_worker, "TradingV3Engine", FakeDecisionEngine)
    monkeypatch.setattr(decision_worker, "load_daily_feature_universe", load_features)
    monkeypatch.setattr(
        decision_worker,
        "load_decision_snapshot",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("research must not load mutable account truth")
        ),
    )
    monkeypatch.setattr(
        decision_worker,
        "load_v3_config",
        lambda: {
            "strategy_version": "current-test-v3",
            "lifecycle_status": "PAPER_TRIAL",
            "paper_discovery": {"enabled": False},
        },
    )
    monkeypatch.setattr(
        decision_worker,
        "classify_regime_probabilities",
        lambda _features: Regime(),
    )
    monkeypatch.setattr(
        decision_worker,
        "strategy_weights_for_regime",
        lambda _regime: {"oversold_reversal": 1.0},
    )
    monkeypatch.setattr(
        decision_worker,
        "build_consensus",
        lambda forecasts, **_kwargs: (
            (Consensus(),) if tuple(forecasts) else ()
        ),
    )
    monkeypatch.setattr(
        decision_worker,
        "build_market_hypothesis",
        lambda **_kwargs: object(),
    )
    monkeypatch.setattr(
        decision_worker,
        "build_stock_hypotheses",
        lambda *_args, **_kwargs: (),
    )
    monkeypatch.setattr(
        decision_worker.ShadowIntelligenceRepository,
        "release_audit",
        lambda _self: {"status": "PASS"},
    )
    for writer_name in (
        "_open_position_codes",
        "freeze_pending_v3_buys",
        "sync_position_states",
        "materialize_internal_paper_orders",
    ):
        monkeypatch.setattr(
            decision_worker,
            writer_name,
            lambda *_args, _name=writer_name, **_kwargs: (_ for _ in ()).throw(
                AssertionError(f"research must not call {_name}")
            ),
        )

    result = decision_worker.run_retrospective_research_v3(
        PrimaryEngine(),
        as_of=date(2026, 9, 3),
        decision_at=datetime(2026, 9, 3, 23, 59, 59, 999999),
        research_known_at=datetime(2026, 9, 6, 21, 30),
        mode="close",
        kline_engine=object(),
    )

    assert observed["context_cutoff_at"] == datetime(2026, 9, 6, 21, 30)
    assert observed["required_codes"] == ()
    assert observed["allow_research_industry_last_known"] is True
    assert result["schema"] == "probiga.trading-v3-retrospective-research.v1"
    assert result["persisted"] is False
    assert "run_uid" not in result
    assert result["canonical_eligible"] is False
    assert result["competition_eligible"] is False
    assert result["order_authority"] is False
    assert result["paper_order_count"] == 0
    artifact = result["research_artifact"]
    assert artifact["historical_fact_cutoff_at"] == (
        "2026-09-03 23:59:59.999999"
    )
    assert artifact["research_known_at"] == "2026-09-06 21:30:00"
    assert artifact["historical_production_decision"] is False
    assert artifact["model_evaluation"]["historical_model_identity_proven"] is False
    assert artifact["research_assumptions"] == {
        "account_snapshot_consumed": False,
        "position_state_consumed": False,
        "open_order_state_consumed": False,
        "paper_learning_consumed": False,
        "portfolio_allocation_computed": False,
    }
    assert artifact["pit_evidence"]["industry"][
        "retrospective_last_known"
    ] is True
    assert artifact["consensus"][0]["stock_code"] == "000001"
    assert artifact["forecasts"][0]["stock_code"] == "000001"
