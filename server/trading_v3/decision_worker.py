from __future__ import annotations

import uuid
import inspect
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from server.common.kline_data import get_kline_engine
from server.common.trading_v3_maintenance import trading_v3_writer

from .config import load_v3_config
from .daily_features import load_daily_feature_universe
from .decision_truth import load_decision_snapshot
from .engine import TradingV3Engine
from .hypotheses import (
    build_market_hypothesis,
    build_stock_hypotheses,
)
from .paper_execution import (
    freeze_pending_v3_buys,
    materialize_internal_paper_orders,
)
from .position_sync import sync_position_states
from .regime import classify_regime_probabilities
from .repository import TradingV3Repository
from .shadow_intelligence_repository import ShadowIntelligenceRepository


_CALIBRATABLE_FORECAST_STATUSES = frozenset({
    "VALIDATED_POSITIVE",
    "RESEARCH_ONLY_PROFIT_GATE_FAILED",
    "RESEARCH_ONLY_SCORE_OUT_OF_RANGE",
})


def _calibrated_universe(
    sleeve: str,
    items: list[Any],
    *,
    compatible_calibrations: dict[str, Any],
    config: dict[str, Any],
) -> list[Any]:
    if (
        sleeve != "right_side_trend"
        or sleeve not in compatible_calibrations
    ):
        return items
    limit = int(
        config.get("calibration", {}).get("top_per_day", 10)
    )
    eligible = [
        item
        for item in items
        if item.status in _CALIBRATABLE_FORECAST_STATUSES
    ]
    return sorted(
        eligible,
        key=lambda item: (
            -float(item.raw_score or 0),
            item.stock_code,
        ),
    )[:limit]


def _account_equity(engine: Engine) -> float:
    with engine.connect() as connection:
        row = connection.execute(
            text(
                """
                SELECT a.status AS account_status,
                       e.total_equity, e.trade_date,
                       r.status AS reconciliation_status
                FROM st_trade_account_v2 a
                JOIN st_equity_daily_v2 e
                  ON e.account_id = a.account_id
                LEFT JOIN st_reconciliation_v2 r
                  ON r.account_id = e.account_id
                 AND r.trade_date = e.trade_date
                 AND r.version = (
                     SELECT MAX(r2.version)
                     FROM st_reconciliation_v2 r2
                     WHERE r2.account_id = e.account_id
                       AND r2.trade_date = e.trade_date
                 )
                WHERE a.account_id = 'paper-main-v2'
                ORDER BY e.trade_date DESC
                LIMIT 1
                """
            )
        ).mappings().first()
    if not row:
        raise RuntimeError(
            "ACCOUNT_SNAPSHOT_MISSING: 禁止使用默认 20 万元替代"
        )
    if str(row.get("account_status") or "") != "ACTIVE":
        raise RuntimeError(
            "ACCOUNT_NOT_ACTIVE: 模拟账户未处于 ACTIVE"
        )
    if str(row.get("reconciliation_status") or "") != "PASS":
        raise RuntimeError(
            "RECONCILIATION_BLOCKED: 最新权益没有 PASS 对账"
        )
    equity = float(row.get("total_equity") or 0)
    if equity <= 0:
        raise RuntimeError(
            "EQUITY_NOT_POSITIVE: 权益为零或无效"
        )
    return equity


def _current_portfolio_state(
    engine: Engine,
    *,
    equity: float,
    stocks: list[dict[str, Any]],
) -> dict[str, Any]:
    features_by_code = {
        str(item["stock_code"]): item
        for item in stocks
    }
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT l.stock_code,
                       MAX(l.theme_code) AS theme_code,
                       SUM(l.remaining_quantity) AS quantity,
                       SUM(l.remaining_quantity * l.cost_price)
                           / NULLIF(SUM(l.remaining_quantity), 0)
                           AS average_cost,
                       MIN(l.protective_stop) AS protective_stop,
                       MAX(
                           CASE
                             WHEN i.reason_code = 'V3_PAPER_DISCOVERY'
                             THEN 1 ELSE 0
                           END
                       ) AS is_paper_discovery
                FROM st_position_lot_v2 l
                LEFT JOIN st_fill_v2 f
                  ON f.fill_id = l.opened_fill_id
                LEFT JOIN st_order_v2 o
                  ON o.order_id = f.order_id
                LEFT JOIN st_trade_intent_v2 i
                  ON i.intent_id = o.intent_id
                WHERE l.account_id = 'paper-main-v2'
                  AND l.remaining_quantity > 0
                GROUP BY l.stock_code
                """
            )
        ).mappings().all()
    position_weights: dict[str, float] = {}
    position_quantities: dict[str, int] = {}
    position_themes: dict[str, tuple[str, ...]] = {}
    theme_weights: dict[str, float] = defaultdict(float)
    open_risk_cny = 0.0
    paper_discovery_codes: set[str] = set()
    for row in rows:
        code = str(row["stock_code"])
        item = features_by_code.get(code, {})
        quantity = int(row["quantity"] or 0)
        price = float(
            item.get("price")
            or row["average_cost"]
            or 0.0
        )
        if quantity <= 0 or price <= 0:
            continue
        weight = quantity * price / max(equity, 1.0)
        raw_themes = item.get("theme_codes")
        themes = {
            str(theme)
            for theme in (
                raw_themes
                if isinstance(raw_themes, (list, tuple, set))
                else ()
            )
            if str(theme)
        }
        if row["theme_code"]:
            themes.add(str(row["theme_code"]))
        position_weights[code] = weight
        if int(row.get("is_paper_discovery") or 0) == 1:
            paper_discovery_codes.add(code)
        position_quantities[code] = quantity
        position_themes[code] = tuple(sorted(themes))
        for theme_code in themes:
            theme_weights[theme_code] += weight
        protective_stop = float(row["protective_stop"] or 0.0)
        risk_per_share = (
            max(0.0, price - protective_stop)
            if protective_stop > 0
            else price * 0.08
        )
        open_risk_cny += quantity * risk_per_share
    return {
        "position_weights": position_weights,
        "position_quantities": position_quantities,
        "position_themes": position_themes,
        "paper_discovery_codes": paper_discovery_codes,
        "theme_weights": dict(theme_weights),
        "open_risk_weight": (
            open_risk_cny / max(equity, 1.0)
        ),
    }


def _previous_trade_date(engine: Engine, as_of: date) -> date:
    with engine.connect() as connection:
        value = connection.execute(
            text(
                """
                SELECT MAX(trade_date)
                FROM si_trade_calendar
                WHERE trade_status = 1
                  AND trade_date < :as_of
                """
            ),
            {"as_of": as_of},
        ).scalar()
    if not isinstance(value, date):
        raise RuntimeError(
            "TRADE_CALENDAR_NOT_READY: 无法取得上一完整交易日"
        )
    return value


def _latest_completed_kline_date(
    engine: Engine,
    as_of: date,
) -> date:
    with engine.connect() as connection:
        value = connection.execute(
            text(
                """
                SELECT MAX(trade_date)
                FROM sm_stock_kline
                WHERE k_type = 1
                  AND trade_date <= :as_of
                """
            ),
            {"as_of": as_of},
        ).scalar()
    if not isinstance(value, date):
        raise RuntimeError(
            "QMT_DAILY_KLINE_NOT_READY: 没有完整日K交易日"
        )
    return value


def _open_position_codes(engine: Engine) -> tuple[str, ...]:
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT DISTINCT stock_code
                FROM st_position_lot_v2
                WHERE account_id = 'paper-main-v2'
                  AND remaining_quantity > 0
                ORDER BY stock_code
                """
            )
        ).scalars().all()
    return tuple(str(code).zfill(6) for code in rows if code)


def _decision_truth_status(
    result: dict[str, Any],
    *,
    execution_enabled: bool,
    forecast_count: int,
) -> tuple[str, str]:
    portfolio = dict(result.get("portfolio") or {})
    regime = dict(result.get("regime") or {})
    data_blocked = (
        str(regime.get("quality_status") or "") != "PASS"
        or str(portfolio.get("status") or "").upper()
        == "DATA_BLOCKED"
    )
    if data_blocked:
        return "BLOCKED", "DATA_BLOCKED"
    # A valid evaluation may conclude that none of its non-empty forecasts
    # deserves a target.  An empty forecast ledger is different: there is no
    # auditable strategy output to evaluate, so calling it NO_ACTION would
    # turn an upstream data/model failure into a successful decision.
    if int(forecast_count) <= 0:
        return "BLOCKED", "DATA_BLOCKED"
    if not execution_enabled:
        return "COMPLETED", "REPLAY_ONLY"
    if portfolio.get("targets"):
        return "COMPLETED", "PAPER_ACTIONABLE"
    return "COMPLETED", "NO_ACTION"


@trading_v3_writer
def run_daily_decision_v3(
    primary_engine: Engine,
    *,
    as_of: date,
    decision_at: datetime,
    mode: str,
    universe_limit: int = 5000,
    per_sleeve_limit: int = 5000,
    kline_engine: Engine | None = None,
    execution_enabled: bool = True,
) -> dict[str, Any]:
    config = load_v3_config()
    repository = TradingV3Repository(primary_engine)
    premarket_freeze = {
        "cancelled_orders": [],
        "cancelled_execution_plans": [],
        "cancelled_partial_orders": [],
    }
    if execution_enabled and mode == "premarket":
        premarket_freeze = freeze_pending_v3_buys(
            primary_engine,
            now=decision_at,
        )
    market_engine = kline_engine or get_kline_engine()
    feature_as_of = as_of
    if mode == "premarket":
        feature_as_of = _previous_trade_date(
            primary_engine,
            as_of,
        )
    elif mode == "manual":
        feature_as_of = _latest_completed_kline_date(
            market_engine,
            as_of,
        )
    load_parameters = inspect.signature(
        load_daily_feature_universe
    ).parameters
    load_kwargs: dict[str, Any] = {
        "as_of": feature_as_of,
        "limit": universe_limit,
    }
    if "context_cutoff_at" in load_parameters:
        load_kwargs["context_cutoff_at"] = decision_at
    if "required_codes" in load_parameters:
        load_kwargs["required_codes"] = _open_position_codes(
            primary_engine
        )
    dataset = load_daily_feature_universe(
        primary_engine,
        market_engine,
        **load_kwargs,
    )
    snapshot = load_decision_snapshot(
        primary_engine,
        requested_as_of=as_of,
        trade_date=dataset["trade_date"],
        decision_at=decision_at,
        feature_time=dataset["feature_time"],
        data_snapshot_hash=dataset["data_snapshot_hash"],
        data_source=dataset["source"],
        stocks=dataset["stocks"],
    )
    calibrations = repository.active_calibrations()
    version_token = str(
        config.get("calibration_version_token") or ""
    )
    version_tokens = dict(
        config.get("calibration_version_tokens") or {}
    )
    compatible_calibrations = {
        key: value
        for key, value in calibrations.items()
        if (
            not str(version_tokens.get(key) or version_token)
            or str(version_tokens.get(key) or version_token)
            in value.model_version
        )
        and value.has_valid_score_direction()
    }
    engine = TradingV3Engine(calibrations)
    feature_time = dataset["feature_time"]
    valid_until = feature_time + timedelta(days=30)
    by_sleeve: dict[str, list[Any]] = defaultdict(list)
    all_forecasts = []
    all_theme_signals: list[dict[str, Any]] = []
    for item in dataset["stocks"]:
        stock_forecasts, stock_theme_signals = (
            engine.evaluate_stock_with_theme_signals(
            item["stock_code"],
            item["stock_name"],
            item,
            feature_time,
            valid_until,
            )
        )
        all_theme_signals.extend(stock_theme_signals)
        for forecast in stock_forecasts:
            by_sleeve[forecast.strategy_key].append(forecast)
            all_forecasts.append(forecast)
    decision_forecasts = []
    for sleeve, items in by_sleeve.items():
        # The calibration is fitted on each day's raw-score Top N. Applying
        # it to ranks N+1..300 would silently widen the live population beyond
        # its OOS evidence.
        items = _calibrated_universe(
            sleeve,
            items,
            compatible_calibrations=compatible_calibrations,
            config=config,
        )
        ranked = sorted(
            items,
            key=lambda item: (
                -float(item.expected_return_net_pct or -10**6),
                -float(item.raw_score or 0),
                item.stock_code,
            ),
        )
        decision_forecasts.extend(ranked[:per_sleeve_limit])
    prices = {
        item["stock_code"]: float(item["price"])
        for item in dataset["stocks"]
    }
    equity = float(snapshot["equity"])
    current_portfolio = dict(snapshot["portfolio_state"])
    learning_reader = getattr(
        repository,
        "strategy_learning_summary",
        None,
    )
    paper_learning = (
        learning_reader("oversold_reversal")
        if learning_reader is not None
        else {}
    )
    result = engine.decide(
        decision_forecasts,
        market_features=dataset["market_features"],
        prices=prices,
        equity=equity,
        current_theme_weights=current_portfolio["theme_weights"],
        current_position_weights=current_portfolio[
            "position_weights"
        ],
        current_position_quantities=current_portfolio[
            "position_quantities"
        ],
        current_position_themes=current_portfolio[
            "position_themes"
        ],
        current_paper_discovery_codes=current_portfolio.get(
            "paper_discovery_codes",
            set(),
        ),
        current_open_risk_weight=current_portfolio[
            "open_risk_weight"
        ],
        allow_paper_discovery=bool(
            config.get("paper_discovery", {}).get("enabled")
        ),
        paper_discovery_learning=paper_learning,
        opportunity_audit_forecasts=all_forecasts,
        decision_at=decision_at,
    )
    try:
        shadow_audit = ShadowIntelligenceRepository(
            primary_engine
        ).release_audit()
    except Exception as exc:
        shadow_audit = {
            "schema_version": "probiga.trading-v3.shadow-release-audit.v1",
            "status": "UNAVAILABLE",
            "blockers": [f"SHADOW_AUDIT_UNAVAILABLE:{type(exc).__name__}"],
            "releases": [],
            "automatic_promotion_allowed": False,
            "order_authority": False,
        }
    portfolio = dict(result["portfolio"])
    opportunity_audit = dict(portfolio.get("opportunity_audit") or {})
    opportunity_audit["shadow_intelligence"] = {
        **shadow_audit,
        "strategy_version": str(config["strategy_version"]),
        "binding_scope": "READ_ONLY_AUDIT",
        "affects_order_decision": False,
        "order_authority": False,
    }
    portfolio["opportunity_audit"] = opportunity_audit
    if not all_forecasts:
        warnings = list(opportunity_audit.get("warnings") or [])
        if "FORECAST_LEDGER_EMPTY" not in warnings:
            warnings.insert(0, "FORECAST_LEDGER_EMPTY")
        opportunity_audit.update({
            "status": "ATTENTION",
            "warnings": warnings,
            "forecast_ledger": {
                "status": "DATA_BLOCKED",
                "forecast_count": 0,
                "reason": "FORECAST_LEDGER_EMPTY",
            },
        })
        portfolio.update({
            "status": "DATA_BLOCKED",
            "targets": [],
            "opportunity_audit": opportunity_audit,
        })
    result = {**result, "portfolio": portfolio}
    run_status, actionable_status = _decision_truth_status(
        result,
        execution_enabled=execution_enabled,
        forecast_count=len(all_forecasts),
    )
    run_uid = uuid.uuid4().hex
    regime = classify_regime_probabilities(
        dataset["market_features"]
    )
    hypotheses = ()
    if regime.quality_status == "PASS":
        hypotheses = (
            build_market_hypothesis(
                run_uid=run_uid,
                trade_date=dataset["trade_date"],
                decision_at=decision_at,
                regime=regime,
            ),
            *build_stock_hypotheses(
                all_forecasts,
                run_uid=run_uid,
                trade_date=dataset["trade_date"],
                decision_at=decision_at,
                regime=regime,
                limit=300,
            ),
        )
    saved = repository.save_decision(
        run_uid=run_uid,
        trade_date=dataset["trade_date"],
        requested_as_of=as_of,
        decision_at=decision_at,
        mode=mode,
        model_version=config["strategy_version"],
        lifecycle_status=config["lifecycle_status"],
        regime=result["regime"],
        portfolio=result["portfolio"],
        forecasts=all_forecasts,
        theme_signals=all_theme_signals,
        data_snapshot_hash=dataset["data_snapshot_hash"],
        hypotheses=hypotheses,
        run_status=run_status,
        actionable_status=actionable_status,
        snapshot_manifest=snapshot["manifest"],
        defer_completion=True,
    )
    if execution_enabled and all_forecasts:
        try:
            position_sync = sync_position_states(
                primary_engine,
                trade_date=dataset["trade_date"],
                equity=equity,
                stocks=dataset["stocks"],
                forecasts=all_forecasts,
                targets=result["portfolio"]["targets"],
                hypotheses=hypotheses,
                decision_quality_status=regime.quality_status,
            )
        except Exception as exc:
            repository.mark_run_failed(
                run_uid,
                stage="POSITION_SYNC",
                error=exc,
            )
            raise RuntimeError(
                f"V3_POSITION_SYNC_FAILED: {exc}"
            ) from exc
    else:
        # Historical/replay decisions and an empty forecast ledger may persist
        # immutable evidence, but neither may rewrite today's position state.
        position_sync = {
            "status": (
                "data_blocked" if execution_enabled else "replay_only"
            ),
            "updated_count": 0,
        }
    if execution_enabled and all_forecasts:
        try:
            execution = materialize_internal_paper_orders(
                primary_engine,
                run_uid=run_uid,
            )
        except Exception as exc:
            repository.mark_run_failed(
                run_uid,
                stage="ORDER_MATERIALIZATION",
                error=exc,
            )
            raise RuntimeError(
                f"V3_ORDER_MATERIALIZATION_FAILED: {exc}"
            ) from exc
    else:
        execution = {
            "status": (
                "data_blocked" if execution_enabled else "replay_only"
            ),
            "paper_order_count": 0,
            "created": [],
            "skipped": [],
        }
    try:
        repository.finalize_run(run_uid, status=run_status)
    except Exception as exc:
        repository.mark_run_failed(
            run_uid,
            stage="RUN_FINALIZATION",
            error=exc,
        )
        raise RuntimeError(f"V3_RUN_FINALIZATION_FAILED: {exc}") from exc
    return {
        "schema": "probiga.trading-v3-decision-result.v1",
        "status": "blocked" if run_status == "BLOCKED" else "ok",
        # DATA_BLOCKED is persisted as decision truth, but remains retryable
        # at the scheduler layer after upstream data arrives the same day.
        "retryable": run_status == "BLOCKED",
        **saved,
        "run_status": run_status,
        "actionable_status": actionable_status,
        "execution_enabled": bool(execution_enabled),
        "snapshot_manifest_hash": snapshot["manifest"]["manifest_hash"],
        "trade_date": dataset["trade_date"].isoformat(),
        "decision_at": decision_at.isoformat(sep=" "),
        "mode": mode,
        "source": dataset["source"],
        "concept_snapshot_date": dataset.get("concept_snapshot_date"),
        "theme_count": dataset.get("theme_count", 0),
        "market_regime": result["regime"]["dominant_state"],
        "risk_asset_cap": result["regime"]["risk_asset_cap"],
        "strategy_weights": result.get("strategy_weights", {}),
        "hypothesis_count": saved.get("hypothesis_count", 0),
        "theme_signal_count": saved.get("theme_signal_count", 0),
        "active_calibrated_sleeves": sorted(
            compatible_calibrations
        ),
        "incompatible_calibrated_sleeves": sorted(
            set(calibrations) - set(compatible_calibrations)
        ),
        "portfolio_status": result["portfolio"]["status"],
        "paper_order_count": execution["paper_order_count"],
        "paper_orders": execution["created"],
        "paper_order_skipped": execution["skipped"],
        "superseded_paper_orders": execution.get(
            "cancelled_orders",
            [],
        ),
        "superseded_execution_plans": execution.get(
            "cancelled_execution_plans",
            [],
        ),
        "superseded_partial_paper_orders": execution.get(
            "cancelled_partial_orders",
            [],
        ),
        "premarket_frozen_paper_orders": premarket_freeze.get(
            "cancelled_orders",
            [],
        ),
        "premarket_frozen_execution_plans": premarket_freeze.get(
            "cancelled_execution_plans",
            [],
        ),
        "position_state_updates": position_sync["updated_count"],
        "real_order_count": 0,
        "real_trading_enabled": False,
        "paper_discovery_learning": paper_learning,
    }
