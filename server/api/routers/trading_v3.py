"""V3 positive-expectancy decision APIs and protected manual actions."""
from __future__ import annotations

import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Path as ApiPath, Query
from sqlalchemy import text

from server.api.routers._engine import get_engine
from server.api.scheduler_runtime import launch_scheduler_task
from server.trading_v3.config import config_hash, load_v3_config
from server.trading_v3.repository import TradingV3Repository
from server.trading_v3.versioning import code_version


router = APIRouter(prefix="/v3", tags=["trading-v3"])

_HYPOTHESIS_NEW_BUY_ACTIONS = frozenset(
    {
        "BUY_OR_HOLD",
        "PAPER_PROBE",
        "PAPER_PROBE_IF_CONFIRMED",
        "PAPER_ORDER_CREATED",
    }
)


def _research_hypothesis_projection(row: dict[str, Any]) -> dict[str, Any]:
    projected = dict(row)
    action = str(projected.get("proposed_action") or "").upper()
    projected["source_proposed_action"] = action
    projected["decision_scope"] = "RESEARCH_ONLY"
    projected["new_buy_eligible"] = False
    if action in _HYPOTHESIS_NEW_BUY_ACTIONS:
        projected["proposed_action"] = "WATCH_CLOSELY"
    return projected


def _research_target_projection(row: dict[str, Any]) -> dict[str, Any]:
    return {
        **row,
        "decision_scope": "RESEARCH_ONLY",
        "new_buy_eligible": False,
        "display_action": "WATCH",
    }


def _repo() -> TradingV3Repository:
    engine = get_engine()
    if engine is None:
        raise HTTPException(status_code=503, detail="V3 database unavailable")
    return TradingV3Repository(engine)


def _envelope(
    data: Any,
    *,
    status: str = "ok",
) -> dict[str, Any]:
    config = load_v3_config()
    resolved_code_version, code_version_source = code_version()
    return {
        "status": status,
        "trace_id": uuid.uuid4().hex,
        "generated_at": datetime.now().astimezone().isoformat(
            timespec="seconds"
        ),
        "code_commit_sha": resolved_code_version,
        "code_version_source": code_version_source,
        "config_version": config["strategy_version"],
        "config_hash": config_hash(),
        "real_trading_enabled": False,
        "data": data,
    }


@router.get("/readiness")
def readiness():
    repository = _repo()
    config = load_v3_config()
    tables = repository.table_readiness()
    missing = [name for name, ready in tables.items() if not ready]
    column_reader = getattr(
        repository,
        "production_column_readiness",
        None,
    )
    columns = (
        column_reader()
        if column_reader is not None and not missing
        else {}
    )
    guard_reader = getattr(
        repository,
        "real_trading_guard_readiness",
        None,
    )
    guards = (
        guard_reader()
        if guard_reader is not None and not missing
        else {"insert": False, "update": False}
    )
    calibration_status_reader = getattr(
        repository,
        "active_calibration_status",
        None,
    )
    if not missing and calibration_status_reader is not None:
        calibration_status = calibration_status_reader()
        calibrations = dict(
            calibration_status.get("calibrations") or {}
        )
        calibration_rejections = dict(
            calibration_status.get("rejections") or {}
        )
    else:
        calibrations = (
            repository.active_calibrations() if not missing else {}
        )
        calibration_rejections = {}
    version_token = str(
        config.get("calibration_version_token") or ""
    )
    version_tokens = dict(
        config.get("calibration_version_tokens") or {}
    )
    compatible = {
        key: value
        for key, value in calibrations.items()
        if (
            not str(version_tokens.get(key) or version_token)
            or str(version_tokens.get(key) or version_token)
            in str(getattr(value, "model_version", ""))
        )
        and value.has_valid_score_direction()
    }
    validation_reader = getattr(
        repository,
        "latest_validations_for_models",
        None,
    )
    matching_validations = (
        validation_reader(
            getattr(value, "model_version", "")
            for value in compatible.values()
        )
        if validation_reader is not None
        else {}
    )
    active_oos_models = []
    for strategy_key, calibration in sorted(compatible.items()):
        model_version = str(
            getattr(calibration, "model_version", "")
        )
        validation = matching_validations.get(model_version) or {}
        active_oos_models.append(
            {
                "strategy_key": strategy_key,
                "model_version": model_version,
                "dataset_hash": str(
                    getattr(calibration, "dataset_hash", "")
                ),
                "validation_status": str(
                    validation.get("result_status") or "MISSING"
                ),
                "validation_id": str(
                    validation.get("validation_id") or ""
                ),
                "validation_created_at": validation.get(
                    "created_at"
                ),
            }
        )
    discovery_ready = bool(
        config.get("paper_discovery", {}).get("enabled")
    )
    blocks = []
    warnings = []
    if missing:
        blocks.append("V3_SCHEMA_INCOMPLETE")
    if columns and not all(columns.values()):
        blocks.append("V3_PRODUCTION_COLUMNS_INCOMPLETE")
    if not all(guards.values()):
        blocks.append("REAL_TRADING_DATABASE_GUARD_MISSING")
    if not compatible:
        if discovery_ready:
            warnings.append("NO_COMPATIBLE_OOS_CALIBRATION")
        else:
            blocks.append("NO_COMPATIBLE_OOS_CALIBRATION")
    validated_portfolio_ready = bool(
        not missing
        and (not columns or all(columns.values()))
        and all(guards.values())
        and compatible
    )
    paper_discovery_ready = bool(
        discovery_ready
        and not missing
        and (not columns or all(columns.values()))
        and all(guards.values())
    )
    paper_ready = bool(
        not missing
        and (not columns or all(columns.values()))
        and all(guards.values())
        and (compatible or discovery_ready)
    )
    return _envelope(
        {
            "schema": tables,
            "production_columns": columns,
            "real_trading_database_guards": guards,
            "active_calibrated_sleeves": sorted(compatible),
            "active_oos_models": active_oos_models,
            "incompatible_calibrated_sleeves": sorted(
                set(calibrations)
                - set(compatible)
                | set(calibration_rejections)
            ),
            "calibration_rejections": calibration_rejections,
            "validated_portfolio_ready": (
                validated_portfolio_ready
            ),
            "paper_discovery_ready": paper_discovery_ready,
            "paper_ready": paper_ready,
            "portfolio_limits": {
                "minimum_positions": int(
                    config.get("portfolio", {}).get(
                        "minimum_positions", 0
                    )
                ),
                "maximum_positions": int(
                    config.get("portfolio", {}).get(
                        "maximum_positions", 0
                    )
                ),
                "maximum_add_count": int(
                    config.get("portfolio", {}).get(
                        "maximum_add_count", 0
                    )
                ),
                "maximum_paper_discovery_positions": int(
                    config.get("paper_discovery", {}).get(
                        "maximum_positions", 0
                    )
                ),
                "maximum_live_positions": int(
                    config.get("paper_execution", {}).get(
                        "maximum_live_positions", 0
                    )
                ),
            },
            "real_trading_enabled": False,
            "blocks": blocks,
            "warnings": warnings,
        },
        status="ok" if paper_ready else "blocked",
    )
@router.get("/overview")
def overview(compact: bool = Query(default=False)):
    data = _repo().overview()
    if compact:
        run = dict(data.get("run") or {})
        portfolio = dict(run.get("portfolio") or {})
        # The full opportunity audit can exceed hundreds of kilobytes.  The
        # main trading desk needs only a short rejection sample; dedicated V3
        # overview pages continue to receive the complete immutable snapshot.
        portfolio.pop("opportunity_audit", None)
        portfolio["rejected"] = list(
            portfolio.get("rejected") or []
        )[:12]
        run["portfolio"] = portfolio
        data = {**data, "run": run}
    return _envelope(data)


@router.get("/forecasts/latest")
def latest_forecasts(
    limit: int = Query(default=200, ge=1, le=5000),
    status: str = Query(default="", max_length=48),
    trade_date: date | None = Query(default=None),
    strategy_key: str = Query(default="", max_length=64),
    q: str = Query(default="", max_length=64),
):
    return _envelope(
        _repo().latest_forecasts(
            limit=limit,
            status=status,
            trade_date=trade_date,
            strategy_key=strategy_key.strip(),
            query=q.strip(),
        )
    )


@router.get("/hypotheses/latest")
def latest_hypotheses(
    limit: int = Query(default=300, ge=1, le=1000),
    trade_date: date | None = Query(default=None),
    scope_type: str = Query(
        default="",
        pattern=r"^(|MARKET|THEME|STOCK)$",
    ),
    state: str = Query(default="", max_length=32),
    q: str = Query(default="", max_length=64),
):
    rows = _repo().latest_hypotheses(
            limit=limit,
            trade_date=trade_date,
            scope_type=scope_type,
            state=state.strip(),
            query=q.strip(),
        )
    return _envelope(
        [_research_hypothesis_projection(row) for row in rows]
    )


@router.get("/hypotheses/{hypothesis_id}/timeline")
def hypothesis_timeline(
    hypothesis_id: str = ApiPath(
        ...,
        pattern=r"^[a-fA-F0-9]{32,64}$",
    ),
    limit: int = Query(default=500, ge=1, le=2000),
):
    result = _repo().hypothesis_timeline(
        hypothesis_id,
        limit=limit,
    )
    if result is None:
        raise HTTPException(
            status_code=404,
            detail="trade hypothesis not found",
        )
    hypothesis = result.get("hypothesis")
    if isinstance(hypothesis, dict):
        result = {
            **result,
            "hypothesis": _research_hypothesis_projection(hypothesis),
        }
    return _envelope(result)


@router.get("/decision-runs")
def decision_runs(
    limit: int = Query(default=60, ge=1, le=500),
):
    return _envelope(_repo().decision_runs(limit=limit))


@router.get("/portfolio/latest")
def latest_portfolio():
    return _envelope(
        [
            _research_target_projection(row)
            for row in _repo().latest_targets()
        ]
    )


@router.get("/paper-ledger")
def paper_ledger(
    account_id: str = Query(default="paper-main-v2", min_length=3, max_length=64),
    limit: int = Query(default=200, ge=1, le=500),
):
    """Return the two internal paper ledgers as one honest read model.

    The legacy event simulator is still the active scheduled executor while
    V2/V3 owns the new immutable plan/order ledger.  This endpoint merges only
    their display projections; it never copies, invents or executes a fill.
    """
    engine = get_engine()
    if engine is None:
        raise HTTPException(status_code=503, detail="paper ledger database unavailable")
    from server.trading_v2.repository import TradingV2ReadRepository

    v2 = TradingV2ReadRepository(engine)
    account = v2.account(account_id) or {}
    v2_positions = v2.positions(account_id)
    v2_orders = v2.orders(account_id, limit)
    with engine.connect() as connection:
        legacy_positions = [dict(row) for row in connection.execute(
            text(
                """
                SELECT id, stock_code, short_name, strategy_type, buy_price,
                       buy_shares, buy_date, buy_time, buy_reason, status
                FROM st_sim_position
                WHERE COALESCE(trade_mode, 'live') = 'live'
                  AND status = 'holding'
                ORDER BY buy_date DESC, buy_time DESC, id DESC
                LIMIT :limit
                """
            ),
            {"limit": int(limit)},
        ).mappings().all()]
        legacy_orders = [dict(row) for row in connection.execute(
            text(
                """
                SELECT id, stock_code, short_name, strategy_type, side,
                       requested_shares, filled_shares, remaining_shares,
                       limit_price, filled_price, status, reason,
                       reject_reason, last_match_reason, order_date,
                       order_time, filled_at, created_at
                FROM st_sim_order
                WHERE COALESCE(trade_mode, 'live') = 'live'
                ORDER BY COALESCE(filled_at, created_at) DESC, id DESC
                LIMIT :limit
                """
            ),
            {"limit": int(limit)},
        ).mappings().all()]

        position_codes = sorted({
            str(row.get("stock_code") or "").zfill(6)
            for row in [*v2_positions, *legacy_positions]
            if row.get("stock_code")
        })
        latest_quotes: dict[str, dict[str, Any]] = {}
        if position_codes:
            quote_params: dict[str, Any] = {}
            quote_placeholders: list[str] = []
            for index, stock_code in enumerate(position_codes):
                key = f"quote_code_{index}"
                quote_params[key] = stock_code
                quote_placeholders.append(f":{key}")
            quote_rows = connection.execute(
                text(
                    f"""
                    SELECT c.stock_code, c.short_name, c.price,
                           c.snapshot_at, c.data_source
                    FROM sm_stock_current c
                    JOIN (
                        SELECT stock_code, MAX(snapshot_at) AS snapshot_at
                        FROM sm_stock_current
                        WHERE stock_code IN ({','.join(quote_placeholders)})
                        GROUP BY stock_code
                    ) latest
                      ON latest.stock_code = c.stock_code
                     AND latest.snapshot_at = c.snapshot_at
                    """
                ),
                quote_params,
            ).mappings().all()
            latest_quotes = {
                str(row.get("stock_code") or "").zfill(6): dict(row)
                for row in quote_rows
            }

    def enrich_position(row: dict[str, Any]) -> dict[str, Any]:
        result = dict(row)
        stock_code = str(result.get("stock_code") or "").zfill(6)
        quote = latest_quotes.get(stock_code) or {}
        quantity = int(result.get("remaining_quantity") or result.get("quantity") or 0)
        cost_price = float(result.get("cost_price") or result.get("average_cost") or 0)
        current_price = float(quote.get("price") or 0)
        result["current_price"] = round(current_price, 4) if current_price > 0 else None
        result["quote_at"] = quote.get("snapshot_at")
        result["quote_source"] = quote.get("data_source")
        result["market_value"] = round(current_price * quantity, 2) if current_price > 0 else None
        result["unrealized_pnl"] = round((current_price - cost_price) * quantity, 2) if current_price > 0 and cost_price > 0 else None
        result["unrealized_pnl_pct"] = round((current_price / cost_price - 1.0) * 100.0, 2) if current_price > 0 and cost_price > 0 else None
        if quote.get("short_name"):
            result["short_name"] = quote.get("short_name")
        return result

    positions = []
    for row in v2_positions:
        positions.append(enrich_position({**row, "ledger_source": "V2_CANONICAL"}))
    for row in legacy_positions:
        positions.append(enrich_position({
            **row,
            "ledger_source": "LEGACY_EVENT_SIM",
            "position_state": "HOLDING",
            "quantity": int(row.get("buy_shares") or 0),
            "remaining_quantity": int(row.get("buy_shares") or 0),
            "sellable_quantity": 0 if str(row.get("buy_date") or "")[:10] == date.today().isoformat() else int(row.get("buy_shares") or 0),
            "cost_price": row.get("buy_price"),
            "last_reason": row.get("buy_reason") or "事件驱动模拟成交",
        }))

    orders = []
    for row in v2_orders:
        orders.append({**row, "ledger_source": "V2_CANONICAL"})
    for row in legacy_orders:
        orders.append({
            **row,
            "ledger_source": "LEGACY_EVENT_SIM",
            "order_id": f"legacy-{row.get('id')}",
            "quantity": int(row.get("requested_shares") or 0),
            "filled_quantity": int(row.get("filled_shares") or 0),
            "waiting_reason": row.get("last_match_reason") or row.get("reject_reason") or row.get("reason") or "",
            "earliest_at": f"{row.get('order_date') or ''} {row.get('order_time') or ''}".strip(),
            "expires_at": row.get("filled_at") or "",
        })
    initial_cash = float(account.get("initial_cash") or 0)
    legacy_market_value = sum(
        float(row.get("buy_price") or 0) * int(row.get("buy_shares") or 0)
        for row in legacy_positions
    )
    v2_cash = float(account.get("cash_balance") or initial_cash)
    total_market_value = round(sum(float(row.get("market_value") or 0) for row in positions), 2)
    total_unrealized_pnl = round(sum(float(row.get("unrealized_pnl") or 0) for row in positions), 2)
    return _envelope({
        "account_id": account_id,
        "account": account,
        "positions": positions,
        "orders": orders,
        "summary": {
            "position_count": len(positions),
            "order_count": len(orders),
            "v2_position_count": len(v2_positions),
            "legacy_position_count": len(legacy_positions),
            "v2_order_count": len(v2_orders),
            "legacy_order_count": len(legacy_orders),
            "cash_balance": v2_cash,
            "legacy_cost_market_value": round(legacy_market_value, 2),
            "current_market_value": total_market_value,
            "total_unrealized_pnl": total_unrealized_pnl,
        },
        "ledger_sources": ["V2_CANONICAL", "LEGACY_EVENT_SIM"],
        "real_trading_enabled": False,
        "merge_policy": "READ_ONLY_NO_FILL_COPY",
    })


@router.get("/validation/latest")
def latest_validation():
    result = _repo().latest_validation()
    return _envelope(result, status="ok" if result else "empty")


@router.get("/opportunity-recall/latest")
def latest_opportunity_recall():
    result = _repo().latest_opportunity_recall()
    return _envelope(result, status="ok" if result else "collecting")


@router.get("/learning/{strategy_key}")
def strategy_learning(
    strategy_key: str = ApiPath(
        ...,
        pattern=r"^[a-z0-9_]{3,64}$",
    ),
):
    result = _repo().strategy_learning_summary(strategy_key)
    return _envelope(
        result,
        status=(
            "ok" if result["observed_count"] else "collecting"
        ),
    )


@router.post("/actions/{action_key}")
def run_manual_action(
    action_key: str = ApiPath(
        ...,
        pattern=r"^(daily|intraday)$",
    ),
):
    """Launch one explicitly allow-listed paper-trading action."""
    if action_key == "intraday":
        raise HTTPException(
            status_code=409,
            detail=(
                "V3_ONLY_ROUTE: V2盘中激活已从V3生产入口隔离；"
                "V3盘中模型通过独立样本外和组合级验收前不允许手动触发买入"
            ),
        )
    task_types = {
        "daily": "trading_v3_close_decision",
    }
    engine = get_engine()
    if engine is None:
        raise HTTPException(
            status_code=503,
            detail="scheduler database unavailable",
        )
    with engine.connect() as connection:
        task = connection.execute(
            text(
                """
                SELECT *
                FROM st_scheduled_tasks
                WHERE task_type = :task_type
                LIMIT 1
                """
            ),
            {"task_type": task_types[action_key]},
        ).mappings().first()
    if not task:
        raise HTTPException(
            status_code=404,
            detail="manual action task is not registered",
        )
    row = dict(task)
    if action_key == "daily":
        row["script_args"] = (
            "--mode manual --universe-limit 5000 "
            "--per-sleeve-limit 5000"
        )
    result = launch_scheduler_task(
        row,
        root=Path(__file__).resolve().parents[3],
        engine=engine,
    )
    return _envelope(
        {
            **result,
            "action": action_key,
            "real_trading_enabled": False,
        },
        status=str(result["status"]),
    )
