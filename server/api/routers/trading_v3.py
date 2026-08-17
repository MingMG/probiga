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


@router.get("/stock-pool")
def stock_pool(
    trade_date: date | None = Query(default=None),
):
    """Read-only, per-stock projection of the latest V3 decision run."""
    return _envelope(_repo().stock_pool(trade_date=trade_date))


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
    v2_fills = v2.fills(account_id, max(int(limit), 500))
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
        legacy_today_sold = [dict(row) for row in connection.execute(
            text(
                """
                SELECT id, stock_code, short_name, strategy_type, buy_price,
                       buy_shares, buy_date, buy_time, buy_reason, status,
                       sell_price, sell_date, sell_time, sell_reason,
                       profit, profit_rate
                FROM st_sim_position
                WHERE COALESCE(trade_mode, 'live') = 'live'
                  AND status = 'sold'
                  AND sell_date = CURDATE()
                ORDER BY sell_time DESC, id DESC
                LIMIT :limit
                """
            ),
            {"limit": int(limit)},
        ).mappings().all()]
        # Test doubles and old compatibility views can return broader rows than
        # the SQL predicate.  Keep the read model strict: only today's closed
        # positions may appear beside open holdings.
        legacy_today_sold = [
            row for row in legacy_today_sold
            if str(row.get("status") or "").lower() == "sold"
            and str(row.get("sell_date") or "")[:10] == date.today().isoformat()
        ]
        legacy_orders = [dict(row) for row in connection.execute(
            text(
                """
                SELECT id, stock_code, short_name, strategy_type, side,
                       requested_shares, filled_shares, remaining_shares,
                       limit_price, target_price, filled_price, status, reason,
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
        legacy_profit_rows = [dict(row) for row in connection.execute(
            text(
                """
                SELECT COALESCE(SUM(profit), 0) AS realized_profit
                FROM st_sim_position
                WHERE COALESCE(trade_mode, 'live') = 'live'
                  AND status = 'sold'
                """
            ),
            {},
        ).mappings().all()]
        legacy_capital_rows = [dict(row) for row in connection.execute(
            text(
                """
                SELECT initial_capital
                FROM st_sim_risk_budget
                WHERE COALESCE(trade_mode, 'live') = 'live'
                  AND initial_capital > 0
                ORDER BY budget_date DESC, updated_at DESC
                LIMIT 1
                """
            ),
            {},
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

    def display_datetime(date_value: Any, time_value: Any = None) -> str | None:
        if date_value in (None, ""):
            return None
        date_text = str(date_value).strip()
        if "T" in date_text or " " in date_text:
            return date_text.replace("T", " ")
        time_text = str(time_value or "").strip()
        return f"{date_text} {time_text}".strip()

    def date_token(value: Any) -> str:
        if value in (None, ""):
            return ""
        return str(value).strip().replace("T", " ")[:10]

    fill_by_id = {
        str(row.get("fill_id") or ""): row
        for row in v2_fills
        if row.get("fill_id")
    }
    v2_buy_times: dict[str, list[str]] = {}
    for fill in v2_fills:
        if str(fill.get("side") or "").upper() != "BUY":
            continue
        code = str(fill.get("stock_code") or "").zfill(6)
        filled_at = display_datetime(fill.get("filled_at"))
        if code and filled_at:
            v2_buy_times.setdefault(code, []).append(filled_at)

    position_lots = []
    for row in v2_positions:
        opened_fill = fill_by_id.get(str(row.get("opened_fill_id") or "")) or {}
        position_lots.append(enrich_position({
            **row,
            "ledger_source": "V2_CANONICAL",
            "buy_at": display_datetime(
                opened_fill.get("filled_at") or row.get("opened_trade_date")
            ),
        }))
    for row in legacy_positions:
        position_lots.append(enrich_position({
            **row,
            "ledger_source": "LEGACY_EVENT_SIM",
            "position_state": "HOLDING",
            "quantity": int(row.get("buy_shares") or 0),
            "remaining_quantity": int(row.get("buy_shares") or 0),
            # Legacy event-sim lots are display-only evidence.  They are not
            # canonical V2 inventory and must never inflate executable shares.
            "sellable_quantity": 0,
            "cost_price": row.get("buy_price"),
            "buy_at": display_datetime(row.get("buy_date"), row.get("buy_time")),
            "last_reason": row.get("buy_reason") or "事件驱动模拟成交",
        }))

    def merge_position_lots(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Merge display positions by security without changing the fill ledger."""
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            stock_code = str(row.get("stock_code") or "").zfill(6)
            grouped.setdefault(stock_code, []).append(row)

        merged: list[dict[str, Any]] = []
        for stock_code, lots in grouped.items():
            result = dict(lots[0])
            quantities = [
                int(lot.get("remaining_quantity") or lot.get("quantity") or 0)
                for lot in lots
            ]
            quantity = sum(quantities)
            cost_amount = sum(
                float(lot.get("cost_price") or lot.get("average_cost") or 0) * lot_quantity
                for lot, lot_quantity in zip(lots, quantities)
            )
            cost_price = cost_amount / quantity if quantity > 0 and cost_amount > 0 else 0.0
            current_prices = [
                float(lot.get("current_price") or 0)
                for lot in lots
                if float(lot.get("current_price") or 0) > 0
            ]
            current_price = current_prices[0] if current_prices else 0.0
            ledger_sources = list(dict.fromkeys(
                str(lot.get("ledger_source") or "") for lot in lots if lot.get("ledger_source")
            ))
            reasons = list(dict.fromkeys(
                str(lot.get("invalidation_condition") or lot.get("last_reason") or "")
                for lot in lots
                if lot.get("invalidation_condition") or lot.get("last_reason")
            ))
            stops = [
                float(lot.get("protective_stop") or 0)
                for lot in lots
                if float(lot.get("protective_stop") or 0) > 0
            ]
            quote_lot = max(lots, key=lambda lot: str(lot.get("quote_at") or ""))
            buy_times = sorted(
                str(lot.get("buy_at")) for lot in lots if lot.get("buy_at")
            )
            lot_details = [
                {
                    "ledger_source": lot.get("ledger_source"),
                    "quantity": lot_quantity,
                    "cost_price": lot.get("cost_price") or lot.get("average_cost"),
                    "buy_at": lot.get("buy_at"),
                    "protective_stop": lot.get("protective_stop"),
                    "note": lot.get("invalidation_condition") or lot.get("last_reason") or "",
                }
                for lot, lot_quantity in zip(lots, quantities)
            ]

            result.update({
                "stock_code": stock_code,
                "short_name": quote_lot.get("short_name") or result.get("short_name"),
                "position_state": result.get("position_state") or result.get("state") or "HOLDING",
                "quantity": quantity,
                "remaining_quantity": quantity,
                "sellable_quantity": sum(int(lot.get("sellable_quantity") or 0) for lot in lots),
                "cost_price": round(cost_price, 4) if cost_price > 0 else None,
                "average_cost": round(cost_price, 4) if cost_price > 0 else None,
                "current_price": round(current_price, 4) if current_price > 0 else None,
                "market_value": round(current_price * quantity, 2) if current_price > 0 else None,
                "unrealized_pnl": round(current_price * quantity - cost_amount, 2)
                if current_price > 0 and cost_amount > 0 else None,
                "unrealized_pnl_pct": round((current_price * quantity / cost_amount - 1.0) * 100.0, 2)
                if current_price > 0 and cost_amount > 0 else None,
                "protective_stop": round(max(stops), 4) if stops else None,
                "add_count": sum(int(lot.get("add_count") or 0) for lot in lots) + max(0, len(lots) - 1),
                "quote_at": quote_lot.get("quote_at"),
                "quote_source": quote_lot.get("quote_source"),
                "ledger_source": ledger_sources[0] if len(ledger_sources) == 1 else "MERGED_LEDGER",
                "ledger_sources": ledger_sources,
                "position_lot_count": len(lots),
                "holding_notes": reasons,
                "last_reason": "；".join(reasons),
                "buy_at": buy_times[0] if buy_times else None,
                "sell_at": None,
                "sell_price": None,
                "sold_quantity_today": 0,
                "lot_details": lot_details,
            })
            merged.append(result)
        return merged

    positions = merge_position_lots(position_lots)

    sale_events: list[dict[str, Any]] = []
    for row in legacy_today_sold:
        sale_events.append({
            "stock_code": str(row.get("stock_code") or "").zfill(6),
            "short_name": row.get("short_name"),
            "ledger_source": "LEGACY_EVENT_SIM",
            "sold_quantity": int(row.get("buy_shares") or 0),
            "cost_price": row.get("buy_price"),
            "buy_at": display_datetime(row.get("buy_date"), row.get("buy_time")),
            "sell_at": display_datetime(row.get("sell_date"), row.get("sell_time")),
            "sell_price": row.get("sell_price"),
            "realized_pnl": row.get("profit"),
            "realized_pnl_pct": row.get("profit_rate"),
            "note": row.get("sell_reason") or row.get("buy_reason") or "今日已卖出",
        })
    today_text = date.today().isoformat()
    for row in v2_fills:
        if (
            str(row.get("side") or "").upper() != "SELL"
            or date_token(row.get("filled_at")) != today_text
        ):
            continue
        stock_code = str(row.get("stock_code") or "").zfill(6)
        buy_times = sorted(v2_buy_times.get(stock_code) or [])
        sale_events.append({
            "stock_code": stock_code,
            "short_name": row.get("short_name"),
            "ledger_source": "V2_CANONICAL",
            "sold_quantity": int(row.get("quantity") or 0),
            "cost_price": None,
            "buy_at": buy_times[0] if buy_times else None,
            "sell_at": display_datetime(row.get("filled_at")),
            "sell_price": row.get("price"),
            "realized_pnl": None,
            "realized_pnl_pct": None,
            "note": "V2/V3 模拟账本今日卖出成交",
        })

    def merge_sale_events(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            stock_code = str(row.get("stock_code") or "").zfill(6)
            if stock_code:
                grouped.setdefault(stock_code, []).append(row)
        merged: dict[str, dict[str, Any]] = {}
        for stock_code, events in grouped.items():
            quantities = [int(event.get("sold_quantity") or 0) for event in events]
            sold_quantity = sum(quantities)
            sell_amount = sum(
                float(event.get("sell_price") or 0) * quantity
                for event, quantity in zip(events, quantities)
            )
            cost_amount = sum(
                float(event.get("cost_price") or 0) * quantity
                for event, quantity in zip(events, quantities)
            )
            cost_quantity = sum(
                quantity
                for event, quantity in zip(events, quantities)
                if float(event.get("cost_price") or 0) > 0
            )
            sell_times = sorted(
                str(event.get("sell_at")) for event in events if event.get("sell_at")
            )
            buy_times = sorted(
                str(event.get("buy_at")) for event in events if event.get("buy_at")
            )
            notes = list(dict.fromkeys(
                str(event.get("note") or "") for event in events if event.get("note")
            ))
            sources = list(dict.fromkeys(
                str(event.get("ledger_source") or "")
                for event in events if event.get("ledger_source")
            ))
            realized_values = [
                float(event.get("realized_pnl") or 0)
                for event in events if event.get("realized_pnl") is not None
            ]
            merged[stock_code] = {
                "stock_code": stock_code,
                "short_name": next(
                    (event.get("short_name") for event in events if event.get("short_name")),
                    None,
                ),
                "position_state": "SOLD_TODAY",
                "quantity": 0,
                "remaining_quantity": 0,
                "sellable_quantity": 0,
                "sold_quantity_today": sold_quantity,
                "cost_price": round(cost_amount / cost_quantity, 4)
                if cost_quantity > 0 and cost_amount > 0 else None,
                "buy_at": buy_times[0] if buy_times else None,
                "sell_at": sell_times[-1] if sell_times else None,
                "sell_price": round(sell_amount / sold_quantity, 4)
                if sold_quantity > 0 and sell_amount > 0 else None,
                "realized_pnl": round(sum(realized_values), 2) if realized_values else None,
                "holding_notes": notes,
                "last_reason": "；".join(notes) or "今日已卖出",
                "ledger_source": sources[0] if len(sources) == 1 else "MERGED_LEDGER",
                "ledger_sources": sources,
                "position_lot_count": len(events),
                "lot_details": [
                    {
                        "ledger_source": event.get("ledger_source"),
                        "quantity": event.get("sold_quantity"),
                        "cost_price": event.get("cost_price"),
                        "buy_at": event.get("buy_at"),
                        "sell_at": event.get("sell_at"),
                        "sell_price": event.get("sell_price"),
                        "realized_pnl": event.get("realized_pnl"),
                        "note": event.get("note"),
                    }
                    for event in events
                ],
            }
        return merged

    sales_by_code = merge_sale_events(sale_events)
    open_codes = {str(row.get("stock_code") or "").zfill(6) for row in positions}
    for position in positions:
        sale = sales_by_code.get(str(position.get("stock_code") or "").zfill(6))
        if not sale:
            continue
        position.update({
            "sell_at": sale.get("sell_at"),
            "sell_price": sale.get("sell_price"),
            "sold_quantity_today": sale.get("sold_quantity_today") or 0,
            "lot_details": [
                *(position.get("lot_details") or []),
                *(sale.get("lot_details") or []),
            ],
        })
    today_closed_positions = [
        sale for stock_code, sale in sales_by_code.items()
        if stock_code not in open_codes
    ]

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
    latest_equity = account.get("latest_equity") or {}
    legacy_market_value = sum(
        float(row.get("buy_price") or 0) * int(row.get("buy_shares") or 0)
        for row in legacy_positions
    )
    legacy_current_market_value = sum(
        float(
            (latest_quotes.get(str(row.get("stock_code") or "").zfill(6)) or {}).get("price")
            or row.get("buy_price")
            or 0
        ) * int(row.get("buy_shares") or 0)
        for row in legacy_positions
    )
    legacy_initial_cash = float(
        (legacy_capital_rows[0] if legacy_capital_rows else {}).get("initial_capital")
        or 1_000_000
    )
    legacy_realized_pnl = float(
        (legacy_profit_rows[0] if legacy_profit_rows else {}).get("realized_profit")
        or 0
    )

    def pending_buy_amount(row: dict[str, Any]) -> float:
        if str(row.get("status") or "").upper() not in {"PENDING", "PARTIAL"}:
            return 0.0
        if str(row.get("side") or "").upper() != "BUY":
            return 0.0
        remaining = int(row.get("remaining_shares") or 0)
        if remaining <= 0:
            remaining = max(
                0,
                int(row.get("requested_shares") or 0)
                - int(row.get("filled_shares") or 0),
            )
        price = float(row.get("limit_price") or row.get("target_price") or 0)
        return remaining * price

    legacy_pending_buy_amount = sum(
        pending_buy_amount(row) for row in legacy_orders
    )
    legacy_unrealized_pnl = legacy_current_market_value - legacy_market_value
    legacy_cash_balance = (
        legacy_initial_cash
        + legacy_realized_pnl
        - legacy_market_value
        - legacy_pending_buy_amount
    )
    legacy_total_equity = (
        legacy_initial_cash + legacy_realized_pnl + legacy_unrealized_pnl
    )
    account_cash = account.get("cash_balance")
    v2_cash = float(account_cash if account_cash is not None else initial_cash)
    equity_cash = latest_equity.get("cash_balance")
    canonical_cash = float(equity_cash if equity_cash is not None else v2_cash)
    equity_market_value = latest_equity.get("market_value")
    canonical_market_value = float(
        equity_market_value if equity_market_value is not None else 0
    )
    equity_total = latest_equity.get("total_equity")
    canonical_total_equity = float(
        equity_total
        if equity_total is not None
        else canonical_cash + canonical_market_value
    )
    total_market_value = round(sum(float(row.get("market_value") or 0) for row in positions), 2)
    total_unrealized_pnl = round(sum(float(row.get("unrealized_pnl") or 0) for row in positions), 2)
    legacy_account_present = bool(
        legacy_capital_rows or legacy_positions or legacy_orders
    )
    if legacy_account_present and v2_positions:
        display_account_scope = "MERGED_LEDGER"
        display_cash_balance = canonical_cash + legacy_cash_balance
        display_total_equity = canonical_total_equity + legacy_total_equity
    elif legacy_account_present:
        display_account_scope = "LEGACY_EVENT_SIM_ACTIVE"
        display_cash_balance = legacy_cash_balance
        display_total_equity = legacy_total_equity
    else:
        display_account_scope = "V2_CANONICAL"
        display_cash_balance = canonical_cash
        display_total_equity = canonical_total_equity
    return _envelope({
        "account_id": account_id,
        "account": account,
        "positions": positions,
        "today_closed_positions": today_closed_positions,
        "orders": orders,
        "summary": {
            "position_count": len(positions),
            "position_lot_count": len(position_lots),
            "order_count": len(orders),
            "v2_position_count": len(v2_positions),
            "legacy_position_count": len(legacy_positions),
            "today_sold_count": len(sales_by_code),
            "today_closed_position_count": len(today_closed_positions),
            "v2_order_count": len(v2_orders),
            "legacy_order_count": len(legacy_orders),
            "cash_balance": v2_cash,
            "canonical_initial_cash": initial_cash,
            "canonical_cash_balance": round(canonical_cash, 2),
            "canonical_market_value": round(canonical_market_value, 2),
            "canonical_total_equity": round(canonical_total_equity, 2),
            "canonical_equity_trade_date": latest_equity.get("trade_date"),
            "canonical_account_name": account.get("account_name") or "V2 主模拟账户",
            "canonical_account_scope": "V2_CANONICAL_ONLY",
            "legacy_initial_cash": round(legacy_initial_cash, 2),
            "legacy_realized_pnl": round(legacy_realized_pnl, 2),
            "legacy_pending_buy_amount": round(legacy_pending_buy_amount, 2),
            "legacy_cash_balance": round(legacy_cash_balance, 2),
            "legacy_market_value": round(legacy_current_market_value, 2),
            "legacy_total_equity": round(legacy_total_equity, 2),
            "legacy_cost_market_value": round(legacy_market_value, 2),
            "current_market_value": total_market_value,
            "total_unrealized_pnl": total_unrealized_pnl,
            "display_account_scope": display_account_scope,
            "display_cash_balance": round(display_cash_balance, 2),
            "display_total_equity": round(display_total_equity, 2),
        },
        "ledger_sources": ["V2_CANONICAL", "LEGACY_EVENT_SIM"],
        "real_trading_enabled": False,
        "merge_policy": "READ_ONLY_GROUP_BY_STOCK_CODE_WEIGHTED_COST",
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
