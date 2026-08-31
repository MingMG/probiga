from __future__ import annotations

import hashlib
import json
import uuid
from datetime import date, datetime, time
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Iterable, Mapping

from sqlalchemy import text
from sqlalchemy.engine import Engine

from server.trading_v2.execution import _sync_v3_execution_plan_states
from server.trading_v2.execution_buy_gate import (
    GATE_MODULE,
    evaluate_buy_gate,
    load_current_buy_gate,
)
from server.trading_v2.legacy_strategy_account_boundary import (
    require_legacy_strategy_account,
)
from server.trading_v2.oms import order_idempotency_key
from server.trading_v2.position_monitor import _persist_exit_chain
from server.common.strategy_governance_mode import (
    strategy_governance_database_deferred,
)

from .config import load_v3_config
from .decision_truth import (
    DECISION_INTEGRITY_SCHEMA_VERSION,
    FORECAST_LEDGER_SQL_COLUMNS,
    canonical_forecast_ledger,
    canonical_hash,
    canonical_target_ledger,
    decision_result_hash,
)
from .forward_evidence import ATTRIBUTION_VERSION, primary_strategy_version


ACTIVE_ORDER_STATES = (
    "CREATED",
    "RISK_APPROVED",
    "QUEUED",
    "PARTIALLY_FILLED",
)
DYNAMIC_SHADOW_BOOTSTRAP_MAX_PAPER_ORDERS_PER_RUN = 20
DYNAMIC_SHADOW_BOOTSTRAP_MAX_PLANS_SCANNED_PER_RUN = 1000


def _canonical_v2_buy_receipt(
    connection,
    *,
    decision_run_uid: str,
    strategy_version: str,
    stock_code: str,
    now: datetime,
) -> tuple[dict[str, Any] | None, str]:
    """Require one V2-executor-verifiable receipt before V3 can enqueue BUY."""

    loaded = load_current_buy_gate(
        connection,
        decision_run_uid=decision_run_uid,
        strategy_version=strategy_version,
        stock_code=stock_code,
        as_of=now,
        lock=True,
    )
    if loaded.binding is None:
        return None, loaded.reason_code or "BUY_GATE_DATA_BLOCKED"
    decision = evaluate_buy_gate(
        now=now,
        decision_run_uid=decision_run_uid,
        strategy_version=strategy_version,
        stock_code=stock_code,
        bound=loaded.binding,
        current=loaded.binding,
    )
    if not decision.allowed:
        return None, decision.reason_code or "BUY_GATE_DATA_BLOCKED"
    return loaded.binding, ""


def _canonical_governance_buy_receipt(
    connection,
    *,
    trade_date: date,
    stock_code: str,
    strategy_keys: list[str],
) -> tuple[dict[str, Any] | None, str]:
    """Require exact canonical governance allocation for every new BUY.

    Governance suspension never blocks an exit.  This receipt is requested
    only in the BUY path and binds the V3 sample owner to the immutable
    stock-level paper plan plus the live lifecycle registry.
    """

    if strategy_governance_database_deferred():
        return None, "GOVERNANCE_DATABASE_DEFERRED"
    try:
        rows = connection.execute(
            text(
                """
                SELECT run_uid, trade_date, input_ready, build_commit_sha,
                       input_hash, decision_hash, result_json, result_hash
                FROM st_strategy_governance_run
                WHERE trade_date = :trade_date
                  AND status = 'COMPLETED'
                  AND is_canonical = 1
                ORDER BY run_revision DESC, created_at DESC
                LIMIT 2
                """
            ),
            {"trade_date": trade_date},
        ).mappings().all()
    except Exception:
        return None, "GOVERNANCE_CANONICAL_LEDGER_UNAVAILABLE"
    if len(rows) != 1 or int(rows[0].get("input_ready") or 0) != 1:
        return None, "GOVERNANCE_CANONICAL_RUN_NOT_READY"
    ledger = dict(rows[0])
    try:
        # Import lazily to keep the trading runtime's module graph acyclic.  A
        # raw result hash and self-consistent nested hashes are not sufficient:
        # the governance validator independently replays the candidate set,
        # allocation, stock plan, pools, market routes and decision hash before
        # a BUY may consume the row.
        from server.engine.strategy_governance import (
            STATISTICAL_DECISION_CONTRACT,
            _canonical_governance_result_from_row,
        )

        result = _canonical_governance_result_from_row(ledger)
    except (RuntimeError, TypeError, ValueError, json.JSONDecodeError):
        return None, "GOVERNANCE_CANONICAL_REPLAY_INVALID"
    if (
        result.get("input_ready") is not True
        or result.get("automatic_real_order_submission") is not False
        or result.get("decision_contract_version")
        != STATISTICAL_DECISION_CONTRACT
        or result.get("statistical_funding_eligible") is not True
    ):
        return None, "GOVERNANCE_CANONICAL_IDENTITY_INVALID"
    plan = result.get("paper_execution_plan")
    if not isinstance(plan, dict):
        return None, "GOVERNANCE_PAPER_PLAN_MISSING"
    plan_payload = {
        str(key): value for key, value in plan.items()
        if str(key) != "plan_hash"
    }
    plan_hash = str(plan.get("plan_hash") or "")
    if (
        canonical_hash(plan_payload) != plan_hash
        or plan_hash != str(result.get("paper_execution_plan_hash") or "")
        or plan.get("automatic_real_order_submission") is not False
        or plan.get("real_order_authority") is not False
    ):
        return None, "GOVERNANCE_PAPER_PLAN_HASH_INVALID"
    targets = [
        item for item in (plan.get("targets") or [])
        if isinstance(item, dict)
        and str(item.get("stock_code") or "") == stock_code
    ]
    if len(targets) != 1:
        return None, "GOVERNANCE_STOCK_NOT_ALLOCATION_BACKED"
    target = targets[0]
    target_payload = {
        str(key): value for key, value in target.items()
        if str(key) != "target_hash"
    }
    owner = str(target.get("strategy_key") or "")
    version = str(target.get("strategy_version") or "")
    normalized_keys = sorted({str(key) for key in strategy_keys if str(key)})
    if (
        canonical_hash({
            "schema": "probiga.governance-paper-target.v1",
            **target_payload,
        }) != str(target.get("target_hash") or "")
        or target.get("allocation_backed") is not True
        or target.get("new_buy_allowed") is not True
        or target.get("real_order_authority") is not False
        or int(target.get("target_bp") or 0) <= 0
        or not owner
        or owner not in normalized_keys
        or not version
    ):
        return None, "GOVERNANCE_STOCK_TARGET_BINDING_INVALID"
    try:
        live = connection.execute(
            text(
                """
                SELECT r.current_version, r.current_status, r.enabled,
                       v.source_kind, v.version_hash
                FROM st_strategy_registry r
                JOIN st_strategy_version v
                  ON v.strategy_key=r.strategy_key
                 AND v.version=r.current_version
                WHERE r.strategy_key = :strategy_key
                LIMIT 1
                """
            ),
            {"strategy_key": owner},
        ).mappings().first()
    except Exception:
        return None, "GOVERNANCE_LIFECYCLE_REGISTRY_UNAVAILABLE"
    if (
        live is None
        or int(live.get("enabled") or 0) != 1
        or str(live.get("current_status") or "") not in {"ACTIVE", "REDUCE"}
        or str(live.get("current_version") or "") != version
    ):
        return None, "GOVERNANCE_LIFECYCLE_BLOCKED"
    receipt_payload = {
        "schema": "probiga.governance-paper-buy-receipt.v1",
        "governance_run_uid": str(ledger.get("run_uid") or ""),
        "trade_date": str(trade_date),
        "build_commit_sha": str(ledger.get("build_commit_sha") or ""),
        "decision_hash": str(ledger.get("decision_hash") or ""),
        "paper_plan_hash": plan_hash,
        "target_hash": str(target.get("target_hash") or ""),
        "stock_code": stock_code,
        "strategy_key": owner,
        "strategy_version": version,
        "strategy_version_hash": str(live.get("version_hash") or ""),
        "strategy_source_kind": str(live.get("source_kind") or ""),
        "target_bp": int(target.get("target_bp") or 0),
        "new_buy_allowed": True,
        "exit_always_allowed": True,
        "real_order_authority": False,
    }
    return {
        **receipt_payload,
        "receipt_hash": canonical_hash(receipt_payload),
    }, ""


def _cancel_superseded_v3_buys(
    connection,
    *,
    account_id: str,
    run_uid: str,
    now: datetime,
) -> dict[str, list[dict[str, Any]]]:
    """Cancel unfilled V3 buy work left by older portfolio decisions.

    The latest completed decision is canonical.  Keeping older discovery
    orders queued can make several mutually exclusive two-stock portfolios
    execute together on the next session.  A partially-filled order keeps its
    completed fills/lots, but its unfilled remainder is cancelled; the latest
    decision can then size a replacement from the reconciled position.
    """

    stale_orders = connection.execute(
        text(
            """
            SELECT o.order_id, o.stock_code, o.quantity,
                   o.filled_quantity, o.status,
                   i.decision_run_uid
            FROM st_order_v2 o
            JOIN st_trade_intent_v2 i ON i.intent_id = o.intent_id
            WHERE o.account_id = :account_id
              AND o.side = 'BUY'
              AND i.reason_code IN (
                  'V3_PAPER_DISCOVERY',
                  'V3_VALIDATED_POSITIVE'
              )
              AND i.decision_run_uid <> :run_uid
              AND o.status IN (
                  'CREATED', 'RISK_APPROVED', 'QUEUED',
                  'PARTIALLY_FILLED'
              )
            FOR UPDATE
            """
        ),
        {"account_id": account_id, "run_uid": run_uid},
    ).mappings().all()
    cancelled_orders: list[dict[str, Any]] = []
    cancelled_partial_orders: list[dict[str, Any]] = []
    for raw in stale_orders:
        order = dict(raw)
        is_partial = int(order.get("filled_quantity") or 0) > 0
        connection.execute(
            text(
                """
                UPDATE st_order_v2
                SET status = 'CANCELLED',
                    waiting_reason = :waiting_reason,
                    updated_at = :updated_at
                WHERE order_id = :order_id
                  AND status IN (
                      'CREATED', 'RISK_APPROVED', 'QUEUED',
                      'PARTIALLY_FILLED'
                  )
                """
            ),
            {
                "order_id": order["order_id"],
                "waiting_reason": (
                    "SUPERSEDED_PARTIAL_BY_V3"
                    if is_partial
                    else "SUPERSEDED_BY_V3_DECISION"
                ),
                "updated_at": now,
            },
        )
        cancelled_orders.append(order)
        if is_partial:
            cancelled_partial_orders.append(order)

    stale_plans = connection.execute(
        text(
            """
            SELECT execution_plan_id, run_uid, stock_code,
                   side, quantity, state
            FROM st_execution_plan_v3
            WHERE account_id = :account_id
              AND source IN (
                  'V3_PAPER_DISCOVERY',
                  'V3_PORTFOLIO'
              )
              AND side = 'BUY'
              AND state IN (
                  'PAPER_QUEUED', 'PAPER_PARTIALLY_FILLED'
              )
              AND run_uid <> :run_uid
            FOR UPDATE
            """
        ),
        {"account_id": account_id, "run_uid": run_uid},
    ).mappings().all()
    cancelled_plans: list[dict[str, Any]] = []
    cancelled_partial_plan_keys = {
        (
            str(item.get("decision_run_uid") or ""),
            str(item.get("stock_code") or ""),
        )
        for item in cancelled_partial_orders
    }
    for raw in stale_plans:
        plan = dict(raw)
        plan_key = (
            str(plan.get("run_uid") or ""),
            str(plan.get("stock_code") or ""),
        )
        next_state = (
            "PAPER_PARTIAL_CANCELLED"
            if plan_key in cancelled_partial_plan_keys
            else "CANCELLED"
        )
        connection.execute(
            text(
                """
                UPDATE st_execution_plan_v3
                SET state = :state,
                    updated_at = :updated_at
                WHERE execution_plan_id = :execution_plan_id
                  AND state IN (
                      'PAPER_QUEUED', 'PAPER_PARTIALLY_FILLED'
                  )
                """
            ),
            {
                "execution_plan_id": plan["execution_plan_id"],
                "state": next_state,
                "updated_at": now,
            },
        )
        cancelled_plans.append(plan)
    return {
        "cancelled_orders": cancelled_orders,
        "cancelled_execution_plans": cancelled_plans,
        "cancelled_partial_orders": cancelled_partial_orders,
    }


def _hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def _ownership_hash(
    run_uid: str,
    forecast_id: str,
    stock_code: str,
    strategy_key: str,
    strategy_version: str,
) -> str:
    return hashlib.sha256(
        (
            f"{run_uid}|{forecast_id}|{stock_code}|{strategy_key}|"
            f"{strategy_version}"
        ).encode("utf-8")
    ).hexdigest()


def freeze_pending_v3_buys(
    engine: Engine,
    *,
    account_id: str = "paper-main-v2",
    now: datetime | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Fail closed before a new premarket decision starts computing."""

    require_legacy_strategy_account(
        account_id,
        entrypoint="trading_v3.freeze_pending_v3_buys",
    )
    frozen_at = (now or datetime.now()).replace(microsecond=0)
    with engine.begin() as connection:
        real_enabled = connection.execute(
            text(
                """
                SELECT real_trading_enabled
                FROM st_trade_account_v2
                WHERE account_id = :account_id
                FOR UPDATE
                """
            ),
            {"account_id": account_id},
        ).scalar()
        if real_enabled is None:
            raise RuntimeError("V3 paper account not found")
        if int(real_enabled or 0) != 0:
            raise RuntimeError(
                "V3 safety violation: real trading must remain disabled"
            )
        return _cancel_superseded_v3_buys(
            connection,
            account_id=account_id,
            run_uid=(
                "__V3_PREMARKET_FREEZE__"
                + frozen_at.date().isoformat()
            ),
            now=frozen_at,
        )


def _next_trade_date(connection, source_date: date) -> date:
    value = connection.execute(
        text(
            """
            SELECT MIN(trade_date)
            FROM si_trade_calendar
            WHERE trade_status = 1
              AND trade_date > :source_date
            """
        ),
        {"source_date": source_date},
    ).scalar()
    if value is None:
        raise RuntimeError("V3_NEXT_TRADE_SESSION_UNAVAILABLE")
    return value if isinstance(value, date) else date.fromisoformat(str(value))


def _verify_persisted_decision_truth(
    connection,
    *,
    account: dict[str, Any],
    run: dict[str, Any],
    targets: list[dict[str, Any]],
    account_id: str,
    now: datetime,
) -> tuple[dict[str, Any], float, bool, str]:
    """Revalidate the immutable decision snapshot before creating BUYs."""

    if str(account.get("status") or "") != "ACTIVE":
        raise RuntimeError("V3_ACCOUNT_NOT_ACTIVE")
    if int(account.get("real_trading_enabled") or 0) != 0:
        raise RuntimeError("V3_REAL_TRADING_SWITCH_ENABLED")
    if float(account.get("cash_balance") or 0) < 0:
        raise RuntimeError("V3_ACCOUNT_CASH_INVALID")
    integrity_reason = ""

    def block(reason: str) -> None:
        nonlocal integrity_reason
        if not integrity_reason:
            integrity_reason = reason

    try:
        portfolio = json.loads(str(run.get("portfolio_json") or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        portfolio = {}
        block("V3_PORTFOLIO_MANIFEST_INVALID")
    manifest = dict(portfolio.get("decision_snapshot") or {})
    decision_truth = dict(portfolio.get("decision_truth") or {})
    if bool(manifest) != bool(decision_truth):
        block("V3_DECISION_TRUTH_ENVELOPE_INCOMPLETE")
    if manifest and decision_truth:
        stored_hash = str(manifest.pop("manifest_hash", ""))
        if not stored_hash or canonical_hash(manifest) != stored_hash:
            block("V3_DECISION_SNAPSHOT_HASH_MISMATCH")
        manifest["manifest_hash"] = stored_hash
        if str(manifest.get("trade_date") or "") != str(run["trade_date"]):
            block("V3_DECISION_SNAPSHOT_DATE_MISMATCH")
        if (
            str((manifest.get("account") or {}).get("account_id") or "")
            != account_id
        ):
            block("V3_DECISION_SNAPSHOT_ACCOUNT_MISMATCH")
        if str(
            (manifest.get("reconciliation") or {}).get("status") or ""
        ) != "PASS":
            block("V3_DECISION_SNAPSHOT_RECONCILIATION_BLOCKED")
        if str(decision_truth.get("schema_version") or "") != (
            "probiga.trading-v3.decision-truth.v1"
        ):
            block("V3_DECISION_TRUTH_SCHEMA_INVALID")
        if decision_truth.get("order_authority") is not False:
            block("V3_DECISION_TRUTH_ORDER_AUTHORITY_INVALID")
        if decision_truth.get("real_order_allowed") is not False:
            block("V3_DECISION_TRUTH_REAL_ORDER_INVALID")
        if str(decision_truth.get("execution_authority") or "") != (
            "V2_CANONICAL_LEDGER"
        ):
            block("V3_DECISION_TRUTH_EXECUTION_AUTHORITY_INVALID")
        lifecycle = str(run.get("lifecycle_status") or "").upper()
        paper_lifecycle = lifecycle in {"PAPER_TRIAL", "PAPER_ACTIVE"}
        expected_scope = (
            "INTERNAL_PAPER_TRIAL" if paper_lifecycle else "RESEARCH_ONLY"
        )
        if str(decision_truth.get("decision_scope") or "") != expected_scope:
            block("V3_DECISION_TRUTH_SCOPE_MISMATCH")
        if (
            not paper_lifecycle
            and str(decision_truth.get("paper_order_authority") or "")
            != "NONE"
        ):
            block("V3_RESEARCH_RUN_CLAIMS_PAPER_AUTHORITY")
        truth_run_status = str(
            decision_truth.get("run_status") or ""
        ).upper()
        if truth_run_status not in {"COMPLETED", "BLOCKED"}:
            block("V3_DECISION_TRUTH_RUN_STATUS_INVALID")
        if str(run.get("status") or "").upper() not in {
            "PROCESSING",
            truth_run_status,
        }:
            block("V3_DECISION_TRUTH_RUN_STATUS_MISMATCH")
    source_date = run["trade_date"]
    equity = connection.execute(
        text(
            """
            SELECT total_equity, cash_balance, trade_date, created_at
            FROM st_equity_daily_v2
            WHERE account_id = :account_id
              AND trade_date = :trade_date
              AND created_at <= :now
            ORDER BY created_at DESC
            LIMIT 1
            """
        ),
        {
            "account_id": account_id,
            "trade_date": source_date,
            "now": now,
        },
    ).mappings().first()
    reconciliation = connection.execute(
        text(
            """
            SELECT status, reconciliation_hash, trade_date, version,
                   created_at
            FROM st_reconciliation_v2
            WHERE account_id = :account_id
              AND trade_date = :trade_date
              AND created_at <= :now
            ORDER BY version DESC, created_at DESC
            LIMIT 1
            """
        ),
        {
            "account_id": account_id,
            "trade_date": source_date,
            "now": now,
        },
    ).mappings().first()
    account_equity = float((equity or {}).get("total_equity") or 0)
    if account_equity <= 0:
        block("V3_EQUITY_SNAPSHOT_MISSING_OR_ZERO")
    if not reconciliation or str(reconciliation.get("status") or "") != "PASS":
        block("V3_RECONCILIATION_NOT_PASS")
    try:
        account_updated_at = datetime.fromisoformat(
            str(account.get("updated_at") or "")
        )
        reconciliation_created_at = datetime.fromisoformat(
            str((reconciliation or {}).get("created_at") or "")
        )
    except ValueError:
        block("V3_ACCOUNT_RECONCILIATION_CLOCK_MISSING")
        account_updated_at = now
        reconciliation_created_at = datetime.min
    if account_updated_at > reconciliation_created_at:
        block("V3_ACCOUNT_CHANGED_AFTER_RECONCILIATION")
    if manifest and decision_truth:
        frozen_equity = float(
            (manifest.get("equity") or {}).get("total_equity") or 0
        )
        if frozen_equity <= 0:
            block("V3_FROZEN_EQUITY_INVALID")
        if abs(frozen_equity - account_equity) > 0.01:
            block("V3_EQUITY_CHANGED_AFTER_DECISION")
        frozen_reconciliation = str(
            (manifest.get("reconciliation") or {}).get(
                "reconciliation_hash"
            )
            or ""
        )
        if frozen_reconciliation != str(
            (reconciliation or {}).get("reconciliation_hash") or ""
        ):
            block("V3_RECONCILIATION_CHANGED_AFTER_DECISION")

    decision_integrity = dict(portfolio.get("decision_integrity") or {})
    if str(decision_integrity.get("schema_version") or "") != (
        DECISION_INTEGRITY_SCHEMA_VERSION
    ):
        block("V3_DECISION_INTEGRITY_V2_REQUIRED")
    else:
        try:
            source_trade_date = (
                source_date
                if isinstance(source_date, date)
                else date.fromisoformat(str(source_date))
            )
            if any(
                str(row.get("run_uid") or run.get("run_uid") or "")
                != str(run.get("run_uid") or "")
                or str(row.get("trade_date") or source_trade_date)
                != str(source_trade_date)
                for row in targets
            ):
                raise ValueError("target run/date binding mismatch")
            target_ledger = canonical_target_ledger(
                targets,
                run_uid=str(run.get("run_uid") or ""),
                trade_date=source_trade_date,
                persisted=True,
            )
            if (
                canonical_hash(target_ledger)
                != str(decision_integrity.get("target_ledger_hash") or "")
            ):
                raise ValueError("target ledger hash mismatch")
            if len(targets) != int(
                decision_integrity.get("target_count") or 0
            ):
                raise ValueError("target count mismatch")
            forecast_rows = connection.execute(
                text(
                    f"""
                    SELECT {FORECAST_LEDGER_SQL_COLUMNS}
                    FROM st_alpha_forecast_v3
                    WHERE run_uid = :run_uid
                    ORDER BY rank_no, stock_code, strategy_key, forecast_id
                    """
                ),
                {"run_uid": str(run.get("run_uid") or "")},
            ).mappings().all()
            forecast_ledger = canonical_forecast_ledger(forecast_rows)
            if (
                canonical_hash(forecast_ledger)
                != str(
                    decision_integrity.get("forecast_ledger_hash") or ""
                )
            ):
                raise ValueError("forecast ledger hash mismatch")
            persisted_counts = connection.execute(
                text(
                    """
                    SELECT
                        (SELECT COUNT(*) FROM st_alpha_forecast_v3
                         WHERE run_uid = :run_uid) AS forecast_count,
                        (SELECT COUNT(*) FROM st_theme_signal_v3
                         WHERE run_uid = :run_uid) AS theme_signal_count,
                        (SELECT COUNT(*) FROM st_trade_hypothesis_v3
                         WHERE run_uid = :run_uid) AS hypothesis_count
                    """
                ),
                {"run_uid": str(run.get("run_uid") or "")},
            ).mappings().first()
            if not persisted_counts:
                raise ValueError("decision ledger counts unavailable")
            expected_forecast_count = int(
                decision_integrity.get("forecast_count") or 0
            )
            if len(forecast_rows) != expected_forecast_count:
                raise ValueError("forecast ledger row count mismatch")
            expected_theme_count = int(
                decision_integrity.get("persisted_theme_signal_count") or 0
            )
            expected_hypothesis_count = int(
                decision_integrity.get("hypothesis_count") or 0
            )
            if int(persisted_counts.get("forecast_count") or 0) != (
                expected_forecast_count
            ):
                raise ValueError("forecast ledger count mismatch")
            if int(persisted_counts.get("theme_signal_count") or 0) != (
                expected_theme_count
            ):
                raise ValueError("theme ledger count mismatch")
            if int(persisted_counts.get("hypothesis_count") or 0) != (
                expected_hypothesis_count
            ):
                raise ValueError("hypothesis ledger count mismatch")
            if int(run.get("forecast_count") or 0) != expected_forecast_count:
                raise ValueError("run forecast count mismatch")
            if int(run.get("target_count") or 0) != len(targets):
                raise ValueError("run target count mismatch")
            regime = json.loads(str(run.get("regime_json") or "{}"))
            recomputed_result_hash = decision_result_hash(
                regime=regime,
                portfolio=portfolio,
                forecast_count=expected_forecast_count,
                theme_signal_count=int(
                    decision_integrity.get("raw_theme_signal_count") or 0
                ),
                hypothesis_count=expected_hypothesis_count,
            )
            if recomputed_result_hash != str(run.get("result_hash") or ""):
                raise ValueError("decision result hash mismatch")
        except (
            ArithmeticError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ):
            block("V3_DECISION_RESULT_OR_TARGET_LEDGER_UNVERIFIED")

    truth_verified = bool(
        manifest
        and decision_truth
        and not integrity_reason
    )
    return portfolio, account_equity, truth_verified, integrity_reason


def _evaluate_cumulative_buy_risk(
    *,
    requested_quantity: int,
    worst_price: float,
    initial_stop: float,
    current_code_value: float,
    current_total_value: float,
    current_theme_value: float,
    current_open_risk_cny: float,
    available_cash_cny: float,
    current_turnover_cny: float,
    equity_cny: float,
    creates_new_position: bool,
    live_position_count: int,
    maximum_live_positions: int,
    maximum_single_weight: float,
    maximum_total_weight: float,
    maximum_theme_weight: float,
    maximum_open_risk_weight: float,
    maximum_daily_turnover_weight: float,
    commission_rate: float = 0.0,
    minimum_commission_cny: float = 0.0,
    transfer_fee_rate: float = 0.0,
) -> dict[str, Any]:
    notional = max(0, requested_quantity) * max(0.0, worst_price)
    estimated_buy_fee = (
        max(minimum_commission_cny, notional * commission_rate)
        + notional * transfer_fee_rate
        if notional > 0
        else 0.0
    )
    cash_reservation = notional + estimated_buy_fee
    trade_risk = max(0, requested_quantity) * max(
        0.0,
        worst_price - initial_stop,
    )
    post_cash = available_cash_cny - cash_reservation
    post_single_weight = (current_code_value + notional) / equity_cny
    post_total_weight = (current_total_value + notional) / equity_cny
    post_theme_weight = (current_theme_value + notional) / equity_cny
    post_open_risk_cny = current_open_risk_cny + trade_risk
    post_turnover_weight = (current_turnover_cny + notional) / equity_cny
    checks = {
        "CASH_AVAILABLE": post_cash >= -0.01,
        "SINGLE_POSITION_CAP": (
            post_single_weight <= maximum_single_weight + 1e-9
        ),
        "TOTAL_RISK_ASSET_CAP": (
            post_total_weight <= maximum_total_weight + 1e-9
        ),
        "THEME_EXPOSURE_CAP": (
            post_theme_weight <= maximum_theme_weight + 1e-9
        ),
        "OPEN_RISK_CAP": (
            post_open_risk_cny / equity_cny
            <= maximum_open_risk_weight + 1e-9
        ),
        "DAILY_TURNOVER_CAP": (
            post_turnover_weight
            <= maximum_daily_turnover_weight + 1e-9
        ),
        "LIVE_POSITION_CAP": (
            not creates_new_position
            or live_position_count < maximum_live_positions
        ),
        "REAL_TRADING_DISABLED": True,
    }
    first_failure = next(
        (key for key, passed in checks.items() if not passed),
        None,
    )
    return {
        "decision_status": "REJECTED" if first_failure else "APPROVED",
        "approved_quantity": 0 if first_failure else requested_quantity,
        "trade_risk": trade_risk,
        "post_cash": post_cash,
        "post_single_weight": post_single_weight,
        "post_total_weight": post_total_weight,
        "post_theme_weight": post_theme_weight,
        "post_open_risk_cny": post_open_risk_cny,
        "post_turnover_weight": post_turnover_weight,
        "checks": checks,
        "first_failure": first_failure,
        "reserved_notional": notional,
        "cash_reservation": cash_reservation,
        "estimated_buy_fee": estimated_buy_fee,
    }


def _bootstrap_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")).hexdigest()


def _bootstrap_db_number(value: Any, places: int) -> float:
    quantum = Decimal("1").scaleb(-places)
    return float(Decimal(str(value or 0)).quantize(
        quantum, rounding=ROUND_HALF_UP,
    ))


def _bootstrap_exact_price(
    connection: Any,
    *,
    trade_date: date,
    stock_code: str,
) -> float:
    rows = connection.execute(text("""
        SELECT close
        FROM sm_stock_kline
        WHERE trade_date=:trade_date AND stock_code=:stock_code
          AND k_type=1 AND adjust_type=0
        ORDER BY stock_code
    """), {
        "trade_date": trade_date,
        "stock_code": stock_code,
    }).all()
    if len(rows) != 1 or float(rows[0][0] or 0) <= 0:
        raise RuntimeError("DYNAMIC_SHADOW_EXACT_DAILY_PRICE_UNAVAILABLE")
    return float(rows[0][0])


def _bootstrap_exact_industry(
    connection: Any,
    *,
    trade_date: date,
    stock_code: str,
) -> str:
    from server.engine.dynamic_shadow_ledger import (
        verify_dynamic_shadow_industry_fact,
    )

    try:
        fact = verify_dynamic_shadow_industry_fact(
            connection,
            trade_date=trade_date.isoformat(),
            stock_code=stock_code,
        )
    except Exception as exc:
        raise RuntimeError(
            "DYNAMIC_SHADOW_EXACT_INDUSTRY_UNAVAILABLE"
        ) from exc
    return str(fact["industry_name"])


def _bootstrap_portfolio_state(
    connection: Any,
    *,
    account_id: str,
    trade_date: date,
    execution_date: date,
    commission_rate: float,
    minimum_commission_cny: float,
    transfer_fee_rate: float,
) -> dict[str, Any]:
    """Load aggregate risk only inside the trusted system controller."""

    positions = [dict(row) for row in connection.execute(text("""
        SELECT stock_code, remaining_quantity, protective_stop
        FROM st_position_lot_v2
        WHERE account_id=:account_id AND remaining_quantity>0
        ORDER BY stock_code, lot_id
    """), {"account_id": account_id}).mappings().all()]
    reservations = [dict(row) for row in connection.execute(text("""
        SELECT o.stock_code, o.quantity, o.filled_quantity,
               o.limit_price, i.worst_price, i.protective_stop
        FROM st_order_v2 o
        JOIN st_trade_intent_v2 i ON i.intent_id=o.intent_id
        WHERE o.account_id=:account_id AND o.side='BUY'
          AND o.status IN ('CREATED','RISK_APPROVED','QUEUED','PARTIALLY_FILLED')
        ORDER BY o.stock_code, o.order_id
    """), {"account_id": account_id}).mappings().all()]
    codes = sorted({
        str(row.get("stock_code") or "")
        for row in (*positions, *reservations)
        if str(row.get("stock_code") or "")
    })
    prices = {
        code: _bootstrap_exact_price(
            connection, trade_date=trade_date, stock_code=code,
        )
        for code in codes
    }
    industries = {
        code: _bootstrap_exact_industry(
            connection, trade_date=trade_date, stock_code=code,
        )
        for code in codes
    }
    code_values: dict[str, float] = {}
    industry_values: dict[str, float] = {}
    total_value = 0.0
    open_risk = 0.0
    turnover = float(connection.execute(text("""
        SELECT COALESCE(SUM(ABS(gross_amount)), 0)
        FROM st_fill_v2
        WHERE account_id=:account_id AND side='BUY'
          AND DATE(filled_at)=:execution_date
    """), {
        "account_id": account_id,
        "execution_date": execution_date,
    }).scalar() or 0)
    reserved_cash = 0.0
    live_codes: set[str] = set()
    for row in positions:
        code = str(row.get("stock_code") or "")
        quantity = int(row.get("remaining_quantity") or 0)
        price = prices[code]
        value = quantity * price
        industry = industries[code]
        code_values[code] = code_values.get(code, 0.0) + value
        industry_values[industry] = industry_values.get(industry, 0.0) + value
        total_value += value
        stop = float(row.get("protective_stop") or 0)
        open_risk += quantity * (
            max(0.0, price - stop) if stop > 0 else price * 0.08
        )
        live_codes.add(code)
    for row in reservations:
        code = str(row.get("stock_code") or "")
        remaining = max(
            0,
            int(row.get("quantity") or 0)
            - int(row.get("filled_quantity") or 0),
        )
        price = max(
            float(row.get("worst_price") or 0),
            float(row.get("limit_price") or 0),
        )
        value = remaining * price
        industry = industries[code]
        code_values[code] = code_values.get(code, 0.0) + value
        industry_values[industry] = industry_values.get(industry, 0.0) + value
        total_value += value
        turnover += value
        reserved_cash += value + (
            max(minimum_commission_cny, value * commission_rate)
            + value * transfer_fee_rate
            if value > 0 else 0.0
        )
        stop = float(row.get("protective_stop") or 0)
        open_risk += remaining * (
            max(0.0, price - stop) if stop > 0 else price * 0.08
        )
        live_codes.add(code)
    return {
        "position_codes": {
            str(row.get("stock_code") or "") for row in positions
        },
        "reservation_codes": {
            str(row.get("stock_code") or "") for row in reservations
        },
        "live_codes": live_codes,
        "code_values": code_values,
        "industry_values": industry_values,
        "total_value": total_value,
        "open_risk_cny": open_risk,
        "daily_buy_turnover_cny": turnover,
        "reserved_cash_cny": reserved_cash,
    }


def materialize_dynamic_shadow_bootstrap_orders(
    connection: Any,
    *,
    plan_ids: Iterable[str],
    account_id: str = "paper-main-v2",
    now: datetime | None = None,
) -> dict[str, Any]:
    """Create a bounded V2/V3 internal-paper BUY from exact shadow plans.

    This is an intentionally separate bootstrap lane.  It does not consume a
    canonical portfolio target, never grants broker authority, and cannot be
    called with caller-enriched signal data.  The trusted controller may read
    other positions/orders solely to fail closed on aggregate risk; no such
    state is exposed to the strategy adapter.
    """

    from server.engine.dynamic_shadow_ledger import (
        BOOTSTRAP_REASON_CODE,
        DynamicShadowLedgerError,
        build_dynamic_shadow_bootstrap_authorization,
        verify_dynamic_shadow_bootstrap_risk_binding,
    )

    require_legacy_strategy_account(
        account_id,
        entrypoint="trading_v3.materialize_dynamic_shadow_bootstrap_orders",
    )
    all_plan_ids = list(dict.fromkeys(
        str(value) for value in plan_ids if str(value)
    ))
    if not all_plan_ids:
        return {
            "status": "ok",
            "created": [],
            "skipped": [],
            "paper_order_count": 0,
            "new_paper_order_count": 0,
            "idempotent_paper_order_count": 0,
            "scanned_plan_count": 0,
            "scanned_plan_ids": [],
            "deferred_plan_count": 0,
            "maximum_paper_orders_per_run": (
                DYNAMIC_SHADOW_BOOTSTRAP_MAX_PAPER_ORDERS_PER_RUN
            ),
            "real_order_count": 0,
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
    observed_at = (now or datetime.now()).replace(microsecond=0)
    lock_clause = (
        " FOR UPDATE"
        if str(getattr(connection.dialect, "name", "")).casefold()
        in {"mysql", "mariadb"}
        else ""
    )
    account = connection.execute(text(
        "SELECT account_id, status, cash_balance, real_trading_enabled "
        "FROM st_trade_account_v2 WHERE account_id=:account_id"
        + lock_clause
    ), {"account_id": account_id}).mappings().first()
    normalized_plan_ids = all_plan_ids[
        :DYNAMIC_SHADOW_BOOTSTRAP_MAX_PLANS_SCANNED_PER_RUN
    ]
    deferred_plan_ids = all_plan_ids[
        DYNAMIC_SHADOW_BOOTSTRAP_MAX_PLANS_SCANNED_PER_RUN:
    ]
    if not account:
        return {
            "status": "BLOCKED",
            "created": [],
            "skipped": [
                {"plan_id": plan_id, "reason": "PAPER_ACCOUNT_MISSING"}
                for plan_id in all_plan_ids
            ],
            "paper_order_count": 0,
            "new_paper_order_count": 0,
            "idempotent_paper_order_count": 0,
            "scanned_plan_count": 0,
            "scanned_plan_ids": [],
            "deferred_plan_count": 0,
            "maximum_paper_orders_per_run": (
                DYNAMIC_SHADOW_BOOTSTRAP_MAX_PAPER_ORDERS_PER_RUN
            ),
            "real_order_count": 0,
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
    if (
        str(account.get("status") or "") != "ACTIVE"
        or int(account.get("real_trading_enabled") or 0) != 0
    ):
        raise RuntimeError("dynamic shadow bootstrap requires active paper-only account")
    config = load_v3_config()
    portfolio_policy = dict(config.get("portfolio") or {})
    execution_policy = dict(config.get("paper_execution") or {})
    account_policy = dict(config.get("account") or {})
    maximum_live_positions = min(
        50,
        max(1, int(execution_policy.get("maximum_live_positions", 12))),
    )
    maximum_single_weight = min(
        0.01,
        float(portfolio_policy.get("maximum_single_position_weight", 0.12)),
    )
    maximum_total_weight = 0.20
    maximum_industry_weight = min(
        0.10,
        float(portfolio_policy.get("maximum_theme_weight", 0.25)),
    )
    maximum_open_risk_weight = min(
        0.02,
        float(portfolio_policy.get("maximum_open_risk", 0.02)),
    )
    maximum_daily_buy_turnover_weight = max(0.0, min(
        0.30,
        float(portfolio_policy.get("maximum_daily_turnover", 0.30)),
    ))
    commission_rate = float(account_policy.get("commission_rate", 0.0))
    minimum_commission_cny = float(
        account_policy.get("minimum_commission_cny", 0.0)
    )
    transfer_fee_rate = float(account_policy.get("transfer_fee_rate", 0.0))
    limit_premium = max(0.0, min(
        0.05,
        float(execution_policy.get("maximum_entry_premium_pct", 0.5))
        / 100.0,
    ))
    worst_premium = max(limit_premium, max(0.0, min(
        0.10,
        float(execution_policy.get("worst_price_premium_pct", 1.0))
        / 100.0,
    )))
    created: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = [
        {
            "plan_id": plan_id,
            "reason": "DYNAMIC_SHADOW_BOOTSTRAP_SCAN_LIMIT_DEFERRED",
        }
        for plan_id in deferred_plan_ids
    ]
    scanned_plan_count = 0
    for plan_index, plan_id in enumerate(normalized_plan_ids):
        if (
            len(created)
            >= DYNAMIC_SHADOW_BOOTSTRAP_MAX_PAPER_ORDERS_PER_RUN
        ):
            skipped.extend({
                "plan_id": deferred_id,
                "reason": "DYNAMIC_SHADOW_BOOTSTRAP_ORDER_CAPACITY_DEFERRED",
            } for deferred_id in normalized_plan_ids[plan_index:])
            break
        scanned_plan_count += 1
        mutation_started = False
        try:
            authorization = build_dynamic_shadow_bootstrap_authorization(
                connection, plan_id=plan_id,
            )
            if str(authorization["account_id"]) != account_id:
                raise DynamicShadowLedgerError("bootstrap计划账户不一致")
            version = str(authorization["strategy_version"])
            if len(version) > 80:
                raise DynamicShadowLedgerError("bootstrap策略版本超过V2冻结列上限")
            if len(str(authorization["strategy_key"])) > 64:
                raise DynamicShadowLedgerError("bootstrap策略键超过V3证据列上限")
            intent_id = _bootstrap_hash({
                "schema": "probiga.dynamic-shadow-bootstrap-intent-id.v1",
                "plan_id": plan_id,
                "plan_hash": authorization["plan_hash"],
                "account_id": account_id,
            })[:32]
            existing = connection.execute(text("""
                SELECT intent_id, evidence_json
                FROM st_trade_intent_v2 WHERE intent_id=:intent_id
            """), {"intent_id": intent_id}).mappings().first()
            if existing:
                evidence = json.loads(str(existing.get("evidence_json") or "{}"))
                risk_binding = evidence.get("dynamic_shadow_risk")
                if not isinstance(risk_binding, Mapping):
                    raise DynamicShadowLedgerError("bootstrap幂等意图风险绑定缺失")
                verify_dynamic_shadow_bootstrap_risk_binding(
                    connection,
                    risk_binding,
                    intent_id=intent_id,
                    require_current_shadow=True,
                )
                order = connection.execute(text("""
                    SELECT order_id, quantity, status
                    FROM st_order_v2 WHERE intent_id=:intent_id
                    ORDER BY order_id
                """), {"intent_id": intent_id}).mappings().first()
                if order:
                    created.append({
                        "plan_id": plan_id,
                        "intent_id": intent_id,
                        "order_id": str(order["order_id"]),
                        "stock_code": authorization["stock_code"],
                        "quantity": int(order["quantity"]),
                        "status": str(order["status"]),
                        "idempotent_replay": True,
                    })
                else:
                    skipped.append({
                        "plan_id": plan_id,
                        "reason": "BOOTSTRAP_RISK_REJECTED_PREVIOUSLY",
                    })
                continue
            trade_date = date.fromisoformat(str(authorization["trade_date"])[:10])
            equity_rows = connection.execute(text("""
                SELECT total_equity
                FROM st_equity_daily_v2
                WHERE account_id=:account_id AND trade_date=:trade_date
            """), {
                "account_id": account_id,
                "trade_date": trade_date,
            }).all()
            if len(equity_rows) != 1 or float(equity_rows[0][0] or 0) <= 0:
                raise RuntimeError("DYNAMIC_SHADOW_EXACT_EQUITY_UNAVAILABLE")
            equity_cny = float(equity_rows[0][0])
            reconciliation = connection.execute(text("""
                SELECT status
                FROM st_reconciliation_v2
                WHERE account_id=:account_id AND trade_date=:trade_date
                ORDER BY version DESC LIMIT 1
            """), {
                "account_id": account_id,
                "trade_date": trade_date,
            }).scalar()
            if str(reconciliation or "") != "PASS":
                raise RuntimeError("DYNAMIC_SHADOW_RECONCILIATION_NOT_PASS")
            execution_date = _next_trade_date(connection, trade_date)
            state = _bootstrap_portfolio_state(
                connection,
                account_id=account_id,
                trade_date=trade_date,
                execution_date=execution_date,
                commission_rate=commission_rate,
                minimum_commission_cny=minimum_commission_cny,
                transfer_fee_rate=transfer_fee_rate,
            )
            code = str(authorization["stock_code"])
            if (
                code in state["position_codes"]
                or code in state["reservation_codes"]
            ):
                raise RuntimeError("DYNAMIC_SHADOW_SINGLE_STOCK_ALREADY_EXPOSED")
            reference_price = _bootstrap_exact_price(
                connection, trade_date=trade_date, stock_code=code,
            )
            limit_price = round(reference_price * (1.0 + limit_premium), 3)
            worst_price = round(reference_price * (1.0 + worst_premium), 3)
            maximum_target_bp = min(
                100,
                int(authorization["maximum_target_bp"]),
            )
            maximum_notional = (
                equity_cny * maximum_target_bp / 10000.0
            )
            requested_quantity = int(maximum_notional / worst_price / 100) * 100
            if requested_quantity < 100:
                raise RuntimeError("DYNAMIC_SHADOW_100BP_BELOW_BOARD_LOT")
            initial_stop = round(reference_price * 0.92, 3)
            industry = str(authorization["industry_name"])
            if len(industry) > 80:
                raise DynamicShadowLedgerError("bootstrap行业名超过V2冻结列上限")
            available_cash = (
                float(account["cash_balance"])
                - float(state["reserved_cash_cny"])
            )
            limits = {
                "maximum_live_positions": maximum_live_positions,
                "maximum_single_weight": maximum_single_weight,
                "maximum_total_weight": maximum_total_weight,
                "maximum_industry_weight": maximum_industry_weight,
                "maximum_open_risk_weight": maximum_open_risk_weight,
                "maximum_daily_buy_turnover_weight": (
                    maximum_daily_buy_turnover_weight
                ),
            }
            risk = _evaluate_cumulative_buy_risk(
                requested_quantity=requested_quantity,
                worst_price=worst_price,
                initial_stop=initial_stop,
                current_code_value=float(state["code_values"].get(code, 0.0)),
                current_total_value=float(state["total_value"]),
                current_theme_value=float(
                    state["industry_values"].get(industry, 0.0)
                ),
                current_open_risk_cny=float(state["open_risk_cny"]),
                available_cash_cny=available_cash,
                current_turnover_cny=float(state["daily_buy_turnover_cny"]),
                equity_cny=equity_cny,
                creates_new_position=True,
                live_position_count=len(state["live_codes"]),
                maximum_live_positions=maximum_live_positions,
                maximum_single_weight=maximum_single_weight,
                maximum_total_weight=maximum_total_weight,
                maximum_theme_weight=maximum_industry_weight,
                maximum_open_risk_weight=maximum_open_risk_weight,
                maximum_daily_turnover_weight=(
                    maximum_daily_buy_turnover_weight
                ),
                commission_rate=commission_rate,
                minimum_commission_cny=minimum_commission_cny,
                transfer_fee_rate=transfer_fee_rate,
            )
            frozen_risk = {
                "trade_risk": _bootstrap_db_number(risk["trade_risk"], 2),
                "post_single_weight": _bootstrap_db_number(
                    risk["post_single_weight"], 8,
                ),
                "post_total_weight": _bootstrap_db_number(
                    risk["post_total_weight"], 8,
                ),
                "post_theme_weight": _bootstrap_db_number(
                    risk["post_theme_weight"], 8,
                ),
                "post_open_risk_cny": _bootstrap_db_number(
                    risk["post_open_risk_cny"], 2,
                ),
                "post_cash": _bootstrap_db_number(risk["post_cash"], 2),
                "post_turnover_weight": _bootstrap_db_number(
                    risk["post_turnover_weight"], 8,
                ),
            }
            risk_decision_payload = {
                "schema": "probiga.dynamic-shadow-bootstrap-risk-decision.v1",
                "plan_id": plan_id,
                "authorization_hash": authorization["authorization_hash"],
                "authorization": authorization,
                "intent_id": intent_id,
                "account_id": account_id,
                "strategy_key": authorization["strategy_key"],
                "strategy_version": version,
                "trade_date": trade_date.isoformat(),
                "execution_date": execution_date.isoformat(),
                "stock_code": code,
                "industry_snapshot_id": authorization["industry_snapshot_id"],
                "industry_row_hash": authorization["industry_row_hash"],
                "industry_name": industry,
                "equity_cny": _bootstrap_db_number(equity_cny, 2),
                "reference_price": reference_price,
                "worst_price": worst_price,
                "initial_stop": initial_stop,
                "maximum_target_bp": maximum_target_bp,
                "requested_quantity": requested_quantity,
                "approved_quantity": int(risk["approved_quantity"]),
                "current_code_value": _bootstrap_db_number(
                    state["code_values"].get(code, 0.0), 2,
                ),
                "current_total_value": _bootstrap_db_number(
                    state["total_value"], 2,
                ),
                "current_industry_value": _bootstrap_db_number(
                    state["industry_values"].get(industry, 0.0), 2,
                ),
                "current_open_risk_cny": _bootstrap_db_number(
                    state["open_risk_cny"], 2,
                ),
                "current_daily_buy_turnover_cny": _bootstrap_db_number(
                    state["daily_buy_turnover_cny"], 2,
                ),
                "available_cash_cny": _bootstrap_db_number(available_cash, 2),
                "live_position_count": len(state["live_codes"]),
                "limits": limits,
                "decision_status": risk["decision_status"],
                "checks": dict(risk["checks"]),
                "first_failure": risk["first_failure"],
                **frozen_risk,
                "automatic_real_order_submission": False,
                "real_order_authority": False,
            }
            decision_hash = _bootstrap_hash(risk_decision_payload)
            risk_binding_payload = {
                "schema": "probiga.dynamic-shadow-bootstrap-risk.v1",
                "decision_payload": risk_decision_payload,
                "decision_hash": decision_hash,
                "automatic_real_order_submission": False,
                "real_order_authority": False,
            }
            risk_binding = {
                **risk_binding_payload,
                "binding_hash": _bootstrap_hash(risk_binding_payload),
            }
            ownership_hash = _ownership_hash(
                authorization["candidate_run_uid"],
                authorization["shadow_forecast_id"],
                code,
                authorization["strategy_key"],
                version,
            )
            evidence = {
                "source": BOOTSTRAP_REASON_CODE,
                "run_uid": authorization["candidate_run_uid"],
                "model_version": version,
                "signal_strategy_keys": [authorization["strategy_key"]],
                "supporting_strategy_keys": [authorization["strategy_key"]],
                "primary_strategy_key": authorization["strategy_key"],
                "primary_strategy_version": version,
                "primary_forecast_id": authorization["shadow_forecast_id"],
                "sample_owner_role": "PRIMARY",
                "attribution_status": "VERIFIED_SNAPSHOT",
                "attribution_version": ATTRIBUTION_VERSION,
                "ownership_hash": ownership_hash,
                "dynamic_shadow_bootstrap": authorization,
                "dynamic_shadow_risk": risk_binding,
                "positive_expectancy_validated": False,
                "real_trading_enabled": False,
                "automatic_real_order_submission": False,
                "real_order_authority": False,
            }
            earliest_at = datetime.combine(execution_date, time(9, 30))
            expires_at = datetime.combine(execution_date, time(14, 45))
            intent_key = _bootstrap_hash({
                "schema": "probiga.dynamic-shadow-bootstrap-intent-key.v1",
                "intent_id": intent_id,
                "plan_id": plan_id,
                "risk_decision_hash": decision_hash,
            })
            mutation_started = True
            connection.execute(text("""
                INSERT INTO st_trade_intent_v2 (
                    intent_id, account_id, decision_run_uid,
                    strategy_version, stock_code, theme_code, action,
                    current_quantity, target_quantity, target_weight,
                    earliest_at, expires_at, limit_price, worst_price,
                    initial_stop, protective_stop, invalidation_condition,
                    reason_code, evidence_json, intent_version,
                    idempotency_key, created_at
                ) VALUES (
                    :intent_id, :account_id, :decision_run_uid,
                    :strategy_version, :stock_code, :theme_code, 'BUY',
                    0, :target_quantity, :target_weight,
                    :earliest_at, :expires_at, :limit_price, :worst_price,
                    :initial_stop, :protective_stop, :invalidation_condition,
                    :reason_code, :evidence_json, 1,
                    :idempotency_key, :created_at
                )
            """), {
                "intent_id": intent_id,
                "account_id": account_id,
                "decision_run_uid": authorization["candidate_run_uid"],
                "strategy_version": version,
                "stock_code": code,
                "theme_code": industry,
                "target_quantity": requested_quantity,
                "target_weight": frozen_risk["post_single_weight"],
                "earliest_at": earliest_at,
                "expires_at": expires_at,
                "limit_price": limit_price,
                "worst_price": worst_price,
                "initial_stop": initial_stop,
                "protective_stop": initial_stop,
                "invalidation_condition": (
                    "动态SHADOW bootstrap固定风险退出；SELL永不受准入门阻断"
                ),
                "reason_code": BOOTSTRAP_REASON_CODE,
                "evidence_json": json.dumps(
                    evidence, ensure_ascii=False, sort_keys=True,
                    separators=(",", ":"), allow_nan=False,
                ),
                "idempotency_key": intent_key,
                "created_at": observed_at,
            })
            connection.execute(text("""
                INSERT INTO st_risk_decision_v2 (
                    intent_id, decision_status, requested_quantity,
                    approved_quantity, trade_risk, post_single_weight,
                    post_total_weight, post_theme_weight, post_open_risk,
                    post_cash, checks_json, first_failure, decision_hash,
                    created_at
                ) VALUES (
                    :intent_id, :decision_status, :requested_quantity,
                    :approved_quantity, :trade_risk, :post_single_weight,
                    :post_total_weight, :post_theme_weight, :post_open_risk,
                    :post_cash, :checks_json, :first_failure,
                    :decision_hash, :created_at
                )
            """), {
                "intent_id": intent_id,
                "decision_status": risk["decision_status"],
                "requested_quantity": requested_quantity,
                "approved_quantity": int(risk["approved_quantity"]),
                "trade_risk": frozen_risk["trade_risk"],
                "post_single_weight": frozen_risk["post_single_weight"],
                "post_total_weight": frozen_risk["post_total_weight"],
                "post_theme_weight": frozen_risk["post_theme_weight"],
                "post_open_risk": frozen_risk["post_open_risk_cny"],
                "post_cash": frozen_risk["post_cash"],
                "checks_json": json.dumps(
                    risk["checks"], sort_keys=True, separators=(",", ":"),
                ),
                "first_failure": risk["first_failure"],
                "decision_hash": decision_hash,
                "created_at": observed_at,
            })
            if risk["decision_status"] != "APPROVED":
                skipped.append({
                    "plan_id": plan_id,
                    "stock_code": code,
                    "reason": str(risk["first_failure"] or "RISK_REJECTED"),
                })
                continue
            verify_dynamic_shadow_bootstrap_risk_binding(
                connection,
                risk_binding,
                intent_id=intent_id,
                require_current_shadow=True,
            )
            order_id = _bootstrap_hash({
                "schema": "probiga.dynamic-shadow-bootstrap-order-id.v1",
                "intent_id": intent_id,
                "decision_hash": decision_hash,
            })[:32]
            order_key = order_idempotency_key(
                account_id=account_id,
                decision_run_uid=authorization["candidate_run_uid"],
                intent_id=intent_id,
                stock_code=code,
                side="BUY",
                target_quantity=requested_quantity,
                intent_version=1,
            )
            connection.execute(text("""
                INSERT INTO st_order_v2 (
                    order_id, account_id, intent_id, stock_code, side,
                    order_type, limit_price, quantity, filled_quantity,
                    status, waiting_reason, earliest_at, expires_at,
                    idempotency_key, created_at, updated_at
                ) VALUES (
                    :order_id, :account_id, :intent_id, :stock_code, 'BUY',
                    'LIMIT', :limit_price, :quantity, 0, 'QUEUED',
                    'DYNAMIC_SHADOW_NEXT_SESSION', :earliest_at, :expires_at,
                    :idempotency_key, :created_at, :updated_at
                )
            """), {
                "order_id": order_id,
                "account_id": account_id,
                "intent_id": intent_id,
                "stock_code": code,
                "limit_price": limit_price,
                "quantity": requested_quantity,
                "earliest_at": earliest_at,
                "expires_at": expires_at,
                "idempotency_key": order_key,
                "created_at": observed_at,
                "updated_at": observed_at,
            })
            execution_plan_id = _bootstrap_hash({
                "schema": "probiga.dynamic-shadow-bootstrap-execution-plan-id.v1",
                "plan_id": plan_id,
                "order_id": order_id,
            })[:32]
            connection.execute(text("""
                INSERT INTO st_execution_plan_v3 (
                    execution_plan_id, run_uid, account_id, trade_date,
                    stock_code, side, quantity, limit_price, state,
                    reason_code, source, real_order_allowed,
                    idempotency_key, created_at, updated_at
                ) VALUES (
                    :execution_plan_id, :run_uid, :account_id, :trade_date,
                    :stock_code, 'BUY', :quantity, :limit_price,
                    'PAPER_QUEUED', :reason_code, :source, 0,
                    :idempotency_key, :created_at, :updated_at
                )
            """), {
                "execution_plan_id": execution_plan_id,
                "run_uid": authorization["candidate_run_uid"],
                "account_id": account_id,
                "trade_date": execution_date,
                "stock_code": code,
                "quantity": requested_quantity,
                "limit_price": limit_price,
                "reason_code": BOOTSTRAP_REASON_CODE,
                "source": BOOTSTRAP_REASON_CODE,
                "idempotency_key": _bootstrap_hash({
                    "plan_id": plan_id,
                    "order_id": order_id,
                    "real_order_allowed": False,
                }),
                "created_at": observed_at,
                "updated_at": observed_at,
            })
            created.append({
                "plan_id": plan_id,
                "intent_id": intent_id,
                "order_id": order_id,
                "execution_plan_id": execution_plan_id,
                "stock_code": code,
                "strategy_key": authorization["strategy_key"],
                "strategy_version": version,
                "quantity": requested_quantity,
                "maximum_target_bp": maximum_target_bp,
                "industry_snapshot_id": authorization["industry_snapshot_id"],
                "industry_row_hash": authorization["industry_row_hash"],
                "execution_date": execution_date.isoformat(),
                "real_order_allowed": False,
                "real_order_authority": False,
                "idempotent_replay": False,
            })
        except (DynamicShadowLedgerError, RuntimeError, ValueError) as exc:
            if mutation_started:
                # Do not commit an approved V2 intent/order without its exact
                # V3 paper plan.  The caller owns the surrounding transaction,
                # so propagation atomically rolls back every partial write.
                raise
            skipped.append({
                "plan_id": plan_id,
                "reason": str(exc),
            })
    return {
        "status": "ok",
        "created": created,
        "skipped": skipped,
        "paper_order_count": len(created),
        "new_paper_order_count": sum(
            item.get("idempotent_replay") is False for item in created
        ),
        "idempotent_paper_order_count": sum(
            item.get("idempotent_replay") is True for item in created
        ),
        "scanned_plan_count": scanned_plan_count,
        "scanned_plan_ids": normalized_plan_ids[:scanned_plan_count],
        "deferred_plan_count": sum(
            str(item.get("reason") or "").endswith("_DEFERRED")
            for item in skipped
        ),
        "maximum_paper_orders_per_run": (
            DYNAMIC_SHADOW_BOOTSTRAP_MAX_PAPER_ORDERS_PER_RUN
        ),
        "real_order_count": 0,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def materialize_internal_paper_orders(
    engine: Engine,
    *,
    run_uid: str,
    account_id: str = "paper-main-v2",
    allowed_buy_codes: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Translate V3 portfolio deltas into the existing internal paper OMS.

    This adapter has no real-order path. It also refuses to run if the
    database real-trading switch is anything other than zero.
    """

    require_legacy_strategy_account(
        account_id,
        entrypoint="trading_v3.materialize_internal_paper_orders",
    )
    config = load_v3_config()
    auction_allowlist = (
        {
            str(code or "").strip().split(".", 1)[0].zfill(6)
            for code in allowed_buy_codes
            if str(code or "").strip()
        }
        if allowed_buy_codes is not None
        else None
    )
    created = []
    skipped = []
    now = datetime.now().replace(microsecond=0)
    _sync_v3_execution_plan_states(
        engine,
        account_id=account_id,
        now=now,
    )
    with engine.begin() as connection:
        account = connection.execute(
            text(
                """
                SELECT *
                FROM st_trade_account_v2
                WHERE account_id = :account_id
                FOR UPDATE
                """
            ),
            {"account_id": account_id},
        ).mappings().first()
        if not account:
            raise RuntimeError("ProBigA 模拟账户不存在")
        run = connection.execute(
            text(
                """
                SELECT *
                FROM st_decision_run_v3
                WHERE run_uid = :run_uid
                FOR UPDATE
                """
            ),
            {"run_uid": run_uid},
        ).mappings().first()
        if not run or str(run["status"]) not in {
            "PROCESSING",
            "COMPLETED",
            "BLOCKED",
        }:
            raise RuntimeError("V3 决策批次不存在或尚未完成")
        target_lock_clause = (
            ""
            if str(
                getattr(getattr(connection, "dialect", None), "name", "")
            ).casefold()
            == "sqlite"
            else " FOR UPDATE"
        )
        targets = [
            dict(row)
            for row in connection.execute(
                text(
                    f"""
                    SELECT t.*,
                           (
                               SELECT MIN(f.initial_stop_pct)
                               FROM st_alpha_forecast_v3 f
                               WHERE f.run_uid = t.run_uid
                                 AND f.stock_code = t.stock_code
                           ) AS initial_stop_pct
                    FROM st_target_portfolio_v3 t
                    WHERE t.run_uid = :run_uid
                    ORDER BY t.rank_no
                    {target_lock_clause}
                    """
                ),
                {"run_uid": run_uid},
            ).mappings().all()
        ]
        (
            portfolio_payload,
            account_equity,
            decision_truth_verified,
            decision_integrity_reason,
        ) = _verify_persisted_decision_truth(
            connection,
            account=dict(account),
            run=dict(run),
            targets=targets,
            account_id=account_id,
            now=now,
        )
        decision_truth = dict(
            portfolio_payload.get("decision_truth") or {}
        )
        buy_materialization_allowed = bool(
            decision_truth_verified
            and str(decision_truth.get("run_status") or "")
            == "COMPLETED"
            and str(decision_truth.get("actionable_status") or "")
            == "PAPER_ACTIONABLE"
            and str(decision_truth.get("paper_order_authority") or "")
            == "V2_GATED"
            and str(run.get("lifecycle_status") or "").upper()
            in {"PAPER_TRIAL", "PAPER_ACTIVE"}
            and str(decision_truth.get("decision_scope") or "")
            == "INTERNAL_PAPER_TRIAL"
        )
        valuation_prices = {
            str(code): float(value or 0)
            for code, value in dict(
                (
                    portfolio_payload.get("decision_snapshot")
                    or {}
                ).get("valuation_prices")
                or {}
            ).items()
            if str(code) and float(value or 0) > 0
        }
        superseded = _cancel_superseded_v3_buys(
            connection,
            account_id=account_id,
            run_uid=run_uid,
            now=now,
        )
        maximum_live_positions = min(
            50,
            max(
                1,
                int(
                    config.get("paper_execution", {}).get(
                        "maximum_live_positions",
                        12,
                    )
                ),
            ),
        )
        live_position_codes = {
            str(row[0])
            for row in connection.execute(
                text(
                    """
                    SELECT stock_code
                    FROM st_position_lot_v2
                    WHERE account_id = :account_id
                      AND remaining_quantity > 0
                    GROUP BY stock_code
                    UNION
                    SELECT stock_code
                    FROM st_order_v2
                    WHERE account_id = :account_id
                      AND side = 'BUY'
                      AND status IN (
                          'CREATED', 'RISK_APPROVED', 'QUEUED',
                          'PARTIALLY_FILLED'
                      )
                    GROUP BY stock_code
                    """
                ),
                {"account_id": account_id},
            ).all()
        }
        existing_positions = connection.execute(
            text(
                """
                SELECT stock_code, theme_code, remaining_quantity,
                       cost_price, protective_stop
                FROM st_position_lot_v2
                WHERE account_id = :account_id
                  AND remaining_quantity > 0
                ORDER BY stock_code, lot_id
                """
            ),
            {"account_id": account_id},
        ).mappings().all()
        active_buy_orders = connection.execute(
            text(
                """
                SELECT o.stock_code, o.quantity, o.filled_quantity,
                       o.limit_price, i.worst_price, i.protective_stop,
                       i.theme_code
                FROM st_order_v2 o
                JOIN st_trade_intent_v2 i ON i.intent_id = o.intent_id
                WHERE o.account_id = :account_id
                  AND o.side = 'BUY'
                  AND o.status IN (
                      'CREATED', 'RISK_APPROVED', 'QUEUED',
                      'PARTIALLY_FILLED'
                  )
                ORDER BY o.stock_code, o.order_id
                """
            ),
            {"account_id": account_id},
        ).mappings().all()
        account_policy = dict(config.get("account") or {})
        commission_rate = float(
            account_policy.get("commission_rate", 0.0)
        )
        minimum_commission_cny = float(
            account_policy.get("minimum_commission_cny", 0.0)
        )
        transfer_fee_rate = float(
            account_policy.get("transfer_fee_rate", 0.0)
        )
        code_values: dict[str, float] = {}
        theme_values: dict[str, float] = {}
        cumulative_total_value = 0.0
        cumulative_open_risk_cny = 0.0
        cumulative_turnover_cny = 0.0
        reserved_cash_cny = 0.0
        valuation_unverified_codes: list[str] = []
        for raw in existing_positions:
            row = dict(raw)
            code = str(row.get("stock_code") or "")
            theme = str(row.get("theme_code") or "")
            quantity = int(row.get("remaining_quantity") or 0)
            price = float(valuation_prices.get(code) or 0)
            if quantity > 0 and price <= 0:
                valuation_unverified_codes.append(code)
                continue
            value = max(0, quantity) * max(0.0, price)
            code_values[code] = code_values.get(code, 0.0) + value
            if theme:
                theme_values[theme] = theme_values.get(theme, 0.0) + value
            cumulative_total_value += value
            stop = float(row.get("protective_stop") or 0)
            cumulative_open_risk_cny += max(0, quantity) * (
                max(0.0, price - stop) if stop > 0 else price * 0.08
            )
        for raw in active_buy_orders:
            row = dict(raw)
            remaining = max(
                0,
                int(row.get("quantity") or 0)
                - int(row.get("filled_quantity") or 0),
            )
            price = max(
                float(row.get("worst_price") or 0),
                float(row.get("limit_price") or 0),
            )
            value = remaining * price
            code = str(row.get("stock_code") or "")
            theme = str(row.get("theme_code") or "")
            code_values[code] = code_values.get(code, 0.0) + value
            if theme:
                theme_values[theme] = theme_values.get(theme, 0.0) + value
            cumulative_total_value += value
            cumulative_turnover_cny += value
            reserved_cash_cny += value + (
                max(minimum_commission_cny, value * commission_rate)
                + value * transfer_fee_rate
                if value > 0
                else 0.0
            )
            stop = float(row.get("protective_stop") or 0)
            cumulative_open_risk_cny += remaining * (
                max(0.0, price - stop) if stop > 0 else price * 0.08
            )
        if valuation_unverified_codes:
            buy_materialization_allowed = False
        available_cash_cny = float(account["cash_balance"]) - reserved_cash_cny
        portfolio_policy = dict(config.get("portfolio") or {})
        maximum_single_weight = float(
            portfolio_policy.get("maximum_single_position_weight", 0.12)
        )
        maximum_total_weight = float(run.get("risk_asset_cap") or 0)
        maximum_theme_weight = float(
            portfolio_policy.get("maximum_theme_weight", 0.25)
        )
        maximum_open_risk_weight = float(
            portfolio_policy.get("maximum_open_risk", 0.02)
        )
        maximum_daily_turnover_weight = float(
            portfolio_policy.get("maximum_daily_turnover", 0.30)
        )
        source_date = run["trade_date"]
        execution_date = _next_trade_date(connection, source_date)
        same_day_filled_turnover_cny = float(
            connection.execute(
                text(
                    """
                    SELECT COALESCE(SUM(ABS(gross_amount)), 0)
                    FROM st_fill_v2
                    WHERE account_id = :account_id
                      AND DATE(filled_at) = :execution_date
                    """
                ),
                {
                    "account_id": account_id,
                    "execution_date": execution_date,
                },
            ).scalar()
            or 0
        )
        cumulative_turnover_cny += same_day_filled_turnover_cny
        earliest_at = datetime.combine(execution_date, time(9, 30))
        expires_at = datetime.combine(execution_date, time(14, 45))
        exit_states = connection.execute(
            text(
                """
                SELECT s.*, lots.actual_quantity
                FROM st_position_state_v3 s
                JOIN (
                    SELECT stock_code,
                           SUM(remaining_quantity) AS actual_quantity
                    FROM st_position_lot_v2
                    WHERE account_id = :account_id
                      AND remaining_quantity > 0
                    GROUP BY stock_code
                ) lots ON lots.stock_code = s.stock_code
                WHERE s.account_id = :account_id
                  AND lots.actual_quantity > 0
                  AND s.last_action IN (
                      'SELL_ALL',
                      'WAIT_SELLABLE'
                  )
                ORDER BY s.updated_at, s.stock_code
                """
            ),
            {"account_id": account_id},
        ).mappings().all()
        exit_codes: set[str] = set()
        for state in exit_states:
            code = str(state["stock_code"])
            exit_codes.add(code)
            try:
                invalidation = json.loads(
                    str(state.get("invalidation_json") or "{}")
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                invalidation = {}
            reference_price = float(
                invalidation.get("latest_price")
                or state.get("average_cost")
                or 0.0
            )
            if reference_price <= 0:
                skipped.append({
                    "stock_code": code,
                    "side": "SELL",
                    "reason": "EXIT_REFERENCE_PRICE_INVALID",
                })
                continue
            protective_stop = float(
                invalidation.get("protective_stop") or 0.0
            )
            limit_price = Decimal(
                str(round(reference_price * 0.995, 3))
            )
            exit_result = _persist_exit_chain(
                connection,
                account_id=account_id,
                run_uid=run_uid,
                strategy_version=config["strategy_version"],
                stock_code=code,
                current_quantity=int(state["actual_quantity"]),
                target_quantity=0,
                earliest_at=earliest_at,
                expires_at=expires_at,
                limit_price=limit_price,
                initial_stop=Decimal(
                    str(protective_stop or reference_price)
                ),
                protective_stop=Decimal(
                    str(protective_stop or reference_price)
                ),
                invalidation=str(
                    state.get("last_reason")
                    or "V3交易逻辑失效"
                ),
                reason_code=str(
                    state.get("last_reason_code")
                    or "V3_POSITION_EXIT"
                ),
                now=now,
            )
            if exit_result.get("status") == "created":
                quantity = int(exit_result["quantity"])
                plan_key = _hash([
                    run_uid,
                    account_id,
                    code,
                    "SELL",
                    quantity,
                ])
                connection.execute(
                    text(
                        """
                        INSERT IGNORE INTO st_execution_plan_v3 (
                            execution_plan_id, run_uid, account_id,
                            trade_date, stock_code, side, quantity,
                            limit_price, state, reason_code, source,
                            real_order_allowed, idempotency_key,
                            created_at, updated_at
                        ) VALUES (
                            :execution_plan_id, :run_uid, :account_id,
                            :trade_date, :stock_code, 'SELL', :quantity,
                            :limit_price, 'PAPER_QUEUED',
                            :reason_code, 'V3_POSITION_STATE',
                            0, :idempotency_key, :created_at, :updated_at
                        )
                        """
                    ),
                    {
                        "execution_plan_id": uuid.uuid4().hex,
                        "run_uid": run_uid,
                        "account_id": account_id,
                        "trade_date": execution_date,
                        "stock_code": code,
                        "quantity": quantity,
                        "limit_price": float(limit_price),
                        "reason_code": str(
                            state.get("last_reason_code")
                            or "V3_POSITION_EXIT"
                        ),
                        "idempotency_key": plan_key,
                        "created_at": now,
                        "updated_at": now,
                    },
                )
                created.append({
                    "stock_code": code,
                    "side": "SELL",
                    "quantity": quantity,
                    "order_id": exit_result["order_id"],
                    "execution_date": execution_date,
                })
            else:
                skipped.append({
                    "stock_code": code,
                    "side": "SELL",
                    "reason": str(exit_result.get("status")),
                })
        for target in targets:
            code = str(target["stock_code"])
            if auction_allowlist is not None and code not in auction_allowlist:
                skipped.append({
                    "stock_code": code,
                    "side": "BUY",
                    "status": "BLOCKED",
                    "reason": "PREMARKET_AUCTION_NOT_CONFIRMED",
                })
                continue
            if not buy_materialization_allowed:
                if decision_integrity_reason:
                    blocked_reason = (
                        "DECISION_RESULT_OR_TARGET_LEDGER_UNVERIFIED"
                    )
                elif not decision_truth_verified:
                    blocked_reason = "DECISION_TRUTH_UNVERIFIED"
                elif valuation_unverified_codes:
                    blocked_reason = "POSITION_VALUATION_UNVERIFIED"
                elif (
                    str(run["status"]) == "BLOCKED"
                    or str(decision_truth.get("run_status") or "")
                    == "BLOCKED"
                ):
                    blocked_reason = "DECISION_RUN_BLOCKED"
                else:
                    blocked_reason = "DECISION_NOT_PAPER_ACTIONABLE"
                skipped.append({
                    "stock_code": code,
                    "side": "BUY",
                    "status": "BLOCKED",
                    "reason": blocked_reason,
                    "integrity_detail": decision_integrity_reason,
                })
                continue
            if code in exit_codes:
                skipped.append({
                    "stock_code": code,
                    "side": "BUY",
                    "reason": "V3_EXIT_HAS_PRIORITY",
                })
                continue
            paper_discovery = str(
                target.get("reason") or ""
            ).startswith("PAPER_DISCOVERY")
            reason_code = (
                "V3_PAPER_DISCOVERY"
                if paper_discovery
                else "V3_VALIDATED_POSITIVE"
            )
            if paper_discovery:
                last_sell_at = connection.execute(
                    text(
                        """
                        SELECT MAX(updated_at)
                        FROM st_order_v2
                        WHERE account_id = :account_id
                          AND stock_code = :stock_code
                          AND side = 'SELL'
                          AND status = 'FILLED'
                        """
                    ),
                    {
                        "account_id": account_id,
                        "stock_code": code,
                    },
                ).scalar()
                if last_sell_at is not None:
                    elapsed_trade_days = int(
                        connection.execute(
                            text(
                                """
                                SELECT COUNT(*)
                                FROM si_trade_calendar
                                WHERE trade_status = 1
                                  AND trade_date > DATE(:last_sell_at)
                                  AND trade_date <= :execution_date
                                """
                            ),
                            {
                                "last_sell_at": last_sell_at,
                                "execution_date": execution_date,
                            },
                        ).scalar()
                        or 0
                    )
                    cooldown_days = int(
                        config.get("paper_discovery", {}).get(
                            "cooldown_trade_days_after_exit",
                            5,
                        )
                    )
                    if elapsed_trade_days <= cooldown_days:
                        skipped.append({
                            "stock_code": code,
                            "reason": (
                                "PAPER_DISCOVERY_COOLDOWN_"
                                f"{cooldown_days}D"
                            ),
                        })
                        continue
            active = connection.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM st_order_v2
                    WHERE account_id = :account_id
                      AND stock_code = :stock_code
                      AND side = 'BUY'
                      AND status IN (
                          'CREATED', 'RISK_APPROVED', 'QUEUED',
                          'PARTIALLY_FILLED'
                      )
                    """
                ),
                {"account_id": account_id, "stock_code": code},
            ).scalar()
            current_quantity = int(
                connection.execute(
                    text(
                        """
                        SELECT COALESCE(SUM(remaining_quantity), 0)
                        FROM st_position_lot_v2
                        WHERE account_id = :account_id
                          AND stock_code = :stock_code
                          AND remaining_quantity > 0
                        """
                    ),
                    {"account_id": account_id, "stock_code": code},
                ).scalar()
                or 0
            )
            target_quantity = int(target["target_quantity"])
            delta = target_quantity - current_quantity
            if active or delta <= 0:
                skipped.append({
                    "stock_code": code,
                    "reason": (
                        "ACTIVE_BUY_ORDER_EXISTS"
                        if active
                        else "TARGET_ALREADY_REACHED"
                    ),
                })
                continue
            reference_price = (
                float(target["target_value"]) / target_quantity
                if target_quantity > 0
                else 0.0
            )
            if reference_price <= 0:
                skipped.append({
                    "stock_code": code,
                    "reason": "REFERENCE_PRICE_INVALID",
                })
                continue
            if current_quantity == 0:
                probe_quantity = (
                    max(1, target_quantity // 200) * 100
                )
                probe_value = probe_quantity * reference_price
                if probe_value >= float(
                    config["portfolio"]["minimum_economic_order_cny"]
                ):
                    delta = min(delta, probe_quantity)
            execution_policy = dict(config.get("paper_execution") or {})
            maximum_entry_premium = float(
                execution_policy.get("maximum_entry_premium_pct", 0.5)
            ) / 100.0
            worst_price_premium = float(
                execution_policy.get("worst_price_premium_pct", 1.0)
            ) / 100.0
            limit_price = round(
                reference_price * (1.0 + maximum_entry_premium),
                3,
            )
            worst_price = round(
                reference_price * (1.0 + worst_price_premium),
                3,
            )
            stop_pct = float(target["initial_stop_pct"] or -5.0)
            initial_stop = round(reference_price * (1 + stop_pct / 100), 3)
            intent_id = uuid.uuid4().hex
            intent_payload = [
                account_id,
                run_uid,
                code,
                "BUY",
                target_quantity,
                config["strategy_version"],
            ]
            intent_key = _hash(intent_payload)
            existing_intent = connection.execute(
                text(
                    """
                    SELECT intent_id
                    FROM st_trade_intent_v2
                    WHERE idempotency_key = :idempotency_key
                    LIMIT 1
                    """
                ),
                {"idempotency_key": intent_key},
            ).scalar()
            if existing_intent:
                skipped.append({
                    "stock_code": code,
                    "reason": "V3_INTENT_ALREADY_MATERIALIZED",
                })
                continue
            try:
                target_strategy_keys = [
                    str(item)
                    for item in json.loads(
                        str(target.get("strategy_keys_json") or "[]")
                    )
                    if str(item) and str(item) != "paper_discovery"
                ]
            except (TypeError, ValueError, json.JSONDecodeError):
                target_strategy_keys = []
            target_strategy_keys = sorted(set(target_strategy_keys))
            primary_strategy_key = str(
                target.get("primary_strategy_key") or ""
            )
            if (
                not primary_strategy_key
                and len(target_strategy_keys) == 1
            ):
                primary_strategy_key = target_strategy_keys[0]
            if (
                not primary_strategy_key
                or primary_strategy_key not in target_strategy_keys
            ):
                skipped.append({
                    "stock_code": code,
                    "reason": "V3_SAMPLE_OWNER_AMBIGUOUS",
                })
                continue
            governance_receipt, governance_reason = (
                _canonical_governance_buy_receipt(
                    connection,
                    trade_date=(
                        source_date
                        if isinstance(source_date, date)
                        else date.fromisoformat(str(source_date))
                    ),
                    stock_code=code,
                    strategy_keys=target_strategy_keys,
                )
            )
            if governance_receipt is None:
                skipped.append({
                    "stock_code": code,
                    "side": "BUY",
                    "status": "BLOCKED",
                    "reason": governance_reason
                    or "GOVERNANCE_PAPER_BUY_NOT_AUTHORIZED",
                })
                continue
            if str(governance_receipt.get("strategy_key") or "") != (
                primary_strategy_key
            ):
                skipped.append({
                    "stock_code": code,
                    "side": "BUY",
                    "status": "BLOCKED",
                    "reason": "GOVERNANCE_PRIMARY_OWNER_MISMATCH",
                })
                continue
            primary_forecast_id = str(
                target.get("primary_forecast_id") or ""
            )
            if not primary_forecast_id:
                primary_forecast_id = str(
                    connection.execute(
                        text(
                            """
                            SELECT forecast_id
                            FROM st_alpha_forecast_v3
                            WHERE run_uid = :run_uid
                              AND stock_code = :stock_code
                              AND strategy_key = :strategy_key
                            LIMIT 1
                            """
                        ),
                        {
                            "run_uid": run_uid,
                            "stock_code": code,
                            "strategy_key": primary_strategy_key,
                        },
                    ).scalar()
                    or ""
                )
            if not primary_forecast_id:
                skipped.append({
                    "stock_code": code,
                    "reason": "V3_SAMPLE_OWNER_FORECAST_MISSING",
                })
                continue
            target_snapshot_strategy_version = primary_strategy_version(
                run["model_version"],
                primary_strategy_key,
            )
            target_snapshot_ownership_hash = _ownership_hash(
                run_uid,
                primary_forecast_id,
                code,
                primary_strategy_key,
                target_snapshot_strategy_version,
            )
            stored_ownership_hash = str(
                target.get("attribution_snapshot_hash") or ""
            )
            if (
                stored_ownership_hash
                and stored_ownership_hash != target_snapshot_ownership_hash
            ):
                skipped.append({
                    "stock_code": code,
                    "reason": "V3_SAMPLE_OWNER_HASH_MISMATCH",
                })
                continue
            frozen_primary_strategy_version = (
                str(governance_receipt.get("strategy_version") or "")
                if str(governance_receipt.get("strategy_source_kind") or "")
                == "runtime_registry"
                else target_snapshot_strategy_version
            )
            ownership_hash = _ownership_hash(
                run_uid,
                primary_forecast_id,
                code,
                primary_strategy_key,
                frozen_primary_strategy_version,
            )
            buy_gate_receipt, buy_gate_reason = _canonical_v2_buy_receipt(
                connection,
                decision_run_uid=run_uid,
                strategy_version=config["strategy_version"],
                stock_code=code,
                now=now,
            )
            if buy_gate_receipt is None:
                skipped.append({
                    "stock_code": code,
                    "side": "BUY",
                    "status": "RESEARCH_ONLY",
                    "reason": buy_gate_reason or "BUY_GATE_DATA_BLOCKED",
                })
                continue
            evidence = {
                "source": (
                    "V3_PAPER_DISCOVERY"
                    if paper_discovery
                    else "V3_POSITIVE_EXPECTANCY_PORTFOLIO"
                ),
                "run_uid": run_uid,
                "model_version": run["model_version"],
                "expected_return_net_pct": float(
                    target["expected_return_net_pct"]
                ),
                "conservative_return_pct": float(
                    target["conservative_return_pct"]
                ),
                "estimated_roundtrip_cost_pct": float(
                    target["estimated_roundtrip_cost_pct"]
                ),
                "real_trading_enabled": False,
                "positive_expectancy_validated": (
                    not paper_discovery
                ),
                "entry_stage": (
                    "PROBE" if current_quantity == 0 else "CONFIRM_ADD"
                ),
                "signal_strategy_keys": sorted(target_strategy_keys),
                "primary_strategy_key": primary_strategy_key,
                "primary_strategy_version": frozen_primary_strategy_version,
                "primary_forecast_id": primary_forecast_id,
                "supporting_strategy_keys": target_strategy_keys,
                "sample_owner_role": "PRIMARY",
                "attribution_version": ATTRIBUTION_VERSION,
                "ownership_hash": ownership_hash,
                "strategy_governance": governance_receipt,
                # Keep V3 attribution as a mapping while exposing the exact
                # canonical receipt shape consumed again by the V2 executor.
                GATE_MODULE: buy_gate_receipt,
            }
            connection.execute(
                text(
                    """
                    INSERT IGNORE INTO st_trade_intent_v2 (
                        intent_id, account_id, decision_run_uid,
                        strategy_version, stock_code, theme_code,
                        action, current_quantity, target_quantity,
                        target_weight, earliest_at, expires_at,
                        limit_price, worst_price, initial_stop,
                        protective_stop, invalidation_condition,
                        reason_code, evidence_json, intent_version,
                        idempotency_key, created_at
                    ) VALUES (
                        :intent_id, :account_id, :decision_run_uid,
                        :strategy_version, :stock_code, :theme_code,
                        'BUY', :current_quantity, :target_quantity,
                        :target_weight, :earliest_at, :expires_at,
                        :limit_price, :worst_price, :initial_stop,
                        :protective_stop, :invalidation_condition,
                        :reason_code, :evidence_json, 1,
                        :idempotency_key, :created_at
                    )
                    """
                ),
                {
                    "intent_id": intent_id,
                    "account_id": account_id,
                    "decision_run_uid": run_uid,
                    "strategy_version": config["strategy_version"],
                    "stock_code": code,
                    "theme_code": target["theme_code"] or "",
                    "current_quantity": current_quantity,
                    "target_quantity": target_quantity,
                    "target_weight": target["target_weight"],
                    "earliest_at": earliest_at,
                    "expires_at": expires_at,
                    "limit_price": limit_price,
                    "worst_price": worst_price,
                    "initial_stop": initial_stop,
                    "protective_stop": initial_stop,
                    "invalidation_condition": (
                        "趋势失效、硬止损或扣费后净期望不再为正时退出；"
                        "T+1 仅延迟卖出执行"
                    ),
                    "reason_code": reason_code,
                    "evidence_json": json.dumps(
                        evidence,
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    "idempotency_key": intent_key,
                    "created_at": now,
                },
            )
            theme_code = str(target.get("theme_code") or "")
            target_theme_cap = maximum_theme_weight
            if paper_discovery:
                target_theme_cap = min(
                    target_theme_cap,
                    float(
                        config.get("paper_discovery", {}).get(
                            "maximum_theme_weight",
                            target_theme_cap,
                        )
                    ),
                )
            risk = _evaluate_cumulative_buy_risk(
                requested_quantity=delta,
                worst_price=worst_price,
                initial_stop=initial_stop,
                current_code_value=code_values.get(code, 0.0),
                current_total_value=cumulative_total_value,
                current_theme_value=theme_values.get(theme_code, 0.0),
                current_open_risk_cny=cumulative_open_risk_cny,
                available_cash_cny=available_cash_cny,
                current_turnover_cny=cumulative_turnover_cny,
                equity_cny=account_equity,
                creates_new_position=code not in live_position_codes,
                live_position_count=len(live_position_codes),
                maximum_live_positions=maximum_live_positions,
                maximum_single_weight=maximum_single_weight,
                maximum_total_weight=maximum_total_weight,
                maximum_theme_weight=target_theme_cap,
                maximum_open_risk_weight=maximum_open_risk_weight,
                maximum_daily_turnover_weight=(
                    maximum_daily_turnover_weight
                ),
                commission_rate=commission_rate,
                minimum_commission_cny=minimum_commission_cny,
                transfer_fee_rate=transfer_fee_rate,
            )
            risk_payload = {
                "run_uid": run_uid,
                "intent_id": intent_id,
                "requested_quantity": delta,
                "approved_quantity": risk["approved_quantity"],
                "decision_status": risk["decision_status"],
                "trade_risk": risk["trade_risk"],
                "post_single_weight": risk["post_single_weight"],
                "post_total_weight": risk["post_total_weight"],
                "post_theme_weight": risk["post_theme_weight"],
                "post_open_risk_cny": risk["post_open_risk_cny"],
                "post_cash": risk["post_cash"],
                "post_turnover_weight": risk["post_turnover_weight"],
                "cash_reservation": risk["cash_reservation"],
                "estimated_buy_fee": risk["estimated_buy_fee"],
                "checks": risk["checks"],
                "first_failure": risk["first_failure"],
            }
            connection.execute(
                text(
                    """
                    INSERT IGNORE INTO st_risk_decision_v2 (
                        intent_id, decision_status, requested_quantity,
                        approved_quantity, trade_risk,
                        post_single_weight, post_total_weight,
                        post_theme_weight, post_open_risk, post_cash,
                        checks_json, first_failure, decision_hash,
                        created_at
                    ) VALUES (
                        :intent_id, :decision_status, :requested_quantity,
                        :approved_quantity, :trade_risk,
                        :post_single_weight, :post_total_weight,
                        :post_theme_weight, :post_open_risk, :post_cash,
                        :checks_json, :first_failure,
                        :decision_hash, :created_at
                    )
                    """
                ),
                {
                    "intent_id": intent_id,
                    "decision_status": risk["decision_status"],
                    "requested_quantity": delta,
                    "approved_quantity": risk["approved_quantity"],
                    "trade_risk": risk["trade_risk"],
                    "post_single_weight": risk["post_single_weight"],
                    "post_total_weight": risk["post_total_weight"],
                    "post_theme_weight": risk["post_theme_weight"],
                    "post_open_risk": risk["post_open_risk_cny"],
                    "post_cash": risk["post_cash"],
                    "checks_json": json.dumps(
                        risk["checks"],
                        sort_keys=True,
                    ),
                    "first_failure": risk["first_failure"],
                    "decision_hash": _hash(risk_payload),
                    "created_at": now,
                },
            )
            if risk["decision_status"] != "APPROVED":
                skipped.append({
                    "stock_code": code,
                    "side": "BUY",
                    "status": "RISK_REJECTED",
                    "reason": risk["first_failure"],
                })
                continue
            reserved_notional = float(risk["reserved_notional"])
            code_values[code] = (
                code_values.get(code, 0.0) + reserved_notional
            )
            if theme_code:
                theme_values[theme_code] = (
                    theme_values.get(theme_code, 0.0)
                    + reserved_notional
                )
            cumulative_total_value += reserved_notional
            cumulative_open_risk_cny = float(
                risk["post_open_risk_cny"]
            )
            cumulative_turnover_cny += reserved_notional
            available_cash_cny = float(risk["post_cash"])
            order_id = uuid.uuid4().hex
            order_key = order_idempotency_key(
                account_id=account_id,
                decision_run_uid=run_uid,
                intent_id=intent_id,
                stock_code=code,
                side="BUY",
                target_quantity=delta,
                intent_version=1,
            )
            connection.execute(
                text(
                    """
                    INSERT IGNORE INTO st_order_v2 (
                        order_id, account_id, intent_id, stock_code,
                        side, order_type, limit_price, quantity,
                        filled_quantity, status, waiting_reason,
                        earliest_at, expires_at, idempotency_key,
                        created_at, updated_at
                    ) VALUES (
                        :order_id, :account_id, :intent_id, :stock_code,
                        'BUY', 'LIMIT', :limit_price, :quantity,
                        0, 'QUEUED', 'V3_NEXT_SESSION',
                        :earliest_at, :expires_at, :idempotency_key,
                        :created_at, :updated_at
                    )
                    """
                ),
                {
                    "order_id": order_id,
                    "account_id": account_id,
                    "intent_id": intent_id,
                    "stock_code": code,
                    "limit_price": limit_price,
                    "quantity": delta,
                    "earliest_at": earliest_at,
                    "expires_at": expires_at,
                    "idempotency_key": order_key,
                    "created_at": now,
                    "updated_at": now,
                },
            )
            plan_key = _hash([
                run_uid,
                account_id,
                code,
                "BUY",
                delta,
            ])
            connection.execute(
                text(
                    """
                    INSERT IGNORE INTO st_execution_plan_v3 (
                        execution_plan_id, run_uid, account_id,
                        trade_date, stock_code, side, quantity,
                        limit_price, state, reason_code, source,
                        real_order_allowed, idempotency_key,
                        created_at, updated_at
                    ) VALUES (
                        :execution_plan_id, :run_uid, :account_id,
                        :trade_date, :stock_code, 'BUY', :quantity,
                        :limit_price, 'PAPER_QUEUED',
                        :reason_code, :source,
                        0, :idempotency_key, :created_at, :updated_at
                    )
                    """
                ),
                {
                    "execution_plan_id": uuid.uuid4().hex,
                    "run_uid": run_uid,
                    "account_id": account_id,
                    "trade_date": execution_date,
                    "stock_code": code,
                    "quantity": delta,
                    "limit_price": limit_price,
                    "idempotency_key": plan_key,
                    "reason_code": reason_code,
                    "source": (
                        "V3_PAPER_DISCOVERY"
                        if paper_discovery
                        else "V3_PORTFOLIO"
                    ),
                    "created_at": now,
                    "updated_at": now,
                },
            )
            created.append({
                "stock_code": code,
                "side": "BUY",
                "quantity": delta,
                "order_id": order_id,
                "execution_date": execution_date,
            })
            live_position_codes.add(code)
    return {
        "status": "ok",
        "created": created,
        "skipped": skipped,
        **superseded,
        "decision_integrity_verified": decision_truth_verified,
        "decision_integrity_reason": decision_integrity_reason,
        "real_order_count": 0,
        "paper_order_count": len(created),
    }
