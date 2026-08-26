from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Iterable, Mapping

from sqlalchemy import text
from sqlalchemy.engine import Engine


ACCOUNT_ID = "paper-main-v2"
ACTIVE_ORDER_STATES = (
    "CREATED",
    "RISK_APPROVED",
    "QUEUED",
    "PARTIALLY_FILLED",
)


class DecisionTruthBlocked(RuntimeError):
    """The decision cannot safely claim an actionable account snapshot."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
        allow_nan=False,
    )


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


_TARGET_DECIMAL_SCALES = {
    "target_weight": 8,
    "target_value": 2,
    "estimated_roundtrip_cost_pct": 8,
    "expected_return_net_pct": 8,
    "conservative_return_pct": 8,
    "expected_mae_pct": 8,
}

DECISION_INTEGRITY_SCHEMA_VERSION = (
    "probiga.trading-v3.decision-integrity.v2"
)
FORECAST_LEDGER_FIELDS = (
    "forecast_id",
    "run_uid",
    "trade_date",
    "rank_no",
    "stock_code",
    "short_name",
    "strategy_key",
    "horizon_days",
    "raw_score",
    "expected_return_net_pct",
    "return_q10_pct",
    "return_q50_pct",
    "return_q90_pct",
    "probability_positive",
    "expected_mae_pct",
    "expected_mfe_pct",
    "profit_factor",
    "payoff_ratio",
    "sample_count",
    "confidence",
    "forecast_status",
    "theme_code",
    "model_version",
    "dataset_hash",
    "feature_time",
    "valid_until",
    "initial_stop_pct",
    "reasons_json",
    "features_json",
    "created_at",
)
FORECAST_LEDGER_SQL_COLUMNS = ", ".join(FORECAST_LEDGER_FIELDS)
_FORECAST_DECIMAL_FIELDS = frozenset({
    "raw_score",
    "expected_return_net_pct",
    "return_q10_pct",
    "return_q50_pct",
    "return_q90_pct",
    "probability_positive",
    "expected_mae_pct",
    "expected_mfe_pct",
    "profit_factor",
    "payoff_ratio",
    "confidence",
    "initial_stop_pct",
})


def _fixed_decimal(value: Any, scale: int) -> str:
    """Match the canonical value written to a MySQL DECIMAL column."""

    quantum = Decimal(1).scaleb(-scale)
    return format(
        Decimal(str(value or 0)).quantize(
            quantum,
            rounding=ROUND_HALF_UP,
        ),
        f".{scale}f",
    )


def _nullable_fixed_decimal(value: Any, scale: int) -> str | None:
    if value is None:
        return None
    return _fixed_decimal(value, scale)


def _canonical_date(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return date.fromisoformat(str(value or "")[:10]).isoformat()


def _canonical_datetime(value: Any) -> str:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(
            str(value or "").strip().replace("Z", "+00:00")
        )
    return parsed.isoformat(sep=" ")


def _canonical_json_field(value: Any, *, expected_type: type) -> Any:
    parsed = json.loads(value) if isinstance(value, str) else value
    if not isinstance(parsed, expected_type):
        raise ValueError("forecast ledger JSON field has an invalid shape")
    return parsed


def canonical_forecast_projection(
    item: Mapping[str, Any],
) -> dict[str, Any]:
    """Canonicalize every persisted forecast field for immutable readback.

    The projection deliberately includes identifiers, timestamps and the full
    feature/reason payload in addition to ranking and scores.  A same-count row
    replacement or any content mutation must therefore invalidate the run.
    """

    projection: dict[str, Any] = {}
    for field in FORECAST_LEDGER_FIELDS:
        value = item.get(field)
        if field in _FORECAST_DECIMAL_FIELDS:
            projection[field] = _nullable_fixed_decimal(value, 8)
        elif field in {"rank_no", "horizon_days", "sample_count"}:
            projection[field] = int(value or 0)
        elif field == "trade_date":
            projection[field] = _canonical_date(value)
        elif field in {"feature_time", "valid_until", "created_at"}:
            projection[field] = _canonical_datetime(value)
        elif field == "reasons_json":
            projection[field] = _canonical_json_field(
                value,
                expected_type=list,
            )
        elif field == "features_json":
            projection[field] = _canonical_json_field(
                value,
                expected_type=dict,
            )
        else:
            projection[field] = str(value or "")
    return projection


def canonical_forecast_ledger(
    rows: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    projections = [canonical_forecast_projection(item) for item in rows]
    return sorted(
        projections,
        key=lambda item: (
            item["rank_no"],
            item["stock_code"],
            item["strategy_key"],
            item["forecast_id"],
        ),
    )


def _string_list(value: Any) -> list[str]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            value = []
    if not isinstance(value, (list, tuple, set)):
        value = []
    return sorted({str(item) for item in value if str(item)})


def canonical_target_projection(
    item: Mapping[str, Any],
    *,
    run_uid: str,
    trade_date: date,
    rank_no: int,
    ownership: Mapping[str, Any] | None = None,
    persisted: bool = False,
) -> dict[str, Any]:
    """Return the immutable business projection shared by JSON and SQL.

    Random row identifiers and timestamps are deliberately excluded.  Every
    field that can change sizing, selection, attribution, or execution is
    included and normalized exactly once so DECIMAL/JSON representation
    differences cannot create false mismatches.
    """

    owner = dict(ownership or {})
    if persisted:
        stock_name = item.get("short_name")
        theme_codes = item.get("theme_codes_json")
        strategy_keys = item.get("strategy_keys_json")
        owner = {
            "primary_strategy_key": item.get("primary_strategy_key"),
            "primary_forecast_id": item.get("primary_forecast_id"),
            "attribution_snapshot_hash": item.get(
                "attribution_snapshot_hash"
            ),
        }
    else:
        stock_name = item.get("stock_name")
        theme_codes = item.get("theme_codes")
        strategy_keys = item.get("strategy_keys")
    projection: dict[str, Any] = {
        "run_uid": str(run_uid),
        "trade_date": trade_date.isoformat(),
        "rank_no": int(rank_no),
        "stock_code": str(item.get("stock_code") or ""),
        "short_name": str(stock_name or ""),
        "target_quantity": int(item.get("target_quantity") or 0),
        "theme_code": str(item.get("theme_code") or ""),
        "theme_codes": _string_list(theme_codes),
        "strategy_keys": _string_list(strategy_keys),
        "primary_strategy_key": str(
            owner.get("primary_strategy_key") or ""
        ),
        "primary_forecast_id": str(
            owner.get("primary_forecast_id") or ""
        ),
        "attribution_snapshot_hash": str(
            owner.get("attribution_snapshot_hash") or ""
        ),
        "reason": str(item.get("reason") or ""),
        "status": str(item.get("status") or "PLANNED"),
    }
    for field, scale in _TARGET_DECIMAL_SCALES.items():
        projection[field] = _fixed_decimal(item.get(field), scale)
    return projection


def canonical_target_ledger(
    rows: Iterable[Mapping[str, Any]],
    *,
    run_uid: str,
    trade_date: date,
    ownership_by_code: Mapping[str, Mapping[str, Any]] | None = None,
    persisted: bool = False,
) -> list[dict[str, Any]]:
    owner_map = dict(ownership_by_code or {})
    projections = [
        canonical_target_projection(
            item,
            run_uid=run_uid,
            trade_date=trade_date,
            rank_no=(
                int(item.get("rank_no") or 0)
                if persisted
                else index
            ),
            ownership=owner_map.get(str(item.get("stock_code") or "")),
            persisted=persisted,
        )
        for index, item in enumerate(rows, 1)
    ]
    return sorted(
        projections,
        key=lambda item: (item["rank_no"], item["stock_code"]),
    )


def decision_result_hash(
    *,
    regime: Mapping[str, Any],
    portfolio: Mapping[str, Any],
    forecast_count: int,
    theme_signal_count: int,
    hypothesis_count: int,
) -> str:
    """Recreate the persisted V3 run hash from its immutable basis."""

    return canonical_hash({
        "regime": dict(regime),
        "portfolio": dict(portfolio),
        "forecast_count": int(forecast_count),
        "theme_signal_count": int(theme_signal_count),
        "hypothesis_count": int(hypothesis_count),
    })


def _as_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value:
        try:
            return date.fromisoformat(str(value)[:10])
        except ValueError:
            return None
    return None


def _as_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if value:
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _plain_row(row: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, Decimal):
            result[str(key)] = str(value)
        elif isinstance(value, datetime):
            result[str(key)] = value.isoformat(sep=" ")
        elif isinstance(value, date):
            result[str(key)] = value.isoformat()
        else:
            result[str(key)] = value
    return result


def _require_account_truth(
    account: Mapping[str, Any] | None,
    equity: Mapping[str, Any] | None,
    reconciliation: Mapping[str, Any] | None,
    *,
    trade_date: date,
    decision_at: datetime,
) -> None:
    if not account:
        raise DecisionTruthBlocked(
            "ACCOUNT_MISSING",
            "模拟账户不存在，禁止使用配置默认资金替代",
        )
    if str(account.get("status") or "") != "ACTIVE":
        raise DecisionTruthBlocked(
            "ACCOUNT_NOT_ACTIVE",
            f"账户状态为 {account.get('status') or 'UNKNOWN'}",
        )
    if int(account.get("real_trading_enabled") or 0) != 0:
        raise DecisionTruthBlocked(
            "REAL_TRADING_SWITCH_ENABLED",
            "V3 仅允许内部模拟盘",
        )
    account_updated_at = _as_datetime(account.get("updated_at"))
    if account_updated_at is None or account_updated_at > decision_at:
        raise DecisionTruthBlocked(
            "ACCOUNT_AS_OF_UNPROVABLE",
            "账户更新时间缺失或晚于 decision_at",
        )
    if not equity:
        raise DecisionTruthBlocked(
            "EQUITY_SNAPSHOT_MISSING",
            f"缺少 {trade_date.isoformat()} 权益快照",
        )
    if _as_date(equity.get("trade_date")) != trade_date:
        raise DecisionTruthBlocked(
            "EQUITY_SNAPSHOT_STALE",
            "权益快照交易日与决策交易日不一致",
        )
    if float(equity.get("total_equity") or 0) <= 0:
        raise DecisionTruthBlocked(
            "EQUITY_NOT_POSITIVE",
            "权益为零或无效，禁止回退到默认 20 万元",
        )
    if not reconciliation:
        raise DecisionTruthBlocked(
            "RECONCILIATION_MISSING",
            f"缺少 {trade_date.isoformat()} 对账证据",
        )
    if _as_date(reconciliation.get("trade_date")) != trade_date:
        raise DecisionTruthBlocked(
            "RECONCILIATION_STALE",
            "对账交易日与决策交易日不一致",
        )
    if str(reconciliation.get("status") or "") != "PASS":
        raise DecisionTruthBlocked(
            "RECONCILIATION_BLOCKED",
            f"对账状态为 {reconciliation.get('status') or 'UNKNOWN'}",
        )
    reconciliation_created_at = _as_datetime(
        reconciliation.get("created_at")
    )
    if (
        reconciliation_created_at is None
        or reconciliation_created_at > decision_at
    ):
        raise DecisionTruthBlocked(
            "RECONCILIATION_AS_OF_UNPROVABLE",
            "对账时间缺失或晚于 decision_at",
        )
    if account_updated_at > reconciliation_created_at:
        raise DecisionTruthBlocked(
            "ACCOUNT_CHANGED_AFTER_RECONCILIATION",
            "账户在最近一次 PASS 对账后发生变化",
        )


def load_decision_snapshot(
    engine: Engine,
    *,
    requested_as_of: date,
    trade_date: date,
    decision_at: datetime,
    feature_time: datetime,
    data_snapshot_hash: str,
    data_source: str,
    stocks: Iterable[Mapping[str, Any]],
    account_id: str = ACCOUNT_ID,
) -> dict[str, Any]:
    """Load one fail-closed account/OMS snapshot at the decision clock.

    The manifest is persisted inside the existing V3 ``portfolio_json`` and
    therefore participates in ``result_hash``.  This avoids pretending that
    the mutable V2 account tables are a historical source of truth while also
    keeping compatibility with installations that have only the current V3
    schema.
    """

    if feature_time > decision_at:
        raise DecisionTruthBlocked(
            "FEATURE_TIME_AFTER_DECISION",
            "特征时间晚于 decision_at，存在未来数据",
        )
    prices = {
        str(item.get("stock_code") or ""): float(item.get("price") or 0)
        for item in stocks
        if str(item.get("stock_code") or "")
        and float(item.get("price") or 0) > 0
    }
    with engine.connect() as connection:
        account = connection.execute(
            text(
                """
                SELECT account_id, status, cash_balance, peak_equity,
                       policy_version, policy_hash, fee_profile_version,
                       instrument_rule_version, real_trading_enabled,
                       created_at, updated_at
                FROM st_trade_account_v2
                WHERE account_id = :account_id
                LIMIT 1
                """
            ),
            {"account_id": account_id},
        ).mappings().first()
        equity = connection.execute(
            text(
                """
                SELECT account_id, trade_date, cash_balance, market_value,
                       receivables, payables, total_equity, peak_equity,
                       drawdown, price_snapshot_hash, created_at
                FROM st_equity_daily_v2
                WHERE account_id = :account_id
                  AND trade_date = :trade_date
                  AND created_at <= :decision_at
                ORDER BY created_at DESC
                LIMIT 1
                """
            ),
            {
                "account_id": account_id,
                "trade_date": trade_date,
                "decision_at": decision_at,
            },
        ).mappings().first()
        reconciliation = connection.execute(
            text(
                """
                SELECT account_id, trade_date, version, status,
                       cash_difference, equity_difference,
                       position_difference, order_difference,
                       fill_difference, checks_json,
                       reconciliation_hash, created_at
                FROM st_reconciliation_v2
                WHERE account_id = :account_id
                  AND trade_date = :trade_date
                  AND created_at <= :decision_at
                ORDER BY version DESC, created_at DESC
                LIMIT 1
                """
            ),
            {
                "account_id": account_id,
                "trade_date": trade_date,
                "decision_at": decision_at,
            },
        ).mappings().first()
        _require_account_truth(
            account,
            equity,
            reconciliation,
            trade_date=trade_date,
            decision_at=decision_at,
        )
        position_rows = connection.execute(
            text(
                """
                SELECT l.lot_id, l.stock_code, l.theme_code,
                       l.remaining_quantity, l.cost_price,
                       l.protective_stop, l.opened_trade_date,
                       l.settlement_date, l.created_at, l.closed_at,
                       MAX(
                           CASE
                             WHEN i.reason_code = 'V3_PAPER_DISCOVERY'
                             THEN 1 ELSE 0
                           END
                       ) AS is_paper_discovery
                FROM st_position_lot_v2 l
                LEFT JOIN st_fill_v2 f ON f.fill_id = l.opened_fill_id
                LEFT JOIN st_order_v2 o ON o.order_id = f.order_id
                LEFT JOIN st_trade_intent_v2 i ON i.intent_id = o.intent_id
                WHERE l.account_id = :account_id
                  AND l.created_at <= :decision_at
                  AND (l.closed_at IS NULL OR l.closed_at > :decision_at)
                  AND l.remaining_quantity > 0
                GROUP BY l.lot_id, l.stock_code, l.theme_code,
                         l.remaining_quantity, l.cost_price,
                         l.protective_stop, l.opened_trade_date,
                         l.settlement_date, l.created_at, l.closed_at
                ORDER BY l.stock_code, l.lot_id
                """
            ),
            {"account_id": account_id, "decision_at": decision_at},
        ).mappings().all()
        open_order_rows = connection.execute(
            text(
                """
                SELECT o.order_id, o.intent_id, o.stock_code, o.side,
                       o.quantity, o.filled_quantity, o.status,
                       o.limit_price, o.earliest_at, o.expires_at,
                       o.created_at, o.updated_at,
                       i.decision_run_uid, i.theme_code, i.worst_price,
                       i.protective_stop, i.reason_code
                FROM st_order_v2 o
                JOIN st_trade_intent_v2 i ON i.intent_id = o.intent_id
                WHERE o.account_id = :account_id
                  AND o.created_at <= :decision_at
                  AND o.status IN (
                      'CREATED', 'RISK_APPROVED', 'QUEUED',
                      'PARTIALLY_FILLED'
                  )
                ORDER BY o.stock_code, o.side, o.order_id
                """
            ),
            {"account_id": account_id, "decision_at": decision_at},
        ).mappings().all()

    equity_value = float(equity["total_equity"])
    position_weights: dict[str, float] = defaultdict(float)
    position_quantities: dict[str, int] = defaultdict(int)
    position_themes: dict[str, set[str]] = defaultdict(set)
    theme_weights: dict[str, float] = defaultdict(float)
    paper_discovery_codes: set[str] = set()
    open_risk_cny = 0.0
    reserved_cash_cny = 0.0
    for raw in position_rows:
        row = dict(raw)
        code = str(row.get("stock_code") or "")
        quantity = int(row.get("remaining_quantity") or 0)
        if not code or quantity <= 0:
            continue
        price = float(prices.get(code) or 0)
        if price <= 0:
            raise DecisionTruthBlocked(
                "POSITION_VALUATION_PRICE_MISSING",
                f"持仓 {code} 缺少 decision_at 可验证估值价",
            )
        value = quantity * price
        position_weights[code] += value / equity_value
        position_quantities[code] += quantity
        theme = str(row.get("theme_code") or "")
        if theme:
            position_themes[code].add(theme)
            theme_weights[theme] += value / equity_value
        stop = float(row.get("protective_stop") or 0)
        open_risk_cny += quantity * (
            max(0.0, price - stop) if stop > 0 else price * 0.08
        )
        if int(row.get("is_paper_discovery") or 0) == 1:
            paper_discovery_codes.add(code)
    for raw in open_order_rows:
        row = dict(raw)
        if str(row.get("side") or "") != "BUY":
            continue
        remaining = max(
            0,
            int(row.get("quantity") or 0)
            - int(row.get("filled_quantity") or 0),
        )
        price = max(
            float(row.get("worst_price") or 0),
            float(row.get("limit_price") or 0),
        )
        if remaining <= 0 or price <= 0:
            continue
        value = remaining * price
        reserved_cash_cny += value
        code = str(row.get("stock_code") or "")
        position_weights[code] += value / equity_value
        theme = str(row.get("theme_code") or "")
        if theme:
            position_themes[code].add(theme)
            theme_weights[theme] += value / equity_value
        stop = float(row.get("protective_stop") or 0)
        open_risk_cny += remaining * (
            max(0.0, price - stop) if stop > 0 else price * 0.08
        )

    account_row = _plain_row(account)
    equity_row = _plain_row(equity)
    reconciliation_row = _plain_row(reconciliation)
    positions = [_plain_row(row) for row in position_rows]
    open_orders = [_plain_row(row) for row in open_order_rows]
    valuation_prices = {
        code: prices[code]
        for code in sorted({
            str(row.get("stock_code") or "")
            for row in position_rows
            if int(row.get("remaining_quantity") or 0) > 0
        })
        if code and float(prices.get(code) or 0) > 0
    }
    manifest = {
        "schema_version": "probiga.trading-v3.decision-snapshot.v1",
        "requested_as_of": requested_as_of.isoformat(),
        "trade_date": trade_date.isoformat(),
        "decision_at": decision_at.isoformat(sep=" "),
        "knowledge_cutoff_at": decision_at.isoformat(sep=" "),
        "feature_time": feature_time.isoformat(sep=" "),
        "data_source": str(data_source or ""),
        "data_snapshot_hash": str(data_snapshot_hash or ""),
        "account": account_row,
        "equity": equity_row,
        "reconciliation": reconciliation_row,
        "positions": positions,
        "open_orders": open_orders,
        "valuation_prices": valuation_prices,
        "section_hashes": {
            "account": canonical_hash(account_row),
            "equity": canonical_hash(equity_row),
            "reconciliation": canonical_hash(reconciliation_row),
            "positions": canonical_hash(positions),
            "open_orders": canonical_hash(open_orders),
            "valuation_prices": canonical_hash(valuation_prices),
        },
    }
    manifest["manifest_hash"] = canonical_hash(manifest)
    return {
        "equity": equity_value,
        "cash_balance": float(equity["cash_balance"]),
        "portfolio_state": {
            "position_weights": dict(position_weights),
            "position_quantities": dict(position_quantities),
            "position_themes": {
                key: tuple(sorted(values))
                for key, values in position_themes.items()
            },
            "paper_discovery_codes": paper_discovery_codes,
            "theme_weights": dict(theme_weights),
            "open_risk_weight": open_risk_cny / equity_value,
            "reserved_cash_cny": reserved_cash_cny,
        },
        "manifest": manifest,
    }


__all__ = [
    "DecisionTruthBlocked",
    "canonical_hash",
    "canonical_json",
    "load_decision_snapshot",
]
