from __future__ import annotations

import hashlib
import json
from collections import defaultdict, deque
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterable, Mapping

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine


EXECUTED_FORWARD_PROTOCOL = "PAPER_EXECUTED_LEDGER_V1"
ATTRIBUTION_VERSION = "V3_PRIMARY_FORECAST_SNAPSHOT_V1"
EXECUTED_INTENT_REASONS = frozenset({
    "V3_PAPER_DISCOVERY",
    "V3_VALIDATED_POSITIVE",
})


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value or 0))


def _evidence_id(entry_fill_id: str, strategy_key: str) -> str:
    return hashlib.sha256(
        f"{entry_fill_id}|{strategy_key}|{EXECUTED_FORWARD_PROTOCOL}".encode(
            "utf-8"
        )
    ).hexdigest()


def _ownership_hash(
    run_uid: str,
    forecast_id: str,
    stock_code: str,
    strategy_key: str,
) -> str:
    return hashlib.sha256(
        (
            f"{run_uid}|{forecast_id}|{stock_code}|{strategy_key}"
        ).encode("utf-8")
    ).hexdigest()


def _intent_evidence(row: Mapping[str, Any]) -> dict[str, Any]:
    try:
        payload = json.loads(str(row.get("evidence_json") or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _strategy_keys(row: Mapping[str, Any]) -> tuple[str, ...]:
    if str(row.get("intent_reason_code") or "") not in EXECUTED_INTENT_REASONS:
        return ()
    payload = _intent_evidence(row)
    values = (
        payload.get("supporting_strategy_keys")
        or payload.get("signal_strategy_keys")
        or ()
    )
    return tuple(sorted({
        str(item)
        for item in values
        if str(item) and str(item) != "paper_discovery"
    }))


def _sample_owner(
    row: Mapping[str, Any],
    forecast_ids: Mapping[tuple[str, str, str], str],
) -> tuple[dict[str, str] | None, str]:
    if str(row.get("intent_reason_code") or "") not in EXECUTED_INTENT_REASONS:
        return None, "NOT_V3_ENTRY"
    payload = _intent_evidence(row)
    relational_run_uid = str(row.get("decision_run_uid") or "")
    payload_run_uid = str(payload.get("run_uid") or "")
    if not relational_run_uid:
        return None, "RELATIONAL_RUN_MISSING"
    if payload_run_uid and payload_run_uid != relational_run_uid:
        return None, "RUN_UID_MISMATCH"
    stock_code = str(row.get("stock_code") or "")
    supporting = _strategy_keys(row)
    primary_strategy_key = str(
        payload.get("primary_strategy_key") or ""
    )
    primary_forecast_id = str(
        payload.get("primary_forecast_id") or ""
    )
    attribution_version = str(
        payload.get("attribution_version") or ""
    )
    if primary_strategy_key:
        if primary_strategy_key not in supporting:
            return None, "PRIMARY_NOT_IN_SUPPORTING_SET"
        expected_forecast_id = str(forecast_ids.get(
            (
                relational_run_uid,
                stock_code,
                primary_strategy_key,
            ),
            "",
        ))
        if not expected_forecast_id:
            return None, "PRIMARY_FORECAST_NOT_FOUND"
        if primary_forecast_id != expected_forecast_id:
            return None, "PRIMARY_FORECAST_ID_MISMATCH"
        expected_hash = _ownership_hash(
            relational_run_uid,
            primary_forecast_id,
            stock_code,
            primary_strategy_key,
        )
        if str(payload.get("ownership_hash") or "") != expected_hash:
            return None, "OWNERSHIP_HASH_MISMATCH"
        if attribution_version != ATTRIBUTION_VERSION:
            return None, "ATTRIBUTION_VERSION_MISMATCH"
        if str(payload.get("sample_owner_role") or "") != "PRIMARY":
            return None, "OWNER_ROLE_MISMATCH"
        return ({
            "strategy_key": primary_strategy_key,
            "source_forecast_id": primary_forecast_id,
            "source_run_uid": relational_run_uid,
            "source_intent_id": str(row.get("intent_id") or ""),
            "sample_owner_role": "PRIMARY",
            "attribution_status": "VERIFIED_SNAPSHOT",
            "attribution_version": attribution_version,
            "supporting_strategy_keys_json": json.dumps(
                list(supporting),
                ensure_ascii=False,
            ),
            "ownership_hash": expected_hash,
        }, "")

    # Compatibility is deliberately limited to a single unambiguous sleeve.
    # Legacy multi-sleeve fills are quarantined instead of being duplicated
    # into several learning ledgers.
    if len(supporting) != 1:
        return None, "LEGACY_OWNER_AMBIGUOUS"
    primary_strategy_key = supporting[0]
    primary_forecast_id = str(forecast_ids.get(
        (relational_run_uid, stock_code, primary_strategy_key),
        "",
    ))
    if not primary_forecast_id:
        return None, "LEGACY_FORECAST_NOT_FOUND"
    expected_hash = _ownership_hash(
        relational_run_uid,
        primary_forecast_id,
        stock_code,
        primary_strategy_key,
    )
    return ({
        "strategy_key": primary_strategy_key,
        "source_forecast_id": primary_forecast_id,
        "source_run_uid": relational_run_uid,
        "source_intent_id": str(row.get("intent_id") or ""),
        "sample_owner_role": "PRIMARY",
        "attribution_status": "LEGACY_SINGLE_STRATEGY_RESOLVED",
        "attribution_version": "LEGACY_SINGLE_STRATEGY_V1",
        "supporting_strategy_keys_json": json.dumps(
            list(supporting),
            ensure_ascii=False,
        ),
        "ownership_hash": expected_hash,
    }, "")


def reconstruct_executed_forward_records(
    fill_rows: Iterable[Mapping[str, Any]],
    *,
    forecast_ids: Mapping[tuple[str, str, str], str] | None = None,
    diagnostics: dict[str, int] | None = None,
) -> list[dict[str, Any]]:
    """Rebuild actual V3 paper round trips from the append-only fill ledger.

    Sell fills are allocated with the same stock-level FIFO rule used by the
    production ledger.  Cancelled, expired, and merely queued orders never
    enter this evidence set because they have no fill event.
    """

    forecast_ids = forecast_ids or {}
    diagnostics = diagnostics if diagnostics is not None else {}
    fifo: dict[str, deque[dict[str, Any]]] = defaultdict(deque)
    executed: dict[str, dict[str, Any]] = {}
    ordered = sorted(
        (dict(row) for row in fill_rows),
        key=lambda row: (
            row.get("filled_at") or datetime.min,
            str(row.get("fill_id") or ""),
        ),
    )
    for row in ordered:
        code = str(row.get("stock_code") or "")
        side = str(row.get("side") or "").upper()
        quantity = max(0, int(row.get("quantity") or 0))
        if not code or quantity <= 0:
            continue
        if side == "BUY":
            owner, rejection = _sample_owner(row, forecast_ids)
            if rejection and rejection != "NOT_V3_ENTRY":
                diagnostics[rejection] = diagnostics.get(rejection, 0) + 1
            lot = {
                "entry_fill_id": str(row.get("fill_id") or ""),
                "entry_order_id": str(row.get("order_id") or ""),
                "stock_code": code,
                "quantity": quantity,
                "remaining_quantity": quantity,
                "entry_price": _decimal(row.get("price")),
                "entry_gross": _decimal(row.get("gross_amount")),
                "entry_fee": _decimal(row.get("fee_amount")),
                "entry_at": row.get("filled_at"),
                "owner": owner,
                "closed_quantity": 0,
                "exit_gross": Decimal("0"),
                "exit_fee": Decimal("0"),
                "exit_fill_ids": [],
                "exit_order_ids": [],
                "exit_at": None,
                "exit_reason": "",
            }
            fifo[code].append(lot)
            if owner:
                executed[lot["entry_fill_id"]] = lot
            continue
        if side != "SELL":
            continue
        sell_remaining = quantity
        sell_fee = _decimal(row.get("fee_amount"))
        sell_price = _decimal(row.get("price"))
        while sell_remaining > 0 and fifo[code]:
            lot = fifo[code][0]
            consumed = min(sell_remaining, int(lot["remaining_quantity"]))
            allocated_fee = sell_fee * Decimal(consumed) / Decimal(quantity)
            lot["remaining_quantity"] -= consumed
            lot["closed_quantity"] += consumed
            lot["exit_gross"] += sell_price * Decimal(consumed)
            lot["exit_fee"] += allocated_fee
            lot["exit_fill_ids"].append(str(row.get("fill_id") or ""))
            lot["exit_order_ids"].append(str(row.get("order_id") or ""))
            lot["exit_at"] = row.get("filled_at")
            lot["exit_reason"] = str(row.get("intent_reason_code") or "")
            sell_remaining -= consumed
            if int(lot["remaining_quantity"]) <= 0:
                fifo[code].popleft()

    records: list[dict[str, Any]] = []
    for lot in executed.values():
        entry_quantity = int(lot["quantity"])
        closed_quantity = int(lot["closed_quantity"])
        entry_fraction = (
            Decimal(closed_quantity) / Decimal(entry_quantity)
            if entry_quantity > 0
            else Decimal("0")
        )
        allocated_entry_gross = lot["entry_gross"] * entry_fraction
        allocated_entry_fee = lot["entry_fee"] * entry_fraction
        net_pnl = (
            lot["exit_gross"]
            - lot["exit_fee"]
            - allocated_entry_gross
            - allocated_entry_fee
        )
        cost_basis = allocated_entry_gross + allocated_entry_fee
        net_return = (
            net_pnl / cost_basis * Decimal("100")
            if closed_quantity > 0 and cost_basis > 0
            else None
        )
        entry_at = lot["entry_at"]
        exit_at = lot["exit_at"]
        status = (
            "MATURED"
            if closed_quantity >= entry_quantity
            else "PARTIALLY_CLOSED"
            if closed_quantity > 0
            else "OPEN"
        )
        owner = dict(lot["owner"] or {})
        strategy_key = str(owner["strategy_key"])
        source_run_uid = str(owner["source_run_uid"])
        records.append({
                "evidence_id": _evidence_id(
                    str(lot["entry_fill_id"]),
                    strategy_key,
                ),
                "account_id": "",
                "source_run_uid": source_run_uid,
                "source_forecast_id": owner["source_forecast_id"],
                "source_intent_id": owner["source_intent_id"],
                "stock_code": lot["stock_code"],
                "strategy_key": strategy_key,
                "sample_owner_role": owner["sample_owner_role"],
                "attribution_status": owner["attribution_status"],
                "attribution_version": owner["attribution_version"],
                "supporting_strategy_keys_json": owner[
                    "supporting_strategy_keys_json"
                ],
                "ownership_hash": owner["ownership_hash"],
                "evidence_kind": "EXECUTED_PAPER",
                "protocol_version": EXECUTED_FORWARD_PROTOCOL,
                "entry_order_id": lot["entry_order_id"],
                "entry_fill_id": lot["entry_fill_id"],
                "entry_trade_date": (
                    entry_at.date()
                    if isinstance(entry_at, datetime)
                    else entry_at
                ),
                "entry_at": entry_at,
                "entry_quantity": entry_quantity,
                "entry_price": lot["entry_price"],
                "entry_gross_cny": lot["entry_gross"],
                "entry_fee_cny": lot["entry_fee"],
                "closed_quantity": closed_quantity,
                "exit_fill_ids_json": json.dumps(
                    list(dict.fromkeys(lot["exit_fill_ids"])),
                    ensure_ascii=False,
                ),
                "exit_order_ids_json": json.dumps(
                    list(dict.fromkeys(lot["exit_order_ids"])),
                    ensure_ascii=False,
                ),
                "exit_at": exit_at,
                "exit_average_price": (
                    lot["exit_gross"] / Decimal(closed_quantity)
                    if closed_quantity > 0
                    else None
                ),
                "exit_gross_cny": lot["exit_gross"],
                "exit_fee_cny": lot["exit_fee"],
                "realized_net_pnl_cny": net_pnl,
                "realized_net_return_pct": net_return,
                "realized_mae_pct": None,
                "realized_mfe_pct": None,
                "exit_reason": lot["exit_reason"],
                "evidence_status": status,
            })
    return sorted(
        records,
        key=lambda item: (
            item["entry_at"] or datetime.min,
            item["stock_code"],
            item["strategy_key"],
        ),
    )


def _attach_excursions(
    records: list[dict[str, Any]],
    kline_engine: Engine,
) -> None:
    matured = [
        item
        for item in records
        if item["evidence_status"] == "MATURED"
        and item.get("entry_trade_date") is not None
        and item.get("exit_at") is not None
    ]
    if not matured:
        return
    codes = sorted({str(item["stock_code"]) for item in matured})
    start_date = min(item["entry_trade_date"] for item in matured)
    end_date = max(item["exit_at"].date() for item in matured)
    statement = text(
        """
        SELECT stock_code, trade_date, high, low
        FROM sm_stock_kline
        WHERE k_type = 1
          AND stock_code IN :codes
          AND trade_date BETWEEN :start_date AND :end_date
        ORDER BY stock_code, trade_date
        """
    ).bindparams(bindparam("codes", expanding=True))
    with kline_engine.connect() as connection:
        rows = connection.execute(
            statement,
            {
                "codes": codes,
                "start_date": start_date,
                "end_date": end_date,
            },
        ).mappings().all()
    by_code: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        by_code[str(row["stock_code"])].append(row)
    for item in matured:
        entry_price = float(item["entry_price"] or 0)
        if entry_price <= 0:
            continue
        exit_date = item["exit_at"].date()
        bars = [
            row
            for row in by_code.get(str(item["stock_code"]), ())
            if item["entry_trade_date"] <= row["trade_date"] <= exit_date
        ]
        if not bars:
            continue
        minimum_low = min(float(row["low"]) for row in bars)
        maximum_high = max(float(row["high"]) for row in bars)
        item["realized_mae_pct"] = (
            minimum_low / entry_price - 1.0
        ) * 100.0
        item["realized_mfe_pct"] = (
            maximum_high / entry_price - 1.0
        ) * 100.0


def sync_executed_forward_evidence(
    primary_engine: Engine,
    kline_engine: Engine,
    *,
    account_id: str = "paper-main-v2",
) -> dict[str, Any]:
    """Persist fill-backed forward evidence; no order or trading state mutates."""

    with primary_engine.connect() as connection:
        fills = [dict(row) for row in connection.execute(
            text(
                """
                SELECT f.fill_id, f.order_id, f.account_id, f.stock_code,
                       f.side, f.quantity, f.price, f.gross_amount,
                       f.fee_amount, f.filled_at,
                       i.intent_id, i.decision_run_uid,
                       i.reason_code AS intent_reason_code,
                       i.evidence_json
                FROM st_fill_v2 f
                JOIN st_order_v2 o ON o.order_id = f.order_id
                JOIN st_trade_intent_v2 i ON i.intent_id = o.intent_id
                WHERE f.account_id = :account_id
                ORDER BY f.filled_at, f.fill_id
                """
            ),
            {"account_id": account_id},
        ).mappings().all()]
    run_uids = sorted({
        str(row.get("decision_run_uid") or "")
        for row in fills
        if _strategy_keys(row)
    } - {""})
    forecast_ids: dict[tuple[str, str, str], str] = {}
    if run_uids:
        statement = text(
            """
            SELECT forecast_id, run_uid, stock_code, strategy_key
            FROM st_alpha_forecast_v3
            WHERE run_uid IN :run_uids
            """
        ).bindparams(bindparam("run_uids", expanding=True))
        with primary_engine.connect() as connection:
            for row in connection.execute(
                statement,
                {"run_uids": run_uids},
            ).mappings():
                forecast_ids[(
                    str(row["run_uid"]),
                    str(row["stock_code"]),
                    str(row["strategy_key"]),
                )] = str(row["forecast_id"])
    attribution_rejections: dict[str, int] = {}
    records = reconstruct_executed_forward_records(
        fills,
        forecast_ids=forecast_ids,
        diagnostics=attribution_rejections,
    )
    for item in records:
        item["account_id"] = account_id
    _attach_excursions(records, kline_engine)
    now = datetime.now().replace(microsecond=0)
    with primary_engine.begin() as connection:
        for item in records:
            connection.execute(
                text(
                    """
                    INSERT INTO st_forward_trade_evidence_v3 (
                        evidence_id, account_id, source_run_uid,
                        source_forecast_id, source_intent_id,
                        stock_code, strategy_key, sample_owner_role,
                        attribution_status, attribution_version,
                        supporting_strategy_keys_json, ownership_hash,
                        evidence_kind, protocol_version,
                        entry_order_id, entry_fill_id, entry_trade_date,
                        entry_at, entry_quantity, entry_price,
                        entry_gross_cny, entry_fee_cny, closed_quantity,
                        exit_fill_ids_json, exit_order_ids_json, exit_at,
                        exit_average_price, exit_gross_cny, exit_fee_cny,
                        realized_net_pnl_cny, realized_net_return_pct,
                        realized_mae_pct, realized_mfe_pct, exit_reason,
                        evidence_status, created_at, updated_at
                    ) VALUES (
                        :evidence_id, :account_id, :source_run_uid,
                        :source_forecast_id, :source_intent_id,
                        :stock_code, :strategy_key, :sample_owner_role,
                        :attribution_status, :attribution_version,
                        :supporting_strategy_keys_json, :ownership_hash,
                        :evidence_kind, :protocol_version,
                        :entry_order_id, :entry_fill_id, :entry_trade_date,
                        :entry_at, :entry_quantity, :entry_price,
                        :entry_gross_cny, :entry_fee_cny, :closed_quantity,
                        :exit_fill_ids_json, :exit_order_ids_json, :exit_at,
                        :exit_average_price, :exit_gross_cny, :exit_fee_cny,
                        :realized_net_pnl_cny, :realized_net_return_pct,
                        :realized_mae_pct, :realized_mfe_pct, :exit_reason,
                        :evidence_status, :created_at, :updated_at
                    )
                    ON DUPLICATE KEY UPDATE
                        protocol_version = VALUES(protocol_version),
                        closed_quantity = VALUES(closed_quantity),
                        exit_fill_ids_json = VALUES(exit_fill_ids_json),
                        exit_order_ids_json = VALUES(exit_order_ids_json),
                        exit_at = VALUES(exit_at),
                        exit_average_price = VALUES(exit_average_price),
                        exit_gross_cny = VALUES(exit_gross_cny),
                        exit_fee_cny = VALUES(exit_fee_cny),
                        realized_net_pnl_cny = VALUES(realized_net_pnl_cny),
                        realized_net_return_pct =
                            VALUES(realized_net_return_pct),
                        realized_mae_pct = VALUES(realized_mae_pct),
                        realized_mfe_pct = VALUES(realized_mfe_pct),
                        exit_reason = VALUES(exit_reason),
                        evidence_status = VALUES(evidence_status),
                        updated_at = VALUES(updated_at)
                    """
                ),
                {**item, "created_at": now, "updated_at": now},
            )
    return {
        "status": "ok",
        "protocol_version": EXECUTED_FORWARD_PROTOCOL,
        "attribution_version": ATTRIBUTION_VERSION,
        "fill_count": len(fills),
        "evidence_count": len(records),
        "unattributed_v3_buy_fill_count": sum(
            attribution_rejections.values()
        ),
        "attribution_rejection_counts": dict(sorted(
            attribution_rejections.items()
        )),
        "open_count": sum(
            item["evidence_status"] == "OPEN" for item in records
        ),
        "partially_closed_count": sum(
            item["evidence_status"] == "PARTIALLY_CLOSED"
            for item in records
        ),
        "matured_count": sum(
            item["evidence_status"] == "MATURED" for item in records
        ),
    }
