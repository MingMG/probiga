from __future__ import annotations

import hashlib
import json
from collections import defaultdict, deque
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Iterable, Mapping

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine


EXECUTED_FORWARD_PROTOCOL = "PAPER_EXECUTED_LEDGER_V1"
EXIT_ALLOCATION_PROTOCOL = "PAPER_FIFO_EXIT_ALLOCATION_V1"
INTENT_EPISODE_PROTOCOL = "PAPER_EXECUTED_INTENT_CASH_EPISODE_V1"
ATTRIBUTION_VERSION = "V3_PRIMARY_FORECAST_SNAPSHOT_V1"
EXECUTED_INTENT_REASONS = frozenset({
    "DYNAMIC_SHADOW_BOOTSTRAP",
    "V3_PAPER_DISCOVERY",
    "V3_VALIDATED_POSITIVE",
})


def primary_strategy_version(
    model_version: Any,
    strategy_key: Any,
) -> str:
    """Return the immutable V3 strategy-sleeve version for one decision run."""

    normalized_model_version = str(model_version or "").strip()
    normalized_strategy_key = str(strategy_key or "").strip()
    if not normalized_model_version or not normalized_strategy_key:
        raise ValueError(
            "V3 primary strategy version requires model_version and strategy_key"
        )
    value = f"{normalized_model_version}:{normalized_strategy_key}"
    if len(value) > 160:
        raise ValueError("V3 primary strategy version exceeds 160 characters")
    return value


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value or 0))


def _evidence_id(entry_fill_id: str, strategy_key: str) -> str:
    return hashlib.sha256(
        f"{entry_fill_id}|{strategy_key}|{EXECUTED_FORWARD_PROTOCOL}".encode(
            "utf-8"
        )
    ).hexdigest()


def intent_episode_id(source_intent_id: Any, strategy_version: Any) -> str:
    """Return the stable sample identity above immutable fill-level facts.

    ``st_forward_trade_evidence_v3`` deliberately remains a fill-backed fact
    ledger.  Statistical trade counts must use this intent/version identity,
    so partial fills and execution slicing cannot manufacture extra samples.
    """

    normalized_intent_id = str(source_intent_id or "").strip()
    normalized_strategy_version = str(strategy_version or "").strip()
    if not normalized_intent_id or not normalized_strategy_version:
        raise ValueError(
            "forward intent episode requires source_intent_id and strategy_version"
        )
    if len(normalized_intent_id) > 64 or len(normalized_strategy_version) > 160:
        raise ValueError("forward intent episode identity exceeds frozen limits")
    return hashlib.sha256(
        (
            f"{normalized_intent_id}|{normalized_strategy_version}|"
            f"{INTENT_EPISODE_PROTOCOL}"
        ).encode("utf-8")
    ).hexdigest()


def _exit_allocation_id(
    exit_fill_id: str,
    allocation_sequence: int,
    entry_fill_id: str,
) -> str:
    return hashlib.sha256(
        (
            f"{exit_fill_id}|{allocation_sequence}|{entry_fill_id}|"
            f"{EXIT_ALLOCATION_PROTOCOL}"
        ).encode("utf-8")
    ).hexdigest()


def _fifo_allocated_amounts(
    total: Decimal,
    quantities: tuple[int, ...],
    total_quantity: int,
) -> tuple[Decimal, ...]:
    """Allocate one raw amount exactly; the last FIFO effect owns the tail."""

    if (
        total_quantity <= 0
        or not quantities
        or any(quantity <= 0 for quantity in quantities)
        or sum(quantities) != total_quantity
    ):
        raise ValueError("FIFO amount allocation requires complete fill coverage")
    quantized_total = total.quantize(
        Decimal("0.000001"),
        rounding=ROUND_HALF_UP,
    )
    allocated = [
        (total * Decimal(quantity) / Decimal(total_quantity)).quantize(
            Decimal("0.000001"),
            rounding=ROUND_HALF_UP,
        )
        for quantity in quantities[:-1]
    ]
    allocated.append(quantized_total - sum(allocated, Decimal("0.000000")))
    if allocated[-1] < 0 or sum(allocated) != quantized_total:
        raise ValueError("FIFO amount allocation tail is invalid")
    return tuple(allocated)


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


def _intent_evidence(row: Mapping[str, Any]) -> dict[str, Any]:
    try:
        payload = json.loads(str(row.get("evidence_json") or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _runtime_governance_strategy_version(
    payload: Mapping[str, Any],
    primary_strategy_key: str,
) -> tuple[str, str]:
    bootstrap = payload.get("dynamic_shadow_bootstrap")
    if isinstance(bootstrap, Mapping):
        observed = dict(bootstrap)
        authorization_hash = str(observed.pop("authorization_hash", ""))
        expected_hash = hashlib.sha256(json.dumps(
            observed,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
            allow_nan=False,
        ).encode("utf-8")).hexdigest()
        version = str(observed.get("strategy_version") or "")
        if (
            observed.get("schema")
            != "probiga.dynamic-shadow-bootstrap-authorization.v1"
            or authorization_hash != expected_hash
            or str(observed.get("strategy_key") or "")
            != primary_strategy_key
            or not version
            or observed.get("real_order_authority") is not False
        ):
            return "", "DYNAMIC_SHADOW_BOOTSTRAP_AUTHORIZATION_INVALID"
        return version, ""
    receipt = payload.get("strategy_governance")
    if not isinstance(receipt, Mapping):
        return "", ""
    if str(receipt.get("strategy_source_kind") or "") != "runtime_registry":
        return "", ""
    observed = dict(receipt)
    receipt_hash = str(observed.pop("receipt_hash", ""))
    expected_hash = hashlib.sha256(json.dumps(
        observed,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
        allow_nan=False,
    ).encode("utf-8")).hexdigest()
    version = str(observed.get("strategy_version") or "")
    if (
        observed.get("schema")
        != "probiga.governance-paper-buy-receipt.v1"
        or receipt_hash != expected_hash
        or str(observed.get("strategy_key") or "") != primary_strategy_key
        or not version
        or observed.get("real_order_authority") is not False
    ):
        return "", "RUNTIME_GOVERNANCE_RECEIPT_INVALID"
    return version, ""


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
    run_model_versions: Mapping[str, str] | None = None,
) -> tuple[dict[str, str] | None, str]:
    if str(row.get("intent_reason_code") or "") not in EXECUTED_INTENT_REASONS:
        return None, "NOT_V3_ENTRY"
    source_intent_id = str(row.get("intent_id") or "").strip()
    if not source_intent_id:
        return None, "SOURCE_INTENT_ID_MISSING"
    payload = _intent_evidence(row)
    relational_run_uid = str(row.get("decision_run_uid") or "")
    payload_run_uid = str(payload.get("run_uid") or "")
    if not relational_run_uid:
        return None, "RELATIONAL_RUN_MISSING"
    if payload_run_uid and payload_run_uid != relational_run_uid:
        return None, "RUN_UID_MISMATCH"
    relational_model_version = str(
        (run_model_versions or {}).get(relational_run_uid) or ""
    ).strip()
    if not relational_model_version:
        return None, "RUN_MODEL_VERSION_NOT_FOUND"
    if str(payload.get("model_version") or "").strip() != (
        relational_model_version
    ):
        return None, "INTENT_MODEL_VERSION_MISMATCH"
    stock_code = str(row.get("stock_code") or "")
    supporting = _strategy_keys(row)
    primary_strategy_key = str(
        payload.get("primary_strategy_key") or ""
    )
    primary_forecast_id = str(
        payload.get("primary_forecast_id") or ""
    )
    frozen_strategy_version = str(
        payload.get("primary_strategy_version") or ""
    ).strip()
    attribution_version = str(
        payload.get("attribution_version") or ""
    )
    if not primary_strategy_key:
        return None, "PRIMARY_STRATEGY_KEY_MISSING"
    if primary_strategy_key not in supporting:
        return None, "PRIMARY_NOT_IN_SUPPORTING_SET"
    runtime_strategy_version, governance_rejection = (
        _runtime_governance_strategy_version(payload, primary_strategy_key)
    )
    if governance_rejection:
        return None, governance_rejection
    expected_strategy_version = (
        runtime_strategy_version
        or primary_strategy_version(
            relational_model_version,
            primary_strategy_key,
        )
    )
    if (
        (runtime_strategy_version and not frozen_strategy_version)
        or (
            frozen_strategy_version
            and frozen_strategy_version != expected_strategy_version
        )
    ):
        return None, "PRIMARY_STRATEGY_VERSION_MISMATCH"
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
        expected_strategy_version,
    )
    if str(payload.get("ownership_hash") or "") != expected_hash:
        return None, "OWNERSHIP_HASH_MISMATCH"
    if attribution_version != ATTRIBUTION_VERSION:
        return None, "ATTRIBUTION_VERSION_MISMATCH"
    if str(payload.get("sample_owner_role") or "") != "PRIMARY":
        return None, "OWNER_ROLE_MISMATCH"
    return ({
        "strategy_key": primary_strategy_key,
        "strategy_version": expected_strategy_version,
        "source_forecast_id": primary_forecast_id,
        "source_run_uid": relational_run_uid,
        "source_intent_id": source_intent_id,
        "sample_owner_role": "PRIMARY",
        "attribution_status": (
            "VERIFIED_SNAPSHOT"
            if frozen_strategy_version
            else "LEGACY_VERSION_DERIVED"
        ),
        "attribution_version": attribution_version,
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
    run_model_versions: Mapping[str, str] | None = None,
    diagnostics: dict[str, int] | None = None,
    diagnostic_fill_ids: set[str] | None = None,
    allocation_rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Rebuild actual V3 paper round trips from the append-only fill ledger.

    Sell fills are allocated with the same stock-level FIFO rule used by the
    production ledger.  Cancelled, expired, and merely queued orders never
    enter this evidence set because they have no fill event.
    """

    forecast_ids = forecast_ids or {}
    diagnostics = diagnostics if diagnostics is not None else {}
    diagnostic_fill_ids = (
        diagnostic_fill_ids if diagnostic_fill_ids is not None else set()
    )
    fifo: dict[str, deque[dict[str, Any]]] = defaultdict(deque)
    executed: dict[str, dict[str, Any]] = {}
    all_lots: list[dict[str, Any]] = []
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
            owner, rejection = _sample_owner(
                row,
                forecast_ids,
                run_model_versions,
            )
            if rejection and rejection != "NOT_V3_ENTRY":
                diagnostics[rejection] = diagnostics.get(rejection, 0) + 1
                diagnostic_fill_ids.add(str(row.get("fill_id") or ""))
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
                "exit_allocations": [],
                "exit_at": None,
                "exit_reason": "",
            }
            fifo[code].append(lot)
            all_lots.append(lot)
            if owner:
                executed[lot["entry_fill_id"]] = lot
            continue
        if side != "SELL":
            continue
        sell_remaining = quantity
        sell_fee = _decimal(row.get("fee_amount"))
        sell_gross = _decimal(row.get("gross_amount"))
        fragments: list[tuple[dict[str, Any], int]] = []
        # Stage the coverage decision before mutating any lot.  A malformed
        # raw SELL must not leave the in-memory replay partially consumed.
        for lot in fifo[code]:
            if sell_remaining <= 0:
                break
            consumed = min(sell_remaining, int(lot["remaining_quantity"]))
            fragments.append((lot, consumed))
            sell_remaining -= consumed
        if sell_remaining > 0 or sum(item[1] for item in fragments) != quantity:
            diagnostics["SELL_FIFO_COVERAGE_GAP"] = (
                diagnostics.get("SELL_FIFO_COVERAGE_GAP", 0) + 1
            )
            diagnostic_fill_ids.add(str(row.get("fill_id") or ""))
            continue
        for lot, consumed in fragments:
            lot["remaining_quantity"] -= consumed
            lot["closed_quantity"] += consumed
            lot["exit_at"] = row.get("filled_at")
            lot["exit_reason"] = str(row.get("intent_reason_code") or "")
        while fifo[code] and int(fifo[code][0]["remaining_quantity"]) <= 0:
            fifo[code].popleft()
        quantities = tuple(item[1] for item in fragments)
        gross_allocations = _fifo_allocated_amounts(
            sell_gross, quantities, quantity,
        )
        fee_allocations = _fifo_allocated_amounts(
            sell_fee, quantities, quantity,
        )
        exit_fill_id = str(row.get("fill_id") or "")
        exit_order_id = str(row.get("order_id") or "")
        for sequence, ((lot, consumed), allocated_gross, allocated_fee) in enumerate(
            zip(fragments, gross_allocations, fee_allocations)
        ):
            owner = dict(lot.get("owner") or {})
            evidence_id = (
                _evidence_id(
                    str(lot["entry_fill_id"]),
                    str(owner["strategy_key"]),
                )
                if owner
                else None
            )
            allocation = {
                "allocation_id": _exit_allocation_id(
                    exit_fill_id,
                    sequence,
                    str(lot["entry_fill_id"]),
                ),
                "evidence_id": evidence_id,
                "attribution_status": (
                    "ATTRIBUTED" if evidence_id else "UNATTRIBUTED"
                ),
                "account_id": "",
                "stock_code": code,
                "entry_fill_id": str(lot["entry_fill_id"]),
                "exit_fill_id": exit_fill_id,
                "exit_order_id": exit_order_id,
                "allocation_sequence": sequence,
                "allocated_quantity": consumed,
                "allocated_gross_cny": allocated_gross,
                "allocated_fee_cny": allocated_fee,
                "exit_filled_at": row.get("filled_at"),
                "allocation_protocol_version": EXIT_ALLOCATION_PROTOCOL,
            }
            lot["exit_gross"] += allocated_gross
            lot["exit_fee"] += allocated_fee
            lot["exit_fill_ids"].append(exit_fill_id)
            lot["exit_order_ids"].append(exit_order_id)
            lot["exit_allocations"].append(allocation)

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
        evidence_id = _evidence_id(
            str(lot["entry_fill_id"]),
            strategy_key,
        )
        exit_allocations = list(lot["exit_allocations"])
        records.append({
                "evidence_id": evidence_id,
                "account_id": "",
                "source_run_uid": source_run_uid,
                "source_forecast_id": owner["source_forecast_id"],
                "source_intent_id": owner["source_intent_id"],
                "stock_code": lot["stock_code"],
                "strategy_key": strategy_key,
                "strategy_version": owner["strategy_version"],
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
                "exit_allocations": exit_allocations,
            })
    normalized_records = sorted(
        records,
        key=lambda item: (
            item["entry_at"] or datetime.min,
            item["stock_code"],
            item["strategy_key"],
        ),
    )
    if allocation_rows is not None:
        allocation_rows.extend(
            allocation
            for lot in all_lots
            for allocation in lot["exit_allocations"]
        )
    return normalized_records


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


def _allocation_contract(row: Mapping[str, Any]) -> tuple[Any, ...]:
    filled_at = row.get("exit_filled_at")
    if isinstance(filled_at, datetime):
        normalized_filled_at = filled_at.replace(microsecond=0).isoformat(
            sep=" "
        )
    else:
        normalized_filled_at = str(filled_at or "").replace("T", " ")
    return (
        str(row.get("allocation_id") or ""),
        str(row.get("evidence_id") or ""),
        str(row.get("attribution_status") or ""),
        str(row.get("account_id") or ""),
        str(row.get("stock_code") or ""),
        str(row.get("entry_fill_id") or ""),
        str(row.get("exit_fill_id") or ""),
        str(row.get("exit_order_id") or ""),
        int(row.get("allocation_sequence") or 0),
        int(row.get("allocated_quantity") or 0),
        str(_decimal(row.get("allocated_gross_cny")).quantize(
            Decimal("0.000001")
        )),
        str(_decimal(row.get("allocated_fee_cny")).quantize(
            Decimal("0.000001")
        )),
        normalized_filled_at,
        str(row.get("allocation_protocol_version") or ""),
    )


def _assert_persisted_exit_allocations(
    connection: Any,
    allocation_rows: list[dict[str, Any]],
    *,
    account_id: str,
) -> int:
    expected = sorted(_allocation_contract(row) for row in allocation_rows)
    statement = text(
        """
        SELECT allocation_id, evidence_id, attribution_status,
               account_id, stock_code,
               entry_fill_id, exit_fill_id, exit_order_id,
               allocation_sequence, allocated_quantity,
               allocated_gross_cny, allocated_fee_cny,
               exit_filled_at, allocation_protocol_version
        FROM st_forward_exit_allocation_v3
        WHERE account_id = :account_id
        ORDER BY exit_filled_at, exit_fill_id, allocation_sequence
        """
    )
    observed = sorted(
        _allocation_contract(row)
        for row in connection.execute(
            statement,
            {"account_id": account_id},
        ).mappings()
    )
    if observed != expected:
        raise RuntimeError(
            "persisted forward exit allocations differ from deterministic FIFO replay"
        )
    return len(observed)


def _require_valid_dynamic_shadow_binding(
    binding: Mapping[str, Any],
) -> None:
    """Fail the scheduled writer when the immutable shadow chain is invalid.

    The forward-evidence rows are idempotent, so surfacing this as a hard task
    failure is safe to retry and prevents an outer ``status=ok`` from masking a
    broken candidate-to-fill evidence chain.
    """

    status = str(binding.get("status") or "")
    if status != "OK":
        raise RuntimeError(
            "dynamic shadow evidence binding failed closed: "
            f"status={status or 'MISSING'}"
        )


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
                       i.strategy_version AS intent_strategy_version,
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
    run_model_versions: dict[str, str] = {}
    attribution_rejections: dict[str, int] = {}
    attribution_rejected_fill_ids: set[str] = set()
    if run_uids:
        statement = text(
            """
            SELECT f.forecast_id, f.run_uid, f.stock_code, f.strategy_key,
                   r.model_version AS run_model_version
            FROM st_alpha_forecast_v3 f
            JOIN st_decision_run_v3 r ON r.run_uid = f.run_uid
            WHERE f.run_uid IN :run_uids
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
                run_model_versions[str(row["run_uid"])] = str(
                    row["run_model_version"]
                )
    bootstrap_rows = [
        row for row in fills
        if str(row.get("intent_reason_code") or "")
        == "DYNAMIC_SHADOW_BOOTSTRAP"
        and str(row.get("side") or "").upper() == "BUY"
    ]
    if bootstrap_rows:
        from server.engine.dynamic_shadow_ledger import (
            verify_dynamic_shadow_bootstrap_authorization,
        )

        with primary_engine.connect() as connection:
            for row in bootstrap_rows:
                payload = _intent_evidence(row)
                authorization = payload.get("dynamic_shadow_bootstrap")
                try:
                    verified = verify_dynamic_shadow_bootstrap_authorization(
                        connection,
                        authorization,
                        require_current_shadow=False,
                    )
                    if (
                        str(row.get("decision_run_uid") or "")
                        != verified["candidate_run_uid"]
                        or str(row.get("intent_strategy_version") or "")
                        != verified["strategy_version"]
                        or str(row.get("stock_code") or "")
                        != verified["stock_code"]
                    ):
                        raise ValueError("bootstrap intent identity mismatch")
                except Exception:
                    key = "DYNAMIC_SHADOW_BOOTSTRAP_AUTHORIZATION_INVALID"
                    attribution_rejections[key] = (
                        attribution_rejections.get(key, 0) + 1
                    )
                    attribution_rejected_fill_ids.add(
                        str(row.get("fill_id") or "")
                    )
                    continue
                owner_key = (
                    verified["candidate_run_uid"],
                    verified["stock_code"],
                    verified["strategy_key"],
                )
                existing = forecast_ids.get(owner_key)
                if existing and existing != verified["shadow_forecast_id"]:
                    attribution_rejections[
                        "DYNAMIC_SHADOW_BOOTSTRAP_OWNER_AMBIGUOUS"
                    ] = attribution_rejections.get(
                        "DYNAMIC_SHADOW_BOOTSTRAP_OWNER_AMBIGUOUS", 0,
                    ) + 1
                    attribution_rejected_fill_ids.add(
                        str(row.get("fill_id") or "")
                    )
                    continue
                forecast_ids[owner_key] = verified["shadow_forecast_id"]
                run_model_versions[verified["candidate_run_uid"]] = (
                    verified["strategy_version"]
                )
    exit_allocations: list[dict[str, Any]] = []
    records = reconstruct_executed_forward_records(
        fills,
        forecast_ids=forecast_ids,
        run_model_versions=run_model_versions,
        diagnostics=attribution_rejections,
        diagnostic_fill_ids=attribution_rejected_fill_ids,
        allocation_rows=exit_allocations,
    )
    if attribution_rejections.get("SELL_FIFO_COVERAGE_GAP", 0):
        raise RuntimeError(
            "raw SELL fills do not have complete deterministic FIFO coverage"
        )
    for item in records:
        item["account_id"] = account_id
    for allocation in exit_allocations:
        allocation["account_id"] = account_id
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
                        stock_code, strategy_key, strategy_version,
                        sample_owner_role,
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
                        :stock_code, :strategy_key, :strategy_version,
                        :sample_owner_role,
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
                        strategy_version = IF(
                            strategy_version='',
                            VALUES(strategy_version),
                            strategy_version
                        ),
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
        for allocation in exit_allocations:
            connection.execute(
                text(
                    """
                    INSERT IGNORE INTO st_forward_exit_allocation_v3 (
                        allocation_id, evidence_id, attribution_status,
                        account_id,
                        stock_code, entry_fill_id, exit_fill_id,
                        exit_order_id, allocation_sequence,
                        allocated_quantity,
                        allocated_gross_cny, allocated_fee_cny,
                        exit_filled_at, allocation_protocol_version,
                        created_at
                    ) VALUES (
                        :allocation_id, :evidence_id,
                        :attribution_status, :account_id,
                        :stock_code, :entry_fill_id, :exit_fill_id,
                        :exit_order_id, :allocation_sequence,
                        :allocated_quantity,
                        :allocated_gross_cny, :allocated_fee_cny,
                        :exit_filled_at, :allocation_protocol_version,
                        :created_at
                    )
                    """
                ),
                {**allocation, "created_at": now},
            )
        allocation_count = _assert_persisted_exit_allocations(
            connection,
            exit_allocations,
            account_id=account_id,
        )
    # The scheduled evidence producer may only bind facts that the existing
    # V2/V3 ledgers have already persisted and matured.  The lazy import avoids
    # a module cycle; this call never creates an intent, order, or fill.
    from server.engine.dynamic_shadow_ledger import (
        bind_pending_dynamic_shadow_trials,
    )

    dynamic_shadow_binding = bind_pending_dynamic_shadow_trials(
        primary_engine,
    )
    _require_valid_dynamic_shadow_binding(dynamic_shadow_binding)
    return {
        "status": "ok",
        "protocol_version": EXECUTED_FORWARD_PROTOCOL,
        "attribution_version": ATTRIBUTION_VERSION,
        "fill_count": len(fills),
        "evidence_count": len(records),
        "exit_allocation_protocol": EXIT_ALLOCATION_PROTOCOL,
        "exit_allocation_count": allocation_count,
        "unattributed_v3_buy_fill_count": len(
            attribution_rejected_fill_ids
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
        "dynamic_shadow_binding": dynamic_shadow_binding,
    }
